use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::broadcast;
use xai_grok_engagement::{EngagementLease, JobLifecycle};

use super::job_registry::{
    DurableExecutionBinding, ExecutionJobHandle, ExecutionJobKind, ExecutionJobRegistry,
};

const SUPERVISOR_TICK: Duration = Duration::from_millis(100);

/// Process-wide coordination point for long-lived execution jobs.
///
/// Child ownership stays with the backend that spawned it, but terminal,
/// Nmap, and future native drivers share one registry and one lifecycle clock.
/// This avoids one periodic timer per terminal session while preserving
/// backend-local process-tree teardown and stream handling.
pub struct ExecutionSupervisor {
    maintenance_tx: broadcast::Sender<()>,
    clock_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    engagement_bindings: Mutex<HashMap<String, EngagementLease>>,
    action_bindings: Mutex<HashMap<(String, String), String>>,
}

impl ExecutionSupervisor {
    pub fn global() -> &'static Self {
        static SUPERVISOR: OnceLock<ExecutionSupervisor> = OnceLock::new();
        SUPERVISOR.get_or_init(|| {
            let (maintenance_tx, _) = broadcast::channel(1);
            ExecutionSupervisor {
                maintenance_tx,
                clock_task: Mutex::new(None),
                engagement_bindings: Mutex::new(HashMap::new()),
                action_bindings: Mutex::new(HashMap::new()),
            }
        })
    }

    pub fn jobs(&self) -> &'static ExecutionJobRegistry {
        ExecutionJobRegistry::global()
    }

    /// Bind the active durable engagement to the owner identity already
    /// propagated through every terminal request.
    ///
    /// Rebinding replaces only an older epoch. This prevents a late guard from
    /// detaching the lease installed by a recovered turn.
    pub fn bind_engagement(&self, owner_session_id: impl Into<String>, lease: EngagementLease) {
        let owner_session_id = owner_session_id.into();
        let mut bindings = self
            .engagement_bindings
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let replace = bindings
            .get(&owner_session_id)
            .is_none_or(|current| current.lease_epoch() <= lease.lease_epoch());
        if replace {
            bindings.insert(owner_session_id, lease);
        }
    }

    pub fn unbind_engagement(&self, owner_session_id: &str, engagement_id: &str, lease_epoch: u64) {
        self.engagement_bindings
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|owner, lease| {
                owner != owner_session_id
                    || lease.engagement_id().0 != engagement_id
                    || lease.lease_epoch() != lease_epoch
            });
        self.action_bindings
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|(owner, _), _| owner != owner_session_id);
    }

    /// Associate the model's stable tool-call identity with the durable action
    /// key. Process drivers use this to link a spawned job back to the exact
    /// plan/task/action identity rather than an attempt-local process ID.
    pub fn bind_action(
        &self,
        owner_session_id: impl Into<String>,
        tool_call_id: impl Into<String>,
        action_key: impl Into<String>,
    ) {
        self.action_bindings
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(
                (owner_session_id.into(), tool_call_id.into()),
                action_key.into(),
            );
    }

    pub fn unbind_action(&self, owner_session_id: &str, tool_call_id: &str) {
        self.action_bindings
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&(owner_session_id.to_string(), tool_call_id.to_string()));
    }

    #[allow(clippy::too_many_arguments)]
    pub fn register_job(
        &self,
        job_id: String,
        kind: ExecutionJobKind,
        owner_session_id: Option<String>,
        artifact: Option<PathBuf>,
        tool_call_id: Option<&str>,
        command_hash: String,
    ) -> Result<ExecutionJobHandle, String> {
        let durable = owner_session_id.as_ref().and_then(|owner| {
            let lease = self
                .engagement_bindings
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(owner)
                .cloned()?;
            let action_key = tool_call_id.and_then(|call_id| {
                self.action_bindings
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .get(&(owner.clone(), call_id.to_string()))
                    .cloned()
            });
            Some(DurableExecutionBinding {
                lease,
                action_key,
                command_hash,
            })
        });
        self.jobs()
            .register_with_durability(job_id, kind, owner_session_id, artifact, durable)
    }

    /// Reconcile jobs that survived in SQLite but lost their in-process child
    /// owner during a crash or restart.
    ///
    /// The recorded PID is retained for diagnostics, never signalled or
    /// reattached without an OS-backed birth identity. This is the safe side
    /// of the PID-reuse boundary: recovered jobs become explicit `Lost`
    /// tombstones while their spool artifacts and cursors remain available.
    pub async fn recover_jobs(&self, lease: &EngagementLease) -> Result<usize, String> {
        let jobs = lease
            .recoverable_jobs()
            .await
            .map_err(|error| error.to_string())?;
        let mut recovered = 0;
        for mut checkpoint in jobs {
            checkpoint.lifecycle = JobLifecycle::Lost;
            checkpoint.last_activity_at_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
                .unwrap_or(0);
            let previous_payload = std::mem::take(&mut checkpoint.payload);
            checkpoint.payload = serde_json::json!({
                "recovery": "supervisor_restarted",
                "reason": "process birth identity unavailable; refusing PID-only reattachment",
                "recorded_pid": checkpoint.pid,
                "stdout_cursor": checkpoint.stdout_cursor,
                "stderr_cursor": checkpoint.stderr_cursor,
                "previous_payload": previous_payload,
            });
            lease
                .checkpoint_job(checkpoint.clone())
                .await
                .map_err(|error| error.to_string())?;
            self.jobs().restore_lost(checkpoint)?;
            recovered += 1;
        }
        Ok(recovered)
    }

    /// Subscribe a backend to the single process-wide maintenance clock.
    ///
    /// This must be called from within a Tokio runtime. The clock is started
    /// exactly once per process runtime even when many terminal sessions
    /// subscribe concurrently. Tests may create and drop several Tokio
    /// runtimes, so a finished clock task is restarted on the current runtime.
    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        let mut clock_task = self
            .clock_task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if clock_task
            .as_ref()
            .is_none_or(tokio::task::JoinHandle::is_finished)
        {
            let maintenance_tx = self.maintenance_tx.clone();
            *clock_task = Some(tokio::spawn(async move {
                let mut ticker = tokio::time::interval(SUPERVISOR_TICK);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    ticker.tick().await;
                    if maintenance_tx.receiver_count() == 0 {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    let _ = maintenance_tx.send(());
                }
            }));
        }
        drop(clock_task);
        self.maintenance_tx.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xai_grok_engagement::{
        EngagementCheckpoint, EngagementCoordinator, JobCheckpoint, JobKind, NewEngagement,
        QueuePriority,
    };

    #[tokio::test]
    async fn subscriptions_share_the_process_wide_clock() {
        let supervisor = ExecutionSupervisor::global();
        let mut first = supervisor.subscribe();
        let mut second = supervisor.subscribe();
        tokio::time::timeout(Duration::from_secs(1), first.recv())
            .await
            .expect("first subscriber should receive maintenance")
            .expect("maintenance channel should remain open");
        tokio::time::timeout(Duration::from_secs(1), second.recv())
            .await
            .expect("second subscriber should receive maintenance")
            .expect("maintenance channel should remain open");
        assert!(
            !supervisor
                .clock_task
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_ref()
                .expect("clock task should be installed")
                .is_finished()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn restart_recovery_preserves_artifacts_but_never_reattaches_by_pid_alone() {
        let directory = tempfile::tempdir().unwrap();
        let coordinator = EngagementCoordinator::shared(directory.path().join("state.sqlite"))
            .await
            .unwrap();
        let lease = coordinator
            .accept_and_claim(
                NewEngagement {
                    session_id: format!("session-{}", uuid::Uuid::now_v7()),
                    prompt_id: "prompt".to_string(),
                    workspace_id: "/workspace".to_string(),
                    user_request: "recover process".to_string(),
                    priority: QueuePriority::Interactive,
                    checkpoint: EngagementCheckpoint::default(),
                },
                "worker",
                Some(60_000),
            )
            .await
            .unwrap()
            .unwrap();
        let job_id = format!("recover-{}", uuid::Uuid::now_v7());
        let artifact = directory.path().join("output.log");
        lease
            .checkpoint_job(JobCheckpoint {
                job_id: job_id.clone(),
                engagement_id: lease.engagement_id().clone(),
                action_key: None,
                kind: JobKind::Terminal,
                lifecycle: JobLifecycle::Running,
                command_hash: "hash".to_string(),
                pid: Some(777),
                process_started_at_ms: Some(1),
                process_group_id: Some(777),
                stdout_artifact: Some(artifact.clone()),
                stderr_artifact: None,
                stdout_cursor: 123,
                stderr_cursor: 4,
                last_activity_at_ms: 1,
                exit_code: None,
                payload: serde_json::json!({"phase": "running"}),
            })
            .await
            .unwrap();

        let supervisor = ExecutionSupervisor::global();
        assert_eq!(supervisor.recover_jobs(&lease).await.unwrap(), 1);
        let durable = coordinator.job(job_id.clone()).await.unwrap().unwrap();
        assert_eq!(durable.lifecycle, JobLifecycle::Lost);
        assert_eq!(durable.pid, Some(777));
        assert_eq!(durable.stdout_artifact, Some(artifact));
        assert_eq!(durable.stdout_cursor, 123);
        let restored = supervisor.jobs().get(&job_id).unwrap().snapshot();
        assert_eq!(
            restored.lifecycle,
            super::super::job_registry::ExecutionJobLifecycle::Lost
        );
    }
}
