use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio_util::sync::CancellationToken;
use xai_grok_engagement::{
    EngagementLease, JobCheckpoint, JobKind, JobLifecycle as DurableJobLifecycle,
};

const DEFAULT_JOB_CAPACITY: usize = 4096;
const FINISHED_JOB_TTL: Duration = Duration::from_secs(30 * 60);
const DURABLE_CHECKPOINT_INTERVAL: Duration = Duration::from_millis(500);
const DURABLE_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const DURABLE_QUIET_AFTER: Duration = Duration::from_secs(30);
const DURABLE_STALE_AFTER: Duration = Duration::from_secs(5 * 60);

#[derive(Clone)]
pub struct DurableExecutionBinding {
    pub lease: EngagementLease,
    pub action_key: Option<String>,
    pub command_hash: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionJobKind {
    Terminal,
    Nmap,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionJobLifecycle {
    Running,
    Completed,
    Failed,
    Cancelled,
    Lost,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExecutionJobSnapshot {
    pub job_id: String,
    pub kind: ExecutionJobKind,
    pub lifecycle: ExecutionJobLifecycle,
    /// True when this registry entry was reconstructed from a durable
    /// checkpoint rather than registered by a live in-process driver.
    pub restored_from_checkpoint: bool,
    pub command_hash: Option<String>,
    pub owner_session_id: Option<String>,
    pub artifact: Option<PathBuf>,
    pub stdout_artifact: Option<PathBuf>,
    pub stderr_artifact: Option<PathBuf>,
    pub status_artifact: Option<PathBuf>,
    pub stdout_cursor: u64,
    pub stderr_cursor: u64,
    pub started_at: SystemTime,
    pub elapsed_millis: u64,
    pub payload: serde_json::Value,
    pub error: Option<String>,
}

struct ExecutionJobState {
    lifecycle: ExecutionJobLifecycle,
    payload: serde_json::Value,
    error: Option<String>,
    finished_at: Option<Instant>,
}

struct ExecutionJobRecord {
    job_id: String,
    kind: ExecutionJobKind,
    owner_session_id: Option<String>,
    artifact: Option<PathBuf>,
    started_at: SystemTime,
    started: Instant,
    elapsed_offset_millis: u64,
    cancel: CancellationToken,
    state: Mutex<ExecutionJobState>,
    durable: Option<Arc<DurableExecutionState>>,
    recovered_process: Option<RecoveredProcessIdentity>,
    restored_from_checkpoint: bool,
}

#[derive(Debug, Clone)]
struct RecoveredProcessIdentity {
    pid: u32,
    start_identity: String,
    process_group_id: Option<i64>,
}

struct DurableCheckpointState {
    checkpoint: JobCheckpoint,
    last_persisted: Instant,
}

struct DurableExecutionState {
    lease: EngagementLease,
    checkpoint: Mutex<DurableCheckpointState>,
    heartbeat_cancel: CancellationToken,
    heartbeat_started: AtomicBool,
}

impl DurableExecutionState {
    fn start_heartbeat(self: &Arc<Self>) {
        if self.heartbeat_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let state = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(DURABLE_HEARTBEAT_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = state.heartbeat_cancel.cancelled() => break,
                    _ = interval.tick() => {
                        if !state.lease.heartbeat().await.unwrap_or(false) {
                            break;
                        }
                    }
                }
            }
        });
    }

    fn checkpoint_detached(&self, force: bool) {
        let checkpoint = {
            let mut state = self
                .checkpoint
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !force && state.last_persisted.elapsed() < DURABLE_CHECKPOINT_INTERVAL {
                return;
            }
            state.last_persisted = Instant::now();
            state.checkpoint.clone()
        };
        let _ = self.lease.checkpoint_job_detached(checkpoint);
    }
}

#[derive(Clone)]
pub struct ExecutionJobHandle {
    record: Arc<ExecutionJobRecord>,
}

impl ExecutionJobHandle {
    pub fn job_id(&self) -> &str {
        &self.record.job_id
    }

    pub fn cancellation(&self) -> CancellationToken {
        self.record.cancel.clone()
    }

    /// Install the immutable child-owned spool paths before the spawn intent is
    /// committed. The child writes these files directly, so output continues
    /// to advance even if the interactive parent exits.
    pub fn configure_durable_artifacts(
        &self,
        stdout: Option<PathBuf>,
        stderr: Option<PathBuf>,
        status: Option<PathBuf>,
    ) {
        let Some(durable) = self.record.durable.as_ref() else {
            return;
        };
        let mut state = durable
            .checkpoint
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.checkpoint.stdout_artifact = stdout;
        state.checkpoint.stderr_artifact = stderr;
        state.checkpoint.status_artifact = status;
    }

    pub fn is_cancelled(&self) -> bool {
        self.record.cancel.is_cancelled()
    }

    pub fn update(&self, payload: serde_json::Value) {
        let mut state = self
            .record
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.lifecycle == ExecutionJobLifecycle::Running {
            merge_json(&mut state.payload, payload.clone());
            drop(state);
            self.update_durable(|checkpoint| {
                merge_json(&mut checkpoint.payload, payload);
                checkpoint.last_activity_at_ms = now_ms();
                checkpoint.lifecycle = DurableJobLifecycle::Running;
            });
        }
    }

    /// Persist the spawn intent before the subprocess exists. A caller must
    /// await this before spawning so a crash cannot leave an untracked child.
    pub async fn persist_spawn_intent(&self) -> Result<(), String> {
        let Some(durable) = self.record.durable.as_ref() else {
            return Ok(());
        };
        let checkpoint = durable
            .checkpoint
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .checkpoint
            .clone();
        durable
            .lease
            .checkpoint_job(checkpoint.clone())
            .await
            .map_err(|error| error.to_string())?;
        if let Some(action_key) = checkpoint.action_key {
            durable
                .lease
                .update_action(
                    action_key,
                    xai_grok_engagement::ActionStatus::Dispatched,
                    Some(checkpoint.job_id),
                    None,
                    None,
                )
                .await
                .map_err(|error| error.to_string())?;
        }
        durable.start_heartbeat();
        Ok(())
    }

    /// Attach the exact process identity immediately after spawn and before a
    /// background handle is returned to the caller.
    pub async fn attach_process(
        &self,
        pid: Option<u32>,
        process_group_id: Option<i64>,
    ) -> Result<(), String> {
        let Some(durable) = self.record.durable.as_ref() else {
            return Ok(());
        };
        let os_identity = match pid {
            Some(pid) => Some(
                tokio::task::spawn_blocking(move || capture_process_identity(pid))
                    .await
                    .map_err(|error| format!("process identity task failed: {error}"))??,
            ),
            None => None,
        };
        let checkpoint = {
            let mut state = durable
                .checkpoint
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.checkpoint.pid = pid;
            state.checkpoint.process_start_identity = os_identity
                .as_ref()
                .map(|identity| identity.start_identity.clone());
            state.checkpoint.process_group_id = os_identity
                .as_ref()
                .and_then(|identity| identity.process_group_id)
                .or(process_group_id);
            state.checkpoint.process_started_at_ms = pid.map(|_| now_ms());
            state.checkpoint.last_activity_at_ms = now_ms();
            state.last_persisted = Instant::now();
            state.checkpoint.clone()
        };
        durable
            .lease
            .checkpoint_job(checkpoint)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    pub fn update_streams(
        &self,
        stdout_cursor: u64,
        stderr_cursor: u64,
        payload: serde_json::Value,
    ) {
        let durable_payload = payload.clone();
        self.update_durable(|checkpoint| {
            let progressed = checkpoint.stdout_cursor != stdout_cursor
                || checkpoint.stderr_cursor != stderr_cursor;
            checkpoint.stdout_cursor = stdout_cursor;
            checkpoint.stderr_cursor = stderr_cursor;
            if progressed {
                checkpoint.last_activity_at_ms = now_ms();
                checkpoint.lifecycle = DurableJobLifecycle::Running;
            }
            merge_json(&mut checkpoint.payload, durable_payload);
        });
        let mut state = self
            .record
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.lifecycle == ExecutionJobLifecycle::Running {
            merge_json(&mut state.payload, payload);
        }
    }

    /// Classify a live but inactive durable process without changing the
    /// model-facing running state. Only actual stream/payload progress resets
    /// the activity clock.
    pub fn refresh_liveness(&self) {
        let Some(durable) = self.record.durable.as_ref() else {
            return;
        };
        let changed = {
            let mut state = durable
                .checkpoint
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !matches!(
                state.checkpoint.lifecycle,
                DurableJobLifecycle::Running
                    | DurableJobLifecycle::Quiet
                    | DurableJobLifecycle::Stale
            ) {
                false
            } else {
                let inactive_ms = now_ms().saturating_sub(state.checkpoint.last_activity_at_ms);
                let inactive =
                    Duration::from_millis(u64::try_from(inactive_ms).unwrap_or(u64::MAX));
                let lifecycle = durable_lifecycle_for_inactivity(inactive);
                if state.checkpoint.lifecycle == lifecycle {
                    false
                } else {
                    state.checkpoint.lifecycle = lifecycle;
                    true
                }
            }
        };
        if changed {
            durable.checkpoint_detached(true);
        }
    }

    pub fn complete(&self, payload: serde_json::Value) {
        self.finish(ExecutionJobLifecycle::Completed, payload, None);
    }

    pub fn fail(&self, error: impl Into<String>) {
        self.fail_with_payload(error, serde_json::Value::Null);
    }

    pub fn fail_with_payload(&self, error: impl Into<String>, payload: serde_json::Value) {
        self.finish(ExecutionJobLifecycle::Failed, payload, Some(error.into()));
    }

    pub fn cancel(&self) {
        if let Some(process) = self.record.recovered_process.as_ref()
            && process_identity_matches(process)
        {
            terminate_recovered_process(process);
        }
        self.cancel_with_payload(serde_json::Value::Null);
    }

    pub fn cancel_with_payload(&self, payload: serde_json::Value) {
        self.record.cancel.cancel();
        self.finish(ExecutionJobLifecycle::Cancelled, payload, None);
    }

    pub fn start_recovered_monitor(&self) {
        let job = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut missing_process_observations = 0_u8;
            loop {
                interval.tick().await;
                if job.is_cancelled() {
                    break;
                }
                let Some(process) = job.record.recovered_process.as_ref() else {
                    break;
                };
                let status_artifact = job
                    .record
                    .durable
                    .as_ref()
                    .and_then(|durable| durable.checkpoint.lock().ok())
                    .and_then(|state| state.checkpoint.status_artifact.clone());
                if let Some(status) = status_artifact.as_deref().and_then(read_guardian_status) {
                    let stdout_cursor = job
                        .record
                        .durable
                        .as_ref()
                        .and_then(|durable| durable.checkpoint.lock().ok())
                        .and_then(|state| artifact_len(state.checkpoint.stdout_artifact.as_deref()))
                        .unwrap_or(0);
                    let stderr_cursor = job
                        .record
                        .durable
                        .as_ref()
                        .and_then(|durable| durable.checkpoint.lock().ok())
                        .and_then(|state| artifact_len(state.checkpoint.stderr_artifact.as_deref()))
                        .unwrap_or(0);
                    let payload = serde_json::json!({
                        "recovery": "guardian_status_observed",
                        "pid": process.pid,
                        "exit_code": status.exit_code,
                        "signal": status.signal,
                        "stdout_cursor": stdout_cursor,
                        "stderr_cursor": stderr_cursor,
                    });
                    job.update_streams(
                        stdout_cursor,
                        stderr_cursor,
                        serde_json::json!({
                            "recovery": "guardian_status_observed",
                            "stdout_cursor": stdout_cursor,
                            "stderr_cursor": stderr_cursor,
                        }),
                    );
                    if status.exit_code == Some(0) && status.signal.is_none() {
                        job.finish(ExecutionJobLifecycle::Completed, payload, None);
                    } else {
                        job.finish(
                            ExecutionJobLifecycle::Failed,
                            payload,
                            Some("recovered process exited unsuccessfully".to_string()),
                        );
                    }
                    break;
                }
                if !process_identity_matches(process) {
                    missing_process_observations = missing_process_observations.saturating_add(1);
                    if missing_process_observations < 2 {
                        continue;
                    }
                    job.finish(
                        ExecutionJobLifecycle::Lost,
                        serde_json::json!({
                            "recovery": "reattached_process_exited",
                            "pid": process.pid,
                            "stdout_cursor": artifact_len(
                                job.record
                                    .durable
                                    .as_ref()
                                    .and_then(|durable| durable.checkpoint.lock().ok())
                                    .and_then(|state| state.checkpoint.stdout_artifact.clone())
                                    .as_deref()
                            ),
                        }),
                        Some(
                            "reattached process exited without an observable child status"
                                .to_string(),
                        ),
                    );
                    break;
                }
                missing_process_observations = 0;
                let (stdout_cursor, stderr_cursor) =
                    if let Some(durable) = job.record.durable.as_ref() {
                        let state = durable
                            .checkpoint
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        (
                            artifact_len(state.checkpoint.stdout_artifact.as_deref())
                                .unwrap_or(state.checkpoint.stdout_cursor),
                            artifact_len(state.checkpoint.stderr_artifact.as_deref())
                                .unwrap_or(state.checkpoint.stderr_cursor),
                        )
                    } else {
                        (0, 0)
                    };
                job.update_streams(
                    stdout_cursor,
                    stderr_cursor,
                    serde_json::json!({
                        "recovery": "reattached",
                        "pid": process.pid,
                        "stdout_cursor": stdout_cursor,
                        "stderr_cursor": stderr_cursor,
                    }),
                );
                job.refresh_liveness();
            }
        });
    }

    fn finish(
        &self,
        lifecycle: ExecutionJobLifecycle,
        payload: serde_json::Value,
        error: Option<String>,
    ) {
        let durable_payload = payload.clone();
        let mut state = self
            .record
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.lifecycle != ExecutionJobLifecycle::Running {
            return;
        }
        state.lifecycle = lifecycle;
        state.payload = payload;
        state.error = error;
        state.finished_at = Some(Instant::now());
        drop(state);
        let durable_lifecycle = match lifecycle {
            ExecutionJobLifecycle::Running => DurableJobLifecycle::Running,
            ExecutionJobLifecycle::Completed => DurableJobLifecycle::Completed,
            ExecutionJobLifecycle::Failed => DurableJobLifecycle::Failed,
            ExecutionJobLifecycle::Cancelled => DurableJobLifecycle::Cancelled,
            ExecutionJobLifecycle::Lost => DurableJobLifecycle::Lost,
        };
        self.update_durable_force(|checkpoint| {
            checkpoint.lifecycle = durable_lifecycle;
            checkpoint.last_activity_at_ms = now_ms();
            checkpoint.payload = durable_payload;
            checkpoint.exit_code = checkpoint
                .payload
                .get("exit_code")
                .and_then(serde_json::Value::as_i64)
                .and_then(|value| i32::try_from(value).ok());
        });
        if let Some(durable) = self.record.durable.as_ref() {
            durable.heartbeat_cancel.cancel();
        }
    }

    pub fn snapshot(&self) -> ExecutionJobSnapshot {
        let state = self
            .record
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let (
            command_hash,
            stdout_artifact,
            stderr_artifact,
            status_artifact,
            stdout_cursor,
            stderr_cursor,
        ) = self
            .record
            .durable
            .as_ref()
            .and_then(|durable| durable.checkpoint.lock().ok())
            .map(|state| {
                (
                    Some(state.checkpoint.command_hash.clone()),
                    state.checkpoint.stdout_artifact.clone(),
                    state.checkpoint.stderr_artifact.clone(),
                    state.checkpoint.status_artifact.clone(),
                    state.checkpoint.stdout_cursor,
                    state.checkpoint.stderr_cursor,
                )
            })
            .unwrap_or_else(|| {
                (
                    None,
                    self.record.artifact.clone(),
                    None,
                    None,
                    artifact_len(self.record.artifact.as_deref()).unwrap_or(0),
                    0,
                )
            });
        ExecutionJobSnapshot {
            job_id: self.record.job_id.clone(),
            kind: self.record.kind,
            lifecycle: state.lifecycle,
            restored_from_checkpoint: self.record.restored_from_checkpoint,
            command_hash,
            owner_session_id: self.record.owner_session_id.clone(),
            artifact: self.record.artifact.clone(),
            stdout_artifact,
            stderr_artifact,
            status_artifact,
            stdout_cursor,
            stderr_cursor,
            started_at: self.record.started_at,
            elapsed_millis: u64::try_from(self.record.started.elapsed().as_millis())
                .unwrap_or(u64::MAX)
                .saturating_add(self.record.elapsed_offset_millis),
            payload: state.payload.clone(),
            error: state.error.clone(),
        }
    }

    fn update_durable(&self, update: impl FnOnce(&mut JobCheckpoint)) {
        let Some(durable) = self.record.durable.as_ref() else {
            return;
        };
        {
            let mut state = durable
                .checkpoint
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            update(&mut state.checkpoint);
        }
        durable.checkpoint_detached(false);
    }

    fn update_durable_force(&self, update: impl FnOnce(&mut JobCheckpoint)) {
        let Some(durable) = self.record.durable.as_ref() else {
            return;
        };
        {
            let mut state = durable
                .checkpoint
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            update(&mut state.checkpoint);
        }
        durable.checkpoint_detached(true);
    }
}

#[derive(serde::Deserialize)]
struct GuardianStatus {
    exit_code: Option<i32>,
    #[serde(default)]
    signal: Option<String>,
}

fn read_guardian_status(path: &std::path::Path) -> Option<GuardianStatus> {
    let bytes = std::fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn merge_json(current: &mut serde_json::Value, update: serde_json::Value) {
    match (current, update) {
        (serde_json::Value::Object(current), serde_json::Value::Object(update)) => {
            current.extend(update);
        }
        (current, update) => *current = update,
    }
}

fn durable_lifecycle_for_inactivity(inactive: Duration) -> DurableJobLifecycle {
    if inactive >= DURABLE_STALE_AFTER {
        DurableJobLifecycle::Stale
    } else if inactive >= DURABLE_QUIET_AFTER {
        DurableJobLifecycle::Quiet
    } else {
        DurableJobLifecycle::Running
    }
}

pub struct ExecutionJobRegistry {
    capacity: usize,
    jobs: Mutex<HashMap<String, Arc<ExecutionJobRecord>>>,
}

impl ExecutionJobRegistry {
    pub fn global() -> &'static Self {
        static REGISTRY: OnceLock<ExecutionJobRegistry> = OnceLock::new();
        REGISTRY.get_or_init(|| ExecutionJobRegistry {
            capacity: DEFAULT_JOB_CAPACITY,
            jobs: Mutex::new(HashMap::new()),
        })
    }

    pub fn register(
        &self,
        job_id: String,
        kind: ExecutionJobKind,
        owner_session_id: Option<String>,
        artifact: Option<PathBuf>,
    ) -> Result<ExecutionJobHandle, String> {
        self.register_with_durability(job_id, kind, owner_session_id, artifact, None)
    }

    pub fn register_with_durability(
        &self,
        job_id: String,
        kind: ExecutionJobKind,
        owner_session_id: Option<String>,
        artifact: Option<PathBuf>,
        durable: Option<DurableExecutionBinding>,
    ) -> Result<ExecutionJobHandle, String> {
        self.cleanup_finished(FINISHED_JOB_TTL);
        let mut jobs = self
            .jobs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if jobs.contains_key(&job_id) {
            return Err(format!("execution job `{job_id}` already exists"));
        }
        if jobs.len() >= self.capacity {
            return Err(format!(
                "execution job registry is full (capacity {})",
                self.capacity
            ));
        }
        let started_at = SystemTime::now();
        let durable = durable.map(|binding| {
            Arc::new(DurableExecutionState {
                checkpoint: Mutex::new(DurableCheckpointState {
                    checkpoint: JobCheckpoint {
                        job_id: job_id.clone(),
                        engagement_id: binding.lease.engagement_id().clone(),
                        action_key: binding.action_key,
                        kind: match kind {
                            ExecutionJobKind::Terminal => JobKind::Terminal,
                            ExecutionJobKind::Nmap => JobKind::Nmap,
                        },
                        lifecycle: DurableJobLifecycle::Running,
                        command_hash: binding.command_hash,
                        pid: None,
                        process_start_identity: None,
                        process_started_at_ms: None,
                        process_group_id: None,
                        stdout_artifact: artifact.clone(),
                        stderr_artifact: None,
                        status_artifact: None,
                        stdout_cursor: 0,
                        stderr_cursor: 0,
                        last_activity_at_ms: system_time_ms(started_at),
                        exit_code: None,
                        payload: serde_json::Value::Null,
                    },
                    last_persisted: Instant::now(),
                }),
                lease: binding.lease,
                heartbeat_cancel: CancellationToken::new(),
                heartbeat_started: AtomicBool::new(false),
            })
        });
        let record = Arc::new(ExecutionJobRecord {
            job_id: job_id.clone(),
            kind,
            owner_session_id,
            artifact,
            started_at,
            started: Instant::now(),
            elapsed_offset_millis: 0,
            cancel: CancellationToken::new(),
            state: Mutex::new(ExecutionJobState {
                lifecycle: ExecutionJobLifecycle::Running,
                payload: serde_json::Value::Null,
                error: None,
                finished_at: None,
            }),
            durable,
            recovered_process: None,
            restored_from_checkpoint: false,
        });
        jobs.insert(job_id, Arc::clone(&record));
        Ok(ExecutionJobHandle { record })
    }

    /// Restore a durable record whose original supervising process is gone.
    ///
    /// We deliberately do not signal or attach to the recorded PID: without an
    /// OS-backed birth identity, PID reuse makes that unsafe. The artifact
    /// paths and cursors remain queryable while the lifecycle is explicitly
    /// `Lost`.
    pub fn restore_lost(&self, checkpoint: JobCheckpoint) -> Result<ExecutionJobHandle, String> {
        self.cleanup_finished(FINISHED_JOB_TTL);
        let mut jobs = self
            .jobs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(record) = jobs.get(&checkpoint.job_id) {
            return Ok(ExecutionJobHandle {
                record: Arc::clone(record),
            });
        }
        if jobs.len() >= self.capacity {
            return Err(format!(
                "execution job registry is full (capacity {})",
                self.capacity
            ));
        }
        let started_at = checkpoint
            .process_started_at_ms
            .and_then(|millis| u64::try_from(millis).ok())
            .map(|millis| UNIX_EPOCH + Duration::from_millis(millis))
            .unwrap_or_else(SystemTime::now);
        let elapsed_offset_millis = SystemTime::now()
            .duration_since(started_at)
            .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
            .unwrap_or(0);
        let owner_session_id = checkpoint_owner_session_id(&checkpoint);
        let record = Arc::new(ExecutionJobRecord {
            job_id: checkpoint.job_id.clone(),
            kind: match checkpoint.kind {
                JobKind::Terminal => ExecutionJobKind::Terminal,
                JobKind::Nmap => ExecutionJobKind::Nmap,
                JobKind::Native => ExecutionJobKind::Terminal,
            },
            owner_session_id,
            artifact: checkpoint
                .stdout_artifact
                .clone()
                .or(checkpoint.stderr_artifact.clone()),
            started_at,
            started: Instant::now(),
            elapsed_offset_millis,
            cancel: CancellationToken::new(),
            state: Mutex::new(ExecutionJobState {
                lifecycle: ExecutionJobLifecycle::Lost,
                payload: checkpoint.payload,
                error: Some(
                    "supervisor restarted; process identity was preserved but cannot be safely reattached"
                        .to_string(),
                ),
                finished_at: Some(Instant::now()),
            }),
            durable: None,
            recovered_process: None,
            restored_from_checkpoint: true,
        });
        jobs.insert(checkpoint.job_id, Arc::clone(&record));
        Ok(ExecutionJobHandle { record })
    }

    pub fn restore_running(
        &self,
        checkpoint: JobCheckpoint,
        lease: EngagementLease,
    ) -> Result<ExecutionJobHandle, String> {
        let pid = checkpoint
            .pid
            .ok_or_else(|| format!("durable job {} has no PID", checkpoint.job_id))?;
        let start_identity = checkpoint.process_start_identity.clone().ok_or_else(|| {
            format!(
                "durable job {} has no OS process birth identity",
                checkpoint.job_id
            )
        })?;
        let recovered_process = RecoveredProcessIdentity {
            pid,
            start_identity,
            process_group_id: checkpoint.process_group_id,
        };
        if !process_identity_matches(&recovered_process) {
            return Err(format!(
                "durable job {} no longer matches PID {pid}",
                checkpoint.job_id
            ));
        }
        self.cleanup_finished(FINISHED_JOB_TTL);
        let mut jobs = self
            .jobs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(record) = jobs.get(&checkpoint.job_id) {
            return Ok(ExecutionJobHandle {
                record: Arc::clone(record),
            });
        }
        if jobs.len() >= self.capacity {
            return Err(format!(
                "execution job registry is full (capacity {})",
                self.capacity
            ));
        }
        let started_at = checkpoint
            .process_started_at_ms
            .and_then(|millis| u64::try_from(millis).ok())
            .map(|millis| UNIX_EPOCH + Duration::from_millis(millis))
            .unwrap_or_else(SystemTime::now);
        let elapsed_offset_millis = SystemTime::now()
            .duration_since(started_at)
            .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
            .unwrap_or(0);
        let owner_session_id = checkpoint_owner_session_id(&checkpoint);
        let durable = Arc::new(DurableExecutionState {
            checkpoint: Mutex::new(DurableCheckpointState {
                checkpoint: checkpoint.clone(),
                last_persisted: Instant::now(),
            }),
            lease,
            heartbeat_cancel: CancellationToken::new(),
            heartbeat_started: AtomicBool::new(false),
        });
        durable.start_heartbeat();
        let record = Arc::new(ExecutionJobRecord {
            job_id: checkpoint.job_id.clone(),
            kind: match checkpoint.kind {
                JobKind::Terminal => ExecutionJobKind::Terminal,
                JobKind::Nmap => ExecutionJobKind::Nmap,
                JobKind::Native => ExecutionJobKind::Terminal,
            },
            owner_session_id,
            artifact: checkpoint
                .stdout_artifact
                .clone()
                .or(checkpoint.stderr_artifact.clone()),
            started_at,
            started: Instant::now(),
            elapsed_offset_millis,
            cancel: CancellationToken::new(),
            state: Mutex::new(ExecutionJobState {
                lifecycle: ExecutionJobLifecycle::Running,
                payload: checkpoint.payload,
                error: None,
                finished_at: None,
            }),
            durable: Some(durable),
            recovered_process: Some(recovered_process),
            restored_from_checkpoint: true,
        });
        jobs.insert(checkpoint.job_id, Arc::clone(&record));
        Ok(ExecutionJobHandle { record })
    }

    pub fn get(&self, job_id: &str) -> Option<ExecutionJobHandle> {
        self.jobs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(job_id)
            .cloned()
            .map(|record| ExecutionJobHandle { record })
    }

    pub fn cancel(&self, job_id: &str) -> bool {
        let Some(job) = self.get(job_id) else {
            return false;
        };
        job.cancel();
        true
    }

    pub fn running_count(&self, kind: ExecutionJobKind) -> usize {
        self.jobs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .filter(|record| {
                record.kind == kind
                    && record
                        .state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .lifecycle
                        == ExecutionJobLifecycle::Running
            })
            .count()
    }

    pub fn list(
        &self,
        kind: Option<ExecutionJobKind>,
        cursor: usize,
        limit: usize,
    ) -> (Vec<ExecutionJobSnapshot>, Option<usize>) {
        let mut jobs = self
            .jobs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .filter(|record| kind.is_none_or(|kind| record.kind == kind))
            .cloned()
            .collect::<Vec<_>>();
        jobs.sort_by_key(|record| record.started_at);
        let limit = limit.clamp(1, 100);
        let snapshots = jobs
            .iter()
            .skip(cursor)
            .take(limit)
            .cloned()
            .map(|record| ExecutionJobHandle { record }.snapshot())
            .collect::<Vec<_>>();
        let next = (cursor.saturating_add(snapshots.len()) < jobs.len())
            .then_some(cursor.saturating_add(snapshots.len()));
        (snapshots, next)
    }

    pub fn cleanup_finished(&self, ttl: Duration) {
        self.jobs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|_, record| {
                let state = record
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                state
                    .finished_at
                    .is_none_or(|finished| finished.elapsed() < ttl)
            });
    }
}

fn now_ms() -> i64 {
    system_time_ms(SystemTime::now())
}

fn checkpoint_owner_session_id(checkpoint: &JobCheckpoint) -> Option<String> {
    checkpoint
        .payload
        .get("owner_session_id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

fn artifact_len(path: Option<&std::path::Path>) -> Option<u64> {
    path.and_then(|path| std::fs::metadata(path).ok())
        .map(|metadata| metadata.len())
}

fn capture_process_identity(pid: u32) -> Result<RecoveredProcessIdentity, String> {
    #[cfg(unix)]
    {
        let start_identity = ps_value(pid, "lstart")?;
        let process_group_id = ps_value(pid, "pgid")?
            .parse::<i64>()
            .map_err(|error| format!("invalid process group for PID {pid}: {error}"))?;
        Ok(RecoveredProcessIdentity {
            pid,
            start_identity,
            process_group_id: Some(process_group_id),
        })
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        Err("OS-backed process birth identity is unavailable on this platform".to_string())
    }
}

#[cfg(unix)]
fn ps_value(pid: u32, field: &str) -> Result<String, String> {
    let output = std::process::Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", &format!("{field}=")])
        .output()
        .map_err(|error| format!("failed to inspect PID {pid}: {error}"))?;
    if !output.status.success() {
        return Err(format!("PID {pid} is not observable"));
    }
    let value = String::from_utf8(output.stdout)
        .map_err(|error| format!("process identity for PID {pid} is not UTF-8: {error}"))?
        .trim()
        .to_string();
    if value.is_empty() {
        return Err(format!("PID {pid} returned an empty {field} identity"));
    }
    Ok(value)
}

fn process_identity_matches(expected: &RecoveredProcessIdentity) -> bool {
    capture_process_identity(expected.pid).is_ok_and(|actual| {
        actual.start_identity == expected.start_identity
            && actual.process_group_id == expected.process_group_id
    })
}

fn terminate_recovered_process(process: &RecoveredProcessIdentity) {
    #[cfg(unix)]
    {
        use nix::sys::signal::{Signal, kill, killpg};
        use nix::unistd::Pid;

        let pid = Pid::from_raw(i32::try_from(process.pid).unwrap_or(i32::MAX));
        if process.process_group_id == Some(i64::from(process.pid)) {
            let _ = killpg(pid, Signal::SIGTERM);
        } else {
            let _ = kill(pid, Signal::SIGTERM);
        }
    }
}

fn system_time_ms(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use xai_grok_engagement::{
        ActionIdentity, ActionSpec, ActionStatus, EngagementCheckpoint, EngagementCoordinator,
        JobLifecycle, NewEngagement, QueuePriority,
    };

    #[test]
    fn durable_liveness_distinguishes_running_quiet_and_stale() {
        assert_eq!(
            durable_lifecycle_for_inactivity(Duration::from_secs(29)),
            DurableJobLifecycle::Running
        );
        assert_eq!(
            durable_lifecycle_for_inactivity(Duration::from_secs(30)),
            DurableJobLifecycle::Quiet
        );
        assert_eq!(
            durable_lifecycle_for_inactivity(Duration::from_secs(5 * 60)),
            DurableJobLifecycle::Stale
        );
    }

    #[test]
    fn registry_unifies_lifecycle_cancellation_and_pagination() {
        let registry = ExecutionJobRegistry {
            capacity: 4,
            jobs: Mutex::new(HashMap::new()),
        };
        let first = registry
            .register(
                "terminal-1".to_string(),
                ExecutionJobKind::Terminal,
                Some("session".to_string()),
                Some(PathBuf::from("/tmp/output")),
            )
            .unwrap();
        let second = registry
            .register(
                "nmap-1".to_string(),
                ExecutionJobKind::Nmap,
                Some("session".to_string()),
                None,
            )
            .unwrap();
        first.complete(serde_json::json!({"exit_code": 0}));
        second.cancel();
        assert!(second.is_cancelled());
        let (page, next) = registry.list(None, 0, 1);
        assert_eq!(page.len(), 1);
        assert_eq!(next, Some(1));
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn durable_job_checkpoints_spawn_identity_stream_cursors_and_completion() {
        use std::os::unix::process::CommandExt as _;

        let directory = tempfile::tempdir().unwrap();
        let coordinator = EngagementCoordinator::shared(directory.path().join("state.sqlite"))
            .await
            .unwrap();
        let lease = coordinator
            .accept_and_claim(
                NewEngagement {
                    session_id: "session-a".to_string(),
                    prompt_id: "prompt-a".to_string(),
                    workspace_id: "/workspace".to_string(),
                    user_request: "run a long process".to_string(),
                    priority: QueuePriority::Interactive,
                    checkpoint: EngagementCheckpoint::default(),
                },
                "worker",
                Some(60_000),
            )
            .await
            .unwrap()
            .unwrap();
        let registry = ExecutionJobRegistry {
            capacity: 4,
            jobs: Mutex::new(HashMap::new()),
        };
        let action = lease
            .prepare_action(ActionSpec {
                identity: ActionIdentity {
                    plan_revision: 0,
                    task_id: "direct".to_string(),
                    action_id: "tool-call".to_string(),
                },
                action_kind: "bash".to_string(),
                command_hash: "command-hash".to_string(),
                replay_policy: xai_grok_engagement::ActionReplayPolicy::NonIdempotent,
                payload: None,
            })
            .await
            .unwrap();
        lease
            .update_action(
                &action.stable_key,
                ActionStatus::Dispatched,
                None,
                None,
                None,
            )
            .await
            .unwrap();
        let artifact = directory.path().join("stdout.log");
        let job = registry
            .register_with_durability(
                "durable-terminal".to_string(),
                ExecutionJobKind::Terminal,
                Some("session-a".to_string()),
                Some(artifact.clone()),
                Some(DurableExecutionBinding {
                    lease: lease.clone(),
                    action_key: Some(action.stable_key.clone()),
                    command_hash: "command-hash".to_string(),
                }),
            )
            .unwrap();

        job.persist_spawn_intent().await.unwrap();
        let mut child = std::process::Command::new("sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .unwrap();
        let pid = child.id();
        job.attach_process(Some(pid), Some(i64::from(pid)))
            .await
            .unwrap();
        let running = coordinator
            .job("durable-terminal".to_string())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(running.lifecycle, JobLifecycle::Running);
        assert_eq!(running.pid, Some(pid));
        assert!(running.process_start_identity.is_some());
        assert_eq!(running.process_group_id, Some(i64::from(pid)));
        assert_eq!(running.stdout_artifact, Some(artifact));
        let linked_action = coordinator
            .action(action.stable_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(linked_action.job_id.as_deref(), Some("durable-terminal"));
        assert_eq!(linked_action.attempt_count, 1);

        tokio::time::sleep(DURABLE_CHECKPOINT_INTERVAL).await;
        job.update_streams(
            8192,
            512,
            serde_json::json!({"completed": false, "total_bytes": 8704}),
        );
        job.complete(serde_json::json!({"exit_code": 0, "total_bytes": 8704}));

        let completed = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let checkpoint = coordinator
                    .job("durable-terminal".to_string())
                    .await
                    .unwrap()
                    .unwrap();
                if checkpoint.lifecycle == JobLifecycle::Completed {
                    break checkpoint;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("detached terminal checkpoint should reach SQLite");
        assert_eq!(completed.stdout_cursor, 8192);
        assert_eq!(completed.stderr_cursor, 512);
        assert_eq!(completed.exit_code, Some(0));
        assert!(coordinator.recoverable_jobs().await.unwrap().is_empty());
        let _ = child.kill();
        let _ = child.wait();
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn recovery_reattaches_only_matching_os_process_identity() {
        use std::os::unix::process::CommandExt as _;

        let directory = tempfile::tempdir().unwrap();
        let coordinator = EngagementCoordinator::shared(directory.path().join("state.sqlite"))
            .await
            .unwrap();
        let lease = coordinator
            .accept_and_claim(
                NewEngagement {
                    session_id: "recovery-session".to_string(),
                    prompt_id: "recovery-prompt".to_string(),
                    workspace_id: "/workspace".to_string(),
                    user_request: "recover a long process".to_string(),
                    priority: QueuePriority::ExecutorContinuation,
                    checkpoint: EngagementCheckpoint::default(),
                },
                "recovery-worker",
                Some(60_000),
            )
            .await
            .unwrap()
            .unwrap();
        let mut child = std::process::Command::new("sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .unwrap();
        let pid = child.id();
        let identity = capture_process_identity(pid).unwrap();
        let artifact = directory.path().join("recovered.stdout");
        std::fs::write(&artifact, b"already spooled").unwrap();
        let checkpoint = JobCheckpoint {
            job_id: "recovered-job".to_string(),
            engagement_id: lease.engagement_id().clone(),
            action_key: None,
            kind: JobKind::Terminal,
            lifecycle: DurableJobLifecycle::Running,
            command_hash: "stable-command-and-environment-hash".to_string(),
            pid: Some(pid),
            process_start_identity: Some(identity.start_identity),
            process_started_at_ms: Some(now_ms()),
            process_group_id: identity.process_group_id,
            stdout_artifact: Some(artifact),
            stderr_artifact: None,
            status_artifact: None,
            stdout_cursor: 15,
            stderr_cursor: 0,
            last_activity_at_ms: now_ms(),
            exit_code: None,
            payload: serde_json::json!({"phase": "running"}),
        };
        lease.checkpoint_job(checkpoint.clone()).await.unwrap();
        let registry = ExecutionJobRegistry {
            capacity: 4,
            jobs: Mutex::new(HashMap::new()),
        };
        let recovered = registry.restore_running(checkpoint, lease.clone()).unwrap();
        assert_eq!(
            recovered.snapshot().lifecycle,
            ExecutionJobLifecycle::Running
        );
        recovered.start_recovered_monitor();

        child.kill().unwrap();
        child.wait().unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if recovered.snapshot().lifecycle == ExecutionJobLifecycle::Lost {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("recovered process monitor should detect exit");
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn recovered_guardian_status_preserves_exit_result_and_artifact_cursors() {
        use std::os::unix::process::CommandExt as _;

        let directory = tempfile::tempdir().unwrap();
        let coordinator = EngagementCoordinator::shared(directory.path().join("state.sqlite"))
            .await
            .unwrap();
        let lease = coordinator
            .accept_and_claim(
                NewEngagement {
                    session_id: "guardian-recovery-session".to_string(),
                    prompt_id: "guardian-recovery-prompt".to_string(),
                    workspace_id: "/workspace".to_string(),
                    user_request: "recover child-owned artifacts".to_string(),
                    priority: QueuePriority::ExecutorContinuation,
                    checkpoint: EngagementCheckpoint::default(),
                },
                "recovery-worker",
                Some(60_000),
            )
            .await
            .unwrap()
            .unwrap();
        let mut child = std::process::Command::new("sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .unwrap();
        let pid = child.id();
        let identity = capture_process_identity(pid).unwrap();
        let stdout = directory.path().join("guardian.stdout");
        let stderr = directory.path().join("guardian.stderr");
        let status = directory.path().join("guardian.status.json");
        std::fs::write(&stdout, b"complete stdout").unwrap();
        std::fs::write(&stderr, b"warning").unwrap();
        let checkpoint = JobCheckpoint {
            job_id: "guardian-recovered-job".to_string(),
            engagement_id: lease.engagement_id().clone(),
            action_key: None,
            kind: JobKind::Terminal,
            lifecycle: DurableJobLifecycle::Running,
            command_hash: "stable-command-and-environment-hash".to_string(),
            pid: Some(pid),
            process_start_identity: Some(identity.start_identity),
            process_started_at_ms: Some(now_ms()),
            process_group_id: identity.process_group_id,
            stdout_artifact: Some(stdout),
            stderr_artifact: Some(stderr),
            status_artifact: Some(status.clone()),
            stdout_cursor: 0,
            stderr_cursor: 0,
            last_activity_at_ms: now_ms(),
            exit_code: None,
            payload: serde_json::json!({"command": "fixture"}),
        };
        lease.checkpoint_job(checkpoint.clone()).await.unwrap();
        let registry = ExecutionJobRegistry {
            capacity: 4,
            jobs: Mutex::new(HashMap::new()),
        };
        let recovered = registry.restore_running(checkpoint, lease).unwrap();
        recovered.start_recovered_monitor();
        std::fs::write(&status, br#"{"exit_code":0,"signal":null}"#).unwrap();

        let completed = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let snapshot = recovered.snapshot();
                if snapshot.lifecycle == ExecutionJobLifecycle::Completed {
                    break snapshot;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("guardian status should resolve recovered lifecycle");
        assert_eq!(completed.stdout_cursor, 15);
        assert_eq!(completed.stderr_cursor, 7);
        assert_eq!(completed.payload["exit_code"], 0);
        let _ = child.kill();
        let _ = child.wait();
    }
}
