use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};

use tokio_util::sync::CancellationToken;

const DEFAULT_JOB_CAPACITY: usize = 4096;
const FINISHED_JOB_TTL: Duration = Duration::from_secs(30 * 60);

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
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExecutionJobSnapshot {
    pub job_id: String,
    pub kind: ExecutionJobKind,
    pub lifecycle: ExecutionJobLifecycle,
    pub owner_session_id: Option<String>,
    pub artifact: Option<PathBuf>,
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
    cancel: CancellationToken,
    state: Mutex<ExecutionJobState>,
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
            state.payload = payload;
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
        self.cancel_with_payload(serde_json::Value::Null);
    }

    pub fn cancel_with_payload(&self, payload: serde_json::Value) {
        self.record.cancel.cancel();
        self.finish(ExecutionJobLifecycle::Cancelled, payload, None);
    }

    fn finish(
        &self,
        lifecycle: ExecutionJobLifecycle,
        payload: serde_json::Value,
        error: Option<String>,
    ) {
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
    }

    pub fn snapshot(&self) -> ExecutionJobSnapshot {
        let state = self
            .record
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        ExecutionJobSnapshot {
            job_id: self.record.job_id.clone(),
            kind: self.record.kind,
            lifecycle: state.lifecycle,
            owner_session_id: self.record.owner_session_id.clone(),
            artifact: self.record.artifact.clone(),
            started_at: self.record.started_at,
            elapsed_millis: u64::try_from(self.record.started.elapsed().as_millis())
                .unwrap_or(u64::MAX),
            payload: state.payload.clone(),
            error: state.error.clone(),
        }
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
        let record = Arc::new(ExecutionJobRecord {
            job_id: job_id.clone(),
            kind,
            owner_session_id,
            artifact,
            started_at: SystemTime::now(),
            started: Instant::now(),
            cancel: CancellationToken::new(),
            state: Mutex::new(ExecutionJobState {
                lifecycle: ExecutionJobLifecycle::Running,
                payload: serde_json::Value::Null,
                error: None,
                finished_at: None,
            }),
        });
        jobs.insert(job_id, Arc::clone(&record));
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
