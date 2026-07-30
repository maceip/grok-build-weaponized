use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EngagementId(pub String);

impl EngagementId {
    pub fn new() -> Self {
        Self(format!("eng_{}", uuid::Uuid::now_v7().simple()))
    }
}

impl Default for EngagementId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for EngagementId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueuePriority {
    Background = 0,
    AdapterPrewarm = 10,
    Reviewer = 20,
    Interactive = 30,
    DurableStage = 35,
    #[default]
    ExecutorContinuation = 40,
}

impl QueuePriority {
    pub(crate) fn from_i64(value: i64) -> Self {
        match value {
            0 => Self::Background,
            10 => Self::AdapterPrewarm,
            20 => Self::Reviewer,
            30 => Self::Interactive,
            35 => Self::DurableStage,
            40 => Self::ExecutorContinuation,
            _ => Self::ExecutorContinuation,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EngagementStatus {
    #[default]
    Queued,
    Planning,
    Executing,
    Reviewing,
    Correcting,
    Suspended,
    Failed,
    Parked,
    Cancelled,
    Completed,
}

impl EngagementStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Planning => "planning",
            Self::Executing => "executing",
            Self::Reviewing => "reviewing",
            Self::Correcting => "correcting",
            Self::Suspended => "suspended",
            Self::Failed => "failed",
            Self::Parked => "parked",
            Self::Cancelled => "cancelled",
            Self::Completed => "completed",
        }
    }

    pub fn is_active(self) -> bool {
        matches!(
            self,
            Self::Planning | Self::Executing | Self::Reviewing | Self::Correcting
        )
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Failed | Self::Parked | Self::Cancelled | Self::Completed
        )
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "queued" => Self::Queued,
            "planning" => Self::Planning,
            "executing" => Self::Executing,
            "reviewing" => Self::Reviewing,
            "correcting" => Self::Correcting,
            "suspended" => Self::Suspended,
            "failed" => Self::Failed,
            "parked" => Self::Parked,
            "cancelled" => Self::Cancelled,
            "completed" => Self::Completed,
            _ => return None,
        })
    }

    pub(crate) fn permits(self, next: Self) -> bool {
        if self == next {
            return true;
        }
        match self {
            Self::Queued => matches!(
                next,
                Self::Planning | Self::Executing | Self::Suspended | Self::Failed | Self::Cancelled
            ),
            Self::Planning => matches!(
                next,
                Self::Executing | Self::Suspended | Self::Failed | Self::Parked | Self::Cancelled
            ),
            Self::Executing => matches!(
                next,
                Self::Executing
                    | Self::Reviewing
                    | Self::Correcting
                    | Self::Suspended
                    | Self::Failed
                    | Self::Parked
                    | Self::Cancelled
                    | Self::Completed
            ),
            Self::Reviewing => matches!(
                next,
                Self::Correcting
                    | Self::Suspended
                    | Self::Failed
                    | Self::Parked
                    | Self::Cancelled
                    | Self::Completed
            ),
            Self::Correcting => matches!(
                next,
                Self::Executing
                    | Self::Reviewing
                    | Self::Suspended
                    | Self::Failed
                    | Self::Parked
                    | Self::Cancelled
                    | Self::Completed
            ),
            Self::Suspended => matches!(
                next,
                Self::Queued
                    | Self::Planning
                    | Self::Executing
                    | Self::Reviewing
                    | Self::Correcting
                    | Self::Failed
                    | Self::Parked
                    | Self::Cancelled
            ),
            Self::Parked => matches!(next, Self::Queued | Self::Cancelled | Self::Failed),
            Self::Failed | Self::Cancelled | Self::Completed => false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EngagementStage {
    Direct,
    Planner,
    Executor,
    Reviewer,
    Correction,
}

impl EngagementStage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Planner => "planner",
            Self::Executor => "executor",
            Self::Reviewer => "reviewer",
            Self::Correction => "correction",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "direct" => Self::Direct,
            "planner" => Self::Planner,
            "executor" => Self::Executor,
            "reviewer" => Self::Reviewer,
            "correction" => Self::Correction,
            _ => return None,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct EngagementCheckpoint {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan: Option<serde_json::Value>,
    #[serde(default)]
    pub plan_revision: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default)]
    pub task_index: u32,
    #[serde(default)]
    pub task_count: u32,
    #[serde(default)]
    pub correction_count: u8,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub completion_tests: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifact_refs: Vec<ArtifactReference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub adapter_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_plan_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub runtime_request_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub background_jobs: Vec<BackgroundJobCursor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unresolved: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactReference {
    pub artifact_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackgroundJobCursor {
    pub job_id: String,
    pub stdout_cursor: u64,
    pub stderr_cursor: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeAdmissionRecord {
    pub request_id: String,
    pub model_id: String,
    pub adapter_id: Option<String>,
    pub context_plan_hash: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NewEngagement {
    pub session_id: String,
    pub prompt_id: String,
    pub workspace_id: String,
    pub user_request: String,
    #[serde(default)]
    pub priority: QueuePriority,
    #[serde(default)]
    pub checkpoint: EngagementCheckpoint,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EngagementRecord {
    pub engagement_id: EngagementId,
    pub session_id: String,
    pub prompt_id: String,
    pub workspace_id: String,
    pub user_request: String,
    pub status: EngagementStatus,
    pub stage: Option<EngagementStage>,
    pub priority: QueuePriority,
    pub lease_epoch: u64,
    pub lease_owner: Option<String>,
    pub lease_expires_at_ms: Option<i64>,
    pub accepted_at_ms: i64,
    pub updated_at_ms: i64,
    pub checkpoint: EngagementCheckpoint,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EngagementEvent {
    pub engagement_id: EngagementId,
    pub seq: u64,
    pub event_id: String,
    pub kind: String,
    pub lease_epoch: u64,
    pub stage: Option<EngagementStage>,
    pub task_id: Option<String>,
    pub action_id: Option<String>,
    pub evidence_id: Option<String>,
    pub artifact_ref: Option<String>,
    pub payload: serde_json::Value,
    pub content_hash: String,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EngagementSnapshot {
    pub engagement_id: EngagementId,
    pub event_seq: u64,
    pub record: EngagementRecord,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionIdentity {
    pub plan_revision: u32,
    pub task_id: String,
    pub action_id: String,
}

impl ActionIdentity {
    pub fn stable_key(&self, engagement_id: &EngagementId) -> String {
        let mut hasher = blake3::Hasher::new();
        let plan_revision = self.plan_revision.to_string();
        for part in [
            engagement_id.0.as_bytes(),
            plan_revision.as_bytes(),
            self.task_id.as_bytes(),
            self.action_id.as_bytes(),
        ] {
            hasher.update(&(part.len() as u64).to_le_bytes());
            hasher.update(part);
        }
        format!("act_{}", &hasher.finalize().to_hex()[..32])
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionStatus {
    Prepared,
    Dispatched,
    Observed,
    Committed,
    Ambiguous,
}

impl ActionStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::Dispatched => "dispatched",
            Self::Observed => "observed",
            Self::Committed => "committed",
            Self::Ambiguous => "ambiguous",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "prepared" => Self::Prepared,
            "dispatched" => Self::Dispatched,
            "observed" => Self::Observed,
            "committed" => Self::Committed,
            "ambiguous" => Self::Ambiguous,
            _ => return None,
        })
    }

    pub(crate) fn permits(self, next: Self) -> bool {
        if self == next {
            return true;
        }
        matches!(
            (self, next),
            (Self::Prepared, Self::Dispatched)
                | (Self::Dispatched, Self::Observed | Self::Ambiguous)
                | (Self::Observed, Self::Committed | Self::Ambiguous)
                | (
                    Self::Ambiguous,
                    Self::Prepared | Self::Observed | Self::Committed
                )
        )
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionReplayPolicy {
    ReadOnly,
    Idempotent,
    #[default]
    NonIdempotent,
}

impl ActionReplayPolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ReadOnly => "read_only",
            Self::Idempotent => "idempotent",
            Self::NonIdempotent => "non_idempotent",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "read_only" => Self::ReadOnly,
            "idempotent" => Self::Idempotent,
            "non_idempotent" => Self::NonIdempotent,
            _ => return None,
        })
    }

    pub fn is_replayable(&self) -> bool {
        matches!(self, Self::ReadOnly | Self::Idempotent)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActionSpec {
    pub identity: ActionIdentity,
    pub action_kind: String,
    pub command_hash: String,
    #[serde(default)]
    pub replay_policy: ActionReplayPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActionRecord {
    pub stable_key: String,
    pub engagement_id: EngagementId,
    pub identity: ActionIdentity,
    pub action_kind: String,
    pub command_hash: String,
    pub replay_policy: ActionReplayPolicy,
    pub status: ActionStatus,
    pub attempt_count: u32,
    pub job_id: Option<String>,
    pub evidence_id: Option<String>,
    pub payload: Option<serde_json::Value>,
    pub result: Option<serde_json::Value>,
    pub prepared_at_ms: i64,
    pub dispatched_at_ms: Option<i64>,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ActionResolution {
    RetryPrepared,
    Observed {
        result: serde_json::Value,
        evidence_id: Option<String>,
    },
    Committed {
        result: serde_json::Value,
        evidence_id: Option<String>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobKind {
    Terminal,
    Nmap,
    Native,
}

impl JobKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Terminal => "terminal",
            Self::Nmap => "nmap",
            Self::Native => "native",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "terminal" => Self::Terminal,
            "nmap" => Self::Nmap,
            "native" => Self::Native,
            _ => return None,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobLifecycle {
    Running,
    Quiet,
    Stale,
    Lost,
    Completed,
    Failed,
    Cancelled,
}

impl JobLifecycle {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Quiet => "quiet",
            Self::Stale => "stale",
            Self::Lost => "lost",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "running" => Self::Running,
            "quiet" => Self::Quiet,
            "stale" => Self::Stale,
            "lost" => Self::Lost,
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            _ => return None,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JobCheckpoint {
    pub job_id: String,
    pub engagement_id: EngagementId,
    pub action_key: Option<String>,
    pub kind: JobKind,
    pub lifecycle: JobLifecycle,
    pub command_hash: String,
    pub pid: Option<u32>,
    /// OS-backed process birth identity. Unlike a wall-clock timestamp taken
    /// by the parent, this value must be read from the operating system after
    /// spawn and compared again before any recovered process is observed or
    /// signalled.
    pub process_start_identity: Option<String>,
    pub process_started_at_ms: Option<i64>,
    pub process_group_id: Option<i64>,
    pub stdout_artifact: Option<PathBuf>,
    pub stderr_artifact: Option<PathBuf>,
    /// Atomically-written terminal status produced by the process guardian.
    /// This lets a restarted supervisor distinguish a clean exit from an
    /// unresolved disappearance even though it is no longer the OS parent.
    #[serde(default)]
    pub status_artifact: Option<PathBuf>,
    pub stdout_cursor: u64,
    pub stderr_cursor: u64,
    pub last_activity_at_ms: i64,
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub payload: serde_json::Value,
}
