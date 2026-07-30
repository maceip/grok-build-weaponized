//! Shell integration for durable turn and tool-action coordination.
//!
//! The storage and fencing implementation lives in `xai-grok-engagement`.
//! This module binds one accepted prompt to its lease and translates model/tool
//! lifecycle events into typed checkpoints.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};
use xai_grok_engagement::{
    ActionIdentity, ActionSpec, ActionStatus, EngagementCheckpoint, EngagementCoordinator,
    EngagementLease, EngagementStage, EngagementStatus, NewEngagement, QueuePriority,
};
use xai_grok_tools::types::output::ToolRunResult;
use xai_tool_runtime::ToolError;

fn key(session_id: &str, prompt_id: &str) -> String {
    format!("{session_id}\0{prompt_id}")
}

fn registry() -> &'static Mutex<HashMap<String, EngagementLease>> {
    static REGISTRY: OnceLock<Mutex<HashMap<String, EngagementLease>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) struct TurnEngagementGuard {
    key: String,
    owner_session_id: String,
    lease: EngagementLease,
    heartbeat_cancel: tokio_util::sync::CancellationToken,
}

impl Drop for TurnEngagementGuard {
    fn drop(&mut self) {
        self.heartbeat_cancel.cancel();
        xai_grok_tools::native::execution_supervisor::ExecutionSupervisor::global()
            .unbind_engagement(
                &self.owner_session_id,
                &self.lease.engagement_id().0,
                self.lease.lease_epoch(),
            );
        registry()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.key);
        let _ = self
            .lease
            .suspend_detached("turn exited before a terminal engagement checkpoint");
    }
}

pub(crate) async fn begin_turn(
    database_path: PathBuf,
    session_id: &str,
    prompt_id: &str,
    workspace_id: &str,
    user_request: &str,
) -> Result<TurnEngagementGuard, String> {
    let coordinator = EngagementCoordinator::shared(database_path)
        .await
        .map_err(|error| error.to_string())?;
    coordinator
        .recover_expired()
        .await
        .map_err(|error| error.to_string())?;
    let owner = format!("shell:{}:{session_id}", std::process::id());
    let lease = coordinator
        .accept_and_claim(
            NewEngagement {
                session_id: session_id.to_string(),
                prompt_id: prompt_id.to_string(),
                workspace_id: workspace_id.to_string(),
                user_request: user_request.to_string(),
                priority: QueuePriority::Interactive,
                checkpoint: EngagementCheckpoint::default(),
            },
            owner,
            Some(30_000),
        )
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| {
            format!("accepted engagement for prompt {prompt_id} is already leased or parked")
        })?;
    let registry_key = key(session_id, prompt_id);
    let supervisor = xai_grok_tools::native::execution_supervisor::ExecutionSupervisor::global();
    supervisor.bind_engagement(session_id, lease.clone());
    if let Err(error) = supervisor.recover_jobs(&lease).await {
        supervisor.unbind_engagement(session_id, &lease.engagement_id().0, lease.lease_epoch());
        return Err(format!("failed to recover durable execution jobs: {error}"));
    }
    registry()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(registry_key.clone(), lease.clone());
    let heartbeat_cancel = tokio_util::sync::CancellationToken::new();
    let heartbeat_task_cancel = heartbeat_cancel.clone();
    let heartbeat_lease = lease.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = heartbeat_task_cancel.cancelled() => break,
                _ = interval.tick() => {
                    if !heartbeat_lease.heartbeat().await.unwrap_or(false) {
                        break;
                    }
                }
            }
        }
    });
    Ok(TurnEngagementGuard {
        key: registry_key,
        owner_session_id: session_id.to_string(),
        lease,
        heartbeat_cancel,
    })
}

pub(crate) fn lease_for(session_id: &str, prompt_id: &str) -> Option<EngagementLease> {
    registry()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&key(session_id, prompt_id))
        .cloned()
}

pub(crate) async fn transition(
    session_id: &str,
    prompt_id: &str,
    status: EngagementStatus,
    stage: Option<EngagementStage>,
    checkpoint: EngagementCheckpoint,
    detail: serde_json::Value,
) -> Result<(), String> {
    let Some(lease) = lease_for(session_id, prompt_id) else {
        return Ok(());
    };
    lease
        .transition(status, stage, checkpoint, detail)
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

pub(crate) async fn heartbeat(session_id: &str, prompt_id: &str) -> Result<(), String> {
    let Some(lease) = lease_for(session_id, prompt_id) else {
        return Ok(());
    };
    match lease.heartbeat().await {
        Ok(true) => Ok(()),
        Ok(false) => Err("engagement lease heartbeat was rejected".to_string()),
        Err(error) => Err(error.to_string()),
    }
}

pub(crate) async fn record_runtime_admission(
    session_id: &str,
    prompt_id: &str,
    request_id: String,
    model_id: String,
    adapter_id: Option<String>,
    context_plan_hash: String,
) -> Result<(), String> {
    let lease = lease_for(session_id, prompt_id).ok_or_else(|| {
        format!("no active durable engagement lease for runtime admission {session_id}/{prompt_id}")
    })?;
    lease
        .record_runtime_admission(xai_grok_engagement::RuntimeAdmissionRecord {
            request_id,
            model_id,
            adapter_id,
            context_plan_hash,
        })
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

pub(crate) async fn complete(
    session_id: &str,
    prompt_id: &str,
    mut checkpoint: EngagementCheckpoint,
    result: serde_json::Value,
) -> Result<(), String> {
    checkpoint.result = Some(result.clone());
    transition(
        session_id,
        prompt_id,
        EngagementStatus::Completed,
        checkpoint_stage(&checkpoint),
        checkpoint,
        serde_json::json!({"result": result}),
    )
    .await
}

fn checkpoint_stage(checkpoint: &EngagementCheckpoint) -> Option<EngagementStage> {
    if checkpoint.correction_count > 0 {
        Some(EngagementStage::Correction)
    } else if checkpoint.plan.is_some() {
        Some(EngagementStage::Reviewer)
    } else {
        Some(EngagementStage::Direct)
    }
}

#[derive(Debug)]
pub(crate) enum DurableActionDecision {
    Execute {
        action_key: String,
    },
    Replay {
        action_key: String,
        outcome: Box<Result<ToolRunResult, ToolError>>,
    },
    Ambiguous {
        action_key: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "outcome", content = "value", rename_all = "snake_case")]
enum StoredToolOutcome {
    Success(ToolRunResult),
    Error(ToolError),
}

impl StoredToolOutcome {
    fn from_result(result: &Result<ToolRunResult, ToolError>) -> Result<Self, String> {
        match result {
            Ok(result) => serde_json::to_value(result)
                .and_then(serde_json::from_value)
                .map(Self::Success)
                .map_err(|error| error.to_string()),
            Err(error) => serde_json::to_value(error)
                .and_then(serde_json::from_value)
                .map(Self::Error)
                .map_err(|error| error.to_string()),
        }
    }

    fn into_result(self) -> Result<ToolRunResult, ToolError> {
        match self {
            Self::Success(result) => Ok(result),
            Self::Error(error) => Err(error),
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn prepare_action(
    session_id: &str,
    prompt_id: &str,
    plan_revision: u32,
    task_id: &str,
    action_id: &str,
    tool_name: &str,
    arguments: &serde_json::Value,
    is_read_only: bool,
) -> Result<Option<DurableActionDecision>, String> {
    let Some(lease) = lease_for(session_id, prompt_id) else {
        return Ok(None);
    };
    let canonical_arguments = canonical_json(arguments);
    let mut hasher = blake3::Hasher::new();
    hasher.update(tool_name.as_bytes());
    hasher.update(&[0]);
    hasher.update(canonical_arguments.to_string().as_bytes());
    let command_hash = hasher.finalize().to_hex().to_string();
    let action = lease
        .prepare_action(ActionSpec {
            identity: ActionIdentity {
                plan_revision,
                task_id: task_id.to_string(),
                action_id: action_id.to_string(),
            },
            action_kind: tool_name.to_string(),
            command_hash,
            replay_policy: if is_read_only {
                xai_grok_engagement::ActionReplayPolicy::ReadOnly
            } else {
                xai_grok_engagement::ActionReplayPolicy::NonIdempotent
            },
            payload: Some(serde_json::json!({
                "tool_name": tool_name,
                "arguments": canonical_arguments,
                "read_only": is_read_only,
            })),
        })
        .await
        .map_err(|error| error.to_string())?;
    match action.status {
        ActionStatus::Prepared => {
            xai_grok_tools::native::execution_supervisor::ExecutionSupervisor::global()
                .bind_action(session_id, action_id, &action.stable_key);
            Ok(Some(DurableActionDecision::Execute {
                action_key: action.stable_key,
            }))
        }
        ActionStatus::Observed | ActionStatus::Committed => {
            let result = action
                .result
                .ok_or_else(|| {
                    format!(
                        "durable action {} is {:?} without a stored result",
                        action.stable_key, action.status
                    )
                })
                .and_then(|value| {
                    serde_json::from_value::<StoredToolOutcome>(value)
                        .map_err(|error| error.to_string())
                })?
                .into_result();
            Ok(Some(DurableActionDecision::Replay {
                action_key: action.stable_key,
                outcome: Box::new(result),
            }))
        }
        ActionStatus::Ambiguous if is_read_only => {
            let action = lease
                .resolve_ambiguous_action(
                    &action.stable_key,
                    xai_grok_engagement::ActionResolution::RetryPrepared,
                )
                .await
                .map_err(|error| error.to_string())?;
            xai_grok_tools::native::execution_supervisor::ExecutionSupervisor::global()
                .bind_action(session_id, action_id, &action.stable_key);
            Ok(Some(DurableActionDecision::Execute {
                action_key: action.stable_key,
            }))
        }
        ActionStatus::Dispatched | ActionStatus::Ambiguous => {
            Ok(Some(DurableActionDecision::Ambiguous {
                action_key: action.stable_key,
            }))
        }
    }
}

pub(crate) async fn mark_action_dispatched(
    session_id: &str,
    prompt_id: &str,
    action_key: &str,
) -> Result<(), String> {
    let Some(lease) = lease_for(session_id, prompt_id) else {
        return Ok(());
    };
    lease
        .update_action(action_key, ActionStatus::Dispatched, None, None, None)
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

pub(crate) async fn observe_action(
    session_id: &str,
    prompt_id: &str,
    action_key: &str,
    result: &Result<ToolRunResult, ToolError>,
) -> Result<(), String> {
    update_action_result(
        session_id,
        prompt_id,
        action_key,
        ActionStatus::Observed,
        result,
    )
    .await
}

pub(crate) async fn commit_action(
    session_id: &str,
    prompt_id: &str,
    action_key: &str,
    result: &Result<ToolRunResult, ToolError>,
) -> Result<(), String> {
    update_action_result(
        session_id,
        prompt_id,
        action_key,
        ActionStatus::Committed,
        result,
    )
    .await
}

async fn update_action_result(
    session_id: &str,
    prompt_id: &str,
    action_key: &str,
    status: ActionStatus,
    result: &Result<ToolRunResult, ToolError>,
) -> Result<(), String> {
    let Some(lease) = lease_for(session_id, prompt_id) else {
        return Ok(());
    };
    let stored = StoredToolOutcome::from_result(result)?;
    let value = serde_json::to_value(stored).map_err(|error| error.to_string())?;
    lease
        .update_action(action_key, status, None, None, Some(value))
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn canonical_json(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let mut entries = map.iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|left, right| left.0.cmp(right.0));
            serde_json::Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key.clone(), canonical_json(value)))
                    .collect(),
            )
        }
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(canonical_json).collect())
        }
        other => other.clone(),
    }
}
