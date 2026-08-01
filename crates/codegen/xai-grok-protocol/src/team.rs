use serde::{Deserialize, Serialize};

use crate::{
    ChannelId, ClientId, EngagementId, EventEnvelope, ExerciseId, MessageId, OperationRunId,
    OperatorSessionId, ProtocolError, ProtocolErrorCode, ResourceClaimId, TeamId, TeamWorkItemId,
    WorkspaceId,
};

pub const MAX_EVENT_BATCH: u32 = 512;
pub const MAX_EVENT_WAIT_MS: u32 = 30_000;

/// Stable identity supplied by reconnecting CLI and GUI clients.
///
/// This is correlation metadata, not an authorization or permission model.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamClient {
    pub team_id: TeamId,
    pub client_id: ClientId,
    #[serde(default)]
    pub display_name: Option<String>,
}

impl TeamClient {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.team_id.as_str().trim().is_empty() || self.client_id.as_str().trim().is_empty() {
            return Err(ProtocolError::new(
                ProtocolErrorCode::InvalidEnvelope,
                "team and client identifiers must not be empty",
            ));
        }
        if self
            .display_name
            .as_deref()
            .is_some_and(|name| name.trim().is_empty() || name.len() > 128)
        {
            return Err(ProtocolError::new(
                ProtocolErrorCode::InvalidEnvelope,
                "client display name must contain 1 to 128 bytes",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamPresenceState {
    #[default]
    Online,
    Away,
    Offline,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetTeamPresence {
    pub client: TeamClient,
    pub state: TeamPresenceState,
    pub workspace_id: Option<WorkspaceId>,
    pub exercise_id: Option<ExerciseId>,
    pub operation_run_id: Option<OperationRunId>,
    pub session_id: Option<OperatorSessionId>,
}

impl SetTeamPresence {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.client.validate()?;
        if self.operation_run_id.is_some() && self.exercise_id.is_none() {
            return Err(ProtocolError::new(
                ProtocolErrorCode::InvalidEnvelope,
                "team presence operation run requires an exercise",
            ));
        }
        if self.session_id.is_some() && self.exercise_id.is_none() {
            return Err(ProtocolError::new(
                ProtocolErrorCode::InvalidEnvelope,
                "team presence session requires an exercise",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamPresence {
    pub client: TeamClient,
    pub state: TeamPresenceState,
    pub workspace_id: Option<WorkspaceId>,
    pub exercise_id: Option<ExerciseId>,
    pub operation_run_id: Option<OperationRunId>,
    pub session_id: Option<OperatorSessionId>,
    pub last_seen_unix_ms: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamWorkItemStatus {
    #[default]
    Open,
    InProgress,
    Blocked,
    Completed,
    Cancelled,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateTeamWorkItem {
    pub team_id: TeamId,
    pub exercise_id: Option<ExerciseId>,
    pub operation_run_id: Option<OperationRunId>,
    pub title: String,
    pub objective: String,
    pub assignee: Option<ClientId>,
}

impl CreateTeamWorkItem {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        required("team work item team_id", self.team_id.as_str())?;
        required("team work item title", &self.title)?;
        required("team work item objective", &self.objective)?;
        if self.operation_run_id.is_some() && self.exercise_id.is_none() {
            return Err(ProtocolError::new(
                ProtocolErrorCode::InvalidEnvelope,
                "team work item operation run requires an exercise",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamWorkItem {
    pub work_item_id: TeamWorkItemId,
    pub team_id: TeamId,
    pub exercise_id: Option<ExerciseId>,
    pub operation_run_id: Option<OperationRunId>,
    pub title: String,
    pub objective: String,
    pub assignee: Option<ClientId>,
    pub status: TeamWorkItemStatus,
    pub revision: u64,
    pub created_unix_ms: u64,
    pub updated_unix_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostTeamMessage {
    pub team_id: TeamId,
    pub channel_id: ChannelId,
    pub sender: ClientId,
    pub body: String,
    pub reply_to: Option<MessageId>,
}

impl PostTeamMessage {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        required("team message team_id", self.team_id.as_str())?;
        required("team message channel_id", self.channel_id.as_str())?;
        required("team message sender", self.sender.as_str())?;
        required("team message body", &self.body)?;
        if self.body.len() > 64 * 1024 {
            return Err(ProtocolError::new(
                ProtocolErrorCode::InvalidEnvelope,
                "team message exceeds 64 KiB",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamMessage {
    pub message_id: MessageId,
    pub team_id: TeamId,
    pub channel_id: ChannelId,
    pub sender: ClientId,
    pub body: String,
    pub reply_to: Option<MessageId>,
    pub sent_unix_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimTeamResource {
    pub team_id: TeamId,
    pub owner: ClientId,
    pub resource_key: String,
    pub lease_ms: u32,
}

impl ClaimTeamResource {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        required("team resource team_id", self.team_id.as_str())?;
        required("team resource owner", self.owner.as_str())?;
        required("team resource key", &self.resource_key)?;
        if self.lease_ms == 0 || self.lease_ms > 24 * 60 * 60 * 1_000 {
            return Err(ProtocolError::new(
                ProtocolErrorCode::InvalidEnvelope,
                "team resource lease must be between 1 ms and 24 hours",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamResourceClaim {
    pub claim_id: ResourceClaimId,
    pub team_id: TeamId,
    pub owner: ClientId,
    pub resource_key: String,
    pub revision: u64,
    pub claimed_unix_ms: u64,
    pub expires_unix_ms: u64,
    pub released_unix_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamProjection {
    pub team_id: TeamId,
    pub presence: Vec<TeamPresence>,
    pub work_items: Vec<TeamWorkItem>,
    pub messages: Vec<TeamMessage>,
    pub resource_claims: Vec<TeamResourceClaim>,
}

fn required(field: &str, value: &str) -> Result<(), ProtocolError> {
    if value.trim().is_empty() {
        return Err(ProtocolError::new(
            ProtocolErrorCode::InvalidEnvelope,
            format!("{field} must not be empty"),
        ));
    }
    Ok(())
}

/// Durable event-cursor request used by reconnecting clients and shell scripts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventReadRequest {
    /// Return events strictly after this durable sequence.
    pub after_sequence: u64,
    /// Bounded batch size. The daemon rejects values outside its protocol cap.
    pub maximum_events: u32,
    /// Optional long-poll interval. Zero performs a non-blocking read.
    pub wait_ms: u32,
    /// Optional server-side engagement filter.
    #[serde(default)]
    pub engagement_id: Option<EngagementId>,
}

impl EventReadRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.maximum_events == 0 || self.maximum_events > MAX_EVENT_BATCH {
            return Err(ProtocolError::new(
                ProtocolErrorCode::InvalidEnvelope,
                format!("maximum_events must be between 1 and {MAX_EVENT_BATCH}"),
            ));
        }
        if self.wait_ms > MAX_EVENT_WAIT_MS {
            return Err(ProtocolError::new(
                ProtocolErrorCode::InvalidEnvelope,
                format!("wait_ms must not exceed {MAX_EVENT_WAIT_MS}"),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventBatch {
    pub events: Vec<EventEnvelope>,
    /// Last durable sequence present when the batch was produced.
    pub high_watermark: u64,
    /// Cursor to use for the next request. It never moves backward.
    pub next_sequence: u64,
    /// True when this batch reaches the high watermark.
    pub caught_up: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_reads_are_strictly_bounded() {
        assert!(
            EventReadRequest {
                after_sequence: 0,
                maximum_events: MAX_EVENT_BATCH,
                wait_ms: MAX_EVENT_WAIT_MS,
                engagement_id: None,
            }
            .validate()
            .is_ok()
        );
        assert!(
            EventReadRequest {
                after_sequence: 0,
                maximum_events: MAX_EVENT_BATCH + 1,
                wait_ms: 0,
                engagement_id: None,
            }
            .validate()
            .is_err()
        );
    }
}
