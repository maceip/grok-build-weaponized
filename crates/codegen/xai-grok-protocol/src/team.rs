use serde::{Deserialize, Serialize};

use crate::{ClientId, EngagementId, EventEnvelope, ProtocolError, ProtocolErrorCode, TeamId};

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
