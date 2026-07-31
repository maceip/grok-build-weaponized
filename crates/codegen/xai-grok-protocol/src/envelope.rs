use serde::{Deserialize, Serialize};

use crate::{
    CapabilityManifest, CommandId, EngagementId, EventId, IngressEnvelope, ProtocolError,
    ProviderDispatch, ProviderId, RequestId, ServiceHealth, ServiceId, TaskId, TaskingPlan,
};

pub const PROTOCOL_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hello {
    pub protocol_version: u32,
    pub client_name: String,
    pub client_version: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HelloAck {
    pub protocol_version: u32,
    pub server_name: String,
    pub server_version: String,
    pub capabilities: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum Command {
    Hello(Hello),
    SubmitIngress(IngressEnvelope),
    SubmitPlan(TaskingPlan),
    Dispatch(ProviderDispatch),
    CancelTask {
        engagement_id: EngagementId,
        task_id: TaskId,
        request_id: RequestId,
    },
    RegisterProvider {
        manifest: CapabilityManifest,
    },
    ProviderHeartbeat {
        provider_id: ProviderId,
        generation: u64,
        health: ServiceHealth,
    },
    PutArtifact {
        media_type: String,
        bytes: Vec<u8>,
    },
    QueryProjection(ProjectionQuery),
    Shutdown,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CommandEnvelope {
    pub protocol_version: u32,
    pub command_id: CommandId,
    pub causation_id: Option<String>,
    pub deadline_unix_ms: u64,
    pub command: Command,
}

impl CommandEnvelope {
    pub fn validate(&self, now_unix_ms: u64) -> Result<(), ProtocolError> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(ProtocolError::new(
                crate::ProtocolErrorCode::IncompatibleVersion,
                format!(
                    "protocol version {} is not supported; expected {}",
                    self.protocol_version, PROTOCOL_VERSION
                ),
            ));
        }
        if self.deadline_unix_ms <= now_unix_ms {
            return Err(ProtocolError::new(
                crate::ProtocolErrorCode::DeadlineExceeded,
                "command deadline has elapsed",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "response", rename_all = "snake_case")]
pub enum Response {
    Hello(HelloAck),
    Accepted {
        engagement_id: EngagementId,
        accepted: bool,
    },
    PlanAccepted {
        engagement_id: EngagementId,
        revision: u32,
    },
    DispatchAccepted {
        request_id: RequestId,
        provider_id: ProviderId,
    },
    ProviderRegistered {
        provider_id: ProviderId,
        generation: u64,
        manifest_hash: String,
    },
    ArtifactStored {
        artifact_id: crate::ArtifactId,
        content_hash: String,
        byte_size: u64,
    },
    Projection(ProjectionSnapshot),
    Ack,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResponseEnvelope {
    pub protocol_version: u32,
    pub command_id: CommandId,
    pub response: Result<Response, ProtocolError>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    EngagementAccepted {
        workspace_id: String,
        session_id: String,
    },
    PlanAccepted {
        revision: u32,
        task_count: u32,
    },
    TaskStatus {
        task_id: TaskId,
        status: crate::TaskStatus,
        provider_id: Option<ProviderId>,
    },
    Observation {
        task_id: TaskId,
        observation: crate::EvidenceObservation,
    },
    ProviderState {
        provider_id: ProviderId,
        service_id: ServiceId,
        generation: u64,
        health: ServiceHealth,
    },
    ArtifactAvailable {
        artifact_id: crate::ArtifactId,
        media_type: String,
        byte_size: u64,
    },
    Overload {
        component: String,
        queue_depth: u32,
        capacity: u32,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub protocol_version: u32,
    pub event_id: EventId,
    pub engagement_id: Option<EngagementId>,
    pub sequence: u64,
    pub causation_id: Option<String>,
    pub generation: u64,
    pub observed_unix_ms: u64,
    pub event: Event,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "query", rename_all = "snake_case")]
pub enum ProjectionQuery {
    Engagement { engagement_id: EngagementId },
    Providers,
    Capacity,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProjectionSnapshot {
    pub as_of_sequence: u64,
    pub value: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_rejects_expired_deadline() {
        let envelope = CommandEnvelope {
            protocol_version: PROTOCOL_VERSION,
            command_id: CommandId::new(),
            causation_id: None,
            deadline_unix_ms: 10,
            command: Command::Shutdown,
        };
        assert_eq!(
            envelope.validate(10).unwrap_err().code,
            crate::ProtocolErrorCode::DeadlineExceeded
        );
    }
}
