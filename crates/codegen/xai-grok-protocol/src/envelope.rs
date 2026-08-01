use serde::{Deserialize, Serialize};

use crate::{
    CapabilityManifest, ClaimTeamResource, ClientId, CommandId, CreateExercise, CreateFinding,
    CreateOperationRun, CreateOperatorSession, CreatePlaybook, CreateTeamWorkItem, EngagementId,
    EventBatch, EventId, EventReadRequest, ExecutionReceipt, Exercise, ExerciseEvidence,
    ExerciseId, Finding, FindingId, FindingStatus, IngressEnvelope, OperationId, OperationRun,
    OperationRunId, OperatorSession, OperatorSessionId, Playbook, PostTeamMessage, ProtocolError,
    ProviderDispatch, ProviderId, RecordExerciseEvidence, RequestId, ResourceClaimId,
    ServiceHealth, ServiceId, SetTeamPresence, TaskId, TaskingPlan, TeamClient, TeamId,
    TeamMessage, TeamPresence, TeamResourceClaim, TeamWorkItem, TeamWorkItemId, TeamWorkItemStatus,
    WorkspaceId,
};

/// Protocol v8 adds durable chunked artifact ingestion. It is
/// not wire-compatible with older clients.
pub const PROTOCOL_VERSION: u32 = 8;
pub const MAX_CONTROL_FRAME_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hello {
    pub protocol_version: u32,
    pub client_name: String,
    pub client_version: String,
    /// Stable reconnect identity for collaborative clients.
    #[serde(default)]
    pub team: Option<TeamClient>,
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
    CreateExercise(CreateExercise),
    CreateOperationRun(CreateOperationRun),
    CreateOperatorSession(CreateOperatorSession),
    CreatePlaybook(CreatePlaybook),
    RecordEvidence(RecordExerciseEvidence),
    CreateFinding(CreateFinding),
    SetFindingStatus {
        finding_id: FindingId,
        status: FindingStatus,
    },
    SetTeamPresence(SetTeamPresence),
    CreateTeamWorkItem(CreateTeamWorkItem),
    AssignTeamWorkItem {
        work_item_id: TeamWorkItemId,
        assignee: Option<ClientId>,
        expected_revision: u64,
    },
    SetTeamWorkItemStatus {
        work_item_id: TeamWorkItemId,
        status: TeamWorkItemStatus,
        expected_revision: u64,
    },
    PostTeamMessage(PostTeamMessage),
    ClaimTeamResource(ClaimTeamResource),
    ReleaseTeamResource {
        claim_id: ResourceClaimId,
        owner: ClientId,
        expected_revision: u64,
    },
    InvokeProvider {
        request_id: RequestId,
        operation_id: OperationId,
        preferred_provider: Option<ProviderId>,
        input: serde_json::Value,
    },
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
    BeginArtifactUpload {
        media_type: String,
        expected_bytes: u64,
        expected_content_hash: Option<String>,
    },
    UploadArtifactChunk {
        upload_id: crate::ArtifactUploadId,
        offset: u64,
        bytes: Vec<u8>,
    },
    InspectArtifactUpload {
        upload_id: crate::ArtifactUploadId,
    },
    CommitArtifactUpload {
        upload_id: crate::ArtifactUploadId,
    },
    AbortArtifactUpload {
        upload_id: crate::ArtifactUploadId,
    },
    ReadArtifact {
        artifact_id: crate::ArtifactId,
        cursor: u64,
        limit: u32,
    },
    ReadEvents(EventReadRequest),
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
    ExerciseCreated {
        exercise: Exercise,
    },
    OperationRunCreated {
        operation_run: OperationRun,
    },
    OperatorSessionCreated {
        session: OperatorSession,
    },
    PlaybookCreated {
        playbook: Playbook,
    },
    EvidenceRecorded {
        evidence: ExerciseEvidence,
    },
    FindingCreated {
        finding: Finding,
    },
    FindingStatusSet {
        finding: Finding,
    },
    TeamPresenceSet {
        presence: TeamPresence,
    },
    TeamWorkItemCreated {
        work_item: TeamWorkItem,
    },
    TeamWorkItemUpdated {
        work_item: TeamWorkItem,
    },
    TeamMessagePosted {
        message: TeamMessage,
    },
    TeamResourceClaimed {
        claim: TeamResourceClaim,
    },
    TeamResourceReleased {
        claim: TeamResourceClaim,
    },
    ProviderInvoked {
        request_id: RequestId,
        provider_id: ProviderId,
        output: serde_json::Value,
        observations: Vec<crate::EvidenceObservation>,
    },
    PlanAccepted {
        engagement_id: EngagementId,
        revision: u32,
    },
    DispatchAccepted {
        request_id: RequestId,
        provider_id: ProviderId,
        receipt: ExecutionReceipt,
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
    ArtifactUploadStarted {
        upload_id: crate::ArtifactUploadId,
        media_type: String,
        expected_bytes: u64,
        expected_content_hash: Option<String>,
        next_offset: u64,
        expires_unix_ms: u64,
    },
    ArtifactUploadState {
        upload_id: crate::ArtifactUploadId,
        media_type: String,
        expected_bytes: u64,
        expected_content_hash: Option<String>,
        next_offset: u64,
        expires_unix_ms: u64,
    },
    ArtifactUploadProgress {
        upload_id: crate::ArtifactUploadId,
        next_offset: u64,
    },
    ArtifactUploadAborted {
        upload_id: crate::ArtifactUploadId,
    },
    ArtifactChunk {
        artifact_id: crate::ArtifactId,
        media_type: String,
        content_hash: String,
        byte_size: u64,
        cursor: u64,
        bytes: Vec<u8>,
        next_cursor: Option<u64>,
    },
    Projection(ProjectionSnapshot),
    Events(EventBatch),
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
    ExerciseCreated {
        exercise: Exercise,
    },
    OperationRunCreated {
        operation_run: OperationRun,
    },
    OperatorSessionCreated {
        session: OperatorSession,
    },
    PlaybookCreated {
        playbook: Playbook,
    },
    EvidenceRecorded {
        evidence: ExerciseEvidence,
    },
    FindingCreated {
        finding: Finding,
    },
    FindingStatusSet {
        finding: Finding,
    },
    TeamPresenceSet {
        presence: TeamPresence,
    },
    TeamWorkItemCreated {
        work_item: TeamWorkItem,
    },
    TeamWorkItemUpdated {
        work_item: TeamWorkItem,
    },
    TeamMessagePosted {
        message: TeamMessage,
    },
    TeamResourceClaimed {
        claim: TeamResourceClaim,
    },
    TeamResourceReleased {
        claim: TeamResourceClaim,
    },
    EngagementAccepted {
        workspace_id: String,
        session_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exercise_id: Option<ExerciseId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        operation_run_id: Option<OperationRunId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        operator_session_id: Option<OperatorSessionId>,
        team_id: Option<crate::TeamId>,
        client_id: Option<crate::ClientId>,
    },
    PlanAccepted {
        revision: u32,
        task_count: u32,
        /// Full typed graph for rebuildable dependency and evidence projections.
        /// Older journals omit this field and remain replayable.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        plan: Option<crate::TaskingPlan>,
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
        #[serde(default, skip_serializing_if = "Option::is_none")]
        task_id: Option<TaskId>,
        media_type: String,
        byte_size: u64,
    },
    ProviderOutput {
        task_id: TaskId,
        artifact_id: crate::ArtifactId,
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
    Engagement {
        engagement_id: EngagementId,
    },
    TaskGraph {
        engagement_id: EngagementId,
    },
    OperatorCatalog {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        workspace_id: Option<WorkspaceId>,
    },
    ExerciseRecord {
        exercise_id: ExerciseId,
    },
    Team {
        team_id: TeamId,
    },
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

    #[test]
    fn older_versions_are_rejected_after_exercise_upgrade() {
        let envelope = CommandEnvelope {
            protocol_version: 2,
            command_id: CommandId::new(),
            causation_id: None,
            deadline_unix_ms: 20,
            command: Command::Shutdown,
        };
        assert_eq!(
            envelope.validate(10).unwrap_err().code,
            crate::ProtocolErrorCode::IncompatibleVersion
        );
    }
}
