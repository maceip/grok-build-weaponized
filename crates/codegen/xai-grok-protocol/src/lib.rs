//! Zero-I/O contracts shared by Grok control-plane clients and services.
//!
//! This crate deliberately performs no filesystem, network, process, model, or
//! database operations. It defines the versioned language used by the TUI,
//! CLI, integrations, control plane, and execution providers.

mod capability;
mod envelope;
mod error;
mod execution;
mod exercise;
mod ids;
mod ingress;
mod profile;
mod tasking;
mod team;

pub use capability::{
    ArtifactContract, CancellationSemantics, CapabilityManifest, ConcurrencyProfile,
    OperationDescriptor, Platform, ProviderKind, RecoverySemantics, ServiceHealth, VersionRange,
};
pub use envelope::{
    Command, CommandEnvelope, Event, EventEnvelope, Hello, HelloAck, MAX_CONTROL_FRAME_BYTES,
    PROTOCOL_VERSION, ProjectionQuery, ProjectionSnapshot, Response, ResponseEnvelope,
};
pub use error::{ProtocolError, ProtocolErrorCode};
pub use execution::{DeferredTask, DetachedJob, ExecutionReceipt, InteractiveSession};
pub use exercise::{
    CreateExercise, CreateFinding, CreateOperationRun, CreateOperatorSession, CreatePlaybook,
    Exercise, ExerciseEvidence, ExerciseObjective, ExerciseRecord, ExerciseStatus, Finding,
    FindingSeverity, FindingStatus, OperationRun, OperationRunStatus, OperatorCatalog,
    OperatorSession, OperatorSessionStatus, Playbook, PlaybookStep, RecordExerciseEvidence,
    ScopeTarget, TargetKind, parse_scope_targets,
};
pub use ids::{
    ArtifactId, ClientId, CommandId, EngagementId, EventId, EvidenceId, ExerciseId, FindingId,
    OperationId, OperationRunId, OperatorSessionId, PlaybookId, ProfileId, ProviderId, RequestId,
    ServiceId, TargetId, TaskId, TeamId, TurnId, WorkspaceId,
};
pub use ingress::{BuzzIngress, IngressEnvelope, IngressSource, QmIngress};
pub use profile::{
    ArtifactBinding, DeploymentTarget, DiagnosticSeverity, GuiProfile, GuiRenderer, LibcTarget,
    ProfileDiagnostic, ProfileProviderRequirement, RUNTIME_PROFILE_SCHEMA_VERSION, RuntimeProfile,
    RuntimeResourceLimits,
};
pub use tasking::{
    CapabilityRequirement, CompletionDecision, CompletionTest, EvidenceObservation, ExecutionMode,
    ExecutionTask, ProviderDispatch, TaskStatus, TaskingPlan,
};
pub use team::{EventBatch, EventReadRequest, MAX_EVENT_BATCH, MAX_EVENT_WAIT_MS, TeamClient};
