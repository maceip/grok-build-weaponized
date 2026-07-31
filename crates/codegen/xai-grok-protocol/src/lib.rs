//! Zero-I/O contracts shared by Grok control-plane clients and services.
//!
//! This crate deliberately performs no filesystem, network, process, model, or
//! database operations. It defines the versioned language used by the TUI,
//! CLI, integrations, control plane, and execution providers.

mod capability;
mod envelope;
mod error;
mod ids;
mod ingress;
mod tasking;

pub use capability::{
    ArtifactContract, CancellationSemantics, CapabilityManifest, ConcurrencyProfile,
    OperationDescriptor, Platform, ProviderKind, RecoverySemantics, ServiceHealth, VersionRange,
};
pub use envelope::{
    Command, CommandEnvelope, Event, EventEnvelope, Hello, HelloAck, PROTOCOL_VERSION,
    ProjectionQuery, ProjectionSnapshot, Response, ResponseEnvelope,
};
pub use error::{ProtocolError, ProtocolErrorCode};
pub use ids::{
    ArtifactId, CommandId, EngagementId, EventId, OperationId, ProviderId, RequestId, ServiceId,
    TaskId, WorkspaceId,
};
pub use ingress::{BuzzIngress, IngressEnvelope, IngressSource, QmIngress};
pub use tasking::{
    CapabilityRequirement, CompletionDecision, CompletionTest, EvidenceObservation, ExecutionMode,
    ExecutionTask, ProviderDispatch, TaskStatus, TaskingPlan,
};
