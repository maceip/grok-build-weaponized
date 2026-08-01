//! Persistent local control-plane services.
//!
//! `grokd` is the sole production owner of durable engagement admission,
//! provider scheduling, service generations, artifact storage, projections,
//! and the local command/event protocol. Clients use the Unix-socket transport;
//! tests may drive [`ControlPlane`] directly.

pub mod agent_provider;
pub mod artifact;
pub mod control_plane;
pub mod journal;
pub mod native_provider;
pub mod process_provider;
pub mod projection;
pub mod provider;
pub mod server;
pub mod service;

pub use agent_provider::{AGENT_TURN_OPERATION, AgentExecutionProvider, AgentProviderConfig};
pub use artifact::{ArtifactDescriptor, ArtifactStore, ArtifactStoreConfig};
pub use control_plane::{ControlPlane, ControlPlaneConfig, ControlPlaneError, ControlPlaneHandle};
pub use journal::{EventJournal, JournalError};
pub use native_provider::{NATIVE_PROVIDER_ID, NativeExecutionProvider};
pub use process_provider::{ProcessExecutionProvider, ProcessProviderConfig};
pub use projection::{
    ArtifactProjection, EngagementProjection, ProjectionStore, ProviderOutputProjection,
    ProviderProjection,
};
pub use provider::{
    ExecutionProvider, FunctionProvider, ProviderArtifact, ProviderArtifactSource, ProviderOutput,
    ProviderRegistry, ProviderRegistryConfig, ProviderTerminalStatus,
};
pub use server::{ControlPlaneServer, ServerConfig, ServerError};
pub use service::{ServiceRecord, ServiceSupervisor};
pub use xai_grok_control_client::{ClientError, ClientIdentity, ControlPlaneClient};

pub const SERVER_NAME: &str = "grokd";
