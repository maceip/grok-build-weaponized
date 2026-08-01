//! Supervised local inference runtime.
//!
//! This crate owns LiteRT-LM engines, session/KV residency, adapter state,
//! context admission, process isolation, and runtime resource accounting. The
//! sampler remains responsible for provider-neutral request/retry behavior.

pub mod adapter;
pub mod capability;
pub mod context;
pub mod events;
pub mod evidence;
pub mod litert_lm;
pub mod manager;
pub mod metrics;
pub mod protocol;
pub mod resource;
mod worker;

pub use adapter::{
    AdapterBinding, AdapterDescriptor, AdapterDtype, AdapterError, AdapterLease, AdapterManager,
    AdapterResidency, AdapterResidencySnapshot, AdapterState, hash_artifact,
};
pub use capability::local_runtime_capability_manifest;
pub use context::{
    AdmissionError, ContextAllocation, ContextBudgetBroker, ContextComponent, ContextComponentKind,
    ContextPlan, DroppedContext, StageBudget,
};
pub use events::{
    RuntimeAdmission, RuntimeAdmissionHook, RuntimeAdmissionReceipt, RuntimeAdmissionReceiver,
    RuntimeChannel, RuntimeEvent, runtime_admission_channel,
};
pub use evidence::{
    ArtifactRef, EvidenceBlackboard, EvidenceError, EvidenceId, EvidenceRecord, EvidenceSource,
};
pub use litert_lm::{
    BoundedLocalText, ContextOverflowStrategy, LiteRtLmConfig, LocalInferenceResult,
    LoraAdapterConfig, PreparedConversation,
};
pub use manager::{
    CapacitySnapshot, RuntimeManager, RuntimeManagerConfig, RuntimeMode, RuntimePriority,
    RuntimeRequest, RuntimeStage, RuntimeStatusSnapshot,
};
pub use metrics::{InferenceLatencyStats, compute_percentiles};
pub use resource::{
    MemoryLimits, ResourceClass, ResourceGovernor, ResourceLease, ResourceSnapshot,
    current_process_resident_bytes,
};

pub(crate) const LOG_TARGET: &str = "xai_grok::local_runtime";
