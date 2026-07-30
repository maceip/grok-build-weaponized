//! Durable coordination for long-running local model and tool engagements.
//!
//! The crate deliberately contains no model, terminal, HTTP, or UI code. It is
//! the local control plane beneath those systems: accepted work, fenced leases,
//! stage checkpoints, stable action identities, process checkpoints, snapshots,
//! and a replayable typed event stream.

mod coordinator;
mod store;
mod types;

pub use coordinator::{
    EngagementCoordinator, EngagementLease, EngagementSubscription, SubscriptionError,
};
pub use store::{
    AcceptOutcome, EngagementError, EngagementMutation, EngagementStore, RecoveryReport,
};
pub use types::{
    ActionIdentity, ActionRecord, ActionResolution, ActionSpec, ActionStatus, EngagementCheckpoint,
    EngagementEvent, EngagementId, EngagementRecord, EngagementSnapshot, EngagementStage,
    EngagementStatus, JobCheckpoint, JobKind, JobLifecycle, NewEngagement, QueuePriority,
};

#[cfg(test)]
mod tests;
