use serde::{Deserialize, Serialize};

use crate::{EngagementId, ExecutionMode, ProviderDispatch, ProviderId, RequestId, TaskId};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InteractiveSession {
    pub request_id: RequestId,
    pub engagement_id: EngagementId,
    pub task_id: TaskId,
    pub provider_id: ProviderId,
    pub lease_epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DeferredTask {
    pub request_id: RequestId,
    pub engagement_id: EngagementId,
    pub task_id: TaskId,
    pub provider_id: ProviderId,
    pub lease_epoch: u64,
    pub result_cursor: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DetachedJob {
    pub request_id: RequestId,
    pub engagement_id: EngagementId,
    pub task_id: TaskId,
    pub provider_id: ProviderId,
    pub lease_epoch: u64,
    pub result_cursor: u64,
}

/// Mode-specific durable receipt returned at dispatch admission.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum ExecutionReceipt {
    Interactive(InteractiveSession),
    Deferred(DeferredTask),
    Detached(DetachedJob),
}

impl ExecutionReceipt {
    pub fn from_dispatch(dispatch: &ProviderDispatch) -> Self {
        let request_id = dispatch.request_id.clone();
        let engagement_id = dispatch.engagement_id.clone();
        let task_id = dispatch.task.task_id.clone();
        let provider_id = dispatch.provider_id.clone();
        let lease_epoch = dispatch.lease_epoch;
        match dispatch.task.mode {
            ExecutionMode::Interactive => Self::Interactive(InteractiveSession {
                request_id,
                engagement_id,
                task_id,
                provider_id,
                lease_epoch,
            }),
            ExecutionMode::Deferred => Self::Deferred(DeferredTask {
                request_id,
                engagement_id,
                task_id,
                provider_id,
                lease_epoch,
                result_cursor: 0,
            }),
            ExecutionMode::Detached => Self::Detached(DetachedJob {
                request_id,
                engagement_id,
                task_id,
                provider_id,
                lease_epoch,
                result_cursor: 0,
            }),
        }
    }
}
