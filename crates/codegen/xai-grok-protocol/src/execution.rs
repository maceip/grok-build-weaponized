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

    pub fn request_id(&self) -> &RequestId {
        match self {
            Self::Interactive(session) => &session.request_id,
            Self::Deferred(task) => &task.request_id,
            Self::Detached(job) => &job.request_id,
        }
    }

    pub fn engagement_id(&self) -> &EngagementId {
        match self {
            Self::Interactive(session) => &session.engagement_id,
            Self::Deferred(task) => &task.engagement_id,
            Self::Detached(job) => &job.engagement_id,
        }
    }

    pub fn task_id(&self) -> &TaskId {
        match self {
            Self::Interactive(session) => &session.task_id,
            Self::Deferred(task) => &task.task_id,
            Self::Detached(job) => &job.task_id,
        }
    }

    pub fn provider_id(&self) -> &ProviderId {
        match self {
            Self::Interactive(session) => &session.provider_id,
            Self::Deferred(task) => &task.provider_id,
            Self::Detached(job) => &job.provider_id,
        }
    }

    pub fn lease_epoch(&self) -> u64 {
        match self {
            Self::Interactive(session) => session.lease_epoch,
            Self::Deferred(task) => task.lease_epoch,
            Self::Detached(job) => job.lease_epoch,
        }
    }
}
