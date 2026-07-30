use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use xai_grok_sampling_types::SamplingError;

const DURABLE_ADMISSION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
type AdmissionAcknowledgement = oneshot::Sender<Result<(), String>>;
type SharedAdmissionAcknowledgement = Arc<Mutex<Option<AdmissionAcknowledgement>>>;

/// Model-visible channel for a streamed local-runtime token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeChannel {
    Text,
    Reasoning,
}

/// Streaming events emitted by a native runtime worker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum RuntimeEvent {
    Admitted {
        request_id: String,
        model_id: String,
        adapter_id: Option<String>,
        context_plan_hash: String,
    },
    StreamStarted {
        request_id: String,
        timestamp_ms: i64,
    },
    FirstToken {
        request_id: String,
    },
    ChannelToken {
        request_id: String,
        channel: RuntimeChannel,
        text: String,
        chunk_index: u64,
    },
    ToolCallDelta {
        request_id: String,
        tool_index: u32,
        id: Option<String>,
        name: Option<String>,
        arguments_delta: Option<String>,
    },
}

/// Exact model/context identity produced only after native prompt admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeAdmission {
    pub request_id: String,
    pub model_id: String,
    pub adapter_id: Option<String>,
    pub context_plan_hash: String,
}

/// Cloneable delivery receipt for the durable admission barrier.
///
/// The runtime blocks before generation until the shell has persisted this
/// record and acknowledges it. The acknowledgement sender is single-use even
/// if the surrounding sampling event is cloned.
#[derive(Clone)]
pub struct RuntimeAdmissionReceipt {
    admission: RuntimeAdmission,
    acknowledgement: SharedAdmissionAcknowledgement,
}

impl std::fmt::Debug for RuntimeAdmissionReceipt {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeAdmissionReceipt")
            .field("admission", &self.admission)
            .finish_non_exhaustive()
    }
}

impl RuntimeAdmissionReceipt {
    pub fn admission(&self) -> &RuntimeAdmission {
        &self.admission
    }

    pub fn acknowledge(&self, result: Result<(), String>) {
        if let Some(sender) = self
            .acknowledgement
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            let _ = sender.send(result);
        }
    }
}

/// Runtime-side endpoint for the durable admission barrier.
#[derive(Debug, Clone)]
pub struct RuntimeAdmissionHook {
    sender: mpsc::UnboundedSender<RuntimeAdmissionReceipt>,
}

pub type RuntimeAdmissionReceiver = mpsc::UnboundedReceiver<RuntimeAdmissionReceipt>;

pub fn runtime_admission_channel() -> (RuntimeAdmissionHook, RuntimeAdmissionReceiver) {
    let (sender, receiver) = mpsc::unbounded_channel();
    (RuntimeAdmissionHook { sender }, receiver)
}

impl RuntimeAdmissionHook {
    pub(crate) async fn persist(
        &self,
        admission: RuntimeAdmission,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), SamplingError> {
        let (acknowledgement, acknowledged) = oneshot::channel();
        self.sender
            .send(RuntimeAdmissionReceipt {
                admission,
                acknowledgement: Arc::new(Mutex::new(Some(acknowledgement))),
            })
            .map_err(|_| admission_error("durable admission consumer is unavailable"))?;

        let persistence_deadline = deadline.min(
            Instant::now()
                .checked_add(DURABLE_ADMISSION_TIMEOUT)
                .unwrap_or(deadline),
        );
        tokio::select! {
            _ = cancellation.cancelled() => {
                Err(admission_error("request cancelled while awaiting durable admission"))
            }
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(persistence_deadline)) => {
                Err(admission_error("deadline elapsed while awaiting durable admission"))
            }
            result = acknowledged => {
                match result {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(error)) => Err(admission_error(error)),
                    Err(_) => Err(admission_error(
                        "durable admission consumer dropped its acknowledgement",
                    )),
                }
            }
        }
    }
}

fn admission_error(message: impl Into<String>) -> SamplingError {
    SamplingError::StreamError {
        error_type: "local_runtime_admission_persistence".to_string(),
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn admission() -> RuntimeAdmission {
        RuntimeAdmission {
            request_id: "request-1".to_string(),
            model_id: "model-1".to_string(),
            adapter_id: Some("adapter-1@revision-1".to_string()),
            context_plan_hash: "context-hash".to_string(),
        }
    }

    #[tokio::test]
    async fn durable_admission_blocks_until_acknowledged() {
        let (hook, mut receiver) = runtime_admission_channel();
        let cancellation = CancellationToken::new();
        let persist = tokio::spawn(async move {
            hook.persist(
                admission(),
                Instant::now() + Duration::from_secs(1),
                &cancellation,
            )
            .await
        });
        tokio::task::yield_now().await;
        assert!(
            !persist.is_finished(),
            "generation admission must wait for durable persistence"
        );

        let receipt = receiver.recv().await.expect("admission receipt");
        assert_eq!(receipt.admission().context_plan_hash, "context-hash");
        receipt.acknowledge(Ok(()));
        persist
            .await
            .expect("admission task")
            .expect("acknowledged admission");
    }

    #[tokio::test]
    async fn durable_admission_propagates_persistence_failure() {
        let (hook, mut receiver) = runtime_admission_channel();
        let cancellation = CancellationToken::new();
        let persist = tokio::spawn(async move {
            hook.persist(
                admission(),
                Instant::now() + Duration::from_secs(1),
                &cancellation,
            )
            .await
        });
        let receipt = receiver.recv().await.expect("admission receipt");
        receipt.acknowledge(Err("sqlite write failed".to_string()));
        let error = persist
            .await
            .expect("admission task")
            .expect_err("persistence failure must reject generation");
        assert!(error.to_string().contains("sqlite write failed"));
    }
}
