//! Sampler adapter for the shared local runtime.

use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use xai_grok_runtime::{
    AdapterBinding, RuntimeEvent, RuntimeManager, RuntimePriority, RuntimeRequest, RuntimeStage,
};
use xai_grok_sampling_types::{ConversationRequest, SamplingError};

use crate::config::SamplerConfig;
use crate::events::{SamplingChannel, SamplingEvent};
use crate::types::RequestId;

const LOCAL_GENERATION_DEADLINE: Duration = Duration::from_secs(30 * 60);

fn requires_durable_admission(runtime_stage: RuntimeStage, turn_index: Option<&str>) -> bool {
    matches!(
        runtime_stage,
        RuntimeStage::Planner | RuntimeStage::Executor | RuntimeStage::Reviewer
    ) || (runtime_stage == RuntimeStage::Direct && turn_index.is_some())
}

pub use xai_grok_runtime::litert_lm::{
    BoundedLocalText, ContextOverflowStrategy, LiteRtLmConfig, LocalInferenceResult,
    LoraAdapterConfig,
};
pub use xai_grok_runtime::{
    ArtifactRef, EvidenceBlackboard, EvidenceError, EvidenceId, EvidenceRecord, EvidenceSource,
};

pub async fn prewarm_local_model(config: &SamplerConfig) -> Result<bool, SamplingError> {
    let Some(mut runtime_config) =
        LiteRtLmConfig::from_base_url(&config.base_url).map_err(|message| {
            SamplingError::StreamError {
                error_type: "litert_lm_config".to_string(),
                message,
            }
        })?
    else {
        return Ok(false);
    };
    RuntimeManager::global()
        .prewarm_model(config.model.clone(), runtime_config.clone())
        .await?;
    if let Some(descriptor) = runtime_config.adapter_descriptor.clone() {
        runtime_config.lora_adapter = None;
        RuntimeManager::global()
            .prewarm_adapter(config.model.clone(), runtime_config, descriptor)
            .await?;
    }
    Ok(true)
}

pub fn local_runtime_capacity() -> xai_grok_runtime::CapacitySnapshot {
    RuntimeManager::global().capacity()
}

pub async fn bound_text_for_sampler(
    sampler: &SamplerConfig,
    text: &str,
    max_tokens: u32,
) -> Result<Option<BoundedLocalText>, SamplingError> {
    xai_grok_runtime::litert_lm::bound_text(&sampler.base_url, text, max_tokens).await
}

pub(crate) async fn run_local_request(
    request_id: RequestId,
    request: ConversationRequest,
    mut config: LiteRtLmConfig,
    model: String,
    event_tx: &mpsc::UnboundedSender<SamplingEvent>,
    cancel_token: &CancellationToken,
) -> Result<LocalInferenceResult, SamplingError> {
    let runtime_stage = request
        .x_grok_req_id
        .as_deref()
        .and_then(|request_id| request_id.strip_prefix("grok-stage-"))
        .and_then(|stage| stage.split(':').next())
        .map_or(RuntimeStage::Direct, |stage| match stage {
            "planner" => RuntimeStage::Planner,
            "executor" => RuntimeStage::Executor,
            "reviewer" => RuntimeStage::Reviewer,
            _ => RuntimeStage::Direct,
        });
    let runtime_priority = match runtime_stage {
        RuntimeStage::Executor => RuntimePriority::Executor,
        RuntimeStage::Reviewer => RuntimePriority::Reviewer,
        RuntimeStage::Planner | RuntimeStage::Direct => RuntimePriority::Interactive,
        RuntimeStage::Embedding => RuntimePriority::Background,
        RuntimeStage::Prewarm => RuntimePriority::Prewarm,
    };
    let durable_admission_required =
        requires_durable_admission(runtime_stage, request.x_grok_turn_idx.as_deref());
    let session_id = request
        .x_grok_session_id
        .clone()
        .unwrap_or_else(|| request_id.as_str().to_string());
    let adapter = if let Some(descriptor) = config.adapter_descriptor.clone() {
        // Descriptor-based adapters always pass through AdapterManager. Clear
        // the legacy static field so neither the worker nor the in-process
        // backend can silently bypass validation, leasing, or native select.
        config.lora_adapter = None;
        RuntimeManager::global()
            .prewarm_adapter(model.clone(), config.clone(), descriptor.clone())
            .await?;
        Some(AdapterBinding {
            adapter_id: descriptor.adapter_id,
            revision: descriptor.revision,
        })
    } else {
        None
    };
    let prepared = xai_grok_runtime::litert_lm::prepare_conversation(request)?;
    let completion_reserve = prepared.max_output_tokens.unwrap_or_default();
    let (runtime_event_tx, mut runtime_event_rx) = mpsc::unbounded_channel();
    let sampler_events = event_tx.clone();
    let event_bridge = tokio::spawn(async move {
        while let Some(event) = runtime_event_rx.recv().await {
            let event = match event {
                // Requests carrying the durable admission hook use the
                // dedicated acknowledged bridge below. This fallback is kept
                // for direct runtime callers that do not install a barrier.
                RuntimeEvent::Admitted { .. } => continue,
                RuntimeEvent::StreamStarted {
                    request_id,
                    timestamp_ms,
                } => SamplingEvent::StreamStarted {
                    request_id: RequestId::from(request_id),
                    timestamp_ms,
                },
                RuntimeEvent::FirstToken { request_id } => SamplingEvent::FirstToken {
                    request_id: RequestId::from(request_id),
                },
                RuntimeEvent::ChannelToken {
                    request_id,
                    channel,
                    text,
                    chunk_index,
                } => SamplingEvent::ChannelToken {
                    request_id: RequestId::from(request_id),
                    channel: match channel {
                        xai_grok_runtime::RuntimeChannel::Text => SamplingChannel::Text,
                        xai_grok_runtime::RuntimeChannel::Reasoning => SamplingChannel::Reasoning,
                    },
                    text,
                    chunk_index,
                },
                RuntimeEvent::ToolCallDelta {
                    request_id,
                    tool_index,
                    id,
                    name,
                    arguments_delta,
                } => SamplingEvent::ToolCallDelta {
                    request_id: RequestId::from(request_id),
                    tool_index,
                    id,
                    name,
                    arguments_delta,
                },
            };
            if sampler_events.send(event).is_err() {
                break;
            }
        }
    });
    let (admission_hook, admission_bridge) = if durable_admission_required {
        let (admission_hook, mut admission_rx) = xai_grok_runtime::runtime_admission_channel();
        let admission_events = event_tx.clone();
        let bridge = tokio::spawn(async move {
            while let Some(admission) = admission_rx.recv().await {
                if admission_events
                    .send(SamplingEvent::RuntimeAdmitted { admission })
                    .is_err()
                {
                    break;
                }
            }
        });
        (Some(admission_hook), Some(bridge))
    } else {
        (None, None)
    };
    let request = RuntimeRequest {
        request_id: request_id.as_str().to_string(),
        session_id,
        model_id: model,
        model_config: config,
        stage: runtime_stage,
        adapter,
        conversation: prepared,
        completion_reserve,
        priority: runtime_priority,
        // Queue admission is independently bounded by RuntimeManager (500 ms
        // optional review, 2 s mandatory work). Once generation starts it must
        // not inherit that queue timeout and be killed mid-token.
        deadline: Instant::now() + LOCAL_GENERATION_DEADLINE,
        admission_hook,
    };
    let result = RuntimeManager::global()
        .generate(request, runtime_event_tx, cancel_token.clone())
        .await;
    let _ = event_bridge.await;
    if let Some(admission_bridge) = admission_bridge {
        let _ = admission_bridge.await;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_admission_is_required_only_for_turn_generation_stages() {
        assert!(requires_durable_admission(RuntimeStage::Planner, None));
        assert!(requires_durable_admission(RuntimeStage::Executor, None));
        assert!(requires_durable_admission(RuntimeStage::Reviewer, None));
        assert!(requires_durable_admission(RuntimeStage::Direct, Some("12")));
        assert!(!requires_durable_admission(RuntimeStage::Direct, None));
        assert!(!requires_durable_admission(
            RuntimeStage::Embedding,
            Some("12")
        ));
        assert!(!requires_durable_admission(
            RuntimeStage::Prewarm,
            Some("12")
        ));
    }
}
