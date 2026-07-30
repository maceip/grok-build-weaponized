use std::collections::HashSet;
use std::sync::{Mutex as StdMutex, OnceLock};
use std::time::Duration;

use super::*;
use crate::agent::turn_coordinator::{
    PlanPacket, ReviewDecision, TurnCooperation, parse_json_payload,
};
use xai_grok_sampler::litert_lm::{ArtifactRef, EvidenceId, EvidenceRecord, EvidenceSource};

const PLANNER_TIMEOUT: Duration = Duration::from_secs(120);
const REVIEWER_TIMEOUT: Duration = Duration::from_secs(120);
const MEMORY_RETRIEVAL_TIMEOUT: Duration = Duration::from_millis(50);
const MAX_REVIEW_EVIDENCE_CHARS: usize = 24_000;
const MAX_EVIDENCE_ITEM_CHARS: usize = 4_000;

fn suppressed_requests() -> &'static StdMutex<HashSet<String>> {
    static REQUESTS: OnceLock<StdMutex<HashSet<String>>> = OnceLock::new();
    REQUESTS.get_or_init(|| StdMutex::new(HashSet::new()))
}

pub(super) struct SuppressedSamplingGuard {
    request_id: String,
}

impl Drop for SuppressedSamplingGuard {
    fn drop(&mut self) {
        suppressed_requests()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.request_id);
    }
}

pub(super) fn suppress_sampling(request_id: &str) -> SuppressedSamplingGuard {
    suppressed_requests()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(request_id.to_string());
    SuppressedSamplingGuard {
        request_id: request_id.to_string(),
    }
}

pub(super) fn sampling_is_suppressed(request_id: &str) -> bool {
    suppressed_requests()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .contains(request_id)
}

impl SessionActor {
    pub(super) async fn cooperation_memory_evidence(
        &self,
        query: &str,
        limit: usize,
    ) -> Vec<serde_json::Value> {
        use xai_grok_tools::types::memory_backend::MemoryBackend as _;

        let (Some(storage), Some(params)) =
            (self.memory.storage(), self.memory.backend_params.as_ref())
        else {
            return Vec::new();
        };
        let backend =
            crate::session::memory::MemoryBackendImpl::from_session_params(storage, params);
        let results = tokio::time::timeout(
            MEMORY_RETRIEVAL_TIMEOUT,
            backend.search(query, limit.clamp(1, 8), 0.0),
        )
        .await
        .ok()
        .and_then(Result::ok)
        .unwrap_or_default();
        results
            .into_iter()
            .map(|result| {
                serde_json::json!({
                    "evidence_id": result.chunk_id,
                    "source": result.source,
                    "path": result.path,
                    "line_start": result.start_line,
                    "line_end": result.end_line,
                    "finding": bounded_chars(&result.snippet, MAX_EVIDENCE_ITEM_CHARS),
                    "confidence": result.score.clamp(0.0, 1.0),
                    "observed_at": result.created_at,
                })
            })
            .collect()
    }

    async fn cooperation_client(
        &self,
        model: &str,
    ) -> Result<xai_grok_sampler::SamplingClient, String> {
        let active = self.reconstruct_full_config().await;
        let mut config = self
            .resolve_aux_sampler_config(model)
            .await
            .ok_or_else(|| format!("cannot resolve cooperation model {model:?}"))?;
        crate::agent::config::stamp_session_local_sampler_fields(
            &mut config,
            &active,
            self.client_identifier.clone(),
            Some(self.max_retries),
        );
        xai_grok_sampler::SamplingClient::new(config).map_err(|error| error.to_string())
    }

    pub(super) async fn prepare_cooperation_plan(
        &self,
        prompt_id: &str,
        user_request: &str,
        cooperation: &TurnCooperation,
    ) -> Result<PlanPacket, String> {
        let client = self.cooperation_client(&cooperation.planner_model).await?;
        let active_executor = self.reconstruct_full_config().await;
        let prewarm = xai_grok_sampler::prewarm_local_model(&active_executor);
        let memory = self
            .cooperation_memory_evidence(
                &format!("workspace goals prior findings architecture relevant to: {user_request}"),
                6,
            )
            .await;
        append_workspace_evidence(cooperation, &memory);
        let planner_input = serde_json::json!({
            "user_request": user_request,
            "workspace_evidence": memory,
        });
        let request = ConversationRequest {
            items: vec![
                ConversationItem::system(
                    "You are the planning stage in a two-model local execution system. Return \
                     one bounded JSON object and nothing else. Do not include private reasoning. \
                     Schema: {\"objective\":string,\"tasks\":[{\"task_id\":string,\
                     \"objective\":string,\"tool_families\":[string],\
                     \"required_evidence\":[string],\"completion_tests\":[string]}],\
                     \"required_tool_families\":[string],\"required_evidence\":[string],\
                     \"completion_tests\":[string]}. Use no more than 12 ordered tasks.",
                ),
                ConversationItem::user(planner_input.to_string()),
            ],
            tools: Vec::new(),
            hosted_tools: Vec::new(),
            tool_choice: None,
            model: Some(cooperation.planner_model.clone()),
            temperature: Some(0.0),
            max_output_tokens: Some(768),
            x_grok_req_id: Some(format!("grok-stage-planner:{prompt_id}")),
            x_grok_session_id: Some(format!("{}:planner:{prompt_id}", self.session_info.id)),
            ..ConversationRequest::default()
        };
        let planner = tokio::time::timeout(PLANNER_TIMEOUT, client.conversation_collect(request));
        let (response, prewarm_result) = tokio::join!(planner, prewarm);
        if let Err(error) = prewarm_result {
            tracing::warn!(%error, "executor prewarm failed while planner was running");
        }
        let response = response
            .map_err(|_| "planner timed out".to_string())?
            .map_err(|error| error.to_string())?;
        let packet: PlanPacket = parse_json_payload(&response.assistant_text())?;
        packet.validate()?;
        Ok(packet)
    }

    fn append_reviewer_evidence(
        &self,
        conversation: &[ConversationItem],
        cooperation: &TurnCooperation,
    ) {
        let turn_start = conversation
            .iter()
            .rposition(|item| matches!(item, ConversationItem::User(_)))
            .unwrap_or(0);
        let mut remaining = MAX_REVIEW_EVIDENCE_CHARS;
        for (index, item) in conversation.iter().skip(turn_start + 1).enumerate() {
            let record = match item {
                ConversationItem::ToolResult(result) => {
                    let wrapped =
                        xai_grok_tools::util::output_filter::WrappedToolResultEnvelope::parse(
                            &result.content,
                        );
                    let finding = wrapped
                        .as_ref()
                        .map(|wrapped| {
                            if wrapped.envelope.findings.is_empty() {
                                wrapped.envelope.preview.clone()
                            } else {
                                wrapped.envelope.findings.join("\n")
                            }
                        })
                        .unwrap_or_else(|| result.content.to_string());
                    let artifact = wrapped.map(|wrapped| ArtifactRef {
                        artifact_id: format!("tool:{}", result.tool_call_id),
                        path: Some(wrapped.envelope.artifact.path.into()),
                        cursor: Some(wrapped.envelope.cursor.next_byte as u64),
                    });
                    Some(EvidenceRecord {
                        evidence_id: EvidenceId(format!("tool:{}:{index}", result.tool_call_id)),
                        source: EvidenceSource::ToolResult {
                            tool_call_id: result.tool_call_id.to_string(),
                        },
                        finding: bounded_chars(&finding, MAX_EVIDENCE_ITEM_CHARS),
                        artifact,
                        confidence: 1.0,
                        observed_at: std::time::SystemTime::now(),
                    })
                }
                ConversationItem::Assistant(assistant) if !assistant.content.is_empty() => {
                    Some(EvidenceRecord {
                        evidence_id: EvidenceId(format!("executor_report:{index}")),
                        source: EvidenceSource::ExecutorReport {
                            model_id: assistant.model_id.clone(),
                        },
                        finding: bounded_chars(&assistant.content, MAX_EVIDENCE_ITEM_CHARS),
                        artifact: None,
                        confidence: 1.0,
                        observed_at: std::time::SystemTime::now(),
                    })
                }
                _ => None,
            };
            let Some(record) = record else {
                continue;
            };
            let length = record.finding.chars().count();
            if length > remaining {
                break;
            }
            remaining -= length;
            let _ = cooperation.evidence.append(record);
        }
    }

    pub(super) async fn append_cooperation_turn_evidence(&self, cooperation: &TurnCooperation) {
        let conversation = self.chat_state_handle.get_conversation().await;
        self.append_reviewer_evidence(&conversation, cooperation);
    }

    pub(super) async fn run_cooperation_review(
        &self,
        prompt_id: &str,
        cooperation: &TurnCooperation,
        executor_response: &str,
    ) -> Result<ReviewDecision, String> {
        let plan = cooperation
            .plan
            .as_ref()
            .ok_or_else(|| "review requested without a PlanPacket".to_string())?;
        let reviewer_memory = if let Some(prefetched) =
            crate::agent::turn_coordinator::take_reviewer_memory(
                self.session_info.id.0.as_ref(),
                prompt_id,
            ) {
            prefetched
        } else {
            self.cooperation_memory_evidence(
                &format!(
                    "completion criteria contradictory evidence for objective {} tests {}",
                    plan.objective,
                    plan.completion_tests.join("; ")
                ),
                6,
            )
            .await
        };
        append_workspace_evidence(cooperation, &reviewer_memory);
        self.append_cooperation_turn_evidence(cooperation).await;
        let evidence = cooperation.evidence.snapshot();
        let payload = serde_json::json!({
            "plan": plan,
            "evidence": evidence,
            "workspace_evidence": reviewer_memory,
            "executor_response": bounded_chars(executor_response, MAX_EVIDENCE_ITEM_CHARS),
            "instruction": "Return Accept with the final user response when completion tests pass. \
                Return Correct with only the minimum remaining tasks otherwise.",
        });
        let client = self.cooperation_client(&cooperation.reviewer_model).await?;
        let request = ConversationRequest {
            items: vec![
                ConversationItem::system(
                    "You are the review and final-synthesis stage. Use only the typed plan and \
                     observations supplied to you; do not invent tool results or transfer private \
                     reasoning. Return exactly one JSON object. Accepted schema: \
                     {\"decision\":\"accept\",\"final_response\":string,\"unresolved\":[string]}. \
                     Correction schema: {\"decision\":\"correct\",\"tasks\":[ExecutionTask]}.",
                ),
                ConversationItem::user(payload.to_string()),
            ],
            tools: Vec::new(),
            hosted_tools: Vec::new(),
            tool_choice: None,
            model: Some(cooperation.reviewer_model.clone()),
            temperature: Some(0.0),
            max_output_tokens: Some(768),
            x_grok_req_id: Some(format!("grok-stage-reviewer:{prompt_id}")),
            x_grok_session_id: Some(format!("{}:reviewer:{prompt_id}", self.session_info.id)),
            ..ConversationRequest::default()
        };
        let response = tokio::time::timeout(REVIEWER_TIMEOUT, client.conversation_collect(request))
            .await
            .map_err(|_| "reviewer timed out".to_string())?
            .map_err(|error| error.to_string())?;
        parse_json_payload(&response.assistant_text())
    }

    pub(super) async fn publish_cooperation_final(&self, model: &str, final_response: String) {
        self.send_update(
            acp::SessionUpdate::AgentMessageChunk(acp::ContentChunk::new(acp::ContentBlock::Text(
                acp::TextContent::new(final_response.clone()),
            ))),
            None,
        )
        .await;
        self.record_assistant_response(ConversationItem::assistant_with_model(
            final_response,
            model.to_string(),
        ))
        .await;
    }
}

fn append_workspace_evidence(cooperation: &TurnCooperation, values: &[serde_json::Value]) {
    for value in values {
        let Some(evidence_id) = value.get("evidence_id").and_then(serde_json::Value::as_str) else {
            continue;
        };
        let Some(finding) = value.get("finding").and_then(serde_json::Value::as_str) else {
            continue;
        };
        let path = value
            .get("path")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let confidence = value
            .get("confidence")
            .and_then(serde_json::Value::as_f64)
            .unwrap_or(0.0)
            .clamp(0.0, 1.0) as f32;
        let record = EvidenceRecord {
            evidence_id: EvidenceId(evidence_id.to_string()),
            source: EvidenceSource::WorkspaceMemory {
                chunk_id: evidence_id.to_string(),
                path: path.into(),
            },
            finding: bounded_chars(finding, MAX_EVIDENCE_ITEM_CHARS),
            artifact: Some(ArtifactRef {
                artifact_id: format!("memory:{evidence_id}"),
                path: (!path.is_empty()).then(|| path.into()),
                cursor: value.get("line_end").and_then(serde_json::Value::as_u64),
            }),
            confidence,
            observed_at: std::time::SystemTime::now(),
        };
        let _ = cooperation.evidence.append(record);
    }
}

fn bounded_chars(value: &str, limit: usize) -> String {
    if value.chars().count() <= limit {
        return value.to_string();
    }
    let head = limit / 2;
    let tail = limit.saturating_sub(head);
    let prefix = value.chars().take(head).collect::<String>();
    let suffix = value
        .chars()
        .rev()
        .take(tail)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<String>();
    format!("{prefix}\n... middle omitted ...\n{suffix}")
}
