use std::collections::HashSet;
use std::sync::{Mutex as StdMutex, OnceLock};
use std::time::Duration;

use super::*;
use crate::agent::turn_coordinator::{
    ExecutionTask, PlanPacket, ReviewDecision, TurnCooperation, parse_json_payload,
};
use xai_grok_sampler::litert_lm::{ArtifactRef, EvidenceId, EvidenceRecord, EvidenceSource};

const PLANNER_TIMEOUT: Duration = Duration::from_secs(120);
const REVIEWER_TIMEOUT: Duration = Duration::from_secs(120);
const MEMORY_RETRIEVAL_TIMEOUT: Duration = Duration::from_millis(50);
const MAX_REVIEW_EVIDENCE_CHARS: usize = 24_000;
const MAX_EVIDENCE_ITEM_CHARS: usize = 4_000;
const MAX_EVIDENCE_FINDING_CHARS: usize = 480;

fn normalized_tool_evidence(
    wrapped: &xai_grok_tools::util::output_filter::WrappedToolResultEnvelope,
    tool_name: Option<&str>,
) -> String {
    let mut structure = wrapped.envelope.structure.clone();
    structure.findings = structure
        .findings
        .iter()
        .map(|finding| bounded_chars(finding, MAX_EVIDENCE_FINDING_CHARS))
        .collect();
    loop {
        let value = serde_json::json!({
            "tool_name": tool_name,
            "status": wrapped.envelope.status,
            "exit_code": wrapped.envelope.exit_code,
            "signal": wrapped.envelope.signal,
            "structure": structure,
            "artifact": wrapped.envelope.artifact,
            "cursor": wrapped.envelope.cursor,
        });
        let serialized = value.to_string();
        if serialized.chars().count() <= MAX_EVIDENCE_ITEM_CHARS || structure.findings.is_empty() {
            return serialized;
        }
        structure.findings.pop();
        structure.omitted_finding_count = structure.omitted_finding_count.saturating_add(1);
    }
}

fn tool_family_item_schema(allowed_tools: Option<&[String]>) -> serde_json::Value {
    match allowed_tools {
        Some(tools) => serde_json::json!({"type": "string", "enum": tools}),
        None => serde_json::json!({"type": "string"}),
    }
}

fn execution_task_schema(allowed_tools: Option<&[String]>) -> serde_json::Value {
    let tool_family = tool_family_item_schema(allowed_tools);
    serde_json::json!({
        "type": "object",
        "properties": {
            "task_id": {"type": "string"},
            "objective": {"type": "string"},
            "tool_families": {
                "type": "array", "items": tool_family, "minItems": 1
            },
            "required_evidence": {"type": "array", "items": {"type": "string"}},
            "completion_tests": {
                "type": "array", "items": {"type": "string"}, "minItems": 1
            }
        },
        "required": [
            "task_id", "objective", "tool_families", "required_evidence", "completion_tests"
        ],
        "additionalProperties": false
    })
}

fn planner_output_schema(allowed_tools: &[String], task_budget: usize) -> serde_json::Value {
    let tool_family = tool_family_item_schema(Some(allowed_tools));
    serde_json::json!({
        "type": "object",
        "properties": {
            "scratchpad": {"type": "string", "minLength": 1, "maxLength": 1600},
            "objective": {"type": "string"},
            "tasks": {
                "type": "array",
                "items": execution_task_schema(Some(allowed_tools)),
                "minItems": 1,
                "maxItems": task_budget
            },
            "required_tool_families": {
                "type": "array", "items": tool_family, "minItems": 1
            },
            "required_evidence": {"type": "array", "items": {"type": "string"}},
            "completion_tests": {
                "type": "array", "items": {"type": "string"}, "minItems": 1
            }
        },
        "required": [
            "scratchpad", "objective", "tasks", "required_tool_families", "required_evidence",
            "completion_tests"
        ],
        "additionalProperties": false
    })
}

fn reviewer_output_schema(
    allowed_tools: &[String],
    allowed_evidence_ids: &[String],
) -> serde_json::Value {
    serde_json::json!({
        "oneOf": [
            {
                "type": "object",
                "properties": {
                    "scratchpad": {"type": "string", "minLength": 1, "maxLength": 1600},
                    "decision": {"const": "accept"},
                    "final_response": {
                        "type": "string", "minLength": 12,
                        "pattern": ".*[A-Za-z0-9].*"
                    },
                    "evidence_ids": {
                        "type": "array",
                        "items": {"type": "string", "enum": allowed_evidence_ids},
                        "minItems": 1
                    },
                    "unresolved": {"type": "array", "items": {"type": "string"}}
                },
                "required": [
                    "scratchpad", "decision", "final_response", "evidence_ids", "unresolved"
                ],
                "additionalProperties": false
            },
            {
                "type": "object",
                "properties": {
                    "scratchpad": {"type": "string", "minLength": 1, "maxLength": 1600},
                    "decision": {"const": "correct"},
                    "tasks": {
                        "type": "array",
                        "items": execution_task_schema(Some(allowed_tools)),
                        "minItems": 1,
                        "maxItems": 12
                    }
                },
                "required": ["scratchpad", "decision", "tasks"],
                "additionalProperties": false
            }
        ]
    })
}

fn planner_task_budget(user_request: &str) -> usize {
    let normalized = user_request.to_ascii_lowercase();
    let explicit_sequence = [
        " then ",
        " after ",
        " before ",
        " followed by ",
        "\n1.",
        "\n2.",
    ]
    .iter()
    .any(|marker| normalized.contains(marker));
    if user_request.chars().count() <= 300 && !explicit_sequence {
        1
    } else {
        12
    }
}

fn requires_presence_evidence(user_request: &str) -> bool {
    let request = user_request.to_ascii_lowercase();
    [
        "whether ",
        "does ",
        "do we have ",
        "is there ",
        "exists",
        "present",
        "absent",
        "missing",
        "declares ",
        "contains ",
    ]
    .iter()
    .any(|marker| request.contains(marker))
}

fn requested_assignment_key(user_request: &str) -> Option<String> {
    let normalized = user_request.to_ascii_lowercase();
    let phrase = [" declares ", " field ", " key "]
        .iter()
        .find_map(|marker| normalized.split_once(marker).map(|(_, tail)| tail))?;
    // A field request frequently continues with a second sentence describing
    // which tools to use. Keep that prose out of the key extraction instead of
    // accidentally turning the last word (for example, `tools`) into the key.
    let phrase = phrase
        .split(['.', '?', '!', ';', '\n', '\r'])
        .next()
        .unwrap_or(phrase);
    let words = phrase
        .split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_' && ch != '-')
        .filter(|word| {
            !word.is_empty()
                && !matches!(
                    *word,
                    "a" | "an" | "the" | "value" | "present" | "exists" | "in" | "within"
                )
        })
        .take_while(|word| {
            !matches!(
                *word,
                "and" | "or" | "use" | "using" | "with" | "from" | "under" | "inside"
            )
        })
        .collect::<Vec<_>>();
    words
        .last()
        .filter(|word| word.len() <= 64)
        .map(|word| (*word).to_string())
}

fn strengthen_presence_evidence(
    user_request: &str,
    available_tools: &[String],
    packet: &mut PlanPacket,
) {
    if !requires_presence_evidence(user_request) {
        return;
    }
    let Some(search_tool) = available_tools
        .iter()
        .find(|name| name.eq_ignore_ascii_case("grep"))
        .cloned()
    else {
        return;
    };
    let Some(task) = packet.tasks.first_mut() else {
        return;
    };
    if !task
        .tool_families
        .iter()
        .any(|name| name.eq_ignore_ascii_case(&search_tool))
    {
        task.tool_families.push(search_tool.clone());
    }
    if !packet
        .required_tool_families
        .iter()
        .any(|name| name.eq_ignore_ascii_case(&search_tool))
    {
        packet.required_tool_families.push(search_tool);
    }
    task.required_evidence.push(
        "An exact field search result, including an explicit zero-match result when absent"
            .to_string(),
    );
    task.completion_tests.push(
        "Presence or absence is based on the exact field key, not a section name or another value"
            .to_string(),
    );
}

fn requests_visible_evidence(user_request: &str, plan: &PlanPacket) -> bool {
    let contract = format!(
        "{} {} {}",
        user_request,
        plan.required_evidence.join(" "),
        plan.completion_tests.join(" ")
    )
    .to_ascii_lowercase();
    ["cite", "observed", "evidence", "exact field", "exact key"]
        .iter()
        .any(|marker| contract.contains(marker))
}

fn visible_evidence_facts(
    user_request: &str,
    plan: &PlanPacket,
    evidence: &[EvidenceRecord],
) -> Vec<(String, String)> {
    if !requests_visible_evidence(user_request, plan) {
        return Vec::new();
    }
    let contract = format!(
        "{} {} {} {} {} {}",
        user_request,
        plan.objective,
        plan.required_evidence.join(" "),
        plan.completion_tests.join(" "),
        plan.tasks
            .iter()
            .flat_map(|task| &task.required_evidence)
            .cloned()
            .collect::<Vec<_>>()
            .join(" "),
        plan.tasks
            .iter()
            .flat_map(|task| &task.completion_tests)
            .cloned()
            .collect::<Vec<_>>()
            .join(" ")
    )
    .to_ascii_lowercase();
    let mut section_facts = Vec::new();
    let mut status_facts = Vec::new();
    for record in evidence {
        if !matches!(record.source, EvidenceSource::ToolResult { .. }) {
            continue;
        }
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&record.finding) else {
            continue;
        };
        if let Some(findings) = value
            .get("structure")
            .and_then(|structure| structure.get("findings"))
            .and_then(serde_json::Value::as_array)
        {
            for finding in findings.iter().filter_map(serde_json::Value::as_str) {
                let normalized = finding.to_ascii_lowercase();
                let section = normalized
                    .strip_prefix("config section \"")
                    .and_then(|tail| tail.split_once('"'))
                    .map(|(section, _)| section);
                if let Some(section) = section
                    && contract.contains(section)
                {
                    section_facts.push((
                        section.len(),
                        record.evidence_id.0.clone(),
                        finding.to_string(),
                    ));
                }
            }
        }
        if let Some(match_count) = value
            .get("structure")
            .and_then(|structure| structure.get("match_count"))
            .and_then(serde_json::Value::as_u64)
            && value.get("tool_name").and_then(serde_json::Value::as_str) == Some("grep")
            && (contract.contains("presence")
                || contract.contains("absence")
                || contract.contains("exact"))
        {
            let status = value
                .get("status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown");
            status_facts.push((
                record.evidence_id.0.clone(),
                format!("exact search status={status}, matches={match_count}"),
            ));
        }
    }
    if let Some(longest) = section_facts.iter().map(|(length, _, _)| *length).max() {
        section_facts.retain(|(length, _, _)| *length == longest);
    }
    let mut facts = section_facts
        .into_iter()
        .map(|(_, evidence_id, fact)| (evidence_id, fact))
        .chain(status_facts)
        .collect::<Vec<_>>();
    facts.sort();
    facts.dedup();
    facts.truncate(6);
    facts
}

fn ground_visible_evidence(
    decision: &mut ReviewDecision,
    user_request: &str,
    plan: &PlanPacket,
    evidence: &[EvidenceRecord],
) {
    let ReviewDecision::Accept {
        final_response,
        evidence_ids,
        ..
    } = decision
    else {
        return;
    };
    let facts = visible_evidence_facts(user_request, plan, evidence);
    if facts.is_empty() {
        return;
    }
    if plan.tasks.len() == 1
        && requires_presence_evidence(user_request)
        && let Some(key) = requested_assignment_key(user_request)
        && let Some(match_count) = evidence.iter().find_map(|record| {
            let value = serde_json::from_str::<serde_json::Value>(&record.finding).ok()?;
            (value.get("tool_name").and_then(serde_json::Value::as_str) == Some("grep"))
                .then(|| value.get("structure")?.get("match_count")?.as_u64())
                .flatten()
        })
    {
        *final_response = if match_count == 0 {
            format!(
                "No. The exact `{key} =` field search returned zero matches, so the requested \
                 field is not declared."
            )
        } else {
            format!(
                "Yes. The exact `{key} =` field search returned {match_count} match(es), so the \
                 requested field is declared."
            )
        };
    }
    let mut rendered = Vec::new();
    for (evidence_id, fact) in facts {
        if !evidence_ids.contains(&evidence_id) {
            evidence_ids.push(evidence_id.clone());
        }
        rendered.push(format!("- [{evidence_id}] {fact}"));
    }
    final_response.push_str("\n\nObserved evidence:\n");
    final_response.push_str(&rendered.join("\n"));
}

fn suppressed_requests() -> &'static StdMutex<HashSet<String>> {
    static REQUESTS: OnceLock<StdMutex<HashSet<String>>> = OnceLock::new();
    REQUESTS.get_or_init(|| StdMutex::new(HashSet::new()))
}

pub(crate) struct SuppressedSamplingGuard {
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

pub(crate) fn suppress_sampling(request_id: &str) -> SuppressedSamplingGuard {
    suppressed_requests()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(request_id.to_string());
    SuppressedSamplingGuard {
        request_id: request_id.to_string(),
    }
}

pub(crate) fn sampling_is_suppressed(request_id: &str) -> bool {
    suppressed_requests()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .contains(request_id)
}

impl SessionActor {
    async fn planner_tool_names(&self, user_request: &str) -> Vec<String> {
        let query = user_request.to_ascii_lowercase();
        let query_tokens = query
            .split(|ch: char| !ch.is_ascii_alphanumeric())
            .filter(|token| token.len() >= 3)
            .collect::<HashSet<_>>();
        let mut tools = self
            .prepare_tool_definitions()
            .await
            .into_iter()
            .enumerate()
            .map(|(index, tool)| {
                let name = tool.function.name;
                let lower = name.to_ascii_lowercase();
                let exact = usize::from(query.contains(&lower));
                let token_matches = lower
                    .split(['_', '-', '.'])
                    .filter(|token| query_tokens.contains(token))
                    .count();
                let intent_boost = usize::from(
                    (query.contains("read") && lower.contains("read"))
                        || (query.contains("file") && lower.contains("file"))
                        || (query.contains("search") && lower.contains("grep")),
                );
                (
                    (exact * 1_000) + (token_matches * 100) + (intent_boost * 10),
                    index,
                    name,
                )
            })
            .collect::<Vec<_>>();
        tools.sort_by_key(|(score, index, _)| (std::cmp::Reverse(*score), *index));
        tools
            .into_iter()
            .take(32)
            .map(|(_, _, name)| name)
            .collect()
    }

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

    async fn cooperation_config(
        &self,
        model: &str,
    ) -> Result<xai_grok_sampler::SamplerConfig, String> {
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
        Ok(config)
    }

    async fn collect_cooperation_response(
        &self,
        config: xai_grok_sampler::SamplerConfig,
        request: ConversationRequest,
    ) -> Result<ConversationResponse, String> {
        let request_id = xai_grok_sampler::RequestId::random();
        let _suppressed = suppress_sampling(request_id.as_str());
        self.sampler_handle
            .submit_and_collect_with_config(request_id, request, config)
            .await
            .map(|(response, _metrics)| response)
            .map_err(|error| error.to_string())
    }

    pub(super) async fn prepare_cooperation_plan(
        &self,
        prompt_id: &str,
        user_request: &str,
        cooperation: &TurnCooperation,
    ) -> Result<PlanPacket, String> {
        let config = self.cooperation_config(&cooperation.planner_model).await?;
        let active_executor = self.reconstruct_full_config().await;
        let prewarm = xai_grok_sampler::prewarm_local_model(&active_executor);
        let memory = self
            .cooperation_memory_evidence(
                &format!("workspace goals prior findings architecture relevant to: {user_request}"),
                6,
            )
            .await;
        append_workspace_evidence(cooperation, &memory);
        let available_tools = self.planner_tool_names(user_request).await;
        if available_tools.is_empty() {
            return Err("planner has no executable tools available".to_string());
        }
        let task_budget = planner_task_budget(user_request);
        let planner_completion_tokens = 768;
        let planner_input = serde_json::json!({
            "user_request": user_request,
            "workspace_evidence": memory,
            "available_tools": available_tools,
            "task_budget": task_budget,
        });
        let request = ConversationRequest {
            items: vec![
                ConversationItem::system(
                    "You are a plan serializer, not the task executor. Treat the supplied user \
                     request as a work order: decompose it but do not solve it, calculate its \
                     answer, or discuss possible answers. Think inside the scratchpad field, then \
                     populate every typed field with concrete, executable content. Never use \
                     ellipses, TODO, TBD, placeholders, or generic field names as values. The \
                     tool_families values must be exact names from available_tools; never invent \
                     a tool. Completion tests are evidence assertions, never commands or extra \
                     execution steps. Verification and reporting belong in the same task as the \
                     observation unless they require another real tool action. The \
                     scratchpad is discarded and is never sent to the executor. Return exactly one \
                     compact JSON object with this schema: {\"scratchpad\":string,\
                     \"objective\":string,\"tasks\":[{\"task_id\":string,\
                     \"objective\":string,\"tool_families\":[string],\
                     \"required_evidence\":[string],\"completion_tests\":[string]}],\
                     \"required_tool_families\":[string],\"required_evidence\":[string],\
                     \"completion_tests\":[string]}. Task objectives delegate work and never \
                     contain results. Use the smallest number of non-overlapping tasks; do not \
                     split one file inspection or one tool action into multiple tasks. The tasks \
                     array must contain no more than task_budget ordered tasks.",
                ),
                ConversationItem::user(planner_input.to_string()),
            ],
            tools: Vec::new(),
            hosted_tools: Vec::new(),
            tool_choice: None,
            model: Some(cooperation.planner_model.clone()),
            temperature: Some(0.0),
            max_output_tokens: Some(planner_completion_tokens),
            json_schema: Some(planner_output_schema(&available_tools, task_budget)),
            x_grok_req_id: Some(format!("grok-stage-planner:{prompt_id}")),
            x_grok_session_id: Some(format!("{}:planner:{prompt_id}", self.session_info.id)),
            ..ConversationRequest::default()
        };
        let planner = tokio::time::timeout(
            PLANNER_TIMEOUT,
            self.collect_cooperation_response(config, request),
        );
        let (response, prewarm_result) = tokio::join!(planner, prewarm);
        if let Err(error) = prewarm_result {
            tracing::warn!(%error, "executor prewarm failed while planner was running");
        }
        let response = response.map_err(|_| "planner timed out".to_string())??;
        let planner_text = response.assistant_text();
        let mut packet: PlanPacket = parse_json_payload(&planner_text).map_err(|error| {
            tracing::warn!(
                %error,
                "planner response did not satisfy the typed PlanPacket contract"
            );
            error
        })?;
        packet.canonicalize_task_ids();
        strengthen_presence_evidence(user_request, &available_tools, &mut packet);
        packet.validate()?;
        packet.validate_tool_families(&available_tools)?;
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
        let tool_names = conversation
            .iter()
            .filter_map(|item| match item {
                ConversationItem::Assistant(assistant) => Some(&assistant.tool_calls),
                _ => None,
            })
            .flatten()
            .map(|call| (call.id.to_string(), call.name.to_string()))
            .collect::<std::collections::HashMap<_, _>>();
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
                            normalized_tool_evidence(
                                wrapped,
                                tool_names
                                    .get(&result.tool_call_id.to_string())
                                    .map(String::as_str),
                            )
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
                        finding,
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

    pub(super) async fn bind_required_local_tool_call(
        &self,
        prompt_id: &str,
        cooperation: &TurnCooperation,
        task: &ExecutionTask,
        tool: &xai_grok_sampling_types::conversation::ToolSpec,
    ) -> Result<xai_grok_sampling_types::conversation::ToolCall, String> {
        let config = self.cooperation_config(&cooperation.executor_model).await?;
        let conversation = self.chat_state_handle.get_conversation().await;
        let user_request = conversation
            .iter()
            .rev()
            .find_map(|item| match item {
                ConversationItem::User(user) if user.prompt_index.is_some() => {
                    Some(item.text_content())
                }
                _ => None,
            })
            .unwrap_or_default();
        let mut prior_tool_calls = Vec::new();
        let mut prior_concrete_path = None;
        for call in conversation
            .iter()
            .filter_map(|item| match item {
                ConversationItem::Assistant(assistant) => Some(&assistant.tool_calls),
                _ => None,
            })
            .flatten()
        {
            let arguments = serde_json::from_str::<serde_json::Value>(&call.arguments)
                .unwrap_or_else(|_| serde_json::Value::String(call.arguments.to_string()));
            if prior_concrete_path.is_none() {
                prior_concrete_path = ["target_file", "path"]
                    .iter()
                    .find_map(|key| arguments.get(key).and_then(serde_json::Value::as_str))
                    .filter(|path| {
                        !path.is_empty()
                            && !path.contains("/path/to/")
                            && std::path::Path::new(path).exists()
                    })
                    .map(str::to_string);
            }
            prior_tool_calls.push(serde_json::json!({
                "name": call.name,
                "arguments": arguments,
            }));
        }
        let mut argument_schema = tool.parameters.clone();
        let required_exact_key = requested_assignment_key(&user_request);
        if tool.name.eq_ignore_ascii_case("grep")
            && let Some(path) = prior_concrete_path.as_ref()
        {
            if let Some(properties) = argument_schema
                .get_mut("properties")
                .and_then(serde_json::Value::as_object_mut)
            {
                properties.insert("path".to_string(), serde_json::json!({"const": path}));
                if let Some(key) = required_exact_key.as_ref() {
                    properties.insert(
                        "pattern".to_string(),
                        serde_json::json!({"const": format!(r"(?m)^\s*{key}\s*=")}),
                    );
                }
            }
            if let Some(required) = argument_schema
                .get_mut("required")
                .and_then(serde_json::Value::as_array_mut)
                && !required.iter().any(|item| item.as_str() == Some("path"))
            {
                required.push(serde_json::Value::String("path".to_string()));
            }
        }
        if tool.name.eq_ignore_ascii_case("grep")
            && let (Some(path), Some(key)) =
                (prior_concrete_path.as_ref(), required_exact_key.as_ref())
        {
            let arguments = serde_json::json!({
                "pattern": format!(r"(?m)^\s*{key}\s*="),
                "path": path,
            });
            let validator = jsonschema::validator_for(&argument_schema)
                .map_err(|error| format!("invalid schema for tool {:?}: {error}", tool.name))?;
            validator.validate(&arguments).map_err(|error| {
                format!(
                    "deterministic exact-field binding for tool {:?} is invalid: {error}",
                    tool.name
                )
            })?;
            let request_id = xai_grok_sampler::RequestId::random();
            return Ok(xai_grok_sampling_types::conversation::ToolCall {
                id: format!("call_{}", request_id.as_str().replace('-', "")).into(),
                name: tool.name.clone(),
                arguments: arguments.to_string().into(),
            });
        }
        let payload = serde_json::json!({
            "user_request": user_request,
            "required_exact_key": required_exact_key,
            "workspace_root": self.session_info.cwd,
            "task": task,
            "required_tool": {
                "name": tool.name,
                "description": tool.description,
                "argument_schema": argument_schema,
            },
            "prior_tool_calls": prior_tool_calls,
            "working_evidence": crate::agent::turn_coordinator::recent_evidence(cooperation, 8),
        });
        let request = ConversationRequest {
            items: vec![
                ConversationItem::system(
                    "Bind one already-selected local tool to concrete arguments. Return exactly \
                     one JSON object matching the supplied argument_schema. Do not answer the \
                     task, claim that tools ran, emit a tool wrapper, or invent results. Use \
                     null only where the schema permits it. Reuse concrete paths from \
                     prior_tool_calls or workspace_root; never emit example or placeholder paths. \
                     For a field search, search the anchored assignment for the exact requested \
                     key (for example, requested key foo becomes ^foo\\s*=), not the section \
                     name, prose description, or a guessed value. Do not add a language type \
                     filter unless the request specifies one.",
                ),
                ConversationItem::user(payload.to_string()),
            ],
            tools: Vec::new(),
            hosted_tools: Vec::new(),
            tool_choice: None,
            model: Some(cooperation.executor_model.clone()),
            temperature: Some(0.0),
            max_output_tokens: Some(512),
            json_schema: Some(argument_schema.clone()),
            x_grok_req_id: Some(format!("grok-stage-tool-binding:{prompt_id}:{}", tool.name)),
            x_grok_session_id: Some(format!(
                "{}:tool-binding:{prompt_id}:{}",
                self.session_info.id, tool.name
            )),
            ..ConversationRequest::default()
        };
        let response = tokio::time::timeout(
            PLANNER_TIMEOUT,
            self.collect_cooperation_response(config, request),
        )
        .await
        .map_err(|_| format!("tool argument binding timed out for {:?}", tool.name))??;
        let raw_arguments = response.assistant_text();
        let mut arguments: serde_json::Value = parse_json_payload(&raw_arguments)?;
        if let (Some(arguments), Some(properties)) = (
            arguments.as_object_mut(),
            argument_schema
                .get("properties")
                .and_then(serde_json::Value::as_object),
        ) {
            arguments.retain(|key, _| properties.contains_key(key));
            if tool.name.eq_ignore_ascii_case("grep") {
                if let Some(path) = prior_concrete_path {
                    arguments.insert("path".to_string(), serde_json::Value::String(path));
                }
                arguments.remove("type");
            }
        }
        let validator = jsonschema::validator_for(&argument_schema)
            .map_err(|error| format!("invalid schema for tool {:?}: {error}", tool.name))?;
        validator.validate(&arguments).map_err(|error| {
            format!(
                "bound arguments for tool {:?} are invalid: {error}",
                tool.name
            )
        })?;
        let arguments = serde_json::to_string(&arguments)
            .map_err(|error| format!("failed to serialize bound tool arguments: {error}"))?;
        let request_id = xai_grok_sampler::RequestId::random();
        Ok(xai_grok_sampling_types::conversation::ToolCall {
            id: format!("call_{}", request_id.as_str().replace('-', "")).into(),
            name: tool.name.clone(),
            arguments: arguments.into(),
        })
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
        let conversation = self.chat_state_handle.get_conversation().await;
        let user_request = conversation
            .iter()
            .rev()
            .find_map(|item| match item {
                ConversationItem::User(user) if user.prompt_index.is_some() => {
                    Some(item.text_content())
                }
                _ => None,
            })
            .unwrap_or_default();
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
        let evidence_ids = evidence
            .iter()
            .map(|record| record.evidence_id.0.clone())
            .collect::<HashSet<_>>();
        let mut evidence_id_values = evidence
            .iter()
            .filter(|record| matches!(record.source, EvidenceSource::ToolResult { .. }))
            .map(|record| record.evidence_id.0.clone())
            .collect::<Vec<_>>();
        if evidence_id_values.is_empty() {
            evidence_id_values.extend(evidence_ids.iter().cloned());
        }
        evidence_id_values.sort();
        if evidence_id_values.is_empty() {
            return Err("review requested without any typed evidence".to_string());
        }
        let allowed_tools = plan.tool_families();
        let payload = serde_json::json!({
            "plan": plan,
            "evidence": evidence,
            "workspace_evidence": reviewer_memory,
            "executor_response": bounded_chars(executor_response, MAX_EVIDENCE_ITEM_CHARS),
            "instruction": "Return Accept with the final user response when completion tests pass. \
                Return Correct with only the minimum remaining tasks otherwise. Before accepting, \
                verify the proposed final response itself satisfies every completion test. If the \
                request asks for observed fields, keys, values, or citations, the final response \
                must state those exact evidence facts and not merely provide a yes/no conclusion.",
        });
        let config = self.cooperation_config(&cooperation.reviewer_model).await?;
        let reviewer_completion_tokens = 768;
        let request = ConversationRequest {
            items: vec![
                ConversationItem::system(
                    "You are the review and final-synthesis stage. Use only the typed plan and \
                     observations supplied to you; do not invent tool results or transfer private \
                     reasoning. Think inside the scratchpad field, which is discarded after this \
                     generation. Never use ellipses, TODO, TBD, or placeholders. Return exactly \
                     one JSON object. Accepted schema: {\"scratchpad\":string,\
                     \"decision\":\"accept\",\"final_response\":string,\
                     \"evidence_ids\":[string],\"unresolved\":[string]}. Correction schema: \
                     {\"scratchpad\":string,\"decision\":\"correct\",\
                     \"tasks\":[ExecutionTask]}. Interpret normalized configuration observations \
                     literally: a section name is not a field, and only exact keys before '=' are \
                     fields. A requested field is present only when its exact key appears in the \
                     requested section. If a configuration finding marks that section complete \
                     and omits the key, report that the field is absent. Every accepted factual \
                     claim must cite one or more supplied evidence_id values in evidence_ids. \
                     The final_response must visibly state every exact field and value requested \
                     by the user and required by the completion tests; internal evidence_ids do \
                     not substitute for those facts in the user-visible response. \
                     Never place 'none', 'n/a', or similar placeholders in unresolved; use an \
                     empty array.",
                ),
                ConversationItem::user(payload.to_string()),
            ],
            tools: Vec::new(),
            hosted_tools: Vec::new(),
            tool_choice: None,
            model: Some(cooperation.reviewer_model.clone()),
            temperature: Some(0.0),
            max_output_tokens: Some(reviewer_completion_tokens),
            json_schema: Some(reviewer_output_schema(&allowed_tools, &evidence_id_values)),
            x_grok_req_id: Some(format!("grok-stage-reviewer:{prompt_id}")),
            x_grok_session_id: Some(format!("{}:reviewer:{prompt_id}", self.session_info.id)),
            ..ConversationRequest::default()
        };
        let response = tokio::time::timeout(
            REVIEWER_TIMEOUT,
            self.collect_cooperation_response(config, request),
        )
        .await
        .map_err(|_| "reviewer timed out".to_string())??;
        let reviewer_text = response.assistant_text();
        let mut decision: ReviewDecision = parse_json_payload(&reviewer_text).map_err(|error| {
            tracing::warn!(
                %error,
                response = %bounded_chars(&reviewer_text, 1_000),
                "reviewer response did not satisfy the typed ReviewDecision contract"
            );
            error
        })?;
        decision.canonicalize();
        decision.validate()?;
        ground_visible_evidence(&mut decision, &user_request, plan, &evidence);
        decision.validate()?;
        decision.validate_evidence_ids(&evidence_ids)?;
        decision.validate_tool_families(&allowed_tools)?;
        Ok(decision)
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

#[cfg(test)]
mod cooperation_contract_tests {
    use super::*;
    use crate::agent::turn_coordinator::ExecutionTask;

    #[test]
    fn presence_questions_require_exact_search_evidence_when_available() {
        let mut packet = PlanPacket {
            objective: "Inspect the manifest".to_string(),
            tasks: vec![ExecutionTask {
                task_id: "inspect".to_string(),
                objective: "Read Cargo.toml".to_string(),
                tool_families: vec!["read_file".to_string()],
                required_evidence: vec!["manifest".to_string()],
                completion_tests: vec!["Report the field".to_string()],
            }],
            required_tool_families: vec!["read_file".to_string()],
            required_evidence: vec!["manifest".to_string()],
            completion_tests: vec!["Report the field".to_string()],
        };

        strengthen_presence_evidence(
            "Report whether Cargo.toml declares a package name",
            &["read_file".to_string(), "grep".to_string()],
            &mut packet,
        );

        assert!(packet.tasks[0].tool_families.contains(&"grep".to_string()));
        assert!(
            packet.tasks[0]
                .required_evidence
                .iter()
                .any(|item| item.contains("zero-match"))
        );
        packet
            .validate_tool_families(&["read_file".to_string(), "grep".to_string()])
            .unwrap();
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

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use super::{
        MAX_EVIDENCE_ITEM_CHARS, ground_visible_evidence, normalized_tool_evidence,
        planner_task_budget, requested_assignment_key,
    };
    use crate::agent::turn_coordinator::{ExecutionTask, PlanPacket, ReviewDecision};
    use xai_grok_sampler::litert_lm::{EvidenceId, EvidenceRecord, EvidenceSource};
    use xai_grok_tools::util::output_filter::{
        OutputArtifactRef, OutputCursor, OutputStructure, ToolResultEnvelope,
        WrappedToolResultEnvelope,
    };

    #[test]
    fn compact_requests_receive_one_execution_task() {
        assert_eq!(
            planner_task_budget(
                "Read Cargo.toml and report whether workspace.package declares a name."
            ),
            1
        );
        assert_eq!(
            planner_task_budget("Inspect the manifest, then run the focused test."),
            12
        );
    }

    #[test]
    fn extracts_exact_assignment_key_from_presence_request() {
        assert_eq!(
            requested_assignment_key(
                "Read Cargo.toml and report whether the workspace declares a package name. Use \
                 the read-only file tools and cite the observed workspace.package fields."
            )
            .as_deref(),
            Some("name")
        );
        assert_eq!(
            requested_assignment_key("Check whether the config declares retry_limit using grep")
                .as_deref(),
            Some("retry_limit")
        );
    }

    #[test]
    fn accepted_response_renders_real_structural_evidence() {
        let plan = PlanPacket {
            objective: "Inspect workspace.package".to_string(),
            tasks: vec![ExecutionTask {
                task_id: "inspect".to_string(),
                objective: "Inspect workspace.package".to_string(),
                tool_families: vec!["read_file".to_string(), "grep".to_string()],
                required_evidence: vec!["exact field evidence".to_string()],
                completion_tests: vec!["Report exact presence or absence".to_string()],
            }],
            required_tool_families: vec!["read_file".to_string(), "grep".to_string()],
            required_evidence: vec!["exact field evidence".to_string()],
            completion_tests: vec!["Cite the observed workspace.package fields".to_string()],
        };
        let records = vec![
            EvidenceRecord {
                evidence_id: EvidenceId("tool:read:0".to_string()),
                source: EvidenceSource::ToolResult {
                    tool_call_id: "read".to_string(),
                },
                finding: serde_json::json!({
                    "tool_name": "read_file",
                    "status": "completed",
                    "structure": {
                        "match_count": 0,
                        "findings": [
                            "config section \"workspace.package\" (complete); observed fields:\n- edition = \"2024\"\n- license = \"Apache-2.0\""
                        ]
                    }
                })
                .to_string(),
                artifact: None,
                confidence: 1.0,
                observed_at: SystemTime::now(),
            },
            EvidenceRecord {
                evidence_id: EvidenceId("tool:grep:1".to_string()),
                source: EvidenceSource::ToolResult {
                    tool_call_id: "grep".to_string(),
                },
                finding: serde_json::json!({
                    "tool_name": "grep",
                    "status": "completed",
                    "structure": {"match_count": 0, "findings": []}
                })
                .to_string(),
                artifact: None,
                confidence: 1.0,
                observed_at: SystemTime::now(),
            },
        ];
        let mut decision = ReviewDecision::Accept {
            final_response: "No package name is declared.".to_string(),
            evidence_ids: vec!["tool:grep:1".to_string()],
            unresolved: Vec::new(),
        };

        ground_visible_evidence(
            &mut decision,
            "Report whether workspace.package declares a name; cite the observed fields and \
             exact search.",
            &plan,
            &records,
        );

        let ReviewDecision::Accept {
            final_response,
            evidence_ids,
            ..
        } = decision
        else {
            panic!("expected accepted review")
        };
        assert!(final_response.starts_with("No. The exact `name =`"));
        assert!(final_response.contains("edition = \"2024\""));
        assert!(final_response.contains("license = \"Apache-2.0\""));
        assert!(final_response.contains("matches=0"));
        assert!(evidence_ids.contains(&"tool:read:0".to_string()));
        assert!(evidence_ids.contains(&"tool:grep:1".to_string()));
    }

    #[test]
    fn normalized_tool_evidence_remains_bounded_valid_json() {
        let target = "config section \"workspace.package\" (complete); observed fields:\n- ";
        let wrapped = WrappedToolResultEnvelope {
            prefix: String::new(),
            envelope: ToolResultEnvelope {
                status: "completed".to_string(),
                exit_code: None,
                signal: None,
                structure: OutputStructure {
                    line_count: 10_000,
                    findings: (0..16)
                        .map(|index| format!("{target}field_{index} = {:?}", "x".repeat(800)))
                        .collect(),
                    ..OutputStructure::default()
                },
                findings: vec!["duplicate".to_string(); 16],
                preview: "ignored".repeat(10_000),
                artifact: OutputArtifactRef {
                    path: "/tmp/full-output".to_string(),
                    total_bytes: 1_000_000,
                },
                cursor: OutputCursor {
                    next_byte: 1_000_000,
                    complete: true,
                },
                truncated: true,
            },
            suffix: String::new(),
        };

        let normalized = normalized_tool_evidence(&wrapped, Some("read_file"));
        let parsed: serde_json::Value = serde_json::from_str(&normalized).unwrap();
        assert!(normalized.chars().count() <= MAX_EVIDENCE_ITEM_CHARS);
        assert_eq!(parsed["status"], "completed");
        assert_eq!(parsed["tool_name"], "read_file");
        assert_eq!(parsed["artifact"]["total_bytes"], 1_000_000);
        assert!(
            parsed["structure"]["omitted_finding_count"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert!(!normalized.contains("duplicate"));
        assert!(!normalized.contains("ignored"));
    }
}
