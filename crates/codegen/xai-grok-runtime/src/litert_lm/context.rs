#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PreparedConversation {
    pub session_id: Option<String>,
    pub system_message: Option<String>,
    /// Retrieved workspace/session memory is kept separate so strict context
    /// admission can evict it atomically without rewriting pinned system
    /// instructions.
    #[serde(default)]
    pub retrieved_memory: Option<String>,
    pub messages: String,
    pub tools: String,
    pub current_message: String,
    pub max_output_tokens: Option<u32>,
    pub temperature: Option<f32>,
    pub top_p: Option<f32>,
    pub lora_adapter: Option<LoraAdapterConfig>,
}

impl PreparedConversation {
    pub fn effective_system_message(&self) -> Option<String> {
        match (
            self.system_message.as_deref(),
            self.retrieved_memory.as_deref(),
        ) {
            (Some(system), Some(memory)) => Some(format!("{system}\n\n{memory}")),
            (Some(system), None) => Some(system.to_string()),
            (None, Some(memory)) => Some(memory.to_string()),
            (None, None) => None,
        }
    }

    fn compatibility(&self) -> ConversationCompatibility {
        ConversationCompatibility {
            system_message: self.effective_system_message(),
            tools: self.tools.clone(),
            max_output_tokens: self.max_output_tokens,
            temperature_bits: self.temperature.map(f32::to_bits),
            top_p_bits: self.top_p.map(f32::to_bits),
            lora_adapter: self.lora_adapter.clone(),
        }
    }
}

fn is_compaction_summary_message(value: &Value) -> bool {
    let text = value.to_string();
    text.contains("<conversation_summary>")
        || text.contains("This session is being continued from a previous conversation")
}

fn is_user_message(value: &Value) -> bool {
    value.get("role").and_then(Value::as_str) == Some("user")
}

fn evict_oldest_complete_turn(
    messages: &mut Vec<Value>,
    protected_prefix: usize,
    minimum_turns: usize,
) -> Option<usize> {
    let user_turns = messages[protected_prefix..]
        .iter()
        .filter(|value| is_user_message(value))
        .count();
    if user_turns <= minimum_turns {
        return None;
    }
    let turn_start = messages
        .iter()
        .enumerate()
        .skip(protected_prefix)
        .find(|(_, value)| is_user_message(value))
        .map(|(index, _)| index)?;
    let turn_end = messages
        .iter()
        .enumerate()
        .skip(turn_start + 1)
        .find(|(_, value)| is_user_message(value))
        .map_or(messages.len(), |(index, _)| index);
    (turn_end > turn_start).then(|| {
        let removed = turn_end - turn_start;
        messages.drain(turn_start..turn_end);
        removed
    })
}

fn fit_sliding_context(
    engine: &Engine,
    prepared: &mut PreparedConversation,
    target_prompt_tokens: u32,
    min_recent_turns: u32,
) -> Result<(u32, usize, usize), SamplingError> {
    let mut input_tokens = engine.measure_prompt(prepared)?;
    if input_tokens <= target_prompt_tokens {
        return Ok((input_tokens, 0, 0));
    }

    let mut messages: Vec<Value> =
        serde_json::from_str(&prepared.messages).map_err(SamplingError::Serialization)?;
    let protected_prefix = if messages.first().is_some_and(is_compaction_summary_message) {
        messages
            .iter()
            .enumerate()
            .skip(1)
            .find(|(_, value)| is_user_message(value))
            .map_or(messages.len(), |(index, _)| index)
    } else {
        0
    };
    let minimum_turns = usize::try_from(min_recent_turns).unwrap_or(usize::MAX);
    let mut dropped_messages = 0_usize;
    let mut dropped_turns = 0_usize;

    while input_tokens > target_prompt_tokens {
        let Some(removed) =
            evict_oldest_complete_turn(&mut messages, protected_prefix, minimum_turns)
        else {
            break;
        };
        dropped_messages = dropped_messages.saturating_add(removed);
        dropped_turns = dropped_turns.saturating_add(1);
        prepared.messages =
            serde_json::to_string(&messages).map_err(SamplingError::Serialization)?;
        input_tokens = engine.measure_prompt(prepared)?;
    }

    Ok((input_tokens, dropped_messages, dropped_turns))
}

struct NativeConversationConfig {
    api: Api,
    conversation: NonNull<c_void>,
    session: NonNull<c_void>,
}

impl Drop for NativeConversationConfig {
    fn drop(&mut self) {
        // SAFETY: both pointers are created and uniquely owned by this guard.
        unsafe {
            (self.api.session_config_delete)(self.session.as_ptr());
            (self.api.conversation_config_delete)(self.conversation.as_ptr());
        }
    }
}

fn build_native_conversation_config(
    api: Api,
    request: &PreparedConversation,
) -> Result<NativeConversationConfig, SamplingError> {
    // SAFETY: constructors have no arguments and return owned configs.
    let conversation = NonNull::new(unsafe { (api.conversation_config_create)() })
        .ok_or_else(|| local_error("litert_lm_create", "conversation config creation failed"))?;
    let Some(session) = NonNull::new(unsafe { (api.session_config_create)() }) else {
        // SAFETY: conversation is uniquely owned until the guard is built.
        unsafe { (api.conversation_config_delete)(conversation.as_ptr()) };
        return Err(local_error(
            "litert_lm_create",
            "session config creation failed",
        ));
    };
    let native = NativeConversationConfig {
        api,
        conversation,
        session,
    };

    if let Some(max_tokens) = request.max_output_tokens {
        if max_tokens == 0 {
            return Err(local_error(
                "litert_lm_config",
                "max_output_tokens must be greater than zero",
            ));
        }
        let max_tokens = u32_to_c_int(max_tokens, "max_output_tokens")
            .map_err(|error| local_error("litert_lm_config", error))?;
        // SAFETY: the guarded session config is live.
        unsafe { (api.session_config_set_max_output_tokens)(session.as_ptr(), max_tokens) };
    }
    if let Some(params) = sampler_params(request.temperature, request.top_p)? {
        // SAFETY: the setter copies from a live parameter struct.
        unsafe { (api.session_config_set_sampler_params)(session.as_ptr(), &params) };
    }
    if let Some(adapter) = request.lora_adapter.as_ref() {
        let path =
            path_to_cstring(&adapter.path).map_err(|error| local_error("litert_lm_lora", error))?;
        let identity = cstring(&adapter.runtime_identity(), "LoRA adapter identity")?;
        let mut error_message = std::ptr::null();
        // SAFETY: the session config and C strings remain live for this call.
        let status = unsafe {
            (api.session_config_set_lora)(
                session.as_ptr(),
                path.as_ptr(),
                identity.as_ptr(),
                &mut error_message,
            )
        };
        if status != 0 {
            let detail = if error_message.is_null() {
                format!("bridge returned status {status}")
            } else {
                // SAFETY: bridge errors are thread-local NUL-terminated strings.
                unsafe { CStr::from_ptr(error_message) }
                    .to_string_lossy()
                    .into_owned()
            };
            return Err(local_error(
                "litert_lm_lora",
                format!(
                    "failed to bind LoRA adapter `{}` from {}: {detail}",
                    adapter.id,
                    adapter.path.display()
                ),
            ));
        }
    }
    // SAFETY: LiteRT-LM copies the guarded session config.
    unsafe {
        (api.conversation_config_set_session_config)(conversation.as_ptr(), session.as_ptr())
    };

    let effective_system_message = request.effective_system_message();
    if let Some(system) = effective_system_message.as_deref() {
        let system = cstring(system, "system message")?;
        // SAFETY: this setter copies the string.
        unsafe {
            (api.conversation_config_set_system_message)(conversation.as_ptr(), system.as_ptr())
        };
    }
    let messages = cstring(&request.messages, "conversation messages")?;
    let tools = cstring(&request.tools, "tool definitions")?;
    // SAFETY: these setters copy their input strings.
    unsafe {
        (api.conversation_config_set_messages)(conversation.as_ptr(), messages.as_ptr());
        (api.conversation_config_set_tools)(conversation.as_ptr(), tools.as_ptr());
        (api.conversation_config_set_enable_constrained_decoding)(
            conversation.as_ptr(),
            request.tools != "[]",
        );
    }
    Ok(native)
}

fn partition_retrieved_memory(system_parts: Vec<String>) -> (Option<String>, Option<String>) {
    const OPEN: &str = "<memory-context>";
    const CLOSE: &str = "</memory-context>";

    let mut pinned = Vec::new();
    let mut memory = Vec::new();
    for mut part in system_parts {
        while let Some(start) = part.find(OPEN) {
            let Some(relative_end) = part[start + OPEN.len()..].find(CLOSE) else {
                break;
            };
            let end = start + OPEN.len() + relative_end + CLOSE.len();
            memory.push(part[start..end].to_string());
            part.replace_range(start..end, "");
        }
        const REMINDER_OPEN: &str = "<system-reminder>";
        const REMINDER_CLOSE: &str = "</system-reminder>";
        while let Some(start) = part.find(REMINDER_OPEN) {
            let body_start = start + REMINDER_OPEN.len();
            let Some(relative_end) = part[body_start..].find(REMINDER_CLOSE) else {
                break;
            };
            let body_end = body_start + relative_end;
            if !part[body_start..body_end].trim().is_empty() {
                break;
            }
            let end = body_end + REMINDER_CLOSE.len();
            part.replace_range(start..end, "");
        }
        let part = part.trim();
        let empty_reminder = part
            .strip_prefix("<system-reminder>")
            .and_then(|value| value.strip_suffix("</system-reminder>"))
            .is_some_and(|value| value.trim().is_empty());
        if !part.is_empty() && !empty_reminder {
            pinned.push(part.to_string());
        }
    }
    (
        (!pinned.is_empty()).then(|| pinned.join("\n\n")),
        (!memory.is_empty()).then(|| memory.join("\n\n")),
    )
}

pub fn prepare_conversation(
    request: ConversationRequest,
) -> Result<PreparedConversation, SamplingError> {
    if !request.hosted_tools.is_empty() {
        return Err(local_error(
            "litert_lm_unsupported",
            "backend-hosted tools are unavailable for local inference",
        ));
    }
    if request.json_schema.is_some() {
        return Err(local_error(
            "litert_lm_unsupported",
            "JSON Schema output constraints are not exposed by LiteRT-LM's public C API",
        ));
    }
    let disable_tools = matches!(
        request.tool_choice,
        Some(xai_grok_sampling_types::ConversationToolChoice::None)
    );
    if matches!(
        request.tool_choice,
        Some(
            xai_grok_sampling_types::ConversationToolChoice::Required
                | xai_grok_sampling_types::ConversationToolChoice::Function(_)
        )
    ) {
        return Err(local_error(
            "litert_lm_unsupported",
            "required or named tool choice is not exposed by LiteRT-LM's public C API",
        ));
    }
    let session_id = request
        .prompt_cache_key
        .clone()
        .or_else(|| request.x_grok_session_id.clone());

    let mut system_parts = Vec::new();
    let mut messages = Vec::new();
    let mut tool_names = HashMap::new();
    let mut omitted_discovery_reminders = 0usize;
    for item in request.items {
        match item {
            ConversationItem::System(system) => system_parts.push(system.content.to_string()),
            ConversationItem::User(user) => {
                // Grok persists plugin-skill and connected-MCP inventories as
                // synthetic user reminders. They are useful to large hosted
                // models, but can consume an entire 4K local window. Tool
                // schemas remain available through `tools`, including the MCP
                // discovery hooks, so omit these redundant catalogs locally.
                if is_discovery_catalog_reminder(&user.content) {
                    omitted_discovery_reminders += 1;
                    continue;
                }
                messages.push(json!({
                    "role": "user",
                    "content": content_parts_to_json(&user.content)?,
                }));
            }
            ConversationItem::Assistant(assistant) => {
                for call in &assistant.tool_calls {
                    tool_names.insert(call.id.to_string(), call.name.clone());
                }
                messages.push(assistant_message_json(&assistant));
            }
            ConversationItem::ToolResult(result) => {
                if !result.images.is_empty() {
                    return Err(local_error(
                        "litert_lm_unsupported",
                        "images in tool results are not yet supported by LiteRT-LM transport",
                    ));
                }
                messages.push(json!({
                    "role": "tool",
                    "tool_call_id": result.tool_call_id,
                    "content": [{
                        "type": "tool_response",
                        "name": tool_names.get(&result.tool_call_id).cloned().unwrap_or_default(),
                        "response": parse_json_or_string(&result.content),
                    }],
                }));
            }
            ConversationItem::Reasoning(_) => {
                // Provider-encrypted/server reasoning has no portable local
                // representation and must not be fed to another model.
            }
            ConversationItem::BackendToolCall(_) => {
                return Err(local_error(
                    "litert_lm_unsupported",
                    "backend tool-call history cannot be replayed into local inference",
                ));
            }
        }
    }

    let current_message = messages.pop().ok_or_else(|| {
        local_error(
            "litert_lm_request",
            "conversation has no user or tool message to send",
        )
    })?;
    if current_message["role"] == "assistant" {
        return Err(local_error(
            "litert_lm_request",
            "conversation must end with a user or tool message",
        ));
    }
    let current_message_text = current_message.to_string().to_ascii_lowercase();
    let offered_tool_count = request.tools.len();
    let tools = if disable_tools {
        Vec::new()
    } else {
        let mut ranked_tools = request
            .tools
            .into_iter()
            .enumerate()
            .map(|(index, tool)| {
                let name = tool.name.to_ascii_lowercase();
                let exact = usize::from(current_message_text.contains(&name));
                let token_matches = name
                    .split(['_', '-', '.'])
                    .filter(|token| token.len() >= 3 && current_message_text.contains(token))
                    .count();
                ((exact * 100) + token_matches, index, tool)
            })
            .collect::<Vec<_>>();
        ranked_tools.sort_by_key(|(score, index, _)| (std::cmp::Reverse(*score), *index));
        ranked_tools.truncate(6);
        // Preserve relevance order in the prepared payload. Strict context
        // admission can then evict complete optional schemas from the tail
        // without reparsing or splitting any schema object.
        ranked_tools
            .into_iter()
            .map(|(_, _, tool)| tool)
            .map(|mut tool| {
                compact_json_schema(&mut tool.parameters);
                json!({
                    "type": "function",
                    "function": {
                        "name": tool.name,
                        "parameters": tool.parameters,
                    }
                })
            })
            .collect::<Vec<_>>()
    };

    let (system_message, retrieved_memory) = partition_retrieved_memory(system_parts);
    let admitted_tool_count = tools.len();
    let messages = serde_json::to_string(&messages).map_err(SamplingError::Serialization)?;
    let tools = serde_json::to_string(&tools).map_err(SamplingError::Serialization)?;
    let current_message =
        serde_json::to_string(&current_message).map_err(SamplingError::Serialization)?;
    tracing::info!(
        target: crate::LOG_TARGET,
        event = "litert_context_prepared",
        system_chars = system_message.as_deref().map_or(0, str::len),
        memory_chars = retrieved_memory.as_deref().map_or(0, str::len),
        history_chars = messages.len(),
        tools_chars = tools.len(),
        current_chars = current_message.len(),
        admitted_tool_schemas = admitted_tool_count,
        omitted_tool_schemas = offered_tool_count.saturating_sub(admitted_tool_count),
        omitted_discovery_reminders,
        "prepared local inference context"
    );

    Ok(PreparedConversation {
        session_id,
        system_message,
        retrieved_memory,
        messages,
        tools,
        current_message,
        max_output_tokens: request.max_output_tokens,
        temperature: request.temperature,
        top_p: request.top_p,
        lora_adapter: None,
    })
}

fn assistant_message_json(assistant: &AssistantItem) -> Value {
    let calls = assistant
        .tool_calls
        .iter()
        .map(|call| {
            json!({
                "id": call.id,
                "type": "function",
                "function": {
                    "name": call.name,
                    "arguments": parse_json_or_string(&call.arguments),
                }
            })
        })
        .collect::<Vec<_>>();
    let mut message = json!({
        "role": "assistant",
        "content": [{"type": "text", "text": assistant.content}],
    });
    if !calls.is_empty() {
        message["tool_calls"] = Value::Array(calls);
    }
    message
}

fn next_history_messages(
    prepared: &PreparedConversation,
    assistant: &AssistantItem,
) -> Result<String, SamplingError> {
    let mut messages: Vec<Value> =
        serde_json::from_str(&prepared.messages).map_err(SamplingError::Serialization)?;
    let current =
        serde_json::from_str(&prepared.current_message).map_err(SamplingError::Serialization)?;
    messages.push(current);
    messages.push(assistant_message_json(assistant));
    serde_json::to_string(&messages).map_err(SamplingError::Serialization)
}

fn is_discovery_catalog_reminder(parts: &[ContentPart]) -> bool {
    let mut saw_text = false;
    for part in parts {
        let ContentPart::Text { text } = part else {
            return false;
        };
        saw_text = true;
        let trimmed = text.trim_start();
        if !(trimmed.starts_with("<system-reminder>\nThe following skills are available")
            || trimmed.starts_with("<system-reminder>\nMCP server connected:")
            || trimmed.starts_with("<system-reminder>\nMCP servers connected:"))
        {
            return false;
        }
    }
    saw_text
}

const REQUIRED_PROPERTY_DESCRIPTION_MAX_CHARS: usize = 120;

fn truncate_schema_description(description: &str) -> String {
    let trimmed = description.trim();
    if trimmed.chars().count() <= REQUIRED_PROPERTY_DESCRIPTION_MAX_CHARS {
        return trimmed.to_owned();
    }
    let mut truncated = trimmed
        .chars()
        .take(REQUIRED_PROPERTY_DESCRIPTION_MAX_CHARS.saturating_sub(1))
        .collect::<String>();
    truncated.push('…');
    truncated
}

/// Remove prose that consumes scarce local-model context while retaining a
/// short description for every required argument. Constraints alone tell a
/// model *what shape* to emit; required-field descriptions tell a compact
/// model *what value belongs there*.
fn compact_json_schema(value: &mut Value) {
    match value {
        Value::Object(object) => {
            let required = object
                .get("required")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect::<std::collections::HashSet<_>>();

            for key in [
                "description",
                "markdownDescription",
                "title",
                "examples",
                "example",
                "default",
                "$comment",
                "deprecated",
            ] {
                object.remove(key);
            }

            if let Some(properties) = object.get_mut("properties").and_then(Value::as_object_mut) {
                for (name, child) in properties {
                    let required_description = required
                        .contains(name)
                        .then(|| {
                            child
                                .get("description")
                                .and_then(Value::as_str)
                                .map(truncate_schema_description)
                        })
                        .flatten();
                    compact_json_schema(child);
                    if let Some(description) = required_description
                        && let Some(child_object) = child.as_object_mut()
                    {
                        child_object.insert("description".to_owned(), Value::String(description));
                    }
                }
            }

            for (key, child) in object {
                if key != "properties" {
                    compact_json_schema(child);
                }
            }
        }
        Value::Array(values) => {
            for child in values {
                compact_json_schema(child);
            }
        }
        _ => {}
    }
}

fn content_parts_to_json(parts: &[ContentPart]) -> Result<Vec<Value>, SamplingError> {
    parts
        .iter()
        .map(|part| match part {
            ContentPart::Text { text } => Ok(json!({"type": "text", "text": text})),
            ContentPart::Image { url } => {
                if let Some((_, blob)) = url.split_once(";base64,") {
                    Ok(json!({"type": "image", "blob": blob}))
                } else if let Some(path) = url.strip_prefix("file://") {
                    Ok(json!({"type": "image", "path": path}))
                } else if url.starts_with('/') {
                    Ok(json!({"type": "image", "path": url}))
                } else {
                    Err(local_error(
                        "litert_lm_unsupported",
                        "local inference accepts image data URIs or absolute local paths only",
                    ))
                }
            }
        })
        .collect()
}
