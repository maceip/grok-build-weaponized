fn start_stream(
    engine: Arc<Engine>,
    request: PreparedConversation,
    callback_tx: SyncSender<CallbackEvent>,
) -> Result<Conversation, SamplingError> {
    let api = engine.api;
    let native = build_native_conversation_config(api, &request)?;
    // SAFETY: engine and config are live for the call.
    let conversation_ptr =
        unsafe { (api.conversation_create)(engine.ptr.as_ptr(), native.conversation.as_ptr()) };
    let conversation_ptr = NonNull::new(conversation_ptr).ok_or_else(|| {
        local_error(
            "litert_lm_create",
            native_last_error(&api).unwrap_or_else(|| "conversation creation failed".to_owned()),
        )
    })?;
    let conversation = Conversation {
        engine,
        ptr: conversation_ptr,
    };

    let message = cstring(&request.current_message, "current message")?;
    let callback_data = Box::into_raw(Box::new(CallbackState { tx: callback_tx })).cast::<c_void>();
    // SAFETY: all pointers are valid. The callback allocation is reclaimed
    // by the final callback, or immediately below if startup fails.
    let status = unsafe {
        (api.conversation_send_message_stream)(
            conversation.ptr.as_ptr(),
            message.as_ptr(),
            std::ptr::null(),
            stream_callback,
            callback_data,
        )
    };
    if status != 0 {
        // SAFETY: no callback owns the allocation when startup fails.
        unsafe { drop(Box::from_raw(callback_data.cast::<CallbackState>())) };
        return Err(local_error(
            "litert_lm_start",
            format!("stream startup failed with status {status}"),
        ));
    }
    Ok(conversation)
}

fn continue_stream(
    conversation: Conversation,
    current_message: String,
    callback_tx: SyncSender<CallbackEvent>,
) -> Result<Conversation, SamplingError> {
    let message = cstring(&current_message, "current message")?;
    let callback_data = Box::into_raw(Box::new(CallbackState { tx: callback_tx })).cast::<c_void>();
    // SAFETY: the cached conversation is live, the engine permit is held by
    // the caller, and the callback allocation remains owned by the native
    // stream until its terminal callback.
    let status = unsafe {
        (conversation.engine.api.conversation_send_message_stream)(
            conversation.ptr.as_ptr(),
            message.as_ptr(),
            std::ptr::null(),
            stream_callback,
            callback_data,
        )
    };
    if status != 0 {
        // SAFETY: startup failure means the callback never took ownership.
        unsafe { drop(Box::from_raw(callback_data.cast::<CallbackState>())) };
        return Err(local_error(
            "litert_lm_start",
            format!("stream continuation failed with status {status}"),
        ));
    }
    Ok(conversation)
}

fn bridge_callback_events(
    native_rx: SyncReceiver<CallbackEvent>,
    async_tx: mpsc::Sender<CallbackEvent>,
) {
    while let Ok(event) = native_rx.recv() {
        let terminal = matches!(event, CallbackEvent::Final(_));
        if async_tx.blocking_send(event).is_err() || terminal {
            break;
        }
    }
}

fn sampler_params(
    temperature: Option<f32>,
    top_p: Option<f32>,
) -> Result<Option<LiteRtLmSamplerParams>, SamplingError> {
    // SessionConfig::CreateDefault currently leaves `k` at zero, which the
    // engine rejects during conversation creation. Always install a complete
    // sampler config; request/model values override these metadata-compatible
    // defaults.
    let temperature = temperature.unwrap_or(1.0);
    if !temperature.is_finite() || temperature < 0.0 {
        return Err(local_error(
            "litert_lm_config",
            "temperature must be finite and non-negative",
        ));
    }
    let top_p = top_p.unwrap_or(0.95);
    if !top_p.is_finite() || !(0.0..=1.0).contains(&top_p) {
        return Err(local_error(
            "litert_lm_config",
            "top_p must be finite and between 0 and 1",
        ));
    }
    let greedy = temperature == 0.0;
    Ok(Some(LiteRtLmSamplerParams {
        // The pinned LiteRT runtime does not implement the GREEDY enum on all
        // backends. Top-p with k=1 is exactly greedy and is portable.
        sampler_type: SAMPLER_TOP_P,
        top_k: if greedy { 1 } else { 40 },
        top_p,
        temperature: if greedy { 1.0 } else { temperature },
        seed: 0,
    }))
}

pub enum LocalInferenceResult {
    Completed {
        response: Box<ConversationResponse>,
        metrics: InferenceLatencyStats,
    },
    Cancelled,
}

pub async fn run_local_request(
    request_id: String,
    request: ConversationRequest,
    config: LiteRtLmConfig,
    model: String,
    event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    cancel_token: &CancellationToken,
) -> Result<LocalInferenceResult, SamplingError> {
    let prepared = prepare_conversation(request)?;
    run_prepared_request(request_id, prepared, config, model, event_tx, cancel_token).await
}

pub async fn run_prepared_request(
    request_id: String,
    mut prepared: PreparedConversation,
    config: LiteRtLmConfig,
    model: String,
    event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    cancel_token: &CancellationToken,
) -> Result<LocalInferenceResult, SamplingError> {
    prepared.lora_adapter = config.lora_adapter.clone();
    let engine_cache_key = config.runtime_key();
    let (engine, _permit, new_adapter): (Arc<Engine>, OwnedSemaphorePermit, bool) = loop {
        let engine = get_engine(config.clone()).await?;
        let permit = Arc::clone(&engine.gate)
            .acquire_owned()
            .await
            .map_err(|_| local_error("litert_lm_gate", "engine gate closed"))?;
        if engine.is_poisoned() {
            continue;
        }
        let admission =
            prepared
                .lora_adapter
                .as_ref()
                .map_or(AdapterAdmission::Resident, |adapter| {
                    engine.admit_adapter(&adapter.runtime_identity(), config.max_resident_adapters)
                });
        match admission {
            AdapterAdmission::Full => {
                let adapter = prepared
                    .lora_adapter
                    .as_ref()
                    .expect("full admission requires an adapter");
                tracing::info!(
                    target: crate::LOG_TARGET,
                    event = "litert_lora_residency_rollover",
                    adapter_id = %adapter.id,
                    max_resident_adapters = config.max_resident_adapters,
                    "replacing local engine to bound resident LoRA memory"
                );
                engine.poison();
                drop(permit);
                evict_cached_conversations_for_engine(&engine);
                continue;
            }
            AdapterAdmission::Resident | AdapterAdmission::New => {
                break (engine, permit, admission == AdapterAdmission::New);
            }
        }
    };
    let mut input_tokens = engine.measure_prompt(&prepared)?;
    if let Some(context_limit) = config.max_context_tokens {
        let safety_margin = crate::context::ContextBudgetBroker::safety_margin(context_limit);
        if config.context_strategy == ContextOverflowStrategy::Sliding {
            let desired_completion = prepared
                .max_output_tokens
                .unwrap_or_else(|| (context_limit / 8).max(1))
                .min((context_limit / 2).max(1));
            let target_prompt_tokens = context_limit
                .saturating_sub(safety_margin)
                .saturating_sub(desired_completion)
                .max(1);
            let (fitted_tokens, dropped_messages, dropped_turns) = fit_sliding_context(
                &engine,
                &mut prepared,
                target_prompt_tokens,
                config.min_recent_turns,
            )?;
            input_tokens = fitted_tokens;
            if dropped_messages != 0 {
                tracing::info!(
                    target: crate::LOG_TARGET,
                    event = "litert_active_context_slid",
                    dropped_messages,
                    dropped_turns,
                    retained_input_tokens = input_tokens,
                    target_prompt_tokens,
                    min_recent_turns = config.min_recent_turns,
                    "evicted oldest complete turns from active local context"
                );
            }
        }
        let available_completion_tokens = context_limit
            .checked_sub(input_tokens)
            .and_then(|tokens| tokens.checked_sub(safety_margin))
            .ok_or_else(|| {
                local_error(
                    "litert_lm_context",
                    format!(
                        "prepared request uses {input_tokens} input tokens plus a \
                         {safety_margin}-token safety margin, \
                         exceeding the {context_limit}-token local context window"
                    ),
                )
            })?;
        if available_completion_tokens == 0 {
            return Err(local_error(
                "litert_lm_context",
                format!(
                    "prepared request leaves no completion capacity in the \
                     {context_limit}-token local context window"
                ),
            ));
        }
        match prepared.max_output_tokens {
            Some(requested) if requested > available_completion_tokens => {
                return Err(local_error(
                    "litert_lm_context",
                    format!(
                        "prepared request leaves {available_completion_tokens} completion tokens \
                         after exact measurement, below the required {requested}-token reserve \
                         (prompt={input_tokens}, safety={safety_margin}, window={context_limit})"
                    ),
                ));
            }
            None => prepared.max_output_tokens = Some(available_completion_tokens),
            _ => {}
        }
    }

    let session_cache_key = prepared.session_id.as_ref().map(|session_id| {
        conversation_cache_key(
            &engine_cache_key,
            session_id,
            prepared.lora_adapter.as_ref(),
        )
    });
    let cached = session_cache_key
        .as_deref()
        .and_then(|key| take_cached_conversation(key, &prepared));
    if cached.is_none() {
        // The pinned LiteRT CPU executor supports exactly one native session
        // per engine. A different logical session therefore has to release
        // the inactive KV owner before a new conversation can be created.
        // Same-session turns take their cached conversation above and retain
        // KV continuity. Multi-session concurrency is provided by supervised
        // engine replicas, not by violating the native executor contract.
        evict_cached_conversations_for_engine(&engine);
    }
    let previous_usage = cached
        .as_ref()
        .map_or((0, 0), |cached| cached.cumulative_usage);
    let reused_conversation = cached.is_some();

    let (native_tx, native_rx) = sync_channel(CALLBACK_BUFFER_CAPACITY);
    let (async_tx, mut rx) = mpsc::channel(CALLBACK_BUFFER_CAPACITY);
    let callback_bridge =
        tokio::task::spawn_blocking(move || bridge_callback_events(native_rx, async_tx));

    let stream_start = Instant::now();
    let prepared_for_start = prepared.clone();
    let start_engine = Arc::clone(&engine);
    let start_result = tokio::task::spawn_blocking(move || {
        if let Some(cached) = cached {
            continue_stream(
                cached.conversation,
                prepared_for_start.current_message,
                native_tx,
            )
        } else {
            start_stream(start_engine, prepared_for_start, native_tx)
        }
    })
    .await;
    let conversation = match start_result {
        Ok(Ok(conversation)) => conversation,
        Ok(Err(error)) => {
            if new_adapter {
                // The native resource manager may have loaded weights before
                // a later conversation-construction step failed. Its ABI has
                // no unload primitive, so retire this engine rather than
                // silently undercounting resident adapter memory.
                engine.poison();
                evict_cached_conversations_for_engine(&engine);
            }
            return Err(error);
        }
        Err(error) => {
            // A panic leaves native ownership indeterminate regardless of
            // adapter state; quarantine the engine before propagating.
            engine.poison();
            evict_cached_conversations_for_engine(&engine);
            return Err(local_error("litert_lm_join", error.to_string()));
        }
    };
    tracing::info!(
        target: crate::LOG_TARGET,
        event = "litert_session_started",
        reused_conversation,
        input_tokens,
        bridge_build_id = %conversation.engine.build_id,
        lora_adapter_id = prepared.lora_adapter.as_ref().map(|adapter| adapter.id.as_str()),
        "started local inference session"
    );
    let _ = event_tx.send(RuntimeEvent::StreamStarted {
        request_id: request_id.clone(),
        timestamp_ms: chrono::Utc::now().timestamp_millis(),
    });

    let mut text = String::new();
    let mut reasoning = String::new();
    let mut reasoning_filter = ReasoningTagFilter::default();
    let mut tool_calls = Vec::new();
    let mut chunk_timestamps = Vec::new();
    let mut message_chunks_emitted = 0_u64;
    let mut first_token_emitted = false;
    let mut user_cancelled = false;
    let mut stopping = false;
    let mut terminal_error = None;
    let cancellation_timeout = tokio::time::sleep(Duration::from_secs(365 * 24 * 60 * 60));
    tokio::pin!(cancellation_timeout);

    loop {
        tokio::select! {
            biased;
            _ = cancel_token.cancelled(), if !stopping => {
                user_cancelled = true;
                stopping = true;
                conversation.cancel();
                cancellation_timeout.as_mut().reset(
                    tokio::time::Instant::now() + CANCEL_DRAIN_TIMEOUT
                );
            }
            _ = &mut cancellation_timeout, if stopping => {
                tracing::error!(
                    "LiteRT-LM did not finish cancellation; quarantining engine"
                );
                conversation.engine.poison();
                std::mem::forget(conversation);
                if let Some(error) = terminal_error {
                    return Err(error);
                }
                return Ok(LocalInferenceResult::Cancelled);
            }
            event = rx.recv() => {
                match event {
                    Some(CallbackEvent::Chunk(chunk)) => {
                        if stopping {
                            continue;
                        }
                        let parsed = match parse_response_chunk(&chunk) {
                            Ok(parsed) => parsed,
                            Err(error) => {
                                terminal_error = Some(error);
                                stopping = true;
                                conversation.cancel();
                                cancellation_timeout.as_mut().reset(
                                    tokio::time::Instant::now() + CANCEL_DRAIN_TIMEOUT
                                );
                                continue;
                            }
                        };
                        for fragment in parsed.text {
                            for filtered in reasoning_filter.push(&fragment) {
                                if !first_token_emitted {
                                    first_token_emitted = true;
                                    let _ = event_tx.send(RuntimeEvent::FirstToken {
                                        request_id: request_id.clone(),
                                    });
                                }
                                let chunk_index =
                                    u64::try_from(chunk_timestamps.len()).unwrap_or(u64::MAX);
                                chunk_timestamps.push(Instant::now());
                                match filtered.channel {
                                    LocalTextChannel::Text => {
                                        message_chunks_emitted =
                                            message_chunks_emitted.saturating_add(1);
                                        text.push_str(&filtered.text);
                                        let _ = event_tx.send(RuntimeEvent::ChannelToken {
                                            request_id: request_id.clone(),
                                            channel: RuntimeChannel::Text,
                                            text: filtered.text,
                                            chunk_index,
                                        });
                                    }
                                    LocalTextChannel::Reasoning => {
                                        reasoning.push_str(&filtered.text);
                                        let _ = event_tx.send(RuntimeEvent::ChannelToken {
                                            request_id: request_id.clone(),
                                            channel: RuntimeChannel::Reasoning,
                                            text: filtered.text,
                                            chunk_index,
                                        });
                                    }
                                }
                            }
                        }
                        for call in parsed.tool_calls {
                            let index = u32::try_from(tool_calls.len()).unwrap_or(u32::MAX);
                            let _ = event_tx.send(RuntimeEvent::ToolCallDelta {
                                request_id: request_id.clone(),
                                tool_index: index,
                                id: Some(call.id.to_string()),
                                name: Some(call.name.clone()),
                                arguments_delta: Some(call.arguments.to_string()),
                            });
                            tool_calls.push(call);
                        }
                    }
                    Some(CallbackEvent::Final(Ok(()))) | None => {
                        for filtered in reasoning_filter.finish() {
                            if !first_token_emitted {
                                first_token_emitted = true;
                                let _ = event_tx.send(RuntimeEvent::FirstToken {
                                    request_id: request_id.clone(),
                                });
                            }
                            let chunk_index =
                                u64::try_from(chunk_timestamps.len()).unwrap_or(u64::MAX);
                            chunk_timestamps.push(Instant::now());
                            match filtered.channel {
                                LocalTextChannel::Text => {
                                    message_chunks_emitted =
                                        message_chunks_emitted.saturating_add(1);
                                    text.push_str(&filtered.text);
                                    let _ = event_tx.send(RuntimeEvent::ChannelToken {
                                        request_id: request_id.clone(),
                                        channel: RuntimeChannel::Text,
                                        text: filtered.text,
                                        chunk_index,
                                    });
                                }
                                LocalTextChannel::Reasoning => {
                                    reasoning.push_str(&filtered.text);
                                    let _ = event_tx.send(RuntimeEvent::ChannelToken {
                                        request_id: request_id.clone(),
                                        channel: RuntimeChannel::Reasoning,
                                        text: filtered.text,
                                        chunk_index,
                                    });
                                }
                            }
                        }
                        break;
                    }
                    Some(CallbackEvent::Final(Err(error))) => {
                        if !stopping {
                            terminal_error = Some(local_error("litert_lm_stream", error));
                        }
                        break;
                    }
                }
            }
        }
    }
    let _ = callback_bridge.await;

    if let Some(error) = terminal_error {
        return Err(error);
    }
    if user_cancelled {
        return Ok(LocalInferenceResult::Cancelled);
    }

    let cumulative_usage = conversation.token_usage()?;
    let current_prefill_tokens = cumulative_usage
        .0
        .checked_sub(previous_usage.0)
        .ok_or_else(|| {
            local_error(
                "litert_lm_metrics",
                "runtime prefill counters moved backwards across a reused conversation",
            )
        })?;
    let completion_tokens = cumulative_usage
        .1
        .checked_sub(previous_usage.1)
        .ok_or_else(|| {
            local_error(
                "litert_lm_metrics",
                "runtime decode counters moved backwards across a reused conversation",
            )
        })?;
    let cached_prompt_tokens = previous_usage
        .0
        .checked_add(previous_usage.1)
        .ok_or_else(|| local_error("litert_lm_metrics", "cached token count overflow"))?;
    let prompt_tokens = cached_prompt_tokens
        .checked_add(current_prefill_tokens)
        .ok_or_else(|| local_error("litert_lm_metrics", "prompt token count overflow"))?;
    let total_tokens = prompt_tokens
        .checked_add(completion_tokens)
        .ok_or_else(|| local_error("litert_lm_metrics", "total token count overflow"))?;
    let stream_end = Instant::now();
    let mut metrics =
        InferenceLatencyStats::from_timestamps(stream_start, &chunk_timestamps, stream_end);
    metrics.attempts = 1;
    let stop_reason = if tool_calls.is_empty() {
        StopReason::Stop
    } else {
        StopReason::ToolCalls
    };
    let reasoning_tokens = if reasoning.is_empty() {
        0
    } else {
        conversation
            .engine
            .count_tokens(&reasoning)?
            .min(completion_tokens)
    };
    let assistant = AssistantItem {
        content: Arc::<str>::from(text),
        tool_calls,
        model_id: Some(model),
        model_fingerprint: None,
        reasoning_effort: None,
    };
    if let Some(session_cache_key) = session_cache_key
        && !conversation.engine.is_poisoned()
        && let Ok(expected_messages) = next_history_messages(&prepared, &assistant)
    {
        cache_conversation(
            session_cache_key,
            CachedConversation {
                conversation,
                expected_messages,
                compatibility: prepared.compatibility(),
                cumulative_usage,
                resident_tokens: cumulative_usage.0.saturating_add(cumulative_usage.1),
                max_resident_sessions: config.max_resident_sessions,
                max_resident_context_tokens: config.max_resident_context_tokens,
                last_used: 0,
            },
        );
    }
    let mut items = Vec::with_capacity(if reasoning.is_empty() { 1 } else { 2 });
    if !reasoning.is_empty() {
        items.push(ConversationItem::Reasoning(
            xai_grok_sampling_types::synthesized_reasoning_item(reasoning),
        ));
    }
    items.push(ConversationItem::Assistant(assistant));
    let response = ConversationResponse {
        items,
        stop_reason: Some(stop_reason),
        usage: Some(TokenUsage {
            prompt_tokens,
            completion_tokens,
            total_tokens,
            reasoning_tokens,
            cached_prompt_tokens,
        }),
        cost_usd_ticks: None,
        message_chunks_emitted,
        doom_loop_signals: Vec::new(),
        stop_message: None,
    };

    Ok(LocalInferenceResult::Completed {
        response: Box::new(response),
        metrics,
    })
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum LocalTextChannel {
    #[default]
    Text,
    Reasoning,
}

#[derive(Debug, Eq, PartialEq)]
struct FilteredLocalText {
    channel: LocalTextChannel,
    text: String,
}

#[derive(Default)]
struct ReasoningTagFilter {
    channel: LocalTextChannel,
    pending: String,
}

impl ReasoningTagFilter {
    fn push(&mut self, fragment: &str) -> Vec<FilteredLocalText> {
        self.pending.push_str(fragment);
        self.drain(false)
    }

    fn finish(&mut self) -> Vec<FilteredLocalText> {
        self.drain(true)
    }

    fn drain(&mut self, finish: bool) -> Vec<FilteredLocalText> {
        let mut output = Vec::new();
        loop {
            let marker = match self.channel {
                LocalTextChannel::Text => "<think>",
                LocalTextChannel::Reasoning => "</think>",
            };
            if let Some(index) = self.pending.find(marker) {
                let before = self.pending[..index].to_owned();
                if !before.is_empty() {
                    output.push(FilteredLocalText {
                        channel: self.channel,
                        text: before,
                    });
                }
                self.pending.drain(..index + marker.len());
                self.channel = match self.channel {
                    LocalTextChannel::Text => LocalTextChannel::Reasoning,
                    LocalTextChannel::Reasoning => LocalTextChannel::Text,
                };
                continue;
            }

            let retained = if finish {
                0
            } else {
                longest_marker_prefix_suffix(&self.pending, marker)
            };
            let emit_len = self.pending.len().saturating_sub(retained);
            if emit_len != 0 {
                let text = self.pending[..emit_len].to_owned();
                self.pending.drain(..emit_len);
                output.push(FilteredLocalText {
                    channel: self.channel,
                    text,
                });
            }
            break;
        }
        output
    }
}

fn longest_marker_prefix_suffix(value: &str, marker: &str) -> usize {
    let max = value.len().min(marker.len().saturating_sub(1));
    (1..=max)
        .rev()
        .find(|&length| value.ends_with(&marker[..length]))
        .unwrap_or(0)
}

#[derive(Default)]
struct ParsedChunk {
    text: Vec<String>,
    tool_calls: Vec<ToolCall>,
}

fn parse_response_chunk(chunk: &str) -> Result<ParsedChunk, SamplingError> {
    let value: Value = serde_json::from_str(chunk).map_err(SamplingError::Serialization)?;
    let mut parsed = ParsedChunk::default();
    if let Some(content) = value.get("content").and_then(Value::as_array) {
        for part in content {
            if part.get("type").and_then(Value::as_str) == Some("text")
                && let Some(text) = part.get("text").and_then(Value::as_str)
            {
                parsed.text.push(text.to_owned());
            }
        }
    }
    if let Some(calls) = value.get("tool_calls").and_then(Value::as_array) {
        for call in calls {
            let Some(function) = call.get("function") else {
                continue;
            };
            let Some(name) = function.get("name").and_then(Value::as_str) else {
                continue;
            };
            let arguments = function
                .get("arguments")
                .map(|value| match value {
                    Value::String(value) => value.clone(),
                    value => value.to_string(),
                })
                .unwrap_or_else(|| "{}".to_owned());
            let id = call
                .get("id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| format!("call_{}", uuid::Uuid::new_v4().simple()));
            parsed.tool_calls.push(ToolCall {
                id: Arc::<str>::from(id),
                name: name.to_owned(),
                arguments: Arc::<str>::from(arguments),
            });
        }
    }
    Ok(parsed)
}

fn parse_json_or_string(value: &str) -> Value {
    serde_json::from_str(value).unwrap_or_else(|_| Value::String(value.to_owned()))
}
