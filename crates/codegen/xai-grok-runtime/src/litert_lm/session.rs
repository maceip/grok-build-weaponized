#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundedLocalText {
    pub text: String,
    pub original_tokens: u32,
    pub retained_tokens: u32,
    pub truncated: bool,
}

/// Apply an exact active-model token budget to text destined for local model
/// history. Returns `Ok(None)` when the sampler is not a LiteRT-LM transport so
/// callers can use their normal provider-specific fallback.
pub async fn bound_text(
    base_url: &str,
    text: &str,
    max_tokens: u32,
) -> Result<Option<BoundedLocalText>, SamplingError> {
    if max_tokens == 0 {
        return Ok(Some(BoundedLocalText {
            text: String::new(),
            original_tokens: 0,
            retained_tokens: 0,
            truncated: !text.is_empty(),
        }));
    }
    let Some(config) = LiteRtLmConfig::from_base_url(base_url)
        .map_err(|error| local_error("litert_lm_config", error))?
    else {
        return Ok(None);
    };
    let engine = get_engine(config).await?;
    let _permit = Arc::clone(&engine.gate)
        .acquire_owned()
        .await
        .map_err(|_| local_error("litert_lm_gate", "engine gate closed"))?;
    if engine.is_poisoned() {
        return Err(local_error(
            "litert_lm_gate",
            "local engine became unavailable while budgeting output",
        ));
    }
    let tokens = engine.tokenize(text)?;
    let original_tokens = u32::try_from(tokens.len()).unwrap_or(u32::MAX);
    if original_tokens <= max_tokens {
        return Ok(Some(BoundedLocalText {
            text: text.to_owned(),
            original_tokens,
            retained_tokens: original_tokens,
            truncated: false,
        }));
    }

    let marker = format!(
        "\n\n... [middle truncated: original_tokens={original_tokens}, \
         retained_budget={max_tokens}] ...\n\n"
    );
    let marker_tokens = engine.tokenize(&marker)?;
    let max_tokens_usize = usize::try_from(max_tokens).unwrap_or(usize::MAX);
    if marker_tokens.len() >= max_tokens_usize {
        let bounded = engine.detokenize(&marker_tokens[..max_tokens_usize])?;
        let retained_tokens = engine.count_tokens(&bounded)?.min(max_tokens);
        return Ok(Some(BoundedLocalText {
            text: bounded,
            original_tokens,
            retained_tokens,
            truncated: true,
        }));
    }

    let retained_payload = max_tokens_usize - marker_tokens.len();
    let head_count = retained_payload / 2;
    let mut tail_count = retained_payload - head_count;
    let head = engine.detokenize(&tokens[..head_count])?;
    let mut bounded = loop {
        let tail_start = tokens.len().saturating_sub(tail_count);
        let tail = engine.detokenize(&tokens[tail_start..])?;
        let candidate = format!("{head}{marker}{tail}");
        let count = engine.count_tokens(&candidate)?;
        if count <= max_tokens || tail_count == 0 {
            break candidate;
        }
        let overshoot = usize::try_from(count - max_tokens)
            .unwrap_or(usize::MAX)
            .max(1);
        tail_count = tail_count.saturating_sub(overshoot);
    };
    let mut retained_tokens = engine.count_tokens(&bounded)?;
    if retained_tokens > max_tokens {
        let final_tokens = engine.tokenize(&bounded)?;
        bounded = engine.detokenize(&final_tokens[..max_tokens_usize])?;
        retained_tokens = engine.count_tokens(&bounded)?.min(max_tokens);
    }
    Ok(Some(BoundedLocalText {
        text: bounded,
        original_tokens,
        retained_tokens,
        truncated: true,
    }))
}

struct Conversation {
    engine: Arc<Engine>,
    ptr: NonNull<c_void>,
}

// SAFETY: the single engine permit remains owned by the async request for the
// entire lifetime of this conversation.
unsafe impl Send for Conversation {}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ConversationCompatibility {
    system_message: Option<String>,
    tools: String,
    max_output_tokens: Option<u32>,
    temperature_bits: Option<u32>,
    top_p_bits: Option<u32>,
    lora_adapter: Option<LoraAdapterConfig>,
}

struct CachedConversation {
    conversation: Conversation,
    expected_messages: String,
    compatibility: ConversationCompatibility,
    cumulative_usage: (u32, u32),
    resident_tokens: u32,
    max_resident_sessions: u32,
    max_resident_context_tokens: u32,
    last_used: u64,
}

static SESSION_CACHE: OnceLock<StdMutex<HashMap<String, CachedConversation>>> = OnceLock::new();
static SESSION_CACHE_CLOCK: AtomicU64 = AtomicU64::new(1);

pub fn resident_session_stats() -> (u32, u64) {
    let Some(cache) = SESSION_CACHE.get() else {
        return (0, 0);
    };
    let cache = cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let sessions = u32::try_from(cache.len()).unwrap_or(u32::MAX);
    let tokens = cache.values().fold(0_u64, |total, session| {
        total.saturating_add(u64::from(session.resident_tokens))
    });
    (sessions, tokens)
}

fn take_cached_conversation(
    key: &str,
    prepared: &PreparedConversation,
) -> Option<CachedConversation> {
    let cache = SESSION_CACHE.get_or_init(|| StdMutex::new(HashMap::new()));
    let cached = cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(key)?;
    if cached.conversation.engine.is_poisoned()
        || cached.expected_messages != prepared.messages
        || cached.compatibility != prepared.compatibility()
    {
        return None;
    }
    Some(cached)
}

fn cache_conversation(key: String, mut cached: CachedConversation) {
    cached.last_used = SESSION_CACHE_CLOCK.fetch_add(1, Ordering::Relaxed);
    let cache = SESSION_CACHE.get_or_init(|| StdMutex::new(HashMap::new()));
    let mut cache = cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let engine = Arc::clone(&cached.conversation.engine);
    let max_resident_sessions = cached.max_resident_sessions;
    let max_resident_context_tokens = cached.max_resident_context_tokens;
    cache.insert(key, cached);
    loop {
        let mut resident_sessions = 0_usize;
        let mut resident_tokens = 0_u64;
        let mut oldest = None::<(String, u64)>;
        for (key, session) in cache.iter() {
            if Arc::ptr_eq(&session.conversation.engine, &engine) {
                resident_sessions = resident_sessions.saturating_add(1);
                resident_tokens =
                    resident_tokens.saturating_add(u64::from(session.resident_tokens));
                if oldest
                    .as_ref()
                    .is_none_or(|(_, oldest_used)| session.last_used < *oldest_used)
                {
                    oldest = Some((key.clone(), session.last_used));
                }
            }
        }
        if resident_sessions <= usize::try_from(max_resident_sessions).unwrap_or(usize::MAX)
            && resident_tokens <= u64::from(max_resident_context_tokens)
        {
            break;
        }
        let Some((oldest_key, _)) = oldest else {
            break;
        };
        cache.remove(&oldest_key);
    }
    while cache.len() > SESSION_CACHE_CAPACITY {
        let Some(oldest_key) = cache
            .iter()
            .min_by_key(|(_, session)| session.last_used)
            .map(|(key, _)| key.clone())
        else {
            break;
        };
        cache.remove(&oldest_key);
    }
}

fn evict_cached_conversations_for_engine(engine: &Arc<Engine>) {
    let cache = SESSION_CACHE.get_or_init(|| StdMutex::new(HashMap::new()));
    cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .retain(|_, cached| !Arc::ptr_eq(&cached.conversation.engine, engine));
}

fn evict_cached_conversations_for_adapter(engine: &Arc<Engine>, adapter: &LoraAdapterConfig) {
    let cache = SESSION_CACHE.get_or_init(|| StdMutex::new(HashMap::new()));
    cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .retain(|_, cached| {
            !Arc::ptr_eq(&cached.conversation.engine, engine)
                || cached.compatibility.lora_adapter.as_ref() != Some(adapter)
        });
}

fn conversation_cache_key(
    engine_cache_key: &str,
    session_id: &str,
    adapter: Option<&LoraAdapterConfig>,
) -> String {
    let adapter_identity = adapter
        .map(LoraAdapterConfig::runtime_identity)
        .unwrap_or_else(|| "base".to_string());
    // Keep the logical session id last so drop_session can release every
    // adapter-specific KV lineage with one suffix match.
    format!("{engine_cache_key}\0adapter={adapter_identity}\0{session_id}")
}

impl Conversation {
    fn cancel(&self) {
        // SAFETY: ptr remains valid and the C API explicitly allows
        // cancellation from asynchronous inference.
        unsafe { (self.engine.api.conversation_cancel_process)(self.ptr.as_ptr()) };
    }

    fn token_usage(&self) -> Result<(u32, u32), SamplingError> {
        let api = self.engine.api;
        // SAFETY: ptr is a live conversation and benchmark collection was
        // enabled before engine construction.
        let info = unsafe { (api.conversation_get_benchmark_info)(self.ptr.as_ptr()) };
        let info = NonNull::new(info).ok_or_else(|| {
            local_error(
                "litert_lm_metrics",
                "runtime returned no benchmark information for token accounting",
            )
        })?;

        let result = (|| {
            // SAFETY: info remains live until the end of this scope.
            let prefill_turns =
                unsafe { (api.benchmark_info_get_num_prefill_turns)(info.as_ptr()) };
            // SAFETY: info remains live until the end of this scope.
            let decode_turns = unsafe { (api.benchmark_info_get_num_decode_turns)(info.as_ptr()) };
            let prefill_turns = u32::try_from(prefill_turns).map_err(|_| {
                local_error(
                    "litert_lm_metrics",
                    "runtime reported a negative prefill-turn count",
                )
            })?;
            let decode_turns = u32::try_from(decode_turns).map_err(|_| {
                local_error(
                    "litert_lm_metrics",
                    "runtime reported a negative decode-turn count",
                )
            })?;

            let prompt_tokens = sum_token_counts(
                info,
                prefill_turns,
                api.benchmark_info_get_prefill_token_count_at,
                "prefill",
            )?;
            let completion_tokens = sum_token_counts(
                info,
                decode_turns,
                api.benchmark_info_get_decode_token_count_at,
                "decode",
            )?;
            Ok((prompt_tokens, completion_tokens))
        })();

        // SAFETY: info is owned by this call and must be destroyed once.
        unsafe { (api.benchmark_info_delete)(info.as_ptr()) };
        result
    }
}

fn sum_token_counts(
    info: NonNull<c_void>,
    turns: u32,
    get_count: BenchmarkGetTokenCount,
    phase: &str,
) -> Result<u32, SamplingError> {
    let mut total = 0_u32;
    for index in 0..turns {
        let index = c_int::try_from(index).map_err(|_| {
            local_error(
                "litert_lm_metrics",
                format!("{phase} turn index exceeds the C API integer range"),
            )
        })?;
        // SAFETY: index is within the turn count returned for this info object.
        let count = unsafe { get_count(info.as_ptr(), index) };
        let count = u32::try_from(count).map_err(|_| {
            local_error(
                "litert_lm_metrics",
                format!("runtime reported a negative {phase} token count"),
            )
        })?;
        total = total.checked_add(count).ok_or_else(|| {
            local_error("litert_lm_metrics", format!("{phase} token count overflow"))
        })?;
    }
    Ok(total)
}

impl Drop for Conversation {
    fn drop(&mut self) {
        // SAFETY: ptr was returned by conversation_create and is destroyed once,
        // after the final callback has been observed.
        unsafe { (self.engine.api.conversation_delete)(self.ptr.as_ptr()) };
    }
}

enum CallbackEvent {
    Chunk(String),
    Final(Result<(), String>),
}

struct CallbackState {
    tx: SyncSender<CallbackEvent>,
}

unsafe extern "C" fn stream_callback(
    callback_data: *mut c_void,
    chunk: *const c_char,
    is_final: bool,
    error: *const c_char,
) {
    if callback_data.is_null() {
        return;
    }
    // SAFETY: callback_data is allocated in `start_stream` and remains owned
    // by the C callback until the one final callback.
    let state = unsafe { &*(callback_data.cast::<CallbackState>()) };
    if is_final {
        let result = if error.is_null() {
            Ok(())
        } else {
            // SAFETY: LiteRT-LM promises a NUL-terminated error string valid
            // for the duration of this callback.
            Err(unsafe { CStr::from_ptr(error) }
                .to_string_lossy()
                .into_owned())
        };
        let _ = state.tx.send(CallbackEvent::Final(result));
        // SAFETY: the C API emits exactly one final callback.
        unsafe { drop(Box::from_raw(callback_data.cast::<CallbackState>())) };
    } else if !chunk.is_null() {
        // SAFETY: LiteRT-LM promises a NUL-terminated chunk string valid for
        // the duration of this callback.
        let chunk = unsafe { CStr::from_ptr(chunk) }
            .to_string_lossy()
            .into_owned();
        let _ = state.tx.send(CallbackEvent::Chunk(chunk));
    }
}
