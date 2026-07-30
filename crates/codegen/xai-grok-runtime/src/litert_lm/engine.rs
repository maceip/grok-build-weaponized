struct Engine {
    api: Api,
    ptr: NonNull<c_void>,
    _library: Library,
    gate: Arc<Semaphore>,
    poisoned: AtomicBool,
    build_id: String,
    resident_adapters: StdMutex<HashMap<String, u64>>,
    adapter_clock: AtomicU64,
}

// SAFETY: all access to the engine and conversations derived from it is
// serialized by `gate`. The library and engine outlive each conversation.
unsafe impl Send for Engine {}
// SAFETY: see the `Send` implementation; no function is invoked concurrently.
unsafe impl Sync for Engine {}

impl Engine {
    fn load(config: &LiteRtLmConfig) -> Result<Self, String> {
        // SAFETY: loading a user-configured native library is the explicit
        // contract of the `litert-lm://` transport. No symbol is invoked until
        // `Api::load` has verified the required ABI surface.
        let library = unsafe { Library::new(&config.library_path) }.map_err(|error| {
            format!("failed to load {}: {error}", config.library_path.display())
        })?;
        // SAFETY: validated by `Api::load`; the Library is stored in Engine.
        let api = unsafe { Api::load(&library)? };
        // SAFETY: these bridge functions take no pointers and return immutable
        // process-lifetime metadata.
        let abi_version = unsafe { (api.bridge_abi_version)() };
        if abi_version != BRIDGE_ABI_VERSION {
            return Err(format!(
                "unsupported Grok LiteRT-LM bridge ABI {abi_version}; expected {BRIDGE_ABI_VERSION}"
            ));
        }
        // SAFETY: see the ABI version call above.
        let capabilities = unsafe { (api.bridge_capabilities)() };
        let missing_capabilities = REQUIRED_BRIDGE_CAPABILITIES & !capabilities;
        if missing_capabilities != 0 {
            return Err(format!(
                "LiteRT-LM bridge is missing required capabilities 0x{missing_capabilities:x}"
            ));
        }
        // SAFETY: the bridge returns a process-lifetime NUL-terminated literal.
        let build_id_ptr = unsafe { (api.bridge_build_id)() };
        if build_id_ptr.is_null() {
            return Err("LiteRT-LM bridge returned a null build identifier".to_owned());
        }
        // SAFETY: null was rejected and the bridge contract guarantees a
        // process-lifetime NUL-terminated string.
        let build_id = unsafe { CStr::from_ptr(build_id_ptr) }
            .to_string_lossy()
            .into_owned();
        let model_path = path_to_cstring(&config.model_path)?;
        let backend = CString::new(config.backend.as_str())
            .map_err(|_| "LiteRT-LM backend contains a NUL byte".to_owned())?;
        // SAFETY: pointers come from owned CStrings and remain valid for the
        // duration of the call. Null optional backends are permitted by the ABI.
        let settings = unsafe {
            (api.engine_settings_create)(
                model_path.as_ptr(),
                backend.as_ptr(),
                std::ptr::null(),
                std::ptr::null(),
            )
        };
        let settings = NonNull::new(settings)
            .ok_or_else(|| "LiteRT-LM settings creation failed".to_owned())?;

        if let Some(max_tokens) = config.max_context_tokens {
            // SAFETY: settings is a valid object returned above.
            unsafe {
                (api.engine_settings_set_max_num_tokens)(
                    settings.as_ptr(),
                    u32_to_c_int(max_tokens, "max_context_tokens")?,
                );
            }
        }
        if !config.supported_lora_ranks.is_empty() {
            let mut error_message = std::ptr::null();
            // SAFETY: settings and the rank slice remain valid for this
            // synchronous copying call.
            let status = unsafe {
                (api.engine_settings_set_supported_lora_ranks)(
                    settings.as_ptr(),
                    config.supported_lora_ranks.as_ptr(),
                    config.supported_lora_ranks.len(),
                    &mut error_message,
                )
            };
            if status != 0 {
                let detail = bridge_error_detail(status, error_message);
                // SAFETY: settings is still owned locally and engine creation
                // has not occurred.
                unsafe { (api.engine_settings_delete)(settings.as_ptr()) };
                return Err(format!(
                    "failed to configure supported LoRA ranks: {detail}"
                ));
            }
        }
        // Runtime benchmark counters are the C API's authoritative source for
        // prompt and completion token usage. Keep them enabled for every local
        // engine so model-visible usage and the token monitor stay exact.
        // SAFETY: settings is a valid object returned above.
        unsafe { (api.engine_settings_enable_benchmark)(settings.as_ptr()) };

        // SAFETY: settings remains valid for the call and the returned engine
        // has independent ownership according to the C API.
        let ptr = unsafe { (api.engine_create)(settings.as_ptr()) };
        // SAFETY: settings must be destroyed regardless of engine creation.
        unsafe { (api.engine_settings_delete)(settings.as_ptr()) };
        let ptr = NonNull::new(ptr).ok_or_else(|| "LiteRT-LM engine creation failed".to_owned())?;

        Ok(Self {
            api,
            ptr,
            _library: library,
            gate: Arc::new(Semaphore::new(1)),
            poisoned: AtomicBool::new(false),
            build_id,
            resident_adapters: StdMutex::new(HashMap::new()),
            adapter_clock: AtomicU64::new(1),
        })
    }

    fn count_tokens(&self, text: &str) -> Result<u32, SamplingError> {
        u32::try_from(self.tokenize(text)?.len()).map_err(|_| {
            local_error(
                "litert_lm_tokenize",
                "token count exceeds the supported u32 range",
            )
        })
    }

    fn measure_prompt(&self, request: &PreparedConversation) -> Result<u32, SamplingError> {
        let native = build_native_conversation_config(self.api, request)?;
        let message = cstring(&request.current_message, "current message")?;
        let mut token_count = 0_usize;
        let mut error_message = std::ptr::null();
        // SAFETY: the engine, guarded config, message, and out-pointers remain
        // live for the duration of this non-mutating bridge call.
        let status = unsafe {
            (self.api.conversation_measure_prompt)(
                self.ptr.as_ptr(),
                native.conversation.as_ptr(),
                message.as_ptr(),
                &mut token_count,
                &mut error_message,
            )
        };
        if status != 0 {
            let detail = if error_message.is_null() {
                native_last_error(&self.api)
                    .unwrap_or_else(|| format!("bridge returned status {status}"))
            } else {
                // SAFETY: bridge errors are thread-local NUL-terminated strings.
                unsafe { CStr::from_ptr(error_message) }
                    .to_string_lossy()
                    .into_owned()
            };
            return Err(local_error(
                "litert_lm_measure",
                format!("exact prompt rendering failed: {detail}"),
            ));
        }
        u32::try_from(token_count).map_err(|_| {
            local_error(
                "litert_lm_measure",
                "exact rendered prompt exceeds the supported u32 token range",
            )
        })
    }

    fn preload_adapter(&self, adapter: &LoraAdapterConfig) -> Result<(), SamplingError> {
        // SAFETY: constructor has no arguments and returns an owned config.
        let session_config = NonNull::new(unsafe { (self.api.session_config_create)() })
            .ok_or_else(|| local_error("litert_lm_lora", "session config creation failed"))?;
        let result = (|| {
            let path = path_to_cstring(&adapter.path)
                .map_err(|error| local_error("litert_lm_lora", error))?;
            let identity = cstring(&adapter.runtime_identity(), "LoRA adapter identity")?;
            let mut error_message = std::ptr::null();
            // SAFETY: all pointers remain live for this binding call.
            let status = unsafe {
                (self.api.session_config_set_lora)(
                    session_config.as_ptr(),
                    path.as_ptr(),
                    identity.as_ptr(),
                    &mut error_message,
                )
            };
            if status != 0 {
                return Err(local_error(
                    "litert_lm_lora",
                    bridge_error_detail(status, error_message),
                ));
            }
            // The pinned LiteRT engine materializes and selects the adapter
            // while creating a session. The disposable session warms the
            // adapter without retaining KV state.
            // SAFETY: engine and session config are valid and owned here.
            let session = unsafe {
                (self.api.engine_create_session)(self.ptr.as_ptr(), session_config.as_ptr())
            };
            let session = NonNull::new(session).ok_or_else(|| {
                local_error(
                    "litert_lm_lora",
                    native_last_error(&self.api)
                        .unwrap_or_else(|| "adapter probe session creation failed".to_owned()),
                )
            })?;
            // SAFETY: the disposable probe session is uniquely owned.
            unsafe { (self.api.session_delete)(session.as_ptr()) };
            Ok(())
        })();
        // SAFETY: the session config is uniquely owned.
        unsafe { (self.api.session_config_delete)(session_config.as_ptr()) };
        result
    }

    fn unload_adapter(&self, adapter: &LoraAdapterConfig) -> Result<(), SamplingError> {
        let identity = cstring(&adapter.runtime_identity(), "LoRA adapter identity")?;
        let mut error_message = std::ptr::null();
        // SAFETY: engine and identity remain live for this synchronous call.
        let status = unsafe {
            (self.api.engine_unload_lora)(self.ptr.as_ptr(), identity.as_ptr(), &mut error_message)
        };
        if status != 0 {
            return Err(local_error(
                "litert_lm_lora_unload",
                bridge_error_detail(status, error_message),
            ));
        }
        self.resident_adapters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&adapter.runtime_identity());
        Ok(())
    }

    fn tokenize(&self, text: &str) -> Result<Vec<c_int>, SamplingError> {
        let text = cstring(text, "tokenization input")?;
        // SAFETY: engine is live and text remains valid for the duration of
        // the call. The result is an owned opaque C object.
        let result = unsafe { (self.api.engine_tokenize)(self.ptr.as_ptr(), text.as_ptr()) };
        let result = NonNull::new(result).ok_or_else(|| {
            local_error(
                "litert_lm_tokenize",
                "runtime failed to tokenize the prepared request",
            )
        })?;
        // SAFETY: result remains live until the delete call below.
        let count = unsafe { (self.api.tokenize_result_get_num_tokens)(result.as_ptr()) };
        // SAFETY: result remains live until the delete call below.
        let tokens = unsafe { (self.api.tokenize_result_get_tokens)(result.as_ptr()) };
        if tokens.is_null() && count != 0 {
            // SAFETY: result is owned by this function and destroyed exactly once.
            unsafe { (self.api.tokenize_result_delete)(result.as_ptr()) };
            return Err(local_error(
                "litert_lm_tokenize",
                "runtime returned a null token buffer for non-empty output",
            ));
        }
        // SAFETY: the C result owns `count` consecutive token ids for its
        // lifetime. Copy them before deleting the result.
        let tokens = if count == 0 {
            Vec::new()
        } else {
            unsafe { std::slice::from_raw_parts(tokens, count) }.to_vec()
        };
        // SAFETY: result is owned by this function and destroyed exactly once.
        unsafe { (self.api.tokenize_result_delete)(result.as_ptr()) };
        Ok(tokens)
    }

    fn detokenize(&self, tokens: &[c_int]) -> Result<String, SamplingError> {
        if tokens.is_empty() {
            return Ok(String::new());
        }
        // SAFETY: engine is live and the token slice remains valid for the
        // duration of the call. The returned opaque object is owned here.
        let result = unsafe {
            (self.api.engine_detokenize)(self.ptr.as_ptr(), tokens.as_ptr(), tokens.len())
        };
        let result = NonNull::new(result).ok_or_else(|| {
            local_error(
                "litert_lm_detokenize",
                "runtime failed to detokenize the bounded output",
            )
        })?;
        // SAFETY: result remains live until the delete call below.
        let text = unsafe { (self.api.detokenize_result_get_string)(result.as_ptr()) };
        let detokenized = if text.is_null() {
            Err(local_error(
                "litert_lm_detokenize",
                "runtime returned a null detokenized string",
            ))
        } else {
            // SAFETY: null was rejected and the result owns a NUL-terminated
            // string until deletion.
            Ok(unsafe { CStr::from_ptr(text) }
                .to_string_lossy()
                .into_owned())
        };
        // SAFETY: result is owned by this function and destroyed exactly once.
        unsafe { (self.api.detokenize_result_delete)(result.as_ptr()) };
        detokenized
    }

    fn poison(&self) {
        self.poisoned.store(true, Ordering::Release);
    }

    fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    /// Reserve a resident adapter slot. LiteRT-LM can switch among adapters
    /// already loaded by one base-model engine, but the pinned runtime does
    /// not expose individual adapter eviction. When the bounded resident set
    /// is full, return false so the caller can replace the engine wholesale
    /// and release every adapter allocation deterministically.
    fn admit_adapter(&self, adapter_id: &str, max_resident: u32) -> AdapterAdmission {
        let mut adapters = self
            .resident_adapters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let now = self.adapter_clock.fetch_add(1, Ordering::Relaxed);
        if let Some(last_used) = adapters.get_mut(adapter_id) {
            *last_used = now;
            return AdapterAdmission::Resident;
        }
        if adapters.len() >= usize::try_from(max_resident).unwrap_or(usize::MAX) {
            return AdapterAdmission::Full;
        }
        adapters.insert(adapter_id.to_owned(), now);
        AdapterAdmission::New
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AdapterAdmission {
    Resident,
    New,
    Full,
}

impl Drop for Engine {
    fn drop(&mut self) {
        // SAFETY: ptr was returned by engine_create and is destroyed once.
        unsafe { (self.api.engine_delete)(self.ptr.as_ptr()) };
    }
}

// Keep healthy engines resident and singleflight cold initialization per
// configuration. A poisoned engine's cell is removed before the next lookup,
// allowing a replacement to load while the abandoned native state remains
// quarantined.
type EngineCell = Arc<OnceCell<Arc<Engine>>>;
static ENGINE_CACHE: OnceLock<AsyncMutex<HashMap<String, EngineCell>>> = OnceLock::new();

async fn get_engine(config: LiteRtLmConfig) -> Result<Arc<Engine>, SamplingError> {
    let key = config.runtime_key();
    let cache = ENGINE_CACHE.get_or_init(|| AsyncMutex::new(HashMap::new()));
    loop {
        let cell = {
            let mut cache = cache.lock().await;
            let poisoned = cache
                .get(&key)
                .and_then(|cell| cell.get())
                .is_some_and(|engine| engine.is_poisoned());
            if poisoned {
                cache.remove(&key);
            }
            Arc::clone(
                cache
                    .entry(key.clone())
                    .or_insert_with(|| Arc::new(OnceCell::new())),
            )
        };
        let load_config = config.clone();
        let engine = cell
            .get_or_try_init(|| async move {
                tokio::task::spawn_blocking(move || Engine::load(&load_config))
                    .await
                    .map_err(|error| local_error("litert_lm_join", error.to_string()))?
                    .map(Arc::new)
                    .map_err(|error| local_error("litert_lm_load", error))
            })
            .await?
            .clone();
        if !engine.is_poisoned() {
            return Ok(engine);
        }
    }
}

/// Load and validate a local engine without starting a conversation.
pub async fn prewarm(config: LiteRtLmConfig) -> Result<(), SamplingError> {
    let engine = get_engine(config).await?;
    if engine.is_poisoned() {
        return Err(local_error(
            "litert_lm_prewarm",
            "prewarmed local engine is poisoned",
        ));
    }
    Ok(())
}

/// Materialize an adapter in an existing base engine and validate it by
/// creating a disposable native session. The base engine remains resident.
pub async fn prewarm_adapter(
    config: LiteRtLmConfig,
    adapter: LoraAdapterConfig,
) -> Result<(), SamplingError> {
    let engine = get_engine(config.clone()).await?;
    let _permit = Arc::clone(&engine.gate)
        .acquire_owned()
        .await
        .map_err(|_| local_error("litert_lm_gate", "engine gate closed"))?;
    match engine.admit_adapter(&adapter.runtime_identity(), config.max_resident_adapters) {
        AdapterAdmission::Resident => Ok(()),
        AdapterAdmission::Full => Err(local_error(
            "litert_lm_lora_capacity",
            "adapter residency limit reached; evict an unleased adapter before prewarming",
        )),
        AdapterAdmission::New => {
            if let Err(error) = engine.preload_adapter(&adapter) {
                // Session creation can fail after the native engine has
                // materialized the tensors. Roll the native revision back
                // before releasing its Rust residency record.
                let _ = engine.unload_adapter(&adapter);
                engine
                    .resident_adapters
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .remove(&adapter.runtime_identity());
                return Err(error);
            }
            Ok(())
        }
    }
}

/// Release one adapter revision and only the cached KV sessions created with
/// that revision.
pub async fn unload_adapter(
    config: LiteRtLmConfig,
    adapter: LoraAdapterConfig,
) -> Result<(), SamplingError> {
    let engine = get_engine(config).await?;
    let _permit = Arc::clone(&engine.gate)
        .acquire_owned()
        .await
        .map_err(|_| local_error("litert_lm_gate", "engine gate closed"))?;
    evict_cached_conversations_for_adapter(&engine, &adapter);
    engine.unload_adapter(&adapter)
}

/// Render through the same native prompt-template path used by generation and
/// count the resulting tokenizer IDs.
pub async fn measure_prepared(
    config: LiteRtLmConfig,
    prepared: &PreparedConversation,
) -> Result<u32, SamplingError> {
    let engine = get_engine(config).await?;
    let _permit = Arc::clone(&engine.gate)
        .acquire_owned()
        .await
        .map_err(|_| local_error("litert_lm_gate", "engine gate closed"))?;
    if engine.is_poisoned() {
        return Err(local_error(
            "litert_lm_measure",
            "local engine became unavailable while measuring context",
        ));
    }
    engine.measure_prompt(prepared)
}

/// Release all cached native conversations for a logical session.
pub fn drop_session(session_id: &str) -> usize {
    let cache = SESSION_CACHE.get_or_init(|| StdMutex::new(HashMap::new()));
    let mut cache = cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let suffix = format!("\0{session_id}");
    let before = cache.len();
    cache.retain(|key, _| !key.ends_with(&suffix));
    before.saturating_sub(cache.len())
}

/// Drop the least-recently-used inactive KV conversations until at most
/// `max_remaining` entries remain. Conversations currently generating are
/// temporarily removed from this cache and can never be selected here.
pub fn drop_inactive_sessions(max_remaining: u32) -> usize {
    let cache = SESSION_CACHE.get_or_init(|| StdMutex::new(HashMap::new()));
    let mut cache = cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let before = cache.len();
    let max_remaining = usize::try_from(max_remaining).unwrap_or(usize::MAX);
    while cache.len() > max_remaining {
        let Some(oldest) = cache
            .iter()
            .min_by_key(|(_, session)| session.last_used)
            .map(|(key, _)| key.clone())
        else {
            break;
        };
        cache.remove(&oldest);
    }
    before.saturating_sub(cache.len())
}
