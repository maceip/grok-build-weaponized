type EngineSettingsCreate =
    unsafe extern "C" fn(*const c_char, *const c_char, *const c_char, *const c_char) -> *mut c_void;
type PtrDelete = unsafe extern "C" fn(*mut c_void);
type BridgeAbiVersion = unsafe extern "C" fn() -> u32;
type BridgeCapabilities = unsafe extern "C" fn() -> u64;
type BridgeBuildId = unsafe extern "C" fn() -> *const c_char;
type LastErrorMessage = unsafe extern "C" fn() -> *const c_char;
type SettingsSetInt = unsafe extern "C" fn(*mut c_void, c_int);
type SettingsEnable = unsafe extern "C" fn(*mut c_void);
type SettingsSetSupportedLoraRanks =
    unsafe extern "C" fn(*mut c_void, *const u32, usize, *mut *const c_char) -> c_int;
type EngineCreate = unsafe extern "C" fn(*const c_void) -> *mut c_void;
type EngineCreateSession = unsafe extern "C" fn(*mut c_void, *mut c_void) -> *mut c_void;
type EngineUnloadLora =
    unsafe extern "C" fn(*mut c_void, *const c_char, *mut *const c_char) -> c_int;
type EngineTokenize = unsafe extern "C" fn(*mut c_void, *const c_char) -> *mut c_void;
type EngineDetokenize = unsafe extern "C" fn(*mut c_void, *const c_int, usize) -> *mut c_void;
type TokenizeResultGetTokens = unsafe extern "C" fn(*const c_void) -> *const c_int;
type TokenizeResultGetNumTokens = unsafe extern "C" fn(*const c_void) -> usize;
type DetokenizeResultGetString = unsafe extern "C" fn(*const c_void) -> *const c_char;
type ConfigCreate = unsafe extern "C" fn() -> *mut c_void;
type ConfigSetString = unsafe extern "C" fn(*mut c_void, *const c_char);
type ConfigSetBool = unsafe extern "C" fn(*mut c_void, bool);
type ConversationConfigSetSession = unsafe extern "C" fn(*mut c_void, *const c_void);
type SessionConfigSetSampler = unsafe extern "C" fn(*mut c_void, *const LiteRtLmSamplerParams);
type SessionConfigSetLora =
    unsafe extern "C" fn(*mut c_void, *const c_char, *const c_char, *mut *const c_char) -> c_int;
type ConversationCreate = unsafe extern "C" fn(*mut c_void, *mut c_void) -> *mut c_void;
type ConversationMeasurePrompt = unsafe extern "C" fn(
    *mut c_void,
    *mut c_void,
    *const c_char,
    *mut usize,
    *mut *const c_char,
) -> c_int;
type StreamCallback = unsafe extern "C" fn(*mut c_void, *const c_char, bool, *const c_char);
type ConversationSendStream = unsafe extern "C" fn(
    *mut c_void,
    *const c_char,
    *const c_char,
    StreamCallback,
    *mut c_void,
) -> c_int;
type ConversationGetBenchmarkInfo = unsafe extern "C" fn(*mut c_void) -> *mut c_void;
type BenchmarkGetTurns = unsafe extern "C" fn(*const c_void) -> c_int;
type BenchmarkGetTokenCount = unsafe extern "C" fn(*const c_void, c_int) -> c_int;
#[derive(Clone, Copy)]
struct Api {
    bridge_abi_version: BridgeAbiVersion,
    bridge_capabilities: BridgeCapabilities,
    bridge_build_id: BridgeBuildId,
    last_error_message: LastErrorMessage,
    engine_settings_create: EngineSettingsCreate,
    engine_settings_delete: PtrDelete,
    engine_settings_set_max_num_tokens: SettingsSetInt,
    engine_settings_set_supported_lora_ranks: SettingsSetSupportedLoraRanks,
    engine_settings_enable_benchmark: SettingsEnable,
    engine_create: EngineCreate,
    engine_delete: PtrDelete,
    engine_create_session: EngineCreateSession,
    session_delete: PtrDelete,
    engine_unload_lora: EngineUnloadLora,
    session_config_create: ConfigCreate,
    session_config_delete: PtrDelete,
    session_config_set_max_output_tokens: SettingsSetInt,
    session_config_set_sampler_params: SessionConfigSetSampler,
    session_config_set_lora: SessionConfigSetLora,
    conversation_config_create: ConfigCreate,
    conversation_config_delete: PtrDelete,
    conversation_config_set_session_config: ConversationConfigSetSession,
    conversation_config_set_system_message: ConfigSetString,
    conversation_config_set_tools: ConfigSetString,
    conversation_config_set_messages: ConfigSetString,
    conversation_config_set_enable_constrained_decoding: ConfigSetBool,
    conversation_config_set_json_schema: ConfigSetString,
    conversation_create: ConversationCreate,
    conversation_delete: PtrDelete,
    conversation_measure_prompt: ConversationMeasurePrompt,
    conversation_send_message_stream: ConversationSendStream,
    conversation_cancel_process: PtrDelete,
    conversation_get_benchmark_info: ConversationGetBenchmarkInfo,
    benchmark_info_delete: PtrDelete,
    benchmark_info_get_num_prefill_turns: BenchmarkGetTurns,
    benchmark_info_get_num_decode_turns: BenchmarkGetTurns,
    benchmark_info_get_prefill_token_count_at: BenchmarkGetTokenCount,
    benchmark_info_get_decode_token_count_at: BenchmarkGetTokenCount,
    engine_tokenize: EngineTokenize,
    engine_detokenize: EngineDetokenize,
    tokenize_result_delete: PtrDelete,
    tokenize_result_get_tokens: TokenizeResultGetTokens,
    tokenize_result_get_num_tokens: TokenizeResultGetNumTokens,
    detokenize_result_delete: PtrDelete,
    detokenize_result_get_string: DetokenizeResultGetString,
}

impl Api {
    unsafe fn load(library: &Library) -> Result<Self, String> {
        macro_rules! symbol {
            ($name:literal, $ty:ty) => {{
                // SAFETY: every requested symbol and signature is copied from
                // LiteRT-LM's public c/engine.h ABI. `Engine` retains the
                // Library for at least as long as any copied function pointer.
                unsafe {
                    *library
                        .get::<$ty>(concat!($name, "\0").as_bytes())
                        .map_err(|error| format!("missing LiteRT-LM symbol `{}`: {error}", $name))?
                }
            }};
        }

        Ok(Self {
            bridge_abi_version: symbol!("grok_litert_bridge_abi_version", BridgeAbiVersion),
            bridge_capabilities: symbol!("grok_litert_bridge_capabilities", BridgeCapabilities),
            bridge_build_id: symbol!("grok_litert_bridge_build_id", BridgeBuildId),
            last_error_message: symbol!("grok_litert_last_error_message", LastErrorMessage),
            engine_settings_create: symbol!(
                "litert_lm_engine_settings_create",
                EngineSettingsCreate
            ),
            engine_settings_delete: symbol!("litert_lm_engine_settings_delete", PtrDelete),
            engine_settings_set_max_num_tokens: symbol!(
                "litert_lm_engine_settings_set_max_num_tokens",
                SettingsSetInt
            ),
            engine_settings_set_supported_lora_ranks: symbol!(
                "grok_litert_engine_settings_set_supported_lora_ranks",
                SettingsSetSupportedLoraRanks
            ),
            engine_settings_enable_benchmark: symbol!(
                "litert_lm_engine_settings_enable_benchmark",
                SettingsEnable
            ),
            engine_create: symbol!("litert_lm_engine_create", EngineCreate),
            engine_delete: symbol!("litert_lm_engine_delete", PtrDelete),
            engine_create_session: symbol!("litert_lm_engine_create_session", EngineCreateSession),
            session_delete: symbol!("litert_lm_session_delete", PtrDelete),
            engine_unload_lora: symbol!("grok_litert_engine_unload_lora", EngineUnloadLora),
            session_config_create: symbol!("litert_lm_session_config_create", ConfigCreate),
            session_config_delete: symbol!("litert_lm_session_config_delete", PtrDelete),
            session_config_set_max_output_tokens: symbol!(
                "litert_lm_session_config_set_max_output_tokens",
                SettingsSetInt
            ),
            session_config_set_sampler_params: symbol!(
                "litert_lm_session_config_set_sampler_params",
                SessionConfigSetSampler
            ),
            session_config_set_lora: symbol!(
                "grok_litert_session_config_set_lora_file",
                SessionConfigSetLora
            ),
            conversation_config_create: symbol!(
                "litert_lm_conversation_config_create",
                ConfigCreate
            ),
            conversation_config_delete: symbol!("litert_lm_conversation_config_delete", PtrDelete),
            conversation_config_set_session_config: symbol!(
                "litert_lm_conversation_config_set_session_config",
                ConversationConfigSetSession
            ),
            conversation_config_set_system_message: symbol!(
                "litert_lm_conversation_config_set_system_message",
                ConfigSetString
            ),
            conversation_config_set_tools: symbol!(
                "litert_lm_conversation_config_set_tools",
                ConfigSetString
            ),
            conversation_config_set_messages: symbol!(
                "litert_lm_conversation_config_set_messages",
                ConfigSetString
            ),
            conversation_config_set_enable_constrained_decoding: symbol!(
                "litert_lm_conversation_config_set_enable_constrained_decoding",
                ConfigSetBool
            ),
            conversation_config_set_json_schema: symbol!(
                "grok_litert_conversation_config_set_json_schema",
                ConfigSetString
            ),
            conversation_create: symbol!("litert_lm_conversation_create", ConversationCreate),
            conversation_delete: symbol!("litert_lm_conversation_delete", PtrDelete),
            conversation_measure_prompt: symbol!(
                "grok_litert_conversation_measure_prompt",
                ConversationMeasurePrompt
            ),
            conversation_send_message_stream: symbol!(
                "litert_lm_conversation_send_message_stream",
                ConversationSendStream
            ),
            conversation_cancel_process: symbol!(
                "litert_lm_conversation_cancel_process",
                PtrDelete
            ),
            conversation_get_benchmark_info: symbol!(
                "litert_lm_conversation_get_benchmark_info",
                ConversationGetBenchmarkInfo
            ),
            benchmark_info_delete: symbol!("litert_lm_benchmark_info_delete", PtrDelete),
            benchmark_info_get_num_prefill_turns: symbol!(
                "litert_lm_benchmark_info_get_num_prefill_turns",
                BenchmarkGetTurns
            ),
            benchmark_info_get_num_decode_turns: symbol!(
                "litert_lm_benchmark_info_get_num_decode_turns",
                BenchmarkGetTurns
            ),
            benchmark_info_get_prefill_token_count_at: symbol!(
                "litert_lm_benchmark_info_get_prefill_token_count_at",
                BenchmarkGetTokenCount
            ),
            benchmark_info_get_decode_token_count_at: symbol!(
                "litert_lm_benchmark_info_get_decode_token_count_at",
                BenchmarkGetTokenCount
            ),
            engine_tokenize: symbol!("litert_lm_engine_tokenize", EngineTokenize),
            engine_detokenize: symbol!("litert_lm_engine_detokenize", EngineDetokenize),
            tokenize_result_delete: symbol!("litert_lm_tokenize_result_delete", PtrDelete),
            tokenize_result_get_tokens: symbol!(
                "litert_lm_tokenize_result_get_tokens",
                TokenizeResultGetTokens
            ),
            tokenize_result_get_num_tokens: symbol!(
                "litert_lm_tokenize_result_get_num_tokens",
                TokenizeResultGetNumTokens
            ),
            detokenize_result_delete: symbol!("litert_lm_detokenize_result_delete", PtrDelete),
            detokenize_result_get_string: symbol!(
                "litert_lm_detokenize_result_get_string",
                DetokenizeResultGetString
            ),
        })
    }
}

fn cstring(value: &str, field: &str) -> Result<CString, SamplingError> {
    CString::new(value)
        .map_err(|_| local_error("litert_lm_request", format!("{field} contains a NUL byte")))
}

fn path_to_cstring(path: &std::path::Path) -> Result<CString, String> {
    CString::new(path.to_string_lossy().as_bytes())
        .map_err(|_| format!("path contains a NUL byte: {}", path.display()))
}

fn u32_to_c_int(value: u32, field: &str) -> Result<c_int, String> {
    c_int::try_from(value).map_err(|_| format!("`{field}` exceeds C API integer range"))
}

fn local_error(error_type: impl Into<String>, message: impl Into<String>) -> SamplingError {
    SamplingError::StreamError {
        error_type: error_type.into(),
        message: message.into(),
    }
}

fn bridge_error_detail(status: c_int, error_message: *const c_char) -> String {
    if error_message.is_null() {
        format!("bridge returned status {status}")
    } else {
        // SAFETY: bridge errors are thread-local NUL-terminated strings.
        unsafe { CStr::from_ptr(error_message) }
            .to_string_lossy()
            .into_owned()
    }
}

fn native_last_error(api: &Api) -> Option<String> {
    // SAFETY: the bridge returns either null or a thread-local,
    // NUL-terminated string that remains valid until the next bridge call on
    // this thread. Copy it immediately.
    let error = unsafe { (api.last_error_message)() };
    (!error.is_null()).then(|| {
        // SAFETY: null was rejected and the bridge contract guarantees a
        // NUL-terminated string for the lifetime described above.
        unsafe { CStr::from_ptr(error) }
            .to_string_lossy()
            .into_owned()
    })
}
