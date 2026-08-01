use std::path::PathBuf;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use xai_grok_runtime::{
    AdapterBinding, AdapterDescriptor, AdapterDtype, ContextOverflowStrategy, LiteRtLmConfig,
    LocalInferenceResult, MemoryLimits, PreparedConversation, RuntimeManager, RuntimeManagerConfig,
    RuntimeMode, RuntimePriority, RuntimeRequest, RuntimeStage, hash_artifact,
};
use xai_grok_sampling_types::ConversationItem;

fn compile_stub() -> (tempfile::TempDir, PathBuf) {
    let temp = tempfile::tempdir().expect("temporary directory");
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../xai-grok-sampler/tests/fixtures/litert_lm_stub.c");
    #[cfg(target_os = "macos")]
    let library = temp.path().join("liblitert_lm_stub.dylib");
    #[cfg(not(target_os = "macos"))]
    let library = temp.path().join("liblitert_lm_stub.so");
    let mut compiler = std::process::Command::new("cc");
    #[cfg(target_os = "macos")]
    compiler.arg("-dynamiclib");
    #[cfg(not(target_os = "macos"))]
    compiler.args(["-shared", "-fPIC"]);
    let status = compiler
        .arg(source)
        .arg("-o")
        .arg(&library)
        .status()
        .expect("C compiler");
    assert!(status.success(), "failed to compile LiteRT-LM test bridge");
    (temp, library)
}

fn prepared() -> PreparedConversation {
    PreparedConversation {
        session_id: Some("ipc-session".to_owned()),
        system_message: Some("You are concise.".to_owned()),
        retrieved_memory: None,
        messages: "[]".to_owned(),
        tools: "[]".to_owned(),
        json_schema: None,
        current_message: r#"{"role":"user","content":[{"type":"text","text":"hello"}]}"#.to_owned(),
        max_output_tokens: Some(64),
        temperature: Some(0.0),
        top_p: Some(1.0),
        lora_adapter: None,
    }
}

fn runtime_fixture() -> (tempfile::TempDir, RuntimeManager, LiteRtLmConfig) {
    let (temp, library_path) = compile_stub();
    let config = LiteRtLmConfig {
        model_path: library_path.with_file_name("model.litertlm"),
        library_path,
        backend: "cpu".to_owned(),
        max_context_tokens: Some(4_096),
        supported_lora_ranks: Vec::new(),
        adapter_descriptor: None,
        lora_adapter: None,
        max_resident_adapters: 8,
        max_resident_sessions: 8,
        max_resident_context_tokens: 32_768,
        context_strategy: ContextOverflowStrategy::Strict,
        min_recent_turns: 4,
    };
    let manager = RuntimeManager::new(RuntimeManagerConfig {
        mode: RuntimeMode::Worker,
        worker_path: Some(PathBuf::from(env!(
            "CARGO_BIN_EXE_grok-local-runtime-worker"
        ))),
        queue_capacity: 8,
        max_active_requests: 2,
        memory_limits: MemoryLimits {
            physical_bytes: 1024 * 1024 * 1024,
            soft_bytes: 768 * 1024 * 1024,
            hard_bytes: 896 * 1024 * 1024,
        },
    });
    (temp, manager, config)
}

async fn generate(
    manager: &RuntimeManager,
    config: LiteRtLmConfig,
    request_id: &str,
    current_message: Option<&str>,
) -> Result<LocalInferenceResult, xai_grok_sampling_types::SamplingError> {
    let (event_tx, _event_rx) = mpsc::unbounded_channel();
    let mut conversation = prepared();
    if let Some(current_message) = current_message {
        conversation.current_message = current_message.to_string();
    }
    manager
        .generate(
            RuntimeRequest {
                request_id: request_id.to_owned(),
                session_id: format!("session-{request_id}"),
                model_id: "stub-model".to_owned(),
                model_config: config.clone(),
                stage: RuntimeStage::Direct,
                adapter: None,
                conversation,
                completion_reserve: 64,
                priority: RuntimePriority::Interactive,
                deadline: Instant::now() + Duration::from_secs(5),
                admission_hook: None,
            },
            event_tx,
            CancellationToken::new(),
        )
        .await
}

async fn generate_with_adapter(
    manager: &RuntimeManager,
    config: LiteRtLmConfig,
    request_id: &str,
    adapter: AdapterBinding,
) -> Result<LocalInferenceResult, xai_grok_sampling_types::SamplingError> {
    let (event_tx, _event_rx) = mpsc::unbounded_channel();
    manager
        .generate(
            RuntimeRequest {
                request_id: request_id.to_owned(),
                session_id: format!("session-{request_id}"),
                model_id: "stub-model".to_owned(),
                model_config: config,
                stage: RuntimeStage::Direct,
                adapter: Some(adapter),
                conversation: prepared(),
                completion_reserve: 64,
                priority: RuntimePriority::Interactive,
                deadline: Instant::now() + Duration::from_secs(5),
                admission_hook: None,
            },
            event_tx,
            CancellationToken::new(),
        )
        .await
}

#[tokio::test]
async fn persistent_worker_measures_streams_and_releases_session() {
    let (_temp, library_path) = compile_stub();
    let config = LiteRtLmConfig {
        model_path: library_path.with_file_name("model.litertlm"),
        library_path,
        backend: "cpu".to_owned(),
        max_context_tokens: Some(4_096),
        supported_lora_ranks: Vec::new(),
        adapter_descriptor: None,
        lora_adapter: None,
        max_resident_adapters: 8,
        max_resident_sessions: 8,
        max_resident_context_tokens: 32_768,
        context_strategy: ContextOverflowStrategy::Strict,
        min_recent_turns: 4,
    };
    let manager = RuntimeManager::new(RuntimeManagerConfig {
        mode: RuntimeMode::Worker,
        worker_path: Some(PathBuf::from(env!(
            "CARGO_BIN_EXE_grok-local-runtime-worker"
        ))),
        queue_capacity: 8,
        max_active_requests: 2,
        memory_limits: MemoryLimits {
            physical_bytes: 16 * 1024 * 1024,
            soft_bytes: 12 * 1024 * 1024,
            hard_bytes: 14 * 1024 * 1024,
        },
    });

    let conversation = prepared();
    let measured = manager
        .measure_exact(
            "stub-model".to_owned(),
            config.clone(),
            &conversation,
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .expect("worker measurement");
    assert_eq!(measured as usize, conversation.current_message.len());

    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let result = manager
        .generate(
            RuntimeRequest {
                request_id: "ipc-request".to_owned(),
                session_id: "ipc-session".to_owned(),
                model_id: "stub-model".to_owned(),
                model_config: config,
                stage: RuntimeStage::Direct,
                adapter: None,
                conversation,
                completion_reserve: 64,
                priority: RuntimePriority::Interactive,
                deadline: Instant::now() + Duration::from_secs(5),
                admission_hook: None,
            },
            event_tx,
            CancellationToken::new(),
        )
        .await
        .expect("worker generation");
    assert!(matches!(result, LocalInferenceResult::Completed { .. }));
    assert!(event_rx.recv().await.is_some(), "worker must stream events");
    manager
        .release_session("ipc-session")
        .await
        .expect("session release");
}

#[tokio::test]
#[ignore = "requires the real Grok LiteRT-LM bridge and a compatible model artifact"]
async fn live_real_worker_measures_generates_and_streams_without_http() {
    let library_path =
        PathBuf::from(std::env::var("LITERT_LM_LIBRARY").expect("LITERT_LM_LIBRARY"));
    let model_path =
        PathBuf::from(std::env::var("LITERT_LM_TEST_MODEL").expect("LITERT_LM_TEST_MODEL"));
    let backend = std::env::var("LITERT_LM_TEST_BACKEND").unwrap_or_else(|_| "gpu".to_owned());
    let config = LiteRtLmConfig {
        model_path,
        library_path,
        backend,
        max_context_tokens: Some(4_096),
        // This checked-in Qwen artifact uses the generic WebGPU executor and
        // is not LoRA-capable. A LoRA acceptance fixture must explicitly
        // declare ranks and is rejected when the native backend cannot honor
        // them.
        supported_lora_ranks: Vec::new(),
        adapter_descriptor: None,
        lora_adapter: None,
        max_resident_adapters: 8,
        max_resident_sessions: 8,
        max_resident_context_tokens: 32_768,
        context_strategy: ContextOverflowStrategy::Strict,
        min_recent_turns: 4,
    };
    let manager = RuntimeManager::new(RuntimeManagerConfig {
        mode: RuntimeMode::Worker,
        worker_path: Some(PathBuf::from(env!(
            "CARGO_BIN_EXE_grok-local-runtime-worker"
        ))),
        queue_capacity: 8,
        max_active_requests: 1,
        memory_limits: MemoryLimits::detect(),
    });
    let max_output_tokens = std::env::var("LITERT_LM_TEST_MAX_OUTPUT")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(256);
    let mut conversation = prepared();
    conversation.max_output_tokens = Some(max_output_tokens);
    conversation.current_message =
        r#"{"role":"user","content":[{"type":"text","text":"Reply with exactly: LOCAL_WORKER_OK"}]}"#
            .to_owned();
    let deadline = Instant::now() + Duration::from_secs(180);
    let measured = manager
        .measure_exact(
            "live-local-model".to_owned(),
            config.clone(),
            &conversation,
            deadline,
        )
        .await
        .expect("real worker exact prompt measurement");
    assert!(measured > 0);

    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let result = manager
        .generate(
            RuntimeRequest {
                request_id: "live-real-worker-request".to_owned(),
                session_id: "live-real-worker-session".to_owned(),
                model_id: "live-local-model".to_owned(),
                model_config: config.clone(),
                stage: RuntimeStage::Direct,
                adapter: None,
                conversation,
                completion_reserve: max_output_tokens,
                priority: RuntimePriority::Interactive,
                deadline,
                admission_hook: None,
            },
            event_tx,
            CancellationToken::new(),
        )
        .await
        .expect("real worker generation");
    let LocalInferenceResult::Completed { response, .. } = result else {
        panic!("real worker generation unexpectedly cancelled");
    };
    let Some(assistant) = response.items.iter().find_map(|item| match item {
        ConversationItem::Assistant(assistant) => Some(assistant),
        _ => None,
    }) else {
        panic!("real worker did not return an assistant item");
    };
    assert!(!assistant.content.trim().is_empty());
    assert!(
        std::iter::from_fn(|| event_rx.try_recv().ok())
            .next()
            .is_some(),
        "real worker must stream at least one event"
    );
    manager
        .release_session("live-real-worker-session")
        .await
        .expect("real worker session release");

    manager.shutdown().await;
}

#[cfg(target_os = "macos")]
#[tokio::test]
#[ignore = "requires a GPU LoRA-capable base artifact and two compatible adapters"]
async fn live_gpu_worker_switches_two_adapters_without_kv_cross_contamination() {
    let library_path =
        PathBuf::from(std::env::var("LITERT_LM_LIBRARY").expect("LITERT_LM_LIBRARY"));
    let model_path = PathBuf::from(
        std::env::var("LITERT_LM_LORA_TEST_MODEL").expect("LITERT_LM_LORA_TEST_MODEL"),
    );
    let first_path = PathBuf::from(
        std::env::var("LITERT_LM_LORA_TEST_ADAPTER_ONE").expect("LITERT_LM_LORA_TEST_ADAPTER_ONE"),
    );
    let second_path = PathBuf::from(
        std::env::var("LITERT_LM_LORA_TEST_ADAPTER_TWO").expect("LITERT_LM_LORA_TEST_ADAPTER_TWO"),
    );
    let rank = std::env::var("LITERT_LM_LORA_TEST_RANK")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(32);
    let base_model_hash = hash_artifact(&model_path).expect("base model hash");
    let descriptor = |adapter_id: &str, path: PathBuf| AdapterDescriptor {
        adapter_id: adapter_id.to_owned(),
        revision: "fixture-v1".to_owned(),
        content_hash: hash_artifact(&path).expect("adapter hash"),
        base_model_hash: base_model_hash.clone(),
        byte_size: std::fs::metadata(&path).expect("adapter metadata").len(),
        path,
        rank,
        dtype: AdapterDtype::F16,
        tensor_names: vec![
            "query_w_prime_left_0".to_owned(),
            "query_w_prime_right_0".to_owned(),
        ],
    };
    let first = descriptor("fixture-ones", first_path);
    let second = descriptor("fixture-twos", second_path);
    let config = LiteRtLmConfig {
        model_path,
        library_path,
        backend: "gpu".to_owned(),
        max_context_tokens: Some(256),
        supported_lora_ranks: vec![rank],
        adapter_descriptor: None,
        lora_adapter: None,
        max_resident_adapters: 4,
        max_resident_sessions: 4,
        max_resident_context_tokens: 1_024,
        context_strategy: ContextOverflowStrategy::Strict,
        min_recent_turns: 1,
    };
    let manager = RuntimeManager::new(RuntimeManagerConfig {
        mode: RuntimeMode::Worker,
        worker_path: Some(PathBuf::from(env!(
            "CARGO_BIN_EXE_grok-local-runtime-worker"
        ))),
        queue_capacity: 8,
        max_active_requests: 2,
        memory_limits: MemoryLimits::detect(),
    });
    manager
        .prewarm_adapter("stub-model".to_owned(), config.clone(), first.clone())
        .await
        .expect("first real adapter prewarm and probe");
    manager
        .prewarm_adapter("stub-model".to_owned(), config.clone(), second.clone())
        .await
        .expect("second real adapter prewarm and probe");

    let first_result = generate_with_adapter(
        &manager,
        config.clone(),
        "real-adapter-one",
        AdapterBinding {
            adapter_id: first.adapter_id.clone(),
            revision: first.revision.clone(),
        },
    )
    .await
    .expect("first adapter generation");
    let second_result = generate_with_adapter(
        &manager,
        config,
        "real-adapter-two",
        AdapterBinding {
            adapter_id: second.adapter_id.clone(),
            revision: second.revision.clone(),
        },
    )
    .await
    .expect("second adapter generation");
    let output = |result: LocalInferenceResult| match result {
        LocalInferenceResult::Completed { response, .. } => response
            .items
            .into_iter()
            .find_map(|item| match item {
                ConversationItem::Assistant(item) => Some(item.content),
                _ => None,
            })
            .expect("adapter generation assistant output"),
        LocalInferenceResult::Cancelled => panic!("adapter generation cancelled"),
    };
    let first_output = output(first_result);
    let second_output = output(second_result);
    assert_ne!(
        first_output, second_output,
        "two compatible adapters must produce observably different output"
    );
    let capacity = manager.worker_capacity().await.expect("worker capacity");
    assert_eq!(
        capacity
            .values()
            .next()
            .expect("resident worker")
            .resident_adapters,
        2
    );
    manager.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn native_worker_crash_is_contained_and_next_request_restarts_it() {
    let (_temp, manager, config) = runtime_fixture();
    let error = match generate(
        &manager,
        config.clone(),
        "crash-request",
        Some(r#"{"role":"user","content":[{"type":"text","text":"__GROK_TEST_NATIVE_CRASH__"}]}"#),
    )
    .await
    {
        Err(error) => error,
        Ok(_) => panic!("worker crash must fail only its request"),
    };
    assert!(error.to_string().contains("worker"));

    let recovered = generate(&manager, config, "recovered-request", None)
        .await
        .expect("manager should replace the dead worker");
    assert!(matches!(recovered, LocalInferenceResult::Completed { .. }));
    manager.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn native_hang_is_killed_after_request_id_cancellation_and_runtime_recovers() {
    let (_temp, manager, config) = runtime_fixture();
    let running_manager = manager.clone();
    let running_config = config.clone();
    let task = tokio::spawn(async move {
        generate(
            &running_manager,
            running_config,
            "hang-request",
            Some(
                r#"{"role":"user","content":[{"type":"text","text":"__GROK_TEST_NATIVE_HANG__"}]}"#,
            ),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if manager.cancel("hang-request") {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("request should become cancellable");
    let result = tokio::time::timeout(Duration::from_millis(2_500), task)
        .await
        .expect("hung worker must be terminated within the cancellation bound")
        .expect("generation task join");
    assert!(result.is_err());

    let recovered = generate(&manager, config, "after-hang", None)
        .await
        .expect("manager should replace the terminated worker");
    assert!(matches!(recovered, LocalInferenceResult::Completed { .. }));
    manager.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn resident_adapters_switch_under_bound_and_concurrent_requests_keep_separate_bindings() {
    let (temp, manager, mut config) = runtime_fixture();
    std::fs::write(&config.model_path, b"model").unwrap();
    config.backend = "gpu".to_string();
    config.supported_lora_ranks = vec![8];
    let base_model_hash = hash_artifact(&config.model_path).unwrap();
    let descriptor = |adapter_id: &str, revision: &str, payload: &[u8]| {
        let path = temp.path().join(format!("{adapter_id}-{revision}.bin"));
        std::fs::write(&path, payload).unwrap();
        AdapterDescriptor {
            adapter_id: adapter_id.to_string(),
            revision: revision.to_string(),
            content_hash: hash_artifact(&path).unwrap(),
            base_model_hash: base_model_hash.clone(),
            path,
            rank: 8,
            dtype: AdapterDtype::F16,
            byte_size: payload.len() as u64,
            tensor_names: vec!["block.lora_A".to_string(), "block.lora_B".to_string()],
        }
    };
    let first = descriptor("first", "r1", b"first-adapter");
    let second = descriptor("second", "r1", b"second-adapter");
    let (first_load, second_load) = tokio::join!(
        manager.prewarm_adapter("stub-model".to_string(), config.clone(), first.clone()),
        manager.prewarm_adapter("stub-model".to_string(), config.clone(), second.clone()),
    );
    first_load.unwrap();
    second_load.unwrap();

    let mut samples = Vec::with_capacity(100);
    for _ in 0..100 {
        let started = Instant::now();
        manager
            .prewarm_adapter("stub-model".to_string(), config.clone(), first.clone())
            .await
            .unwrap();
        samples.push(started.elapsed());
    }
    samples.sort_unstable();
    let p95 = samples[samples.len() * 95 / 100];
    eprintln!("resident adapter selection p95: {p95:?}");
    assert!(
        p95 <= Duration::from_millis(25),
        "resident adapter selection p95 was {p95:?}"
    );

    let first_binding = AdapterBinding {
        adapter_id: first.adapter_id,
        revision: first.revision,
    };
    let second_binding = AdapterBinding {
        adapter_id: second.adapter_id,
        revision: second.revision,
    };
    let (first_result, second_result) = tokio::join!(
        generate_with_adapter(&manager, config.clone(), "adapter-first", first_binding),
        generate_with_adapter(&manager, config, "adapter-second", second_binding),
    );
    assert!(matches!(
        first_result.unwrap(),
        LocalInferenceResult::Completed { .. }
    ));
    assert!(matches!(
        second_result.unwrap(),
        LocalInferenceResult::Completed { .. }
    ));
    let capacity = manager.worker_capacity().await.unwrap();
    let primary = capacity.values().next().unwrap();
    assert_eq!(primary.resident_adapters, 2);
    manager.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "explicit IPC latency gate for the release target host"]
async fn worker_control_dispatch_stays_below_two_milliseconds_p95() {
    let (_temp, manager, config) = runtime_fixture();
    manager
        .prewarm_model("stub-model".to_string(), config)
        .await
        .unwrap();
    for _ in 0..20 {
        manager.worker_capacity().await.unwrap();
    }
    let mut samples = Vec::with_capacity(500);
    for _ in 0..500 {
        let started = Instant::now();
        manager.worker_capacity().await.unwrap();
        samples.push(started.elapsed());
    }
    samples.sort_unstable();
    let p95 = samples[samples.len() * 95 / 100];
    eprintln!("runtime IPC dispatch p95: {p95:?}");
    assert!(p95 < Duration::from_millis(2), "IPC p95 was {p95:?}");
    manager.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "explicit cached context-admission gate for the release target host"]
async fn cached_context_admission_stays_below_ten_milliseconds_p95() {
    let (_temp, manager, config) = runtime_fixture();
    let conversation = prepared();
    for _ in 0..20 {
        manager
            .measure_exact(
                "stub-model".to_string(),
                config.clone(),
                &conversation,
                Instant::now() + Duration::from_secs(5),
            )
            .await
            .unwrap();
    }
    let mut samples = Vec::with_capacity(500);
    for _ in 0..500 {
        let started = Instant::now();
        manager
            .measure_exact(
                "stub-model".to_string(),
                config.clone(),
                &conversation,
                Instant::now() + Duration::from_secs(5),
            )
            .await
            .unwrap();
        samples.push(started.elapsed());
    }
    samples.sort_unstable();
    let p95 = samples[samples.len() * 95 / 100];
    eprintln!("cached context admission p95: {p95:?}");
    assert!(
        p95 < Duration::from_millis(10),
        "cached context admission p95 was {p95:?}"
    );
    manager.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "explicit direct-turn orchestration gate for the release target host"]
async fn direct_turn_orchestration_stays_below_five_milliseconds_p95() {
    let (_temp, manager, config) = runtime_fixture();
    for index in 0..20 {
        generate(&manager, config.clone(), &format!("warm-{index}"), None)
            .await
            .unwrap();
    }
    let mut samples = Vec::with_capacity(200);
    for index in 0..200 {
        let started = Instant::now();
        generate(
            &manager,
            config.clone(),
            &format!("direct-perf-{index}"),
            None,
        )
        .await
        .unwrap();
        samples.push(started.elapsed());
    }
    samples.sort_unstable();
    let p95 = samples[samples.len() * 95 / 100];
    eprintln!("direct-turn orchestration p95: {p95:?}");
    assert!(
        p95 < Duration::from_millis(5),
        "direct-turn orchestration p95 was {p95:?}"
    );
    manager.shutdown().await;
}
