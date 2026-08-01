//! Persistent production-path qualification for the local execution platform.
//!
//! This binary is deliberately separate from shipped operator binaries. It
//! holds the real managers and native workers open for the complete run, emits
//! JSONL evidence, and exits non-zero on the first violated invariant.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail, ensure};
use clap::{Parser, ValueEnum};
use serde::Serialize;
use tokio::io::AsyncWriteExt as _;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use xai_grok_config_types::{MemoryEmbeddingConfig, MemoryIndexConfig, MemorySearchConfig};
use xai_grok_memory::backend::{MemoryBackendImpl, embedding_backfill_status};
use xai_grok_memory::embedding::{EmbeddingProvider, LocalEmbeddingProvider};
use xai_grok_memory::watcher::MemoryFileWatcher;
use xai_grok_memory::{MemoryIndex, MemoryScope, MemoryStorage, init_sqlite_vec};
use xai_grok_native_execution::{
    CommandRequest, CommandStdin, JobLifecycle, NativeExecutionLimits, NativeExecutionSupervisor,
    OutputStream,
};
use xai_grok_runtime::{
    AdapterBinding, AdapterDescriptor, AdapterDtype, AdapterState, ContextOverflowStrategy,
    LiteRtLmConfig, LocalInferenceResult, MemoryLimits, PreparedConversation, ResourceClass,
    ResourceGovernor, RuntimeEvent, RuntimeManager, RuntimeManagerConfig, RuntimeMode,
    RuntimePriority, RuntimeRequest, RuntimeStage, hash_artifact,
};
use xai_grok_test_support::ResourceSnapshot;
use xai_grok_tools::types::memory_backend::MemoryBackend;

const MIB: u64 = 1024 * 1024;
const OPERATION_TIMEOUT: Duration = Duration::from_secs(180);

#[derive(Clone, Copy, Debug, ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
enum SoakProfile {
    Smoke,
    Ci,
    Release,
}

impl SoakProfile {
    fn duration(self, override_seconds: Option<u64>) -> Result<Duration> {
        match (self, override_seconds) {
            (Self::Smoke, Some(0)) => bail!("smoke duration must be greater than zero"),
            (Self::Smoke, Some(seconds)) => Ok(Duration::from_secs(seconds)),
            (Self::Smoke, None) => Ok(Duration::from_secs(5 * 60)),
            (Self::Ci, Some(_)) => {
                bail!("the 24-hour CI profile does not permit a duration override")
            }
            (Self::Release, Some(_)) => {
                bail!("the 72-hour release profile does not permit a duration override")
            }
            (Self::Ci, None) => Ok(Duration::from_secs(24 * 60 * 60)),
            (Self::Release, None) => Ok(Duration::from_secs(72 * 60 * 60)),
        }
    }
}

#[derive(Debug, Parser)]
#[command(about = "Run persistent, fail-closed local-platform qualification")]
struct Args {
    #[arg(long, value_enum, default_value = "smoke")]
    profile: SoakProfile,
    /// Only the smoke profile may override its five-minute duration.
    #[arg(long)]
    duration_seconds: Option<u64>,
    #[arg(long, default_value_t = 5_000)]
    interval_ms: u64,
    #[arg(long, default_value_t = 10)]
    cancellation_every: u64,
    #[arg(long, default_value_t = 100)]
    busy_embedding_every: u64,
    #[arg(long, default_value_t = 2_048)]
    max_rss_growth_mib: u64,
    #[arg(long, default_value_t = 16)]
    max_thread_growth: usize,
    #[arg(long, default_value_t = 32)]
    max_fd_growth: usize,
    #[arg(long, default_value_t = 50)]
    max_memory_query_ms: u64,
    #[arg(long, default_value_t = 25)]
    max_resident_adapter_selection_ms: u64,
    #[arg(long, default_value = ".grok/release-soak")]
    state_dir: PathBuf,
    #[arg(long, env = "GROK_LOCAL_RUNTIME_WORKER")]
    runtime_worker: Option<PathBuf>,
    #[arg(long, env = "LITERT_LM_LIBRARY")]
    library: PathBuf,
    /// General generation artifact. Defaults to the LoRA-capable base.
    #[arg(long, env = "LITERT_LM_TEST_MODEL")]
    model: Option<PathBuf>,
    #[arg(long, env = "LITERT_LM_TEST_BACKEND", default_value = "gpu")]
    backend: String,
    #[arg(long, default_value_t = 4_096)]
    context_window: u32,
    #[arg(long, env = "LITERT_LM_LORA_TEST_BACKEND", default_value = "cpu")]
    lora_backend: String,
    #[arg(long, default_value_t = 128)]
    lora_context_window: u32,
    #[arg(long, env = "LITERT_LM_LORA_TEST_MODEL")]
    lora_model: PathBuf,
    #[arg(long, env = "LITERT_LM_LORA_TEST_ADAPTER_ONE")]
    adapter_one: PathBuf,
    #[arg(long, env = "LITERT_LM_LORA_TEST_ADAPTER_TWO")]
    adapter_two: PathBuf,
    #[arg(long, env = "LITERT_LM_LORA_TEST_RANK")]
    adapter_rank: u32,
    #[arg(
        long,
        env = "LITERT_LM_LORA_TEST_TENSORS",
        value_delimiter = ',',
        required = true
    )]
    adapter_tensor_name: Vec<String>,
    #[arg(long, default_value = "embeddinggemma-300m-q4")]
    embedding_model: String,
    #[arg(long, default_value_t = 768)]
    embedding_dimensions: usize,
    #[arg(long, default_value_t = 2_048)]
    cancellation_tokens: u32,
}

#[derive(Clone, Copy, Debug, Serialize)]
struct AggregateResources {
    harness_rss_bytes: u64,
    worker_rss_bytes: u64,
    total_rss_bytes: u64,
    harness_threads: u64,
    worker_threads: u64,
    total_threads: u64,
    harness_fds: u64,
    worker_fds: u64,
    total_fds: u64,
}

impl AggregateResources {
    fn growth_from(self, baseline: Self) -> Self {
        Self {
            harness_rss_bytes: self
                .harness_rss_bytes
                .saturating_sub(baseline.harness_rss_bytes),
            worker_rss_bytes: self
                .worker_rss_bytes
                .saturating_sub(baseline.worker_rss_bytes),
            total_rss_bytes: self
                .total_rss_bytes
                .saturating_sub(baseline.total_rss_bytes),
            harness_threads: self
                .harness_threads
                .saturating_sub(baseline.harness_threads),
            worker_threads: self.worker_threads.saturating_sub(baseline.worker_threads),
            total_threads: self.total_threads.saturating_sub(baseline.total_threads),
            harness_fds: self.harness_fds.saturating_sub(baseline.harness_fds),
            worker_fds: self.worker_fds.saturating_sub(baseline.worker_fds),
            total_fds: self.total_fds.saturating_sub(baseline.total_fds),
        }
    }
}

#[derive(Debug, Default, Serialize)]
struct Counters {
    cycles: u64,
    direct_generations: u64,
    adapter_generations: u64,
    model_cancellations: u64,
    native_jobs: u64,
    native_cancellations: u64,
    memory_queries: u64,
    busy_memory_queries: u64,
}

#[derive(Debug, Serialize)]
struct Report<'a, T: Serialize> {
    timestamp_unix_ms: u128,
    event: &'a str,
    data: T,
}

struct Reporter {
    path: PathBuf,
    file: tokio::fs::File,
}

impl Reporter {
    async fn create(state_dir: &Path) -> Result<Self> {
        let report_dir = state_dir.join("reports");
        tokio::fs::create_dir_all(&report_dir).await?;
        let path = report_dir.join(format!("soak-{}.jsonl", now_ms()));
        let file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .await
            .with_context(|| format!("create report {}", path.display()))?;
        Ok(Self { path, file })
    }

    async fn emit<T: Serialize>(&mut self, event: &str, data: T) -> Result<()> {
        let line = serde_json::to_string(&Report {
            timestamp_unix_ms: now_ms(),
            event,
            data,
        })?;
        println!("{line}");
        self.file.write_all(line.as_bytes()).await?;
        self.file.write_all(b"\n").await?;
        self.file.flush().await?;
        Ok(())
    }
}

struct MemoryFixture {
    storage: MemoryStorage,
    backend: MemoryBackendImpl,
    provider: LocalEmbeddingProvider,
    watcher: Arc<MemoryFileWatcher>,
    database: PathBuf,
    embedding_config: MemoryEmbeddingConfig,
}

#[derive(Serialize)]
struct StartRecord<'a> {
    profile: SoakProfile,
    duration_seconds: u64,
    report: &'a Path,
    state_dir: &'a Path,
    baseline: AggregateResources,
    adapter_selection_p95_micros: u128,
    context_window: u32,
    lora_context_window: u32,
    cancellation_tokens: u32,
    governor_reserved_bytes: u64,
    embedding_reserved_bytes: u64,
}

#[derive(Serialize)]
struct CycleRecord<'a> {
    cycle: u64,
    elapsed_seconds: u64,
    resources: AggregateResources,
    growth: AggregateResources,
    peak_growth: AggregateResources,
    runtime_queue_depth: usize,
    runtime_active_requests: u32,
    resident_workers: u32,
    resident_adapters: usize,
    native_jobs: usize,
    native_spool_bytes: u64,
    governor_reserved_bytes: u64,
    embedding_reserved_bytes: u64,
    soft_memory_pressure: bool,
    embedding_backfill: &'a xai_grok_memory::backend::EmbeddingBackfillStatus,
    counters: &'a Counters,
}

#[derive(Serialize)]
struct FinalRecord<'a> {
    profile: SoakProfile,
    requested_duration_seconds: u64,
    elapsed_seconds: u64,
    peak_growth: AggregateResources,
    counters: &'a Counters,
    embedding_backfill: xai_grok_memory::backend::EmbeddingBackfillStatus,
    passed: bool,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("xai-grok-release-soak: {error:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let args = Args::parse();
    ensure!(
        args.interval_ms > 0,
        "interval-ms must be greater than zero"
    );
    ensure!(
        args.cancellation_every > 0 && args.busy_embedding_every > 0,
        "exercise intervals must be greater than zero"
    );
    ensure!(
        args.adapter_rank > 0,
        "adapter rank must be greater than zero"
    );
    ensure!(
        args.context_window > 0,
        "context-window must be greater than zero"
    );
    ensure!(
        args.lora_context_window > 0,
        "lora-context-window must be greater than zero"
    );
    ensure!(
        args.cancellation_tokens > 0 && args.cancellation_tokens < args.context_window,
        "cancellation-tokens must be greater than zero and smaller than context-window"
    );
    let duration = args.profile.duration(args.duration_seconds)?;

    tokio::fs::create_dir_all(&args.state_dir).await?;
    let state_dir = dunce::canonicalize(&args.state_dir)
        .with_context(|| format!("canonicalize state directory {}", args.state_dir.display()))?;
    let library = canonical_file(&args.library, "LiteRT-LM bridge")?;
    let lora_model = canonical_file(&args.lora_model, "LoRA-capable model")?;
    let general_model = canonical_file(
        args.model.as_deref().unwrap_or(&lora_model),
        "general generation model",
    )?;
    let adapter_one_path = canonical_file(&args.adapter_one, "first adapter")?;
    let adapter_two_path = canonical_file(&args.adapter_two, "second adapter")?;
    ensure!(
        adapter_one_path != adapter_two_path,
        "the two adapter paths must be distinct"
    );
    let runtime_worker = resolve_worker(args.runtime_worker.as_deref())?;

    let memory_limits = MemoryLimits::detect();
    let manager = RuntimeManager::with_resource_governor(
        RuntimeManagerConfig {
            mode: RuntimeMode::Worker,
            worker_path: Some(runtime_worker),
            queue_capacity: 64,
            max_active_requests: 2,
            max_worker_processes: 4,
            memory_limits,
        },
        ResourceGovernor::global().clone(),
    );
    let native = NativeExecutionSupervisor::open_with_limits(
        state_dir.join("native"),
        NativeExecutionLimits {
            maximum_parallel: 32,
            maximum_jobs: 256,
            maximum_spool_bytes_per_job: 8 * MIB,
            maximum_spool_bytes_per_owner: 64 * MIB,
            maximum_total_spool_bytes: 128 * MIB,
        },
    )
    .await?;
    cleanup_recovered_jobs(&native).await?;

    let general_config = model_config(
        general_model,
        library.clone(),
        args.backend.clone(),
        args.context_window,
        vec![],
    );
    let lora_config = model_config(
        lora_model.clone(),
        library,
        args.lora_backend.clone(),
        args.lora_context_window,
        vec![args.adapter_rank],
    );
    let base_hash = hash_artifact(&lora_model).context("hash LoRA base model")?;
    let first = adapter_descriptor(
        "release-soak-one",
        adapter_one_path,
        &base_hash,
        args.adapter_rank,
        &args.adapter_tensor_name,
    )?;
    let second = adapter_descriptor(
        "release-soak-two",
        adapter_two_path,
        &base_hash,
        args.adapter_rank,
        &args.adapter_tensor_name,
    )?;

    manager
        .prewarm_model("soak-general".to_owned(), general_config.clone())
        .await
        .context("prewarm real general model")?;
    manager
        .prewarm_adapter("soak-lora".to_owned(), lora_config.clone(), first.clone())
        .await
        .context("prewarm and probe first real adapter")?;
    manager
        .prewarm_adapter("soak-lora".to_owned(), lora_config.clone(), second.clone())
        .await
        .context("prewarm and probe second real adapter")?;

    let selection_p95 = qualify_resident_selection(
        &manager,
        &lora_config,
        &first,
        Duration::from_millis(args.max_resident_adapter_selection_ms),
    )
    .await?;
    qualify_adapter_isolation(&manager, &lora_config, &first, &second).await?;

    let memory = initialize_memory(&args, &state_dir).await?;
    qualify_busy_embedding_fallback(&memory, args.max_memory_query_ms).await?;
    let direct = generate(
        &manager,
        "soak-general",
        &general_config,
        "warmup-direct",
        "warmup-direct-session",
        None,
        "Reply with a short readiness statement.",
        64,
    )
    .await?;
    ensure!(
        !direct.trim().is_empty(),
        "warmup generation returned empty output"
    );
    manager.release_session("warmup-direct-session").await?;
    wait_runtime_stable(&manager).await?;

    let baseline = aggregate_resources(&manager).await?;
    ensure!(
        baseline.total_rss_bytes > 0,
        "aggregate RSS is not observable"
    );
    ensure!(
        baseline.total_threads > 0,
        "aggregate thread count is not observable"
    );
    ensure!(
        baseline.total_fds > 0,
        "aggregate file descriptor count is not observable"
    );
    let governor = manager.resource_governor().snapshot();
    let embedding_reserved_bytes = governor
        .by_class
        .get(&ResourceClass::Embedding)
        .copied()
        .unwrap_or_default();
    ensure!(
        embedding_reserved_bytes > 0,
        "local embedding residency is missing from the process-wide memory governor"
    );
    let mut reporter = Reporter::create(&state_dir).await?;
    let report_path = reporter.path.clone();
    reporter
        .emit(
            "started",
            StartRecord {
                profile: args.profile,
                duration_seconds: duration.as_secs(),
                report: &report_path,
                state_dir: &state_dir,
                baseline,
                adapter_selection_p95_micros: selection_p95.as_micros(),
                context_window: args.context_window,
                lora_context_window: args.lora_context_window,
                cancellation_tokens: args.cancellation_tokens,
                governor_reserved_bytes: governor.reserved_bytes,
                embedding_reserved_bytes,
            },
        )
        .await?;

    let started = Instant::now();
    let deadline = started + duration;
    let mut counters = Counters::default();
    let mut peak_growth = baseline.growth_from(baseline);
    let mut adapter_toggle = false;
    while Instant::now() < deadline {
        counters.cycles += 1;
        let cycle = counters.cycles;

        let direct = generate(
            &manager,
            "soak-general",
            &general_config,
            &format!("direct-{cycle}"),
            &format!("direct-session-{cycle}"),
            None,
            &format!("Reply briefly with the cycle identifier {cycle}."),
            64,
        )
        .await?;
        ensure!(
            !direct.trim().is_empty(),
            "direct generation {cycle} was empty"
        );
        manager
            .release_session(&format!("direct-session-{cycle}"))
            .await?;
        counters.direct_generations += 1;

        let selected = if adapter_toggle { &first } else { &second };
        adapter_toggle = !adapter_toggle;
        let adapter_output = generate(
            &manager,
            "soak-lora",
            &lora_config,
            &format!("adapter-{cycle}"),
            &format!("adapter-session-{cycle}"),
            Some(binding(selected)),
            "Generate one short token sequence.",
            16,
        )
        .await?;
        ensure!(
            !adapter_output.trim().is_empty(),
            "adapter generation {cycle} was empty"
        );
        manager
            .release_session(&format!("adapter-session-{cycle}"))
            .await?;
        counters.adapter_generations += 1;

        if cycle % args.cancellation_every == 0 {
            cancel_generation(&manager, &general_config, cycle, args.cancellation_tokens).await?;
            counters.model_cancellations += 1;
            cancel_native_job(&native, cycle).await?;
            counters.native_cancellations += 1;
        }

        exercise_native_job(&native, cycle).await?;
        counters.native_jobs += 1;
        exercise_memory(&memory, cycle, args.max_memory_query_ms).await?;
        counters.memory_queries += 1;
        if cycle % args.busy_embedding_every == 0 {
            qualify_busy_embedding_fallback(&memory, args.max_memory_query_ms).await?;
            counters.busy_memory_queries += 1;
        }

        let resources = aggregate_resources(&manager).await?;
        let growth = resources.growth_from(baseline);
        peak_growth = component_max(peak_growth, growth);
        enforce_resource_bounds(&args, peak_growth)?;
        let runtime = manager.status().await?;
        ensure!(
            runtime.capacity.queue_depth == 0,
            "runtime queue did not drain"
        );
        ensure!(
            runtime.capacity.active_requests == 0,
            "runtime request remained active"
        );
        ensure!(
            runtime.capacity.resident_workers <= runtime.capacity.maximum_workers,
            "runtime worker count exceeds its configured maximum"
        );
        ensure!(
            runtime
                .adapters
                .iter()
                .filter(|adapter| adapter_is_resident(adapter))
                .count()
                >= 2,
            "both qualified adapters must remain resident"
        );
        let native_capacity = native.capacity().await;
        ensure!(
            native_capacity.jobs == 0,
            "native job registry did not drain"
        );
        ensure!(
            native_capacity.spool.used_bytes == 0,
            "native spool budget did not reclaim"
        );
        let backfill = embedding_backfill_status();
        let governor = manager.resource_governor().snapshot();
        reporter
            .emit(
                "cycle",
                CycleRecord {
                    cycle,
                    elapsed_seconds: started.elapsed().as_secs(),
                    resources,
                    growth,
                    peak_growth,
                    runtime_queue_depth: runtime.capacity.queue_depth,
                    runtime_active_requests: runtime.capacity.active_requests,
                    resident_workers: runtime.capacity.resident_workers,
                    resident_adapters: runtime
                        .adapters
                        .iter()
                        .filter(|adapter| adapter_is_resident(adapter))
                        .count(),
                    native_jobs: native_capacity.jobs,
                    native_spool_bytes: native_capacity.spool.used_bytes,
                    governor_reserved_bytes: governor.reserved_bytes,
                    embedding_reserved_bytes: governor
                        .by_class
                        .get(&ResourceClass::Embedding)
                        .copied()
                        .unwrap_or_default(),
                    soft_memory_pressure: governor.soft_pressure,
                    embedding_backfill: &backfill,
                    counters: &counters,
                },
            )
            .await?;

        let remaining = deadline.saturating_duration_since(Instant::now());
        if !remaining.is_zero() {
            tokio::time::sleep(remaining.min(Duration::from_millis(args.interval_ms))).await;
        }
    }

    wait_for_embedding_backfill(&memory.database, Duration::from_secs(60)).await?;
    let final_backfill = embedding_backfill_status();
    ensure!(
        final_backfill.active_tasks == 0
            && final_backfill.pending_chunks == 0
            && final_backfill.failed_batches == 0,
        "embedding backfill did not drain cleanly: {final_backfill:?}"
    );
    let final_runtime = manager.status().await?;
    ensure!(
        final_runtime.capacity.queue_depth == 0 && final_runtime.capacity.active_requests == 0,
        "runtime work remained queued at qualification shutdown"
    );
    let final_native = native.capacity().await;
    ensure!(
        final_native.jobs == 0 && final_native.spool.used_bytes == 0,
        "native jobs or spool bytes remained at qualification shutdown"
    );
    manager.shutdown().await;
    reporter
        .emit(
            "completed",
            FinalRecord {
                profile: args.profile,
                requested_duration_seconds: duration.as_secs(),
                elapsed_seconds: started.elapsed().as_secs(),
                peak_growth,
                counters: &counters,
                embedding_backfill: final_backfill,
                passed: true,
            },
        )
        .await?;
    Ok(())
}

fn model_config(
    model_path: PathBuf,
    library_path: PathBuf,
    backend: String,
    max_context_tokens: u32,
    supported_lora_ranks: Vec<u32>,
) -> LiteRtLmConfig {
    LiteRtLmConfig {
        model_path,
        library_path,
        backend,
        max_context_tokens: Some(max_context_tokens),
        supported_lora_ranks,
        adapter_descriptor: None,
        lora_adapter: None,
        max_resident_adapters: 8,
        max_resident_sessions: 32,
        max_resident_context_tokens: max_context_tokens.saturating_mul(32),
        context_strategy: ContextOverflowStrategy::Strict,
        min_recent_turns: 2,
    }
}

fn canonical_file(path: &Path, label: &str) -> Result<PathBuf> {
    let canonical = dunce::canonicalize(path)
        .with_context(|| format!("resolve {label} at {}", path.display()))?;
    ensure!(
        canonical.is_file(),
        "{label} is not a regular file: {}",
        canonical.display()
    );
    Ok(canonical)
}

fn resolve_worker(explicit: Option<&Path>) -> Result<PathBuf> {
    if let Some(path) = explicit {
        return canonical_file(path, "local runtime worker");
    }
    let sibling = std::env::current_exe()?
        .parent()
        .ok_or_else(|| anyhow!("qualification binary has no parent directory"))?
        .join("grok-local-runtime-worker");
    canonical_file(&sibling, "sibling local runtime worker")
}

fn adapter_descriptor(
    adapter_id: &str,
    path: PathBuf,
    base_model_hash: &str,
    rank: u32,
    tensor_names: &[String],
) -> Result<AdapterDescriptor> {
    let descriptor = AdapterDescriptor {
        adapter_id: adapter_id.to_owned(),
        revision: hash_artifact(&path)?.chars().take(16).collect(),
        content_hash: hash_artifact(&path)?,
        base_model_hash: base_model_hash.to_owned(),
        byte_size: std::fs::metadata(&path)?.len(),
        path,
        rank,
        dtype: AdapterDtype::F16,
        tensor_names: tensor_names.to_vec(),
    };
    descriptor.validate()?;
    Ok(descriptor)
}

fn binding(descriptor: &AdapterDescriptor) -> AdapterBinding {
    AdapterBinding {
        adapter_id: descriptor.adapter_id.clone(),
        revision: descriptor.revision.clone(),
    }
}

fn adapter_is_resident(adapter: &xai_grok_runtime::AdapterResidencySnapshot) -> bool {
    matches!(
        adapter.state,
        AdapterState::HostResident | AdapterState::DeviceResident | AdapterState::InUse { .. }
    )
}

fn prepared(session_id: &str, prompt: &str, max_output_tokens: u32) -> PreparedConversation {
    PreparedConversation {
        session_id: Some(session_id.to_owned()),
        system_message: Some("Return a concise, concrete answer.".to_owned()),
        retrieved_memory: None,
        messages: "[]".to_owned(),
        tools: "[]".to_owned(),
        json_schema: None,
        current_message: serde_json::json!({
            "role": "user",
            "content": [{"type": "text", "text": prompt}],
        })
        .to_string(),
        max_output_tokens: Some(max_output_tokens),
        temperature: Some(0.0),
        top_p: Some(1.0),
        lora_adapter: None,
    }
}

async fn generate(
    manager: &RuntimeManager,
    model_id: &str,
    config: &LiteRtLmConfig,
    request_id: &str,
    session_id: &str,
    adapter: Option<AdapterBinding>,
    prompt: &str,
    max_output_tokens: u32,
) -> Result<String> {
    let conversation = prepared(session_id, prompt, max_output_tokens);
    let deadline = Instant::now() + OPERATION_TIMEOUT;
    let measured = manager
        .measure_exact(model_id.to_owned(), config.clone(), &conversation, deadline)
        .await
        .with_context(|| format!("exact prompt measurement for {request_id}"))?;
    ensure!(
        measured > 0,
        "native prompt measurement returned zero tokens"
    );
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let result = manager
        .generate(
            RuntimeRequest {
                request_id: request_id.to_owned(),
                session_id: session_id.to_owned(),
                model_id: model_id.to_owned(),
                model_config: config.clone(),
                stage: RuntimeStage::Direct,
                adapter,
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
        .with_context(|| format!("native generation {request_id}"))?;
    let events = std::iter::from_fn(|| event_rx.try_recv().ok()).count();
    ensure!(
        events > 0,
        "native generation {request_id} emitted no stream events"
    );
    match result {
        LocalInferenceResult::Completed { response, .. } => {
            let output = response.assistant_text();
            ensure!(
                !output.trim().is_empty(),
                "native generation {request_id} returned no assistant text"
            );
            Ok(output)
        }
        LocalInferenceResult::Cancelled => bail!("native generation {request_id} was cancelled"),
    }
}

async fn qualify_resident_selection(
    manager: &RuntimeManager,
    config: &LiteRtLmConfig,
    descriptor: &AdapterDescriptor,
    maximum: Duration,
) -> Result<Duration> {
    let mut samples = Vec::with_capacity(100);
    for _ in 0..100 {
        let started = Instant::now();
        manager
            .prewarm_adapter("soak-lora".to_owned(), config.clone(), descriptor.clone())
            .await?;
        samples.push(started.elapsed());
    }
    samples.sort_unstable();
    let p95 = samples[samples.len() * 95 / 100];
    ensure!(
        p95 <= maximum,
        "resident adapter selection p95 {p95:?} exceeds {maximum:?}"
    );
    Ok(p95)
}

async fn qualify_adapter_isolation(
    manager: &RuntimeManager,
    config: &LiteRtLmConfig,
    first: &AdapterDescriptor,
    second: &AdapterDescriptor,
) -> Result<()> {
    let first_future = generate(
        manager,
        "soak-lora",
        config,
        "adapter-isolation-one",
        "adapter-isolation-session-one",
        Some(binding(first)),
        "Generate one short token sequence.",
        16,
    );
    let second_future = generate(
        manager,
        "soak-lora",
        config,
        "adapter-isolation-two",
        "adapter-isolation-session-two",
        Some(binding(second)),
        "Generate one short token sequence.",
        16,
    );
    let (first_output, second_output) = tokio::try_join!(first_future, second_future)?;
    ensure!(
        first_output != second_output,
        "two real adapters produced identical outputs"
    );
    manager
        .release_session("adapter-isolation-session-one")
        .await?;
    manager
        .release_session("adapter-isolation-session-two")
        .await?;
    let status = manager.status().await?;
    ensure!(
        status
            .adapters
            .iter()
            .filter(|adapter| adapter_is_resident(adapter))
            .count()
            >= 2,
        "two real adapters are not resident after qualification"
    );
    Ok(())
}

async fn cancel_generation(
    manager: &RuntimeManager,
    config: &LiteRtLmConfig,
    cycle: u64,
    cancellation_tokens: u32,
) -> Result<()> {
    let request_id = format!("cancel-{cycle}");
    let session_id = format!("cancel-session-{cycle}");
    let conversation = prepared(
        &session_id,
        "Generate a long numbered technical inventory with detailed explanations.",
        cancellation_tokens,
    );
    let running_manager = manager.clone();
    let running_config = config.clone();
    let running_request_id = request_id.clone();
    let running_session_id = session_id.clone();
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        running_manager
            .generate(
                RuntimeRequest {
                    request_id: running_request_id,
                    session_id: running_session_id,
                    model_id: "soak-general".to_owned(),
                    model_config: running_config,
                    stage: RuntimeStage::Direct,
                    adapter: None,
                    conversation,
                    completion_reserve: cancellation_tokens,
                    priority: RuntimePriority::Interactive,
                    deadline: Instant::now() + OPERATION_TIMEOUT,
                    admission_hook: None,
                },
                event_tx,
                CancellationToken::new(),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            match event_rx.recv().await {
                Some(RuntimeEvent::FirstToken { .. }) => break,
                Some(_) => {}
                None => bail!("generation ended before first token"),
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("wait for first real token before cancellation")??;
    ensure!(
        manager.cancel(&request_id),
        "active generation was not cancellable"
    );
    let cancelled_at = Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .context("native model cancellation exceeded two seconds")??
        .context("cancelled generation returned an error")?;
    ensure!(
        matches!(result, LocalInferenceResult::Cancelled),
        "cancelled generation completed normally"
    );
    ensure!(cancelled_at.elapsed() <= Duration::from_secs(2));
    manager.release_session(&session_id).await?;
    Ok(())
}

async fn cleanup_recovered_jobs(supervisor: &Arc<NativeExecutionSupervisor>) -> Result<()> {
    for snapshot in supervisor.list(None, 0, 512).await {
        ensure!(
            snapshot.lifecycle.is_terminal(),
            "recovered native job {} is still active",
            snapshot.job_id
        );
        supervisor.cleanup(&snapshot.job_id).await?;
    }
    Ok(())
}

async fn exercise_native_job(
    supervisor: &Arc<NativeExecutionSupervisor>,
    cycle: u64,
) -> Result<()> {
    let script = format!(
        "printf 'HTTP/1.1 200 OK\\ncycle={cycle}\\n'; printf 'port 443 open cycle={cycle}\\n' >&2"
    );
    let job = supervisor
        .start_command_for(
            "release-soak",
            CommandRequest {
                executable: "/bin/sh".to_owned(),
                args: vec!["-c".to_owned(), script],
                cwd: None,
                env: BTreeMap::new(),
                timeout_ms: 5_000,
                stdin: CommandStdin::Null,
                metadata: BTreeMap::from([("cycle".to_owned(), cycle.into())]),
            },
        )
        .await?;
    let completed = supervisor.wait(&job.job_id, Duration::from_secs(5)).await?;
    ensure!(
        completed.lifecycle == JobLifecycle::Completed,
        "native job failed: {completed:?}"
    );
    ensure!(
        completed.exit_code == Some(0),
        "native job returned non-zero"
    );
    let page = supervisor
        .output_page(&job.job_id, 0, 128, 64 * 1024)
        .await?;
    ensure!(!page.records.is_empty(), "native spool was empty");
    ensure!(
        page.records
            .windows(2)
            .all(|pair| pair[0].sequence < pair[1].sequence),
        "native spool sequence is not strictly increasing"
    );
    ensure!(
        page.records
            .iter()
            .any(|record| record.stream == OutputStream::Stdout)
    );
    ensure!(
        page.records
            .iter()
            .any(|record| record.stream == OutputStream::Stderr)
    );
    let artifacts = supervisor.artifacts(&job.job_id).await?;
    ensure!(
        artifacts
            .iter()
            .any(|artifact| artifact.byte_size > 0 && artifact.path.is_file()),
        "native job did not expose a durable spool artifact"
    );
    supervisor.cleanup(&job.job_id).await?;
    Ok(())
}

async fn cancel_native_job(supervisor: &Arc<NativeExecutionSupervisor>, cycle: u64) -> Result<()> {
    let job = supervisor
        .start_command_for(
            "release-soak",
            CommandRequest {
                executable: "/bin/sh".to_owned(),
                args: vec![
                    "-c".to_owned(),
                    format!("printf 'started {cycle}\\n'; sleep 30"),
                ],
                cwd: None,
                env: BTreeMap::new(),
                timeout_ms: 60_000,
                stdin: CommandStdin::Null,
                metadata: BTreeMap::new(),
            },
        )
        .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let started = Instant::now();
    let cancelled = supervisor.cancel(&job.job_id).await?;
    let cancellation_elapsed = started.elapsed();
    ensure!(
        cancellation_elapsed <= Duration::from_secs(1),
        "subprocess cancellation took {cancellation_elapsed:?}, exceeding one second"
    );
    ensure!(
        cancelled.lifecycle == JobLifecycle::Cancelled,
        "subprocess was not cancelled"
    );
    supervisor.cleanup(&job.job_id).await?;
    Ok(())
}

async fn initialize_memory(args: &Args, state_dir: &Path) -> Result<MemoryFixture> {
    init_sqlite_vec();
    let root = state_dir.join("memory");
    let storage = MemoryStorage::new_flat(&std::env::current_dir()?, &root);
    storage.ensure_initialized()?;
    let watcher = Arc::new(
        MemoryFileWatcher::start(storage.workspace_dir())
            .ok_or_else(|| anyhow!("local memory watcher could not start"))?,
    );
    let embedding_config = MemoryEmbeddingConfig {
        provider: "local".to_owned(),
        model: Some(args.embedding_model.clone()),
        dimensions: args.embedding_dimensions,
    };
    let provider = LocalEmbeddingProvider::from_config(&embedding_config).ok_or_else(|| {
        anyhow!(
            "unsupported local embedding profile {}",
            args.embedding_model
        )
    })?;
    provider
        .wait_ready(Duration::from_secs(15 * 60))
        .await
        .map_err(|error| anyhow!("local embedding model failed to materialize: {error}"))?;
    let database = storage.workspace_dir().join("index.sqlite");
    {
        let _ = MemoryIndex::open_or_create(
            &database,
            storage.clone(),
            MemoryIndexConfig::default(),
            args.embedding_dimensions,
        )?;
    }
    let search = MemorySearchConfig {
        min_score: 0.0,
        max_results: 8,
        ..Default::default()
    };
    let backend = MemoryBackendImpl::new(database.clone(), storage.clone())
        .with_session_id("release-soak".to_owned())
        .with_embedding(embedding_config.clone(), String::new(), None)
        .with_search_config(search)
        .with_watcher(Arc::clone(&watcher), 60);
    let fixture = MemoryFixture {
        storage,
        backend,
        provider,
        watcher,
        database,
        embedding_config,
    };
    exercise_memory(&fixture, 0, args.max_memory_query_ms).await?;
    wait_for_embedding_backfill(&fixture.database, Duration::from_secs(60)).await?;
    Ok(fixture)
}

async fn exercise_memory(fixture: &MemoryFixture, cycle: u64, maximum_ms: u64) -> Result<()> {
    let marker = format!("soak-evidence-{cycle}");
    fixture.storage.append_to_memory(
        MemoryScope::Workspace,
        &format!("## Cycle {cycle}\n\n{marker}: HTTP 200 and port 443 open."),
    )?;
    wait_for_watcher(&fixture.watcher, Duration::from_secs(2)).await?;
    let started = Instant::now();
    let results = fixture
        .backend
        .search(&marker, 8, 0.0)
        .await
        .map_err(|error| anyhow!(error.to_string()))?;
    let elapsed = started.elapsed();
    ensure!(
        elapsed <= Duration::from_millis(maximum_ms),
        "memory retrieval took {elapsed:?}, exceeding {maximum_ms} ms"
    );
    ensure!(
        results
            .iter()
            .any(|result| result.snippet.contains(&marker)),
        "memory query did not retrieve newly persisted evidence"
    );
    Ok(())
}

async fn wait_for_watcher(watcher: &MemoryFileWatcher, maximum: Duration) -> Result<()> {
    tokio::time::timeout(maximum, async {
        while !watcher.is_dirty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("memory watcher did not observe the durable write")?;
    Ok(())
}

async fn wait_for_embedding_backfill(database: &Path, maximum: Duration) -> Result<()> {
    tokio::time::timeout(maximum, async {
        loop {
            let status = embedding_backfill_status();
            if let Some(index) = status
                .indexes
                .iter()
                .find(|index| index.database == database)
                && !index.active
                && index.pending_chunks == 0
                && index.embedded_chunks > 0
                && index.failed_batches == 0
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .context("local embedding backfill did not complete successfully")?;
    Ok(())
}

async fn qualify_busy_embedding_fallback(fixture: &MemoryFixture, maximum_ms: u64) -> Result<()> {
    ensure!(
        fixture.embedding_config.provider == "local",
        "busy fallback qualification requires local embeddings"
    );
    let provider = fixture.provider.clone();
    // Match the production backfill batch shape. Oversized synthetic
    // documents would benchmark ONNX truncation and allocator pressure rather
    // than the interactive busy-provider fallback contract.
    let documents = vec!["x".repeat(2 * 1024); 32];
    let task = tokio::spawn(async move {
        let refs = documents.iter().map(String::as_str).collect::<Vec<_>>();
        provider.embed_batch(&refs).await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !fixture.provider.is_busy() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("local embedding inference did not enter busy state")?;
    let started = Instant::now();
    let results = fixture
        .backend
        .search("soak-evidence", 8, 0.0)
        .await
        .map_err(|error| anyhow!(error.to_string()))?;
    let elapsed = started.elapsed();
    ensure!(
        !results.is_empty(),
        "busy embedding fallback returned no FTS evidence"
    );
    ensure!(
        elapsed <= Duration::from_millis(maximum_ms),
        "FTS fallback while embeddings were busy took {elapsed:?}, exceeding {maximum_ms} ms"
    );
    task.await
        .context("busy embedding task join")?
        .map_err(|error| anyhow!(error.to_string()))?;
    Ok(())
}

async fn wait_runtime_stable(manager: &RuntimeManager) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut previous = None;
    let mut stable_samples = 0;
    loop {
        let status = manager.status().await?;
        let signature = (
            status.capacity.resident_workers,
            status
                .workers
                .values()
                .map(|worker| worker.resident_bytes)
                .sum::<u64>(),
        );
        if previous == Some(signature) {
            stable_samples += 1;
            if stable_samples >= 3 {
                return Ok(());
            }
        } else {
            previous = Some(signature);
            stable_samples = 0;
        }
        ensure!(
            Instant::now() < deadline,
            "runtime did not reach a stable warm state"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn aggregate_resources(manager: &RuntimeManager) -> Result<AggregateResources> {
    let harness = ResourceSnapshot::capture();
    let harness_rss = u64::try_from(harness.rss.ok_or_else(|| anyhow!("RSS unavailable"))?)?;
    let harness_threads = u64::try_from(
        harness
            .threads
            .ok_or_else(|| anyhow!("thread count unavailable"))?,
    )?;
    let harness_fds = u64::try_from(
        harness
            .fds
            .ok_or_else(|| anyhow!("file descriptor count unavailable"))?,
    )?;
    let status = manager.status().await?;
    let worker_rss = status
        .workers
        .values()
        .map(|worker| worker.resident_bytes)
        .sum();
    let worker_threads = status
        .workers
        .values()
        .map(|worker| u64::from(worker.thread_count))
        .sum();
    let worker_fds = status
        .workers
        .values()
        .map(|worker| u64::from(worker.file_descriptor_count))
        .sum();
    Ok(AggregateResources {
        harness_rss_bytes: harness_rss,
        worker_rss_bytes: worker_rss,
        total_rss_bytes: harness_rss.saturating_add(worker_rss),
        harness_threads,
        worker_threads,
        total_threads: harness_threads.saturating_add(worker_threads),
        harness_fds,
        worker_fds,
        total_fds: harness_fds.saturating_add(worker_fds),
    })
}

fn component_max(left: AggregateResources, right: AggregateResources) -> AggregateResources {
    AggregateResources {
        harness_rss_bytes: left.harness_rss_bytes.max(right.harness_rss_bytes),
        worker_rss_bytes: left.worker_rss_bytes.max(right.worker_rss_bytes),
        total_rss_bytes: left.total_rss_bytes.max(right.total_rss_bytes),
        harness_threads: left.harness_threads.max(right.harness_threads),
        worker_threads: left.worker_threads.max(right.worker_threads),
        total_threads: left.total_threads.max(right.total_threads),
        harness_fds: left.harness_fds.max(right.harness_fds),
        worker_fds: left.worker_fds.max(right.worker_fds),
        total_fds: left.total_fds.max(right.total_fds),
    }
}

fn enforce_resource_bounds(args: &Args, peak: AggregateResources) -> Result<()> {
    ensure!(
        peak.total_rss_bytes <= args.max_rss_growth_mib.saturating_mul(MIB),
        "aggregate RSS growth {} MiB exceeds {} MiB",
        peak.total_rss_bytes / MIB,
        args.max_rss_growth_mib
    );
    ensure!(
        peak.total_threads <= args.max_thread_growth as u64,
        "aggregate thread growth {} exceeds {}",
        peak.total_threads,
        args.max_thread_growth
    );
    ensure!(
        peak.total_fds <= args.max_fd_growth as u64,
        "aggregate file descriptor growth {} exceeds {}",
        peak.total_fds,
        args.max_fd_growth
    );
    Ok(())
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn long_profiles_cannot_be_shortened() {
        assert!(SoakProfile::Ci.duration(Some(1)).is_err());
        assert!(SoakProfile::Release.duration(Some(1)).is_err());
        assert_eq!(
            SoakProfile::Ci.duration(None).unwrap(),
            Duration::from_secs(24 * 60 * 60)
        );
        assert_eq!(
            SoakProfile::Release.duration(None).unwrap(),
            Duration::from_secs(72 * 60 * 60)
        );
    }

    #[test]
    fn aggregate_growth_saturates_per_process_class() {
        let baseline = AggregateResources {
            harness_rss_bytes: 10,
            worker_rss_bytes: 20,
            total_rss_bytes: 30,
            harness_threads: 4,
            worker_threads: 5,
            total_threads: 9,
            harness_fds: 6,
            worker_fds: 7,
            total_fds: 13,
        };
        let current = AggregateResources {
            harness_rss_bytes: 9,
            worker_rss_bytes: 25,
            total_rss_bytes: 34,
            harness_threads: 7,
            worker_threads: 2,
            total_threads: 9,
            harness_fds: 5,
            worker_fds: 9,
            total_fds: 14,
        };
        let growth = current.growth_from(baseline);
        assert_eq!(growth.harness_rss_bytes, 0);
        assert_eq!(growth.worker_rss_bytes, 5);
        assert_eq!(growth.total_rss_bytes, 4);
        assert_eq!(growth.harness_threads, 3);
        assert_eq!(growth.worker_threads, 0);
        assert_eq!(growth.total_fds, 1);
    }
}
