use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex as AsyncMutex, OnceCell, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use xai_grok_sampling_types::SamplingError;

use crate::adapter::{
    AdapterBinding, AdapterDescriptor, AdapterError, AdapterManager, hash_artifact,
};
use crate::context::{ContextBudgetBroker, ContextComponent, ContextComponentKind, StageBudget};
use crate::events::{RuntimeAdmission, RuntimeAdmissionHook, RuntimeEvent};
use crate::litert_lm::{
    LiteRtLmConfig, LocalInferenceResult, LoraAdapterConfig, PreparedConversation,
    drop_inactive_sessions, drop_session, measure_prepared, prewarm, prewarm_adapter,
    run_prepared_request, unload_adapter as unload_native_adapter,
};
use crate::protocol::WorkerStats;
use crate::resource::{
    MemoryLimits, ResourceClass, ResourceGovernor, ResourceLease, ResourceSnapshot,
};
use crate::worker::WorkerClient;

const DEFAULT_MODEL_LOAD_TIMEOUT: Duration = Duration::from_secs(300);

const REVIEWER_MAX_QUEUE_WAIT: Duration = Duration::from_millis(500);
const MANDATORY_MAX_QUEUE_WAIT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeMode {
    Auto,
    Worker,
    InProcess,
}

impl RuntimeMode {
    fn resolve(self) -> Self {
        match self {
            Self::Auto if cfg!(debug_assertions) => Self::InProcess,
            Self::Auto => Self::Worker,
            explicit => explicit,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeManagerConfig {
    pub mode: RuntimeMode,
    pub worker_path: Option<PathBuf>,
    pub queue_capacity: usize,
    pub max_active_requests: usize,
    pub memory_limits: MemoryLimits,
}

impl Default for RuntimeManagerConfig {
    fn default() -> Self {
        let mode = match std::env::var("GROK_LOCAL_RUNTIME_MODE").as_deref() {
            Ok("worker") => RuntimeMode::Worker,
            Ok("in_process") | Ok("in-process") => RuntimeMode::InProcess,
            _ => RuntimeMode::Auto,
        };
        Self {
            mode,
            worker_path: std::env::var_os("GROK_LOCAL_RUNTIME_WORKER").map(PathBuf::from),
            queue_capacity: 64,
            max_active_requests: 2,
            memory_limits: MemoryLimits::detect(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeStage {
    Direct,
    Planner,
    Executor,
    Reviewer,
    Embedding,
    Prewarm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimePriority {
    Executor,
    Interactive,
    Reviewer,
    Prewarm,
    Background,
}

impl RuntimePriority {
    fn index(self) -> usize {
        match self {
            Self::Executor => 0,
            Self::Interactive => 1,
            Self::Reviewer => 2,
            Self::Prewarm => 3,
            Self::Background => 4,
        }
    }
}

impl RuntimeStage {
    fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Planner => "planner",
            Self::Executor => "executor",
            Self::Reviewer => "reviewer",
            Self::Embedding => "embedding",
            Self::Prewarm => "prewarm",
        }
    }

    fn completion_reserve(self, requested: u32, context_window: Option<u32>) -> u32 {
        match self {
            Self::Planner => 768,
            Self::Executor => 1_024,
            Self::Reviewer => 768,
            // A caller-supplied direct limit is the selected model's
            // configured maximum and must not be silently increased. Only
            // derive a conservative default when the request did not carry a
            // limit at all.
            Self::Direct if requested != 0 => requested,
            Self::Direct => context_window
                .map(|window| (window / 8).max(1))
                .unwrap_or(1),
            Self::Embedding | Self::Prewarm => requested,
        }
    }

    fn stage_budget(self, reserve: u32) -> StageBudget {
        match self {
            Self::Planner => StageBudget::planner(),
            Self::Executor => StageBudget::executor(),
            Self::Reviewer => StageBudget::reviewer(),
            Self::Direct | Self::Embedding | Self::Prewarm => StageBudget::direct(reserve),
        }
    }
}

pub struct RuntimeRequest {
    pub request_id: String,
    pub session_id: String,
    pub model_id: String,
    pub model_config: LiteRtLmConfig,
    pub stage: RuntimeStage,
    pub adapter: Option<AdapterBinding>,
    pub conversation: PreparedConversation,
    pub completion_reserve: u32,
    pub priority: RuntimePriority,
    pub deadline: Instant,
    /// Optional durable barrier invoked after exact context admission and
    /// before any native generation work starts.
    pub admission_hook: Option<RuntimeAdmissionHook>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapacitySnapshot {
    pub mode: RuntimeMode,
    pub queue_depth: usize,
    pub active_requests: u32,
    pub completed_requests: u64,
    pub resident_workers: u32,
    pub resources: ResourceSnapshot,
}

struct RuntimeManagerInner {
    mode: RuntimeMode,
    worker_path: Option<PathBuf>,
    scheduler: Scheduler,
    queue_depth: Arc<AtomicUsize>,
    active_requests: AtomicU32,
    completed_requests: AtomicU64,
    governor: ResourceGovernor,
    adapters: AdapterManager,
    adapter_deployments: AsyncMutex<HashMap<String, AdapterDeployment>>,
    adapter_load_gates: AsyncMutex<HashMap<String, Arc<AsyncMutex<()>>>>,
    backend_selections: AsyncMutex<HashMap<String, String>>,
    backend_probe_gates: AsyncMutex<HashMap<String, Arc<AsyncMutex<()>>>>,
    workers: AsyncMutex<HashMap<String, Arc<OnceCell<Arc<WorkerClient>>>>>,
    worker_leases: AsyncMutex<HashMap<String, ResourceLease>>,
    replica_counts: AsyncMutex<HashMap<String, usize>>,
    replica_cursors: AsyncMutex<HashMap<String, usize>>,
    calibrating_models: AsyncMutex<HashSet<String>>,
    resident_workers: AtomicU32,
    active_cancellations: StdMutex<HashMap<String, CancellationToken>>,
}

#[derive(Clone)]
struct AdapterDeployment {
    model_id: String,
    config: LiteRtLmConfig,
}

struct CalibrationSample {
    completion_tokens: u32,
    ttft_millis: u64,
}

struct BackendProbeSample {
    config: LiteRtLmConfig,
    sample: CalibrationSample,
    elapsed: Duration,
}

/// Process-wide owner for local model admission and lifecycle.
#[derive(Clone)]
pub struct RuntimeManager {
    inner: Arc<RuntimeManagerInner>,
}

impl RuntimeManager {
    pub fn capability_manifest(
        config: &RuntimeManagerConfig,
    ) -> xai_grok_protocol::CapabilityManifest {
        crate::local_runtime_capability_manifest(config)
    }

    pub fn new(config: RuntimeManagerConfig) -> Self {
        let governor = ResourceGovernor::new(config.memory_limits);
        Self::with_governor(config, governor)
    }

    fn with_governor(config: RuntimeManagerConfig, governor: ResourceGovernor) -> Self {
        let queue_depth = Arc::new(AtomicUsize::new(0));
        let scheduler = Scheduler::spawn(
            config.max_active_requests.max(1),
            config.queue_capacity.max(1),
            Arc::clone(&queue_depth),
        );
        Self {
            inner: Arc::new(RuntimeManagerInner {
                mode: config.mode.resolve(),
                worker_path: config.worker_path,
                scheduler,
                queue_depth,
                active_requests: AtomicU32::new(0),
                completed_requests: AtomicU64::new(0),
                adapters: AdapterManager::new(governor.clone()),
                adapter_deployments: AsyncMutex::new(HashMap::new()),
                adapter_load_gates: AsyncMutex::new(HashMap::new()),
                backend_selections: AsyncMutex::new(HashMap::new()),
                backend_probe_gates: AsyncMutex::new(HashMap::new()),
                governor,
                workers: AsyncMutex::new(HashMap::new()),
                worker_leases: AsyncMutex::new(HashMap::new()),
                replica_counts: AsyncMutex::new(HashMap::new()),
                replica_cursors: AsyncMutex::new(HashMap::new()),
                calibrating_models: AsyncMutex::new(HashSet::new()),
                resident_workers: AtomicU32::new(0),
                active_cancellations: StdMutex::new(HashMap::new()),
            }),
        }
    }

    pub fn global() -> &'static Self {
        static GLOBAL: OnceLock<RuntimeManager> = OnceLock::new();
        GLOBAL.get_or_init(|| {
            RuntimeManager::with_governor(
                RuntimeManagerConfig::default(),
                ResourceGovernor::global().clone(),
            )
        })
    }

    pub fn resource_governor(&self) -> ResourceGovernor {
        self.inner.governor.clone()
    }

    pub fn adapter_manager(&self) -> AdapterManager {
        self.inner.adapters.clone()
    }

    async fn resolve_backend(
        &self,
        mut config: LiteRtLmConfig,
        model_id: &str,
        require_gpu: bool,
    ) -> Result<LiteRtLmConfig, SamplingError> {
        if config.backend != "auto" {
            if require_gpu && !matches!(config.backend.as_str(), "gpu" | "gpu_artisan") {
                return Err(runtime_error(
                    "local_runtime_backend",
                    "LoRA-capable requests require a GPU backend",
                ));
            }
            return Ok(config);
        }
        let selection_key = backend_selection_key(&config, model_id, require_gpu);
        if let Some(selected) = self
            .inner
            .backend_selections
            .lock()
            .await
            .get(&selection_key)
            .cloned()
        {
            config.backend = selected;
            return Ok(config);
        }
        let gate = {
            let mut gates = self.inner.backend_probe_gates.lock().await;
            Arc::clone(
                gates
                    .entry(selection_key.clone())
                    .or_insert_with(|| Arc::new(AsyncMutex::new(()))),
            )
        };
        let _guard = gate.lock().await;
        if let Some(selected) = self
            .inner
            .backend_selections
            .lock()
            .await
            .get(&selection_key)
            .cloned()
        {
            config.backend = selected;
            return Ok(config);
        }

        let candidates: &[&str] = if require_gpu {
            &["gpu"]
        } else if cfg!(target_os = "macos") {
            &["gpu", "cpu"]
        } else {
            &["cpu", "gpu"]
        };
        let mut probes = Vec::new();
        let mut failures = Vec::new();
        for backend in candidates {
            let mut candidate = config.clone();
            candidate.backend = (*backend).to_string();
            let started = Instant::now();
            let result = match self.inner.mode {
                RuntimeMode::Worker => match self
                    .worker_at(candidate.clone(), model_id.to_string(), 0)
                    .await
                {
                    Ok(worker) => {
                        calibration_request(&worker, model_id, &format!("backend-{backend}")).await
                    }
                    Err(error) => Err(error),
                },
                RuntimeMode::InProcess | RuntimeMode::Auto => {
                    in_process_calibration(candidate.clone(), model_id).await
                }
            };
            match result {
                Ok(sample) => probes.push(BackendProbeSample {
                    config: candidate,
                    sample,
                    elapsed: started.elapsed(),
                }),
                Err(error) => failures.push(format!("{backend}: {error}")),
            }
        }
        let selected_index = select_backend_probe(&probes, require_gpu).ok_or_else(|| {
            runtime_error(
                "local_runtime_backend",
                format!(
                    "no compatible local inference backend passed startup probing: {}",
                    failures.join("; ")
                ),
            )
        })?;
        let selected = probes[selected_index].config.clone();
        if self.inner.mode == RuntimeMode::Worker {
            let rejected_backends: &[&str] = if require_gpu { &["cpu"] } else { candidates };
            for backend in rejected_backends {
                if *backend != selected.backend {
                    let mut rejected = config.clone();
                    rejected.backend = (*backend).to_string();
                    self.remove_backend_probe_worker(&rejected, model_id).await;
                }
            }
        }
        self.inner
            .backend_selections
            .lock()
            .await
            .insert(selection_key, selected.backend.clone());
        tracing::info!(
            target: crate::LOG_TARGET,
            event = "local_runtime_backend_selected",
            model_id,
            backend = %selected.backend,
            gpu_required = require_gpu,
            "selected local backend after compatibility and startup benchmark probes"
        );
        Ok(selected)
    }

    async fn remove_backend_probe_worker(&self, config: &LiteRtLmConfig, model_id: &str) {
        if self.inner.active_requests.load(Ordering::Acquire) != 0 {
            return;
        }
        let base_key = replica_base_key(config, model_id);
        let key = replica_worker_key(&base_key, 0);
        let worker = self
            .inner
            .workers
            .lock()
            .await
            .remove(&key)
            .and_then(|cell| cell.get().cloned());
        if let Some(worker) = worker {
            worker.shutdown().await;
        }
        self.release_worker_reservation(&key).await;
        self.inner.replica_counts.lock().await.remove(&base_key);
        self.inner.replica_cursors.lock().await.remove(&base_key);
    }

    pub async fn prewarm_model(
        &self,
        model_id: String,
        config: LiteRtLmConfig,
    ) -> Result<(), SamplingError> {
        let config = self
            .resolve_backend(
                config.clone(),
                &model_id,
                config.adapter_descriptor.is_some() || config.lora_adapter.is_some(),
            )
            .await?;
        self.relieve_memory_pressure().await?;
        let deadline = Instant::now() + DEFAULT_MODEL_LOAD_TIMEOUT;
        let _permit = self
            .inner
            .scheduler
            .acquire(RuntimePriority::Prewarm, model_id.clone(), deadline)
            .await?;
        let result = match self.inner.mode {
            RuntimeMode::InProcess | RuntimeMode::Auto => prewarm(config.clone()).await,
            RuntimeMode::Worker => self
                .worker(config.clone(), model_id.clone())
                .await
                .map(|_| ()),
        };
        if result.is_ok() && self.inner.mode == RuntimeMode::Worker {
            self.start_replica_calibration(config, model_id).await;
        }
        result
    }

    pub async fn prewarm_adapter(
        &self,
        model_id: String,
        config: LiteRtLmConfig,
        descriptor: AdapterDescriptor,
    ) -> Result<u32, SamplingError> {
        let config = self.resolve_backend(config, &model_id, true).await?;
        self.relieve_memory_pressure().await?;
        if !config.backend.to_ascii_lowercase().contains("gpu") {
            return Err(runtime_error(
                "local_runtime_adapter",
                "LoRA requests require a GPU LiteRT-LM backend",
            ));
        }
        if config.supported_lora_ranks.is_empty()
            || !config.supported_lora_ranks.contains(&descriptor.rank)
        {
            return Err(runtime_error(
                "local_runtime_adapter",
                format!(
                    "adapter rank {} is not declared by the base model; supported ranks: {:?}",
                    descriptor.rank, config.supported_lora_ranks
                ),
            ));
        }
        let binding = AdapterBinding {
            adapter_id: descriptor.adapter_id.clone(),
            revision: descriptor.revision.clone(),
        };
        let load_gate = {
            let mut gates = self.inner.adapter_load_gates.lock().await;
            Arc::clone(
                gates
                    .entry(binding.immutable_id())
                    .or_insert_with(|| Arc::new(AsyncMutex::new(()))),
            )
        };
        let _load_guard = load_gate.lock().await;
        if let Some(existing) = self.inner.adapters.descriptor(&binding) {
            if existing != descriptor {
                return Err(runtime_error(
                    "local_runtime_adapter",
                    format!(
                        "resident adapter descriptor conflicts with requested immutable revision: {}",
                        binding.immutable_id()
                    ),
                ));
            }
            let lease = self
                .inner
                .adapters
                .acquire(&binding)
                .map_err(adapter_error)?;
            let native_id = lease.native_id();
            drop(lease);
            return Ok(native_id);
        }
        let model_path = config.model_path.clone();
        let actual_base_hash = tokio::task::spawn_blocking(move || hash_artifact(&model_path))
            .await
            .map_err(|error| {
                runtime_error(
                    "local_runtime_adapter",
                    format!("base model hash task failed: {error}"),
                )
            })?
            .map_err(adapter_error)?;
        if actual_base_hash != descriptor.base_model_hash {
            return Err(runtime_error(
                "local_runtime_adapter",
                format!(
                    "adapter base model hash mismatch: expected={}, actual={actual_base_hash}",
                    descriptor.base_model_hash
                ),
            ));
        }
        // A new immutable revision must pass native materialization and its
        // probe while every currently resident revision of the same logical
        // adapter remains leased. Under hard pressure we fail the replacement
        // instead of destroying the known-good revision first.
        let replacement_leases = self
            .inner
            .adapters
            .bindings_for_adapter(&descriptor.adapter_id)
            .into_iter()
            .filter_map(|binding| self.inner.adapters.acquire(&binding).ok())
            .collect::<Vec<_>>();
        let already_resident = false;
        let native_id = loop {
            match self
                .inner
                .adapters
                .register_host_resident(descriptor.clone())
            {
                Ok(native_id) => break native_id,
                Err(resource_error @ AdapterError::Resource(_)) => {
                    let Some(victim) = self.inner.adapters.next_eviction_candidate() else {
                        return Err(adapter_error(resource_error));
                    };
                    let deployment = self
                        .inner
                        .adapter_deployments
                        .lock()
                        .await
                        .get(&victim.immutable_id())
                        .cloned();
                    let Some(deployment) = deployment else {
                        return Err(runtime_error(
                            "local_runtime_adapter",
                            format!(
                                "cannot reclaim adapter {} because its native deployment is unknown",
                                victim.immutable_id()
                            ),
                        ));
                    };
                    self.unload_adapter(deployment.model_id, deployment.config, victim)
                        .await?;
                }
                Err(error) => return Err(adapter_error(error)),
            }
        };
        let deadline = Instant::now() + DEFAULT_MODEL_LOAD_TIMEOUT;
        let _permit = self
            .inner
            .scheduler
            .acquire(RuntimePriority::Prewarm, model_id.clone(), deadline)
            .await?;
        let adapter_identity = descriptor.immutable_id();
        let deployment = AdapterDeployment {
            model_id: model_id.clone(),
            config: config.clone(),
        };
        let result = match self.inner.mode {
            RuntimeMode::InProcess | RuntimeMode::Auto => prewarm_adapter(
                config.clone(),
                LoraAdapterConfig {
                    path: descriptor.path.clone(),
                    id: adapter_identity.clone(),
                },
            )
            .await
            .map(|_| native_id),
            RuntimeMode::Worker => {
                let worker = self.worker(config.clone(), model_id.clone()).await?;
                worker.load_adapter(descriptor.clone()).await
            }
        };
        let outcome = match result {
            Ok(worker_native_id) => {
                let probe_request_id = format!(
                    "adapter-probe:{}:{}",
                    binding.immutable_id(),
                    uuid::Uuid::new_v4()
                );
                let adapter = LoraAdapterConfig {
                    path: descriptor.path.clone(),
                    id: adapter_identity.clone(),
                };
                let mut probe = PreparedConversation {
                    session_id: None,
                    system_message: Some(
                        "This is a local adapter compatibility probe.".to_string(),
                    ),
                    retrieved_memory: None,
                    messages: "[]".to_string(),
                    tools: "[]".to_string(),
                    current_message: serde_json::json!({
                        "role": "user",
                        "content": [{"type": "text", "text": "Reply with READY."}]
                    })
                    .to_string(),
                    max_output_tokens: Some(8),
                    temperature: Some(0.0),
                    top_p: Some(1.0),
                    lora_adapter: Some(adapter.clone()),
                };
                let (probe_events, _probe_event_rx) = mpsc::unbounded_channel();
                let probe_result = match self.inner.mode {
                    RuntimeMode::InProcess | RuntimeMode::Auto => {
                        let mut probe_config = config.clone();
                        probe_config.lora_adapter = Some(adapter.clone());
                        run_prepared_request(
                            probe_request_id,
                            probe,
                            probe_config,
                            model_id.clone(),
                            &probe_events,
                            &CancellationToken::new(),
                        )
                        .await
                    }
                    RuntimeMode::Worker => {
                        probe.lora_adapter = None;
                        match self.worker(config.clone(), model_id.clone()).await {
                            Ok(worker) => match worker
                                .select_adapter(probe_request_id.clone(), &binding)
                                .await
                            {
                                Ok(()) => {
                                    worker
                                        .generate(
                                            probe_request_id,
                                            probe,
                                            probe_events,
                                            CancellationToken::new(),
                                        )
                                        .await
                                }
                                Err(error) => Err(error),
                            },
                            Err(error) => Err(error),
                        }
                    }
                };
                if !matches!(probe_result, Ok(LocalInferenceResult::Completed { .. })) {
                    let probe_error = match probe_result {
                        Ok(LocalInferenceResult::Cancelled) => {
                            "adapter probe generation was cancelled".to_string()
                        }
                        Err(error) => error.to_string(),
                        Ok(LocalInferenceResult::Completed { .. }) => unreachable!(),
                    };
                    if !already_resident {
                        match self.inner.mode {
                            RuntimeMode::InProcess | RuntimeMode::Auto => {
                                let _ = unload_native_adapter(config.clone(), adapter).await;
                            }
                            RuntimeMode::Worker => {
                                if let Ok(worker) =
                                    self.worker(config.clone(), model_id.clone()).await
                                {
                                    let _ = worker.unload_adapter(&binding).await;
                                }
                            }
                        }
                        let _ = self.inner.adapters.evict(&binding);
                    }
                    return Err(runtime_error(
                        "local_runtime_adapter_probe",
                        format!(
                            "adapter {} loaded but failed probe generation: {probe_error}",
                            binding.immutable_id()
                        ),
                    ));
                }
                self.inner
                    .adapters
                    .mark_device_resident(&binding)
                    .map_err(adapter_error)?;
                self.inner
                    .adapter_deployments
                    .lock()
                    .await
                    .insert(binding.immutable_id(), deployment);
                Ok(worker_native_id)
            }
            Err(error) => {
                if !already_resident {
                    let _ = self.inner.adapters.evict(&binding);
                }
                Err(error)
            }
        };
        drop(replacement_leases);
        outcome
    }

    pub async fn unload_adapter(
        &self,
        model_id: String,
        config: LiteRtLmConfig,
        binding: AdapterBinding,
    ) -> Result<(), SamplingError> {
        let config = self.resolve_backend(config, &model_id, true).await?;
        let descriptor = self
            .inner
            .adapters
            .begin_eviction(&binding)
            .map_err(adapter_error)?;
        let adapter_identity = descriptor.immutable_id();
        let result = match self.inner.mode {
            RuntimeMode::InProcess | RuntimeMode::Auto => {
                unload_native_adapter(
                    config,
                    LoraAdapterConfig {
                        path: descriptor.path,
                        id: adapter_identity,
                    },
                )
                .await
            }
            RuntimeMode::Worker => {
                let worker = self.worker(config, model_id).await?;
                worker.unload_adapter(&binding).await
            }
        };
        if let Err(error) = result {
            self.inner.adapters.rollback_eviction(&binding);
            return Err(error);
        }
        self.inner
            .adapters
            .commit_eviction(&binding)
            .map_err(adapter_error)?;
        self.inner
            .adapter_deployments
            .lock()
            .await
            .remove(&binding.immutable_id());
        Ok(())
    }

    pub async fn measure_exact(
        &self,
        model_id: String,
        config: LiteRtLmConfig,
        prepared: &PreparedConversation,
        deadline: Instant,
    ) -> Result<u32, SamplingError> {
        let config = self.resolve_backend(config, &model_id, false).await?;
        let _permit = self
            .inner
            .scheduler
            .acquire(RuntimePriority::Interactive, model_id.clone(), deadline)
            .await?;
        match self.inner.mode {
            RuntimeMode::InProcess | RuntimeMode::Auto => measure_prepared(config, prepared).await,
            RuntimeMode::Worker => {
                let worker = self.worker_until(config, model_id, deadline).await?;
                match tokio::time::timeout_at(
                    tokio::time::Instant::from_std(deadline),
                    worker.measure(uuid::Uuid::new_v4().to_string(), prepared.clone()),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => {
                        worker.terminate().await;
                        Err(runtime_error(
                            "local_runtime_measure_timeout",
                            "native worker exceeded the exact-measurement deadline",
                        ))
                    }
                }
            }
        }
    }

    async fn measure_without_admission(
        &self,
        request_id: &str,
        model_id: &str,
        config: LiteRtLmConfig,
        prepared: &PreparedConversation,
        deadline: Instant,
    ) -> Result<u32, SamplingError> {
        match self.inner.mode {
            RuntimeMode::InProcess | RuntimeMode::Auto => measure_prepared(config, prepared).await,
            RuntimeMode::Worker => {
                let worker = self
                    .worker_until(config, model_id.to_string(), deadline)
                    .await?;
                match tokio::time::timeout_at(
                    tokio::time::Instant::from_std(deadline),
                    worker.measure(request_id.to_string(), prepared.clone()),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => {
                        worker.terminate().await;
                        Err(runtime_error(
                            "local_runtime_measure_timeout",
                            "native worker exceeded the prompt-admission measurement deadline",
                        ))
                    }
                }
            }
        }
    }

    async fn context_breakdown(
        &self,
        request: &RuntimeRequest,
        exact_total: u32,
    ) -> Vec<(String, u32)> {
        let mut baseline = request.conversation.clone();
        baseline.system_message = None;
        baseline.retrieved_memory = None;
        baseline.messages = "[]".to_string();
        baseline.tools = "[]".to_string();
        baseline.current_message =
            r#"{"role":"user","content":[{"type":"text","text":""}]}"#.to_string();
        let base_tokens = self
            .measure_without_admission(
                &format!("{}:breakdown:base", request.request_id),
                &request.model_id,
                request.model_config.clone(),
                &baseline,
                request.deadline,
            )
            .await
            .unwrap_or_default();

        let variants = [
            ("system", {
                let mut value = baseline.clone();
                value.system_message = request.conversation.system_message.clone();
                value
            }),
            ("retrieved_memory", {
                let mut value = baseline.clone();
                value.retrieved_memory = request.conversation.retrieved_memory.clone();
                value
            }),
            ("history", {
                let mut value = baseline.clone();
                value.messages = request.conversation.messages.clone();
                value
            }),
            ("tool_schemas", {
                let mut value = baseline.clone();
                value.tools = request.conversation.tools.clone();
                value
            }),
            ("current_message", {
                let mut value = baseline.clone();
                value.current_message = request.conversation.current_message.clone();
                value
            }),
        ];
        let mut breakdown = Vec::with_capacity(6);
        breakdown.push(("render_framing".to_string(), base_tokens));
        for (index, (name, variant)) in variants.into_iter().enumerate() {
            let measured = self
                .measure_without_admission(
                    &format!("{}:breakdown:{index}", request.request_id),
                    &request.model_id,
                    request.model_config.clone(),
                    &variant,
                    request.deadline,
                )
                .await
                .unwrap_or(base_tokens);
            breakdown.push((name.to_string(), measured.saturating_sub(base_tokens)));
        }
        breakdown.push(("exact_rendered_total".to_string(), exact_total));
        breakdown
    }

    async fn fit_strict_context(
        &self,
        request: &mut RuntimeRequest,
        context_window: u32,
        completion_reserve: u32,
    ) -> Result<(u32, Vec<(String, ContextComponentKind)>), SamplingError> {
        let safety_margin = ContextBudgetBroker::safety_margin(context_window);
        let target_prompt_tokens = context_window
            .checked_sub(completion_reserve)
            .and_then(|tokens| tokens.checked_sub(safety_margin))
            .unwrap_or(0);
        let mut measured = self
            .measure_without_admission(
                &format!("{}:admission", request.request_id),
                &request.model_id,
                request.model_config.clone(),
                &request.conversation,
                request.deadline,
            )
            .await?;
        if measured <= target_prompt_tokens {
            return Ok((measured, Vec::new()));
        }

        let tools = serde_json::from_str::<Vec<serde_json::Value>>(&request.conversation.tools)
            .map_err(|error| {
                runtime_error(
                    "local_runtime_context_admission",
                    format!("tool schema payload is not a JSON array: {error}"),
                )
            })?;
        let mut admitted_tools = vec![true; tools.len()];
        let mut groups = atomic_history_groups(
            &request.conversation.messages,
            &request.conversation.current_message,
        )?;
        let mut candidates = if request.stage == RuntimeStage::Executor {
            Vec::new()
        } else {
            tools
                .iter()
                .enumerate()
                .map(
                    |(index, tool)| StrictEvictionCandidate::OptionalToolSchema {
                        index,
                        id: tool
                            .pointer("/function/name")
                            .and_then(serde_json::Value::as_str)
                            .map_or_else(
                                || format!("tool_schema_{index}"),
                                |name| format!("tool_schema:{name}"),
                            ),
                    },
                )
                .collect::<Vec<_>>()
        };
        candidates.extend(
            groups
                .iter()
                .enumerate()
                .filter(|(_, group)| !group.required)
                .map(|(index, group)| StrictEvictionCandidate::History {
                    index,
                    kind: group.kind,
                    id: group.id.clone(),
                }),
        );
        if request.conversation.retrieved_memory.is_some() {
            candidates.push(StrictEvictionCandidate::RetrievedMemory);
        }
        candidates.sort_by_key(StrictEvictionCandidate::sort_key);

        let mut dropped = Vec::new();
        for candidate in candidates {
            let (id, kind) = match candidate {
                StrictEvictionCandidate::OptionalToolSchema { index, id } => {
                    admitted_tools[index] = false;
                    request.conversation.tools =
                        serialize_admitted_tool_schemas(&tools, &admitted_tools)?;
                    (id, ContextComponentKind::OptionalToolSchema)
                }
                StrictEvictionCandidate::RetrievedMemory => {
                    request.conversation.retrieved_memory = None;
                    (
                        "retrieved_memory".to_string(),
                        ContextComponentKind::RetrievedMemory,
                    )
                }
                StrictEvictionCandidate::History { index, kind, id } => {
                    groups[index].admitted = false;
                    request.conversation.messages = serialize_history_groups(&groups)?;
                    (id, kind)
                }
            };
            dropped.push((id, kind));
            measured = self
                .measure_without_admission(
                    &format!("{}:admission:drop:{}", request.request_id, dropped.len()),
                    &request.model_id,
                    request.model_config.clone(),
                    &request.conversation,
                    request.deadline,
                )
                .await?;
            if measured <= target_prompt_tokens {
                break;
            }
        }
        Ok((measured, dropped))
    }

    pub async fn generate(
        &self,
        mut request: RuntimeRequest,
        event_tx: mpsc::UnboundedSender<RuntimeEvent>,
        cancel_token: CancellationToken,
    ) -> Result<LocalInferenceResult, SamplingError> {
        request.model_config = self
            .resolve_backend(
                request.model_config,
                &request.model_id,
                request.adapter.is_some(),
            )
            .await?;
        if request.stage == RuntimeStage::Executor {
            let tool_count =
                serde_json::from_str::<Vec<serde_json::Value>>(&request.conversation.tools)
                    .map_err(|error| {
                        runtime_error(
                            "local_runtime_context_admission",
                            format!("executor tool schema payload is not a JSON array: {error}"),
                        )
                    })?
                    .len();
            if tool_count > 6 {
                return Err(runtime_error(
                    "local_runtime_context_admission",
                    format!(
                        "executor stage received {tool_count} tool schemas; split the execution \
                         task so no stage receives more than 6 complete schemas"
                    ),
                ));
            }
        }
        let permit = self
            .inner
            .scheduler
            .acquire(
                request.priority,
                request.session_id.clone(),
                request.deadline,
            )
            .await?;
        {
            let mut active_cancellations = self
                .inner
                .active_cancellations
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if active_cancellations.contains_key(&request.request_id) {
                return Err(runtime_error(
                    "local_runtime_request_id",
                    format!(
                        "request ID is already active and cannot be reused: {}",
                        request.request_id
                    ),
                ));
            }
            active_cancellations.insert(request.request_id.clone(), cancel_token.clone());
        }
        let cancellation_registration = ActiveCancellationGuard {
            inner: Arc::clone(&self.inner),
            request_id: request.request_id.clone(),
        };
        let deadline_cancel = cancel_token.clone();
        let deadline = tokio::time::Instant::from_std(request.deadline);
        let deadline_cancellation = DeadlineCancellationGuard(tokio::spawn(async move {
            tokio::time::sleep_until(deadline).await;
            deadline_cancel.cancel();
        }));
        self.inner.active_requests.fetch_add(1, Ordering::AcqRel);
        let active = ActiveRequestGuard {
            inner: Arc::clone(&self.inner),
        };
        let completion_reserve = request.stage.completion_reserve(
            request.completion_reserve,
            request.model_config.max_context_tokens,
        );
        if completion_reserve != 0 {
            request.conversation.max_output_tokens = Some(completion_reserve);
        }
        let adapter_lease = if let Some(binding) = request.adapter.as_ref() {
            let descriptor = self.inner.adapters.descriptor(binding).ok_or_else(|| {
                runtime_error(
                    "local_runtime_adapter",
                    format!(
                        "requested adapter is not resident: {}",
                        binding.immutable_id()
                    ),
                )
            })?;
            let lease = self
                .inner
                .adapters
                .acquire(binding)
                .map_err(adapter_error)?;
            if self.inner.mode != RuntimeMode::Worker {
                let adapter_identity = descriptor.immutable_id();
                let adapter = LoraAdapterConfig {
                    path: descriptor.path,
                    id: adapter_identity,
                };
                request.model_config.lora_adapter = Some(adapter.clone());
                request.conversation.lora_adapter = Some(adapter);
            }
            Some(lease)
        } else {
            None
        };
        if self.inner.governor.snapshot().soft_pressure {
            self.relieve_memory_pressure().await?;
        }

        if request.model_config.context_strategy
            == crate::litert_lm::ContextOverflowStrategy::Strict
            && let Some(context_window) = request.model_config.max_context_tokens
        {
            let (measured, dropped) = self
                .fit_strict_context(&mut request, context_window, completion_reserve)
                .await?;
            if !dropped.is_empty() {
                tracing::info!(
                    target: crate::LOG_TARGET,
                    event = "local_runtime_context_evicted",
                    request_id = %request.request_id,
                    dropped = ?dropped,
                    measured_prompt_tokens = measured,
                    "evicted complete optional context components before native admission"
                );
            }
            let broker = ContextBudgetBroker;
            let plan = match broker.admit(
                context_window,
                request.stage.stage_budget(completion_reserve),
                [ContextComponent {
                    id: "rendered_prompt".to_string(),
                    kind: ContextComponentKind::CurrentUser,
                    tokens: measured,
                    required: true,
                }],
            ) {
                Ok(plan) => plan,
                Err(error) => {
                    let breakdown = self.context_breakdown(&request, measured).await;
                    return Err(runtime_error(
                        "local_runtime_context_admission",
                        format!("{error}; component_tokens={breakdown:?}"),
                    ));
                }
            };
            broker.finalize_exact(plan, measured).map_err(|error| {
                runtime_error("local_runtime_context_admission", error.to_string())
            })?;
        }

        let context_plan_hash = {
            let mut hasher = blake3::Hasher::new();
            let rendered_context = serde_json::to_vec(&request.conversation).map_err(|error| {
                runtime_error(
                    "local_runtime_context_admission",
                    format!("failed to hash admitted context: {error}"),
                )
            })?;
            for part in [
                request.model_id.as_bytes(),
                request.stage.as_str().as_bytes(),
                rendered_context.as_slice(),
            ] {
                hasher.update(&(part.len() as u64).to_le_bytes());
                hasher.update(part);
            }
            hasher.finalize().to_hex().to_string()
        };
        let admission = RuntimeAdmission {
            request_id: request.request_id.clone(),
            model_id: request.model_id.clone(),
            adapter_id: request.adapter.as_ref().map(AdapterBinding::immutable_id),
            context_plan_hash,
        };
        if let Some(hook) = request.admission_hook.as_ref() {
            hook.persist(admission, request.deadline, &cancel_token)
                .await?;
        } else {
            let _ = event_tx.send(RuntimeEvent::Admitted {
                request_id: admission.request_id,
                model_id: admission.model_id,
                adapter_id: admission.adapter_id,
                context_plan_hash: admission.context_plan_hash,
            });
        }

        let result = match self.inner.mode {
            RuntimeMode::InProcess | RuntimeMode::Auto => {
                run_prepared_request(
                    request.request_id,
                    request.conversation,
                    request.model_config,
                    request.model_id,
                    &event_tx,
                    &cancel_token,
                )
                .await
            }
            RuntimeMode::Worker => {
                let (worker_key, worker) = if request.adapter.is_some() {
                    let base_key = replica_base_key(&request.model_config, &request.model_id);
                    let worker_key = replica_worker_key(&base_key, 0);
                    let worker = self
                        .worker_until(request.model_config, request.model_id, request.deadline)
                        .await?;
                    (worker_key, worker)
                } else {
                    self.select_generation_worker(
                        request.model_config,
                        request.model_id,
                        permit.queued_for(),
                        request.deadline,
                    )
                    .await?
                };
                if let Some(binding) = request.adapter.as_ref() {
                    worker
                        .select_adapter(request.request_id.clone(), binding)
                        .await?;
                }
                let result = worker
                    .generate(
                        request.request_id,
                        request.conversation,
                        event_tx,
                        cancel_token,
                    )
                    .await;
                if let Err(error) = self.ensure_worker_reservation(&worker_key, &worker).await {
                    tracing::warn!(
                        %error,
                        worker_key,
                        active_requests = worker.active_requests(),
                        "worker residency exceeded resource admission"
                    );
                    if worker.active_requests() == 0 {
                        worker.terminate().await;
                        self.inner.workers.lock().await.remove(&worker_key);
                        self.release_worker_reservation(&worker_key).await;
                    }
                }
                result
            }
        };
        drop(adapter_lease);
        if result.is_ok() {
            self.inner
                .completed_requests
                .fetch_add(1, Ordering::Relaxed);
        }
        drop(active);
        drop(deadline_cancellation);
        drop(cancellation_registration);
        result
    }

    /// Reclaim only inactive resources, in the platform pressure order.
    ///
    /// Reranker and embedding callbacks are synchronous because those models
    /// are process-local. Adapter unloads and worker shutdowns remain
    /// supervised asynchronous operations.
    pub async fn relieve_memory_pressure(&self) -> Result<u64, SamplingError> {
        if !self.inner.governor.snapshot().soft_pressure {
            return Ok(0);
        }
        let mut released = self.inner.governor.reclaim_auxiliary_models();

        while self.inner.governor.snapshot().soft_pressure {
            let Some(binding) = self.inner.adapters.next_eviction_candidate() else {
                break;
            };
            let bytes = self
                .inner
                .adapters
                .descriptor(&binding)
                .map_or(0, |descriptor| descriptor.byte_size);
            let deployment = self
                .inner
                .adapter_deployments
                .lock()
                .await
                .get(&binding.immutable_id())
                .cloned();
            let Some(deployment) = deployment else {
                break;
            };
            self.unload_adapter(deployment.model_id, deployment.config, binding)
                .await?;
            released = released.saturating_add(bytes);
        }

        if self.inner.governor.snapshot().soft_pressure {
            if self.inner.mode == RuntimeMode::Worker {
                let workers = {
                    let workers = self.inner.workers.lock().await;
                    workers
                        .iter()
                        .filter_map(|(key, cell)| {
                            cell.get().cloned().map(|worker| (key.clone(), worker))
                        })
                        .collect::<Vec<_>>()
                };
                for (key, worker) in workers {
                    let before = self
                        .inner
                        .worker_leases
                        .lock()
                        .await
                        .get(&key)
                        .map_or(0, ResourceLease::bytes);
                    worker.drop_inactive_sessions(0).await?;
                    self.ensure_worker_reservation(&key, &worker).await?;
                    let after = self
                        .inner
                        .worker_leases
                        .lock()
                        .await
                        .get(&key)
                        .map_or(0, ResourceLease::bytes);
                    released = released.saturating_add(before.saturating_sub(after));
                }
            } else {
                let _ = drop_inactive_sessions(0);
            }
        }

        if self.inner.governor.snapshot().soft_pressure {
            let idle_replicas = {
                let workers = self.inner.workers.lock().await;
                workers
                    .iter()
                    .filter_map(|(key, cell)| {
                        let (_, replica) = split_replica_worker_key(key)?;
                        (replica > 0)
                            .then(|| cell.get().cloned().map(|worker| (key.clone(), worker)))
                            .flatten()
                    })
                    .filter(|(_, worker)| worker.active_requests() == 0)
                    .collect::<Vec<_>>()
            };
            for (key, worker) in idle_replicas {
                if !self.inner.governor.snapshot().soft_pressure {
                    break;
                }
                let bytes = self
                    .inner
                    .worker_leases
                    .lock()
                    .await
                    .get(&key)
                    .map_or(0, ResourceLease::bytes);
                worker.shutdown().await;
                self.inner.workers.lock().await.remove(&key);
                self.release_worker_reservation(&key).await;
                if let Some((base_key, replica)) = split_replica_worker_key(&key) {
                    let mut counts = self.inner.replica_counts.lock().await;
                    if counts.get(base_key).is_some_and(|count| *count > replica) {
                        counts.insert(base_key.to_string(), replica.max(1));
                    }
                }
                released = released.saturating_add(bytes);
            }
        }
        Ok(released)
    }

    /// Cancel an admitted or running request by its stable protocol ID.
    ///
    /// The same token drives the in-process native callback and the isolated
    /// worker's binary `Cancel` frame, so callers do not need backend-specific
    /// cancellation logic.
    pub fn cancel(&self, request_id: &str) -> bool {
        let active_cancellations = self
            .inner
            .active_cancellations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(cancel) = active_cancellations.get(request_id) else {
            return false;
        };
        cancel.cancel();
        true
    }

    pub async fn release_session(&self, session_id: &str) -> Result<usize, SamplingError> {
        let released = drop_session(session_id);
        if self.inner.mode == RuntimeMode::Worker {
            let workers = {
                let cache = self.inner.workers.lock().await;
                cache
                    .values()
                    .filter_map(|cell| cell.get().cloned())
                    .collect::<Vec<_>>()
            };
            for worker in workers {
                if !worker.is_dead() {
                    worker.drop_session(session_id.to_string()).await?;
                }
            }
        }
        Ok(released)
    }

    pub fn capacity(&self) -> CapacitySnapshot {
        CapacitySnapshot {
            mode: self.inner.mode,
            queue_depth: self.inner.queue_depth.load(Ordering::Acquire),
            active_requests: self.inner.active_requests.load(Ordering::Acquire),
            completed_requests: self.inner.completed_requests.load(Ordering::Acquire),
            resident_workers: self.inner.resident_workers.load(Ordering::Acquire),
            resources: self.inner.governor.snapshot(),
        }
    }

    /// Stop accepting useful work from existing workers, cancel active
    /// generations, and complete the protocol-level graceful shutdown before
    /// falling back to process termination.
    pub async fn shutdown(&self) {
        let cancellations = self
            .inner
            .active_cancellations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for cancellation in cancellations {
            cancellation.cancel();
        }

        let workers = {
            let mut cache = self.inner.workers.lock().await;
            let workers = cache
                .values()
                .filter_map(|cell| cell.get().cloned())
                .collect::<Vec<_>>();
            cache.clear();
            workers
        };
        for worker in workers {
            worker.shutdown().await;
        }
        self.inner.worker_leases.lock().await.clear();
        self.inner.adapter_load_gates.lock().await.clear();
        self.inner.backend_probe_gates.lock().await.clear();
        self.inner.backend_selections.lock().await.clear();
        self.inner.replica_counts.lock().await.clear();
        self.inner.replica_cursors.lock().await.clear();
        self.inner.resident_workers.store(0, Ordering::Release);
    }

    pub async fn worker_capacity(&self) -> Result<HashMap<String, WorkerStats>, SamplingError> {
        if self.inner.mode != RuntimeMode::Worker {
            return Ok(HashMap::new());
        }
        let workers = {
            let cache = self.inner.workers.lock().await;
            cache
                .iter()
                .filter_map(|(key, cell)| cell.get().cloned().map(|worker| (key.clone(), worker)))
                .collect::<Vec<_>>()
        };
        let mut stats = HashMap::with_capacity(workers.len());
        for (key, worker) in workers {
            if !worker.is_dead() {
                self.ensure_worker_reservation(&key, &worker).await?;
                stats.insert(key, worker.stats().await?);
            }
        }
        Ok(stats)
    }

    async fn worker(
        &self,
        config: LiteRtLmConfig,
        model_id: String,
    ) -> Result<Arc<WorkerClient>, SamplingError> {
        self.worker_at_with_timeout(config, model_id, 0, DEFAULT_MODEL_LOAD_TIMEOUT)
            .await
    }

    async fn worker_until(
        &self,
        config: LiteRtLmConfig,
        model_id: String,
        deadline: Instant,
    ) -> Result<Arc<WorkerClient>, SamplingError> {
        self.worker_at_until(config, model_id, 0, deadline).await
    }

    async fn worker_at(
        &self,
        config: LiteRtLmConfig,
        model_id: String,
        replica: usize,
    ) -> Result<Arc<WorkerClient>, SamplingError> {
        self.worker_at_with_timeout(config, model_id, replica, DEFAULT_MODEL_LOAD_TIMEOUT)
            .await
    }

    async fn worker_at_until(
        &self,
        config: LiteRtLmConfig,
        model_id: String,
        replica: usize,
        deadline: Instant,
    ) -> Result<Arc<WorkerClient>, SamplingError> {
        let now = Instant::now();
        if deadline <= now {
            return Err(runtime_error(
                "local_runtime_worker_deadline",
                "local runtime worker deadline elapsed before model load",
            ));
        }
        self.worker_at_with_timeout(config, model_id, replica, deadline.duration_since(now))
            .await
    }

    async fn worker_at_with_timeout(
        &self,
        config: LiteRtLmConfig,
        model_id: String,
        replica: usize,
        model_load_timeout: Duration,
    ) -> Result<Arc<WorkerClient>, SamplingError> {
        let base_key = replica_base_key(&config, &model_id);
        let key = replica_worker_key(&base_key, replica);
        let (cell, removed_dead) = {
            let mut workers = self.inner.workers.lock().await;
            let dead = workers
                .get(&key)
                .and_then(|cell| cell.get())
                .is_some_and(|worker| worker.is_dead());
            if dead {
                workers.remove(&key);
            }
            (
                Arc::clone(
                    workers
                        .entry(key.clone())
                        .or_insert_with(|| Arc::new(OnceCell::new())),
                ),
                dead,
            )
        };
        if removed_dead {
            self.release_worker_reservation(&key).await;
        }
        let path = self.worker_path()?;
        let worker = cell
            .get_or_try_init(|| async move {
                WorkerClient::spawn(&path, config, model_id, model_load_timeout)
                    .await
                    .map(Arc::new)
            })
            .await
            .cloned()?;
        if let Err(error) = self.ensure_worker_reservation(&key, &worker).await {
            worker.terminate().await;
            self.inner.workers.lock().await.remove(&key);
            return Err(error);
        }
        if replica == 0 {
            self.inner
                .replica_counts
                .lock()
                .await
                .entry(base_key)
                .or_insert(1);
        }
        Ok(worker)
    }

    async fn ensure_worker_reservation(
        &self,
        key: &str,
        worker: &Arc<WorkerClient>,
    ) -> Result<(), SamplingError> {
        let stats = worker.stats().await?;
        let engine_bytes = stats
            .resident_bytes
            .saturating_sub(stats.resident_adapter_bytes)
            .max(1);
        let mut leases = self.inner.worker_leases.lock().await;
        if let Some(lease) = leases.get_mut(key) {
            lease
                .resize(engine_bytes)
                .map_err(|error| runtime_error("local_runtime_memory", error.to_string()))?;
        } else {
            let lease = self
                .inner
                .governor
                .reserve(ResourceClass::Engine, engine_bytes)
                .map_err(|error| runtime_error("local_runtime_memory", error.to_string()))?;
            leases.insert(key.to_string(), lease);
            self.inner.resident_workers.fetch_add(1, Ordering::AcqRel);
        }
        Ok(())
    }

    async fn release_worker_reservation(&self, key: &str) {
        if self.inner.worker_leases.lock().await.remove(key).is_some() {
            self.inner.resident_workers.fetch_sub(1, Ordering::AcqRel);
        }
    }

    async fn select_generation_worker(
        &self,
        config: LiteRtLmConfig,
        model_id: String,
        queued_for: Duration,
        deadline: Instant,
    ) -> Result<(String, Arc<WorkerClient>), SamplingError> {
        let base_key = replica_base_key(&config, &model_id);
        if queued_for >= Duration::from_millis(100) {
            self.start_additional_replica(config.clone(), model_id.clone())
                .await;
        }
        let count = self
            .inner
            .replica_counts
            .lock()
            .await
            .get(&base_key)
            .copied()
            .unwrap_or(1)
            .clamp(1, 4);
        let replica = {
            let mut cursors = self.inner.replica_cursors.lock().await;
            let cursor = cursors.entry(base_key.clone()).or_default();
            let selected = *cursor % count;
            *cursor = cursor.saturating_add(1);
            selected
        };
        let worker_key = replica_worker_key(&base_key, replica);
        self.worker_at_until(config, model_id, replica, deadline)
            .await
            .map(|worker| (worker_key, worker))
    }

    async fn start_additional_replica(&self, config: LiteRtLmConfig, model_id: String) {
        if self.inner.governor.snapshot().soft_pressure {
            return;
        }
        let base_key = replica_base_key(&config, &model_id);
        let replica = {
            let mut counts = self.inner.replica_counts.lock().await;
            let count = counts.entry(base_key.clone()).or_insert(1);
            // A count above one proves the two-worker calibration gate passed.
            if *count < 2 || *count >= 4 {
                return;
            }
            let replica = *count;
            *count += 1;
            replica
        };
        let manager = self.clone();
        tokio::spawn(async move {
            if let Err(error) = manager.worker_at(config, model_id, replica).await {
                tracing::warn!(replica, %error, "failed to start calibrated runtime replica");
                let mut counts = manager.inner.replica_counts.lock().await;
                if counts.get(&base_key).copied() == Some(replica + 1) {
                    counts.insert(base_key, replica);
                }
            }
        });
    }

    async fn start_replica_calibration(&self, config: LiteRtLmConfig, model_id: String) {
        let base_key = replica_base_key(&config, &model_id);
        if self
            .inner
            .replica_counts
            .lock()
            .await
            .get(&base_key)
            .copied()
            .unwrap_or(1)
            > 1
        {
            return;
        }
        {
            let mut calibrating = self.inner.calibrating_models.lock().await;
            if !calibrating.insert(base_key.clone()) {
                return;
            }
        }
        let manager = self.clone();
        tokio::spawn(async move {
            if let Err(error) = manager
                .calibrate_second_replica(config, model_id, &base_key)
                .await
            {
                tracing::debug!(%error, "runtime replica calibration did not pass");
            }
            manager
                .inner
                .calibrating_models
                .lock()
                .await
                .remove(&base_key);
        });
    }

    async fn calibrate_second_replica(
        &self,
        config: LiteRtLmConfig,
        model_id: String,
        base_key: &str,
    ) -> Result<(), SamplingError> {
        let deadline = Instant::now() + DEFAULT_MODEL_LOAD_TIMEOUT;
        let _permit = self
            .inner
            .scheduler
            .acquire(RuntimePriority::Background, model_id.clone(), deadline)
            .await?;
        let primary = self.worker(config.clone(), model_id.clone()).await?;
        let path = self.worker_path()?;
        let candidate = Arc::new(
            WorkerClient::spawn(&path, config, model_id.clone(), DEFAULT_MODEL_LOAD_TIMEOUT)
                .await?,
        );
        let candidate_key = replica_worker_key(base_key, 1);
        if let Err(error) = self
            .ensure_worker_reservation(&candidate_key, &candidate)
            .await
        {
            candidate.terminate().await;
            return Err(error);
        }

        let baseline_started = Instant::now();
        let baseline_a = calibration_request(&primary, &model_id, "baseline-a").await;
        let baseline_b = calibration_request(&primary, &model_id, "baseline-b").await;
        let baseline_elapsed = baseline_started.elapsed();
        let (baseline_a, baseline_b) = match (baseline_a, baseline_b) {
            (Ok(a), Ok(b)) => (a, b),
            (Err(error), _) | (_, Err(error)) => {
                candidate.terminate().await;
                self.release_worker_reservation(&candidate_key).await;
                return Err(error);
            }
        };

        let scaled_started = Instant::now();
        let (scaled_a, scaled_b) = tokio::join!(
            calibration_request(&primary, &model_id, "scaled-a"),
            calibration_request(&candidate, &model_id, "scaled-b"),
        );
        let scaled_elapsed = scaled_started.elapsed();
        let (scaled_a, scaled_b) = match (scaled_a, scaled_b) {
            (Ok(a), Ok(b)) => (a, b),
            (Err(error), _) | (_, Err(error)) => {
                candidate.terminate().await;
                self.release_worker_reservation(&candidate_key).await;
                return Err(error);
            }
        };

        let baseline_tokens =
            u64::from(baseline_a.completion_tokens) + u64::from(baseline_b.completion_tokens);
        let scaled_tokens =
            u64::from(scaled_a.completion_tokens) + u64::from(scaled_b.completion_tokens);
        let baseline_rate = tokens_per_second(baseline_tokens, baseline_elapsed);
        let scaled_rate = tokens_per_second(scaled_tokens, scaled_elapsed);
        let baseline_ttft = baseline_a.ttft_millis.max(baseline_b.ttft_millis).max(1);
        let scaled_ttft = scaled_a.ttft_millis.max(scaled_b.ttft_millis);
        if !replica_calibration_passes(baseline_rate, scaled_rate, baseline_ttft, scaled_ttft) {
            candidate.terminate().await;
            self.release_worker_reservation(&candidate_key).await;
            return Err(runtime_error(
                "local_runtime_replica_calibration",
                format!(
                    "replica rejected: baseline_tps={baseline_rate:.2}, \
                     scaled_tps={scaled_rate:.2}, baseline_ttft_ms={baseline_ttft}, \
                     scaled_ttft_ms={scaled_ttft}"
                ),
            ));
        }

        let cell = Arc::new(OnceCell::new());
        let _ = cell.set(candidate);
        self.inner.workers.lock().await.insert(candidate_key, cell);
        self.inner
            .replica_counts
            .lock()
            .await
            .insert(base_key.to_string(), 2);
        tracing::info!(
            model_id,
            baseline_tps = baseline_rate,
            scaled_tps = scaled_rate,
            baseline_ttft_ms = baseline_ttft,
            scaled_ttft_ms = scaled_ttft,
            "enabled second local runtime replica after calibration"
        );
        Ok(())
    }

    fn worker_path(&self) -> Result<PathBuf, SamplingError> {
        if let Some(path) = self.inner.worker_path.as_ref() {
            if path.is_file() {
                return Ok(path.clone());
            }
            return Err(runtime_error(
                "local_runtime_worker",
                format!("runtime worker does not exist: {}", path.display()),
            ));
        }
        let executable = std::env::current_exe().map_err(|error| {
            runtime_error(
                "local_runtime_worker",
                format!("cannot locate current executable: {error}"),
            )
        })?;
        let worker = executable
            .parent()
            .map(|parent| parent.join("grok-local-runtime-worker"))
            .ok_or_else(|| {
                runtime_error(
                    "local_runtime_worker",
                    "current executable has no parent directory",
                )
            })?;
        if worker.is_file() {
            Ok(worker)
        } else {
            Err(runtime_error(
                "local_runtime_worker",
                format!(
                    "packaged local runtime worker is missing: {}",
                    worker.display()
                ),
            ))
        }
    }
}

fn replica_base_key(config: &LiteRtLmConfig, model_id: &str) -> String {
    format!("{}\0{model_id}", config.runtime_key())
}

fn replica_worker_key(base_key: &str, replica: usize) -> String {
    format!("{base_key}\0replica={replica}")
}

fn split_replica_worker_key(key: &str) -> Option<(&str, usize)> {
    let (base, replica) = key.rsplit_once("\0replica=")?;
    Some((base, replica.parse().ok()?))
}

fn backend_selection_key(config: &LiteRtLmConfig, model_id: &str, require_gpu: bool) -> String {
    format!(
        "{}\0{}\0{model_id}\0gpu_required={require_gpu}",
        config.library_path.display(),
        config.model_path.display()
    )
}

fn select_backend_probe(probes: &[BackendProbeSample], require_gpu: bool) -> Option<usize> {
    let gpu = probes
        .iter()
        .position(|probe| matches!(probe.config.backend.as_str(), "gpu" | "gpu_artisan"));
    if require_gpu {
        return gpu;
    }
    let cpu = probes
        .iter()
        .position(|probe| probe.config.backend == "cpu");
    match (gpu, cpu) {
        (Some(gpu), Some(cpu)) => {
            let gpu_probe = &probes[gpu];
            let cpu_probe = &probes[cpu];
            let gpu_rate = tokens_per_second(
                u64::from(gpu_probe.sample.completion_tokens),
                gpu_probe.elapsed,
            );
            let cpu_rate = tokens_per_second(
                u64::from(cpu_probe.sample.completion_tokens),
                cpu_probe.elapsed,
            );
            let gpu_ttft = gpu_probe.sample.ttft_millis.max(1);
            let cpu_ttft = cpu_probe.sample.ttft_millis.max(1);
            if gpu_rate >= cpu_rate * 0.85 && gpu_ttft as f64 <= cpu_ttft as f64 * 1.20 {
                Some(gpu)
            } else {
                Some(cpu)
            }
        }
        (Some(gpu), None) => Some(gpu),
        (None, Some(cpu)) => Some(cpu),
        (None, None) => None,
    }
}

fn tokens_per_second(tokens: u64, elapsed: Duration) -> f64 {
    let seconds = elapsed.as_secs_f64().max(0.001);
    tokens as f64 / seconds
}

fn replica_calibration_passes(
    baseline_rate: f64,
    scaled_rate: f64,
    baseline_ttft_millis: u64,
    scaled_ttft_millis: u64,
) -> bool {
    baseline_rate.is_finite()
        && scaled_rate.is_finite()
        && baseline_rate > 0.0
        && scaled_rate >= baseline_rate * 1.15
        && scaled_ttft_millis as f64 <= baseline_ttft_millis.max(1) as f64 * 1.20
}

async fn calibration_request(
    worker: &Arc<WorkerClient>,
    model_id: &str,
    lane: &str,
) -> Result<CalibrationSample, SamplingError> {
    let request_id = format!("replica-calibration-{}-{lane}", uuid::Uuid::new_v4());
    let session_id = request_id.clone();
    let prepared = calibration_conversation(session_id.clone());
    let (events, _event_rx) = mpsc::unbounded_channel();
    let result = worker
        .generate(request_id, prepared, events, CancellationToken::new())
        .await;
    let _ = worker.drop_session(session_id).await;
    calibration_sample(model_id, result?)
}

async fn in_process_calibration(
    config: LiteRtLmConfig,
    model_id: &str,
) -> Result<CalibrationSample, SamplingError> {
    let request_id = format!("backend-calibration-{}", uuid::Uuid::new_v4());
    let session_id = request_id.clone();
    let prepared = calibration_conversation(session_id.clone());
    let (events, _event_rx) = mpsc::unbounded_channel();
    let result = run_prepared_request(
        request_id,
        prepared,
        config,
        model_id.to_string(),
        &events,
        &CancellationToken::new(),
    )
    .await?;
    let _ = drop_session(&session_id);
    calibration_sample(model_id, result)
}

fn calibration_conversation(session_id: String) -> PreparedConversation {
    PreparedConversation {
        session_id: Some(session_id.clone()),
        system_message: Some(
            "This is a local throughput calibration. Respond only with a short numbered list."
                .to_string(),
        ),
        retrieved_memory: None,
        messages: "[]".to_string(),
        tools: "[]".to_string(),
        current_message: serde_json::json!({
            "role": "user",
            "content": [{
                "type": "text",
                "text": "Write the integers from 1 through 32, separated by single spaces."
            }]
        })
        .to_string(),
        max_output_tokens: Some(64),
        temperature: Some(0.0),
        top_p: Some(1.0),
        lora_adapter: None,
    }
}

fn calibration_sample(
    model_id: &str,
    result: LocalInferenceResult,
) -> Result<CalibrationSample, SamplingError> {
    match result {
        LocalInferenceResult::Completed { response, metrics } => {
            let completion_tokens = response
                .usage
                .as_ref()
                .map(|usage| usage.completion_tokens)
                .unwrap_or(metrics.chunk_count);
            if completion_tokens == 0 {
                return Err(runtime_error(
                    "local_runtime_replica_calibration",
                    format!("{model_id} produced no calibration tokens"),
                ));
            }
            Ok(CalibrationSample {
                completion_tokens,
                ttft_millis: metrics.time_to_first_token_ms.unwrap_or(u64::MAX),
            })
        }
        LocalInferenceResult::Cancelled => Err(runtime_error(
            "local_runtime_replica_calibration",
            format!("{model_id} calibration was cancelled"),
        )),
    }
}

struct ActiveRequestGuard {
    inner: Arc<RuntimeManagerInner>,
}

impl Drop for ActiveRequestGuard {
    fn drop(&mut self) {
        self.inner.active_requests.fetch_sub(1, Ordering::AcqRel);
    }
}

struct ActiveCancellationGuard {
    inner: Arc<RuntimeManagerInner>,
    request_id: String,
}

impl Drop for ActiveCancellationGuard {
    fn drop(&mut self) {
        self.inner
            .active_cancellations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.request_id);
    }
}

struct DeadlineCancellationGuard(tokio::task::JoinHandle<()>);

impl Drop for DeadlineCancellationGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

enum SchedulerCommand {
    Acquire {
        priority: RuntimePriority,
        session_id: String,
        granted: oneshot::Sender<Result<(), ()>>,
    },
    Release,
    Prune,
}

#[derive(Clone)]
struct Scheduler {
    tx: mpsc::UnboundedSender<SchedulerCommand>,
}

impl Scheduler {
    fn spawn(max_active: usize, max_queue: usize, queue_depth: Arc<AtomicUsize>) -> Self {
        // Releases must never be dropped when the admission queue is full:
        // losing one permanently reduces scheduler capacity. Queue bounds are
        // therefore enforced by the actor while lifecycle commands travel on
        // an unbounded control channel.
        let (tx, mut rx) = mpsc::unbounded_channel::<SchedulerCommand>();
        let tx_for_permits = tx.clone();
        let queue_depth_actor = Arc::clone(&queue_depth);
        tokio::spawn(async move {
            let mut state = SchedulerState::new(max_active, max_queue);
            while let Some(command) = rx.recv().await {
                match command {
                    SchedulerCommand::Acquire {
                        priority,
                        session_id,
                        granted,
                    } => {
                        state.prune_closed();
                        // `enqueue` returns an explicit queue-full result to
                        // the caller when the actor's hard bound is reached.
                        let _ = state.enqueue(priority, session_id, granted);
                    }
                    SchedulerCommand::Release => {
                        state.active = state.active.saturating_sub(1);
                    }
                    SchedulerCommand::Prune => state.prune_closed(),
                }
                state.dispatch();
                queue_depth_actor.store(state.pending, Ordering::Release);
            }
        });
        Self { tx: tx_for_permits }
    }

    async fn acquire(
        &self,
        priority: RuntimePriority,
        session_id: String,
        deadline: Instant,
    ) -> Result<SchedulerPermit, SamplingError> {
        let queued_at = Instant::now();
        let (granted_tx, granted_rx) = oneshot::channel();
        self.tx
            .send(SchedulerCommand::Acquire {
                priority,
                session_id,
                granted: granted_tx,
            })
            .map_err(|error| {
                runtime_error(
                    "local_runtime_overloaded",
                    format!("local runtime queue rejected request: {error}"),
                )
            })?;
        let now = Instant::now();
        let admission_deadline = queue_admission_deadline(priority, deadline, now);
        let remaining = admission_deadline.saturating_duration_since(now);
        match tokio::time::timeout(remaining, granted_rx).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(()))) => {
                return Err(runtime_error(
                    "local_runtime_overloaded",
                    "local runtime queue reached its configured bound",
                ));
            }
            Ok(Err(_)) => {
                return Err(runtime_error(
                    "local_runtime_scheduler",
                    "local runtime scheduler stopped",
                ));
            }
            Err(_) => {
                // Wake the actor so the now-closed waiter is removed even if
                // every active request remains busy for a long time.
                let _ = self.tx.send(SchedulerCommand::Prune);
                return Err(runtime_error(
                    "local_runtime_overloaded",
                    match priority {
                        RuntimePriority::Reviewer => {
                            "optional reviewer omitted after 500 ms of queue contention"
                        }
                        RuntimePriority::Executor | RuntimePriority::Interactive => {
                            "mandatory local runtime work rejected after 2 seconds of queue contention"
                        }
                        RuntimePriority::Prewarm | RuntimePriority::Background => {
                            "local runtime admission deadline expired"
                        }
                    },
                ));
            }
        }
        Ok(SchedulerPermit {
            tx: self.tx.clone(),
            queued_for: queued_at.elapsed(),
        })
    }
}

fn queue_admission_deadline(
    priority: RuntimePriority,
    requested: Instant,
    now: Instant,
) -> Instant {
    match priority {
        RuntimePriority::Reviewer => requested.min(now + REVIEWER_MAX_QUEUE_WAIT),
        RuntimePriority::Executor | RuntimePriority::Interactive => {
            requested.min(now + MANDATORY_MAX_QUEUE_WAIT)
        }
        RuntimePriority::Prewarm | RuntimePriority::Background => requested,
    }
}

struct SchedulerPermit {
    tx: mpsc::UnboundedSender<SchedulerCommand>,
    queued_for: Duration,
}

impl SchedulerPermit {
    fn queued_for(&self) -> Duration {
        self.queued_for
    }
}

impl Drop for SchedulerPermit {
    fn drop(&mut self) {
        let _ = self.tx.send(SchedulerCommand::Release);
    }
}

type SchedulerWaiter = oneshot::Sender<Result<(), ()>>;
type SessionWaiters = HashMap<String, VecDeque<SchedulerWaiter>>;

struct SchedulerState {
    max_active: usize,
    max_pending: usize,
    active: usize,
    pending: usize,
    queues: Vec<SessionWaiters>,
    sessions: Vec<VecDeque<String>>,
    weighted_priorities: Vec<usize>,
    priority_cursor: usize,
}

impl SchedulerState {
    fn new(max_active: usize, max_pending: usize) -> Self {
        Self {
            max_active,
            max_pending,
            active: 0,
            pending: 0,
            queues: (0..5).map(|_| HashMap::new()).collect(),
            sessions: (0..5).map(|_| VecDeque::new()).collect(),
            weighted_priorities: vec![0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 2, 2, 2, 3, 4],
            priority_cursor: 0,
        }
    }

    fn enqueue(
        &mut self,
        priority: RuntimePriority,
        session_id: String,
        granted: oneshot::Sender<Result<(), ()>>,
    ) -> bool {
        if self.pending >= self.max_pending {
            let _ = granted.send(Err(()));
            return false;
        }
        let index = priority.index();
        let queue = self.queues[index].entry(session_id.clone()).or_default();
        if queue.is_empty() {
            self.sessions[index].push_back(session_id);
        }
        queue.push_back(granted);
        self.pending = self.pending.saturating_add(1);
        true
    }

    fn prune_closed(&mut self) {
        for priority in 0..self.queues.len() {
            let mut removed = 0usize;
            self.queues[priority].retain(|_, queue| {
                let before = queue.len();
                queue.retain(|sender| !sender.is_closed());
                removed = removed.saturating_add(before.saturating_sub(queue.len()));
                !queue.is_empty()
            });
            self.pending = self.pending.saturating_sub(removed);
            let live_sessions = self.queues[priority]
                .keys()
                .cloned()
                .collect::<HashSet<_>>();
            self.sessions[priority].retain(|session| live_sessions.contains(session));
        }
    }

    fn dispatch(&mut self) {
        while self.active < self.max_active && self.pending != 0 {
            let Some(sender) = self.next_waiter() else {
                self.pending = 0;
                break;
            };
            self.pending = self.pending.saturating_sub(1);
            if sender.send(Ok(())).is_ok() {
                self.active = self.active.saturating_add(1);
            }
        }
    }

    fn next_waiter(&mut self) -> Option<oneshot::Sender<Result<(), ()>>> {
        for _ in 0..self.weighted_priorities.len() {
            let priority = self.weighted_priorities[self.priority_cursor];
            self.priority_cursor = (self.priority_cursor + 1) % self.weighted_priorities.len();
            let Some(session_id) = self.sessions[priority].pop_front() else {
                continue;
            };
            let queue = self.queues[priority]
                .get_mut(&session_id)
                .expect("scheduler session queue must exist");
            let waiter = queue.pop_front();
            if queue.is_empty() {
                self.queues[priority].remove(&session_id);
            } else {
                self.sessions[priority].push_back(session_id);
            }
            if waiter.is_some() {
                return waiter;
            }
        }
        None
    }
}

#[derive(Debug)]
struct AtomicHistoryGroup {
    id: String,
    kind: ContextComponentKind,
    required: bool,
    admitted: bool,
    messages: Vec<serde_json::Value>,
}

#[derive(Debug)]
enum StrictEvictionCandidate {
    OptionalToolSchema {
        index: usize,
        id: String,
    },
    RetrievedMemory,
    History {
        index: usize,
        kind: ContextComponentKind,
        id: String,
    },
}

impl StrictEvictionCandidate {
    fn sort_key(&self) -> (u8, usize) {
        match self {
            // Optional schemas are ranked most-to-least relevant in the
            // prepared conversation. Drop the least relevant tail first.
            Self::OptionalToolSchema { index, .. } => (0, usize::MAX.saturating_sub(*index)),
            Self::RetrievedMemory => (2, usize::MAX),
            Self::History { index, kind, .. } => {
                let priority = match kind {
                    ContextComponentKind::OlderSummary => 1,
                    ContextComponentKind::RetrievedMemory => 2,
                    ContextComponentKind::RecentTurn => 3,
                    ContextComponentKind::CurrentEvidence => 4,
                    _ => 5,
                };
                (priority, *index)
            }
        }
    }
}

fn serialize_admitted_tool_schemas(
    tools: &[serde_json::Value],
    admitted: &[bool],
) -> Result<String, SamplingError> {
    if tools.len() != admitted.len() {
        return Err(runtime_error(
            "local_runtime_context_admission",
            "tool schema admission bitmap length does not match the schema payload",
        ));
    }
    serde_json::to_string(
        &tools
            .iter()
            .zip(admitted)
            .filter(|(_, admitted)| **admitted)
            .map(|(tool, _)| tool)
            .collect::<Vec<_>>(),
    )
    .map_err(SamplingError::Serialization)
}

fn atomic_history_groups(
    messages_json: &str,
    current_message_json: &str,
) -> Result<Vec<AtomicHistoryGroup>, SamplingError> {
    let messages = serde_json::from_str::<Vec<serde_json::Value>>(messages_json)
        .map_err(SamplingError::Serialization)?;
    let current_message = serde_json::from_str::<serde_json::Value>(current_message_json)
        .map_err(SamplingError::Serialization)?;
    let current_is_continuation = current_message
        .get("role")
        .and_then(serde_json::Value::as_str)
        != Some("user");

    let mut raw_groups = Vec::<Vec<serde_json::Value>>::new();
    let mut current = Vec::new();
    for message in messages {
        let starts_turn = message.get("role").and_then(serde_json::Value::as_str) == Some("user");
        if starts_turn && !current.is_empty() {
            raw_groups.push(std::mem::take(&mut current));
        }
        current.push(message);
    }
    if !current.is_empty() {
        raw_groups.push(current);
    }
    let last_index = raw_groups.len().saturating_sub(1);
    Ok(raw_groups
        .into_iter()
        .enumerate()
        .map(|(index, messages)| {
            let rendered = serde_json::Value::Array(messages.clone()).to_string();
            let kind = if rendered.contains("<conversation_summary>")
                || rendered.contains("This session is being continued from a previous conversation")
            {
                ContextComponentKind::OlderSummary
            } else if rendered.contains("<memory-context>") {
                ContextComponentKind::RetrievedMemory
            } else if current_is_continuation && index == last_index {
                ContextComponentKind::CurrentEvidence
            } else {
                ContextComponentKind::RecentTurn
            };
            AtomicHistoryGroup {
                id: format!("history_group_{index}"),
                kind,
                // When the current message is a tool/result continuation, the
                // final history group contains the pinned execution task and
                // the tool-call IDs referenced by that current envelope.
                required: current_is_continuation && index == last_index,
                admitted: true,
                messages,
            }
        })
        .collect())
}

fn serialize_history_groups(groups: &[AtomicHistoryGroup]) -> Result<String, SamplingError> {
    let messages = groups
        .iter()
        .filter(|group| group.admitted)
        .flat_map(|group| group.messages.iter().cloned())
        .collect::<Vec<_>>();
    serde_json::to_string(&messages).map_err(SamplingError::Serialization)
}

fn runtime_error(error_type: &str, message: impl Into<String>) -> SamplingError {
    SamplingError::StreamError {
        error_type: error_type.to_string(),
        message: message.into(),
    }
}

fn adapter_error(error: crate::adapter::AdapterError) -> SamplingError {
    runtime_error("local_runtime_adapter", error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn scheduler_is_bounded_and_releases_capacity() {
        let depth = Arc::new(AtomicUsize::new(0));
        let scheduler = Scheduler::spawn(1, 2, Arc::clone(&depth));
        let first = scheduler
            .acquire(
                RuntimePriority::Interactive,
                "a".to_string(),
                Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();
        let pending_scheduler = scheduler.clone();
        let pending = tokio::spawn(async move {
            pending_scheduler
                .acquire(
                    RuntimePriority::Executor,
                    "b".to_string(),
                    Instant::now() + Duration::from_secs(1),
                )
                .await
        });
        tokio::task::yield_now().await;
        assert_eq!(depth.load(Ordering::Acquire), 1);
        drop(first);
        assert!(pending.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn scheduler_never_loses_release_when_queue_is_saturated() {
        let depth = Arc::new(AtomicUsize::new(0));
        let scheduler = Scheduler::spawn(1, 1, Arc::clone(&depth));
        let first = scheduler
            .acquire(
                RuntimePriority::Interactive,
                "active".to_string(),
                Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();
        let waiting_scheduler = scheduler.clone();
        let waiting = tokio::spawn(async move {
            waiting_scheduler
                .acquire(
                    RuntimePriority::Executor,
                    "waiting".to_string(),
                    Instant::now() + Duration::from_secs(1),
                )
                .await
        });
        while depth.load(Ordering::Acquire) != 1 {
            tokio::task::yield_now().await;
        }

        let rejected = scheduler
            .acquire(
                RuntimePriority::Reviewer,
                "overflow".to_string(),
                Instant::now() + Duration::from_secs(1),
            )
            .await;
        assert!(rejected.is_err());

        waiting.abort();
        let _ = waiting.await;
        drop(first);
        let replacement = scheduler
            .acquire(
                RuntimePriority::Interactive,
                "replacement".to_string(),
                Instant::now() + Duration::from_secs(1),
            )
            .await;
        assert!(replacement.is_ok(), "release must survive queue saturation");
    }

    #[tokio::test]
    async fn capacity_uses_configured_limits() {
        let manager = RuntimeManager::new(RuntimeManagerConfig {
            mode: RuntimeMode::InProcess,
            worker_path: None,
            queue_capacity: 4,
            max_active_requests: 1,
            memory_limits: MemoryLimits {
                physical_bytes: 1000,
                soft_bytes: 600,
                hard_bytes: 750,
            },
        });
        assert_eq!(manager.capacity().resources.limits.hard_bytes, 750);
    }

    #[test]
    fn queue_wait_boundaries_match_stage_degradation_contract() {
        let now = Instant::now();
        let requested = now + Duration::from_secs(30);
        assert_eq!(
            queue_admission_deadline(RuntimePriority::Reviewer, requested, now),
            now + Duration::from_millis(500)
        );
        assert_eq!(
            queue_admission_deadline(RuntimePriority::Executor, requested, now),
            now + Duration::from_secs(2)
        );
        assert_eq!(
            queue_admission_deadline(RuntimePriority::Background, requested, now),
            requested
        );
    }

    #[test]
    fn direct_completion_reserve_respects_the_selected_model_limit() {
        assert_eq!(
            RuntimeStage::Direct.completion_reserve(321, Some(8_192)),
            321
        );
        assert_eq!(
            RuntimeStage::Direct.completion_reserve(0, Some(8_192)),
            1_024
        );
    }

    #[test]
    fn replica_gate_requires_throughput_gain_and_ttft_bound() {
        assert!(!replica_calibration_passes(100.0, 114.9, 100, 100));
        assert!(replica_calibration_passes(100.0, 115.0, 100, 120));
        assert!(!replica_calibration_passes(100.0, 150.0, 100, 121));
    }

    #[test]
    fn apple_backend_probe_prefers_compatible_gpu_and_rejects_regression() {
        let probe = |backend: &str, elapsed_ms: u64, ttft_millis: u64| BackendProbeSample {
            config: LiteRtLmConfig {
                model_path: PathBuf::from("/tmp/model"),
                library_path: PathBuf::from("/tmp/lib"),
                backend: backend.to_string(),
                max_context_tokens: Some(4096),
                supported_lora_ranks: Vec::new(),
                adapter_descriptor: None,
                lora_adapter: None,
                max_resident_adapters: 1,
                max_resident_sessions: 1,
                max_resident_context_tokens: 4096,
                context_strategy: crate::litert_lm::ContextOverflowStrategy::Strict,
                min_recent_turns: 1,
            },
            sample: CalibrationSample {
                completion_tokens: 100,
                ttft_millis,
            },
            elapsed: Duration::from_millis(elapsed_ms),
        };
        let compatible = [probe("gpu", 90, 110), probe("cpu", 100, 100)];
        assert_eq!(select_backend_probe(&compatible, false), Some(0));
        assert_eq!(select_backend_probe(&compatible, true), Some(0));

        let regressed = [probe("gpu", 200, 150), probe("cpu", 100, 100)];
        assert_eq!(select_backend_probe(&regressed, false), Some(1));
    }

    #[test]
    fn strict_history_admission_keeps_turns_atomic_and_pins_active_tool_cycle() {
        let groups = atomic_history_groups(
            r#"[
                {"role":"user","content":"old"},
                {"role":"assistant","content":"old reply"},
                {"role":"user","content":"current task"},
                {"role":"assistant","tool_calls":[{"id":"call-1"}]}
            ]"#,
            r#"{"role":"tool","tool_call_id":"call-1","content":[]}"#,
        )
        .unwrap();
        assert_eq!(groups.len(), 2);
        assert!(!groups[0].required);
        assert_eq!(groups[0].messages.len(), 2);
        assert!(groups[1].required);
        assert_eq!(groups[1].kind, ContextComponentKind::CurrentEvidence);

        let mut groups = groups;
        groups[0].admitted = false;
        let serialized = serialize_history_groups(&groups).unwrap();
        let retained: Vec<serde_json::Value> = serde_json::from_str(&serialized).unwrap();
        assert_eq!(retained.len(), 2);
        assert_eq!(retained[0]["content"], "current task");
    }

    #[test]
    fn strict_eviction_order_is_tools_then_summary_then_memory_then_complete_turns() {
        let mut candidates = [
            StrictEvictionCandidate::History {
                index: 0,
                kind: ContextComponentKind::RecentTurn,
                id: "turn".to_string(),
            },
            StrictEvictionCandidate::RetrievedMemory,
            StrictEvictionCandidate::OptionalToolSchema {
                index: 0,
                id: "tool-a".to_string(),
            },
            StrictEvictionCandidate::OptionalToolSchema {
                index: 1,
                id: "tool-b".to_string(),
            },
            StrictEvictionCandidate::History {
                index: 1,
                kind: ContextComponentKind::OlderSummary,
                id: "summary".to_string(),
            },
        ];
        candidates.sort_by_key(StrictEvictionCandidate::sort_key);
        assert!(matches!(
            candidates[0],
            StrictEvictionCandidate::OptionalToolSchema { index: 1, .. }
        ));
        assert!(matches!(
            candidates[1],
            StrictEvictionCandidate::OptionalToolSchema { index: 0, .. }
        ));
        assert!(matches!(
            candidates[2],
            StrictEvictionCandidate::History {
                kind: ContextComponentKind::OlderSummary,
                ..
            }
        ));
        assert!(matches!(
            candidates[3],
            StrictEvictionCandidate::RetrievedMemory
        ));
        assert!(matches!(
            candidates[4],
            StrictEvictionCandidate::History {
                kind: ContextComponentKind::RecentTurn,
                ..
            }
        ));
    }

    #[test]
    fn optional_tool_schema_eviction_keeps_complete_json_objects() {
        let tools = vec![
            serde_json::json!({"function": {"name": "first", "parameters": {"type": "object"}}}),
            serde_json::json!({"function": {"name": "second", "parameters": {"type": "object"}}}),
        ];
        let serialized = serialize_admitted_tool_schemas(&tools, &[true, false]).unwrap();
        let retained: Vec<serde_json::Value> = serde_json::from_str(&serialized).unwrap();
        assert_eq!(retained, vec![tools[0].clone()]);
    }
}
