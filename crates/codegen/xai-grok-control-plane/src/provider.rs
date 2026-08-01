use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use tokio::sync::{RwLock, Semaphore};
use xai_grok_protocol::{
    CapabilityManifest, CapabilityRequirement, ExecutionMode, OperationId, PROTOCOL_VERSION,
    ProtocolError, ProtocolErrorCode, ProviderDispatch, ProviderId, RequestId, ServiceHealth,
};

type ProviderFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;
type ExecuteFunction =
    dyn Fn(ProviderDispatch) -> ProviderFuture<Result<ProviderOutput, ProtocolError>> + Send + Sync;
type CancelFunction = dyn Fn(RequestId) -> ProviderFuture<Result<(), ProtocolError>> + Send + Sync;

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum ProviderArtifactSource {
    Inline { bytes: Vec<u8> },
    File { path: PathBuf },
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ProviderArtifact {
    pub media_type: String,
    pub content: ProviderArtifactSource,
}

impl ProviderArtifact {
    pub fn inline(media_type: impl Into<String>, bytes: Vec<u8>) -> Self {
        Self {
            media_type: media_type.into(),
            content: ProviderArtifactSource::Inline { bytes },
        }
    }

    pub fn file(media_type: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        Self {
            media_type: media_type.into(),
            content: ProviderArtifactSource::File { path: path.into() },
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ProviderOutput {
    pub output: serde_json::Value,
    #[serde(default)]
    pub observations: Vec<xai_grok_protocol::EvidenceObservation>,
    #[serde(default)]
    pub artifacts: Vec<ProviderArtifact>,
}

#[async_trait]
pub trait ExecutionProvider: Send + Sync + 'static {
    fn manifest(&self) -> CapabilityManifest;

    /// Monotonically identifies the live child process or native worker owned
    /// by this provider object. A value change means any worker-local session,
    /// lease, or cache state from the previous generation is no longer valid.
    async fn worker_generation(&self) -> u64 {
        1
    }

    async fn health(&self) -> ServiceHealth {
        ServiceHealth::Ready
    }

    async fn status(&self) -> serde_json::Value {
        serde_json::Value::Null
    }

    async fn execute(&self, dispatch: ProviderDispatch) -> Result<ProviderOutput, ProtocolError>;

    async fn cancel(&self, request_id: &RequestId) -> Result<(), ProtocolError>;
}

pub struct FunctionProvider {
    manifest: CapabilityManifest,
    execute: Arc<ExecuteFunction>,
    cancel: Arc<CancelFunction>,
}

impl FunctionProvider {
    pub fn new<EF, EFu, CF, CFu>(manifest: CapabilityManifest, execute: EF, cancel: CF) -> Self
    where
        EF: Fn(ProviderDispatch) -> EFu + Send + Sync + 'static,
        EFu: Future<Output = Result<ProviderOutput, ProtocolError>> + Send + 'static,
        CF: Fn(RequestId) -> CFu + Send + Sync + 'static,
        CFu: Future<Output = Result<(), ProtocolError>> + Send + 'static,
    {
        Self {
            manifest,
            execute: Arc::new(move |dispatch| Box::pin(execute(dispatch))),
            cancel: Arc::new(move |request_id| Box::pin(cancel(request_id))),
        }
    }
}

#[async_trait]
impl ExecutionProvider for FunctionProvider {
    fn manifest(&self) -> CapabilityManifest {
        self.manifest.clone()
    }

    async fn execute(&self, dispatch: ProviderDispatch) -> Result<ProviderOutput, ProtocolError> {
        (self.execute)(dispatch).await
    }

    async fn cancel(&self, request_id: &RequestId) -> Result<(), ProtocolError> {
        (self.cancel)(request_id.clone()).await
    }
}

#[derive(Clone, Debug)]
pub struct ProviderRegistryConfig {
    pub maximum_providers: usize,
}

impl Default for ProviderRegistryConfig {
    fn default() -> Self {
        Self {
            maximum_providers: 256,
        }
    }
}

struct ProviderEntry {
    manifest: CapabilityManifest,
    provider: Arc<dyn ExecutionProvider>,
    permits: Arc<Semaphore>,
    queued: AtomicU32,
    draining: AtomicBool,
    generation: u64,
    completed: AtomicU64,
    failed: AtomicU64,
}

struct QueueDepthGuard<'a>(&'a AtomicU32);

impl Drop for QueueDepthGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct ProviderCapacity {
    pub provider_id: ProviderId,
    /// Generation of this registry entry. This changes when the provider
    /// object itself is drained, removed, and registered again.
    pub generation: u64,
    /// Generation of the executable worker behind the provider object. This
    /// also changes for transparent child-process restarts.
    pub worker_generation: u64,
    pub maximum_parallel: u32,
    pub available_permits: u32,
    pub queued: u32,
    pub queue_capacity: u32,
    pub draining: bool,
    pub completed: u64,
    pub failed: u64,
    pub status: serde_json::Value,
}

pub struct ProviderRegistry {
    config: ProviderRegistryConfig,
    entries: RwLock<BTreeMap<ProviderId, Arc<ProviderEntry>>>,
    active: RwLock<HashMap<RequestId, ProviderId>>,
    generations: RwLock<HashMap<ProviderId, u64>>,
}

impl ProviderRegistry {
    pub fn new(config: ProviderRegistryConfig) -> Self {
        Self {
            config,
            entries: RwLock::new(BTreeMap::new()),
            active: RwLock::new(HashMap::new()),
            generations: RwLock::new(HashMap::new()),
        }
    }

    pub async fn register(
        &self,
        provider: Arc<dyn ExecutionProvider>,
    ) -> Result<(u64, String), ProtocolError> {
        let manifest = provider.manifest();
        manifest.validate()?;
        if !manifest.protocol.supports(PROTOCOL_VERSION) {
            return Err(ProtocolError::new(
                ProtocolErrorCode::IncompatibleVersion,
                format!(
                    "provider {} does not support control-plane protocol {}",
                    manifest.provider_id, PROTOCOL_VERSION
                ),
            ));
        }
        let mut entries = self.entries.write().await;
        if entries.contains_key(&manifest.provider_id) {
            return Err(ProtocolError::new(
                ProtocolErrorCode::Conflict,
                format!(
                    "provider {} is already registered; drain and unregister its generation before replacement",
                    manifest.provider_id
                ),
            ));
        }
        if entries.len() >= self.config.maximum_providers {
            return Err(ProtocolError::new(
                ProtocolErrorCode::Overloaded,
                "provider registry is full",
            )
            .retryable());
        }
        let generation = {
            let mut generations = self.generations.write().await;
            let generation = generations
                .get(&manifest.provider_id)
                .copied()
                .unwrap_or(0)
                .saturating_add(1);
            generations.insert(manifest.provider_id.clone(), generation);
            generation
        };
        let manifest_hash = manifest.content_hash();
        entries.insert(
            manifest.provider_id.clone(),
            Arc::new(ProviderEntry {
                permits: Arc::new(Semaphore::new(
                    manifest.concurrency.maximum_parallel as usize,
                )),
                manifest,
                provider,
                queued: AtomicU32::new(0),
                draining: AtomicBool::new(false),
                generation,
                completed: AtomicU64::new(0),
                failed: AtomicU64::new(0),
            }),
        );
        Ok((generation, manifest_hash))
    }

    pub async fn unregister(&self, provider_id: &ProviderId, generation: u64) -> bool {
        let entry = {
            let entries = self.entries.read().await;
            let Some(entry) = entries.get(provider_id) else {
                return false;
            };
            if entry.generation != generation {
                return false;
            }
            entry.clone()
        };
        entry.draining.store(true, Ordering::Release);
        let is_active = self
            .active
            .read()
            .await
            .values()
            .any(|active_provider| active_provider == provider_id);
        let has_admitted_work = entry.queued.load(Ordering::Acquire) != 0
            || entry.permits.available_permits()
                != entry.manifest.concurrency.maximum_parallel as usize;
        if is_active || has_admitted_work {
            entry.draining.store(false, Ordering::Release);
            return false;
        }
        let mut entries = self.entries.write().await;
        if entries
            .get(provider_id)
            .is_some_and(|current| current.generation == generation && Arc::ptr_eq(current, &entry))
        {
            entries.remove(provider_id);
            true
        } else {
            false
        }
    }

    pub async fn resolve(
        &self,
        operation_id: &OperationId,
        preferred: Option<&ProviderId>,
        mode: ExecutionMode,
    ) -> Result<ProviderId, ProtocolError> {
        let entries = self.entries.read().await;
        if let Some(preferred) = preferred {
            let entry = entries.get(preferred).ok_or_else(|| {
                ProtocolError::new(
                    ProtocolErrorCode::UnknownProvider,
                    format!("unknown provider {preferred}"),
                )
            })?;
            validate_operation(&entry.manifest, operation_id, mode)?;
            return Ok(preferred.clone());
        }
        entries
            .iter()
            .find_map(|(provider_id, entry)| {
                validate_operation(&entry.manifest, operation_id, mode)
                    .is_ok()
                    .then(|| provider_id.clone())
            })
            .ok_or_else(|| {
                ProtocolError::new(
                    ProtocolErrorCode::UnsupportedOperation,
                    format!("no provider supports operation {operation_id} in {mode:?} mode"),
                )
            })
    }

    pub async fn resolve_requirement(
        &self,
        requirement: &CapabilityRequirement,
        preferred: Option<&ProviderId>,
        mode: ExecutionMode,
    ) -> Result<ProviderId, ProtocolError> {
        let entries = self.entries.read().await;
        if let Some(preferred) = preferred {
            let entry = entries.get(preferred).ok_or_else(|| {
                ProtocolError::new(
                    ProtocolErrorCode::UnknownProvider,
                    format!("unknown provider {preferred}"),
                )
            })?;
            validate_operation(&entry.manifest, &requirement.operation_id, mode)?;
            validate_features(&entry.manifest, &requirement.required_features)?;
            return Ok(preferred.clone());
        }
        entries
            .iter()
            .find_map(|(provider_id, entry)| {
                (validate_operation(&entry.manifest, &requirement.operation_id, mode).is_ok()
                    && validate_features(&entry.manifest, &requirement.required_features).is_ok())
                .then(|| provider_id.clone())
            })
            .ok_or_else(|| {
                ProtocolError::new(
                    ProtocolErrorCode::UnsupportedOperation,
                    format!(
                        "no provider satisfies operation {} with features [{}]",
                        requirement.operation_id,
                        requirement.required_features.join(", ")
                    ),
                )
            })
    }

    pub async fn dispatch(
        &self,
        mut dispatch: ProviderDispatch,
    ) -> Result<ProviderOutput, ProtocolError> {
        let provider_id = self
            .resolve_requirement(
                &dispatch.task.capability,
                Some(&dispatch.provider_id),
                dispatch.task.mode,
            )
            .await?;
        dispatch.provider_id = provider_id.clone();
        let entry = self
            .entries
            .read()
            .await
            .get(&provider_id)
            .cloned()
            .ok_or_else(|| {
                ProtocolError::new(
                    ProtocolErrorCode::UnknownProvider,
                    format!("provider {provider_id} disappeared before dispatch"),
                )
            })?;
        if entry.draining.load(Ordering::Acquire) {
            return Err(ProtocolError::new(
                ProtocolErrorCode::ServiceUnavailable,
                format!("provider {provider_id} is draining"),
            )
            .retryable());
        }
        let queue_capacity = entry.manifest.concurrency.queue_capacity;
        let queued = entry.queued.fetch_add(1, Ordering::AcqRel);
        if queued >= queue_capacity {
            entry.queued.fetch_sub(1, Ordering::AcqRel);
            return Err(ProtocolError::new(
                ProtocolErrorCode::Overloaded,
                format!("provider {provider_id} queue is full"),
            )
            .retryable());
        }
        let queue_depth_guard = QueueDepthGuard(&entry.queued);
        if entry.draining.load(Ordering::Acquire) {
            return Err(ProtocolError::new(
                ProtocolErrorCode::ServiceUnavailable,
                format!("provider {provider_id} began draining before admission"),
            )
            .retryable());
        }
        let remaining = dispatch.task.deadline_unix_ms.saturating_sub(now_unix_ms());
        if remaining == 0 {
            return Err(ProtocolError::new(
                ProtocolErrorCode::DeadlineExceeded,
                "task deadline elapsed before provider admission",
            ));
        }
        let permit = tokio::time::timeout(
            std::time::Duration::from_millis(remaining),
            entry.permits.clone().acquire_owned(),
        )
        .await
        .map_err(|_| {
            ProtocolError::new(
                ProtocolErrorCode::DeadlineExceeded,
                "task deadline elapsed in provider queue",
            )
        })?
        .map_err(|_| {
            ProtocolError::new(
                ProtocolErrorCode::ServiceUnavailable,
                "provider admission semaphore closed",
            )
        })?;
        drop(queue_depth_guard);
        self.active
            .write()
            .await
            .insert(dispatch.request_id.clone(), provider_id);
        let request_id = dispatch.request_id.clone();
        let result = entry.provider.execute(dispatch).await;
        self.active.write().await.remove(&request_id);
        drop(permit);
        if result.is_ok() {
            entry.completed.fetch_add(1, Ordering::Relaxed);
        } else {
            entry.failed.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    pub async fn cancel(&self, request_id: &RequestId) -> Result<(), ProtocolError> {
        let provider_id = self
            .active
            .read()
            .await
            .get(request_id)
            .cloned()
            .ok_or_else(|| {
                ProtocolError::new(
                    ProtocolErrorCode::NotFound,
                    format!("active request {request_id} was not found"),
                )
            })?;
        let provider = self
            .entries
            .read()
            .await
            .get(&provider_id)
            .map(|entry| entry.provider.clone())
            .ok_or_else(|| {
                ProtocolError::new(
                    ProtocolErrorCode::ServiceUnavailable,
                    format!("provider {provider_id} is unavailable"),
                )
            })?;
        provider.cancel(request_id).await
    }

    pub async fn manifests(&self) -> Vec<CapabilityManifest> {
        self.entries
            .read()
            .await
            .values()
            .map(|entry| entry.manifest.clone())
            .collect()
    }

    pub async fn generation(&self, provider_id: &ProviderId) -> Option<u64> {
        self.entries
            .read()
            .await
            .get(provider_id)
            .map(|entry| entry.generation)
    }

    pub async fn health(&self, provider_id: &ProviderId) -> Result<ServiceHealth, ProtocolError> {
        let provider = self
            .entries
            .read()
            .await
            .get(provider_id)
            .map(|entry| entry.provider.clone())
            .ok_or_else(|| {
                ProtocolError::new(
                    ProtocolErrorCode::UnknownProvider,
                    format!("unknown provider {provider_id}"),
                )
            })?;
        Ok(provider.health().await)
    }

    pub async fn capacity(&self) -> Vec<ProviderCapacity> {
        let entries = self
            .entries
            .read()
            .await
            .iter()
            .map(|(provider_id, entry)| (provider_id.clone(), entry.clone()))
            .collect::<Vec<_>>();
        futures::future::join_all(entries.into_iter().map(|(provider_id, entry)| async move {
            let (worker_generation, status) =
                tokio::join!(entry.provider.worker_generation(), entry.provider.status());
            ProviderCapacity {
                provider_id: provider_id.clone(),
                generation: entry.generation,
                worker_generation,
                maximum_parallel: entry.manifest.concurrency.maximum_parallel,
                available_permits: entry.permits.available_permits() as u32,
                queued: entry.queued.load(Ordering::Acquire),
                queue_capacity: entry.manifest.concurrency.queue_capacity,
                draining: entry.draining.load(Ordering::Acquire),
                completed: entry.completed.load(Ordering::Relaxed),
                failed: entry.failed.load(Ordering::Relaxed),
                status,
            }
        }))
        .await
    }
}

fn validate_operation(
    manifest: &CapabilityManifest,
    operation_id: &OperationId,
    mode: ExecutionMode,
) -> Result<(), ProtocolError> {
    let operation = manifest.operation(operation_id).ok_or_else(|| {
        ProtocolError::new(
            ProtocolErrorCode::UnsupportedOperation,
            format!(
                "provider {} does not support operation {operation_id}",
                manifest.provider_id
            ),
        )
    })?;
    let supported = match mode {
        ExecutionMode::Interactive => operation.interactive,
        ExecutionMode::Deferred | ExecutionMode::Detached => operation.deferred,
    };
    if !supported {
        return Err(ProtocolError::new(
            ProtocolErrorCode::UnsupportedOperation,
            format!(
                "provider {} does not support operation {operation_id} in {mode:?} mode",
                manifest.provider_id
            ),
        ));
    }
    Ok(())
}

fn validate_features(
    manifest: &CapabilityManifest,
    required_features: &[String],
) -> Result<(), ProtocolError> {
    let missing = required_features
        .iter()
        .filter(|feature| !manifest.features.contains(feature.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(ProtocolError::new(
            ProtocolErrorCode::UnsupportedOperation,
            format!(
                "provider {} is missing required features: {}",
                manifest.provider_id,
                missing.join(", ")
            ),
        ))
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::atomic::AtomicUsize;

    use tokio::sync::Barrier;
    use xai_grok_protocol::{
        ArtifactContract, CancellationSemantics, CapabilityRequirement, ConcurrencyProfile,
        EngagementId, ExecutionTask, OperationDescriptor, ProviderKind, RecoverySemantics, TaskId,
        VersionRange,
    };

    use super::*;

    struct BarrierStatusProvider {
        manifest: CapabilityManifest,
        barrier: Arc<Barrier>,
    }

    #[async_trait::async_trait]
    impl ExecutionProvider for BarrierStatusProvider {
        fn manifest(&self) -> CapabilityManifest {
            self.manifest.clone()
        }

        async fn status(&self) -> serde_json::Value {
            self.barrier.wait().await;
            serde_json::json!({"observed": true})
        }

        async fn execute(
            &self,
            _dispatch: ProviderDispatch,
        ) -> Result<ProviderOutput, ProtocolError> {
            Ok(ProviderOutput::default())
        }

        async fn cancel(&self, _request_id: &RequestId) -> Result<(), ProtocolError> {
            Ok(())
        }
    }

    fn manifest() -> CapabilityManifest {
        CapabilityManifest {
            provider_id: "echo".into(),
            provider_version: "1".to_owned(),
            protocol: VersionRange::exact(PROTOCOL_VERSION),
            kind: ProviderKind::NativeExecution,
            features: BTreeSet::new(),
            operations: vec![OperationDescriptor {
                operation_id: "echo".into(),
                display_name: "Echo".to_owned(),
                input_schema: serde_json::json!({"type": "object"}),
                output_schema: serde_json::json!({"type": "object"}),
                streaming: false,
                interactive: true,
                deferred: true,
            }],
            concurrency: ConcurrencyProfile {
                maximum_parallel: 1,
                queue_capacity: 1,
                exclusive_resource: None,
            },
            cancellation: CancellationSemantics::Cooperative,
            recovery: RecoverySemantics::Restartable,
            artifacts: ArtifactContract::Optional,
            platforms: BTreeSet::new(),
            metadata: serde_json::Map::new(),
        }
    }

    #[tokio::test]
    async fn registry_dispatches_by_capability() {
        let registry = ProviderRegistry::new(ProviderRegistryConfig::default());
        registry
            .register(Arc::new(FunctionProvider::new(
                manifest(),
                |dispatch| async move {
                    Ok(ProviderOutput {
                        output: dispatch.task.input,
                        ..ProviderOutput::default()
                    })
                },
                |_| async { Ok(()) },
            )))
            .await
            .unwrap();
        let output = registry
            .dispatch(ProviderDispatch {
                request_id: RequestId::new(),
                engagement_id: EngagementId::new(),
                plan_revision: 1,
                task: ExecutionTask {
                    task_id: TaskId::new(),
                    objective: "echo".to_owned(),
                    mode: ExecutionMode::Interactive,
                    capability: CapabilityRequirement {
                        operation_id: "echo".into(),
                        preferred_provider: None,
                        required_features: Vec::new(),
                    },
                    input: serde_json::json!({"ok": true}),
                    deadline_unix_ms: now_unix_ms() + 1_000,
                    completion_tests: Vec::new(),
                    depends_on: Vec::new(),
                },
                provider_id: "echo".into(),
                lease_epoch: 1,
            })
            .await
            .unwrap();
        assert_eq!(output.output, serde_json::json!({"ok": true}));
    }

    #[tokio::test]
    async fn capacity_queries_provider_status_concurrently() {
        let registry = ProviderRegistry::new(ProviderRegistryConfig::default());
        let barrier = Arc::new(Barrier::new(3));
        for index in 0..3 {
            let mut provider_manifest = manifest();
            provider_manifest.provider_id = format!("status-{index}").into();
            registry
                .register(Arc::new(BarrierStatusProvider {
                    manifest: provider_manifest,
                    barrier: Arc::clone(&barrier),
                }))
                .await
                .unwrap();
        }

        let capacity = tokio::time::timeout(std::time::Duration::from_secs(1), registry.capacity())
            .await
            .expect("all provider status futures must be polled together");
        assert_eq!(capacity.len(), 3);
        assert!(
            capacity
                .iter()
                .all(|provider| provider.status["observed"] == true)
        );
    }

    #[tokio::test]
    async fn registry_rejects_missing_required_features() {
        let registry = ProviderRegistry::new(ProviderRegistryConfig::default());
        registry
            .register(Arc::new(FunctionProvider::new(
                manifest(),
                |_| async { Ok(ProviderOutput::default()) },
                |_| async { Ok(()) },
            )))
            .await
            .unwrap();
        let error = registry
            .resolve_requirement(
                &CapabilityRequirement {
                    operation_id: "echo".into(),
                    preferred_provider: None,
                    required_features: vec!["stream_identity".to_owned()],
                },
                None,
                ExecutionMode::Interactive,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, ProtocolErrorCode::UnsupportedOperation);
    }

    #[tokio::test]
    async fn timed_out_queue_wait_releases_depth_accounting() {
        let registry = Arc::new(ProviderRegistry::new(ProviderRegistryConfig::default()));
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let active = Arc::new(AtomicUsize::new(0));
        registry
            .register(Arc::new(FunctionProvider::new(
                manifest(),
                {
                    let started = started.clone();
                    let release = release.clone();
                    let active = active.clone();
                    move |_| {
                        let started = started.clone();
                        let release = release.clone();
                        let active = active.clone();
                        async move {
                            active.fetch_add(1, Ordering::SeqCst);
                            started.notify_one();
                            release.notified().await;
                            active.fetch_sub(1, Ordering::SeqCst);
                            Ok(ProviderOutput::default())
                        }
                    }
                },
                |_| async { Ok(()) },
            )))
            .await
            .unwrap();
        let make_dispatch = |deadline_unix_ms| ProviderDispatch {
            request_id: RequestId::new(),
            engagement_id: EngagementId::new(),
            plan_revision: 1,
            task: ExecutionTask {
                task_id: TaskId::new(),
                objective: "echo".to_owned(),
                mode: ExecutionMode::Interactive,
                capability: CapabilityRequirement {
                    operation_id: "echo".into(),
                    preferred_provider: None,
                    required_features: Vec::new(),
                },
                input: serde_json::json!({}),
                deadline_unix_ms,
                completion_tests: Vec::new(),
                depends_on: Vec::new(),
            },
            provider_id: "echo".into(),
            lease_epoch: 1,
        };
        let first_registry = registry.clone();
        let first = tokio::spawn(async move {
            first_registry
                .dispatch(make_dispatch(now_unix_ms() + 5_000))
                .await
        });
        started.notified().await;
        assert_eq!(active.load(Ordering::SeqCst), 1);
        assert!(!registry.unregister(&"echo".into(), 1).await);
        let error = registry
            .dispatch(make_dispatch(now_unix_ms() + 10))
            .await
            .unwrap_err();
        assert_eq!(error.code, ProtocolErrorCode::DeadlineExceeded);
        assert_eq!(registry.capacity().await[0].queued, 0);
        release.notify_one();
        first.await.unwrap().unwrap();
        assert!(registry.unregister(&"echo".into(), 1).await);
    }
}
