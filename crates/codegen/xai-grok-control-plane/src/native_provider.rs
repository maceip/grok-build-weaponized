use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use tokio::sync::{Notify, RwLock};
use xai_grok_native_execution::{
    CommandRequest, JobLifecycle, JobSnapshot, NativeExecutionError, NativeExecutionLimits,
    NativeExecutionSupervisor, NmapRequest,
};
use xai_grok_protocol::{
    ArtifactContract, CancellationSemantics, CapabilityManifest, ConcurrencyProfile,
    EvidenceObservation, OperationDescriptor, PROTOCOL_VERSION, Platform, ProtocolError,
    ProtocolErrorCode, ProviderDispatch, ProviderKind, RecoverySemantics, RequestId, ServiceHealth,
    VersionRange,
};

use crate::provider::{
    ExecutionProvider, ProviderArtifact, ProviderOutput, ProviderTerminalStatus,
};

pub const NATIVE_PROVIDER_ID: &str = "native-execution";

struct RequestJobBinding {
    job_id: RwLock<Option<String>>,
    closed: AtomicBool,
    changed: Notify,
}

impl RequestJobBinding {
    fn pending() -> Self {
        Self {
            job_id: RwLock::new(None),
            closed: AtomicBool::new(false),
            changed: Notify::new(),
        }
    }

    async fn bind(&self, job_id: String) {
        *self.job_id.write().await = Some(job_id);
        self.changed.notify_waiters();
    }

    async fn job_id(&self) -> Option<String> {
        loop {
            // Register before inspecting state so a bind/close between the
            // inspection and await cannot strand cancellation forever.
            let changed = self.changed.notified();
            if let Some(job_id) = self.job_id.read().await.clone() {
                return Some(job_id);
            }
            if self.closed.load(Ordering::Acquire) {
                return None;
            }
            changed.await;
        }
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }
}

pub struct NativeExecutionProvider {
    supervisor: Arc<NativeExecutionSupervisor>,
    request_jobs: RwLock<HashMap<RequestId, Arc<RequestJobBinding>>>,
    request_changes: Notify,
    maximum_parallel: u32,
    queue_capacity: u32,
}

impl NativeExecutionProvider {
    pub async fn open(root: PathBuf) -> Result<Arc<Self>, NativeExecutionError> {
        Self::open_with_limits(
            root,
            NativeExecutionLimits {
                maximum_parallel: 100,
                maximum_jobs: 10_000,
                maximum_spool_bytes_per_job: 4 * 1024 * 1024 * 1024,
                maximum_spool_bytes_per_owner: 16 * 1024 * 1024 * 1024,
                maximum_total_spool_bytes: 128 * 1024 * 1024 * 1024,
            },
        )
        .await
    }

    pub async fn open_with_limits(
        root: PathBuf,
        limits: NativeExecutionLimits,
    ) -> Result<Arc<Self>, NativeExecutionError> {
        let maximum_parallel = u32::try_from(limits.maximum_parallel.max(1)).unwrap_or(u32::MAX);
        let queue_capacity = u32::try_from(limits.maximum_jobs.max(1)).unwrap_or(u32::MAX);
        Ok(Arc::new(Self {
            supervisor: NativeExecutionSupervisor::open_with_limits(root, limits).await?,
            request_jobs: RwLock::new(HashMap::new()),
            request_changes: Notify::new(),
            maximum_parallel,
            queue_capacity,
        }))
    }

    pub fn capability_manifest() -> CapabilityManifest {
        Self::capability_manifest_with_capacity(100, 10_000)
    }

    fn capability_manifest_with_capacity(
        maximum_parallel: u32,
        queue_capacity: u32,
    ) -> CapabilityManifest {
        let operation = |id: &str, name: &str, streaming: bool| OperationDescriptor {
            operation_id: id.into(),
            display_name: name.to_owned(),
            input_schema: serde_json::json!({"type":"object","additionalProperties":true}),
            output_schema: serde_json::json!({"type":"object","additionalProperties":true}),
            streaming,
            interactive: true,
            deferred: true,
        };
        let operations = vec![
            operation("native.command.start", "Start native command", true),
            operation("native.command.status", "Read native command status", false),
            operation("native.command.list", "List native commands", false),
            operation(
                "native.command.metadata",
                "Update native command metadata",
                false,
            ),
            operation("native.command.wait", "Wait for native command", true),
            operation("native.command.output", "Read native command output", true),
            operation("native.command.stdin", "Write native command stdin", true),
            operation("native.command.cancel", "Cancel native command", false),
            operation(
                "native.command.cleanup",
                "Delete terminal command state",
                false,
            ),
            operation("native.nmap.start", "Start typed Nmap scan", true),
            operation("native.nmap.status", "Read Nmap status", false),
            operation("native.nmap.result", "Read normalized Nmap result", true),
            operation("native.nmap.cancel", "Cancel Nmap scan", false),
            operation("native.nmap.cleanup", "Delete terminal Nmap state", false),
        ];
        let mut platforms = BTreeSet::new();
        platforms.insert(Platform {
            os: std::env::consts::OS.to_owned(),
            architecture: std::env::consts::ARCH.to_owned(),
            accelerator: None,
        });
        CapabilityManifest {
            provider_id: NATIVE_PROVIDER_ID.into(),
            provider_version: env!("CARGO_PKG_VERSION").to_owned(),
            protocol: VersionRange::exact(PROTOCOL_VERSION),
            kind: ProviderKind::NativeExecution,
            features: [
                "bounded_spool",
                "cursor_artifacts",
                "durable_job_state",
                "engagement_spool_budget",
                "global_spool_budget",
                "nmap_xml",
                "process_tree_cancellation",
                "stream_identity",
                "stream_sequence",
                "streaming_stdin",
                "terminal_cleanup",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
            operations,
            concurrency: ConcurrencyProfile {
                maximum_parallel,
                queue_capacity,
                exclusive_resource: None,
            },
            cancellation: CancellationSemantics::ProcessTree,
            recovery: RecoverySemantics::Restartable,
            artifacts: ArtifactContract::CursorStream,
            platforms,
            metadata: [
                ("shell_interpolation".to_owned(), false.into()),
                ("typed_nmap_scope".to_owned(), true.into()),
            ]
            .into_iter()
            .collect(),
        }
    }

    async fn register_request(
        &self,
        request_id: RequestId,
    ) -> Result<Arc<RequestJobBinding>, ProtocolError> {
        let binding = Arc::new(RequestJobBinding::pending());
        let mut requests = self.request_jobs.write().await;
        if requests.contains_key(&request_id) {
            return Err(ProtocolError::new(
                ProtocolErrorCode::Conflict,
                format!("native request {request_id} is already active"),
            ));
        }
        requests.insert(request_id, binding.clone());
        self.request_changes.notify_waiters();
        Ok(binding)
    }

    async fn release_request(&self, request_id: &RequestId, binding: &Arc<RequestJobBinding>) {
        binding.close();
        let mut requests = self.request_jobs.write().await;
        if requests
            .get(request_id)
            .is_some_and(|current| Arc::ptr_eq(current, binding))
        {
            requests.remove(request_id);
            self.request_changes.notify_waiters();
        }
    }

    async fn wait_until_terminal(
        &self,
        job_id: &str,
        deadline_unix_ms: u64,
    ) -> Result<(JobSnapshot, bool), ProtocolError> {
        loop {
            let snapshot = self
                .supervisor
                .snapshot(job_id)
                .await
                .map_err(native_error)?;
            if snapshot.lifecycle.is_terminal() {
                return Ok((snapshot, false));
            }
            let now = now_unix_ms();
            if now >= deadline_unix_ms {
                let snapshot = self.supervisor.cancel(job_id).await.map_err(native_error)?;
                return Ok((snapshot, true));
            }
            tokio::time::sleep(Duration::from_millis(
                deadline_unix_ms.saturating_sub(now).min(50),
            ))
            .await;
        }
    }

    async fn terminal_artifacts(
        &self,
        job_id: &str,
    ) -> Result<Vec<ProviderArtifact>, ProtocolError> {
        Ok(self
            .supervisor
            .artifacts(job_id)
            .await
            .map_err(native_error)?
            .into_iter()
            .map(|artifact| ProviderArtifact::file(artifact.media_type, artifact.path))
            .collect())
    }

    async fn execute_inner(
        &self,
        dispatch: ProviderDispatch,
        binding: Option<&Arc<RequestJobBinding>>,
    ) -> Result<ProviderOutput, ProtocolError> {
        let operation = dispatch.task.capability.operation_id.as_str();
        let mode = dispatch.task.mode;
        let deadline_unix_ms = dispatch.task.deadline_unix_ms;
        let owner_id = dispatch.engagement_id.to_string();
        let input = dispatch.task.input;
        let mut artifacts = Vec::new();
        let mut terminal_status = None;
        let mut task_deadline_exceeded = false;
        let output = match operation {
            "native.command.start" => {
                let input: StartCommandInput = parse(input)?;
                let mut snapshot = self
                    .supervisor
                    .start_command_for(input.owner_id.unwrap_or(owner_id), input.request)
                    .await
                    .map_err(native_error)?;
                binding
                    .expect("start operations register a request binding")
                    .bind(snapshot.job_id.clone())
                    .await;
                if mode == xai_grok_protocol::ExecutionMode::Detached {
                    let waited = self
                        .wait_until_terminal(&snapshot.job_id, deadline_unix_ms)
                        .await?;
                    snapshot = waited.0;
                    task_deadline_exceeded = waited.1;
                    terminal_status = if task_deadline_exceeded {
                        Some(ProviderTerminalStatus::Failed)
                    } else {
                        native_terminal_status(snapshot.lifecycle)
                    };
                    artifacts.extend(self.terminal_artifacts(&snapshot.job_id).await?);
                }
                snapshot_output(snapshot, task_deadline_exceeded)?
            }
            "native.nmap.start" => {
                let request: NmapRequest = parse(input)?;
                let mut snapshot = self
                    .supervisor
                    .start_nmap_for(owner_id, request)
                    .await
                    .map_err(native_error)?;
                binding
                    .expect("start operations register a request binding")
                    .bind(snapshot.job_id.clone())
                    .await;
                if mode == xai_grok_protocol::ExecutionMode::Detached {
                    let waited = self
                        .wait_until_terminal(&snapshot.job_id, deadline_unix_ms)
                        .await?;
                    snapshot = waited.0;
                    task_deadline_exceeded = waited.1;
                    terminal_status = if task_deadline_exceeded {
                        Some(ProviderTerminalStatus::Failed)
                    } else {
                        native_terminal_status(snapshot.lifecycle)
                    };
                    artifacts.extend(self.terminal_artifacts(&snapshot.job_id).await?);
                }
                snapshot_output(snapshot, task_deadline_exceeded)?
            }
            "native.command.status" => {
                let input: JobInput = parse(input)?;
                serde_json::to_value(
                    self.supervisor
                        .snapshot(&input.job_id)
                        .await
                        .map_err(native_error)?,
                )
                .map_err(internal_error)?
            }
            "native.nmap.status" => {
                let input: JobInput = parse(input)?;
                serde_json::to_value(
                    self.supervisor
                        .nmap_snapshot(&input.job_id)
                        .await
                        .map_err(native_error)?,
                )
                .map_err(internal_error)?
            }
            "native.command.list" => {
                let input: ListInput = parse(input)?;
                serde_json::to_value(
                    self.supervisor
                        .list(input.owner_id.as_deref(), input.cursor, input.limit)
                        .await,
                )
                .map_err(internal_error)?
            }
            "native.command.metadata" => {
                let input: MetadataInput = parse(input)?;
                serde_json::to_value(
                    self.supervisor
                        .update_metadata(&input.job_id, input.metadata)
                        .await
                        .map_err(native_error)?,
                )
                .map_err(internal_error)?
            }
            "native.command.wait" => {
                let input: WaitInput = parse(input)?;
                binding
                    .expect("wait operations register a request binding")
                    .bind(input.job_id.clone())
                    .await;
                let snapshot = self
                    .supervisor
                    .wait(
                        &input.job_id,
                        Duration::from_millis(input.wait_ms.min(30_000)),
                    )
                    .await
                    .map_err(native_error)?;
                if snapshot.lifecycle.is_terminal() {
                    artifacts.extend(self.terminal_artifacts(&input.job_id).await?);
                }
                serde_json::to_value(snapshot).map_err(internal_error)?
            }
            "native.command.output" => {
                let input: OutputInput = parse(input)?;
                serde_json::to_value(
                    self.supervisor
                        .output_page(
                            &input.job_id,
                            input.cursor,
                            input.maximum_records,
                            input.maximum_bytes,
                        )
                        .await
                        .map_err(native_error)?,
                )
                .map_err(internal_error)?
            }
            "native.command.stdin" => {
                let input: StdinInput = parse(input)?;
                serde_json::to_value(
                    self.supervisor
                        .write_stdin(&input.job_id, &input.bytes, input.close)
                        .await
                        .map_err(native_error)?,
                )
                .map_err(internal_error)?
            }
            "native.command.cancel" | "native.nmap.cancel" => {
                let input: JobInput = parse(input)?;
                serde_json::to_value(
                    self.supervisor
                        .cancel(&input.job_id)
                        .await
                        .map_err(native_error)?,
                )
                .map_err(internal_error)?
            }
            "native.command.cleanup" | "native.nmap.cleanup" => {
                let input: JobInput = parse(input)?;
                serde_json::to_value(
                    self.supervisor
                        .cleanup(&input.job_id)
                        .await
                        .map_err(native_error)?,
                )
                .map_err(internal_error)?
            }
            "native.nmap.result" => {
                let input: JobInput = parse(input)?;
                let result = self
                    .supervisor
                    .nmap_result(&input.job_id)
                    .await
                    .map_err(native_error)?;
                let observations = result
                    .findings
                    .iter()
                    .map(|finding| EvidenceObservation {
                        finding: format!(
                            "{} {}/{} is open{}",
                            finding.address.as_deref().unwrap_or(result.target.as_str()),
                            finding.port,
                            finding.protocol,
                            finding
                                .banner
                                .as_deref()
                                .map(|banner| format!(": {banner}"))
                                .unwrap_or_default()
                        ),
                        confidence: 1.0,
                        artifact_id: None,
                        attributes: [
                            ("port".to_owned(), finding.port.into()),
                            ("protocol".to_owned(), finding.protocol.clone().into()),
                            ("state".to_owned(), finding.state.clone().into()),
                            (
                                "service".to_owned(),
                                finding
                                    .service
                                    .clone()
                                    .map_or(serde_json::Value::Null, Into::into),
                            ),
                        ]
                        .into_iter()
                        .collect(),
                    })
                    .collect();
                let artifacts = self.terminal_artifacts(&input.job_id).await?;
                return Ok(ProviderOutput {
                    output: serde_json::to_value(result).map_err(internal_error)?,
                    observations,
                    artifacts,
                    terminal_status: None,
                });
            }
            other => {
                return Err(ProtocolError::new(
                    ProtocolErrorCode::UnsupportedOperation,
                    format!("native provider does not implement {other}"),
                ));
            }
        };
        Ok(ProviderOutput {
            output,
            observations: Vec::new(),
            artifacts,
            terminal_status,
        })
    }
}

#[async_trait]
impl ExecutionProvider for NativeExecutionProvider {
    fn manifest(&self) -> CapabilityManifest {
        Self::capability_manifest_with_capacity(self.maximum_parallel, self.queue_capacity)
    }

    async fn health(&self) -> ServiceHealth {
        ServiceHealth::Ready
    }

    async fn status(&self) -> serde_json::Value {
        serde_json::to_value(self.supervisor.capacity().await)
            .unwrap_or_else(|error| serde_json::json!({"error":error.to_string()}))
    }

    async fn execute(&self, dispatch: ProviderDispatch) -> Result<ProviderOutput, ProtocolError> {
        let request_id = dispatch.request_id.clone();
        let operation = dispatch.task.capability.operation_id.as_str();
        let owns_job = matches!(
            operation,
            "native.command.start" | "native.nmap.start" | "native.command.wait"
        );
        let binding = if owns_job {
            Some(self.register_request(request_id.clone()).await?)
        } else {
            None
        };
        let result = self.execute_inner(dispatch, binding.as_ref()).await;
        if let Some(binding) = &binding {
            self.release_request(&request_id, binding).await;
        }
        result
    }

    async fn cancel(&self, request_id: &RequestId) -> Result<(), ProtocolError> {
        let binding = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let changed = self.request_changes.notified();
                if let Some(binding) = self.request_jobs.read().await.get(request_id).cloned() {
                    return binding;
                }
                changed.await;
            }
        })
        .await
        .map_err(|_| {
            ProtocolError::new(
                ProtocolErrorCode::NotFound,
                format!("active native request {request_id} was not found"),
            )
        })?;
        let job_id = tokio::time::timeout(Duration::from_secs(5), binding.job_id())
            .await
            .map_err(|_| {
                ProtocolError::new(
                    ProtocolErrorCode::ServiceUnavailable,
                    format!("native request {request_id} did not expose its job before timeout"),
                )
                .retryable()
            })?
            .ok_or_else(|| {
                ProtocolError::new(
                    ProtocolErrorCode::NotFound,
                    format!("native request {request_id} completed before cancellation"),
                )
            })?;
        self.supervisor
            .cancel(&job_id)
            .await
            .map_err(native_error)?;
        Ok(())
    }
}

#[derive(Deserialize)]
struct JobInput {
    job_id: String,
}

#[derive(Deserialize)]
struct StartCommandInput {
    #[serde(flatten)]
    request: CommandRequest,
    #[serde(default)]
    owner_id: Option<String>,
}

#[derive(Deserialize)]
struct ListInput {
    #[serde(default)]
    owner_id: Option<String>,
    #[serde(default)]
    cursor: usize,
    #[serde(default = "default_list_limit")]
    limit: usize,
}

#[derive(Deserialize)]
struct MetadataInput {
    job_id: String,
    #[serde(default)]
    metadata: std::collections::BTreeMap<String, serde_json::Value>,
}

fn default_list_limit() -> usize {
    128
}

#[derive(Deserialize)]
struct WaitInput {
    job_id: String,
    #[serde(default = "default_wait_ms")]
    wait_ms: u64,
}

fn default_wait_ms() -> u64 {
    1_000
}

#[derive(Deserialize)]
struct OutputInput {
    job_id: String,
    #[serde(default)]
    cursor: u64,
    #[serde(default = "default_records")]
    maximum_records: usize,
    #[serde(default = "default_bytes")]
    maximum_bytes: usize,
}

#[derive(Deserialize)]
struct StdinInput {
    job_id: String,
    #[serde(default)]
    bytes: Vec<u8>,
    #[serde(default)]
    close: bool,
}

fn default_records() -> usize {
    128
}

fn default_bytes() -> usize {
    1024 * 1024
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn native_terminal_status(lifecycle: JobLifecycle) -> Option<ProviderTerminalStatus> {
    match lifecycle {
        JobLifecycle::Completed => Some(ProviderTerminalStatus::Completed),
        JobLifecycle::Failed | JobLifecycle::TimedOut => Some(ProviderTerminalStatus::Failed),
        JobLifecycle::Cancelled => Some(ProviderTerminalStatus::Cancelled),
        JobLifecycle::Lost => Some(ProviderTerminalStatus::Lost),
        JobLifecycle::Queued | JobLifecycle::Running => None,
    }
}

fn snapshot_output(
    snapshot: JobSnapshot,
    task_deadline_exceeded: bool,
) -> Result<serde_json::Value, ProtocolError> {
    let mut value = serde_json::to_value(snapshot).map_err(internal_error)?;
    if task_deadline_exceeded && let Some(object) = value.as_object_mut() {
        object.insert("task_deadline_exceeded".to_owned(), true.into());
    }
    Ok(value)
}

fn parse<T: serde::de::DeserializeOwned>(value: serde_json::Value) -> Result<T, ProtocolError> {
    serde_json::from_value(value).map_err(|error| {
        ProtocolError::new(
            ProtocolErrorCode::InvalidEnvelope,
            format!("invalid native provider input: {error}"),
        )
    })
}

fn native_error(error: NativeExecutionError) -> ProtocolError {
    match error {
        NativeExecutionError::NotFound(_) => {
            ProtocolError::new(ProtocolErrorCode::NotFound, error.to_string())
        }
        NativeExecutionError::Overloaded => {
            ProtocolError::new(ProtocolErrorCode::Overloaded, error.to_string()).retryable()
        }
        NativeExecutionError::InvalidRequest(_)
        | NativeExecutionError::OutOfScope(_)
        | NativeExecutionError::NotComplete
        | NativeExecutionError::NotNmap => {
            ProtocolError::new(ProtocolErrorCode::InvalidEnvelope, error.to_string())
        }
        NativeExecutionError::Io(_) | NativeExecutionError::InvalidOutput(_) => {
            internal_error(error)
        }
    }
}

fn internal_error(error: impl std::fmt::Display) -> ProtocolError {
    ProtocolError::new(ProtocolErrorCode::Internal, error.to_string())
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use xai_grok_protocol::{
        CapabilityRequirement, EngagementId, ExecutionMode, ExecutionTask, ProviderId, TaskId,
    };

    use super::*;

    fn dispatch(operation: &str, input: serde_json::Value) -> ProviderDispatch {
        dispatch_with_mode(operation, input, ExecutionMode::Deferred)
    }

    fn dispatch_with_mode(
        operation: &str,
        input: serde_json::Value,
        mode: ExecutionMode,
    ) -> ProviderDispatch {
        ProviderDispatch {
            request_id: RequestId::new(),
            engagement_id: EngagementId::new(),
            plan_revision: 1,
            task: ExecutionTask {
                task_id: TaskId::new(),
                objective: "native provider integration test".to_owned(),
                mode,
                capability: CapabilityRequirement {
                    operation_id: operation.into(),
                    preferred_provider: Some(ProviderId::from_string(NATIVE_PROVIDER_ID)),
                    required_features: Vec::new(),
                },
                input,
                deadline_unix_ms: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64
                    + 60_000,
                completion_tests: Vec::new(),
                depends_on: Vec::new(),
            },
            provider_id: ProviderId::from_string(NATIVE_PROVIDER_ID),
            lease_epoch: 1,
        }
    }

    async fn wait_for_job(provider: &NativeExecutionProvider) -> JobSnapshot {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Some(snapshot) = provider.supervisor.list(None, 0, 1).await.pop() {
                    return snapshot;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("real native job did not enter the supervisor")
    }

    #[tokio::test]
    async fn provider_runs_and_pages_a_real_background_command() {
        let directory = tempfile::tempdir().unwrap();
        let provider = NativeExecutionProvider::open(directory.path().to_path_buf())
            .await
            .unwrap();
        provider.manifest().validate().unwrap();
        let started = provider
            .execute(dispatch(
                "native.command.start",
                serde_json::json!({
                    "executable": "/bin/sh",
                    "args": ["-c", "printf provider-stdout; printf provider-stderr >&2"],
                    "timeout_ms": 5000,
                    "metadata": {"is_backgrounded": false}
                }),
            ))
            .await
            .unwrap();
        let job_id = started.output["job_id"].as_str().unwrap().to_owned();
        let status = provider.status().await;
        assert_eq!(status["jobs"], 1);
        assert_eq!(status["maximum_parallel"], 100);
        assert!(status["spool"]["used_bytes"].as_u64().is_some());
        let waited = provider
            .execute(dispatch(
                "native.command.wait",
                serde_json::json!({"job_id": job_id, "wait_ms": 5000}),
            ))
            .await
            .unwrap();
        assert_eq!(waited.output["lifecycle"], "completed");
        assert_eq!(waited.artifacts.len(), 1);
        let crate::provider::ProviderArtifactSource::File { path } = &waited.artifacts[0].content
        else {
            panic!("native output must be exposed as a daemon-local file artifact");
        };
        assert!(path.is_file());
        let page = provider
            .execute(dispatch(
                "native.command.output",
                serde_json::json!({
                    "job_id": job_id,
                    "cursor": 0,
                    "maximum_records": 10,
                    "maximum_bytes": 1024
                }),
            ))
            .await
            .unwrap();
        let records = page.output["records"].as_array().unwrap();
        assert_eq!(records.len(), 2);
        assert!(page.output["end_cursor"].as_u64().unwrap() > 0);
        assert!(page.output["next_cursor"].is_null());
        assert!(records.iter().any(|record| record["stream"] == "stdout"));
        assert!(records.iter().any(|record| record["stream"] == "stderr"));

        let updated = provider
            .execute(dispatch(
                "native.command.metadata",
                serde_json::json!({
                    "job_id": job_id,
                    "metadata": {
                        "is_backgrounded": true,
                        "owner_session_id": "reparented-session"
                    }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(updated.output["metadata"]["is_backgrounded"], true);
        assert_eq!(
            updated.output["metadata"]["owner_session_id"],
            "reparented-session"
        );
        let listed = provider
            .execute(dispatch(
                "native.command.list",
                serde_json::json!({"cursor": 0, "limit": 10}),
            ))
            .await
            .unwrap();
        assert!(listed.output.as_array().unwrap().iter().any(|snapshot| {
            snapshot["job_id"] == job_id
                && snapshot["metadata"]["owner_session_id"] == "reparented-session"
        }));

        let cleanup = provider
            .execute(dispatch(
                "native.command.cleanup",
                serde_json::json!({"job_id": job_id}),
            ))
            .await
            .unwrap();
        assert_eq!(cleanup.output["lifecycle"], "completed");
        let missing = provider
            .execute(dispatch(
                "native.command.status",
                serde_json::json!({"job_id": job_id}),
            ))
            .await
            .unwrap_err();
        assert_eq!(missing.code, ProtocolErrorCode::NotFound);
    }

    #[tokio::test]
    async fn provider_streams_input_to_a_real_daemon_owned_process() {
        let directory = tempfile::tempdir().unwrap();
        let provider = NativeExecutionProvider::open(directory.path().to_path_buf())
            .await
            .unwrap();
        let started = provider
            .execute(dispatch(
                "native.command.start",
                serde_json::json!({
                    "executable": "/bin/sh",
                    "args": ["-c", "cat"],
                    "timeout_ms": 5000,
                    "stdin": "pipe"
                }),
            ))
            .await
            .unwrap();
        let job_id = started.output["job_id"].as_str().unwrap().to_owned();
        let written = provider
            .execute(dispatch(
                "native.command.stdin",
                serde_json::json!({
                    "job_id": job_id,
                    "bytes": [100, 97, 101, 109, 111, 110, 45, 115, 116, 100, 105, 110],
                    "close": true
                }),
            ))
            .await
            .unwrap();
        assert_eq!(written.output["stdin_closed"], true);
        let waited = provider
            .execute(dispatch(
                "native.command.wait",
                serde_json::json!({"job_id": job_id, "wait_ms": 5000}),
            ))
            .await
            .unwrap();
        assert_eq!(waited.output["lifecycle"], "completed");
        let page = provider
            .execute(dispatch(
                "native.command.output",
                serde_json::json!({"job_id": job_id}),
            ))
            .await
            .unwrap();
        let bytes = page.output["records"][0]["bytes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|byte| byte.as_u64().unwrap() as u8)
            .collect::<Vec<_>>();
        assert_eq!(bytes, b"daemon-stdin");
    }

    #[tokio::test]
    async fn detached_request_owns_and_cancels_its_real_process() {
        let directory = tempfile::tempdir().unwrap();
        let provider = NativeExecutionProvider::open(directory.path().to_path_buf())
            .await
            .unwrap();
        let dispatch = dispatch_with_mode(
            "native.command.start",
            serde_json::json!({
                "executable": "/bin/sh",
                "args": ["-c", "printf detached-started; sleep 30"],
                "timeout_ms": 60_000
            }),
            ExecutionMode::Detached,
        );
        let request_id = dispatch.request_id.clone();
        let running_provider = provider.clone();
        let execution = tokio::spawn(async move { running_provider.execute(dispatch).await });

        let job = wait_for_job(&provider).await;
        provider.cancel(&request_id).await.unwrap();
        let output = execution.await.unwrap().unwrap();
        assert_eq!(output.output["job_id"], job.job_id);
        assert_eq!(output.output["lifecycle"], "cancelled");
        assert_eq!(
            provider
                .supervisor
                .snapshot(output.output["job_id"].as_str().unwrap())
                .await
                .unwrap()
                .lifecycle,
            xai_grok_native_execution::JobLifecycle::Cancelled
        );
        assert!(provider.request_jobs.read().await.is_empty());
    }

    #[tokio::test]
    async fn detached_nonzero_exit_preserves_output_and_reports_failure() {
        let directory = tempfile::tempdir().unwrap();
        let provider = NativeExecutionProvider::open(directory.path().to_path_buf())
            .await
            .unwrap();
        let output = provider
            .execute(dispatch_with_mode(
                "native.command.start",
                serde_json::json!({
                    "executable": "/bin/sh",
                    "args": ["-c", "printf failure-evidence; exit 7"],
                    "timeout_ms": 5_000
                }),
                ExecutionMode::Detached,
            ))
            .await
            .unwrap();
        assert_eq!(output.output["lifecycle"], "failed");
        assert_eq!(output.output["exit_code"], 7);
        assert_eq!(output.terminal_status, Some(ProviderTerminalStatus::Failed));
        assert_eq!(output.artifacts.len(), 1);
        let crate::provider::ProviderArtifactSource::File { path } = &output.artifacts[0].content
        else {
            panic!("failed process output must remain available as an artifact");
        };
        assert!(path.is_file());
    }

    #[tokio::test]
    async fn detached_task_deadline_cancels_process_but_preserves_partial_output() {
        let directory = tempfile::tempdir().unwrap();
        let provider = NativeExecutionProvider::open(directory.path().to_path_buf())
            .await
            .unwrap();
        let mut request = dispatch_with_mode(
            "native.command.start",
            serde_json::json!({
                "executable": "/bin/sh",
                "args": ["-c", "printf before-deadline; sleep 30"],
                "timeout_ms": 60_000
            }),
            ExecutionMode::Detached,
        );
        request.task.deadline_unix_ms = now_unix_ms() + 500;
        let output = provider.execute(request).await.unwrap();
        assert_eq!(output.output["lifecycle"], "cancelled");
        assert_eq!(output.output["task_deadline_exceeded"], true);
        assert_eq!(output.terminal_status, Some(ProviderTerminalStatus::Failed));
        assert_eq!(output.artifacts.len(), 1);
        let crate::provider::ProviderArtifactSource::File { path } = &output.artifacts[0].content
        else {
            panic!("partial deadline output must remain available as an artifact");
        };
        assert!(path.is_file());
    }

    #[tokio::test]
    async fn wait_request_cancellation_is_routed_to_its_real_process() {
        let directory = tempfile::tempdir().unwrap();
        let provider = NativeExecutionProvider::open(directory.path().to_path_buf())
            .await
            .unwrap();
        let started = provider
            .execute(dispatch(
                "native.command.start",
                serde_json::json!({
                    "executable": "/bin/sh",
                    "args": ["-c", "sleep 30"],
                    "timeout_ms": 60_000
                }),
            ))
            .await
            .unwrap();
        let job_id = started.output["job_id"].as_str().unwrap().to_owned();
        let wait_dispatch = dispatch(
            "native.command.wait",
            serde_json::json!({"job_id": job_id, "wait_ms": 30_000}),
        );
        let request_id = wait_dispatch.request_id.clone();
        let waiting_provider = provider.clone();
        let waiting = tokio::spawn(async move { waiting_provider.execute(wait_dispatch).await });

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if provider.request_jobs.read().await.contains_key(&request_id) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("wait request did not register for cancellation");
        provider.cancel(&request_id).await.unwrap();
        let output = waiting.await.unwrap().unwrap();
        assert_eq!(output.output["lifecycle"], "cancelled");
        assert!(provider.request_jobs.read().await.is_empty());
    }

    #[tokio::test]
    async fn cancellation_never_claims_an_unknown_request_was_cancelled() {
        let directory = tempfile::tempdir().unwrap();
        let provider = NativeExecutionProvider::open(directory.path().to_path_buf())
            .await
            .unwrap();
        let error = provider.cancel(&RequestId::new()).await.unwrap_err();
        assert_eq!(error.code, ProtocolErrorCode::NotFound);
    }
}
