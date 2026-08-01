use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use tokio::sync::RwLock;
use xai_grok_native_execution::{
    CommandRequest, NativeExecutionError, NativeExecutionLimits, NativeExecutionSupervisor,
    NmapRequest,
};
use xai_grok_protocol::{
    ArtifactContract, CancellationSemantics, CapabilityManifest, ConcurrencyProfile,
    EvidenceObservation, OperationDescriptor, PROTOCOL_VERSION, Platform, ProtocolError,
    ProtocolErrorCode, ProviderDispatch, ProviderKind, RecoverySemantics, RequestId, ServiceHealth,
    VersionRange,
};

use crate::provider::{ExecutionProvider, ProviderArtifact, ProviderOutput};

pub const NATIVE_PROVIDER_ID: &str = "native-execution";

pub struct NativeExecutionProvider {
    supervisor: Arc<NativeExecutionSupervisor>,
    request_jobs: RwLock<HashMap<RequestId, String>>,
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
        Ok(Arc::new(Self {
            supervisor: NativeExecutionSupervisor::open_with_limits(root, limits).await?,
            request_jobs: RwLock::new(HashMap::new()),
        }))
    }

    pub fn capability_manifest() -> CapabilityManifest {
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
            operation("native.command.wait", "Wait for native command", true),
            operation("native.command.output", "Read native command output", true),
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
                "terminal_cleanup",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
            operations,
            concurrency: ConcurrencyProfile {
                maximum_parallel: 100,
                queue_capacity: 512,
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

    async fn remember(&self, request_id: RequestId, job_id: String) {
        self.request_jobs.write().await.insert(request_id, job_id);
    }
}

#[async_trait]
impl ExecutionProvider for NativeExecutionProvider {
    fn manifest(&self) -> CapabilityManifest {
        Self::capability_manifest()
    }

    async fn health(&self) -> ServiceHealth {
        ServiceHealth::Ready
    }

    async fn execute(&self, dispatch: ProviderDispatch) -> Result<ProviderOutput, ProtocolError> {
        let operation = dispatch.task.capability.operation_id.as_str();
        let owner_id = dispatch.engagement_id.to_string();
        let input = dispatch.task.input;
        let mut artifacts = Vec::new();
        let output = match operation {
            "native.command.start" => {
                let request: CommandRequest = parse(input)?;
                let snapshot = self
                    .supervisor
                    .start_command_for(owner_id, request)
                    .await
                    .map_err(native_error)?;
                self.remember(dispatch.request_id, snapshot.job_id.clone())
                    .await;
                serde_json::to_value(snapshot).map_err(internal_error)?
            }
            "native.nmap.start" => {
                let request: NmapRequest = parse(input)?;
                let snapshot = self
                    .supervisor
                    .start_nmap_for(owner_id, request)
                    .await
                    .map_err(native_error)?;
                self.remember(dispatch.request_id, snapshot.job_id.clone())
                    .await;
                serde_json::to_value(snapshot).map_err(internal_error)?
            }
            "native.command.status" | "native.nmap.status" => {
                let input: JobInput = parse(input)?;
                serde_json::to_value(
                    self.supervisor
                        .snapshot(&input.job_id)
                        .await
                        .map_err(native_error)?,
                )
                .map_err(internal_error)?
            }
            "native.command.wait" => {
                let input: WaitInput = parse(input)?;
                let snapshot = self
                    .supervisor
                    .wait(
                        &input.job_id,
                        Duration::from_millis(input.wait_ms.min(30_000)),
                    )
                    .await
                    .map_err(native_error)?;
                if snapshot.lifecycle.is_terminal() {
                    artifacts.extend(
                        self.supervisor
                            .artifacts(&input.job_id)
                            .await
                            .map_err(native_error)?
                            .into_iter()
                            .map(|artifact| {
                                ProviderArtifact::file(artifact.media_type, artifact.path)
                            }),
                    );
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
                let artifacts = self
                    .supervisor
                    .artifacts(&input.job_id)
                    .await
                    .map_err(native_error)?
                    .into_iter()
                    .map(|artifact| ProviderArtifact::file(artifact.media_type, artifact.path))
                    .collect();
                return Ok(ProviderOutput {
                    output: serde_json::to_value(result).map_err(internal_error)?,
                    observations,
                    artifacts,
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
        })
    }

    async fn cancel(&self, request_id: &RequestId) -> Result<(), ProtocolError> {
        let job_id = self.request_jobs.read().await.get(request_id).cloned();
        if let Some(job_id) = job_id {
            self.supervisor
                .cancel(&job_id)
                .await
                .map_err(native_error)?;
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct JobInput {
    job_id: String,
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

fn default_records() -> usize {
    128
}

fn default_bytes() -> usize {
    1024 * 1024
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
        ProviderDispatch {
            request_id: RequestId::new(),
            engagement_id: EngagementId::new(),
            plan_revision: 1,
            task: ExecutionTask {
                task_id: TaskId::new(),
                objective: "native provider integration test".to_owned(),
                mode: ExecutionMode::Deferred,
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
                    + 10_000,
                completion_tests: Vec::new(),
                depends_on: Vec::new(),
            },
            provider_id: ProviderId::from_string(NATIVE_PROVIDER_ID),
            lease_epoch: 1,
        }
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
                    "timeout_ms": 5000
                }),
            ))
            .await
            .unwrap();
        let job_id = started.output["job_id"].as_str().unwrap().to_owned();
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
        assert!(records.iter().any(|record| record["stream"] == "stdout"));
        assert!(records.iter().any(|record| record["stream"] == "stderr"));

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
}
