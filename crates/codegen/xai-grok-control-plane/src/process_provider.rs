use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout};
use tokio::sync::Mutex;
use xai_grok_protocol::{
    CapabilityManifest, MAX_PROVIDER_WORKER_FRAME_BYTES, PROVIDER_WORKER_PROTOCOL_VERSION,
    ProtocolError, ProtocolErrorCode, ProviderDispatch, ProviderWorkerArtifactSource,
    ProviderWorkerRequest, ProviderWorkerResponse, RequestId, ServiceHealth,
};

use crate::provider::{
    ExecutionProvider, ProviderArtifact, ProviderArtifactSource, ProviderOutput,
};

const RESTART_BACKOFF: Duration = Duration::from_millis(250);

#[derive(Clone, Debug)]
pub struct ProcessProviderConfig {
    pub binary: PathBuf,
    pub arguments: Vec<String>,
    pub environment: BTreeMap<String, String>,
    pub startup_timeout: Duration,
}

impl ProcessProviderConfig {
    pub fn new(binary: impl Into<PathBuf>) -> Self {
        Self {
            binary: binary.into(),
            arguments: Vec::new(),
            environment: BTreeMap::new(),
            startup_timeout: Duration::from_secs(30),
        }
    }

    fn validate(&self) -> Result<(), ProtocolError> {
        if !self.binary.is_absolute() || !self.binary.is_file() {
            return Err(unavailable(format!(
                "provider worker is not an absolute regular file: {}",
                self.binary.display()
            )));
        }
        if self.startup_timeout.is_zero() {
            return Err(ProtocolError::new(
                ProtocolErrorCode::InvalidEnvelope,
                "provider worker startup timeout must be non-zero",
            ));
        }
        Ok(())
    }
}

struct WorkerProcess {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
}

impl WorkerProcess {
    async fn start(
        config: &ProcessProviderConfig,
    ) -> Result<(Self, CapabilityManifest), ProtocolError> {
        let mut child = tokio::process::Command::new(&config.binary)
            .args(&config.arguments)
            .envs(&config.environment)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| unavailable(format!("failed to spawn provider worker: {error}")))?;
        let input = child
            .stdin
            .take()
            .ok_or_else(|| unavailable("provider worker has no stdin"))?;
        let output = child
            .stdout
            .take()
            .ok_or_else(|| unavailable("provider worker has no stdout"))?;
        let mut process = Self {
            child,
            input,
            output: BufReader::new(output),
        };
        let response = tokio::time::timeout(config.startup_timeout, async {
            write_worker_frame(
                &mut process.input,
                &ProviderWorkerRequest::Hello {
                    protocol_version: PROVIDER_WORKER_PROTOCOL_VERSION,
                },
            )
            .await?;
            read_worker_frame(&mut process.output).await
        })
        .await
        .map_err(|_| unavailable("provider worker handshake timed out"))??;
        let ProviderWorkerResponse::Hello {
            protocol_version,
            manifest,
        } = response
        else {
            return Err(unavailable("provider worker returned an invalid handshake"));
        };
        if protocol_version != PROVIDER_WORKER_PROTOCOL_VERSION {
            return Err(ProtocolError::new(
                ProtocolErrorCode::IncompatibleVersion,
                format!(
                    "provider worker protocol {protocol_version} is incompatible with {}",
                    PROVIDER_WORKER_PROTOCOL_VERSION
                ),
            ));
        }
        manifest.validate()?;
        Ok((process, manifest))
    }

    fn exited(&mut self) -> Result<bool, ProtocolError> {
        self.child
            .try_wait()
            .map(|status| status.is_some())
            .map_err(|error| unavailable(format!("failed to inspect provider worker: {error}")))
    }
}

/// Persistent executable provider attached to `grokd` over bounded local
/// MessagePack frames. The daemon owns process lifetime and retries only by
/// starting a new, capability-identical worker generation.
pub struct ProcessExecutionProvider {
    config: ProcessProviderConfig,
    manifest: CapabilityManifest,
    manifest_hash: String,
    process: Mutex<Option<WorkerProcess>>,
    restart_not_before: StdMutex<Option<tokio::time::Instant>>,
    worker_generation: AtomicU64,
}

impl ProcessExecutionProvider {
    pub async fn start(config: ProcessProviderConfig) -> Result<Arc<Self>, ProtocolError> {
        config.validate()?;
        let (process, manifest) = WorkerProcess::start(&config).await?;
        let manifest_hash = manifest.content_hash();
        Ok(Arc::new(Self {
            config,
            manifest,
            manifest_hash,
            process: Mutex::new(Some(process)),
            restart_not_before: StdMutex::new(None),
            worker_generation: AtomicU64::new(1),
        }))
    }

    async fn ensure_process(
        &self,
        process: &mut Option<WorkerProcess>,
    ) -> Result<(), ProtocolError> {
        if process.is_some() {
            return Ok(());
        }
        let retry_at = *self
            .restart_not_before
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if retry_at.is_some_and(|retry_at| retry_at > tokio::time::Instant::now()) {
            return Err(unavailable("provider worker is in restart backoff").retryable());
        }
        let (replacement, manifest) = WorkerProcess::start(&self.config).await?;
        if manifest.content_hash() != self.manifest_hash {
            return Err(ProtocolError::new(
                ProtocolErrorCode::Conflict,
                format!(
                    "restarted provider {} changed its capability manifest",
                    self.manifest.provider_id
                ),
            ));
        }
        let next_generation = self
            .worker_generation
            .load(Ordering::Acquire)
            .checked_add(1)
            .ok_or_else(|| {
                ProtocolError::new(
                    ProtocolErrorCode::Conflict,
                    format!(
                        "provider {} worker generation exhausted",
                        self.manifest.provider_id
                    ),
                )
            })?;
        *process = Some(replacement);
        self.worker_generation
            .store(next_generation, Ordering::Release);
        *self
            .restart_not_before
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        Ok(())
    }

    fn record_failure(&self) {
        *self
            .restart_not_before
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            Some(tokio::time::Instant::now() + RESTART_BACKOFF);
    }
}

#[async_trait]
impl ExecutionProvider for ProcessExecutionProvider {
    fn manifest(&self) -> CapabilityManifest {
        self.manifest.clone()
    }

    async fn worker_generation(&self) -> u64 {
        self.worker_generation.load(Ordering::Acquire)
    }

    async fn health(&self) -> ServiceHealth {
        let Ok(mut process) = self.process.try_lock() else {
            return ServiceHealth::Ready;
        };
        let Some(worker) = process.as_mut() else {
            return ServiceHealth::Failed;
        };
        match worker.exited() {
            Ok(false) => ServiceHealth::Ready,
            Ok(true) | Err(_) => {
                process.take();
                self.record_failure();
                ServiceHealth::Failed
            }
        }
    }

    async fn status(&self) -> serde_json::Value {
        let retry_in_ms = self
            .restart_not_before
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .and_then(|deadline| deadline.checked_duration_since(tokio::time::Instant::now()))
            .map(|remaining| remaining.as_millis() as u64);
        let Ok(process) = self.process.try_lock() else {
            return serde_json::json!({
                "transport":"local_binary_ipc",
                "busy":true,
                "restart_in_ms":retry_in_ms,
                "worker_generation":self.worker_generation.load(Ordering::Acquire),
            });
        };
        serde_json::json!({
            "transport":"local_binary_ipc",
            "busy":false,
            "child_pid":process.as_ref().and_then(|worker| worker.child.id()),
            "restart_in_ms":retry_in_ms,
            "manifest_hash":self.manifest_hash,
            "worker_generation":self.worker_generation.load(Ordering::Acquire),
        })
    }

    async fn execute(&self, dispatch: ProviderDispatch) -> Result<ProviderOutput, ProtocolError> {
        let request_id = dispatch.request_id.clone();
        let remaining = dispatch.task.deadline_unix_ms.saturating_sub(now_unix_ms());
        if remaining == 0 {
            return Err(ProtocolError::new(
                ProtocolErrorCode::DeadlineExceeded,
                "provider task deadline elapsed before worker dispatch",
            ));
        }
        let mut process = self.process.lock().await;
        self.ensure_process(&mut process).await?;
        let result = tokio::time::timeout(Duration::from_millis(remaining), async {
            let worker = process
                .as_mut()
                .ok_or_else(|| unavailable("provider worker is unavailable"))?;
            write_worker_frame(
                &mut worker.input,
                &ProviderWorkerRequest::Execute {
                    dispatch: Box::new(dispatch),
                },
            )
            .await?;
            read_worker_frame(&mut worker.output).await
        })
        .await;
        let response = match result {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                process.take();
                self.record_failure();
                return Err(error.retryable());
            }
            Err(_) => {
                process.take();
                self.record_failure();
                return Err(ProtocolError::new(
                    ProtocolErrorCode::DeadlineExceeded,
                    "provider worker execution exceeded the task deadline",
                ));
            }
        };
        let ProviderWorkerResponse::Execute {
            request_id: response_id,
            result,
        } = response
        else {
            process.take();
            self.record_failure();
            return Err(unavailable(
                "provider worker returned an invalid execution response",
            ));
        };
        if response_id != request_id {
            process.take();
            self.record_failure();
            return Err(ProtocolError::new(
                ProtocolErrorCode::Conflict,
                "provider worker response request id did not match dispatch",
            ));
        }
        let output = result?;
        Ok(ProviderOutput {
            output: output.output,
            observations: output.observations,
            artifacts: output
                .artifacts
                .into_iter()
                .map(|artifact| ProviderArtifact {
                    media_type: artifact.media_type,
                    content: match artifact.content {
                        ProviderWorkerArtifactSource::Inline { bytes } => {
                            ProviderArtifactSource::Inline { bytes }
                        }
                        ProviderWorkerArtifactSource::File { path } => {
                            ProviderArtifactSource::File { path }
                        }
                    },
                })
                .collect(),
        })
    }

    async fn cancel(&self, request_id: &RequestId) -> Result<(), ProtocolError> {
        Err(ProtocolError::new(
            ProtocolErrorCode::UnsupportedOperation,
            format!(
                "provider {} does not support cancellation for request {request_id}",
                self.manifest.provider_id
            ),
        ))
    }
}

async fn write_worker_frame<W: AsyncWrite + Unpin, T: serde::Serialize>(
    writer: &mut W,
    value: &T,
) -> Result<(), ProtocolError> {
    let bytes = rmp_serde::to_vec_named(value)
        .map_err(|error| unavailable(format!("encode provider worker frame: {error}")))?;
    if bytes.len() > MAX_PROVIDER_WORKER_FRAME_BYTES {
        return Err(ProtocolError::new(
            ProtocolErrorCode::InvalidEnvelope,
            format!(
                "provider worker frame is {} bytes; maximum is {}",
                bytes.len(),
                MAX_PROVIDER_WORKER_FRAME_BYTES
            ),
        ));
    }
    writer
        .write_u32(u32::try_from(bytes.len()).unwrap_or(u32::MAX))
        .await
        .map_err(worker_io)?;
    writer.write_all(&bytes).await.map_err(worker_io)?;
    writer.flush().await.map_err(worker_io)
}

async fn read_worker_frame<R: AsyncRead + Unpin, T: serde::de::DeserializeOwned>(
    reader: &mut R,
) -> Result<T, ProtocolError> {
    let length = reader.read_u32().await.map_err(worker_io)? as usize;
    if length > MAX_PROVIDER_WORKER_FRAME_BYTES {
        return Err(ProtocolError::new(
            ProtocolErrorCode::InvalidEnvelope,
            format!(
                "provider worker frame is {length} bytes; maximum is {MAX_PROVIDER_WORKER_FRAME_BYTES}"
            ),
        ));
    }
    let mut bytes = vec![0; length];
    reader.read_exact(&mut bytes).await.map_err(worker_io)?;
    rmp_serde::from_slice(&bytes)
        .map_err(|error| unavailable(format!("decode provider worker frame: {error}")))
}

fn worker_io(error: std::io::Error) -> ProtocolError {
    unavailable(format!("provider worker I/O failed: {error}"))
}

fn unavailable(message: impl Into<String>) -> ProtocolError {
    ProtocolError::new(ProtocolErrorCode::ServiceUnavailable, message)
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn worker_frames_round_trip_and_remain_bounded() {
        let (mut left, mut right) = tokio::io::duplex(4096);
        let expected = ProviderWorkerRequest::Cancel {
            request_id: RequestId::from_string("request-one"),
        };
        let sent = expected.clone();
        let writer = tokio::spawn(async move { write_worker_frame(&mut left, &sent).await });
        let actual: ProviderWorkerRequest = read_worker_frame(&mut right).await.unwrap();
        writer.await.unwrap().unwrap();
        assert_eq!(actual, expected);
    }
}
