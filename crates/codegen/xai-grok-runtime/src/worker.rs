use std::collections::{HashMap, VecDeque};
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use tokio::sync::{Mutex, OwnedSemaphorePermit, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use xai_grok_sampling_types::SamplingError;

use crate::adapter::{AdapterBinding, AdapterDescriptor};
use crate::events::RuntimeEvent;
use crate::litert_lm::{LiteRtLmConfig, LocalInferenceResult, PreparedConversation};
use crate::protocol::{
    WORKER_PROTOCOL_VERSION, WorkerCommand, WorkerMessage, WorkerStats, read_frame, write_frame,
};

const NATIVE_CANCEL_GRACE: Duration = Duration::from_secs(1);
const FORCED_KILL_WAIT: Duration = Duration::from_millis(250);

enum PendingRequest {
    Generation {
        events: mpsc::UnboundedSender<RuntimeEvent>,
        completion: oneshot::Sender<Result<LocalInferenceResult, SamplingError>>,
    },
    Measurement {
        completion: oneshot::Sender<Result<u32, SamplingError>>,
    },
}

struct WorkerClientInner {
    writer: Mutex<tokio::net::unix::OwnedWriteHalf>,
    child: Mutex<tokio::process::Child>,
    pending: Mutex<HashMap<String, PendingRequest>>,
    control_waiters: Mutex<VecDeque<oneshot::Sender<WorkerMessage>>>,
    control_gate: Mutex<()>,
    dead: AtomicBool,
    active_requests: AtomicU32,
    _process_slot: OwnedSemaphorePermit,
}

/// Persistent client for one isolated native engine worker.
#[derive(Clone)]
pub(crate) struct WorkerClient {
    inner: Arc<WorkerClientInner>,
}

impl WorkerClient {
    #[cfg(unix)]
    pub async fn spawn(
        path: &Path,
        config: LiteRtLmConfig,
        model_id: String,
        model_load_timeout: Duration,
        process_slot: OwnedSemaphorePermit,
    ) -> Result<Self, SamplingError> {
        use command_fds::{CommandFdExt, FdMapping};
        use std::os::fd::OwnedFd;

        let (parent, child_socket) = std::os::unix::net::UnixStream::pair().map_err(worker_io)?;
        parent.set_nonblocking(true).map_err(worker_io)?;
        let child_fd: OwnedFd = child_socket.into();
        let mut command = tokio::process::Command::new(path);
        command
            .env("GROK_RUNTIME_FD", "3")
            .kill_on_drop(true)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::inherit());
        command
            .fd_mappings(vec![FdMapping {
                parent_fd: child_fd,
                child_fd: 3,
            }])
            .map_err(|error| worker_error("worker_spawn", error.to_string()))?;
        let child = command.spawn().map_err(worker_io)?;
        let socket = tokio::net::UnixStream::from_std(parent).map_err(worker_io)?;
        let (reader, writer) = socket.into_split();
        let client = Self {
            inner: Arc::new(WorkerClientInner {
                writer: Mutex::new(writer),
                child: Mutex::new(child),
                pending: Mutex::new(HashMap::new()),
                control_waiters: Mutex::new(VecDeque::new()),
                control_gate: Mutex::new(()),
                dead: AtomicBool::new(false),
                active_requests: AtomicU32::new(0),
                _process_slot: process_slot,
            }),
        };
        client.spawn_reader(reader);

        match client
            .send_control(WorkerCommand::Hello {
                protocol_version: WORKER_PROTOCOL_VERSION,
            })
            .await?
        {
            WorkerMessage::Hello {
                protocol_version, ..
            } if protocol_version == WORKER_PROTOCOL_VERSION => {}
            message => {
                return Err(worker_error(
                    "worker_handshake",
                    format!("unexpected worker handshake: {message:?}"),
                ));
            }
        }
        match client
            .send_control_with_timeout(
                WorkerCommand::LoadModel { config, model_id },
                model_load_timeout,
            )
            .await?
        {
            WorkerMessage::Ready { .. } => Ok(client),
            message => Err(worker_error(
                "worker_load",
                format!("unexpected worker load response: {message:?}"),
            )),
        }
    }

    #[cfg(not(unix))]
    pub async fn spawn(
        _path: &Path,
        _config: LiteRtLmConfig,
        _model_id: String,
        _model_load_timeout: Duration,
        _process_slot: OwnedSemaphorePermit,
    ) -> Result<Self, SamplingError> {
        Err(worker_error(
            "worker_platform",
            "isolated local runtime workers currently require Unix sockets",
        ))
    }

    fn spawn_reader(&self, mut reader: tokio::net::unix::OwnedReadHalf) {
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            loop {
                let message = match read_frame::<_, WorkerMessage>(&mut reader).await {
                    Ok(message) => message,
                    Err(error) => {
                        fail_all(&inner, worker_io(error)).await;
                        break;
                    }
                };
                dispatch_message(&inner, message).await;
            }
        });
    }

    pub fn is_dead(&self) -> bool {
        self.inner.dead.load(Ordering::Acquire)
    }

    pub fn active_requests(&self) -> u32 {
        self.inner.active_requests.load(Ordering::Acquire)
    }

    pub async fn measure(
        &self,
        request_id: String,
        prepared: PreparedConversation,
    ) -> Result<u32, SamplingError> {
        let (completion_tx, completion_rx) = oneshot::channel();
        self.inner.pending.lock().await.insert(
            request_id.clone(),
            PendingRequest::Measurement {
                completion: completion_tx,
            },
        );
        if let Err(error) = self
            .write_command(&WorkerCommand::Measure {
                request_id: request_id.clone(),
                prepared,
            })
            .await
        {
            self.inner.pending.lock().await.remove(&request_id);
            return Err(error);
        }
        completion_rx
            .await
            .map_err(|_| worker_error("worker_measure", "worker dropped measurement completion"))?
    }

    pub async fn generate(
        &self,
        request_id: String,
        prepared: PreparedConversation,
        events: mpsc::UnboundedSender<RuntimeEvent>,
        cancel: CancellationToken,
    ) -> Result<LocalInferenceResult, SamplingError> {
        self.inner.active_requests.fetch_add(1, Ordering::AcqRel);
        let _active = WorkerActiveGuard {
            inner: Arc::clone(&self.inner),
        };
        let (completion_tx, mut completion_rx) = oneshot::channel();
        self.inner.pending.lock().await.insert(
            request_id.clone(),
            PendingRequest::Generation {
                events,
                completion: completion_tx,
            },
        );
        if let Err(error) = self
            .write_command(&WorkerCommand::Generate {
                request_id: request_id.clone(),
                prepared,
            })
            .await
        {
            self.inner.pending.lock().await.remove(&request_id);
            return Err(error);
        }
        tokio::select! {
            result = &mut completion_rx => {
                result.map_err(|_| worker_error("worker_generate", "worker dropped generation completion"))?
            }
            _ = cancel.cancelled() => {
                self.write_command(&WorkerCommand::Cancel {
                    request_id: request_id.clone(),
                }).await?;
                match tokio::time::timeout(NATIVE_CANCEL_GRACE, &mut completion_rx).await {
                    Ok(result) => result.map_err(|_| {
                        worker_error("worker_cancel", "worker dropped cancellation completion")
                    })?,
                    Err(_) => {
                        self.terminate().await;
                        Err(worker_error(
                            "worker_cancel_timeout",
                            "native worker did not stop within two seconds",
                        ))
                    }
                }
            }
        }
    }

    pub async fn drop_session(&self, session_id: String) -> Result<(), SamplingError> {
        match self
            .send_control(WorkerCommand::DropSession { session_id })
            .await?
        {
            WorkerMessage::Ack => Ok(()),
            message => Err(worker_error(
                "worker_session",
                format!("unexpected session response: {message:?}"),
            )),
        }
    }

    pub async fn drop_inactive_sessions(&self, max_remaining: u32) -> Result<(), SamplingError> {
        match self
            .send_control(WorkerCommand::DropInactiveSessions { max_remaining })
            .await?
        {
            WorkerMessage::Ack => Ok(()),
            message => Err(worker_error(
                "worker_session",
                format!("unexpected inactive-session response: {message:?}"),
            )),
        }
    }

    pub async fn load_adapter(&self, descriptor: AdapterDescriptor) -> Result<u32, SamplingError> {
        match self
            .send_control(WorkerCommand::LoadAdapter { descriptor })
            .await?
        {
            WorkerMessage::AdapterReady { native_id, .. } => Ok(native_id),
            message => Err(worker_error(
                "worker_adapter_load",
                format!("unexpected adapter load response: {message:?}"),
            )),
        }
    }

    pub async fn select_adapter(
        &self,
        request_id: String,
        binding: &AdapterBinding,
    ) -> Result<(), SamplingError> {
        match self
            .send_control(WorkerCommand::SelectAdapter {
                request_id,
                adapter_id: binding.adapter_id.clone(),
                revision: binding.revision.clone(),
            })
            .await?
        {
            WorkerMessage::Ack => Ok(()),
            message => Err(worker_error(
                "worker_adapter_select",
                format!("unexpected adapter selection response: {message:?}"),
            )),
        }
    }

    pub async fn unload_adapter(&self, binding: &AdapterBinding) -> Result<(), SamplingError> {
        match self
            .send_control(WorkerCommand::UnloadAdapter {
                adapter_id: binding.adapter_id.clone(),
                revision: binding.revision.clone(),
            })
            .await?
        {
            WorkerMessage::AdapterUnloaded { .. } => Ok(()),
            message => Err(worker_error(
                "worker_adapter_unload",
                format!("unexpected adapter unload response: {message:?}"),
            )),
        }
    }

    pub async fn stats(&self) -> Result<WorkerStats, SamplingError> {
        match self.send_control(WorkerCommand::Stats).await? {
            WorkerMessage::Stats { stats } => Ok(stats),
            message => Err(worker_error(
                "worker_stats",
                format!("unexpected stats response: {message:?}"),
            )),
        }
    }

    pub async fn terminate(&self) {
        self.inner.dead.store(true, Ordering::Release);
        let mut child = self.inner.child.lock().await;
        let _ = child.start_kill();
        let _ = tokio::time::timeout(FORCED_KILL_WAIT, child.wait()).await;
    }

    pub async fn shutdown(&self) {
        if self.is_dead() {
            return;
        }
        let acknowledged = matches!(
            tokio::time::timeout(
                Duration::from_secs(2),
                self.send_control(WorkerCommand::Shutdown)
            )
            .await,
            Ok(Ok(WorkerMessage::Ack))
        );
        self.inner.dead.store(true, Ordering::Release);
        let mut child = self.inner.child.lock().await;
        if acknowledged
            && tokio::time::timeout(Duration::from_secs(2), child.wait())
                .await
                .is_ok()
        {
            return;
        }
        let _ = child.start_kill();
        let _ = tokio::time::timeout(FORCED_KILL_WAIT, child.wait()).await;
    }

    async fn send_control(&self, command: WorkerCommand) -> Result<WorkerMessage, SamplingError> {
        let timeout = match &command {
            WorkerCommand::LoadModel { .. } | WorkerCommand::LoadAdapter { .. } => {
                Duration::from_secs(120)
            }
            WorkerCommand::Shutdown => Duration::from_secs(2),
            _ => Duration::from_secs(5),
        };
        self.send_control_with_timeout(command, timeout).await
    }

    async fn send_control_with_timeout(
        &self,
        command: WorkerCommand,
        timeout: Duration,
    ) -> Result<WorkerMessage, SamplingError> {
        let _gate = self.inner.control_gate.lock().await;
        let (response_tx, response_rx) = oneshot::channel();
        self.inner
            .control_waiters
            .lock()
            .await
            .push_back(response_tx);
        if let Err(error) = self.write_command(&command).await {
            self.inner.control_waiters.lock().await.pop_back();
            return Err(error);
        }
        match tokio::time::timeout(timeout, response_rx).await {
            Ok(result) => result
                .map_err(|_| worker_error("worker_control", "worker dropped control response")),
            Err(_) => {
                self.inner.control_waiters.lock().await.pop_front();
                self.terminate().await;
                Err(worker_error(
                    "worker_control_timeout",
                    format!("native worker did not answer control request within {timeout:?}"),
                ))
            }
        }
    }

    async fn write_command(&self, command: &WorkerCommand) -> Result<(), SamplingError> {
        if self.is_dead() {
            return Err(worker_error(
                "worker_dead",
                "local runtime worker is unavailable",
            ));
        }
        write_frame(&mut *self.inner.writer.lock().await, command)
            .await
            .map_err(worker_io)
    }
}

struct WorkerActiveGuard {
    inner: Arc<WorkerClientInner>,
}

impl Drop for WorkerActiveGuard {
    fn drop(&mut self) {
        self.inner.active_requests.fetch_sub(1, Ordering::AcqRel);
    }
}

async fn dispatch_message(inner: &Arc<WorkerClientInner>, message: WorkerMessage) {
    match message {
        WorkerMessage::Event { event } => {
            let request_id = event_request_id(&event);
            let pending = inner.pending.lock().await;
            if let Some(PendingRequest::Generation { events, .. }) = pending.get(request_id) {
                let _ = events.send(event);
            }
        }
        WorkerMessage::Completed {
            request_id,
            response,
            metrics,
        } => {
            if let Some(PendingRequest::Generation { completion, .. }) =
                inner.pending.lock().await.remove(&request_id)
            {
                let _ = completion.send(Ok(LocalInferenceResult::Completed {
                    response: Box::new(response),
                    metrics,
                }));
            }
        }
        WorkerMessage::Cancelled { request_id } => {
            if let Some(PendingRequest::Generation { completion, .. }) =
                inner.pending.lock().await.remove(&request_id)
            {
                let _ = completion.send(Ok(LocalInferenceResult::Cancelled));
            }
        }
        WorkerMessage::Measurement {
            request_id,
            prompt_tokens,
        } => {
            if let Some(PendingRequest::Measurement { completion }) =
                inner.pending.lock().await.remove(&request_id)
            {
                let _ = completion.send(Ok(prompt_tokens));
            }
        }
        WorkerMessage::Error {
            request_id: Some(request_id),
            code,
            message,
            ..
        } => {
            if let Some(pending) = inner.pending.lock().await.remove(&request_id) {
                let error = worker_error(&code, message);
                match pending {
                    PendingRequest::Generation { completion, .. } => {
                        let _ = completion.send(Err(error));
                    }
                    PendingRequest::Measurement { completion } => {
                        let _ = completion.send(Err(error));
                    }
                }
            }
        }
        control => {
            if let Some(waiter) = inner.control_waiters.lock().await.pop_front() {
                let _ = waiter.send(control);
            }
        }
    }
}

async fn fail_all(inner: &Arc<WorkerClientInner>, error: SamplingError) {
    inner.dead.store(true, Ordering::Release);
    let mut pending = inner.pending.lock().await;
    for (_, request) in pending.drain() {
        let error = worker_error("worker_disconnected", error.to_string());
        match request {
            PendingRequest::Generation { completion, .. } => {
                let _ = completion.send(Err(error));
            }
            PendingRequest::Measurement { completion } => {
                let _ = completion.send(Err(error));
            }
        }
    }
    let mut controls = inner.control_waiters.lock().await;
    controls.clear();
}

fn event_request_id(event: &RuntimeEvent) -> &str {
    match event {
        RuntimeEvent::Admitted { request_id, .. }
        | RuntimeEvent::StreamStarted { request_id, .. }
        | RuntimeEvent::FirstToken { request_id }
        | RuntimeEvent::ChannelToken { request_id, .. }
        | RuntimeEvent::ToolCallDelta { request_id, .. } => request_id,
    }
}

fn worker_io(error: io::Error) -> SamplingError {
    worker_error("worker_io", error.to_string())
}

fn worker_error(error_type: &str, message: impl Into<String>) -> SamplingError {
    SamplingError::StreamError {
        error_type: error_type.to_string(),
        message: message.into(),
    }
}
