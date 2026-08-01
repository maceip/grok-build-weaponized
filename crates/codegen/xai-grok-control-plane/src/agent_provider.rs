use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agent_client_protocol::{self as acp, Agent as _};
use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::compat::{TokioAsyncReadCompatExt as _, TokioAsyncWriteCompatExt as _};
use xai_acp_lib::LineBufferedRead;
use xai_grok_protocol::{
    ArtifactContract, CancellationSemantics, CapabilityManifest, ConcurrencyProfile,
    OperationDescriptor, PROTOCOL_VERSION, Platform, ProtocolError, ProtocolErrorCode,
    ProviderDispatch, ProviderId, ProviderKind, RecoverySemantics, RequestId, ServiceHealth,
    VersionRange,
};

use crate::provider::{ExecutionProvider, ProviderOutput};

pub const AGENT_TURN_OPERATION: &str = "agent.turn";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
pub struct AgentProviderConfig {
    pub binary: PathBuf,
    pub workspace: PathBuf,
    pub model_id: Option<String>,
    pub environment: BTreeMap<String, String>,
    pub maximum_parallel: u32,
    pub queue_capacity: u32,
}

impl AgentProviderConfig {
    pub fn new(binary: impl Into<PathBuf>, workspace: impl Into<PathBuf>) -> Self {
        Self {
            binary: binary.into(),
            workspace: workspace.into(),
            model_id: None,
            environment: BTreeMap::new(),
            maximum_parallel: 8,
            queue_capacity: 64,
        }
    }

    fn validate(&self) -> Result<(), ProtocolError> {
        if !self.binary.is_absolute() || !self.binary.is_file() {
            return Err(ProtocolError::new(
                ProtocolErrorCode::ServiceUnavailable,
                format!(
                    "agent provider binary is not an absolute regular file: {}",
                    self.binary.display()
                ),
            ));
        }
        if !self.workspace.is_absolute() || !self.workspace.is_dir() {
            return Err(ProtocolError::new(
                ProtocolErrorCode::InvalidEnvelope,
                format!(
                    "agent provider workspace is not an absolute directory: {}",
                    self.workspace.display()
                ),
            ));
        }
        if self.maximum_parallel == 0 || self.queue_capacity == 0 {
            return Err(ProtocolError::new(
                ProtocolErrorCode::InvalidEnvelope,
                "agent provider concurrency and queue capacity must be non-zero",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
struct AgentTurnInput {
    request: String,
    #[serde(default)]
    session_key: Option<String>,
    #[serde(default)]
    workspace: Option<PathBuf>,
    #[serde(default)]
    model_id: Option<String>,
}

impl AgentTurnInput {
    fn validate(&self, fallback_workspace: &std::path::Path) -> Result<(), ProtocolError> {
        if self.request.trim().is_empty() {
            return Err(ProtocolError::new(
                ProtocolErrorCode::InvalidEnvelope,
                "agent.turn request must not be empty",
            ));
        }
        let workspace = self.workspace.as_deref().unwrap_or(fallback_workspace);
        if !workspace.is_absolute() || !workspace.is_dir() {
            return Err(ProtocolError::new(
                ProtocolErrorCode::InvalidEnvelope,
                format!(
                    "agent.turn workspace is not an absolute directory: {}",
                    workspace.display()
                ),
            ));
        }
        Ok(())
    }
}

enum WorkerCommand {
    Execute {
        dispatch: ProviderDispatch,
        respond_to: oneshot::Sender<Result<ProviderOutput, ProtocolError>>,
    },
    Cancel {
        request_id: RequestId,
        respond_to: oneshot::Sender<Result<(), ProtocolError>>,
    },
    Shutdown,
}

#[derive(Default)]
struct TurnCapture {
    text: String,
    notifications: Vec<serde_json::Value>,
}

#[derive(Default)]
struct CaptureStore {
    active: RefCell<HashMap<String, TurnCapture>>,
}

impl CaptureStore {
    fn begin(&self, session_id: &acp::SessionId) {
        self.active
            .borrow_mut()
            .insert(session_id.0.to_string(), TurnCapture::default());
    }

    fn finish(&self, session_id: &acp::SessionId) -> TurnCapture {
        self.active
            .borrow_mut()
            .remove(session_id.0.as_ref())
            .unwrap_or_default()
    }
}

struct ProviderAcpClient {
    captures: Rc<CaptureStore>,
}

#[async_trait(?Send)]
impl acp::Client for ProviderAcpClient {
    async fn request_permission(
        &self,
        args: acp::RequestPermissionRequest,
    ) -> acp::Result<acp::RequestPermissionResponse> {
        let outcome = args
            .options
            .iter()
            .find(|option| option.kind == acp::PermissionOptionKind::AllowOnce)
            .or(args.options.first())
            .map(|option| {
                acp::RequestPermissionOutcome::Selected(acp::SelectedPermissionOutcome::new(
                    option.option_id.clone(),
                ))
            })
            .unwrap_or(acp::RequestPermissionOutcome::Cancelled);
        Ok(acp::RequestPermissionResponse::new(outcome))
    }

    async fn session_notification(
        &self,
        notification: acp::SessionNotification,
    ) -> acp::Result<()> {
        let key = notification.session_id.0.to_string();
        let serialized = serde_json::to_value(&notification).unwrap_or_else(
            |error| serde_json::json!({"notification_serialization_error": error.to_string()}),
        );
        let mut active = self.captures.active.borrow_mut();
        let Some(capture) = active.get_mut(&key) else {
            return Ok(());
        };
        if let acp::SessionUpdate::AgentMessageChunk(acp::ContentChunk { content, .. }) =
            &notification.update
            && let acp::ContentBlock::Text(text) = content
        {
            capture.text.push_str(&text.text);
        }
        capture.notifications.push(serialized);
        Ok(())
    }
}

/// A production provider backed by one persistent `grok agent stdio` process.
///
/// The provider process owns the existing turn coordinator, tool loop, memory
/// integration, and local LiteRT runtime manager. `grokd` owns its lifecycle,
/// admission, durable task state, result artifact, and cancellation routing.
pub struct AgentExecutionProvider {
    manifest: CapabilityManifest,
    commands: mpsc::Sender<WorkerCommand>,
    health: watch::Receiver<ServiceHealth>,
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl AgentExecutionProvider {
    pub async fn start(config: AgentProviderConfig) -> Result<Arc<Self>, ProtocolError> {
        config.validate()?;
        let manifest = manifest(&config);
        manifest.validate()?;
        let (commands, receiver) = mpsc::channel(config.queue_capacity as usize);
        let (health_tx, health) = watch::channel(ServiceHealth::Starting);
        let (startup_tx, startup_rx) = oneshot::channel();
        let worker_config = config.clone();
        let thread = std::thread::Builder::new()
            .name("grok-agent-provider".to_owned())
            .spawn(move || run_worker_thread(worker_config, receiver, health_tx, startup_tx))
            .map_err(|error| {
                ProtocolError::new(
                    ProtocolErrorCode::ServiceUnavailable,
                    format!("failed to start agent provider thread: {error}"),
                )
            })?;
        match tokio::time::timeout(STARTUP_TIMEOUT, startup_rx).await {
            Ok(Ok(Ok(()))) => Ok(Arc::new(Self {
                manifest,
                commands,
                health,
                thread: Mutex::new(Some(thread)),
            })),
            Ok(Ok(Err(error))) => {
                let _ = thread.join();
                Err(error)
            }
            Ok(Err(_)) => {
                let _ = commands.try_send(WorkerCommand::Shutdown);
                let _ = thread.join();
                Err(ProtocolError::new(
                    ProtocolErrorCode::ServiceUnavailable,
                    "agent provider stopped during startup",
                ))
            }
            Err(_) => {
                let _ = commands.try_send(WorkerCommand::Shutdown);
                let _ = thread.join();
                Err(ProtocolError::new(
                    ProtocolErrorCode::DeadlineExceeded,
                    "agent provider startup timed out",
                ))
            }
        }
    }
}

impl Drop for AgentExecutionProvider {
    fn drop(&mut self) {
        let _ = self.commands.try_send(WorkerCommand::Shutdown);
        // The provider is normally dropped during runtime shutdown. Do not
        // block an async executor thread waiting for the child process; the
        // worker owns kill-on-drop and exits independently.
        let _ = self
            .thread
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
    }
}

#[async_trait]
impl ExecutionProvider for AgentExecutionProvider {
    fn manifest(&self) -> CapabilityManifest {
        self.manifest.clone()
    }

    async fn health(&self) -> ServiceHealth {
        *self.health.borrow()
    }

    async fn execute(&self, dispatch: ProviderDispatch) -> Result<ProviderOutput, ProtocolError> {
        let (respond_to, response) = oneshot::channel();
        self.commands
            .send(WorkerCommand::Execute {
                dispatch,
                respond_to,
            })
            .await
            .map_err(|_| unavailable("agent provider command channel is closed"))?;
        response
            .await
            .map_err(|_| unavailable("agent provider dropped an execution response"))?
    }

    async fn cancel(&self, request_id: &RequestId) -> Result<(), ProtocolError> {
        let (respond_to, response) = oneshot::channel();
        self.commands
            .send(WorkerCommand::Cancel {
                request_id: request_id.clone(),
                respond_to,
            })
            .await
            .map_err(|_| unavailable("agent provider command channel is closed"))?;
        response
            .await
            .map_err(|_| unavailable("agent provider dropped a cancellation response"))?
    }
}

fn run_worker_thread(
    config: AgentProviderConfig,
    receiver: mpsc::Receiver<WorkerCommand>,
    health: watch::Sender<ServiceHealth>,
    startup: oneshot::Sender<Result<(), ProtocolError>>,
) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            let _ = startup.send(Err(unavailable(format!(
                "failed to build agent provider runtime: {error}"
            ))));
            return;
        }
    };
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, run_worker(config, receiver, health, startup));
}

async fn run_worker(
    config: AgentProviderConfig,
    mut receiver: mpsc::Receiver<WorkerCommand>,
    health: watch::Sender<ServiceHealth>,
    startup: oneshot::Sender<Result<(), ProtocolError>>,
) {
    let mut startup = Some(startup);
    loop {
        let _ = health.send(ServiceHealth::Starting);
        match run_generation(&config, &mut receiver, &health, &mut startup).await {
            Ok(WorkerExit::Shutdown) => {
                if let Some(startup) = startup.take() {
                    let _ = startup.send(Err(unavailable(
                        "agent provider shut down before becoming ready",
                    )));
                }
                return;
            }
            Err(error) => {
                let first_start = startup.is_some();
                if let Some(startup) = startup.take() {
                    let _ = startup.send(Err(error));
                }
                let _ = health.send(ServiceHealth::Failed);
                if first_start {
                    return;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

enum WorkerExit {
    Shutdown,
}

async fn run_generation(
    config: &AgentProviderConfig,
    receiver: &mut mpsc::Receiver<WorkerCommand>,
    health: &watch::Sender<ServiceHealth>,
    startup: &mut Option<oneshot::Sender<Result<(), ProtocolError>>>,
) -> Result<WorkerExit, ProtocolError> {
    let mut child = tokio::process::Command::new(&config.binary)
        .args(["agent", "stdio"])
        .current_dir(&config.workspace)
        .envs(&config.environment)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| unavailable(format!("failed to spawn agent provider process: {error}")))?;
    let outgoing = child
        .stdin
        .take()
        .ok_or_else(|| unavailable("agent provider child has no stdin"))?
        .compat_write();
    let incoming = child
        .stdout
        .take()
        .ok_or_else(|| unavailable("agent provider child has no stdout"))?
        .compat();
    let captures = Rc::new(CaptureStore::default());
    let client = ProviderAcpClient {
        captures: captures.clone(),
    };
    let incoming = LineBufferedRead::spawn_local(incoming);
    let (connection, io) = acp::ClientSideConnection::new(client, outgoing, incoming, |future| {
        tokio::task::spawn_local(future);
    });
    let connection = Rc::new(connection);
    let (io_done_tx, mut io_done_rx) = mpsc::channel(1);
    tokio::task::spawn_local(async move {
        let result = io.await;
        let _ = io_done_tx
            .send(result.map_err(|error| error.to_string()))
            .await;
    });
    initialize_agent(&connection).await?;
    let _ = health.send(ServiceHealth::Ready);
    if let Some(startup) = startup.take() {
        let _ = startup.send(Ok(()));
    }

    let sessions = Rc::new(RefCell::new(HashMap::<String, acp::SessionId>::new()));
    let session_locks = Rc::new(RefCell::new(
        HashMap::<String, Rc<tokio::sync::Mutex<()>>>::new(),
    ));
    let active = Rc::new(RefCell::new(HashMap::<RequestId, acp::SessionId>::new()));

    loop {
        tokio::select! {
            io_result = io_done_rx.recv() => {
                let detail = match io_result {
                    Some(Err(detail)) => detail,
                    Some(Ok(())) => "agent provider process exited".to_owned(),
                    None => "agent provider I/O monitor stopped".to_owned(),
                };
                let _ = child.start_kill();
                return Err(unavailable(detail));
            }
            command = receiver.recv() => {
                let Some(command) = command else {
                    let _ = child.start_kill();
                    return Ok(WorkerExit::Shutdown);
                };
                match command {
                    WorkerCommand::Shutdown => {
                        let _ = child.start_kill();
                        return Ok(WorkerExit::Shutdown);
                    }
                    WorkerCommand::Cancel { request_id, respond_to } => {
                        let result = if let Some(session_id) = active.borrow().get(&request_id).cloned() {
                            connection
                                .cancel(acp::CancelNotification::new(session_id))
                                .await
                                .map_err(|error| unavailable(format!("agent cancellation failed: {error}")))
                        } else {
                            Err(ProtocolError::new(
                                ProtocolErrorCode::NotFound,
                                format!("active agent request {request_id} was not found"),
                            ))
                        };
                        let _ = respond_to.send(result);
                    }
                    WorkerCommand::Execute { dispatch, respond_to } => {
                        let connection = connection.clone();
                        let captures = captures.clone();
                        let sessions = sessions.clone();
                        let session_locks = session_locks.clone();
                        let active = active.clone();
                        let config = config.clone();
                        tokio::task::spawn_local(async move {
                            let result = execute_turn(
                                &connection,
                                &captures,
                                &sessions,
                                &session_locks,
                                &active,
                                &config,
                                dispatch,
                            )
                            .await;
                            let _ = respond_to.send(result);
                        });
                    }
                }
            }
        }
    }
}

async fn initialize_agent(connection: &acp::ClientSideConnection) -> Result<(), ProtocolError> {
    let response = tokio::time::timeout(
        STARTUP_TIMEOUT,
        connection.initialize(
            acp::InitializeRequest::new(acp::ProtocolVersion::V1)
                .client_capabilities(
                    acp::ClientCapabilities::new()
                        .fs(acp::FileSystemCapabilities::new())
                        .terminal(false),
                )
                .meta(
                    serde_json::json!({
                        "startupHints": {
                            "nonInteractive": true,
                            "skipGitStatus": true,
                            "skipProjectLayout": true
                        },
                        "clientType": "grokd-provider",
                        "clientVersion": env!("CARGO_PKG_VERSION")
                    })
                    .as_object()
                    .cloned(),
                ),
        ),
    )
    .await
    .map_err(|_| unavailable("agent provider initialize timed out"))?
    .map_err(|error| unavailable(format!("agent provider initialize failed: {error}")))?;
    if let Some(method) = response
        .auth_methods
        .iter()
        .find(|method| method.id().0.as_ref() == "xai.api_key")
        .or(response.auth_methods.first())
    {
        connection
            .authenticate(
                acp::AuthenticateRequest::new(method.id().clone())
                    .meta(serde_json::json!({"headless": true}).as_object().cloned()),
            )
            .await
            .map_err(|error| {
                unavailable(format!("agent provider authentication failed: {error}"))
            })?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn execute_turn(
    connection: &acp::ClientSideConnection,
    captures: &CaptureStore,
    sessions: &RefCell<HashMap<String, acp::SessionId>>,
    session_locks: &RefCell<HashMap<String, Rc<tokio::sync::Mutex<()>>>>,
    active: &RefCell<HashMap<RequestId, acp::SessionId>>,
    config: &AgentProviderConfig,
    dispatch: ProviderDispatch,
) -> Result<ProviderOutput, ProtocolError> {
    let input: AgentTurnInput =
        serde_json::from_value(dispatch.task.input.clone()).map_err(|error| {
            ProtocolError::new(
                ProtocolErrorCode::InvalidEnvelope,
                format!("invalid agent.turn input: {error}"),
            )
        })?;
    input.validate(&config.workspace)?;
    let session_key = input
        .session_key
        .clone()
        .unwrap_or_else(|| dispatch.engagement_id.to_string());
    let lock = session_locks
        .borrow_mut()
        .entry(session_key.clone())
        .or_insert_with(|| Rc::new(tokio::sync::Mutex::new(())))
        .clone();
    let _session_guard = lock.lock().await;
    let workspace = input.workspace.as_deref().unwrap_or(&config.workspace);
    let session_id = if let Some(session_id) = sessions.borrow().get(&session_key).cloned() {
        session_id
    } else {
        let model_id = input.model_id.as_ref().or(config.model_id.as_ref());
        let mut request = acp::NewSessionRequest::new(workspace.to_path_buf()).mcp_servers(vec![]);
        if let Some(model_id) = model_id {
            request = request.meta(
                serde_json::json!({"modelId": model_id})
                    .as_object()
                    .cloned(),
            );
        }
        let response = connection
            .new_session(request)
            .await
            .map_err(|error| unavailable(format!("agent session creation failed: {error}")))?;
        sessions
            .borrow_mut()
            .insert(session_key.clone(), response.session_id.clone());
        response.session_id
    };
    active
        .borrow_mut()
        .insert(dispatch.request_id.clone(), session_id.clone());
    captures.begin(&session_id);
    let remaining_ms = dispatch.task.deadline_unix_ms.saturating_sub(now_unix_ms());
    let prompt = tokio::time::timeout(
        Duration::from_millis(remaining_ms.max(1)),
        connection.prompt(acp::PromptRequest::new(
            session_id.clone(),
            vec![acp::ContentBlock::Text(acp::TextContent::new(
                input.request,
            ))],
        )),
    )
    .await;
    active.borrow_mut().remove(&dispatch.request_id);
    let capture = captures.finish(&session_id);
    let response = prompt
        .map_err(|_| {
            ProtocolError::new(
                ProtocolErrorCode::DeadlineExceeded,
                "agent turn exceeded its durable task deadline",
            )
        })?
        .map_err(|error| unavailable(format!("agent turn failed: {error}")))?;
    let output = serde_json::json!({
        "session_key": session_key,
        "agent_session_id": session_id.0.as_ref(),
        "stop_reason": format!("{:?}", response.stop_reason),
        "text": capture.text,
        "notifications": capture.notifications,
    });
    Ok(ProviderOutput {
        output,
        observations: Vec::new(),
        artifacts: Vec::new(),
    })
}

fn manifest(config: &AgentProviderConfig) -> CapabilityManifest {
    let mut platforms = BTreeSet::new();
    platforms.insert(Platform {
        os: std::env::consts::OS.to_owned(),
        architecture: std::env::consts::ARCH.to_owned(),
        accelerator: None,
    });
    CapabilityManifest {
        provider_id: ProviderId::from_string("agent-runtime"),
        provider_version: env!("CARGO_PKG_VERSION").to_owned(),
        protocol: VersionRange::exact(PROTOCOL_VERSION),
        kind: ProviderKind::ModelRuntime,
        features: [
            "persistent_acp_worker",
            "session_continuity",
            "tool_loop",
            "turn_cooperation",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect(),
        operations: vec![OperationDescriptor {
            operation_id: AGENT_TURN_OPERATION.into(),
            display_name: "Execute coordinated agent turn".to_owned(),
            input_schema: serde_json::json!({
                "type": "object",
                "required": ["request"],
                "properties": {
                    "request": {"type": "string"},
                    "session_key": {"type": "string"},
                    "workspace": {"type": "string"},
                    "model_id": {"type": "string"}
                },
                "additionalProperties": false
            }),
            output_schema: serde_json::json!({
                "type": "object",
                "required": ["agent_session_id", "stop_reason", "text", "notifications"]
            }),
            streaming: false,
            interactive: true,
            deferred: true,
        }],
        concurrency: ConcurrencyProfile {
            maximum_parallel: config.maximum_parallel,
            queue_capacity: config.queue_capacity,
            exclusive_resource: Some("agent-runtime".to_owned()),
        },
        cancellation: CancellationSemantics::Cooperative,
        recovery: RecoverySemantics::Restartable,
        artifacts: ArtifactContract::Required,
        platforms,
        metadata: serde_json::Map::from_iter([
            ("transport".to_owned(), "acp_stdio".into()),
            ("persistent_process".to_owned(), true.into()),
            (
                "binary".to_owned(),
                config.binary.display().to_string().into(),
            ),
        ]),
    }
}

fn unavailable(message: impl Into<String>) -> ProtocolError {
    ProtocolError::new(ProtocolErrorCode::ServiceUnavailable, message).retryable()
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
