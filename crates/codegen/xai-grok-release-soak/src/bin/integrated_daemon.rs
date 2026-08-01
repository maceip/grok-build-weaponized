//! End-to-end qualification of the shipped local execution boundary.
//!
//! Unlike component fixtures, this gate starts the real `grokd`, persistent
//! ACP agent, isolated LiteRT-LM worker, journal, projections, and artifact
//! store. It fails closed unless the selected model is a readable local
//! LiteRT artifact and proves the completed result survives a daemon restart.

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail, ensure};
use clap::Parser;
use serde::Serialize;
use tokio::process::{Child, Command};
use xai_grok_control_client::{ClientIdentity, ControlPlaneClient};
use xai_grok_protocol::{
    ArtifactId, Command as ControlCommand, CommandId, EngagementId, Event, EventReadRequest,
    IngressEnvelope, IngressSource, ProjectionQuery, Response, TaskGraphProjection, TaskStatus,
    WorkspaceId,
};
use xai_grok_runtime::LiteRtLmConfig;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const CONTROL_TIMEOUT: Duration = Duration::from_secs(15);
const ARTIFACT_CHUNK_BYTES: u32 = 256 * 1024;

#[derive(Debug, Parser)]
#[command(
    name = "grok-integrated-qualifier",
    about = "Qualify the real grokd -> ACP agent -> isolated LiteRT worker path"
)]
struct Arguments {
    /// Directory containing config.toml for the local-only model selection.
    #[arg(long, env = "GROK_HOME")]
    grok_home: PathBuf,

    /// Compiled grokd executable under qualification.
    #[arg(long, env = "GROKD_BINARY")]
    grokd: PathBuf,

    /// Compiled Grok ACP agent executable under qualification.
    #[arg(long, env = "GROK_AGENT_BINARY")]
    agent: PathBuf,

    /// Compiled isolated LiteRT runtime worker executable under qualification.
    #[arg(long, env = "GROK_LOCAL_RUNTIME_WORKER")]
    runtime_worker: PathBuf,

    /// Model entry in GROK_HOME/config.toml. HTTP-backed entries are rejected.
    #[arg(long, default_value = "local-qwen2.5-1.5b-chat")]
    model_id: String,

    /// Workspace supplied to the daemon-owned agent.
    #[arg(long, default_value = ".")]
    workspace: PathBuf,

    /// Parent for immutable qualification runs. A unique run directory is added.
    #[arg(long, default_value = "target/integrated-qualification")]
    state_root: PathBuf,

    /// Maximum wall-clock time for the real model turn.
    #[arg(long, default_value_t = 300)]
    turn_timeout_seconds: u64,

    /// A direct, deterministic request that does not authorize tool execution.
    #[arg(
        long,
        default_value = "Reply with one short sentence confirming that local inference is operational. Do not use tools."
    )]
    prompt: String,
}

#[derive(Debug, Serialize)]
struct QualificationReport {
    qualified: bool,
    run_directory: PathBuf,
    model_id: String,
    model_path: PathBuf,
    library_path: PathBuf,
    backend: String,
    engagement_id: String,
    task_id: String,
    artifact_id: String,
    artifact_hash: String,
    artifact_bytes: usize,
    response_text_bytes: usize,
    runtime_completed_requests: u64,
    runtime_resident_workers: u64,
    first_daemon_seconds: f64,
    restart_seconds: f64,
    deduplicated_after_restart: bool,
    artifact_identical_after_restart: bool,
    no_task_redispatch_after_restart: bool,
}

struct Daemon {
    child: Child,
    socket: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let arguments = Arguments::parse();
    ensure!(
        arguments.turn_timeout_seconds > 0,
        "turn timeout must be positive"
    );
    ensure!(
        !arguments.prompt.trim().is_empty(),
        "prompt must not be empty"
    );

    let grok_home = canonical_directory(&arguments.grok_home, "GROK_HOME")?;
    let grokd = canonical_file(&arguments.grokd, "grokd")?;
    let agent = canonical_file(&arguments.agent, "agent")?;
    let runtime_worker = canonical_file(&arguments.runtime_worker, "runtime worker")?;
    let workspace = canonical_directory(&arguments.workspace, "workspace")?;
    let local_model = load_local_model(&grok_home, &arguments.model_id)?;

    let state_root = absolute_path(&arguments.state_root)?;
    std::fs::create_dir_all(&state_root)
        .with_context(|| format!("create qualification root {}", state_root.display()))?;
    let run_directory = create_run_directory(&state_root)?;
    let state_directory = run_directory.join("state");
    std::fs::create_dir(&state_directory)
        .with_context(|| format!("create daemon state {}", state_directory.display()))?;
    let socket = qualification_socket(&run_directory);
    ensure!(
        !socket.exists(),
        "qualification socket unexpectedly exists: {}",
        socket.display()
    );
    let daemon_log = run_directory.join("grokd.log");

    let first_started = Instant::now();
    let mut daemon = spawn_daemon(DaemonSpec {
        grokd: &grokd,
        agent: &agent,
        runtime_worker: &runtime_worker,
        grok_home: &grok_home,
        workspace: &workspace,
        model_id: &arguments.model_id,
        state_directory: &state_directory,
        socket: &socket,
        log_path: &daemon_log,
    })?;
    let client = connect_with_retry(&mut daemon, CONNECT_TIMEOUT)
        .await
        .with_context(|| daemon_failure_context(&daemon_log))?;
    await_warm_runtime(
        &client,
        &arguments.model_id,
        Instant::now() + Duration::from_secs(180),
    )
    .await
    .with_context(|| daemon_failure_context(&daemon_log))?;

    let ingress = qualification_ingress(&arguments.prompt, &workspace);
    let (engagement_id, accepted) = submit_ingress(&client, ingress.clone()).await?;
    ensure!(
        accepted,
        "fresh qualification ingress was unexpectedly deduplicated"
    );

    let turn_deadline = Instant::now() + Duration::from_secs(arguments.turn_timeout_seconds);
    let completion = await_completed_turn(&client, &engagement_id, turn_deadline)
        .await
        .with_context(|| daemon_failure_context(&daemon_log))?;
    let artifact = read_artifact(&client, &completion.artifact_id).await?;
    let provider_output: serde_json::Value =
        serde_json::from_slice(&artifact).context("provider output artifact is not valid JSON")?;
    let response_text = provider_output
        .get("text")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow!("provider output artifact has no text field"))?;
    ensure!(
        !response_text.trim().is_empty(),
        "real local model returned an empty response"
    );
    ensure!(
        provider_output
            .get("agent_session_id")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| !value.trim().is_empty()),
        "provider output omitted the real ACP session id"
    );

    let graph = task_graph(&client, &engagement_id).await?;
    ensure_completed_graph(&graph, &completion.task_id, &completion.artifact_id)?;
    let runtime = runtime_evidence(&client).await?;
    let first_daemon_seconds = first_started.elapsed().as_secs_f64();
    let high_watermark = graph.last_sequence;

    shutdown_daemon(client, &mut daemon, &daemon_log).await?;

    let restart_started = Instant::now();
    let mut restarted = spawn_daemon(DaemonSpec {
        grokd: &grokd,
        agent: &agent,
        runtime_worker: &runtime_worker,
        grok_home: &grok_home,
        workspace: &workspace,
        model_id: &arguments.model_id,
        state_directory: &state_directory,
        socket: &socket,
        log_path: &daemon_log,
    })?;
    let restarted_client = connect_with_retry(&mut restarted, CONNECT_TIMEOUT)
        .await
        .with_context(|| daemon_failure_context(&daemon_log))?;
    let restarted_graph = task_graph(&restarted_client, &engagement_id).await?;
    ensure_completed_graph(
        &restarted_graph,
        &completion.task_id,
        &completion.artifact_id,
    )?;
    let restarted_artifact = read_artifact(&restarted_client, &completion.artifact_id).await?;
    ensure!(
        artifact == restarted_artifact,
        "provider output artifact changed across daemon restart"
    );

    let (_, accepted_after_restart) = submit_ingress(&restarted_client, ingress).await?;
    ensure!(
        !accepted_after_restart,
        "identical ingress was reaccepted after daemon restart"
    );
    let no_task_redispatch_after_restart =
        no_redispatch_after(&restarted_client, &engagement_id, high_watermark).await?;
    ensure!(
        no_task_redispatch_after_restart,
        "completed task was redispatched during recovery"
    );
    let restart_seconds = restart_started.elapsed().as_secs_f64();

    shutdown_daemon(restarted_client, &mut restarted, &daemon_log).await?;
    if socket.exists() {
        std::fs::remove_file(&socket)
            .with_context(|| format!("remove qualification socket {}", socket.display()))?;
    }

    let report = QualificationReport {
        qualified: true,
        run_directory: run_directory.clone(),
        model_id: arguments.model_id,
        model_path: local_model.model_path,
        library_path: local_model.library_path,
        backend: local_model.backend,
        engagement_id: engagement_id.to_string(),
        task_id: completion.task_id.to_string(),
        artifact_id: completion.artifact_id.to_string(),
        artifact_hash: blake3::hash(&artifact).to_hex().to_string(),
        artifact_bytes: artifact.len(),
        response_text_bytes: response_text.len(),
        runtime_completed_requests: runtime.completed_requests,
        runtime_resident_workers: runtime.resident_workers,
        first_daemon_seconds,
        restart_seconds,
        deduplicated_after_restart: true,
        artifact_identical_after_restart: true,
        no_task_redispatch_after_restart,
    };
    let report_bytes = serde_json::to_vec_pretty(&report)?;
    std::fs::write(run_directory.join("qualification.json"), &report_bytes)
        .context("write qualification report")?;
    println!("{}", String::from_utf8_lossy(&report_bytes));
    Ok(())
}

#[derive(Clone, Debug)]
struct LocalModelEvidence {
    model_path: PathBuf,
    library_path: PathBuf,
    backend: String,
}

fn load_local_model(grok_home: &Path, model_id: &str) -> Result<LocalModelEvidence> {
    ensure!(!model_id.trim().is_empty(), "model id must not be empty");
    let config_path = grok_home.join("config.toml");
    let config = std::fs::read_to_string(&config_path)
        .with_context(|| format!("read {}", config_path.display()))?;
    let document: toml::Value =
        toml::from_str(&config).with_context(|| format!("parse {}", config_path.display()))?;
    let model = document
        .get("model")
        .and_then(toml::Value::as_table)
        .and_then(|models| models.get(model_id))
        .and_then(toml::Value::as_table)
        .ok_or_else(|| {
            anyhow!(
                "model `{model_id}` is absent from {}",
                config_path.display()
            )
        })?;
    let base_url = model
        .get("base_url")
        .and_then(toml::Value::as_str)
        .ok_or_else(|| anyhow!("model `{model_id}` has no base_url"))?;
    let parsed = LiteRtLmConfig::from_base_url(base_url)
        .map_err(|error| anyhow!("invalid local model `{model_id}`: {error}"))?
        .ok_or_else(|| {
            anyhow!(
                "model `{model_id}` uses a non-local URL; integrated qualification requires litert-lm://"
            )
        })?;
    let model_path = canonical_file(&parsed.model_path, "LiteRT model")?;
    let library_path = canonical_file(&parsed.library_path, "LiteRT library")?;
    ensure!(
        parsed.max_context_tokens.is_some_and(|window| window > 0),
        "model `{model_id}` must declare max_context_tokens"
    );
    Ok(LocalModelEvidence {
        model_path,
        library_path,
        backend: parsed.backend,
    })
}

struct DaemonSpec<'a> {
    grokd: &'a Path,
    agent: &'a Path,
    runtime_worker: &'a Path,
    grok_home: &'a Path,
    workspace: &'a Path,
    model_id: &'a str,
    state_directory: &'a Path,
    socket: &'a Path,
    log_path: &'a Path,
}

fn spawn_daemon(spec: DaemonSpec<'_>) -> Result<Daemon> {
    let stdout = append_log(spec.log_path)?;
    let stderr = stdout.try_clone().context("clone daemon log handle")?;
    let child = Command::new(spec.grokd)
        .arg("--state-directory")
        .arg(spec.state_directory)
        .arg("--socket")
        .arg(spec.socket)
        .arg("--agent-binary")
        .arg(spec.agent)
        .arg("--agent-workspace")
        .arg(spec.workspace)
        .arg("--agent-model")
        .arg(spec.model_id)
        .arg("--local-runtime-worker")
        .arg(spec.runtime_worker)
        .env("GROK_HOME", spec.grok_home)
        .env("GROK_EXECUTION_BACKEND", "daemon")
        .env("GROK_LOCAL_RUNTIME_MODE", "worker")
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("spawn {}", spec.grokd.display()))?;
    Ok(Daemon {
        child,
        socket: spec.socket.to_path_buf(),
    })
}

async fn connect_with_retry(daemon: &mut Daemon, timeout: Duration) -> Result<ControlPlaneClient> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = daemon.child.try_wait().context("inspect grokd process")? {
            bail!("grokd exited before accepting connections: {status}");
        }
        match ControlPlaneClient::connect(
            &daemon.socket,
            ClientIdentity::new("grok-integrated-qualifier", env!("CARGO_PKG_VERSION")),
        )
        .await
        {
            Ok(client) => return Ok(client),
            Err(error) if Instant::now() >= deadline => {
                bail!(
                    "grokd did not accept a protocol handshake within {} seconds: {error}",
                    timeout.as_secs()
                );
            }
            Err(_) => {}
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn qualification_ingress(prompt: &str, workspace: &Path) -> IngressEnvelope {
    let nonce = format!("{}-{}", std::process::id(), now_unix_ms());
    IngressEnvelope {
        command_id: CommandId::new(),
        source: IngressSource::Cli,
        source_event_id: format!("integrated-source-{nonce}"),
        workspace_id: WorkspaceId::from_string(format!(
            "integrated:{}",
            blake3::hash(workspace.to_string_lossy().as_bytes()).to_hex()
        )),
        exercise_id: None,
        operation_run_id: None,
        operator_session_id: None,
        session_id: format!("integrated-session-{nonce}"),
        prompt_id: format!("integrated-prompt-{nonce}"),
        request: prompt.to_owned(),
        team: None,
        metadata: serde_json::Map::new(),
    }
}

async fn submit_ingress(
    client: &ControlPlaneClient,
    ingress: IngressEnvelope,
) -> Result<(EngagementId, bool)> {
    match client
        .send(ControlCommand::SubmitIngress(ingress), CONTROL_TIMEOUT)
        .await?
    {
        Response::Accepted {
            engagement_id,
            accepted,
        } => Ok((engagement_id, accepted)),
        response => bail!("submit ingress returned unexpected response: {response:?}"),
    }
}

struct CompletedTurn {
    task_id: xai_grok_protocol::TaskId,
    artifact_id: ArtifactId,
}

async fn await_completed_turn(
    client: &ControlPlaneClient,
    engagement_id: &EngagementId,
    deadline: Instant,
) -> Result<CompletedTurn> {
    let mut cursor = 0;
    let mut completed_task: Option<xai_grok_protocol::TaskId> = None;
    let mut output_artifact: Option<ArtifactId> = None;
    loop {
        if let (Some(task_id), Some(artifact_id)) = (&completed_task, &output_artifact) {
            return Ok(CompletedTurn {
                task_id: task_id.clone(),
                artifact_id: artifact_id.clone(),
            });
        }
        ensure!(Instant::now() < deadline, "real model turn timed out");
        let remaining = deadline.saturating_duration_since(Instant::now());
        let wait_ms = remaining
            .min(Duration::from_secs(2))
            .as_millis()
            .try_into()
            .unwrap_or(2_000);
        let batch = client
            .read_events(EventReadRequest {
                after_sequence: cursor,
                maximum_events: 256,
                wait_ms,
                engagement_id: Some(engagement_id.clone()),
            })
            .await?;
        cursor = batch.next_sequence;
        for envelope in batch.events {
            match envelope.event {
                Event::TaskStatus {
                    task_id,
                    status: TaskStatus::Completed,
                    ..
                } => completed_task = Some(task_id),
                Event::TaskStatus {
                    task_id, status, ..
                } if matches!(
                    status,
                    TaskStatus::Failed | TaskStatus::Cancelled | TaskStatus::Lost
                ) =>
                {
                    bail!("task {task_id} reached terminal failure state {status:?}")
                }
                Event::ProviderOutput {
                    task_id: _,
                    artifact_id,
                } => output_artifact = Some(artifact_id),
                _ => {}
            }
        }
    }
}

async fn read_artifact(client: &ControlPlaneClient, artifact_id: &ArtifactId) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut cursor = 0;
    loop {
        match client
            .send(
                ControlCommand::ReadArtifact {
                    artifact_id: artifact_id.clone(),
                    cursor,
                    limit: ARTIFACT_CHUNK_BYTES,
                },
                CONTROL_TIMEOUT,
            )
            .await?
        {
            Response::ArtifactChunk {
                artifact_id: returned_id,
                byte_size,
                cursor: returned_cursor,
                bytes: chunk,
                next_cursor,
                ..
            } => {
                ensure!(returned_id == *artifact_id, "artifact response id changed");
                ensure!(returned_cursor == cursor, "artifact cursor changed");
                bytes.extend_from_slice(&chunk);
                if let Some(next) = next_cursor {
                    ensure!(next > cursor, "artifact cursor did not advance");
                    cursor = next;
                } else {
                    ensure!(
                        bytes.len() as u64 == byte_size,
                        "artifact byte size differs from descriptor"
                    );
                    return Ok(bytes);
                }
            }
            response => bail!("read artifact returned unexpected response: {response:?}"),
        }
    }
}

async fn task_graph(
    client: &ControlPlaneClient,
    engagement_id: &EngagementId,
) -> Result<TaskGraphProjection> {
    match client
        .send(
            ControlCommand::QueryProjection(ProjectionQuery::TaskGraph {
                engagement_id: engagement_id.clone(),
            }),
            CONTROL_TIMEOUT,
        )
        .await?
    {
        Response::Projection(snapshot) => {
            serde_json::from_value(snapshot.value).context("decode durable task graph projection")
        }
        response => bail!("task graph query returned unexpected response: {response:?}"),
    }
}

fn ensure_completed_graph(
    graph: &TaskGraphProjection,
    task_id: &xai_grok_protocol::TaskId,
    artifact_id: &ArtifactId,
) -> Result<()> {
    let task = graph
        .tasks
        .iter()
        .find(|task| task.task.task_id == *task_id)
        .ok_or_else(|| anyhow!("completed task is absent from durable projection"))?;
    ensure!(
        task.status == TaskStatus::Completed,
        "durable task is not completed"
    );
    ensure!(
        task.artifacts
            .iter()
            .any(|artifact| artifact.provider_output && artifact.artifact_id == *artifact_id),
        "durable task projection does not reference its provider output artifact"
    );
    Ok(())
}

struct RuntimeEvidence {
    completed_requests: u64,
    resident_workers: u64,
}

async fn await_warm_runtime(
    client: &ControlPlaneClient,
    selected_model: &str,
    deadline: Instant,
) -> Result<()> {
    let mut last_detail = "runtime telemetry not yet available".to_owned();
    let mut stable_topology: Option<(String, Instant)> = None;
    loop {
        ensure!(
            Instant::now() < deadline,
            "both local models did not become resident before the startup deadline: {last_detail}"
        );
        match capacity_value(client).await {
            Ok(value) => {
                let runtime = value
                    .get("providers")
                    .and_then(serde_json::Value::as_array)
                    .and_then(|providers| {
                        providers.iter().find(|provider| {
                            provider
                                .get("provider_id")
                                .and_then(serde_json::Value::as_str)
                                == Some("agent-runtime")
                        })
                    })
                    .and_then(|agent| agent.pointer("/status/runtime"));
                if let Some(runtime) = runtime {
                    let capacity = runtime.get("capacity");
                    let worker_mode = capacity
                        .and_then(|value| value.get("mode"))
                        .and_then(serde_json::Value::as_str)
                        == Some("worker");
                    let resident_workers = capacity
                        .and_then(|value| value.get("resident_workers"))
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or(0);
                    let workers = runtime
                        .get("workers")
                        .and_then(serde_json::Value::as_object);
                    let selected_resident = workers.is_some_and(|workers| {
                        workers.keys().any(|key| key.contains(selected_model))
                    });
                    let warm_model_count = workers.map_or(0, serde_json::Map::len);
                    if worker_mode
                        && resident_workers >= 2
                        && warm_model_count >= 2
                        && selected_resident
                    {
                        let mut worker_keys = workers
                            .into_iter()
                            .flat_map(serde_json::Map::keys)
                            .cloned()
                            .collect::<Vec<_>>();
                        worker_keys.sort();
                        let topology = format!("{resident_workers}:{}", worker_keys.join("|"));
                        match &mut stable_topology {
                            Some((previous, since)) if *previous == topology => {
                                if since.elapsed() >= Duration::from_secs(3) {
                                    return Ok(());
                                }
                            }
                            slot => *slot = Some((topology, Instant::now())),
                        }
                    } else {
                        stable_topology = None;
                    }
                    last_detail = format!(
                        "mode={worker_mode} resident_workers={resident_workers} warm_models={warm_model_count} selected_resident={selected_resident}"
                    );
                } else {
                    last_detail = "agent-runtime has not published runtime telemetry".to_owned();
                }
            }
            Err(error) => last_detail = error.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn capacity_value(client: &ControlPlaneClient) -> Result<serde_json::Value> {
    match client
        .send(
            ControlCommand::QueryProjection(ProjectionQuery::Capacity),
            CONTROL_TIMEOUT,
        )
        .await?
    {
        Response::Projection(snapshot) => Ok(snapshot.value),
        response => bail!("capacity query returned unexpected response: {response:?}"),
    }
}

async fn runtime_evidence(client: &ControlPlaneClient) -> Result<RuntimeEvidence> {
    let value = capacity_value(client).await?;
    let providers = value
        .get("providers")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| anyhow!("capacity projection omitted provider telemetry"))?;
    let agent = providers
        .iter()
        .find(|provider| {
            provider
                .get("provider_id")
                .and_then(serde_json::Value::as_str)
                == Some("agent-runtime")
        })
        .ok_or_else(|| anyhow!("capacity projection omitted agent-runtime"))?;
    let capacity = agent
        .pointer("/status/runtime/capacity")
        .ok_or_else(|| anyhow!("agent-runtime omitted local runtime capacity"))?;
    ensure!(
        capacity.get("mode").and_then(serde_json::Value::as_str) == Some("worker"),
        "agent runtime did not use isolated worker mode"
    );
    let completed_requests = capacity
        .get("completed_requests")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| anyhow!("runtime omitted completed request count"))?;
    let resident_workers = capacity
        .get("resident_workers")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| anyhow!("runtime omitted resident worker count"))?;
    ensure!(
        completed_requests >= 1,
        "runtime did not record a completed request"
    );
    ensure!(
        resident_workers >= 1,
        "runtime has no resident isolated worker"
    );
    ensure!(
        agent
            .pointer("/status/runtime/workers")
            .and_then(serde_json::Value::as_object)
            .is_some_and(|workers| !workers.is_empty()),
        "runtime worker telemetry is empty"
    );
    Ok(RuntimeEvidence {
        completed_requests,
        resident_workers,
    })
}

async fn no_redispatch_after(
    client: &ControlPlaneClient,
    engagement_id: &EngagementId,
    sequence: u64,
) -> Result<bool> {
    let batch = client
        .read_events(EventReadRequest {
            after_sequence: sequence,
            maximum_events: 256,
            wait_ms: 250,
            engagement_id: Some(engagement_id.clone()),
        })
        .await?;
    Ok(!batch.events.iter().any(|envelope| {
        matches!(
            envelope.event,
            Event::TaskStatus {
                status: TaskStatus::Dispatched | TaskStatus::Running,
                ..
            }
        )
    }))
}

async fn shutdown_daemon(
    client: ControlPlaneClient,
    daemon: &mut Daemon,
    log_path: &Path,
) -> Result<()> {
    let response = client
        .send(ControlCommand::Shutdown, CONTROL_TIMEOUT)
        .await
        .with_context(|| daemon_failure_context(log_path))?;
    ensure!(matches!(response, Response::Ack), "grokd rejected shutdown");
    drop(client);
    match tokio::time::timeout(Duration::from_secs(10), daemon.child.wait()).await {
        Ok(status) => ensure!(status?.success(), "grokd exited unsuccessfully"),
        Err(_) => {
            daemon
                .child
                .kill()
                .await
                .context("terminate unresponsive grokd")?;
            bail!("grokd did not shut down within ten seconds")
        }
    }
    Ok(())
}

fn canonical_file(path: &Path, label: &str) -> Result<PathBuf> {
    let path =
        dunce::canonicalize(path).with_context(|| format!("resolve {label} {}", path.display()))?;
    ensure!(
        path.is_file(),
        "{label} is not a regular file: {}",
        path.display()
    );
    Ok(path)
}

fn canonical_directory(path: &Path, label: &str) -> Result<PathBuf> {
    let path =
        dunce::canonicalize(path).with_context(|| format!("resolve {label} {}", path.display()))?;
    ensure!(
        path.is_dir(),
        "{label} is not a directory: {}",
        path.display()
    );
    Ok(path)
}

fn absolute_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    Ok(std::env::current_dir()?.join(path))
}

fn create_run_directory(root: &Path) -> Result<PathBuf> {
    let run = root.join(format!("run-{}-{}", now_unix_ms(), std::process::id()));
    std::fs::create_dir(&run)
        .with_context(|| format!("create immutable run directory {}", run.display()))?;
    Ok(run)
}

fn qualification_socket(run_directory: &Path) -> PathBuf {
    let digest = blake3::hash(run_directory.to_string_lossy().as_bytes())
        .to_hex()
        .to_string();
    std::env::temp_dir().join(format!(
        "grok-q-{}-{}.sock",
        std::process::id(),
        &digest[..16]
    ))
}

fn append_log(path: &Path) -> Result<File> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open daemon log {}", path.display()))
}

fn daemon_failure_context(path: &Path) -> String {
    let tail = std::fs::read_to_string(path)
        .map(|content| {
            let lines = content.lines().collect::<Vec<_>>();
            lines[lines.len().saturating_sub(80)..].join("\n")
        })
        .unwrap_or_else(|error| format!("<unable to read log: {error}>"));
    format!("grokd log {}:\n{tail}", path.display())
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_directory(label: &str) -> Result<PathBuf> {
        let path = std::env::temp_dir().join(format!(
            "grok-integrated-qualifier-{label}-{}-{}",
            std::process::id(),
            now_unix_ms()
        ));
        std::fs::create_dir(&path)?;
        Ok(path)
    }

    #[test]
    fn local_model_validation_rejects_http() -> Result<()> {
        let home = temp_directory("http")?;
        std::fs::write(
            home.join("config.toml"),
            "[model.test]\nbase_url = \"http://127.0.0.1:8000\"\n",
        )?;
        let error = load_local_model(&home, "test").expect_err("HTTP must fail closed");
        assert!(error.to_string().contains("requires litert-lm://"));
        std::fs::remove_dir_all(home)?;
        Ok(())
    }

    #[test]
    fn local_model_validation_requires_real_assets() -> Result<()> {
        let home = temp_directory("missing")?;
        std::fs::write(
            home.join("config.toml"),
            "[model.test]\nbase_url = \"litert-lm:///missing/model?library=/missing/library&max_context_tokens=4096\"\n",
        )?;
        let error = load_local_model(&home, "test").expect_err("missing assets must fail");
        assert!(error.to_string().contains("LiteRT model"));
        std::fs::remove_dir_all(home)?;
        Ok(())
    }

    #[test]
    fn qualification_socket_fits_macos_unix_socket_limit() {
        let deliberately_long_run = PathBuf::from("/private/tmp")
            .join("nested-qualification-root".repeat(12))
            .join("run-1234567890-12345");
        let socket = qualification_socket(&deliberately_long_run);
        assert!(socket.to_string_lossy().len() < 100);
        assert!(socket.starts_with(std::env::temp_dir()));
    }
}
