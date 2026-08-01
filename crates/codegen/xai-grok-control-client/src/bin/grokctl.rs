use std::collections::VecDeque;
use std::io::{Read as _, Seek as _, Write as _};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use xai_grok_control_client::{ClientIdentity, ControlPlaneClient};
use xai_grok_protocol::{
    ArtifactId, ChannelId, ClaimTeamResource, ClientId, Command, CommandId, CreateExercise,
    CreateFinding, CreateOperationRun, CreateOperatorSession, CreatePlaybook, CreateTeamWorkItem,
    EventReadRequest, EvidenceId, ExerciseId, FindingSeverity, FindingStatus, IngressEnvelope,
    IngressSource, OperationId, OperationRunId, OperatorSessionId, PlaybookId, PlaybookStep,
    PostTeamMessage, ProjectionQuery, ProviderId, RecordExerciseEvidence, RequestId,
    ResourceClaimId, Response, RuntimeProfile, SetTeamPresence, TargetId, TaskId, TeamClient,
    TeamId, TeamPresenceState, TeamWorkItemId, TeamWorkItemStatus, WorkspaceId,
    parse_scope_targets,
};

const USAGE: &str = "\
grokctl [--socket PATH] [--team ID --client ID] COMMAND

Commands:
  status [capacity|providers|catalog [WORKSPACE]|exercise ID|engagement ID|tasks ENGAGEMENT_ID]
  events [--after N] [--limit N] [--wait-ms N] [--engagement ID] [--follow]
  exercise create --workspace ID --name NAME --objective TEXT --scope SELECTORS
  playbook create --workspace ID --name NAME --description TEXT --steps FILE|-
  run create --exercise ID [--playbook ID] --name NAME --objective TEXT
  session create --exercise ID [--run ID] --name NAME --purpose TEXT
  evidence record --exercise ID [--run ID] [--session ID] [--task ID] [--artifact ID] --finding TEXT --confidence 0..1 [--attribute K=V ...]
  finding create --exercise ID [--run ID] --title TEXT --summary TEXT --severity LEVEL [--evidence IDS] [--targets IDS]
  finding status ID candidate|confirmed|remediated|rejected
  team status ID
  team presence --team ID --client ID [--display-name NAME] [--state online|away|offline] [--workspace ID] [--exercise ID] [--run ID] [--session ID]
  team work create --team ID [--exercise ID] [--run ID] --title TEXT --objective TEXT [--assignee CLIENT]
  team work assign ID CLIENT|- --revision N
  team work status ID STATUS --revision N
  team message --team ID --channel ID --sender CLIENT --body TEXT [--reply-to ID]
  team claim --team ID --owner CLIENT --resource KEY --lease-ms N
  team release CLAIM_ID --owner CLIENT --revision N
  job start --exec PATH [--arg VALUE ...] [--cwd PATH] [--env K=V ...] [--timeout-ms N]
  job nmap --target TARGET --scope SELECTOR[,SELECTOR] [--profile host_discovery|tcp_connect|service_discovery] [--ports LIST] [--timeout-ms N]
  job status ID
  job wait ID [--wait-ms N]
  job output ID [--cursor N] [--records N] [--bytes N] [--raw]
  job result ID
  job cancel ID
  job cleanup ID
  submit --workspace ID [--exercise ID] [--run ID] --session ID --request TEXT|-
  artifact put FILE --media-type TYPE [--content-hash BLAKE3] [--chunk-bytes N]
  artifact resume UPLOAD_ID FILE [--chunk-bytes N]
  artifact status UPLOAD_ID
  artifact abort UPLOAD_ID
  artifact read ID [--cursor N] [--limit N] [--all] [--raw]
  profile lint FILE
";

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("grokctl: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1).collect::<VecDeque<_>>();
    if arguments.is_empty()
        || arguments
            .front()
            .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print!("{USAGE}");
        return Ok(());
    }
    let socket = take_leading_option(&mut arguments, "--socket")?
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("GROKD_SOCKET").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from(".grok/grokd.sock"));
    let team_option = take_leading_option(&mut arguments, "--team")?;
    let client = take_leading_option(&mut arguments, "--client")?;
    let display_name = take_leading_option(&mut arguments, "--display-name")?;
    let command = arguments.pop_front().ok_or("missing command")?;

    if command == "profile" {
        return lint_profile(arguments);
    }

    let mut identity = ClientIdentity::new("grokctl", env!("CARGO_PKG_VERSION"));
    match (team_option, client) {
        (Some(team_id), Some(client_id)) => {
            identity.team = Some(TeamClient {
                team_id: TeamId::from_string(team_id),
                client_id: ClientId::from_string(client_id),
                display_name,
            });
        }
        (None, None) if display_name.is_none() => {}
        _ => return Err("--team and --client must be supplied together".into()),
    }
    let control = ControlPlaneClient::connect(socket, identity).await?;
    match command.as_str() {
        "status" => status(&control, arguments).await,
        "events" => events(&control, arguments).await,
        "exercise" => create_exercise(&control, arguments).await,
        "playbook" => create_playbook(&control, arguments).await,
        "run" => create_run(&control, arguments).await,
        "session" => create_session(&control, arguments).await,
        "evidence" => record_evidence(&control, arguments).await,
        "finding" => finding(&control, arguments).await,
        "team" => team(&control, arguments).await,
        "job" => job(&control, arguments).await,
        "submit" => submit(&control, arguments).await,
        "artifact" => artifact(&control, arguments).await,
        _ => Err(format!("unknown command {command:?}\n{USAGE}").into()),
    }
}

async fn job(
    control: &ControlPlaneClient,
    mut arguments: VecDeque<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    match arguments.pop_front().as_deref() {
        Some("start") => start_job(control, arguments).await,
        Some("nmap") => start_nmap_job(control, arguments).await,
        Some("status") => invoke_job_with_id(control, "native.command.status", arguments).await,
        Some("result") => invoke_job_with_id(control, "native.nmap.result", arguments).await,
        Some("cancel") => invoke_job_with_id(control, "native.command.cancel", arguments).await,
        Some("cleanup") => invoke_job_with_id(control, "native.command.cleanup", arguments).await,
        Some("wait") => wait_for_job(control, arguments).await,
        Some("output") => read_job_output(control, arguments).await,
        _ => Err(format!(
            "job requires start, nmap, status, wait, output, result, cancel, or cleanup\n{USAGE}"
        )
        .into()),
    }
}

async fn start_job(
    control: &ControlPlaneClient,
    mut arguments: VecDeque<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut executable = None;
    let mut args = Vec::new();
    let mut cwd = None;
    let mut env = serde_json::Map::new();
    let mut timeout_ms = 20 * 60 * 1_000_u64;
    while let Some(argument) = arguments.pop_front() {
        match argument.as_str() {
            "--exec" => executable = Some(value(&mut arguments, "--exec")?),
            "--arg" => args.push(value(&mut arguments, "--arg")?),
            "--cwd" => cwd = Some(value(&mut arguments, "--cwd")?),
            "--env" => {
                let pair = value(&mut arguments, "--env")?;
                let (key, value) = pair.split_once('=').ok_or("--env requires K=V")?;
                if key.is_empty() || key.as_bytes().contains(&0) || value.as_bytes().contains(&0) {
                    return Err("--env requires a non-empty NUL-free key and value".into());
                }
                env.insert(key.to_owned(), value.to_owned().into());
            }
            "--timeout-ms" => timeout_ms = value(&mut arguments, "--timeout-ms")?.parse()?,
            other => return Err(format!("unknown job start option {other:?}").into()),
        }
    }
    let response = invoke_native(
        control,
        "native.command.start",
        serde_json::json!({
            "executable": executable.ok_or("job start requires --exec")?,
            "args": args,
            "cwd": cwd,
            "env": env,
            "timeout_ms": timeout_ms,
        }),
    )
    .await?;
    write_json(&response)
}

async fn start_nmap_job(
    control: &ControlPlaneClient,
    mut arguments: VecDeque<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut target = None;
    let mut allowed_targets = None;
    let mut profile = "service_discovery".to_owned();
    let mut ports = Vec::new();
    let mut timeout_ms = 20 * 60 * 1_000_u64;
    while let Some(argument) = arguments.pop_front() {
        match argument.as_str() {
            "--target" => target = Some(value(&mut arguments, "--target")?),
            "--scope" => allowed_targets = Some(value(&mut arguments, "--scope")?),
            "--profile" => {
                profile = value(&mut arguments, "--profile")?;
                if !matches!(
                    profile.as_str(),
                    "host_discovery" | "tcp_connect" | "service_discovery"
                ) {
                    return Err(format!("unknown Nmap profile {profile:?}").into());
                }
            }
            "--ports" => {
                ports = split_ids(&value(&mut arguments, "--ports")?)
                    .map(|port| port.parse::<u16>())
                    .collect::<Result<Vec<_>, _>>()?;
            }
            "--timeout-ms" => timeout_ms = value(&mut arguments, "--timeout-ms")?.parse()?,
            other => return Err(format!("unknown job nmap option {other:?}").into()),
        }
    }
    let allowed_targets =
        split_ids(&allowed_targets.ok_or("job nmap requires --scope")?).collect::<Vec<_>>();
    let response = invoke_native(
        control,
        "native.nmap.start",
        serde_json::json!({
            "target": target.ok_or("job nmap requires --target")?,
            "allowed_targets": allowed_targets,
            "profile": profile,
            "ports": ports,
            "timeout_ms": timeout_ms,
        }),
    )
    .await?;
    write_json(&response)
}

async fn invoke_job_with_id(
    control: &ControlPlaneClient,
    operation: &str,
    mut arguments: VecDeque<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let job_id = arguments
        .pop_front()
        .ok_or("job operation requires an id")?;
    reject_remaining(&arguments)?;
    write_json(&invoke_native(control, operation, serde_json::json!({"job_id": job_id})).await?)
}

async fn wait_for_job(
    control: &ControlPlaneClient,
    mut arguments: VecDeque<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let job_id = arguments.pop_front().ok_or("job wait requires an id")?;
    let mut wait_ms = 1_000_u64;
    while let Some(argument) = arguments.pop_front() {
        match argument.as_str() {
            "--wait-ms" => wait_ms = value(&mut arguments, "--wait-ms")?.parse()?,
            other => return Err(format!("unknown job wait option {other:?}").into()),
        }
    }
    write_json(
        &invoke_native(
            control,
            "native.command.wait",
            serde_json::json!({"job_id": job_id, "wait_ms": wait_ms}),
        )
        .await?,
    )
}

async fn read_job_output(
    control: &ControlPlaneClient,
    mut arguments: VecDeque<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let job_id = arguments.pop_front().ok_or("job output requires an id")?;
    let mut cursor = 0_u64;
    let mut maximum_records = 128_usize;
    let mut maximum_bytes = 1024 * 1024_usize;
    let mut raw = false;
    while let Some(argument) = arguments.pop_front() {
        match argument.as_str() {
            "--cursor" => cursor = value(&mut arguments, "--cursor")?.parse()?,
            "--records" => maximum_records = value(&mut arguments, "--records")?.parse()?,
            "--bytes" => maximum_bytes = value(&mut arguments, "--bytes")?.parse()?,
            "--raw" => raw = true,
            other => return Err(format!("unknown job output option {other:?}").into()),
        }
    }
    let response = invoke_native(
        control,
        "native.command.output",
        serde_json::json!({
            "job_id": job_id,
            "cursor": cursor,
            "maximum_records": maximum_records,
            "maximum_bytes": maximum_bytes,
        }),
    )
    .await?;
    if !raw {
        return write_json(&response);
    }
    let Response::ProviderInvoked { output, .. } = response else {
        return Err("daemon returned a non-provider response".into());
    };
    let records = output["records"]
        .as_array()
        .ok_or("native provider output did not contain records")?;
    let mut stdout = std::io::stdout().lock();
    for record in records {
        let bytes = record["bytes"]
            .as_array()
            .ok_or("native output record did not contain bytes")?
            .iter()
            .map(|byte| {
                byte.as_u64()
                    .and_then(|byte| u8::try_from(byte).ok())
                    .ok_or("native output record contained an invalid byte")
            })
            .collect::<Result<Vec<_>, _>>()?;
        stdout.write_all(&bytes)?;
    }
    stdout.flush()?;
    Ok(())
}

async fn invoke_native(
    control: &ControlPlaneClient,
    operation: &str,
    input: serde_json::Value,
) -> Result<Response, Box<dyn std::error::Error>> {
    Ok(control
        .send(
            Command::InvokeProvider {
                request_id: RequestId::new(),
                operation_id: OperationId::from_string(operation),
                preferred_provider: Some(ProviderId::from_string("native-execution")),
                input,
            },
            Duration::from_secs(35),
        )
        .await?)
}

async fn artifact(
    control: &ControlPlaneClient,
    arguments: VecDeque<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    match arguments.front().map(String::as_str) {
        Some("put") | Some("resume") => upload_artifact(control, arguments).await,
        Some("status") => artifact_upload_status(control, arguments).await,
        Some("abort") => abort_artifact_upload(control, arguments).await,
        Some("read") => read_artifact(control, arguments).await,
        _ => Err(format!("artifact requires put, resume, status, abort, or read\n{USAGE}").into()),
    }
}

async fn upload_artifact(
    control: &ControlPlaneClient,
    mut arguments: VecDeque<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mode = arguments
        .pop_front()
        .ok_or("artifact upload mode is missing")?;
    let (resume_id, file_path) = if mode == "resume" {
        (
            Some(xai_grok_protocol::ArtifactUploadId::from_string(
                arguments
                    .pop_front()
                    .ok_or("artifact resume requires an upload id")?,
            )),
            PathBuf::from(
                arguments
                    .pop_front()
                    .ok_or("artifact resume requires a file")?,
            ),
        )
    } else {
        (
            None,
            PathBuf::from(
                arguments
                    .pop_front()
                    .ok_or("artifact put requires a file")?,
            ),
        )
    };
    let mut media_type = None;
    let mut expected_content_hash = None;
    let mut chunk_bytes = 4 * 1024 * 1024_usize;
    while let Some(argument) = arguments.pop_front() {
        match argument.as_str() {
            "--media-type" => media_type = Some(value(&mut arguments, "--media-type")?),
            "--content-hash" => {
                expected_content_hash = Some(value(&mut arguments, "--content-hash")?)
            }
            "--chunk-bytes" => {
                chunk_bytes = value(&mut arguments, "--chunk-bytes")?.parse()?;
            }
            other => return Err(format!("unknown artifact upload option {other:?}").into()),
        }
    }
    if chunk_bytes == 0 || chunk_bytes > 8 * 1024 * 1024 {
        return Err("artifact --chunk-bytes must be between 1 and 8388608".into());
    }
    let mut file = std::fs::File::open(&file_path)?;
    let file_metadata = file.metadata()?;
    if !file_metadata.is_file() {
        return Err(format!(
            "artifact source is not a regular file: {}",
            file_path.display()
        )
        .into());
    }
    let expected_bytes = file_metadata.len();
    let state = if let Some(upload_id) = resume_id {
        control
            .send(
                Command::InspectArtifactUpload { upload_id },
                Duration::from_secs(10),
            )
            .await?
    } else {
        control
            .send(
                Command::BeginArtifactUpload {
                    media_type: media_type.ok_or("artifact put requires --media-type")?,
                    expected_bytes,
                    expected_content_hash,
                },
                Duration::from_secs(10),
            )
            .await?
    };
    let (upload_id, expected, mut offset) = match state {
        Response::ArtifactUploadStarted {
            upload_id,
            expected_bytes,
            next_offset,
            ..
        }
        | Response::ArtifactUploadState {
            upload_id,
            expected_bytes,
            next_offset,
            ..
        } => (upload_id, expected_bytes, next_offset),
        _ => return Err("daemon returned a non-upload response".into()),
    };
    if expected != expected_bytes || offset > expected_bytes {
        return Err(format!(
            "artifact upload {upload_id} expects {expected} bytes at offset {offset}, but {} is {expected_bytes} bytes",
            file_path.display()
        )
        .into());
    }
    file.seek(std::io::SeekFrom::Start(offset))?;
    let mut buffer = vec![0_u8; chunk_bytes];
    while offset < expected_bytes {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            return Err(format!("artifact source ended before offset {expected_bytes}").into());
        }
        let response = control
            .send(
                Command::UploadArtifactChunk {
                    upload_id: upload_id.clone(),
                    offset,
                    bytes: buffer[..read].to_vec(),
                },
                Duration::from_secs(30),
            )
            .await
            .map_err(|error| {
                format!(
                    "artifact upload {upload_id} stopped at offset {offset}; resume with `grokctl artifact resume {upload_id} {}`: {error}",
                    file_path.display()
                )
            })?;
        let Response::ArtifactUploadProgress {
            upload_id: response_id,
            next_offset,
        } = response
        else {
            return Err("daemon returned a non-progress response".into());
        };
        if response_id != upload_id || next_offset != offset.saturating_add(read as u64) {
            return Err("daemon returned an inconsistent artifact upload offset".into());
        }
        offset = next_offset;
    }
    let response = control
        .send(
            Command::CommitArtifactUpload { upload_id },
            Duration::from_secs(120),
        )
        .await?;
    write_json(&response)?;
    Ok(())
}

async fn artifact_upload_status(
    control: &ControlPlaneClient,
    mut arguments: VecDeque<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    arguments.pop_front();
    let upload_id = xai_grok_protocol::ArtifactUploadId::from_string(
        arguments
            .pop_front()
            .ok_or("artifact status requires an upload id")?,
    );
    if !arguments.is_empty() {
        return Err("artifact status accepts exactly one upload id".into());
    }
    let response = control
        .send(
            Command::InspectArtifactUpload { upload_id },
            Duration::from_secs(10),
        )
        .await?;
    write_json(&response)?;
    Ok(())
}

async fn abort_artifact_upload(
    control: &ControlPlaneClient,
    mut arguments: VecDeque<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    arguments.pop_front();
    let upload_id = xai_grok_protocol::ArtifactUploadId::from_string(
        arguments
            .pop_front()
            .ok_or("artifact abort requires an upload id")?,
    );
    if !arguments.is_empty() {
        return Err("artifact abort accepts exactly one upload id".into());
    }
    let response = control
        .send(
            Command::AbortArtifactUpload { upload_id },
            Duration::from_secs(10),
        )
        .await?;
    write_json(&response)?;
    Ok(())
}

async fn read_artifact(
    control: &ControlPlaneClient,
    mut arguments: VecDeque<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    if arguments.pop_front().as_deref() != Some("read") {
        return Err(format!("artifact requires `read ID`\n{USAGE}").into());
    }
    let artifact_id = ArtifactId::from_string(
        arguments
            .pop_front()
            .ok_or("artifact read requires an id")?,
    );
    let mut cursor = 0_u64;
    let mut limit = 1024_u32 * 1024;
    let mut read_all = false;
    let mut raw = false;
    while let Some(argument) = arguments.pop_front() {
        match argument.as_str() {
            "--cursor" => cursor = value(&mut arguments, "--cursor")?.parse()?,
            "--limit" => limit = value(&mut arguments, "--limit")?.parse()?,
            "--all" => read_all = true,
            "--raw" => raw = true,
            other => return Err(format!("unknown artifact read option {other:?}").into()),
        }
    }
    if limit == 0 {
        return Err("artifact read --limit must be greater than zero".into());
    }
    loop {
        let response = control
            .send(
                Command::ReadArtifact {
                    artifact_id: artifact_id.clone(),
                    cursor,
                    limit,
                },
                Duration::from_secs(10),
            )
            .await?;
        let Response::ArtifactChunk {
            bytes, next_cursor, ..
        } = &response
        else {
            return Err("daemon returned a non-artifact response".into());
        };
        if raw {
            std::io::stdout().lock().write_all(bytes)?;
        } else {
            write_json(&response)?;
        }
        let Some(next) = next_cursor else {
            break;
        };
        if !read_all {
            break;
        }
        cursor = *next;
    }
    if raw {
        std::io::stdout().flush()?;
    }
    Ok(())
}

async fn status(
    control: &ControlPlaneClient,
    mut arguments: VecDeque<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let query = match arguments.pop_front().as_deref() {
        None | Some("capacity") => ProjectionQuery::Capacity,
        Some("providers") => ProjectionQuery::Providers,
        Some("catalog") => ProjectionQuery::OperatorCatalog {
            workspace_id: arguments.pop_front().map(WorkspaceId::from_string),
        },
        Some("exercise") => ProjectionQuery::ExerciseRecord {
            exercise_id: ExerciseId::from_string(
                arguments
                    .pop_front()
                    .ok_or("status exercise requires an id")?,
            ),
        },
        Some("engagement") => ProjectionQuery::Engagement {
            engagement_id: arguments
                .pop_front()
                .ok_or("status engagement requires an id")?
                .into(),
        },
        Some("tasks") | Some("task-graph") => ProjectionQuery::TaskGraph {
            engagement_id: arguments
                .pop_front()
                .ok_or("status tasks requires an engagement id")?
                .into(),
        },
        Some(other) => return Err(format!("unknown status projection {other:?}").into()),
    };
    reject_remaining(&arguments)?;
    let response = control
        .send(Command::QueryProjection(query), Duration::from_secs(5))
        .await?;
    write_json(&response)
}

async fn create_exercise(
    control: &ControlPlaneClient,
    mut arguments: VecDeque<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    require_create(&mut arguments, "exercise")?;
    let mut workspace = None;
    let mut name = None;
    let mut objective = None;
    let mut scope = None;
    while let Some(argument) = arguments.pop_front() {
        match argument.as_str() {
            "--workspace" => workspace = Some(value(&mut arguments, "--workspace")?),
            "--name" => name = Some(value(&mut arguments, "--name")?),
            "--objective" => objective = Some(value(&mut arguments, "--objective")?),
            "--scope" => scope = Some(value(&mut arguments, "--scope")?),
            other => return Err(format!("unknown exercise create option {other:?}").into()),
        }
    }
    let response = control
        .send(
            Command::CreateExercise(CreateExercise {
                workspace_id: WorkspaceId::from_string(
                    workspace.ok_or("exercise create requires --workspace")?,
                ),
                name: name.ok_or("exercise create requires --name")?,
                objective: objective.ok_or("exercise create requires --objective")?,
                scope: parse_scope_targets(&scope.ok_or("exercise create requires --scope")?),
            }),
            Duration::from_secs(5),
        )
        .await?;
    write_json(&response)
}

async fn create_run(
    control: &ControlPlaneClient,
    mut arguments: VecDeque<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    require_create(&mut arguments, "run")?;
    let mut exercise = None;
    let mut name = None;
    let mut objective = None;
    let mut playbook_id = None;
    while let Some(argument) = arguments.pop_front() {
        match argument.as_str() {
            "--exercise" => exercise = Some(value(&mut arguments, "--exercise")?),
            "--name" => name = Some(value(&mut arguments, "--name")?),
            "--objective" => objective = Some(value(&mut arguments, "--objective")?),
            "--playbook" => playbook_id = Some(value(&mut arguments, "--playbook")?),
            other => return Err(format!("unknown run create option {other:?}").into()),
        }
    }
    let response = control
        .send(
            Command::CreateOperationRun(CreateOperationRun {
                exercise_id: ExerciseId::from_string(
                    exercise.ok_or("run create requires --exercise")?,
                ),
                name: name.ok_or("run create requires --name")?,
                objective: objective.ok_or("run create requires --objective")?,
                playbook_id: playbook_id.map(PlaybookId::from_string),
            }),
            Duration::from_secs(5),
        )
        .await?;
    write_json(&response)
}

async fn create_playbook(
    control: &ControlPlaneClient,
    mut arguments: VecDeque<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    require_create(&mut arguments, "playbook")?;
    let mut workspace = None;
    let mut name = None;
    let mut description = None;
    let mut steps_path = None;
    while let Some(argument) = arguments.pop_front() {
        match argument.as_str() {
            "--workspace" => workspace = Some(value(&mut arguments, "--workspace")?),
            "--name" => name = Some(value(&mut arguments, "--name")?),
            "--description" => description = Some(value(&mut arguments, "--description")?),
            "--steps" => steps_path = Some(value(&mut arguments, "--steps")?),
            other => return Err(format!("unknown playbook create option {other:?}").into()),
        }
    }
    let mut bytes = Vec::new();
    let steps_path = steps_path.ok_or("playbook create requires --steps FILE|-")?;
    if steps_path == "-" {
        std::io::stdin().read_to_end(&mut bytes)?;
    } else {
        bytes = std::fs::read(steps_path)?;
    }
    let steps: Vec<PlaybookStep> = serde_json::from_slice(&bytes)?;
    let response = control
        .send(
            Command::CreatePlaybook(CreatePlaybook {
                workspace_id: WorkspaceId::from_string(
                    workspace.ok_or("playbook create requires --workspace")?,
                ),
                name: name.ok_or("playbook create requires --name")?,
                description: description.ok_or("playbook create requires --description")?,
                steps,
            }),
            Duration::from_secs(5),
        )
        .await?;
    write_json(&response)
}

async fn create_session(
    control: &ControlPlaneClient,
    mut arguments: VecDeque<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    require_create(&mut arguments, "session")?;
    let mut exercise = None;
    let mut operation_run = None;
    let mut name = None;
    let mut purpose = None;
    while let Some(argument) = arguments.pop_front() {
        match argument.as_str() {
            "--exercise" => exercise = Some(value(&mut arguments, "--exercise")?),
            "--run" => operation_run = Some(value(&mut arguments, "--run")?),
            "--name" => name = Some(value(&mut arguments, "--name")?),
            "--purpose" => purpose = Some(value(&mut arguments, "--purpose")?),
            other => return Err(format!("unknown session create option {other:?}").into()),
        }
    }
    let response = control
        .send(
            Command::CreateOperatorSession(CreateOperatorSession {
                exercise_id: ExerciseId::from_string(
                    exercise.ok_or("session create requires --exercise")?,
                ),
                operation_run_id: operation_run.map(OperationRunId::from_string),
                name: name.ok_or("session create requires --name")?,
                purpose: purpose.ok_or("session create requires --purpose")?,
            }),
            Duration::from_secs(5),
        )
        .await?;
    write_json(&response)
}

async fn record_evidence(
    control: &ControlPlaneClient,
    mut arguments: VecDeque<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    if arguments.pop_front().as_deref() != Some("record") {
        return Err("evidence requires `record`".into());
    }
    let mut exercise = None;
    let mut operation_run = None;
    let mut session = None;
    let mut task = None;
    let mut artifact = None;
    let mut finding = None;
    let mut confidence = None;
    let mut attributes = std::collections::BTreeMap::new();
    while let Some(argument) = arguments.pop_front() {
        match argument.as_str() {
            "--exercise" => exercise = Some(value(&mut arguments, "--exercise")?),
            "--run" => operation_run = Some(value(&mut arguments, "--run")?),
            "--session" => session = Some(value(&mut arguments, "--session")?),
            "--task" => task = Some(value(&mut arguments, "--task")?),
            "--artifact" => artifact = Some(value(&mut arguments, "--artifact")?),
            "--finding" => finding = Some(value(&mut arguments, "--finding")?),
            "--confidence" => {
                confidence = Some(value(&mut arguments, "--confidence")?.parse::<f32>()?)
            }
            "--attribute" => {
                let pair = value(&mut arguments, "--attribute")?;
                let (key, value) = pair.split_once('=').ok_or("--attribute requires K=V")?;
                if key.trim().is_empty() {
                    return Err("--attribute key must not be empty".into());
                }
                attributes.insert(key.to_owned(), value.to_owned());
            }
            other => return Err(format!("unknown evidence record option {other:?}").into()),
        }
    }
    let response = control
        .send(
            Command::RecordEvidence(RecordExerciseEvidence {
                exercise_id: ExerciseId::from_string(
                    exercise.ok_or("evidence record requires --exercise")?,
                ),
                operation_run_id: operation_run.map(OperationRunId::from_string),
                session_id: session.map(OperatorSessionId::from_string),
                task_id: task.map(TaskId::from_string),
                finding: finding.ok_or("evidence record requires --finding")?,
                confidence: confidence.ok_or("evidence record requires --confidence")?,
                artifact_id: artifact.map(ArtifactId::from_string),
                attributes,
            }),
            Duration::from_secs(5),
        )
        .await?;
    write_json(&response)
}

async fn finding(
    control: &ControlPlaneClient,
    mut arguments: VecDeque<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    match arguments.pop_front().as_deref() {
        Some("create") => create_finding(control, arguments).await,
        Some("status") => set_finding_status(control, arguments).await,
        _ => Err("finding requires `create` or `status`".into()),
    }
}

async fn create_finding(
    control: &ControlPlaneClient,
    mut arguments: VecDeque<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut exercise = None;
    let mut operation_run = None;
    let mut title = None;
    let mut summary = None;
    let mut severity = None;
    let mut evidence_ids = Vec::new();
    let mut target_ids = Vec::new();
    while let Some(argument) = arguments.pop_front() {
        match argument.as_str() {
            "--exercise" => exercise = Some(value(&mut arguments, "--exercise")?),
            "--run" => operation_run = Some(value(&mut arguments, "--run")?),
            "--title" => title = Some(value(&mut arguments, "--title")?),
            "--summary" => summary = Some(value(&mut arguments, "--summary")?),
            "--severity" => {
                severity = Some(parse_finding_severity(&value(
                    &mut arguments,
                    "--severity",
                )?)?)
            }
            "--evidence" => {
                evidence_ids.extend(
                    split_ids(&value(&mut arguments, "--evidence")?).map(EvidenceId::from_string),
                );
            }
            "--targets" => {
                target_ids.extend(
                    split_ids(&value(&mut arguments, "--targets")?).map(TargetId::from_string),
                );
            }
            other => return Err(format!("unknown finding create option {other:?}").into()),
        }
    }
    let response = control
        .send(
            Command::CreateFinding(CreateFinding {
                exercise_id: ExerciseId::from_string(
                    exercise.ok_or("finding create requires --exercise")?,
                ),
                operation_run_id: operation_run.map(OperationRunId::from_string),
                title: title.ok_or("finding create requires --title")?,
                summary: summary.ok_or("finding create requires --summary")?,
                severity: severity.ok_or("finding create requires --severity")?,
                evidence_ids,
                target_ids,
            }),
            Duration::from_secs(5),
        )
        .await?;
    write_json(&response)
}

async fn set_finding_status(
    control: &ControlPlaneClient,
    mut arguments: VecDeque<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let finding_id = xai_grok_protocol::FindingId::from_string(
        arguments
            .pop_front()
            .ok_or("finding status requires an id")?,
    );
    let status = parse_finding_status(
        &arguments
            .pop_front()
            .ok_or("finding status requires a status")?,
    )?;
    reject_remaining(&arguments)?;
    let response = control
        .send(
            Command::SetFindingStatus { finding_id, status },
            Duration::from_secs(5),
        )
        .await?;
    write_json(&response)
}

fn split_ids(value: &str) -> impl Iterator<Item = String> + '_ {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn parse_finding_severity(value: &str) -> Result<FindingSeverity, Box<dyn std::error::Error>> {
    match value {
        "informational" | "info" => Ok(FindingSeverity::Informational),
        "low" => Ok(FindingSeverity::Low),
        "moderate" | "medium" => Ok(FindingSeverity::Moderate),
        "high" => Ok(FindingSeverity::High),
        "critical" => Ok(FindingSeverity::Critical),
        _ => Err(format!("unknown finding severity {value:?}").into()),
    }
}

fn parse_finding_status(value: &str) -> Result<FindingStatus, Box<dyn std::error::Error>> {
    match value {
        "candidate" => Ok(FindingStatus::Candidate),
        "confirmed" => Ok(FindingStatus::Confirmed),
        "remediated" => Ok(FindingStatus::Remediated),
        "rejected" => Ok(FindingStatus::Rejected),
        _ => Err(format!("unknown finding status {value:?}").into()),
    }
}

async fn team(
    control: &ControlPlaneClient,
    mut arguments: VecDeque<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    match arguments.pop_front().as_deref() {
        Some("status") => {
            let team_id =
                TeamId::from_string(arguments.pop_front().ok_or("team status requires an id")?);
            reject_remaining(&arguments)?;
            write_json(
                &control
                    .send(
                        Command::QueryProjection(ProjectionQuery::Team { team_id }),
                        Duration::from_secs(5),
                    )
                    .await?,
            )
        }
        Some("presence") => team_presence(control, arguments).await,
        Some("work") => team_work(control, arguments).await,
        Some("message") => team_message(control, arguments).await,
        Some("claim") => team_claim(control, arguments).await,
        Some("release") => team_release(control, arguments).await,
        _ => Err("team requires status, presence, work, message, claim, or release".into()),
    }
}

async fn team_presence(
    control: &ControlPlaneClient,
    mut arguments: VecDeque<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut team_id = None;
    let mut client_id = None;
    let mut display_name = None;
    let mut state = TeamPresenceState::Online;
    let mut workspace_id = None;
    let mut exercise_id = None;
    let mut operation_run_id = None;
    let mut session_id = None;
    while let Some(argument) = arguments.pop_front() {
        match argument.as_str() {
            "--team" => team_id = Some(value(&mut arguments, "--team")?),
            "--client" => client_id = Some(value(&mut arguments, "--client")?),
            "--display-name" => display_name = Some(value(&mut arguments, "--display-name")?),
            "--state" => {
                state = parse_presence_state(&value(&mut arguments, "--state")?)?;
            }
            "--workspace" => workspace_id = Some(value(&mut arguments, "--workspace")?),
            "--exercise" => exercise_id = Some(value(&mut arguments, "--exercise")?),
            "--run" => operation_run_id = Some(value(&mut arguments, "--run")?),
            "--session" => session_id = Some(value(&mut arguments, "--session")?),
            other => return Err(format!("unknown team presence option {other:?}").into()),
        }
    }
    write_json(
        &control
            .send(
                Command::SetTeamPresence(SetTeamPresence {
                    client: TeamClient {
                        team_id: TeamId::from_string(
                            team_id.ok_or("team presence requires --team")?,
                        ),
                        client_id: ClientId::from_string(
                            client_id.ok_or("team presence requires --client")?,
                        ),
                        display_name,
                    },
                    state,
                    workspace_id: workspace_id.map(WorkspaceId::from_string),
                    exercise_id: exercise_id.map(ExerciseId::from_string),
                    operation_run_id: operation_run_id.map(OperationRunId::from_string),
                    session_id: session_id.map(OperatorSessionId::from_string),
                }),
                Duration::from_secs(5),
            )
            .await?,
    )
}

async fn team_work(
    control: &ControlPlaneClient,
    mut arguments: VecDeque<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    match arguments.pop_front().as_deref() {
        Some("create") => {
            let mut team_id = None;
            let mut exercise_id = None;
            let mut operation_run_id = None;
            let mut title = None;
            let mut objective = None;
            let mut assignee = None;
            while let Some(argument) = arguments.pop_front() {
                match argument.as_str() {
                    "--team" => team_id = Some(value(&mut arguments, "--team")?),
                    "--exercise" => exercise_id = Some(value(&mut arguments, "--exercise")?),
                    "--run" => operation_run_id = Some(value(&mut arguments, "--run")?),
                    "--title" => title = Some(value(&mut arguments, "--title")?),
                    "--objective" => objective = Some(value(&mut arguments, "--objective")?),
                    "--assignee" => assignee = Some(value(&mut arguments, "--assignee")?),
                    other => return Err(format!("unknown team work option {other:?}").into()),
                }
            }
            write_json(
                &control
                    .send(
                        Command::CreateTeamWorkItem(CreateTeamWorkItem {
                            team_id: TeamId::from_string(
                                team_id.ok_or("team work create requires --team")?,
                            ),
                            exercise_id: exercise_id.map(ExerciseId::from_string),
                            operation_run_id: operation_run_id.map(OperationRunId::from_string),
                            title: title.ok_or("team work create requires --title")?,
                            objective: objective.ok_or("team work create requires --objective")?,
                            assignee: assignee.map(ClientId::from_string),
                        }),
                        Duration::from_secs(5),
                    )
                    .await?,
            )
        }
        Some("assign") => {
            let work_item_id = TeamWorkItemId::from_string(
                arguments
                    .pop_front()
                    .ok_or("team work assign requires an id")?,
            );
            let assignee = arguments
                .pop_front()
                .ok_or("team work assign requires CLIENT or -")?;
            let expected_revision = parse_revision_option(&mut arguments)?;
            write_json(
                &control
                    .send(
                        Command::AssignTeamWorkItem {
                            work_item_id,
                            assignee: (assignee != "-").then(|| ClientId::from_string(assignee)),
                            expected_revision,
                        },
                        Duration::from_secs(5),
                    )
                    .await?,
            )
        }
        Some("status") => {
            let work_item_id = TeamWorkItemId::from_string(
                arguments
                    .pop_front()
                    .ok_or("team work status requires an id")?,
            );
            let status = parse_work_item_status(
                &arguments
                    .pop_front()
                    .ok_or("team work status requires a status")?,
            )?;
            let expected_revision = parse_revision_option(&mut arguments)?;
            write_json(
                &control
                    .send(
                        Command::SetTeamWorkItemStatus {
                            work_item_id,
                            status,
                            expected_revision,
                        },
                        Duration::from_secs(5),
                    )
                    .await?,
            )
        }
        _ => Err("team work requires create, assign, or status".into()),
    }
}

async fn team_message(
    control: &ControlPlaneClient,
    mut arguments: VecDeque<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut team_id = None;
    let mut channel_id = None;
    let mut sender = None;
    let mut body = None;
    let mut reply_to = None;
    while let Some(argument) = arguments.pop_front() {
        match argument.as_str() {
            "--team" => team_id = Some(value(&mut arguments, "--team")?),
            "--channel" => channel_id = Some(value(&mut arguments, "--channel")?),
            "--sender" => sender = Some(value(&mut arguments, "--sender")?),
            "--body" => body = Some(value(&mut arguments, "--body")?),
            "--reply-to" => reply_to = Some(value(&mut arguments, "--reply-to")?),
            other => return Err(format!("unknown team message option {other:?}").into()),
        }
    }
    write_json(
        &control
            .send(
                Command::PostTeamMessage(PostTeamMessage {
                    team_id: TeamId::from_string(team_id.ok_or("team message requires --team")?),
                    channel_id: ChannelId::from_string(
                        channel_id.ok_or("team message requires --channel")?,
                    ),
                    sender: ClientId::from_string(sender.ok_or("team message requires --sender")?),
                    body: body.ok_or("team message requires --body")?,
                    reply_to: reply_to.map(xai_grok_protocol::MessageId::from_string),
                }),
                Duration::from_secs(5),
            )
            .await?,
    )
}

async fn team_claim(
    control: &ControlPlaneClient,
    mut arguments: VecDeque<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut team_id = None;
    let mut owner = None;
    let mut resource_key = None;
    let mut lease_ms = None;
    while let Some(argument) = arguments.pop_front() {
        match argument.as_str() {
            "--team" => team_id = Some(value(&mut arguments, "--team")?),
            "--owner" => owner = Some(value(&mut arguments, "--owner")?),
            "--resource" => resource_key = Some(value(&mut arguments, "--resource")?),
            "--lease-ms" => lease_ms = Some(value(&mut arguments, "--lease-ms")?.parse()?),
            other => return Err(format!("unknown team claim option {other:?}").into()),
        }
    }
    write_json(
        &control
            .send(
                Command::ClaimTeamResource(ClaimTeamResource {
                    team_id: TeamId::from_string(team_id.ok_or("team claim requires --team")?),
                    owner: ClientId::from_string(owner.ok_or("team claim requires --owner")?),
                    resource_key: resource_key.ok_or("team claim requires --resource")?,
                    lease_ms: lease_ms.ok_or("team claim requires --lease-ms")?,
                }),
                Duration::from_secs(5),
            )
            .await?,
    )
}

async fn team_release(
    control: &ControlPlaneClient,
    mut arguments: VecDeque<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let claim_id =
        ResourceClaimId::from_string(arguments.pop_front().ok_or("team release requires an id")?);
    let mut owner = None;
    let mut revision = None;
    while let Some(argument) = arguments.pop_front() {
        match argument.as_str() {
            "--owner" => owner = Some(value(&mut arguments, "--owner")?),
            "--revision" => revision = Some(value(&mut arguments, "--revision")?.parse()?),
            other => return Err(format!("unknown team release option {other:?}").into()),
        }
    }
    write_json(
        &control
            .send(
                Command::ReleaseTeamResource {
                    claim_id,
                    owner: ClientId::from_string(owner.ok_or("team release requires --owner")?),
                    expected_revision: revision.ok_or("team release requires --revision")?,
                },
                Duration::from_secs(5),
            )
            .await?,
    )
}

fn parse_revision_option(
    arguments: &mut VecDeque<String>,
) -> Result<u64, Box<dyn std::error::Error>> {
    if arguments.pop_front().as_deref() != Some("--revision") {
        return Err("operation requires --revision N".into());
    }
    let revision = value(arguments, "--revision")?.parse()?;
    reject_remaining(arguments)?;
    Ok(revision)
}

fn parse_presence_state(value: &str) -> Result<TeamPresenceState, Box<dyn std::error::Error>> {
    match value {
        "online" => Ok(TeamPresenceState::Online),
        "away" => Ok(TeamPresenceState::Away),
        "offline" => Ok(TeamPresenceState::Offline),
        _ => Err(format!("unknown team presence state {value:?}").into()),
    }
}

fn parse_work_item_status(value: &str) -> Result<TeamWorkItemStatus, Box<dyn std::error::Error>> {
    match value {
        "open" => Ok(TeamWorkItemStatus::Open),
        "in_progress" | "in-progress" => Ok(TeamWorkItemStatus::InProgress),
        "blocked" => Ok(TeamWorkItemStatus::Blocked),
        "completed" => Ok(TeamWorkItemStatus::Completed),
        "cancelled" | "canceled" => Ok(TeamWorkItemStatus::Cancelled),
        _ => Err(format!("unknown team work item status {value:?}").into()),
    }
}

async fn events(
    control: &ControlPlaneClient,
    mut arguments: VecDeque<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut after_sequence = 0_u64;
    let mut maximum_events = 128_u32;
    let mut wait_ms = 0_u32;
    let mut engagement_id = None;
    let mut follow = false;
    while let Some(argument) = arguments.pop_front() {
        match argument.as_str() {
            "--after" => after_sequence = value(&mut arguments, "--after")?.parse()?,
            "--limit" => maximum_events = value(&mut arguments, "--limit")?.parse()?,
            "--wait-ms" => wait_ms = value(&mut arguments, "--wait-ms")?.parse()?,
            "--engagement" => engagement_id = Some(value(&mut arguments, "--engagement")?.into()),
            "--follow" => follow = true,
            other => return Err(format!("unknown events option {other:?}").into()),
        }
    }
    if follow && wait_ms == 0 {
        wait_ms = 30_000;
    }
    loop {
        let batch = control
            .read_events(EventReadRequest {
                after_sequence,
                maximum_events,
                wait_ms,
                engagement_id: engagement_id.clone(),
            })
            .await?;
        for event in &batch.events {
            serde_json::to_writer(std::io::stdout().lock(), event)?;
            println!();
        }
        std::io::stdout().flush()?;
        after_sequence = batch.next_sequence;
        if !follow {
            break;
        }
    }
    Ok(())
}

async fn submit(
    control: &ControlPlaneClient,
    mut arguments: VecDeque<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut workspace = None;
    let mut exercise = None;
    let mut operation_run = None;
    let mut session = None;
    let mut request = None;
    while let Some(argument) = arguments.pop_front() {
        match argument.as_str() {
            "--workspace" => workspace = Some(value(&mut arguments, "--workspace")?),
            "--exercise" => exercise = Some(value(&mut arguments, "--exercise")?),
            "--run" => operation_run = Some(value(&mut arguments, "--run")?),
            "--session" => session = Some(value(&mut arguments, "--session")?),
            "--request" => request = Some(value(&mut arguments, "--request")?),
            other => return Err(format!("unknown submit option {other:?}").into()),
        }
    }
    let mut request = request.ok_or("submit requires --request")?;
    if request == "-" {
        request.clear();
        std::io::stdin().read_to_string(&mut request)?;
    }
    let session = session.ok_or("submit requires --session")?;
    if operation_run.is_some() && exercise.is_none() {
        return Err("submit --run requires --exercise".into());
    }
    let operator_session_id = exercise
        .as_ref()
        .map(|_| OperatorSessionId::from_string(session.clone()));
    let response = control
        .send(
            Command::SubmitIngress(IngressEnvelope {
                command_id: CommandId::new(),
                source: IngressSource::Cli,
                source_event_id: CommandId::new().to_string(),
                workspace_id: WorkspaceId::from_string(
                    workspace.ok_or("submit requires --workspace")?,
                ),
                exercise_id: exercise.map(ExerciseId::from_string),
                operation_run_id: operation_run.map(OperationRunId::from_string),
                operator_session_id,
                session_id: session,
                prompt_id: CommandId::new().to_string(),
                request,
                team: None,
                metadata: serde_json::Map::new(),
            }),
            Duration::from_secs(10),
        )
        .await?;
    write_json(&response)
}

fn require_create(
    arguments: &mut VecDeque<String>,
    resource: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if arguments.pop_front().as_deref() == Some("create") {
        Ok(())
    } else {
        Err(format!("{resource} requires `create`").into())
    }
}

fn lint_profile(mut arguments: VecDeque<String>) -> Result<(), Box<dyn std::error::Error>> {
    if arguments.pop_front().as_deref() != Some("lint") {
        return Err(format!("profile requires `lint FILE`\n{USAGE}").into());
    }
    let path = PathBuf::from(
        arguments
            .pop_front()
            .ok_or("profile lint requires a file")?,
    );
    reject_remaining(&arguments)?;
    let profile: RuntimeProfile = serde_json::from_slice(&std::fs::read(path)?)?;
    let diagnostics = profile.lint();
    write_json(&diagnostics)?;
    if profile.is_valid() {
        Ok(())
    } else {
        Err("runtime profile is invalid".into())
    }
}

fn take_leading_option(
    arguments: &mut VecDeque<String>,
    name: &str,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    if arguments.front().is_some_and(|argument| argument == name) {
        arguments.pop_front();
        Ok(Some(value(arguments, name)?))
    } else {
        Ok(None)
    }
}

fn value(
    arguments: &mut VecDeque<String>,
    name: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    arguments
        .pop_front()
        .ok_or_else(|| format!("{name} requires a value").into())
}

fn reject_remaining(arguments: &VecDeque<String>) -> Result<(), Box<dyn std::error::Error>> {
    if arguments.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "unexpected arguments: {}",
            arguments.iter().cloned().collect::<Vec<_>>().join(" ")
        )
        .into())
    }
}

fn write_json(value: &impl serde::Serialize) -> Result<(), Box<dyn std::error::Error>> {
    serde_json::to_writer_pretty(std::io::stdout().lock(), value)?;
    println!();
    Ok(())
}
