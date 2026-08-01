use std::collections::VecDeque;
use std::io::{Read as _, Write as _};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use xai_grok_control_client::{ClientIdentity, ControlPlaneClient};
use xai_grok_protocol::{
    ArtifactId, ClientId, Command, CommandId, CreateExercise, CreateFinding, CreateOperationRun,
    CreateOperatorSession, CreatePlaybook, EventReadRequest, EvidenceId, ExerciseId,
    FindingSeverity, FindingStatus, IngressEnvelope, IngressSource, OperationRunId,
    OperatorSessionId, PlaybookId, PlaybookStep, ProjectionQuery, RecordExerciseEvidence, Response,
    RuntimeProfile, TargetId, TaskId, TeamClient, TeamId, WorkspaceId, parse_scope_targets,
};

const USAGE: &str = "\
grokctl [--socket PATH] [--team ID --client ID] COMMAND

Commands:
  status [capacity|providers|catalog [WORKSPACE]|exercise ID|engagement ID]
  events [--after N] [--limit N] [--wait-ms N] [--engagement ID] [--follow]
  exercise create --workspace ID --name NAME --objective TEXT --scope SELECTORS
  playbook create --workspace ID --name NAME --description TEXT --steps FILE|-
  run create --exercise ID [--playbook ID] --name NAME --objective TEXT
  session create --exercise ID [--run ID] --name NAME --purpose TEXT
  evidence record --exercise ID [--run ID] [--session ID] [--task ID] [--artifact ID] --finding TEXT --confidence 0..1 [--attribute K=V ...]
  finding create --exercise ID [--run ID] --title TEXT --summary TEXT --severity LEVEL [--evidence IDS] [--targets IDS]
  finding status ID candidate|confirmed|remediated|rejected
  submit --workspace ID [--exercise ID] [--run ID] --session ID --request TEXT|-
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
    let team = take_leading_option(&mut arguments, "--team")?;
    let client = take_leading_option(&mut arguments, "--client")?;
    let display_name = take_leading_option(&mut arguments, "--display-name")?;
    let command = arguments.pop_front().ok_or("missing command")?;

    if command == "profile" {
        return lint_profile(arguments);
    }

    let mut identity = ClientIdentity::new("grokctl", env!("CARGO_PKG_VERSION"));
    match (team, client) {
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
        "submit" => submit(&control, arguments).await,
        "artifact" => read_artifact(&control, arguments).await,
        _ => Err(format!("unknown command {command:?}\n{USAGE}").into()),
    }
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
