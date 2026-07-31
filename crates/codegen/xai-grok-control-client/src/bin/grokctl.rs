use std::collections::VecDeque;
use std::io::{Read as _, Write as _};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use xai_grok_control_client::{ClientIdentity, ControlPlaneClient};
use xai_grok_protocol::{
    ClientId, Command, CommandId, EventReadRequest, IngressEnvelope, IngressSource,
    ProjectionQuery, RuntimeProfile, TeamClient, TeamId, WorkspaceId,
};

const USAGE: &str = "\
grokctl [--socket PATH] [--team ID --client ID] COMMAND

Commands:
  status [capacity|providers|engagement ID]
  events [--after N] [--limit N] [--wait-ms N] [--engagement ID] [--follow]
  submit --workspace ID --session ID --request TEXT|-
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
        "submit" => submit(&control, arguments).await,
        _ => Err(format!("unknown command {command:?}\n{USAGE}").into()),
    }
}

async fn status(
    control: &ControlPlaneClient,
    mut arguments: VecDeque<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let query = match arguments.pop_front().as_deref() {
        None | Some("capacity") => ProjectionQuery::Capacity,
        Some("providers") => ProjectionQuery::Providers,
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
    let mut session = None;
    let mut request = None;
    while let Some(argument) = arguments.pop_front() {
        match argument.as_str() {
            "--workspace" => workspace = Some(value(&mut arguments, "--workspace")?),
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
    let response = control
        .send(
            Command::SubmitIngress(IngressEnvelope {
                command_id: CommandId::new(),
                source: IngressSource::Cli,
                source_event_id: CommandId::new().to_string(),
                workspace_id: WorkspaceId::from_string(
                    workspace.ok_or("submit requires --workspace")?,
                ),
                session_id: session.ok_or("submit requires --session")?,
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
