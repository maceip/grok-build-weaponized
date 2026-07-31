use std::path::PathBuf;

use clap::Parser;
use xai_grok_control_plane::{ControlPlane, ControlPlaneConfig, ControlPlaneServer, ServerConfig};
use xai_grok_protocol::{PROTOCOL_VERSION, RuntimeProfile};

#[derive(Debug, Parser)]
#[command(
    name = "grokd",
    about = "Persistent local Grok execution control plane"
)]
struct Arguments {
    /// Durable state directory for engagements, events, plans, and artifacts.
    #[arg(long, default_value = ".grok")]
    state_directory: PathBuf,

    /// Unix-domain socket path. Defaults to <state-directory>/grokd.sock.
    #[arg(long)]
    socket: Option<PathBuf>,

    /// Validated JSON runtime profile activated before daemon state is opened.
    #[arg(long)]
    profile: Option<PathBuf>,

    /// Maximum accepted control-plane commands waiting for the serial actor.
    #[arg(long)]
    command_capacity: Option<usize>,

    /// Maximum simultaneous local socket clients.
    #[arg(long)]
    maximum_connections: Option<usize>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "xai_grok_control_plane=info,grokd=info".into()),
        )
        .with_target(false)
        .init();

    let arguments = Arguments::parse();
    let profile = arguments.profile.as_ref().map(load_profile).transpose()?;
    let socket_path = arguments
        .socket
        .unwrap_or_else(|| arguments.state_directory.join("grokd.sock"));
    let mut control_config = ControlPlaneConfig::new(&arguments.state_directory);
    control_config.command_capacity = arguments
        .command_capacity
        .or_else(|| {
            profile
                .as_ref()
                .map(|profile| profile.limits.command_queue as usize)
        })
        .unwrap_or(1_024)
        .max(1);
    if let Some(profile) = &profile {
        control_config.event_capacity = (profile.limits.event_batch as usize)
            .saturating_mul(32)
            .max(1);
        control_config.maximum_artifact_bytes = profile.limits.maximum_spool_bytes;
        control_config.maximum_artifact_store_bytes = profile.limits.maximum_spool_bytes;
    }
    let control_plane = ControlPlane::open(control_config).await?;
    let handle = control_plane.handle();
    let mut server_config = ServerConfig::new(&socket_path);
    server_config.maximum_connections = arguments
        .maximum_connections
        .or_else(|| {
            profile
                .as_ref()
                .map(|profile| profile.limits.maximum_clients as usize)
        })
        .unwrap_or(128)
        .max(1);
    let server = ControlPlaneServer::bind(server_config, handle.clone()).await?;

    tracing::info!(
        socket = %socket_path.display(),
        state = %arguments.state_directory.display(),
        profile = profile.as_ref().map(|profile| profile.profile_id.as_str()).unwrap_or("default"),
        profile_revision = profile.as_ref().map_or(0, |profile| profile.revision),
        profile_hash = profile.as_ref().map(RuntimeProfile::content_hash).unwrap_or_default(),
        "grokd ready"
    );

    let server_result = tokio::select! {
        result = server.run() => result,
        signal = tokio::signal::ctrl_c() => {
            signal?;
            handle.shutdown_token().cancel();
            Ok(())
        }
    };
    handle.shutdown_token().cancel();
    control_plane.wait().await;
    server_result?;
    Ok(())
}

fn load_profile(path: &PathBuf) -> Result<RuntimeProfile, Box<dyn std::error::Error>> {
    let profile: RuntimeProfile = serde_json::from_slice(&std::fs::read(path)?)?;
    let diagnostics = profile.lint();
    if diagnostics
        .iter()
        .any(|diagnostic| diagnostic.severity == xai_grok_protocol::DiagnosticSeverity::Error)
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "runtime profile failed validation: {}",
                serde_json::to_string(&diagnostics)?
            ),
        )
        .into());
    }
    if !profile.protocol_range.supports(PROTOCOL_VERSION) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("runtime profile does not support protocol version {PROTOCOL_VERSION}"),
        )
        .into());
    }
    Ok(profile)
}
