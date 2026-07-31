use std::path::PathBuf;

use clap::Parser;
use xai_grok_control_plane::{ControlPlane, ControlPlaneConfig, ControlPlaneServer, ServerConfig};

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

    /// Maximum accepted control-plane commands waiting for the serial actor.
    #[arg(long, default_value_t = 1_024)]
    command_capacity: usize,

    /// Maximum simultaneous local socket clients.
    #[arg(long, default_value_t = 128)]
    maximum_connections: usize,
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
    let socket_path = arguments
        .socket
        .unwrap_or_else(|| arguments.state_directory.join("grokd.sock"));
    let mut control_config = ControlPlaneConfig::new(&arguments.state_directory);
    control_config.command_capacity = arguments.command_capacity.max(1);
    let control_plane = ControlPlane::open(control_config).await?;
    let handle = control_plane.handle();
    let mut server_config = ServerConfig::new(&socket_path);
    server_config.maximum_connections = arguments.maximum_connections.max(1);
    let server = ControlPlaneServer::bind(server_config, handle.clone()).await?;

    tracing::info!(
        socket = %socket_path.display(),
        state = %arguments.state_directory.display(),
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
