use std::path::PathBuf;

use clap::Parser;
use xai_grok_control_plane::{
    AgentExecutionProvider, AgentProviderConfig, ControlPlane, ControlPlaneConfig,
    ControlPlaneServer, NativeExecutionProvider, ProcessExecutionProvider, ProcessProviderConfig,
    ServerConfig,
};
use xai_grok_native_execution::NativeExecutionLimits;
use xai_grok_protocol::{
    CapabilityManifest, PROTOCOL_VERSION, ProfileProviderRequirement, ProviderKind, RuntimeProfile,
};

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

    /// Persistent Grok ACP worker executable. Defaults to a sibling
    /// `xai-grok-pager` or `grok` binary.
    #[arg(long, env = "GROK_AGENT_BINARY")]
    agent_binary: Option<PathBuf>,

    /// Workspace used when an ingress request does not provide one.
    #[arg(long, env = "GROK_AGENT_WORKSPACE")]
    agent_workspace: Option<PathBuf>,

    /// Model selected for new daemon-owned agent sessions.
    #[arg(long, env = "GROK_AGENT_MODEL")]
    agent_model: Option<String>,

    /// Start only the durable protocol/state service. This mode deliberately
    /// does not advertise or accept executable agent work.
    #[arg(long)]
    control_only: bool,

    /// Start the native execution provider without an agent worker. Useful for
    /// compact execution nodes that are driven by another client.
    #[arg(long)]
    no_agent: bool,

    /// `grok-ops-mcp` executable used for daemon-supervised connector workers.
    /// Defaults to a sibling binary when at least one connector is enabled.
    #[arg(long, env = "GROK_OPS_MCP_BINARY")]
    ops_mcp_binary: Option<PathBuf>,

    /// Start a real persistent connector worker. Repeat for metasploit,
    /// bloodhound, and/or vulnerability-index.
    #[arg(long = "ops-connector")]
    ops_connectors: Vec<String>,
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
    let socket_path = absolute_path(
        arguments
            .socket
            .unwrap_or_else(|| arguments.state_directory.join("grokd.sock")),
    )?;
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
    let native_provider = if arguments.control_only {
        None
    } else {
        let provider = NativeExecutionProvider::open_with_limits(
            arguments.state_directory.join("native-execution"),
            native_execution_limits(profile.as_ref()),
        )
        .await?;
        handle.register_provider(provider.clone()).await?;
        Some(provider)
    };
    let agent_provider = if arguments.control_only || arguments.no_agent {
        None
    } else {
        let binary = resolve_agent_binary(arguments.agent_binary.as_ref())?;
        let workspace = arguments
            .agent_workspace
            .unwrap_or(std::env::current_dir()?)
            .canonicalize()?;
        let mut provider_config = AgentProviderConfig::new(binary, workspace);
        if let Some(requirement) = profile_requirement(profile.as_ref(), ProviderKind::ModelRuntime)
        {
            provider_config.maximum_parallel = requirement.maximum_concurrency;
            provider_config.queue_capacity = profile
                .as_ref()
                .map_or(provider_config.queue_capacity, |profile| {
                    profile.limits.command_queue.max(1)
                });
        }
        provider_config.model_id = arguments.agent_model;
        provider_config
            .environment
            .insert("GROK_EXECUTION_BACKEND".to_owned(), "daemon".to_owned());
        provider_config
            .environment
            .insert("GROK_MEMORY".to_owned(), "1".to_owned());
        provider_config.environment.insert(
            "GROKD_SOCKET".to_owned(),
            socket_path.to_string_lossy().into_owned(),
        );
        let provider = AgentExecutionProvider::start(provider_config).await?;
        handle.register_provider(provider.clone()).await?;
        Some(provider)
    };
    let connector_names = requested_connectors(&arguments.ops_connectors, profile.as_ref())?;
    let mut connector_providers = Vec::new();
    if !arguments.control_only && !connector_names.is_empty() {
        let binary = resolve_sibling_binary(
            arguments.ops_mcp_binary.as_ref(),
            "grok-ops-mcp",
            "--ops-mcp-binary",
        )?;
        for connector in connector_names {
            let mut config = ProcessProviderConfig::new(binary.clone());
            config.arguments = vec![connector, "--provider-worker".to_owned()];
            let provider = ProcessExecutionProvider::start(config).await?;
            handle.register_provider(provider.clone()).await?;
            connector_providers.push(provider);
        }
    }
    if let Some(profile) = &profile {
        validate_active_providers(profile, handle.providers().manifests().await)?;
    }
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
        agent_provider = agent_provider.is_some(),
        native_provider = native_provider.is_some(),
        connector_providers = connector_providers.len(),
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

fn native_execution_limits(profile: Option<&RuntimeProfile>) -> NativeExecutionLimits {
    const GIB: u64 = 1024 * 1024 * 1024;
    let Some(profile) = profile else {
        return NativeExecutionLimits {
            maximum_parallel: 100,
            maximum_jobs: 10_000,
            maximum_spool_bytes_per_job: 4 * GIB,
            maximum_spool_bytes_per_owner: 16 * GIB,
            maximum_total_spool_bytes: 128 * GIB,
        };
    };
    let maximum_parallel = profile
        .providers
        .iter()
        .find(|provider| provider.kind == ProviderKind::NativeExecution)
        .map(|provider| provider.maximum_concurrency)
        .unwrap_or(profile.limits.maximum_worker_processes)
        .min(profile.limits.maximum_worker_processes)
        .max(1) as usize;
    let maximum_total_spool_bytes = profile.limits.maximum_spool_bytes.max(1024);
    let maximum_spool_bytes_per_owner = maximum_total_spool_bytes.min(16 * GIB).max(1024);
    NativeExecutionLimits {
        maximum_parallel,
        maximum_jobs: (profile.limits.command_queue as usize)
            .saturating_add(maximum_parallel)
            .max(1),
        maximum_spool_bytes_per_job: maximum_spool_bytes_per_owner.min(4 * GIB).max(1024),
        maximum_spool_bytes_per_owner,
        maximum_total_spool_bytes,
    }
}

fn profile_requirement(
    profile: Option<&RuntimeProfile>,
    kind: ProviderKind,
) -> Option<&ProfileProviderRequirement> {
    profile?
        .providers
        .iter()
        .find(|provider| provider.kind == kind)
}

fn requested_connectors(
    explicit: &[String],
    profile: Option<&RuntimeProfile>,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut connectors = std::collections::BTreeSet::new();
    for connector in explicit {
        connectors.insert(normalize_connector(connector)?);
    }
    if let Some(profile) = profile {
        for requirement in profile
            .providers
            .iter()
            .filter(|provider| provider.kind == ProviderKind::McpConnector)
        {
            let mut matched = false;
            for operation in &requirement.operations {
                let connector = if operation.as_str().starts_with("metasploit.") {
                    Some("metasploit")
                } else if operation.as_str().starts_with("bloodhound.") {
                    Some("bloodhound")
                } else if operation.as_str().starts_with("vulnerability.") {
                    Some("vulnerability-index")
                } else {
                    None
                };
                if let Some(connector) = connector {
                    connectors.insert(connector.to_owned());
                    matched = true;
                }
            }
            if !matched && explicit.is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "an MCP provider requirement must name a metasploit.*, bloodhound.*, or vulnerability.* operation, or grokd must receive --ops-connector",
                )
                .into());
            }
        }
    }
    Ok(connectors.into_iter().collect())
}

fn normalize_connector(connector: &str) -> Result<String, Box<dyn std::error::Error>> {
    match connector {
        "metasploit" => Ok("metasploit".to_owned()),
        "bloodhound" | "neo4j" => Ok("bloodhound".to_owned()),
        "vulnerability-index" | "vuln-index" => Ok("vulnerability-index".to_owned()),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("unknown operational connector {connector:?}"),
        )
        .into()),
    }
}

fn validate_active_providers(
    profile: &RuntimeProfile,
    manifests: Vec<CapabilityManifest>,
) -> Result<(), Box<dyn std::error::Error>> {
    for requirement in &profile.providers {
        let matching = manifests.iter().find(|manifest| {
            manifest.kind == requirement.kind
                && manifest.concurrency.maximum_parallel <= requirement.maximum_concurrency
                && requirement
                    .required_features
                    .iter()
                    .all(|feature| manifest.features.contains(feature))
                && requirement
                    .operations
                    .iter()
                    .all(|operation| manifest.operation(operation).is_some())
        });
        if matching.is_none() {
            let providers = manifests
                .iter()
                .filter(|manifest| manifest.kind == requirement.kind)
                .map(|manifest| manifest.provider_id.as_str())
                .collect::<Vec<_>>();
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "runtime profile requires a dispatchable {:?} provider with operations [{}], features [{}], and maximum concurrency {}; active providers of that kind: [{}]",
                    requirement.kind,
                    requirement
                        .operations
                        .iter()
                        .map(|operation| operation.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    requirement.required_features.join(", "),
                    requirement.maximum_concurrency,
                    providers.join(", "),
                ),
            )
            .into());
        }
    }
    Ok(())
}

fn resolve_agent_binary(explicit: Option<&PathBuf>) -> Result<PathBuf, Box<dyn std::error::Error>> {
    if let Some(explicit) = explicit {
        return Ok(explicit.canonicalize()?);
    }
    let current_executable = std::env::current_exe()?;
    let directory = current_executable.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "grokd executable has no parent directory",
        )
    })?;
    for name in ["xai-grok-pager", "grok"] {
        let candidate = directory.join(name);
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!(
            "no persistent agent worker found beside {}; pass --agent-binary or use --control-only",
            current_executable.display()
        ),
    )
    .into())
}

fn resolve_sibling_binary(
    explicit: Option<&PathBuf>,
    name: &str,
    option: &str,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    if let Some(explicit) = explicit {
        return Ok(explicit.canonicalize()?);
    }
    let current_executable = std::env::current_exe()?;
    let candidate = current_executable
        .parent()
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "grokd executable has no parent directory",
            )
        })?
        .join(name);
    if candidate.is_file() {
        return Ok(candidate);
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!("no {name} found beside grokd; pass {option}"),
    )
    .into())
}

fn absolute_path(path: PathBuf) -> Result<PathBuf, std::io::Error> {
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use xai_grok_protocol::{
        ArtifactContract, ConcurrencyProfile, DeploymentTarget, LibcTarget, OperationDescriptor,
        ProfileId, RecoverySemantics, RuntimeResourceLimits, VersionRange,
    };

    use super::*;

    fn profile(requirement: ProfileProviderRequirement) -> RuntimeProfile {
        RuntimeProfile {
            schema_version: xai_grok_protocol::RUNTIME_PROFILE_SCHEMA_VERSION,
            profile_id: ProfileId::from_string("test-profile"),
            revision: 1,
            protocol_range: VersionRange::exact(PROTOCOL_VERSION),
            targets: vec![DeploymentTarget {
                target_triple: format!("{}-{}", std::env::consts::ARCH, std::env::consts::OS),
                libc: LibcTarget::Native,
            }],
            limits: RuntimeResourceLimits {
                command_queue: 32,
                event_batch: 32,
                maximum_clients: 8,
                maximum_worker_processes: 4,
                maximum_memory_bytes: 1 << 30,
                maximum_spool_bytes: 1 << 30,
            },
            providers: vec![requirement],
            models: Vec::new(),
            adapters: Vec::new(),
            tool_bundles: Vec::new(),
            maximum_headless_binary_bytes: 32 << 20,
            maximum_daemon_binary_bytes: 32 << 20,
            gui: None,
        }
    }

    fn manifest() -> CapabilityManifest {
        CapabilityManifest {
            provider_id: "native-test".into(),
            provider_version: "1".to_owned(),
            protocol: VersionRange::exact(PROTOCOL_VERSION),
            kind: ProviderKind::NativeExecution,
            features: ["cursor_artifacts".to_owned()].into_iter().collect(),
            operations: vec![OperationDescriptor {
                operation_id: "native.test".into(),
                display_name: "Native test".to_owned(),
                input_schema: serde_json::json!({"type":"object"}),
                output_schema: serde_json::json!({"type":"object"}),
                streaming: false,
                interactive: true,
                deferred: true,
            }],
            concurrency: ConcurrencyProfile {
                maximum_parallel: 2,
                queue_capacity: 16,
                exclusive_resource: None,
            },
            cancellation: xai_grok_protocol::CancellationSemantics::ProcessTree,
            recovery: RecoverySemantics::Restartable,
            artifacts: ArtifactContract::CursorStream,
            platforms: BTreeSet::new(),
            metadata: serde_json::Map::new(),
        }
    }

    #[test]
    fn active_profile_requires_a_real_matching_manifest() {
        let required = ProfileProviderRequirement {
            kind: ProviderKind::NativeExecution,
            operations: vec!["native.test".into()],
            required_features: vec!["cursor_artifacts".to_owned()],
            maximum_concurrency: 2,
        };
        let profile = profile(required);
        assert!(validate_active_providers(&profile, vec![manifest()]).is_ok());
        let error = validate_active_providers(&profile, Vec::new()).unwrap_err();
        assert!(error.to_string().contains("requires a dispatchable"));
    }

    #[test]
    fn active_profile_rejects_manifest_exceeding_its_concurrency_cap() {
        let required = ProfileProviderRequirement {
            kind: ProviderKind::NativeExecution,
            operations: vec!["native.test".into()],
            required_features: Vec::new(),
            maximum_concurrency: 1,
        };
        let error = validate_active_providers(&profile(required), vec![manifest()]).unwrap_err();
        assert!(error.to_string().contains("maximum concurrency 1"));
    }

    #[test]
    fn connector_workers_are_derived_from_required_operations() {
        let required = ProfileProviderRequirement {
            kind: ProviderKind::McpConnector,
            operations: vec!["bloodhound.query".into(), "bloodhound.schema".into()],
            required_features: vec!["read_only".to_owned()],
            maximum_concurrency: 1,
        };
        assert_eq!(
            requested_connectors(&[], Some(&profile(required))).unwrap(),
            vec!["bloodhound"]
        );
        assert_eq!(
            requested_connectors(&["neo4j".to_owned()], None).unwrap(),
            vec!["bloodhound"]
        );
    }
}
