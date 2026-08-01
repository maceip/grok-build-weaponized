use std::io::Read as _;
use std::path::{Path, PathBuf};

use clap::Parser;
use xai_grok_control_plane::{
    AgentExecutionProvider, AgentProviderConfig, ControlPlane, ControlPlaneConfig,
    ControlPlaneServer, NativeExecutionProvider, ProcessExecutionProvider, ProcessProviderConfig,
    ServerConfig,
};
use xai_grok_native_execution::NativeExecutionLimits;
use xai_grok_protocol::{
    ArtifactBinding, CapabilityManifest, LibcTarget, PROTOCOL_VERSION, ProfileProviderRequirement,
    ProviderKind, RuntimeProfile,
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

    /// Isolated LiteRT-LM worker executable used by the daemon-owned agent.
    /// Defaults to `grok-local-runtime-worker` beside the agent binary.
    #[arg(long, env = "GROK_LOCAL_RUNTIME_WORKER")]
    local_runtime_worker: Option<PathBuf>,

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
        let runtime_worker =
            resolve_runtime_worker(arguments.local_runtime_worker.as_ref(), &binary)?;
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
        provider_config
            .environment
            .insert("GROK_LOCAL_RUNTIME_MODE".to_owned(), "worker".to_owned());
        provider_config.environment.insert(
            "GROK_LOCAL_RUNTIME_WORKER".to_owned(),
            runtime_worker.to_string_lossy().into_owned(),
        );
        if let Some(profile) = profile.as_ref() {
            provider_config.environment.insert(
                "GROK_RESOURCE_MAX_MEMORY_BYTES".to_owned(),
                profile.limits.maximum_memory_bytes.to_string(),
            );
            let runtime_workers = profile_requirement(Some(profile), ProviderKind::ModelRuntime)
                .map(|requirement| requirement.maximum_concurrency)
                .unwrap_or(1)
                .min(profile.limits.maximum_worker_processes)
                .max(1);
            provider_config.environment.insert(
                "GROK_LOCAL_RUNTIME_MAX_WORKERS".to_owned(),
                runtime_workers.to_string(),
            );
        }
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

fn resolve_runtime_worker(
    explicit: Option<&PathBuf>,
    agent_binary: &std::path::Path,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    if let Some(explicit) = explicit {
        return Ok(explicit.canonicalize()?);
    }
    let candidate = agent_binary
        .parent()
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "agent executable has no parent directory",
            )
        })?
        .join("grok-local-runtime-worker");
    if candidate.is_file() {
        return Ok(candidate);
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!(
            "no grok-local-runtime-worker found beside {}; pass --local-runtime-worker",
            agent_binary.display()
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
    verify_deployment_compatibility(&profile, &std::env::current_exe()?)?;
    verify_profile_artifacts(&profile)?;
    Ok(profile)
}

fn verify_deployment_compatibility(
    profile: &RuntimeProfile,
    daemon_binary: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let current_target = current_target_triple();
    let target = profile
        .targets
        .iter()
        .find(|target| target.target_triple == current_target)
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("runtime profile does not contain current target {current_target}"),
            )
        })?;
    verify_current_libc(&target.libc)?;
    let actual_bytes = std::fs::metadata(daemon_binary)?.len();
    if actual_bytes > profile.maximum_daemon_binary_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "grokd binary exceeds runtime profile budget: actual={actual_bytes}, maximum={}",
                profile.maximum_daemon_binary_bytes
            ),
        )
        .into());
    }
    Ok(())
}

fn current_target_triple() -> String {
    let architecture = std::env::consts::ARCH;
    if cfg!(target_os = "macos") {
        format!("{architecture}-apple-darwin")
    } else if cfg!(all(target_os = "linux", target_env = "musl")) {
        format!("{architecture}-unknown-linux-musl")
    } else if cfg!(all(target_os = "linux", target_env = "gnu")) {
        format!("{architecture}-unknown-linux-gnu")
    } else {
        format!("{architecture}-unknown-{}", std::env::consts::OS)
    }
}

fn verify_current_libc(required: &LibcTarget) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(target_os = "macos")]
    if matches!(required, LibcTarget::Native) {
        return Ok(());
    }
    #[cfg(all(target_os = "linux", target_env = "musl"))]
    if matches!(required, LibcTarget::MuslStatic) {
        return Ok(());
    }
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    if let LibcTarget::Glibc { minimum } = required {
        let actual = current_glibc_version()?;
        if numeric_version_at_least(&actual, minimum) {
            return Ok(());
        }
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("glibc {actual} is older than profile minimum {minimum}"),
        )
        .into());
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("runtime profile libc target {required:?} does not match this binary"),
    )
    .into())
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn current_glibc_version() -> Result<String, Box<dyn std::error::Error>> {
    unsafe extern "C" {
        fn gnu_get_libc_version() -> *const std::ffi::c_char;
    }
    // SAFETY: glibc returns a process-lifetime NUL-terminated version string.
    let pointer = unsafe { gnu_get_libc_version() };
    if pointer.is_null() {
        return Err(std::io::Error::other("glibc version query returned null").into());
    }
    // SAFETY: the non-null pointer is owned by glibc and remains valid.
    Ok(unsafe { std::ffi::CStr::from_ptr(pointer) }
        .to_str()?
        .to_owned())
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn numeric_version_at_least(actual: &str, minimum: &str) -> bool {
    let components = |version: &str| {
        version
            .split('.')
            .map(|component| component.parse::<u64>().unwrap_or(0))
            .collect::<Vec<_>>()
    };
    let mut actual = components(actual);
    let mut minimum = components(minimum);
    let width = actual.len().max(minimum.len());
    actual.resize(width, 0);
    minimum.resize(width, 0);
    actual >= minimum
}

fn verify_profile_artifacts(profile: &RuntimeProfile) -> Result<(), Box<dyn std::error::Error>> {
    for (collection, artifacts) in [
        ("models", &profile.models),
        ("adapters", &profile.adapters),
        ("tool_bundles", &profile.tool_bundles),
    ] {
        for (index, artifact) in artifacts.iter().enumerate() {
            verify_artifact(artifact).map_err(|message| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("runtime profile {collection}[{index}] failed activation: {message}"),
                )
            })?;
        }
    }
    Ok(())
}

fn verify_artifact(artifact: &ArtifactBinding) -> Result<(), String> {
    let (content_hash, byte_size) = hash_artifact(&artifact.path)?;
    if content_hash != artifact.content_hash {
        return Err(format!(
            "content hash mismatch for {}: expected={}, actual={content_hash}",
            artifact.path.display(),
            artifact.content_hash
        ));
    }
    if byte_size != artifact.byte_size {
        return Err(format!(
            "byte size mismatch for {}: expected={}, actual={byte_size}",
            artifact.path.display(),
            artifact.byte_size
        ));
    }
    Ok(())
}

fn hash_artifact(path: &Path) -> Result<(String, u64), String> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| format!("cannot inspect {}: {error}", path.display()))?;
    if metadata.file_type().is_symlink() {
        return Err(format!(
            "symbolic links are not allowed: {}",
            path.display()
        ));
    }
    if metadata.is_file() {
        let mut hasher = blake3::Hasher::new();
        hash_file_contents(path, &mut hasher)?;
        return Ok((hasher.finalize().to_hex().to_string(), metadata.len()));
    }
    if !metadata.is_dir() {
        return Err(format!(
            "artifact is not a regular file or directory: {}",
            path.display()
        ));
    }
    let mut hasher = blake3::Hasher::new();
    let mut byte_size = 0_u64;
    hash_directory(path, path, &mut hasher, &mut byte_size)?;
    Ok((hasher.finalize().to_hex().to_string(), byte_size))
}

fn hash_directory(
    root: &Path,
    path: &Path,
    hasher: &mut blake3::Hasher,
    byte_size: &mut u64,
) -> Result<(), String> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| format!("cannot inspect {}: {error}", path.display()))?;
    if metadata.file_type().is_symlink() {
        return Err(format!("artifact contains a symlink: {}", path.display()));
    }
    if metadata.is_dir() {
        let mut children = std::fs::read_dir(path)
            .map_err(|error| format!("cannot enumerate {}: {error}", path.display()))?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("cannot enumerate {}: {error}", path.display()))?;
        children.sort();
        for child in children {
            hash_directory(root, &child, hasher, byte_size)?;
        }
        return Ok(());
    }
    if !metadata.is_file() {
        return Err(format!("unsupported artifact entry: {}", path.display()));
    }
    let relative = path.strip_prefix(root).unwrap_or(path);
    hasher.update(relative.as_os_str().as_encoded_bytes());
    hasher.update(&metadata.len().to_le_bytes());
    *byte_size = byte_size.saturating_add(metadata.len());
    hash_file_contents(path, hasher)
}

fn hash_file_contents(path: &Path, hasher: &mut blake3::Hasher) -> Result<(), String> {
    let mut file = std::fs::File::open(path)
        .map_err(|error| format!("cannot open {}: {error}", path.display()))?;
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
        if read == 0 {
            return Ok(());
        }
        hasher.update(&buffer[..read]);
    }
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
                target_triple: current_target_triple(),
                libc: if cfg!(target_os = "macos") {
                    LibcTarget::Native
                } else if cfg!(all(target_os = "linux", target_env = "musl")) {
                    LibcTarget::MuslStatic
                } else {
                    LibcTarget::Glibc {
                        minimum: "2.0".to_owned(),
                    }
                },
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

    #[test]
    fn profile_activation_verifies_real_artifact_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let artifact_path = directory.path().join("executor.litertlm");
        std::fs::write(&artifact_path, b"immutable model bytes").unwrap();
        let required = ProfileProviderRequirement {
            kind: ProviderKind::ModelRuntime,
            operations: vec!["agent.turn".into()],
            required_features: Vec::new(),
            maximum_concurrency: 1,
        };
        let mut profile = profile(required);
        profile.models.push(ArtifactBinding {
            logical_name: "executor".to_owned(),
            path: artifact_path.clone(),
            content_hash: blake3::hash(b"immutable model bytes").to_hex().to_string(),
            byte_size: 21,
        });
        verify_profile_artifacts(&profile).unwrap();

        std::fs::write(artifact_path, b"changed model bytes").unwrap();
        let error = verify_profile_artifacts(&profile).unwrap_err();
        assert!(error.to_string().contains("content hash mismatch"));
    }

    #[test]
    fn profile_activation_enforces_target_and_daemon_size() {
        let directory = tempfile::tempdir().unwrap();
        let daemon = directory.path().join("grokd");
        std::fs::write(&daemon, [7_u8; 32]).unwrap();
        let required = ProfileProviderRequirement {
            kind: ProviderKind::ModelRuntime,
            operations: Vec::new(),
            required_features: Vec::new(),
            maximum_concurrency: 1,
        };
        let mut profile = profile(required);
        profile.maximum_daemon_binary_bytes = 32;
        verify_deployment_compatibility(&profile, &daemon).unwrap();

        profile.maximum_daemon_binary_bytes = 31;
        let error = verify_deployment_compatibility(&profile, &daemon).unwrap_err();
        assert!(error.to_string().contains("exceeds runtime profile budget"));

        profile.maximum_daemon_binary_bytes = 32;
        profile.targets[0].target_triple = "wrong-unknown-target".to_owned();
        let error = verify_deployment_compatibility(&profile, &daemon).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("does not contain current target")
        );
    }
}
