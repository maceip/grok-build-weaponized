//! Model-facing wrapper for the scoped native Nmap driver.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::computer::local::daemon_terminal::{invoke as invoke_daemon, provider_result};
use crate::native::execution_supervisor::ExecutionSupervisor;
use crate::native::job_registry::{ExecutionJobHandle, ExecutionJobKind, ExecutionJobLifecycle};
use crate::native::nmap::{
    NativeNmapDriver, NmapAddress, NmapHost, NmapPort, NmapScanArtifacts, NmapScanEvent,
    NmapScanReport, NmapScanRequest, NmapService, ScanProfile, ScanScope, parse_nmap_xml,
};
use crate::types::output::{DynamicOutput, ToolOutput};
use crate::types::requirements::Expr;
use crate::types::tool::{ToolKind, ToolNamespace};
use crate::types::tool_io::{MCPToolInput, ToolInput};

pub const OPERATIONAL_SCOPE_ENV: &str = "GROK_OPERATIONAL_SCOPE";
const EXECUTION_BACKEND_ENV: &str = "GROK_EXECUTION_BACKEND";
const GROKD_SOCKET_ENV: &str = "GROKD_SOCKET";
const MAX_BACKGROUND_NMAP_JOBS: usize = 32;
const MAX_NMAP_PORTS: usize = 1_024;
const MAX_NMAP_TIMEOUT_SECS: u64 = 2 * 60 * 60;
const DAEMON_POLL_INTERVAL: Duration = Duration::from_millis(100);

fn validate_scan_request(
    scan: &NmapScanRequest,
) -> Result<(), xai_tool_runtime::ToolError> {
    if scan.ports.len() > MAX_NMAP_PORTS {
        return Err(xai_tool_runtime::ToolError::invalid_arguments(format!(
            "at most {MAX_NMAP_PORTS} explicit ports are allowed"
        )));
    }
    let timeout = scan.timeout_secs.unwrap_or(30 * 60);
    if timeout == 0 || timeout > MAX_NMAP_TIMEOUT_SECS {
        return Err(xai_tool_runtime::ToolError::invalid_arguments(format!(
            "nmap timeout must be between 1 and {MAX_NMAP_TIMEOUT_SECS} seconds"
        )));
    }
    Ok(())
}

fn daemon_socket() -> Result<Option<PathBuf>, xai_tool_runtime::ToolError> {
    if std::env::var(EXECUTION_BACKEND_ENV).as_deref() != Ok("daemon") {
        return Ok(None);
    }
    let socket = std::env::var_os(GROKD_SOCKET_ENV)
        .map(PathBuf::from)
        .ok_or_else(|| {
            xai_tool_runtime::ToolError::custom(
                "native_nmap",
                format!(
                    "{EXECUTION_BACKEND_ENV}=daemon requires an absolute {GROKD_SOCKET_ENV}"
                ),
            )
        })?;
    if !socket.is_absolute() {
        return Err(xai_tool_runtime::ToolError::custom(
            "native_nmap",
            format!("{GROKD_SOCKET_ENV} must be an absolute path in daemon mode"),
        ));
    }
    Ok(Some(socket))
}

fn daemon_request(
    scan: &NmapScanRequest,
    allowed_targets: Vec<String>,
) -> xai_grok_native_execution::NmapRequest {
    xai_grok_native_execution::NmapRequest {
        target: scan.target.clone(),
        allowed_targets,
        profile: match scan.profile {
            ScanProfile::HostDiscovery => xai_grok_native_execution::ScanProfile::HostDiscovery,
            ScanProfile::TcpConnect => xai_grok_native_execution::ScanProfile::TcpConnect,
            ScanProfile::ServiceDiscovery => {
                xai_grok_native_execution::ScanProfile::ServiceDiscovery
            }
        },
        ports: scan.ports.clone(),
        timeout_ms: scan
            .timeout_secs
            .unwrap_or(30 * 60)
            .saturating_mul(1_000),
    }
}

fn daemon_report(result: xai_grok_native_execution::NmapResult) -> NmapScanReport {
    NmapScanReport {
        target: result.target,
        profile: match result.profile {
            xai_grok_native_execution::ScanProfile::HostDiscovery => ScanProfile::HostDiscovery,
            xai_grok_native_execution::ScanProfile::TcpConnect => ScanProfile::TcpConnect,
            xai_grok_native_execution::ScanProfile::ServiceDiscovery => {
                ScanProfile::ServiceDiscovery
            }
        },
        scanner_version: result.scanner_version,
        started_at: result.started_at,
        elapsed_seconds: result.elapsed_seconds,
        hosts_up: result.hosts_up,
        hosts_down: result.hosts_down,
        hosts_total: result.hosts_total,
        hosts: result.hosts.into_iter().map(daemon_host).collect(),
    }
}

fn daemon_host(host: xai_grok_native_execution::NmapHost) -> NmapHost {
    NmapHost {
        status: host.status,
        status_reason: host.status_reason,
        addresses: host
            .addresses
            .into_iter()
            .map(|address| NmapAddress {
                address: address.address,
                address_type: address.address_type,
                vendor: address.vendor,
            })
            .collect(),
        hostnames: host.hostnames,
        ports: host.ports.into_iter().map(daemon_port).collect(),
    }
}

fn daemon_port(port: xai_grok_native_execution::NmapPort) -> NmapPort {
    NmapPort {
        protocol: port.protocol,
        port: port.port,
        state: port.state,
        state_reason: port.state_reason,
        service: port.service.map(|service| NmapService {
            name: service.name,
            product: service.product,
            version: service.version,
            extra_info: service.extra_info,
            tunnel: service.tunnel,
            os_type: service.os_type,
            method: service.method,
            confidence: service.confidence,
        }),
    }
}

async fn daemon_start(
    socket: &Path,
    scan: &NmapScanRequest,
    allowed_targets: Vec<String>,
) -> Result<String, xai_tool_runtime::ToolError> {
    let output = invoke_daemon(
        socket,
        "native.nmap.start",
        serde_json::to_value(daemon_request(scan, allowed_targets)).map_err(|error| {
            xai_tool_runtime::ToolError::custom("native_nmap", error.to_string())
        })?,
        Duration::from_secs(10),
    )
    .await
    .map_err(|error| xai_tool_runtime::ToolError::custom("native_nmap", error.to_string()))?;
    let snapshot: xai_grok_native_execution::JobSnapshot =
        serde_json::from_value(provider_result(output)).map_err(|error| {
            xai_tool_runtime::ToolError::custom(
                "native_nmap",
                format!("invalid daemon Nmap start response: {error}"),
            )
        })?;
    Ok(snapshot.job_id)
}

async fn daemon_snapshot(
    socket: &Path,
    job_id: &str,
    operation: &str,
) -> Result<xai_grok_native_execution::JobSnapshot, xai_tool_runtime::ToolError> {
    let output = invoke_daemon(
        socket,
        operation,
        serde_json::json!({"job_id": job_id}),
        Duration::from_secs(10),
    )
    .await
    .map_err(|error| xai_tool_runtime::ToolError::custom("native_nmap", error.to_string()))?;
    serde_json::from_value(provider_result(output)).map_err(|error| {
        xai_tool_runtime::ToolError::custom(
            "native_nmap",
            format!("invalid daemon Nmap snapshot: {error}"),
        )
    })
}

async fn daemon_result(
    socket: &Path,
    job_id: &str,
) -> Result<NmapScanReport, xai_tool_runtime::ToolError> {
    let output = invoke_daemon(
        socket,
        "native.nmap.result",
        serde_json::json!({"job_id": job_id}),
        Duration::from_secs(10),
    )
    .await
    .map_err(|error| xai_tool_runtime::ToolError::custom("native_nmap", error.to_string()))?;
    let result = serde_json::from_value(provider_result(output)).map_err(|error| {
        xai_tool_runtime::ToolError::custom(
            "native_nmap",
            format!("invalid daemon Nmap result: {error}"),
        )
    })?;
    Ok(daemon_report(result))
}

fn elapsed_millis(snapshot: &xai_grok_native_execution::JobSnapshot) -> u64 {
    let end = snapshot.finished_unix_ms.unwrap_or_else(now_unix_ms);
    end.saturating_sub(snapshot.started_unix_ms.unwrap_or(snapshot.created_unix_ms))
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum NativeNmapJobRequest {
    /// Start a scan and return immediately with a job identifier.
    Start { scan: NmapScanRequest },
    /// Read the latest progressively parsed hosts or final report.
    Status { job_id: String },
    /// Read a bounded page of progressively parsed or completed hosts.
    Page {
        job_id: String,
        cursor: usize,
        limit: usize,
    },
    /// Cancel a running scan. The subprocess is killed on future drop.
    Cancel { job_id: String },
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum NativeNmapJobResponse {
    Started {
        job_id: String,
    },
    Running {
        job_id: String,
        elapsed_millis: u64,
        hosts: Vec<NmapHost>,
        total_hosts: usize,
        next_cursor: Option<usize>,
    },
    Completed {
        job_id: String,
        elapsed_millis: u64,
        report: NmapScanReport,
        total_hosts: usize,
        next_cursor: Option<usize>,
    },
    Failed {
        job_id: String,
        elapsed_millis: u64,
        error: String,
    },
    Cancelled {
        job_id: String,
        elapsed_millis: u64,
    },
}

fn nmap_job_response(
    job: &ExecutionJobHandle,
    cursor: usize,
    limit: usize,
) -> Result<NativeNmapJobResponse, String> {
    let snapshot = job.snapshot();
    let limit = limit.clamp(1, 100);
    match snapshot.lifecycle {
        ExecutionJobLifecycle::Running => {
            let hosts = report_from_snapshot_artifact(&snapshot)
                .map(|report| report.hosts)
                .or_else(|| {
                    snapshot
                        .payload
                        .get("hosts")
                        .cloned()
                        .and_then(|hosts| serde_json::from_value(hosts).ok())
                })
                .unwrap_or_default();
            let total_hosts = hosts.len();
            let page = hosts
                .into_iter()
                .skip(cursor)
                .take(limit)
                .collect::<Vec<_>>();
            let next_cursor = (cursor.saturating_add(page.len()) < total_hosts)
                .then_some(cursor.saturating_add(page.len()));
            Ok(NativeNmapJobResponse::Running {
                job_id: snapshot.job_id,
                elapsed_millis: snapshot.elapsed_millis,
                hosts: page,
                total_hosts,
                next_cursor,
            })
        }
        ExecutionJobLifecycle::Completed => {
            let mut report = report_from_snapshot_artifact(&snapshot)
                .or_else(|| serde_json::from_value::<NmapScanReport>(snapshot.payload).ok())
                .ok_or_else(|| {
                    "completed Nmap job has neither a valid XML artifact nor a report".to_string()
                })?;
            let total_hosts = report.hosts.len();
            report.hosts = report.hosts.into_iter().skip(cursor).take(limit).collect();
            let next_cursor = (cursor.saturating_add(report.hosts.len()) < total_hosts)
                .then_some(cursor.saturating_add(report.hosts.len()));
            Ok(NativeNmapJobResponse::Completed {
                job_id: snapshot.job_id,
                elapsed_millis: snapshot.elapsed_millis,
                report,
                total_hosts,
                next_cursor,
            })
        }
        ExecutionJobLifecycle::Failed => Ok(NativeNmapJobResponse::Failed {
            job_id: snapshot.job_id,
            elapsed_millis: snapshot.elapsed_millis,
            error: snapshot
                .error
                .unwrap_or_else(|| "native Nmap job failed".to_string()),
        }),
        ExecutionJobLifecycle::Cancelled => Ok(NativeNmapJobResponse::Cancelled {
            job_id: snapshot.job_id,
            elapsed_millis: snapshot.elapsed_millis,
        }),
        ExecutionJobLifecycle::Lost => Ok(NativeNmapJobResponse::Failed {
            job_id: snapshot.job_id,
            elapsed_millis: snapshot.elapsed_millis,
            error: snapshot.error.unwrap_or_else(|| {
                "native Nmap process identity could not be safely recovered".to_string()
            }),
        }),
    }
}

async fn daemon_job_response(
    socket: &Path,
    job_id: &str,
    cursor: usize,
    limit: usize,
) -> Result<NativeNmapJobResponse, xai_tool_runtime::ToolError> {
    let snapshot = daemon_snapshot(socket, job_id, "native.nmap.status").await?;
    let elapsed_millis = elapsed_millis(&snapshot);
    let limit = limit.clamp(1, 100);
    match snapshot.lifecycle {
        xai_grok_native_execution::JobLifecycle::Queued
        | xai_grok_native_execution::JobLifecycle::Running => {
            let hosts = snapshot
                .result
                .and_then(|result| {
                    serde_json::from_value::<xai_grok_native_execution::NmapResult>(result).ok()
                })
                .map(|result| daemon_report(result).hosts)
                .unwrap_or_default();
            let total_hosts = hosts.len();
            let hosts = hosts
                .into_iter()
                .skip(cursor)
                .take(limit)
                .collect::<Vec<_>>();
            let next_cursor = (cursor.saturating_add(hosts.len()) < total_hosts)
                .then_some(cursor.saturating_add(hosts.len()));
            Ok(NativeNmapJobResponse::Running {
                job_id: snapshot.job_id,
                elapsed_millis,
                hosts,
                total_hosts,
                next_cursor,
            })
        }
        xai_grok_native_execution::JobLifecycle::Completed => {
            let mut report = daemon_result(socket, job_id).await?;
            let total_hosts = report.hosts.len();
            report.hosts = report.hosts.into_iter().skip(cursor).take(limit).collect();
            let next_cursor = (cursor.saturating_add(report.hosts.len()) < total_hosts)
                .then_some(cursor.saturating_add(report.hosts.len()));
            Ok(NativeNmapJobResponse::Completed {
                job_id: snapshot.job_id,
                elapsed_millis,
                report,
                total_hosts,
                next_cursor,
            })
        }
        xai_grok_native_execution::JobLifecycle::Cancelled => {
            Ok(NativeNmapJobResponse::Cancelled {
                job_id: snapshot.job_id,
                elapsed_millis,
            })
        }
        xai_grok_native_execution::JobLifecycle::Failed
        | xai_grok_native_execution::JobLifecycle::TimedOut
        | xai_grok_native_execution::JobLifecycle::Lost => {
            Ok(NativeNmapJobResponse::Failed {
                job_id: snapshot.job_id,
                elapsed_millis,
                error: snapshot.error.unwrap_or_else(|| {
                    format!("daemon Nmap job terminated as {:?}", snapshot.lifecycle)
                }),
            })
        }
    }
}

fn report_from_snapshot_artifact(
    snapshot: &crate::native::job_registry::ExecutionJobSnapshot,
) -> Option<NmapScanReport> {
    let request: NmapScanRequest =
        serde_json::from_value(snapshot.payload.get("scan")?.clone()).ok()?;
    let path = snapshot
        .stdout_artifact
        .as_deref()
        .or(snapshot.artifact.as_deref())?;
    let xml = std::fs::read(path).ok()?;
    parse_nmap_xml(&request.target, request.profile, &xml).ok()
}

fn nmap_job_artifacts(job_id: &str) -> std::io::Result<NmapScanArtifacts> {
    let root = std::env::var_os("GROK_JOB_ARTIFACT_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::env::temp_dir()
                .join("grok-build")
                .join("job-artifacts")
        })
        .join("nmap");
    std::fs::create_dir_all(&root)?;
    Ok(NmapScanArtifacts {
        xml: root.join(format!("{job_id}.xml")),
        stderr: root.join(format!("{job_id}.stderr")),
        status: root.join(format!("{job_id}.status.json")),
    })
}

#[derive(Debug, Default)]
pub struct NativeNmapTool;

impl crate::types::tool_metadata::ToolMetadata for NativeNmapTool {
    fn kind(&self) -> ToolKind {
        ToolKind::Execute
    }

    fn tool_namespace(&self) -> ToolNamespace {
        ToolNamespace::GrokBuild
    }

    fn description_template(&self) -> &str {
        "Run a local, scope-constrained Nmap observability scan and return parsed JSON. \
         Supports host discovery, TCP connect scanning, and lightweight service discovery. \
         Raw arguments, NSE scripts, OS detection, spoofing, and targets outside the operator's \
         configured scope are rejected."
    }

    fn requires_expr(&self) -> Expr<crate::types::requirements::ToolRequirement> {
        Expr::True
    }
}

impl xai_tool_runtime::Tool for NativeNmapTool {
    type Args = NmapScanRequest;
    type Output = NmapScanReport;

    fn id(&self) -> xai_tool_protocol::ToolId {
        xai_tool_protocol::ToolId::new("native_nmap").expect("valid tool id")
    }

    fn description(
        &self,
        _ctx: &xai_tool_runtime::ListToolsContext,
    ) -> xai_tool_types::ToolDescription {
        xai_tool_types::ToolDescription::new(
            "native_nmap",
            crate::types::tool_metadata::ToolMetadata::sanitized_description_template(self),
        )
    }

    fn capabilities(&self) -> xai_tool_protocol::ToolCapabilities {
        xai_tool_protocol::ToolCapabilities {
            is_read_only: false,
            tool_scope: Some(xai_tool_protocol::ToolScope::Write),
            ..Default::default()
        }
    }

    fn should_list(&self, _ctx: &xai_tool_runtime::ListToolsContext) -> bool {
        configured_scope().is_some()
    }

    #[tracing::instrument(name = "tool.native_nmap", skip_all, fields(target = %input.target))]
    async fn run(
        &self,
        ctx: xai_tool_runtime::ToolCallContext,
        input: NmapScanRequest,
    ) -> Result<NmapScanReport, xai_tool_runtime::ToolError> {
        let scope_targets = configured_scope().ok_or_else(|| {
            xai_tool_runtime::ToolError::permission_denied(format!(
                "native_nmap requires an explicit operational scope; start Grok with \
                 --operational-scope or set {OPERATIONAL_SCOPE_ENV}"
            ))
        })?;
        let scope = ScanScope::new(scope_targets.clone())
            .map_err(|error| xai_tool_runtime::ToolError::invalid_arguments(error.to_string()))?;
        validate_scan_request(&input)?;
        if !scope.allows(&input.target).map_err(|error| {
            xai_tool_runtime::ToolError::invalid_arguments(error.to_string())
        })? {
            return Err(xai_tool_runtime::ToolError::invalid_arguments(format!(
                "target `{}` is outside the configured scan scope",
                input.target
            )));
        }
        if let Some(socket) = daemon_socket()? {
            let job_id = daemon_start(&socket, &input, scope_targets).await?;
            let tool_id = xai_tool_runtime::Tool::id(self);
            loop {
                let status = daemon_snapshot(&socket, &job_id, "native.nmap.status");
                let snapshot = if let Some(cancellation) =
                    ctx.get::<xai_tool_runtime::Cancellation>()
                {
                    tokio::select! {
                        result = status => result?,
                        _ = cancellation.0.cancelled() => {
                            daemon_snapshot(&socket, &job_id, "native.nmap.cancel").await?;
                            return Err(xai_tool_runtime::ToolError::cancelled(
                                tool_id,
                                "daemon-owned native Nmap scan cancelled",
                            ));
                        }
                    }
                } else {
                    status.await?
                };
                match snapshot.lifecycle {
                    xai_grok_native_execution::JobLifecycle::Completed => {
                        return daemon_result(&socket, &job_id).await;
                    }
                    xai_grok_native_execution::JobLifecycle::Queued
                    | xai_grok_native_execution::JobLifecycle::Running => {
                        if let Some(cancellation) = ctx.get::<xai_tool_runtime::Cancellation>() {
                            tokio::select! {
                                _ = tokio::time::sleep(DAEMON_POLL_INTERVAL) => {}
                                _ = cancellation.0.cancelled() => {
                                    daemon_snapshot(&socket, &job_id, "native.nmap.cancel").await?;
                                    return Err(xai_tool_runtime::ToolError::cancelled(
                                        tool_id,
                                        "daemon-owned native Nmap scan cancelled",
                                    ));
                                }
                            }
                        } else {
                            tokio::time::sleep(DAEMON_POLL_INTERVAL).await;
                        }
                    }
                    xai_grok_native_execution::JobLifecycle::Cancelled => {
                        return Err(xai_tool_runtime::ToolError::cancelled(
                            tool_id,
                            "daemon-owned native Nmap scan cancelled",
                        ));
                    }
                    lifecycle => {
                        return Err(xai_tool_runtime::ToolError::custom(
                            "native_nmap",
                            snapshot.error.unwrap_or_else(|| {
                                format!("daemon-owned native Nmap scan ended as {lifecycle:?}")
                            }),
                        ));
                    }
                }
            }
        }
        let driver = NativeNmapDriver::discover().map_err(|error| {
            xai_tool_runtime::ToolError::custom("native_nmap", error.to_string())
        })?;
        let tool_id = xai_tool_runtime::Tool::id(self);
        let scan = driver.scan(&scope, input);
        tokio::pin!(scan);

        if let Some(cancellation) = ctx.get::<xai_tool_runtime::Cancellation>() {
            tokio::select! {
                result = &mut scan => result.map_err(|error| {
                    xai_tool_runtime::ToolError::custom("native_nmap", error.to_string())
                }),
                _ = cancellation.0.cancelled() => {
                    Err(xai_tool_runtime::ToolError::cancelled(
                        tool_id,
                        "native Nmap scan cancelled",
                    ))
                }
            }
        } else {
            scan.await.map_err(|error| {
                xai_tool_runtime::ToolError::custom("native_nmap", error.to_string())
            })
        }
    }
}

fn configured_scope() -> Option<Vec<String>> {
    let values = std::env::var(OPERATIONAL_SCOPE_ENV).ok()?;
    let targets = values
        .split(',')
        .map(str::trim)
        .filter(|target| !target.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    (!targets.is_empty()).then_some(targets)
}

impl From<NmapScanRequest> for ToolInput {
    fn from(input: NmapScanRequest) -> Self {
        let value = serde_json::to_value(&input)
            .expect("NmapScanRequest contains only JSON-serializable fields");
        Self::MCPTool(MCPToolInput {
            tool_name: "native_nmap".to_owned(),
            tool_input: value,
        })
    }
}

impl From<NmapScanReport> for ToolOutput {
    fn from(report: NmapScanReport) -> Self {
        let value = serde_json::to_value(report).expect("NmapScanReport contains only JSON values");
        Self::Dynamic(DynamicOutput { value })
    }
}

impl xai_tool_runtime::ToolOutput for NmapScanReport {}

#[derive(Debug, Default)]
pub struct NativeNmapJobTool;

impl crate::types::tool_metadata::ToolMetadata for NativeNmapJobTool {
    fn kind(&self) -> ToolKind {
        ToolKind::Execute
    }

    fn tool_namespace(&self) -> ToolNamespace {
        ToolNamespace::GrokBuild
    }

    fn description_template(&self) -> &str {
        "Manage long-running native Nmap scans without occupying the interactive turn. \
         Start returns a job ID immediately; status returns progressively parsed hosts or the \
         final typed report; cancel terminates the subprocess."
    }

    fn requires_expr(&self) -> Expr<crate::types::requirements::ToolRequirement> {
        Expr::True
    }
}

impl xai_tool_runtime::Tool for NativeNmapJobTool {
    type Args = NativeNmapJobRequest;
    type Output = NativeNmapJobResponse;

    fn id(&self) -> xai_tool_protocol::ToolId {
        xai_tool_protocol::ToolId::new("native_nmap_job").expect("valid tool id")
    }

    fn description(
        &self,
        _ctx: &xai_tool_runtime::ListToolsContext,
    ) -> xai_tool_types::ToolDescription {
        xai_tool_types::ToolDescription::new(
            "native_nmap_job",
            crate::types::tool_metadata::ToolMetadata::sanitized_description_template(self),
        )
    }

    fn capabilities(&self) -> xai_tool_protocol::ToolCapabilities {
        xai_tool_protocol::ToolCapabilities {
            is_read_only: false,
            tool_scope: Some(xai_tool_protocol::ToolScope::Write),
            ..Default::default()
        }
    }

    fn should_list(&self, _ctx: &xai_tool_runtime::ListToolsContext) -> bool {
        configured_scope().is_some()
    }

    #[tracing::instrument(name = "tool.native_nmap_job", skip_all)]
    async fn run(
        &self,
        ctx: xai_tool_runtime::ToolCallContext,
        input: NativeNmapJobRequest,
    ) -> Result<NativeNmapJobResponse, xai_tool_runtime::ToolError> {
        let daemon = daemon_socket()?;
        let owner_session_id =
            if let Ok(resources) = crate::types::tool_metadata::shared_resources(&ctx) {
                resources
                    .lock()
                    .await
                    .get::<crate::types::resources::OwnerSessionId>()
                    .map(|owner| owner.0.clone())
            } else {
                None
            };
        match input {
            NativeNmapJobRequest::Start { scan } => {
                let scope_targets = configured_scope().ok_or_else(|| {
                    xai_tool_runtime::ToolError::invalid_arguments(format!(
                        "native_nmap_job requires {OPERATIONAL_SCOPE_ENV}"
                    ))
                })?;
                let scope = ScanScope::new(scope_targets.clone()).map_err(|error| {
                    xai_tool_runtime::ToolError::invalid_arguments(error.to_string())
                })?;
                validate_scan_request(&scan)?;
                // Validate the target synchronously so Start never returns a
                // job that was doomed before process creation.
                if !scope.allows(&scan.target).map_err(|error| {
                    xai_tool_runtime::ToolError::invalid_arguments(error.to_string())
                })? {
                    return Err(xai_tool_runtime::ToolError::invalid_arguments(format!(
                        "target `{}` is outside the configured scan scope",
                        scan.target
                    )));
                }
                if let Some(socket) = daemon.as_deref() {
                    let job_id = daemon_start(socket, &scan, scope_targets).await?;
                    return Ok(NativeNmapJobResponse::Started { job_id });
                }
                let driver = NativeNmapDriver::discover().map_err(|error| {
                    xai_tool_runtime::ToolError::custom("native_nmap_job", error.to_string())
                })?;
                let registry = ExecutionSupervisor::global().jobs();
                if registry.running_count(ExecutionJobKind::Nmap) >= MAX_BACKGROUND_NMAP_JOBS {
                    return Err(xai_tool_runtime::ToolError::custom(
                        "native_nmap_job",
                        format!(
                            "all {MAX_BACKGROUND_NMAP_JOBS} background Nmap job slots are active"
                        ),
                    ));
                }
                let job_id = uuid::Uuid::now_v7().simple().to_string();
                let artifacts = nmap_job_artifacts(&job_id).map_err(|error| {
                    xai_tool_runtime::ToolError::custom(
                        "native_nmap_job",
                        format!("failed to prepare Nmap artifacts: {error}"),
                    )
                })?;
                let command_hash = {
                    let mut hasher = blake3::Hasher::new();
                    let scan_bytes = serde_json::to_vec(&scan)
                        .unwrap_or_else(|_| scan.target.as_bytes().to_vec());
                    for part in [
                        driver.binary().as_os_str().as_encoded_bytes(),
                        scan_bytes.as_slice(),
                    ] {
                        hasher.update(&(part.len() as u64).to_le_bytes());
                        hasher.update(part);
                    }
                    hasher.finalize().to_hex().to_string()
                };
                let job = ExecutionSupervisor::global()
                    .register_job(
                        job_id.clone(),
                        ExecutionJobKind::Nmap,
                        owner_session_id,
                        Some(artifacts.xml.clone()),
                        Some(ctx.call_id.as_str()),
                        command_hash,
                    )
                    .map_err(|error| {
                        xai_tool_runtime::ToolError::custom("native_nmap_job", error)
                    })?;
                job.configure_durable_artifacts(
                    Some(artifacts.xml.clone()),
                    Some(artifacts.stderr.clone()),
                    Some(artifacts.status.clone()),
                );
                job.persist_spawn_intent().await.map_err(|error| {
                    xai_tool_runtime::ToolError::custom(
                        "native_nmap_job",
                        format!("failed to persist Nmap spawn intent: {error}"),
                    )
                })?;
                job.update(serde_json::json!({
                    "scan": scan.clone(),
                    "hosts": [],
                }));
                tokio::spawn(run_background_nmap_job(driver, scope, scan, artifacts, job));
                Ok(NativeNmapJobResponse::Started { job_id })
            }
            NativeNmapJobRequest::Status { job_id } => {
                if let Some(socket) = daemon.as_deref() {
                    return daemon_job_response(socket, &job_id, 0, 100).await;
                }
                let job = ExecutionSupervisor::global()
                    .jobs()
                    .get(&job_id)
                    .ok_or_else(|| {
                        xai_tool_runtime::ToolError::invalid_arguments(format!(
                            "unknown native Nmap job `{job_id}`"
                        ))
                    })?;
                nmap_job_response(&job, 0, 100)
                    .map_err(|error| xai_tool_runtime::ToolError::custom("native_nmap_job", error))
            }
            NativeNmapJobRequest::Page {
                job_id,
                cursor,
                limit,
            } => {
                if let Some(socket) = daemon.as_deref() {
                    return daemon_job_response(socket, &job_id, cursor, limit).await;
                }
                let job = ExecutionSupervisor::global()
                    .jobs()
                    .get(&job_id)
                    .ok_or_else(|| {
                        xai_tool_runtime::ToolError::invalid_arguments(format!(
                            "unknown native Nmap job `{job_id}`"
                        ))
                    })?;
                nmap_job_response(&job, cursor, limit)
                    .map_err(|error| xai_tool_runtime::ToolError::custom("native_nmap_job", error))
            }
            NativeNmapJobRequest::Cancel { job_id } => {
                if let Some(socket) = daemon.as_deref() {
                    daemon_snapshot(socket, &job_id, "native.nmap.cancel").await?;
                    return daemon_job_response(socket, &job_id, 0, 100).await;
                }
                let job = ExecutionSupervisor::global()
                    .jobs()
                    .get(&job_id)
                    .ok_or_else(|| {
                        xai_tool_runtime::ToolError::invalid_arguments(format!(
                            "unknown native Nmap job `{job_id}`"
                        ))
                    })?;
                job.cancel();
                nmap_job_response(&job, 0, 100)
                    .map_err(|error| xai_tool_runtime::ToolError::custom("native_nmap_job", error))
            }
        }
    }
}

async fn run_background_nmap_job(
    driver: NativeNmapDriver,
    scope: ScanScope,
    request: NmapScanRequest,
    artifacts: NmapScanArtifacts,
    job: ExecutionJobHandle,
) {
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(64);
    #[cfg(unix)]
    let scan =
        driver.scan_with_events_to_artifacts(&scope, request.clone(), artifacts, Some(event_tx));
    #[cfg(not(unix))]
    let scan = driver.scan_with_events(&scope, request.clone(), Some(event_tx));
    tokio::pin!(scan);

    let mut hosts = Vec::new();
    let cancellation = job.cancellation();
    let mut liveness = tokio::time::interval(std::time::Duration::from_secs(5));
    liveness.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                job.cancel();
                break;
            }
            result = &mut scan => {
                match result {
                    Ok(report) => job.complete(
                        serde_json::to_value(report).unwrap_or(serde_json::Value::Null)
                    ),
                    Err(error) => job.fail(error.to_string()),
                }
                break;
            }
            event = event_rx.recv() => {
                match event {
                    Some(NmapScanEvent::Started { pid, .. }) => {
                        if let Err(error) = job
                            .attach_process(Some(pid), Some(i64::from(pid)))
                            .await
                        {
                            job.fail(format!(
                                "failed to persist Nmap process identity: {error}"
                            ));
                            break;
                        }
                    }
                    Some(NmapScanEvent::HostDiscovered { host, .. }) => {
                        hosts.push(host);
                        job.update(serde_json::json!({
                            "scan": request.clone(),
                            "hosts": &hosts,
                        }));
                    }
                    Some(NmapScanEvent::Finished { .. }) | None => {}
                }
            }
            _ = liveness.tick() => job.refresh_liveness(),
        }
    }
}

impl From<NativeNmapJobRequest> for ToolInput {
    fn from(input: NativeNmapJobRequest) -> Self {
        let value = serde_json::to_value(&input)
            .expect("NativeNmapJobRequest contains only JSON-serializable fields");
        Self::MCPTool(MCPToolInput {
            tool_name: "native_nmap_job".to_owned(),
            tool_input: value,
        })
    }
}

impl From<NativeNmapJobResponse> for ToolOutput {
    fn from(response: NativeNmapJobResponse) -> Self {
        let value = serde_json::to_value(response)
            .expect("NativeNmapJobResponse contains only JSON-serializable fields");
        Self::Dynamic(DynamicOutput { value })
    }
}

impl xai_tool_runtime::ToolOutput for NativeNmapJobResponse {}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[tokio::test]
    async fn background_job_exposes_host_before_process_finishes() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let scanner = directory.path().join("nmap-fixture");
        std::fs::write(
            &scanner,
            "#!/bin/sh\n\
             output='-'\n\
             while [ \"$#\" -gt 0 ]; do\n\
               if [ \"$1\" = '-oX' ]; then output=$2; shift 2; else shift; fi\n\
             done\n\
             printf '%s\\n' '<?xml version=\"1.0\"?><nmaprun scanner=\"nmap\" version=\"fixture\"><host><status state=\"up\"/><address addr=\"127.0.0.1\" addrtype=\"ipv4\"/></host>' > \"$output\"\n\
             sleep 1\n\
             printf '%s\\n' '<runstats><finished elapsed=\"1\"/><hosts up=\"1\" down=\"0\" total=\"1\"/></runstats></nmaprun>' >> \"$output\"\n",
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&scanner).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&scanner, permissions).unwrap();

        let job_id = format!("nmap-test-{}", uuid::Uuid::now_v7().simple());
        let job = ExecutionSupervisor::global()
            .jobs()
            .register(job_id, ExecutionJobKind::Nmap, None, None)
            .unwrap();
        let artifacts = NmapScanArtifacts {
            xml: directory.path().join("scan.xml"),
            stderr: directory.path().join("scan.stderr"),
            status: directory.path().join("scan.status.json"),
        };
        job.update(serde_json::json!({
            "scan": {
                "target": "127.0.0.1",
                "profile": "tcp_connect",
                "ports": [1],
                "timeout_secs": 5
            },
            "hosts": [],
        }));
        let task = tokio::spawn(run_background_nmap_job(
            NativeNmapDriver::with_binary(scanner).unwrap(),
            ScanScope::new(["127.0.0.1".to_owned()]).unwrap(),
            NmapScanRequest {
                target: "127.0.0.1".to_owned(),
                profile: Default::default(),
                ports: vec![1],
                timeout_secs: Some(5),
            },
            artifacts.clone(),
            job.clone(),
        ));

        tokio::time::timeout(std::time::Duration::from_millis(750), async {
            loop {
                let snapshot = job.snapshot();
                let hosts = snapshot
                    .payload
                    .get("hosts")
                    .cloned()
                    .and_then(|hosts| serde_json::from_value::<Vec<NmapHost>>(hosts).ok())
                    .unwrap_or_default();
                let has_host =
                    snapshot.lifecycle == ExecutionJobLifecycle::Running && hosts.len() == 1;
                if has_host {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("progress host should arrive before fixture exits");

        task.await.unwrap();
        let snapshot = job.snapshot();
        assert_eq!(snapshot.lifecycle, ExecutionJobLifecycle::Completed);
        let report = serde_json::from_value::<NmapScanReport>(snapshot.payload).unwrap();
        assert_eq!(report.hosts_total, 1);
        assert!(artifacts.xml.exists());
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&std::fs::read(artifacts.status).unwrap())
                .unwrap()["exit_code"],
            0
        );
    }
}
