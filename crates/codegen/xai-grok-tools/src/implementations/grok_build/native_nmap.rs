//! Model-facing wrapper for the scoped native Nmap driver.

use crate::native::execution_supervisor::ExecutionSupervisor;
use crate::native::job_registry::{ExecutionJobHandle, ExecutionJobKind, ExecutionJobLifecycle};
use crate::native::nmap::{
    NativeNmapDriver, NmapHost, NmapScanEvent, NmapScanReport, NmapScanRequest, ScanScope,
};
use crate::types::output::{DynamicOutput, ToolOutput};
use crate::types::requirements::Expr;
use crate::types::tool::{ToolKind, ToolNamespace};
use crate::types::tool_io::{MCPToolInput, ToolInput};

pub const OPERATIONAL_SCOPE_ENV: &str = "GROK_OPERATIONAL_SCOPE";
const MAX_BACKGROUND_NMAP_JOBS: usize = 32;

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
            let hosts =
                serde_json::from_value::<Vec<NmapHost>>(snapshot.payload).unwrap_or_default();
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
            let mut report = serde_json::from_value::<NmapScanReport>(snapshot.payload)
                .map_err(|error| error.to_string())?;
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
        let scope = ScanScope::new(scope_targets)
            .map_err(|error| xai_tool_runtime::ToolError::invalid_arguments(error.to_string()))?;
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
                let scope = ScanScope::new(scope_targets).map_err(|error| {
                    xai_tool_runtime::ToolError::invalid_arguments(error.to_string())
                })?;
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
                let command_hash = blake3::hash(
                    &serde_json::to_vec(&scan).unwrap_or_else(|_| scan.target.as_bytes().to_vec()),
                )
                .to_hex()
                .to_string();
                let job = ExecutionSupervisor::global()
                    .register_job(
                        job_id.clone(),
                        ExecutionJobKind::Nmap,
                        owner_session_id,
                        None,
                        Some(ctx.call_id.as_str()),
                        command_hash,
                    )
                    .map_err(|error| {
                        xai_tool_runtime::ToolError::custom("native_nmap_job", error)
                    })?;
                job.persist_spawn_intent().await.map_err(|error| {
                    xai_tool_runtime::ToolError::custom(
                        "native_nmap_job",
                        format!("failed to persist Nmap spawn intent: {error}"),
                    )
                })?;
                job.update(serde_json::json!([]));
                tokio::spawn(run_background_nmap_job(driver, scope, scan, job));
                Ok(NativeNmapJobResponse::Started { job_id })
            }
            NativeNmapJobRequest::Status { job_id } => {
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
    job: ExecutionJobHandle,
) {
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(64);
    let scan = driver.scan_with_events(&scope, request, Some(event_tx));
    tokio::pin!(scan);

    let mut hosts = Vec::new();
    let cancellation = job.cancellation();
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
                if let Some(NmapScanEvent::HostDiscovered { host, .. }) = event
                {
                    hosts.push(host);
                    job.update(
                        serde_json::to_value(&hosts).unwrap_or(serde_json::Value::Null)
                    );
                }
            }
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
             printf '%s\\n' '<?xml version=\"1.0\"?><nmaprun scanner=\"nmap\" version=\"fixture\"><host><status state=\"up\"/><address addr=\"127.0.0.1\" addrtype=\"ipv4\"/></host>'\n\
             sleep 1\n\
             printf '%s\\n' '<runstats><finished elapsed=\"1\"/><hosts up=\"1\" down=\"0\" total=\"1\"/></runstats></nmaprun>'\n",
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
        job.update(serde_json::json!([]));
        let task = tokio::spawn(run_background_nmap_job(
            NativeNmapDriver::with_binary(scanner).unwrap(),
            ScanScope::new(["127.0.0.1".to_owned()]).unwrap(),
            NmapScanRequest {
                target: "127.0.0.1".to_owned(),
                profile: Default::default(),
                ports: vec![1],
                timeout_secs: Some(5),
            },
            job.clone(),
        ));

        tokio::time::timeout(std::time::Duration::from_millis(750), async {
            loop {
                let snapshot = job.snapshot();
                let hosts =
                    serde_json::from_value::<Vec<NmapHost>>(snapshot.payload).unwrap_or_default();
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
    }
}
