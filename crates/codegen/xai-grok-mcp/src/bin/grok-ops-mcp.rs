//! Local-only MCP bridges for heavyweight operational frameworks.
//!
//! The binary intentionally keeps these integrations out of the interactive
//! Grok process. Select one connector per stdio server:
//!
//! ```text
//! grok-ops-mcp metasploit
//! grok-ops-mcp bloodhound
//! grok-ops-mcp vulnerability-index
//! ```
//!
//! Framework credentials are read from environment variables and are never
//! accepted as model-visible tool parameters.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use flate2::read::GzDecoder;
use futures::StreamExt;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};
use walkdir::WalkDir;
use xai_grok_mcp::rmcp;
use xai_grok_mcp::rmcp::{
    ServerHandler, ServiceExt,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    schemars, tool, tool_handler, tool_router,
};
use xai_grok_protocol::{
    ArtifactContract, CancellationSemantics, CapabilityManifest, ConcurrencyProfile,
    MAX_PROVIDER_WORKER_FRAME_BYTES, OperationDescriptor, PROVIDER_WORKER_PROTOCOL_VERSION,
    Platform, ProtocolError, ProtocolErrorCode, ProviderDispatch, ProviderKind,
    ProviderWorkerOutput, ProviderWorkerRequest, ProviderWorkerResponse, RecoverySemantics,
    VersionRange,
};

const MAX_FRAMEWORK_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_QUERY_ROWS: usize = 200;
const MAX_QUERY_ROWS: usize = 1_000;
const DEFAULT_SEARCH_RESULTS: usize = 10;
const MAX_SEARCH_RESULTS: usize = 50;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = env::args().skip(1);
    let connector = arguments.next().unwrap_or_default();
    let provider_worker = arguments.next().as_deref() == Some("--provider-worker");
    if arguments.next().is_some() {
        return Err(
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "too many arguments").into(),
        );
    }
    if provider_worker {
        return run_provider_worker(&connector).await;
    }
    match connector.as_str() {
        "metasploit" => {
            let server = MetasploitServer::from_env().map_err(std::io::Error::other)?;
            server
                .serve(rmcp::transport::stdio())
                .await?
                .waiting()
                .await?;
        }
        "bloodhound" | "neo4j" => {
            let server = BloodHoundServer::from_env().map_err(std::io::Error::other)?;
            server
                .serve(rmcp::transport::stdio())
                .await?
                .waiting()
                .await?;
        }
        "vulnerability-index" | "vuln-index" => {
            let server = VulnerabilityServer::from_env().map_err(std::io::Error::other)?;
            server
                .serve(rmcp::transport::stdio())
                .await?
                .waiting()
                .await?;
        }
        "--help" | "-h" => {
            eprintln!(
                "Usage: grok-ops-mcp <metasploit|bloodhound|vulnerability-index> [--provider-worker]\n\
                 Configuration is supplied through environment variables; see the Grok Build \
                 operational-observability documentation."
            );
        }
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "choose one connector: metasploit, bloodhound, or vulnerability-index",
            )
            .into());
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
enum ConnectorWorker {
    Metasploit(MetasploitServer),
    BloodHound(BloodHoundServer),
    Vulnerability(VulnerabilityServer),
}

impl ConnectorWorker {
    fn from_env(connector: &str) -> Result<Self, String> {
        match connector {
            "metasploit" => MetasploitServer::from_env().map(Self::Metasploit),
            "bloodhound" | "neo4j" => BloodHoundServer::from_env().map(Self::BloodHound),
            "vulnerability-index" | "vuln-index" => {
                VulnerabilityServer::from_env().map(Self::Vulnerability)
            }
            _ => Err("choose one connector: metasploit, bloodhound, or vulnerability-index".into()),
        }
    }

    fn manifest(&self) -> CapabilityManifest {
        let operation = |operation_id: &str,
                         display_name: &str,
                         input_schema: Value,
                         deferred: bool| OperationDescriptor {
            operation_id: operation_id.into(),
            display_name: display_name.to_owned(),
            input_schema,
            output_schema: json!({"type":"object","additionalProperties":true}),
            streaming: false,
            interactive: true,
            deferred,
        };
        let (provider_id, features, operations) = match self {
            Self::Metasploit(_) => (
                "mcp-metasploit",
                vec!["external_framework", "metasploit", "messagepack_rpc"],
                vec![
                    operation(
                        "metasploit.module_info",
                        "Read Metasploit module metadata",
                        json!({
                            "type":"object",
                            "required":["module_type","module_name"],
                            "properties":{
                                "module_type":{"type":"string"},
                                "module_name":{"type":"string"}
                            },
                            "additionalProperties":false
                        }),
                        false,
                    ),
                    operation(
                        "metasploit.execute_module",
                        "Execute a Metasploit module",
                        json!({
                            "type":"object",
                            "required":["module_type","module_name","target"],
                            "properties":{
                                "module_type":{"type":"string"},
                                "module_name":{"type":"string"},
                                "target":{"type":"string"},
                                "options":{"type":"object"}
                            },
                            "additionalProperties":false
                        }),
                        true,
                    ),
                    operation(
                        "metasploit.execution_status",
                        "Read Metasploit execution status",
                        json!({
                            "type":"object",
                            "required":["target"],
                            "properties":{
                                "target":{"type":"string"},
                                "job_id":{"type":["integer","null"]},
                                "execution_uuid":{"type":["string","null"]}
                            },
                            "additionalProperties":false
                        }),
                        true,
                    ),
                ],
            ),
            Self::BloodHound(_) => (
                "mcp-bloodhound",
                vec!["bloodhound", "external_framework", "neo4j", "read_only"],
                vec![
                    operation(
                        "bloodhound.schema",
                        "Read the BloodHound graph schema",
                        json!({"type":"object","additionalProperties":false}),
                        false,
                    ),
                    operation(
                        "bloodhound.query",
                        "Execute bounded read-only Cypher",
                        json!({
                            "type":"object",
                            "required":["cypher"],
                            "properties":{
                                "cypher":{"type":"string"},
                                "parameters":{"type":"object"},
                                "max_rows":{"type":["integer","null"],"minimum":1,"maximum":1000}
                            },
                            "additionalProperties":false
                        }),
                        true,
                    ),
                ],
            ),
            Self::Vulnerability(_) => (
                "mcp-vulnerability-index",
                vec!["exploit_db", "fts5", "nvd", "offline_index"],
                vec![
                    operation(
                        "vulnerability.refresh_index",
                        "Refresh the offline vulnerability index",
                        json!({"type":"object","additionalProperties":false}),
                        true,
                    ),
                    operation(
                        "vulnerability.search",
                        "Search the offline vulnerability index",
                        json!({
                            "type":"object",
                            "required":["query"],
                            "properties":{
                                "query":{"type":"string"},
                                "limit":{"type":["integer","null"],"minimum":1,"maximum":50}
                            },
                            "additionalProperties":false
                        }),
                        false,
                    ),
                ],
            ),
        };
        let mut platforms = BTreeSet::new();
        platforms.insert(Platform {
            os: env::consts::OS.to_owned(),
            architecture: env::consts::ARCH.to_owned(),
            accelerator: None,
        });
        CapabilityManifest {
            provider_id: provider_id.into(),
            provider_version: env!("CARGO_PKG_VERSION").to_owned(),
            protocol: VersionRange::exact(xai_grok_protocol::PROTOCOL_VERSION),
            kind: ProviderKind::McpConnector,
            features: features.into_iter().map(str::to_owned).collect(),
            operations,
            concurrency: ConcurrencyProfile {
                maximum_parallel: 1,
                queue_capacity: 32,
                exclusive_resource: Some(provider_id.to_owned()),
            },
            cancellation: CancellationSemantics::Unsupported,
            recovery: RecoverySemantics::Restartable,
            artifacts: ArtifactContract::Optional,
            platforms,
            metadata: [
                ("transport".to_owned(), "local_binary_ipc".into()),
                ("mcp_stdio_available".to_owned(), true.into()),
            ]
            .into_iter()
            .collect(),
        }
    }

    async fn execute(
        &self,
        dispatch: ProviderDispatch,
    ) -> Result<ProviderWorkerOutput, ProtocolError> {
        let manifest = self.manifest();
        if dispatch.provider_id != manifest.provider_id {
            return Err(ProtocolError::new(
                ProtocolErrorCode::UnknownProvider,
                format!(
                    "dispatch selected {} but worker is {}",
                    dispatch.provider_id, manifest.provider_id
                ),
            ));
        }
        let operation = dispatch.task.capability.operation_id.as_str();
        if manifest
            .operation(&dispatch.task.capability.operation_id)
            .is_none()
        {
            return Err(ProtocolError::new(
                ProtocolErrorCode::UnsupportedOperation,
                format!(
                    "provider {} does not support {operation}",
                    manifest.provider_id
                ),
            ));
        }
        let input = dispatch.task.input;
        let text = match (self, operation) {
            (Self::Metasploit(server), "metasploit.module_info") => {
                server
                    .metasploit_module_info(Parameters(decode_input(input)?))
                    .await
            }
            (Self::Metasploit(server), "metasploit.execute_module") => {
                server
                    .metasploit_execute_module(Parameters(decode_input(input)?))
                    .await
            }
            (Self::Metasploit(server), "metasploit.execution_status") => {
                server
                    .metasploit_execution_status(Parameters(decode_input(input)?))
                    .await
            }
            (Self::BloodHound(server), "bloodhound.schema") => server.bloodhound_schema().await,
            (Self::BloodHound(server), "bloodhound.query") => {
                server
                    .bloodhound_query(Parameters(decode_input(input)?))
                    .await
            }
            (Self::Vulnerability(server), "vulnerability.refresh_index") => {
                server.vulnerability_refresh_index().await
            }
            (Self::Vulnerability(server), "vulnerability.search") => {
                server
                    .vulnerability_search(Parameters(decode_input(input)?))
                    .await
            }
            _ => unreachable!("operation was checked against this worker manifest"),
        }
        .map_err(|message| ProtocolError::new(ProtocolErrorCode::ServiceUnavailable, message))?;
        Ok(ProviderWorkerOutput {
            output: serde_json::from_str(&text).unwrap_or_else(|_| json!({"text": text})),
            ..ProviderWorkerOutput::default()
        })
    }
}

fn decode_input<T: serde::de::DeserializeOwned>(input: Value) -> Result<T, ProtocolError> {
    serde_json::from_value(input).map_err(|error| {
        ProtocolError::new(
            ProtocolErrorCode::InvalidEnvelope,
            format!("connector input does not match its schema: {error}"),
        )
    })
}

async fn run_provider_worker(connector: &str) -> Result<(), Box<dyn std::error::Error>> {
    let worker = ConnectorWorker::from_env(connector).map_err(std::io::Error::other)?;
    let mut input = tokio::io::stdin();
    let mut output = tokio::io::stdout();
    let mut negotiated = false;
    loop {
        let request: ProviderWorkerRequest = read_provider_frame(&mut input).await?;
        let response = match request {
            ProviderWorkerRequest::Hello { protocol_version } => {
                if negotiated || protocol_version != PROVIDER_WORKER_PROTOCOL_VERSION {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "incompatible or duplicate provider worker handshake",
                    )
                    .into());
                }
                negotiated = true;
                ProviderWorkerResponse::Hello {
                    protocol_version: PROVIDER_WORKER_PROTOCOL_VERSION,
                    manifest: worker.manifest(),
                }
            }
            ProviderWorkerRequest::Execute { dispatch } if negotiated => {
                let request_id = dispatch.request_id.clone();
                ProviderWorkerResponse::Execute {
                    request_id,
                    result: worker.execute(dispatch).await,
                }
            }
            ProviderWorkerRequest::Cancel { request_id } if negotiated => {
                ProviderWorkerResponse::Cancel {
                    request_id,
                    result: Err(ProtocolError::new(
                        ProtocolErrorCode::UnsupportedOperation,
                        "this connector does not expose cancellable in-flight requests",
                    )),
                }
            }
            ProviderWorkerRequest::Shutdown if negotiated => {
                write_provider_frame(&mut output, &ProviderWorkerResponse::Ack).await?;
                return Ok(());
            }
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "provider worker requires a successful hello before commands",
                )
                .into());
            }
        };
        write_provider_frame(&mut output, &response).await?;
    }
}

async fn read_provider_frame<R: AsyncRead + Unpin, T: serde::de::DeserializeOwned>(
    reader: &mut R,
) -> Result<T, Box<dyn std::error::Error>> {
    let length = reader.read_u32().await? as usize;
    if length > MAX_PROVIDER_WORKER_FRAME_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("provider frame exceeds {MAX_PROVIDER_WORKER_FRAME_BYTES} bytes"),
        )
        .into());
    }
    let mut bytes = vec![0; length];
    reader.read_exact(&mut bytes).await?;
    Ok(rmp_serde::from_slice(&bytes)?)
}

async fn write_provider_frame<W: AsyncWrite + Unpin, T: Serialize>(
    writer: &mut W,
    value: &T,
) -> Result<(), Box<dyn std::error::Error>> {
    let bytes = rmp_serde::to_vec_named(value)?;
    if bytes.len() > MAX_PROVIDER_WORKER_FRAME_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("provider frame exceeds {MAX_PROVIDER_WORKER_FRAME_BYTES} bytes"),
        )
        .into());
    }
    writer.write_u32(u32::try_from(bytes.len())?).await?;
    writer.write_all(&bytes).await?;
    writer.flush().await?;
    Ok(())
}

fn env_enabled(name: &str) -> bool {
    env::var(name).is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

fn local_framework_url(variable: &str, default: &str) -> Result<url::Url, String> {
    let raw = env::var(variable).unwrap_or_else(|_| default.to_owned());
    let url = url::Url::parse(&raw).map_err(|error| format!("{variable} is invalid: {error}"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(format!("{variable} must use http or https"));
    }

    let loopback = url.host_str().is_some_and(|host| {
        host.eq_ignore_ascii_case("localhost")
            || host
                .parse::<IpAddr>()
                .is_ok_and(|address| address.is_loopback())
    });
    if !loopback && !env_enabled("GROK_MCP_ALLOW_REMOTE") {
        return Err(format!(
            "{variable} must resolve to localhost/loopback; set \
             GROK_MCP_ALLOW_REMOTE=1 only for an explicitly trusted remote service"
        ));
    }
    Ok(url)
}

fn pretty_json(value: &Value) -> Result<String, String> {
    serde_json::to_string_pretty(value).map_err(|error| error.to_string())
}

async fn read_response_limited(
    response: reqwest::Response,
    framework: &str,
) -> Result<(reqwest::StatusCode, Vec<u8>), String> {
    let status = response.status();
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk =
            chunk.map_err(|error| format!("failed to read {framework} response: {error}"))?;
        if body.len().saturating_add(chunk.len()) > MAX_FRAMEWORK_RESPONSE_BYTES {
            return Err(format!(
                "{framework} response exceeded the {} MiB limit",
                MAX_FRAMEWORK_RESPONSE_BYTES / (1024 * 1024)
            ));
        }
        body.extend_from_slice(&chunk);
    }
    Ok((status, body))
}

// ──────────────────────────────────────────────────────────────────────────
// Metasploit RPC
// ──────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct MetasploitRpc {
    client: reqwest::Client,
    url: url::Url,
    username: Option<String>,
    password: Option<String>,
    token: Arc<tokio::sync::Mutex<Option<String>>>,
}

impl std::fmt::Debug for MetasploitRpc {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MetasploitRpc")
            .field("url", &self.url)
            .field("has_username", &self.username.is_some())
            .field("has_password", &self.password.is_some())
            .finish_non_exhaustive()
    }
}

impl MetasploitRpc {
    fn from_env() -> Result<Self, String> {
        let url = local_framework_url("GROK_METASPLOIT_RPC_URL", "http://127.0.0.1:55553/api/")?;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .map_err(|error| format!("failed to build Metasploit RPC client: {error}"))?;
        Ok(Self {
            client,
            url,
            username: env::var("GROK_METASPLOIT_RPC_USER").ok(),
            password: env::var("GROK_METASPLOIT_RPC_PASSWORD").ok(),
            token: Arc::new(tokio::sync::Mutex::new(
                env::var("GROK_METASPLOIT_RPC_TOKEN").ok(),
            )),
        })
    }

    async fn post(&self, request: Value) -> Result<Value, String> {
        let body = rmp_serde::to_vec(&request)
            .map_err(|error| format!("failed to encode Metasploit RPC request: {error}"))?;
        let response = self
            .client
            .post(self.url.clone())
            .header(reqwest::header::CONTENT_TYPE, "binary/message-pack")
            .body(body)
            .send()
            .await
            .map_err(|error| format!("Metasploit RPC request failed: {error}"))?;
        let (status, body) = read_response_limited(response, "Metasploit RPC").await?;
        if !status.is_success() {
            return Err(format!(
                "Metasploit RPC returned HTTP {status}: {}",
                String::from_utf8_lossy(&body)
            ));
        }
        rmp_serde::from_slice(&body)
            .map_err(|error| format!("invalid Metasploit MessagePack response: {error}"))
    }

    async fn token(&self) -> Result<String, String> {
        let mut token = self.token.lock().await;
        if let Some(current) = token.as_ref() {
            return Ok(current.clone());
        }
        let username = self.username.as_deref().ok_or_else(|| {
            "set GROK_METASPLOIT_RPC_USER or GROK_METASPLOIT_RPC_TOKEN".to_owned()
        })?;
        let password = self.password.as_deref().ok_or_else(|| {
            "set GROK_METASPLOIT_RPC_PASSWORD or GROK_METASPLOIT_RPC_TOKEN".to_owned()
        })?;
        let response = self.post(json!(["auth.login", username, password])).await?;
        if response.get("result").and_then(Value::as_str) != Some("success") {
            return Err(format!("Metasploit RPC authentication failed: {response}"));
        }
        let authenticated = response
            .get("token")
            .and_then(Value::as_str)
            .ok_or_else(|| "Metasploit RPC authentication returned no token".to_owned())?
            .to_owned();
        *token = Some(authenticated.clone());
        Ok(authenticated)
    }

    async fn call(&self, method: &str, arguments: Vec<Value>) -> Result<Value, String> {
        let mut request = vec![Value::String(method.to_owned())];
        request.push(Value::String(self.token().await?));
        request.extend(arguments);
        let response = self.post(Value::Array(request)).await?;
        if let Some(error) = response.get("error").and_then(Value::as_bool)
            && error
        {
            let message = response
                .get("error_message")
                .and_then(Value::as_str)
                .unwrap_or("unknown Metasploit RPC error");
            return Err(message.to_owned());
        }
        Ok(response)
    }
}

fn validate_module_name(module_type: &str, module_name: &str) -> Result<(), String> {
    if !matches!(module_type, "exploit" | "auxiliary" | "post" | "payload") {
        return Err("module_type must be exploit, auxiliary, post, or payload".to_owned());
    }
    if module_name.is_empty()
        || module_name.len() > 256
        || module_name.contains("..")
        || !module_name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '_' | '-' | '.'))
    {
        return Err("module_name contains unsupported characters".to_owned());
    }
    Ok(())
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct MetasploitModuleInfoRequest {
    #[schemars(description = "Metasploit module type: exploit, auxiliary, post, or payload.")]
    module_type: String,
    #[schemars(description = "Canonical module path, for example windows/smb/example.")]
    module_name: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct MetasploitExecuteRequest {
    #[schemars(description = "Metasploit module type (exploit or auxiliary).")]
    module_type: String,
    #[schemars(description = "Canonical Metasploit module path.")]
    module_name: String,
    #[schemars(description = "Explicit IP target copied into the module datastore.")]
    target: String,
    #[serde(default)]
    #[schemars(description = "Module datastore options. Credentials remain server-side.")]
    options: BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct MetasploitStatusRequest {
    #[schemars(description = "Explicit IP target used to filter returned sessions.")]
    target: String,
    #[serde(default)]
    job_id: Option<u64>,
    #[serde(default)]
    execution_uuid: Option<String>,
}

#[derive(Debug, Clone)]
struct MetasploitServer {
    rpc: MetasploitRpc,
    tool_router: ToolRouter<Self>,
}

impl MetasploitServer {
    fn from_env() -> Result<Self, String> {
        let rpc = MetasploitRpc::from_env()?;
        Ok(Self {
            rpc,
            tool_router: Self::tool_router(),
        })
    }
}

#[tool_router(router = tool_router)]
impl MetasploitServer {
    #[tool(
        description = "Read exact Metasploit module metadata and datastore constraints from the running local RPC service. This does not execute a module."
    )]
    async fn metasploit_module_info(
        &self,
        Parameters(request): Parameters<MetasploitModuleInfoRequest>,
    ) -> Result<String, String> {
        validate_module_name(&request.module_type, &request.module_name)?;
        let info = self
            .rpc
            .call(
                "module.info",
                vec![
                    json!(request.module_type),
                    json!(request.module_name.clone()),
                ],
            )
            .await?;
        let options = self
            .rpc
            .call(
                "module.options",
                vec![json!(request.module_type), json!(request.module_name)],
            )
            .await?;
        pretty_json(&json!({"info": info, "options": options}))
    }

    #[tool(
        description = "Execute one Metasploit exploit/auxiliary module with a typed target and datastore options, returning the asynchronous job identity for polling."
    )]
    async fn metasploit_execute_module(
        &self,
        Parameters(mut request): Parameters<MetasploitExecuteRequest>,
    ) -> Result<String, String> {
        if !matches!(request.module_type.as_str(), "exploit" | "auxiliary") {
            return Err("execution only accepts exploit or auxiliary modules".to_owned());
        }
        validate_module_name(&request.module_type, &request.module_name)?;

        for key in ["RHOST", "RHOSTS"] {
            if let Some(configured) = request.options.get(key).and_then(Value::as_str)
                && configured != request.target
            {
                return Err(format!(
                    "{key} `{configured}` does not match the approved target `{}`",
                    request.target
                ));
            }
        }
        request
            .options
            .entry("RHOSTS".to_owned())
            .or_insert_with(|| json!(request.target));

        let response = self
            .rpc
            .call(
                "module.execute",
                vec![
                    json!(request.module_type),
                    json!(request.module_name),
                    serde_json::to_value(request.options).map_err(|error| error.to_string())?,
                ],
            )
            .await?;
        pretty_json(&json!({
            "target": request.target,
            "execution": response,
            "next": "poll metasploit_execution_status with the returned job_id/uuid"
        }))
    }

    #[tool(
        description = "Read the status of a previously approved Metasploit execution and return only jobs/sessions associated with the explicit in-scope target."
    )]
    async fn metasploit_execution_status(
        &self,
        Parameters(request): Parameters<MetasploitStatusRequest>,
    ) -> Result<String, String> {
        let job = match request.job_id {
            Some(job_id) => Some(self.rpc.call("job.info", vec![json!(job_id)]).await?),
            None => None,
        };
        let sessions = self.rpc.call("session.list", Vec::new()).await?;
        let filtered_sessions = sessions
            .as_object()
            .map(|entries| {
                entries
                    .iter()
                    .filter(|(_, session)| {
                        let target_matches = session
                            .get("target_host")
                            .and_then(Value::as_str)
                            .is_some_and(|value| value == request.target);
                        let uuid_matches = request.execution_uuid.as_deref().is_some_and(|uuid| {
                            session
                                .get("exploit_uuid")
                                .and_then(Value::as_str)
                                .is_some_and(|value| value == uuid)
                        });
                        target_matches || uuid_matches
                    })
                    .map(|(id, session)| (id.clone(), session.clone()))
                    .collect::<serde_json::Map<_, _>>()
            })
            .unwrap_or_default();
        pretty_json(&json!({
            "target": request.target,
            "job": job,
            "sessions": filtered_sessions
        }))
    }
}

#[tool_handler(
    router = self.tool_router,
    name = "grok-metasploit-bridge",
    version = "0.1.0",
    instructions = "Local MessagePack RPC bridge with typed module execution and asynchronous job/session status."
)]
impl ServerHandler for MetasploitServer {}

// ──────────────────────────────────────────────────────────────────────────
// BloodHound / Neo4j
// ──────────────────────────────────────────────────────────────────────────

fn cypher_is_read_only(query: &str) -> bool {
    let normalized = query
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    let starts_read_only = [
        "match ",
        "optional match ",
        "return ",
        "with ",
        "unwind ",
        "show ",
        "call db.schema.",
        "call db.labels",
        "call db.relationshiptypes",
        "call db.propertykeys",
    ]
    .iter()
    .any(|prefix| normalized.starts_with(prefix));
    let mutating = [
        " create ",
        " delete ",
        " detach ",
        " set ",
        " remove ",
        " merge ",
        " drop ",
        " load csv ",
        " foreach ",
        " call apoc.",
        " call dbms.",
        " call db.create",
        " call db.index.fulltext.create",
    ]
    .iter()
    .any(|needle| format!(" {normalized} ").contains(needle));
    starts_read_only && !mutating && !normalized.contains(';')
}

#[derive(Debug, Clone)]
struct Neo4jClient {
    client: reqwest::Client,
    url: url::Url,
    username: String,
    password: Option<String>,
}

impl Neo4jClient {
    fn from_env() -> Result<Self, String> {
        let url =
            local_framework_url("GROK_NEO4J_URL", "http://127.0.0.1:7474/db/neo4j/tx/commit")?;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|error| format!("failed to build Neo4j client: {error}"))?;
        Ok(Self {
            client,
            url,
            username: env::var("GROK_NEO4J_USER").unwrap_or_else(|_| "neo4j".to_owned()),
            password: env::var("GROK_NEO4J_PASSWORD").ok(),
        })
    }

    async fn query(
        &self,
        statement: &str,
        parameters: Value,
        max_rows: usize,
    ) -> Result<Value, String> {
        let password = self.password.as_deref().ok_or_else(|| {
            "GROK_NEO4J_PASSWORD is required in the MCP server environment".to_owned()
        })?;
        let response = self
            .client
            .post(self.url.clone())
            .basic_auth(&self.username, Some(password))
            .json(&json!({
                "statements": [{
                    "statement": statement,
                    "parameters": parameters,
                    "resultDataContents": ["row", "graph"],
                    "includeStats": true
                }]
            }))
            .send()
            .await
            .map_err(|error| format!("Neo4j request failed: {error}"))?;
        let (status, bytes) = read_response_limited(response, "Neo4j").await?;
        if !status.is_success() {
            return Err(format!(
                "Neo4j returned HTTP {status}: {}",
                String::from_utf8_lossy(&bytes)
            ));
        }
        let mut value: Value = serde_json::from_slice(&bytes)
            .map_err(|error| format!("invalid Neo4j JSON response: {error}"))?;
        if let Some(errors) = value.get("errors").and_then(Value::as_array)
            && !errors.is_empty()
        {
            return Err(format!("Neo4j query error: {}", errors[0]));
        }
        if let Some(results) = value.get_mut("results").and_then(Value::as_array_mut) {
            for result in results {
                if let Some(data) = result.get_mut("data").and_then(Value::as_array_mut) {
                    data.truncate(max_rows);
                }
            }
        }
        Ok(value)
    }
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct BloodHoundQueryRequest {
    #[schemars(
        description = "Read-only Cypher generated from the operator's natural-language question."
    )]
    cypher: String,
    #[serde(default)]
    #[schemars(description = "Cypher parameters; use these instead of interpolating values.")]
    parameters: BTreeMap<String, Value>,
    #[serde(default)]
    #[schemars(description = "Maximum rows returned (default 200, maximum 1000).")]
    max_rows: Option<usize>,
}

#[derive(Debug, Clone)]
struct BloodHoundServer {
    neo4j: Neo4jClient,
    tool_router: ToolRouter<Self>,
}

impl BloodHoundServer {
    fn from_env() -> Result<Self, String> {
        let neo4j = Neo4jClient::from_env()?;
        Ok(Self {
            neo4j,
            tool_router: Self::tool_router(),
        })
    }
}

#[tool_router(router = tool_router)]
impl BloodHoundServer {
    #[tool(
        description = "Inspect the local BloodHound Neo4j graph schema (labels, relationship types, and property keys) so natural-language questions can be translated into exact Cypher."
    )]
    async fn bloodhound_schema(&self) -> Result<String, String> {
        let labels = self
            .neo4j
            .query("CALL db.labels()", json!({}), MAX_QUERY_ROWS)
            .await?;
        let relationships = self
            .neo4j
            .query("CALL db.relationshipTypes()", json!({}), MAX_QUERY_ROWS)
            .await?;
        let properties = self
            .neo4j
            .query("CALL db.propertyKeys()", json!({}), MAX_QUERY_ROWS)
            .await?;
        pretty_json(&json!({
            "labels": labels,
            "relationship_types": relationships,
            "property_keys": properties
        }))
    }

    #[tool(
        description = "Execute bounded, read-only Cypher against the local BloodHound Neo4j database and return row plus graph/path mappings. Mutating Cypher, APOC, DBMS calls, and multi-statement queries are rejected."
    )]
    async fn bloodhound_query(
        &self,
        Parameters(request): Parameters<BloodHoundQueryRequest>,
    ) -> Result<String, String> {
        if !cypher_is_read_only(&request.cypher) {
            return Err(
                "only a single read-only MATCH/RETURN/SHOW or approved db.schema query is allowed"
                    .to_owned(),
            );
        }
        let max_rows = request
            .max_rows
            .unwrap_or(DEFAULT_QUERY_ROWS)
            .clamp(1, MAX_QUERY_ROWS);
        let response = self
            .neo4j
            .query(
                &request.cypher,
                serde_json::to_value(request.parameters).map_err(|error| error.to_string())?,
                max_rows,
            )
            .await?;
        pretty_json(&response)
    }
}

#[tool_handler(
    router = self.tool_router,
    name = "grok-bloodhound-bridge",
    version = "0.1.0",
    instructions = "Read-only, row-bounded Neo4j connector for BloodHound graph and path queries."
)]
impl ServerHandler for BloodHoundServer {}

// ──────────────────────────────────────────────────────────────────────────
// Offline NVD + Exploit-DB index
// ──────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct VulnerabilityIndexConfig {
    database: PathBuf,
    nvd_paths: Vec<PathBuf>,
    exploitdb_csv: Option<PathBuf>,
    exploitdb_root: Option<PathBuf>,
}

impl VulnerabilityIndexConfig {
    fn from_env() -> Result<Self, String> {
        let database = env::var_os("GROK_VULN_INDEX_DB")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                env::var_os("HOME")
                    .map(PathBuf::from)
                    .unwrap_or_else(|| PathBuf::from("."))
                    .join(".grok/cache/vulnerability-index.sqlite")
            });
        let nvd_paths: Vec<PathBuf> = env::var_os("GROK_NVD_PATHS")
            .map(|paths| env::split_paths(&paths).collect())
            .unwrap_or_default();
        let exploitdb_csv = env::var_os("GROK_EXPLOITDB_CSV").map(PathBuf::from);
        let exploitdb_root = env::var_os("GROK_EXPLOITDB_ROOT").map(PathBuf::from);
        if nvd_paths.is_empty() && exploitdb_csv.is_none() {
            return Err(
                "configure GROK_NVD_PATHS and/or GROK_EXPLOITDB_CSV for the offline index"
                    .to_owned(),
            );
        }
        Ok(Self {
            database,
            nvd_paths,
            exploitdb_csv,
            exploitdb_root,
        })
    }
}

#[derive(Debug, Clone, Serialize)]
struct IndexedDocument {
    id: String,
    source: String,
    title: String,
    summary: String,
    constraints: String,
    execution_syntax: String,
    references: String,
    source_path: String,
}

#[derive(Debug, Default, Serialize)]
struct RefreshStats {
    indexed_nvd_records: usize,
    indexed_exploitdb_records: usize,
    skipped_unchanged_sources: usize,
    refreshed_sources: usize,
}

fn initialize_index(connection: &Connection) -> Result<(), String> {
    connection
        .execute_batch(
            "
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            CREATE TABLE IF NOT EXISTS documents (
                id TEXT NOT NULL,
                source TEXT NOT NULL,
                title TEXT NOT NULL,
                summary TEXT NOT NULL,
                constraints TEXT NOT NULL,
                execution_syntax TEXT NOT NULL,
                reference_text TEXT NOT NULL,
                source_path TEXT NOT NULL,
                UNIQUE(id, source, source_path)
            );
            CREATE INDEX IF NOT EXISTS idx_documents_id ON documents(id);
            CREATE INDEX IF NOT EXISTS idx_documents_source ON documents(source);
            CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(
                id,
                title,
                summary,
                constraints,
                content='documents',
                content_rowid='rowid',
                tokenize='unicode61 remove_diacritics 2'
            );
            CREATE TRIGGER IF NOT EXISTS documents_ai AFTER INSERT ON documents BEGIN
                INSERT INTO documents_fts(rowid, id, title, summary, constraints)
                VALUES (new.rowid, new.id, new.title, new.summary, new.constraints);
            END;
            CREATE TRIGGER IF NOT EXISTS documents_ad AFTER DELETE ON documents BEGIN
                INSERT INTO documents_fts(documents_fts, rowid, id, title, summary, constraints)
                VALUES ('delete', old.rowid, old.id, old.title, old.summary, old.constraints);
            END;
            CREATE TRIGGER IF NOT EXISTS documents_au AFTER UPDATE ON documents BEGIN
                INSERT INTO documents_fts(documents_fts, rowid, id, title, summary, constraints)
                VALUES ('delete', old.rowid, old.id, old.title, old.summary, old.constraints);
                INSERT INTO documents_fts(rowid, id, title, summary, constraints)
                VALUES (new.rowid, new.id, new.title, new.summary, new.constraints);
            END;
            CREATE TABLE IF NOT EXISTS source_meta (
                source_path TEXT PRIMARY KEY,
                modified_secs INTEGER NOT NULL,
                byte_size INTEGER NOT NULL
            );
            ",
        )
        .map_err(|error| format!("failed to initialize vulnerability index: {error}"))?;
    let document_count: i64 = connection
        .query_row("SELECT count(*) FROM documents", [], |row| row.get(0))
        .map_err(|error| error.to_string())?;
    let indexed_count: i64 = connection
        .query_row("SELECT count(*) FROM documents_fts", [], |row| row.get(0))
        .map_err(|error| error.to_string())?;
    if document_count != indexed_count {
        connection
            .execute(
                "INSERT INTO documents_fts(documents_fts) VALUES ('rebuild')",
                [],
            )
            .map_err(|error| format!("failed to rebuild vulnerability FTS index: {error}"))?;
    }
    Ok(())
}

fn source_fingerprint(path: &Path) -> Result<(i64, i64), String> {
    let metadata = fs::metadata(path).map_err(|error| format!("{}: {error}", path.display()))?;
    let modified = metadata
        .modified()
        .unwrap_or(SystemTime::UNIX_EPOCH)
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .try_into()
        .unwrap_or(i64::MAX);
    let size = metadata.len().try_into().unwrap_or(i64::MAX);
    Ok((modified, size))
}

fn source_is_current(
    connection: &Connection,
    path: &Path,
    fingerprint: (i64, i64),
) -> Result<bool, String> {
    let current = connection
        .query_row(
            "SELECT modified_secs, byte_size FROM source_meta WHERE source_path = ?1",
            [path.to_string_lossy().as_ref()],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(|error| error.to_string())?;
    Ok(current == Some(fingerprint))
}

fn replace_source(
    connection: &mut Connection,
    path: &Path,
    fingerprint: (i64, i64),
    documents: &[IndexedDocument],
) -> Result<(), String> {
    let source_path = path.to_string_lossy();
    let transaction = connection
        .transaction()
        .map_err(|error| error.to_string())?;
    transaction
        .execute(
            "DELETE FROM documents WHERE source_path = ?1",
            [source_path.as_ref()],
        )
        .map_err(|error| error.to_string())?;
    {
        let mut statement = transaction
            .prepare(
                "INSERT OR REPLACE INTO documents
                 (id, source, title, summary, constraints, execution_syntax, reference_text, source_path)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            )
            .map_err(|error| error.to_string())?;
        for document in documents {
            statement
                .execute(params![
                    document.id,
                    document.source,
                    document.title,
                    document.summary,
                    document.constraints,
                    document.execution_syntax,
                    document.references,
                    document.source_path,
                ])
                .map_err(|error| error.to_string())?;
        }
    }
    transaction
        .execute(
            "INSERT INTO source_meta(source_path, modified_secs, byte_size)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(source_path) DO UPDATE SET
               modified_secs=excluded.modified_secs,
               byte_size=excluded.byte_size",
            params![source_path.as_ref(), fingerprint.0, fingerprint.1],
        )
        .map_err(|error| error.to_string())?;
    transaction.commit().map_err(|error| error.to_string())
}

fn nvd_source_files(paths: &[PathBuf]) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for path in paths {
        if path.is_file() {
            files.push(path.clone());
            continue;
        }
        for entry in WalkDir::new(path)
            .follow_links(false)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_file())
        {
            let name = entry.file_name().to_string_lossy().to_ascii_lowercase();
            if name.ends_with(".json") || name.ends_with(".json.gz") {
                files.push(entry.into_path());
            }
        }
    }
    files.sort();
    files.dedup();
    files
}

fn open_json_source(path: &Path) -> Result<Box<dyn Read>, String> {
    let file = File::open(path).map_err(|error| format!("{}: {error}", path.display()))?;
    if path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(".gz"))
    {
        Ok(Box::new(GzDecoder::new(file)))
    } else {
        Ok(Box::new(file))
    }
}

fn truncate_text(text: String, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text;
    }
    let end = text
        .char_indices()
        .nth(max_chars.saturating_sub(1))
        .map(|(index, _)| index)
        .unwrap_or(text.len());
    format!("{}…", &text[..end])
}

fn english_description(cve: &Value) -> String {
    cve.get("descriptions")
        .and_then(Value::as_array)
        .and_then(|descriptions| {
            descriptions
                .iter()
                .find(|description| description.get("lang").and_then(Value::as_str) == Some("en"))
                .or_else(|| descriptions.first())
        })
        .and_then(|description| description.get("value"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn nvd_documents(path: &Path) -> Result<Vec<IndexedDocument>, String> {
    let value: Value = serde_json::from_reader(BufReader::new(open_json_source(path)?))
        .map_err(|error| format!("failed to parse {}: {error}", path.display()))?;
    let entries = value
        .get("vulnerabilities")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            format!(
                "{} is not an NVD 2.x feed (missing vulnerabilities array)",
                path.display()
            )
        })?;
    let source_path = path.to_string_lossy().to_string();
    let mut documents = Vec::with_capacity(entries.len());
    for entry in entries {
        let Some(cve) = entry.get("cve") else {
            continue;
        };
        let Some(id) = cve.get("id").and_then(Value::as_str) else {
            continue;
        };
        let constraints = json!({
            "published": cve.get("published"),
            "last_modified": cve.get("lastModified"),
            "metrics": cve.get("metrics"),
            "weaknesses": cve.get("weaknesses"),
            "configurations": cve.get("configurations")
        });
        let references = cve
            .get("references")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|reference| reference.get("url").and_then(Value::as_str))
            .take(20)
            .collect::<Vec<_>>()
            .join("\n");
        documents.push(IndexedDocument {
            id: id.to_owned(),
            source: "nvd".to_owned(),
            title: id.to_owned(),
            summary: english_description(cve),
            constraints: truncate_text(constraints.to_string(), 16_000),
            execution_syntax: String::new(),
            references,
            source_path: source_path.clone(),
        });
    }
    Ok(documents)
}

#[derive(Debug, Deserialize)]
struct ExploitDbRow {
    id: String,
    file: String,
    description: String,
    #[serde(default)]
    date: String,
    #[serde(default)]
    author: String,
    #[serde(default, rename = "type")]
    exploit_type: String,
    #[serde(default)]
    platform: String,
    #[serde(default)]
    port: String,
    #[serde(default)]
    codes: String,
    #[serde(default)]
    tags: String,
}

fn exploit_usage(path: &Path) -> String {
    let Ok(file) = File::open(path) else {
        return String::new();
    };
    let mut lines = Vec::new();
    for line in BufReader::new(file).lines().map_while(Result::ok).take(160) {
        let trimmed = line.trim();
        let lowercase = trimmed.to_ascii_lowercase();
        if lowercase.contains("usage:")
            || lowercase.contains("example:")
            || trimmed.starts_with("$ ")
        {
            lines.push(trimmed.to_owned());
        }
        if lines.len() >= 12 {
            break;
        }
    }
    truncate_text(lines.join("\n"), 2_000)
}

fn exploitdb_documents(
    csv_path: &Path,
    exploitdb_root: Option<&Path>,
) -> Result<Vec<IndexedDocument>, String> {
    let mut reader = csv::ReaderBuilder::new()
        .flexible(true)
        .from_path(csv_path)
        .map_err(|error| format!("failed to open {}: {error}", csv_path.display()))?;
    let source_path = csv_path.to_string_lossy().to_string();
    let mut documents = Vec::new();
    for row in reader.deserialize::<ExploitDbRow>() {
        let row = row.map_err(|error| format!("invalid Exploit-DB row: {error}"))?;
        let local_path = exploitdb_root.map(|root| root.join(&row.file));
        let execution_syntax = local_path.as_deref().map(exploit_usage).unwrap_or_default();
        let constraints = json!({
            "platform": row.platform,
            "type": row.exploit_type,
            "port": row.port,
            "codes": row.codes,
            "tags": row.tags,
            "published": row.date,
            "author": row.author,
            "local_file": local_path
        });
        documents.push(IndexedDocument {
            id: format!("EDB-{}", row.id),
            source: "exploit-db".to_owned(),
            title: row.description.clone(),
            summary: row.description,
            constraints: constraints.to_string(),
            execution_syntax,
            references: format!("https://www.exploit-db.com/exploits/{}", row.id),
            source_path: source_path.clone(),
        });
    }
    Ok(documents)
}

fn refresh_vulnerability_index(config: &VulnerabilityIndexConfig) -> Result<RefreshStats, String> {
    if let Some(parent) = config.database.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("failed to create {}: {error}", parent.display()))?;
    }
    let mut connection =
        Connection::open(&config.database).map_err(|error| format!("open index: {error}"))?;
    initialize_index(&connection)?;
    let mut stats = RefreshStats::default();

    for path in nvd_source_files(&config.nvd_paths) {
        let fingerprint = source_fingerprint(&path)?;
        if source_is_current(&connection, &path, fingerprint)? {
            stats.skipped_unchanged_sources += 1;
            continue;
        }
        let documents = nvd_documents(&path)?;
        stats.indexed_nvd_records += documents.len();
        replace_source(&mut connection, &path, fingerprint, &documents)?;
        stats.refreshed_sources += 1;
    }

    if let Some(path) = config.exploitdb_csv.as_deref() {
        let fingerprint = source_fingerprint(path)?;
        if source_is_current(&connection, path, fingerprint)? {
            stats.skipped_unchanged_sources += 1;
        } else {
            let documents = exploitdb_documents(path, config.exploitdb_root.as_deref())?;
            stats.indexed_exploitdb_records += documents.len();
            replace_source(&mut connection, path, fingerprint, &documents)?;
            stats.refreshed_sources += 1;
        }
    }
    Ok(stats)
}

fn search_vulnerability_index(
    database: &Path,
    query: &str,
    limit: usize,
) -> Result<Vec<IndexedDocument>, String> {
    let connection =
        Connection::open(database).map_err(|error| format!("open vulnerability index: {error}"))?;
    initialize_index(&connection)?;
    let normalized = query.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Err("query must not be empty".to_owned());
    }
    let fts_query = normalized
        .split_whitespace()
        .take(12)
        .map(|term| format!("\"{}\"*", term.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(" AND ");
    if fts_query.is_empty() {
        return Err("query must contain searchable text".to_owned());
    }

    let exact = connection
        .query_row(
            "SELECT id, source, title, summary, constraints, execution_syntax, reference_text, source_path
             FROM documents WHERE lower(id) = ?1 LIMIT 1",
            [normalized.as_str()],
            |row| {
                Ok(IndexedDocument {
                    id: row.get(0)?,
                    source: row.get(1)?,
                    title: row.get(2)?,
                    summary: row.get(3)?,
                    constraints: row.get(4)?,
                    execution_syntax: row.get(5)?,
                    references: row.get(6)?,
                    source_path: row.get(7)?,
                })
            },
        )
        .optional()
        .map_err(|error| error.to_string())?;
    let mut documents = exact.into_iter().collect::<Vec<_>>();
    if documents.len() >= limit {
        return Ok(documents);
    }

    let remaining = limit - documents.len();
    let mut statement = connection
        .prepare(
            "SELECT d.id, d.source, d.title, d.summary, d.constraints,
                    d.execution_syntax, d.reference_text, d.source_path
             FROM documents_fts
             JOIN documents AS d ON d.rowid = documents_fts.rowid
             WHERE documents_fts MATCH ?1
               AND lower(d.id) != ?2
             ORDER BY bm25(documents_fts, 8.0, 4.0, 2.0, 1.0), d.id
             LIMIT ?3",
        )
        .map_err(|error| error.to_string())?;
    let rows = statement
        .query_map(
            params![
                fts_query,
                normalized,
                i64::try_from(remaining).unwrap_or(i64::MAX)
            ],
            |row| {
                Ok(IndexedDocument {
                    id: row.get(0)?,
                    source: row.get(1)?,
                    title: row.get(2)?,
                    summary: row.get(3)?,
                    constraints: row.get(4)?,
                    execution_syntax: row.get(5)?,
                    references: row.get(6)?,
                    source_path: row.get(7)?,
                })
            },
        )
        .map_err(|error| error.to_string())?;
    for document in rows {
        let document = document.map_err(|error| error.to_string())?;
        if !documents.iter().any(|existing| existing.id == document.id) {
            documents.push(document);
        }
    }
    Ok(documents)
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct VulnerabilitySearchRequest {
    #[schemars(
        description = "CVE ID, service/product/version, platform, or Exploit-DB search phrase."
    )]
    query: String,
    #[serde(default)]
    #[schemars(description = "Maximum records (default 10, maximum 50).")]
    limit: Option<usize>,
}

#[derive(Debug, Clone)]
struct VulnerabilityServer {
    config: VulnerabilityIndexConfig,
    tool_router: ToolRouter<Self>,
}

impl VulnerabilityServer {
    fn from_env() -> Result<Self, String> {
        let config = VulnerabilityIndexConfig::from_env()?;
        Ok(Self {
            config,
            tool_router: Self::tool_router(),
        })
    }
}

#[tool_router(router = tool_router)]
impl VulnerabilityServer {
    #[tool(
        description = "Incrementally index configured local NVD 2.x JSON/JSON.GZ feeds and Exploit-DB CSV metadata into the offline SQLite vulnerability index. No network access is used."
    )]
    async fn vulnerability_refresh_index(&self) -> Result<String, String> {
        let config = self.config.clone();
        let stats = tokio::task::spawn_blocking(move || refresh_vulnerability_index(&config))
            .await
            .map_err(|error| format!("index worker failed: {error}"))??;
        serde_json::to_string_pretty(&stats).map_err(|error| error.to_string())
    }

    #[tool(
        description = "Search the offline NVD and Exploit-DB index for exact CVE constraints, references, local exploit paths, and extracted usage syntax. This never executes exploit code."
    )]
    async fn vulnerability_search(
        &self,
        Parameters(request): Parameters<VulnerabilitySearchRequest>,
    ) -> Result<String, String> {
        let database = self.config.database.clone();
        let limit = request
            .limit
            .unwrap_or(DEFAULT_SEARCH_RESULTS)
            .clamp(1, MAX_SEARCH_RESULTS);
        let records = tokio::task::spawn_blocking(move || {
            search_vulnerability_index(&database, &request.query, limit)
        })
        .await
        .map_err(|error| format!("index worker failed: {error}"))??;
        if records.is_empty() {
            return Ok(
                "No indexed match. Run vulnerability_refresh_index after updating local feeds."
                    .to_owned(),
            );
        }
        serde_json::to_string_pretty(&records).map_err(|error| error.to_string())
    }
}

#[tool_handler(
    router = self.tool_router,
    name = "grok-vulnerability-index",
    version = "0.1.0",
    instructions = "Offline-only NVD and Exploit-DB indexing and lookup. Returned exploit metadata is never executed."
)]
impl ServerHandler for VulnerabilityServer {}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, body::Body, body::Bytes, response::Response, routing::post};

    fn worker_dispatch(operation: &str, input: Value) -> ProviderDispatch {
        ProviderDispatch {
            request_id: "request-worker".into(),
            engagement_id: "engagement-worker".into(),
            plan_revision: 1,
            task: xai_grok_protocol::ExecutionTask {
                task_id: "task-worker".into(),
                objective: "exercise connector worker".to_owned(),
                mode: xai_grok_protocol::ExecutionMode::Interactive,
                capability: xai_grok_protocol::CapabilityRequirement {
                    operation_id: operation.into(),
                    preferred_provider: Some("mcp-vulnerability-index".into()),
                    required_features: Vec::new(),
                },
                input,
                deadline_unix_ms: SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64
                    + 10_000,
                completion_tests: Vec::new(),
                depends_on: Vec::new(),
            },
            provider_id: "mcp-vulnerability-index".into(),
            lease_epoch: 1,
        }
    }

    #[tokio::test]
    async fn provider_worker_dispatches_the_real_offline_index() {
        let directory = tempfile::tempdir().unwrap();
        let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/exploitdb");
        let worker = ConnectorWorker::Vulnerability(VulnerabilityServer {
            config: VulnerabilityIndexConfig {
                database: directory.path().join("index.sqlite"),
                nvd_paths: Vec::new(),
                exploitdb_csv: Some(fixture.join("files_exploits.csv")),
                exploitdb_root: Some(fixture),
            },
            tool_router: VulnerabilityServer::tool_router(),
        });
        worker
            .execute(worker_dispatch("vulnerability.refresh_index", json!({})))
            .await
            .unwrap();
        let output = worker
            .execute(worker_dispatch(
                "vulnerability.search",
                json!({"query":"CVE-2026-4242","limit":5}),
            ))
            .await
            .unwrap();
        assert_eq!(output.output[0]["id"], "EDB-424242");
        assert!(
            output.output[0]["execution_syntax"]
                .as_str()
                .unwrap()
                .contains("example_check.py")
        );
    }

    #[test]
    fn cypher_gate_accepts_paths_and_rejects_mutations() {
        assert!(cypher_is_read_only(
            "MATCH p=shortestPath((a)-[*1..]->(b)) RETURN p LIMIT 10"
        ));
        assert!(cypher_is_read_only("CALL db.labels()"));
        assert!(!cypher_is_read_only("MATCH (n) DETACH DELETE n"));
        assert!(!cypher_is_read_only("CALL apoc.create.node([], {})"));
        assert!(!cypher_is_read_only(
            "MATCH (n) RETURN n; MATCH (m) RETURN m"
        ));
    }

    #[test]
    fn module_names_are_typed_and_path_safe() {
        assert!(validate_module_name("exploit", "windows/smb/example").is_ok());
        assert!(validate_module_name("custom", "windows/smb/example").is_err());
        assert!(validate_module_name("exploit", "../../payload").is_err());
    }

    #[test]
    fn nvd_v2_fixture_preserves_constraints() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("nvd.json");
        fs::write(
            &path,
            serde_json::to_vec(&json!({
                "vulnerabilities": [{
                    "cve": {
                        "id": "CVE-2026-1234",
                        "descriptions": [{"lang": "en", "value": "Example service flaw"}],
                        "metrics": {"cvssMetricV31": [{"cvssData": {"baseScore": 9.8}}]},
                        "weaknesses": [{"description": [{"lang": "en", "value": "CWE-79"}]}],
                        "configurations": [{"nodes": [{"cpeMatch": [{"criteria": "cpe:2.3:a:example"}]}]}],
                        "references": [{"url": "https://example.test/CVE-2026-1234"}]
                    }
                }]
            }))
            .unwrap(),
        )
        .unwrap();

        let documents = nvd_documents(&path).unwrap();
        assert_eq!(documents.len(), 1);
        assert_eq!(documents[0].id, "CVE-2026-1234");
        assert!(documents[0].constraints.contains("baseScore"));
        assert!(documents[0].constraints.contains("cpe:2.3:a:example"));
    }

    #[test]
    fn exploitdb_fixture_indexes_usage_and_searches_offline() {
        let directory = tempfile::tempdir().unwrap();
        let exploits = directory.path().join("exploits");
        fs::create_dir_all(&exploits).unwrap();
        fs::write(
            exploits.join("example.rb"),
            "# Usage: ruby example.rb TARGET\n# Example: ruby example.rb 127.0.0.1\n",
        )
        .unwrap();
        let csv = directory.path().join("files_exploits.csv");
        fs::write(
            &csv,
            "id,file,description,date,author,type,platform,port,codes,tags\n\
             4242,example.rb,Example Service 1.2 RCE,2026-01-01,Researcher,remote,linux,443,CVE-2026-1234,verified\n",
        )
        .unwrap();
        let database = directory.path().join("index.sqlite");
        let config = VulnerabilityIndexConfig {
            database: database.clone(),
            nvd_paths: Vec::new(),
            exploitdb_csv: Some(csv),
            exploitdb_root: Some(exploits),
        };

        let stats = refresh_vulnerability_index(&config).unwrap();
        assert_eq!(stats.indexed_exploitdb_records, 1);
        let records = search_vulnerability_index(&database, "CVE-2026-1234", 10).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].id, "EDB-4242");
        assert!(records[0].execution_syntax.contains("Usage:"));
    }

    #[tokio::test]
    async fn metasploit_client_speaks_messagepack_rpc() {
        let application = Router::new().route(
            "/api/",
            post(|body: Bytes| async move {
                let request: Value = rmp_serde::from_slice(&body).unwrap();
                assert_eq!(request[0], "module.info");
                assert_eq!(request[1], "server-side-token");
                let encoded = rmp_serde::to_vec(&json!({
                    "result": "success",
                    "name": "fixture"
                }))
                .unwrap();
                Response::builder()
                    .header("content-type", "binary/message-pack")
                    .body(Body::from(encoded))
                    .unwrap()
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, application).await.unwrap() });
        let rpc = MetasploitRpc {
            client: reqwest::Client::new(),
            url: url::Url::parse(&format!("http://{address}/api/")).unwrap(),
            username: None,
            password: None,
            token: Arc::new(tokio::sync::Mutex::new(Some(
                "server-side-token".to_owned(),
            ))),
        };

        let response = rpc
            .call(
                "module.info",
                vec![json!("exploit"), json!("windows/smb/fixture")],
            )
            .await
            .unwrap();
        assert_eq!(response["name"], "fixture");
        server.abort();
    }

    #[tokio::test]
    async fn neo4j_client_bounds_graph_rows() {
        let application = Router::new().route(
            "/db/neo4j/tx/commit",
            post(|| async {
                axum::Json(json!({
                    "results": [{
                        "columns": ["path"],
                        "data": [
                            {"row": [1], "graph": {"nodes": [], "relationships": []}},
                            {"row": [2], "graph": {"nodes": [], "relationships": []}},
                            {"row": [3], "graph": {"nodes": [], "relationships": []}}
                        ]
                    }],
                    "errors": []
                }))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, application).await.unwrap() });
        let client = Neo4jClient {
            client: reqwest::Client::new(),
            url: url::Url::parse(&format!("http://{address}/db/neo4j/tx/commit")).unwrap(),
            username: "neo4j".to_owned(),
            password: Some("fixture".to_owned()),
        };

        let response = client
            .query("MATCH p=()-[]->() RETURN p", json!({}), 2)
            .await
            .unwrap();
        assert_eq!(response["results"][0]["data"].as_array().unwrap().len(), 2);
        server.abort();
    }
}
