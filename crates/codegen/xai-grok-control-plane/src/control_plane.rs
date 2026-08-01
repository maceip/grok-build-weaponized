use std::collections::{HashMap, VecDeque};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::{Mutex, broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use xai_grok_engagement::{
    ControlCommandClaim, EngagementCheckpoint, EngagementCoordinator, NewEngagement, QueuePriority,
};
use xai_grok_protocol::{
    CapabilityRequirement, Command, CommandEnvelope, CommandId, EngagementId, Event, EventBatch,
    EventEnvelope, EventId, EventReadRequest, EvidenceId, EvidenceObservation, ExecutionMode,
    ExecutionReceipt, ExecutionTask, Exercise, ExerciseEvidence, ExerciseId, ExerciseStatus,
    Finding, FindingId, FindingStatus, MessageId, OperationRun, OperationRunId, OperationRunStatus,
    OperatorSession, OperatorSessionId, OperatorSessionStatus, PROTOCOL_VERSION, Playbook,
    PlaybookId, ProjectionQuery, ProtocolError, ProtocolErrorCode, ProviderDispatch, ProviderId,
    ResourceClaimId, Response, ResponseEnvelope, ServiceHealth, TaskId, TaskStatus, TaskingPlan,
    TeamMessage, TeamPresence, TeamResourceClaim, TeamWorkItem, TeamWorkItemId, TeamWorkItemStatus,
};

use crate::SERVER_NAME;
use crate::agent_provider::AGENT_TURN_OPERATION;
use crate::artifact::{ArtifactDescriptor, ArtifactStore, ArtifactStoreConfig};
use crate::journal::{EventJournal, JournalError};
use crate::projection::ProjectionStore;
use crate::provider::{
    ExecutionProvider, ProviderArtifact, ProviderArtifactSource, ProviderOutput, ProviderRegistry,
    ProviderRegistryConfig,
};
use crate::service::ServiceSupervisor;

const DEFAULT_COMMAND_CAPACITY: usize = 1_024;
const DEFAULT_EVENT_CAPACITY: usize = 4_096;
const DEFAULT_RESPONSE_CACHE: usize = 4_096;
const DEFAULT_HEARTBEAT_TIMEOUT_MS: u64 = 15_000;
const MAX_PLAN_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct ControlPlaneConfig {
    pub state_directory: PathBuf,
    pub command_capacity: usize,
    pub event_capacity: usize,
    pub response_cache_capacity: usize,
    pub heartbeat_timeout_ms: u64,
    pub provider_registry: ProviderRegistryConfig,
    pub maximum_artifact_bytes: u64,
    pub maximum_artifact_store_bytes: u64,
    pub auto_schedule_plans: bool,
}

impl ControlPlaneConfig {
    pub fn new(state_directory: impl Into<PathBuf>) -> Self {
        Self {
            state_directory: state_directory.into(),
            command_capacity: DEFAULT_COMMAND_CAPACITY,
            event_capacity: DEFAULT_EVENT_CAPACITY,
            response_cache_capacity: DEFAULT_RESPONSE_CACHE,
            heartbeat_timeout_ms: DEFAULT_HEARTBEAT_TIMEOUT_MS,
            provider_registry: ProviderRegistryConfig::default(),
            maximum_artifact_bytes: 4 * 1024 * 1024 * 1024,
            maximum_artifact_store_bytes: 128 * 1024 * 1024 * 1024,
            auto_schedule_plans: true,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ControlPlaneError {
    #[error("control-plane I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("control-plane journal: {0}")]
    Journal(#[from] JournalError),
    #[error("control-plane engagement store: {0}")]
    Engagement(#[from] xai_grok_engagement::EngagementError),
    #[error("control-plane artifact store: {0}")]
    Artifact(#[from] crate::artifact::ArtifactError),
}

struct QueuedCommand {
    envelope: CommandEnvelope,
    respond_to: oneshot::Sender<ResponseEnvelope>,
}

struct ResponseCache {
    capacity: usize,
    order: VecDeque<CommandId>,
    responses: HashMap<CommandId, CachedResponse>,
}

struct CachedResponse {
    command_hash: String,
    response: ResponseEnvelope,
}

impl ResponseCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            order: VecDeque::new(),
            responses: HashMap::new(),
        }
    }

    fn get(
        &self,
        command_id: &CommandId,
        command_hash: &str,
    ) -> Result<Option<ResponseEnvelope>, ProtocolError> {
        let Some(cached) = self.responses.get(command_id) else {
            return Ok(None);
        };
        if cached.command_hash != command_hash {
            return Err(ProtocolError::new(
                ProtocolErrorCode::Conflict,
                format!("command ID {command_id} was reused with different command content"),
            ));
        }
        Ok(Some(cached.response.clone()))
    }

    fn insert(&mut self, command_hash: String, response: ResponseEnvelope) {
        let command_id = response.command_id.clone();
        if self.responses.contains_key(&command_id) {
            return;
        }
        self.order.push_back(command_id.clone());
        self.responses.insert(
            command_id,
            CachedResponse {
                command_hash,
                response,
            },
        );
        while self.responses.len() > self.capacity {
            if let Some(expired) = self.order.pop_front() {
                self.responses.remove(&expired);
            }
        }
    }
}

struct PlanStore {
    root: PathBuf,
    dispatch_root: PathBuf,
    dispatch_lock: StdMutex<()>,
}

impl PlanStore {
    fn open(root: impl Into<PathBuf>) -> Result<Self, std::io::Error> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        let dispatch_root = root.join("dispatches");
        std::fs::create_dir_all(&dispatch_root)?;
        Ok(Self {
            root,
            dispatch_root,
            dispatch_lock: StdMutex::new(()),
        })
    }

    fn put(&self, plan: &TaskingPlan) -> Result<(), ProtocolError> {
        let path = self.path(&plan.engagement_id, plan.revision);
        let bytes = canonical_json_bytes(plan)?;
        if bytes.len() > MAX_PLAN_BYTES {
            return Err(ProtocolError::new(
                ProtocolErrorCode::InvalidEnvelope,
                format!(
                    "serialized plan is {} bytes; maximum is {MAX_PLAN_BYTES}",
                    bytes.len()
                ),
            ));
        }
        if path.exists() {
            let current = std::fs::read(&path).map_err(internal_error)?;
            if current == bytes {
                return Ok(());
            }
            return Err(ProtocolError::new(
                ProtocolErrorCode::Conflict,
                format!(
                    "engagement {} plan revision {} already exists with different content",
                    plan.engagement_id, plan.revision
                ),
            ));
        }
        let mut temporary = tempfile::NamedTempFile::new_in(&self.root).map_err(internal_error)?;
        temporary.write_all(&bytes).map_err(internal_error)?;
        temporary.as_file().sync_all().map_err(internal_error)?;
        temporary.persist_noclobber(path).map_err(|error| {
            if error.error.kind() == std::io::ErrorKind::AlreadyExists {
                ProtocolError::new(
                    ProtocolErrorCode::Conflict,
                    "plan revision was concurrently persisted",
                )
            } else {
                internal_error(error.error)
            }
        })?;
        Ok(())
    }

    fn get(
        &self,
        engagement_id: &EngagementId,
        revision: u32,
    ) -> Result<Option<TaskingPlan>, ProtocolError> {
        let path = self.path(engagement_id, revision);
        match std::fs::read(path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(internal_error),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(internal_error(error)),
        }
    }

    fn path(&self, engagement_id: &EngagementId, revision: u32) -> PathBuf {
        let engagement_hash = blake3::hash(engagement_id.as_str().as_bytes())
            .to_hex()
            .to_string();
        self.root.join(format!("{engagement_hash}-{revision}.json"))
    }

    fn all_plans(&self) -> Result<Vec<TaskingPlan>, ProtocolError> {
        let mut plans: Vec<TaskingPlan> = Vec::new();
        for entry in std::fs::read_dir(&self.root).map_err(internal_error)? {
            let entry = entry.map_err(internal_error)?;
            if !entry.file_type().map_err(internal_error)?.is_file()
                || entry.path().extension().and_then(|value| value.to_str()) != Some("json")
            {
                continue;
            }
            let bytes = std::fs::read(entry.path()).map_err(internal_error)?;
            plans.push(serde_json::from_slice(&bytes).map_err(internal_error)?);
        }
        plans.sort_by(|left, right| {
            left.engagement_id
                .cmp(&right.engagement_id)
                .then_with(|| left.revision.cmp(&right.revision))
        });
        Ok(plans)
    }

    fn claim_dispatch(
        &self,
        dispatch: &ProviderDispatch,
        owner_epoch: &str,
    ) -> Result<DispatchClaim, ProtocolError> {
        let _guard = self
            .dispatch_lock
            .lock()
            .map_err(|_| internal_error("dispatch ledger lock is poisoned"))?;
        let path = self.dispatch_path(&dispatch.request_id);
        let encoded = canonical_json_bytes(dispatch)?;
        let dispatch_hash = blake3::hash(&encoded).to_hex().to_string();
        if path.exists() {
            let record: DispatchRecord =
                serde_json::from_slice(&std::fs::read(path).map_err(internal_error)?)
                    .map_err(internal_error)?;
            if record.dispatch_hash != dispatch_hash {
                return Err(ProtocolError::new(
                    ProtocolErrorCode::Conflict,
                    format!(
                        "request {} was reused with different dispatch content",
                        dispatch.request_id
                    ),
                ));
            }
            if record.status == DispatchLedgerStatus::Running && record.owner_epoch != owner_epoch {
                return Err(ProtocolError::new(
                    ProtocolErrorCode::Conflict,
                    format!(
                        "request {} was running in a previous daemon generation; it will not be replayed automatically",
                        dispatch.request_id
                    ),
                ));
            }
            return Ok(DispatchClaim::Existing(record));
        }
        let record = DispatchRecord {
            request_id: dispatch.request_id.clone(),
            engagement_id: dispatch.engagement_id.clone(),
            task_id: dispatch.task.task_id.clone(),
            provider_id: dispatch.provider_id.clone(),
            dispatch_hash,
            owner_epoch: owner_epoch.to_owned(),
            status: DispatchLedgerStatus::Running,
            updated_unix_ms: now_unix_ms(),
        };
        self.write_dispatch(&path, &record, true)?;
        Ok(DispatchClaim::New)
    }

    fn update_dispatch(
        &self,
        request_id: &xai_grok_protocol::RequestId,
        status: DispatchLedgerStatus,
    ) -> Result<(), ProtocolError> {
        let _guard = self
            .dispatch_lock
            .lock()
            .map_err(|_| internal_error("dispatch ledger lock is poisoned"))?;
        let path = self.dispatch_path(request_id);
        let mut record: DispatchRecord =
            serde_json::from_slice(&std::fs::read(&path).map_err(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    ProtocolError::new(
                        ProtocolErrorCode::NotFound,
                        format!("dispatch ledger for request {request_id} was not found"),
                    )
                } else {
                    internal_error(error)
                }
            })?)
            .map_err(internal_error)?;
        record.status = status;
        record.updated_unix_ms = now_unix_ms();
        self.write_dispatch(&path, &record, false)
    }

    fn release_unstarted(
        &self,
        request_id: &xai_grok_protocol::RequestId,
        owner_epoch: &str,
    ) -> Result<(), ProtocolError> {
        let _guard = self
            .dispatch_lock
            .lock()
            .map_err(|_| internal_error("dispatch ledger lock is poisoned"))?;
        let path = self.dispatch_path(request_id);
        let record: DispatchRecord =
            serde_json::from_slice(&std::fs::read(&path).map_err(internal_error)?)
                .map_err(internal_error)?;
        if record.status != DispatchLedgerStatus::Running || record.owner_epoch != owner_epoch {
            return Err(ProtocolError::new(
                ProtocolErrorCode::Conflict,
                format!(
                    "request {request_id} is no longer an unstarted dispatch owned by this daemon"
                ),
            ));
        }
        std::fs::remove_file(path).map_err(internal_error)
    }

    fn dispatch_path(&self, request_id: &xai_grok_protocol::RequestId) -> PathBuf {
        let request_hash = blake3::hash(request_id.as_str().as_bytes())
            .to_hex()
            .to_string();
        self.dispatch_root.join(format!("{request_hash}.json"))
    }

    fn write_dispatch(
        &self,
        path: &Path,
        record: &DispatchRecord,
        no_clobber: bool,
    ) -> Result<(), ProtocolError> {
        let bytes = serde_json::to_vec(record).map_err(internal_error)?;
        let mut temporary =
            tempfile::NamedTempFile::new_in(&self.dispatch_root).map_err(internal_error)?;
        temporary.write_all(&bytes).map_err(internal_error)?;
        temporary.as_file().sync_all().map_err(internal_error)?;
        if no_clobber {
            temporary
                .persist_noclobber(path)
                .map_err(|error| internal_error(error.error))?;
        } else {
            temporary
                .persist(path)
                .map_err(|error| internal_error(error.error))?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum DispatchLedgerStatus {
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct DispatchRecord {
    request_id: xai_grok_protocol::RequestId,
    engagement_id: EngagementId,
    task_id: xai_grok_protocol::TaskId,
    provider_id: ProviderId,
    dispatch_hash: String,
    owner_epoch: String,
    status: DispatchLedgerStatus,
    updated_unix_ms: u64,
}

enum DispatchClaim {
    New,
    Existing(DispatchRecord),
}

struct ControlPlaneCore {
    engagement: EngagementCoordinator,
    providers: Arc<ProviderRegistry>,
    services: Arc<ServiceSupervisor>,
    artifacts: Arc<ArtifactStore>,
    plans: Arc<PlanStore>,
    projections: Arc<ProjectionStore>,
    journal: Arc<EventJournal>,
    event_order: Mutex<()>,
    next_sequence: AtomicU64,
    events: broadcast::Sender<EventEnvelope>,
    event_cache: Mutex<VecDeque<EventEnvelope>>,
    event_cache_capacity: usize,
    responses: Mutex<ResponseCache>,
    owner_epoch: String,
    auto_schedule_plans: bool,
    shutdown: CancellationToken,
}

impl ControlPlaneCore {
    async fn process(self: &Arc<Self>, envelope: CommandEnvelope) -> ResponseEnvelope {
        let command_id = envelope.command_id.clone();
        let command_hash = match canonical_json_bytes(&envelope.command) {
            Ok(bytes) => blake3::hash(&bytes).to_hex().to_string(),
            Err(error) => {
                return ResponseEnvelope {
                    protocol_version: PROTOCOL_VERSION,
                    command_id,
                    response: Err(error),
                };
            }
        };
        match self
            .responses
            .lock()
            .await
            .get(&envelope.command_id, &command_hash)
        {
            Ok(Some(response)) => return response,
            Ok(None) => {}
            Err(error) => {
                return ResponseEnvelope {
                    protocol_version: PROTOCOL_VERSION,
                    command_id,
                    response: Err(error),
                };
            }
        }
        let durable = command_requires_durable_response(&envelope.command);
        if durable {
            match self
                .engagement
                .claim_control_command(command_id.to_string(), command_hash.clone())
                .await
            {
                Ok(ControlCommandClaim::New) => {}
                Ok(ControlCommandClaim::Completed(response_json)) => {
                    let response = match serde_json::from_str::<ResponseEnvelope>(&response_json) {
                        Ok(response) if response.command_id == command_id => response,
                        Ok(_) => ResponseEnvelope {
                            protocol_version: PROTOCOL_VERSION,
                            command_id,
                            response: Err(internal_error(
                                "persisted command response has a mismatched command ID",
                            )),
                        },
                        Err(error) => ResponseEnvelope {
                            protocol_version: PROTOCOL_VERSION,
                            command_id,
                            response: Err(internal_error(format!(
                                "persisted command response is invalid: {error}"
                            ))),
                        },
                    };
                    self.responses
                        .lock()
                        .await
                        .insert(command_hash, response.clone());
                    return response;
                }
                Ok(ControlCommandClaim::InDoubt) => {
                    return ResponseEnvelope {
                        protocol_version: PROTOCOL_VERSION,
                        command_id,
                        response: Err(ProtocolError::new(
                            ProtocolErrorCode::ServiceUnavailable,
                            "command execution began before daemon recovery; it will not be repeated because its outcome is indeterminate",
                        )),
                    };
                }
                Err(error) => {
                    return ResponseEnvelope {
                        protocol_version: PROTOCOL_VERSION,
                        command_id,
                        response: Err(engagement_error(error)),
                    };
                }
            }
        }
        let result = match envelope.validate(now_unix_ms()) {
            Ok(()) => self.handle(envelope).await,
            Err(error) => Err(error),
        };
        let response = ResponseEnvelope {
            protocol_version: PROTOCOL_VERSION,
            command_id,
            response: result,
        };
        if durable {
            let persisted = serde_json::to_string(&response).map_err(internal_error);
            let persisted = match persisted {
                Ok(response_json) => self
                    .engagement
                    .complete_control_command(
                        response.command_id.to_string(),
                        command_hash.clone(),
                        response_json,
                    )
                    .await
                    .map_err(engagement_error),
                Err(error) => Err(error),
            };
            if let Err(error) = persisted {
                tracing::error!(%error, command_id = %response.command_id, "failed to persist command result; stopping daemon");
                self.shutdown.cancel();
            }
        }
        self.responses
            .lock()
            .await
            .insert(command_hash, response.clone());
        response
    }

    async fn handle(
        self: &Arc<Self>,
        envelope: CommandEnvelope,
    ) -> Result<Response, ProtocolError> {
        let causation_id = Some(envelope.command_id.to_string());
        let command_deadline_unix_ms = envelope.deadline_unix_ms;
        match envelope.command {
            Command::Hello(hello) => {
                if hello.protocol_version != PROTOCOL_VERSION {
                    return Err(ProtocolError::new(
                        ProtocolErrorCode::IncompatibleVersion,
                        format!(
                            "client protocol {} is incompatible with {}",
                            hello.protocol_version, PROTOCOL_VERSION
                        ),
                    ));
                }
                if let Some(team) = &hello.team {
                    team.validate()?;
                }
                Ok(Response::Hello(xai_grok_protocol::HelloAck {
                    protocol_version: PROTOCOL_VERSION,
                    server_name: SERVER_NAME.to_owned(),
                    server_version: env!("CARGO_PKG_VERSION").to_owned(),
                    capabilities: vec![
                        "durable_engagements".to_owned(),
                        "capability_registry".to_owned(),
                        "artifact_spooling".to_owned(),
                        "generation_fencing".to_owned(),
                        "rebuildable_projections".to_owned(),
                        "durable_event_cursors".to_owned(),
                        "bounded_event_long_poll".to_owned(),
                        "team_client_reconnect".to_owned(),
                        "typed_execution_receipts".to_owned(),
                        "exercise_catalog".to_owned(),
                        "operation_runs".to_owned(),
                        "operator_sessions".to_owned(),
                        "workspace_playbooks".to_owned(),
                        "normalized_exercise_evidence".to_owned(),
                        "finding_lifecycle".to_owned(),
                        "team_presence".to_owned(),
                        "team_work_handoff".to_owned(),
                        "team_channels".to_owned(),
                        "team_resource_leases".to_owned(),
                    ],
                }))
            }
            Command::CreateExercise(create) => {
                create.validate()?;
                let now = now_unix_ms();
                let exercise = Exercise {
                    exercise_id: ExerciseId::new(),
                    workspace_id: create.workspace_id,
                    name: create.name,
                    objective: create.objective,
                    status: ExerciseStatus::Active,
                    scope: create.scope,
                    created_unix_ms: now,
                    updated_unix_ms: now,
                };
                self.emit(
                    None,
                    causation_id,
                    0,
                    Event::ExerciseCreated {
                        exercise: exercise.clone(),
                    },
                )
                .await?;
                Ok(Response::ExerciseCreated { exercise })
            }
            Command::CreateOperationRun(create) => {
                create.validate()?;
                let exercise = self
                    .projections
                    .exercise(&create.exercise_id)
                    .await
                    .ok_or_else(|| {
                        ProtocolError::new(
                            ProtocolErrorCode::NotFound,
                            format!("exercise {} does not exist", create.exercise_id),
                        )
                    })?;
                if let Some(playbook_id) = &create.playbook_id {
                    let playbook =
                        self.projections
                            .playbook(playbook_id)
                            .await
                            .ok_or_else(|| {
                                ProtocolError::new(
                                    ProtocolErrorCode::NotFound,
                                    format!("playbook {playbook_id} does not exist"),
                                )
                            })?;
                    if playbook.workspace_id != exercise.workspace_id {
                        return Err(ProtocolError::new(
                            ProtocolErrorCode::Conflict,
                            "playbook belongs to a different workspace",
                        ));
                    }
                }
                let now = now_unix_ms();
                let operation_run = OperationRun {
                    operation_run_id: OperationRunId::new(),
                    exercise_id: create.exercise_id,
                    name: create.name,
                    objective: create.objective,
                    playbook_id: create.playbook_id,
                    status: OperationRunStatus::Planned,
                    created_unix_ms: now,
                    updated_unix_ms: now,
                };
                self.emit(
                    None,
                    causation_id,
                    0,
                    Event::OperationRunCreated {
                        operation_run: operation_run.clone(),
                    },
                )
                .await?;
                Ok(Response::OperationRunCreated { operation_run })
            }
            Command::CreateOperatorSession(create) => {
                create.validate()?;
                if !self
                    .projections
                    .contains_exercise(&create.exercise_id)
                    .await
                {
                    return Err(ProtocolError::new(
                        ProtocolErrorCode::NotFound,
                        format!("exercise {} does not exist", create.exercise_id),
                    ));
                }
                if let Some(operation_run_id) = &create.operation_run_id {
                    let operation_run = self
                        .projections
                        .operation_run(operation_run_id)
                        .await
                        .ok_or_else(|| {
                            ProtocolError::new(
                                ProtocolErrorCode::NotFound,
                                format!("operation run {operation_run_id} does not exist"),
                            )
                        })?;
                    if operation_run.exercise_id != create.exercise_id {
                        return Err(ProtocolError::new(
                            ProtocolErrorCode::Conflict,
                            "operation run belongs to a different exercise",
                        ));
                    }
                }
                let now = now_unix_ms();
                let session = OperatorSession {
                    session_id: OperatorSessionId::new(),
                    exercise_id: create.exercise_id,
                    operation_run_id: create.operation_run_id,
                    name: create.name,
                    purpose: create.purpose,
                    status: OperatorSessionStatus::Active,
                    created_unix_ms: now,
                    last_active_unix_ms: now,
                };
                self.emit(
                    None,
                    causation_id,
                    0,
                    Event::OperatorSessionCreated {
                        session: session.clone(),
                    },
                )
                .await?;
                Ok(Response::OperatorSessionCreated { session })
            }
            Command::CreatePlaybook(create) => {
                create.validate()?;
                let playbook = Playbook {
                    playbook_id: PlaybookId::new(),
                    workspace_id: create.workspace_id,
                    revision: 1,
                    name: create.name,
                    description: create.description,
                    steps: create.steps,
                };
                self.emit(
                    None,
                    causation_id,
                    0,
                    Event::PlaybookCreated {
                        playbook: playbook.clone(),
                    },
                )
                .await?;
                Ok(Response::PlaybookCreated { playbook })
            }
            Command::RecordEvidence(record) => {
                record.validate()?;
                self.validate_exercise_links(
                    &record.exercise_id,
                    record.operation_run_id.as_ref(),
                    record.session_id.as_ref(),
                )
                .await?;
                if let Some(task_id) = &record.task_id
                    && !self
                        .projections
                        .task_belongs_to_exercise(task_id, &record.exercise_id)
                        .await
                {
                    return Err(ProtocolError::new(
                        ProtocolErrorCode::Conflict,
                        format!("task {task_id} is not associated with the exercise"),
                    ));
                }
                if let Some(artifact_id) = &record.artifact_id {
                    let artifacts = self.artifacts.clone();
                    let artifact_id = artifact_id.clone();
                    tokio::task::spawn_blocking(move || artifacts.descriptor(&artifact_id))
                        .await
                        .map_err(|error| internal_error(error.to_string()))?
                        .map_err(|error| match error {
                            crate::artifact::ArtifactError::NotFound(id) => ProtocolError::new(
                                ProtocolErrorCode::NotFound,
                                format!("artifact {id} does not exist"),
                            ),
                            other => internal_error(other.to_string()),
                        })?;
                }
                let evidence = ExerciseEvidence {
                    evidence_id: EvidenceId::new(),
                    exercise_id: record.exercise_id,
                    operation_run_id: record.operation_run_id,
                    session_id: record.session_id,
                    task_id: record.task_id,
                    finding: record.finding,
                    confidence: record.confidence,
                    artifact_id: record.artifact_id,
                    attributes: record.attributes,
                    observed_unix_ms: now_unix_ms(),
                };
                self.emit(
                    None,
                    causation_id,
                    0,
                    Event::EvidenceRecorded {
                        evidence: evidence.clone(),
                    },
                )
                .await?;
                Ok(Response::EvidenceRecorded { evidence })
            }
            Command::CreateFinding(create) => {
                create.validate()?;
                self.validate_exercise_links(
                    &create.exercise_id,
                    create.operation_run_id.as_ref(),
                    None,
                )
                .await?;
                for evidence_id in &create.evidence_ids {
                    let evidence =
                        self.projections
                            .evidence(evidence_id)
                            .await
                            .ok_or_else(|| {
                                ProtocolError::new(
                                    ProtocolErrorCode::NotFound,
                                    format!("evidence {evidence_id} does not exist"),
                                )
                            })?;
                    if evidence.exercise_id != create.exercise_id {
                        return Err(ProtocolError::new(
                            ProtocolErrorCode::Conflict,
                            format!("evidence {evidence_id} belongs to a different exercise"),
                        ));
                    }
                }
                let exercise = self
                    .projections
                    .exercise(&create.exercise_id)
                    .await
                    .expect("validated exercise is present");
                for target_id in &create.target_ids {
                    if !exercise
                        .scope
                        .iter()
                        .any(|target| &target.target_id == target_id)
                    {
                        return Err(ProtocolError::new(
                            ProtocolErrorCode::Conflict,
                            format!("target {target_id} is not in the exercise scope"),
                        ));
                    }
                }
                let now = now_unix_ms();
                let finding = Finding {
                    finding_id: FindingId::new(),
                    exercise_id: create.exercise_id,
                    operation_run_id: create.operation_run_id,
                    title: create.title,
                    summary: create.summary,
                    severity: create.severity,
                    status: FindingStatus::Candidate,
                    evidence_ids: create.evidence_ids,
                    target_ids: create.target_ids,
                    created_unix_ms: now,
                    updated_unix_ms: now,
                };
                self.emit(
                    None,
                    causation_id,
                    0,
                    Event::FindingCreated {
                        finding: finding.clone(),
                    },
                )
                .await?;
                Ok(Response::FindingCreated { finding })
            }
            Command::SetFindingStatus { finding_id, status } => {
                let mut finding = self.projections.finding(&finding_id).await.ok_or_else(|| {
                    ProtocolError::new(
                        ProtocolErrorCode::NotFound,
                        format!("finding {finding_id} does not exist"),
                    )
                })?;
                finding.status = status;
                finding.updated_unix_ms = now_unix_ms();
                self.emit(
                    None,
                    causation_id,
                    0,
                    Event::FindingStatusSet {
                        finding: finding.clone(),
                    },
                )
                .await?;
                Ok(Response::FindingStatusSet { finding })
            }
            Command::SetTeamPresence(update) => {
                update.validate()?;
                if let Some(exercise_id) = &update.exercise_id {
                    self.validate_exercise_links(
                        exercise_id,
                        update.operation_run_id.as_ref(),
                        update.session_id.as_ref(),
                    )
                    .await?;
                    if let Some(workspace_id) = &update.workspace_id {
                        let exercise = self
                            .projections
                            .exercise(exercise_id)
                            .await
                            .expect("validated exercise exists");
                        if &exercise.workspace_id != workspace_id {
                            return Err(ProtocolError::new(
                                ProtocolErrorCode::Conflict,
                                "team presence workspace does not own the selected exercise",
                            ));
                        }
                    }
                }
                let presence = TeamPresence {
                    client: update.client,
                    state: update.state,
                    workspace_id: update.workspace_id,
                    exercise_id: update.exercise_id,
                    operation_run_id: update.operation_run_id,
                    session_id: update.session_id,
                    last_seen_unix_ms: now_unix_ms(),
                };
                self.emit(
                    None,
                    causation_id,
                    0,
                    Event::TeamPresenceSet {
                        presence: presence.clone(),
                    },
                )
                .await?;
                Ok(Response::TeamPresenceSet { presence })
            }
            Command::CreateTeamWorkItem(create) => {
                create.validate()?;
                if let Some(exercise_id) = &create.exercise_id {
                    self.validate_exercise_links(
                        exercise_id,
                        create.operation_run_id.as_ref(),
                        None,
                    )
                    .await?;
                }
                if let Some(assignee) = &create.assignee
                    && !self
                        .projections
                        .team_member_exists(&create.team_id, assignee)
                        .await
                {
                    return Err(ProtocolError::new(
                        ProtocolErrorCode::NotFound,
                        format!("team member {assignee} has not announced presence"),
                    ));
                }
                let now = now_unix_ms();
                let work_item = TeamWorkItem {
                    work_item_id: TeamWorkItemId::new(),
                    team_id: create.team_id,
                    exercise_id: create.exercise_id,
                    operation_run_id: create.operation_run_id,
                    title: create.title,
                    objective: create.objective,
                    assignee: create.assignee,
                    status: TeamWorkItemStatus::Open,
                    revision: 1,
                    created_unix_ms: now,
                    updated_unix_ms: now,
                };
                self.emit(
                    None,
                    causation_id,
                    0,
                    Event::TeamWorkItemCreated {
                        work_item: work_item.clone(),
                    },
                )
                .await?;
                Ok(Response::TeamWorkItemCreated { work_item })
            }
            Command::AssignTeamWorkItem {
                work_item_id,
                assignee,
                expected_revision,
            } => {
                let mut work_item = self
                    .team_work_item_at_revision(&work_item_id, expected_revision)
                    .await?;
                if let Some(assignee) = &assignee
                    && !self
                        .projections
                        .team_member_exists(&work_item.team_id, assignee)
                        .await
                {
                    return Err(ProtocolError::new(
                        ProtocolErrorCode::NotFound,
                        format!("team member {assignee} has not announced presence"),
                    ));
                }
                work_item.assignee = assignee;
                work_item.revision = work_item.revision.saturating_add(1);
                work_item.updated_unix_ms = now_unix_ms();
                self.emit(
                    None,
                    causation_id,
                    0,
                    Event::TeamWorkItemUpdated {
                        work_item: work_item.clone(),
                    },
                )
                .await?;
                Ok(Response::TeamWorkItemUpdated { work_item })
            }
            Command::SetTeamWorkItemStatus {
                work_item_id,
                status,
                expected_revision,
            } => {
                let mut work_item = self
                    .team_work_item_at_revision(&work_item_id, expected_revision)
                    .await?;
                work_item.status = status;
                work_item.revision = work_item.revision.saturating_add(1);
                work_item.updated_unix_ms = now_unix_ms();
                self.emit(
                    None,
                    causation_id,
                    0,
                    Event::TeamWorkItemUpdated {
                        work_item: work_item.clone(),
                    },
                )
                .await?;
                Ok(Response::TeamWorkItemUpdated { work_item })
            }
            Command::PostTeamMessage(post) => {
                post.validate()?;
                if !self
                    .projections
                    .team_member_exists(&post.team_id, &post.sender)
                    .await
                {
                    return Err(ProtocolError::new(
                        ProtocolErrorCode::NotFound,
                        format!("team member {} has not announced presence", post.sender),
                    ));
                }
                if let Some(reply_to) = &post.reply_to {
                    let parent =
                        self.projections
                            .team_message(reply_to)
                            .await
                            .ok_or_else(|| {
                                ProtocolError::new(
                                    ProtocolErrorCode::NotFound,
                                    format!("team message {reply_to} does not exist"),
                                )
                            })?;
                    if parent.team_id != post.team_id || parent.channel_id != post.channel_id {
                        return Err(ProtocolError::new(
                            ProtocolErrorCode::Conflict,
                            "reply target belongs to a different team channel",
                        ));
                    }
                }
                let message = TeamMessage {
                    message_id: MessageId::new(),
                    team_id: post.team_id,
                    channel_id: post.channel_id,
                    sender: post.sender,
                    body: post.body,
                    reply_to: post.reply_to,
                    sent_unix_ms: now_unix_ms(),
                };
                self.emit(
                    None,
                    causation_id,
                    0,
                    Event::TeamMessagePosted {
                        message: message.clone(),
                    },
                )
                .await?;
                Ok(Response::TeamMessagePosted { message })
            }
            Command::ClaimTeamResource(request) => {
                request.validate()?;
                if !self
                    .projections
                    .team_member_exists(&request.team_id, &request.owner)
                    .await
                {
                    return Err(ProtocolError::new(
                        ProtocolErrorCode::NotFound,
                        format!("team member {} has not announced presence", request.owner),
                    ));
                }
                let now = now_unix_ms();
                let claim = if let Some(mut claim) = self
                    .projections
                    .active_team_resource_claim(&request.team_id, &request.resource_key, now)
                    .await
                {
                    if claim.owner != request.owner {
                        return Err(ProtocolError::new(
                            ProtocolErrorCode::Conflict,
                            format!(
                                "resource {} is claimed by {} until {}",
                                request.resource_key, claim.owner, claim.expires_unix_ms
                            ),
                        ));
                    }
                    claim.revision = claim.revision.saturating_add(1);
                    claim.expires_unix_ms = now.saturating_add(u64::from(request.lease_ms));
                    claim
                } else {
                    TeamResourceClaim {
                        claim_id: ResourceClaimId::new(),
                        team_id: request.team_id,
                        owner: request.owner,
                        resource_key: request.resource_key,
                        revision: 1,
                        claimed_unix_ms: now,
                        expires_unix_ms: now.saturating_add(u64::from(request.lease_ms)),
                        released_unix_ms: None,
                    }
                };
                self.emit(
                    None,
                    causation_id,
                    0,
                    Event::TeamResourceClaimed {
                        claim: claim.clone(),
                    },
                )
                .await?;
                Ok(Response::TeamResourceClaimed { claim })
            }
            Command::ReleaseTeamResource {
                claim_id,
                owner,
                expected_revision,
            } => {
                let mut claim = self
                    .projections
                    .team_resource_claim(&claim_id)
                    .await
                    .ok_or_else(|| {
                        ProtocolError::new(
                            ProtocolErrorCode::NotFound,
                            format!("team resource claim {claim_id} does not exist"),
                        )
                    })?;
                if claim.owner != owner {
                    return Err(ProtocolError::new(
                        ProtocolErrorCode::Conflict,
                        "team resource claim belongs to a different owner",
                    ));
                }
                if claim.revision != expected_revision {
                    return Err(revision_conflict(
                        "team resource claim",
                        expected_revision,
                        claim.revision,
                    ));
                }
                if claim.released_unix_ms.is_some() {
                    return Err(ProtocolError::new(
                        ProtocolErrorCode::Conflict,
                        "team resource claim is already released",
                    ));
                }
                claim.revision = claim.revision.saturating_add(1);
                claim.released_unix_ms = Some(now_unix_ms());
                self.emit(
                    None,
                    causation_id,
                    0,
                    Event::TeamResourceReleased {
                        claim: claim.clone(),
                    },
                )
                .await?;
                Ok(Response::TeamResourceReleased { claim })
            }
            Command::InvokeProvider {
                request_id,
                operation_id,
                preferred_provider,
                input,
            } => {
                let requirement = CapabilityRequirement {
                    operation_id,
                    preferred_provider: preferred_provider.clone(),
                    required_features: Vec::new(),
                };
                let provider_id = self
                    .providers
                    .resolve_requirement(
                        &requirement,
                        preferred_provider.as_ref(),
                        ExecutionMode::Interactive,
                    )
                    .await?;
                let dispatch = ProviderDispatch {
                    request_id: request_id.clone(),
                    engagement_id: EngagementId::from_string(format!(
                        "invoke_{}",
                        request_id.as_str()
                    )),
                    plan_revision: 0,
                    task: ExecutionTask {
                        task_id: TaskId::from_string(format!("invoke_{}", request_id.as_str())),
                        objective: "Direct provider invocation".to_owned(),
                        mode: ExecutionMode::Interactive,
                        capability: requirement,
                        input,
                        deadline_unix_ms: command_deadline_unix_ms,
                        completion_tests: Vec::new(),
                        depends_on: Vec::new(),
                    },
                    provider_id: provider_id.clone(),
                    lease_epoch: 0,
                };
                let provider_output = self.providers.dispatch(dispatch).await?;
                let mut artifacts = Vec::new();
                for artifact in provider_output.artifacts {
                    let descriptor = self.store_provider_artifact(artifact).await?;
                    self.emit(
                        None,
                        causation_id.clone(),
                        0,
                        Event::ArtifactAvailable {
                            artifact_id: descriptor.artifact_id.clone(),
                            task_id: None,
                            media_type: descriptor.media_type.clone(),
                            byte_size: descriptor.byte_size,
                        },
                    )
                    .await?;
                    artifacts.push(serde_json::to_value(descriptor).map_err(internal_error)?);
                }
                let output = if artifacts.is_empty() {
                    provider_output.output
                } else {
                    serde_json::json!({
                        "result": provider_output.output,
                        "artifacts": artifacts,
                    })
                };
                Ok(Response::ProviderInvoked {
                    request_id,
                    provider_id,
                    output,
                    observations: provider_output.observations,
                })
            }
            Command::SubmitIngress(ingress) => {
                ingress.validate()?;
                if let Some(exercise_id) = &ingress.exercise_id
                    && !self.projections.contains_exercise(exercise_id).await
                {
                    return Err(ProtocolError::new(
                        ProtocolErrorCode::NotFound,
                        format!("exercise {exercise_id} does not exist"),
                    ));
                }
                if let Some(operation_run_id) = &ingress.operation_run_id {
                    let run = self
                        .projections
                        .operation_run(operation_run_id)
                        .await
                        .ok_or_else(|| {
                            ProtocolError::new(
                                ProtocolErrorCode::NotFound,
                                format!("operation run {operation_run_id} does not exist"),
                            )
                        })?;
                    if ingress.exercise_id.as_ref() != Some(&run.exercise_id) {
                        return Err(ProtocolError::new(
                            ProtocolErrorCode::Conflict,
                            "operation run belongs to a different exercise",
                        ));
                    }
                }
                if let Some(operator_session_id) = &ingress.operator_session_id {
                    let session = self
                        .projections
                        .operator_session(operator_session_id)
                        .await
                        .ok_or_else(|| {
                            ProtocolError::new(
                                ProtocolErrorCode::NotFound,
                                format!("operator session {operator_session_id} does not exist"),
                            )
                        })?;
                    if ingress.exercise_id.as_ref() != Some(&session.exercise_id)
                        || ingress.operation_run_id != session.operation_run_id
                    {
                        return Err(ProtocolError::new(
                            ProtocolErrorCode::Conflict,
                            "operator session does not belong to the selected exercise/run",
                        ));
                    }
                }
                let ingress_team = ingress.team.clone();
                let exercise_id = ingress.exercise_id.clone();
                let operation_run_id = ingress.operation_run_id.clone();
                let operator_session_id = ingress.operator_session_id.clone();
                let turn_request = ingress.request.clone();
                let turn_session_key = ingress
                    .operator_session_id
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| ingress.session_id.clone());
                let turn_task_id = scheduled_turn_task_id(&ingress.prompt_id);
                let outcome = self
                    .engagement
                    .accept(NewEngagement {
                        session_id: ingress.session_id.clone(),
                        prompt_id: ingress.prompt_id,
                        workspace_id: ingress.workspace_id.to_string(),
                        user_request: ingress.request,
                        priority: QueuePriority::Interactive,
                        checkpoint: EngagementCheckpoint::default(),
                    })
                    .await
                    .map_err(engagement_error)?;
                let engagement_id =
                    EngagementId::from_string(outcome.record.engagement_id.to_string());
                if outcome.accepted {
                    self.emit(
                        Some(engagement_id.clone()),
                        causation_id.clone(),
                        0,
                        Event::EngagementAccepted {
                            workspace_id: outcome.record.workspace_id,
                            session_id: outcome.record.session_id,
                            exercise_id,
                            operation_run_id,
                            operator_session_id,
                            team_id: ingress_team.as_ref().map(|team| team.team_id.clone()),
                            client_id: ingress_team.as_ref().map(|team| team.client_id.clone()),
                        },
                    )
                    .await?;
                    if self.auto_schedule_plans {
                        let plan = TaskingPlan {
                            engagement_id: engagement_id.clone(),
                            revision: 1,
                            objective: turn_request.clone(),
                            tasks: vec![ExecutionTask {
                                task_id: turn_task_id,
                                objective: "Execute the submitted operator turn".to_owned(),
                                mode: ExecutionMode::Interactive,
                                capability: CapabilityRequirement {
                                    operation_id: AGENT_TURN_OPERATION.into(),
                                    preferred_provider: Some("agent-runtime".into()),
                                    required_features: vec![
                                        "session_continuity".to_owned(),
                                        "tool_loop".to_owned(),
                                    ],
                                },
                                input: serde_json::json!({
                                    "request": turn_request,
                                    "session_key": turn_session_key,
                                }),
                                deadline_unix_ms: now_unix_ms().saturating_add(30 * 60 * 1_000),
                                completion_tests: Vec::new(),
                                depends_on: Vec::new(),
                            }],
                        };
                        let stored = plan.clone();
                        let plans = self.plans.clone();
                        tokio::task::spawn_blocking(move || plans.put(&stored))
                            .await
                            .map_err(|error| internal_error(error.to_string()))??;
                        self.emit(
                            Some(engagement_id.clone()),
                            causation_id.clone(),
                            0,
                            Event::PlanAccepted {
                                revision: plan.revision,
                                task_count: plan.tasks.len() as u32,
                                plan: Some(plan.clone()),
                            },
                        )
                        .await?;
                        self.emit(
                            Some(engagement_id.clone()),
                            causation_id.clone(),
                            0,
                            Event::TaskStatus {
                                task_id: plan.tasks[0].task_id.clone(),
                                status: TaskStatus::Prepared,
                                provider_id: None,
                            },
                        )
                        .await?;
                        self.spawn_schedule_plan(plan, causation_id.clone());
                    }
                }
                Ok(Response::Accepted {
                    engagement_id,
                    accepted: outcome.accepted,
                })
            }
            Command::SubmitPlan(plan) => {
                plan.validate().map_err(|message| {
                    ProtocolError::new(ProtocolErrorCode::InvalidEnvelope, message)
                })?;
                let known = self
                    .engagement
                    .get(xai_grok_engagement::EngagementId(
                        plan.engagement_id.to_string(),
                    ))
                    .await
                    .map_err(engagement_error)?
                    .is_some();
                if !known {
                    return Err(ProtocolError::new(
                        ProtocolErrorCode::NotFound,
                        format!("engagement {} does not exist", plan.engagement_id),
                    ));
                }
                let plan_for_store = plan.clone();
                let plans = self.plans.clone();
                tokio::task::spawn_blocking(move || plans.put(&plan_for_store))
                    .await
                    .map_err(|error| internal_error(error.to_string()))??;
                self.emit(
                    Some(plan.engagement_id.clone()),
                    causation_id.clone(),
                    0,
                    Event::PlanAccepted {
                        revision: plan.revision,
                        task_count: plan.tasks.len() as u32,
                        plan: Some(plan.clone()),
                    },
                )
                .await?;
                let existing = self.projections.task_statuses(&plan.engagement_id).await;
                for task in &plan.tasks {
                    if !existing.contains_key(&task.task_id) {
                        self.emit(
                            Some(plan.engagement_id.clone()),
                            causation_id.clone(),
                            0,
                            Event::TaskStatus {
                                task_id: task.task_id.clone(),
                                status: TaskStatus::Prepared,
                                provider_id: None,
                            },
                        )
                        .await?;
                    }
                }
                let response = Response::PlanAccepted {
                    engagement_id: plan.engagement_id.clone(),
                    revision: plan.revision,
                };
                self.spawn_schedule_plan(plan, causation_id);
                Ok(response)
            }
            Command::Dispatch(mut dispatch) => {
                let plans = self.plans.clone();
                let engagement_id = dispatch.engagement_id.clone();
                let revision = dispatch.plan_revision;
                let plan = tokio::task::spawn_blocking(move || plans.get(&engagement_id, revision))
                    .await
                    .map_err(|error| internal_error(error.to_string()))??
                    .ok_or_else(|| {
                        ProtocolError::new(
                            ProtocolErrorCode::NotFound,
                            format!(
                                "engagement {} plan revision {} does not exist",
                                dispatch.engagement_id, dispatch.plan_revision
                            ),
                        )
                    })?;
                let planned_task = plan
                    .tasks
                    .iter()
                    .find(|task| task.task_id == dispatch.task.task_id)
                    .ok_or_else(|| {
                        ProtocolError::new(
                            ProtocolErrorCode::InvalidEnvelope,
                            format!(
                                "task {} is not in the persisted plan",
                                dispatch.task.task_id
                            ),
                        )
                    })?;
                if planned_task != &dispatch.task {
                    return Err(ProtocolError::new(
                        ProtocolErrorCode::Conflict,
                        "dispatch task differs from the persisted plan",
                    ));
                }
                let preferred = dispatch
                    .task
                    .capability
                    .preferred_provider
                    .as_ref()
                    .or(Some(&dispatch.provider_id));
                let provider_id = self
                    .providers
                    .resolve_requirement(&dispatch.task.capability, preferred, dispatch.task.mode)
                    .await?;
                dispatch.provider_id = provider_id.clone();
                let request_id = dispatch.request_id.clone();
                let dispatch_for_ledger = dispatch.clone();
                let plans = self.plans.clone();
                let owner_epoch = self.owner_epoch.clone();
                let claim = tokio::task::spawn_blocking(move || {
                    plans.claim_dispatch(&dispatch_for_ledger, &owner_epoch)
                })
                .await
                .map_err(|error| internal_error(error.to_string()))??;
                if let DispatchClaim::Existing(record) = claim {
                    dispatch.provider_id = record.provider_id.clone();
                    return Ok(Response::DispatchAccepted {
                        request_id: record.request_id,
                        provider_id: record.provider_id,
                        receipt: ExecutionReceipt::from_dispatch(&dispatch),
                    });
                }
                if let Err(error) = self
                    .emit(
                        Some(dispatch.engagement_id.clone()),
                        causation_id.clone(),
                        dispatch.lease_epoch,
                        Event::TaskStatus {
                            task_id: dispatch.task.task_id.clone(),
                            status: TaskStatus::Dispatched,
                            provider_id: Some(provider_id.clone()),
                        },
                    )
                    .await
                {
                    let plans = self.plans.clone();
                    let request_id = dispatch.request_id.clone();
                    let owner_epoch = self.owner_epoch.clone();
                    let _ = tokio::task::spawn_blocking(move || {
                        plans.release_unstarted(&request_id, &owner_epoch)
                    })
                    .await;
                    return Err(error);
                }
                let receipt = ExecutionReceipt::from_dispatch(&dispatch);
                self.spawn_dispatch(dispatch, causation_id);
                Ok(Response::DispatchAccepted {
                    request_id,
                    provider_id,
                    receipt,
                })
            }
            Command::CancelTask {
                engagement_id,
                task_id,
                request_id,
            } => {
                self.providers.cancel(&request_id).await?;
                self.update_dispatch_status(request_id.clone(), DispatchLedgerStatus::Cancelled)
                    .await?;
                self.emit(
                    Some(engagement_id),
                    causation_id,
                    0,
                    Event::TaskStatus {
                        task_id,
                        status: TaskStatus::Cancelled,
                        provider_id: None,
                    },
                )
                .await?;
                Ok(Response::Ack)
            }
            Command::RegisterProvider { manifest } => {
                let _ = manifest;
                Err(ProtocolError::new(
                    ProtocolErrorCode::UnsupportedOperation,
                    "metadata-only provider registration is disabled because it cannot attach an executable provider; providers must be registered through a supervised executable transport",
                ))
            }
            Command::ProviderHeartbeat {
                provider_id,
                generation,
                health,
            } => {
                if self.providers.generation(&provider_id).await.is_none() {
                    return Err(ProtocolError::new(
                        ProtocolErrorCode::UnknownProvider,
                        format!("provider {provider_id} has no executable registration"),
                    ));
                }
                let record = self
                    .services
                    .heartbeat(&provider_id, generation, health)
                    .await?;
                self.emit(
                    None,
                    causation_id,
                    generation,
                    Event::ProviderState {
                        provider_id,
                        service_id: record.service_id,
                        generation,
                        health,
                    },
                )
                .await?;
                Ok(Response::Ack)
            }
            Command::PutArtifact { media_type, bytes } => {
                let artifacts = self.artifacts.clone();
                let descriptor =
                    tokio::task::spawn_blocking(move || artifacts.put(media_type, &bytes))
                        .await
                        .map_err(|error| internal_error(error.to_string()))?
                        .map_err(artifact_error)?;
                self.emit(
                    None,
                    causation_id,
                    0,
                    Event::ArtifactAvailable {
                        artifact_id: descriptor.artifact_id.clone(),
                        task_id: None,
                        media_type: descriptor.media_type,
                        byte_size: descriptor.byte_size,
                    },
                )
                .await?;
                Ok(Response::ArtifactStored {
                    artifact_id: descriptor.artifact_id,
                    content_hash: descriptor.content_hash,
                    byte_size: descriptor.byte_size,
                })
            }
            Command::BeginArtifactUpload {
                media_type,
                expected_bytes,
                expected_content_hash,
            } => {
                let artifacts = self.artifacts.clone();
                let lease = tokio::task::spawn_blocking(move || {
                    artifacts.begin_upload(media_type, expected_bytes, expected_content_hash)
                })
                .await
                .map_err(|error| internal_error(error.to_string()))?
                .map_err(artifact_error)?;
                Ok(Response::ArtifactUploadStarted {
                    upload_id: lease.upload_id,
                    media_type: lease.media_type,
                    expected_bytes: lease.expected_bytes,
                    expected_content_hash: lease.expected_content_hash,
                    next_offset: lease.next_offset,
                    expires_unix_ms: lease.expires_unix_ms,
                })
            }
            Command::UploadArtifactChunk {
                upload_id,
                offset,
                bytes,
            } => {
                let artifacts = self.artifacts.clone();
                let requested_upload_id = upload_id.clone();
                let progress = tokio::task::spawn_blocking(move || {
                    artifacts.upload_chunk(&requested_upload_id, offset, &bytes)
                })
                .await
                .map_err(|error| internal_error(error.to_string()))?
                .map_err(artifact_error)?;
                Ok(Response::ArtifactUploadProgress {
                    upload_id: progress.upload_id,
                    next_offset: progress.next_offset,
                })
            }
            Command::InspectArtifactUpload { upload_id } => {
                let artifacts = self.artifacts.clone();
                let requested_upload_id = upload_id.clone();
                let lease = tokio::task::spawn_blocking(move || {
                    artifacts.inspect_upload(&requested_upload_id)
                })
                .await
                .map_err(|error| internal_error(error.to_string()))?
                .map_err(artifact_error)?;
                Ok(Response::ArtifactUploadState {
                    upload_id: lease.upload_id,
                    media_type: lease.media_type,
                    expected_bytes: lease.expected_bytes,
                    expected_content_hash: lease.expected_content_hash,
                    next_offset: lease.next_offset,
                    expires_unix_ms: lease.expires_unix_ms,
                })
            }
            Command::CommitArtifactUpload { upload_id } => {
                let artifacts = self.artifacts.clone();
                let requested_upload_id = upload_id.clone();
                let descriptor = tokio::task::spawn_blocking(move || {
                    artifacts.commit_upload(&requested_upload_id)
                })
                .await
                .map_err(|error| internal_error(error.to_string()))?
                .map_err(artifact_error)?;
                self.emit(
                    None,
                    causation_id,
                    0,
                    Event::ArtifactAvailable {
                        artifact_id: descriptor.artifact_id.clone(),
                        task_id: None,
                        media_type: descriptor.media_type,
                        byte_size: descriptor.byte_size,
                    },
                )
                .await?;
                Ok(Response::ArtifactStored {
                    artifact_id: descriptor.artifact_id,
                    content_hash: descriptor.content_hash,
                    byte_size: descriptor.byte_size,
                })
            }
            Command::AbortArtifactUpload { upload_id } => {
                let artifacts = self.artifacts.clone();
                let requested_upload_id = upload_id.clone();
                tokio::task::spawn_blocking(move || artifacts.abort_upload(&requested_upload_id))
                    .await
                    .map_err(|error| internal_error(error.to_string()))?
                    .map_err(artifact_error)?;
                Ok(Response::ArtifactUploadAborted { upload_id })
            }
            Command::ReadArtifact {
                artifact_id,
                cursor,
                limit,
            } => {
                let artifacts = self.artifacts.clone();
                let requested_artifact_id = artifact_id.clone();
                let (descriptor, bytes, next_cursor) = tokio::task::spawn_blocking(move || {
                    let descriptor = artifacts.descriptor(&requested_artifact_id)?;
                    let (bytes, next_cursor) = artifacts.read_range(
                        &requested_artifact_id,
                        cursor,
                        usize::try_from(limit).unwrap_or(usize::MAX),
                    )?;
                    Ok::<_, crate::artifact::ArtifactError>((descriptor, bytes, next_cursor))
                })
                .await
                .map_err(|error| internal_error(error.to_string()))?
                .map_err(|error| internal_error(error.to_string()))?;
                Ok(Response::ArtifactChunk {
                    artifact_id,
                    media_type: descriptor.media_type,
                    content_hash: descriptor.content_hash,
                    byte_size: descriptor.byte_size,
                    cursor,
                    bytes,
                    next_cursor,
                })
            }
            Command::ReadEvents(request) => Ok(Response::Events(self.read_events(&request).await?)),
            Command::QueryProjection(query) => {
                let mut snapshot = self.projections.query(query.clone()).await;
                if matches!(query, ProjectionQuery::Capacity) {
                    snapshot.value = serde_json::json!({
                        "providers": self.providers.capacity().await,
                        "overloads": snapshot.value,
                    });
                }
                Ok(Response::Projection(snapshot))
            }
            Command::Shutdown => {
                self.shutdown.cancel();
                Ok(Response::Ack)
            }
        }
    }

    async fn team_work_item_at_revision(
        &self,
        work_item_id: &TeamWorkItemId,
        expected_revision: u64,
    ) -> Result<TeamWorkItem, ProtocolError> {
        let work_item = self
            .projections
            .team_work_item(work_item_id)
            .await
            .ok_or_else(|| {
                ProtocolError::new(
                    ProtocolErrorCode::NotFound,
                    format!("team work item {work_item_id} does not exist"),
                )
            })?;
        if work_item.revision != expected_revision {
            return Err(revision_conflict(
                "team work item",
                expected_revision,
                work_item.revision,
            ));
        }
        Ok(work_item)
    }

    async fn validate_exercise_links(
        &self,
        exercise_id: &ExerciseId,
        operation_run_id: Option<&OperationRunId>,
        session_id: Option<&OperatorSessionId>,
    ) -> Result<(), ProtocolError> {
        if !self.projections.contains_exercise(exercise_id).await {
            return Err(ProtocolError::new(
                ProtocolErrorCode::NotFound,
                format!("exercise {exercise_id} does not exist"),
            ));
        }
        if let Some(operation_run_id) = operation_run_id {
            let run = self
                .projections
                .operation_run(operation_run_id)
                .await
                .ok_or_else(|| {
                    ProtocolError::new(
                        ProtocolErrorCode::NotFound,
                        format!("operation run {operation_run_id} does not exist"),
                    )
                })?;
            if &run.exercise_id != exercise_id {
                return Err(ProtocolError::new(
                    ProtocolErrorCode::Conflict,
                    "operation run belongs to a different exercise",
                ));
            }
        }
        if let Some(session_id) = session_id {
            let session = self
                .projections
                .operator_session(session_id)
                .await
                .ok_or_else(|| {
                    ProtocolError::new(
                        ProtocolErrorCode::NotFound,
                        format!("operator session {session_id} does not exist"),
                    )
                })?;
            if &session.exercise_id != exercise_id
                || session.operation_run_id.as_ref() != operation_run_id
            {
                return Err(ProtocolError::new(
                    ProtocolErrorCode::Conflict,
                    "operator session does not belong to the selected exercise/run",
                ));
            }
        }
        Ok(())
    }

    fn spawn_dispatch(self: &Arc<Self>, dispatch: ProviderDispatch, causation_id: Option<String>) {
        let core = self.clone();
        tokio::spawn(async move {
            let engagement_id = dispatch.engagement_id.clone();
            let schedule_engagement_id = engagement_id.clone();
            let task_id = dispatch.task.task_id.clone();
            let provider_id = dispatch.provider_id.clone();
            let request_id = dispatch.request_id.clone();
            let plan_revision = dispatch.plan_revision;
            let generation = dispatch.lease_epoch;
            if let Err(error) = core
                .emit(
                    Some(engagement_id.clone()),
                    causation_id.clone(),
                    generation,
                    Event::TaskStatus {
                        task_id: task_id.clone(),
                        status: TaskStatus::Running,
                        provider_id: Some(provider_id.clone()),
                    },
                )
                .await
            {
                tracing::error!(%error, "failed to persist running task event");
                let plans = core.plans.clone();
                let owner_epoch = core.owner_epoch.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    plans.release_unstarted(&request_id, &owner_epoch)
                })
                .await;
                return;
            }
            match core.providers.dispatch(dispatch).await {
                Ok(output) => {
                    let recorded = core
                        .record_provider_output(
                            engagement_id.clone(),
                            task_id.clone(),
                            generation,
                            causation_id.clone(),
                            output,
                        )
                        .await;
                    let status = if recorded.is_ok() {
                        TaskStatus::Completed
                    } else {
                        TaskStatus::Failed
                    };
                    if let Err(error) = recorded {
                        tracing::error!(%error, "failed to persist provider output");
                    }
                    let ledger_status = if status == TaskStatus::Completed {
                        DispatchLedgerStatus::Completed
                    } else {
                        DispatchLedgerStatus::Failed
                    };
                    if let Err(error) = core.update_dispatch_status(request_id, ledger_status).await
                    {
                        tracing::error!(%error, "failed to update dispatch ledger");
                    }
                    if let Err(error) = core
                        .emit(
                            Some(engagement_id),
                            causation_id,
                            generation,
                            Event::TaskStatus {
                                task_id,
                                status,
                                provider_id: Some(provider_id),
                            },
                        )
                        .await
                    {
                        tracing::error!(%error, "failed to persist completed task event");
                    }
                }
                Err(error) => {
                    let observation = EvidenceObservation {
                        finding: error.to_string(),
                        confidence: 1.0,
                        artifact_id: None,
                        attributes: serde_json::Map::from_iter([(
                            "protocol_error_code".to_owned(),
                            format!("{:?}", error.code).into(),
                        )]),
                    };
                    if let Err(ledger_error) = core
                        .update_dispatch_status(request_id, DispatchLedgerStatus::Failed)
                        .await
                    {
                        tracing::error!(%ledger_error, "failed to update failed dispatch ledger");
                    }
                    let _ = core
                        .emit(
                            Some(engagement_id.clone()),
                            causation_id.clone(),
                            generation,
                            Event::Observation {
                                task_id: task_id.clone(),
                                observation,
                            },
                        )
                        .await;
                    let _ = core
                        .emit(
                            Some(engagement_id),
                            causation_id,
                            generation,
                            Event::TaskStatus {
                                task_id,
                                status: TaskStatus::Failed,
                                provider_id: Some(provider_id),
                            },
                        )
                        .await;
                }
            }
            let plans = core.plans.clone();
            let engagement_for_plan = schedule_engagement_id.clone();
            match tokio::task::spawn_blocking(move || {
                plans.get(&engagement_for_plan, plan_revision)
            })
            .await
            {
                Ok(Ok(Some(plan))) => {
                    if let Err(error) = core.schedule_plan(plan, None).await {
                        tracing::error!(%error, "failed to advance durable task graph");
                    }
                }
                Ok(Ok(None)) => {
                    tracing::error!(engagement_id = %schedule_engagement_id, plan_revision, "completed dispatch lost its plan");
                }
                Ok(Err(error)) => {
                    tracing::error!(%error, "failed to reload plan after dispatch");
                }
                Err(error) => {
                    tracing::error!(%error, "plan reload task failed after dispatch");
                }
            }
        });
    }

    fn spawn_schedule_plan(self: &Arc<Self>, plan: TaskingPlan, causation_id: Option<String>) {
        if !self.auto_schedule_plans {
            return;
        }
        let core = self.clone();
        tokio::spawn(async move {
            if let Err(error) = core.schedule_plan(plan, causation_id).await {
                tracing::error!(%error, "durable plan scheduling failed");
            }
        });
    }

    async fn resume_all_plans(self: &Arc<Self>) -> Result<(), ProtocolError> {
        if !self.auto_schedule_plans {
            return Ok(());
        }
        let plans = self.plans.clone();
        let plans = tokio::task::spawn_blocking(move || plans.all_plans())
            .await
            .map_err(|error| internal_error(error.to_string()))??;
        for plan in plans {
            self.schedule_plan(plan, None).await?;
        }
        Ok(())
    }

    async fn schedule_plan(
        self: &Arc<Self>,
        plan: TaskingPlan,
        causation_id: Option<String>,
    ) -> Result<(), ProtocolError> {
        let statuses = self.projections.task_statuses(&plan.engagement_id).await;
        for task in &plan.tasks {
            if matches!(
                statuses.get(&task.task_id),
                Some(
                    TaskStatus::Dispatched
                        | TaskStatus::Running
                        | TaskStatus::Completed
                        | TaskStatus::Failed
                        | TaskStatus::Cancelled
                        | TaskStatus::Lost
                        | TaskStatus::Suspended
                )
            ) {
                continue;
            }
            let dependencies_complete = task
                .depends_on
                .iter()
                .all(|dependency| statuses.get(dependency) == Some(&TaskStatus::Completed));
            if !dependencies_complete {
                let dependency_failed = task.depends_on.iter().any(|dependency| {
                    matches!(
                        statuses.get(dependency),
                        Some(TaskStatus::Failed | TaskStatus::Cancelled | TaskStatus::Lost)
                    )
                });
                if dependency_failed {
                    self.emit(
                        Some(plan.engagement_id.clone()),
                        causation_id.clone(),
                        0,
                        Event::TaskStatus {
                            task_id: task.task_id.clone(),
                            status: TaskStatus::Suspended,
                            provider_id: None,
                        },
                    )
                    .await?;
                }
                continue;
            }
            let provider_id = match self
                .providers
                .resolve_requirement(
                    &task.capability,
                    task.capability.preferred_provider.as_ref(),
                    task.mode,
                )
                .await
            {
                Ok(provider_id) => provider_id,
                Err(error)
                    if matches!(
                        error.code,
                        ProtocolErrorCode::UnknownProvider
                            | ProtocolErrorCode::UnsupportedOperation
                            | ProtocolErrorCode::ServiceUnavailable
                    ) =>
                {
                    continue;
                }
                Err(error) => return Err(error),
            };
            let generation = self
                .providers
                .generation(&provider_id)
                .await
                .ok_or_else(|| {
                    ProtocolError::new(
                        ProtocolErrorCode::ServiceUnavailable,
                        format!("provider {provider_id} disappeared during scheduling"),
                    )
                })?;
            let dispatch = ProviderDispatch {
                request_id: scheduled_request_id(&plan.engagement_id, plan.revision, &task.task_id),
                engagement_id: plan.engagement_id.clone(),
                plan_revision: plan.revision,
                task: task.clone(),
                provider_id: provider_id.clone(),
                lease_epoch: generation,
            };
            let plans = self.plans.clone();
            let owner_epoch = self.owner_epoch.clone();
            let dispatch_for_claim = dispatch.clone();
            let claim = tokio::task::spawn_blocking(move || {
                plans.claim_dispatch(&dispatch_for_claim, &owner_epoch)
            })
            .await
            .map_err(|error| internal_error(error.to_string()))??;
            if matches!(claim, DispatchClaim::Existing(_)) {
                continue;
            }
            if let Err(error) = self
                .emit(
                    Some(plan.engagement_id.clone()),
                    causation_id.clone(),
                    generation,
                    Event::TaskStatus {
                        task_id: task.task_id.clone(),
                        status: TaskStatus::Dispatched,
                        provider_id: Some(provider_id),
                    },
                )
                .await
            {
                let plans = self.plans.clone();
                let request_id = dispatch.request_id.clone();
                let owner_epoch = self.owner_epoch.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    plans.release_unstarted(&request_id, &owner_epoch)
                })
                .await;
                return Err(error);
            }
            self.spawn_dispatch(dispatch, causation_id.clone());
        }
        Ok(())
    }

    async fn record_provider_output(
        &self,
        engagement_id: EngagementId,
        task_id: xai_grok_protocol::TaskId,
        generation: u64,
        causation_id: Option<String>,
        output: ProviderOutput,
    ) -> Result<(), ProtocolError> {
        for artifact in output.artifacts {
            let descriptor = self.store_provider_artifact(artifact).await?;
            self.emit(
                Some(engagement_id.clone()),
                causation_id.clone(),
                generation,
                Event::ArtifactAvailable {
                    artifact_id: descriptor.artifact_id,
                    task_id: Some(task_id.clone()),
                    media_type: descriptor.media_type,
                    byte_size: descriptor.byte_size,
                },
            )
            .await?;
        }
        let output_bytes = serde_json::to_vec(&output.output).map_err(internal_error)?;
        let store = self.artifacts.clone();
        let descriptor = tokio::task::spawn_blocking(move || {
            store.put("application/vnd.grok.provider-output+json", &output_bytes)
        })
        .await
        .map_err(|error| internal_error(error.to_string()))?
        .map_err(|error| internal_error(error.to_string()))?;
        self.emit(
            Some(engagement_id.clone()),
            causation_id.clone(),
            generation,
            Event::ArtifactAvailable {
                artifact_id: descriptor.artifact_id.clone(),
                task_id: Some(task_id.clone()),
                media_type: descriptor.media_type,
                byte_size: descriptor.byte_size,
            },
        )
        .await?;
        self.emit(
            Some(engagement_id.clone()),
            causation_id.clone(),
            generation,
            Event::ProviderOutput {
                task_id: task_id.clone(),
                artifact_id: descriptor.artifact_id,
            },
        )
        .await?;
        for observation in output.observations {
            self.emit(
                Some(engagement_id.clone()),
                causation_id.clone(),
                generation,
                Event::Observation {
                    task_id: task_id.clone(),
                    observation,
                },
            )
            .await?;
        }
        Ok(())
    }

    async fn store_provider_artifact(
        &self,
        artifact: ProviderArtifact,
    ) -> Result<ArtifactDescriptor, ProtocolError> {
        let store = self.artifacts.clone();
        tokio::task::spawn_blocking(move || match artifact.content {
            ProviderArtifactSource::Inline { bytes } => store.put(artifact.media_type, &bytes),
            ProviderArtifactSource::File { path } => store.put_file(artifact.media_type, &path),
        })
        .await
        .map_err(|error| internal_error(error.to_string()))?
        .map_err(artifact_error)
    }

    async fn update_dispatch_status(
        &self,
        request_id: xai_grok_protocol::RequestId,
        status: DispatchLedgerStatus,
    ) -> Result<(), ProtocolError> {
        let plans = self.plans.clone();
        tokio::task::spawn_blocking(move || plans.update_dispatch(&request_id, status))
            .await
            .map_err(|error| internal_error(error.to_string()))?
    }

    async fn emit(
        &self,
        engagement_id: Option<EngagementId>,
        causation_id: Option<String>,
        generation: u64,
        event: Event,
    ) -> Result<EventEnvelope, ProtocolError> {
        let _guard = self.event_order.lock().await;
        let sequence = self
            .next_sequence
            .load(Ordering::Acquire)
            .checked_add(1)
            .ok_or_else(|| internal_error("event sequence exhausted"))?;
        let envelope = EventEnvelope {
            protocol_version: PROTOCOL_VERSION,
            event_id: EventId::new(),
            engagement_id,
            sequence,
            causation_id,
            generation,
            observed_unix_ms: now_unix_ms(),
            event,
        };
        let journal = self.journal.clone();
        let persisted = envelope.clone();
        let persisted = tokio::task::spawn_blocking(move || journal.append(&persisted))
            .await
            .map_err(|error| internal_error(error.to_string()))
            .and_then(|result| result.map_err(|error| internal_error(error.to_string())));
        if let Err(error) = persisted {
            self.shutdown.cancel();
            return Err(error);
        }
        self.next_sequence.store(sequence, Ordering::Release);
        self.projections.apply(&envelope).await;
        let mut cache = self.event_cache.lock().await;
        cache.push_back(envelope.clone());
        while cache.len() > self.event_cache_capacity {
            cache.pop_front();
        }
        drop(cache);
        let _ = self.events.send(envelope.clone());
        Ok(envelope)
    }

    async fn read_events(&self, request: &EventReadRequest) -> Result<EventBatch, ProtocolError> {
        request.validate()?;
        let mut live_events = self.events.subscribe();
        let batch = self.read_event_page(request).await?;
        if request.wait_ms == 0 || !batch.events.is_empty() || !batch.caught_up {
            return Ok(batch);
        }

        let deadline = tokio::time::Instant::now() + Duration::from_millis(request.wait_ms.into());
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if !remaining.is_zero() {
            // Even an unrelated durable event advances a filtered cursor.
            // Read a fresh indexed page after the first wakeup so a quiet
            // engagement does not repeatedly scan or wait behind global
            // traffic it has already observed.
            let _ = tokio::time::timeout(remaining, live_events.recv()).await;
        }
        self.read_event_page(request).await
    }

    async fn read_event_page(
        &self,
        request: &EventReadRequest,
    ) -> Result<EventBatch, ProtocolError> {
        let high_watermark = self.next_sequence.load(Ordering::Acquire);
        if request.after_sequence > high_watermark {
            return Err(ProtocolError::new(
                ProtocolErrorCode::Conflict,
                format!(
                    "event cursor {} is ahead of durable high watermark {high_watermark}",
                    request.after_sequence
                ),
            ));
        }

        let maximum_events = request.maximum_events as usize;
        let cached = {
            let cache = self.event_cache.lock().await;
            let covers_cursor = cache
                .front()
                .is_none_or(|first| request.after_sequence >= first.sequence.saturating_sub(1));
            let reaches_high_watermark = cache
                .back()
                .is_none_or(|last| last.sequence == high_watermark);
            (covers_cursor && reaches_high_watermark).then(|| {
                let mut events = Vec::with_capacity(maximum_events);
                let mut scanned_through = request.after_sequence;
                for event in cache
                    .iter()
                    .filter(|event| event.sequence > request.after_sequence)
                {
                    scanned_through = event.sequence;
                    if request
                        .engagement_id
                        .as_ref()
                        .is_none_or(|expected| event.engagement_id.as_ref() == Some(expected))
                    {
                        events.push(event.clone());
                        if events.len() == maximum_events {
                            break;
                        }
                    }
                }
                (events, scanned_through)
            })
        };

        let (events, next_sequence) = if let Some(cached) = cached {
            cached
        } else {
            let journal = self.journal.clone();
            let after_sequence = request.after_sequence;
            let engagement_id = request.engagement_id.clone();
            tokio::task::spawn_blocking(move || {
                journal.read_after(after_sequence, maximum_events, engagement_id.as_ref())
            })
            .await
            .map_err(|error| internal_error(error.to_string()))?
            .map_err(|error| internal_error(error.to_string()))?
        };
        Ok(EventBatch {
            events,
            high_watermark,
            next_sequence,
            caught_up: next_sequence >= high_watermark,
        })
    }
}

/// Cloneable bounded command handle used by the daemon transport and tests.
#[derive(Clone)]
pub struct ControlPlaneHandle {
    command_tx: mpsc::Sender<QueuedCommand>,
    core: Arc<ControlPlaneCore>,
    command_capacity: usize,
}

impl ControlPlaneHandle {
    pub async fn submit(
        &self,
        envelope: CommandEnvelope,
    ) -> Result<ResponseEnvelope, ProtocolError> {
        if let Command::ReadEvents(request) = &envelope.command {
            envelope.validate(now_unix_ms())?;
            return Ok(ResponseEnvelope {
                protocol_version: PROTOCOL_VERSION,
                command_id: envelope.command_id,
                response: self.core.read_events(request).await.map(Response::Events),
            });
        }
        let (respond_to, response) = oneshot::channel();
        self.command_tx
            .try_send(QueuedCommand {
                envelope,
                respond_to,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => ProtocolError::new(
                    ProtocolErrorCode::Overloaded,
                    format!(
                        "control-plane command queue is full (capacity {})",
                        self.command_capacity
                    ),
                )
                .retryable(),
                mpsc::error::TrySendError::Closed(_) => ProtocolError::new(
                    ProtocolErrorCode::ServiceUnavailable,
                    "control-plane command queue is closed",
                ),
            })?;
        response.await.map_err(|_| {
            ProtocolError::new(
                ProtocolErrorCode::ServiceUnavailable,
                "control-plane command actor stopped",
            )
        })
    }

    pub async fn register_provider(
        &self,
        provider: Arc<dyn ExecutionProvider>,
    ) -> Result<(u64, String), ProtocolError> {
        let manifest = provider.manifest();
        let provider_id = manifest.provider_id.clone();
        let worker_generation = provider.worker_generation().await;
        let (registry_generation, manifest_hash) = self.core.providers.register(provider).await?;
        let mut service = match self
            .core
            .services
            .register(manifest, worker_generation)
            .await
        {
            Ok(service) => service,
            Err(error) => {
                self.core
                    .providers
                    .unregister(&provider_id, registry_generation)
                    .await;
                return Err(error);
            }
        };
        service = self
            .core
            .services
            .heartbeat(
                &service.provider_id,
                service.generation,
                ServiceHealth::Ready,
            )
            .await?;
        self.core
            .emit(
                None,
                None,
                service.generation,
                Event::ProviderState {
                    provider_id: service.provider_id,
                    service_id: service.service_id,
                    generation: service.generation,
                    health: ServiceHealth::Ready,
                },
            )
            .await?;
        self.core.resume_all_plans().await?;
        Ok((registry_generation, manifest_hash))
    }

    pub fn subscribe(&self) -> broadcast::Receiver<EventEnvelope> {
        self.core.events.subscribe()
    }

    pub fn shutdown_token(&self) -> CancellationToken {
        self.core.shutdown.clone()
    }

    pub fn providers(&self) -> Arc<ProviderRegistry> {
        self.core.providers.clone()
    }
}

pub struct ControlPlane {
    handle: ControlPlaneHandle,
    actor: tokio::task::JoinHandle<()>,
    maintenance: tokio::task::JoinHandle<()>,
}

impl ControlPlane {
    pub async fn open(config: ControlPlaneConfig) -> Result<Self, ControlPlaneError> {
        let maintenance_interval =
            std::time::Duration::from_millis((config.heartbeat_timeout_ms / 2).clamp(100, 5_000));
        std::fs::create_dir_all(&config.state_directory)?;
        let journal = Arc::new(EventJournal::open(
            config.state_directory.join("events.bin"),
        )?);
        let replay = journal.replay()?;
        let projections = Arc::new(ProjectionStore::new());
        projections.replay(replay.iter().cloned()).await;
        let next_sequence = replay.last().map_or(0, |event| event.sequence);
        let engagement =
            EngagementCoordinator::shared(config.state_directory.join("engagements.sqlite3"))
                .await?;
        let mut artifact_config =
            ArtifactStoreConfig::new(config.state_directory.join("artifacts"));
        artifact_config.maximum_artifact_bytes = config.maximum_artifact_bytes;
        artifact_config.maximum_store_bytes = config.maximum_artifact_store_bytes;
        let artifacts = Arc::new(ArtifactStore::open(artifact_config)?);
        let plans = Arc::new(PlanStore::open(config.state_directory.join("plans"))?);
        let (events, _) = broadcast::channel(config.event_capacity.max(1));
        let core = Arc::new(ControlPlaneCore {
            engagement,
            providers: Arc::new(ProviderRegistry::new(config.provider_registry)),
            services: Arc::new(ServiceSupervisor::new(config.heartbeat_timeout_ms)),
            artifacts,
            plans,
            projections,
            journal,
            event_order: Mutex::new(()),
            next_sequence: AtomicU64::new(next_sequence),
            events,
            event_cache: Mutex::new(
                replay[replay.len().saturating_sub(config.event_capacity.max(1))..]
                    .iter()
                    .cloned()
                    .collect(),
            ),
            event_cache_capacity: config.event_capacity.max(1),
            responses: Mutex::new(ResponseCache::new(config.response_cache_capacity)),
            owner_epoch: uuid::Uuid::new_v4().simple().to_string(),
            auto_schedule_plans: config.auto_schedule_plans,
            shutdown: CancellationToken::new(),
        });
        let (command_tx, mut command_rx) =
            mpsc::channel::<QueuedCommand>(config.command_capacity.max(1));
        let actor_core = core.clone();
        let actor = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = actor_core.shutdown.cancelled() => break,
                    command = command_rx.recv() => {
                        let Some(command) = command else { break };
                        let response = actor_core.process(command.envelope).await;
                        let _ = command.respond_to.send(response);
                    }
                }
            }
        });
        let maintenance_core = core.clone();
        let maintenance = tokio::spawn(async move {
            let mut interval = tokio::time::interval(maintenance_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = maintenance_core.shutdown.cancelled() => break,
                    _ = interval.tick() => {
                        for capacity in maintenance_core.providers.capacity().await {
                            let health = tokio::time::timeout(
                                std::time::Duration::from_secs(1),
                                maintenance_core.providers.health(&capacity.provider_id),
                            )
                            .await
                            .ok()
                            .and_then(Result::ok)
                            .unwrap_or(ServiceHealth::Degraded);
                            let previous = maintenance_core
                                .services
                                .get(&capacity.provider_id)
                                .await;
                            if let Ok(record) = maintenance_core
                                .services
                                .observe_worker_generation(
                                    &capacity.provider_id,
                                    capacity.worker_generation,
                                    health,
                                )
                                .await
                                && previous.as_ref().is_some_and(|previous| {
                                    previous.health != record.health
                                        || previous.generation != record.generation
                                        || previous.service_id != record.service_id
                                })
                                && let Err(error) = maintenance_core
                                    .emit(
                                        None,
                                        None,
                                        record.generation,
                                        Event::ProviderState {
                                            provider_id: record.provider_id,
                                            service_id: record.service_id,
                                            generation: record.generation,
                                            health: record.health,
                                        },
                                    )
                                    .await
                            {
                                tracing::error!(%error, "failed to persist local provider health");
                            }
                        }
                        for record in maintenance_core.services.sweep_stale().await {
                            if let Err(error) = maintenance_core
                                .emit(
                                    None,
                                    None,
                                    record.generation,
                                    Event::ProviderState {
                                        provider_id: record.provider_id,
                                        service_id: record.service_id,
                                        generation: record.generation,
                                        health: ServiceHealth::Failed,
                                    },
                                )
                                .await
                            {
                                tracing::error!(%error, "failed to persist stale provider state");
                            }
                        }
                    }
                }
            }
        });
        Ok(Self {
            handle: ControlPlaneHandle {
                command_tx,
                core,
                command_capacity: config.command_capacity.max(1),
            },
            actor,
            maintenance,
        })
    }

    pub fn handle(&self) -> ControlPlaneHandle {
        self.handle.clone()
    }

    pub async fn wait(self) {
        let _ = self.actor.await;
        let _ = self.maintenance.await;
    }
}

fn engagement_error(error: xai_grok_engagement::EngagementError) -> ProtocolError {
    match error {
        error @ (xai_grok_engagement::EngagementError::AdmissionOverloaded { .. }
        | xai_grok_engagement::EngagementError::WriterOverloaded { .. }) => {
            ProtocolError::new(ProtocolErrorCode::Overloaded, error.to_string()).retryable()
        }
        error @ xai_grok_engagement::EngagementError::NotFound(_) => {
            ProtocolError::new(ProtocolErrorCode::NotFound, error.to_string())
        }
        error @ xai_grok_engagement::EngagementError::ControlCommandConflict(_) => {
            ProtocolError::new(ProtocolErrorCode::Conflict, error.to_string())
        }
        error => internal_error(error),
    }
}

fn command_requires_durable_response(command: &Command) -> bool {
    !matches!(
        command,
        Command::Hello(_)
            | Command::ProviderHeartbeat { .. }
            | Command::InspectArtifactUpload { .. }
            | Command::ReadArtifact { .. }
            | Command::ReadEvents(_)
            | Command::QueryProjection(_)
    )
}

fn internal_error(error: impl std::fmt::Display) -> ProtocolError {
    ProtocolError::new(ProtocolErrorCode::Internal, error.to_string())
}

fn artifact_error(error: crate::artifact::ArtifactError) -> ProtocolError {
    use crate::artifact::ArtifactError;
    match error {
        ArtifactError::NotFound(_) | ArtifactError::UploadNotFound(_) => {
            ProtocolError::new(ProtocolErrorCode::NotFound, error.to_string())
        }
        ArtifactError::StoreFull { .. } => {
            ProtocolError::new(ProtocolErrorCode::Overloaded, error.to_string()).retryable()
        }
        ArtifactError::OffsetMismatch { .. } | ArtifactError::UploadFinalizing => {
            ProtocolError::new(ProtocolErrorCode::Conflict, error.to_string())
        }
        ArtifactError::Io(_) => internal_error(error),
        _ => ProtocolError::new(ProtocolErrorCode::InvalidEnvelope, error.to_string()),
    }
}

fn revision_conflict(resource: &str, expected: u64, actual: u64) -> ProtocolError {
    ProtocolError::new(
        ProtocolErrorCode::Conflict,
        format!("{resource} revision conflict: expected {expected}, current {actual}"),
    )
}

fn scheduled_request_id(
    engagement_id: &EngagementId,
    revision: u32,
    task_id: &xai_grok_protocol::TaskId,
) -> xai_grok_protocol::RequestId {
    let mut identity = blake3::Hasher::new();
    identity.update(engagement_id.as_str().as_bytes());
    identity.update(&revision.to_le_bytes());
    identity.update(task_id.as_str().as_bytes());
    xai_grok_protocol::RequestId::from_string(format!("req_{}", identity.finalize().to_hex()))
}

fn scheduled_turn_task_id(prompt_id: &str) -> TaskId {
    TaskId::from_string(format!(
        "turn_{}",
        blake3::hash(prompt_id.as_bytes()).to_hex()
    ))
}

fn canonical_json_bytes<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, ProtocolError> {
    let mut value = serde_json::to_value(value).map_err(internal_error)?;
    canonicalize_json(&mut value);
    serde_json::to_vec(&value).map_err(internal_error)
}

fn canonicalize_json(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                canonicalize_json(value);
            }
        }
        serde_json::Value::Object(object) => {
            let old = std::mem::take(object);
            let mut entries = old.into_iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            for (key, mut value) in entries {
                canonicalize_json(&mut value);
                object.insert(key, value);
            }
        }
        _ => {}
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    use xai_grok_protocol::{
        ArtifactContract, CancellationSemantics, CapabilityManifest, CapabilityRequirement,
        ChannelId, ClaimTeamResource, ClientId, Command, CommandEnvelope, CommandId,
        ConcurrencyProfile, CreateExercise, CreateFinding, CreateOperationRun,
        CreateOperatorSession, CreatePlaybook, CreateTeamWorkItem, ExecutionMode, ExecutionTask,
        FindingSeverity, FindingStatus, IngressEnvelope, IngressSource, OperationDescriptor,
        OperatorCatalog, PlaybookStep, PostTeamMessage, ProjectionQuery, ProviderKind,
        RecordExerciseEvidence, RecoverySemantics, RequestId, ScopeTarget, SetTeamPresence,
        TargetId, TargetKind, TaskGraphProjection, TaskId, TeamClient, TeamId, TeamPresenceState,
        TeamWorkItemStatus, VersionRange, WorkspaceId,
    };

    use super::*;
    use crate::provider::FunctionProvider;

    struct GenerationProvider {
        manifest: CapabilityManifest,
        worker_generation: Arc<AtomicU64>,
    }

    #[async_trait::async_trait]
    impl ExecutionProvider for GenerationProvider {
        fn manifest(&self) -> CapabilityManifest {
            self.manifest.clone()
        }

        async fn worker_generation(&self) -> u64 {
            self.worker_generation.load(Ordering::Acquire)
        }

        async fn execute(
            &self,
            _dispatch: ProviderDispatch,
        ) -> Result<ProviderOutput, ProtocolError> {
            Ok(ProviderOutput::default())
        }

        async fn cancel(&self, _request_id: &RequestId) -> Result<(), ProtocolError> {
            Ok(())
        }
    }

    fn envelope(command: Command) -> CommandEnvelope {
        CommandEnvelope {
            protocol_version: PROTOCOL_VERSION,
            command_id: CommandId::new(),
            causation_id: None,
            deadline_unix_ms: now_unix_ms() + 5_000,
            command,
        }
    }

    #[tokio::test]
    async fn chunked_artifact_upload_resumes_across_control_plane_restart() {
        let directory = tempfile::tempdir().unwrap();
        let state_path = directory.path().to_path_buf();
        let upload_id = {
            let control_plane = ControlPlane::open(ControlPlaneConfig::new(&state_path))
                .await
                .unwrap();
            let handle = control_plane.handle();
            let response = handle
                .submit(envelope(Command::BeginArtifactUpload {
                    media_type: "text/plain".to_owned(),
                    expected_bytes: 6,
                    expected_content_hash: Some(blake3::hash(b"abcdef").to_hex().to_string()),
                }))
                .await
                .unwrap()
                .response
                .unwrap();
            let Response::ArtifactUploadStarted { upload_id, .. } = response else {
                panic!("expected upload lease")
            };
            let response = handle
                .submit(envelope(Command::UploadArtifactChunk {
                    upload_id: upload_id.clone(),
                    offset: 0,
                    bytes: b"abc".to_vec(),
                }))
                .await
                .unwrap()
                .response
                .unwrap();
            assert!(matches!(
                response,
                Response::ArtifactUploadProgress { next_offset: 3, .. }
            ));
            handle.shutdown_token().cancel();
            control_plane.wait().await;
            upload_id
        };

        let control_plane = ControlPlane::open(ControlPlaneConfig::new(&state_path))
            .await
            .unwrap();
        let handle = control_plane.handle();
        let state = handle
            .submit(envelope(Command::InspectArtifactUpload {
                upload_id: upload_id.clone(),
            }))
            .await
            .unwrap()
            .response
            .unwrap();
        assert!(matches!(
            state,
            Response::ArtifactUploadState { next_offset: 3, .. }
        ));
        handle
            .submit(envelope(Command::UploadArtifactChunk {
                upload_id: upload_id.clone(),
                offset: 3,
                bytes: b"def".to_vec(),
            }))
            .await
            .unwrap()
            .response
            .unwrap();
        let stored = handle
            .submit(envelope(Command::CommitArtifactUpload { upload_id }))
            .await
            .unwrap()
            .response
            .unwrap();
        let Response::ArtifactStored {
            artifact_id,
            byte_size: 6,
            ..
        } = stored
        else {
            panic!("expected committed artifact")
        };
        let read = handle
            .submit(envelope(Command::ReadArtifact {
                artifact_id,
                cursor: 0,
                limit: 6,
            }))
            .await
            .unwrap()
            .response
            .unwrap();
        assert!(matches!(
            read,
            Response::ArtifactChunk { bytes, next_cursor: None, .. } if bytes == b"abcdef"
        ));
        handle.shutdown_token().cancel();
        control_plane.wait().await;
    }

    #[tokio::test]
    async fn exercise_run_and_session_survive_event_replay() {
        let directory = tempfile::tempdir().unwrap();
        let state_path = directory.path().to_path_buf();
        let (exercise_id, run_id, session_id) = {
            let control_plane = ControlPlane::open(ControlPlaneConfig::new(&state_path))
                .await
                .unwrap();
            let handle = control_plane.handle();
            let created = handle
                .submit(envelope(Command::CreateExercise(CreateExercise {
                    workspace_id: WorkspaceId::from_string("workspace-a"),
                    name: "Internal assessment".to_owned(),
                    objective: "Validate internal segmentation".to_owned(),
                    scope: Vec::new(),
                })))
                .await
                .unwrap()
                .response
                .unwrap();
            let Response::ExerciseCreated { exercise } = created else {
                panic!("expected created exercise");
            };
            let created = handle
                .submit(envelope(Command::CreateOperationRun(CreateOperationRun {
                    exercise_id: exercise.exercise_id.clone(),
                    name: "Discovery wave".to_owned(),
                    objective: "Inventory reachable services".to_owned(),
                    playbook_id: None,
                })))
                .await
                .unwrap()
                .response
                .unwrap();
            let Response::OperationRunCreated { operation_run } = created else {
                panic!("expected created operation run");
            };
            let created = handle
                .submit(envelope(Command::CreateOperatorSession(
                    CreateOperatorSession {
                        exercise_id: exercise.exercise_id.clone(),
                        operation_run_id: Some(operation_run.operation_run_id.clone()),
                        name: "Primary operator".to_owned(),
                        purpose: "Execute discovery tasks".to_owned(),
                    },
                )))
                .await
                .unwrap()
                .response
                .unwrap();
            let Response::OperatorSessionCreated { session } = created else {
                panic!("expected created operator session");
            };
            handle.shutdown_token().cancel();
            control_plane.wait().await;
            (
                exercise.exercise_id,
                operation_run.operation_run_id,
                session.session_id,
            )
        };

        let reopened = ControlPlane::open(ControlPlaneConfig::new(&state_path))
            .await
            .unwrap();
        let handle = reopened.handle();
        let response = handle
            .submit(envelope(Command::QueryProjection(
                ProjectionQuery::OperatorCatalog {
                    workspace_id: Some(WorkspaceId::from_string("workspace-a")),
                },
            )))
            .await
            .unwrap()
            .response
            .unwrap();
        let Response::Projection(snapshot) = response else {
            panic!("expected operator catalog projection");
        };
        let catalog: OperatorCatalog = serde_json::from_value(snapshot.value).unwrap();
        assert_eq!(catalog.exercises[0].exercise_id, exercise_id);
        assert_eq!(catalog.operation_runs[0].operation_run_id, run_id);
        assert_eq!(catalog.sessions[0].session_id, session_id);
        handle.shutdown_token().cancel();
        reopened.wait().await;
    }

    #[tokio::test]
    async fn mutating_command_response_is_replayed_after_restart_and_bound_to_content() {
        let directory = tempfile::tempdir().unwrap();
        let state_path = directory.path().to_path_buf();
        let command_id = CommandId::from_string("stable-create-exercise");
        let command = Command::CreateExercise(CreateExercise {
            workspace_id: WorkspaceId::from_string("workspace-a"),
            name: "Restart-safe assessment".to_owned(),
            objective: "Prove durable command identity".to_owned(),
            scope: Vec::new(),
        });
        let first_response = {
            let control_plane = ControlPlane::open(ControlPlaneConfig::new(&state_path))
                .await
                .unwrap();
            let handle = control_plane.handle();
            let response = handle
                .submit(CommandEnvelope {
                    protocol_version: PROTOCOL_VERSION,
                    command_id: command_id.clone(),
                    causation_id: None,
                    deadline_unix_ms: now_unix_ms() + 5_000,
                    command: command.clone(),
                })
                .await
                .unwrap();
            assert!(matches!(
                response.response,
                Ok(Response::ExerciseCreated { .. })
            ));
            handle.shutdown_token().cancel();
            control_plane.wait().await;
            response
        };

        let reopened = ControlPlane::open(ControlPlaneConfig::new(&state_path))
            .await
            .unwrap();
        let handle = reopened.handle();
        let replay = handle
            .submit(CommandEnvelope {
                protocol_version: PROTOCOL_VERSION,
                command_id: command_id.clone(),
                causation_id: None,
                deadline_unix_ms: now_unix_ms() + 5_000,
                command,
            })
            .await
            .unwrap();
        assert_eq!(replay, first_response);

        let conflict = handle
            .submit(CommandEnvelope {
                protocol_version: PROTOCOL_VERSION,
                command_id,
                causation_id: None,
                deadline_unix_ms: now_unix_ms() + 5_000,
                command: Command::CreateExercise(CreateExercise {
                    workspace_id: WorkspaceId::from_string("workspace-a"),
                    name: "Different content".to_owned(),
                    objective: "Must not execute".to_owned(),
                    scope: Vec::new(),
                }),
            })
            .await
            .unwrap();
        assert!(matches!(
            conflict.response,
            Err(ProtocolError {
                code: ProtocolErrorCode::Conflict,
                ..
            })
        ));

        let catalog = handle
            .submit(envelope(Command::QueryProjection(
                ProjectionQuery::OperatorCatalog {
                    workspace_id: Some(WorkspaceId::from_string("workspace-a")),
                },
            )))
            .await
            .unwrap()
            .response
            .unwrap();
        let Response::Projection(snapshot) = catalog else {
            panic!("expected operator catalog");
        };
        let catalog: OperatorCatalog = serde_json::from_value(snapshot.value).unwrap();
        assert_eq!(catalog.exercises.len(), 1);

        handle.shutdown_token().cancel();
        reopened.wait().await;
    }

    #[tokio::test]
    async fn indeterminate_pre_restart_command_is_never_executed_twice() {
        let directory = tempfile::tempdir().unwrap();
        let state_path = directory.path().to_path_buf();
        std::fs::create_dir_all(&state_path).unwrap();
        let command_id = CommandId::from_string("interrupted-create-exercise");
        let command = Command::CreateExercise(CreateExercise {
            workspace_id: WorkspaceId::from_string("workspace-a"),
            name: "Must remain absent".to_owned(),
            objective: "Simulated interrupted command".to_owned(),
            scope: Vec::new(),
        });
        let command_hash = blake3::hash(&canonical_json_bytes(&command).unwrap())
            .to_hex()
            .to_string();
        {
            let mut store =
                xai_grok_engagement::EngagementStore::open(&state_path.join("engagements.sqlite3"))
                    .unwrap();
            assert_eq!(
                store
                    .claim_control_command(command_id.as_str(), &command_hash)
                    .unwrap(),
                ControlCommandClaim::New
            );
        }

        let control_plane = ControlPlane::open(ControlPlaneConfig::new(&state_path))
            .await
            .unwrap();
        let handle = control_plane.handle();
        let response = handle
            .submit(CommandEnvelope {
                protocol_version: PROTOCOL_VERSION,
                command_id,
                causation_id: None,
                deadline_unix_ms: now_unix_ms() + 5_000,
                command,
            })
            .await
            .unwrap();
        assert!(matches!(
            response.response,
            Err(ProtocolError {
                code: ProtocolErrorCode::ServiceUnavailable,
                retryable: false,
                ..
            })
        ));
        let catalog = handle
            .submit(envelope(Command::QueryProjection(
                ProjectionQuery::OperatorCatalog {
                    workspace_id: Some(WorkspaceId::from_string("workspace-a")),
                },
            )))
            .await
            .unwrap()
            .response
            .unwrap();
        let Response::Projection(snapshot) = catalog else {
            panic!("expected operator catalog");
        };
        let catalog: OperatorCatalog = serde_json::from_value(snapshot.value).unwrap();
        assert!(catalog.exercises.is_empty());

        handle.shutdown_token().cancel();
        control_plane.wait().await;
    }

    #[tokio::test]
    async fn playbook_evidence_and_findings_are_durable_real_records() {
        let directory = tempfile::tempdir().unwrap();
        let state_path = directory.path().to_path_buf();
        let control_plane = ControlPlane::open(ControlPlaneConfig::new(&state_path))
            .await
            .unwrap();
        let handle = control_plane.handle();
        let target_id = TargetId::from_string("target-web");
        let Response::ExerciseCreated { exercise } = handle
            .submit(envelope(Command::CreateExercise(CreateExercise {
                workspace_id: WorkspaceId::from_string("workspace-a"),
                name: "Web assessment".to_owned(),
                objective: "Validate exposed services".to_owned(),
                scope: vec![ScopeTarget {
                    target_id: target_id.clone(),
                    kind: TargetKind::Host,
                    selector: "10.10.4.8".to_owned(),
                    excluded: false,
                    labels: Default::default(),
                }],
            })))
            .await
            .unwrap()
            .response
            .unwrap()
        else {
            panic!("expected exercise");
        };
        let Response::PlaybookCreated { playbook } = handle
            .submit(envelope(Command::CreatePlaybook(CreatePlaybook {
                workspace_id: exercise.workspace_id.clone(),
                name: "Web discovery".to_owned(),
                description: "Collect and normalize exposed services".to_owned(),
                steps: vec![PlaybookStep {
                    step_id: "scan".to_owned(),
                    name: "Scan web ports".to_owned(),
                    capability: "native.nmap".to_owned(),
                    depends_on: Vec::new(),
                    completion_tests: vec!["scan completed".to_owned()],
                }],
            })))
            .await
            .unwrap()
            .response
            .unwrap()
        else {
            panic!("expected playbook");
        };
        let Response::OperationRunCreated { operation_run } = handle
            .submit(envelope(Command::CreateOperationRun(CreateOperationRun {
                exercise_id: exercise.exercise_id.clone(),
                name: "Discovery run".to_owned(),
                objective: "Run the web discovery playbook".to_owned(),
                playbook_id: Some(playbook.playbook_id.clone()),
            })))
            .await
            .unwrap()
            .response
            .unwrap()
        else {
            panic!("expected run");
        };
        let Response::OperatorSessionCreated { session } = handle
            .submit(envelope(Command::CreateOperatorSession(
                CreateOperatorSession {
                    exercise_id: exercise.exercise_id.clone(),
                    operation_run_id: Some(operation_run.operation_run_id.clone()),
                    name: "Operator one".to_owned(),
                    purpose: "Collect discovery evidence".to_owned(),
                },
            )))
            .await
            .unwrap()
            .response
            .unwrap()
        else {
            panic!("expected session");
        };
        let Response::ArtifactStored { artifact_id, .. } = handle
            .submit(envelope(Command::PutArtifact {
                media_type: "application/xml".to_owned(),
                bytes: br#"<host><port protocol="tcp" portid="443"><state state="open"/></port></host>"#
                    .to_vec(),
            }))
            .await
            .unwrap()
            .response
            .unwrap()
        else {
            panic!("expected artifact");
        };
        let Response::EvidenceRecorded { evidence } = handle
            .submit(envelope(Command::RecordEvidence(RecordExerciseEvidence {
                exercise_id: exercise.exercise_id.clone(),
                operation_run_id: Some(operation_run.operation_run_id.clone()),
                session_id: Some(session.session_id.clone()),
                task_id: None,
                finding: "TCP 443 is open".to_owned(),
                confidence: 1.0,
                artifact_id: Some(artifact_id.clone()),
                attributes: [("port".to_owned(), "443".to_owned())]
                    .into_iter()
                    .collect(),
            })))
            .await
            .unwrap()
            .response
            .unwrap()
        else {
            panic!("expected evidence");
        };
        let Response::FindingCreated { finding } = handle
            .submit(envelope(Command::CreateFinding(CreateFinding {
                exercise_id: exercise.exercise_id.clone(),
                operation_run_id: Some(operation_run.operation_run_id.clone()),
                title: "Exposed TLS service".to_owned(),
                summary: "A TLS listener is reachable on TCP 443.".to_owned(),
                severity: FindingSeverity::Moderate,
                evidence_ids: vec![evidence.evidence_id.clone()],
                target_ids: vec![target_id],
            })))
            .await
            .unwrap()
            .response
            .unwrap()
        else {
            panic!("expected finding");
        };
        let Response::FindingStatusSet { finding } = handle
            .submit(envelope(Command::SetFindingStatus {
                finding_id: finding.finding_id,
                status: FindingStatus::Confirmed,
            }))
            .await
            .unwrap()
            .response
            .unwrap()
        else {
            panic!("expected finding status update");
        };
        assert_eq!(finding.status, FindingStatus::Confirmed);
        handle.shutdown_token().cancel();
        control_plane.wait().await;

        let reopened = ControlPlane::open(ControlPlaneConfig::new(&state_path))
            .await
            .unwrap();
        let handle = reopened.handle();
        let Response::Projection(snapshot) = handle
            .submit(envelope(Command::QueryProjection(
                ProjectionQuery::ExerciseRecord {
                    exercise_id: exercise.exercise_id,
                },
            )))
            .await
            .unwrap()
            .response
            .unwrap()
        else {
            panic!("expected exercise record");
        };
        let record: xai_grok_protocol::ExerciseRecord =
            serde_json::from_value(snapshot.value).unwrap();
        assert_eq!(record.evidence.len(), 1);
        assert_eq!(record.evidence[0].artifact_id.as_ref(), Some(&artifact_id));
        assert_eq!(record.findings.len(), 1);
        assert_eq!(record.findings[0].status, FindingStatus::Confirmed);
        assert_eq!(
            record.operation_runs[0].playbook_id,
            Some(playbook.playbook_id)
        );
        handle.shutdown_token().cancel();
        reopened.wait().await;
    }

    #[tokio::test]
    async fn team_handoffs_messages_and_resource_leases_are_coordinated_and_durable() {
        let directory = tempfile::tempdir().unwrap();
        let state_path = directory.path().to_path_buf();
        let control_plane = ControlPlane::open(ControlPlaneConfig::new(&state_path))
            .await
            .unwrap();
        let handle = control_plane.handle();
        let team_id = TeamId::from_string("team-red");
        let alice = ClientId::from_string("alice");
        let bob = ClientId::from_string("bob");
        for (client_id, display_name) in [(&alice, "Alice"), (&bob, "Bob")] {
            let Response::TeamPresenceSet { .. } = handle
                .submit(envelope(Command::SetTeamPresence(SetTeamPresence {
                    client: TeamClient {
                        team_id: team_id.clone(),
                        client_id: client_id.clone(),
                        display_name: Some(display_name.to_owned()),
                    },
                    state: TeamPresenceState::Online,
                    workspace_id: None,
                    exercise_id: None,
                    operation_run_id: None,
                    session_id: None,
                })))
                .await
                .unwrap()
                .response
                .unwrap()
            else {
                panic!("expected presence response");
            };
        }
        let Response::TeamWorkItemCreated { work_item } = handle
            .submit(envelope(Command::CreateTeamWorkItem(CreateTeamWorkItem {
                team_id: team_id.clone(),
                exercise_id: None,
                operation_run_id: None,
                title: "Enumerate web surface".to_owned(),
                objective: "Collect HTTP endpoints and banners".to_owned(),
                assignee: Some(alice.clone()),
            })))
            .await
            .unwrap()
            .response
            .unwrap()
        else {
            panic!("expected work item");
        };
        let Response::TeamWorkItemUpdated { work_item } = handle
            .submit(envelope(Command::AssignTeamWorkItem {
                work_item_id: work_item.work_item_id.clone(),
                assignee: Some(bob.clone()),
                expected_revision: 1,
            }))
            .await
            .unwrap()
            .response
            .unwrap()
        else {
            panic!("expected work item handoff");
        };
        assert_eq!(work_item.assignee.as_ref(), Some(&bob));
        let stale = handle
            .submit(envelope(Command::SetTeamWorkItemStatus {
                work_item_id: work_item.work_item_id.clone(),
                status: TeamWorkItemStatus::InProgress,
                expected_revision: 1,
            }))
            .await
            .unwrap()
            .response
            .unwrap_err();
        assert_eq!(stale.code, ProtocolErrorCode::Conflict);
        let Response::TeamWorkItemUpdated { work_item: _ } = handle
            .submit(envelope(Command::SetTeamWorkItemStatus {
                work_item_id: work_item.work_item_id.clone(),
                status: TeamWorkItemStatus::InProgress,
                expected_revision: 2,
            }))
            .await
            .unwrap()
            .response
            .unwrap()
        else {
            panic!("expected work item status update");
        };
        let channel_id = ChannelId::from_string("channel-ops");
        let Response::TeamMessagePosted { message } = handle
            .submit(envelope(Command::PostTeamMessage(PostTeamMessage {
                team_id: team_id.clone(),
                channel_id: channel_id.clone(),
                sender: alice.clone(),
                body: "Handing web enumeration to Bob".to_owned(),
                reply_to: None,
            })))
            .await
            .unwrap()
            .response
            .unwrap()
        else {
            panic!("expected team message");
        };
        let Response::TeamMessagePosted { .. } = handle
            .submit(envelope(Command::PostTeamMessage(PostTeamMessage {
                team_id: team_id.clone(),
                channel_id,
                sender: bob.clone(),
                body: "Accepted; starting now".to_owned(),
                reply_to: Some(message.message_id),
            })))
            .await
            .unwrap()
            .response
            .unwrap()
        else {
            panic!("expected team reply");
        };
        let Response::TeamResourceClaimed { claim } = handle
            .submit(envelope(Command::ClaimTeamResource(ClaimTeamResource {
                team_id: team_id.clone(),
                owner: alice.clone(),
                resource_key: "target:10.10.4.8".to_owned(),
                lease_ms: 60_000,
            })))
            .await
            .unwrap()
            .response
            .unwrap()
        else {
            panic!("expected team claim");
        };
        let conflict = handle
            .submit(envelope(Command::ClaimTeamResource(ClaimTeamResource {
                team_id: team_id.clone(),
                owner: bob,
                resource_key: "target:10.10.4.8".to_owned(),
                lease_ms: 60_000,
            })))
            .await
            .unwrap()
            .response
            .unwrap_err();
        assert_eq!(conflict.code, ProtocolErrorCode::Conflict);
        let Response::TeamResourceReleased { claim } = handle
            .submit(envelope(Command::ReleaseTeamResource {
                claim_id: claim.claim_id,
                owner: alice,
                expected_revision: claim.revision,
            }))
            .await
            .unwrap()
            .response
            .unwrap()
        else {
            panic!("expected team resource release");
        };
        assert!(claim.released_unix_ms.is_some());
        handle.shutdown_token().cancel();
        control_plane.wait().await;

        let reopened = ControlPlane::open(ControlPlaneConfig::new(&state_path))
            .await
            .unwrap();
        let handle = reopened.handle();
        let Response::Projection(snapshot) = handle
            .submit(envelope(Command::QueryProjection(ProjectionQuery::Team {
                team_id,
            })))
            .await
            .unwrap()
            .response
            .unwrap()
        else {
            panic!("expected team projection");
        };
        let team: xai_grok_protocol::TeamProjection =
            serde_json::from_value(snapshot.value).unwrap();
        assert_eq!(team.presence.len(), 2);
        assert_eq!(team.work_items[0].revision, 3);
        assert_eq!(team.work_items[0].status, TeamWorkItemStatus::InProgress);
        assert_eq!(team.messages.len(), 2);
        assert!(team.resource_claims[0].released_unix_ms.is_some());
        handle.shutdown_token().cancel();
        reopened.wait().await;
    }

    #[tokio::test]
    async fn duplicate_ingress_is_idempotent() {
        let directory = tempfile::tempdir().unwrap();
        let control_plane = ControlPlane::open(ControlPlaneConfig::new(directory.path()))
            .await
            .unwrap();
        let command = envelope(Command::SubmitIngress(IngressEnvelope {
            command_id: CommandId::new(),
            source: IngressSource::Cli,
            source_event_id: "event-1".to_owned(),
            workspace_id: WorkspaceId::from_string("workspace"),
            exercise_id: None,
            operation_run_id: None,
            operator_session_id: None,
            session_id: "session".to_owned(),
            prompt_id: "prompt".to_owned(),
            request: "test".to_owned(),
            team: None,
            metadata: serde_json::Map::new(),
        }));
        let first = control_plane
            .handle()
            .submit(command.clone())
            .await
            .unwrap();
        let second = control_plane.handle().submit(command).await.unwrap();
        assert_eq!(first, second);
        control_plane.handle.shutdown_token().cancel();
        control_plane.wait().await;
    }

    #[tokio::test]
    async fn reconnecting_client_reads_durable_events_by_cursor() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = ControlPlaneConfig::new(directory.path());
        config.event_capacity = 1;
        config.auto_schedule_plans = false;
        let control_plane = ControlPlane::open(config).await.unwrap();
        let handle = control_plane.handle();
        for index in 0..3 {
            handle
                .submit(envelope(Command::SubmitIngress(IngressEnvelope {
                    command_id: CommandId::new(),
                    source: IngressSource::Cli,
                    source_event_id: format!("cursor-event-{index}"),
                    workspace_id: WorkspaceId::from_string("workspace"),
                    exercise_id: None,
                    operation_run_id: None,
                    operator_session_id: None,
                    session_id: format!("session-{index}"),
                    prompt_id: format!("prompt-{index}"),
                    request: "test".to_owned(),
                    team: None,
                    metadata: serde_json::Map::new(),
                })))
                .await
                .unwrap();
        }

        let first = handle
            .submit(envelope(Command::ReadEvents(EventReadRequest {
                after_sequence: 0,
                maximum_events: 2,
                wait_ms: 0,
                engagement_id: None,
            })))
            .await
            .unwrap()
            .response
            .unwrap();
        let Response::Events(first) = first else {
            panic!("expected event batch");
        };
        assert_eq!(first.events.len(), 2);
        assert!(!first.caught_up);

        let second = handle
            .submit(envelope(Command::ReadEvents(EventReadRequest {
                after_sequence: first.next_sequence,
                maximum_events: 2,
                wait_ms: 0,
                engagement_id: None,
            })))
            .await
            .unwrap()
            .response
            .unwrap();
        let Response::Events(second) = second else {
            panic!("expected event batch");
        };
        assert_eq!(second.events.len(), 1);
        assert!(second.caught_up);
        assert_eq!(second.high_watermark, second.next_sequence);

        handle.shutdown_token().cancel();
        control_plane.wait().await;
    }

    #[tokio::test]
    async fn event_long_poll_does_not_block_command_admission() {
        let directory = tempfile::tempdir().unwrap();
        let control_plane = ControlPlane::open(ControlPlaneConfig::new(directory.path()))
            .await
            .unwrap();
        let handle = control_plane.handle();
        let reader = {
            let handle = handle.clone();
            tokio::spawn(async move {
                handle
                    .submit(envelope(Command::ReadEvents(EventReadRequest {
                        after_sequence: 0,
                        maximum_events: 8,
                        wait_ms: 1_000,
                        engagement_id: None,
                    })))
                    .await
                    .unwrap()
            })
        };
        tokio::task::yield_now().await;
        handle
            .submit(envelope(Command::SubmitIngress(IngressEnvelope {
                command_id: CommandId::new(),
                source: IngressSource::Cli,
                source_event_id: "wake-reader".to_owned(),
                workspace_id: WorkspaceId::from_string("workspace"),
                exercise_id: None,
                operation_run_id: None,
                operator_session_id: None,
                session_id: "session".to_owned(),
                prompt_id: "prompt".to_owned(),
                request: "test".to_owned(),
                team: None,
                metadata: serde_json::Map::new(),
            })))
            .await
            .unwrap();
        let Response::Events(batch) = reader.await.unwrap().response.unwrap() else {
            panic!("expected event batch");
        };
        assert_eq!(batch.events.len(), 1);

        handle.shutdown_token().cancel();
        control_plane.wait().await;
    }

    #[tokio::test]
    async fn filtered_long_poll_advances_past_unrelated_events() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = ControlPlaneConfig::new(directory.path());
        config.auto_schedule_plans = false;
        let control_plane = ControlPlane::open(config).await.unwrap();
        let handle = control_plane.handle();
        let reader = {
            let handle = handle.clone();
            tokio::spawn(async move {
                handle
                    .submit(envelope(Command::ReadEvents(EventReadRequest {
                        after_sequence: 0,
                        maximum_events: 8,
                        wait_ms: 20,
                        engagement_id: Some(EngagementId::from_string("not-present")),
                    })))
                    .await
                    .unwrap()
            })
        };
        tokio::task::yield_now().await;
        handle
            .submit(envelope(Command::SubmitIngress(IngressEnvelope {
                command_id: CommandId::new(),
                source: IngressSource::Cli,
                source_event_id: "unrelated-event".to_owned(),
                workspace_id: WorkspaceId::from_string("workspace"),
                exercise_id: None,
                operation_run_id: None,
                operator_session_id: None,
                session_id: "session".to_owned(),
                prompt_id: "prompt".to_owned(),
                request: "test".to_owned(),
                team: None,
                metadata: serde_json::Map::new(),
            })))
            .await
            .unwrap();
        let Response::Events(batch) = reader.await.unwrap().response.unwrap() else {
            panic!("expected event batch");
        };
        assert!(batch.events.is_empty());
        assert_eq!(batch.next_sequence, 1);
        assert!(batch.caught_up);

        handle.shutdown_token().cancel();
        control_plane.wait().await;
    }

    fn provider_manifest() -> CapabilityManifest {
        CapabilityManifest {
            provider_id: "test-provider".into(),
            provider_version: "1".to_owned(),
            protocol: VersionRange::exact(PROTOCOL_VERSION),
            kind: ProviderKind::NativeExecution,
            features: BTreeSet::new(),
            operations: vec![OperationDescriptor {
                operation_id: "test.execute".into(),
                display_name: "Test execute".to_owned(),
                input_schema: serde_json::json!({"type": "object"}),
                output_schema: serde_json::json!({"type": "object"}),
                streaming: false,
                interactive: true,
                deferred: true,
            }],
            concurrency: ConcurrencyProfile {
                maximum_parallel: 1,
                queue_capacity: 4,
                exclusive_resource: None,
            },
            cancellation: CancellationSemantics::Cooperative,
            recovery: RecoverySemantics::Restartable,
            artifacts: ArtifactContract::Optional,
            platforms: BTreeSet::new(),
            metadata: serde_json::Map::new(),
        }
    }

    fn agent_provider_manifest() -> CapabilityManifest {
        let mut manifest = provider_manifest();
        manifest.provider_id = ProviderId::from_string("agent-runtime");
        manifest.kind = ProviderKind::ModelRuntime;
        manifest.features = ["session_continuity", "tool_loop"]
            .into_iter()
            .map(str::to_owned)
            .collect();
        manifest.operations[0].operation_id = AGENT_TURN_OPERATION.into();
        manifest.operations[0].display_name = "Agent turn".to_owned();
        manifest
    }

    #[tokio::test]
    async fn socket_command_cannot_publish_a_metadata_only_provider() {
        let directory = tempfile::tempdir().unwrap();
        let control_plane = ControlPlane::open(ControlPlaneConfig::new(directory.path()))
            .await
            .unwrap();
        let handle = control_plane.handle();
        let error = handle
            .submit(envelope(Command::RegisterProvider {
                manifest: provider_manifest(),
            }))
            .await
            .unwrap()
            .response
            .unwrap_err();
        assert_eq!(error.code, ProtocolErrorCode::UnsupportedOperation);
        assert!(handle.providers().manifests().await.is_empty());
        assert!(
            handle
                .submit(envelope(Command::QueryProjection(
                    ProjectionQuery::Providers,
                )))
                .await
                .unwrap()
                .response
                .is_ok()
        );

        handle.shutdown_token().cancel();
        control_plane.wait().await;
    }

    #[tokio::test]
    async fn capacity_projection_reports_live_dispatchable_provider_limits() {
        let directory = tempfile::tempdir().unwrap();
        let control_plane = ControlPlane::open(ControlPlaneConfig::new(directory.path()))
            .await
            .unwrap();
        let handle = control_plane.handle();
        handle
            .register_provider(Arc::new(FunctionProvider::new(
                provider_manifest(),
                |_| async { Ok(ProviderOutput::default()) },
                |_| async { Ok(()) },
            )))
            .await
            .unwrap();
        let response = handle
            .submit(envelope(Command::QueryProjection(
                ProjectionQuery::Capacity,
            )))
            .await
            .unwrap()
            .response
            .unwrap();
        let Response::Projection(snapshot) = response else {
            panic!("expected capacity projection");
        };
        assert_eq!(
            snapshot.value["providers"][0]["provider_id"],
            "test-provider"
        );
        assert_eq!(snapshot.value["providers"][0]["maximum_parallel"], 1);
        assert_eq!(snapshot.value["providers"][0]["available_permits"], 1);
        assert_eq!(snapshot.value["providers"][0]["queue_capacity"], 4);
        assert_eq!(snapshot.value["overloads"], serde_json::json!({}));

        handle.shutdown_token().cancel();
        control_plane.wait().await;
    }

    #[tokio::test]
    async fn maintenance_rotates_service_generation_after_owned_worker_restart() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = ControlPlaneConfig::new(directory.path());
        config.heartbeat_timeout_ms = 200;
        let control_plane = ControlPlane::open(config).await.unwrap();
        let handle = control_plane.handle();
        let worker_generation = Arc::new(AtomicU64::new(1));
        handle
            .register_provider(Arc::new(GenerationProvider {
                manifest: provider_manifest(),
                worker_generation: worker_generation.clone(),
            }))
            .await
            .unwrap();

        let first = handle
            .submit(envelope(Command::QueryProjection(
                ProjectionQuery::Providers,
            )))
            .await
            .unwrap()
            .response
            .unwrap();
        let Response::Projection(first) = first else {
            panic!("expected provider projection");
        };
        let first_generation = first.value[0]["generation"].as_u64().unwrap();
        let first_service_id = first.value[0]["service_id"].as_str().unwrap().to_owned();

        worker_generation.store(2, Ordering::Release);
        let updated = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let response = handle
                    .submit(envelope(Command::QueryProjection(
                        ProjectionQuery::Providers,
                    )))
                    .await
                    .unwrap()
                    .response
                    .unwrap();
                let Response::Projection(snapshot) = response else {
                    panic!("expected provider projection");
                };
                if snapshot.value[0]["generation"].as_u64() == Some(first_generation + 1) {
                    break snapshot;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("maintenance must observe the replacement worker");
        assert_ne!(
            updated.value[0]["service_id"].as_str().unwrap(),
            first_service_id
        );
        assert_eq!(handle.providers().capacity().await[0].worker_generation, 2);

        handle.shutdown_token().cancel();
        control_plane.wait().await;
    }

    async fn all_events(handle: &ControlPlaneHandle) -> Vec<EventEnvelope> {
        let response = handle
            .submit(envelope(Command::ReadEvents(EventReadRequest {
                after_sequence: 0,
                maximum_events: 512,
                wait_ms: 0,
                engagement_id: None,
            })))
            .await
            .unwrap()
            .response
            .unwrap();
        let Response::Events(batch) = response else {
            panic!("expected event batch");
        };
        batch.events
    }

    #[tokio::test]
    async fn accepted_ingress_executes_and_exposes_provider_output_artifact() {
        let directory = tempfile::tempdir().unwrap();
        let control_plane = ControlPlane::open(ControlPlaneConfig::new(directory.path()))
            .await
            .unwrap();
        let handle = control_plane.handle();
        handle
            .register_provider(Arc::new(FunctionProvider::new(
                agent_provider_manifest(),
                |dispatch| async move {
                    let request = dispatch.task.input["request"].as_str().unwrap();
                    Ok(ProviderOutput {
                        output: serde_json::json!({"text": format!("executed: {request}")}),
                        ..ProviderOutput::default()
                    })
                },
                |_| async { Ok(()) },
            )))
            .await
            .unwrap();

        let accepted = handle
            .submit(envelope(Command::SubmitIngress(IngressEnvelope {
                command_id: CommandId::new(),
                source: IngressSource::Gui,
                source_event_id: "real-ingress-event".to_owned(),
                workspace_id: WorkspaceId::from_string("workspace"),
                exercise_id: None,
                operation_run_id: None,
                operator_session_id: None,
                session_id: "operator-session".to_owned(),
                prompt_id: "real-prompt".to_owned(),
                request: "inspect the supplied evidence".to_owned(),
                team: None,
                metadata: serde_json::Map::new(),
            })))
            .await
            .unwrap()
            .response
            .unwrap();
        let Response::Accepted { engagement_id, .. } = accepted else {
            panic!("expected accepted ingress");
        };

        let (task_id, artifact_id) = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let events = all_events(&handle).await;
                let completed = events.iter().any(|event| {
                    event.engagement_id.as_ref() == Some(&engagement_id)
                        && matches!(
                            &event.event,
                            Event::TaskStatus {
                                status: TaskStatus::Completed,
                                ..
                            }
                        )
                });
                let output = events.iter().find_map(|event| match &event.event {
                    Event::ProviderOutput {
                        task_id,
                        artifact_id,
                    } if event.engagement_id.as_ref() == Some(&engagement_id) => {
                        Some((task_id.clone(), artifact_id.clone()))
                    }
                    _ => None,
                });
                if completed && let Some(output) = output {
                    break output;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(task_id, scheduled_turn_task_id("real-prompt"));

        let artifact = handle
            .submit(envelope(Command::ReadArtifact {
                artifact_id: artifact_id.clone(),
                cursor: 0,
                limit: 1024,
            }))
            .await
            .unwrap()
            .response
            .unwrap();
        let Response::ArtifactChunk {
            artifact_id: read_id,
            bytes,
            next_cursor,
            ..
        } = artifact
        else {
            panic!("expected provider output artifact");
        };
        assert_eq!(read_id, artifact_id);
        assert!(next_cursor.is_none());
        let output: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(output["text"], "executed: inspect the supplied evidence");

        handle.shutdown_token().cancel();
        control_plane.wait().await;
    }

    #[tokio::test]
    async fn direct_provider_file_artifact_is_streamed_into_the_daemon_store() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("native-output.spool");
        std::fs::write(&source, b"real-file-backed-provider-output").unwrap();
        let control_plane = ControlPlane::open(ControlPlaneConfig::new(directory.path()))
            .await
            .unwrap();
        let handle = control_plane.handle();
        handle
            .register_provider(Arc::new(FunctionProvider::new(
                provider_manifest(),
                {
                    let source = source.clone();
                    move |_| {
                        let source = source.clone();
                        async move {
                            Ok(ProviderOutput {
                                output: serde_json::json!({"complete": true}),
                                artifacts: vec![ProviderArtifact::file(
                                    "application/vnd.grok.native-output-spool",
                                    source,
                                )],
                                ..ProviderOutput::default()
                            })
                        }
                    }
                },
                |_| async { Ok(()) },
            )))
            .await
            .unwrap();

        let response = handle
            .submit(envelope(Command::InvokeProvider {
                request_id: xai_grok_protocol::RequestId::new(),
                operation_id: "test.execute".into(),
                preferred_provider: Some("test-provider".into()),
                input: serde_json::json!({}),
            }))
            .await
            .unwrap()
            .response
            .unwrap();
        let Response::ProviderInvoked { output, .. } = response else {
            panic!("expected provider invocation response");
        };
        let artifact_id: xai_grok_protocol::ArtifactId =
            serde_json::from_value(output["artifacts"][0]["artifact_id"].clone()).unwrap();
        std::fs::write(&source, b"source-mutated-after-return").unwrap();
        let response = handle
            .submit(envelope(Command::ReadArtifact {
                artifact_id,
                cursor: 0,
                limit: 1024,
            }))
            .await
            .unwrap()
            .response
            .unwrap();
        let Response::ArtifactChunk {
            bytes, next_cursor, ..
        } = response
        else {
            panic!("expected stored provider artifact");
        };
        assert_eq!(bytes, b"real-file-backed-provider-output");
        assert!(next_cursor.is_none());

        handle.shutdown_token().cancel();
        control_plane.wait().await;
    }

    #[tokio::test]
    async fn scheduler_runs_dependencies_in_order_without_manual_dispatch() {
        let directory = tempfile::tempdir().unwrap();
        let control_plane = ControlPlane::open(ControlPlaneConfig::new(directory.path()))
            .await
            .unwrap();
        let handle = control_plane.handle();
        let executed = Arc::new(StdMutex::new(Vec::<TaskId>::new()));
        handle
            .register_provider(Arc::new(FunctionProvider::new(
                provider_manifest(),
                {
                    let executed = executed.clone();
                    move |dispatch| {
                        let executed = executed.clone();
                        async move {
                            executed.lock().unwrap().push(dispatch.task.task_id);
                            Ok(ProviderOutput {
                                output: serde_json::json!({"ok": true}),
                                ..ProviderOutput::default()
                            })
                        }
                    }
                },
                |_| async { Ok(()) },
            )))
            .await
            .unwrap();
        let accepted = handle
            .submit(envelope(Command::SubmitIngress(IngressEnvelope {
                command_id: CommandId::new(),
                source: IngressSource::Cli,
                source_event_id: "dependency-ingress".to_owned(),
                workspace_id: WorkspaceId::from_string("workspace"),
                exercise_id: None,
                operation_run_id: None,
                operator_session_id: None,
                session_id: "dependency-session".to_owned(),
                prompt_id: "dependency-prompt".to_owned(),
                request: "run the dependency plan".to_owned(),
                team: None,
                metadata: serde_json::Map::new(),
            })))
            .await
            .unwrap()
            .response
            .unwrap();
        let Response::Accepted { engagement_id, .. } = accepted else {
            panic!("expected accepted ingress");
        };
        let first_id = TaskId::from_string("first");
        let second_id = TaskId::from_string("second");
        let task = |task_id: TaskId, depends_on: Vec<TaskId>| ExecutionTask {
            task_id,
            objective: "execute".to_owned(),
            mode: ExecutionMode::Interactive,
            capability: CapabilityRequirement {
                operation_id: "test.execute".into(),
                preferred_provider: Some("test-provider".into()),
                required_features: Vec::new(),
            },
            input: serde_json::json!({}),
            deadline_unix_ms: now_unix_ms() + 10_000,
            completion_tests: Vec::new(),
            depends_on,
        };
        handle
            .submit(envelope(Command::SubmitPlan(TaskingPlan {
                engagement_id: engagement_id.clone(),
                revision: 2,
                objective: "ordered execution".to_owned(),
                tasks: vec![
                    task(first_id.clone(), Vec::new()),
                    task(second_id.clone(), vec![first_id.clone()]),
                ],
            })))
            .await
            .unwrap()
            .response
            .unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if executed.lock().unwrap().len() == 2 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(*executed.lock().unwrap(), vec![first_id, second_id]);
        let graph = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let response = handle
                    .submit(envelope(Command::QueryProjection(
                        ProjectionQuery::TaskGraph {
                            engagement_id: engagement_id.clone(),
                        },
                    )))
                    .await
                    .unwrap()
                    .response
                    .unwrap();
                let Response::Projection(snapshot) = response else {
                    panic!("expected task graph projection");
                };
                let graph: TaskGraphProjection =
                    serde_json::from_value(snapshot.value).expect("task graph exists");
                if graph
                    .tasks
                    .iter()
                    .all(|task| task.status == TaskStatus::Completed)
                {
                    break graph;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(graph.revision, 2);
        assert_eq!(graph.objective, "ordered execution");
        assert_eq!(
            graph.tasks[1].task.depends_on,
            vec![graph.tasks[0].task.task_id.clone()]
        );
        assert!(
            graph
                .tasks
                .iter()
                .all(|task| task.provider_id.as_ref().map(ProviderId::as_str)
                    == Some("test-provider"))
        );
        assert!(graph.tasks.iter().all(|task| {
            task.artifacts
                .iter()
                .any(|artifact| artifact.provider_output)
        }));

        handle.shutdown_token().cancel();
        control_plane.wait().await;

        let reopened = ControlPlane::open(ControlPlaneConfig::new(directory.path()))
            .await
            .unwrap();
        let response = reopened
            .handle()
            .submit(envelope(Command::QueryProjection(
                ProjectionQuery::TaskGraph { engagement_id },
            )))
            .await
            .unwrap()
            .response
            .unwrap();
        let Response::Projection(snapshot) = response else {
            panic!("expected replayed task graph projection");
        };
        let replayed: TaskGraphProjection = serde_json::from_value(snapshot.value).unwrap();
        assert_eq!(replayed, graph);
        reopened.handle().shutdown_token().cancel();
        reopened.wait().await;
    }

    async fn register_counting_provider(handle: &ControlPlaneHandle, executions: Arc<AtomicUsize>) {
        handle
            .register_provider(Arc::new(FunctionProvider::new(
                provider_manifest(),
                move |_| {
                    let executions = executions.clone();
                    async move {
                        executions.fetch_add(1, Ordering::SeqCst);
                        Ok(ProviderOutput {
                            output: serde_json::json!({"ok": true}),
                            ..ProviderOutput::default()
                        })
                    }
                },
                |_| async { Ok(()) },
            )))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn completed_dispatch_is_not_replayed_after_restart() {
        let directory = tempfile::tempdir().unwrap();
        let state = directory.path().join("state");
        let executions = Arc::new(AtomicUsize::new(0));
        let dispatch;
        {
            let mut config = ControlPlaneConfig::new(&state);
            config.auto_schedule_plans = false;
            let control_plane = ControlPlane::open(config).await.unwrap();
            let handle = control_plane.handle();
            register_counting_provider(&handle, executions.clone()).await;
            let accepted = handle
                .submit(envelope(Command::SubmitIngress(IngressEnvelope {
                    command_id: CommandId::new(),
                    source: IngressSource::Cli,
                    source_event_id: "event-dispatch".to_owned(),
                    workspace_id: WorkspaceId::from_string("workspace"),
                    exercise_id: None,
                    operation_run_id: None,
                    operator_session_id: None,
                    session_id: "session-dispatch".to_owned(),
                    prompt_id: "prompt-dispatch".to_owned(),
                    request: "execute once".to_owned(),
                    team: None,
                    metadata: serde_json::Map::new(),
                })))
                .await
                .unwrap()
                .response
                .unwrap();
            let Response::Accepted { engagement_id, .. } = accepted else {
                panic!("expected accepted engagement");
            };
            let task = ExecutionTask {
                task_id: TaskId::new(),
                objective: "execute".to_owned(),
                mode: ExecutionMode::Interactive,
                capability: CapabilityRequirement {
                    operation_id: "test.execute".into(),
                    preferred_provider: Some("test-provider".into()),
                    required_features: Vec::new(),
                },
                input: serde_json::json!({}),
                deadline_unix_ms: now_unix_ms() + 10_000,
                completion_tests: Vec::new(),
                depends_on: Vec::new(),
            };
            let plan = TaskingPlan {
                engagement_id: engagement_id.clone(),
                revision: 1,
                objective: "execute once".to_owned(),
                tasks: vec![task.clone()],
            };
            handle
                .submit(envelope(Command::SubmitPlan(plan)))
                .await
                .unwrap()
                .response
                .unwrap();
            dispatch = ProviderDispatch {
                request_id: RequestId::new(),
                engagement_id,
                plan_revision: 1,
                task,
                provider_id: "test-provider".into(),
                lease_epoch: 1,
            };
            handle
                .submit(envelope(Command::Dispatch(dispatch.clone())))
                .await
                .unwrap()
                .response
                .unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                while executions.load(Ordering::SeqCst) != 1 {
                    tokio::task::yield_now().await;
                }
                loop {
                    let snapshot = handle
                        .submit(envelope(Command::QueryProjection(
                            xai_grok_protocol::ProjectionQuery::Engagement {
                                engagement_id: dispatch.engagement_id.clone(),
                            },
                        )))
                        .await
                        .unwrap()
                        .response
                        .unwrap();
                    let Response::Projection(snapshot) = snapshot else {
                        panic!("expected projection");
                    };
                    if snapshot.value.to_string().contains("\"completed\"") {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            handle.shutdown_token().cancel();
            control_plane.wait().await;
        }

        let mut config = ControlPlaneConfig::new(&state);
        config.auto_schedule_plans = false;
        let control_plane = ControlPlane::open(config).await.unwrap();
        let handle = control_plane.handle();
        register_counting_provider(&handle, executions.clone()).await;
        handle
            .submit(envelope(Command::Dispatch(dispatch)))
            .await
            .unwrap()
            .response
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        assert_eq!(executions.load(Ordering::SeqCst), 1);
        handle.shutdown_token().cancel();
        control_plane.wait().await;
    }
}
