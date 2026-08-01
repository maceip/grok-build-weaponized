use std::collections::{HashMap, VecDeque};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::{Mutex, broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use xai_grok_engagement::{
    EngagementCheckpoint, EngagementCoordinator, NewEngagement, QueuePriority,
};
use xai_grok_protocol::{
    Command, CommandEnvelope, CommandId, EngagementId, Event, EventBatch, EventEnvelope, EventId,
    EventReadRequest, EvidenceObservation, ExecutionReceipt, Exercise, ExerciseId, ExerciseStatus,
    OperationRun, OperationRunId, OperationRunStatus, OperatorSession, OperatorSessionId,
    OperatorSessionStatus, PROTOCOL_VERSION, ProtocolError, ProtocolErrorCode, ProviderDispatch,
    ProviderId, Response, ResponseEnvelope, ServiceHealth, TaskStatus, TaskingPlan,
};

use crate::SERVER_NAME;
use crate::artifact::{ArtifactStore, ArtifactStoreConfig};
use crate::journal::{EventJournal, JournalError};
use crate::projection::ProjectionStore;
use crate::provider::{
    ExecutionProvider, ProviderOutput, ProviderRegistry, ProviderRegistryConfig,
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
    responses: HashMap<CommandId, ResponseEnvelope>,
}

impl ResponseCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            order: VecDeque::new(),
            responses: HashMap::new(),
        }
    }

    fn get(&self, command_id: &CommandId) -> Option<ResponseEnvelope> {
        self.responses.get(command_id).cloned()
    }

    fn insert(&mut self, response: ResponseEnvelope) {
        let command_id = response.command_id.clone();
        if self.responses.contains_key(&command_id) {
            return;
        }
        self.order.push_back(command_id.clone());
        self.responses.insert(command_id, response);
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
    shutdown: CancellationToken,
}

impl ControlPlaneCore {
    async fn process(self: &Arc<Self>, envelope: CommandEnvelope) -> ResponseEnvelope {
        if let Some(response) = self.responses.lock().await.get(&envelope.command_id) {
            return response;
        }
        let command_id = envelope.command_id.clone();
        let result = match envelope.validate(now_unix_ms()) {
            Ok(()) => self.handle(envelope).await,
            Err(error) => Err(error),
        };
        let response = ResponseEnvelope {
            protocol_version: PROTOCOL_VERSION,
            command_id,
            response: result,
        };
        self.responses.lock().await.insert(response.clone());
        response
    }

    async fn handle(
        self: &Arc<Self>,
        envelope: CommandEnvelope,
    ) -> Result<Response, ProtocolError> {
        let causation_id = Some(envelope.command_id.to_string());
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
                        causation_id,
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
                    causation_id,
                    0,
                    Event::PlanAccepted {
                        revision: plan.revision,
                        task_count: plan.tasks.len() as u32,
                    },
                )
                .await?;
                Ok(Response::PlanAccepted {
                    engagement_id: plan.engagement_id,
                    revision: plan.revision,
                })
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
                let record = self.services.register(manifest).await?;
                self.emit(
                    None,
                    causation_id,
                    record.generation,
                    Event::ProviderState {
                        provider_id: record.provider_id.clone(),
                        service_id: record.service_id,
                        generation: record.generation,
                        health: record.health,
                    },
                )
                .await?;
                Ok(Response::ProviderRegistered {
                    provider_id: record.provider_id,
                    generation: record.generation,
                    manifest_hash: record.manifest_hash,
                })
            }
            Command::ProviderHeartbeat {
                provider_id,
                generation,
                health,
            } => {
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
                        .map_err(|error| internal_error(error.to_string()))?;
                self.emit(
                    None,
                    causation_id,
                    0,
                    Event::ArtifactAvailable {
                        artifact_id: descriptor.artifact_id.clone(),
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
            Command::ReadEvents(request) => Ok(Response::Events(self.read_events(&request).await?)),
            Command::QueryProjection(query) => {
                Ok(Response::Projection(self.projections.query(query).await))
            }
            Command::Shutdown => {
                self.shutdown.cancel();
                Ok(Response::Ack)
            }
        }
    }

    fn spawn_dispatch(self: &Arc<Self>, dispatch: ProviderDispatch, causation_id: Option<String>) {
        let core = self.clone();
        tokio::spawn(async move {
            let engagement_id = dispatch.engagement_id.clone();
            let task_id = dispatch.task.task_id.clone();
            let provider_id = dispatch.provider_id.clone();
            let request_id = dispatch.request_id.clone();
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
        });
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
            let store = self.artifacts.clone();
            let descriptor = tokio::task::spawn_blocking(move || {
                store.put(artifact.media_type, &artifact.bytes)
            })
            .await
            .map_err(|error| internal_error(error.to_string()))?
            .map_err(|error| internal_error(error.to_string()))?;
            self.emit(
                Some(engagement_id.clone()),
                causation_id.clone(),
                generation,
                Event::ArtifactAvailable {
                    artifact_id: descriptor.artifact_id,
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
                artifact_id: descriptor.artifact_id,
                media_type: descriptor.media_type,
                byte_size: descriptor.byte_size,
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
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return self.read_event_page(request).await;
            }
            match tokio::time::timeout(remaining, live_events.recv()).await {
                Ok(Ok(event))
                    if request
                        .engagement_id
                        .as_ref()
                        .is_none_or(|expected| event.engagement_id.as_ref() == Some(expected)) =>
                {
                    return self.read_event_page(request).await;
                }
                Ok(Ok(_)) => continue,
                Ok(Err(broadcast::error::RecvError::Lagged(_))) => {
                    return self.read_event_page(request).await;
                }
                Ok(Err(broadcast::error::RecvError::Closed)) | Err(_) => {
                    return self.read_event_page(request).await;
                }
            }
        }
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
        let (registry_generation, manifest_hash) = self.core.providers.register(provider).await?;
        let mut service = match self.core.services.register(manifest).await {
            Ok(service) => service,
            Err(error) => {
                self.core
                    .providers
                    .unregister(&provider_id, registry_generation)
                    .await;
                return Err(error);
            }
        };
        if registry_generation != service.generation {
            self.core
                .providers
                .unregister(&provider_id, registry_generation)
                .await;
            self.core
                .services
                .remove(&provider_id, service.generation)
                .await;
            return Err(ProtocolError::new(
                ProtocolErrorCode::Conflict,
                "provider registry and service supervisor generations diverged",
            ));
        }
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
                                .heartbeat(
                                    &capacity.provider_id,
                                    capacity.generation,
                                    health,
                                )
                                .await
                                && previous.as_ref().is_some_and(|record| record.health != health)
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
        error => internal_error(error),
    }
}

fn internal_error(error: impl std::fmt::Display) -> ProtocolError {
    ProtocolError::new(ProtocolErrorCode::Internal, error.to_string())
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    use xai_grok_protocol::{
        ArtifactContract, CancellationSemantics, CapabilityManifest, CapabilityRequirement,
        Command, CommandEnvelope, CommandId, ConcurrencyProfile, CreateExercise,
        CreateOperationRun, CreateOperatorSession, ExecutionMode, ExecutionTask, IngressEnvelope,
        IngressSource, OperationDescriptor, OperatorCatalog, ProjectionQuery, ProviderKind,
        RecoverySemantics, RequestId, TaskId, VersionRange, WorkspaceId,
    };

    use super::*;
    use crate::provider::FunctionProvider;

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
            let control_plane = ControlPlane::open(ControlPlaneConfig::new(&state))
                .await
                .unwrap();
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

        let control_plane = ControlPlane::open(ControlPlaneConfig::new(&state))
            .await
            .unwrap();
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
