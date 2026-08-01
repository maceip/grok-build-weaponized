//! Renderer-neutral operator application core.
//!
//! Ratatui and egui are adapters over this state machine. Neither renderer
//! owns exercise/session semantics or constructs control-plane messages.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

use xai_grok_control_client::{ClientIdentity, ControlPlaneClient};
use xai_grok_protocol::{
    ArtifactId, ClientId, Command, CommandId, CreateExercise, CreateOperationRun,
    CreateOperatorSession, EngagementId, EventEnvelope, EventReadRequest, ExerciseId,
    IngressEnvelope, IngressSource, OperationRunId, OperatorCatalog, OperatorSessionId,
    ProjectionQuery, Response, ScopeTarget, TaskId, TeamClient, TeamId, WorkspaceId,
};

pub const MAX_VISIBLE_EVENTS: usize = 2_000;
pub const MAX_VISIBLE_OUTPUTS: usize = 512;
const OUTPUT_PREVIEW_BYTES: u32 = 1024 * 1024;
pub use xai_grok_protocol::parse_scope_targets;

#[derive(Clone, Debug)]
pub struct OperatorClientConfig {
    pub socket: PathBuf,
    pub team_id: TeamId,
    pub client_id: ClientId,
    pub display_name: Option<String>,
    pub client_name: String,
    pub source: IngressSource,
    pub workspace_filter: Option<WorkspaceId>,
}

impl OperatorClientConfig {
    pub fn local(
        socket: impl Into<PathBuf>,
        client_name: impl Into<String>,
        source: IngressSource,
    ) -> Self {
        Self {
            socket: socket.into(),
            team_id: TeamId::from_string("local-team"),
            client_id: ClientId::new(),
            display_name: None,
            client_name: client_name.into(),
            source,
            workspace_filter: None,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OperatorSelection {
    pub exercise_id: Option<ExerciseId>,
    pub operation_run_id: Option<OperationRunId>,
    pub session_id: Option<OperatorSessionId>,
}

#[derive(Clone, Debug)]
pub struct OperatorState {
    pub connection: String,
    pub catalog_loaded: bool,
    pub cursor: u64,
    pub catalog: OperatorCatalog,
    pub selection: OperatorSelection,
    pub capacity: serde_json::Value,
    pub providers: serde_json::Value,
    pub events: VecDeque<EventEnvelope>,
    pub outputs: VecDeque<OperatorOutput>,
    pub notice: String,
}

#[derive(Clone, Debug)]
pub struct OperatorOutput {
    pub engagement_id: EngagementId,
    pub task_id: TaskId,
    pub artifact_id: ArtifactId,
    pub text: String,
    pub truncated: bool,
}

impl Default for OperatorState {
    fn default() -> Self {
        Self {
            connection: "connecting".to_owned(),
            catalog_loaded: false,
            cursor: 0,
            catalog: OperatorCatalog::default(),
            selection: OperatorSelection::default(),
            capacity: serde_json::Value::Null,
            providers: serde_json::Value::Null,
            events: VecDeque::new(),
            outputs: VecDeque::new(),
            notice: "idle".to_owned(),
        }
    }
}

impl OperatorState {
    pub fn first_run(&self) -> bool {
        self.catalog_loaded && self.catalog.exercises.is_empty()
    }

    pub fn selected_exercise(&self) -> Option<&xai_grok_protocol::Exercise> {
        let selected = self.selection.exercise_id.as_ref()?;
        self.catalog
            .exercises
            .iter()
            .find(|exercise| &exercise.exercise_id == selected)
    }

    pub fn selected_operation_run(&self) -> Option<&xai_grok_protocol::OperationRun> {
        let selected = self.selection.operation_run_id.as_ref()?;
        self.catalog
            .operation_runs
            .iter()
            .find(|run| &run.operation_run_id == selected)
    }

    pub fn selected_session(&self) -> Option<&xai_grok_protocol::OperatorSession> {
        let selected = self.selection.session_id.as_ref()?;
        self.catalog
            .sessions
            .iter()
            .find(|session| &session.session_id == selected)
    }

    pub fn output_is_in_selected_session(&self, output: &OperatorOutput) -> bool {
        let Some(selected) = self.selection.session_id.as_ref() else {
            return false;
        };
        self.events.iter().any(|event| {
            event.engagement_id.as_ref() == Some(&output.engagement_id)
                && matches!(
                    &event.event,
                    xai_grok_protocol::Event::EngagementAccepted {
                        operator_session_id: Some(session_id),
                        ..
                    } if session_id == selected
                )
        })
    }

    pub fn select_exercise(&mut self, exercise_id: ExerciseId) {
        self.selection.exercise_id = Some(exercise_id.clone());
        let latest_session = self
            .catalog
            .sessions
            .iter()
            .rev()
            .find(|session| session.exercise_id == exercise_id)
            .cloned();
        if let Some(session) = latest_session {
            self.selection.operation_run_id = session.operation_run_id;
            self.selection.session_id = Some(session.session_id);
        } else {
            self.selection.operation_run_id = self
                .catalog
                .operation_runs
                .iter()
                .rev()
                .find(|run| run.exercise_id == exercise_id)
                .map(|run| run.operation_run_id.clone());
            self.selection.session_id = None;
        }
    }

    pub fn select_operation_run(&mut self, operation_run_id: OperationRunId) {
        let Some(run) = self
            .catalog
            .operation_runs
            .iter()
            .find(|run| run.operation_run_id == operation_run_id)
        else {
            return;
        };
        self.selection.exercise_id = Some(run.exercise_id.clone());
        self.selection.operation_run_id = Some(operation_run_id.clone());
        self.selection.session_id = self
            .catalog
            .sessions
            .iter()
            .rev()
            .find(|session| session.operation_run_id.as_ref() == Some(&operation_run_id))
            .map(|session| session.session_id.clone());
    }

    pub fn select_session(&mut self, session_id: OperatorSessionId) {
        let Some(session) = self
            .catalog
            .sessions
            .iter()
            .find(|session| session.session_id == session_id)
        else {
            return;
        };
        self.selection.exercise_id = Some(session.exercise_id.clone());
        self.selection.operation_run_id = session.operation_run_id.clone();
        self.selection.session_id = Some(session_id);
    }

    pub fn apply(&mut self, update: OperatorUpdate) {
        match update {
            OperatorUpdate::Connection(connection) => self.connection = connection,
            OperatorUpdate::Catalog(catalog) => {
                self.catalog = catalog;
                self.catalog_loaded = true;
                self.reconcile_selection();
            }
            OperatorUpdate::Events { events, cursor } => {
                for event in events {
                    self.apply_catalog_event(&event);
                    self.events.push_back(event);
                }
                self.cursor = cursor;
                while self.events.len() > MAX_VISIBLE_EVENTS {
                    self.events.pop_front();
                }
                self.reconcile_selection();
            }
            OperatorUpdate::Capacity(capacity) => self.capacity = capacity,
            OperatorUpdate::Providers(providers) => self.providers = providers,
            OperatorUpdate::Output(output) => {
                self.outputs.push_back(output);
                while self.outputs.len() > MAX_VISIBLE_OUTPUTS {
                    self.outputs.pop_front();
                }
            }
            OperatorUpdate::Notice(notice) => self.notice = notice,
        }
    }

    fn apply_catalog_event(&mut self, envelope: &EventEnvelope) {
        match &envelope.event {
            xai_grok_protocol::Event::ExerciseCreated { exercise } => {
                if !self
                    .catalog
                    .exercises
                    .iter()
                    .any(|known| known.exercise_id == exercise.exercise_id)
                {
                    self.catalog.exercises.push(exercise.clone());
                }
            }
            xai_grok_protocol::Event::OperationRunCreated { operation_run } => {
                if !self
                    .catalog
                    .operation_runs
                    .iter()
                    .any(|known| known.operation_run_id == operation_run.operation_run_id)
                {
                    self.catalog.operation_runs.push(operation_run.clone());
                }
            }
            xai_grok_protocol::Event::OperatorSessionCreated { session } => {
                if !self
                    .catalog
                    .sessions
                    .iter()
                    .any(|known| known.session_id == session.session_id)
                {
                    self.catalog.sessions.push(session.clone());
                }
            }
            xai_grok_protocol::Event::PlaybookCreated { playbook } => {
                if !self
                    .catalog
                    .playbooks
                    .iter()
                    .any(|known| known.playbook_id == playbook.playbook_id)
                {
                    self.catalog.playbooks.push(playbook.clone());
                }
            }
            _ => {}
        }
    }

    fn reconcile_selection(&mut self) {
        let exercise_valid = self.selection.exercise_id.as_ref().is_some_and(|selected| {
            self.catalog
                .exercises
                .iter()
                .any(|exercise| &exercise.exercise_id == selected)
        });
        if !exercise_valid {
            if let Some(exercise) = self.catalog.exercises.last() {
                self.select_exercise(exercise.exercise_id.clone());
            } else {
                self.selection = OperatorSelection::default();
                return;
            }
        }
        let exercise_id = self
            .selection
            .exercise_id
            .clone()
            .expect("selected exercise");
        if self
            .selection
            .operation_run_id
            .as_ref()
            .is_some_and(|selected| {
                !self
                    .catalog
                    .operation_runs
                    .iter()
                    .any(|run| &run.operation_run_id == selected && run.exercise_id == exercise_id)
            })
        {
            self.selection.operation_run_id = None;
        }
        if self.selection.session_id.as_ref().is_some_and(|selected| {
            !self.catalog.sessions.iter().any(|session| {
                &session.session_id == selected && session.exercise_id == exercise_id
            })
        }) {
            self.selection.session_id = None;
        }
        if let Some(session_id) = &self.selection.session_id
            && let Some(session) = self
                .catalog
                .sessions
                .iter()
                .find(|session| &session.session_id == session_id)
        {
            self.selection.operation_run_id = session.operation_run_id.clone();
        }
    }
}

#[derive(Clone, Debug)]
pub enum OperatorCommand {
    CreateExercise {
        workspace_id: String,
        name: String,
        objective: String,
        scope: Vec<ScopeTarget>,
    },
    CreateOperationRun {
        exercise_id: ExerciseId,
        name: String,
        objective: String,
    },
    CreateSession {
        exercise_id: ExerciseId,
        operation_run_id: Option<OperationRunId>,
        name: String,
        purpose: String,
    },
    SubmitTurn {
        workspace_id: WorkspaceId,
        exercise_id: ExerciseId,
        operation_run_id: Option<OperationRunId>,
        session_id: OperatorSessionId,
        request: String,
    },
}

impl OperatorCommand {
    pub fn into_protocol(self, source: IngressSource) -> Command {
        match self {
            Self::CreateExercise {
                workspace_id,
                name,
                objective,
                scope,
            } => Command::CreateExercise(CreateExercise {
                workspace_id: WorkspaceId::from_string(workspace_id),
                name,
                objective,
                scope,
            }),
            Self::CreateOperationRun {
                exercise_id,
                name,
                objective,
            } => Command::CreateOperationRun(CreateOperationRun {
                exercise_id,
                name,
                objective,
                playbook_id: None,
            }),
            Self::CreateSession {
                exercise_id,
                operation_run_id,
                name,
                purpose,
            } => Command::CreateOperatorSession(CreateOperatorSession {
                exercise_id,
                operation_run_id,
                name,
                purpose,
            }),
            Self::SubmitTurn {
                workspace_id,
                exercise_id,
                operation_run_id,
                session_id,
                request,
            } => {
                let source_id = CommandId::new();
                Command::SubmitIngress(IngressEnvelope {
                    command_id: CommandId::new(),
                    source,
                    source_event_id: source_id.to_string(),
                    workspace_id,
                    exercise_id: Some(exercise_id),
                    operation_run_id,
                    operator_session_id: Some(session_id.clone()),
                    session_id: session_id.to_string(),
                    prompt_id: source_id.to_string(),
                    request,
                    team: None,
                    metadata: serde_json::Map::new(),
                })
            }
        }
    }
}

#[derive(Clone, Debug)]
pub enum OperatorUpdate {
    Connection(String),
    Catalog(OperatorCatalog),
    Events {
        events: Vec<EventEnvelope>,
        cursor: u64,
    },
    Capacity(serde_json::Value),
    Providers(serde_json::Value),
    Output(OperatorOutput),
    Notice(String),
}

pub fn spawn_client_worker(
    config: OperatorClientConfig,
    updates: mpsc::SyncSender<OperatorUpdate>,
    commands: mpsc::Receiver<OperatorCommand>,
    stop: Arc<AtomicBool>,
) {
    thread::Builder::new()
        .name(format!("{}-client", config.client_name))
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build();
            match runtime {
                Ok(runtime) => runtime.block_on(client_loop(config, updates, commands, stop)),
                Err(error) => {
                    let _ = updates.send(OperatorUpdate::Connection(format!(
                        "runtime error: {error}"
                    )));
                }
            }
        })
        .expect("failed to start bounded operator client worker");
}

async fn client_loop(
    config: OperatorClientConfig,
    updates: mpsc::SyncSender<OperatorUpdate>,
    commands: mpsc::Receiver<OperatorCommand>,
    stop: Arc<AtomicBool>,
) {
    let mut cursor = 0_u64;
    while !stop.load(Ordering::Acquire) {
        let mut identity = ClientIdentity::new(&config.client_name, env!("CARGO_PKG_VERSION"));
        identity.team = Some(TeamClient {
            team_id: config.team_id.clone(),
            client_id: config.client_id.clone(),
            display_name: config.display_name.clone(),
        });
        let control = match ControlPlaneClient::connect(&config.socket, identity).await {
            Ok(control) => control,
            Err(error) => {
                if updates
                    .send(OperatorUpdate::Connection(format!("reconnecting: {error}")))
                    .is_err()
                {
                    return;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        if updates
            .send(OperatorUpdate::Connection(format!(
                "connected to {}",
                config.socket.display()
            )))
            .is_err()
        {
            return;
        }
        if refresh(&control, &updates, config.workspace_filter.clone())
            .await
            .is_err()
        {
            continue;
        }
        let mut next_projection = tokio::time::Instant::now() + Duration::from_secs(2);
        'connected: loop {
            if stop.load(Ordering::Acquire) {
                return;
            }
            while let Ok(command) = commands.try_recv() {
                match submit(&control, command, config.source).await {
                    Ok(notice) => {
                        let _ = updates.send(OperatorUpdate::Notice(notice));
                        if refresh(&control, &updates, config.workspace_filter.clone())
                            .await
                            .is_err()
                        {
                            break 'connected;
                        }
                    }
                    Err(error) => {
                        let _ = updates.send(OperatorUpdate::Notice(format!("failed: {error}")));
                        let _ = updates
                            .send(OperatorUpdate::Connection(format!("reconnecting: {error}")));
                        break 'connected;
                    }
                }
            }
            match control
                .read_events(EventReadRequest {
                    after_sequence: cursor,
                    maximum_events: 128,
                    wait_ms: 100,
                    engagement_id: None,
                })
                .await
            {
                Ok(batch) => {
                    cursor = batch.next_sequence;
                    let provider_outputs = batch
                        .events
                        .iter()
                        .filter_map(|event| match (&event.engagement_id, &event.event) {
                            (
                                Some(engagement_id),
                                xai_grok_protocol::Event::ProviderOutput {
                                    task_id,
                                    artifact_id,
                                },
                            ) => {
                                Some((engagement_id.clone(), task_id.clone(), artifact_id.clone()))
                            }
                            _ => None,
                        })
                        .collect::<Vec<_>>();
                    if updates
                        .send(OperatorUpdate::Events {
                            events: batch.events,
                            cursor,
                        })
                        .is_err()
                    {
                        return;
                    }
                    for (engagement_id, task_id, artifact_id) in provider_outputs {
                        match read_provider_output(&control, engagement_id, task_id, artifact_id)
                            .await
                        {
                            Ok(output) => {
                                if updates.send(OperatorUpdate::Output(output)).is_err() {
                                    return;
                                }
                            }
                            Err(error) => {
                                let _ = updates.send(OperatorUpdate::Notice(format!(
                                    "failed to read provider output: {error}"
                                )));
                            }
                        }
                    }
                }
                Err(error) => {
                    let _ =
                        updates.send(OperatorUpdate::Connection(format!("reconnecting: {error}")));
                    break;
                }
            }
            if tokio::time::Instant::now() >= next_projection {
                if let Err(error) =
                    refresh(&control, &updates, config.workspace_filter.clone()).await
                {
                    let _ =
                        updates.send(OperatorUpdate::Connection(format!("reconnecting: {error}")));
                    break;
                }
                next_projection = tokio::time::Instant::now() + Duration::from_secs(2);
            }
        }
    }
}

async fn read_provider_output(
    control: &ControlPlaneClient,
    engagement_id: EngagementId,
    task_id: TaskId,
    artifact_id: ArtifactId,
) -> Result<OperatorOutput, Box<dyn std::error::Error>> {
    let response = control
        .send(
            Command::ReadArtifact {
                artifact_id: artifact_id.clone(),
                cursor: 0,
                limit: OUTPUT_PREVIEW_BYTES,
            },
            Duration::from_secs(5),
        )
        .await?;
    let Response::ArtifactChunk {
        bytes, next_cursor, ..
    } = response
    else {
        return Err("daemon returned an unexpected artifact response".into());
    };
    let truncated = next_cursor.is_some();
    let text = if !truncated {
        serde_json::from_slice::<serde_json::Value>(&bytes)
            .ok()
            .and_then(|value| {
                value
                    .get("text")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| String::from_utf8_lossy(&bytes).into_owned())
    } else {
        String::from_utf8_lossy(&bytes).into_owned()
    };
    Ok(OperatorOutput {
        engagement_id,
        task_id,
        artifact_id,
        text,
        truncated,
    })
}

async fn submit(
    control: &ControlPlaneClient,
    command: OperatorCommand,
    source: IngressSource,
) -> Result<String, Box<dyn std::error::Error>> {
    let response = control
        .send(command.into_protocol(source), Duration::from_secs(10))
        .await?;
    let notice = match response {
        Response::ExerciseCreated { exercise } => {
            format!("created exercise {}", exercise.name)
        }
        Response::OperationRunCreated { operation_run } => {
            format!("created run {}", operation_run.name)
        }
        Response::OperatorSessionCreated { session } => {
            format!("created session {}", session.name)
        }
        Response::Accepted { engagement_id, .. } => {
            format!("turn accepted as {}", engagement_id.as_str())
        }
        _ => return Err("daemon returned an unexpected operator response".into()),
    };
    Ok(notice)
}

async fn refresh(
    control: &ControlPlaneClient,
    updates: &mpsc::SyncSender<OperatorUpdate>,
    workspace_filter: Option<WorkspaceId>,
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = control
        .send(
            Command::QueryProjection(ProjectionQuery::OperatorCatalog {
                workspace_id: workspace_filter,
            }),
            Duration::from_secs(2),
        )
        .await?;
    let capacity = control
        .send(
            Command::QueryProjection(ProjectionQuery::Capacity),
            Duration::from_secs(2),
        )
        .await?;
    let providers = control
        .send(
            Command::QueryProjection(ProjectionQuery::Providers),
            Duration::from_secs(2),
        )
        .await?;
    if let Response::Projection(snapshot) = catalog {
        updates.send(OperatorUpdate::Catalog(serde_json::from_value(
            snapshot.value,
        )?))?;
    }
    if let Response::Projection(snapshot) = capacity {
        updates.send(OperatorUpdate::Capacity(snapshot.value))?;
    }
    if let Response::Projection(snapshot) = providers {
        updates.send(OperatorUpdate::Providers(snapshot.value))?;
    }
    Ok(())
}

pub fn event_name(event: &EventEnvelope) -> &'static str {
    match &event.event {
        xai_grok_protocol::Event::ExerciseCreated { .. } => "exercise created",
        xai_grok_protocol::Event::OperationRunCreated { .. } => "operation run created",
        xai_grok_protocol::Event::OperatorSessionCreated { .. } => "session created",
        xai_grok_protocol::Event::PlaybookCreated { .. } => "playbook created",
        xai_grok_protocol::Event::EvidenceRecorded { .. } => "evidence recorded",
        xai_grok_protocol::Event::FindingCreated { .. } => "finding created",
        xai_grok_protocol::Event::FindingStatusSet { .. } => "finding status set",
        xai_grok_protocol::Event::EngagementAccepted { .. } => "turn accepted",
        xai_grok_protocol::Event::PlanAccepted { .. } => "plan accepted",
        xai_grok_protocol::Event::TaskStatus { .. } => "task status",
        xai_grok_protocol::Event::Observation { .. } => "observation",
        xai_grok_protocol::Event::ProviderState { .. } => "provider state",
        xai_grok_protocol::Event::ArtifactAvailable { .. } => "artifact available",
        xai_grok_protocol::Event::ProviderOutput { .. } => "provider output",
        xai_grok_protocol::Event::Overload { .. } => "overload",
    }
}

pub fn event_summary(event: &EventEnvelope) -> String {
    match &event.event {
        xai_grok_protocol::Event::TaskStatus {
            task_id,
            status,
            provider_id,
        } => format!(
            "task {} {:?}{}",
            task_id.as_str(),
            status,
            provider_id
                .as_ref()
                .map(|provider| format!(" via {}", provider.as_str()))
                .unwrap_or_default()
        ),
        xai_grok_protocol::Event::Observation {
            task_id,
            observation,
        } => format!("{}: {}", task_id.as_str(), observation.finding),
        xai_grok_protocol::Event::ProviderOutput {
            task_id,
            artifact_id,
        } => format!(
            "task {} produced {}",
            task_id.as_str(),
            artifact_id.as_str()
        ),
        xai_grok_protocol::Event::ArtifactAvailable {
            artifact_id,
            byte_size,
            ..
        } => format!("artifact {} ({} bytes)", artifact_id.as_str(), byte_size),
        xai_grok_protocol::Event::ProviderState {
            provider_id,
            health,
            ..
        } => format!("provider {} {:?}", provider_id.as_str(), health),
        xai_grok_protocol::Event::EvidenceRecorded { evidence } => {
            format!("evidence {}: {}", evidence.evidence_id, evidence.finding)
        }
        xai_grok_protocol::Event::FindingCreated { finding }
        | xai_grok_protocol::Event::FindingStatusSet { finding } => format!(
            "finding {} [{:?}/{:?}]: {}",
            finding.finding_id, finding.severity, finding.status, finding.title
        ),
        _ => event_name(event).to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use xai_grok_protocol::{
        Exercise, ExerciseStatus, OperationRun, OperationRunStatus, OperatorSession,
        OperatorSessionStatus,
    };

    use super::*;

    fn catalog() -> OperatorCatalog {
        let exercise_id = ExerciseId::from_string("exercise-a");
        let run_id = OperationRunId::from_string("run-a");
        OperatorCatalog {
            exercises: vec![Exercise {
                exercise_id: exercise_id.clone(),
                workspace_id: WorkspaceId::from_string("workspace-a"),
                name: "Assessment".to_owned(),
                objective: "Validate segmentation".to_owned(),
                status: ExerciseStatus::Active,
                scope: Vec::new(),
                created_unix_ms: 1,
                updated_unix_ms: 1,
            }],
            operation_runs: vec![OperationRun {
                operation_run_id: run_id.clone(),
                exercise_id: exercise_id.clone(),
                name: "Discovery".to_owned(),
                objective: "Inventory hosts".to_owned(),
                playbook_id: None,
                status: OperationRunStatus::Running,
                created_unix_ms: 2,
                updated_unix_ms: 2,
            }],
            sessions: vec![OperatorSession {
                session_id: OperatorSessionId::from_string("session-a"),
                exercise_id,
                operation_run_id: Some(run_id),
                name: "Primary".to_owned(),
                purpose: "Run discovery tasks".to_owned(),
                status: OperatorSessionStatus::Active,
                created_unix_ms: 3,
                last_active_unix_ms: 3,
            }],
            playbooks: Vec::new(),
        }
    }

    #[test]
    fn first_catalog_selects_a_complete_execution_lane() {
        let mut state = OperatorState::default();
        state.apply(OperatorUpdate::Catalog(catalog()));
        assert_eq!(state.selected_exercise().unwrap().name, "Assessment");
        assert_eq!(state.selected_operation_run().unwrap().name, "Discovery");
        assert_eq!(state.selected_session().unwrap().name, "Primary");
    }

    #[test]
    fn changing_exercise_never_leaks_a_session_from_another_exercise() {
        let mut state = OperatorState::default();
        state.apply(OperatorUpdate::Catalog(catalog()));
        let other = ExerciseId::from_string("exercise-b");
        state.catalog.exercises.push(Exercise {
            exercise_id: other.clone(),
            workspace_id: WorkspaceId::from_string("workspace-b"),
            name: "Other".to_owned(),
            objective: "Other objective".to_owned(),
            status: ExerciseStatus::Active,
            scope: Vec::new(),
            created_unix_ms: 4,
            updated_unix_ms: 4,
        });
        state.select_exercise(other);
        assert!(state.selected_session().is_none());
    }

    #[test]
    fn selecting_an_exercise_keeps_run_and_session_on_the_same_lane() {
        let mut state = OperatorState::default();
        let mut catalog = catalog();
        let newer_run = OperationRunId::from_string("run-b");
        catalog.operation_runs.push(OperationRun {
            operation_run_id: newer_run,
            exercise_id: ExerciseId::from_string("exercise-a"),
            name: "Newer empty run".to_owned(),
            objective: "No session yet".to_owned(),
            playbook_id: None,
            status: OperationRunStatus::Planned,
            created_unix_ms: 4,
            updated_unix_ms: 4,
        });
        state.apply(OperatorUpdate::Catalog(catalog));
        assert_eq!(
            state.selection.operation_run_id.as_ref().unwrap().as_str(),
            "run-a"
        );
        assert_eq!(
            state.selection.session_id.as_ref().unwrap().as_str(),
            "session-a"
        );
    }
}
