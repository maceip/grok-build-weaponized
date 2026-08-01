//! Renderer-neutral operator application core.
//!
//! Ratatui and egui are adapters over this state machine. Neither renderer
//! owns exercise/session semantics or constructs control-plane messages.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

use xai_grok_control_client::{ClientIdentity, ControlPlaneClient};
use xai_grok_protocol::{
    ArtifactId, ClientId, Command, CommandId, CreateExercise, CreateOperationRun,
    CreateOperatorSession, EngagementId, EventEnvelope, EventReadRequest, ExecutionMode,
    ExerciseId, IngressEnvelope, IngressSource, OperationRunId, OperatorCatalog, OperatorSessionId,
    ProjectionQuery, RequestId, Response, ScopeTarget, SetTeamPresence, TaskGraphProjection,
    TaskId, TaskStatus, TeamClient, TeamId, TeamPresenceState, TeamProjection, WorkspaceId,
};

/// Renderer-independent visual language shared by the windowed and terminal
/// operator surfaces. Colors are stored as raw RGB so the core does not depend
/// on either renderer.
pub mod theme {
    use xai_grok_protocol::{
        Event, EventEnvelope, ServiceHealth, TaskStatus, TeamPresenceState, TeamWorkItemStatus,
    };

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct Rgb(pub u8, pub u8, pub u8);

    impl Rgb {
        pub const fn tuple(self) -> (u8, u8, u8) {
            (self.0, self.1, self.2)
        }
    }

    // Gilded Glitch / Cyber-Art-Deco tokens from the attached source design.
    pub const OBSIDIAN: Rgb = Rgb(0x08, 0x08, 0x08);
    pub const SURFACE: Rgb = Rgb(0x13, 0x13, 0x13);
    pub const SURFACE_LOW: Rgb = Rgb(0x1c, 0x1b, 0x1b);
    pub const SURFACE_HIGH: Rgb = Rgb(0x2a, 0x2a, 0x2a);
    pub const SURFACE_HIGHEST: Rgb = Rgb(0x35, 0x35, 0x34);
    pub const ON_SURFACE: Rgb = Rgb(0xe5, 0xe2, 0xe1);
    pub const ON_SURFACE_VARIANT: Rgb = Rgb(0xd0, 0xc5, 0xaf);
    pub const OUTLINE: Rgb = Rgb(0x99, 0x90, 0x7c);
    pub const OUTLINE_VARIANT: Rgb = Rgb(0x4d, 0x46, 0x35);
    pub const GOLD: Rgb = Rgb(0xf2, 0xca, 0x50);
    pub const GOLD_MATTE: Rgb = Rgb(0xd4, 0xaf, 0x37);
    pub const ON_GOLD: Rgb = Rgb(0x3c, 0x2f, 0x00);
    pub const NEON_GREEN: Rgb = Rgb(0x2f, 0xf8, 0x01);
    pub const GREEN_TEXT: Rgb = Rgb(0x79, 0xff, 0x5b);
    pub const ON_GREEN: Rgb = Rgb(0x05, 0x39, 0x00);
    pub const HOT_PINK: Rgb = Rgb(0xff, 0x8f, 0xc2);
    pub const PINK_TEXT: Rgb = Rgb(0xff, 0xbb, 0xd6);
    pub const ERROR: Rgb = Rgb(0xff, 0xb4, 0xab);

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum SemanticTone {
        Neutral,
        Primary,
        Live,
        Interrupt,
        Error,
        Muted,
    }

    pub const fn tone_rgb(tone: SemanticTone) -> Rgb {
        match tone {
            SemanticTone::Neutral => ON_SURFACE,
            SemanticTone::Primary => GOLD,
            SemanticTone::Live => GREEN_TEXT,
            SemanticTone::Interrupt => PINK_TEXT,
            SemanticTone::Error => ERROR,
            SemanticTone::Muted => OUTLINE,
        }
    }

    pub const fn task_tone(status: TaskStatus) -> SemanticTone {
        match status {
            TaskStatus::Running | TaskStatus::Dispatched => SemanticTone::Live,
            TaskStatus::Completed => SemanticTone::Primary,
            TaskStatus::Failed | TaskStatus::Lost => SemanticTone::Error,
            TaskStatus::Suspended | TaskStatus::Cancelled => SemanticTone::Interrupt,
            TaskStatus::Prepared | TaskStatus::Admitted => SemanticTone::Muted,
        }
    }

    pub const fn presence_tone(state: TeamPresenceState) -> SemanticTone {
        match state {
            TeamPresenceState::Online => SemanticTone::Live,
            TeamPresenceState::Away => SemanticTone::Primary,
            TeamPresenceState::Offline => SemanticTone::Muted,
        }
    }

    pub const fn work_item_tone(status: TeamWorkItemStatus) -> SemanticTone {
        match status {
            TeamWorkItemStatus::InProgress => SemanticTone::Live,
            TeamWorkItemStatus::Completed => SemanticTone::Primary,
            TeamWorkItemStatus::Blocked => SemanticTone::Interrupt,
            TeamWorkItemStatus::Cancelled => SemanticTone::Error,
            TeamWorkItemStatus::Open => SemanticTone::Muted,
        }
    }

    pub fn connection_tone(connection: &str) -> SemanticTone {
        let normalized = connection.to_ascii_lowercase();
        if normalized.contains("connected") && !normalized.contains("disconnected") {
            SemanticTone::Live
        } else if normalized.contains("error")
            || normalized.contains("failed")
            || normalized.contains("stopped")
        {
            SemanticTone::Error
        } else if normalized.contains("reconnect") || normalized.contains("disconnect") {
            SemanticTone::Interrupt
        } else {
            SemanticTone::Muted
        }
    }

    pub fn notice_tone(notice: &str) -> SemanticTone {
        let normalized = notice.to_ascii_lowercase();
        if normalized.contains("error")
            || normalized.contains("failed")
            || normalized.contains("full")
            || normalized.contains("stopped")
            || normalized.contains("reject")
        {
            SemanticTone::Error
        } else if normalized.contains("queued")
            || normalized.contains("created")
            || normalized.contains("accepted")
        {
            SemanticTone::Live
        } else if normalized == "idle" {
            SemanticTone::Muted
        } else {
            SemanticTone::Primary
        }
    }

    pub fn event_tone(event: &EventEnvelope) -> SemanticTone {
        match &event.event {
            Event::TaskStatus { status, .. } => task_tone(*status),
            Event::ProviderState { health, .. } => match health {
                ServiceHealth::Ready => SemanticTone::Live,
                ServiceHealth::Starting | ServiceHealth::Draining => SemanticTone::Primary,
                ServiceHealth::Degraded => SemanticTone::Interrupt,
                ServiceHealth::Stopped | ServiceHealth::Failed => SemanticTone::Error,
            },
            Event::Overload { .. } => SemanticTone::Error,
            Event::CompletionEvaluated {
                mandatory_passed, ..
            } => {
                if *mandatory_passed {
                    SemanticTone::Live
                } else {
                    SemanticTone::Error
                }
            }
            Event::Observation { .. }
            | Event::EvidenceRecorded { .. }
            | Event::ProviderOutput { .. }
            | Event::ArtifactAvailable { .. } => SemanticTone::Live,
            Event::TeamWorkItemCreated { work_item } | Event::TeamWorkItemUpdated { work_item } => {
                work_item_tone(work_item.status)
            }
            Event::TeamPresenceSet { presence } => presence_tone(presence.state),
            Event::EngagementAccepted { .. }
            | Event::PlanAccepted { .. }
            | Event::ExerciseCreated { .. }
            | Event::OperationRunCreated { .. }
            | Event::OperatorSessionCreated { .. }
            | Event::PlaybookCreated { .. } => SemanticTone::Primary,
            _ => SemanticTone::Neutral,
        }
    }
}

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
    pub team: TeamProjection,
    pub events: VecDeque<EventEnvelope>,
    pub outputs: VecDeque<OperatorOutput>,
    pub task_graphs: HashMap<EngagementId, TaskGraphProjection>,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancellableTask {
    pub engagement_id: EngagementId,
    pub task_id: TaskId,
    pub request_id: RequestId,
    pub objective: String,
    pub mode: ExecutionMode,
    pub status: TaskStatus,
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
            team: TeamProjection {
                team_id: TeamId::from_string("local-team"),
                ..TeamProjection::default()
            },
            events: VecDeque::new(),
            outputs: VecDeque::new(),
            task_graphs: HashMap::new(),
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
        self.task_graphs
            .get(&output.engagement_id)
            .and_then(|graph| graph.operator_session_id.as_ref())
            == Some(selected)
    }

    pub fn engagement_is_in_selected_session(&self, engagement_id: &EngagementId) -> bool {
        let Some(selected) = self.selection.session_id.as_ref() else {
            return false;
        };
        self.task_graphs
            .get(engagement_id)
            .and_then(|graph| graph.operator_session_id.as_ref())
            == Some(selected)
    }

    pub fn selected_task_graphs(&self) -> Vec<&TaskGraphProjection> {
        let mut graphs = self
            .task_graphs
            .values()
            .filter(|graph| self.engagement_is_in_selected_session(&graph.engagement_id))
            .collect::<Vec<_>>();
        graphs.sort_by_key(|graph| graph.last_sequence);
        graphs
    }

    /// Returns only active tasks whose durable receipt agrees with the graph.
    /// The identity check is fail-closed so a corrupted projection can never
    /// cause an operator surface to cancel a different request.
    pub fn cancellable_tasks(&self) -> Vec<CancellableTask> {
        let mut cancellable = Vec::new();
        for graph in self.selected_task_graphs().into_iter().rev() {
            for node in graph.tasks.iter().rev() {
                if !matches!(node.status, TaskStatus::Dispatched | TaskStatus::Running) {
                    continue;
                }
                let Some(receipt) = node.execution.as_ref() else {
                    continue;
                };
                if receipt.engagement_id() != &graph.engagement_id
                    || receipt.task_id() != &node.task.task_id
                {
                    continue;
                }
                cancellable.push(CancellableTask {
                    engagement_id: graph.engagement_id.clone(),
                    task_id: node.task.task_id.clone(),
                    request_id: receipt.request_id().clone(),
                    objective: node.task.objective.clone(),
                    mode: node.task.mode,
                    status: node.status,
                });
            }
        }
        cancellable
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
            OperatorUpdate::Team(team) => self.team = team,
            OperatorUpdate::Output(output) => {
                self.outputs.push_back(output);
                while self.outputs.len() > MAX_VISIBLE_OUTPUTS {
                    self.outputs.pop_front();
                }
            }
            OperatorUpdate::TaskGraph(graph) => {
                self.task_graphs.insert(graph.engagement_id.clone(), graph);
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
            xai_grok_protocol::Event::TeamPresenceSet { presence } => {
                if let Some(known) = self.state_team_presence_mut(presence) {
                    *known = presence.clone();
                } else if presence.client.team_id == self.team.team_id {
                    self.team.presence.push(presence.clone());
                }
            }
            xai_grok_protocol::Event::TeamWorkItemCreated { work_item }
            | xai_grok_protocol::Event::TeamWorkItemUpdated { work_item } => {
                if work_item.team_id == self.team.team_id {
                    if let Some(known) = self
                        .team
                        .work_items
                        .iter_mut()
                        .find(|known| known.work_item_id == work_item.work_item_id)
                    {
                        *known = work_item.clone();
                    } else {
                        self.team.work_items.push(work_item.clone());
                    }
                }
            }
            xai_grok_protocol::Event::TeamMessagePosted { message } => {
                if message.team_id == self.team.team_id
                    && !self
                        .team
                        .messages
                        .iter()
                        .any(|known| known.message_id == message.message_id)
                {
                    self.team.messages.push(message.clone());
                    if self.team.messages.len() > 500 {
                        self.team.messages.remove(0);
                    }
                }
            }
            xai_grok_protocol::Event::TeamResourceClaimed { claim }
            | xai_grok_protocol::Event::TeamResourceReleased { claim } => {
                if claim.team_id == self.team.team_id {
                    if let Some(known) = self
                        .team
                        .resource_claims
                        .iter_mut()
                        .find(|known| known.claim_id == claim.claim_id)
                    {
                        *known = claim.clone();
                    } else {
                        self.team.resource_claims.push(claim.clone());
                    }
                }
            }
            _ => {}
        }
    }

    fn state_team_presence_mut(
        &mut self,
        presence: &xai_grok_protocol::TeamPresence,
    ) -> Option<&mut xai_grok_protocol::TeamPresence> {
        self.team.presence.iter_mut().find(|known| {
            known.client.team_id == presence.client.team_id
                && known.client.client_id == presence.client.client_id
        })
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
    CancelTask {
        engagement_id: EngagementId,
        task_id: TaskId,
        request_id: RequestId,
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
            Self::CancelTask {
                engagement_id,
                task_id,
                request_id,
            } => Command::CancelTask {
                engagement_id,
                task_id,
                request_id,
            },
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
    Team(TeamProjection),
    Output(OperatorOutput),
    TaskGraph(TaskGraphProjection),
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
        let team_client = TeamClient {
            team_id: config.team_id.clone(),
            client_id: config.client_id.clone(),
            display_name: config.display_name.clone(),
        };
        if let Err(error) = set_presence(
            &control,
            team_client.clone(),
            TeamPresenceState::Online,
            config.workspace_filter.clone(),
            None,
            None,
            None,
        )
        .await
        {
            let _ = updates.send(OperatorUpdate::Notice(format!(
                "failed to publish team presence: {error}"
            )));
        }
        if refresh(
            &control,
            &updates,
            config.workspace_filter.clone(),
            config.team_id.clone(),
        )
        .await
        .is_err()
        {
            continue;
        }
        let mut next_projection = tokio::time::Instant::now() + Duration::from_secs(2);
        let mut next_presence = tokio::time::Instant::now() + Duration::from_secs(10);
        'connected: loop {
            if stop.load(Ordering::Acquire) {
                let _ = set_presence(
                    &control,
                    team_client.clone(),
                    TeamPresenceState::Offline,
                    config.workspace_filter.clone(),
                    None,
                    None,
                    None,
                )
                .await;
                return;
            }
            while let Ok(command) = commands.try_recv() {
                match submit(&control, command, config.source, team_client.clone()).await {
                    Ok(notice) => {
                        let _ = updates.send(OperatorUpdate::Notice(notice));
                        if refresh(
                            &control,
                            &updates,
                            config.workspace_filter.clone(),
                            config.team_id.clone(),
                        )
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
                    let graph_engagements = batch
                        .events
                        .iter()
                        .filter_map(|event| {
                            matches!(
                                event.event,
                                xai_grok_protocol::Event::PlanAccepted { .. }
                                    | xai_grok_protocol::Event::TaskStatus { .. }
                                    | xai_grok_protocol::Event::Observation { .. }
                                    | xai_grok_protocol::Event::ArtifactAvailable {
                                        task_id: Some(_),
                                        ..
                                    }
                                    | xai_grok_protocol::Event::ProviderOutput { .. }
                                    | xai_grok_protocol::Event::CompletionEvaluated { .. }
                            )
                            .then(|| event.engagement_id.clone())
                            .flatten()
                        })
                        .collect::<std::collections::BTreeSet<_>>();
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
                    for engagement_id in graph_engagements {
                        match read_task_graph(&control, engagement_id).await {
                            Ok(Some(graph)) => {
                                if updates.send(OperatorUpdate::TaskGraph(graph)).is_err() {
                                    return;
                                }
                            }
                            Ok(None) => {}
                            Err(error) => {
                                let _ = updates.send(OperatorUpdate::Notice(format!(
                                    "failed to read task graph: {error}"
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
                if let Err(error) = refresh(
                    &control,
                    &updates,
                    config.workspace_filter.clone(),
                    config.team_id.clone(),
                )
                .await
                {
                    let _ =
                        updates.send(OperatorUpdate::Connection(format!("reconnecting: {error}")));
                    break;
                }
                next_projection = tokio::time::Instant::now() + Duration::from_secs(2);
            }
            if tokio::time::Instant::now() >= next_presence {
                if set_presence(
                    &control,
                    team_client.clone(),
                    TeamPresenceState::Online,
                    config.workspace_filter.clone(),
                    None,
                    None,
                    None,
                )
                .await
                .is_err()
                {
                    break;
                }
                next_presence = tokio::time::Instant::now() + Duration::from_secs(10);
            }
        }
    }
}

async fn read_task_graph(
    control: &ControlPlaneClient,
    engagement_id: EngagementId,
) -> Result<Option<TaskGraphProjection>, Box<dyn std::error::Error>> {
    let response = control
        .send(
            Command::QueryProjection(ProjectionQuery::TaskGraph { engagement_id }),
            Duration::from_secs(2),
        )
        .await?;
    let Response::Projection(snapshot) = response else {
        return Err("daemon returned an unexpected task graph response".into());
    };
    if snapshot.value.is_null() {
        return Ok(None);
    }
    Ok(Some(serde_json::from_value(snapshot.value)?))
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
    team_client: TeamClient,
) -> Result<String, Box<dyn std::error::Error>> {
    if let OperatorCommand::SubmitTurn {
        workspace_id,
        exercise_id,
        operation_run_id,
        session_id,
        ..
    } = &command
    {
        set_presence(
            control,
            team_client,
            TeamPresenceState::Online,
            Some(workspace_id.clone()),
            Some(exercise_id.clone()),
            operation_run_id.clone(),
            Some(session_id.clone()),
        )
        .await?;
    }
    let cancelled_task = match &command {
        OperatorCommand::CancelTask { task_id, .. } => Some(task_id.clone()),
        _ => None,
    };
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
        Response::Ack if cancelled_task.is_some() => format!(
            "cancelled task {}",
            cancelled_task.expect("checked cancellation task").as_str()
        ),
        _ => return Err("daemon returned an unexpected operator response".into()),
    };
    Ok(notice)
}

async fn set_presence(
    control: &ControlPlaneClient,
    client: TeamClient,
    state: TeamPresenceState,
    workspace_id: Option<WorkspaceId>,
    exercise_id: Option<ExerciseId>,
    operation_run_id: Option<OperationRunId>,
    session_id: Option<OperatorSessionId>,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = control
        .send(
            Command::SetTeamPresence(SetTeamPresence {
                client,
                state,
                workspace_id,
                exercise_id,
                operation_run_id,
                session_id,
            }),
            Duration::from_secs(2),
        )
        .await?;
    if matches!(response, Response::TeamPresenceSet { .. }) {
        Ok(())
    } else {
        Err("daemon returned an unexpected team presence response".into())
    }
}

async fn refresh(
    control: &ControlPlaneClient,
    updates: &mpsc::SyncSender<OperatorUpdate>,
    workspace_filter: Option<WorkspaceId>,
    team_id: TeamId,
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
    let team = control
        .send(
            Command::QueryProjection(ProjectionQuery::Team { team_id }),
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
    if let Response::Projection(snapshot) = team {
        updates.send(OperatorUpdate::Team(serde_json::from_value(
            snapshot.value,
        )?))?;
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
        xai_grok_protocol::Event::TeamPresenceSet { .. } => "team presence",
        xai_grok_protocol::Event::TeamWorkItemCreated { .. } => "team work created",
        xai_grok_protocol::Event::TeamWorkItemUpdated { .. } => "team work updated",
        xai_grok_protocol::Event::TeamMessagePosted { .. } => "team message",
        xai_grok_protocol::Event::TeamResourceClaimed { .. } => "team resource claimed",
        xai_grok_protocol::Event::TeamResourceReleased { .. } => "team resource released",
        xai_grok_protocol::Event::EngagementAccepted { .. } => "turn accepted",
        xai_grok_protocol::Event::PlanAccepted { .. } => "plan accepted",
        xai_grok_protocol::Event::TaskStatus { .. } => "task status",
        xai_grok_protocol::Event::Observation { .. } => "observation",
        xai_grok_protocol::Event::ProviderState { .. } => "provider state",
        xai_grok_protocol::Event::ArtifactAvailable { .. } => "artifact available",
        xai_grok_protocol::Event::ProviderOutput { .. } => "provider output",
        xai_grok_protocol::Event::CompletionEvaluated { .. } => "completion evaluated",
        xai_grok_protocol::Event::Overload { .. } => "overload",
    }
}

pub fn event_summary(event: &EventEnvelope) -> String {
    match &event.event {
        xai_grok_protocol::Event::TaskStatus {
            task_id,
            status,
            provider_id,
            ..
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
        xai_grok_protocol::Event::CompletionEvaluated {
            task_id,
            mandatory_passed,
            results,
        } => format!(
            "task {} completion {} ({} criteria)",
            task_id.as_str(),
            if *mandatory_passed {
                "passed"
            } else {
                "failed"
            },
            results.len()
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
        xai_grok_protocol::Event::TeamPresenceSet { presence } => format!(
            "{} {:?}",
            presence
                .client
                .display_name
                .as_deref()
                .unwrap_or(presence.client.client_id.as_str()),
            presence.state
        ),
        xai_grok_protocol::Event::TeamWorkItemCreated { work_item }
        | xai_grok_protocol::Event::TeamWorkItemUpdated { work_item } => format!(
            "team work {} {:?}: {}",
            work_item.work_item_id, work_item.status, work_item.title
        ),
        xai_grok_protocol::Event::TeamMessagePosted { message } => format!(
            "{} in {}: {}",
            message.sender, message.channel_id, message.body
        ),
        xai_grok_protocol::Event::TeamResourceClaimed { claim } => {
            format!("{} claimed {}", claim.owner, claim.resource_key)
        }
        xai_grok_protocol::Event::TeamResourceReleased { claim } => {
            format!("{} released {}", claim.owner, claim.resource_key)
        }
        _ => event_name(event).to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use xai_grok_protocol::{
        CapabilityRequirement, DeferredTask, Event, EventId, ExecutionReceipt, ExecutionTask,
        Exercise, ExerciseStatus, OperationRun, OperationRunStatus, OperatorSession,
        OperatorSessionStatus, PROTOCOL_VERSION, TaskProjection, TaskStatus,
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
    fn task_graphs_are_scoped_to_the_selected_real_session() {
        let mut state = OperatorState::default();
        state.apply(OperatorUpdate::Catalog(catalog()));
        let engagement_id = EngagementId::from_string("engagement-a");
        state.apply(OperatorUpdate::Events {
            events: vec![EventEnvelope {
                protocol_version: PROTOCOL_VERSION,
                event_id: EventId::new(),
                engagement_id: Some(engagement_id.clone()),
                sequence: 1,
                causation_id: None,
                generation: 0,
                observed_unix_ms: 1,
                event: Event::EngagementAccepted {
                    workspace_id: "workspace-a".to_owned(),
                    session_id: "session-a".to_owned(),
                    exercise_id: Some(ExerciseId::from_string("exercise-a")),
                    operation_run_id: Some(OperationRunId::from_string("run-a")),
                    operator_session_id: Some(OperatorSessionId::from_string("session-a")),
                    team_id: None,
                    client_id: None,
                },
            }],
            cursor: 1,
        });
        state.apply(OperatorUpdate::TaskGraph(TaskGraphProjection {
            engagement_id: engagement_id.clone(),
            workspace_id: Some("workspace-a".to_owned()),
            session_id: Some("session-a".to_owned()),
            exercise_id: Some(ExerciseId::from_string("exercise-a")),
            operation_run_id: Some(OperationRunId::from_string("run-a")),
            operator_session_id: Some(OperatorSessionId::from_string("session-a")),
            team_id: None,
            client_id: None,
            revision: 1,
            objective: "Inventory hosts".to_owned(),
            tasks: Vec::new(),
            last_sequence: 1,
        }));
        state.events.clear();
        assert_eq!(state.selected_task_graphs().len(), 1);
        assert_eq!(state.selected_task_graphs()[0].engagement_id, engagement_id);

        state.selection.session_id = Some(OperatorSessionId::from_string("session-other"));
        assert!(state.selected_task_graphs().is_empty());
    }

    #[test]
    fn cancellable_tasks_require_a_matching_durable_active_receipt() {
        let mut state = OperatorState::default();
        state.apply(OperatorUpdate::Catalog(catalog()));
        let engagement_id = EngagementId::from_string("engagement-a");
        let task = |task_id: &str| ExecutionTask {
            task_id: TaskId::from_string(task_id),
            objective: format!("execute {task_id}"),
            mode: ExecutionMode::Deferred,
            capability: CapabilityRequirement {
                operation_id: "test.execute".into(),
                preferred_provider: Some("provider-a".into()),
                required_features: Vec::new(),
            },
            input: serde_json::json!({}),
            deadline_unix_ms: 10_000,
            completion_tests: Vec::new(),
            depends_on: Vec::new(),
        };
        let receipt = |task_id: &str, receipt_task_id: &str| {
            ExecutionReceipt::Deferred(DeferredTask {
                request_id: RequestId::from_string(format!("request-{task_id}")),
                engagement_id: engagement_id.clone(),
                task_id: TaskId::from_string(receipt_task_id),
                provider_id: "provider-a".into(),
                lease_epoch: 7,
                result_cursor: 0,
            })
        };
        state.apply(OperatorUpdate::TaskGraph(TaskGraphProjection {
            engagement_id: engagement_id.clone(),
            workspace_id: Some("workspace-a".to_owned()),
            session_id: Some("session-a".to_owned()),
            exercise_id: Some(ExerciseId::from_string("exercise-a")),
            operation_run_id: Some(OperationRunId::from_string("run-a")),
            operator_session_id: Some(OperatorSessionId::from_string("session-a")),
            team_id: None,
            client_id: None,
            revision: 1,
            objective: "exercise durable cancellation".to_owned(),
            tasks: vec![
                TaskProjection {
                    task: task("active"),
                    status: TaskStatus::Running,
                    provider_id: Some("provider-a".into()),
                    execution: Some(receipt("active", "active")),
                    observations: Vec::new(),
                    artifacts: Vec::new(),
                    completion: None,
                    last_sequence: 4,
                },
                TaskProjection {
                    task: task("terminal"),
                    status: TaskStatus::Completed,
                    provider_id: Some("provider-a".into()),
                    execution: Some(receipt("terminal", "terminal")),
                    observations: Vec::new(),
                    artifacts: Vec::new(),
                    completion: None,
                    last_sequence: 5,
                },
                TaskProjection {
                    task: task("mismatch"),
                    status: TaskStatus::Running,
                    provider_id: Some("provider-a".into()),
                    execution: Some(receipt("mismatch", "different-task")),
                    observations: Vec::new(),
                    artifacts: Vec::new(),
                    completion: None,
                    last_sequence: 6,
                },
            ],
            last_sequence: 6,
        }));

        let cancellable = state.cancellable_tasks();
        assert_eq!(cancellable.len(), 1);
        assert_eq!(cancellable[0].task_id.as_str(), "active");
        assert_eq!(cancellable[0].request_id.as_str(), "request-active");
        assert_eq!(cancellable[0].mode, ExecutionMode::Deferred);
        assert!(matches!(
            OperatorCommand::CancelTask {
                engagement_id: cancellable[0].engagement_id.clone(),
                task_id: cancellable[0].task_id.clone(),
                request_id: cancellable[0].request_id.clone(),
            }
            .into_protocol(IngressSource::Tui),
            Command::CancelTask { request_id, .. }
                if request_id.as_str() == "request-active"
        ));
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

    #[test]
    fn operator_theme_preserves_the_locked_reference_tokens() {
        assert_eq!(theme::OBSIDIAN.tuple(), (0x08, 0x08, 0x08));
        assert_eq!(theme::GOLD.tuple(), (0xf2, 0xca, 0x50));
        assert_eq!(theme::NEON_GREEN.tuple(), (0x2f, 0xf8, 0x01));
        assert_eq!(theme::HOT_PINK.tuple(), (0xff, 0x8f, 0xc2));
    }

    #[test]
    fn operator_theme_assigns_operational_state_semantically() {
        assert_eq!(
            theme::task_tone(TaskStatus::Running),
            theme::SemanticTone::Live
        );
        assert_eq!(
            theme::task_tone(TaskStatus::Suspended),
            theme::SemanticTone::Interrupt
        );
        assert_eq!(
            theme::task_tone(TaskStatus::Failed),
            theme::SemanticTone::Error
        );
        assert_eq!(
            theme::connection_tone("connected to grokd"),
            theme::SemanticTone::Live
        );
        assert_eq!(
            theme::notice_tone("local command queue is full"),
            theme::SemanticTone::Error
        );
    }
}
