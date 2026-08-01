use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use eframe::egui;
use xai_grok_operator_core::{
    OperatorClientConfig, OperatorCommand, OperatorState, OperatorUpdate, event_name,
    parse_scope_targets, spawn_client_worker,
};
use xai_grok_protocol::{ClientId, IngressSource, TeamId};

#[derive(Clone, Debug)]
struct Arguments {
    socket: PathBuf,
    team_id: TeamId,
    client_id: ClientId,
    display_name: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DialogKind {
    Exercise,
    Run,
    Session,
}

struct OperatorApp {
    state: OperatorState,
    updates: mpsc::Receiver<OperatorUpdate>,
    commands: mpsc::SyncSender<OperatorCommand>,
    stop: Arc<AtomicBool>,
    dialog: Option<DialogKind>,
    workspace: String,
    name: String,
    objective: String,
    scope: String,
    request: String,
}

impl OperatorApp {
    fn new(arguments: Arguments) -> Self {
        let (updates_tx, updates) = mpsc::sync_channel(128);
        let (commands, command_rx) = mpsc::sync_channel(16);
        let stop = Arc::new(AtomicBool::new(false));
        spawn_client_worker(
            OperatorClientConfig {
                socket: arguments.socket,
                team_id: arguments.team_id,
                client_id: arguments.client_id,
                display_name: arguments.display_name,
                client_name: "grok-ui".to_owned(),
                source: IngressSource::Gui,
                workspace_filter: None,
            },
            updates_tx,
            command_rx,
            stop.clone(),
        );
        Self {
            state: OperatorState::default(),
            updates,
            commands,
            stop,
            dialog: None,
            workspace: "default".to_owned(),
            name: String::new(),
            objective: String::new(),
            scope: String::new(),
            request: String::new(),
        }
    }

    fn receive_updates(&mut self) {
        while let Ok(update) = self.updates.try_recv() {
            self.state.apply(update);
        }
        if self.state.first_run() && self.dialog.is_none() {
            self.dialog = Some(DialogKind::Exercise);
        }
        if !self.state.first_run() && self.dialog == Some(DialogKind::Exercise) {
            self.dialog = None;
            self.clear_dialog();
        }
    }

    fn queue(&mut self, command: OperatorCommand) -> bool {
        match self.commands.try_send(command) {
            Ok(()) => {
                self.state.notice = "queued".to_owned();
                true
            }
            Err(mpsc::TrySendError::Full(_)) => {
                self.state.notice = "local command queue is full".to_owned();
                false
            }
            Err(mpsc::TrySendError::Disconnected(_)) => {
                self.state.notice = "operator client stopped".to_owned();
                false
            }
        }
    }

    fn clear_dialog(&mut self) {
        self.name.clear();
        self.objective.clear();
        self.scope.clear();
    }

    fn show_catalog(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.heading("Exercises");
            if ui.small_button("+").on_hover_text("New exercise").clicked() {
                self.dialog = Some(DialogKind::Exercise);
            }
        });
        ui.separator();
        let exercises = self.state.catalog.exercises.clone();
        for exercise in exercises {
            let selected = self.state.selection.exercise_id.as_ref() == Some(&exercise.exercise_id);
            if ui
                .selectable_label(selected, format!("◆ {}", exercise.name))
                .clicked()
            {
                self.state.select_exercise(exercise.exercise_id.clone());
            }
            let runs = self
                .state
                .catalog
                .operation_runs
                .iter()
                .filter(|run| run.exercise_id == exercise.exercise_id)
                .cloned()
                .collect::<Vec<_>>();
            for run in runs {
                let selected =
                    self.state.selection.operation_run_id.as_ref() == Some(&run.operation_run_id);
                ui.horizontal(|ui| {
                    ui.add_space(12.0);
                    if ui
                        .selectable_label(selected, format!("├─ {}", run.name))
                        .clicked()
                    {
                        self.state
                            .select_operation_run(run.operation_run_id.clone());
                    }
                });
                let sessions = self
                    .state
                    .catalog
                    .sessions
                    .iter()
                    .filter(|session| {
                        session.operation_run_id.as_ref() == Some(&run.operation_run_id)
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                for session in sessions {
                    self.session_row(ui, &session);
                }
            }
            let unbound_sessions = self
                .state
                .catalog
                .sessions
                .iter()
                .filter(|session| {
                    session.exercise_id == exercise.exercise_id
                        && session.operation_run_id.is_none()
                })
                .cloned()
                .collect::<Vec<_>>();
            for session in unbound_sessions {
                self.session_row(ui, &session);
            }
        }
        if self.state.catalog.exercises.is_empty() {
            ui.label("No exercises yet.");
        }
        ui.separator();
        ui.horizontal_wrapped(|ui| {
            if ui
                .add_enabled(
                    self.state.selected_exercise().is_some(),
                    egui::Button::new("New run"),
                )
                .clicked()
            {
                self.dialog = Some(DialogKind::Run);
            }
            if ui
                .add_enabled(
                    self.state.selected_exercise().is_some(),
                    egui::Button::new("New session"),
                )
                .clicked()
            {
                self.dialog = Some(DialogKind::Session);
            }
        });
    }

    fn session_row(&mut self, ui: &mut egui::Ui, session: &xai_grok_protocol::OperatorSession) {
        let selected = self.state.selection.session_id.as_ref() == Some(&session.session_id);
        ui.horizontal(|ui| {
            ui.add_space(28.0);
            if ui
                .selectable_label(selected, format!("└─ {}", session.name))
                .clicked()
            {
                self.state.select_session(session.session_id.clone());
            }
        });
    }

    fn show_activity(&mut self, ui: &mut egui::Ui) {
        if let Some(session) = self.state.selected_session() {
            ui.heading(format!("Session: {}", session.name));
            ui.label(&session.purpose);
            ui.add(
                egui::TextEdit::multiline(&mut self.request)
                    .desired_rows(4)
                    .hint_text("Give the agent work within this session"),
            );
            let can_submit = !self.request.trim().is_empty();
            if ui
                .add_enabled(can_submit, egui::Button::new("Send to session"))
                .clicked()
            {
                let session = session.clone();
                let exercise = self
                    .state
                    .selected_exercise()
                    .expect("selected session has an exercise")
                    .clone();
                let command = OperatorCommand::SubmitTurn {
                    workspace_id: exercise.workspace_id,
                    exercise_id: exercise.exercise_id,
                    operation_run_id: session.operation_run_id,
                    session_id: session.session_id,
                    request: self.request.trim().to_owned(),
                };
                if self.queue(command) {
                    self.request.clear();
                }
            }
        } else {
            ui.heading("Choose an execution lane");
            ui.label(
                "Create or select a session. Sessions are separate model/context lanes inside an exercise; they are not new engagements.",
            );
        }
        ui.separator();
        ui.heading("Live activity");
        egui::ScrollArea::vertical()
            .stick_to_bottom(true)
            .show(ui, |ui| {
                for event in &self.state.events {
                    ui.horizontal_wrapped(|ui| {
                        ui.monospace(format!("{:>8}", event.sequence));
                        ui.label(event_name(event));
                        if let Some(engagement_id) = &event.engagement_id {
                            ui.monospace(engagement_id.as_str());
                        }
                    });
                }
            });
    }

    fn show_context(&self, ui: &mut egui::Ui) {
        ui.heading("Operational context");
        if let Some(exercise) = self.state.selected_exercise() {
            ui.strong(&exercise.name);
            ui.label(&exercise.objective);
            ui.monospace(exercise.exercise_id.as_str());
        }
        if let Some(run) = self.state.selected_operation_run() {
            ui.separator();
            ui.strong(format!("Run: {}", run.name));
            ui.label(&run.objective);
            ui.monospace(run.operation_run_id.as_str());
        }
        if let Some(session) = self.state.selected_session() {
            ui.separator();
            ui.strong(format!("Session: {}", session.name));
            ui.label(&session.purpose);
            ui.monospace(session.session_id.as_str());
        }
        ui.separator();
        ui.heading("Capacity");
        ui.monospace(pretty_json(&self.state.capacity));
        ui.separator();
        ui.heading("Providers");
        ui.monospace(pretty_json(&self.state.providers));
    }

    fn show_dialog(&mut self, context: &egui::Context) {
        let Some(kind) = self.dialog else {
            return;
        };
        let first_run = self.state.first_run();
        let title = match kind {
            DialogKind::Exercise if first_run => "Create the first exercise",
            DialogKind::Exercise => "Create exercise",
            DialogKind::Run => "Create operation run",
            DialogKind::Session => "Create operator session",
        };
        egui::Window::new(title)
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(context, |ui| {
                if kind == DialogKind::Exercise {
                    ui.label("Workspace");
                    ui.text_edit_singleline(&mut self.workspace);
                }
                ui.label(match kind {
                    DialogKind::Exercise => "Exercise name",
                    DialogKind::Run => "Run name",
                    DialogKind::Session => "Session name",
                });
                ui.text_edit_singleline(&mut self.name);
                ui.label(match kind {
                    DialogKind::Session => "Purpose",
                    _ => "Objective",
                });
                ui.add(egui::TextEdit::multiline(&mut self.objective).desired_rows(3));
                if kind == DialogKind::Exercise {
                    ui.label("Initial scope (comma/newline separated; prefix ! to exclude)");
                    ui.add(
                        egui::TextEdit::multiline(&mut self.scope)
                            .desired_rows(2)
                            .hint_text("10.10.4.0/24, !10.10.4.9, portal.internal"),
                    );
                }
                let fields_valid = !self.name.trim().is_empty()
                    && !self.objective.trim().is_empty()
                    && (kind != DialogKind::Exercise
                        || (!self.workspace.trim().is_empty() && !self.scope.trim().is_empty()));
                ui.horizontal(|ui| {
                    if ui
                        .add_enabled(fields_valid, egui::Button::new("Create"))
                        .clicked()
                    {
                        let command = match kind {
                            DialogKind::Exercise => OperatorCommand::CreateExercise {
                                workspace_id: self.workspace.trim().to_owned(),
                                name: self.name.trim().to_owned(),
                                objective: self.objective.trim().to_owned(),
                                scope: parse_scope_targets(&self.scope),
                            },
                            DialogKind::Run => {
                                let exercise = self
                                    .state
                                    .selected_exercise()
                                    .expect("run dialog requires an exercise");
                                OperatorCommand::CreateOperationRun {
                                    exercise_id: exercise.exercise_id.clone(),
                                    name: self.name.trim().to_owned(),
                                    objective: self.objective.trim().to_owned(),
                                }
                            }
                            DialogKind::Session => {
                                let exercise = self
                                    .state
                                    .selected_exercise()
                                    .expect("session dialog requires an exercise");
                                OperatorCommand::CreateSession {
                                    exercise_id: exercise.exercise_id.clone(),
                                    operation_run_id: self.state.selection.operation_run_id.clone(),
                                    name: self.name.trim().to_owned(),
                                    purpose: self.objective.trim().to_owned(),
                                }
                            }
                        };
                        if self.queue(command) {
                            self.dialog = None;
                            self.clear_dialog();
                        }
                    }
                    if !first_run && ui.button("Cancel").clicked() {
                        self.dialog = None;
                        self.clear_dialog();
                    }
                });
                if first_run {
                    ui.small(
                        "An exercise is the long-lived assessment boundary. Runs and sessions are created inside it.",
                    );
                }
            });
    }
}

impl Drop for OperatorApp {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
    }
}

impl eframe::App for OperatorApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        self.receive_updates();
        egui::Panel::top("status").show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.heading("Grok operator");
                ui.separator();
                ui.label(&self.state.connection);
                ui.separator();
                ui.monospace(format!("event cursor {}", self.state.cursor));
                ui.separator();
                ui.label(&self.state.notice);
            });
        });
        egui::Panel::left("catalog")
            .resizable(true)
            .default_size(280.0)
            .show(ui, |ui| self.show_catalog(ui));
        egui::Panel::right("context")
            .resizable(true)
            .default_size(300.0)
            .show(ui, |ui| {
                egui::ScrollArea::vertical().show(ui, |ui| self.show_context(ui));
            });
        egui::CentralPanel::default().show(ui, |ui| self.show_activity(ui));
        self.show_dialog(ui.ctx());
        ui.ctx().request_repaint_after(Duration::from_millis(100));
    }
}

fn pretty_json(value: &serde_json::Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| "null".to_owned())
}

fn parse_arguments() -> Result<Arguments, String> {
    let mut socket = std::env::var_os("GROKD_SOCKET")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(".grok/grokd.sock"));
    let mut team_id = TeamId::from_string("local-team");
    let mut client_id = ClientId::new();
    let mut display_name = None;
    let mut arguments = std::env::args().skip(1);
    while let Some(argument) = arguments.next() {
        let mut value = || {
            arguments
                .next()
                .ok_or_else(|| format!("{argument} requires a value"))
        };
        match argument.as_str() {
            "--socket" => socket = PathBuf::from(value()?),
            "--team" => team_id = TeamId::from_string(value()?),
            "--client" => client_id = ClientId::from_string(value()?),
            "--display-name" => display_name = Some(value()?),
            "--help" | "-h" => {
                return Err(
                    "usage: grok-ui [--socket PATH] [--team ID] [--client ID] [--display-name NAME]"
                        .to_owned(),
                );
            }
            _ => return Err(format!("unknown option {argument:?}")),
        }
    }
    Ok(Arguments {
        socket,
        team_id,
        client_id,
        display_name,
    })
}

fn main() -> eframe::Result {
    let arguments =
        parse_arguments().map_err(|message| eframe::Error::AppCreation(message.into()))?;
    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1_240.0, 780.0])
            .with_min_inner_size([900.0, 560.0]),
        renderer: eframe::Renderer::Glow,
        ..Default::default()
    };
    eframe::run_native(
        "Grok operator",
        native_options,
        Box::new(move |_context| Ok(Box::new(OperatorApp::new(arguments)))),
    )
}
