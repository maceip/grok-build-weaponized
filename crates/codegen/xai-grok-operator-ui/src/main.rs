use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use eframe::egui;
use xai_grok_operator_core::{
    OperatorClientConfig, OperatorCommand, OperatorState, OperatorUpdate, event_summary,
    parse_scope_targets, spawn_client_worker,
    theme::{
        self as shared_theme, GOLD, GREEN_TEXT, HOT_PINK, OBSIDIAN, ON_GREEN, ON_SURFACE,
        ON_SURFACE_VARIANT, OUTLINE, PINK_TEXT, SURFACE, SURFACE_LOW, SemanticTone,
    },
};
use xai_grok_protocol::{ClientId, IngressSource, TeamId};

mod theme;

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
    fn new(arguments: Arguments, context: &egui::Context) -> Self {
        theme::configure(context);
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
        theme::section_heading(ui, "mission index", "Exercises");
        ui.horizontal(|ui| {
            if ui
                .add(
                    egui::Button::new(egui::RichText::new("+ NEW EXERCISE").monospace().strong())
                        .fill(theme::color(SURFACE_LOW))
                        .stroke(egui::Stroke::new(1.0, theme::color(GOLD)))
                        .corner_radius(egui::CornerRadius::ZERO),
                )
                .clicked()
            {
                self.dialog = Some(DialogKind::Exercise);
            }
        });
        ui.add_space(8.0);
        let exercises = self.state.catalog.exercises.clone();
        for exercise in exercises {
            let selected = self.state.selection.exercise_id.as_ref() == Some(&exercise.exercise_id);
            if ui
                .add_sized(
                    [ui.available_width(), 34.0],
                    egui::Button::new(
                        egui::RichText::new(format!("◆  {}", exercise.name.to_ascii_uppercase()))
                            .monospace()
                            .strong(),
                    )
                    .selected(selected)
                    .fill(if selected {
                        theme::color(GOLD)
                    } else {
                        theme::color(SURFACE)
                    })
                    .stroke(egui::Stroke::new(
                        if selected { 2.0 } else { 1.0 },
                        if selected {
                            theme::color(GREEN_TEXT)
                        } else {
                            theme::color(shared_theme::OUTLINE_VARIANT)
                        },
                    ))
                    .corner_radius(egui::CornerRadius::ZERO),
                )
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
                        .add_sized(
                            [ui.available_width(), 29.0],
                            egui::Button::new(
                                egui::RichText::new(format!("├─  {}", run.name))
                                    .monospace()
                                    .color(if selected {
                                        theme::color(GREEN_TEXT)
                                    } else {
                                        theme::color(ON_SURFACE_VARIANT)
                                    }),
                            )
                            .selected(selected)
                            .fill(theme::color(if selected { SURFACE_LOW } else { OBSIDIAN }))
                            .stroke(egui::Stroke::new(
                                1.0,
                                theme::color(if selected {
                                    shared_theme::NEON_GREEN
                                } else {
                                    shared_theme::OUTLINE_VARIANT
                                }),
                            ))
                            .corner_radius(egui::CornerRadius::ZERO),
                        )
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
            theme::deco_frame(SURFACE, SemanticTone::Muted).show(ui, |ui| {
                ui.label(
                    egui::RichText::new("NO EXERCISES — CREATE THE FIRST MISSION BOUNDARY")
                        .monospace()
                        .color(theme::color(OUTLINE)),
                );
            });
        }
        ui.add_space(12.0);
        ui.horizontal_wrapped(|ui| {
            if ui
                .add_enabled(
                    self.state.selected_exercise().is_some(),
                    egui::Button::new("NEW RUN")
                        .fill(theme::color(SURFACE_LOW))
                        .stroke(egui::Stroke::new(1.0, theme::color(GOLD)))
                        .corner_radius(egui::CornerRadius::ZERO),
                )
                .clicked()
            {
                self.dialog = Some(DialogKind::Run);
            }
            if ui
                .add_enabled(
                    self.state.selected_exercise().is_some(),
                    egui::Button::new("NEW SESSION")
                        .fill(theme::color(SURFACE_LOW))
                        .stroke(egui::Stroke::new(1.0, theme::color(GOLD)))
                        .corner_radius(egui::CornerRadius::ZERO),
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
                .add_sized(
                    [ui.available_width(), 27.0],
                    egui::Button::new(
                        egui::RichText::new(format!("└─  {}", session.name))
                            .monospace()
                            .color(if selected {
                                theme::color(ON_GREEN)
                            } else {
                                theme::color(ON_SURFACE_VARIANT)
                            }),
                    )
                    .selected(selected)
                    .fill(if selected {
                        theme::color(shared_theme::NEON_GREEN)
                    } else {
                        theme::color(OBSIDIAN)
                    })
                    .stroke(egui::Stroke::new(
                        1.0,
                        if selected {
                            theme::color(HOT_PINK)
                        } else {
                            theme::color(shared_theme::OUTLINE_VARIANT)
                        },
                    ))
                    .corner_radius(egui::CornerRadius::ZERO),
                )
                .clicked()
            {
                self.state.select_session(session.session_id.clone());
            }
        });
    }

    fn show_activity(&mut self, ui: &mut egui::Ui) {
        if let Some(session) = self.state.selected_session() {
            theme::section_heading(ui, "active execution lane", &session.name);
            ui.label(
                egui::RichText::new(&session.purpose)
                    .size(17.0)
                    .italics()
                    .color(theme::color(ON_SURFACE_VARIANT)),
            );
            ui.add_space(8.0);
            theme::deco_frame(SURFACE, SemanticTone::Primary).show(ui, |ui| {
                ui.label(
                    egui::RichText::new("OPERATOR DIRECTIVE")
                        .monospace()
                        .strong()
                        .color(theme::color(GREEN_TEXT)),
                );
                ui.add_sized(
                    [ui.available_width(), 92.0],
                    egui::TextEdit::multiline(&mut self.request)
                        .desired_rows(4)
                        .hint_text("Give the agent work within this session"),
                );
            });
            let can_submit = !self.request.trim().is_empty();
            if theme::action_button(ui, "Send to session", can_submit).clicked() {
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
            theme::section_heading(ui, "execution lane", "Select a session");
            theme::deco_frame(SURFACE, SemanticTone::Interrupt).show(ui, |ui| {
                ui.label(
                    egui::RichText::new(
                        "Create or select a session. Sessions are separate model/context lanes inside an exercise; they are not new engagements.",
                    )
                    .italics()
                    .color(theme::color(ON_SURFACE_VARIANT)),
                );
            });
        }
        ui.add_space(18.0);
        let cancellable = self.state.cancellable_tasks();
        let task_graphs = self
            .state
            .selected_task_graphs()
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        if !task_graphs.is_empty() {
            theme::section_heading(ui, "live control plane", "Execution graphs");
        }
        for graph in task_graphs {
            theme::deco_frame(SURFACE_LOW, SemanticTone::Primary).show(ui, |ui| {
                ui.horizontal_wrapped(|ui| {
                    theme::status_chip(
                        ui,
                        format!("revision {}", graph.revision),
                        SemanticTone::Primary,
                    );
                    ui.monospace(graph.engagement_id.as_str());
                });
                ui.label(
                    egui::RichText::new(&graph.objective)
                        .size(18.0)
                        .strong()
                        .color(theme::color(ON_SURFACE)),
                );
                ui.add_space(6.0);
                for node in &graph.tasks {
                    let tone = shared_theme::task_tone(node.status);
                    let cancel = cancellable
                        .iter()
                        .find(|task| {
                            task.engagement_id == graph.engagement_id
                                && task.task_id == node.task.task_id
                        })
                        .map(|task| OperatorCommand::CancelTask {
                            engagement_id: task.engagement_id.clone(),
                            task_id: task.task_id.clone(),
                            request_id: task.request_id.clone(),
                        });
                    theme::deco_frame(SURFACE, tone).show(ui, |ui| {
                        ui.horizontal_wrapped(|ui| {
                            theme::status_chip(ui, format!("{:?}", node.status), tone);
                            ui.monospace(node.task.task_id.as_str());
                            if let Some(provider) = &node.provider_id {
                                ui.label(
                                    egui::RichText::new(format!("VIA {}", provider.as_str()))
                                        .monospace()
                                        .color(theme::color(OUTLINE)),
                                );
                            }
                            if let Some(command) = cancel.clone()
                                && ui
                                    .add(
                                        egui::Button::new(
                                            egui::RichText::new("CANCEL TASK").monospace().strong(),
                                        )
                                        .fill(theme::color(SURFACE_LOW))
                                        .stroke(egui::Stroke::new(1.0, theme::color(HOT_PINK)))
                                        .corner_radius(egui::CornerRadius::ZERO),
                                    )
                                    .clicked()
                            {
                                self.queue(command);
                            }
                        });
                        ui.label(&node.task.objective);
                        if !node.task.depends_on.is_empty() {
                            ui.label(
                                egui::RichText::new(format!(
                                    "DEPENDS ON  {}",
                                    node.task
                                        .depends_on
                                        .iter()
                                        .map(|dependency| dependency.as_str())
                                        .collect::<Vec<_>>()
                                        .join(", ")
                                ))
                                .monospace()
                                .size(11.0)
                                .color(theme::color(OUTLINE)),
                            );
                        }
                        for observation in &node.observations {
                            ui.label(
                                egui::RichText::new(format!(
                                    "▸ {}",
                                    observation.observation.finding
                                ))
                                .color(theme::color(GREEN_TEXT)),
                            );
                        }
                        if !node.artifacts.is_empty() {
                            ui.label(
                                egui::RichText::new(format!(
                                    "{} ARTIFACT{}",
                                    node.artifacts.len(),
                                    if node.artifacts.len() == 1 { "" } else { "S" }
                                ))
                                .monospace()
                                .color(theme::color(GOLD)),
                            );
                        }
                    });
                }
            });
            ui.add_space(10.0);
        }
        let mut showed_output_heading = false;
        for output in self
            .state
            .outputs
            .iter()
            .filter(|output| self.state.output_is_in_selected_session(output))
        {
            if !showed_output_heading {
                theme::section_heading(ui, "model channel", "Agent output");
                showed_output_heading = true;
            }
            theme::deco_frame(SURFACE, SemanticTone::Live).show(ui, |ui| {
                theme::status_chip(ui, "agent", SemanticTone::Live);
                ui.label(egui::RichText::new(&output.text).color(theme::color(ON_SURFACE)));
                if output.truncated {
                    ui.label(
                        egui::RichText::new(format!(
                            "PREVIEW TRUNCATED · ARTIFACT {}",
                            output.artifact_id.as_str()
                        ))
                        .monospace()
                        .color(theme::color(HOT_PINK)),
                    );
                }
            });
            ui.add_space(8.0);
        }
        ui.add_space(12.0);
        theme::section_heading(ui, "streaming journal", "Live activity");
        egui::ScrollArea::vertical()
            .stick_to_bottom(true)
            .max_height(280.0)
            .show(ui, |ui| {
                for event in &self.state.events {
                    theme::deco_frame(SURFACE, shared_theme::event_tone(event)).show(ui, |ui| {
                        ui.horizontal_wrapped(|ui| {
                            ui.label(
                                egui::RichText::new(format!("#{:08}", event.sequence))
                                    .monospace()
                                    .color(theme::color(GOLD)),
                            );
                            ui.label(event_summary(event));
                            if let Some(engagement_id) = &event.engagement_id {
                                ui.label(
                                    egui::RichText::new(engagement_id.as_str())
                                        .monospace()
                                        .size(11.0)
                                        .color(theme::color(OUTLINE)),
                                );
                            }
                        });
                    });
                }
            });
    }

    fn show_context(&self, ui: &mut egui::Ui) {
        theme::section_heading(ui, "mission telemetry", "Operational context");
        if let Some(exercise) = self.state.selected_exercise() {
            theme::deco_frame(SURFACE, SemanticTone::Primary).show(ui, |ui| {
                theme::status_chip(ui, "exercise", SemanticTone::Primary);
                ui.label(
                    egui::RichText::new(&exercise.name)
                        .size(19.0)
                        .strong()
                        .color(theme::color(GOLD)),
                );
                ui.label(&exercise.objective);
                ui.label(
                    egui::RichText::new(exercise.exercise_id.as_str())
                        .monospace()
                        .size(11.0)
                        .color(theme::color(OUTLINE)),
                );
            });
        }
        if let Some(run) = self.state.selected_operation_run() {
            theme::deco_frame(SURFACE, SemanticTone::Live).show(ui, |ui| {
                theme::status_chip(ui, format!("{:?}", run.status), SemanticTone::Live);
                ui.label(
                    egui::RichText::new(&run.name)
                        .strong()
                        .color(theme::color(GREEN_TEXT)),
                );
                ui.label(&run.objective);
                ui.label(
                    egui::RichText::new(run.operation_run_id.as_str())
                        .monospace()
                        .size(11.0)
                        .color(theme::color(OUTLINE)),
                );
            });
        }
        if let Some(session) = self.state.selected_session() {
            theme::deco_frame(SURFACE, SemanticTone::Interrupt).show(ui, |ui| {
                theme::status_chip(ui, format!("{:?}", session.status), SemanticTone::Interrupt);
                ui.label(
                    egui::RichText::new(&session.name)
                        .strong()
                        .color(theme::color(PINK_TEXT)),
                );
                ui.label(&session.purpose);
                ui.label(
                    egui::RichText::new(session.session_id.as_str())
                        .monospace()
                        .size(11.0)
                        .color(theme::color(OUTLINE)),
                );
            });
        }
        ui.add_space(14.0);
        theme::section_heading(ui, "connected operators", "Team");
        if self.state.team.presence.is_empty() {
            ui.label(
                egui::RichText::new("NO CONNECTED TEAM CLIENTS")
                    .monospace()
                    .color(theme::color(OUTLINE)),
            );
        }
        for presence in &self.state.team.presence {
            ui.horizontal_wrapped(|ui| {
                theme::status_chip(
                    ui,
                    format!("{:?}", presence.state),
                    shared_theme::presence_tone(presence.state),
                );
                ui.label(
                    egui::RichText::new(
                        presence
                            .client
                            .display_name
                            .as_deref()
                            .unwrap_or(presence.client.client_id.as_str()),
                    )
                    .strong(),
                );
            });
        }
        for work_item in self.state.team.work_items.iter().filter(|item| {
            self.state
                .selection
                .exercise_id
                .as_ref()
                .is_none_or(|selected| item.exercise_id.as_ref() == Some(selected))
        }) {
            theme::deco_frame(SURFACE, shared_theme::work_item_tone(work_item.status)).show(
                ui,
                |ui| {
                    theme::status_chip(
                        ui,
                        format!("{:?}", work_item.status),
                        shared_theme::work_item_tone(work_item.status),
                    );
                    ui.label(format!(
                        "{}{}",
                        work_item.title,
                        work_item
                            .assignee
                            .as_ref()
                            .map(|assignee| format!(" → {}", assignee.as_str()))
                            .unwrap_or_default()
                    ));
                },
            );
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        for claim in self
            .state
            .team
            .resource_claims
            .iter()
            .filter(|claim| claim.released_unix_ms.is_none() && claim.expires_unix_ms > now)
        {
            ui.label(
                egui::RichText::new(format!(
                    "CLAIM {} BY {}",
                    claim.resource_key,
                    claim.owner.as_str()
                ))
                .monospace()
                .color(theme::color(GREEN_TEXT)),
            );
        }
        ui.add_space(14.0);
        theme::section_heading(ui, "resource governor", "Capacity");
        theme::deco_frame(SURFACE, SemanticTone::Primary).show(ui, |ui| {
            ui.label(
                egui::RichText::new(pretty_json(&self.state.capacity))
                    .monospace()
                    .size(11.0)
                    .color(theme::color(ON_SURFACE_VARIANT)),
            );
        });
        ui.add_space(14.0);
        theme::section_heading(ui, "execution fabric", "Providers");
        theme::deco_frame(SURFACE, SemanticTone::Live).show(ui, |ui| {
            ui.label(
                egui::RichText::new(pretty_json(&self.state.providers))
                    .monospace()
                    .size(11.0)
                    .color(theme::color(ON_SURFACE_VARIANT)),
            );
        });
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
            .frame(theme::deco_frame(SURFACE_LOW, SemanticTone::Primary))
            .show(context, |ui| {
                theme::section_heading(ui, "operator control", title);
                if kind == DialogKind::Exercise {
                    field_label(ui, "Workspace");
                    ui.add_sized(
                        [ui.available_width(), 32.0],
                        egui::TextEdit::singleline(&mut self.workspace),
                    );
                }
                field_label(ui, match kind {
                    DialogKind::Exercise => "Exercise name",
                    DialogKind::Run => "Run name",
                    DialogKind::Session => "Session name",
                });
                ui.add_sized(
                    [ui.available_width(), 32.0],
                    egui::TextEdit::singleline(&mut self.name),
                );
                field_label(ui, match kind {
                    DialogKind::Session => "Purpose",
                    _ => "Objective",
                });
                ui.add_sized(
                    [ui.available_width(), 72.0],
                    egui::TextEdit::multiline(&mut self.objective).desired_rows(3),
                );
                if kind == DialogKind::Exercise {
                    field_label(
                        ui,
                        "Initial scope (comma/newline separated; prefix ! to exclude)",
                    );
                    ui.add_sized(
                        [ui.available_width(), 58.0],
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
                    if theme::action_button(ui, "Create", fields_valid).clicked() {
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
                    if !first_run
                        && ui
                            .add(
                                egui::Button::new("CANCEL")
                                    .fill(theme::color(SURFACE))
                                    .stroke(egui::Stroke::new(1.0, theme::color(HOT_PINK)))
                                    .corner_radius(egui::CornerRadius::ZERO),
                            )
                            .clicked()
                    {
                        self.dialog = None;
                        self.clear_dialog();
                    }
                });
                if first_run {
                    ui.label(
                        egui::RichText::new(
                            "AN EXERCISE IS THE LONG-LIVED ASSESSMENT BOUNDARY. RUNS AND SESSIONS ARE CREATED INSIDE IT.",
                        )
                        .monospace()
                        .size(11.0)
                        .color(theme::color(GREEN_TEXT)),
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
        egui::Panel::top("status")
            .frame(
                egui::Frame::new()
                    .inner_margin(egui::Margin::symmetric(18, 10))
                    .fill(theme::color(OBSIDIAN))
                    .stroke(egui::Stroke::new(3.0, theme::color(GOLD)))
                    .corner_radius(egui::CornerRadius::ZERO)
                    .shadow(egui::Shadow {
                        offset: [0, 4],
                        blur: 8,
                        spread: 0,
                        color: theme::color(GOLD).gamma_multiply(0.24),
                    }),
            )
            .show(ui, |ui| {
                ui.horizontal_wrapped(|ui| {
                    ui.label(
                        egui::RichText::new("GROK // OPERATIONS")
                            .size(27.0)
                            .strong()
                            .italics()
                            .color(theme::color(GOLD)),
                    );
                    theme::status_chip(
                        ui,
                        &self.state.connection,
                        shared_theme::connection_tone(&self.state.connection),
                    );
                    theme::status_chip(
                        ui,
                        format!("cursor {}", self.state.cursor),
                        SemanticTone::Primary,
                    );
                    theme::status_chip(
                        ui,
                        &self.state.notice,
                        shared_theme::notice_tone(&self.state.notice),
                    );
                });
            });
        egui::Panel::left("catalog")
            .resizable(true)
            .default_size(300.0)
            .frame(theme::panel_frame(SURFACE_LOW))
            .show(ui, |ui| self.show_catalog(ui));
        egui::Panel::right("context")
            .resizable(true)
            .default_size(330.0)
            .frame(theme::panel_frame(SURFACE_LOW))
            .show(ui, |ui| {
                egui::ScrollArea::vertical().show(ui, |ui| self.show_context(ui));
            });
        egui::CentralPanel::default()
            .frame(theme::panel_frame(OBSIDIAN))
            .show(ui, |ui| {
                theme::draw_backdrop(ui);
                egui::ScrollArea::vertical().show(ui, |ui| self.show_activity(ui));
            });
        self.show_dialog(ui.ctx());
        ui.ctx().request_repaint_after(Duration::from_millis(100));
    }
}

fn field_label(ui: &mut egui::Ui, label: &str) {
    ui.label(
        egui::RichText::new(label.to_ascii_uppercase())
            .monospace()
            .size(11.0)
            .strong()
            .color(theme::color(GREEN_TEXT)),
    );
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
        "Grok // Operations",
        native_options,
        Box::new(move |context| Ok(Box::new(OperatorApp::new(arguments, &context.egui_ctx)))),
    )
}
