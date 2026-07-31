use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

use eframe::egui;
use xai_grok_control_client::{ClientIdentity, ControlPlaneClient};
use xai_grok_protocol::{
    ClientId, Command, CommandId, EventEnvelope, EventReadRequest, IngressEnvelope, IngressSource,
    ProjectionQuery, Response, TeamClient, TeamId, WorkspaceId,
};

const MAX_VISIBLE_EVENTS: usize = 2_000;

#[derive(Clone, Debug)]
struct Arguments {
    socket: PathBuf,
    team_id: TeamId,
    client_id: ClientId,
    display_name: Option<String>,
}

#[derive(Debug)]
enum UiUpdate {
    Connection(String),
    Events {
        events: Vec<EventEnvelope>,
        cursor: u64,
    },
    Capacity(serde_json::Value),
    Providers(serde_json::Value),
    Submission(String),
}

#[derive(Debug)]
enum UiCommand {
    Submit {
        workspace_id: String,
        session_id: String,
        request: String,
    },
}

struct OperatorApp {
    updates: mpsc::Receiver<UiUpdate>,
    commands: mpsc::SyncSender<UiCommand>,
    stop: Arc<AtomicBool>,
    connection: String,
    cursor: u64,
    capacity: serde_json::Value,
    providers: serde_json::Value,
    events: VecDeque<EventEnvelope>,
    workspace_id: String,
    session_id: String,
    request: String,
    submission: String,
}

impl OperatorApp {
    fn new(arguments: Arguments) -> Self {
        let (updates_tx, updates) = mpsc::sync_channel(128);
        let (commands, command_rx) = mpsc::sync_channel(16);
        let stop = Arc::new(AtomicBool::new(false));
        spawn_client_worker(arguments, updates_tx, command_rx, stop.clone());
        Self {
            updates,
            commands,
            stop,
            connection: "connecting".to_owned(),
            cursor: 0,
            capacity: serde_json::Value::Null,
            providers: serde_json::Value::Null,
            events: VecDeque::new(),
            workspace_id: "default".to_owned(),
            session_id: format!("gui-{}", std::process::id()),
            request: String::new(),
            submission: "idle".to_owned(),
        }
    }

    fn receive_updates(&mut self) {
        while let Ok(update) = self.updates.try_recv() {
            match update {
                UiUpdate::Connection(connection) => self.connection = connection,
                UiUpdate::Events { events, cursor } => {
                    self.cursor = cursor;
                    self.events.extend(events);
                    while self.events.len() > MAX_VISIBLE_EVENTS {
                        self.events.pop_front();
                    }
                }
                UiUpdate::Capacity(capacity) => self.capacity = capacity,
                UiUpdate::Providers(providers) => self.providers = providers,
                UiUpdate::Submission(submission) => self.submission = submission,
            }
        }
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
                ui.label(&self.connection);
                ui.separator();
                ui.monospace(format!("event cursor {}", self.cursor));
            });
        });
        egui::Panel::left("projections")
            .resizable(true)
            .default_size(320.0)
            .show(ui, |ui| {
                ui.heading("Capacity");
                ui.monospace(pretty_json(&self.capacity));
                ui.separator();
                ui.heading("Providers");
                egui::ScrollArea::vertical().show(ui, |ui| {
                    ui.monospace(pretty_json(&self.providers));
                });
            });
        egui::CentralPanel::default().show(ui, |ui| {
            ui.heading("Submit engagement");
            ui.horizontal(|ui| {
                ui.label("Workspace");
                ui.text_edit_singleline(&mut self.workspace_id);
                ui.label("Session");
                ui.text_edit_singleline(&mut self.session_id);
            });
            ui.add(
                egui::TextEdit::multiline(&mut self.request)
                    .desired_rows(3)
                    .hint_text("Request"),
            );
            ui.horizontal(|ui| {
                if ui.button("Submit").clicked() {
                    if self.workspace_id.trim().is_empty()
                        || self.session_id.trim().is_empty()
                        || self.request.trim().is_empty()
                    {
                        self.submission = "workspace, session, and request are required".to_owned();
                    } else {
                        let command = UiCommand::Submit {
                            workspace_id: self.workspace_id.clone(),
                            session_id: self.session_id.clone(),
                            request: self.request.clone(),
                        };
                        match self.commands.try_send(command) {
                            Ok(()) => self.submission = "queued".to_owned(),
                            Err(mpsc::TrySendError::Full(_)) => {
                                self.submission = "local submission queue is full".to_owned();
                            }
                            Err(mpsc::TrySendError::Disconnected(_)) => {
                                self.submission = "client worker stopped".to_owned();
                            }
                        }
                    }
                }
                ui.label(&self.submission);
            });
            ui.separator();
            ui.heading("Durable event stream");
            egui::ScrollArea::vertical()
                .stick_to_bottom(true)
                .show(ui, |ui| {
                    for event in &self.events {
                        ui.horizontal_wrapped(|ui| {
                            ui.monospace(format!("{:>8}", event.sequence));
                            ui.label(event_name(event));
                            if let Some(engagement_id) = &event.engagement_id {
                                ui.monospace(engagement_id.as_str());
                            }
                        });
                    }
                });
        });
        ui.ctx().request_repaint_after(Duration::from_millis(100));
    }
}

fn spawn_client_worker(
    arguments: Arguments,
    updates: mpsc::SyncSender<UiUpdate>,
    commands: mpsc::Receiver<UiCommand>,
    stop: Arc<AtomicBool>,
) {
    thread::Builder::new()
        .name("grok-ui-client".to_owned())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build();
            match runtime {
                Ok(runtime) => runtime.block_on(client_worker(arguments, updates, commands, stop)),
                Err(error) => {
                    let _ = updates.send(UiUpdate::Connection(format!("runtime error: {error}")));
                }
            }
        })
        .expect("failed to start the bounded GUI client worker");
}

async fn client_worker(
    arguments: Arguments,
    updates: mpsc::SyncSender<UiUpdate>,
    commands: mpsc::Receiver<UiCommand>,
    stop: Arc<AtomicBool>,
) {
    let mut cursor = 0_u64;
    while !stop.load(Ordering::Acquire) {
        let mut identity = ClientIdentity::new("grok-ui", env!("CARGO_PKG_VERSION"));
        identity.team = Some(TeamClient {
            team_id: arguments.team_id.clone(),
            client_id: arguments.client_id.clone(),
            display_name: arguments.display_name.clone(),
        });
        let control = match ControlPlaneClient::connect(&arguments.socket, identity).await {
            Ok(control) => control,
            Err(error) => {
                if updates
                    .send(UiUpdate::Connection(format!("reconnecting: {error}")))
                    .is_err()
                {
                    return;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        if updates
            .send(UiUpdate::Connection(format!(
                "connected to {}",
                arguments.socket.display()
            )))
            .is_err()
        {
            return;
        }
        if refresh_projections(&control, &updates).await.is_err() {
            continue;
        }
        let mut next_projection = tokio::time::Instant::now() + Duration::from_secs(2);
        'connected: loop {
            if stop.load(Ordering::Acquire) {
                return;
            }
            while let Ok(command) = commands.try_recv() {
                if let Err(error) = submit_command(&control, command, &updates).await {
                    let _ = updates.send(UiUpdate::Submission(format!("failed: {error}")));
                    let _ = updates.send(UiUpdate::Connection(format!("reconnecting: {error}")));
                    break 'connected;
                }
            }
            match control
                .read_events(EventReadRequest {
                    after_sequence: cursor,
                    maximum_events: 128,
                    wait_ms: 250,
                    engagement_id: None,
                })
                .await
            {
                Ok(batch) => {
                    if updates
                        .send(UiUpdate::Events {
                            events: batch.events,
                            cursor: batch.next_sequence,
                        })
                        .is_err()
                    {
                        return;
                    }
                    cursor = batch.next_sequence;
                }
                Err(error) => {
                    let _ = updates.send(UiUpdate::Connection(format!("reconnecting: {error}")));
                    break;
                }
            }
            if tokio::time::Instant::now() >= next_projection {
                if let Err(error) = refresh_projections(&control, &updates).await {
                    let _ = updates.send(UiUpdate::Connection(format!("reconnecting: {error}")));
                    break;
                }
                next_projection = tokio::time::Instant::now() + Duration::from_secs(2);
            }
        }
    }
}

async fn submit_command(
    control: &ControlPlaneClient,
    command: UiCommand,
    updates: &mpsc::SyncSender<UiUpdate>,
) -> Result<(), Box<dyn std::error::Error>> {
    let UiCommand::Submit {
        workspace_id,
        session_id,
        request,
    } = command;
    let source_id = CommandId::new();
    let response = control
        .send(
            Command::SubmitIngress(IngressEnvelope {
                command_id: CommandId::new(),
                source: IngressSource::Gui,
                source_event_id: source_id.to_string(),
                workspace_id: WorkspaceId::from_string(workspace_id),
                session_id,
                prompt_id: source_id.to_string(),
                request,
                team: None,
                metadata: serde_json::Map::new(),
            }),
            Duration::from_secs(5),
        )
        .await?;
    match response {
        Response::Accepted { engagement_id, .. } => {
            updates.send(UiUpdate::Submission(format!(
                "accepted as {}",
                engagement_id.as_str()
            )))?;
            Ok(())
        }
        _ => Err("daemon returned an unexpected submission response".into()),
    }
}

async fn refresh_projections(
    control: &ControlPlaneClient,
    updates: &mpsc::SyncSender<UiUpdate>,
) -> Result<(), Box<dyn std::error::Error>> {
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
    if let Response::Projection(snapshot) = capacity {
        updates.send(UiUpdate::Capacity(snapshot.value))?;
    }
    if let Response::Projection(snapshot) = providers {
        updates.send(UiUpdate::Providers(snapshot.value))?;
    }
    Ok(())
}

fn event_name(event: &EventEnvelope) -> &'static str {
    match &event.event {
        xai_grok_protocol::Event::EngagementAccepted { .. } => "engagement accepted",
        xai_grok_protocol::Event::PlanAccepted { .. } => "plan accepted",
        xai_grok_protocol::Event::TaskStatus { .. } => "task status",
        xai_grok_protocol::Event::Observation { .. } => "observation",
        xai_grok_protocol::Event::ProviderState { .. } => "provider state",
        xai_grok_protocol::Event::ArtifactAvailable { .. } => "artifact available",
        xai_grok_protocol::Event::Overload { .. } => "overload",
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
            .with_inner_size([1_100.0, 720.0])
            .with_min_inner_size([800.0, 500.0]),
        renderer: eframe::Renderer::Glow,
        ..Default::default()
    };
    eframe::run_native(
        "Grok operator",
        native_options,
        Box::new(move |_context| Ok(Box::new(OperatorApp::new(arguments)))),
    )
}
