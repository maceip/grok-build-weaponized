use std::io::{self, stdout};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, Paragraph, Wrap};
use xai_grok_operator_core::{
    OperatorClientConfig, OperatorCommand, OperatorState, OperatorUpdate, event_summary,
    parse_scope_targets, spawn_client_worker,
};
use xai_grok_protocol::{
    ClientId, ExerciseId, IngressSource, OperationRunId, OperatorSessionId, TeamId,
};

const ACCENT: Color = Color::Rgb(122, 162, 247);
const MUTED: Color = Color::Rgb(86, 95, 137);

#[derive(Clone, Debug)]
struct Arguments {
    socket: PathBuf,
    team_id: TeamId,
    client_id: ClientId,
    display_name: Option<String>,
}

#[derive(Clone, Debug)]
enum NavItem {
    Exercise(ExerciseId),
    Run(OperationRunId),
    Session(OperatorSessionId),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FormKind {
    Exercise,
    Run,
    Session,
    Turn,
}

#[derive(Clone, Debug)]
struct Form {
    kind: FormKind,
    title: &'static str,
    labels: Vec<&'static str>,
    values: Vec<String>,
    active: usize,
    error: Option<String>,
}

impl Form {
    fn exercise() -> Self {
        Self {
            kind: FormKind::Exercise,
            title: "Create the first exercise",
            labels: vec![
                "Workspace",
                "Exercise name",
                "Objective",
                "Scope (comma-separated; ! excludes)",
            ],
            values: vec![
                "default".to_owned(),
                String::new(),
                String::new(),
                String::new(),
            ],
            active: 0,
            error: None,
        }
    }

    fn run() -> Self {
        Self {
            kind: FormKind::Run,
            title: "Create operation run",
            labels: vec!["Run name", "Objective"],
            values: vec![String::new(), String::new()],
            active: 0,
            error: None,
        }
    }

    fn session() -> Self {
        Self {
            kind: FormKind::Session,
            title: "Create operator session",
            labels: vec!["Session name", "Purpose"],
            values: vec![String::new(), String::new()],
            active: 0,
            error: None,
        }
    }

    fn turn() -> Self {
        Self {
            kind: FormKind::Turn,
            title: "Send work to the selected session",
            labels: vec!["Request"],
            values: vec![String::new()],
            active: 0,
            error: None,
        }
    }
}

struct App {
    state: OperatorState,
    updates: mpsc::Receiver<OperatorUpdate>,
    commands: mpsc::SyncSender<OperatorCommand>,
    stop: Arc<AtomicBool>,
    nav_index: usize,
    form: Option<Form>,
}

impl App {
    fn new(
        updates: mpsc::Receiver<OperatorUpdate>,
        commands: mpsc::SyncSender<OperatorCommand>,
        stop: Arc<AtomicBool>,
    ) -> Self {
        Self {
            state: OperatorState::default(),
            updates,
            commands,
            stop,
            nav_index: 0,
            form: None,
        }
    }

    fn receive_updates(&mut self) {
        while let Ok(update) = self.updates.try_recv() {
            self.state.apply(update);
        }
        if self.state.first_run() && self.form.is_none() {
            self.form = Some(Form::exercise());
        }
        if !self.state.first_run()
            && self
                .form
                .as_ref()
                .is_some_and(|form| form.kind == FormKind::Exercise)
        {
            self.form = None;
            self.sync_nav_to_selection();
        }
    }

    fn nav_items(&self) -> Vec<NavItem> {
        let mut items = Vec::new();
        for exercise in &self.state.catalog.exercises {
            items.push(NavItem::Exercise(exercise.exercise_id.clone()));
            for run in self
                .state
                .catalog
                .operation_runs
                .iter()
                .filter(|run| run.exercise_id == exercise.exercise_id)
            {
                items.push(NavItem::Run(run.operation_run_id.clone()));
                for session in self.state.catalog.sessions.iter().filter(|session| {
                    session.operation_run_id.as_ref() == Some(&run.operation_run_id)
                }) {
                    items.push(NavItem::Session(session.session_id.clone()));
                }
            }
            for session in self.state.catalog.sessions.iter().filter(|session| {
                session.exercise_id == exercise.exercise_id && session.operation_run_id.is_none()
            }) {
                items.push(NavItem::Session(session.session_id.clone()));
            }
        }
        items
    }

    fn select_nav(&mut self) {
        let Some(item) = self.nav_items().get(self.nav_index).cloned() else {
            return;
        };
        match item {
            NavItem::Exercise(id) => self.state.select_exercise(id),
            NavItem::Run(id) => self.state.select_operation_run(id),
            NavItem::Session(id) => self.state.select_session(id),
        }
    }

    fn sync_nav_to_selection(&mut self) {
        let items = self.nav_items();
        if let Some(session_id) = &self.state.selection.session_id
            && let Some(index) = items
                .iter()
                .position(|item| matches!(item, NavItem::Session(id) if id == session_id))
        {
            self.nav_index = index;
            return;
        }
        if let Some(run_id) = &self.state.selection.operation_run_id
            && let Some(index) = items
                .iter()
                .position(|item| matches!(item, NavItem::Run(id) if id == run_id))
        {
            self.nav_index = index;
            return;
        }
        if let Some(exercise_id) = &self.state.selection.exercise_id
            && let Some(index) = items
                .iter()
                .position(|item| matches!(item, NavItem::Exercise(id) if id == exercise_id))
        {
            self.nav_index = index;
        }
    }

    fn handle_key(&mut self, key: KeyEvent) -> bool {
        if self.form.is_some() {
            self.handle_form_key(key);
            return false;
        }
        match key.code {
            KeyCode::Char('q') => return true,
            KeyCode::Up | KeyCode::Char('k') => {
                self.nav_index = self.nav_index.saturating_sub(1);
                self.select_nav();
            }
            KeyCode::Down | KeyCode::Char('j') => {
                let maximum = self.nav_items().len().saturating_sub(1);
                self.nav_index = self.nav_index.saturating_add(1).min(maximum);
                self.select_nav();
            }
            KeyCode::Enter => self.select_nav(),
            KeyCode::Char('n') => self.form = Some(Form::exercise()),
            KeyCode::Char('r') if self.state.selected_exercise().is_some() => {
                self.form = Some(Form::run());
            }
            KeyCode::Char('s') if self.state.selected_exercise().is_some() => {
                self.form = Some(Form::session());
            }
            KeyCode::Char('i') if self.state.selected_session().is_some() => {
                self.form = Some(Form::turn());
            }
            _ => {}
        }
        false
    }

    fn handle_form_key(&mut self, key: KeyEvent) {
        let first_run = self.state.first_run();
        let Some(form) = self.form.as_mut() else {
            return;
        };
        match key.code {
            KeyCode::Esc if !first_run => self.form = None,
            KeyCode::Tab | KeyCode::Down => {
                form.active = (form.active + 1) % form.values.len();
            }
            KeyCode::BackTab | KeyCode::Up => {
                form.active = form.active.checked_sub(1).unwrap_or(form.values.len() - 1);
            }
            KeyCode::Backspace => {
                form.values[form.active].pop();
                form.error = None;
            }
            KeyCode::Enter => {
                if form.active + 1 < form.values.len()
                    && !key.modifiers.contains(KeyModifiers::CONTROL)
                {
                    form.active += 1;
                } else {
                    self.submit_form();
                }
            }
            KeyCode::Char(character)
                if !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
            {
                form.values[form.active].push(character);
                form.error = None;
            }
            _ => {}
        }
    }

    fn submit_form(&mut self) {
        let Some(form) = self.form.as_mut() else {
            return;
        };
        if let Some((index, _)) = form
            .values
            .iter()
            .enumerate()
            .find(|(_, value)| value.trim().is_empty())
        {
            form.active = index;
            form.error = Some(format!("{} is required", form.labels[index]));
            return;
        }
        let command = match form.kind {
            FormKind::Exercise => OperatorCommand::CreateExercise {
                workspace_id: form.values[0].trim().to_owned(),
                name: form.values[1].trim().to_owned(),
                objective: form.values[2].trim().to_owned(),
                scope: parse_scope_targets(&form.values[3]),
            },
            FormKind::Run => {
                let Some(exercise) = self.state.selected_exercise() else {
                    form.error = Some("select an exercise first".to_owned());
                    return;
                };
                OperatorCommand::CreateOperationRun {
                    exercise_id: exercise.exercise_id.clone(),
                    name: form.values[0].trim().to_owned(),
                    objective: form.values[1].trim().to_owned(),
                }
            }
            FormKind::Session => {
                let Some(exercise) = self.state.selected_exercise() else {
                    form.error = Some("select an exercise first".to_owned());
                    return;
                };
                OperatorCommand::CreateSession {
                    exercise_id: exercise.exercise_id.clone(),
                    operation_run_id: self.state.selection.operation_run_id.clone(),
                    name: form.values[0].trim().to_owned(),
                    purpose: form.values[1].trim().to_owned(),
                }
            }
            FormKind::Turn => {
                let (Some(exercise), Some(session)) = (
                    self.state.selected_exercise(),
                    self.state.selected_session(),
                ) else {
                    form.error = Some("select a session first".to_owned());
                    return;
                };
                OperatorCommand::SubmitTurn {
                    workspace_id: exercise.workspace_id.clone(),
                    exercise_id: exercise.exercise_id.clone(),
                    operation_run_id: session.operation_run_id.clone(),
                    session_id: session.session_id.clone(),
                    request: form.values[0].trim().to_owned(),
                }
            }
        };
        match self.commands.try_send(command) {
            Ok(()) => {
                self.state.notice = "queued".to_owned();
                if !self.state.first_run() {
                    self.form = None;
                }
            }
            Err(mpsc::TrySendError::Full(_)) => {
                form.error = Some("local command queue is full".to_owned());
            }
            Err(mpsc::TrySendError::Disconnected(_)) => {
                form.error = Some("operator client stopped".to_owned());
            }
        }
    }

    fn draw(&self, frame: &mut ratatui::Frame<'_>) {
        let outer = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(10),
                Constraint::Length(3),
            ])
            .split(frame.area());
        self.draw_header(frame, outer[0]);
        let columns = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(30),
                Constraint::Percentage(45),
                Constraint::Percentage(25),
            ])
            .split(outer[1]);
        self.draw_navigation(frame, columns[0]);
        self.draw_activity(frame, columns[1]);
        self.draw_context(frame, columns[2]);
        self.draw_footer(frame, outer[2]);
        if let Some(form) = &self.form {
            self.draw_form(frame, form);
        }
    }

    fn draw_header(&self, frame: &mut ratatui::Frame<'_>, area: Rect) {
        let selected = self
            .state
            .selected_exercise()
            .map_or("no exercise", |exercise| exercise.name.as_str());
        let line = Line::from(vec![
            Span::styled(
                " GROK OPS ",
                Style::default()
                    .fg(Color::Black)
                    .bg(ACCENT)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("  "),
            Span::styled(selected, Style::default().add_modifier(Modifier::BOLD)),
            Span::raw("  "),
            Span::styled(&self.state.connection, Style::default().fg(MUTED)),
            Span::raw(format!("  cursor {}", self.state.cursor)),
        ]);
        frame.render_widget(
            Paragraph::new(line).block(Block::default().borders(Borders::BOTTOM)),
            area,
        );
    }

    fn draw_navigation(&self, frame: &mut ratatui::Frame<'_>, area: Rect) {
        let items = self.nav_items();
        let rows = if items.is_empty() {
            vec![ListItem::new("No exercises. Press n to create one.")]
        } else {
            items
                .iter()
                .enumerate()
                .map(|(index, item)| {
                    let (prefix, label) = match item {
                        NavItem::Exercise(id) => (
                            "◆ ",
                            self.state
                                .catalog
                                .exercises
                                .iter()
                                .find(|exercise| &exercise.exercise_id == id)
                                .map_or("unknown", |exercise| exercise.name.as_str()),
                        ),
                        NavItem::Run(id) => (
                            "  ├─ ",
                            self.state
                                .catalog
                                .operation_runs
                                .iter()
                                .find(|run| &run.operation_run_id == id)
                                .map_or("unknown", |run| run.name.as_str()),
                        ),
                        NavItem::Session(id) => (
                            "     └─ ",
                            self.state
                                .catalog
                                .sessions
                                .iter()
                                .find(|session| &session.session_id == id)
                                .map_or("unknown", |session| session.name.as_str()),
                        ),
                    };
                    let style = if index == self.nav_index {
                        Style::default().fg(Color::Black).bg(ACCENT)
                    } else {
                        Style::default()
                    };
                    ListItem::new(format!("{prefix}{label}")).style(style)
                })
                .collect()
        };
        frame.render_widget(
            List::new(rows).block(
                Block::default()
                    .title(" Exercises / runs / sessions ")
                    .borders(Borders::ALL),
            ),
            area,
        );
    }

    fn draw_activity(&self, frame: &mut ratatui::Frame<'_>, area: Rect) {
        let height = area.height.saturating_sub(2) as usize;
        let mut rows = self
            .state
            .selected_task_graphs()
            .into_iter()
            .flat_map(|graph| {
                let mut graph_rows = vec![ListItem::new(Line::from(vec![
                    Span::styled("turn   ", Style::default().fg(ACCENT)),
                    Span::raw(format!(
                        "{} r{} — {}",
                        graph.engagement_id.as_str(),
                        graph.revision,
                        graph.objective
                    )),
                ]))];
                graph_rows.extend(graph.tasks.iter().map(|node| {
                    ListItem::new(Line::from(vec![
                        Span::styled(format!("{:?} ", node.status), Style::default().fg(ACCENT)),
                        Span::raw(format!(
                            "{} — {}",
                            node.task.task_id.as_str(),
                            node.task.objective
                        )),
                    ]))
                }));
                graph_rows
            })
            .collect::<Vec<_>>();
        rows.extend(
            self.state
                .outputs
                .iter()
                .filter(|output| self.state.output_is_in_selected_session(output))
                .flat_map(|output| {
                    output.text.lines().map(|line| {
                        ListItem::new(Line::from(vec![
                            Span::styled("agent  ", Style::default().fg(ACCENT)),
                            Span::raw(line.to_owned()),
                        ]))
                    })
                })
                .collect::<Vec<_>>(),
        );
        rows.extend(
            self.state
                .events
                .iter()
                .rev()
                .take(height.max(1))
                .rev()
                .map(|event| {
                    ListItem::new(Line::from(vec![
                        Span::styled(
                            format!("{:>7} ", event.sequence),
                            Style::default().fg(MUTED),
                        ),
                        Span::raw(event_summary(event)),
                    ]))
                })
                .collect::<Vec<_>>(),
        );
        if rows.len() > height.max(1) {
            rows.drain(0..rows.len() - height.max(1));
        }
        frame.render_widget(
            List::new(rows).block(
                Block::default()
                    .title(" Live activity ")
                    .borders(Borders::ALL),
            ),
            area,
        );
    }

    fn draw_context(&self, frame: &mut ratatui::Frame<'_>, area: Rect) {
        let mut lines = Vec::new();
        if let Some(exercise) = self.state.selected_exercise() {
            lines.push(Line::styled(
                "EXERCISE",
                Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
            ));
            lines.push(Line::raw(exercise.name.clone()));
            lines.push(Line::raw(exercise.objective.clone()));
            lines.push(Line::raw(""));
        }
        if let Some(run) = self.state.selected_operation_run() {
            lines.push(Line::styled(
                "RUN",
                Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
            ));
            lines.push(Line::raw(run.name.clone()));
            lines.push(Line::raw(run.objective.clone()));
            lines.push(Line::raw(""));
        }
        if let Some(session) = self.state.selected_session() {
            lines.push(Line::styled(
                "SESSION",
                Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
            ));
            lines.push(Line::raw(session.name.clone()));
            lines.push(Line::raw(session.purpose.clone()));
            lines.push(Line::raw(""));
        }
        lines.push(Line::styled(
            "TEAM",
            Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
        ));
        if self.state.team.presence.is_empty() {
            lines.push(Line::raw("No connected team clients"));
        }
        for presence in &self.state.team.presence {
            lines.push(Line::raw(format!(
                "{} {:?}",
                presence
                    .client
                    .display_name
                    .as_deref()
                    .unwrap_or(presence.client.client_id.as_str()),
                presence.state
            )));
        }
        for work_item in self.state.team.work_items.iter().filter(|item| {
            self.state
                .selection
                .exercise_id
                .as_ref()
                .is_none_or(|selected| item.exercise_id.as_ref() == Some(selected))
        }) {
            lines.push(Line::raw(format!(
                "{:?}: {}{}",
                work_item.status,
                work_item.title,
                work_item
                    .assignee
                    .as_ref()
                    .map(|assignee| format!(" -> {}", assignee.as_str()))
                    .unwrap_or_default()
            )));
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
            lines.push(Line::raw(format!(
                "claim {} by {}",
                claim.resource_key,
                claim.owner.as_str()
            )));
        }
        lines.push(Line::raw(""));
        lines.push(Line::styled(
            "CAPACITY",
            Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
        ));
        lines.extend(
            pretty_json(&self.state.capacity)
                .lines()
                .map(|line| Line::raw(line.to_owned())),
        );
        lines.push(Line::raw(""));
        lines.push(Line::styled(
            "PROVIDERS",
            Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
        ));
        lines.extend(
            pretty_json(&self.state.providers)
                .lines()
                .map(|line| Line::raw(line.to_owned())),
        );
        frame.render_widget(
            Paragraph::new(lines)
                .wrap(Wrap { trim: false })
                .block(Block::default().title(" Context ").borders(Borders::ALL)),
            area,
        );
    }

    fn draw_footer(&self, frame: &mut ratatui::Frame<'_>, area: Rect) {
        let help = Line::from(vec![
            Span::styled(" n ", Style::default().fg(Color::Black).bg(MUTED)),
            Span::raw(" exercise  "),
            Span::styled(" r ", Style::default().fg(Color::Black).bg(MUTED)),
            Span::raw(" run  "),
            Span::styled(" s ", Style::default().fg(Color::Black).bg(MUTED)),
            Span::raw(" session  "),
            Span::styled(" i ", Style::default().fg(Color::Black).bg(MUTED)),
            Span::raw(" send  "),
            Span::styled(" q ", Style::default().fg(Color::Black).bg(MUTED)),
            Span::raw(" quit  •  "),
            Span::styled(&self.state.notice, Style::default().fg(ACCENT)),
        ]);
        frame.render_widget(
            Paragraph::new(help).block(Block::default().borders(Borders::TOP)),
            area,
        );
    }

    fn draw_form(&self, frame: &mut ratatui::Frame<'_>, form: &Form) {
        let area = centered(68, form.values.len() as u16 * 3 + 7, frame.area());
        frame.render_widget(Clear, area);
        let inner = Block::default()
            .title(format!(" {} ", form.title))
            .title_alignment(Alignment::Center)
            .borders(Borders::ALL)
            .border_style(Style::default().fg(ACCENT))
            .inner(area);
        frame.render_widget(
            Block::default()
                .title(format!(" {} ", form.title))
                .title_alignment(Alignment::Center)
                .borders(Borders::ALL)
                .border_style(Style::default().fg(ACCENT)),
            area,
        );
        let constraints = std::iter::repeat_n(Constraint::Length(3), form.values.len())
            .chain([Constraint::Min(2)])
            .collect::<Vec<_>>();
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints(constraints)
            .split(inner);
        for (index, ((label, value), row)) in form
            .labels
            .iter()
            .zip(&form.values)
            .zip(rows.iter())
            .enumerate()
        {
            let border = if index == form.active { ACCENT } else { MUTED };
            frame.render_widget(
                Paragraph::new(value.as_str()).block(
                    Block::default()
                        .title(format!(" {label} "))
                        .borders(Borders::ALL)
                        .border_style(Style::default().fg(border)),
                ),
                *row,
            );
        }
        let instruction = form.error.as_deref().unwrap_or(
            "Tab selects the next field • Enter advances • Enter on final field submits • Esc cancels",
        );
        frame.render_widget(
            Paragraph::new(instruction).style(Style::default().fg(if form.error.is_some() {
                Color::Red
            } else {
                MUTED
            })),
            rows[form.values.len()],
        );
    }
}

impl Drop for App {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
    }
}

struct TerminalGuard;

impl TerminalGuard {
    fn enter() -> io::Result<Self> {
        enable_raw_mode()?;
        execute!(stdout(), EnterAlternateScreen)?;
        Ok(Self)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(stdout(), LeaveAlternateScreen);
    }
}

fn run(arguments: Arguments) -> Result<(), Box<dyn std::error::Error>> {
    let (updates_tx, updates) = mpsc::sync_channel(128);
    let (commands, command_rx) = mpsc::sync_channel(16);
    let stop = Arc::new(AtomicBool::new(false));
    spawn_client_worker(
        OperatorClientConfig {
            socket: arguments.socket,
            team_id: arguments.team_id,
            client_id: arguments.client_id,
            display_name: arguments.display_name,
            client_name: "grok-ops".to_owned(),
            source: IngressSource::Tui,
            workspace_filter: None,
        },
        updates_tx,
        command_rx,
        stop.clone(),
    );
    let mut app = App::new(updates, commands, stop);
    let _guard = TerminalGuard::enter()?;
    let mut terminal = Terminal::new(CrosstermBackend::new(stdout()))?;
    terminal.clear()?;
    loop {
        app.receive_updates();
        terminal.draw(|frame| app.draw(frame))?;
        if event::poll(Duration::from_millis(50))?
            && let Event::Key(key) = event::read()?
            && key.kind == crossterm::event::KeyEventKind::Press
            && app.handle_key(key)
        {
            break;
        }
    }
    Ok(())
}

fn centered(width_percent: u16, height_rows: u16, area: Rect) -> Rect {
    let width = area.width.saturating_mul(width_percent).saturating_div(100);
    let height = height_rows.min(area.height.saturating_sub(2));
    Rect {
        x: area.x + area.width.saturating_sub(width) / 2,
        y: area.y + area.height.saturating_sub(height) / 2,
        width,
        height,
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
                    "usage: grok-ops [--socket PATH] [--team ID] [--client ID] [--display-name NAME]"
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

fn main() {
    match parse_arguments().and_then(|arguments| run(arguments).map_err(|error| error.to_string()))
    {
        Ok(()) => {}
        Err(error) => {
            eprintln!("grok-ops: {error}");
            std::process::exit(2);
        }
    }
}

#[cfg(test)]
mod tests {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;

    #[test]
    fn first_run_renders_a_real_exercise_form() {
        let (_updates_tx, updates) = mpsc::sync_channel(1);
        let (commands, _command_rx) = mpsc::sync_channel(1);
        let mut app = App::new(updates, commands, Arc::new(AtomicBool::new(false)));
        app.state.catalog_loaded = true;
        app.receive_updates();
        let mut terminal = Terminal::new(TestBackend::new(120, 36)).unwrap();
        terminal.draw(|frame| app.draw(frame)).unwrap();
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("Create the first exercise"));
        assert!(rendered.contains("Workspace"));
        assert!(rendered.contains("Objective"));
        assert!(rendered.contains("Scope"));
    }
}
