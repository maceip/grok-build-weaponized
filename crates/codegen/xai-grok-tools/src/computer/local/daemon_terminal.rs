//! Terminal backend whose subprocesses are owned by the local `grokd` daemon.
//!
//! The renderer/agent process retains only bounded presentation state. Child
//! lifetime, process-tree cancellation, complete stream spooling, quotas, and
//! restart tombstones live in `xai-grok-native-execution` inside `grokd`.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::io::AsyncWriteExt as _;
use tokio::sync::{Mutex, Notify, RwLock};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use xai_grok_control_client::{ClientIdentity, ControlPlaneClient};
use xai_grok_native_execution::{JobLifecycle, JobSnapshot, OutputPage};
use xai_grok_protocol::{Command, OperationId, ProviderId, RequestId, Response};

use crate::computer::types::{
    BackgroundHandle, ComputerError, KillOutcome, TaskKind, TaskSnapshot, TerminalBackend,
    TerminalRunRequest, TerminalRunResult,
};
use crate::notification::types::{
    BashExecutionBackgrounded, BashNotificationBase, BashOutputChunk, ToolNotificationHandle,
};
use crate::util::output_filter::{FilteredOutput, OutputAnalysis};

const NATIVE_PROVIDER_ID: &str = "native-execution";
const SHELL_BACKGROUND_TIMEOUT: Duration = Duration::from_secs(10 * 60 * 60);
const DEFAULT_FOREGROUND_BUDGET: Duration = Duration::from_secs(15);
const PAGE_BYTES: usize = 1024 * 1024;
const PAGE_RECORDS: usize = 128;
const DAEMON_RECONNECT_BUDGET: Duration = Duration::from_secs(30);

pub struct DaemonTerminalBackend {
    socket: PathBuf,
    tasks: Arc<RwLock<HashMap<String, Arc<DaemonTask>>>>,
    shutdown: CancellationToken,
}

struct DaemonTask {
    metadata: RwLock<TaskMetadata>,
    native: RwLock<JobSnapshot>,
    capture: Mutex<BoundedCapture>,
    completion: Notify,
    backgrounded: AtomicBool,
    backgrounded_notify: Notify,
    block_waited: AtomicBool,
    explicitly_killed: AtomicBool,
    tailer_error: Mutex<Option<String>>,
    tailer_started: AtomicBool,
    tailer_complete: AtomicBool,
}

#[derive(Clone)]
struct TaskMetadata {
    command: String,
    display_command: Option<String>,
    cwd: PathBuf,
    output_file: PathBuf,
    output_limit: usize,
    notification: ToolNotificationHandle,
    tool_call_id: String,
    kind: TaskKind,
    owner_session_id: Option<String>,
    description: Option<String>,
}

#[derive(Default)]
struct BoundedCapture {
    front: Option<Vec<u8>>,
    tail: Vec<u8>,
    total_bytes: usize,
    truncated: bool,
    analysis: OutputAnalysis,
    limit: usize,
}

impl BoundedCapture {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            ..Self::default()
        }
    }

    fn push(&mut self, bytes: &[u8]) {
        self.total_bytes = self.total_bytes.saturating_add(bytes.len());
        self.analysis.push(bytes);
        self.tail.extend_from_slice(bytes);
        self.truncate();
    }

    fn truncate(&mut self) {
        let value = String::from_utf8_lossy(&self.tail);
        let chars = value.chars().count();
        if chars <= self.limit {
            return;
        }
        let half = self.limit / 2;
        if self.front.is_none() {
            let end = value
                .char_indices()
                .nth(half)
                .map_or(value.len(), |(offset, _)| offset);
            self.front = Some(value[..end].as_bytes().to_vec());
        }
        let start_char = chars.saturating_sub(half);
        let start = value
            .char_indices()
            .nth(start_char)
            .map_or(value.len(), |(offset, _)| offset);
        self.tail = value[start..].as_bytes().to_vec();
        self.truncated = true;
    }

    fn raw_preview(&self) -> String {
        match &self.front {
            Some(front) => format!(
                "{}\n\n... (output truncated) ...\n\n{}",
                String::from_utf8_lossy(front).trim_end(),
                String::from_utf8_lossy(&self.tail).trim_start()
            ),
            None => String::from_utf8_lossy(&self.tail).into_owned(),
        }
    }

    fn render(&self) -> FilteredOutput {
        self.analysis
            .render(&self.raw_preview(), self.truncated, self.limit)
    }
}

impl DaemonTerminalBackend {
    pub fn new(socket: impl Into<PathBuf>) -> Self {
        Self {
            socket: socket.into(),
            tasks: Arc::new(RwLock::new(HashMap::new())),
            shutdown: CancellationToken::new(),
        }
    }

    async fn start(
        &self,
        request: TerminalRunRequest,
        backgrounded: bool,
    ) -> Result<Arc<DaemonTask>, ComputerError> {
        if request.command.trim().is_empty() {
            return Err(ComputerError::io("daemon terminal command is empty"));
        }
        if let Some(parent) = request.output_file.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::File::create(&request.output_file).await?;

        let metadata = TaskMetadata {
            command: request.command.clone(),
            display_command: request.display_command.clone(),
            cwd: request.working_directory.clone(),
            output_file: request.output_file.clone(),
            output_limit: request.output_byte_limit,
            notification: request.notification_handle.clone(),
            tool_call_id: request.tool_call_id.clone(),
            kind: request.kind,
            owner_session_id: request.owner_session_id.clone(),
            description: request.description.clone(),
        };
        let durable_metadata = serialize_metadata(&metadata, backgrounded)?;
        let daemon_timeout = if request.auto_background_on_timeout {
            SHELL_BACKGROUND_TIMEOUT
        } else {
            request.timeout
        };
        let owner_id = request
            .owner_session_id
            .clone()
            .unwrap_or_else(|| "local-terminal".to_owned());
        let output = invoke(
            &self.socket,
            "native.command.start",
            serde_json::json!({
                "executable": "/bin/sh",
                "args": ["-lc", request.command],
                "cwd": request.working_directory,
                "env": request.env,
                "timeout_ms": duration_millis(daemon_timeout),
                "stdin": "null",
                "metadata": durable_metadata,
                "owner_id": owner_id,
            }),
            Duration::from_secs(10),
        )
        .await?;
        let snapshot: JobSnapshot = serde_json::from_value(provider_result(output))
            .map_err(|error| ComputerError::io(format!("invalid daemon job snapshot: {error}")))?;
        let task = Arc::new(DaemonTask {
            metadata: RwLock::new(metadata),
            native: RwLock::new(snapshot.clone()),
            capture: Mutex::new(BoundedCapture::new(request.output_byte_limit)),
            completion: Notify::new(),
            backgrounded: AtomicBool::new(backgrounded),
            backgrounded_notify: Notify::new(),
            block_waited: AtomicBool::new(false),
            explicitly_killed: AtomicBool::new(false),
            tailer_error: Mutex::new(None),
            tailer_started: AtomicBool::new(false),
            tailer_complete: AtomicBool::new(false),
        });
        self.tasks
            .write()
            .await
            .insert(snapshot.job_id.clone(), task.clone());
        request
            .notification_handle
            .send_output_chunk(BashOutputChunk {
                base: notification_base(&task, Vec::new(), 0, false).await,
            });
        self.start_tailer(task.clone());
        Ok(task)
    }

    fn start_tailer(&self, task: Arc<DaemonTask>) {
        if task.tailer_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let socket = self.socket.clone();
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            if let Err(error) = tail_job(&socket, task.clone(), shutdown).await {
                *task.tailer_error.lock().await = Some(error.to_string());
                task.completion.notify_waiters();
            }
        });
    }

    async fn wait_until_terminal(
        &self,
        task: &Arc<DaemonTask>,
        maximum_wait: Option<Duration>,
    ) -> Result<bool, ComputerError> {
        let wait = async {
            loop {
                let notified = task.completion.notified();
                if task.tailer_complete.load(Ordering::Acquire) {
                    return Ok(true);
                }
                if let Some(error) = task.tailer_error.lock().await.clone() {
                    return Err(ComputerError::io(error));
                }
                notified.await;
            }
        };
        match maximum_wait {
            Some(timeout) => match tokio::time::timeout(timeout, wait).await {
                Ok(result) => result,
                Err(_) => Ok(false),
            },
            None => wait.await,
        }
    }

    async fn task_result(&self, task: &Arc<DaemonTask>) -> TerminalRunResult {
        let native = task.native.read().await.clone();
        let metadata = task.metadata.read().await.clone();
        let capture = task.capture.lock().await;
        let filtered = capture.render();
        TerminalRunResult {
            combined_output: filtered.text,
            exit_code: native.exit_code,
            truncated: filtered.truncated,
            signal: lifecycle_signal(native.lifecycle)
                .map(str::to_owned)
                .or_else(|| {
                    task.backgrounded
                        .load(Ordering::Acquire)
                        .then(|| "backgrounded".to_owned())
                }),
            timed_out: native.lifecycle == JobLifecycle::TimedOut,
            output_file: metadata.output_file,
            total_bytes: capture.total_bytes,
            structure: filtered.structure,
            pid: native.pid,
        }
    }

    async fn task_snapshot(task: &Arc<DaemonTask>) -> TaskSnapshot {
        let native = task.native.read().await.clone();
        let metadata = task.metadata.read().await.clone();
        let capture = task.capture.lock().await;
        let filtered = capture.render();
        TaskSnapshot {
            task_id: native.job_id,
            command: metadata.command,
            display_command: metadata.display_command,
            cwd: metadata.cwd.display().to_string(),
            start_time: system_time(native.started_unix_ms.unwrap_or(native.created_unix_ms)),
            end_time: native.finished_unix_ms.map(system_time),
            output: filtered.text,
            output_file: metadata.output_file,
            truncated: filtered.truncated,
            exit_code: native.exit_code,
            signal: lifecycle_signal(native.lifecycle).map(str::to_owned),
            completed: native.lifecycle.is_terminal()
                && task.tailer_complete.load(Ordering::Acquire),
            kind: metadata.kind,
            block_waited: task.block_waited.load(Ordering::Acquire),
            explicitly_killed: task.explicitly_killed.load(Ordering::Acquire),
            owner_session_id: metadata.owner_session_id,
            description: metadata.description,
            is_backgrounded: task.backgrounded.load(Ordering::Acquire),
        }
    }

    async fn restore(&self, snapshot: JobSnapshot) -> Option<Arc<DaemonTask>> {
        if let Some(existing) = self.tasks.read().await.get(&snapshot.job_id).cloned() {
            *existing.native.write().await = snapshot;
            return Some(existing);
        }
        let metadata = deserialize_metadata(&snapshot.metadata)?;
        if let Some(parent) = metadata.output_file.parent()
            && tokio::fs::create_dir_all(parent).await.is_err()
        {
            return None;
        }
        if tokio::fs::File::create(&metadata.output_file)
            .await
            .is_err()
        {
            return None;
        }
        let backgrounded = snapshot
            .metadata
            .get("is_backgrounded")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true);
        let task = Arc::new(DaemonTask {
            capture: Mutex::new(BoundedCapture::new(metadata.output_limit)),
            metadata: RwLock::new(metadata),
            native: RwLock::new(snapshot.clone()),
            completion: Notify::new(),
            backgrounded: AtomicBool::new(backgrounded),
            backgrounded_notify: Notify::new(),
            block_waited: AtomicBool::new(false),
            explicitly_killed: AtomicBool::new(false),
            tailer_error: Mutex::new(None),
            tailer_started: AtomicBool::new(false),
            tailer_complete: AtomicBool::new(false),
        });
        self.tasks
            .write()
            .await
            .insert(snapshot.job_id, task.clone());
        self.start_tailer(task.clone());
        Some(task)
    }

    async fn list_native(&self) -> Result<Vec<JobSnapshot>, ComputerError> {
        let output = invoke(
            &self.socket,
            "native.command.list",
            serde_json::json!({"cursor": 0, "limit": 512}),
            Duration::from_secs(5),
        )
        .await?;
        serde_json::from_value(provider_result(output))
            .map_err(|error| ComputerError::io(format!("invalid daemon job list: {error}")))
    }

    async fn set_backgrounded(&self, task: &Arc<DaemonTask>) -> Result<(), ComputerError> {
        if task.backgrounded.load(Ordering::Acquire) {
            return Ok(());
        }
        let job_id = task.native.read().await.job_id.clone();
        invoke(
            &self.socket,
            "native.command.metadata",
            serde_json::json!({
                "job_id": job_id,
                "metadata": {"is_backgrounded": true},
            }),
            Duration::from_secs(5),
        )
        .await?;
        if task.backgrounded.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        task.backgrounded_notify.notify_waiters();
        let snapshot = Self::task_snapshot(task).await;
        let metadata = task.metadata.read().await.clone();
        let total_bytes = task.capture.lock().await.total_bytes;
        metadata
            .notification
            .send_backgrounded(BashExecutionBackgrounded {
                base: BashNotificationBase {
                    tool_call_id: metadata.tool_call_id.clone(),
                    command: metadata.command.clone(),
                    output: snapshot.output.as_bytes().to_vec(),
                    total_bytes,
                    truncated: snapshot.truncated,
                    cwd: metadata.cwd.clone(),
                },
                output_file: metadata.output_file.clone(),
                task_id: snapshot.task_id,
                monitor_description: (metadata.kind == TaskKind::Monitor)
                    .then(|| metadata.description.clone())
                    .flatten(),
                description: metadata.description.clone(),
            });
        Ok(())
    }
}

impl Drop for DaemonTerminalBackend {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

#[async_trait::async_trait]
impl TerminalBackend for DaemonTerminalBackend {
    async fn run(&self, request: TerminalRunRequest) -> Result<TerminalRunResult, ComputerError> {
        let auto_background = request.auto_background_on_timeout;
        let block_budget = request
            .foreground_block_budget
            .unwrap_or(DEFAULT_FOREGROUND_BUDGET);
        let command_timeout = request.timeout;
        let task = self.start(request, false).await?;
        if auto_background {
            let maximum_wait = if block_budget == Duration::MAX {
                command_timeout
            } else {
                block_budget.min(command_timeout)
            };
            if !self.wait_until_terminal(&task, Some(maximum_wait)).await? {
                self.set_backgrounded(&task).await?;
            }
        } else {
            self.wait_until_terminal(&task, None).await?;
        }
        Ok(self.task_result(&task).await)
    }

    async fn run_background(
        &self,
        request: TerminalRunRequest,
    ) -> Result<BackgroundHandle, ComputerError> {
        let output_file = request.output_file.clone();
        let task = self.start(request, true).await?;
        let native = task.native.read().await;
        Ok(BackgroundHandle {
            task_id: native.job_id.clone(),
            output_file,
            pid: native.pid,
        })
    }

    async fn get_task(&self, task_id: &str) -> Option<TaskSnapshot> {
        if let Some(task) = self.tasks.read().await.get(task_id).cloned() {
            return Some(Self::task_snapshot(&task).await);
        }
        let output = invoke(
            &self.socket,
            "native.command.status",
            serde_json::json!({"job_id": task_id}),
            Duration::from_secs(5),
        )
        .await
        .ok()?;
        let snapshot = serde_json::from_value(provider_result(output)).ok()?;
        let task = self.restore(snapshot).await?;
        Some(Self::task_snapshot(&task).await)
    }

    async fn kill_task(&self, task_id: &str) -> KillOutcome {
        let _ = self.get_task(task_id).await;
        if let Some(task) = self.tasks.read().await.get(task_id).cloned()
            && task.native.read().await.lifecycle.is_terminal()
        {
            return KillOutcome::AlreadyExited;
        }
        match invoke(
            &self.socket,
            "native.command.cancel",
            serde_json::json!({"job_id": task_id}),
            Duration::from_secs(5),
        )
        .await
        {
            Ok(_) => {
                if let Some(task) = self.tasks.read().await.get(task_id) {
                    task.explicitly_killed.store(true, Ordering::Release);
                }
                KillOutcome::Killed
            }
            Err(_) => KillOutcome::NotFound,
        }
    }

    async fn kill_foreground_commands(&self) {
        let tasks = self
            .tasks
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for task in tasks {
            if !task.backgrounded.load(Ordering::Acquire)
                && !task.native.read().await.lifecycle.is_terminal()
            {
                let id = task.native.read().await.job_id.clone();
                let _ = self.kill_task(&id).await;
            }
        }
    }

    async fn kill_foreground_commands_by_owner(&self, owner_session_id: &str) {
        let tasks = self
            .tasks
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for task in tasks {
            if task.metadata.read().await.owner_session_id.as_deref() == Some(owner_session_id)
                && !task.backgrounded.load(Ordering::Acquire)
                && !task.native.read().await.lifecycle.is_terminal()
            {
                let id = task.native.read().await.job_id.clone();
                let _ = self.kill_task(&id).await;
            }
        }
    }

    async fn kill_all_background_tasks(&self) {
        let tasks = self
            .tasks
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for task in tasks {
            if task.backgrounded.load(Ordering::Acquire)
                && !task.native.read().await.lifecycle.is_terminal()
            {
                let id = task.native.read().await.job_id.clone();
                let _ = self.kill_task(&id).await;
            }
        }
    }

    async fn kill_all_background_tasks_by_owner(&self, owner_session_id: &str) {
        let tasks = self
            .tasks
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for task in tasks {
            if task.metadata.read().await.owner_session_id.as_deref() == Some(owner_session_id)
                && task.backgrounded.load(Ordering::Acquire)
                && !task.native.read().await.lifecycle.is_terminal()
            {
                let id = task.native.read().await.job_id.clone();
                let _ = self.kill_task(&id).await;
            }
        }
    }

    async fn reparent_notifications(
        &self,
        old_owner_session_id: &str,
        new_owner_session_id: &str,
        new_handle: ToolNotificationHandle,
        _backend_weak: std::sync::Weak<dyn TerminalBackend>,
    ) {
        let tasks = self
            .tasks
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for task in tasks {
            let job_id = task.native.read().await.job_id.clone();
            let changed = {
                let mut metadata = task.metadata.write().await;
                if metadata.owner_session_id.as_deref() == Some(old_owner_session_id) {
                    metadata.owner_session_id = Some(new_owner_session_id.to_owned());
                    metadata.notification = new_handle.clone();
                    true
                } else {
                    false
                }
            };
            if changed {
                if let Err(error) = invoke(
                    &self.socket,
                    "native.command.metadata",
                    serde_json::json!({
                        "job_id": job_id,
                        "metadata": {"owner_session_id": new_owner_session_id},
                    }),
                    Duration::from_secs(5),
                )
                .await
                {
                    tracing::warn!(
                        job_id,
                        %error,
                        "failed to persist daemon terminal notification reparenting"
                    );
                }
            }
        }
    }

    async fn background_foreground_command(&self, tool_call_id: &str) -> bool {
        let tasks = self
            .tasks
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for task in tasks {
            if task.metadata.read().await.tool_call_id == tool_call_id
                && !task.native.read().await.lifecycle.is_terminal()
            {
                return self.set_backgrounded(&task).await.is_ok();
            }
        }
        false
    }

    async fn wait_for_completion(
        &self,
        task_id: &str,
        timeout: Option<Duration>,
    ) -> Option<TaskSnapshot> {
        let task = if let Some(task) = self.tasks.read().await.get(task_id).cloned() {
            task
        } else {
            self.get_task(task_id).await?;
            self.tasks.read().await.get(task_id).cloned()?
        };
        task.block_waited.store(true, Ordering::Release);
        match self.wait_until_terminal(&task, timeout).await {
            Ok(true) => Some(Self::task_snapshot(&task).await),
            Ok(false) | Err(_) => {
                task.block_waited.store(false, Ordering::Release);
                Some(Self::task_snapshot(&task).await)
            }
        }
    }

    async fn list_tasks(&self) -> Vec<TaskSnapshot> {
        if let Ok(snapshots) = self.list_native().await {
            for snapshot in snapshots {
                if snapshot
                    .metadata
                    .get("terminal_backend")
                    .and_then(|v| v.as_str())
                    == Some("grokd")
                {
                    let _ = self.restore(snapshot).await;
                }
            }
        }
        let tasks = self
            .tasks
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut snapshots = Vec::with_capacity(tasks.len());
        for task in tasks {
            snapshots.push(Self::task_snapshot(&task).await);
        }
        snapshots.sort_by(|left, right| left.start_time.cmp(&right.start_time));
        snapshots
    }
}

async fn tail_job(
    socket: &Path,
    task: Arc<DaemonTask>,
    shutdown: CancellationToken,
) -> Result<(), ComputerError> {
    let output_file = task.metadata.read().await.output_file.clone();
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&output_file)
        .await?;
    let mut cursor = 0_u64;
    let mut disconnected_at = None;
    loop {
        if shutdown.is_cancelled() {
            return Ok(());
        }
        let job_id = task.native.read().await.job_id.clone();
        let output = match invoke(
            socket,
            "native.command.output",
            serde_json::json!({
                "job_id": job_id,
                "cursor": cursor,
                "maximum_records": PAGE_RECORDS,
                "maximum_bytes": PAGE_BYTES,
            }),
            Duration::from_secs(5),
        )
        .await
        {
            Ok(output) => output,
            Err(error) => {
                record_daemon_failure(&mut disconnected_at, &error)?;
                tokio::select! {
                    _ = shutdown.cancelled() => return Ok(()),
                    _ = tokio::time::sleep(Duration::from_millis(250)) => continue,
                }
            }
        };
        disconnected_at = None;
        let page: OutputPage = serde_json::from_value(provider_result(output))
            .map_err(|error| ComputerError::io(format!("invalid daemon output page: {error}")))?;
        let has_more = page.next_cursor.is_some();
        publish_page(&mut file, &task, page.records).await?;
        cursor = page.end_cursor;
        if has_more {
            continue;
        }
        let job_id = task.native.read().await.job_id.clone();
        let status = invoke(
            socket,
            "native.command.status",
            serde_json::json!({"job_id": job_id}),
            Duration::from_secs(5),
        )
        .await;
        match status {
            Ok(status) => {
                disconnected_at = None;
                let snapshot: JobSnapshot = serde_json::from_value(provider_result(status))
                    .map_err(|error| {
                        ComputerError::io(format!("invalid daemon job status: {error}"))
                    })?;
                let terminal = snapshot.lifecycle.is_terminal();
                *task.native.write().await = snapshot;
                if terminal {
                    // The supervisor marks a job terminal only after both stream
                    // readers stop. Drain every page written between the last
                    // read and that terminal snapshot before publishing completion.
                    loop {
                        let final_output = invoke(
                            socket,
                            "native.command.output",
                            serde_json::json!({
                                "job_id": job_id,
                                "cursor": cursor,
                                "maximum_records": PAGE_RECORDS,
                                "maximum_bytes": PAGE_BYTES,
                            }),
                            Duration::from_secs(5),
                        )
                        .await?;
                        let final_page: OutputPage = serde_json::from_value(provider_result(
                            final_output,
                        ))
                        .map_err(|error| {
                            ComputerError::io(format!("invalid final daemon output page: {error}"))
                        })?;
                        let has_more = final_page.next_cursor.is_some();
                        cursor = final_page.end_cursor;
                        publish_page(&mut file, &task, final_page.records).await?;
                        if !has_more {
                            break;
                        }
                    }
                    task.tailer_complete.store(true, Ordering::Release);
                    task.completion.notify_waiters();
                    if task.backgrounded.load(Ordering::Acquire) {
                        let snapshot = DaemonTerminalBackend::task_snapshot(&task).await;
                        task.metadata
                            .read()
                            .await
                            .notification
                            .send_task_complete(snapshot);
                    }
                    return Ok(());
                }
            }
            Err(error) => record_daemon_failure(&mut disconnected_at, &error)?,
        }
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    }
}

fn record_daemon_failure(
    disconnected_at: &mut Option<Instant>,
    error: &ComputerError,
) -> Result<(), ComputerError> {
    let since = disconnected_at.get_or_insert_with(Instant::now);
    if since.elapsed() >= DAEMON_RECONNECT_BUDGET {
        Err(ComputerError::io(format!(
            "daemon terminal remained unavailable for {} seconds: {error}",
            DAEMON_RECONNECT_BUDGET.as_secs()
        )))
    } else {
        Ok(())
    }
}

async fn publish_page(
    file: &mut tokio::fs::File,
    task: &DaemonTask,
    records: Vec<xai_grok_native_execution::OutputRecord>,
) -> Result<(), ComputerError> {
    if records.is_empty() {
        return Ok(());
    }
    let metadata = task.metadata.read().await.clone();
    let base = {
        let mut capture = task.capture.lock().await;
        for record in records {
            file.write_all(&record.bytes).await?;
            capture.push(&record.bytes);
        }
        file.flush().await?;
        BashNotificationBase {
            tool_call_id: metadata.tool_call_id.clone(),
            command: metadata.command.clone(),
            output: capture.tail.clone(),
            total_bytes: capture.total_bytes,
            truncated: capture.truncated,
            cwd: metadata.cwd.clone(),
        }
    };
    metadata
        .notification
        .send_output_chunk(BashOutputChunk { base });
    Ok(())
}

async fn invoke(
    socket: &Path,
    operation: &str,
    input: serde_json::Value,
    timeout: Duration,
) -> Result<serde_json::Value, ComputerError> {
    let client = ControlPlaneClient::connect(
        socket,
        ClientIdentity::new("grok-daemon-terminal", env!("CARGO_PKG_VERSION")),
    )
    .await
    .map_err(|error| ComputerError::io(error.to_string()))?;
    let response = client
        .send(
            Command::InvokeProvider {
                request_id: RequestId::new(),
                operation_id: OperationId::from_string(operation),
                preferred_provider: Some(ProviderId::from_string(NATIVE_PROVIDER_ID)),
                input,
            },
            timeout,
        )
        .await
        .map_err(|error| ComputerError::io(error.to_string()))?;
    match response {
        Response::ProviderInvoked { output, .. } => Ok(output),
        other => Err(ComputerError::io(format!(
            "daemon returned unexpected terminal response: {other:?}"
        ))),
    }
}

fn provider_result(output: serde_json::Value) -> serde_json::Value {
    if output.get("artifacts").is_some() {
        output.get("result").cloned().unwrap_or(output)
    } else {
        output
    }
}

fn serialize_metadata(
    metadata: &TaskMetadata,
    backgrounded: bool,
) -> Result<BTreeMap<String, serde_json::Value>, ComputerError> {
    let mut values = BTreeMap::new();
    values.insert("terminal_backend".to_owned(), "grokd".into());
    values.insert("command".to_owned(), metadata.command.clone().into());
    values.insert(
        "display_command".to_owned(),
        serde_json::to_value(&metadata.display_command)
            .map_err(|error| ComputerError::io(error.to_string()))?,
    );
    values.insert("cwd".to_owned(), metadata.cwd.display().to_string().into());
    values.insert(
        "output_file".to_owned(),
        metadata.output_file.display().to_string().into(),
    );
    values.insert("output_limit".to_owned(), metadata.output_limit.into());
    values.insert(
        "tool_call_id".to_owned(),
        metadata.tool_call_id.clone().into(),
    );
    values.insert(
        "kind".to_owned(),
        serde_json::to_value(metadata.kind)
            .map_err(|error| ComputerError::io(error.to_string()))?,
    );
    values.insert(
        "owner_session_id".to_owned(),
        serde_json::to_value(&metadata.owner_session_id)
            .map_err(|error| ComputerError::io(error.to_string()))?,
    );
    values.insert(
        "description".to_owned(),
        serde_json::to_value(&metadata.description)
            .map_err(|error| ComputerError::io(error.to_string()))?,
    );
    values.insert("is_backgrounded".to_owned(), backgrounded.into());
    Ok(values)
}

fn deserialize_metadata(values: &BTreeMap<String, serde_json::Value>) -> Option<TaskMetadata> {
    (values.get("terminal_backend")?.as_str()? == "grokd").then_some(())?;
    Some(TaskMetadata {
        command: values.get("command")?.as_str()?.to_owned(),
        display_command: serde_json::from_value(values.get("display_command")?.clone()).ok()?,
        cwd: PathBuf::from(values.get("cwd")?.as_str()?),
        output_file: PathBuf::from(values.get("output_file")?.as_str()?),
        output_limit: values
            .get("output_limit")?
            .as_u64()
            .and_then(|value| usize::try_from(value).ok())?,
        notification: ToolNotificationHandle::noop(),
        tool_call_id: values.get("tool_call_id")?.as_str()?.to_owned(),
        kind: serde_json::from_value(values.get("kind")?.clone()).ok()?,
        owner_session_id: serde_json::from_value(values.get("owner_session_id")?.clone()).ok()?,
        description: serde_json::from_value(values.get("description")?.clone()).ok()?,
    })
}

async fn notification_base(
    task: &DaemonTask,
    output: Vec<u8>,
    total_bytes: usize,
    truncated: bool,
) -> BashNotificationBase {
    let metadata = task.metadata.read().await;
    BashNotificationBase {
        tool_call_id: metadata.tool_call_id.clone(),
        command: metadata.command.clone(),
        output,
        total_bytes,
        truncated,
        cwd: metadata.cwd.clone(),
    }
}

fn lifecycle_signal(lifecycle: JobLifecycle) -> Option<&'static str> {
    match lifecycle {
        JobLifecycle::Cancelled => Some("killed"),
        JobLifecycle::TimedOut => Some("timeout"),
        JobLifecycle::Lost => Some("lost"),
        _ => None,
    }
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis())
        .unwrap_or(u64::MAX)
        .clamp(1, 24 * 60 * 60 * 1_000)
}

fn system_time(unix_ms: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(unix_ms)
}
