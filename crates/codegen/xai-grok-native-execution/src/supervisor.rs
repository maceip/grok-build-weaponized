use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{Mutex, RwLock, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::nmap::{NmapRequest, NmapResult, parse_result};
use crate::spool::{OutputPage, OutputStream, SequencedSpool, SpoolBudget, read_page};

const DEFAULT_TIMEOUT_MS: u64 = 20 * 60 * 1_000;
const MAX_PAGE_RECORDS: usize = 512;
const MAX_PAGE_BYTES: usize = 8 * 1024 * 1024;
const MAX_STDIN_WRITE_BYTES: usize = 1024 * 1024;
const STDIN_READY_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
pub enum NativeExecutionError {
    #[error("native execution I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid native execution request: {0}")]
    InvalidRequest(String),
    #[error("native execution target is outside the declared scope: {0}")]
    OutOfScope(String),
    #[error("native job does not exist: {0}")]
    NotFound(String),
    #[error("native execution supervisor is at capacity")]
    Overloaded,
    #[error("native job output is invalid: {0}")]
    InvalidOutput(String),
    #[error("native job is not complete")]
    NotComplete,
    #[error("native job is not an Nmap job")]
    NotNmap,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobKind {
    Command,
    Nmap,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobLifecycle {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
    TimedOut,
    Lost,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandStdin {
    #[default]
    Null,
    Pipe,
}

impl JobLifecycle {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::TimedOut | Self::Lost
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandRequest {
    pub executable: String,
    #[serde(default)]
    pub args: Vec<String>,
    pub cwd: Option<PathBuf>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
    #[serde(default)]
    pub stdin: CommandStdin,
}

fn default_timeout() -> u64 {
    DEFAULT_TIMEOUT_MS
}

impl CommandRequest {
    fn validate(&self) -> Result<(), NativeExecutionError> {
        if self.executable.trim().is_empty() || self.executable.as_bytes().contains(&0) {
            return Err(NativeExecutionError::InvalidRequest(
                "executable must not be empty or contain NUL".to_owned(),
            ));
        }
        if self.args.iter().any(|arg| arg.as_bytes().contains(&0)) {
            return Err(NativeExecutionError::InvalidRequest(
                "command arguments must not contain NUL".to_owned(),
            ));
        }
        if self.timeout_ms == 0 || self.timeout_ms > 24 * 60 * 60 * 1_000 {
            return Err(NativeExecutionError::InvalidRequest(
                "command timeout must be between 1 ms and 24 hours".to_owned(),
            ));
        }
        if let Some(cwd) = &self.cwd
            && !cwd.is_dir()
        {
            return Err(NativeExecutionError::InvalidRequest(format!(
                "working directory does not exist: {}",
                cwd.display()
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JobSnapshot {
    pub job_id: String,
    pub kind: JobKind,
    pub lifecycle: JobLifecycle,
    pub executable: PathBuf,
    pub args: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub command_hash: String,
    pub pid: Option<u32>,
    pub created_unix_ms: u64,
    pub started_unix_ms: Option<u64>,
    pub finished_unix_ms: Option<u64>,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
    pub spool_bytes: u64,
    pub result: Option<serde_json::Value>,
    #[serde(default)]
    pub owner_id: String,
    #[serde(default)]
    pub stdin: CommandStdin,
    #[serde(default)]
    pub stdin_closed: bool,
}

enum JobStdinState {
    Null,
    Pending,
    Open(ChildStdin),
    Closed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobArtifact {
    pub media_type: String,
    pub path: PathBuf,
    pub byte_size: u64,
}

struct JobState {
    snapshot: Mutex<JobSnapshot>,
    cancellation: CancellationToken,
    spool_path: PathBuf,
    metadata_path: PathBuf,
    nmap_target: Option<String>,
    nmap_xml_path: Option<PathBuf>,
    stdin: Mutex<JobStdinState>,
}

pub struct NativeExecutionSupervisor {
    root: PathBuf,
    jobs: RwLock<HashMap<String, Arc<JobState>>>,
    permits: Arc<Semaphore>,
    maximum_jobs: usize,
    maximum_spool_bytes_per_job: u64,
    spool_budget: Arc<SpoolBudget>,
}

#[derive(Clone, Debug)]
pub struct NativeExecutionLimits {
    pub maximum_parallel: usize,
    pub maximum_jobs: usize,
    pub maximum_spool_bytes_per_job: u64,
    pub maximum_spool_bytes_per_owner: u64,
    pub maximum_total_spool_bytes: u64,
}

impl NativeExecutionSupervisor {
    pub async fn open(
        root: impl Into<PathBuf>,
        maximum_parallel: usize,
        maximum_jobs: usize,
        maximum_spool_bytes_per_job: u64,
    ) -> Result<Arc<Self>, NativeExecutionError> {
        let maximum_total_spool_bytes = maximum_spool_bytes_per_job
            .saturating_mul(u64::try_from(maximum_jobs.max(1)).unwrap_or(u64::MAX));
        Self::open_with_limits(
            root,
            NativeExecutionLimits {
                maximum_parallel,
                maximum_jobs,
                maximum_spool_bytes_per_job,
                maximum_spool_bytes_per_owner: maximum_total_spool_bytes,
                maximum_total_spool_bytes,
            },
        )
        .await
    }

    pub async fn open_with_limits(
        root: impl Into<PathBuf>,
        limits: NativeExecutionLimits,
    ) -> Result<Arc<Self>, NativeExecutionError> {
        let root = root.into();
        tokio::fs::create_dir_all(root.join("jobs")).await?;
        let maximum_total_spool_bytes = limits.maximum_total_spool_bytes.max(1024);
        let maximum_spool_bytes_per_owner = limits
            .maximum_spool_bytes_per_owner
            .max(1024)
            .min(maximum_total_spool_bytes);
        let supervisor = Arc::new(Self {
            root,
            jobs: RwLock::new(HashMap::new()),
            permits: Arc::new(Semaphore::new(limits.maximum_parallel.max(1))),
            maximum_jobs: limits.maximum_jobs.max(1),
            maximum_spool_bytes_per_job: limits.maximum_spool_bytes_per_job.max(1024),
            spool_budget: Arc::new(SpoolBudget::new(
                maximum_total_spool_bytes,
                maximum_spool_bytes_per_owner,
            )),
        });
        supervisor.recover().await?;
        Ok(supervisor)
    }

    pub async fn start_command(
        self: &Arc<Self>,
        request: CommandRequest,
    ) -> Result<JobSnapshot, NativeExecutionError> {
        self.start_command_for("local", request).await
    }

    pub async fn start_command_for(
        self: &Arc<Self>,
        owner_id: impl Into<String>,
        request: CommandRequest,
    ) -> Result<JobSnapshot, NativeExecutionError> {
        request.validate()?;
        let executable = resolve_executable(&request.executable).await?;
        self.start_process(
            owner_id.into(),
            JobKind::Command,
            executable,
            request.args,
            request.cwd,
            request.env,
            request.timeout_ms,
            None,
            None,
            request.stdin,
        )
        .await
    }

    pub async fn start_nmap(
        self: &Arc<Self>,
        request: NmapRequest,
    ) -> Result<JobSnapshot, NativeExecutionError> {
        self.start_nmap_for("local", request).await
    }

    pub async fn start_nmap_for(
        self: &Arc<Self>,
        owner_id: impl Into<String>,
        request: NmapRequest,
    ) -> Result<JobSnapshot, NativeExecutionError> {
        request.validate()?;
        let executable = resolve_executable("nmap").await?;
        let job_id = new_job_id();
        let job_root = self.root.join("jobs").join(&job_id);
        let xml_path = job_root.join("nmap.xml");
        let args = request.arguments(&xml_path);
        self.start_process_with_id(
            job_id,
            owner_id.into(),
            JobKind::Nmap,
            executable,
            args,
            None,
            BTreeMap::new(),
            request.timeout_ms,
            Some(request.target),
            Some(xml_path),
            CommandStdin::Null,
        )
        .await
    }

    pub async fn snapshot(&self, job_id: &str) -> Result<JobSnapshot, NativeExecutionError> {
        let job = self.job(job_id).await?;
        let mut snapshot = job.snapshot.lock().await.clone();
        snapshot.spool_bytes = tokio::fs::metadata(&job.spool_path)
            .await
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        Ok(snapshot)
    }

    pub async fn wait(
        &self,
        job_id: &str,
        maximum_wait: Duration,
    ) -> Result<JobSnapshot, NativeExecutionError> {
        let deadline = tokio::time::Instant::now() + maximum_wait;
        loop {
            let snapshot = self.snapshot(job_id).await?;
            if snapshot.lifecycle.is_terminal() || tokio::time::Instant::now() >= deadline {
                return Ok(snapshot);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    pub async fn output_page(
        &self,
        job_id: &str,
        cursor: u64,
        maximum_records: usize,
        maximum_bytes: usize,
    ) -> Result<OutputPage, NativeExecutionError> {
        let job = self.job(job_id).await?;
        Ok(read_page(
            &job.spool_path,
            cursor,
            maximum_records.clamp(1, MAX_PAGE_RECORDS),
            maximum_bytes.clamp(1, MAX_PAGE_BYTES),
        )
        .await?)
    }

    pub async fn cancel(&self, job_id: &str) -> Result<JobSnapshot, NativeExecutionError> {
        let job = self.job(job_id).await?;
        job.cancellation.cancel();
        self.wait(job_id, Duration::from_secs(2)).await
    }

    /// Write a bounded chunk to a command's daemon-owned stdin pipe.
    ///
    /// A start request may still be queued behind the execution semaphore, so
    /// writers wait for that exact job's pipe to materialize. The caller's
    /// control-plane command deadline remains the outer bound; this local wait
    /// prevents an orphaned write from hanging forever if process startup is
    /// poisoned.
    pub async fn write_stdin(
        &self,
        job_id: &str,
        bytes: &[u8],
        close: bool,
    ) -> Result<JobSnapshot, NativeExecutionError> {
        if bytes.len() > MAX_STDIN_WRITE_BYTES {
            return Err(NativeExecutionError::InvalidRequest(format!(
                "stdin write is {} bytes; maximum is {MAX_STDIN_WRITE_BYTES}",
                bytes.len()
            )));
        }
        if bytes.is_empty() && !close {
            return Err(NativeExecutionError::InvalidRequest(
                "stdin write must contain bytes or request close".to_owned(),
            ));
        }
        let job = self.job(job_id).await?;
        let deadline = tokio::time::Instant::now() + STDIN_READY_TIMEOUT;
        loop {
            let mut stdin = job.stdin.lock().await;
            match &mut *stdin {
                JobStdinState::Null => {
                    return Err(NativeExecutionError::InvalidRequest(format!(
                        "native job {job_id} was not started with piped stdin"
                    )));
                }
                JobStdinState::Pending => {
                    drop(stdin);
                    let snapshot = job.snapshot.lock().await.clone();
                    if snapshot.lifecycle.is_terminal() {
                        return Err(NativeExecutionError::InvalidRequest(format!(
                            "native job {job_id} terminated before stdin became available"
                        )));
                    }
                    if tokio::time::Instant::now() >= deadline {
                        return Err(NativeExecutionError::InvalidRequest(format!(
                            "native job {job_id} stdin did not become ready"
                        )));
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                JobStdinState::Open(writer) => {
                    writer.write_all(bytes).await?;
                    writer.flush().await?;
                    if close {
                        *stdin = JobStdinState::Closed;
                    }
                    drop(stdin);
                    if close {
                        let mut snapshot = job.snapshot.lock().await;
                        snapshot.stdin_closed = true;
                        persist_snapshot(&job.metadata_path, &snapshot).await?;
                    }
                    return self.snapshot(job_id).await;
                }
                JobStdinState::Closed => {
                    return Err(NativeExecutionError::InvalidRequest(format!(
                        "native job {job_id} stdin is closed"
                    )));
                }
            }
        }
    }

    /// Return immutable file-backed artifacts only after every output reader
    /// has stopped and the terminal snapshot has been persisted.
    pub async fn artifacts(&self, job_id: &str) -> Result<Vec<JobArtifact>, NativeExecutionError> {
        let job = self.job(job_id).await?;
        let snapshot = job.snapshot.lock().await.clone();
        if !snapshot.lifecycle.is_terminal() {
            return Err(NativeExecutionError::NotComplete);
        }
        let mut artifacts = Vec::new();
        if let Ok(metadata) = tokio::fs::symlink_metadata(&job.spool_path).await
            && metadata.file_type().is_file()
            && metadata.len() > 0
        {
            artifacts.push(JobArtifact {
                media_type: "application/vnd.grok.native-output-spool".to_owned(),
                path: job.spool_path.clone(),
                byte_size: metadata.len(),
            });
        }
        if snapshot.kind == JobKind::Nmap
            && snapshot.lifecycle == JobLifecycle::Completed
            && let Some(path) = &job.nmap_xml_path
            && let Ok(metadata) = tokio::fs::symlink_metadata(path).await
            && metadata.file_type().is_file()
        {
            artifacts.push(JobArtifact {
                media_type: "application/xml".to_owned(),
                path: path.clone(),
                byte_size: metadata.len(),
            });
        }
        Ok(artifacts)
    }

    pub async fn cleanup(&self, job_id: &str) -> Result<JobSnapshot, NativeExecutionError> {
        let job = self.job(job_id).await?;
        let snapshot = job.snapshot.lock().await.clone();
        if !snapshot.lifecycle.is_terminal() {
            return Err(NativeExecutionError::NotComplete);
        }
        let spool_bytes = tokio::fs::symlink_metadata(&job.spool_path)
            .await
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        let job_root = job
            .metadata_path
            .parent()
            .ok_or_else(|| {
                NativeExecutionError::InvalidOutput("job metadata has no parent".to_owned())
            })?
            .to_path_buf();
        tokio::fs::remove_dir_all(&job_root).await?;
        self.jobs.write().await.remove(job_id);
        self.spool_budget
            .release(&snapshot.owner_id, spool_bytes)
            .await;
        Ok(snapshot)
    }

    pub async fn nmap_result(&self, job_id: &str) -> Result<NmapResult, NativeExecutionError> {
        let job = self.job(job_id).await?;
        let snapshot = job.snapshot.lock().await.clone();
        if snapshot.kind != JobKind::Nmap {
            return Err(NativeExecutionError::NotNmap);
        }
        if snapshot.lifecycle != JobLifecycle::Completed {
            return Err(NativeExecutionError::NotComplete);
        }
        if let Some(result) = snapshot.result {
            return serde_json::from_value(result)
                .map_err(|error| NativeExecutionError::InvalidOutput(error.to_string()));
        }
        let target = job
            .nmap_target
            .as_deref()
            .ok_or(NativeExecutionError::NotNmap)?;
        let path = job
            .nmap_xml_path
            .as_deref()
            .ok_or(NativeExecutionError::NotNmap)?;
        parse_result(target, path).await
    }

    async fn start_process(
        self: &Arc<Self>,
        owner_id: String,
        kind: JobKind,
        executable: PathBuf,
        args: Vec<String>,
        cwd: Option<PathBuf>,
        env: BTreeMap<String, String>,
        timeout_ms: u64,
        nmap_target: Option<String>,
        nmap_xml_path: Option<PathBuf>,
        stdin: CommandStdin,
    ) -> Result<JobSnapshot, NativeExecutionError> {
        self.start_process_with_id(
            new_job_id(),
            owner_id,
            kind,
            executable,
            args,
            cwd,
            env,
            timeout_ms,
            nmap_target,
            nmap_xml_path,
            stdin,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_process_with_id(
        self: &Arc<Self>,
        job_id: String,
        owner_id: String,
        kind: JobKind,
        executable: PathBuf,
        args: Vec<String>,
        cwd: Option<PathBuf>,
        env: BTreeMap<String, String>,
        timeout_ms: u64,
        nmap_target: Option<String>,
        nmap_xml_path: Option<PathBuf>,
        stdin: CommandStdin,
    ) -> Result<JobSnapshot, NativeExecutionError> {
        if self.jobs.read().await.len() >= self.maximum_jobs {
            return Err(NativeExecutionError::Overloaded);
        }
        let job_root = self.root.join("jobs").join(&job_id);
        tokio::fs::create_dir_all(&job_root).await?;
        let spool_path = job_root.join("output.spool");
        let metadata_path = job_root.join("job.json");
        let command_hash = command_hash(&executable, &args, cwd.as_deref(), &env);
        let snapshot = JobSnapshot {
            job_id: job_id.clone(),
            kind,
            lifecycle: JobLifecycle::Queued,
            executable: executable.clone(),
            args: args.clone(),
            cwd: cwd.clone(),
            command_hash,
            pid: None,
            created_unix_ms: now_unix_ms(),
            started_unix_ms: None,
            finished_unix_ms: None,
            exit_code: None,
            error: None,
            spool_bytes: 0,
            result: None,
            owner_id,
            stdin,
            stdin_closed: stdin == CommandStdin::Null,
        };
        persist_snapshot(&metadata_path, &snapshot).await?;
        let job = Arc::new(JobState {
            snapshot: Mutex::new(snapshot.clone()),
            cancellation: CancellationToken::new(),
            spool_path,
            metadata_path,
            nmap_target,
            nmap_xml_path,
            stdin: Mutex::new(match stdin {
                CommandStdin::Null => JobStdinState::Null,
                CommandStdin::Pipe => JobStdinState::Pending,
            }),
        });
        self.jobs.write().await.insert(job_id, job.clone());
        let supervisor = self.clone();
        tokio::spawn(async move {
            supervisor
                .run_job(job, executable, args, cwd, env, timeout_ms)
                .await;
        });
        Ok(snapshot)
    }

    async fn run_job(
        self: Arc<Self>,
        job: Arc<JobState>,
        executable: PathBuf,
        args: Vec<String>,
        cwd: Option<PathBuf>,
        env: BTreeMap<String, String>,
        timeout_ms: u64,
    ) {
        let permit = tokio::select! {
            permit = self.permits.clone().acquire_owned() => match permit {
                Ok(permit) => permit,
                Err(_) => return self.finish_with_error(&job, JobLifecycle::Failed, "execution supervisor stopped").await,
            },
            _ = job.cancellation.cancelled() => {
                return self.finish_with_error(&job, JobLifecycle::Cancelled, "cancelled before start").await;
            }
        };
        let owner_id = job.snapshot.lock().await.owner_id.clone();
        let spool = match SequencedSpool::open(
            &job.spool_path,
            self.maximum_spool_bytes_per_job,
            owner_id,
            self.spool_budget.clone(),
        )
        .await
        {
            Ok(spool) => Arc::new(spool),
            Err(error) => {
                drop(permit);
                return self
                    .finish_with_error(&job, JobLifecycle::Failed, error.to_string())
                    .await;
            }
        };
        let mut command = Command::new(&executable);
        command
            .args(&args)
            .stdin(match job.snapshot.lock().await.stdin {
                CommandStdin::Null => Stdio::null(),
                CommandStdin::Pipe => Stdio::piped(),
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if let Some(cwd) = cwd {
            command.current_dir(cwd);
        }
        if !env.is_empty() {
            command.envs(env);
        }
        #[cfg(unix)]
        command.process_group(0);
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                drop(permit);
                return self
                    .finish_with_error(&job, JobLifecycle::Failed, error.to_string())
                    .await;
            }
        };
        if job.snapshot.lock().await.stdin == CommandStdin::Pipe {
            let Some(child_stdin) = child.stdin.take() else {
                drop(permit);
                return self
                    .finish_with_error(
                        &job,
                        JobLifecycle::Failed,
                        "process was configured with piped stdin but no pipe was created",
                    )
                    .await;
            };
            *job.stdin.lock().await = JobStdinState::Open(child_stdin);
        }
        {
            let mut snapshot = job.snapshot.lock().await;
            snapshot.lifecycle = JobLifecycle::Running;
            snapshot.pid = child.id();
            snapshot.started_unix_ms = Some(now_unix_ms());
            let _ = persist_snapshot(&job.metadata_path, &snapshot).await;
        }
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let stdout_reader = stdout.map(|reader| {
            tokio::spawn(copy_stream(
                reader,
                spool.clone(),
                OutputStream::Stdout,
                job.cancellation.clone(),
            ))
        });
        let stderr_reader = stderr.map(|reader| {
            tokio::spawn(copy_stream(
                reader,
                spool.clone(),
                OutputStream::Stderr,
                job.cancellation.clone(),
            ))
        });
        let timeout = tokio::time::sleep(Duration::from_millis(timeout_ms));
        tokio::pin!(timeout);
        let (lifecycle, status, error) = tokio::select! {
            status = child.wait() => match status {
                Ok(status) if status.success() => (JobLifecycle::Completed, Some(status), None),
                Ok(status) => (JobLifecycle::Failed, Some(status), Some(format!("process exited with {status}"))),
                Err(error) => (JobLifecycle::Failed, None, Some(error.to_string())),
            },
            _ = job.cancellation.cancelled() => {
                let error = terminate_child(&mut child).await.err().map(|error| error.to_string());
                (JobLifecycle::Cancelled, None, error)
            }
            _ = &mut timeout => {
                let error = terminate_child(&mut child).await.err().map(|error| error.to_string());
                (JobLifecycle::TimedOut, None, error.or_else(|| Some("process deadline elapsed".to_owned())))
            }
        };
        let mut reader_error = None;
        for reader in [stdout_reader, stderr_reader].into_iter().flatten() {
            match reader.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => reader_error = Some(error.to_string()),
                Err(error) => reader_error = Some(error.to_string()),
            }
        }
        drop(permit);
        let piped_stdin = job.snapshot.lock().await.stdin == CommandStdin::Pipe;
        if piped_stdin {
            *job.stdin.lock().await = JobStdinState::Closed;
        }
        let mut snapshot = job.snapshot.lock().await;
        snapshot.stdin_closed = piped_stdin || snapshot.stdin_closed;
        snapshot.lifecycle = if reader_error.is_some() {
            JobLifecycle::Failed
        } else {
            lifecycle
        };
        snapshot.exit_code = status.and_then(|status| status.code());
        snapshot.error = reader_error.or(error);
        snapshot.finished_unix_ms = Some(now_unix_ms());
        snapshot.spool_bytes = tokio::fs::metadata(&job.spool_path)
            .await
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        if snapshot.lifecycle == JobLifecycle::Completed
            && snapshot.kind == JobKind::Nmap
            && let (Some(target), Some(path)) = (&job.nmap_target, &job.nmap_xml_path)
        {
            match parse_result(target, path).await {
                Ok(result) => snapshot.result = serde_json::to_value(result).ok(),
                Err(error) => {
                    snapshot.lifecycle = JobLifecycle::Failed;
                    snapshot.error = Some(error.to_string());
                }
            }
        }
        let _ = persist_snapshot(&job.metadata_path, &snapshot).await;
    }

    async fn finish_with_error(
        &self,
        job: &JobState,
        lifecycle: JobLifecycle,
        error: impl Into<String>,
    ) {
        let mut snapshot = job.snapshot.lock().await;
        snapshot.lifecycle = lifecycle;
        snapshot.error = Some(error.into());
        snapshot.finished_unix_ms = Some(now_unix_ms());
        let _ = persist_snapshot(&job.metadata_path, &snapshot).await;
    }

    async fn job(&self, job_id: &str) -> Result<Arc<JobState>, NativeExecutionError> {
        self.jobs
            .read()
            .await
            .get(job_id)
            .cloned()
            .ok_or_else(|| NativeExecutionError::NotFound(job_id.to_owned()))
    }

    async fn recover(&self) -> Result<(), NativeExecutionError> {
        let mut entries = tokio::fs::read_dir(self.root.join("jobs")).await?;
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            let metadata_path = entry.path().join("job.json");
            let Ok(bytes) = tokio::fs::read(&metadata_path).await else {
                continue;
            };
            let Ok(mut snapshot) = serde_json::from_slice::<JobSnapshot>(&bytes) else {
                continue;
            };
            if !snapshot.lifecycle.is_terminal() {
                snapshot.lifecycle = JobLifecycle::Lost;
                snapshot.error = Some(
                    "daemon restarted without a provable child-process birth identity".to_owned(),
                );
                snapshot.finished_unix_ms = Some(now_unix_ms());
                persist_snapshot(&metadata_path, &snapshot).await?;
            }
            let job_root = entry.path();
            let nmap_xml_path = (snapshot.kind == JobKind::Nmap).then(|| job_root.join("nmap.xml"));
            let spool_path = job_root.join("output.spool");
            let spool_bytes = tokio::fs::symlink_metadata(&spool_path)
                .await
                .ok()
                .filter(|metadata| metadata.file_type().is_file())
                .map(|metadata| metadata.len())
                .unwrap_or(0);
            if snapshot.owner_id.is_empty() {
                snapshot.owner_id = "recovered".to_owned();
            }
            self.spool_budget
                .reserve_existing(&snapshot.owner_id, spool_bytes)
                .await?;
            let stdin = snapshot.stdin;
            self.jobs.write().await.insert(
                snapshot.job_id.clone(),
                Arc::new(JobState {
                    snapshot: Mutex::new(snapshot),
                    cancellation: CancellationToken::new(),
                    spool_path,
                    metadata_path,
                    nmap_target: None,
                    nmap_xml_path,
                    stdin: Mutex::new(if stdin == CommandStdin::Pipe {
                        JobStdinState::Closed
                    } else {
                        JobStdinState::Null
                    }),
                }),
            );
        }
        Ok(())
    }
}

async fn copy_stream<R>(
    mut reader: R,
    spool: Arc<SequencedSpool>,
    stream: OutputStream,
    cancellation: CancellationToken,
) -> std::io::Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            return Ok(());
        }
        if let Err(error) = spool.append(stream, &buffer[..read]).await {
            cancellation.cancel();
            return Err(error);
        }
    }
}

async fn resolve_executable(executable: &str) -> Result<PathBuf, NativeExecutionError> {
    let executable = executable.to_owned();
    tokio::task::spawn_blocking(move || which::which(executable))
        .await
        .map_err(|error| NativeExecutionError::InvalidRequest(error.to_string()))?
        .map_err(|error| NativeExecutionError::InvalidRequest(error.to_string()))
}

async fn persist_snapshot(path: &Path, snapshot: &JobSnapshot) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(snapshot).map_err(std::io::Error::other)?;
    let temporary = path.with_extension("json.tmp");
    tokio::fs::write(&temporary, bytes).await?;
    tokio::fs::rename(temporary, path).await
}

async fn terminate_child(child: &mut Child) -> std::io::Result<()> {
    #[cfg(unix)]
    if let Some(pid) = child.id() {
        let group = nix::unistd::Pid::from_raw(pid as i32);
        let _ = nix::sys::signal::killpg(group, nix::sys::signal::Signal::SIGTERM);
        if tokio::time::timeout(Duration::from_millis(750), child.wait())
            .await
            .is_ok()
        {
            return Ok(());
        }
        let _ = nix::sys::signal::killpg(group, nix::sys::signal::Signal::SIGKILL);
    }
    child.kill().await.or_else(|error| {
        if error.kind() == std::io::ErrorKind::InvalidInput {
            Ok(())
        } else {
            Err(error)
        }
    })?;
    let _ = child.wait().await;
    Ok(())
}

fn command_hash(
    executable: &Path,
    args: &[String],
    cwd: Option<&Path>,
    env: &BTreeMap<String, String>,
) -> String {
    let mut hash = blake3::Hasher::new();
    hash.update(executable.as_os_str().as_encoded_bytes());
    for arg in args {
        hash.update(&(arg.len() as u64).to_be_bytes());
        hash.update(arg.as_bytes());
    }
    if let Some(cwd) = cwd {
        hash.update(cwd.as_os_str().as_encoded_bytes());
    }
    for (key, value) in env {
        hash.update(key.as_bytes());
        hash.update(value.as_bytes());
    }
    hash.finalize().to_hex().to_string()
}

fn new_job_id() -> String {
    format!("job_{}", uuid::Uuid::new_v4().simple())
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn command_output_is_sequenced_cursor_readable_and_durable() {
        let directory = tempfile::tempdir().unwrap();
        let supervisor = NativeExecutionSupervisor::open(directory.path(), 2, 16, 1024 * 1024)
            .await
            .unwrap();
        let started = supervisor
            .start_command(CommandRequest {
                executable: "/bin/sh".to_owned(),
                args: vec![
                    "-c".to_owned(),
                    "printf stdout-data; printf stderr-data >&2".to_owned(),
                ],
                cwd: None,
                env: BTreeMap::new(),
                timeout_ms: 5_000,
                stdin: CommandStdin::Null,
            })
            .await
            .unwrap();
        let completed = supervisor
            .wait(&started.job_id, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(completed.lifecycle, JobLifecycle::Completed);
        let page = supervisor
            .output_page(&started.job_id, 0, 10, 1024)
            .await
            .unwrap();
        assert_eq!(page.records.len(), 2);
        assert!(page.records[0].sequence < page.records[1].sequence);
        assert!(page.records.iter().any(|record| {
            record.stream == OutputStream::Stdout && record.bytes == b"stdout-data"
        }));
        drop(supervisor);

        let reopened = NativeExecutionSupervisor::open(directory.path(), 2, 16, 1024 * 1024)
            .await
            .unwrap();
        assert_eq!(
            reopened.snapshot(&started.job_id).await.unwrap().lifecycle,
            JobLifecycle::Completed
        );
        assert_eq!(
            reopened
                .output_page(&started.job_id, 0, 10, 1024)
                .await
                .unwrap()
                .records
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn cancellation_reaches_the_process_tree() {
        let directory = tempfile::tempdir().unwrap();
        let supervisor = NativeExecutionSupervisor::open(directory.path(), 1, 16, 1024 * 1024)
            .await
            .unwrap();
        let started = supervisor
            .start_command(CommandRequest {
                executable: "/bin/sh".to_owned(),
                args: vec!["-c".to_owned(), "sleep 30 & wait".to_owned()],
                cwd: None,
                env: BTreeMap::new(),
                timeout_ms: 60_000,
                stdin: CommandStdin::Null,
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let cancelled = supervisor.cancel(&started.job_id).await.unwrap();
        assert_eq!(cancelled.lifecycle, JobLifecycle::Cancelled);
    }

    #[tokio::test]
    async fn aggregate_owner_budget_is_enforced_and_terminal_cleanup_reclaims_it() {
        let directory = tempfile::tempdir().unwrap();
        let supervisor = NativeExecutionSupervisor::open_with_limits(
            directory.path(),
            NativeExecutionLimits {
                maximum_parallel: 1,
                maximum_jobs: 8,
                maximum_spool_bytes_per_job: 1024,
                maximum_spool_bytes_per_owner: 1024,
                maximum_total_spool_bytes: 2048,
            },
        )
        .await
        .unwrap();
        let request = || CommandRequest {
            executable: "/bin/sh".to_owned(),
            args: vec!["-c".to_owned(), "head -c 700 /dev/zero".to_owned()],
            cwd: None,
            env: BTreeMap::new(),
            timeout_ms: 5_000,
            stdin: CommandStdin::Null,
        };

        let first = supervisor
            .start_command_for("engagement-a", request())
            .await
            .unwrap();
        let first = supervisor
            .wait(&first.job_id, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(first.lifecycle, JobLifecycle::Completed);
        assert_eq!(first.owner_id, "engagement-a");
        assert_eq!(supervisor.spool_budget.used_bytes().await, 713);

        let blocked = supervisor
            .start_command_for("engagement-a", request())
            .await
            .unwrap();
        let blocked = supervisor
            .wait(&blocked.job_id, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(blocked.lifecycle, JobLifecycle::Failed);
        assert!(
            blocked
                .error
                .as_deref()
                .is_some_and(|error| error.contains("engagement native output spool limit"))
        );

        supervisor.cleanup(&blocked.job_id).await.unwrap();
        supervisor.cleanup(&first.job_id).await.unwrap();
        assert_eq!(supervisor.spool_budget.used_bytes().await, 0);
        assert!(matches!(
            supervisor.snapshot(&first.job_id).await,
            Err(NativeExecutionError::NotFound(_))
        ));

        let after_cleanup = supervisor
            .start_command_for("engagement-a", request())
            .await
            .unwrap();
        let after_cleanup = supervisor
            .wait(&after_cleanup.job_id, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(after_cleanup.lifecycle, JobLifecycle::Completed);
    }

    #[tokio::test]
    async fn piped_stdin_is_bounded_streamed_and_explicitly_closed() {
        let directory = tempfile::tempdir().unwrap();
        let supervisor = NativeExecutionSupervisor::open(directory.path(), 1, 16, 1024 * 1024)
            .await
            .unwrap();
        let started = supervisor
            .start_command(CommandRequest {
                executable: "/bin/sh".to_owned(),
                args: vec![
                    "-c".to_owned(),
                    "IFS= read -r line; printf 'received:%s' \"$line\"".to_owned(),
                ],
                cwd: None,
                env: BTreeMap::new(),
                timeout_ms: 5_000,
                stdin: CommandStdin::Pipe,
            })
            .await
            .unwrap();
        let after_write = supervisor
            .write_stdin(&started.job_id, b"daemon-input\n", true)
            .await
            .unwrap();
        assert!(after_write.stdin_closed);
        let completed = supervisor
            .wait(&started.job_id, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(completed.lifecycle, JobLifecycle::Completed);
        let page = supervisor
            .output_page(&started.job_id, 0, 10, 1024)
            .await
            .unwrap();
        assert_eq!(page.records.len(), 1);
        assert_eq!(page.records[0].bytes, b"received:daemon-input");
        assert!(matches!(
            supervisor
                .write_stdin(&started.job_id, b"late", false)
                .await,
            Err(NativeExecutionError::InvalidRequest(_))
        ));
    }
}
