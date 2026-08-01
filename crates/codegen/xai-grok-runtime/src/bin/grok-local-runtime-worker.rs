use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use xai_grok_runtime::protocol::{
    CAP_ADAPTER_LOAD, CAP_ADAPTER_UNLOAD, CAP_CANCELLATION, CAP_EXACT_TOKENIZE, CAP_MEMORY_STATS,
    CAP_SESSION_CACHE, CAP_STREAMING, WORKER_PROTOCOL_VERSION, WorkerCommand, WorkerMessage,
    WorkerStats, read_frame, write_frame,
};
use xai_grok_runtime::{
    AdapterBinding, AdapterDescriptor, AdapterError, AdapterManager, LiteRtLmConfig,
    LocalInferenceResult, ResourceGovernor,
};

struct WorkerState {
    config: Mutex<Option<LiteRtLmConfig>>,
    model_id: Mutex<Option<String>>,
    active: Mutex<HashMap<String, CancellationToken>>,
    selected_adapters: Mutex<HashMap<String, AdapterBinding>>,
    adapters: AdapterManager,
    completed: AtomicU64,
    active_count: AtomicU32,
}

impl WorkerState {
    fn new() -> Self {
        let governor = ResourceGovernor::detected();
        Self {
            config: Mutex::new(None),
            model_id: Mutex::new(None),
            active: Mutex::new(HashMap::new()),
            selected_adapters: Mutex::new(HashMap::new()),
            adapters: AdapterManager::new(governor),
            completed: AtomicU64::new(0),
            active_count: AtomicU32::new(0),
        }
    }

    fn loaded(&self) -> Result<(LiteRtLmConfig, String), String> {
        let config = self
            .config
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
            .ok_or_else(|| "no model is loaded".to_string())?;
        let model_id = self
            .model_id
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
            .ok_or_else(|| "no model is loaded".to_string())?;
        Ok((config, model_id))
    }

    fn stats(&self) -> WorkerStats {
        let (resident_sessions, resident_context_tokens) =
            xai_grok_runtime::litert_lm::resident_session_stats();
        WorkerStats {
            process_id: std::process::id(),
            resident_bytes: process_resident_bytes(),
            thread_count: process_thread_count(),
            file_descriptor_count: process_file_descriptor_count(),
            resident_adapter_bytes: self.adapters.resident_bytes(),
            resident_adapters: self.adapters.resident_count(),
            resident_sessions,
            resident_context_tokens,
            resident_session_owners: xai_grok_runtime::litert_lm::resident_session_owners(),
            active_requests: self.active_count.load(Ordering::Acquire),
            completed_requests: self.completed.load(Ordering::Acquire),
            poisoned: false,
        }
    }
}

#[cfg(target_os = "macos")]
fn process_resident_bytes() -> u64 {
    macos_task_info().map_or(0, |info| info.pti_resident_size)
}

#[cfg(all(unix, not(target_os = "macos")))]
fn process_resident_bytes() -> u64 {
    let resident_pages = std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|statm| statm.split_whitespace().nth(1)?.parse::<u64>().ok())
        .unwrap_or(0);
    // SAFETY: sysconf is thread-safe and has no pointer arguments.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    resident_pages.saturating_mul(u64::try_from(page_size).unwrap_or(0))
}

#[cfg(target_os = "macos")]
fn macos_task_info() -> Option<libc::proc_taskinfo> {
    use std::ffi::c_void;
    use std::mem::{MaybeUninit, size_of};

    let mut info = MaybeUninit::<libc::proc_taskinfo>::zeroed();
    let expected = i32::try_from(size_of::<libc::proc_taskinfo>()).ok()?;
    // SAFETY: `info` is writable for exactly `expected` bytes and a full-size
    // return is required before the initialized record is read.
    let written = unsafe {
        libc::proc_pidinfo(
            std::process::id() as libc::pid_t,
            libc::PROC_PIDTASKINFO,
            0,
            info.as_mut_ptr().cast::<c_void>(),
            expected,
        )
    };
    (written == expected).then(|| {
        // SAFETY: proc_pidinfo initialized the complete record.
        unsafe { info.assume_init() }
    })
}

#[cfg(target_os = "macos")]
fn process_thread_count() -> u32 {
    macos_task_info()
        .and_then(|info| u32::try_from(info.pti_threadnum).ok())
        .unwrap_or(0)
}

#[cfg(target_os = "linux")]
fn process_thread_count() -> u32 {
    std::fs::read_dir("/proc/self/task")
        .ok()
        .and_then(|entries| u32::try_from(entries.count()).ok())
        .unwrap_or(0)
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn process_thread_count() -> u32 {
    0
}

#[cfg(target_os = "macos")]
fn process_file_descriptor_count() -> u32 {
    use std::ffi::c_void;

    // SAFETY: null and size zero form the documented PROC_PIDLISTFDS size
    // probe. The return value is a byte count for proc_fdinfo records.
    let bytes = unsafe {
        libc::proc_pidinfo(
            std::process::id() as libc::pid_t,
            libc::PROC_PIDLISTFDS,
            0,
            std::ptr::null_mut::<c_void>(),
            0,
        )
    };
    usize::try_from(bytes)
        .ok()
        .and_then(|bytes| u32::try_from(bytes / std::mem::size_of::<libc::proc_fdinfo>()).ok())
        .unwrap_or(0)
}

#[cfg(target_os = "linux")]
fn process_file_descriptor_count() -> u32 {
    std::fs::read_dir("/proc/self/fd")
        .ok()
        .and_then(|entries| u32::try_from(entries.count()).ok())
        .unwrap_or(0)
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn process_file_descriptor_count() -> u32 {
    0
}

#[cfg(not(unix))]
fn process_resident_bytes() -> u64 {
    0
}

async fn evict_worker_adapter(state: &WorkerState, binding: &AdapterBinding) -> Result<(), String> {
    let descriptor = state
        .adapters
        .begin_eviction(binding)
        .map_err(|error| error.to_string())?;
    let (config, _) = state.loaded()?;
    let adapter_identity = descriptor.immutable_id();
    let native_adapter = xai_grok_runtime::LoraAdapterConfig {
        path: descriptor.path,
        id: adapter_identity,
    };
    if let Err(error) = xai_grok_runtime::litert_lm::unload_adapter(config, native_adapter).await {
        state.adapters.rollback_eviction(binding);
        return Err(error.to_string());
    }
    state
        .adapters
        .commit_eviction(binding)
        .map_err(|error| error.to_string())?;
    state
        .selected_adapters
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .retain(|_, selected| selected != binding);
    Ok(())
}

async fn admit_worker_adapter(
    state: &WorkerState,
    descriptor: &AdapterDescriptor,
) -> Result<u32, String> {
    loop {
        match state.adapters.register_host_resident(descriptor.clone()) {
            Ok(native_id) => return Ok(native_id),
            Err(AdapterError::Resource(error)) => {
                let Some(candidate) = state.adapters.next_eviction_candidate() else {
                    return Err(error.to_string());
                };
                evict_worker_adapter(state, &candidate).await?;
            }
            Err(error) => return Err(error.to_string()),
        }
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("grok-local-runtime-worker: {error}");
        std::process::exit(1);
    }
}

#[cfg(unix)]
async fn run() -> io::Result<()> {
    use std::os::fd::FromRawFd;

    let fd = std::env::var("GROK_RUNTIME_FD")
        .ok()
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(3);
    // SAFETY: the supervisor passes exclusive ownership of this connected
    // Unix socket descriptor to the worker.
    let socket = unsafe { std::os::unix::net::UnixStream::from_raw_fd(fd) };
    socket.set_nonblocking(true)?;
    let socket = UnixStream::from_std(socket)?;
    worker_loop(socket).await
}

#[cfg(not(unix))]
async fn run() -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "local runtime worker currently requires Unix sockets",
    ))
}

async fn worker_loop(socket: UnixStream) -> io::Result<()> {
    let (mut reader, mut writer) = socket.into_split();
    let (out_tx, mut out_rx) = mpsc::channel::<WorkerMessage>(256);
    let writer_task = tokio::spawn(async move {
        while let Some(message) = out_rx.recv().await {
            write_frame(&mut writer, &message).await?;
        }
        Ok::<(), io::Error>(())
    });
    let state = Arc::new(WorkerState::new());

    loop {
        let command = match read_frame::<_, WorkerCommand>(&mut reader).await {
            Ok(command) => command,
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error),
        };
        match command {
            WorkerCommand::Hello { protocol_version } => {
                if protocol_version != WORKER_PROTOCOL_VERSION {
                    send_error(
                        &out_tx,
                        None,
                        "protocol_version",
                        format!(
                            "worker protocol {WORKER_PROTOCOL_VERSION} does not support {protocol_version}"
                        ),
                        false,
                    )
                    .await?;
                    continue;
                }
                out_tx
                    .send(WorkerMessage::Hello {
                        protocol_version: WORKER_PROTOCOL_VERSION,
                        worker_pid: std::process::id(),
                        capabilities: CAP_STREAMING
                            | CAP_EXACT_TOKENIZE
                            | CAP_SESSION_CACHE
                            | CAP_ADAPTER_LOAD
                            | CAP_ADAPTER_UNLOAD
                            | CAP_MEMORY_STATS
                            | CAP_CANCELLATION,
                    })
                    .await
                    .map_err(channel_closed)?;
            }
            WorkerCommand::LoadModel { config, model_id } => {
                match xai_grok_runtime::litert_lm::prewarm(config.clone()).await {
                    Ok(()) => {
                        *state
                            .config
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(config);
                        *state
                            .model_id
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                            Some(model_id.clone());
                        out_tx
                            .send(WorkerMessage::Ready { model_id })
                            .await
                            .map_err(channel_closed)?;
                    }
                    Err(error) => {
                        send_error(&out_tx, None, "model_load", error.to_string(), false).await?;
                    }
                }
            }
            WorkerCommand::Measure {
                request_id,
                prepared,
            } => match state.loaded() {
                Ok((config, _)) => {
                    match xai_grok_runtime::litert_lm::measure_prepared(config, &prepared).await {
                        Ok(prompt_tokens) => {
                            out_tx
                                .send(WorkerMessage::Measurement {
                                    request_id,
                                    prompt_tokens,
                                })
                                .await
                                .map_err(channel_closed)?;
                        }
                        Err(error) => {
                            send_error(
                                &out_tx,
                                Some(request_id),
                                "measure",
                                error.to_string(),
                                false,
                            )
                            .await?;
                        }
                    }
                }
                Err(error) => {
                    send_error(&out_tx, Some(request_id), "model_not_loaded", error, false).await?;
                }
            },
            WorkerCommand::Generate {
                request_id,
                mut prepared,
            } => {
                let (mut config, model_id) = match state.loaded() {
                    Ok(loaded) => loaded,
                    Err(error) => {
                        send_error(&out_tx, Some(request_id), "model_not_loaded", error, false)
                            .await?;
                        continue;
                    }
                };
                let binding = state
                    .selected_adapters
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .remove(&request_id);
                let adapter_lease = if let Some(binding) = binding.as_ref() {
                    let Some(descriptor) = state.adapters.descriptor(binding) else {
                        send_error(
                            &out_tx,
                            Some(request_id.clone()),
                            "adapter_not_resident",
                            binding.immutable_id(),
                            false,
                        )
                        .await?;
                        continue;
                    };
                    let lease = match state.adapters.acquire(binding) {
                        Ok(lease) => lease,
                        Err(error) => {
                            send_error(
                                &out_tx,
                                Some(request_id.clone()),
                                "adapter_acquire",
                                error.to_string(),
                                false,
                            )
                            .await?;
                            continue;
                        }
                    };
                    let immutable_id = descriptor.immutable_id();
                    config.lora_adapter = Some(xai_grok_runtime::litert_lm::LoraAdapterConfig {
                        path: descriptor.path,
                        id: immutable_id,
                    });
                    prepared.lora_adapter = config.lora_adapter.clone();
                    Some(lease)
                } else {
                    None
                };
                let cancel = CancellationToken::new();
                state
                    .active
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .insert(request_id.clone(), cancel.clone());
                state.active_count.fetch_add(1, Ordering::AcqRel);
                let state_for_task = Arc::clone(&state);
                let out_for_task = out_tx.clone();
                tokio::spawn(async move {
                    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
                    let out_for_events = out_for_task.clone();
                    let event_forwarder = tokio::spawn(async move {
                        while let Some(event) = event_rx.recv().await {
                            if out_for_events
                                .send(WorkerMessage::Event { event })
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                    });
                    let result = xai_grok_runtime::litert_lm::run_prepared_request(
                        request_id.clone(),
                        prepared,
                        config,
                        model_id,
                        &event_tx,
                        &cancel,
                    )
                    .await;
                    drop(event_tx);
                    let _ = event_forwarder.await;
                    drop(adapter_lease);
                    state_for_task
                        .active
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .remove(&request_id);
                    state_for_task.active_count.fetch_sub(1, Ordering::AcqRel);
                    match result {
                        Ok(LocalInferenceResult::Completed { response, metrics }) => {
                            state_for_task.completed.fetch_add(1, Ordering::Relaxed);
                            let _ = out_for_task
                                .send(WorkerMessage::Completed {
                                    request_id,
                                    response: *response,
                                    metrics,
                                })
                                .await;
                        }
                        Ok(LocalInferenceResult::Cancelled) => {
                            let _ = out_for_task
                                .send(WorkerMessage::Cancelled { request_id })
                                .await;
                        }
                        Err(error) => {
                            let _ = out_for_task
                                .send(WorkerMessage::Error {
                                    request_id: Some(request_id),
                                    code: "generation".to_string(),
                                    message: error.to_string(),
                                    retryable: error.is_retryable(),
                                })
                                .await;
                        }
                    }
                });
            }
            WorkerCommand::Cancel { request_id } => {
                if let Some(token) = state
                    .active
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .get(&request_id)
                    .cloned()
                {
                    token.cancel();
                }
                out_tx
                    .send(WorkerMessage::Ack)
                    .await
                    .map_err(channel_closed)?;
            }
            WorkerCommand::DropSession { session_id } => {
                xai_grok_runtime::litert_lm::drop_session(&session_id);
                out_tx
                    .send(WorkerMessage::Ack)
                    .await
                    .map_err(channel_closed)?;
            }
            WorkerCommand::DropInactiveSessions { max_remaining } => {
                xai_grok_runtime::litert_lm::drop_inactive_sessions(max_remaining);
                out_tx
                    .send(WorkerMessage::Ack)
                    .await
                    .map_err(channel_closed)?;
            }
            WorkerCommand::LoadAdapter { descriptor } => {
                let adapter_id = descriptor.adapter_id.clone();
                let revision = descriptor.revision.clone();
                let native_adapter = xai_grok_runtime::LoraAdapterConfig {
                    path: descriptor.path.clone(),
                    id: descriptor.immutable_id(),
                };
                let native_id = match admit_worker_adapter(&state, &descriptor).await {
                    Ok(native_id) => match state.loaded() {
                        Ok((config, _)) => {
                            if let Err(error) =
                                xai_grok_runtime::litert_lm::prewarm_adapter(config, native_adapter)
                                    .await
                            {
                                let _ = state.adapters.evict(&AdapterBinding {
                                    adapter_id: adapter_id.clone(),
                                    revision: revision.clone(),
                                });
                                send_error(
                                    &out_tx,
                                    None,
                                    "adapter_native_load",
                                    error.to_string(),
                                    false,
                                )
                                .await?;
                                continue;
                            }
                            let binding = AdapterBinding {
                                adapter_id: adapter_id.clone(),
                                revision: revision.clone(),
                            };
                            let _ = state.adapters.mark_device_resident(&binding);
                            native_id
                        }
                        Err(error) => {
                            let _ = state.adapters.evict(&AdapterBinding {
                                adapter_id: adapter_id.clone(),
                                revision: revision.clone(),
                            });
                            send_error(&out_tx, None, "model_not_loaded", error, false).await?;
                            continue;
                        }
                    },
                    Err(error) => {
                        send_error(&out_tx, None, "adapter_load", error.to_string(), false).await?;
                        continue;
                    }
                };
                out_tx
                    .send(WorkerMessage::AdapterReady {
                        adapter_id,
                        revision,
                        native_id,
                    })
                    .await
                    .map_err(channel_closed)?;
            }
            WorkerCommand::SelectAdapter {
                request_id,
                adapter_id,
                revision,
            } => {
                let binding = AdapterBinding {
                    adapter_id,
                    revision,
                };
                if matches!(
                    state.adapters.state(&binding),
                    xai_grok_runtime::AdapterState::Absent
                ) {
                    send_error(
                        &out_tx,
                        Some(request_id),
                        "adapter_not_resident",
                        binding.immutable_id(),
                        false,
                    )
                    .await?;
                } else {
                    state
                        .selected_adapters
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .insert(request_id, binding);
                    out_tx
                        .send(WorkerMessage::Ack)
                        .await
                        .map_err(channel_closed)?;
                }
            }
            WorkerCommand::UnloadAdapter {
                adapter_id,
                revision,
            } => {
                let binding = AdapterBinding {
                    adapter_id: adapter_id.clone(),
                    revision: revision.clone(),
                };
                match state.adapters.begin_eviction(&binding) {
                    Ok(descriptor) => {
                        let (config, _) = match state.loaded() {
                            Ok(loaded) => loaded,
                            Err(error) => {
                                send_error(&out_tx, None, "model_not_loaded", error, false).await?;
                                continue;
                            }
                        };
                        let adapter_identity = descriptor.immutable_id();
                        let native_adapter = xai_grok_runtime::LoraAdapterConfig {
                            path: descriptor.path,
                            id: adapter_identity,
                        };
                        if let Err(error) =
                            xai_grok_runtime::litert_lm::unload_adapter(config, native_adapter)
                                .await
                        {
                            state.adapters.rollback_eviction(&binding);
                            send_error(
                                &out_tx,
                                None,
                                "adapter_native_unload",
                                error.to_string(),
                                false,
                            )
                            .await?;
                            continue;
                        }
                        if let Err(error) = state.adapters.commit_eviction(&binding) {
                            send_error(&out_tx, None, "adapter_unload", error.to_string(), false)
                                .await?;
                            continue;
                        }
                        state
                            .selected_adapters
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .retain(|_, selected| selected != &binding);
                        out_tx
                            .send(WorkerMessage::AdapterUnloaded {
                                adapter_id,
                                revision,
                            })
                            .await
                            .map_err(channel_closed)?;
                    }
                    Err(error) => {
                        send_error(&out_tx, None, "adapter_unload", error.to_string(), false)
                            .await?;
                    }
                }
            }
            WorkerCommand::Stats => {
                out_tx
                    .send(WorkerMessage::Stats {
                        stats: state.stats(),
                    })
                    .await
                    .map_err(channel_closed)?;
            }
            WorkerCommand::Shutdown => {
                for token in state
                    .active
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .values()
                {
                    token.cancel();
                }
                out_tx
                    .send(WorkerMessage::Ack)
                    .await
                    .map_err(channel_closed)?;
                break;
            }
        }
    }
    drop(out_tx);
    writer_task
        .await
        .map_err(|error| io::Error::other(error.to_string()))?
}

async fn send_error(
    tx: &mpsc::Sender<WorkerMessage>,
    request_id: Option<String>,
    code: &str,
    message: impl Into<String>,
    retryable: bool,
) -> io::Result<()> {
    tx.send(WorkerMessage::Error {
        request_id,
        code: code.to_string(),
        message: message.into(),
        retryable,
    })
    .await
    .map_err(channel_closed)
}

fn channel_closed<T>(error: mpsc::error::SendError<T>) -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, error.to_string())
}
