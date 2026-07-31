use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Semaphore;
use xai_grok_protocol::{
    Command, CommandEnvelope, PROTOCOL_VERSION, ProtocolError, ProtocolErrorCode, ResponseEnvelope,
};

use crate::control_plane::ControlPlaneHandle;

pub(crate) const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub socket_path: PathBuf,
    pub maximum_connections: usize,
    pub maximum_frame_bytes: usize,
}

impl ServerConfig {
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
            maximum_connections: 128,
            maximum_frame_bytes: MAX_FRAME_BYTES,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("control-plane socket I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("control-plane frame encode: {0}")]
    Encode(#[from] rmp_serde::encode::Error),
    #[error("control-plane frame decode: {0}")]
    Decode(#[from] rmp_serde::decode::Error),
    #[error("control-plane frame is {actual} bytes; maximum is {maximum}")]
    FrameTooLarge { actual: usize, maximum: usize },
    #[error("refusing to replace non-socket path {0}")]
    UnsafeSocketPath(PathBuf),
}

pub struct ControlPlaneServer {
    config: ServerConfig,
    listener: UnixListener,
    handle: ControlPlaneHandle,
}

impl ControlPlaneServer {
    pub async fn bind(
        config: ServerConfig,
        handle: ControlPlaneHandle,
    ) -> Result<Self, ServerError> {
        prepare_socket_path(&config.socket_path)?;
        let listener = UnixListener::bind(&config.socket_path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&config.socket_path, std::fs::Permissions::from_mode(0o600))?;
        }
        Ok(Self {
            config,
            listener,
            handle,
        })
    }

    pub async fn run(self) -> Result<(), ServerError> {
        let permits = Arc::new(Semaphore::new(self.config.maximum_connections.max(1)));
        let shutdown = self.handle.shutdown_token();
        loop {
            let accepted = tokio::select! {
                _ = shutdown.cancelled() => break,
                accepted = self.listener.accept() => accepted,
            };
            let (stream, _) = accepted?;
            let Ok(permit) = permits.clone().try_acquire_owned() else {
                drop(stream);
                continue;
            };
            let handle = self.handle.clone();
            let maximum_frame_bytes = self.config.maximum_frame_bytes.clamp(1, MAX_FRAME_BYTES);
            tokio::spawn(async move {
                let _permit = permit;
                if let Err(error) = serve_connection(stream, handle, maximum_frame_bytes).await {
                    tracing::debug!(%error, "control-plane client disconnected");
                }
            });
        }
        remove_socket_if_owned(&self.config.socket_path)?;
        Ok(())
    }
}

async fn serve_connection(
    mut stream: UnixStream,
    handle: ControlPlaneHandle,
    maximum_frame_bytes: usize,
) -> Result<(), ServerError> {
    let mut negotiated = false;
    loop {
        let envelope: CommandEnvelope = match read_frame(&mut stream, maximum_frame_bytes).await {
            Ok(envelope) => envelope,
            Err(ServerError::Io(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::UnexpectedEof
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::BrokenPipe
                ) =>
            {
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        if !negotiated && !matches!(&envelope.command, Command::Hello(_)) {
            let response = ResponseEnvelope {
                protocol_version: PROTOCOL_VERSION,
                command_id: envelope.command_id,
                response: Err(ProtocolError::new(
                    ProtocolErrorCode::InvalidEnvelope,
                    "the first command on a connection must be hello",
                )),
            };
            write_frame(&mut stream, &response, maximum_frame_bytes).await?;
            return Ok(());
        }
        let is_hello = matches!(&envelope.command, Command::Hello(_));
        let command_id = envelope.command_id.clone();
        let response = match handle.submit(envelope).await {
            Ok(response) => response,
            Err(error) => ResponseEnvelope {
                protocol_version: PROTOCOL_VERSION,
                command_id,
                response: Err(error),
            },
        };
        let hello_ok = is_hello && response.response.is_ok();
        write_frame(&mut stream, &response, maximum_frame_bytes).await?;
        if is_hello {
            if !hello_ok {
                return Ok(());
            }
            negotiated = true;
        }
    }
}

pub(crate) async fn read_frame<T: serde::de::DeserializeOwned>(
    stream: &mut UnixStream,
    maximum_frame_bytes: usize,
) -> Result<T, ServerError> {
    let length = stream.read_u32().await? as usize;
    if length > maximum_frame_bytes {
        return Err(ServerError::FrameTooLarge {
            actual: length,
            maximum: maximum_frame_bytes,
        });
    }
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes).await?;
    Ok(rmp_serde::from_slice(&bytes)?)
}

pub(crate) async fn write_frame<T: serde::Serialize>(
    stream: &mut UnixStream,
    value: &T,
    maximum_frame_bytes: usize,
) -> Result<(), ServerError> {
    let bytes = rmp_serde::to_vec_named(value)?;
    if bytes.len() > maximum_frame_bytes {
        return Err(ServerError::FrameTooLarge {
            actual: bytes.len(),
            maximum: maximum_frame_bytes,
        });
    }
    stream.write_u32(bytes.len() as u32).await?;
    stream.write_all(&bytes).await?;
    stream.flush().await?;
    Ok(())
}

fn prepare_socket_path(path: &Path) -> Result<(), ServerError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileTypeExt as _;
                if !metadata.file_type().is_socket() {
                    return Err(ServerError::UnsafeSocketPath(path.to_path_buf()));
                }
            }
            std::fs::remove_file(path)?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(ServerError::Io(error)),
    }
    Ok(())
}

fn remove_socket_if_owned(path: &Path) -> Result<(), ServerError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileTypeExt as _;
                if metadata.file_type().is_socket() {
                    std::fs::remove_file(path)?;
                }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(ServerError::Io(error)),
    }
    Ok(())
}
