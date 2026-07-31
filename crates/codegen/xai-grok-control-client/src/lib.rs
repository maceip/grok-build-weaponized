//! Thin reconnecting client for the local `grokd` protocol.
//!
//! This crate deliberately excludes daemon storage, execution providers,
//! model runtimes, GUI code, HTTP clients, and MCP dependencies.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::UnixStream;
use tokio::sync::Mutex;
use xai_grok_protocol::{
    Command, CommandEnvelope, CommandId, EventBatch, EventReadRequest, Hello,
    MAX_CONTROL_FRAME_BYTES, PROTOCOL_VERSION, ProtocolError, ProtocolErrorCode, Response,
    ResponseEnvelope, TeamClient,
};

#[derive(Clone, Debug)]
pub struct ClientIdentity {
    pub client_name: String,
    pub client_version: String,
    pub team: Option<TeamClient>,
}

impl ClientIdentity {
    pub fn new(client_name: impl Into<String>, client_version: impl Into<String>) -> Self {
        Self {
            client_name: client_name.into(),
            client_version: client_version.into(),
            team: None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("control-plane socket I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("control-plane frame encode: {0}")]
    Encode(#[from] rmp_serde::encode::Error),
    #[error("control-plane frame decode: {0}")]
    Decode(#[from] rmp_serde::decode::Error),
    #[error("control-plane frame is {actual} bytes; maximum is {maximum}")]
    FrameTooLarge { actual: usize, maximum: usize },
    #[error("control-plane protocol: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("control-plane returned an incompatible handshake")]
    InvalidHandshake,
    #[error("control-plane request timed out after {0} milliseconds; reconnect before retrying")]
    Timeout(u128),
    #[error("control-plane connection is no longer synchronized; reconnect before retrying")]
    ConnectionPoisoned,
    #[error("control-plane response id does not match the submitted command")]
    MismatchedResponse,
    #[error("control-plane returned {0} instead of an event batch")]
    ExpectedEvents(&'static str),
}

pub struct ControlPlaneClient {
    stream: Mutex<UnixStream>,
    maximum_frame_bytes: usize,
    team: Option<TeamClient>,
    poisoned: AtomicBool,
}

impl ControlPlaneClient {
    pub async fn connect(
        socket_path: impl AsRef<Path>,
        identity: ClientIdentity,
    ) -> Result<Self, ClientError> {
        if let Some(team) = &identity.team {
            team.validate()?;
        }
        let stream = UnixStream::connect(socket_path).await?;
        let team = identity.team.clone();
        let client = Self {
            stream: Mutex::new(stream),
            maximum_frame_bytes: MAX_CONTROL_FRAME_BYTES,
            team,
            poisoned: AtomicBool::new(false),
        };
        let response = client
            .send(
                Command::Hello(Hello {
                    protocol_version: PROTOCOL_VERSION,
                    client_name: identity.client_name,
                    client_version: identity.client_version,
                    team: identity.team,
                }),
                Duration::from_secs(5),
            )
            .await?;
        match response {
            Response::Hello(ack)
                if ack.protocol_version == PROTOCOL_VERSION && ack.server_name == "grokd" =>
            {
                Ok(client)
            }
            _ => Err(ClientError::InvalidHandshake),
        }
    }

    pub async fn send(
        &self,
        mut command: Command,
        timeout: Duration,
    ) -> Result<Response, ClientError> {
        if let Command::SubmitIngress(ingress) = &mut command
            && ingress.team.is_none()
        {
            ingress.team.clone_from(&self.team);
        }
        let envelope = CommandEnvelope {
            protocol_version: PROTOCOL_VERSION,
            command_id: CommandId::new(),
            causation_id: None,
            deadline_unix_ms: now_unix_ms()
                .saturating_add(u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX)),
            command,
        };
        let response = match tokio::time::timeout(timeout, self.send_envelope(envelope)).await {
            Ok(response) => response?,
            Err(_) => {
                self.poisoned.store(true, Ordering::Release);
                return Err(ClientError::Timeout(timeout.as_millis()));
            }
        };
        response.response.map_err(ClientError::Protocol)
    }

    pub async fn read_events(&self, request: EventReadRequest) -> Result<EventBatch, ClientError> {
        request.validate()?;
        let timeout = Duration::from_millis(u64::from(request.wait_ms) + 5_000);
        match self.send(Command::ReadEvents(request), timeout).await? {
            Response::Events(batch) => Ok(batch),
            Response::Hello(_) => Err(ClientError::ExpectedEvents("hello")),
            Response::Accepted { .. } => Err(ClientError::ExpectedEvents("accepted")),
            Response::PlanAccepted { .. } => Err(ClientError::ExpectedEvents("plan_accepted")),
            Response::DispatchAccepted { .. } => {
                Err(ClientError::ExpectedEvents("dispatch_accepted"))
            }
            Response::ProviderRegistered { .. } => {
                Err(ClientError::ExpectedEvents("provider_registered"))
            }
            Response::ArtifactStored { .. } => Err(ClientError::ExpectedEvents("artifact_stored")),
            Response::Projection(_) => Err(ClientError::ExpectedEvents("projection")),
            Response::Ack => Err(ClientError::ExpectedEvents("ack")),
        }
    }

    pub async fn send_envelope(
        &self,
        envelope: CommandEnvelope,
    ) -> Result<ResponseEnvelope, ClientError> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(ClientError::ConnectionPoisoned);
        }
        if envelope.protocol_version != PROTOCOL_VERSION {
            return Err(ClientError::Protocol(ProtocolError::new(
                ProtocolErrorCode::IncompatibleVersion,
                "client envelope protocol version is incompatible",
            )));
        }
        let mut stream = self.stream.lock().await;
        let command_id = envelope.command_id.clone();
        let response = async {
            write_frame(&mut stream, &envelope, self.maximum_frame_bytes).await?;
            read_frame(&mut stream, self.maximum_frame_bytes).await
        }
        .await;
        let response: ResponseEnvelope = match response {
            Ok(response) => response,
            Err(error) => {
                self.poisoned.store(true, Ordering::Release);
                return Err(error);
            }
        };
        if response.command_id != command_id {
            self.poisoned.store(true, Ordering::Release);
            return Err(ClientError::MismatchedResponse);
        }
        Ok(response)
    }
}

async fn read_frame<T: serde::de::DeserializeOwned>(
    stream: &mut UnixStream,
    maximum_frame_bytes: usize,
) -> Result<T, ClientError> {
    let length = stream.read_u32().await? as usize;
    if length > maximum_frame_bytes {
        return Err(ClientError::FrameTooLarge {
            actual: length,
            maximum: maximum_frame_bytes,
        });
    }
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes).await?;
    Ok(rmp_serde::from_slice(&bytes)?)
}

async fn write_frame<T: serde::Serialize>(
    stream: &mut UnixStream,
    value: &T,
    maximum_frame_bytes: usize,
) -> Result<(), ClientError> {
    let bytes = rmp_serde::to_vec_named(value)?;
    if bytes.len() > maximum_frame_bytes {
        return Err(ClientError::FrameTooLarge {
            actual: bytes.len(),
            maximum: maximum_frame_bytes,
        });
    }
    stream.write_u32(bytes.len() as u32).await?;
    stream.write_all(&bytes).await?;
    stream.flush().await?;
    Ok(())
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
    async fn mismatched_response_id_is_rejected() {
        let (client_stream, mut server_stream) = UnixStream::pair().unwrap();
        let client = ControlPlaneClient {
            stream: Mutex::new(client_stream),
            maximum_frame_bytes: MAX_CONTROL_FRAME_BYTES,
            team: None,
            poisoned: AtomicBool::new(false),
        };
        let server = tokio::spawn(async move {
            let _: CommandEnvelope = read_frame(&mut server_stream, MAX_CONTROL_FRAME_BYTES)
                .await
                .unwrap();
            write_frame(
                &mut server_stream,
                &ResponseEnvelope {
                    protocol_version: PROTOCOL_VERSION,
                    command_id: CommandId::new(),
                    response: Ok(Response::Ack),
                },
                MAX_CONTROL_FRAME_BYTES,
            )
            .await
            .unwrap();
        });
        assert!(matches!(
            client.send(Command::Shutdown, Duration::from_secs(1)).await,
            Err(ClientError::MismatchedResponse)
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn timeout_poisons_the_stream_until_reconnect() {
        let (client_stream, _server_stream) = UnixStream::pair().unwrap();
        let client = ControlPlaneClient {
            stream: Mutex::new(client_stream),
            maximum_frame_bytes: MAX_CONTROL_FRAME_BYTES,
            team: None,
            poisoned: AtomicBool::new(false),
        };
        assert!(matches!(
            client
                .send(Command::Shutdown, Duration::from_millis(1))
                .await,
            Err(ClientError::Timeout(_))
        ));
        assert!(matches!(
            client.send(Command::Shutdown, Duration::from_secs(1)).await,
            Err(ClientError::ConnectionPoisoned)
        ));
    }
}
