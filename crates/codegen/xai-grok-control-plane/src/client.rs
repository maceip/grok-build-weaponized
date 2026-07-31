use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::net::UnixStream;
use tokio::sync::Mutex;
use xai_grok_protocol::{
    Command, CommandEnvelope, CommandId, Hello, PROTOCOL_VERSION, ProtocolError, ProtocolErrorCode,
    Response, ResponseEnvelope,
};

use crate::server::{MAX_FRAME_BYTES, ServerError, read_frame, write_frame};

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("control-plane transport: {0}")]
    Transport(#[from] ServerError),
    #[error("control-plane protocol: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("control-plane returned an unexpected handshake response")]
    InvalidHandshake,
}

pub struct ControlPlaneClient {
    stream: Mutex<UnixStream>,
    maximum_frame_bytes: usize,
}

impl ControlPlaneClient {
    pub async fn connect(
        socket_path: impl AsRef<Path>,
        client_name: impl Into<String>,
        client_version: impl Into<String>,
    ) -> Result<Self, ClientError> {
        let stream = UnixStream::connect(socket_path)
            .await
            .map_err(ServerError::Io)?;
        let client = Self {
            stream: Mutex::new(stream),
            maximum_frame_bytes: MAX_FRAME_BYTES,
        };
        let response = client
            .send(
                Command::Hello(Hello {
                    protocol_version: PROTOCOL_VERSION,
                    client_name: client_name.into(),
                    client_version: client_version.into(),
                }),
                Duration::from_secs(5),
            )
            .await?;
        match response {
            Response::Hello(ack)
                if ack.protocol_version == PROTOCOL_VERSION
                    && ack.server_name == crate::SERVER_NAME =>
            {
                Ok(client)
            }
            _ => Err(ClientError::InvalidHandshake),
        }
    }

    pub async fn send(&self, command: Command, timeout: Duration) -> Result<Response, ClientError> {
        let envelope = CommandEnvelope {
            protocol_version: PROTOCOL_VERSION,
            command_id: CommandId::new(),
            causation_id: None,
            deadline_unix_ms: now_unix_ms()
                .saturating_add(u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX)),
            command,
        };
        let response = self.send_envelope(envelope).await?;
        response.response.map_err(ClientError::Protocol)
    }

    pub async fn send_envelope(
        &self,
        envelope: CommandEnvelope,
    ) -> Result<ResponseEnvelope, ClientError> {
        if envelope.protocol_version != PROTOCOL_VERSION {
            return Err(ClientError::Protocol(ProtocolError::new(
                ProtocolErrorCode::IncompatibleVersion,
                "client envelope protocol version is incompatible",
            )));
        }
        let mut stream = self.stream.lock().await;
        write_frame(&mut stream, &envelope, self.maximum_frame_bytes).await?;
        let response = read_frame(&mut stream, self.maximum_frame_bytes).await?;
        Ok(response)
    }
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
    use crate::{ControlPlane, ControlPlaneConfig, ControlPlaneServer, ServerConfig};

    #[tokio::test]
    async fn client_negotiates_and_queries_server() {
        let directory = tempfile::tempdir().unwrap();
        let socket_path = directory.path().join("grokd.sock");
        let control_plane =
            ControlPlane::open(ControlPlaneConfig::new(directory.path().join("state")))
                .await
                .unwrap();
        let handle = control_plane.handle();
        let server = ControlPlaneServer::bind(ServerConfig::new(&socket_path), handle.clone())
            .await
            .unwrap();
        let server_task = tokio::spawn(server.run());
        let client = ControlPlaneClient::connect(&socket_path, "test", "1")
            .await
            .unwrap();
        let response = client
            .send(
                Command::QueryProjection(xai_grok_protocol::ProjectionQuery::Providers),
                Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert!(matches!(response, Response::Projection(_)));
        handle.shutdown_token().cancel();
        control_plane.wait().await;
        server_task.await.unwrap().unwrap();
    }
}
