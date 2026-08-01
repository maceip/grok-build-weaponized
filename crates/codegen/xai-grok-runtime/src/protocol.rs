use std::io;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use xai_grok_sampling_types::ConversationResponse;

use crate::adapter::AdapterDescriptor;
use crate::events::RuntimeEvent;
use crate::litert_lm::{LiteRtLmConfig, PreparedConversation};
use crate::metrics::InferenceLatencyStats;

pub const WORKER_PROTOCOL_VERSION: u32 = 1;
pub const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

pub const CAP_STREAMING: u64 = 1 << 0;
pub const CAP_EXACT_TOKENIZE: u64 = 1 << 1;
pub const CAP_SESSION_CACHE: u64 = 1 << 2;
pub const CAP_ADAPTER_LOAD: u64 = 1 << 3;
pub const CAP_ADAPTER_UNLOAD: u64 = 1 << 4;
pub const CAP_MEMORY_STATS: u64 = 1 << 5;
pub const CAP_CANCELLATION: u64 = 1 << 6;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum WorkerCommand {
    Hello {
        protocol_version: u32,
    },
    LoadModel {
        config: LiteRtLmConfig,
        model_id: String,
    },
    Measure {
        request_id: String,
        prepared: PreparedConversation,
    },
    Generate {
        request_id: String,
        prepared: PreparedConversation,
    },
    Cancel {
        request_id: String,
    },
    DropSession {
        session_id: String,
    },
    DropInactiveSessions {
        max_remaining: u32,
    },
    LoadAdapter {
        descriptor: AdapterDescriptor,
    },
    SelectAdapter {
        request_id: String,
        adapter_id: String,
        revision: String,
    },
    UnloadAdapter {
        adapter_id: String,
        revision: String,
    },
    Stats,
    Shutdown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "message_type", rename_all = "snake_case")]
pub enum WorkerMessage {
    Hello {
        protocol_version: u32,
        worker_pid: u32,
        capabilities: u64,
    },
    Ready {
        model_id: String,
    },
    Measurement {
        request_id: String,
        prompt_tokens: u32,
    },
    Event {
        event: RuntimeEvent,
    },
    Completed {
        request_id: String,
        response: ConversationResponse,
        metrics: InferenceLatencyStats,
    },
    Cancelled {
        request_id: String,
    },
    AdapterReady {
        adapter_id: String,
        revision: String,
        native_id: u32,
    },
    AdapterUnloaded {
        adapter_id: String,
        revision: String,
    },
    Stats {
        stats: WorkerStats,
    },
    Ack,
    Error {
        request_id: Option<String>,
        code: String,
        message: String,
        retryable: bool,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerStats {
    /// Current resident set size of the isolated worker process.
    pub resident_bytes: u64,
    pub resident_adapter_bytes: u64,
    pub resident_adapters: u32,
    pub resident_sessions: u32,
    pub resident_context_tokens: u64,
    #[serde(default)]
    pub resident_session_owners: Vec<ResidentSessionOwner>,
    pub active_requests: u32,
    pub completed_requests: u64,
    pub poisoned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResidentSessionOwner {
    pub session_id: String,
    pub adapter_id: Option<String>,
    pub resident_tokens: u32,
}

pub async fn write_frame<W, T>(writer: &mut W, value: &T) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let payload = rmp_serde::to_vec_named(value)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if payload.len() > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "runtime frame exceeds {} bytes: {}",
                MAX_FRAME_BYTES,
                payload.len()
            ),
        ));
    }
    let length = u32::try_from(payload.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "runtime frame is too large"))?;
    writer.write_all(&length.to_be_bytes()).await?;
    writer.write_all(&payload).await?;
    writer.flush().await
}

pub async fn read_frame<R, T>(reader: &mut R) -> io::Result<T>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let length = reader.read_u32().await? as usize;
    if length > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("runtime frame length {length} exceeds {MAX_FRAME_BYTES}"),
        ));
    }
    let mut payload = vec![0_u8; length];
    reader.read_exact(&mut payload).await?;
    rmp_serde::from_slice(&payload)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn messagepack_frames_roundtrip() {
        let (mut left, mut right) = tokio::io::duplex(4096);
        let expected = WorkerCommand::Hello {
            protocol_version: WORKER_PROTOCOL_VERSION,
        };
        let sent = expected.clone();
        let writer = tokio::spawn(async move { write_frame(&mut left, &sent).await });
        let received: WorkerCommand = read_frame(&mut right).await.unwrap();
        writer.await.unwrap().unwrap();
        assert_eq!(received, expected);
    }

    #[tokio::test]
    async fn oversized_length_is_rejected_before_allocation() {
        let (mut left, mut right) = tokio::io::duplex(16);
        let writer = tokio::spawn(async move {
            left.write_all(&((MAX_FRAME_BYTES as u32) + 1).to_be_bytes())
                .await
                .unwrap();
        });
        let error = read_frame::<_, WorkerCommand>(&mut right)
            .await
            .unwrap_err();
        writer.await.unwrap();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }
}
