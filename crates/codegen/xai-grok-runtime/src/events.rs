use serde::{Deserialize, Serialize};

/// Model-visible channel for a streamed local-runtime token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeChannel {
    Text,
    Reasoning,
}

/// Streaming events emitted by a native runtime worker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum RuntimeEvent {
    StreamStarted {
        request_id: String,
        timestamp_ms: i64,
    },
    FirstToken {
        request_id: String,
    },
    ChannelToken {
        request_id: String,
        channel: RuntimeChannel,
        text: String,
        chunk_index: u64,
    },
    ToolCallDelta {
        request_id: String,
        tool_index: u32,
        id: Option<String>,
        name: Option<String>,
        arguments_delta: Option<String>,
    },
}
