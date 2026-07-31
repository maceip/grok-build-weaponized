use serde::{Deserialize, Serialize};

use crate::{CommandId, ProtocolError, ProtocolErrorCode, WorkspaceId};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IngressSource {
    Cli,
    Tui,
    Buzz,
    Qm,
    Api,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IngressEnvelope {
    pub command_id: CommandId,
    pub source: IngressSource,
    pub source_event_id: String,
    pub workspace_id: WorkspaceId,
    pub session_id: String,
    pub prompt_id: String,
    pub request: String,
    #[serde(default)]
    pub metadata: serde_json::Map<String, serde_json::Value>,
}

impl IngressEnvelope {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        for (field, value) in [
            ("source_event_id", self.source_event_id.as_str()),
            ("workspace_id", self.workspace_id.as_str()),
            ("session_id", self.session_id.as_str()),
            ("prompt_id", self.prompt_id.as_str()),
            ("request", self.request.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(ProtocolError::new(
                    ProtocolErrorCode::InvalidEnvelope,
                    format!("ingress {field} must not be empty"),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BuzzIngress {
    pub event_id: String,
    pub community: String,
    pub channel_id: String,
    pub author: String,
    pub content: String,
}

impl BuzzIngress {
    pub fn into_envelope(self) -> IngressEnvelope {
        let mut metadata = serde_json::Map::new();
        metadata.insert("community".to_owned(), self.community.into());
        metadata.insert("channel_id".to_owned(), self.channel_id.clone().into());
        metadata.insert("author".to_owned(), self.author.into());
        IngressEnvelope {
            command_id: CommandId::new(),
            source: IngressSource::Buzz,
            source_event_id: self.event_id.clone(),
            workspace_id: WorkspaceId::from_string(format!("buzz:{}", self.channel_id)),
            session_id: format!("buzz:{}", self.channel_id),
            prompt_id: self.event_id,
            request: self.content,
            metadata,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QmIngress {
    pub job_id: String,
    pub scope_id: String,
    pub room_id: Option<String>,
    pub request: String,
}

impl QmIngress {
    pub fn into_envelope(self) -> IngressEnvelope {
        let mut metadata = serde_json::Map::new();
        if let Some(room_id) = self.room_id {
            metadata.insert("room_id".to_owned(), room_id.into());
        }
        IngressEnvelope {
            command_id: CommandId::new(),
            source: IngressSource::Qm,
            source_event_id: self.job_id.clone(),
            workspace_id: WorkspaceId::from_string(format!("qm:{}", self.scope_id)),
            session_id: format!("qm:{}", self.scope_id),
            prompt_id: self.job_id,
            request: self.request,
            metadata,
        }
    }
}
