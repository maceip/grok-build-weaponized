use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::{CapabilityManifest, EvidenceObservation, ProtocolError, ProviderDispatch, RequestId};

pub const PROVIDER_WORKER_PROTOCOL_VERSION: u32 = 1;
pub const MAX_PROVIDER_WORKER_FRAME_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "request", rename_all = "snake_case")]
pub enum ProviderWorkerRequest {
    Hello { protocol_version: u32 },
    Execute { dispatch: ProviderDispatch },
    Cancel { request_id: RequestId },
    Shutdown,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "response", rename_all = "snake_case")]
pub enum ProviderWorkerResponse {
    Hello {
        protocol_version: u32,
        manifest: CapabilityManifest,
    },
    Execute {
        request_id: RequestId,
        result: Result<ProviderWorkerOutput, ProtocolError>,
    },
    Cancel {
        request_id: RequestId,
        result: Result<(), ProtocolError>,
    },
    Ack,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum ProviderWorkerArtifactSource {
    Inline { bytes: Vec<u8> },
    File { path: PathBuf },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProviderWorkerArtifact {
    pub media_type: String,
    pub content: ProviderWorkerArtifactSource,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ProviderWorkerOutput {
    pub output: serde_json::Value,
    #[serde(default)]
    pub observations: Vec<EvidenceObservation>,
    #[serde(default)]
    pub artifacts: Vec<ProviderWorkerArtifact>,
}
