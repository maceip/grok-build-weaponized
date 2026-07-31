use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{OperationId, ProtocolError, ProtocolErrorCode, ProviderId};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionRange {
    pub minimum: u32,
    pub maximum: u32,
}

impl VersionRange {
    pub const fn exact(version: u32) -> Self {
        Self {
            minimum: version,
            maximum: version,
        }
    }

    pub fn supports(self, version: u32) -> bool {
        self.minimum <= version && version <= self.maximum
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    ModelRuntime,
    NativeExecution,
    McpConnector,
    MemoryIndexer,
    ArtifactProcessor,
    Integration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancellationSemantics {
    Unsupported,
    Cooperative,
    ProcessTree,
    Immediate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoverySemantics {
    None,
    Restartable,
    Resumable,
    Reattachable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactContract {
    InlineOnly,
    Optional,
    Required,
    CursorStream,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConcurrencyProfile {
    pub maximum_parallel: u32,
    pub queue_capacity: u32,
    pub exclusive_resource: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Platform {
    pub os: String,
    pub architecture: String,
    pub accelerator: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OperationDescriptor {
    pub operation_id: OperationId,
    pub display_name: String,
    pub input_schema: serde_json::Value,
    pub output_schema: serde_json::Value,
    #[serde(default)]
    pub streaming: bool,
    #[serde(default)]
    pub interactive: bool,
    #[serde(default)]
    pub deferred: bool,
}

impl OperationDescriptor {
    pub fn schema_hash(&self) -> String {
        let encoded =
            serde_json::to_vec(&(&self.operation_id, &self.input_schema, &self.output_schema))
                .expect("serializing JSON values is infallible");
        blake3::hash(&encoded).to_hex().to_string()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CapabilityManifest {
    pub provider_id: ProviderId,
    pub provider_version: String,
    pub protocol: VersionRange,
    pub kind: ProviderKind,
    #[serde(default)]
    pub features: BTreeSet<String>,
    pub operations: Vec<OperationDescriptor>,
    pub concurrency: ConcurrencyProfile,
    pub cancellation: CancellationSemantics,
    pub recovery: RecoverySemantics,
    pub artifacts: ArtifactContract,
    #[serde(default)]
    pub platforms: BTreeSet<Platform>,
    #[serde(default)]
    pub metadata: serde_json::Map<String, serde_json::Value>,
}

impl CapabilityManifest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.provider_id.0.trim().is_empty() || self.provider_version.trim().is_empty() {
            return Err(ProtocolError::new(
                ProtocolErrorCode::InvalidManifest,
                "provider id and version must not be empty",
            ));
        }
        if self.protocol.minimum == 0 || self.protocol.minimum > self.protocol.maximum {
            return Err(ProtocolError::new(
                ProtocolErrorCode::InvalidManifest,
                "provider protocol range is invalid",
            ));
        }
        if self.concurrency.maximum_parallel == 0 || self.concurrency.queue_capacity == 0 {
            return Err(ProtocolError::new(
                ProtocolErrorCode::InvalidManifest,
                "provider concurrency and queue capacity must be non-zero",
            ));
        }
        if self.operations.is_empty() {
            return Err(ProtocolError::new(
                ProtocolErrorCode::InvalidManifest,
                "provider must expose at least one operation",
            ));
        }
        if self
            .features
            .iter()
            .any(|feature| feature.trim().is_empty())
        {
            return Err(ProtocolError::new(
                ProtocolErrorCode::InvalidManifest,
                "provider features must not be empty",
            ));
        }
        let mut operation_ids = BTreeSet::new();
        for operation in &self.operations {
            if operation.operation_id.0.trim().is_empty()
                || operation.display_name.trim().is_empty()
            {
                return Err(ProtocolError::new(
                    ProtocolErrorCode::InvalidManifest,
                    "operation id and display name must not be empty",
                ));
            }
            if !operation_ids.insert(operation.operation_id.clone()) {
                return Err(ProtocolError::new(
                    ProtocolErrorCode::InvalidManifest,
                    format!("duplicate operation id {}", operation.operation_id),
                ));
            }
            if !operation.interactive && !operation.deferred {
                return Err(ProtocolError::new(
                    ProtocolErrorCode::InvalidManifest,
                    format!(
                        "operation {} is unreachable because it supports no execution mode",
                        operation.operation_id
                    ),
                ));
            }
        }
        Ok(())
    }

    pub fn operation(&self, operation_id: &OperationId) -> Option<&OperationDescriptor> {
        self.operations
            .iter()
            .find(|operation| &operation.operation_id == operation_id)
    }

    pub fn content_hash(&self) -> String {
        let mut value = serde_json::to_value(self).expect("manifest serialization cannot fail");
        canonicalize_json(&mut value);
        let encoded = serde_json::to_vec(&value).expect("manifest serialization cannot fail");
        blake3::hash(&encoded).to_hex().to_string()
    }
}

fn canonicalize_json(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                canonicalize_json(value);
            }
        }
        serde_json::Value::Object(object) => {
            let old = std::mem::take(object);
            let mut entries = old.into_iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            for (key, mut value) in entries {
                canonicalize_json(&mut value);
                object.insert(key, value);
            }
        }
        _ => {}
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceHealth {
    Starting,
    Ready,
    Degraded,
    Draining,
    Stopped,
    Failed,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn operation(id: &str) -> OperationDescriptor {
        OperationDescriptor {
            operation_id: id.into(),
            display_name: id.to_owned(),
            input_schema: serde_json::json!({"type": "object"}),
            output_schema: serde_json::json!({"type": "object"}),
            streaming: false,
            interactive: true,
            deferred: true,
        }
    }

    #[test]
    fn manifest_rejects_duplicate_operations() {
        let manifest = CapabilityManifest {
            provider_id: "native".into(),
            provider_version: "1.0.0".to_owned(),
            protocol: VersionRange::exact(1),
            kind: ProviderKind::NativeExecution,
            features: BTreeSet::new(),
            operations: vec![operation("run"), operation("run")],
            concurrency: ConcurrencyProfile {
                maximum_parallel: 4,
                queue_capacity: 16,
                exclusive_resource: None,
            },
            cancellation: CancellationSemantics::ProcessTree,
            recovery: RecoverySemantics::Reattachable,
            artifacts: ArtifactContract::CursorStream,
            platforms: BTreeSet::new(),
            metadata: serde_json::Map::new(),
        };
        assert_eq!(
            manifest.validate().unwrap_err().code,
            ProtocolErrorCode::InvalidManifest
        );
    }
}
