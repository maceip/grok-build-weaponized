use std::collections::BTreeSet;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::{OperationId, ProfileId, ProviderKind, VersionRange};

pub const RUNTIME_PROFILE_SCHEMA_VERSION: u32 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticSeverity {
    Error,
    Warning,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileDiagnostic {
    pub severity: DiagnosticSeverity,
    pub path: String,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "libc", rename_all = "snake_case")]
pub enum LibcTarget {
    Native,
    MuslStatic,
    Glibc { minimum: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentTarget {
    pub target_triple: String,
    pub libc: LibcTarget,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactBinding {
    pub logical_name: String,
    pub path: PathBuf,
    pub content_hash: String,
    pub byte_size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileProviderRequirement {
    pub kind: ProviderKind,
    #[serde(default)]
    pub operations: Vec<OperationId>,
    #[serde(default)]
    pub required_features: Vec<String>,
    pub maximum_concurrency: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeResourceLimits {
    pub command_queue: u32,
    pub event_batch: u32,
    pub maximum_clients: u32,
    pub maximum_worker_processes: u32,
    pub maximum_memory_bytes: u64,
    pub maximum_spool_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GuiRenderer {
    /// Compact OpenGL renderer used by the optional egui operator client.
    Glow,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GuiProfile {
    pub renderer: GuiRenderer,
    pub maximum_binary_bytes: u64,
}

/// Declarative local runtime profile validated before daemon activation.
///
/// Profiles describe deployment compatibility, resource ceilings, providers,
/// and immutable artifacts. They intentionally contain no transport camouflage,
/// implant, or arbitrary scripting fields.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeProfile {
    pub schema_version: u32,
    pub profile_id: ProfileId,
    pub revision: u32,
    pub protocol_range: VersionRange,
    #[serde(default)]
    pub targets: Vec<DeploymentTarget>,
    pub limits: RuntimeResourceLimits,
    #[serde(default)]
    pub providers: Vec<ProfileProviderRequirement>,
    #[serde(default)]
    pub models: Vec<ArtifactBinding>,
    #[serde(default)]
    pub adapters: Vec<ArtifactBinding>,
    #[serde(default)]
    pub tool_bundles: Vec<ArtifactBinding>,
    /// Maximum stripped size of the headless `grokctl` client.
    pub maximum_headless_binary_bytes: u64,
    /// Maximum stripped size of the headless `grokd` daemon.
    pub maximum_daemon_binary_bytes: u64,
    #[serde(default)]
    pub gui: Option<GuiProfile>,
}

impl RuntimeProfile {
    pub fn content_hash(&self) -> String {
        let encoded = serde_json::to_vec(self).expect("runtime profile serialization cannot fail");
        blake3::hash(&encoded).to_hex().to_string()
    }

    pub fn lint(&self) -> Vec<ProfileDiagnostic> {
        let mut diagnostics = Vec::new();
        let mut error = |path: &str, message: String| {
            diagnostics.push(ProfileDiagnostic {
                severity: DiagnosticSeverity::Error,
                path: path.to_owned(),
                message,
            });
        };

        if self.schema_version != RUNTIME_PROFILE_SCHEMA_VERSION {
            error(
                "schema_version",
                format!(
                    "unsupported schema version {}; expected {RUNTIME_PROFILE_SCHEMA_VERSION}",
                    self.schema_version
                ),
            );
        }
        if self.profile_id.as_str().trim().is_empty() {
            error("profile_id", "profile id must not be empty".to_owned());
        }
        if self.revision == 0 {
            error("revision", "revision must be greater than zero".to_owned());
        }
        if self.protocol_range.minimum == 0
            || self.protocol_range.minimum > self.protocol_range.maximum
        {
            error(
                "protocol_range",
                "protocol range must be non-zero and ordered".to_owned(),
            );
        }
        if self.targets.is_empty() {
            error(
                "targets",
                "at least one deployment target is required".to_owned(),
            );
        }
        let mut target_triples = BTreeSet::new();
        for (index, target) in self.targets.iter().enumerate() {
            let path = format!("targets[{index}]");
            if target.target_triple.trim().is_empty() {
                error(
                    &format!("{path}.target_triple"),
                    "target triple is empty".to_owned(),
                );
            }
            if !target_triples.insert(target.target_triple.as_str()) {
                error(&path, "target triple is duplicated".to_owned());
            }
            if let LibcTarget::Glibc { minimum } = &target.libc
                && !valid_numeric_version(minimum)
            {
                error(
                    &format!("{path}.libc.minimum"),
                    "glibc minimum must be a numeric dotted version".to_owned(),
                );
            }
        }

        let limits = &self.limits;
        for (path, value) in [
            ("limits.command_queue", limits.command_queue),
            ("limits.event_batch", limits.event_batch),
            ("limits.maximum_clients", limits.maximum_clients),
            (
                "limits.maximum_worker_processes",
                limits.maximum_worker_processes,
            ),
        ] {
            if value == 0 {
                error(path, "limit must be greater than zero".to_owned());
            }
        }
        if limits.event_batch > crate::MAX_EVENT_BATCH {
            error(
                "limits.event_batch",
                format!(
                    "event batch exceeds protocol cap {}",
                    crate::MAX_EVENT_BATCH
                ),
            );
        }
        if limits.maximum_memory_bytes == 0 || limits.maximum_spool_bytes == 0 {
            error(
                "limits",
                "memory and spool byte limits must be greater than zero".to_owned(),
            );
        }
        if self.maximum_headless_binary_bytes == 0 {
            error(
                "maximum_headless_binary_bytes",
                "headless binary size budget must be greater than zero".to_owned(),
            );
        }
        if self.maximum_daemon_binary_bytes == 0 {
            error(
                "maximum_daemon_binary_bytes",
                "daemon binary size budget must be greater than zero".to_owned(),
            );
        }
        if self
            .gui
            .as_ref()
            .is_some_and(|gui| gui.maximum_binary_bytes == 0)
        {
            error(
                "gui.maximum_binary_bytes",
                "GUI binary size budget must be greater than zero".to_owned(),
            );
        }

        for (index, provider) in self.providers.iter().enumerate() {
            if provider.maximum_concurrency == 0 {
                error(
                    &format!("providers[{index}].maximum_concurrency"),
                    "provider concurrency must be greater than zero".to_owned(),
                );
            }
            if provider
                .required_features
                .iter()
                .any(|feature| feature.trim().is_empty())
            {
                error(
                    &format!("providers[{index}].required_features"),
                    "required features must not contain empty values".to_owned(),
                );
            }
        }

        let mut names = BTreeSet::new();
        for (collection, artifacts) in [
            ("models", &self.models),
            ("adapters", &self.adapters),
            ("tool_bundles", &self.tool_bundles),
        ] {
            for (index, artifact) in artifacts.iter().enumerate() {
                let path = format!("{collection}[{index}]");
                if artifact.logical_name.trim().is_empty() {
                    error(
                        &format!("{path}.logical_name"),
                        "artifact name is empty".to_owned(),
                    );
                }
                if !names.insert(artifact.logical_name.as_str()) {
                    error(&path, "artifact logical name is duplicated".to_owned());
                }
                if !artifact.path.is_absolute() {
                    error(
                        &format!("{path}.path"),
                        "artifact path must be absolute".to_owned(),
                    );
                }
                if !valid_blake3_hex(&artifact.content_hash) {
                    error(
                        &format!("{path}.content_hash"),
                        "content hash must be 64 lowercase hexadecimal characters".to_owned(),
                    );
                }
                if artifact.byte_size == 0 {
                    error(&format!("{path}.byte_size"), "artifact is empty".to_owned());
                }
            }
        }

        diagnostics
    }

    pub fn is_valid(&self) -> bool {
        !self
            .lint()
            .iter()
            .any(|diagnostic| diagnostic.severity == DiagnosticSeverity::Error)
    }
}

fn valid_numeric_version(version: &str) -> bool {
    let mut components = version.split('.');
    let valid = components.by_ref().all(|component| {
        !component.is_empty() && component.bytes().all(|byte| byte.is_ascii_digit())
    });
    valid && version.contains('.')
}

fn valid_blake3_hex(hash: &str) -> bool {
    hash.len() == 64
        && hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile() -> RuntimeProfile {
        RuntimeProfile {
            schema_version: RUNTIME_PROFILE_SCHEMA_VERSION,
            profile_id: "profile_compact".into(),
            revision: 1,
            protocol_range: VersionRange::exact(crate::PROTOCOL_VERSION),
            targets: vec![DeploymentTarget {
                target_triple: "x86_64-unknown-linux-musl".to_owned(),
                libc: LibcTarget::MuslStatic,
            }],
            limits: RuntimeResourceLimits {
                command_queue: 256,
                event_batch: 128,
                maximum_clients: 32,
                maximum_worker_processes: 8,
                maximum_memory_bytes: 1 << 30,
                maximum_spool_bytes: 1 << 30,
            },
            providers: Vec::new(),
            models: vec![ArtifactBinding {
                logical_name: "executor".to_owned(),
                path: PathBuf::from("/opt/grok/models/executor.litertlm"),
                content_hash: "a".repeat(64),
                byte_size: 1,
            }],
            adapters: Vec::new(),
            tool_bundles: Vec::new(),
            maximum_headless_binary_bytes: 32 * 1024 * 1024,
            maximum_daemon_binary_bytes: 64 * 1024 * 1024,
            gui: Some(GuiProfile {
                renderer: GuiRenderer::Glow,
                maximum_binary_bytes: 64 * 1024 * 1024,
            }),
        }
    }

    #[test]
    fn compact_profile_lints_without_io() {
        assert!(profile().lint().is_empty());
        let mut invalid = profile();
        invalid.limits.event_batch = crate::MAX_EVENT_BATCH + 1;
        invalid.models[0].content_hash = "not-a-hash".to_owned();
        assert_eq!(invalid.lint().len(), 2);
    }
}
