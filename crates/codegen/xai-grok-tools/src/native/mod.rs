//! Native, in-process drivers for high-volume local observability.

pub mod execution_supervisor;
pub mod job_registry;
pub mod nmap;

use std::collections::BTreeSet;

use xai_grok_protocol::{
    ArtifactContract, CancellationSemantics, CapabilityManifest, ConcurrencyProfile,
    OperationDescriptor, PROTOCOL_VERSION, Platform, ProviderKind, RecoverySemantics, VersionRange,
};

/// Control-plane capability declaration for high-volume in-process execution.
pub fn native_execution_capability_manifest() -> CapabilityManifest {
    let operation = |operation_id: &str,
                     display_name: &str,
                     streaming: bool,
                     interactive: bool,
                     deferred: bool| OperationDescriptor {
        operation_id: operation_id.into(),
        display_name: display_name.to_owned(),
        input_schema: serde_json::json!({
            "type": "object",
            "additionalProperties": true
        }),
        output_schema: serde_json::json!({
            "type": "object",
            "additionalProperties": true
        }),
        streaming,
        interactive,
        deferred,
    };
    let operations = vec![
        operation("native.command.start", "Start command", true, true, true),
        operation(
            "native.command.status",
            "Read command status",
            false,
            true,
            true,
        ),
        operation("native.command.wait", "Wait for command", true, true, true),
        operation(
            "native.command.output",
            "Read command output cursor",
            true,
            true,
            true,
        ),
        operation(
            "native.command.cancel",
            "Cancel command tree",
            false,
            true,
            true,
        ),
        operation(
            "native.nmap.start",
            "Start typed Nmap scan",
            true,
            true,
            true,
        ),
        operation(
            "native.nmap.status",
            "Read Nmap scan status",
            false,
            true,
            true,
        ),
        operation("native.nmap.result", "Read Nmap report", true, true, true),
        operation("native.nmap.cancel", "Cancel Nmap scan", false, true, true),
    ];
    let mut platforms = BTreeSet::new();
    platforms.insert(Platform {
        os: std::env::consts::OS.to_owned(),
        architecture: std::env::consts::ARCH.to_owned(),
        accelerator: None,
    });
    let mut metadata = serde_json::Map::new();
    metadata.insert("shared_job_registry".to_owned(), true.into());
    metadata.insert("cursor_artifacts".to_owned(), true.into());
    metadata.insert("process_tree_cancellation".to_owned(), true.into());
    metadata.insert("nmap_typed_arguments_only".to_owned(), true.into());
    let features = [
        "cursor_artifacts",
        "job_recovery",
        "nmap_xml",
        "process_tree_cancellation",
        "stream_identity",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    CapabilityManifest {
        provider_id: "native-execution".into(),
        provider_version: env!("CARGO_PKG_VERSION").to_owned(),
        protocol: VersionRange::exact(PROTOCOL_VERSION),
        kind: ProviderKind::NativeExecution,
        features,
        operations,
        concurrency: ConcurrencyProfile {
            maximum_parallel: 100,
            queue_capacity: 512,
            exclusive_resource: None,
        },
        cancellation: CancellationSemantics::ProcessTree,
        recovery: RecoverySemantics::Reattachable,
        artifacts: ArtifactContract::CursorStream,
        platforms,
        metadata,
    }
}

#[cfg(test)]
mod capability_tests {
    use super::*;

    #[test]
    fn native_manifest_is_valid_and_declares_cursor_output() {
        let manifest = native_execution_capability_manifest();
        manifest.validate().unwrap();
        assert_eq!(manifest.artifacts, ArtifactContract::CursorStream);
        assert!(manifest.operation(&"native.nmap.start".into()).is_some());
    }
}
