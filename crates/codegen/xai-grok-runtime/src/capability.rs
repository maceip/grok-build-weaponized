use std::collections::BTreeSet;

use xai_grok_protocol::{
    ArtifactContract, CancellationSemantics, CapabilityManifest, ConcurrencyProfile,
    OperationDescriptor, PROTOCOL_VERSION, Platform, ProviderKind, RecoverySemantics, VersionRange,
};

use crate::RuntimeManagerConfig;

/// Versioned control-plane manifest for the process-isolated LiteRT-LM runtime.
pub fn local_runtime_capability_manifest(config: &RuntimeManagerConfig) -> CapabilityManifest {
    let operations = [
        (
            "local_model.measure",
            "Render and measure prompt",
            false,
            true,
            true,
        ),
        (
            "local_model.generate",
            "Generate token stream",
            true,
            true,
            true,
        ),
        (
            "local_model.prewarm",
            "Prewarm model replica",
            false,
            false,
            true,
        ),
        ("local_model.cancel", "Cancel generation", false, true, true),
        (
            "local_model.adapter.load",
            "Load LoRA adapter",
            false,
            false,
            true,
        ),
        (
            "local_model.adapter.select",
            "Select resident LoRA adapter",
            false,
            true,
            true,
        ),
        (
            "local_model.adapter.unload",
            "Unload LoRA adapter",
            false,
            false,
            true,
        ),
        (
            "local_model.session.release",
            "Release session and KV state",
            false,
            true,
            true,
        ),
        (
            "local_model.capacity",
            "Read runtime capacity",
            false,
            true,
            true,
        ),
    ]
    .into_iter()
    .map(
        |(operation_id, display_name, streaming, interactive, deferred)| OperationDescriptor {
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
        },
    )
    .collect();
    let mut platforms = BTreeSet::new();
    platforms.insert(Platform {
        os: std::env::consts::OS.to_owned(),
        architecture: std::env::consts::ARCH.to_owned(),
        accelerator: cfg!(target_os = "macos").then(|| "metal".to_owned()),
    });
    let mut metadata = serde_json::Map::new();
    metadata.insert("transport".to_owned(), "inherited_unix_socket".into());
    metadata.insert("wire_format".to_owned(), "bounded_messagepack".into());
    metadata.insert("engine".to_owned(), "litert-lm".into());
    metadata.insert("isolated_worker_default".to_owned(), true.into());
    metadata.insert("exact_prompt_measurement".to_owned(), true.into());
    metadata.insert("json_schema_constrained_decoding".to_owned(), true.into());
    metadata.insert("single_adapter_per_request".to_owned(), true.into());
    let features = [
        "adapter_residency",
        "bounded_binary_ipc",
        "cancellation",
        "exact_prompt_measurement",
        "json_schema_constrained_decoding",
        "memory_counters",
        "session_release",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    CapabilityManifest {
        provider_id: "local-litert-runtime".into(),
        provider_version: env!("CARGO_PKG_VERSION").to_owned(),
        protocol: VersionRange::exact(PROTOCOL_VERSION),
        kind: ProviderKind::ModelRuntime,
        features,
        operations,
        concurrency: ConcurrencyProfile {
            maximum_parallel: u32::try_from(config.max_active_requests.max(1)).unwrap_or(u32::MAX),
            queue_capacity: u32::try_from(config.queue_capacity.max(1)).unwrap_or(u32::MAX),
            exclusive_resource: Some("local-accelerator".to_owned()),
        },
        cancellation: CancellationSemantics::Cooperative,
        recovery: RecoverySemantics::Restartable,
        artifacts: ArtifactContract::Optional,
        platforms,
        metadata,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_manifest_is_valid_and_declares_exact_measurement() {
        let manifest = local_runtime_capability_manifest(&RuntimeManagerConfig::default());
        manifest.validate().unwrap();
        assert!(manifest.operation(&"local_model.generate".into()).is_some());
        assert_eq!(
            manifest.metadata.get("exact_prompt_measurement"),
            Some(&serde_json::Value::Bool(true))
        );
    }
}
