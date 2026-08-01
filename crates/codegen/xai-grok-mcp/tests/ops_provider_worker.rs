use std::time::{Duration, SystemTime};

use xai_grok_control_plane::{ExecutionProvider, ProcessExecutionProvider, ProcessProviderConfig};
use xai_grok_protocol::{
    CapabilityRequirement, ExecutionMode, ExecutionTask, ProviderDispatch, TaskId,
};

fn dispatch(operation: &str, input: serde_json::Value) -> ProviderDispatch {
    ProviderDispatch {
        request_id: format!("request-{operation}").into(),
        engagement_id: "engagement-provider-worker".into(),
        plan_revision: 1,
        task: ExecutionTask {
            task_id: TaskId::from_string(format!("task-{operation}")),
            objective: "validate the daemon connector transport".to_owned(),
            mode: ExecutionMode::Interactive,
            capability: CapabilityRequirement {
                operation_id: operation.into(),
                preferred_provider: Some("mcp-vulnerability-index".into()),
                required_features: vec!["offline_index".to_owned()],
            },
            input,
            deadline_unix_ms: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
                + 30_000,
            completion_tests: Vec::new(),
            depends_on: Vec::new(),
        },
        provider_id: "mcp-vulnerability-index".into(),
        lease_epoch: 1,
    }
}

#[tokio::test]
async fn daemon_process_provider_executes_the_real_vulnerability_worker() {
    let state = tempfile::tempdir().unwrap();
    let fixture =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/exploitdb");
    let mut config = ProcessProviderConfig::new(env!("CARGO_BIN_EXE_grok-ops-mcp"));
    config.arguments = vec![
        "vulnerability-index".to_owned(),
        "--provider-worker".to_owned(),
    ];
    config.environment.insert(
        "GROK_EXPLOITDB_CSV".to_owned(),
        fixture
            .join("files_exploits.csv")
            .to_string_lossy()
            .into_owned(),
    );
    config.environment.insert(
        "GROK_EXPLOITDB_ROOT".to_owned(),
        fixture.to_string_lossy().into_owned(),
    );
    config.environment.insert(
        "GROK_VULN_INDEX_DB".to_owned(),
        state
            .path()
            .join("vulnerability.sqlite")
            .to_string_lossy()
            .into_owned(),
    );
    config.startup_timeout = Duration::from_secs(10);

    let provider = ProcessExecutionProvider::start(config).await.unwrap();
    assert_eq!(
        provider.manifest().provider_id.as_str(),
        "mcp-vulnerability-index"
    );
    provider
        .execute(dispatch(
            "vulnerability.refresh_index",
            serde_json::json!({}),
        ))
        .await
        .unwrap();
    let result = provider
        .execute(dispatch(
            "vulnerability.search",
            serde_json::json!({"query":"CVE-2026-4242","limit":1}),
        ))
        .await
        .unwrap();
    assert_eq!(result.output[0]["id"], "EDB-424242");
    assert!(
        result.output[0]["execution_syntax"]
            .as_str()
            .unwrap()
            .contains("example_check.py")
    );
}
