use std::time::{Duration, SystemTime};

use xai_grok_control_plane::{ExecutionProvider, ProcessExecutionProvider, ProcessProviderConfig};
use xai_grok_protocol::{
    CapabilityRequirement, ExecutionMode, ExecutionTask, ProviderDispatch, ServiceHealth, TaskId,
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

    let first_generation = provider.worker_generation().await;
    let first_pid = provider.status().await["child_pid"].as_u64().unwrap();
    let first_pid = i32::try_from(first_pid).unwrap();
    // SAFETY: `first_pid` was reported by the child owned by this provider,
    // remains within pid_t range, and the test waits for it to be reaped before
    // issuing more work. SIGKILL is intentional to exercise crash recovery.
    assert_eq!(unsafe { libc::kill(first_pid, libc::SIGKILL) }, 0);

    let failure_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if provider.health().await == ServiceHealth::Failed {
            break;
        }
        assert!(
            tokio::time::Instant::now() < failure_deadline,
            "provider did not observe the killed worker"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(provider.worker_generation().await, first_generation);

    tokio::time::sleep(Duration::from_millis(300)).await;
    let result = provider
        .execute(dispatch(
            "vulnerability.search",
            serde_json::json!({"query":"CVE-2026-4242","limit":1}),
        ))
        .await
        .unwrap();
    assert_eq!(result.output[0]["id"], "EDB-424242");
    assert_eq!(provider.worker_generation().await, first_generation + 1);
    assert!(provider.status().await["child_pid"].as_u64().is_some());
}
