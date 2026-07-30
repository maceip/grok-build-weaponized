use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tempfile::TempDir;

use crate::{
    ActionIdentity, ActionResolution, ActionSpec, ActionStatus, EngagementCheckpoint,
    EngagementCoordinator, EngagementStage, EngagementStatus, EngagementStore, JobCheckpoint,
    JobKind, JobLifecycle, NewEngagement, QueuePriority,
};

fn new_engagement(session: &str, prompt: &str) -> NewEngagement {
    NewEngagement {
        session_id: session.to_string(),
        prompt_id: prompt.to_string(),
        workspace_id: "/workspace".to_string(),
        user_request: "run a durable engagement".to_string(),
        priority: QueuePriority::Interactive,
        checkpoint: EngagementCheckpoint::default(),
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[test]
fn accepted_work_is_idempotent_and_survives_reopen() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("engagements.sqlite");
    let engagement_id = {
        let mut store = EngagementStore::open(&path).unwrap();
        let first = store
            .accept(new_engagement("session-a", "prompt-a"))
            .unwrap();
        assert!(first.value.accepted);
        let second = store
            .accept(new_engagement("session-a", "prompt-a"))
            .unwrap();
        assert!(!second.value.accepted);
        assert_eq!(
            first.value.record.engagement_id,
            second.value.record.engagement_id
        );
        assert_eq!(store.queue_depth().unwrap(), 1);
        first.value.record.engagement_id
    };
    let reopened = EngagementStore::open(&path).unwrap();
    assert_eq!(
        reopened.get(&engagement_id).unwrap().unwrap().status,
        EngagementStatus::Queued
    );
    assert_eq!(
        reopened
            .latest_snapshot(&engagement_id)
            .unwrap()
            .unwrap()
            .event_seq,
        0
    );
}

#[test]
fn event_replay_rejects_tampered_payloads() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("engagements.sqlite");
    let engagement_id = {
        let mut store = EngagementStore::open(&path).unwrap();
        store
            .accept(new_engagement("session-a", "prompt-a"))
            .unwrap()
            .value
            .record
            .engagement_id
    };
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute(
            "UPDATE engagement_events SET payload_json='{}'
             WHERE engagement_id=?1 AND seq=0",
            [engagement_id.0.as_str()],
        )
        .unwrap();
    drop(connection);
    let store = EngagementStore::open(&path).unwrap();
    assert!(
        store.events_after(&engagement_id, None, 10).is_err(),
        "event payload corruption must not be replayed as valid state"
    );
}

#[test]
fn event_replay_rejects_tampered_envelope_fields() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("engagements.sqlite");
    let engagement_id = {
        let mut store = EngagementStore::open(&path).unwrap();
        store
            .accept(new_engagement("session-a", "prompt-a"))
            .unwrap()
            .value
            .record
            .engagement_id
    };
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute(
            "UPDATE engagement_events SET stage='direct'
             WHERE engagement_id=?1 AND seq=0",
            [engagement_id.0.as_str()],
        )
        .unwrap();
    drop(connection);
    let store = EngagementStore::open(&path).unwrap();
    assert!(
        store.events_after(&engagement_id, None, 10).is_err(),
        "event envelope corruption must not be replayed as valid state"
    );
}

#[test]
fn lease_epoch_fences_stale_workers_and_expiration_requeues_safe_work() {
    let directory = TempDir::new().unwrap();
    let mut store = EngagementStore::open(&directory.path().join("state.sqlite")).unwrap();
    let accepted = store
        .accept(new_engagement("session-a", "prompt-a"))
        .unwrap()
        .value
        .record;
    let first = store
        .claim(&accepted.engagement_id, "worker-a", 1, false)
        .unwrap()
        .value
        .unwrap();
    std::thread::sleep(Duration::from_millis(3));
    let recovery = store.recover_expired(now_ms()).unwrap();
    assert_eq!(recovery.requeued, 1);
    let second = store
        .claim(&accepted.engagement_id, "worker-b", 10_000, false)
        .unwrap()
        .value
        .unwrap();
    assert!(second.lease_epoch > first.lease_epoch);
    let stale = store.transition(
        &accepted.engagement_id,
        first.lease_epoch,
        EngagementStatus::Executing,
        Some(EngagementStage::Direct),
        EngagementCheckpoint::default(),
        serde_json::json!({}),
    );
    assert!(matches!(
        stale,
        Err(crate::EngagementError::StaleLease { .. })
    ));
}

#[test]
fn every_cooperation_stage_checkpoint_survives_store_restart() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("state.sqlite");
    let (engagement_id, lease_epoch) = {
        let mut store = EngagementStore::open(&path).unwrap();
        let accepted = store
            .accept(new_engagement("session-a", "prompt-a"))
            .unwrap()
            .value
            .record;
        let claimed = store
            .claim(&accepted.engagement_id, "worker", 60_000, false)
            .unwrap()
            .value
            .unwrap();
        (claimed.engagement_id, claimed.lease_epoch)
    };
    let stages = [
        (EngagementStatus::Planning, EngagementStage::Planner, "plan"),
        (
            EngagementStatus::Executing,
            EngagementStage::Executor,
            "execute",
        ),
        (
            EngagementStatus::Reviewing,
            EngagementStage::Reviewer,
            "review",
        ),
        (
            EngagementStatus::Correcting,
            EngagementStage::Correction,
            "correct",
        ),
        (
            EngagementStatus::Executing,
            EngagementStage::Executor,
            "re-execute",
        ),
    ];
    for (index, (status, stage, task_id)) in stages.into_iter().enumerate() {
        let checkpoint = EngagementCheckpoint {
            plan_revision: if index >= 3 { 1 } else { 0 },
            task_id: Some(task_id.to_string()),
            task_index: u32::try_from(index).unwrap(),
            task_count: 5,
            correction_count: u8::from(index >= 3),
            evidence_ids: vec![format!("evidence-{index}")],
            ..EngagementCheckpoint::default()
        };
        {
            let mut store = EngagementStore::open(&path).unwrap();
            store
                .transition(
                    &engagement_id,
                    lease_epoch,
                    status,
                    Some(stage),
                    checkpoint.clone(),
                    serde_json::json!({"crash_boundary": index}),
                )
                .unwrap();
        }
        let reopened = EngagementStore::open(&path).unwrap();
        let record = reopened.get(&engagement_id).unwrap().unwrap();
        assert_eq!(record.status, status);
        assert_eq!(record.stage, Some(stage));
        assert_eq!(record.checkpoint, checkpoint);
    }
}

#[test]
fn dispatched_action_is_parked_as_ambiguous_and_identity_survives_attempts() {
    let directory = TempDir::new().unwrap();
    let mut store = EngagementStore::open(&directory.path().join("state.sqlite")).unwrap();
    let accepted = store
        .accept(new_engagement("session-a", "prompt-a"))
        .unwrap()
        .value
        .record;
    let first = store
        .claim(&accepted.engagement_id, "worker-a", 50, false)
        .unwrap()
        .value
        .unwrap();
    store
        .transition(
            &accepted.engagement_id,
            first.lease_epoch,
            EngagementStatus::Executing,
            Some(EngagementStage::Executor),
            EngagementCheckpoint {
                plan_revision: 4,
                task_id: Some("scan".to_string()),
                ..EngagementCheckpoint::default()
            },
            serde_json::json!({}),
        )
        .unwrap();
    let spec = ActionSpec {
        identity: ActionIdentity {
            plan_revision: 4,
            task_id: "scan".to_string(),
            action_id: "nmap-target".to_string(),
        },
        action_kind: "native_nmap".to_string(),
        command_hash: "command-hash".to_string(),
        payload: Some(serde_json::json!({"target": "127.0.0.1"})),
    };
    let prepared = store
        .prepare_action(&accepted.engagement_id, first.lease_epoch, spec.clone())
        .unwrap()
        .value;
    store
        .update_action(
            &accepted.engagement_id,
            first.lease_epoch,
            &prepared.stable_key,
            ActionStatus::Dispatched,
            Some("job-1"),
            None,
            None,
        )
        .unwrap();
    std::thread::sleep(Duration::from_millis(60));
    let recovery = store.recover_expired(now_ms()).unwrap();
    assert_eq!(recovery.parked, 1);
    assert_eq!(recovery.ambiguous_actions, 1);
    assert_eq!(
        store.action(&prepared.stable_key).unwrap().unwrap().status,
        ActionStatus::Ambiguous
    );

    let second = store
        .claim(&accepted.engagement_id, "reconciler", 10_000, true)
        .unwrap()
        .value
        .unwrap();
    let same = store
        .prepare_action(&accepted.engagement_id, second.lease_epoch, spec)
        .unwrap()
        .value;
    assert_eq!(same.stable_key, prepared.stable_key);
    assert_eq!(same.attempt_count, 1);
    store
        .resolve_ambiguous_action(
            &accepted.engagement_id,
            second.lease_epoch,
            &same.stable_key,
            ActionResolution::Committed {
                result: serde_json::json!({"reconciled": true}),
                evidence_id: Some("evidence-1".to_string()),
            },
        )
        .unwrap();
    let resolved = store.action(&same.stable_key).unwrap().unwrap();
    assert_eq!(resolved.status, ActionStatus::Committed);
    assert_eq!(resolved.attempt_count, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn bounded_subscriber_repairs_lag_from_the_durable_cursor() {
    let directory = TempDir::new().unwrap();
    let coordinator =
        EngagementCoordinator::shared_for_test(directory.path().join("state.sqlite"), 2)
            .await
            .unwrap();
    let accepted = coordinator
        .accept(new_engagement("session-a", "prompt-a"))
        .await
        .unwrap();
    let mut subscription = coordinator.subscribe(accepted.record.engagement_id.clone(), None);
    let lease = coordinator
        .claim(
            accepted.record.engagement_id.clone(),
            "worker",
            Some(60_000),
            false,
        )
        .await
        .unwrap()
        .unwrap();
    for index in 0..8 {
        lease
            .transition(
                EngagementStatus::Executing,
                Some(EngagementStage::Executor),
                EngagementCheckpoint {
                    task_index: index,
                    ..EngagementCheckpoint::default()
                },
                serde_json::json!({"index": index}),
            )
            .await
            .unwrap();
    }
    let mut sequences = Vec::new();
    for _ in 0..10 {
        sequences.push(subscription.next().await.unwrap().seq);
    }
    assert_eq!(sequences, (0..10).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_preserve_single_acceptance_and_monotonic_events() {
    let directory = TempDir::new().unwrap();
    let coordinator = EngagementCoordinator::shared(directory.path().join("state.sqlite"))
        .await
        .unwrap();
    let mut accepts = Vec::new();
    for _ in 0..64 {
        let coordinator = coordinator.clone();
        accepts.push(tokio::spawn(async move {
            coordinator
                .accept(new_engagement("session-a", "prompt-a"))
                .await
                .unwrap()
        }));
    }
    let mut accepted_count = 0;
    let mut engagement_id = None;
    for accept in accepts {
        let outcome = accept.await.unwrap();
        accepted_count += usize::from(outcome.accepted);
        engagement_id = Some(outcome.record.engagement_id);
    }
    assert_eq!(accepted_count, 1);
    let engagement_id = engagement_id.unwrap();
    let lease = coordinator
        .claim(engagement_id.clone(), "worker", Some(60_000), false)
        .await
        .unwrap()
        .unwrap();
    let mut writes = Vec::new();
    for index in 0..128 {
        let lease = lease.clone();
        writes.push(tokio::spawn(async move {
            lease
                .transition(
                    EngagementStatus::Executing,
                    Some(EngagementStage::Executor),
                    EngagementCheckpoint {
                        task_index: index,
                        ..EngagementCheckpoint::default()
                    },
                    serde_json::json!({"writer": index}),
                )
                .await
                .unwrap();
        }));
    }
    for write in writes {
        write.await.unwrap();
    }
    let events = coordinator
        .events_after(engagement_id, None, 256)
        .await
        .unwrap();
    assert_eq!(events.len(), 130);
    assert_eq!(
        events.iter().map(|event| event.seq).collect::<Vec<_>>(),
        (0..130).collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn process_checkpoint_is_durable_and_recoverable() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("state.sqlite");
    let coordinator = EngagementCoordinator::shared(&path).await.unwrap();
    let lease = coordinator
        .accept_and_claim(
            new_engagement("session-a", "prompt-a"),
            "worker",
            Some(60_000),
        )
        .await
        .unwrap()
        .unwrap();
    lease
        .transition(
            EngagementStatus::Executing,
            Some(EngagementStage::Executor),
            EngagementCheckpoint::default(),
            serde_json::json!({}),
        )
        .await
        .unwrap();
    let checkpoint = JobCheckpoint {
        job_id: "job-1".to_string(),
        engagement_id: lease.engagement_id().clone(),
        action_key: None,
        kind: JobKind::Terminal,
        lifecycle: JobLifecycle::Running,
        command_hash: "hash".to_string(),
        pid: Some(42),
        process_started_at_ms: Some(now_ms()),
        process_group_id: Some(42),
        stdout_artifact: Some(directory.path().join("stdout")),
        stderr_artifact: Some(directory.path().join("stderr")),
        stdout_cursor: 15,
        stderr_cursor: 2,
        last_activity_at_ms: now_ms(),
        exit_code: None,
        payload: serde_json::json!({"quiet": false}),
    };
    lease.checkpoint_job(checkpoint.clone()).await.unwrap();
    let stored = coordinator.job("job-1".to_string()).await.unwrap().unwrap();
    assert_eq!(stored, checkpoint);
    let mut conflicting = checkpoint.clone();
    conflicting.command_hash = "different-command".to_string();
    assert!(matches!(
        lease.checkpoint_job(conflicting).await,
        Err(crate::EngagementError::JobConflict(job_id)) if job_id == "job-1"
    ));
    assert_eq!(
        coordinator.recoverable_jobs().await.unwrap(),
        vec![checkpoint.clone()]
    );
    let completed_turn = lease
        .transition(
            EngagementStatus::Completed,
            Some(EngagementStage::Executor),
            EngagementCheckpoint::default(),
            serde_json::json!({"response": "background task continues"}),
        )
        .await
        .unwrap();
    assert!(
        completed_turn.lease_owner.is_some(),
        "a background job must retain its fenced lease after the turn ends"
    );
    let mut completed_job = checkpoint;
    completed_job.lifecycle = JobLifecycle::Completed;
    completed_job.exit_code = Some(0);
    lease.checkpoint_job(completed_job).await.unwrap();
    let released = coordinator
        .get(lease.engagement_id().clone())
        .await
        .unwrap()
        .unwrap();
    assert!(
        released.lease_owner.is_none(),
        "the last terminal job checkpoint releases the inherited lease"
    );
}
