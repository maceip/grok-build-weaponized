use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tempfile::TempDir;

use crate::{
    ActionIdentity, ActionReplayPolicy, ActionResolution, ActionSpec, ActionStatus,
    EngagementCheckpoint, EngagementCoordinator, EngagementStage, EngagementStatus,
    EngagementStore, JobCheckpoint, JobKind, JobLifecycle, NewEngagement, QueuePriority,
    SubscriptionItem,
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
fn overload_rejects_before_acceptance_without_dropping_already_accepted_work() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("engagements.sqlite");
    let mut store = EngagementStore::open_with_test_capacity(&path, 2).unwrap();
    let first = store
        .accept(new_engagement("session-capacity-a", "prompt-a"))
        .unwrap()
        .value
        .record;
    let second = store
        .accept(new_engagement("session-capacity-b", "prompt-b"))
        .unwrap()
        .value
        .record;
    let rejected = store.accept(new_engagement("session-capacity-c", "prompt-c"));
    assert!(matches!(
        rejected,
        Err(crate::EngagementError::AdmissionOverloaded { capacity: 2 })
    ));
    assert_eq!(store.queue_depth().unwrap(), 2);
    assert_eq!(store.list(None, 0, 10).unwrap().len(), 2);
    assert_eq!(
        store
            .events_after(&first.engagement_id, None, 10)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store
            .events_after(&second.engagement_id, None, 10)
            .unwrap()
            .len(),
        1
    );
    drop(store);

    let reopened = EngagementStore::open(&path).unwrap();
    assert_eq!(reopened.queue_depth().unwrap(), 2);
    assert!(
        reopened
            .list(None, 0, 10)
            .unwrap()
            .iter()
            .all(|record| record.status == EngagementStatus::Queued)
    );
}

#[test]
fn legacy_engagement_tables_migrate_in_place_without_losing_events() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("engagements.sqlite");
    let engagement_id = {
        let mut store = EngagementStore::open(&path).unwrap();
        store
            .accept(new_engagement("session-legacy", "prompt-legacy"))
            .unwrap()
            .value
            .record
            .engagement_id
    };
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "PRAGMA foreign_keys=OFF;
             ALTER TABLE events RENAME TO engagement_events;
             ALTER TABLE snapshots RENAME TO engagement_snapshots;
             ALTER TABLE actions RENAME TO engagement_actions;
             ALTER TABLE job_checkpoints RENAME TO engagement_jobs;
             DROP TABLE pending_stages;
             DROP TABLE stages;
             DROP TABLE tasks;
             DROP TABLE leases;
             UPDATE engagement_meta SET value='1' WHERE key='schema_version';",
        )
        .unwrap();
    drop(connection);

    let reopened = EngagementStore::open(&path).unwrap();
    let record = reopened.get(&engagement_id).unwrap().unwrap();
    assert_eq!(record.status, EngagementStatus::Queued);
    let events = reopened.events_after(&engagement_id, None, 10).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, "engagement_accepted");
    assert_eq!(reopened.queue_depth().unwrap(), 1);
}

#[test]
fn durable_queue_resumes_suspended_execution_before_new_work_and_optional_review() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("engagements.sqlite");
    let (executor_id, ordinary_id, reviewer_id) = {
        let mut store = EngagementStore::open(&path).unwrap();

        let executor = store
            .accept(new_engagement("session-executor", "prompt-executor"))
            .unwrap()
            .value
            .record;
        let executor_lease = store
            .claim(&executor.engagement_id, "worker", 60_000, false)
            .unwrap()
            .value
            .unwrap();
        store
            .transition(
                &executor.engagement_id,
                executor_lease.lease_epoch,
                EngagementStatus::Executing,
                Some(EngagementStage::Executor),
                EngagementCheckpoint {
                    task_id: Some("active-task".to_string()),
                    ..EngagementCheckpoint::default()
                },
                serde_json::json!({}),
            )
            .unwrap();
        store
            .transition(
                &executor.engagement_id,
                executor_lease.lease_epoch,
                EngagementStatus::Suspended,
                Some(EngagementStage::Executor),
                EngagementCheckpoint {
                    task_id: Some("active-task".to_string()),
                    ..EngagementCheckpoint::default()
                },
                serde_json::json!({"reason": "restart"}),
            )
            .unwrap();

        let ordinary = store
            .accept(new_engagement("session-ordinary", "prompt-ordinary"))
            .unwrap()
            .value
            .record;

        let reviewer = store
            .accept(new_engagement("session-reviewer", "prompt-reviewer"))
            .unwrap()
            .value
            .record;
        let reviewer_lease = store
            .claim(&reviewer.engagement_id, "worker", 60_000, false)
            .unwrap()
            .value
            .unwrap();
        store
            .transition(
                &reviewer.engagement_id,
                reviewer_lease.lease_epoch,
                EngagementStatus::Executing,
                Some(EngagementStage::Executor),
                EngagementCheckpoint::default(),
                serde_json::json!({}),
            )
            .unwrap();
        store
            .transition(
                &reviewer.engagement_id,
                reviewer_lease.lease_epoch,
                EngagementStatus::Reviewing,
                Some(EngagementStage::Reviewer),
                EngagementCheckpoint::default(),
                serde_json::json!({}),
            )
            .unwrap();
        store
            .transition(
                &reviewer.engagement_id,
                reviewer_lease.lease_epoch,
                EngagementStatus::Suspended,
                Some(EngagementStage::Reviewer),
                EngagementCheckpoint::default(),
                serde_json::json!({"reason": "optional review deferred"}),
            )
            .unwrap();
        (
            executor.engagement_id,
            ordinary.engagement_id,
            reviewer.engagement_id,
        )
    };

    let mut reopened = EngagementStore::open(&path).unwrap();
    assert_eq!(reopened.queue_depth().unwrap(), 3);
    let first = reopened
        .claim_next("recovery-worker", 60_000)
        .unwrap()
        .value
        .unwrap();
    assert_eq!(first.engagement_id, executor_id);
    assert_eq!(first.status, EngagementStatus::Suspended);
    let second = reopened
        .claim_next("interactive-worker", 60_000)
        .unwrap()
        .value
        .unwrap();
    assert_eq!(second.engagement_id, ordinary_id);
    let third = reopened
        .claim_next("review-worker", 60_000)
        .unwrap()
        .value
        .unwrap();
    assert_eq!(third.engagement_id, reviewer_id);
    assert_eq!(third.stage, Some(EngagementStage::Reviewer));
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
            "UPDATE events SET payload_json='{}'
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
            "UPDATE events SET stage='direct'
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
        replay_policy: crate::ActionReplayPolicy::NonIdempotent,
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

#[test]
fn recovery_replays_safe_actions_with_the_same_identity_and_never_repeats_committed_work() {
    let directory = TempDir::new().unwrap();
    let mut store = EngagementStore::open(&directory.path().join("state.sqlite")).unwrap();
    let accepted = store
        .accept(new_engagement("session-replay", "prompt-replay"))
        .unwrap()
        .value
        .record;
    let first = store
        .claim(&accepted.engagement_id, "worker-a", 60_000, false)
        .unwrap()
        .value
        .unwrap();
    let specs = [
        ("read", ActionReplayPolicy::ReadOnly),
        ("put", ActionReplayPolicy::Idempotent),
        ("complete", ActionReplayPolicy::Idempotent),
    ]
    .map(|(action_id, replay_policy)| ActionSpec {
        identity: ActionIdentity {
            plan_revision: 2,
            task_id: "task-a".to_string(),
            action_id: action_id.to_string(),
        },
        action_kind: "test".to_string(),
        command_hash: format!("hash-{action_id}"),
        replay_policy,
        payload: None,
    });
    let mut prepared = Vec::new();
    for spec in &specs {
        let action = store
            .prepare_action(&accepted.engagement_id, first.lease_epoch, spec.clone())
            .unwrap()
            .value;
        store
            .update_action(
                &accepted.engagement_id,
                first.lease_epoch,
                &action.stable_key,
                ActionStatus::Dispatched,
                None,
                None,
                None,
            )
            .unwrap();
        prepared.push(action);
    }
    store
        .update_action(
            &accepted.engagement_id,
            first.lease_epoch,
            &prepared[2].stable_key,
            ActionStatus::Observed,
            None,
            Some("evidence-complete"),
            Some(&serde_json::json!({"done": true})),
        )
        .unwrap();
    store
        .update_action(
            &accepted.engagement_id,
            first.lease_epoch,
            &prepared[2].stable_key,
            ActionStatus::Committed,
            None,
            Some("evidence-complete"),
            Some(&serde_json::json!({"done": true})),
        )
        .unwrap();

    let recovery = store
        .recover_expired(first.lease_expires_at_ms.unwrap())
        .unwrap();
    assert_eq!(recovery.replayable_actions, 2);
    assert_eq!(recovery.ambiguous_actions, 0);
    assert_eq!(recovery.requeued, 1);
    assert_eq!(recovery.parked, 0);

    let second = store
        .claim(&accepted.engagement_id, "worker-b", 60_000, false)
        .unwrap()
        .value
        .unwrap();
    for (index, spec) in specs.iter().enumerate() {
        let recovered = store
            .prepare_action(&accepted.engagement_id, second.lease_epoch, spec.clone())
            .unwrap()
            .value;
        assert_eq!(recovered.stable_key, prepared[index].stable_key);
        assert_eq!(recovered.attempt_count, 1);
        if index < 2 {
            assert_eq!(recovered.status, ActionStatus::Prepared);
        } else {
            assert_eq!(recovered.status, ActionStatus::Committed);
            assert_eq!(recovered.result, Some(serde_json::json!({"done": true})));
        }
    }
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
    assert_eq!(subscription.next().await.unwrap().seq, 0);
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
    let gap = subscription.next_item().await.unwrap();
    assert!(matches!(
        gap,
        SubscriptionItem::CursorGap(crate::CursorGap {
            expected_seq: 1,
            durable_through: Some(9),
            ..
        })
    ));
    let mut sequences = vec![0];
    for _ in 0..9 {
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
        process_start_identity: Some("test-process-birth".to_string()),
        process_started_at_ms: Some(now_ms()),
        process_group_id: Some(42),
        stdout_artifact: Some(directory.path().join("stdout")),
        stderr_artifact: Some(directory.path().join("stderr")),
        status_artifact: None,
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

#[test]
fn normalized_schema_and_checkpoint_cover_every_durable_coordinate() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("state.sqlite");
    let mut store = EngagementStore::open(&path).unwrap();
    let accepted = store
        .accept(new_engagement("session-schema", "prompt-schema"))
        .unwrap()
        .value
        .record;
    let lease = store
        .claim(&accepted.engagement_id, "worker-schema", 60_000, false)
        .unwrap()
        .value
        .unwrap();
    let plan = serde_json::json!({
        "tasks": [
            {"task_id": "task-a", "objective": "first"},
            {"task_id": "task-b", "objective": "second"}
        ]
    });
    store
        .transition(
            &lease.engagement_id,
            lease.lease_epoch,
            EngagementStatus::Executing,
            Some(EngagementStage::Executor),
            EngagementCheckpoint {
                plan: Some(plan),
                plan_revision: 7,
                task_id: Some("task-a".to_string()),
                task_count: 2,
                completion_tests: vec!["result observed".to_string()],
                evidence_ids: vec!["evidence-a".to_string()],
                ..EngagementCheckpoint::default()
            },
            serde_json::json!({"task_id": "task-a"}),
        )
        .unwrap();
    store
        .record_runtime_admission(
            &lease.engagement_id,
            lease.lease_epoch,
            crate::RuntimeAdmissionRecord {
                request_id: "runtime-request-a".to_string(),
                model_id: "qwen".to_string(),
                adapter_id: Some("operator@v1".to_string()),
                context_plan_hash: "context-hash-a".to_string(),
            },
        )
        .unwrap();
    let action = store
        .prepare_action(
            &lease.engagement_id,
            lease.lease_epoch,
            ActionSpec {
                identity: ActionIdentity {
                    plan_revision: 7,
                    task_id: "task-a".to_string(),
                    action_id: "action-a".to_string(),
                },
                action_kind: "bash".to_string(),
                command_hash: "command-hash-a".to_string(),
                replay_policy: crate::ActionReplayPolicy::NonIdempotent,
                payload: None,
            },
        )
        .unwrap()
        .value;
    store
        .upsert_job(
            &lease.engagement_id,
            lease.lease_epoch,
            JobCheckpoint {
                job_id: "job-a".to_string(),
                engagement_id: lease.engagement_id.clone(),
                action_key: Some(action.stable_key),
                kind: JobKind::Terminal,
                lifecycle: JobLifecycle::Running,
                command_hash: "command-hash-a".to_string(),
                pid: Some(42),
                process_start_identity: Some("birth-a".to_string()),
                process_started_at_ms: Some(now_ms()),
                process_group_id: Some(42),
                stdout_artifact: Some(directory.path().join("stdout-a")),
                stderr_artifact: Some(directory.path().join("stderr-a")),
                status_artifact: Some(directory.path().join("status-a")),
                stdout_cursor: 100,
                stderr_cursor: 25,
                last_activity_at_ms: now_ms(),
                exit_code: None,
                payload: serde_json::json!({"running": true}),
            },
        )
        .unwrap();
    let checkpoint = store.get(&lease.engagement_id).unwrap().unwrap().checkpoint;
    let record = store
        .transition(
            &lease.engagement_id,
            lease.lease_epoch,
            EngagementStatus::Executing,
            Some(EngagementStage::Executor),
            checkpoint,
            serde_json::json!({"task_id": "task-a"}),
        )
        .unwrap()
        .value;
    assert_eq!(record.checkpoint.plan_revision, 7);
    assert_eq!(
        record.checkpoint.runtime_request_ids,
        vec!["runtime-request-a"]
    );
    assert_eq!(
        record.checkpoint.context_plan_hash.as_deref(),
        Some("context-hash-a")
    );
    assert_eq!(record.checkpoint.background_jobs.len(), 1);
    assert_eq!(record.checkpoint.background_jobs[0].stdout_cursor, 100);
    assert_eq!(record.checkpoint.artifact_refs.len(), 3);

    drop(store);
    let connection = rusqlite::Connection::open(&path).unwrap();
    for table in [
        "engagements",
        "stages",
        "tasks",
        "actions",
        "events",
        "leases",
        "snapshots",
        "job_checkpoints",
        "pending_stages",
    ] {
        let exists = connection
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1
                 )",
                [table],
                |row| row.get::<_, bool>(0),
            )
            .unwrap();
        assert!(exists, "required durable table `{table}` is missing");
    }
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM stages", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        4
    );
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM tasks", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        2
    );
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM leases", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
    let (task_id, action_id): (Option<String>, Option<String>) = connection
        .query_row(
            "SELECT task_id, action_id FROM events
             WHERE kind='action_prepared'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(task_id.as_deref(), Some("task-a"));
    assert_eq!(action_id.as_deref(), Some("action-a"));
}

#[test]
fn every_action_and_job_write_is_fenced_by_the_current_lease_epoch() {
    let directory = TempDir::new().unwrap();
    let mut store = EngagementStore::open(&directory.path().join("state.sqlite")).unwrap();
    let accepted = store
        .accept(new_engagement("session-fence", "prompt-fence"))
        .unwrap()
        .value
        .record;
    let first = store
        .claim(&accepted.engagement_id, "worker-a", 1, false)
        .unwrap()
        .value
        .unwrap();
    std::thread::sleep(Duration::from_millis(3));
    store.recover_expired(now_ms()).unwrap();
    let second = store
        .claim(&accepted.engagement_id, "worker-b", 60_000, false)
        .unwrap()
        .value
        .unwrap();
    assert!(second.lease_epoch > first.lease_epoch);
    let action = store.prepare_action(
        &accepted.engagement_id,
        first.lease_epoch,
        ActionSpec {
            identity: ActionIdentity {
                plan_revision: 0,
                task_id: "task".to_string(),
                action_id: "action".to_string(),
            },
            action_kind: "read".to_string(),
            command_hash: "hash".to_string(),
            replay_policy: crate::ActionReplayPolicy::ReadOnly,
            payload: None,
        },
    );
    assert!(matches!(
        action,
        Err(crate::EngagementError::StaleLease { .. })
    ));
    let job = store.upsert_job(
        &accepted.engagement_id,
        first.lease_epoch,
        JobCheckpoint {
            job_id: "stale-job".to_string(),
            engagement_id: accepted.engagement_id.clone(),
            action_key: None,
            kind: JobKind::Native,
            lifecycle: JobLifecycle::Running,
            command_hash: "hash".to_string(),
            pid: None,
            process_start_identity: None,
            process_started_at_ms: None,
            process_group_id: None,
            stdout_artifact: None,
            stderr_artifact: None,
            status_artifact: None,
            stdout_cursor: 0,
            stderr_cursor: 0,
            last_activity_at_ms: now_ms(),
            exit_code: None,
            payload: serde_json::Value::Null,
        },
    );
    assert!(matches!(
        job,
        Err(crate::EngagementError::StaleLease { .. })
    ));
}
