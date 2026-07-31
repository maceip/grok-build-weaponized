use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OptionalExtension as _, Transaction, TransactionBehavior, params};

use crate::types::{
    ActionIdentity, ActionRecord, ActionReplayPolicy, ActionResolution, ActionSpec, ActionStatus,
    ArtifactReference, BackgroundJobCursor, EngagementCheckpoint, EngagementEvent, EngagementId,
    EngagementRecord, EngagementSnapshot, EngagementStage, EngagementStatus, JobCheckpoint,
    JobKind, JobLifecycle, NewEngagement, QueuePriority, RuntimeAdmissionRecord,
};

const SCHEMA_VERSION: i64 = 4;
const MAX_CHECKPOINT_BYTES: usize = 1024 * 1024;
const MAX_EVENT_PAYLOAD_BYTES: usize = 256 * 1024;
const MAX_ACTION_PAYLOAD_BYTES: usize = 1024 * 1024;
const MAX_EVENT_PAGE: usize = 1_000;
const MAX_LIST_PAGE: usize = 512;
const MAX_ACCEPTED_NONTERMINAL_ENGAGEMENTS: usize = 4_096;

#[derive(Debug, thiserror::Error)]
pub enum EngagementError {
    #[error("engagement database: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("engagement serialization: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("engagement database setup: {0}")]
    Io(#[from] std::io::Error),
    #[error("engagement database schema {found} is newer than supported schema {supported}")]
    UnsupportedSchema { found: i64, supported: i64 },
    #[error("engagement not found: {0}")]
    NotFound(String),
    #[error("stale or expired lease for engagement {engagement_id} at epoch {lease_epoch}")]
    StaleLease {
        engagement_id: String,
        lease_epoch: u64,
    },
    #[error("invalid engagement transition: {from} -> {to}")]
    InvalidTransition { from: String, to: String },
    #[error("invalid action transition for {action_key}: {from} -> {to}")]
    InvalidActionTransition {
        action_key: String,
        from: String,
        to: String,
    },
    #[error("stable action identity {0} was reused with different action content")]
    ActionConflict(String),
    #[error("durable job identity {0} was reused with different job content")]
    JobConflict(String),
    #[error("{field} exceeds its {limit}-byte durable storage limit")]
    TooLarge { field: &'static str, limit: usize },
    #[error("invalid persisted {field} value: {value}")]
    InvalidPersistedValue { field: &'static str, value: String },
    #[error("engagement writer stopped")]
    WriterStopped,
    #[error("engagement writer queue is full (capacity {capacity})")]
    WriterOverloaded { capacity: usize },
    #[error("engagement admission rejected before acceptance: durable queue capacity {capacity}")]
    AdmissionOverloaded { capacity: usize },
}

#[derive(Clone, Debug)]
pub struct EngagementMutation<T> {
    pub value: T,
    pub event: Option<EngagementEvent>,
}

#[derive(Clone, Debug)]
pub struct AcceptOutcome {
    pub record: EngagementRecord,
    pub accepted: bool,
}

#[derive(Clone, Debug, Default)]
pub struct RecoveryReport {
    pub requeued: usize,
    pub parked: usize,
    pub replayable_actions: usize,
    pub ambiguous_actions: usize,
    pub events: Vec<EngagementEvent>,
}

pub struct EngagementStore {
    connection: Connection,
    admission_capacity: usize,
}

impl EngagementStore {
    pub fn open(path: &Path) -> Result<Self, EngagementError> {
        Self::open_with_capacity(path, MAX_ACCEPTED_NONTERMINAL_ENGAGEMENTS)
    }

    fn open_with_capacity(path: &Path, admission_capacity: usize) -> Result<Self, EngagementError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mode = xai_sqlite_journal::JournalMode::for_db_path(path);
        let mut connection = mode.open(path)?;
        connection.pragma_update(None, "foreign_keys", true)?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.pragma_update(None, "wal_autocheckpoint", 1_000)?;
        migrate_legacy_table_names(&connection)?;
        connection.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS engagement_meta(
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS engagements(
                engagement_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                prompt_id TEXT NOT NULL,
                workspace_id TEXT NOT NULL,
                user_request TEXT NOT NULL,
                status TEXT NOT NULL,
                stage TEXT,
                priority INTEGER NOT NULL,
                lease_epoch INTEGER NOT NULL DEFAULT 0,
                lease_owner TEXT,
                lease_expires_at_ms INTEGER,
                accepted_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                checkpoint_json TEXT NOT NULL,
                UNIQUE(session_id, prompt_id)
            );

            CREATE INDEX IF NOT EXISTS idx_engagement_queue
                ON engagements(status, priority DESC, accepted_at_ms ASC);
            CREATE INDEX IF NOT EXISTS idx_engagement_session
                ON engagements(session_id, accepted_at_ms DESC);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_engagement_one_active_session
                ON engagements(session_id)
                WHERE status IN ('planning', 'executing', 'reviewing', 'correcting');

            CREATE TABLE IF NOT EXISTS pending_stages(
                engagement_id TEXT PRIMARY KEY REFERENCES engagements(engagement_id) ON DELETE CASCADE,
                queue_class TEXT NOT NULL,
                priority INTEGER NOT NULL,
                status TEXT NOT NULL,
                stage TEXT,
                available_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_pending_stages_dispatch
                ON pending_stages(priority DESC, available_at_ms ASC);

            CREATE TABLE IF NOT EXISTS events(
                engagement_id TEXT NOT NULL REFERENCES engagements(engagement_id) ON DELETE CASCADE,
                seq INTEGER NOT NULL,
                event_id TEXT NOT NULL UNIQUE,
                kind TEXT NOT NULL,
                lease_epoch INTEGER NOT NULL,
                stage TEXT,
                task_id TEXT,
                action_id TEXT,
                evidence_id TEXT,
                artifact_ref TEXT,
                payload_json TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                PRIMARY KEY(engagement_id, seq)
            );

            CREATE TABLE IF NOT EXISTS snapshots(
                engagement_id TEXT PRIMARY KEY REFERENCES engagements(engagement_id) ON DELETE CASCADE,
                event_seq INTEGER NOT NULL,
                record_json TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS stages(
                engagement_id TEXT NOT NULL REFERENCES engagements(engagement_id) ON DELETE CASCADE,
                stage_seq INTEGER NOT NULL,
                status TEXT NOT NULL,
                stage TEXT,
                plan_revision INTEGER NOT NULL,
                task_id TEXT,
                task_cursor INTEGER NOT NULL,
                checkpoint_json TEXT NOT NULL,
                entered_at_ms INTEGER NOT NULL,
                PRIMARY KEY(engagement_id, stage_seq)
            );

            CREATE TABLE IF NOT EXISTS tasks(
                engagement_id TEXT NOT NULL REFERENCES engagements(engagement_id) ON DELETE CASCADE,
                plan_revision INTEGER NOT NULL,
                task_id TEXT NOT NULL,
                task_index INTEGER NOT NULL,
                status TEXT NOT NULL,
                task_json TEXT NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY(engagement_id, plan_revision, task_id)
            );
            CREATE INDEX IF NOT EXISTS idx_tasks_cursor
                ON tasks(engagement_id, plan_revision, task_index);

            CREATE TABLE IF NOT EXISTS actions(
                stable_key TEXT PRIMARY KEY,
                engagement_id TEXT NOT NULL REFERENCES engagements(engagement_id) ON DELETE CASCADE,
                plan_revision INTEGER NOT NULL,
                task_id TEXT NOT NULL,
                action_id TEXT NOT NULL,
                action_kind TEXT NOT NULL,
                command_hash TEXT NOT NULL,
                replay_policy TEXT NOT NULL DEFAULT 'non_idempotent',
                status TEXT NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                job_id TEXT,
                evidence_id TEXT,
                payload_json TEXT,
                result_json TEXT,
                prepared_at_ms INTEGER NOT NULL,
                dispatched_at_ms INTEGER,
                updated_at_ms INTEGER NOT NULL,
                UNIQUE(engagement_id, plan_revision, task_id, action_id)
            );
            CREATE INDEX IF NOT EXISTS idx_actions_status
                ON actions(engagement_id, status);

            CREATE TABLE IF NOT EXISTS leases(
                engagement_id TEXT NOT NULL REFERENCES engagements(engagement_id) ON DELETE CASCADE,
                lease_epoch INTEGER NOT NULL,
                owner TEXT NOT NULL,
                acquired_at_ms INTEGER NOT NULL,
                expires_at_ms INTEGER NOT NULL,
                last_heartbeat_at_ms INTEGER NOT NULL,
                released_at_ms INTEGER,
                release_reason TEXT,
                PRIMARY KEY(engagement_id, lease_epoch)
            );

            CREATE TABLE IF NOT EXISTS job_checkpoints(
                job_id TEXT PRIMARY KEY,
                engagement_id TEXT NOT NULL REFERENCES engagements(engagement_id) ON DELETE CASCADE,
                action_key TEXT REFERENCES actions(stable_key),
                kind TEXT NOT NULL,
                lifecycle TEXT NOT NULL,
                command_hash TEXT NOT NULL,
                pid INTEGER,
                process_start_identity TEXT,
                process_started_at_ms INTEGER,
                process_group_id INTEGER,
                stdout_artifact TEXT,
                stderr_artifact TEXT,
                status_artifact TEXT,
                stdout_cursor INTEGER NOT NULL,
                stderr_cursor INTEGER NOT NULL,
                last_activity_at_ms INTEGER NOT NULL,
                exit_code INTEGER,
                payload_json TEXT NOT NULL,
                updated_at_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_job_checkpoints_recovery
                ON job_checkpoints(lifecycle, updated_at_ms);
            "#,
        )?;
        ensure_column(&connection, "events", "task_id", "TEXT")?;
        ensure_column(&connection, "events", "action_id", "TEXT")?;
        ensure_column(&connection, "events", "evidence_id", "TEXT")?;
        ensure_column(&connection, "events", "artifact_ref", "TEXT")?;
        ensure_column(
            &connection,
            "actions",
            "replay_policy",
            "TEXT NOT NULL DEFAULT 'non_idempotent'",
        )?;
        ensure_column(
            &connection,
            "job_checkpoints",
            "process_start_identity",
            "TEXT",
        )?;
        ensure_column(&connection, "job_checkpoints", "status_artifact", "TEXT")?;
        let stored_version = connection
            .query_row(
                "SELECT value FROM engagement_meta WHERE key='schema_version'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0);
        if stored_version > SCHEMA_VERSION {
            return Err(EngagementError::UnsupportedSchema {
                found: stored_version,
                supported: SCHEMA_VERSION,
            });
        }
        backfill_normalized_state(&mut connection)?;
        connection.execute(
            "INSERT INTO engagement_meta(key, value) VALUES ('schema_version', ?1)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            [SCHEMA_VERSION.to_string()],
        )?;
        Ok(Self {
            connection,
            admission_capacity,
        })
    }

    #[cfg(test)]
    pub(crate) fn open_with_test_capacity(
        path: &Path,
        admission_capacity: usize,
    ) -> Result<Self, EngagementError> {
        Self::open_with_capacity(path, admission_capacity)
    }

    pub fn accept(
        &mut self,
        input: NewEngagement,
    ) -> Result<EngagementMutation<AcceptOutcome>, EngagementError> {
        check_json_size(
            &serde_json::to_vec(&input.checkpoint)?,
            "engagement checkpoint",
            MAX_CHECKPOINT_BYTES,
        )?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(record) =
            load_by_session_prompt_tx(&transaction, &input.session_id, &input.prompt_id)?
        {
            transaction.commit()?;
            return Ok(EngagementMutation {
                value: AcceptOutcome {
                    record,
                    accepted: false,
                },
                event: None,
            });
        }
        let pending = transaction.query_row(
            "SELECT COUNT(*) FROM engagements
             WHERE status NOT IN ('completed','failed','cancelled')",
            [],
            |row| row.get::<_, i64>(0),
        )?;
        if usize::try_from(pending).unwrap_or(usize::MAX) >= self.admission_capacity {
            return Err(EngagementError::AdmissionOverloaded {
                capacity: self.admission_capacity,
            });
        }

        let now = now_ms();
        let engagement_id = EngagementId::new();
        let checkpoint_json = serde_json::to_string(&input.checkpoint)?;
        transaction.execute(
            "INSERT INTO engagements(
                engagement_id, session_id, prompt_id, workspace_id, user_request,
                status, stage, priority, lease_epoch, accepted_at_ms, updated_at_ms,
                checkpoint_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, 'queued', NULL, ?6, 0, ?7, ?7, ?8)",
            params![
                engagement_id.0,
                input.session_id,
                input.prompt_id,
                input.workspace_id,
                input.user_request,
                input.priority as i64,
                now,
                checkpoint_json,
            ],
        )?;
        sync_pending_stage_tx(
            &transaction,
            &engagement_id,
            EngagementStatus::Queued,
            None,
            QueuePriority::DurableStage,
            now,
        )?;
        let payload = serde_json::json!({
            "session_id": input.session_id,
            "prompt_id": input.prompt_id,
            "workspace_id": input.workspace_id,
            "priority": input.priority,
        });
        let event = append_event_tx(
            &transaction,
            &engagement_id,
            0,
            None,
            "engagement_accepted",
            payload,
        )?;
        let record = load_engagement_tx(&transaction, &engagement_id)?;
        record_stage_and_tasks_tx(&transaction, &record, event.seq)?;
        write_snapshot_tx(&transaction, &record, event.seq)?;
        transaction.commit()?;
        Ok(EngagementMutation {
            value: AcceptOutcome {
                record,
                accepted: true,
            },
            event: Some(event),
        })
    }

    pub fn get(
        &self,
        engagement_id: &EngagementId,
    ) -> Result<Option<EngagementRecord>, EngagementError> {
        load_engagement_conn(&self.connection, engagement_id).map_err(Into::into)
    }

    pub fn list(
        &self,
        session_id: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<EngagementRecord>, EngagementError> {
        let limit = limit.clamp(1, MAX_LIST_PAGE);
        let mut records = Vec::new();
        if let Some(session_id) = session_id {
            let mut statement = self.connection.prepare(
                "SELECT engagement_id, session_id, prompt_id, workspace_id, user_request,
                        status, stage, priority, lease_epoch, lease_owner,
                        lease_expires_at_ms, accepted_at_ms, updated_at_ms, checkpoint_json
                 FROM engagements WHERE session_id=?1
                 ORDER BY accepted_at_ms DESC LIMIT ?2 OFFSET ?3",
            )?;
            let rows = statement.query_map(
                params![session_id, limit as i64, offset as i64],
                decode_engagement_row,
            )?;
            for row in rows {
                records.push(row?);
            }
        } else {
            let mut statement = self.connection.prepare(
                "SELECT engagement_id, session_id, prompt_id, workspace_id, user_request,
                        status, stage, priority, lease_epoch, lease_owner,
                        lease_expires_at_ms, accepted_at_ms, updated_at_ms, checkpoint_json
                 FROM engagements ORDER BY accepted_at_ms DESC LIMIT ?1 OFFSET ?2",
            )?;
            let rows =
                statement.query_map(params![limit as i64, offset as i64], decode_engagement_row)?;
            for row in rows {
                records.push(row?);
            }
        }
        Ok(records)
    }

    pub fn claim_next(
        &mut self,
        owner: &str,
        ttl_ms: u64,
    ) -> Result<EngagementMutation<Option<EngagementRecord>>, EngagementError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = now_ms();
        let engagement_id = transaction
            .query_row(
                "SELECT candidate.engagement_id
                 FROM pending_stages pending
                 JOIN engagements candidate
                   ON candidate.engagement_id=pending.engagement_id
                 WHERE candidate.status IN ('queued','suspended')
                   AND pending.available_at_ms <= ?1
                   AND (candidate.lease_expires_at_ms IS NULL OR candidate.lease_expires_at_ms <= ?1)
                   AND NOT EXISTS (
                       SELECT 1 FROM engagements active
                       WHERE active.session_id=candidate.session_id
                         AND active.status IN ('planning','executing','reviewing','correcting')
                   )
                 ORDER BY pending.priority DESC, pending.available_at_ms ASC,
                          candidate.accepted_at_ms ASC
                 LIMIT 1",
                [now],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let Some(engagement_id) = engagement_id else {
            transaction.commit()?;
            return Ok(EngagementMutation {
                value: None,
                event: None,
            });
        };
        claim_tx(
            transaction,
            &EngagementId(engagement_id),
            owner,
            ttl_ms,
            false,
        )
    }

    pub fn claim(
        &mut self,
        engagement_id: &EngagementId,
        owner: &str,
        ttl_ms: u64,
        allow_parked: bool,
    ) -> Result<EngagementMutation<Option<EngagementRecord>>, EngagementError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        claim_tx(transaction, engagement_id, owner, ttl_ms, allow_parked)
    }

    pub fn heartbeat(
        &mut self,
        engagement_id: &EngagementId,
        lease_epoch: u64,
        ttl_ms: u64,
    ) -> Result<bool, EngagementError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = now_ms();
        let expires_at_ms = add_ttl(now, ttl_ms);
        let changed = transaction.execute(
            "UPDATE engagements
             SET lease_expires_at_ms=?1, updated_at_ms=?2
             WHERE engagement_id=?3 AND lease_epoch=?4
               AND lease_owner IS NOT NULL AND lease_expires_at_ms > ?2",
            params![expires_at_ms, now, engagement_id.0, as_i64(lease_epoch),],
        )?;
        if changed == 1 {
            transaction.execute(
                "UPDATE leases
                 SET expires_at_ms=?1, last_heartbeat_at_ms=?2
                 WHERE engagement_id=?3 AND lease_epoch=?4
                   AND released_at_ms IS NULL",
                params![expires_at_ms, now, engagement_id.0, as_i64(lease_epoch)],
            )?;
        }
        transaction.commit()?;
        Ok(changed == 1)
    }

    pub fn transition(
        &mut self,
        engagement_id: &EngagementId,
        lease_epoch: u64,
        status: EngagementStatus,
        stage: Option<EngagementStage>,
        mut checkpoint: EngagementCheckpoint,
        detail: serde_json::Value,
    ) -> Result<EngagementMutation<EngagementRecord>, EngagementError> {
        let detail_bytes = serde_json::to_vec(&detail)?;
        check_json_size(
            &detail_bytes,
            "engagement event payload",
            MAX_EVENT_PAYLOAD_BYTES,
        )?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = load_engagement_tx(&transaction, engagement_id)?;
        ensure_lease(&current, lease_epoch)?;
        hydrate_checkpoint_job_refs_tx(&transaction, engagement_id, &mut checkpoint)?;
        let checkpoint_bytes = serde_json::to_vec(&checkpoint)?;
        check_json_size(
            &checkpoint_bytes,
            "engagement checkpoint",
            MAX_CHECKPOINT_BYTES,
        )?;
        if !current.status.permits(status) {
            return Err(EngagementError::InvalidTransition {
                from: current.status.as_str().to_string(),
                to: status.as_str().to_string(),
            });
        }
        let now = now_ms();
        let has_active_jobs = transaction.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM job_checkpoints
                 WHERE engagement_id=?1 AND lifecycle IN ('running','quiet','stale')
             )",
            [engagement_id.0.as_str()],
            |row| row.get::<_, bool>(0),
        )?;
        // A completed interactive turn may intentionally leave background
        // execution running. The process supervisor inherits this same fenced
        // lease and keeps heartbeating it until the last durable job reaches a
        // terminal state.
        let release_lease =
            (status.is_terminal() || status == EngagementStatus::Suspended) && !has_active_jobs;
        let next_priority = queue_priority_for(status, stage, current.priority);
        let changed = transaction.execute(
            "UPDATE engagements
             SET status=?1, stage=?2, checkpoint_json=?3, priority=?4, updated_at_ms=?5,
                 lease_owner=CASE WHEN ?6 THEN NULL ELSE lease_owner END,
                 lease_expires_at_ms=CASE WHEN ?6 THEN NULL ELSE lease_expires_at_ms END
             WHERE engagement_id=?7 AND lease_epoch=?8
               AND lease_owner IS NOT NULL AND lease_expires_at_ms > ?5",
            params![
                status.as_str(),
                stage.map(EngagementStage::as_str),
                String::from_utf8(checkpoint_bytes).expect("JSON is UTF-8"),
                next_priority as i64,
                now,
                release_lease,
                engagement_id.0,
                as_i64(lease_epoch),
            ],
        )?;
        if changed != 1 {
            return Err(EngagementError::StaleLease {
                engagement_id: engagement_id.0.clone(),
                lease_epoch,
            });
        }
        sync_pending_stage_tx(
            &transaction,
            engagement_id,
            status,
            stage,
            next_priority,
            now,
        )?;
        if release_lease {
            release_lease_tx(
                &transaction,
                engagement_id,
                lease_epoch,
                now,
                status.as_str(),
            )?;
        }
        let payload = serde_json::json!({
            "from": current.status,
            "to": status,
            "stage": stage,
            "detail": detail,
        });
        let event = append_event_tx(
            &transaction,
            engagement_id,
            lease_epoch,
            stage,
            "engagement_transitioned",
            payload,
        )?;
        let record = load_engagement_tx(&transaction, engagement_id)?;
        record_stage_and_tasks_tx(&transaction, &record, event.seq)?;
        write_snapshot_tx(&transaction, &record, event.seq)?;
        transaction.commit()?;
        Ok(EngagementMutation {
            value: record,
            event: Some(event),
        })
    }

    pub fn record_runtime_admission(
        &mut self,
        engagement_id: &EngagementId,
        lease_epoch: u64,
        admission: RuntimeAdmissionRecord,
    ) -> Result<EngagementMutation<EngagementRecord>, EngagementError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = load_engagement_tx(&transaction, engagement_id)?;
        ensure_lease(&current, lease_epoch)?;
        fence_lease_tx(&transaction, engagement_id, lease_epoch)?;
        let mut checkpoint = current.checkpoint;
        checkpoint.runtime_request_id = Some(admission.request_id.clone());
        if !checkpoint
            .runtime_request_ids
            .iter()
            .any(|request_id| request_id == &admission.request_id)
        {
            checkpoint
                .runtime_request_ids
                .push(admission.request_id.clone());
        }
        checkpoint.model_id = Some(admission.model_id.clone());
        checkpoint.adapter_id = admission.adapter_id.clone();
        checkpoint.context_plan_hash = Some(admission.context_plan_hash.clone());
        hydrate_checkpoint_job_refs_tx(&transaction, engagement_id, &mut checkpoint)?;
        let checkpoint_json = serde_json::to_string(&checkpoint)?;
        check_json_size(
            checkpoint_json.as_bytes(),
            "engagement checkpoint",
            MAX_CHECKPOINT_BYTES,
        )?;
        let now = now_ms();
        let changed = transaction.execute(
            "UPDATE engagements
             SET checkpoint_json=?1, updated_at_ms=?2
             WHERE engagement_id=?3 AND lease_epoch=?4
               AND lease_owner IS NOT NULL AND lease_expires_at_ms > ?2",
            params![checkpoint_json, now, engagement_id.0, as_i64(lease_epoch)],
        )?;
        if changed != 1 {
            return Err(EngagementError::StaleLease {
                engagement_id: engagement_id.0.clone(),
                lease_epoch,
            });
        }
        let event = append_event_tx(
            &transaction,
            engagement_id,
            lease_epoch,
            current.stage,
            "runtime_request_admitted",
            serde_json::to_value(admission)?,
        )?;
        let record = load_engagement_tx(&transaction, engagement_id)?;
        record_stage_and_tasks_tx(&transaction, &record, event.seq)?;
        write_snapshot_tx(&transaction, &record, event.seq)?;
        transaction.commit()?;
        Ok(EngagementMutation {
            value: record,
            event: Some(event),
        })
    }

    pub fn suspend_preserving_checkpoint(
        &mut self,
        engagement_id: &EngagementId,
        lease_epoch: u64,
        reason: String,
    ) -> Result<EngagementMutation<Option<EngagementRecord>>, EngagementError> {
        let Some(current) = self.get(engagement_id)? else {
            return Ok(EngagementMutation {
                value: None,
                event: None,
            });
        };
        if current.status.is_terminal() || current.status == EngagementStatus::Suspended {
            return Ok(EngagementMutation {
                value: Some(current),
                event: None,
            });
        }
        match self.transition(
            engagement_id,
            lease_epoch,
            EngagementStatus::Suspended,
            current.stage,
            current.checkpoint,
            serde_json::json!({"reason": reason}),
        ) {
            Ok(mutation) => Ok(EngagementMutation {
                value: Some(mutation.value),
                event: mutation.event,
            }),
            Err(EngagementError::StaleLease { .. }) => Ok(EngagementMutation {
                value: None,
                event: None,
            }),
            Err(error) => Err(error),
        }
    }

    pub fn prepare_action(
        &mut self,
        engagement_id: &EngagementId,
        lease_epoch: u64,
        spec: ActionSpec,
    ) -> Result<EngagementMutation<ActionRecord>, EngagementError> {
        if let Some(payload) = spec.payload.as_ref() {
            check_json_size(
                &serde_json::to_vec(payload)?,
                "action payload",
                MAX_ACTION_PAYLOAD_BYTES,
            )?;
        }
        let stable_key = spec.identity.stable_key(engagement_id);
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let engagement = load_engagement_tx(&transaction, engagement_id)?;
        ensure_lease(&engagement, lease_epoch)?;
        fence_lease_tx(&transaction, engagement_id, lease_epoch)?;
        if let Some(existing) = load_action_tx(&transaction, &stable_key)? {
            if existing.command_hash != spec.command_hash
                || existing.action_kind != spec.action_kind
                || existing.identity != spec.identity
                || existing.replay_policy != spec.replay_policy
            {
                return Err(EngagementError::ActionConflict(stable_key));
            }
            transaction.commit()?;
            return Ok(EngagementMutation {
                value: existing,
                event: None,
            });
        }
        let now = now_ms();
        transaction.execute(
            "INSERT INTO actions(
                stable_key, engagement_id, plan_revision, task_id, action_id,
                action_kind, command_hash, replay_policy, status, attempt_count,
                payload_json, prepared_at_ms, updated_at_ms
             ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'prepared', 0, ?9, ?10, ?10
             )",
            params![
                stable_key,
                engagement_id.0,
                i64::from(spec.identity.plan_revision),
                spec.identity.task_id,
                spec.identity.action_id,
                spec.action_kind,
                spec.command_hash,
                spec.replay_policy.as_str(),
                optional_json(spec.payload.as_ref())?,
                now,
            ],
        )?;
        let payload = serde_json::json!({
            "action_key": stable_key,
            "identity": spec.identity,
            "action_kind": spec.action_kind,
            "command_hash": spec.command_hash,
            "replay_policy": spec.replay_policy,
            "status": ActionStatus::Prepared,
        });
        let event = append_event_tx(
            &transaction,
            engagement_id,
            lease_epoch,
            engagement.stage,
            "action_prepared",
            payload,
        )?;
        let action = load_action_tx(&transaction, &stable_key)?
            .ok_or_else(|| EngagementError::NotFound(stable_key.clone()))?;
        let engagement = load_engagement_tx(&transaction, engagement_id)?;
        write_snapshot_tx(&transaction, &engagement, event.seq)?;
        transaction.commit()?;
        Ok(EngagementMutation {
            value: action,
            event: Some(event),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn update_action(
        &mut self,
        engagement_id: &EngagementId,
        lease_epoch: u64,
        action_key: &str,
        status: ActionStatus,
        job_id: Option<&str>,
        evidence_id: Option<&str>,
        result: Option<&serde_json::Value>,
    ) -> Result<EngagementMutation<ActionRecord>, EngagementError> {
        if let Some(result) = result {
            check_json_size(
                &serde_json::to_vec(result)?,
                "action result",
                MAX_ACTION_PAYLOAD_BYTES,
            )?;
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let engagement = load_engagement_tx(&transaction, engagement_id)?;
        ensure_lease(&engagement, lease_epoch)?;
        fence_lease_tx(&transaction, engagement_id, lease_epoch)?;
        let current = load_action_tx(&transaction, action_key)?
            .ok_or_else(|| EngagementError::NotFound(action_key.to_string()))?;
        if current.engagement_id != *engagement_id {
            return Err(EngagementError::NotFound(action_key.to_string()));
        }
        if !current.status.permits(status) {
            return Err(EngagementError::InvalidActionTransition {
                action_key: action_key.to_string(),
                from: current.status.as_str().to_string(),
                to: status.as_str().to_string(),
            });
        }
        let now = now_ms();
        let increment_attempt =
            status == ActionStatus::Dispatched && current.status != ActionStatus::Dispatched;
        transaction.execute(
            "UPDATE actions
             SET status=?1,
                 attempt_count=attempt_count + CASE WHEN ?7 THEN 1 ELSE 0 END,
                 job_id=COALESCE(?2, job_id),
                 evidence_id=COALESCE(?3, evidence_id),
                 result_json=COALESCE(?4, result_json),
                 dispatched_at_ms=CASE
                     WHEN ?1='dispatched' THEN COALESCE(dispatched_at_ms, ?5)
                     ELSE dispatched_at_ms
                 END,
                 updated_at_ms=?5
             WHERE stable_key=?6",
            params![
                status.as_str(),
                job_id,
                evidence_id,
                optional_json(result)?,
                now,
                action_key,
                increment_attempt,
            ],
        )?;
        let payload = serde_json::json!({
            "action_key": action_key,
            "from": current.status,
            "to": status,
            "job_id": job_id,
            "evidence_id": evidence_id,
        });
        let event = append_event_tx(
            &transaction,
            engagement_id,
            lease_epoch,
            engagement.stage,
            "action_transitioned",
            payload,
        )?;
        let action = load_action_tx(&transaction, action_key)?
            .ok_or_else(|| EngagementError::NotFound(action_key.to_string()))?;
        write_snapshot_tx(&transaction, &engagement, event.seq)?;
        transaction.commit()?;
        Ok(EngagementMutation {
            value: action,
            event: Some(event),
        })
    }

    pub fn resolve_ambiguous_action(
        &mut self,
        engagement_id: &EngagementId,
        lease_epoch: u64,
        action_key: &str,
        resolution: ActionResolution,
    ) -> Result<EngagementMutation<ActionRecord>, EngagementError> {
        let (status, result, evidence_id) = match &resolution {
            ActionResolution::RetryPrepared => (ActionStatus::Prepared, None, None),
            ActionResolution::Observed {
                result,
                evidence_id,
            } => (ActionStatus::Observed, Some(result), evidence_id.as_deref()),
            ActionResolution::Committed {
                result,
                evidence_id,
            } => (
                ActionStatus::Committed,
                Some(result),
                evidence_id.as_deref(),
            ),
        };
        self.update_action(
            engagement_id,
            lease_epoch,
            action_key,
            status,
            None,
            evidence_id,
            result,
        )
    }

    pub fn action(&self, action_key: &str) -> Result<Option<ActionRecord>, EngagementError> {
        load_action_conn(&self.connection, action_key).map_err(Into::into)
    }

    pub fn actions(
        &self,
        engagement_id: &EngagementId,
    ) -> Result<Vec<ActionRecord>, EngagementError> {
        let mut statement = self.connection.prepare(
            "SELECT stable_key, engagement_id, plan_revision, task_id, action_id,
                    action_kind, command_hash, replay_policy, status, attempt_count,
                    job_id, evidence_id, payload_json, result_json, prepared_at_ms,
                    dispatched_at_ms, updated_at_ms
             FROM actions WHERE engagement_id=?1
             ORDER BY prepared_at_ms, stable_key",
        )?;
        let rows = statement.query_map([engagement_id.0.as_str()], decode_action_row)?;
        let mut actions = Vec::new();
        for row in rows {
            actions.push(row?);
        }
        Ok(actions)
    }

    pub fn upsert_job(
        &mut self,
        engagement_id: &EngagementId,
        lease_epoch: u64,
        checkpoint: JobCheckpoint,
    ) -> Result<EngagementMutation<JobCheckpoint>, EngagementError> {
        check_json_size(
            &serde_json::to_vec(&checkpoint.payload)?,
            "job payload",
            MAX_ACTION_PAYLOAD_BYTES,
        )?;
        if checkpoint.engagement_id != *engagement_id {
            return Err(EngagementError::NotFound(checkpoint.job_id));
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let engagement = load_engagement_tx(&transaction, engagement_id)?;
        ensure_lease(&engagement, lease_epoch)?;
        fence_lease_tx(&transaction, engagement_id, lease_epoch)?;
        if let Some(existing) = load_job_tx(&transaction, &checkpoint.job_id)?
            && (existing.engagement_id != checkpoint.engagement_id
                || existing.command_hash != checkpoint.command_hash
                || existing.kind != checkpoint.kind)
        {
            return Err(EngagementError::JobConflict(checkpoint.job_id));
        }
        let now = now_ms();
        transaction.execute(
            "INSERT INTO job_checkpoints(
                job_id, engagement_id, action_key, kind, lifecycle, command_hash,
                pid, process_start_identity, process_started_at_ms, process_group_id, stdout_artifact,
                stderr_artifact, status_artifact, stdout_cursor, stderr_cursor,
                last_activity_at_ms, exit_code, payload_json, updated_at_ms
             ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
                ?14, ?15, ?16, ?17, ?18, ?19
             )
             ON CONFLICT(job_id) DO UPDATE SET
                action_key=excluded.action_key,
                lifecycle=excluded.lifecycle,
                pid=excluded.pid,
                process_start_identity=excluded.process_start_identity,
                process_started_at_ms=excluded.process_started_at_ms,
                process_group_id=excluded.process_group_id,
                stdout_artifact=excluded.stdout_artifact,
                stderr_artifact=excluded.stderr_artifact,
                status_artifact=excluded.status_artifact,
                stdout_cursor=excluded.stdout_cursor,
                stderr_cursor=excluded.stderr_cursor,
                last_activity_at_ms=excluded.last_activity_at_ms,
                exit_code=excluded.exit_code,
                payload_json=excluded.payload_json,
                updated_at_ms=excluded.updated_at_ms
             WHERE job_checkpoints.engagement_id=excluded.engagement_id
               AND job_checkpoints.command_hash=excluded.command_hash",
            params![
                checkpoint.job_id,
                engagement_id.0,
                checkpoint.action_key,
                checkpoint.kind.as_str(),
                checkpoint.lifecycle.as_str(),
                checkpoint.command_hash,
                checkpoint.pid.map(i64::from),
                checkpoint.process_start_identity,
                checkpoint.process_started_at_ms,
                checkpoint.process_group_id,
                checkpoint
                    .stdout_artifact
                    .as_ref()
                    .map(|path| path.to_string_lossy().into_owned()),
                checkpoint
                    .stderr_artifact
                    .as_ref()
                    .map(|path| path.to_string_lossy().into_owned()),
                checkpoint
                    .status_artifact
                    .as_ref()
                    .map(|path| path.to_string_lossy().into_owned()),
                as_i64(checkpoint.stdout_cursor),
                as_i64(checkpoint.stderr_cursor),
                checkpoint.last_activity_at_ms,
                checkpoint.exit_code,
                serde_json::to_string(&checkpoint.payload)?,
                now,
            ],
        )?;
        if matches!(
            checkpoint.lifecycle,
            JobLifecycle::Completed
                | JobLifecycle::Failed
                | JobLifecycle::Cancelled
                | JobLifecycle::Lost
        ) {
            transaction.execute(
                "UPDATE engagements
                 SET lease_owner=NULL, lease_expires_at_ms=NULL, updated_at_ms=?1
                 WHERE engagement_id=?2
                   AND status IN ('suspended','failed','parked','cancelled','completed')
                   AND NOT EXISTS(
                       SELECT 1 FROM job_checkpoints
                       WHERE engagement_id=?2
                         AND lifecycle IN ('running','quiet','stale')
                   )",
                params![now, engagement_id.0],
            )?;
            let released = transaction.query_row(
                "SELECT lease_owner IS NULL FROM engagements WHERE engagement_id=?1",
                [engagement_id.0.as_str()],
                |row| row.get::<_, bool>(0),
            )?;
            if released {
                release_lease_tx(
                    &transaction,
                    engagement_id,
                    lease_epoch,
                    now,
                    checkpoint.lifecycle.as_str(),
                )?;
            }
        }
        let payload = serde_json::json!({
            "job_id": checkpoint.job_id,
            "kind": checkpoint.kind,
            "lifecycle": checkpoint.lifecycle,
            "action_key": checkpoint.action_key,
            "stdout_artifact": checkpoint.stdout_artifact,
            "stderr_artifact": checkpoint.stderr_artifact,
            "status_artifact": checkpoint.status_artifact,
            "stdout_cursor": checkpoint.stdout_cursor,
            "stderr_cursor": checkpoint.stderr_cursor,
            "exit_code": checkpoint.exit_code,
        });
        let event = append_event_tx(
            &transaction,
            engagement_id,
            lease_epoch,
            engagement.stage,
            "job_checkpointed",
            payload,
        )?;
        let stored = load_job_tx(&transaction, &checkpoint.job_id)?
            .ok_or_else(|| EngagementError::NotFound(checkpoint.job_id.clone()))?;
        let engagement = load_engagement_tx(&transaction, engagement_id)?;
        write_snapshot_tx(&transaction, &engagement, event.seq)?;
        transaction.commit()?;
        Ok(EngagementMutation {
            value: stored,
            event: Some(event),
        })
    }

    pub fn recoverable_jobs(&self) -> Result<Vec<JobCheckpoint>, EngagementError> {
        let mut statement = self.connection.prepare(
            "SELECT job_id, engagement_id, action_key, kind, lifecycle, command_hash,
                    pid, process_start_identity, process_started_at_ms, process_group_id, stdout_artifact,
                    stderr_artifact, status_artifact, stdout_cursor, stderr_cursor,
                    last_activity_at_ms, exit_code, payload_json
             FROM job_checkpoints
             WHERE lifecycle IN ('running','quiet','stale')
             ORDER BY updated_at_ms",
        )?;
        let rows = statement.query_map([], decode_job_row)?;
        let mut jobs = Vec::new();
        for row in rows {
            jobs.push(row?);
        }
        Ok(jobs)
    }

    pub fn job(&self, job_id: &str) -> Result<Option<JobCheckpoint>, EngagementError> {
        load_job_conn(&self.connection, job_id).map_err(Into::into)
    }

    pub fn events_after(
        &self,
        engagement_id: &EngagementId,
        after_seq: Option<u64>,
        limit: usize,
    ) -> Result<Vec<EngagementEvent>, EngagementError> {
        let after_seq = after_seq.map_or(-1, as_i64);
        let limit = limit.clamp(1, MAX_EVENT_PAGE);
        let mut statement = self.connection.prepare(
            "SELECT engagement_id, seq, event_id, kind, lease_epoch, stage,
                    task_id, action_id, evidence_id, artifact_ref,
                    payload_json, content_hash, created_at_ms
             FROM events
             WHERE engagement_id=?1 AND seq>?2
             ORDER BY seq ASC LIMIT ?3",
        )?;
        let rows = statement.query_map(
            params![engagement_id.0, after_seq, limit as i64],
            decode_event_row,
        )?;
        let mut events = Vec::new();
        for row in rows {
            events.push(row?);
        }
        Ok(events)
    }

    pub fn latest_snapshot(
        &self,
        engagement_id: &EngagementId,
    ) -> Result<Option<EngagementSnapshot>, EngagementError> {
        self.connection
            .query_row(
                "SELECT event_seq, record_json, created_at_ms
                 FROM snapshots WHERE engagement_id=?1",
                [engagement_id.0.as_str()],
                |row| {
                    let record_json: String = row.get(1)?;
                    let record = serde_json::from_str(&record_json).map_err(sql_decode_error)?;
                    Ok(EngagementSnapshot {
                        engagement_id: engagement_id.clone(),
                        event_seq: as_u64(row.get::<_, i64>(0)?),
                        record,
                        created_at_ms: row.get(2)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn recover_expired(&mut self, at_ms: i64) -> Result<RecoveryReport, EngagementError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let expired = {
            let mut statement = transaction.prepare(
                "SELECT engagement_id, lease_epoch, status, stage
                 FROM engagements
                 WHERE lease_owner IS NOT NULL
                   AND lease_expires_at_ms IS NOT NULL
                   AND lease_expires_at_ms <= ?1
                   AND status NOT IN ('completed','failed','cancelled')",
            )?;
            let rows = statement.query_map([at_ms], |row| {
                let status_value: String = row.get(2)?;
                let stage_value: Option<String> = row.get(3)?;
                Ok((
                    EngagementId(row.get(0)?),
                    as_u64(row.get::<_, i64>(1)?),
                    parse_status(status_value)?,
                    stage_value.map(parse_stage).transpose()?,
                ))
            })?;
            let mut expired = Vec::new();
            for row in rows {
                expired.push(row?);
            }
            expired
        };

        let mut report = RecoveryReport::default();
        for (engagement_id, lease_epoch, prior_status, stage) in expired {
            let replayable = transaction.execute(
                "UPDATE actions
                 SET status='prepared', updated_at_ms=?1
                 WHERE engagement_id=?2
                   AND status='dispatched'
                   AND replay_policy IN ('read_only','idempotent')",
                params![at_ms, engagement_id.0],
            )?;
            let ambiguous = transaction.execute(
                "UPDATE actions
                 SET status='ambiguous', updated_at_ms=?1
                 WHERE engagement_id=?2
                   AND status='dispatched'
                   AND replay_policy='non_idempotent'",
                params![at_ms, engagement_id.0],
            )?;
            report.replayable_actions = report.replayable_actions.saturating_add(replayable);
            report.ambiguous_actions = report.ambiguous_actions.saturating_add(ambiguous);
            let next_status = if ambiguous > 0 {
                report.parked += 1;
                EngagementStatus::Parked
            } else {
                report.requeued += 1;
                EngagementStatus::Queued
            };
            transaction.execute(
                "UPDATE engagements
                 SET status=?1, lease_owner=NULL, lease_expires_at_ms=NULL, updated_at_ms=?2
                 WHERE engagement_id=?3 AND lease_epoch=?4",
                params![
                    next_status.as_str(),
                    at_ms,
                    engagement_id.0,
                    as_i64(lease_epoch),
                ],
            )?;
            sync_pending_stage_tx(
                &transaction,
                &engagement_id,
                next_status,
                stage,
                queue_priority_for(next_status, stage, QueuePriority::DurableStage),
                at_ms,
            )?;
            release_lease_tx(
                &transaction,
                &engagement_id,
                lease_epoch,
                at_ms,
                if ambiguous > 0 {
                    "ambiguous_action"
                } else {
                    "expired"
                },
            )?;
            let event = append_event_tx(
                &transaction,
                &engagement_id,
                lease_epoch,
                stage,
                if ambiguous > 0 {
                    "engagement_parked_ambiguous_action"
                } else {
                    "engagement_requeued_expired_lease"
                },
                serde_json::json!({
                    "prior_status": prior_status,
                    "replayable_actions": replayable,
                    "ambiguous_actions": ambiguous,
                }),
            )?;
            let record = load_engagement_tx(&transaction, &engagement_id)?;
            write_snapshot_tx(&transaction, &record, event.seq)?;
            report.events.push(event);
        }
        transaction.commit()?;
        Ok(report)
    }

    pub fn queue_depth(&self) -> Result<usize, EngagementError> {
        let count =
            self.connection
                .query_row("SELECT COUNT(*) FROM pending_stages", [], |row| {
                    row.get::<_, i64>(0)
                })?;
        Ok(usize::try_from(count).unwrap_or(usize::MAX))
    }
}

fn backfill_normalized_state(connection: &mut Connection) -> Result<(), EngagementError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute_batch(
        "INSERT OR IGNORE INTO pending_stages(
             engagement_id, queue_class, priority, status, stage,
             available_at_ms, updated_at_ms
         )
         SELECT engagement_id,
                CASE
                    WHEN stage IN ('executor','correction')
                        THEN 'active_execution_continuation'
                    WHEN stage='reviewer' THEN 'optional_reviewer'
                    ELSE 'durable_pending_stage'
                END,
                CASE
                    WHEN stage IN ('executor','correction') THEN 40
                    WHEN stage='reviewer' THEN 20
                    ELSE 35
                END,
                status, stage, updated_at_ms, updated_at_ms
         FROM engagements
         WHERE status IN ('queued','suspended');

         INSERT OR IGNORE INTO leases(
             engagement_id, lease_epoch, owner, acquired_at_ms, expires_at_ms,
             last_heartbeat_at_ms
         )
         SELECT engagement_id, lease_epoch, lease_owner, updated_at_ms,
                lease_expires_at_ms, updated_at_ms
         FROM engagements
         WHERE lease_owner IS NOT NULL AND lease_expires_at_ms IS NOT NULL;",
    )?;
    let records = {
        let mut statement = transaction.prepare(
            "SELECT engagement_id, session_id, prompt_id, workspace_id, user_request,
                    status, stage, priority, lease_epoch, lease_owner,
                    lease_expires_at_ms, accepted_at_ms, updated_at_ms, checkpoint_json
             FROM engagements",
        )?;
        let rows = statement.query_map([], decode_engagement_row)?;
        let mut records = Vec::new();
        for row in rows {
            records.push(row?);
        }
        records
    };
    for record in records {
        let event_seq = transaction.query_row(
            "SELECT COALESCE(MAX(seq), 0) FROM events WHERE engagement_id=?1",
            [record.engagement_id.0.as_str()],
            |row| row.get::<_, i64>(0),
        )?;
        record_stage_and_tasks_tx(&transaction, &record, as_u64(event_seq))?;
    }
    transaction.commit()?;
    Ok(())
}

fn migrate_legacy_table_names(connection: &Connection) -> Result<(), rusqlite::Error> {
    for (legacy, current) in [
        ("engagement_events", "events"),
        ("engagement_snapshots", "snapshots"),
        ("engagement_actions", "actions"),
        ("engagement_jobs", "job_checkpoints"),
    ] {
        let legacy_exists = sqlite_object_exists(connection, "table", legacy)?;
        let current_exists = sqlite_object_exists(connection, "table", current)?;
        if legacy_exists && !current_exists {
            connection.execute_batch(&format!("ALTER TABLE {legacy} RENAME TO {current}"))?;
        }
    }
    Ok(())
}

fn sqlite_object_exists(
    connection: &Connection,
    object_type: &str,
    name: &str,
) -> Result<bool, rusqlite::Error> {
    connection.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM sqlite_master WHERE type=?1 AND name=?2
         )",
        params![object_type, name],
        |row| row.get(0),
    )
}

fn ensure_column(
    connection: &Connection,
    table: &str,
    column: &str,
    definition: &str,
) -> Result<(), rusqlite::Error> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
    for existing in columns {
        if existing? == column {
            return Ok(());
        }
    }
    connection.execute_batch(&format!(
        "ALTER TABLE {table} ADD COLUMN {column} {definition}"
    ))
}

fn claim_tx(
    transaction: Transaction<'_>,
    engagement_id: &EngagementId,
    owner: &str,
    ttl_ms: u64,
    allow_parked: bool,
) -> Result<EngagementMutation<Option<EngagementRecord>>, EngagementError> {
    let now = now_ms();
    let changed = transaction.execute(
        "UPDATE engagements
         SET lease_epoch=lease_epoch+1, lease_owner=?1, lease_expires_at_ms=?2,
             updated_at_ms=?3
         WHERE engagement_id=?4
           AND (
             status IN ('queued','suspended')
             OR (?5 AND status='parked')
           )
           AND (lease_expires_at_ms IS NULL OR lease_expires_at_ms <= ?3)",
        params![
            owner,
            add_ttl(now, ttl_ms),
            now,
            engagement_id.0,
            allow_parked,
        ],
    )?;
    if changed != 1 {
        transaction.commit()?;
        return Ok(EngagementMutation {
            value: None,
            event: None,
        });
    }
    let record = load_engagement_tx(&transaction, engagement_id)?;
    transaction.execute(
        "DELETE FROM pending_stages WHERE engagement_id=?1",
        [engagement_id.0.as_str()],
    )?;
    transaction.execute(
        "INSERT INTO leases(
            engagement_id, lease_epoch, owner, acquired_at_ms, expires_at_ms,
            last_heartbeat_at_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?4)",
        params![
            engagement_id.0,
            as_i64(record.lease_epoch),
            owner,
            now,
            record.lease_expires_at_ms.unwrap_or(now),
        ],
    )?;
    let event = append_event_tx(
        &transaction,
        engagement_id,
        record.lease_epoch,
        record.stage,
        "lease_claimed",
        serde_json::json!({
            "owner": owner,
            "lease_epoch": record.lease_epoch,
            "lease_expires_at_ms": record.lease_expires_at_ms,
        }),
    )?;
    write_snapshot_tx(&transaction, &record, event.seq)?;
    transaction.commit()?;
    Ok(EngagementMutation {
        value: Some(record),
        event: Some(event),
    })
}

fn ensure_lease(record: &EngagementRecord, lease_epoch: u64) -> Result<(), EngagementError> {
    if record.lease_epoch != lease_epoch
        || record.lease_owner.is_none()
        || record
            .lease_expires_at_ms
            .is_none_or(|expires| expires <= now_ms())
    {
        return Err(EngagementError::StaleLease {
            engagement_id: record.engagement_id.0.clone(),
            lease_epoch,
        });
    }
    Ok(())
}

fn fence_lease_tx(
    transaction: &Transaction<'_>,
    engagement_id: &EngagementId,
    lease_epoch: u64,
) -> Result<(), EngagementError> {
    let now = now_ms();
    let changed = transaction.execute(
        "UPDATE engagements
         SET updated_at_ms=updated_at_ms
         WHERE engagement_id=?1 AND lease_epoch=?2
           AND lease_owner IS NOT NULL AND lease_expires_at_ms > ?3",
        params![engagement_id.0, as_i64(lease_epoch), now],
    )?;
    if changed != 1 {
        return Err(EngagementError::StaleLease {
            engagement_id: engagement_id.0.clone(),
            lease_epoch,
        });
    }
    Ok(())
}

fn release_lease_tx(
    transaction: &Transaction<'_>,
    engagement_id: &EngagementId,
    lease_epoch: u64,
    released_at_ms: i64,
    reason: &str,
) -> Result<(), EngagementError> {
    transaction.execute(
        "UPDATE leases
         SET released_at_ms=?1, release_reason=?2
         WHERE engagement_id=?3 AND lease_epoch=?4 AND released_at_ms IS NULL",
        params![released_at_ms, reason, engagement_id.0, as_i64(lease_epoch)],
    )?;
    Ok(())
}

fn queue_priority_for(
    status: EngagementStatus,
    stage: Option<EngagementStage>,
    current: QueuePriority,
) -> QueuePriority {
    match (status, stage) {
        (EngagementStatus::Executing | EngagementStatus::Correcting, _) => {
            QueuePriority::ExecutorContinuation
        }
        (EngagementStatus::Reviewing, _) | (_, Some(EngagementStage::Reviewer)) => {
            QueuePriority::Reviewer
        }
        (EngagementStatus::Planning, _) => QueuePriority::Interactive,
        (
            EngagementStatus::Queued | EngagementStatus::Suspended,
            Some(EngagementStage::Executor),
        )
        | (
            EngagementStatus::Queued | EngagementStatus::Suspended,
            Some(EngagementStage::Correction),
        ) => QueuePriority::ExecutorContinuation,
        (EngagementStatus::Queued | EngagementStatus::Suspended, _) => {
            if current == QueuePriority::Reviewer {
                QueuePriority::Reviewer
            } else {
                QueuePriority::DurableStage
            }
        }
        _ => current,
    }
}

fn sync_pending_stage_tx(
    transaction: &Transaction<'_>,
    engagement_id: &EngagementId,
    status: EngagementStatus,
    stage: Option<EngagementStage>,
    priority: QueuePriority,
    now: i64,
) -> Result<(), EngagementError> {
    if matches!(
        status,
        EngagementStatus::Queued | EngagementStatus::Suspended
    ) {
        let queue_class = match priority {
            QueuePriority::ExecutorContinuation => "active_execution_continuation",
            QueuePriority::Reviewer => "optional_reviewer",
            QueuePriority::AdapterPrewarm => "adapter_prewarm",
            QueuePriority::Background => "embedding_prewarm",
            QueuePriority::Interactive | QueuePriority::DurableStage => "durable_pending_stage",
        };
        transaction.execute(
            "INSERT INTO pending_stages(
                engagement_id, queue_class, priority, status, stage,
                available_at_ms, updated_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)
             ON CONFLICT(engagement_id) DO UPDATE SET
                queue_class=excluded.queue_class,
                priority=excluded.priority,
                status=excluded.status,
                stage=excluded.stage,
                available_at_ms=excluded.available_at_ms,
                updated_at_ms=excluded.updated_at_ms",
            params![
                engagement_id.0,
                queue_class,
                priority as i64,
                status.as_str(),
                stage.map(EngagementStage::as_str),
                now,
            ],
        )?;
    } else {
        transaction.execute(
            "DELETE FROM pending_stages WHERE engagement_id=?1",
            [engagement_id.0.as_str()],
        )?;
    }
    Ok(())
}

fn hydrate_checkpoint_job_refs_tx(
    transaction: &Transaction<'_>,
    engagement_id: &EngagementId,
    checkpoint: &mut EngagementCheckpoint,
) -> Result<(), EngagementError> {
    let mut statement = transaction.prepare(
        "SELECT job_id, stdout_cursor, stderr_cursor, stdout_artifact, stderr_artifact,
                status_artifact
         FROM job_checkpoints
         WHERE engagement_id=?1
         ORDER BY updated_at_ms, job_id",
    )?;
    let rows = statement.query_map([engagement_id.0.as_str()], |row| {
        Ok((
            row.get::<_, String>(0)?,
            as_u64(row.get(1)?),
            as_u64(row.get(2)?),
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<String>>(4)?,
            row.get::<_, Option<String>>(5)?,
        ))
    })?;
    checkpoint.background_jobs.clear();
    checkpoint.artifact_refs.retain(|artifact| {
        !artifact.artifact_id.starts_with("job-stdout:")
            && !artifact.artifact_id.starts_with("job-stderr:")
            && !artifact.artifact_id.starts_with("job-status:")
    });
    for row in rows {
        let (
            job_id,
            stdout_cursor,
            stderr_cursor,
            stdout_artifact,
            stderr_artifact,
            status_artifact,
        ) = row?;
        checkpoint.background_jobs.push(BackgroundJobCursor {
            job_id: job_id.clone(),
            stdout_cursor,
            stderr_cursor,
        });
        if let Some(path) = stdout_artifact {
            checkpoint.artifact_refs.push(ArtifactReference {
                artifact_id: format!("job-stdout:{job_id}:{path}"),
                content_hash: None,
                cursor: Some(stdout_cursor),
            });
        }
        if let Some(path) = stderr_artifact {
            checkpoint.artifact_refs.push(ArtifactReference {
                artifact_id: format!("job-stderr:{job_id}:{path}"),
                content_hash: None,
                cursor: Some(stderr_cursor),
            });
        }
        if let Some(path) = status_artifact {
            checkpoint.artifact_refs.push(ArtifactReference {
                artifact_id: format!("job-status:{job_id}:{path}"),
                content_hash: None,
                cursor: None,
            });
        }
    }
    Ok(())
}

fn record_stage_and_tasks_tx(
    transaction: &Transaction<'_>,
    record: &EngagementRecord,
    event_seq: u64,
) -> Result<(), EngagementError> {
    transaction.execute(
        "INSERT OR IGNORE INTO stages(
            engagement_id, stage_seq, status, stage, plan_revision, task_id,
            task_cursor, checkpoint_json, entered_at_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            record.engagement_id.0,
            as_i64(event_seq),
            record.status.as_str(),
            record.stage.map(EngagementStage::as_str),
            i64::from(record.checkpoint.plan_revision),
            record.checkpoint.task_id,
            i64::from(record.checkpoint.task_index),
            serde_json::to_string(&record.checkpoint)?,
            record.updated_at_ms,
        ],
    )?;
    let Some(tasks) = record
        .checkpoint
        .plan
        .as_ref()
        .and_then(|plan| plan.get("tasks"))
        .and_then(serde_json::Value::as_array)
    else {
        return Ok(());
    };
    for (index, task) in tasks.iter().enumerate() {
        let Some(task_id) = task
            .get("task_id")
            .and_then(serde_json::Value::as_str)
            .filter(|task_id| !task_id.is_empty())
        else {
            continue;
        };
        let task_index = u32::try_from(index).unwrap_or(u32::MAX);
        let status = if task_index < record.checkpoint.task_index {
            "completed"
        } else if record.checkpoint.task_id.as_deref() == Some(task_id) {
            "active"
        } else {
            "pending"
        };
        transaction.execute(
            "INSERT INTO tasks(
                engagement_id, plan_revision, task_id, task_index, status,
                task_json, updated_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(engagement_id, plan_revision, task_id) DO UPDATE SET
                task_index=excluded.task_index,
                status=excluded.status,
                task_json=excluded.task_json,
                updated_at_ms=excluded.updated_at_ms",
            params![
                record.engagement_id.0,
                i64::from(record.checkpoint.plan_revision),
                task_id,
                i64::from(task_index),
                status,
                serde_json::to_string(task)?,
                record.updated_at_ms,
            ],
        )?;
    }
    Ok(())
}

#[derive(Default)]
struct EventCoordinates {
    task_id: Option<String>,
    action_id: Option<String>,
    evidence_id: Option<String>,
    artifact_ref: Option<String>,
}

fn event_coordinates_tx(
    transaction: &Transaction<'_>,
    engagement_id: &EngagementId,
    payload: &serde_json::Value,
) -> Result<EventCoordinates, EngagementError> {
    let string_at = |pointer: &str| {
        payload
            .pointer(pointer)
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned)
    };
    let action_key = string_at("/action_key");
    let action = action_key
        .as_deref()
        .map(|key| load_action_tx(transaction, key))
        .transpose()?
        .flatten();
    let checkpoint = transaction
        .query_row(
            "SELECT checkpoint_json FROM engagements WHERE engagement_id=?1",
            [engagement_id.0.as_str()],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str::<EngagementCheckpoint>(&json))
        .transpose()?;
    let artifact_ref = string_at("/artifact_ref")
        .or_else(|| string_at("/stdout_artifact"))
        .or_else(|| string_at("/stderr_artifact"));
    Ok(EventCoordinates {
        task_id: string_at("/identity/task_id")
            .or_else(|| string_at("/task_id"))
            .or_else(|| {
                action
                    .as_ref()
                    .map(|record| record.identity.task_id.clone())
            })
            .or_else(|| checkpoint.as_ref().and_then(|value| value.task_id.clone())),
        action_id: string_at("/identity/action_id")
            .or_else(|| string_at("/action_id"))
            .or_else(|| {
                action
                    .as_ref()
                    .map(|record| record.identity.action_id.clone())
            }),
        evidence_id: string_at("/evidence_id"),
        artifact_ref,
    })
}

fn append_event_tx(
    transaction: &Transaction<'_>,
    engagement_id: &EngagementId,
    lease_epoch: u64,
    stage: Option<EngagementStage>,
    kind: &str,
    payload: serde_json::Value,
) -> Result<EngagementEvent, EngagementError> {
    let payload = canonical_json(&payload);
    let coordinates = event_coordinates_tx(transaction, engagement_id, &payload)?;
    let payload_json = serde_json::to_string(&payload)?;
    check_json_size(
        payload_json.as_bytes(),
        "engagement event payload",
        MAX_EVENT_PAYLOAD_BYTES,
    )?;
    let seq = transaction.query_row(
        "SELECT COALESCE(MAX(seq), -1) + 1 FROM events WHERE engagement_id=?1",
        [engagement_id.0.as_str()],
        |row| row.get::<_, i64>(0),
    )?;
    let seq = as_u64(seq);
    let created_at_ms = now_ms();
    let content_hash = event_content_hash(
        engagement_id,
        seq,
        lease_epoch,
        stage,
        &coordinates,
        kind,
        &payload_json,
        created_at_ms,
    );
    let event_id = format!("evt_{}", &content_hash[..32]);
    transaction.execute(
        "INSERT INTO events(
            engagement_id, seq, event_id, kind, lease_epoch, stage,
            task_id, action_id, evidence_id, artifact_ref,
            payload_json, content_hash, created_at_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
        params![
            engagement_id.0,
            as_i64(seq),
            event_id,
            kind,
            as_i64(lease_epoch),
            stage.map(EngagementStage::as_str),
            coordinates.task_id.as_deref(),
            coordinates.action_id.as_deref(),
            coordinates.evidence_id.as_deref(),
            coordinates.artifact_ref.as_deref(),
            payload_json,
            content_hash,
            created_at_ms,
        ],
    )?;
    Ok(EngagementEvent {
        engagement_id: engagement_id.clone(),
        seq,
        event_id,
        kind: kind.to_string(),
        lease_epoch,
        stage,
        task_id: coordinates.task_id,
        action_id: coordinates.action_id,
        evidence_id: coordinates.evidence_id,
        artifact_ref: coordinates.artifact_ref,
        payload,
        content_hash,
        created_at_ms,
    })
}

fn write_snapshot_tx(
    transaction: &Transaction<'_>,
    record: &EngagementRecord,
    event_seq: u64,
) -> Result<(), EngagementError> {
    let now = now_ms();
    transaction.execute(
        "INSERT INTO snapshots(engagement_id, event_seq, record_json, created_at_ms)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(engagement_id) DO UPDATE SET
            event_seq=excluded.event_seq,
            record_json=excluded.record_json,
            created_at_ms=excluded.created_at_ms
         WHERE excluded.event_seq >= snapshots.event_seq",
        params![
            record.engagement_id.0,
            as_i64(event_seq),
            serde_json::to_string(record)?,
            now,
        ],
    )?;
    Ok(())
}

fn load_by_session_prompt_tx(
    transaction: &Transaction<'_>,
    session_id: &str,
    prompt_id: &str,
) -> rusqlite::Result<Option<EngagementRecord>> {
    transaction
        .query_row(
            "SELECT engagement_id, session_id, prompt_id, workspace_id, user_request,
                    status, stage, priority, lease_epoch, lease_owner,
                    lease_expires_at_ms, accepted_at_ms, updated_at_ms, checkpoint_json
             FROM engagements WHERE session_id=?1 AND prompt_id=?2",
            params![session_id, prompt_id],
            decode_engagement_row,
        )
        .optional()
}

fn load_engagement_tx(
    transaction: &Transaction<'_>,
    engagement_id: &EngagementId,
) -> Result<EngagementRecord, EngagementError> {
    transaction
        .query_row(
            "SELECT engagement_id, session_id, prompt_id, workspace_id, user_request,
                    status, stage, priority, lease_epoch, lease_owner,
                    lease_expires_at_ms, accepted_at_ms, updated_at_ms, checkpoint_json
             FROM engagements WHERE engagement_id=?1",
            [engagement_id.0.as_str()],
            decode_engagement_row,
        )
        .optional()?
        .ok_or_else(|| EngagementError::NotFound(engagement_id.0.clone()))
}

fn load_engagement_conn(
    connection: &Connection,
    engagement_id: &EngagementId,
) -> rusqlite::Result<Option<EngagementRecord>> {
    connection
        .query_row(
            "SELECT engagement_id, session_id, prompt_id, workspace_id, user_request,
                    status, stage, priority, lease_epoch, lease_owner,
                    lease_expires_at_ms, accepted_at_ms, updated_at_ms, checkpoint_json
             FROM engagements WHERE engagement_id=?1",
            [engagement_id.0.as_str()],
            decode_engagement_row,
        )
        .optional()
}

fn decode_engagement_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<EngagementRecord> {
    let status = parse_status(row.get(5)?)?;
    let stage = row
        .get::<_, Option<String>>(6)?
        .map(parse_stage)
        .transpose()?;
    let checkpoint_json: String = row.get(13)?;
    Ok(EngagementRecord {
        engagement_id: EngagementId(row.get(0)?),
        session_id: row.get(1)?,
        prompt_id: row.get(2)?,
        workspace_id: row.get(3)?,
        user_request: row.get(4)?,
        status,
        stage,
        priority: QueuePriority::from_i64(row.get(7)?),
        lease_epoch: as_u64(row.get::<_, i64>(8)?),
        lease_owner: row.get(9)?,
        lease_expires_at_ms: row.get(10)?,
        accepted_at_ms: row.get(11)?,
        updated_at_ms: row.get(12)?,
        checkpoint: serde_json::from_str(&checkpoint_json).map_err(sql_decode_error)?,
    })
}

fn load_action_tx(
    transaction: &Transaction<'_>,
    action_key: &str,
) -> rusqlite::Result<Option<ActionRecord>> {
    transaction
        .query_row(
            "SELECT stable_key, engagement_id, plan_revision, task_id, action_id,
                    action_kind, command_hash, replay_policy, status, attempt_count,
                    job_id, evidence_id, payload_json, result_json, prepared_at_ms,
                    dispatched_at_ms, updated_at_ms
             FROM actions WHERE stable_key=?1",
            [action_key],
            decode_action_row,
        )
        .optional()
}

fn load_action_conn(
    connection: &Connection,
    action_key: &str,
) -> rusqlite::Result<Option<ActionRecord>> {
    connection
        .query_row(
            "SELECT stable_key, engagement_id, plan_revision, task_id, action_id,
                    action_kind, command_hash, replay_policy, status, attempt_count,
                    job_id, evidence_id, payload_json, result_json, prepared_at_ms,
                    dispatched_at_ms, updated_at_ms
             FROM actions WHERE stable_key=?1",
            [action_key],
            decode_action_row,
        )
        .optional()
}

fn decode_action_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ActionRecord> {
    let payload_json: Option<String> = row.get(12)?;
    let result_json: Option<String> = row.get(13)?;
    Ok(ActionRecord {
        stable_key: row.get(0)?,
        engagement_id: EngagementId(row.get(1)?),
        identity: ActionIdentity {
            plan_revision: u32::try_from(row.get::<_, i64>(2)?).unwrap_or(u32::MAX),
            task_id: row.get(3)?,
            action_id: row.get(4)?,
        },
        action_kind: row.get(5)?,
        command_hash: row.get(6)?,
        replay_policy: parse_action_replay_policy(row.get(7)?)?,
        status: parse_action_status(row.get(8)?)?,
        attempt_count: u32::try_from(row.get::<_, i64>(9)?).unwrap_or(u32::MAX),
        job_id: row.get(10)?,
        evidence_id: row.get(11)?,
        payload: payload_json
            .map(|json| serde_json::from_str(&json).map_err(sql_decode_error))
            .transpose()?,
        result: result_json
            .map(|json| serde_json::from_str(&json).map_err(sql_decode_error))
            .transpose()?,
        prepared_at_ms: row.get(14)?,
        dispatched_at_ms: row.get(15)?,
        updated_at_ms: row.get(16)?,
    })
}

fn load_job_tx(
    transaction: &Transaction<'_>,
    job_id: &str,
) -> rusqlite::Result<Option<JobCheckpoint>> {
    transaction
        .query_row(
            "SELECT job_id, engagement_id, action_key, kind, lifecycle, command_hash,
                    pid, process_start_identity, process_started_at_ms, process_group_id, stdout_artifact,
                    stderr_artifact, status_artifact, stdout_cursor, stderr_cursor,
                    last_activity_at_ms, exit_code, payload_json
             FROM job_checkpoints WHERE job_id=?1",
            [job_id],
            decode_job_row,
        )
        .optional()
}

fn load_job_conn(connection: &Connection, job_id: &str) -> rusqlite::Result<Option<JobCheckpoint>> {
    connection
        .query_row(
            "SELECT job_id, engagement_id, action_key, kind, lifecycle, command_hash,
                    pid, process_start_identity, process_started_at_ms, process_group_id, stdout_artifact,
                    stderr_artifact, status_artifact, stdout_cursor, stderr_cursor,
                    last_activity_at_ms, exit_code, payload_json
             FROM job_checkpoints WHERE job_id=?1",
            [job_id],
            decode_job_row,
        )
        .optional()
}

fn decode_job_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<JobCheckpoint> {
    let payload_json: String = row.get(17)?;
    Ok(JobCheckpoint {
        job_id: row.get(0)?,
        engagement_id: EngagementId(row.get(1)?),
        action_key: row.get(2)?,
        kind: parse_job_kind(row.get(3)?)?,
        lifecycle: parse_job_lifecycle(row.get(4)?)?,
        command_hash: row.get(5)?,
        pid: row
            .get::<_, Option<i64>>(6)?
            .map(|value| u32::try_from(value).unwrap_or(u32::MAX)),
        process_start_identity: row.get(7)?,
        process_started_at_ms: row.get(8)?,
        process_group_id: row.get(9)?,
        stdout_artifact: row.get::<_, Option<String>>(10)?.map(Into::into),
        stderr_artifact: row.get::<_, Option<String>>(11)?.map(Into::into),
        status_artifact: row.get::<_, Option<String>>(12)?.map(Into::into),
        stdout_cursor: as_u64(row.get(13)?),
        stderr_cursor: as_u64(row.get(14)?),
        last_activity_at_ms: row.get(15)?,
        exit_code: row.get(16)?,
        payload: serde_json::from_str(&payload_json).map_err(sql_decode_error)?,
    })
}

fn decode_event_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<EngagementEvent> {
    let stage = row
        .get::<_, Option<String>>(5)?
        .map(parse_stage)
        .transpose()?;
    let task_id: Option<String> = row.get(6)?;
    let action_id: Option<String> = row.get(7)?;
    let evidence_id: Option<String> = row.get(8)?;
    let artifact_ref: Option<String> = row.get(9)?;
    let payload_json: String = row.get(10)?;
    let engagement_id = EngagementId(row.get(0)?);
    let seq = as_u64(row.get(1)?);
    let event_id: String = row.get(2)?;
    let kind: String = row.get(3)?;
    let lease_epoch = as_u64(row.get(4)?);
    let content_hash: String = row.get(11)?;
    let created_at_ms = row.get(12)?;
    let expected_hash = event_content_hash(
        &engagement_id,
        seq,
        lease_epoch,
        stage,
        &EventCoordinates {
            task_id: task_id.clone(),
            action_id: action_id.clone(),
            evidence_id: evidence_id.clone(),
            artifact_ref: artifact_ref.clone(),
        },
        &kind,
        &payload_json,
        created_at_ms,
    );
    let expected_event_id = format!("evt_{}", &expected_hash[..32]);
    if content_hash != expected_hash || event_id != expected_event_id {
        return Err(sql_invalid_value(
            "engagement event integrity",
            format!("{engagement_id}:{seq}"),
        ));
    }
    Ok(EngagementEvent {
        engagement_id,
        seq,
        event_id,
        kind,
        lease_epoch,
        stage,
        task_id,
        action_id,
        evidence_id,
        artifact_ref,
        payload: serde_json::from_str(&payload_json).map_err(sql_decode_error)?,
        content_hash,
        created_at_ms,
    })
}

fn event_content_hash(
    engagement_id: &EngagementId,
    seq: u64,
    lease_epoch: u64,
    stage: Option<EngagementStage>,
    coordinates: &EventCoordinates,
    kind: &str,
    payload_json: &str,
    created_at_ms: i64,
) -> String {
    let mut hasher = blake3::Hasher::new();
    let seq_text = seq.to_string();
    let lease_epoch_text = lease_epoch.to_string();
    let created_at_text = created_at_ms.to_string();
    for part in [
        engagement_id.0.as_bytes(),
        seq_text.as_bytes(),
        lease_epoch_text.as_bytes(),
        stage.map_or(b"", |value| value.as_str().as_bytes()),
        coordinates
            .task_id
            .as_deref()
            .unwrap_or_default()
            .as_bytes(),
        coordinates
            .action_id
            .as_deref()
            .unwrap_or_default()
            .as_bytes(),
        coordinates
            .evidence_id
            .as_deref()
            .unwrap_or_default()
            .as_bytes(),
        coordinates
            .artifact_ref
            .as_deref()
            .unwrap_or_default()
            .as_bytes(),
        kind.as_bytes(),
        payload_json.as_bytes(),
        created_at_text.as_bytes(),
    ] {
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part);
    }
    hasher.finalize().to_hex().to_string()
}

fn parse_status(value: String) -> rusqlite::Result<EngagementStatus> {
    EngagementStatus::parse(&value).ok_or_else(|| sql_invalid_value("engagement status", value))
}

fn parse_stage(value: String) -> rusqlite::Result<EngagementStage> {
    EngagementStage::parse(&value).ok_or_else(|| sql_invalid_value("engagement stage", value))
}

fn parse_action_status(value: String) -> rusqlite::Result<ActionStatus> {
    ActionStatus::parse(&value).ok_or_else(|| sql_invalid_value("action status", value))
}

fn parse_action_replay_policy(value: String) -> rusqlite::Result<ActionReplayPolicy> {
    ActionReplayPolicy::parse(&value)
        .ok_or_else(|| sql_invalid_value("action replay policy", value))
}

fn parse_job_kind(value: String) -> rusqlite::Result<JobKind> {
    JobKind::parse(&value).ok_or_else(|| sql_invalid_value("job kind", value))
}

fn parse_job_lifecycle(value: String) -> rusqlite::Result<JobLifecycle> {
    JobLifecycle::parse(&value).ok_or_else(|| sql_invalid_value("job lifecycle", value))
}

fn sql_invalid_value(field: &'static str, value: String) -> rusqlite::Error {
    sql_decode_error(EngagementError::InvalidPersistedValue { field, value })
}

fn sql_decode_error(error: impl std::error::Error + Send + Sync + 'static) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
}

fn check_json_size(bytes: &[u8], field: &'static str, limit: usize) -> Result<(), EngagementError> {
    if bytes.len() > limit {
        return Err(EngagementError::TooLarge { field, limit });
    }
    Ok(())
}

fn optional_json(value: Option<&serde_json::Value>) -> Result<Option<String>, serde_json::Error> {
    value.map(serde_json::to_string).transpose()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn add_ttl(now: i64, ttl_ms: u64) -> i64 {
    now.saturating_add(i64::try_from(ttl_ms.max(1)).unwrap_or(i64::MAX))
}

fn as_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn as_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

fn canonical_json(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let mut entries = map.iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|left, right| left.0.cmp(right.0));
            serde_json::Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key.clone(), canonical_json(value)))
                    .collect(),
            )
        }
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(canonical_json).collect())
        }
        other => other.clone(),
    }
}
