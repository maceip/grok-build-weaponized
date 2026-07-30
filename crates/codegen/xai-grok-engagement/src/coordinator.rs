use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::{broadcast, mpsc, oneshot};

use crate::store::{
    AcceptOutcome, EngagementError, EngagementMutation, EngagementStore, RecoveryReport,
};
use crate::types::{
    ActionRecord, ActionResolution, ActionSpec, ActionStatus, EngagementCheckpoint,
    EngagementEvent, EngagementId, EngagementRecord, EngagementSnapshot, EngagementStage,
    EngagementStatus, JobCheckpoint, NewEngagement,
};

const DEFAULT_EVENT_CAPACITY: usize = 1_024;
const DEFAULT_LEASE_TTL_MS: u64 = 30_000;
const CATCH_UP_PAGE: usize = 1_000;

type StoreTask = Box<dyn FnOnce(&mut EngagementStore) + Send + 'static>;

struct CoordinatorInner {
    writer_tx: mpsc::UnboundedSender<StoreTask>,
    events_tx: broadcast::Sender<EngagementEvent>,
}

/// Process-shared asynchronous facade over one serial SQLite writer.
///
/// Every database path has at most one live writer actor in a process. Reads
/// also pass through that actor, which keeps rusqlite off async executor
/// threads and gives every state change one deterministic ordering point.
#[derive(Clone)]
pub struct EngagementCoordinator {
    inner: Arc<CoordinatorInner>,
}

impl EngagementCoordinator {
    pub async fn shared(path: impl AsRef<Path>) -> Result<Self, EngagementError> {
        Self::shared_with_event_capacity(path.as_ref().to_path_buf(), DEFAULT_EVENT_CAPACITY).await
    }

    async fn shared_with_event_capacity(
        path: PathBuf,
        event_capacity: usize,
    ) -> Result<Self, EngagementError> {
        static SERVICES: OnceLock<tokio::sync::Mutex<HashMap<PathBuf, Weak<CoordinatorInner>>>> =
            OnceLock::new();
        let services = SERVICES.get_or_init(|| tokio::sync::Mutex::new(HashMap::new()));
        let mut services = services.lock().await;
        if let Some(existing) = services.get(&path).and_then(Weak::upgrade) {
            return Ok(Self { inner: existing });
        }

        let open_path = path.clone();
        let store = tokio::task::spawn_blocking(move || EngagementStore::open(&open_path))
            .await
            .map_err(|_| EngagementError::WriterStopped)??;
        let (writer_tx, mut writer_rx) = mpsc::unbounded_channel::<StoreTask>();
        let (events_tx, _) = broadcast::channel(event_capacity.max(1));
        tokio::task::spawn_blocking(move || {
            let mut store = store;
            while let Some(task) = writer_rx.blocking_recv() {
                task(&mut store);
            }
        });
        let inner = Arc::new(CoordinatorInner {
            writer_tx,
            events_tx,
        });
        services.insert(path, Arc::downgrade(&inner));
        Ok(Self { inner })
    }

    #[cfg(test)]
    pub(crate) async fn shared_for_test(
        path: impl AsRef<Path>,
        event_capacity: usize,
    ) -> Result<Self, EngagementError> {
        Self::shared_with_event_capacity(path.as_ref().to_path_buf(), event_capacity).await
    }

    async fn call<T, F>(&self, operation: F) -> Result<T, EngagementError>
    where
        T: Send + 'static,
        F: FnOnce(&mut EngagementStore) -> Result<T, EngagementError> + Send + 'static,
    {
        let (respond_to, response) = oneshot::channel();
        self.inner
            .writer_tx
            .send(Box::new(move |store| {
                let _ = respond_to.send(operation(store));
            }))
            .map_err(|_| EngagementError::WriterStopped)?;
        response.await.map_err(|_| EngagementError::WriterStopped)?
    }

    fn publish<T>(&self, mutation: &EngagementMutation<T>) {
        if let Some(event) = mutation.event.as_ref() {
            let _ = self.inner.events_tx.send(event.clone());
        }
    }

    pub async fn accept(&self, input: NewEngagement) -> Result<AcceptOutcome, EngagementError> {
        let mutation = self.call(move |store| store.accept(input)).await?;
        self.publish(&mutation);
        Ok(mutation.value)
    }

    pub async fn accept_and_claim(
        &self,
        input: NewEngagement,
        owner: impl Into<String>,
        ttl_ms: Option<u64>,
    ) -> Result<Option<EngagementLease>, EngagementError> {
        let accepted = self.accept(input).await?;
        self.claim(
            accepted.record.engagement_id,
            owner,
            ttl_ms,
            accepted.record.status == EngagementStatus::Parked,
        )
        .await
    }

    pub async fn claim_next(
        &self,
        owner: impl Into<String>,
        ttl_ms: Option<u64>,
    ) -> Result<Option<EngagementLease>, EngagementError> {
        let owner = owner.into();
        let ttl_ms = ttl_ms.unwrap_or(DEFAULT_LEASE_TTL_MS);
        let mutation = self
            .call(move |store| store.claim_next(&owner, ttl_ms))
            .await?;
        self.publish(&mutation);
        Ok(mutation
            .value
            .map(|record| EngagementLease::new(self.clone(), record, ttl_ms)))
    }

    pub async fn claim(
        &self,
        engagement_id: EngagementId,
        owner: impl Into<String>,
        ttl_ms: Option<u64>,
        allow_parked: bool,
    ) -> Result<Option<EngagementLease>, EngagementError> {
        let owner = owner.into();
        let ttl_ms = ttl_ms.unwrap_or(DEFAULT_LEASE_TTL_MS);
        let mutation = self
            .call(move |store| store.claim(&engagement_id, &owner, ttl_ms, allow_parked))
            .await?;
        self.publish(&mutation);
        Ok(mutation
            .value
            .map(|record| EngagementLease::new(self.clone(), record, ttl_ms)))
    }

    pub async fn get(
        &self,
        engagement_id: EngagementId,
    ) -> Result<Option<EngagementRecord>, EngagementError> {
        self.call(move |store| store.get(&engagement_id)).await
    }

    pub async fn list(
        &self,
        session_id: Option<String>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<EngagementRecord>, EngagementError> {
        self.call(move |store| store.list(session_id.as_deref(), offset, limit))
            .await
    }

    pub async fn events_after(
        &self,
        engagement_id: EngagementId,
        after_seq: Option<u64>,
        limit: usize,
    ) -> Result<Vec<EngagementEvent>, EngagementError> {
        self.call(move |store| store.events_after(&engagement_id, after_seq, limit))
            .await
    }

    pub async fn latest_snapshot(
        &self,
        engagement_id: EngagementId,
    ) -> Result<Option<EngagementSnapshot>, EngagementError> {
        self.call(move |store| store.latest_snapshot(&engagement_id))
            .await
    }

    pub async fn action(
        &self,
        action_key: String,
    ) -> Result<Option<ActionRecord>, EngagementError> {
        self.call(move |store| store.action(&action_key)).await
    }

    pub async fn actions(
        &self,
        engagement_id: EngagementId,
    ) -> Result<Vec<ActionRecord>, EngagementError> {
        self.call(move |store| store.actions(&engagement_id)).await
    }

    pub async fn recover_expired(&self) -> Result<RecoveryReport, EngagementError> {
        let at_ms = now_ms();
        let report = self.call(move |store| store.recover_expired(at_ms)).await?;
        for event in &report.events {
            let _ = self.inner.events_tx.send(event.clone());
        }
        Ok(report)
    }

    pub async fn recoverable_jobs(&self) -> Result<Vec<JobCheckpoint>, EngagementError> {
        self.call(|store| store.recoverable_jobs()).await
    }

    pub async fn job(&self, job_id: String) -> Result<Option<JobCheckpoint>, EngagementError> {
        self.call(move |store| store.job(&job_id)).await
    }

    pub async fn queue_depth(&self) -> Result<usize, EngagementError> {
        self.call(|store| store.queue_depth()).await
    }

    pub fn subscribe(
        &self,
        engagement_id: EngagementId,
        after_seq: Option<u64>,
    ) -> EngagementSubscription {
        EngagementSubscription {
            coordinator: self.clone(),
            engagement_id,
            cursor: after_seq,
            receiver: self.inner.events_tx.subscribe(),
            pending: VecDeque::new(),
            initial_catch_up: true,
        }
    }
}

#[derive(Clone)]
pub struct EngagementLease {
    coordinator: EngagementCoordinator,
    engagement_id: EngagementId,
    lease_epoch: u64,
    ttl_ms: u64,
}

impl std::fmt::Debug for EngagementLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EngagementLease")
            .field("engagement_id", &self.engagement_id)
            .field("lease_epoch", &self.lease_epoch)
            .field("ttl_ms", &self.ttl_ms)
            .finish_non_exhaustive()
    }
}

impl EngagementLease {
    fn new(coordinator: EngagementCoordinator, record: EngagementRecord, ttl_ms: u64) -> Self {
        Self {
            coordinator,
            engagement_id: record.engagement_id,
            lease_epoch: record.lease_epoch,
            ttl_ms,
        }
    }

    pub fn engagement_id(&self) -> &EngagementId {
        &self.engagement_id
    }

    pub fn lease_epoch(&self) -> u64 {
        self.lease_epoch
    }

    pub async fn heartbeat(&self) -> Result<bool, EngagementError> {
        let engagement_id = self.engagement_id.clone();
        let lease_epoch = self.lease_epoch;
        let ttl_ms = self.ttl_ms;
        self.coordinator
            .call(move |store| store.heartbeat(&engagement_id, lease_epoch, ttl_ms))
            .await
    }

    /// Queue a best-effort durable suspension without blocking the caller.
    ///
    /// Turn guards use this on unwinding and early returns. The writer still
    /// performs the same lease-epoch compare-and-swap; a completed or replaced
    /// lease cannot be changed by the detached request.
    pub fn suspend_detached(&self, reason: impl Into<String>) -> bool {
        let engagement_id = self.engagement_id.clone();
        let lease_epoch = self.lease_epoch;
        let reason = reason.into();
        let events_tx = self.coordinator.inner.events_tx.clone();
        self.coordinator
            .inner
            .writer_tx
            .send(Box::new(move |store| {
                match store.suspend_preserving_checkpoint(&engagement_id, lease_epoch, reason) {
                    Ok(mutation) => {
                        if let Some(event) = mutation.event {
                            let _ = events_tx.send(event);
                        }
                    }
                    Err(
                        EngagementError::InvalidTransition { .. }
                        | EngagementError::StaleLease { .. },
                    ) => {
                        // A normal completed turn or a successfully recovered
                        // lease can race this best-effort drop guard.
                    }
                    Err(error) => {
                        tracing::warn!(
                            engagement_id = %engagement_id,
                            lease_epoch,
                            %error,
                            "failed to persist detached engagement suspension"
                        );
                    }
                }
            }))
            .is_ok()
    }

    pub async fn transition(
        &self,
        status: EngagementStatus,
        stage: Option<EngagementStage>,
        checkpoint: EngagementCheckpoint,
        detail: serde_json::Value,
    ) -> Result<EngagementRecord, EngagementError> {
        let engagement_id = self.engagement_id.clone();
        let lease_epoch = self.lease_epoch;
        let mutation = self
            .coordinator
            .call(move |store| {
                store.transition(
                    &engagement_id,
                    lease_epoch,
                    status,
                    stage,
                    checkpoint,
                    detail,
                )
            })
            .await?;
        self.coordinator.publish(&mutation);
        Ok(mutation.value)
    }

    pub async fn prepare_action(&self, spec: ActionSpec) -> Result<ActionRecord, EngagementError> {
        let engagement_id = self.engagement_id.clone();
        let lease_epoch = self.lease_epoch;
        let mutation = self
            .coordinator
            .call(move |store| store.prepare_action(&engagement_id, lease_epoch, spec))
            .await?;
        self.coordinator.publish(&mutation);
        Ok(mutation.value)
    }

    pub async fn update_action(
        &self,
        action_key: impl Into<String>,
        status: ActionStatus,
        job_id: Option<String>,
        evidence_id: Option<String>,
        result: Option<serde_json::Value>,
    ) -> Result<ActionRecord, EngagementError> {
        let engagement_id = self.engagement_id.clone();
        let lease_epoch = self.lease_epoch;
        let action_key = action_key.into();
        let mutation = self
            .coordinator
            .call(move |store| {
                store.update_action(
                    &engagement_id,
                    lease_epoch,
                    &action_key,
                    status,
                    job_id.as_deref(),
                    evidence_id.as_deref(),
                    result.as_ref(),
                )
            })
            .await?;
        self.coordinator.publish(&mutation);
        Ok(mutation.value)
    }

    pub async fn resolve_ambiguous_action(
        &self,
        action_key: impl Into<String>,
        resolution: ActionResolution,
    ) -> Result<ActionRecord, EngagementError> {
        let engagement_id = self.engagement_id.clone();
        let lease_epoch = self.lease_epoch;
        let action_key = action_key.into();
        let mutation = self
            .coordinator
            .call(move |store| {
                store.resolve_ambiguous_action(&engagement_id, lease_epoch, &action_key, resolution)
            })
            .await?;
        self.coordinator.publish(&mutation);
        Ok(mutation.value)
    }

    pub async fn checkpoint_job(
        &self,
        checkpoint: JobCheckpoint,
    ) -> Result<JobCheckpoint, EngagementError> {
        let engagement_id = self.engagement_id.clone();
        let lease_epoch = self.lease_epoch;
        let mutation = self
            .coordinator
            .call(move |store| store.upsert_job(&engagement_id, lease_epoch, checkpoint))
            .await?;
        self.coordinator.publish(&mutation);
        Ok(mutation.value)
    }

    /// Queue a job checkpoint on the coordinator's serial writer without
    /// blocking a stream reader or process-reaping path.
    ///
    /// The lease epoch is still checked by SQLite, so a restarted worker
    /// cannot overwrite a newer supervisor's job state. Callers must use the
    /// awaited [`Self::checkpoint_job`] for the pre-spawn intent record and
    /// the first PID/process-group attachment.
    pub fn checkpoint_job_detached(&self, checkpoint: JobCheckpoint) -> bool {
        let engagement_id = self.engagement_id.clone();
        let lease_epoch = self.lease_epoch;
        let events_tx = self.coordinator.inner.events_tx.clone();
        self.coordinator
            .inner
            .writer_tx
            .send(Box::new(move |store| {
                match store.upsert_job(&engagement_id, lease_epoch, checkpoint) {
                    Ok(mutation) => {
                        if let Some(event) = mutation.event {
                            let _ = events_tx.send(event);
                        }
                    }
                    Err(error) => {
                        tracing::warn!(
                            engagement_id = %engagement_id,
                            lease_epoch,
                            %error,
                            "failed to persist detached execution-job checkpoint"
                        );
                    }
                }
            }))
            .is_ok()
    }

    pub async fn recoverable_jobs(&self) -> Result<Vec<JobCheckpoint>, EngagementError> {
        let engagement_id = self.engagement_id.clone();
        self.coordinator
            .call(move |store| {
                Ok(store
                    .recoverable_jobs()?
                    .into_iter()
                    .filter(|job| job.engagement_id == engagement_id)
                    .collect())
            })
            .await
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SubscriptionError {
    #[error(transparent)]
    Store(#[from] EngagementError),
    #[error("engagement event stream closed")]
    Closed,
}

/// A bounded live subscription with durable cursor repair.
///
/// Broadcast lag is never interpreted as event loss. A lagged subscriber
/// reads the missing range from SQLite before delivering any newer live event.
pub struct EngagementSubscription {
    coordinator: EngagementCoordinator,
    engagement_id: EngagementId,
    cursor: Option<u64>,
    receiver: broadcast::Receiver<EngagementEvent>,
    pending: VecDeque<EngagementEvent>,
    initial_catch_up: bool,
}

impl EngagementSubscription {
    pub fn cursor(&self) -> Option<u64> {
        self.cursor
    }

    pub async fn next(&mut self) -> Result<EngagementEvent, SubscriptionError> {
        loop {
            if self.initial_catch_up {
                self.initial_catch_up = false;
                self.catch_up().await?;
            }
            if let Some(event) = self.pending.pop_front() {
                self.cursor = Some(event.seq);
                return Ok(event);
            }
            match self.receiver.recv().await {
                Ok(event) if event.engagement_id != self.engagement_id => continue,
                Ok(event) => {
                    let expected = self.cursor.map_or(0, |cursor| cursor.saturating_add(1));
                    if event.seq == expected {
                        self.cursor = Some(event.seq);
                        return Ok(event);
                    }
                    self.catch_up().await?;
                }
                Err(broadcast::error::RecvError::Lagged(_)) => self.catch_up().await?,
                Err(broadcast::error::RecvError::Closed) => {
                    self.catch_up().await?;
                    if self.pending.is_empty() {
                        return Err(SubscriptionError::Closed);
                    }
                }
            }
        }
    }

    async fn catch_up(&mut self) -> Result<(), EngagementError> {
        let events = self
            .coordinator
            .events_after(self.engagement_id.clone(), self.cursor, CATCH_UP_PAGE)
            .await?;
        self.pending.extend(events);
        Ok(())
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}
