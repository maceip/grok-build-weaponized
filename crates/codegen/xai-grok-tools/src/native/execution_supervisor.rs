use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;

use tokio::sync::broadcast;

use super::job_registry::ExecutionJobRegistry;

const SUPERVISOR_TICK: Duration = Duration::from_millis(100);

/// Process-wide coordination point for long-lived execution jobs.
///
/// Child ownership stays with the backend that spawned it, but terminal,
/// Nmap, and future native drivers share one registry and one lifecycle clock.
/// This avoids one periodic timer per terminal session while preserving
/// backend-local process-tree teardown and stream handling.
pub struct ExecutionSupervisor {
    maintenance_tx: broadcast::Sender<()>,
    clock_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl ExecutionSupervisor {
    pub fn global() -> &'static Self {
        static SUPERVISOR: OnceLock<ExecutionSupervisor> = OnceLock::new();
        SUPERVISOR.get_or_init(|| {
            let (maintenance_tx, _) = broadcast::channel(1);
            ExecutionSupervisor {
                maintenance_tx,
                clock_task: Mutex::new(None),
            }
        })
    }

    pub fn jobs(&self) -> &'static ExecutionJobRegistry {
        ExecutionJobRegistry::global()
    }

    /// Subscribe a backend to the single process-wide maintenance clock.
    ///
    /// This must be called from within a Tokio runtime. The clock is started
    /// exactly once per process runtime even when many terminal sessions
    /// subscribe concurrently. Tests may create and drop several Tokio
    /// runtimes, so a finished clock task is restarted on the current runtime.
    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        let mut clock_task = self
            .clock_task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if clock_task
            .as_ref()
            .is_none_or(tokio::task::JoinHandle::is_finished)
        {
            let maintenance_tx = self.maintenance_tx.clone();
            *clock_task = Some(tokio::spawn(async move {
                let mut ticker = tokio::time::interval(SUPERVISOR_TICK);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    ticker.tick().await;
                    if maintenance_tx.receiver_count() == 0 {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    let _ = maintenance_tx.send(());
                }
            }));
        }
        drop(clock_task);
        self.maintenance_tx.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn subscriptions_share_the_process_wide_clock() {
        let supervisor = ExecutionSupervisor::global();
        let mut first = supervisor.subscribe();
        let mut second = supervisor.subscribe();
        tokio::time::timeout(Duration::from_secs(1), first.recv())
            .await
            .expect("first subscriber should receive maintenance")
            .expect("maintenance channel should remain open");
        tokio::time::timeout(Duration::from_secs(1), second.recv())
            .await
            .expect("second subscriber should receive maintenance")
            .expect("maintenance channel should remain open");
        assert!(
            !supervisor
                .clock_task
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_ref()
                .expect("clock task should be installed")
                .is_finished()
        );
    }
}
