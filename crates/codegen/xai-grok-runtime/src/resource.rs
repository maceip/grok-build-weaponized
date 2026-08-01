use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, Weak};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceClass {
    Engine,
    KvCache,
    Adapter,
    Embedding,
    Reranker,
    NativeJob,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryLimits {
    pub physical_bytes: u64,
    pub soft_bytes: u64,
    pub hard_bytes: u64,
}

impl MemoryLimits {
    pub fn from_physical(physical_bytes: u64) -> Self {
        const GIB: u64 = 1024 * 1024 * 1024;
        let reserved = 16 * GIB;
        let soft_bytes = physical_bytes.saturating_mul(60) / 100;
        let hard_bytes =
            (physical_bytes.saturating_mul(75) / 100).min(physical_bytes.saturating_sub(reserved));
        Self {
            physical_bytes,
            soft_bytes: soft_bytes.min(hard_bytes),
            hard_bytes,
        }
    }

    pub fn detect() -> Self {
        let detected =
            Self::from_physical(physical_memory_bytes().unwrap_or(16 * 1024 * 1024 * 1024));
        std::env::var("GROK_RESOURCE_MAX_MEMORY_BYTES")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .map_or(detected, |hard_cap| detected.with_hard_cap(hard_cap))
    }

    /// Apply a deployment ceiling while preserving an early soft-pressure
    /// signal. The soft limit never rises above the host-derived limit and is
    /// kept below the configured hard ceiling.
    pub fn with_hard_cap(self, hard_cap: u64) -> Self {
        let hard_bytes = self.hard_bytes.min(hard_cap.max(1));
        let soft_bytes = self
            .soft_bytes
            .min(hard_bytes.saturating_mul(80) / 100)
            .min(hard_bytes);
        Self {
            physical_bytes: self.physical_bytes,
            soft_bytes,
            hard_bytes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceSnapshot {
    pub limits: MemoryLimits,
    pub reserved_bytes: u64,
    pub by_class: HashMap<ResourceClass, u64>,
    pub soft_pressure: bool,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ResourceError {
    #[error(
        "runtime memory admission rejected: requested={requested_bytes}, \
         reserved={reserved_bytes}, hard_limit={hard_limit_bytes}"
    )]
    HardLimit {
        requested_bytes: u64,
        reserved_bytes: u64,
        hard_limit_bytes: u64,
    },
}

#[derive(Debug)]
struct ResourceState {
    total: u64,
    by_class: HashMap<ResourceClass, u64>,
}

struct ResourceGovernorInner {
    limits: MemoryLimits,
    state: Mutex<ResourceState>,
    reclaimers: Mutex<HashMap<ResourceClass, Vec<ResourceReclaimer>>>,
}

type ReclaimCallback = dyn Fn() -> u64 + Send + Sync + 'static;

#[derive(Clone)]
struct ResourceReclaimer {
    callback: Arc<ReclaimCallback>,
}

impl std::fmt::Debug for ResourceReclaimer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ResourceReclaimer(..)")
    }
}

impl std::fmt::Debug for ResourceGovernorInner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResourceGovernorInner")
            .field("limits", &self.limits)
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

/// Process-wide byte-accounted runtime capacity governor.
#[derive(Debug, Clone)]
pub struct ResourceGovernor {
    inner: Arc<ResourceGovernorInner>,
}

impl ResourceGovernor {
    pub fn new(limits: MemoryLimits) -> Self {
        Self {
            inner: Arc::new(ResourceGovernorInner {
                limits,
                state: Mutex::new(ResourceState {
                    total: 0,
                    by_class: HashMap::new(),
                }),
                reclaimers: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub fn global() -> &'static Self {
        static GLOBAL: OnceLock<ResourceGovernor> = OnceLock::new();
        GLOBAL.get_or_init(Self::detected)
    }

    pub fn detected() -> Self {
        Self::new(MemoryLimits::detect())
    }

    pub fn limits(&self) -> MemoryLimits {
        self.inner.limits
    }

    pub fn reserve(
        &self,
        class: ResourceClass,
        bytes: u64,
    ) -> Result<ResourceLease, ResourceError> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let next = state.total.saturating_add(bytes);
        if next > self.inner.limits.hard_bytes {
            return Err(ResourceError::HardLimit {
                requested_bytes: bytes,
                reserved_bytes: state.total,
                hard_limit_bytes: self.inner.limits.hard_bytes,
            });
        }
        state.total = next;
        let class_total = state.by_class.entry(class).or_default();
        *class_total = class_total.saturating_add(bytes);
        Ok(ResourceLease {
            owner: Arc::downgrade(&self.inner),
            class,
            bytes,
            released: false,
        })
    }

    pub fn snapshot(&self) -> ResourceSnapshot {
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        ResourceSnapshot {
            limits: self.inner.limits,
            reserved_bytes: state.total,
            by_class: state.by_class.clone(),
            soft_pressure: state.total >= self.inner.limits.soft_bytes,
        }
    }

    /// Register an idempotent, non-blocking pressure callback for a resource
    /// class. Callbacks must never evict active resources and return the number
    /// of bytes they actually released.
    pub fn register_reclaimer(&self, class: ResourceClass, callback: Arc<ReclaimCallback>) {
        self.inner
            .reclaimers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entry(class)
            .or_default()
            .push(ResourceReclaimer { callback });
    }

    pub fn reclaim_class(&self, class: ResourceClass) -> u64 {
        let callbacks = self
            .inner
            .reclaimers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&class)
            .cloned()
            .unwrap_or_default();
        callbacks.into_iter().fold(0_u64, |released, entry| {
            released.saturating_add((entry.callback)())
        })
    }

    /// Run the synchronous front of the pressure order. Native adapters, KV
    /// sessions, and engine replicas are reclaimed asynchronously by
    /// `RuntimeManager` after these registered model resources have yielded.
    pub fn reclaim_auxiliary_models(&self) -> u64 {
        [ResourceClass::Reranker, ResourceClass::Embedding]
            .into_iter()
            .fold(0_u64, |released, class| {
                if !self.snapshot().soft_pressure {
                    return released;
                }
                released.saturating_add(self.reclaim_class(class))
            })
    }
}

/// RAII reservation. Dropping the lease returns its bytes to the governor.
#[derive(Debug)]
pub struct ResourceLease {
    owner: Weak<ResourceGovernorInner>,
    class: ResourceClass,
    bytes: u64,
    released: bool,
}

impl ResourceLease {
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    pub fn class(&self) -> ResourceClass {
        self.class
    }

    pub fn resize(&mut self, bytes: u64) -> Result<(), ResourceError> {
        if self.released || bytes == self.bytes {
            return Ok(());
        }
        let Some(owner) = self.owner.upgrade() else {
            self.bytes = bytes;
            return Ok(());
        };
        let mut state = owner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if bytes > self.bytes {
            let additional = bytes - self.bytes;
            let next = state.total.saturating_add(additional);
            if next > owner.limits.hard_bytes {
                return Err(ResourceError::HardLimit {
                    requested_bytes: additional,
                    reserved_bytes: state.total,
                    hard_limit_bytes: owner.limits.hard_bytes,
                });
            }
            state.total = next;
            let class_total = state.by_class.entry(self.class).or_default();
            *class_total = class_total.saturating_add(additional);
        } else {
            let released = self.bytes - bytes;
            state.total = state.total.saturating_sub(released);
            if let Some(class_total) = state.by_class.get_mut(&self.class) {
                *class_total = class_total.saturating_sub(released);
                if *class_total == 0 {
                    state.by_class.remove(&self.class);
                }
            }
        }
        self.bytes = bytes;
        Ok(())
    }

    pub fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        let Some(owner) = self.owner.upgrade() else {
            return;
        };
        let mut state = owner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.total = state.total.saturating_sub(self.bytes);
        if let Some(value) = state.by_class.get_mut(&self.class) {
            *value = value.saturating_sub(self.bytes);
            if *value == 0 {
                state.by_class.remove(&self.class);
            }
        }
    }
}

impl Drop for ResourceLease {
    fn drop(&mut self) {
        self.release_inner();
    }
}

#[cfg(target_os = "macos")]
fn physical_memory_bytes() -> Option<u64> {
    let mut value = 0_u64;
    let mut size = std::mem::size_of::<u64>();
    let name = b"hw.memsize\0";
    // SAFETY: pointers reference writable storage of the declared size and a
    // static NUL-terminated sysctl name. No output buffer aliasing occurs.
    let status = unsafe {
        libc::sysctlbyname(
            name.as_ptr().cast(),
            (&mut value as *mut u64).cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    (status == 0 && size == std::mem::size_of::<u64>()).then_some(value)
}

#[cfg(all(unix, not(target_os = "macos")))]
fn physical_memory_bytes() -> Option<u64> {
    // SAFETY: sysconf is thread-safe and has no pointer arguments.
    let pages = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) };
    // SAFETY: sysconf is thread-safe and has no pointer arguments.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    (pages > 0 && page_size > 0).then(|| (pages as u64).saturating_mul(page_size as u64))
}

#[cfg(not(unix))]
fn physical_memory_bytes() -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_expected_limits_for_current_class_of_host() {
        let limits = MemoryLimits::from_physical(128 * 1024 * 1024 * 1024);
        assert_eq!(limits.soft_bytes, 76_8_u64 * 1024 * 1024 * 1024 / 10);
        assert_eq!(limits.hard_bytes, 96 * 1024 * 1024 * 1024);
    }

    #[test]
    fn deployment_cap_reduces_hard_and_soft_limits() {
        let limits = MemoryLimits {
            physical_bytes: 1_000,
            soft_bytes: 600,
            hard_bytes: 750,
        }
        .with_hard_cap(500);
        assert_eq!(limits.physical_bytes, 1_000);
        assert_eq!(limits.hard_bytes, 500);
        assert_eq!(limits.soft_bytes, 400);
    }

    #[test]
    fn leases_release_capacity() {
        let governor = ResourceGovernor::new(MemoryLimits {
            physical_bytes: 1000,
            soft_bytes: 600,
            hard_bytes: 750,
        });
        let lease = governor.reserve(ResourceClass::Engine, 500).unwrap();
        assert_eq!(governor.snapshot().reserved_bytes, 500);
        drop(lease);
        assert_eq!(governor.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn hard_limit_is_enforced_without_partial_reservation() {
        let governor = ResourceGovernor::new(MemoryLimits {
            physical_bytes: 1000,
            soft_bytes: 600,
            hard_bytes: 750,
        });
        let _lease = governor.reserve(ResourceClass::Engine, 700).unwrap();
        assert!(matches!(
            governor.reserve(ResourceClass::Adapter, 51),
            Err(ResourceError::HardLimit { .. })
        ));
        assert_eq!(governor.snapshot().reserved_bytes, 700);
    }

    #[test]
    fn resizing_tracks_growth_shrink_and_rejects_overcommit_atomically() {
        let governor = ResourceGovernor::new(MemoryLimits {
            physical_bytes: 1000,
            soft_bytes: 600,
            hard_bytes: 750,
        });
        let mut lease = governor.reserve(ResourceClass::Engine, 400).unwrap();
        lease.resize(650).unwrap();
        assert_eq!(governor.snapshot().reserved_bytes, 650);
        assert!(matches!(
            lease.resize(751),
            Err(ResourceError::HardLimit { .. })
        ));
        assert_eq!(lease.bytes(), 650);
        assert_eq!(governor.snapshot().reserved_bytes, 650);
        lease.resize(200).unwrap();
        assert_eq!(governor.snapshot().reserved_bytes, 200);
    }

    #[test]
    fn auxiliary_reclaimers_run_in_pressure_order_and_release_accounting() {
        let governor = ResourceGovernor::new(MemoryLimits {
            physical_bytes: 1000,
            soft_bytes: 600,
            hard_bytes: 900,
        });
        let embedding = Arc::new(Mutex::new(Some(
            governor.reserve(ResourceClass::Embedding, 650).unwrap(),
        )));
        let embedding_for_reclaimer = Arc::clone(&embedding);
        governor.register_reclaimer(
            ResourceClass::Embedding,
            Arc::new(move || {
                embedding_for_reclaimer
                    .lock()
                    .unwrap()
                    .take()
                    .map_or(0, |lease| {
                        let bytes = lease.bytes();
                        drop(lease);
                        bytes
                    })
            }),
        );
        assert_eq!(governor.reclaim_auxiliary_models(), 650);
        assert_eq!(governor.snapshot().reserved_bytes, 0);
    }
}
