use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::io::{self, Read};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::resource::{ResourceClass, ResourceError, ResourceGovernor, ResourceLease};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdapterDtype {
    F16,
    Bf16,
    F32,
    I8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterDescriptor {
    pub adapter_id: String,
    pub revision: String,
    pub content_hash: String,
    pub base_model_hash: String,
    pub path: PathBuf,
    pub rank: u32,
    pub dtype: AdapterDtype,
    pub byte_size: u64,
    /// Immutable tensor manifest reported by the adapter conversion pipeline.
    /// The native load/probe remains authoritative, but validating this list
    /// catches incomplete or mismatched artifacts before device allocation.
    #[serde(default)]
    pub tensor_names: Vec<String>,
}

impl AdapterDescriptor {
    pub fn immutable_id(&self) -> String {
        format!("{}@{}", self.adapter_id, self.revision)
    }

    pub fn validate(&self) -> Result<(), AdapterError> {
        if self.adapter_id.trim().is_empty() || self.revision.trim().is_empty() {
            return Err(AdapterError::InvalidDescriptor(
                "adapter_id and revision must be non-empty".to_string(),
            ));
        }
        if self.base_model_hash.trim().is_empty() || self.content_hash.trim().is_empty() {
            return Err(AdapterError::InvalidDescriptor(
                "base_model_hash and content_hash must be non-empty".to_string(),
            ));
        }
        if !self.path.is_absolute() {
            return Err(AdapterError::InvalidDescriptor(
                "adapter path must be absolute".to_string(),
            ));
        }
        if self.rank == 0 {
            return Err(AdapterError::InvalidDescriptor(
                "adapter rank must be greater than zero".to_string(),
            ));
        }
        if self.tensor_names.is_empty() {
            return Err(AdapterError::InvalidDescriptor(
                "adapter tensor manifest must be non-empty".to_string(),
            ));
        }
        let mut unique = HashSet::with_capacity(self.tensor_names.len());
        let mut has_a = false;
        let mut has_b = false;
        for tensor in &self.tensor_names {
            let tensor = tensor.trim();
            if tensor.is_empty() || tensor.contains('\0') {
                return Err(AdapterError::InvalidDescriptor(
                    "adapter tensor names must be non-empty and NUL-free".to_string(),
                ));
            }
            if !unique.insert(tensor) {
                return Err(AdapterError::InvalidDescriptor(format!(
                    "adapter tensor manifest contains duplicate name: {tensor}"
                )));
            }
            let normalized = tensor.to_ascii_lowercase();
            has_a |= normalized.contains("lora_a")
                || normalized.contains("_a_prime_weight_")
                || normalized.contains("_prime_left_")
                || normalized.ends_with(".a");
            has_b |= normalized.contains("lora_b")
                || normalized.contains("_b_prime_weight_")
                || normalized.contains("_prime_right_")
                || normalized.ends_with(".b");
        }
        if !has_a || !has_b {
            return Err(AdapterError::InvalidDescriptor(
                "adapter tensor manifest must contain both LoRA A and LoRA B tensors".to_string(),
            ));
        }
        let metadata = std::fs::metadata(&self.path).map_err(AdapterError::Io)?;
        if !metadata.is_file() {
            return Err(AdapterError::InvalidDescriptor(
                "adapter path is not a regular file".to_string(),
            ));
        }
        if metadata.len() != self.byte_size {
            return Err(AdapterError::SizeMismatch {
                expected: self.byte_size,
                actual: metadata.len(),
            });
        }
        let actual = hash_file(&self.path)?;
        if actual != self.content_hash {
            return Err(AdapterError::HashMismatch {
                expected: self.content_hash.clone(),
                actual,
            });
        }
        Ok(())
    }
}

pub fn hash_artifact(path: &std::path::Path) -> Result<String, AdapterError> {
    if path.is_file() {
        return hash_file(&path.to_path_buf());
    }
    let mut hasher = blake3::Hasher::new();
    hash_artifact_entry(path, path, &mut hasher)?;
    Ok(hasher.finalize().to_hex().to_string())
}

fn hash_artifact_entry(
    root: &std::path::Path,
    path: &std::path::Path,
    hasher: &mut blake3::Hasher,
) -> Result<(), AdapterError> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Err(AdapterError::InvalidDescriptor(format!(
            "model artifact contains a symbolic link: {}",
            path.display()
        )));
    }
    if metadata.is_dir() {
        let mut children = std::fs::read_dir(path)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<Result<Vec<_>, _>>()?;
        children.sort();
        for child in children {
            hash_artifact_entry(root, &child, hasher)?;
        }
        return Ok(());
    }
    if !metadata.is_file() {
        return Err(AdapterError::InvalidDescriptor(format!(
            "model artifact contains an unsupported entry: {}",
            path.display()
        )));
    }
    let relative = path.strip_prefix(root).unwrap_or(path);
    hasher.update(relative.as_os_str().as_encoded_bytes());
    hasher.update(&metadata.len().to_le_bytes());
    let mut file = File::open(path)?;
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterBinding {
    pub adapter_id: String,
    pub revision: String,
}

impl AdapterBinding {
    pub fn immutable_id(&self) -> String {
        format!("{}@{}", self.adapter_id, self.revision)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdapterResidency {
    Host,
    Device,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdapterState {
    Absent,
    Loading,
    HostResident,
    DeviceResident,
    InUse { leases: u32 },
    Evicting,
    Failed { message: String },
}

#[derive(Debug, Error)]
pub enum AdapterError {
    #[error("invalid adapter descriptor: {0}")]
    InvalidDescriptor(String),
    #[error("adapter file error: {0}")]
    Io(#[from] io::Error),
    #[error("adapter size mismatch: expected={expected}, actual={actual}")]
    SizeMismatch { expected: u64, actual: u64 },
    #[error("adapter hash mismatch: expected={expected}, actual={actual}")]
    HashMismatch { expected: String, actual: String },
    #[error("adapter {0} is not resident")]
    NotResident(String),
    #[error("adapter {0} is currently leased")]
    InUse(String),
    #[error("adapter {0} is already registered with different metadata")]
    DescriptorConflict(String),
    #[error("adapter memory admission failed: {0}")]
    Resource(#[from] ResourceError),
}

struct AdapterRecord {
    descriptor: AdapterDescriptor,
    native_id: u32,
    residency: AdapterResidency,
    state: AdapterState,
    active_leases: u32,
    last_used: u64,
    use_count: u64,
    predicted_use: u64,
    memory: Option<ResourceLease>,
}

struct AdapterManagerInner {
    records: Mutex<HashMap<String, AdapterRecord>>,
    clock: AtomicU64,
    next_native_id: AtomicU32,
    governor: ResourceGovernor,
}

/// Byte-accounted, lease-safe adapter residency manager.
#[derive(Clone)]
pub struct AdapterManager {
    inner: Arc<AdapterManagerInner>,
}

impl AdapterManager {
    pub fn new(governor: ResourceGovernor) -> Self {
        Self {
            inner: Arc::new(AdapterManagerInner {
                records: Mutex::new(HashMap::new()),
                clock: AtomicU64::new(1),
                next_native_id: AtomicU32::new(1),
                governor,
            }),
        }
    }

    pub fn register_host_resident(
        &self,
        descriptor: AdapterDescriptor,
    ) -> Result<u32, AdapterError> {
        descriptor.validate()?;
        let key = descriptor.immutable_id();
        let records = self
            .inner
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(existing) = records.get(&key) {
            if existing.descriptor == descriptor {
                return Ok(existing.native_id);
            }
            return Err(AdapterError::DescriptorConflict(key));
        }
        drop(records);

        let memory = self
            .inner
            .governor
            .reserve(ResourceClass::Adapter, descriptor.byte_size)?;
        let native_id = self.inner.next_native_id.fetch_add(1, Ordering::Relaxed);
        let now = self.inner.clock.fetch_add(1, Ordering::Relaxed);
        let mut records = self
            .inner
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        records.insert(
            key,
            AdapterRecord {
                descriptor,
                native_id,
                residency: AdapterResidency::Host,
                state: AdapterState::HostResident,
                active_leases: 0,
                last_used: now,
                use_count: 0,
                predicted_use: 0,
                memory: Some(memory),
            },
        );
        Ok(native_id)
    }

    pub fn mark_device_resident(&self, binding: &AdapterBinding) -> Result<(), AdapterError> {
        let key = binding.immutable_id();
        let mut records = self
            .inner
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let record = records
            .get_mut(&key)
            .ok_or_else(|| AdapterError::NotResident(key.clone()))?;
        record.residency = AdapterResidency::Device;
        record.state = if record.active_leases == 0 {
            AdapterState::DeviceResident
        } else {
            AdapterState::InUse {
                leases: record.active_leases,
            }
        };
        Ok(())
    }

    pub fn predict_use(&self, binding: &AdapterBinding) {
        let key = binding.immutable_id();
        let mut records = self
            .inner
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(record) = records.get_mut(&key) {
            record.predicted_use = record.predicted_use.saturating_add(1);
        }
    }

    pub fn acquire(&self, binding: &AdapterBinding) -> Result<AdapterLease, AdapterError> {
        let key = binding.immutable_id();
        let mut records = self
            .inner
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let record = records
            .get_mut(&key)
            .ok_or_else(|| AdapterError::NotResident(key.clone()))?;
        if matches!(
            record.state,
            AdapterState::Loading
                | AdapterState::Absent
                | AdapterState::Evicting
                | AdapterState::Failed { .. }
        ) {
            return Err(AdapterError::NotResident(key));
        }
        record.active_leases = record.active_leases.saturating_add(1);
        record.use_count = record.use_count.saturating_add(1);
        record.last_used = self.inner.clock.fetch_add(1, Ordering::Relaxed);
        record.predicted_use = record.predicted_use.saturating_sub(1);
        record.state = AdapterState::InUse {
            leases: record.active_leases,
        };
        Ok(AdapterLease {
            owner: Arc::downgrade(&self.inner),
            key,
            native_id: record.native_id,
            released: false,
        })
    }

    pub fn state(&self, binding: &AdapterBinding) -> AdapterState {
        self.inner
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&binding.immutable_id())
            .map_or(AdapterState::Absent, |record| record.state.clone())
    }

    pub fn descriptor(&self, binding: &AdapterBinding) -> Option<AdapterDescriptor> {
        self.inner
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&binding.immutable_id())
            .map(|record| record.descriptor.clone())
    }

    pub fn resident_count(&self) -> u32 {
        u32::try_from(
            self.inner
                .records
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .len(),
        )
        .unwrap_or(u32::MAX)
    }

    pub fn resident_bytes(&self) -> u64 {
        self.inner
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .map(|record| record.descriptor.byte_size)
            .fold(0_u64, u64::saturating_add)
    }

    pub fn bindings_for_adapter(&self, adapter_id: &str) -> Vec<AdapterBinding> {
        self.inner
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .filter(|record| record.descriptor.adapter_id == adapter_id)
            .map(|record| AdapterBinding {
                adapter_id: record.descriptor.adapter_id.clone(),
                revision: record.descriptor.revision.clone(),
            })
            .collect()
    }

    /// Returns the lowest-value unleased adapter without changing its state.
    ///
    /// The runtime supervisor must perform and confirm the native unload before
    /// calling `commit_eviction`; this manager never frees only its accounting
    /// record under memory pressure.
    pub fn next_eviction_candidate(&self) -> Option<AdapterBinding> {
        self.inner
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .filter(|record| {
                record.active_leases == 0
                    && !matches!(
                        record.state,
                        AdapterState::Loading
                            | AdapterState::Evicting
                            | AdapterState::Failed { .. }
                    )
            })
            .min_by_key(|record| {
                (
                    record.predicted_use,
                    record.use_count,
                    record.last_used,
                    std::cmp::Reverse(record.descriptor.byte_size),
                )
            })
            .map(|record| AdapterBinding {
                adapter_id: record.descriptor.adapter_id.clone(),
                revision: record.descriptor.revision.clone(),
            })
    }

    pub fn evict(&self, binding: &AdapterBinding) -> Result<AdapterDescriptor, AdapterError> {
        let key = binding.immutable_id();
        let descriptor = self.begin_eviction(binding)?;
        self.commit_eviction_key(&key)?;
        Ok(descriptor)
    }

    pub fn begin_eviction(
        &self,
        binding: &AdapterBinding,
    ) -> Result<AdapterDescriptor, AdapterError> {
        let key = binding.immutable_id();
        let mut records = self
            .inner
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let record = records
            .get_mut(&key)
            .ok_or_else(|| AdapterError::NotResident(key.to_owned()))?;
        if record.active_leases != 0 {
            return Err(AdapterError::InUse(key.to_owned()));
        }
        record.state = AdapterState::Evicting;
        Ok(record.descriptor.clone())
    }

    pub fn commit_eviction(&self, binding: &AdapterBinding) -> Result<(), AdapterError> {
        self.commit_eviction_key(&binding.immutable_id())
    }

    fn commit_eviction_key(&self, key: &str) -> Result<(), AdapterError> {
        let mut records = self
            .inner
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let record = records
            .get(key)
            .ok_or_else(|| AdapterError::NotResident(key.to_owned()))?;
        if !matches!(record.state, AdapterState::Evicting) {
            return Err(AdapterError::InUse(key.to_owned()));
        }
        let mut record = records
            .remove(key)
            .expect("adapter record existed immediately before removal");
        drop(record.memory.take());
        Ok(())
    }

    pub fn rollback_eviction(&self, binding: &AdapterBinding) {
        let mut records = self
            .inner
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(record) = records.get_mut(&binding.immutable_id())
            && matches!(record.state, AdapterState::Evicting)
        {
            record.state = match record.residency {
                AdapterResidency::Host => AdapterState::HostResident,
                AdapterResidency::Device => AdapterState::DeviceResident,
            };
        }
    }
}

pub struct AdapterLease {
    owner: Weak<AdapterManagerInner>,
    key: String,
    native_id: u32,
    released: bool,
}

impl AdapterLease {
    pub fn native_id(&self) -> u32 {
        self.native_id
    }

    pub fn immutable_id(&self) -> &str {
        &self.key
    }

    fn release_inner(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        let Some(owner) = self.owner.upgrade() else {
            return;
        };
        let mut records = owner
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(record) = records.get_mut(&self.key) else {
            return;
        };
        record.active_leases = record.active_leases.saturating_sub(1);
        record.state = if record.active_leases == 0 {
            match record.residency {
                AdapterResidency::Host => AdapterState::HostResident,
                AdapterResidency::Device => AdapterState::DeviceResident,
            }
        } else {
            AdapterState::InUse {
                leases: record.active_leases,
            }
        };
    }
}

impl Drop for AdapterLease {
    fn drop(&mut self) {
        self.release_inner();
    }
}

fn hash_file(path: &PathBuf) -> Result<String, AdapterError> {
    let mut file = File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn descriptor(path: PathBuf, id: &str) -> AdapterDescriptor {
        let metadata = std::fs::metadata(&path).unwrap();
        AdapterDescriptor {
            adapter_id: id.to_string(),
            revision: "v1".to_string(),
            content_hash: hash_file(&path).unwrap(),
            base_model_hash: "base-v1".to_string(),
            path,
            rank: 16,
            dtype: AdapterDtype::F16,
            byte_size: metadata.len(),
            tensor_names: vec![
                "layers.0.attention.q_proj.lora_a".to_string(),
                "layers.0.attention.q_proj.lora_b".to_string(),
            ],
        }
    }

    #[test]
    fn leases_prevent_eviction_and_release_cleanly() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(temp.path(), vec![7_u8; 128]).unwrap();
        let governor = ResourceGovernor::new(crate::resource::MemoryLimits {
            physical_bytes: 1024,
            soft_bytes: 512,
            hard_bytes: 512,
        });
        let manager = AdapterManager::new(governor);
        let descriptor = descriptor(temp.path().to_path_buf(), "adapter");
        manager.register_host_resident(descriptor).unwrap();
        let binding = AdapterBinding {
            adapter_id: "adapter".to_string(),
            revision: "v1".to_string(),
        };
        let lease = manager.acquire(&binding).unwrap();
        assert!(matches!(
            manager.evict(&binding),
            Err(AdapterError::InUse(_))
        ));
        drop(lease);
        assert!(manager.evict(&binding).is_ok());
        assert_eq!(manager.state(&binding), AdapterState::Absent);
    }

    #[test]
    fn rejects_hash_mismatch() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(temp.path(), b"adapter").unwrap();
        let mut descriptor = descriptor(temp.path().to_path_buf(), "adapter");
        descriptor.content_hash = "bad".to_string();
        assert!(matches!(
            descriptor.validate(),
            Err(AdapterError::HashMismatch { .. })
        ));
    }

    #[test]
    fn accepts_litert_native_left_right_tensor_pairs() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(temp.path(), b"adapter").unwrap();
        let mut descriptor = descriptor(temp.path().to_path_buf(), "adapter");
        descriptor.tensor_names = vec![
            "query_w_prime_left_0".to_owned(),
            "query_w_prime_right_0".to_owned(),
        ];
        descriptor.validate().unwrap();
    }

    #[test]
    fn pressure_requires_confirmed_native_eviction() {
        let first = tempfile::NamedTempFile::new().unwrap();
        let second = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(first.path(), vec![1_u8; 128]).unwrap();
        std::fs::write(second.path(), vec![2_u8; 128]).unwrap();
        let governor = ResourceGovernor::new(crate::resource::MemoryLimits {
            physical_bytes: 512,
            soft_bytes: 192,
            hard_bytes: 192,
        });
        let manager = AdapterManager::new(governor);
        manager
            .register_host_resident(descriptor(first.path().to_path_buf(), "team@adapter"))
            .unwrap();
        assert!(matches!(
            manager.register_host_resident(descriptor(second.path().to_path_buf(), "replacement")),
            Err(AdapterError::Resource(_))
        ));
        assert_eq!(manager.resident_count(), 1);
        assert!(matches!(
            manager.state(&AdapterBinding {
                adapter_id: "team@adapter".to_owned(),
                revision: "v1".to_owned(),
            }),
            AdapterState::HostResident
        ));
        assert_eq!(
            manager.next_eviction_candidate(),
            Some(AdapterBinding {
                adapter_id: "team@adapter".to_owned(),
                revision: "v1".to_owned(),
            })
        );
    }
}
