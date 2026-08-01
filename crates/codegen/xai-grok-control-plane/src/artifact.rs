use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Read as _, Seek as _, Write as _};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use xai_grok_protocol::{ArtifactId, ArtifactUploadId};

const DEFAULT_MAX_ARTIFACT_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const DEFAULT_MAX_STORE_BYTES: u64 = 128 * 1024 * 1024 * 1024;
const MAX_READ_BYTES: usize = 8 * 1024 * 1024;
const MAX_UPLOAD_CHUNK_BYTES: usize = 8 * 1024 * 1024;
const UPLOAD_LEASE_MS: u64 = 24 * 60 * 60 * 1_000;

#[derive(Clone, Debug)]
pub struct ArtifactStoreConfig {
    pub root: PathBuf,
    pub maximum_artifact_bytes: u64,
    pub maximum_store_bytes: u64,
}

impl ArtifactStoreConfig {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            maximum_artifact_bytes: DEFAULT_MAX_ARTIFACT_BYTES,
            maximum_store_bytes: DEFAULT_MAX_STORE_BYTES,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ArtifactDescriptor {
    pub artifact_id: ArtifactId,
    pub content_hash: String,
    pub media_type: String,
    pub byte_size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ArtifactUploadLease {
    pub upload_id: ArtifactUploadId,
    pub media_type: String,
    pub expected_bytes: u64,
    pub expected_content_hash: Option<String>,
    pub next_offset: u64,
    pub expires_unix_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ArtifactUploadProgress {
    pub upload_id: ArtifactUploadId,
    pub next_offset: u64,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct UploadMetadata {
    upload_id: ArtifactUploadId,
    media_type: String,
    expected_bytes: u64,
    expected_content_hash: Option<String>,
    next_offset: u64,
    expires_unix_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UploadLifecycle {
    Active,
    Finalizing,
    Aborted,
}

struct ActiveUpload {
    metadata: UploadMetadata,
    lifecycle: UploadLifecycle,
}

type RecoveredUploads = (HashMap<ArtifactUploadId, Arc<Mutex<ActiveUpload>>>, u64);

#[derive(Default)]
struct CapacityUsage {
    stored: u64,
    reserved_uploads: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum ArtifactError {
    #[error("artifact store I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("artifact is {actual} bytes; maximum is {maximum}")]
    ArtifactTooLarge { actual: u64, maximum: u64 },
    #[error("artifact store capacity would exceed {maximum} bytes")]
    StoreFull { maximum: u64 },
    #[error("invalid artifact id: {0}")]
    InvalidId(String),
    #[error("invalid artifact media type: {0}")]
    InvalidMediaType(String),
    #[error("artifact not found: {0}")]
    NotFound(String),
    #[error("artifact content hash mismatch")]
    HashMismatch,
    #[error("artifact upload not found: {0}")]
    UploadNotFound(String),
    #[error("artifact upload offset mismatch: expected {expected}, received {actual}")]
    OffsetMismatch { expected: u64, actual: u64 },
    #[error("artifact upload chunk is {actual} bytes; maximum is {maximum}")]
    ChunkTooLarge { actual: usize, maximum: usize },
    #[error("artifact upload is incomplete: expected {expected} bytes, received {actual}")]
    UploadIncomplete { expected: u64, actual: u64 },
    #[error("artifact upload is already being finalized")]
    UploadFinalizing,
    #[error("artifact upload expired")]
    UploadExpired,
}

pub struct ArtifactStore {
    config: ArtifactStoreConfig,
    capacity: Mutex<CapacityUsage>,
    uploads: Mutex<HashMap<ArtifactUploadId, Arc<Mutex<ActiveUpload>>>>,
}

impl ArtifactStore {
    pub fn open(config: ArtifactStoreConfig) -> Result<Self, ArtifactError> {
        std::fs::create_dir_all(config.root.join("objects"))?;
        std::fs::create_dir_all(config.root.join("metadata"))?;
        std::fs::create_dir_all(config.root.join("uploads"))?;
        let stored_bytes = calculate_store_bytes(&config.root.join("objects"))?;
        if stored_bytes > config.maximum_store_bytes {
            return Err(ArtifactError::StoreFull {
                maximum: config.maximum_store_bytes,
            });
        }
        let (uploads, reserved_uploads) = recover_uploads(
            &config,
            config.maximum_store_bytes.saturating_sub(stored_bytes),
        )?;
        Ok(Self {
            config,
            capacity: Mutex::new(CapacityUsage {
                stored: stored_bytes,
                reserved_uploads,
            }),
            uploads: Mutex::new(uploads),
        })
    }

    pub fn put(
        &self,
        media_type: impl Into<String>,
        bytes: &[u8],
    ) -> Result<ArtifactDescriptor, ArtifactError> {
        let byte_size = bytes.len() as u64;
        if byte_size > self.config.maximum_artifact_bytes {
            return Err(ArtifactError::ArtifactTooLarge {
                actual: byte_size,
                maximum: self.config.maximum_artifact_bytes,
            });
        }
        let media_type = media_type.into();
        if media_type.trim().is_empty()
            || media_type.len() > 255
            || media_type.as_bytes().contains(&0)
        {
            return Err(ArtifactError::InvalidMediaType(media_type));
        }
        let content_hash = blake3::hash(bytes).to_hex().to_string();
        let mut identity = blake3::Hasher::new();
        identity.update(&(media_type.len() as u64).to_le_bytes());
        identity.update(media_type.as_bytes());
        identity.update(bytes);
        let artifact_id = ArtifactId::from_string(format!("art_{}", identity.finalize().to_hex()));
        let object_path = self.object_path(&artifact_id)?;
        if object_path.exists() {
            verify_object(&object_path, &content_hash, byte_size)?;
            let descriptor = ArtifactDescriptor {
                artifact_id,
                content_hash,
                media_type,
                byte_size,
            };
            if !self.metadata_path(&descriptor.artifact_id)?.exists() {
                self.write_metadata(&descriptor)?;
            }
            return Ok(descriptor);
        }

        let parent = object_path.parent().expect("object path has parent");
        std::fs::create_dir_all(parent)?;
        let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
        temporary.write_all(bytes)?;
        temporary.as_file().sync_all()?;
        self.reserve(byte_size)?;
        match temporary.persist_noclobber(&object_path) {
            Ok(_) => {}
            Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                self.release_stored(byte_size);
                verify_object(&object_path, &content_hash, byte_size)?;
            }
            Err(error) => {
                self.release_stored(byte_size);
                return Err(ArtifactError::Io(error.error));
            }
        }
        let descriptor = ArtifactDescriptor {
            artifact_id,
            content_hash,
            media_type,
            byte_size,
        };
        self.write_metadata(&descriptor)?;
        Ok(descriptor)
    }

    /// Import a daemon-local regular file without materializing it in memory or
    /// carrying it through the control protocol. The source is copied into a
    /// private temporary object while its content and typed identity hashes are
    /// computed, so subsequent source mutation cannot corrupt the immutable
    /// content-addressed object.
    pub fn put_file(
        &self,
        media_type: impl Into<String>,
        source: &Path,
    ) -> Result<ArtifactDescriptor, ArtifactError> {
        ensure_regular_file(source)?;
        let byte_size = std::fs::metadata(source)?.len();
        if byte_size > self.config.maximum_artifact_bytes {
            return Err(ArtifactError::ArtifactTooLarge {
                actual: byte_size,
                maximum: self.config.maximum_artifact_bytes,
            });
        }
        let media_type = media_type.into();
        validate_media_type(&media_type)?;
        self.reserve(byte_size)?;

        let result = (|| {
            let staging = self.config.root.join("staging");
            std::fs::create_dir_all(&staging)?;
            let mut temporary = tempfile::NamedTempFile::new_in(&staging)?;
            let mut input = OpenOptions::new().read(true).open(source)?;
            let mut content = blake3::Hasher::new();
            let mut identity = blake3::Hasher::new();
            identity.update(&(media_type.len() as u64).to_le_bytes());
            identity.update(media_type.as_bytes());
            let mut copied = 0_u64;
            let mut buffer = [0_u8; 1024 * 1024];
            loop {
                let read = input.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                copied = copied.saturating_add(read as u64);
                if copied > self.config.maximum_artifact_bytes {
                    return Err(ArtifactError::ArtifactTooLarge {
                        actual: copied,
                        maximum: self.config.maximum_artifact_bytes,
                    });
                }
                content.update(&buffer[..read]);
                identity.update(&buffer[..read]);
                temporary.write_all(&buffer[..read])?;
            }
            if copied != byte_size {
                return Err(ArtifactError::Io(std::io::Error::other(format!(
                    "artifact source changed size while being imported: expected {byte_size}, copied {copied}"
                ))));
            }
            temporary.as_file().sync_all()?;
            let content_hash = content.finalize().to_hex().to_string();
            let artifact_id =
                ArtifactId::from_string(format!("art_{}", identity.finalize().to_hex()));
            let object_path = self.object_path(&artifact_id)?;
            let parent = object_path.parent().expect("object path has parent");
            std::fs::create_dir_all(parent)?;
            let installed = match temporary.persist_noclobber(&object_path) {
                Ok(_) => true,
                Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                    verify_object(&object_path, &content_hash, byte_size)?;
                    false
                }
                Err(error) => return Err(ArtifactError::Io(error.error)),
            };
            let descriptor = ArtifactDescriptor {
                artifact_id,
                content_hash,
                media_type,
                byte_size,
            };
            if let Err(error) = self.write_metadata(&descriptor) {
                if installed {
                    remove_if_exists(&object_path);
                }
                return Err(error);
            }
            Ok((descriptor, installed))
        })();

        match result {
            Ok((descriptor, true)) => Ok(descriptor),
            Ok((descriptor, false)) => {
                self.release_stored(byte_size);
                Ok(descriptor)
            }
            Err(error) => {
                self.release_stored(byte_size);
                Err(error)
            }
        }
    }

    pub fn begin_upload(
        &self,
        media_type: impl Into<String>,
        expected_bytes: u64,
        expected_content_hash: Option<String>,
    ) -> Result<ArtifactUploadLease, ArtifactError> {
        let media_type = media_type.into();
        validate_media_type(&media_type)?;
        if expected_bytes > self.config.maximum_artifact_bytes {
            return Err(ArtifactError::ArtifactTooLarge {
                actual: expected_bytes,
                maximum: self.config.maximum_artifact_bytes,
            });
        }
        let expected_content_hash = expected_content_hash
            .map(|hash| hash.to_ascii_lowercase())
            .map(|hash| {
                if hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                    Ok(hash)
                } else {
                    Err(ArtifactError::HashMismatch)
                }
            })
            .transpose()?;
        self.cleanup_expired_uploads()?;
        self.reserve_upload(expected_bytes)?;
        let upload_id = ArtifactUploadId::new();
        let expires_unix_ms = now_unix_ms().saturating_add(UPLOAD_LEASE_MS);
        let metadata = UploadMetadata {
            upload_id: upload_id.clone(),
            media_type,
            expected_bytes,
            expected_content_hash,
            next_offset: 0,
            expires_unix_ms,
        };
        let result = (|| {
            let part_path = self.upload_part_path(&upload_id)?;
            OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&part_path)?
                .sync_all()?;
            self.write_upload_metadata(&metadata)?;
            self.uploads
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(
                    upload_id.clone(),
                    Arc::new(Mutex::new(ActiveUpload {
                        metadata: metadata.clone(),
                        lifecycle: UploadLifecycle::Active,
                    })),
                );
            Ok(ArtifactUploadLease {
                upload_id: upload_id.clone(),
                media_type: metadata.media_type.clone(),
                expected_bytes: metadata.expected_bytes,
                expected_content_hash: metadata.expected_content_hash.clone(),
                next_offset: 0,
                expires_unix_ms,
            })
        })();
        if result.is_err() {
            self.release_upload(expected_bytes);
            if let Ok(path) = self.upload_part_path(&upload_id) {
                remove_if_exists(&path);
            }
            if let Ok(path) = self.upload_metadata_path(&upload_id) {
                remove_if_exists(&path);
            }
        }
        result
    }

    pub fn inspect_upload(
        &self,
        upload_id: &ArtifactUploadId,
    ) -> Result<ArtifactUploadLease, ArtifactError> {
        let upload = self.upload(upload_id)?;
        let upload = upload
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.ensure_upload_active(&upload)?;
        Ok(ArtifactUploadLease {
            upload_id: upload.metadata.upload_id.clone(),
            media_type: upload.metadata.media_type.clone(),
            expected_bytes: upload.metadata.expected_bytes,
            expected_content_hash: upload.metadata.expected_content_hash.clone(),
            next_offset: upload.metadata.next_offset,
            expires_unix_ms: upload.metadata.expires_unix_ms,
        })
    }

    pub fn upload_chunk(
        &self,
        upload_id: &ArtifactUploadId,
        offset: u64,
        bytes: &[u8],
    ) -> Result<ArtifactUploadProgress, ArtifactError> {
        if bytes.len() > MAX_UPLOAD_CHUNK_BYTES {
            return Err(ArtifactError::ChunkTooLarge {
                actual: bytes.len(),
                maximum: MAX_UPLOAD_CHUNK_BYTES,
            });
        }
        let upload = self.upload(upload_id)?;
        let mut upload = upload
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.ensure_upload_active(&upload)?;
        if offset != upload.metadata.next_offset {
            return Err(ArtifactError::OffsetMismatch {
                expected: upload.metadata.next_offset,
                actual: offset,
            });
        }
        let next_offset = offset.saturating_add(bytes.len() as u64);
        if next_offset > upload.metadata.expected_bytes {
            return Err(ArtifactError::ArtifactTooLarge {
                actual: next_offset,
                maximum: upload.metadata.expected_bytes,
            });
        }
        let part_path = self.upload_part_path(upload_id)?;
        ensure_regular_file(&part_path)?;
        let mut file = OpenOptions::new().append(true).open(&part_path)?;
        file.write_all(bytes)?;
        file.sync_data()?;
        upload.metadata.next_offset = next_offset;
        self.write_upload_metadata(&upload.metadata)?;
        Ok(ArtifactUploadProgress {
            upload_id: upload_id.clone(),
            next_offset,
        })
    }

    pub fn commit_upload(
        &self,
        upload_id: &ArtifactUploadId,
    ) -> Result<ArtifactDescriptor, ArtifactError> {
        let upload = self.upload(upload_id)?;
        let mut upload = upload
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.ensure_upload_active(&upload)?;
        if upload.metadata.next_offset != upload.metadata.expected_bytes {
            return Err(ArtifactError::UploadIncomplete {
                expected: upload.metadata.expected_bytes,
                actual: upload.metadata.next_offset,
            });
        }
        upload.lifecycle = UploadLifecycle::Finalizing;
        let result = self.commit_upload_locked(&upload.metadata);
        if result.is_err() {
            upload.lifecycle = UploadLifecycle::Active;
            return result;
        }
        let descriptor = result?;
        upload.lifecycle = UploadLifecycle::Aborted;
        drop(upload);
        self.uploads
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(upload_id);
        Ok(descriptor)
    }

    pub fn abort_upload(&self, upload_id: &ArtifactUploadId) -> Result<(), ArtifactError> {
        let upload = self.upload(upload_id)?;
        let mut upload = upload
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if upload.lifecycle == UploadLifecycle::Finalizing {
            return Err(ArtifactError::UploadFinalizing);
        }
        if upload.lifecycle == UploadLifecycle::Aborted {
            return Err(ArtifactError::UploadNotFound(upload_id.0.clone()));
        }
        upload.lifecycle = UploadLifecycle::Aborted;
        let expected_bytes = upload.metadata.expected_bytes;
        drop(upload);
        self.uploads
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(upload_id);
        remove_if_exists(&self.upload_part_path(upload_id)?);
        remove_if_exists(&self.upload_metadata_path(upload_id)?);
        self.release_upload(expected_bytes);
        Ok(())
    }

    pub fn descriptor(
        &self,
        artifact_id: &ArtifactId,
    ) -> Result<ArtifactDescriptor, ArtifactError> {
        let metadata_path = self.metadata_path(artifact_id)?;
        if !metadata_path.exists() {
            return Err(ArtifactError::NotFound(artifact_id.0.clone()));
        }
        Ok(
            serde_json::from_slice(&std::fs::read(metadata_path)?)
                .map_err(std::io::Error::other)?,
        )
    }

    pub fn read_range(
        &self,
        artifact_id: &ArtifactId,
        cursor: u64,
        limit: usize,
    ) -> Result<(Vec<u8>, Option<u64>), ArtifactError> {
        let limit = limit.min(MAX_READ_BYTES);
        let path = self.object_path(artifact_id)?;
        let mut file = OpenOptions::new().read(true).open(&path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                ArtifactError::NotFound(artifact_id.0.clone())
            } else {
                ArtifactError::Io(error)
            }
        })?;
        let length = file.metadata()?.len();
        let cursor = cursor.min(length);
        file.seek(std::io::SeekFrom::Start(cursor))?;
        let mut bytes = Vec::with_capacity(limit.min(length.saturating_sub(cursor) as usize));
        file.take(limit as u64).read_to_end(&mut bytes)?;
        let next = cursor + bytes.len() as u64;
        Ok((bytes, (next < length).then_some(next)))
    }

    pub fn stored_bytes(&self) -> u64 {
        self.capacity
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .stored
    }

    fn reserve(&self, byte_size: u64) -> Result<(), ArtifactError> {
        let maximum = self.config.maximum_store_bytes;
        let mut capacity = self
            .capacity
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let next = capacity
            .stored
            .checked_add(capacity.reserved_uploads)
            .and_then(|current| current.checked_add(byte_size))
            .filter(|next| *next <= maximum)
            .ok_or(ArtifactError::StoreFull { maximum })?;
        let _ = next;
        capacity.stored = capacity.stored.saturating_add(byte_size);
        Ok(())
    }

    fn reserve_upload(&self, byte_size: u64) -> Result<(), ArtifactError> {
        let maximum = self.config.maximum_store_bytes;
        let mut capacity = self
            .capacity
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        capacity
            .stored
            .checked_add(capacity.reserved_uploads)
            .and_then(|current| current.checked_add(byte_size))
            .filter(|next| *next <= maximum)
            .ok_or(ArtifactError::StoreFull { maximum })?;
        capacity.reserved_uploads = capacity.reserved_uploads.saturating_add(byte_size);
        Ok(())
    }

    fn release_stored(&self, byte_size: u64) {
        let mut capacity = self
            .capacity
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        capacity.stored = capacity.stored.saturating_sub(byte_size);
    }

    fn release_upload(&self, byte_size: u64) {
        let mut capacity = self
            .capacity
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        capacity.reserved_uploads = capacity.reserved_uploads.saturating_sub(byte_size);
    }

    fn upload(
        &self,
        upload_id: &ArtifactUploadId,
    ) -> Result<Arc<Mutex<ActiveUpload>>, ArtifactError> {
        self.cleanup_expired_uploads()?;
        self.uploads
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(upload_id)
            .cloned()
            .ok_or_else(|| ArtifactError::UploadNotFound(upload_id.0.clone()))
    }

    fn ensure_upload_active(&self, upload: &ActiveUpload) -> Result<(), ArtifactError> {
        match upload.lifecycle {
            UploadLifecycle::Active => {}
            UploadLifecycle::Finalizing => return Err(ArtifactError::UploadFinalizing),
            UploadLifecycle::Aborted => {
                return Err(ArtifactError::UploadNotFound(
                    upload.metadata.upload_id.0.clone(),
                ));
            }
        }
        if upload.metadata.expires_unix_ms <= now_unix_ms() {
            return Err(ArtifactError::UploadExpired);
        }
        Ok(())
    }

    fn commit_upload_locked(
        &self,
        metadata: &UploadMetadata,
    ) -> Result<ArtifactDescriptor, ArtifactError> {
        let part_path = self.upload_part_path(&metadata.upload_id)?;
        ensure_regular_file(&part_path)?;
        let (content_hash, artifact_id, byte_size) =
            hash_upload(&part_path, &metadata.media_type, metadata.expected_bytes)?;
        if metadata
            .expected_content_hash
            .as_ref()
            .is_some_and(|expected| expected != &content_hash)
        {
            return Err(ArtifactError::HashMismatch);
        }
        let object_path = self.object_path(&artifact_id)?;
        let parent = object_path.parent().expect("object path has parent");
        std::fs::create_dir_all(parent)?;
        let mut installed = false;
        if object_path.exists() {
            verify_object(&object_path, &content_hash, byte_size)?;
        } else {
            match std::fs::hard_link(&part_path, &object_path) {
                Ok(()) => installed = true,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    verify_object(&object_path, &content_hash, byte_size)?;
                }
                Err(error) => return Err(ArtifactError::Io(error)),
            }
        }
        let descriptor = ArtifactDescriptor {
            artifact_id,
            content_hash,
            media_type: metadata.media_type.clone(),
            byte_size,
        };
        if let Err(error) = self.write_metadata(&descriptor) {
            if installed {
                remove_if_exists(&object_path);
            }
            return Err(error);
        }
        {
            let mut capacity = self
                .capacity
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            capacity.reserved_uploads = capacity
                .reserved_uploads
                .saturating_sub(metadata.expected_bytes);
            if installed {
                capacity.stored = capacity.stored.saturating_add(byte_size);
            }
        }
        remove_if_exists(&part_path);
        remove_if_exists(&self.upload_metadata_path(&metadata.upload_id)?);
        Ok(descriptor)
    }

    fn cleanup_expired_uploads(&self) -> Result<(), ArtifactError> {
        let now = now_unix_ms();
        let mut uploads = self
            .uploads
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let expired = uploads
            .iter()
            .filter_map(|(upload_id, upload)| {
                let mut upload = upload
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                (upload.lifecycle == UploadLifecycle::Active
                    && upload.metadata.expires_unix_ms <= now)
                    .then(|| {
                        upload.lifecycle = UploadLifecycle::Aborted;
                        (upload_id.clone(), upload.metadata.expected_bytes)
                    })
            })
            .collect::<Vec<_>>();
        for (upload_id, expected_bytes) in expired {
            uploads.remove(&upload_id);
            remove_if_exists(&self.upload_part_path(&upload_id)?);
            remove_if_exists(&self.upload_metadata_path(&upload_id)?);
            self.release_upload(expected_bytes);
        }
        Ok(())
    }

    fn upload_part_path(&self, upload_id: &ArtifactUploadId) -> Result<PathBuf, ArtifactError> {
        validate_upload_id(upload_id)?;
        Ok(self
            .config
            .root
            .join("uploads")
            .join(format!("{}.part", upload_id.0)))
    }

    fn upload_metadata_path(&self, upload_id: &ArtifactUploadId) -> Result<PathBuf, ArtifactError> {
        validate_upload_id(upload_id)?;
        Ok(self
            .config
            .root
            .join("uploads")
            .join(format!("{}.json", upload_id.0)))
    }

    fn write_upload_metadata(&self, metadata: &UploadMetadata) -> Result<(), ArtifactError> {
        let path = self.upload_metadata_path(&metadata.upload_id)?;
        let bytes = serde_json::to_vec(metadata).map_err(std::io::Error::other)?;
        let parent = path.parent().expect("upload metadata path has parent");
        let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
        temporary.write_all(&bytes)?;
        temporary.as_file().sync_all()?;
        temporary
            .persist(&path)
            .map_err(|error| ArtifactError::Io(error.error))?;
        Ok(())
    }

    fn object_path(&self, artifact_id: &ArtifactId) -> Result<PathBuf, ArtifactError> {
        let hash = parse_artifact_hash(artifact_id)?;
        Ok(self.config.root.join("objects").join(&hash[..2]).join(hash))
    }

    fn metadata_path(&self, artifact_id: &ArtifactId) -> Result<PathBuf, ArtifactError> {
        let hash = parse_artifact_hash(artifact_id)?;
        Ok(self
            .config
            .root
            .join("metadata")
            .join(format!("{hash}.json")))
    }

    fn write_metadata(&self, descriptor: &ArtifactDescriptor) -> Result<(), ArtifactError> {
        let path = self.metadata_path(&descriptor.artifact_id)?;
        let parent = path.parent().expect("metadata path has parent");
        std::fs::create_dir_all(parent)?;
        let bytes = serde_json::to_vec(descriptor).map_err(std::io::Error::other)?;
        let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
        temporary.write_all(&bytes)?;
        temporary.as_file().sync_all()?;
        if let Err(error) = temporary.persist(&path) {
            return Err(ArtifactError::Io(error.error));
        }
        Ok(())
    }
}

fn validate_media_type(media_type: &str) -> Result<(), ArtifactError> {
    if media_type.trim().is_empty() || media_type.len() > 255 || media_type.as_bytes().contains(&0)
    {
        return Err(ArtifactError::InvalidMediaType(media_type.to_owned()));
    }
    Ok(())
}

fn validate_upload_id(upload_id: &ArtifactUploadId) -> Result<(), ArtifactError> {
    let value = upload_id
        .0
        .strip_prefix("upload_")
        .ok_or_else(|| ArtifactError::InvalidId(upload_id.0.clone()))?;
    if value.len() != 32 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ArtifactError::InvalidId(upload_id.0.clone()));
    }
    Ok(())
}

fn ensure_regular_file(path: &Path) -> Result<(), ArtifactError> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(ArtifactError::Io(std::io::Error::other(format!(
            "artifact upload path is not a regular file: {}",
            path.display()
        ))));
    }
    Ok(())
}

fn remove_if_exists(path: &Path) {
    if let Err(error) = std::fs::remove_file(path)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(path = %path.display(), %error, "failed to remove artifact upload file");
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn hash_upload(
    path: &Path,
    media_type: &str,
    expected_bytes: u64,
) -> Result<(String, ArtifactId, u64), ArtifactError> {
    let mut file = OpenOptions::new().read(true).open(path)?;
    let byte_size = file.metadata()?.len();
    if byte_size != expected_bytes {
        return Err(ArtifactError::UploadIncomplete {
            expected: expected_bytes,
            actual: byte_size,
        });
    }
    let mut content = blake3::Hasher::new();
    let mut identity = blake3::Hasher::new();
    identity.update(&(media_type.len() as u64).to_le_bytes());
    identity.update(media_type.as_bytes());
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        content.update(&buffer[..read]);
        identity.update(&buffer[..read]);
    }
    Ok((
        content.finalize().to_hex().to_string(),
        ArtifactId::from_string(format!("art_{}", identity.finalize().to_hex())),
        byte_size,
    ))
}

fn recover_uploads(
    config: &ArtifactStoreConfig,
    available_bytes: u64,
) -> Result<RecoveredUploads, ArtifactError> {
    let root = config.root.join("uploads");
    let mut metadata_paths = std::fs::read_dir(&root)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("json"))
        .collect::<Vec<_>>();
    metadata_paths.sort();
    let mut uploads = HashMap::new();
    let mut reserved = 0_u64;
    let now = now_unix_ms();
    for metadata_path in metadata_paths {
        let metadata = (|| {
            ensure_regular_file(&metadata_path)?;
            serde_json::from_slice::<UploadMetadata>(&std::fs::read(&metadata_path)?)
                .map_err(std::io::Error::other)
                .map_err(ArtifactError::Io)
        })();
        let Ok(metadata) = metadata else {
            remove_if_exists(&metadata_path);
            continue;
        };
        let part_path = root.join(format!("{}.part", metadata.upload_id.0));
        let valid = validate_upload_id(&metadata.upload_id).is_ok()
            && metadata_path.file_stem().and_then(|value| value.to_str())
                == Some(metadata.upload_id.0.as_str())
            && validate_media_type(&metadata.media_type).is_ok()
            && metadata.expected_bytes <= config.maximum_artifact_bytes
            && metadata.next_offset <= metadata.expected_bytes
            && metadata.expires_unix_ms > now
            && ensure_regular_file(&part_path).is_ok()
            && std::fs::metadata(&part_path).is_ok_and(|part| part.len() == metadata.next_offset)
            && reserved
                .checked_add(metadata.expected_bytes)
                .is_some_and(|next| next <= available_bytes);
        if !valid {
            remove_if_exists(&metadata_path);
            remove_if_exists(&part_path);
            continue;
        }
        reserved = reserved.saturating_add(metadata.expected_bytes);
        uploads.insert(
            metadata.upload_id.clone(),
            Arc::new(Mutex::new(ActiveUpload {
                metadata,
                lifecycle: UploadLifecycle::Active,
            })),
        );
    }
    let known = uploads
        .keys()
        .map(|upload_id| format!("{}.part", upload_id.0))
        .collect::<std::collections::HashSet<_>>();
    for entry in std::fs::read_dir(&root)?.filter_map(Result::ok) {
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) == Some("part")
            && !path
                .file_name()
                .and_then(|value| value.to_str())
                .is_some_and(|name| known.contains(name))
        {
            remove_if_exists(&path);
        }
    }
    Ok((uploads, reserved))
}

fn parse_artifact_hash(artifact_id: &ArtifactId) -> Result<&str, ArtifactError> {
    let hash = artifact_id
        .0
        .strip_prefix("art_")
        .ok_or_else(|| ArtifactError::InvalidId(artifact_id.0.clone()))?;
    if hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ArtifactError::InvalidId(artifact_id.0.clone()));
    }
    Ok(hash)
}

fn calculate_store_bytes(root: &Path) -> Result<u64, ArtifactError> {
    if !root.exists() {
        return Ok(0);
    }
    let mut total = 0_u64;
    for prefix in std::fs::read_dir(root)? {
        let prefix = prefix?;
        if !prefix.file_type()?.is_dir() {
            continue;
        }
        for object in std::fs::read_dir(prefix.path())? {
            let object = object?;
            if object.file_type()?.is_file() {
                total = total.saturating_add(object.metadata()?.len());
            }
        }
    }
    Ok(total)
}

fn verify_object(
    path: &Path,
    expected_hash: &str,
    expected_size: u64,
) -> Result<(), ArtifactError> {
    let mut file = OpenOptions::new().read(true).open(path)?;
    if file.metadata()?.len() != expected_size {
        return Err(ArtifactError::HashMismatch);
    }
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    if hasher.finalize().to_hex().as_str() != expected_hash {
        return Err(ArtifactError::HashMismatch);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use super::*;

    #[test]
    fn artifacts_are_deduplicated_and_cursor_readable() {
        let directory = tempfile::tempdir().unwrap();
        let store = ArtifactStore::open(ArtifactStoreConfig::new(directory.path())).unwrap();
        let first = store.put("text/plain", b"abcdef").unwrap();
        let second = store.put("text/plain", b"abcdef").unwrap();
        let typed_differently = store.put("application/octet-stream", b"abcdef").unwrap();
        assert_eq!(first.artifact_id, second.artifact_id);
        assert_ne!(first.artifact_id, typed_differently.artifact_id);
        assert_eq!(store.stored_bytes(), 12);
        let (page, next) = store.read_range(&first.artifact_id, 0, 3).unwrap();
        assert_eq!(page, b"abc");
        assert_eq!(next, Some(3));
    }

    #[test]
    fn file_import_streams_into_an_immutable_deduplicated_object() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("provider-output.spool");
        std::fs::write(&source, b"streamed-provider-output").unwrap();
        let store =
            ArtifactStore::open(ArtifactStoreConfig::new(directory.path().join("store"))).unwrap();

        let imported = store
            .put_file("application/vnd.grok.native-output-spool", &source)
            .unwrap();
        let inline = store
            .put(
                "application/vnd.grok.native-output-spool",
                b"streamed-provider-output",
            )
            .unwrap();
        assert_eq!(imported, inline);
        assert_eq!(store.stored_bytes(), imported.byte_size);

        std::fs::write(&source, b"mutated-after-import").unwrap();
        let (stored, next) = store.read_range(&imported.artifact_id, 0, 1024).unwrap();
        assert_eq!(stored, b"streamed-provider-output");
        assert!(next.is_none());
    }

    #[test]
    fn concurrent_writers_cannot_overcommit_store_capacity() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = ArtifactStoreConfig::new(directory.path());
        config.maximum_store_bytes = 6;
        let store = Arc::new(ArtifactStore::open(config).unwrap());
        let barrier = Arc::new(Barrier::new(3));
        let mut writers = Vec::new();
        for bytes in [b"aaaa".to_vec(), b"bbbb".to_vec()] {
            let store = store.clone();
            let barrier = barrier.clone();
            writers.push(std::thread::spawn(move || {
                barrier.wait();
                store.put("application/octet-stream", &bytes)
            }));
        }
        barrier.wait();
        let successes = writers
            .into_iter()
            .map(|writer| usize::from(writer.join().unwrap().is_ok()))
            .sum::<usize>();
        assert_eq!(successes, 1);
        assert_eq!(store.stored_bytes(), 4);
    }

    #[test]
    fn chunked_upload_recovers_offset_and_commits_without_buffering_whole_object() {
        let directory = tempfile::tempdir().unwrap();
        let config = ArtifactStoreConfig::new(directory.path());
        let expected_hash = blake3::hash(b"abcdef").to_hex().to_string();
        let upload_id = {
            let store = ArtifactStore::open(config.clone()).unwrap();
            let lease = store
                .begin_upload("text/plain", 6, Some(expected_hash.clone()))
                .unwrap();
            assert_eq!(
                store
                    .upload_chunk(&lease.upload_id, 0, b"abc")
                    .unwrap()
                    .next_offset,
                3
            );
            lease.upload_id
        };

        let store = ArtifactStore::open(config).unwrap();
        let recovered = store.inspect_upload(&upload_id).unwrap();
        assert_eq!(recovered.next_offset, 3);
        assert_eq!(recovered.expected_content_hash, Some(expected_hash));
        assert!(matches!(
            store.upload_chunk(&upload_id, 0, b"def"),
            Err(ArtifactError::OffsetMismatch {
                expected: 3,
                actual: 0
            })
        ));
        store.upload_chunk(&upload_id, 3, b"def").unwrap();
        let descriptor = store.commit_upload(&upload_id).unwrap();
        assert_eq!(descriptor.byte_size, 6);
        assert_eq!(store.stored_bytes(), 6);
        let deduplicated = store.put("text/plain", b"abcdef").unwrap();
        assert_eq!(descriptor, deduplicated);
        assert_eq!(store.stored_bytes(), 6);
        let (bytes, next) = store
            .read_range(&descriptor.artifact_id, 0, usize::MAX)
            .unwrap();
        assert_eq!(bytes, b"abcdef");
        assert_eq!(next, None);
        assert!(matches!(
            store.inspect_upload(&upload_id),
            Err(ArtifactError::UploadNotFound(_))
        ));
    }

    #[test]
    fn upload_reservations_are_bounded_and_abort_releases_capacity() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = ArtifactStoreConfig::new(directory.path());
        config.maximum_store_bytes = 6;
        let store = ArtifactStore::open(config).unwrap();
        let lease = store
            .begin_upload("application/octet-stream", 6, None)
            .unwrap();
        assert!(matches!(
            store.put("application/octet-stream", b"x"),
            Err(ArtifactError::StoreFull { maximum: 6 })
        ));
        store.abort_upload(&lease.upload_id).unwrap();
        assert_eq!(
            store
                .put("application/octet-stream", b"x")
                .unwrap()
                .byte_size,
            1
        );
        assert_eq!(store.stored_bytes(), 1);
    }

    #[test]
    fn hash_mismatch_never_publishes_or_discards_resumable_upload() {
        let directory = tempfile::tempdir().unwrap();
        let store = ArtifactStore::open(ArtifactStoreConfig::new(directory.path())).unwrap();
        let lease = store
            .begin_upload("text/plain", 3, Some("0".repeat(64)))
            .unwrap();
        store.upload_chunk(&lease.upload_id, 0, b"abc").unwrap();
        assert!(matches!(
            store.commit_upload(&lease.upload_id),
            Err(ArtifactError::HashMismatch)
        ));
        assert_eq!(
            store.inspect_upload(&lease.upload_id).unwrap().next_offset,
            3
        );
        assert_eq!(store.stored_bytes(), 0);
        store.abort_upload(&lease.upload_id).unwrap();
    }
}
