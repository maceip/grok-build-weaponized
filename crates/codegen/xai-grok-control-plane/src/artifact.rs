use std::fs::OpenOptions;
use std::io::{Read as _, Seek as _, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use xai_grok_protocol::ArtifactId;

const DEFAULT_MAX_ARTIFACT_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const DEFAULT_MAX_STORE_BYTES: u64 = 128 * 1024 * 1024 * 1024;
const MAX_READ_BYTES: usize = 8 * 1024 * 1024;

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
}

pub struct ArtifactStore {
    config: ArtifactStoreConfig,
    stored_bytes: AtomicU64,
}

impl ArtifactStore {
    pub fn open(config: ArtifactStoreConfig) -> Result<Self, ArtifactError> {
        std::fs::create_dir_all(config.root.join("objects"))?;
        std::fs::create_dir_all(config.root.join("metadata"))?;
        let stored_bytes = calculate_store_bytes(&config.root.join("objects"))?;
        if stored_bytes > config.maximum_store_bytes {
            return Err(ArtifactError::StoreFull {
                maximum: config.maximum_store_bytes,
            });
        }
        Ok(Self {
            config,
            stored_bytes: AtomicU64::new(stored_bytes),
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
                self.stored_bytes.fetch_sub(byte_size, Ordering::AcqRel);
                verify_object(&object_path, &content_hash, byte_size)?;
            }
            Err(error) => {
                self.stored_bytes.fetch_sub(byte_size, Ordering::AcqRel);
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
        self.stored_bytes.load(Ordering::Acquire)
    }

    fn reserve(&self, byte_size: u64) -> Result<(), ArtifactError> {
        let maximum = self.config.maximum_store_bytes;
        self.stored_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current
                    .checked_add(byte_size)
                    .filter(|next| *next <= maximum)
            })
            .map(|_| ())
            .map_err(|_| ArtifactError::StoreFull { maximum })
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
}
