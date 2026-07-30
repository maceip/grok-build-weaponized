use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};

use rusqlite::{Connection, params};
use sha2::Digest as _;

pub const MAX_JOURNAL_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_JOURNAL_ENTRIES: usize = crate::MAX_HOST_CALLS as usize;

pub(crate) const HOST_ERROR_KEY: &str = "__xai_workflow_host_error";

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct JournalEntry {
    pub seq: u64,
    pub kind: String,
    pub req_hash: String,
    pub result: serde_json::Value,
    pub at_ms: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    #[error("journal io: {0}")]
    Io(#[from] std::io::Error),
    #[error("journal sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("journal parse at line {line}: {error}")]
    Parse { line: usize, error: String },
    #[error("journal restore rejected (limit {limit}): {reason}")]
    UnsafeRestore { limit: u64, reason: String },
    #[error(
        "journal full: appending seq {seq} would exceed the {limit}-byte cap \
         that restore enforces, which would strand the run unresumable"
    )]
    Full { seq: u64, limit: u64 },
    #[error("journal is not dense at entry {index}: expected sequence {expected}, found {actual}")]
    Sequence {
        index: usize,
        expected: u64,
        actual: u64,
    },
    #[error(
        "replay divergence at seq {seq} ({kind}): the script issued a different call than the \
         recorded run — the workflow script is nondeterministic or was edited mid-run"
    )]
    Divergence { seq: u64, kind: String },
}

#[derive(Debug)]
pub struct Journal {
    entries: Vec<JournalEntry>,
    store: Box<dyn StateStore>,
    bytes: u64,
    last_line_start: Option<u64>,
}

#[derive(Debug)]
pub struct LoadedState {
    pub entries: Vec<JournalEntry>,
    pub bytes: u64,
    pub last_record_offset: Option<u64>,
}

/// Persistence boundary shared by the legacy JSONL workflow journal and the
/// transactional SQLite backend. `Journal` owns deterministic replay policy;
/// stores own durable append/truncate semantics only.
pub trait StateStore: std::fmt::Debug + Send {
    fn load(&mut self) -> Result<LoadedState, JournalError>;
    fn append(&mut self, entry: &JournalEntry, encoded: &str) -> Result<(), JournalError>;
    fn truncate_last(&mut self, entry: &JournalEntry, new_len: u64) -> Result<(), JournalError>;
}

#[derive(Debug, Default)]
struct MemoryStateStore;

#[derive(Debug)]
pub struct JsonlStateStore {
    path: PathBuf,
}

pub struct SqliteStateStore {
    connection: Connection,
    stream_id: String,
}

impl std::fmt::Debug for SqliteStateStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqliteStateStore")
            .field("stream_id", &self.stream_id)
            .finish_non_exhaustive()
    }
}

impl Default for Journal {
    fn default() -> Self {
        Self::new(None)
    }
}

impl Journal {
    pub fn new(path: Option<PathBuf>) -> Self {
        Self {
            entries: Vec::new(),
            store: path.map_or_else(
                || Box::<MemoryStateStore>::default() as Box<dyn StateStore>,
                |path| Box::new(JsonlStateStore { path }),
            ),
            bytes: 0,
            last_line_start: None,
        }
    }

    pub fn load(path: PathBuf) -> Result<Self, JournalError> {
        Self::from_store(Box::new(JsonlStateStore { path }))
    }

    pub fn load_sqlite(path: PathBuf, stream_id: impl Into<String>) -> Result<Self, JournalError> {
        Self::from_store(Box::new(SqliteStateStore::open(path, stream_id.into())?))
    }

    pub fn from_store(mut store: Box<dyn StateStore>) -> Result<Self, JournalError> {
        let loaded = store.load()?;
        for (index, entry) in loaded.entries.iter().enumerate() {
            if index >= MAX_JOURNAL_ENTRIES {
                return Err(JournalError::UnsafeRestore {
                    limit: MAX_JOURNAL_ENTRIES as u64,
                    reason: "too many journal entries".into(),
                });
            }
            validate_sequence(&loaded.entries[..index], entry)?;
        }
        if loaded.bytes > MAX_JOURNAL_BYTES {
            return Err(JournalError::UnsafeRestore {
                limit: MAX_JOURNAL_BYTES,
                reason: format!("journal contains {} encoded bytes", loaded.bytes),
            });
        }
        Ok(Self {
            entries: loaded.entries,
            store,
            bytes: loaded.bytes,
            last_line_start: loaded.last_record_offset,
        })
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn agent_reservation_count(&self) -> u64 {
        u64::try_from(
            self.entries
                .iter()
                .filter(|entry| entry.kind == "spawn_agent")
                .count(),
        )
        .unwrap_or(u64::MAX)
    }

    pub fn covers(&self, seq: u64) -> bool {
        usize::try_from(seq).is_ok_and(|seq| seq < self.entries.len())
    }

    pub fn replay(
        &self,
        seq: u64,
        kind: &str,
        req_hash: &str,
    ) -> Result<Option<serde_json::Value>, JournalError> {
        let Some(entry) = usize::try_from(seq)
            .ok()
            .and_then(|seq| self.entries.get(seq))
        else {
            return Ok(None);
        };
        if entry.seq != seq || entry.kind != kind || entry.req_hash != req_hash {
            return Err(JournalError::Divergence {
                seq,
                kind: kind.to_string(),
            });
        }
        Ok(Some(entry.result.clone()))
    }

    pub fn record(
        &mut self,
        seq: u64,
        kind: &str,
        req_hash: String,
        result: serde_json::Value,
    ) -> Result<(), JournalError> {
        let entry = JournalEntry {
            seq,
            kind: kind.to_string(),
            req_hash,
            result,
            at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        };
        validate_sequence(&self.entries, &entry)?;
        let mut line = serde_json::to_string(&entry)
            .map_err(|error| JournalError::Io(std::io::Error::other(error)))?;
        line.push('\n');
        if self.bytes.saturating_add(line.len() as u64) > MAX_JOURNAL_BYTES {
            return Err(JournalError::Full {
                seq,
                limit: MAX_JOURNAL_BYTES,
            });
        }
        self.store.append(&entry, &line)?;
        self.last_line_start = Some(self.bytes);
        self.bytes = self.bytes.saturating_add(line.len() as u64);
        self.entries.push(entry);
        Ok(())
    }

    pub fn prune_trailing_host_error(
        &mut self,
        failure_detail: &str,
    ) -> Result<bool, JournalError> {
        let Some(last) = self.entries.last() else {
            return Ok(false);
        };
        let Some(message) = last.result.get(HOST_ERROR_KEY).and_then(|v| v.as_str()) else {
            return Ok(false);
        };
        if message.is_empty() || !failure_detail.contains(message) {
            return Ok(false);
        }
        let Some(new_len) = self.last_line_start else {
            return Err(JournalError::Io(std::io::Error::other(
                "journal cannot locate the trailing entry's byte offset",
            )));
        };
        self.store.truncate_last(last, new_len)?;
        self.entries.pop();
        self.bytes = new_len;
        self.last_line_start = None;
        Ok(true)
    }
}

impl StateStore for MemoryStateStore {
    fn load(&mut self) -> Result<LoadedState, JournalError> {
        Ok(LoadedState {
            entries: Vec::new(),
            bytes: 0,
            last_record_offset: None,
        })
    }

    fn append(&mut self, _entry: &JournalEntry, _encoded: &str) -> Result<(), JournalError> {
        Ok(())
    }

    fn truncate_last(&mut self, _entry: &JournalEntry, _new_len: u64) -> Result<(), JournalError> {
        Ok(())
    }
}

impl StateStore for JsonlStateStore {
    fn load(&mut self) -> Result<LoadedState, JournalError> {
        load_jsonl_state(&self.path)
    }

    fn append(&mut self, _entry: &JournalEntry, encoded: &str) -> Result<(), JournalError> {
        append_line(&self.path, encoded)?;
        Ok(())
    }

    fn truncate_last(&mut self, _entry: &JournalEntry, new_len: u64) -> Result<(), JournalError> {
        truncate_tail(&self.path, new_len)?;
        Ok(())
    }
}

impl SqliteStateStore {
    pub fn open(path: PathBuf, stream_id: String) -> Result<Self, JournalError> {
        if stream_id.trim().is_empty() {
            return Err(JournalError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "workflow journal stream ID must not be empty",
            )));
        }
        let connection = xai_sqlite_journal::JournalMode::for_db_path(&path).open(&path)?;
        connection.pragma_update(None, "foreign_keys", true)?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS workflow_journal(
                stream_id TEXT NOT NULL,
                seq INTEGER NOT NULL,
                kind TEXT NOT NULL,
                req_hash TEXT NOT NULL,
                result_json TEXT NOT NULL,
                at_ms INTEGER NOT NULL,
                encoded_bytes INTEGER NOT NULL,
                PRIMARY KEY(stream_id, seq)
             );",
        )?;
        Ok(Self {
            connection,
            stream_id,
        })
    }
}

impl StateStore for SqliteStateStore {
    fn load(&mut self) -> Result<LoadedState, JournalError> {
        let mut statement = self.connection.prepare(
            "SELECT seq, kind, req_hash, result_json, at_ms, encoded_bytes
             FROM workflow_journal
             WHERE stream_id=?1
             ORDER BY seq",
        )?;
        let rows = statement.query_map([self.stream_id.as_str()], |row| {
            let result_json: String = row.get(3)?;
            Ok((
                JournalEntry {
                    seq: row.get(0)?,
                    kind: row.get(1)?,
                    req_hash: row.get(2)?,
                    result: serde_json::from_str(&result_json).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            3,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?,
                    at_ms: row.get(4)?,
                },
                row.get::<_, u64>(5)?,
            ))
        })?;
        let mut entries = Vec::new();
        let mut bytes = 0_u64;
        let mut last_record_offset = None;
        for row in rows {
            let (entry, encoded_bytes) = row?;
            last_record_offset = Some(bytes);
            bytes = bytes.saturating_add(encoded_bytes);
            entries.push(entry);
        }
        Ok(LoadedState {
            entries,
            bytes,
            last_record_offset,
        })
    }

    fn append(&mut self, entry: &JournalEntry, encoded: &str) -> Result<(), JournalError> {
        let transaction = self
            .connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        transaction.execute(
            "INSERT INTO workflow_journal(
                stream_id, seq, kind, req_hash, result_json, at_ms, encoded_bytes
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                self.stream_id,
                entry.seq,
                entry.kind,
                entry.req_hash,
                serde_json::to_string(&entry.result).map_err(std::io::Error::other)?,
                entry.at_ms,
                encoded.len() as u64,
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn truncate_last(&mut self, entry: &JournalEntry, _new_len: u64) -> Result<(), JournalError> {
        let transaction = self
            .connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let changed = transaction.execute(
            "DELETE FROM workflow_journal WHERE stream_id=?1 AND seq=?2",
            params![self.stream_id, entry.seq],
        )?;
        if changed != 1 {
            return Err(JournalError::Io(std::io::Error::other(format!(
                "workflow journal cannot truncate missing sequence {}",
                entry.seq
            ))));
        }
        transaction.commit()?;
        Ok(())
    }
}

fn load_jsonl_state(path: &Path) -> Result<LoadedState, JournalError> {
    let content = match read_journal_bounded(path) {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) if error.kind() == std::io::ErrorKind::InvalidData => {
            return Err(JournalError::UnsafeRestore {
                limit: MAX_JOURNAL_BYTES,
                reason: error.to_string(),
            });
        }
        Err(error) => return Err(error.into()),
    };
    let mut entries = Vec::new();
    let mut offset = 0usize;
    let mut line_number = 0usize;
    let mut bytes = content.len() as u64;
    let mut last_record_offset = None;
    while offset < content.len() {
        line_number += 1;
        let Some(relative_newline) = content[offset..].iter().position(|byte| *byte == b'\n')
        else {
            let tail = &content[offset..];
            if tail.iter().all(u8::is_ascii_whitespace) {
                truncate_tail(path, offset as u64)?;
                bytes = offset as u64;
                break;
            }
            match serde_json::from_slice::<JournalEntry>(tail) {
                Ok(entry) => {
                    if entries.len() >= MAX_JOURNAL_ENTRIES {
                        return Err(JournalError::UnsafeRestore {
                            limit: MAX_JOURNAL_ENTRIES as u64,
                            reason: "too many journal entries".into(),
                        });
                    }
                    validate_sequence(&entries, &entry)?;
                    entries.push(entry);
                    last_record_offset = Some(offset as u64);
                    terminate_line(path)?;
                    bytes = bytes.saturating_add(1);
                }
                Err(error) => {
                    tracing::warn!(
                        line = line_number,
                        %error,
                        "truncating torn workflow journal tail"
                    );
                    truncate_tail(path, offset as u64)?;
                    bytes = offset as u64;
                }
            }
            break;
        };
        let end = offset + relative_newline;
        let line = &content[offset..end];
        let line_start = offset as u64;
        offset = end + 1;
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let entry =
            serde_json::from_slice::<JournalEntry>(line).map_err(|error| JournalError::Parse {
                line: line_number,
                error: error.to_string(),
            })?;
        if entries.len() >= MAX_JOURNAL_ENTRIES {
            return Err(JournalError::UnsafeRestore {
                limit: MAX_JOURNAL_ENTRIES as u64,
                reason: "too many journal entries".into(),
            });
        }
        validate_sequence(&entries, &entry)?;
        entries.push(entry);
        last_record_offset = Some(line_start);
    }
    Ok(LoadedState {
        entries,
        bytes,
        last_record_offset,
    })
}

fn read_journal_bounded(path: &Path) -> std::io::Result<Vec<u8>> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("journal is not a regular file: {}", path.display()),
        ));
    }
    if metadata.len() > MAX_JOURNAL_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("journal exceeds {MAX_JOURNAL_BYTES} bytes"),
        ));
    }
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(path)?;
    let opened = file.metadata()?;
    if !opened.is_file() || opened.len() > MAX_JOURNAL_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "journal changed during open",
        ));
    }
    let mut content = Vec::with_capacity(opened.len() as usize);
    file.take(MAX_JOURNAL_BYTES.saturating_add(1))
        .read_to_end(&mut content)?;
    if content.len() as u64 > MAX_JOURNAL_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("journal exceeds {MAX_JOURNAL_BYTES} bytes"),
        ));
    }
    Ok(content)
}

fn validate_sequence(entries: &[JournalEntry], entry: &JournalEntry) -> Result<(), JournalError> {
    let expected = entries.len() as u64;
    if entry.seq != expected {
        return Err(JournalError::Sequence {
            index: entries.len(),
            expected,
            actual: entry.seq,
        });
    }
    Ok(())
}

fn truncate_tail(path: &Path, len: u64) -> std::io::Result<()> {
    let file = std::fs::OpenOptions::new().write(true).open(path)?;
    file.set_len(len)?;
    file.sync_data()
}

fn terminate_line(path: &Path) -> std::io::Result<()> {
    let mut file = std::fs::OpenOptions::new().append(true).open(path)?;
    file.write_all(b"\n")?;
    file.sync_data()
}

fn append_line(path: &Path, line: &str) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    file.write_all(line.as_bytes())?;
    file.sync_data()
}

fn canonical_json(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let mut entries: Vec<(&String, &serde_json::Value)> = map.iter().collect();
            entries.sort_unstable_by(|a, b| a.0.cmp(b.0));
            serde_json::Value::Object(
                entries
                    .into_iter()
                    .map(|(k, v)| (k.clone(), canonical_json(v)))
                    .collect(),
            )
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(canonical_json).collect())
        }
        other => other.clone(),
    }
}

pub fn request_hash(kind: &str, payload: &serde_json::Value) -> String {
    let mut hasher = sha2::Sha256::new();
    hasher.update(kind.as_bytes());
    hasher.update([0u8]);
    hasher.update(canonical_json(payload).to_string().as_bytes());
    let digest = hasher.finalize();
    let mut out = String::with_capacity(32);
    for byte in digest.iter().take(16) {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_and_replay_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("journal.jsonl");
        let mut journal = Journal::new(Some(path.clone()));
        let hash = request_hash("spawn_agent", &serde_json::json!({"prompt": "hi"}));
        journal
            .record(
                0,
                "spawn_agent",
                hash.clone(),
                serde_json::json!({"ok": true}),
            )
            .unwrap();

        let loaded = Journal::load(path).unwrap();
        assert_eq!(loaded.len(), 1);
        let replayed = loaded.replay(0, "spawn_agent", &hash).unwrap();
        assert_eq!(replayed, Some(serde_json::json!({"ok": true})));
        assert!(loaded.replay(1, "spawn_agent", &hash).unwrap().is_none());
    }

    #[test]
    fn divergence_on_hash_mismatch() {
        let mut journal = Journal::new(None);
        journal
            .record(0, "spawn_agent", "aaaa".into(), serde_json::json!(1))
            .unwrap();
        assert!(matches!(
            journal.replay(0, "spawn_agent", "bbbb"),
            Err(JournalError::Divergence { seq: 0, .. })
        ));
        assert!(matches!(
            journal.replay(0, "budget", "aaaa"),
            Err(JournalError::Divergence { seq: 0, .. })
        ));
    }

    #[test]
    fn torn_tail_is_truncated_before_the_next_append() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("journal.jsonl");
        let first = "{\"seq\":0,\"kind\":\"log\",\"req_hash\":\"x\",\"result\":null,\"at_ms\":1}\n";
        std::fs::write(&path, format!("{first}{{\"seq\":1,\"kind")).unwrap();

        let mut journal = Journal::load(path.clone()).unwrap();
        assert_eq!(journal.len(), 1);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), first);
        journal
            .record(1, "log", "y".into(), serde_json::Value::Null)
            .unwrap();

        assert_eq!(Journal::load(path).unwrap().len(), 2);
    }

    #[test]
    fn valid_unterminated_tail_is_kept_and_terminated() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("journal.jsonl");
        let line = "{\"seq\":0,\"kind\":\"log\",\"req_hash\":\"x\",\"result\":null,\"at_ms\":1}";
        std::fs::write(&path, line).unwrap();

        assert_eq!(Journal::load(path.clone()).unwrap().len(), 1);
        assert_eq!(std::fs::read_to_string(path).unwrap(), format!("{line}\n"));
    }

    #[cfg(unix)]
    #[test]
    fn load_rejects_symlink_journal() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target.jsonl");
        let linked = dir.path().join("journal.jsonl");
        std::fs::write(&target, "").unwrap();
        symlink(&target, &linked).unwrap();
        assert!(matches!(
            Journal::load(linked),
            Err(JournalError::UnsafeRestore { .. })
        ));
    }

    #[test]
    fn load_rejects_oversize_journal_before_reading() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("journal.jsonl");
        let file = std::fs::File::create(&path).unwrap();
        file.set_len(MAX_JOURNAL_BYTES + 1).unwrap();
        assert!(matches!(
            Journal::load(path),
            Err(JournalError::UnsafeRestore { .. })
        ));
    }

    #[test]
    fn record_refuses_to_grow_past_the_restore_cap() {
        let mut journal = Journal::new(None);
        let big = "x".repeat(MAX_JOURNAL_BYTES as usize + 1);
        let hash = request_hash("spawn_agent", &serde_json::json!({}));
        let err = journal
            .record(0, "spawn_agent", hash.clone(), serde_json::json!(big))
            .unwrap_err();
        assert!(matches!(err, JournalError::Full { seq: 0, .. }), "{err}");
        journal
            .record(0, "spawn_agent", hash, serde_json::json!({"ok": true}))
            .unwrap();
        assert_eq!(journal.len(), 1);
    }

    #[test]
    fn complete_malformed_line_is_not_treated_as_torn() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("journal.jsonl");
        std::fs::write(&path, b"not-json\n").unwrap();
        assert!(matches!(
            Journal::load(path),
            Err(JournalError::Parse { .. })
        ));
    }

    #[test]
    fn load_and_record_require_dense_sequences() {
        let mut journal = Journal::new(None);
        assert!(matches!(
            journal.record(1, "log", "x".into(), serde_json::Value::Null),
            Err(JournalError::Sequence {
                expected: 0,
                actual: 1,
                ..
            })
        ));

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("journal.jsonl");
        std::fs::write(
            &path,
            "{\"seq\":1,\"kind\":\"log\",\"req_hash\":\"x\",\"result\":null,\"at_ms\":1}\n",
        )
        .unwrap();
        assert!(matches!(
            Journal::load(path),
            Err(JournalError::Sequence {
                expected: 0,
                actual: 1,
                ..
            })
        ));
    }

    #[test]
    fn persistence_error_does_not_advance_memory() {
        let dir = tempfile::tempdir().unwrap();
        let mut journal = Journal::new(Some(dir.path().join("journal.jsonl")));
        std::fs::create_dir(dir.path().join("journal.jsonl")).unwrap();
        assert!(matches!(
            journal.record(0, "log", "x".into(), serde_json::Value::Null),
            Err(JournalError::Io(_))
        ));
        assert!(journal.is_empty());
    }

    #[test]
    fn prune_removes_trailing_host_error_sentinel_and_truncates_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("journal.jsonl");
        let mut journal = Journal::new(Some(path.clone()));
        journal
            .record(
                0,
                "spawn_agent",
                "aaaa".into(),
                serde_json::json!({"ok": true}),
            )
            .unwrap();
        journal
            .record(
                1,
                "write_scratch_file",
                "bbbb".into(),
                serde_json::json!({ HOST_ERROR_KEY: "scratch byte quota exceeded" }),
            )
            .unwrap();
        let before = std::fs::read_to_string(&path).unwrap();
        assert_eq!(before.lines().count(), 2);

        let mut loaded = Journal::load(path.clone()).unwrap();
        assert!(
            loaded
                .prune_trailing_host_error("Runtime error: scratch byte quota exceeded")
                .unwrap()
        );
        assert_eq!(loaded.len(), 1);

        let after = std::fs::read_to_string(&path).unwrap();
        assert_eq!(after.lines().count(), 1);
        assert!(!after.contains(HOST_ERROR_KEY));
        assert!(before.starts_with(&after), "prune must only truncate");

        loaded
            .record(
                1,
                "write_scratch_file",
                "bbbb".into(),
                serde_json::json!("ok"),
            )
            .unwrap();
        let reloaded = Journal::load(path).unwrap();
        assert_eq!(reloaded.len(), 2);
        assert_eq!(
            reloaded.replay(1, "write_scratch_file", "bbbb").unwrap(),
            Some(serde_json::json!("ok"))
        );
    }

    #[test]
    fn prune_after_in_memory_record_truncates_and_allows_reappend() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("journal.jsonl");
        let mut journal = Journal::new(Some(path.clone()));
        journal
            .record(
                0,
                "read_scratch_file",
                "cccc".into(),
                serde_json::json!({ HOST_ERROR_KEY: "boom" }),
            )
            .unwrap();
        assert!(journal.prune_trailing_host_error("boom").unwrap());
        assert!(journal.is_empty());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "");
        journal
            .record(
                0,
                "read_scratch_file",
                "cccc".into(),
                serde_json::json!("live"),
            )
            .unwrap();
        assert_eq!(Journal::load(path).unwrap().len(), 1);
    }

    #[test]
    fn prune_is_a_noop_when_last_entry_is_a_success() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("journal.jsonl");
        let mut journal = Journal::new(Some(path.clone()));
        journal
            .record(
                0,
                "spawn_agent",
                "aaaa".into(),
                serde_json::json!({ HOST_ERROR_KEY: "caught mid-journal error" }),
            )
            .unwrap();
        journal
            .record(
                1,
                "spawn_agent",
                "bbbb".into(),
                serde_json::json!({"ok": true}),
            )
            .unwrap();
        let before = std::fs::read_to_string(&path).unwrap();
        assert!(
            !journal
                .prune_trailing_host_error("caught mid-journal error")
                .unwrap()
        );
        assert_eq!(journal.len(), 2);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), before);
    }

    #[test]
    fn prune_is_a_noop_when_trailing_sentinel_was_caught_and_run_died_elsewhere() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("journal.jsonl");
        let mut journal = Journal::new(Some(path.clone()));
        journal
            .record(
                0,
                "read_scratch_file",
                "aaaa".into(),
                serde_json::json!({ HOST_ERROR_KEY: "scratch file not found: data.txt" }),
            )
            .unwrap();
        let before = std::fs::read_to_string(&path).unwrap();
        assert!(
            !journal
                .prune_trailing_host_error("Runtime error: array index out of bounds (line 9)")
                .unwrap(),
            "a caught trailing sentinel must keep replaying when the run failed elsewhere"
        );
        assert_eq!(journal.len(), 1);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), before);
    }

    #[test]
    fn prune_is_a_noop_on_empty_journal() {
        let mut journal = Journal::new(None);
        assert!(!journal.prune_trailing_host_error("boom").unwrap());

        let dir = tempfile::tempdir().unwrap();
        let mut loaded = Journal::load(dir.path().join("missing.jsonl")).unwrap();
        assert!(!loaded.prune_trailing_host_error("boom").unwrap());
    }

    #[test]
    fn sqlite_state_store_replays_and_transactionally_prunes_one_stream() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("workflow-state.sqlite");
        let mut first = Journal::load_sqlite(path.clone(), "run-a").unwrap();
        first
            .record(
                0,
                "spawn_agent",
                "hash-a".into(),
                serde_json::json!({"ok": true}),
            )
            .unwrap();
        first
            .record(
                1,
                "write_scratch_file",
                "hash-b".into(),
                serde_json::json!({ HOST_ERROR_KEY: "quota" }),
            )
            .unwrap();
        let mut other = Journal::load_sqlite(path.clone(), "run-b").unwrap();
        other
            .record(0, "log", "other".into(), serde_json::json!("separate"))
            .unwrap();

        let mut restored = Journal::load_sqlite(path.clone(), "run-a").unwrap();
        assert_eq!(restored.len(), 2);
        assert_eq!(
            restored.replay(0, "spawn_agent", "hash-a").unwrap(),
            Some(serde_json::json!({"ok": true}))
        );
        assert!(restored.prune_trailing_host_error("quota").unwrap());
        assert_eq!(
            Journal::load_sqlite(path.clone(), "run-a").unwrap().len(),
            1
        );
        assert_eq!(Journal::load_sqlite(path, "run-b").unwrap().len(), 1);
    }

    #[test]
    fn request_hash_is_stable() {
        let a = request_hash("k", &serde_json::json!({"b": 2, "a": 1}));
        let b = request_hash("k", &serde_json::json!({"a": 1, "b": 2}));
        assert_eq!(a, b, "map key order must not affect the hash");
    }
}
