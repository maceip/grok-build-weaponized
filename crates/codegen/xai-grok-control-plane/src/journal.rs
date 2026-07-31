use std::fs::{File, OpenOptions};
use std::io::{Read as _, Seek as _, Write as _};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use xai_grok_protocol::{EngagementId, EventEnvelope};

const MAX_EVENT_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    #[error("event journal I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("event journal serialization: {0}")]
    Encode(#[from] rmp_serde::encode::Error),
    #[error("event journal decoding: {0}")]
    Decode(#[from] rmp_serde::decode::Error),
    #[error("event journal record is {actual} bytes; maximum is {maximum}")]
    RecordTooLarge { actual: usize, maximum: usize },
    #[error("event journal has a truncated record")]
    Truncated,
    #[error("event journal record checksum mismatch")]
    ChecksumMismatch,
    #[error("event sequence {current} does not follow {previous}")]
    NonMonotonic { previous: u64, current: u64 },
    #[error("event journal lock is poisoned")]
    Poisoned,
}

/// Append-only, sync-on-append event journal.
///
/// Records are length-prefixed MessagePack frames. A partially persisted final
/// frame is detected during replay and never silently ignored.
pub struct EventJournal {
    path: PathBuf,
    writer: Mutex<File>,
}

impl EventJournal {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, JournalError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let writer = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;
        let journal = Self {
            path,
            writer: Mutex::new(writer),
        };
        journal.replay()?;
        Ok(journal)
    }

    pub fn append(&self, event: &EventEnvelope) -> Result<(), JournalError> {
        let bytes = rmp_serde::to_vec_named(event)?;
        if bytes.len() > MAX_EVENT_BYTES {
            return Err(JournalError::RecordTooLarge {
                actual: bytes.len(),
                maximum: MAX_EVENT_BYTES,
            });
        }
        let length = u32::try_from(bytes.len())
            .map_err(|_| JournalError::RecordTooLarge {
                actual: bytes.len(),
                maximum: u32::MAX as usize,
            })?
            .to_be_bytes();
        let checksum = blake3::hash(&bytes);
        let mut writer = self.writer.lock().map_err(|_| JournalError::Poisoned)?;
        writer.write_all(&length)?;
        writer.write_all(&bytes)?;
        writer.write_all(checksum.as_bytes())?;
        writer.sync_data()?;
        Ok(())
    }

    pub fn replay(&self) -> Result<Vec<EventEnvelope>, JournalError> {
        let _writer_guard = self.writer.lock().map_err(|_| JournalError::Poisoned)?;
        let mut reader = File::open(&self.path)?;
        reader.seek(std::io::SeekFrom::Start(0))?;
        let mut events = Vec::new();
        let mut previous: Option<u64> = None;
        loop {
            let mut header = [0_u8; 4];
            match reader.read(&mut header)? {
                0 => break,
                4 => {}
                _ => return Err(JournalError::Truncated),
            }
            let length = u32::from_be_bytes(header) as usize;
            if length > MAX_EVENT_BYTES {
                return Err(JournalError::RecordTooLarge {
                    actual: length,
                    maximum: MAX_EVENT_BYTES,
                });
            }
            let mut bytes = vec![0; length];
            reader
                .read_exact(&mut bytes)
                .map_err(|error| match error.kind() {
                    std::io::ErrorKind::UnexpectedEof => JournalError::Truncated,
                    _ => JournalError::Io(error),
                })?;
            let mut checksum = [0_u8; 32];
            reader
                .read_exact(&mut checksum)
                .map_err(|error| match error.kind() {
                    std::io::ErrorKind::UnexpectedEof => JournalError::Truncated,
                    _ => JournalError::Io(error),
                })?;
            if blake3::hash(&bytes).as_bytes() != &checksum {
                return Err(JournalError::ChecksumMismatch);
            }
            let event: EventEnvelope = rmp_serde::from_slice(&bytes)?;
            if let Some(previous) = previous
                && previous.checked_add(1) != Some(event.sequence)
            {
                return Err(JournalError::NonMonotonic {
                    previous,
                    current: event.sequence,
                });
            }
            previous = Some(event.sequence);
            events.push(event);
        }
        Ok(events)
    }

    /// Read a bounded cursor page without materializing the complete journal.
    ///
    /// The startup replay verifies the entire journal. Cursor reads retain the
    /// same frame and checksum checks for every record they traverse while
    /// keeping result memory bounded by `maximum_events`.
    pub fn read_after(
        &self,
        after_sequence: u64,
        maximum_events: usize,
        engagement_id: Option<&EngagementId>,
    ) -> Result<(Vec<EventEnvelope>, u64), JournalError> {
        let _writer_guard = self.writer.lock().map_err(|_| JournalError::Poisoned)?;
        let mut reader = File::open(&self.path)?;
        let mut events = Vec::with_capacity(maximum_events);
        let mut previous: Option<u64> = None;
        let mut scanned_through = after_sequence;
        while events.len() < maximum_events {
            let Some(event) = read_event_record(&mut reader, &mut previous)? else {
                break;
            };
            if event.sequence <= after_sequence {
                continue;
            }
            scanned_through = event.sequence;
            if engagement_id.is_none_or(|expected| event.engagement_id.as_ref() == Some(expected)) {
                events.push(event);
            }
        }
        Ok((events, scanned_through))
    }
}

fn read_event_record(
    reader: &mut File,
    previous: &mut Option<u64>,
) -> Result<Option<EventEnvelope>, JournalError> {
    let mut header = [0_u8; 4];
    match reader.read(&mut header)? {
        0 => return Ok(None),
        4 => {}
        _ => return Err(JournalError::Truncated),
    }
    let length = u32::from_be_bytes(header) as usize;
    if length > MAX_EVENT_BYTES {
        return Err(JournalError::RecordTooLarge {
            actual: length,
            maximum: MAX_EVENT_BYTES,
        });
    }
    let mut bytes = vec![0; length];
    reader
        .read_exact(&mut bytes)
        .map_err(|error| match error.kind() {
            std::io::ErrorKind::UnexpectedEof => JournalError::Truncated,
            _ => JournalError::Io(error),
        })?;
    let mut checksum = [0_u8; 32];
    reader
        .read_exact(&mut checksum)
        .map_err(|error| match error.kind() {
            std::io::ErrorKind::UnexpectedEof => JournalError::Truncated,
            _ => JournalError::Io(error),
        })?;
    if blake3::hash(&bytes).as_bytes() != &checksum {
        return Err(JournalError::ChecksumMismatch);
    }
    let event: EventEnvelope = rmp_serde::from_slice(&bytes)?;
    if let Some(previous) = *previous
        && previous.checked_add(1) != Some(event.sequence)
    {
        return Err(JournalError::NonMonotonic {
            previous,
            current: event.sequence,
        });
    }
    *previous = Some(event.sequence);
    Ok(Some(event))
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use xai_grok_protocol::{Event, EventId, PROTOCOL_VERSION};

    use super::*;

    fn event(sequence: u64) -> EventEnvelope {
        EventEnvelope {
            protocol_version: PROTOCOL_VERSION,
            event_id: EventId::new(),
            engagement_id: None,
            sequence,
            causation_id: None,
            generation: 0,
            observed_unix_ms: 1,
            event: Event::Overload {
                component: "test".to_owned(),
                queue_depth: 1,
                capacity: 1,
            },
        }
    }

    #[test]
    fn journal_round_trips_and_rejects_truncation() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("events.bin");
        let journal = EventJournal::open(&path).unwrap();
        journal.append(&event(1)).unwrap();
        journal.append(&event(2)).unwrap();
        assert_eq!(journal.replay().unwrap().len(), 2);
        drop(journal);

        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&[0, 0, 0, 4, 1])
            .unwrap();
        assert!(matches!(
            EventJournal::open(path),
            Err(JournalError::Truncated)
        ));
    }

    #[test]
    fn journal_rejects_validly_sized_corruption() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("events.bin");
        let journal = EventJournal::open(&path).unwrap();
        journal.append(&event(1)).unwrap();
        drop(journal);
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.last_mut().unwrap();
        *last ^= 0xff;
        std::fs::write(&path, bytes).unwrap();
        assert!(matches!(
            EventJournal::open(path),
            Err(JournalError::ChecksumMismatch)
        ));
    }
}
