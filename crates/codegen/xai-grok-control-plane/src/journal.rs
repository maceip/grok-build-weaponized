use std::collections::HashMap;
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
    #[error("event journal index expected sequence {expected} but decoded {actual}")]
    IndexMismatch { expected: u64, actual: u64 },
    #[error("event journal lock is poisoned")]
    Poisoned,
}

/// Append-only, sync-on-append event journal.
///
/// Records are length-prefixed MessagePack frames. A partially persisted final
/// frame is detected during replay and never silently ignored.
pub struct EventJournal {
    path: PathBuf,
    state: Mutex<JournalState>,
}

struct JournalState {
    writer: File,
    index: JournalIndex,
}

#[derive(Default)]
struct JournalIndex {
    records: Vec<RecordIndex>,
    by_engagement: HashMap<EngagementId, Vec<usize>>,
}

#[derive(Clone, Copy)]
struct RecordIndex {
    sequence: u64,
    offset: u64,
}

impl JournalIndex {
    fn push(&mut self, event: &EventEnvelope, offset: u64) {
        let position = self.records.len();
        self.records.push(RecordIndex {
            sequence: event.sequence,
            offset,
        });
        if let Some(engagement_id) = &event.engagement_id {
            self.by_engagement
                .entry(engagement_id.clone())
                .or_default()
                .push(position);
        }
    }

    fn last_sequence(&self) -> Option<u64> {
        self.records.last().map(|record| record.sequence)
    }
}

impl EventJournal {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, JournalError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let index = build_index(&path)?;
        let writer = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;
        Ok(Self {
            path,
            state: Mutex::new(JournalState { writer, index }),
        })
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
        let mut state = self.state.lock().map_err(|_| JournalError::Poisoned)?;
        if let Some(previous) = state.index.last_sequence()
            && previous.checked_add(1) != Some(event.sequence)
        {
            return Err(JournalError::NonMonotonic {
                previous,
                current: event.sequence,
            });
        }
        let offset = state.writer.seek(std::io::SeekFrom::End(0))?;
        state.writer.write_all(&length)?;
        state.writer.write_all(&bytes)?;
        state.writer.write_all(checksum.as_bytes())?;
        state.writer.sync_data()?;
        state.index.push(event, offset);
        Ok(())
    }

    pub fn replay(&self) -> Result<Vec<EventEnvelope>, JournalError> {
        let state = self.state.lock().map_err(|_| JournalError::Poisoned)?;
        let mut reader = File::open(&self.path)?;
        reader.seek(std::io::SeekFrom::Start(0))?;
        let mut events = Vec::with_capacity(state.index.records.len());
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
    /// Opening the journal verifies every frame while constructing a sequence
    /// and engagement index. Cursor reads seek directly to indexed records and
    /// revalidate their frame checksums, keeping both work and result memory
    /// bounded by `maximum_events`.
    pub fn read_after(
        &self,
        after_sequence: u64,
        maximum_events: usize,
        engagement_id: Option<&EngagementId>,
    ) -> Result<(Vec<EventEnvelope>, u64), JournalError> {
        let (events, scanned_through, _) =
            self.read_after_indexed(after_sequence, maximum_events, engagement_id)?;
        Ok((events, scanned_through))
    }

    fn read_after_indexed(
        &self,
        after_sequence: u64,
        maximum_events: usize,
        engagement_id: Option<&EngagementId>,
    ) -> Result<(Vec<EventEnvelope>, u64, usize), JournalError> {
        if maximum_events == 0 {
            return Ok((Vec::new(), after_sequence, 0));
        }
        let state = self.state.lock().map_err(|_| JournalError::Poisoned)?;
        let mut reader = File::open(&self.path)?;
        let mut events = Vec::with_capacity(maximum_events);
        let mut records_read = 0;
        let matching_positions = engagement_id.and_then(|id| state.index.by_engagement.get(id));
        let remaining_matches;
        let positions: Box<dyn Iterator<Item = usize> + '_> = if engagement_id.is_some() {
            let matching_positions = matching_positions.map_or(&[][..], Vec::as_slice);
            let start = matching_positions.partition_point(|position| {
                state.index.records[*position].sequence <= after_sequence
            });
            remaining_matches = matching_positions.len().saturating_sub(start);
            Box::new(
                matching_positions[start..]
                    .iter()
                    .copied()
                    .take(maximum_events),
            )
        } else {
            let start = state
                .index
                .records
                .partition_point(|record| record.sequence <= after_sequence);
            remaining_matches = state.index.records.len().saturating_sub(start);
            Box::new(start..state.index.records.len().min(start + maximum_events))
        };
        for position in positions {
            let record = state.index.records[position];
            events.push(read_indexed_event(&mut reader, record)?);
            records_read += 1;
        }
        let scanned_through = if remaining_matches < maximum_events {
            state.index.last_sequence().unwrap_or(after_sequence)
        } else {
            events.last().map_or(after_sequence, |event| event.sequence)
        };
        Ok((events, scanned_through, records_read))
    }
}

fn build_index(path: &Path) -> Result<JournalIndex, JournalError> {
    if !path.exists() {
        return Ok(JournalIndex::default());
    }
    let mut reader = File::open(path)?;
    let mut index = JournalIndex::default();
    let mut previous = None;
    loop {
        let offset = reader.stream_position()?;
        let Some(event) = read_event_record(&mut reader, &mut previous)? else {
            break;
        };
        index.push(&event, offset);
    }
    Ok(index)
}

fn read_indexed_event(
    reader: &mut File,
    record: RecordIndex,
) -> Result<EventEnvelope, JournalError> {
    reader.seek(std::io::SeekFrom::Start(record.offset))?;
    let event = read_event_record(reader, &mut None)?.ok_or(JournalError::Truncated)?;
    if event.sequence != record.sequence {
        return Err(JournalError::IndexMismatch {
            expected: record.sequence,
            actual: event.sequence,
        });
    }
    Ok(event)
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

    #[test]
    fn cursor_reads_seek_directly_to_the_requested_tail() {
        let directory = tempfile::tempdir().unwrap();
        let journal = EventJournal::open(directory.path().join("events.bin")).unwrap();
        for sequence in 1..=32 {
            journal.append(&event(sequence)).unwrap();
        }

        let (events, scanned_through, records_read) =
            journal.read_after_indexed(29, 2, None).unwrap();
        assert_eq!(
            events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![30, 31]
        );
        assert_eq!(scanned_through, 31);
        assert_eq!(records_read, 2);

        let (events, scanned_through, records_read) =
            journal.read_after_indexed(31, 8, None).unwrap();
        assert_eq!(events[0].sequence, 32);
        assert_eq!(scanned_through, 32);
        assert_eq!(records_read, 1);
    }

    #[test]
    fn engagement_cursor_uses_its_index_and_advances_across_irrelevant_tail() {
        let directory = tempfile::tempdir().unwrap();
        let journal = EventJournal::open(directory.path().join("events.bin")).unwrap();
        let selected: EngagementId = "selected".into();
        for sequence in 1..=32 {
            let mut envelope = event(sequence);
            if sequence == 7 || sequence == 17 {
                envelope.engagement_id = Some(selected.clone());
            } else {
                envelope.engagement_id = Some("other".into());
            }
            journal.append(&envelope).unwrap();
        }

        let (events, scanned_through, records_read) =
            journal.read_after_indexed(7, 16, Some(&selected)).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].sequence, 17);
        assert_eq!(records_read, 1);
        assert_eq!(scanned_through, 32);
    }

    #[test]
    fn append_rejects_non_monotonic_sequences_before_persisting() {
        let directory = tempfile::tempdir().unwrap();
        let journal = EventJournal::open(directory.path().join("events.bin")).unwrap();
        journal.append(&event(10)).unwrap();
        assert!(matches!(
            journal.append(&event(12)),
            Err(JournalError::NonMonotonic {
                previous: 10,
                current: 12
            })
        ));
        assert_eq!(journal.replay().unwrap().len(), 1);
    }
}
