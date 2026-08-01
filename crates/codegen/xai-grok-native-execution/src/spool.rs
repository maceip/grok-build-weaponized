use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _, AsyncWriteExt as _};
use tokio::sync::Mutex;

const HEADER_BYTES: usize = 13;
const MAX_RECORD_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputStream {
    Stdout,
    Stderr,
}

impl OutputStream {
    fn as_byte(self) -> u8 {
        match self {
            Self::Stdout => 1,
            Self::Stderr => 2,
        }
    }

    fn from_byte(value: u8) -> std::io::Result<Self> {
        match value {
            1 => Ok(Self::Stdout),
            2 => Ok(Self::Stderr),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid output stream tag {value}"),
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputRecord {
    pub sequence: u64,
    pub stream: OutputStream,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputPage {
    pub cursor: u64,
    pub next_cursor: Option<u64>,
    pub records: Vec<OutputRecord>,
}

pub(crate) struct SequencedSpool {
    file: Mutex<tokio::fs::File>,
    next_sequence: AtomicU64,
    maximum_bytes: u64,
}

impl SequencedSpool {
    pub(crate) async fn open(path: &Path, maximum_bytes: u64) -> std::io::Result<Self> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)
            .await?;
        let next_sequence = count_records(path).await?.saturating_add(1);
        Ok(Self {
            file: Mutex::new(file),
            next_sequence: AtomicU64::new(next_sequence),
            maximum_bytes,
        })
    }

    pub(crate) async fn append(&self, stream: OutputStream, bytes: &[u8]) -> std::io::Result<u64> {
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "native output record exceeds 1 MiB",
            ));
        }
        let mut file = self.file.lock().await;
        let current = file.metadata().await?.len();
        let appended = HEADER_BYTES as u64 + bytes.len() as u64;
        if current.saturating_add(appended) > self.maximum_bytes {
            return Err(std::io::Error::other("native job spool limit reached"));
        }
        let sequence = self.next_sequence.fetch_add(1, Ordering::AcqRel);
        file.write_all(&sequence.to_be_bytes()).await?;
        file.write_all(&[stream.as_byte()]).await?;
        file.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
        file.write_all(bytes).await?;
        file.flush().await?;
        Ok(sequence)
    }
}

pub(crate) async fn read_page(
    path: &Path,
    cursor: u64,
    maximum_records: usize,
    maximum_bytes: usize,
) -> std::io::Result<OutputPage> {
    let mut file = tokio::fs::File::open(path).await?;
    let length = file.metadata().await?.len();
    let mut position = cursor.min(length);
    file.seek(std::io::SeekFrom::Start(position)).await?;
    let mut records = Vec::new();
    let mut returned_bytes = 0_usize;
    while position < length && records.len() < maximum_records {
        let mut header = [0_u8; HEADER_BYTES];
        match file.read_exact(&mut header).await {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "truncated native output spool header",
                ));
            }
            Err(error) => return Err(error),
        }
        let sequence = u64::from_be_bytes(header[..8].try_into().expect("eight bytes"));
        let stream = OutputStream::from_byte(header[8])?;
        let record_bytes =
            u32::from_be_bytes(header[9..13].try_into().expect("four bytes")) as usize;
        if record_bytes > MAX_RECORD_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "native output spool record exceeds limit",
            ));
        }
        if !records.is_empty() && returned_bytes.saturating_add(record_bytes) > maximum_bytes {
            break;
        }
        let mut bytes = vec![0; record_bytes];
        file.read_exact(&mut bytes).await?;
        position = position.saturating_add(HEADER_BYTES as u64 + record_bytes as u64);
        returned_bytes = returned_bytes.saturating_add(record_bytes);
        records.push(OutputRecord {
            sequence,
            stream,
            bytes,
        });
    }
    Ok(OutputPage {
        cursor,
        next_cursor: (position < length).then_some(position),
        records,
    })
}

async fn count_records(path: &Path) -> std::io::Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    let mut file = tokio::fs::File::open(path).await?;
    let mut count = 0_u64;
    loop {
        let mut header = [0_u8; HEADER_BYTES];
        match file.read_exact(&mut header).await {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error),
        }
        let record_bytes = u32::from_be_bytes(header[9..13].try_into().expect("four bytes"));
        file.seek(std::io::SeekFrom::Current(i64::from(record_bytes)))
            .await?;
        count = count.saturating_add(1);
    }
    Ok(count)
}
