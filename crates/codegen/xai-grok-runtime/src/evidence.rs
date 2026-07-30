use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EvidenceId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum EvidenceSource {
    ToolResult { tool_call_id: String },
    ExecutorReport { model_id: Option<String> },
    WorkspaceMemory { chunk_id: String, path: PathBuf },
    SessionMemory { event_id: String },
    NativeJob { job_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRef {
    pub artifact_id: String,
    pub path: Option<PathBuf>,
    pub cursor: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvidenceRecord {
    pub evidence_id: EvidenceId,
    pub source: EvidenceSource,
    pub finding: String,
    pub artifact: Option<ArtifactRef>,
    pub confidence: f32,
    pub observed_at: SystemTime,
}

#[derive(Debug, Error, Clone, PartialEq)]
pub enum EvidenceError {
    #[error("evidence ID must not be empty")]
    EmptyId,
    #[error("evidence finding must not be empty")]
    EmptyFinding,
    #[error("evidence confidence must be finite and between zero and one")]
    InvalidConfidence,
    #[error("evidence ID already exists in append-only blackboard: {0}")]
    DuplicateId(String),
}

impl EvidenceRecord {
    pub fn validate(&self) -> Result<(), EvidenceError> {
        if self.evidence_id.0.trim().is_empty() {
            return Err(EvidenceError::EmptyId);
        }
        if self.finding.trim().is_empty() {
            return Err(EvidenceError::EmptyFinding);
        }
        if !self.confidence.is_finite() || !(0.0..=1.0).contains(&self.confidence) {
            return Err(EvidenceError::InvalidConfidence);
        }
        Ok(())
    }
}

/// Session-scoped, append-only typed evidence shared by planner, executor, and
/// reviewer stages. Clones share the same underlying blackboard.
#[derive(Debug, Clone, Default)]
pub struct EvidenceBlackboard {
    records: Arc<Mutex<Vec<EvidenceRecord>>>,
}

impl EvidenceBlackboard {
    pub fn append(&self, record: EvidenceRecord) -> Result<(), EvidenceError> {
        record.validate()?;
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if records
            .iter()
            .any(|existing| existing.evidence_id == record.evidence_id)
        {
            return Err(EvidenceError::DuplicateId(record.evidence_id.0));
        }
        records.push(record);
        Ok(())
    }

    pub fn snapshot(&self) -> Vec<EvidenceRecord> {
        self.records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn len(&self) -> usize {
        self.records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blackboard_is_append_only_and_shared_across_clones() {
        let blackboard = EvidenceBlackboard::default();
        let clone = blackboard.clone();
        let record = EvidenceRecord {
            evidence_id: EvidenceId("e-1".to_string()),
            source: EvidenceSource::NativeJob {
                job_id: "job-1".to_string(),
            },
            finding: "port 443 is open".to_string(),
            artifact: None,
            confidence: 1.0,
            observed_at: SystemTime::UNIX_EPOCH,
        };
        blackboard.append(record.clone()).unwrap();
        assert_eq!(clone.snapshot(), vec![record.clone()]);
        assert!(matches!(
            clone.append(record),
            Err(EvidenceError::DuplicateId(_))
        ));
    }
}
