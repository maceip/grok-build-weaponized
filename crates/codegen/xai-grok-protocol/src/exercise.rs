use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
    ArtifactId, EvidenceId, ExerciseId, FindingId, OperationRunId, OperatorSessionId, PlaybookId,
    ProtocolError, ProtocolErrorCode, TargetId, TaskId, WorkspaceId,
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExerciseStatus {
    #[default]
    Active,
    Paused,
    Completed,
    Archived,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationRunStatus {
    #[default]
    Planned,
    Running,
    Paused,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorSessionStatus {
    #[default]
    Active,
    Idle,
    Closed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetKind {
    Host,
    Network,
    Domain,
    Url,
    CloudAccount,
    Identity,
    Repository,
    Other,
}

/// One explicit in-scope or out-of-scope selector. Targets are data, not prose,
/// so every surface and execution provider can apply the same scope boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeTarget {
    pub target_id: TargetId,
    pub kind: TargetKind,
    pub selector: String,
    #[serde(default)]
    pub excluded: bool,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

/// Parse the compact scope syntax used by local operator surfaces. Commas or
/// newlines separate selectors; a leading `!` creates an explicit exclusion.
pub fn parse_scope_targets(input: &str) -> Vec<ScopeTarget> {
    input
        .split([',', '\n'])
        .filter_map(|raw| {
            let raw = raw.trim();
            if raw.is_empty() {
                return None;
            }
            let (excluded, selector) = raw
                .strip_prefix('!')
                .map_or((false, raw), |value| (true, value.trim()));
            if selector.is_empty() {
                return None;
            }
            let kind = if selector.starts_with("http://") || selector.starts_with("https://") {
                TargetKind::Url
            } else if selector.contains('/') {
                TargetKind::Network
            } else if selector.parse::<std::net::IpAddr>().is_ok() {
                TargetKind::Host
            } else if selector.contains('.') {
                TargetKind::Domain
            } else {
                TargetKind::Other
            };
            Some(ScopeTarget {
                target_id: TargetId::new(),
                kind,
                selector: selector.to_owned(),
                excluded,
                labels: BTreeMap::new(),
            })
        })
        .collect()
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExerciseObjective {
    pub objective_id: String,
    pub statement: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub completion_tests: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateExercise {
    pub workspace_id: WorkspaceId,
    pub name: String,
    pub objective: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scope: Vec<ScopeTarget>,
}

impl CreateExercise {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        required("exercise workspace_id", self.workspace_id.as_str())?;
        required("exercise name", &self.name)?;
        required("exercise objective", &self.objective)?;
        for target in &self.scope {
            required("scope selector", &target.selector)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Exercise {
    pub exercise_id: ExerciseId,
    pub workspace_id: WorkspaceId,
    pub name: String,
    pub objective: String,
    pub status: ExerciseStatus,
    pub scope: Vec<ScopeTarget>,
    pub created_unix_ms: u64,
    pub updated_unix_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateOperationRun {
    pub exercise_id: ExerciseId,
    pub name: String,
    pub objective: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub playbook_id: Option<PlaybookId>,
}

impl CreateOperationRun {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        required("operation run name", &self.name)?;
        required("operation run objective", &self.objective)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationRun {
    pub operation_run_id: OperationRunId,
    pub exercise_id: ExerciseId,
    pub name: String,
    pub objective: String,
    pub playbook_id: Option<PlaybookId>,
    pub status: OperationRunStatus,
    pub created_unix_ms: u64,
    pub updated_unix_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateOperatorSession {
    pub exercise_id: ExerciseId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_run_id: Option<OperationRunId>,
    pub name: String,
    pub purpose: String,
}

impl CreateOperatorSession {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        required("session name", &self.name)?;
        required("session purpose", &self.purpose)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperatorSession {
    pub session_id: OperatorSessionId,
    pub exercise_id: ExerciseId,
    pub operation_run_id: Option<OperationRunId>,
    pub name: String,
    pub purpose: String,
    pub status: OperatorSessionStatus,
    pub created_unix_ms: u64,
    pub last_active_unix_ms: u64,
}

/// Immutable procedure template. A run references a revision; generated task
/// plans remain run-specific and are not written back into the template.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Playbook {
    pub playbook_id: PlaybookId,
    pub revision: u32,
    pub name: String,
    pub description: String,
    pub steps: Vec<PlaybookStep>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlaybookStep {
    pub step_id: String,
    pub name: String,
    pub capability: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub completion_tests: Vec<String>,
}

/// Durable normalized fact. Complete raw output remains in artifact storage.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExerciseEvidence {
    pub evidence_id: EvidenceId,
    pub exercise_id: ExerciseId,
    pub operation_run_id: Option<OperationRunId>,
    pub session_id: Option<OperatorSessionId>,
    pub task_id: Option<TaskId>,
    pub finding: String,
    pub confidence: f32,
    pub artifact_id: Option<ArtifactId>,
    #[serde(default)]
    pub attributes: BTreeMap<String, String>,
    pub observed_unix_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingStatus {
    Candidate,
    Confirmed,
    Remediated,
    Rejected,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Finding {
    pub finding_id: FindingId,
    pub exercise_id: ExerciseId,
    pub title: String,
    pub status: FindingStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_ids: Vec<EvidenceId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub target_ids: Vec<TargetId>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperatorCatalog {
    pub exercises: Vec<Exercise>,
    pub operation_runs: Vec<OperationRun>,
    pub sessions: Vec<OperatorSession>,
}

fn required(field: &str, value: &str) -> Result<(), ProtocolError> {
    if value.trim().is_empty() {
        return Err(ProtocolError::new(
            ProtocolErrorCode::InvalidEnvelope,
            format!("{field} must not be empty"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exercise_requires_a_named_objective_and_workspace() {
        let create = CreateExercise {
            workspace_id: WorkspaceId::from_string("workspace-1"),
            name: "Quarterly assessment".to_owned(),
            objective: "".to_owned(),
            scope: Vec::new(),
        };
        assert_eq!(
            create.validate().unwrap_err().code,
            ProtocolErrorCode::InvalidEnvelope
        );
    }

    #[test]
    fn scope_can_express_explicit_exclusions() {
        let create = CreateExercise {
            workspace_id: WorkspaceId::from_string("workspace-1"),
            name: "Internal exercise".to_owned(),
            objective: "Map reachable services".to_owned(),
            scope: vec![ScopeTarget {
                target_id: TargetId::new(),
                kind: TargetKind::Network,
                selector: "10.10.4.0/24".to_owned(),
                excluded: true,
                labels: BTreeMap::new(),
            }],
        };
        create.validate().unwrap();
        assert!(create.scope[0].excluded);
    }

    #[test]
    fn compact_scope_preserves_networks_urls_and_exclusions() {
        let targets = parse_scope_targets("10.10.4.0/24, !10.10.4.9, https://portal.test");
        assert_eq!(targets.len(), 3);
        assert_eq!(targets[0].kind, TargetKind::Network);
        assert!(targets[1].excluded);
        assert_eq!(targets[2].kind, TargetKind::Url);
    }
}
