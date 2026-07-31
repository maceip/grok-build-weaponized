use serde::{Deserialize, Serialize};

use crate::{ArtifactId, EngagementId, OperationId, ProviderId, RequestId, TaskId};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    Interactive,
    Deferred,
    Detached,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Prepared,
    Admitted,
    Dispatched,
    Running,
    Suspended,
    Completed,
    Failed,
    Cancelled,
    Lost,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CapabilityRequirement {
    pub operation_id: OperationId,
    pub preferred_provider: Option<ProviderId>,
    #[serde(default)]
    pub required_features: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompletionTest {
    pub description: String,
    pub predicate: serde_json::Value,
    #[serde(default)]
    pub mandatory: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionTask {
    pub task_id: TaskId,
    pub objective: String,
    pub mode: ExecutionMode,
    pub capability: CapabilityRequirement,
    pub input: serde_json::Value,
    pub deadline_unix_ms: u64,
    #[serde(default)]
    pub completion_tests: Vec<CompletionTest>,
    #[serde(default)]
    pub depends_on: Vec<TaskId>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TaskingPlan {
    pub engagement_id: EngagementId,
    pub revision: u32,
    pub objective: String,
    pub tasks: Vec<ExecutionTask>,
}

impl TaskingPlan {
    pub fn validate(&self) -> Result<(), String> {
        if self.objective.trim().is_empty() {
            return Err("plan objective must not be empty".to_owned());
        }
        if self.revision == 0 {
            return Err("plan revision must be greater than zero".to_owned());
        }
        if self.tasks.is_empty() {
            return Err("plan must contain at least one task".to_owned());
        }
        let ids = self
            .tasks
            .iter()
            .map(|task| task.task_id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        if ids.len() != self.tasks.len() {
            return Err("task ids must be unique".to_owned());
        }
        for task in &self.tasks {
            if task.objective.trim().is_empty() {
                return Err(format!("task {} has no objective", task.task_id));
            }
            if task
                .depends_on
                .iter()
                .any(|dependency| !ids.contains(dependency))
            {
                return Err(format!("task {} has an unknown dependency", task.task_id));
            }
            if task.depends_on.contains(&task.task_id) {
                return Err(format!("task {} depends on itself", task.task_id));
            }
            if task.deadline_unix_ms == 0 {
                return Err(format!("task {} has no deadline", task.task_id));
            }
            if task
                .capability
                .required_features
                .iter()
                .any(|feature| feature.trim().is_empty())
            {
                return Err(format!(
                    "task {} has an empty required feature",
                    task.task_id
                ));
            }
            if task
                .completion_tests
                .iter()
                .any(|test| test.description.trim().is_empty())
            {
                return Err(format!(
                    "task {} has an unnamed completion test",
                    task.task_id
                ));
            }
        }
        let mut remaining_dependencies = self
            .tasks
            .iter()
            .map(|task| (task.task_id.clone(), task.depends_on.len()))
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut dependants = std::collections::BTreeMap::<TaskId, Vec<TaskId>>::new();
        for task in &self.tasks {
            for dependency in &task.depends_on {
                dependants
                    .entry(dependency.clone())
                    .or_default()
                    .push(task.task_id.clone());
            }
        }
        let mut ready = remaining_dependencies
            .iter()
            .filter_map(|(task_id, count)| (*count == 0).then_some(task_id.clone()))
            .collect::<std::collections::VecDeque<_>>();
        let mut visited = 0;
        while let Some(task_id) = ready.pop_front() {
            visited += 1;
            if let Some(tasks) = dependants.get(&task_id) {
                for dependant in tasks {
                    let count = remaining_dependencies
                        .get_mut(dependant)
                        .expect("dependant came from validated task ids");
                    *count -= 1;
                    if *count == 0 {
                        ready.push_back(dependant.clone());
                    }
                }
            }
        }
        if visited != self.tasks.len() {
            return Err("task dependency graph contains a cycle".to_owned());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProviderDispatch {
    pub request_id: RequestId,
    pub engagement_id: EngagementId,
    pub plan_revision: u32,
    pub task: ExecutionTask,
    pub provider_id: ProviderId,
    pub lease_epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EvidenceObservation {
    pub finding: String,
    pub confidence: f32,
    pub artifact_id: Option<ArtifactId>,
    #[serde(default)]
    pub attributes: serde_json::Map<String, serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum CompletionDecision {
    Complete {
        summary: String,
        #[serde(default)]
        evidence: Vec<EvidenceObservation>,
    },
    Incomplete {
        #[serde(default)]
        unresolved: Vec<String>,
        #[serde(default)]
        follow_up: Vec<ExecutionTask>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_rejects_unknown_dependencies() {
        let plan = TaskingPlan {
            engagement_id: "eng".into(),
            revision: 1,
            objective: "test".to_owned(),
            tasks: vec![ExecutionTask {
                task_id: "one".into(),
                objective: "run".to_owned(),
                mode: ExecutionMode::Deferred,
                capability: CapabilityRequirement {
                    operation_id: "scan".into(),
                    preferred_provider: None,
                    required_features: Vec::new(),
                },
                input: serde_json::json!({}),
                deadline_unix_ms: 1,
                completion_tests: Vec::new(),
                depends_on: vec!["missing".into()],
            }],
        };
        assert!(plan.validate().is_err());
    }

    #[test]
    fn plan_rejects_dependency_cycles() {
        let task = |task_id: &str, dependency: &str| ExecutionTask {
            task_id: task_id.into(),
            objective: "run".to_owned(),
            mode: ExecutionMode::Deferred,
            capability: CapabilityRequirement {
                operation_id: "scan".into(),
                preferred_provider: None,
                required_features: Vec::new(),
            },
            input: serde_json::json!({}),
            deadline_unix_ms: 1,
            completion_tests: Vec::new(),
            depends_on: vec![dependency.into()],
        };
        let plan = TaskingPlan {
            engagement_id: "eng".into(),
            revision: 1,
            objective: "test".to_owned(),
            tasks: vec![task("one", "two"), task("two", "one")],
        };
        assert_eq!(
            plan.validate().unwrap_err(),
            "task dependency graph contains a cycle"
        );
    }
}
