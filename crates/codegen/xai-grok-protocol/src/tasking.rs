use serde::{Deserialize, Serialize};

use crate::{
    ArtifactId, ClientId, EngagementId, ExerciseId, OperationId, OperationRunId, OperatorSessionId,
    ProviderId, RequestId, TaskId, TeamId,
};

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

/// Bounded deterministic predicate language for provider-result admission.
///
/// `CompletionTest` retains its JSON value on the wire for protocol evolution;
/// plans must parse that value into this enum before they can be admitted.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum CompletionPredicate {
    JsonPointerExists {
        pointer: String,
    },
    JsonPointerEquals {
        pointer: String,
        value: serde_json::Value,
    },
    ObservationCount {
        minimum: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        finding_contains: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        minimum_confidence: Option<f32>,
    },
    ArtifactCount {
        minimum: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        media_type: Option<String>,
    },
    All {
        predicates: Vec<CompletionPredicate>,
    },
    Any {
        predicates: Vec<CompletionPredicate>,
    },
    Not {
        predicate: Box<CompletionPredicate>,
    },
}

impl CompletionPredicate {
    pub fn parse(value: &serde_json::Value) -> Result<Self, String> {
        let predicate: Self = serde_json::from_value(value.clone())
            .map_err(|error| format!("invalid completion predicate: {error}"))?;
        let mut nodes = 0;
        predicate.validate_at_depth(0, &mut nodes)?;
        Ok(predicate)
    }

    fn validate_at_depth(&self, depth: usize, nodes: &mut usize) -> Result<(), String> {
        const MAX_DEPTH: usize = 16;
        const MAX_CHILDREN: usize = 64;
        const MAX_NODES: usize = 1_024;
        const MAX_EXPECTED_JSON_BYTES: usize = 64 * 1_024;
        if depth > MAX_DEPTH {
            return Err(format!(
                "completion predicate nesting exceeds {MAX_DEPTH} levels"
            ));
        }
        *nodes = nodes.saturating_add(1);
        if *nodes > MAX_NODES {
            return Err(format!(
                "completion predicate contains more than {MAX_NODES} nodes"
            ));
        }
        match self {
            Self::JsonPointerExists { pointer } => validate_json_pointer(pointer),
            Self::JsonPointerEquals { pointer, value } => {
                validate_json_pointer(pointer)?;
                let byte_size = serde_json::to_vec(value)
                    .map_err(|error| format!("completion expected value is invalid: {error}"))?
                    .len();
                if byte_size > MAX_EXPECTED_JSON_BYTES {
                    return Err(format!(
                        "completion expected value is {byte_size} bytes; maximum is {MAX_EXPECTED_JSON_BYTES}"
                    ));
                }
                Ok(())
            }
            Self::ObservationCount {
                minimum,
                finding_contains,
                minimum_confidence,
            } => {
                if *minimum == 0 {
                    return Err("observation_count minimum must be greater than zero".to_owned());
                }
                if let Some(needle) = finding_contains
                    && (needle.trim().is_empty() || needle.len() > 4_096)
                {
                    return Err(
                        "observation_count finding_contains must contain 1..=4096 bytes".to_owned(),
                    );
                }
                if minimum_confidence.is_some_and(|value| {
                    !value.is_finite() || !(0.0_f32..=1.0_f32).contains(&value)
                }) {
                    return Err(
                        "observation_count minimum_confidence must be finite and within 0..=1"
                            .to_owned(),
                    );
                }
                Ok(())
            }
            Self::ArtifactCount {
                minimum,
                media_type,
            } => {
                if *minimum == 0 {
                    return Err("artifact_count minimum must be greater than zero".to_owned());
                }
                if let Some(media_type) = media_type
                    && (media_type.trim().is_empty() || media_type.len() > 255)
                {
                    return Err("artifact_count media_type must contain 1..=255 bytes".to_owned());
                }
                Ok(())
            }
            Self::All { predicates } | Self::Any { predicates } => {
                if predicates.is_empty() || predicates.len() > MAX_CHILDREN {
                    return Err(format!(
                        "completion predicate groups must contain 1..={MAX_CHILDREN} children"
                    ));
                }
                for predicate in predicates {
                    predicate.validate_at_depth(depth + 1, nodes)?;
                }
                Ok(())
            }
            Self::Not { predicate } => predicate.validate_at_depth(depth + 1, nodes),
        }
    }
}

fn validate_json_pointer(pointer: &str) -> Result<(), String> {
    if pointer.is_empty() || !pointer.starts_with('/') || pointer.len() > 4_096 {
        return Err(
            "completion JSON pointer must contain 1..=4096 bytes and start with '/'".into(),
        );
    }
    // RFC 6901 permits only ~0 and ~1 escape sequences.
    let bytes = pointer.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'~' {
            let Some(escaped) = bytes.get(index + 1) else {
                return Err("completion JSON pointer ends with an incomplete escape".to_owned());
            };
            if !matches!(escaped, b'0' | b'1') {
                return Err("completion JSON pointer contains an invalid escape".to_owned());
            }
            index += 1;
        }
        index += 1;
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletionTestResult {
    pub description: String,
    pub mandatory: bool,
    pub passed: bool,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskCompletionProjection {
    pub sequence: u64,
    pub mandatory_passed: bool,
    pub results: Vec<CompletionTestResult>,
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
        const MAX_COMPLETION_TESTS_PER_TASK: usize = 64;
        const MAX_COMPLETION_DESCRIPTION_BYTES: usize = 512;
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
            if task.completion_tests.len() > MAX_COMPLETION_TESTS_PER_TASK {
                return Err(format!(
                    "task {} has more than {MAX_COMPLETION_TESTS_PER_TASK} completion tests",
                    task.task_id
                ));
            }
            if task.completion_tests.iter().any(|test| {
                test.description.trim().is_empty()
                    || test.description.len() > MAX_COMPLETION_DESCRIPTION_BYTES
            }) {
                return Err(format!(
                    "task {} has a completion test description outside 1..={MAX_COMPLETION_DESCRIPTION_BYTES} bytes",
                    task.task_id
                ));
            }
            for test in &task.completion_tests {
                CompletionPredicate::parse(&test.predicate).map_err(|error| {
                    format!(
                        "task {} has an invalid completion test {:?}: {error}",
                        task.task_id, test.description
                    )
                })?;
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
pub struct TaskObservationProjection {
    pub sequence: u64,
    pub observation: EvidenceObservation,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskArtifactProjection {
    pub artifact_id: ArtifactId,
    pub media_type: String,
    pub byte_size: u64,
    pub sequence: u64,
    #[serde(default)]
    pub provider_output: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TaskProjection {
    pub task: ExecutionTask,
    pub status: TaskStatus,
    pub provider_id: Option<ProviderId>,
    /// Durable mode-specific dispatch identity. This is populated once the
    /// task is admitted to a provider and retained through terminal status
    /// changes so a new client can reattach or cancel after reconnecting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<crate::ExecutionReceipt>,
    #[serde(default)]
    pub observations: Vec<TaskObservationProjection>,
    #[serde(default)]
    pub artifacts: Vec<TaskArtifactProjection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion: Option<TaskCompletionProjection>,
    pub last_sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TaskGraphProjection {
    pub engagement_id: EngagementId,
    /// Durable ownership copied from the engagement projection. These fields
    /// let stateless clients scope graphs without retaining historical events.
    #[serde(default)]
    pub workspace_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub exercise_id: Option<ExerciseId>,
    #[serde(default)]
    pub operation_run_id: Option<OperationRunId>,
    #[serde(default)]
    pub operator_session_id: Option<OperatorSessionId>,
    #[serde(default)]
    pub team_id: Option<TeamId>,
    #[serde(default)]
    pub client_id: Option<ClientId>,
    pub revision: u32,
    pub objective: String,
    pub tasks: Vec<TaskProjection>,
    pub last_sequence: u64,
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

    fn task_with_completion_tests(completion_tests: Vec<CompletionTest>) -> ExecutionTask {
        ExecutionTask {
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
            completion_tests,
            depends_on: Vec::new(),
        }
    }

    fn plan_with_completion_tests(completion_tests: Vec<CompletionTest>) -> TaskingPlan {
        TaskingPlan {
            engagement_id: "eng".into(),
            revision: 1,
            objective: "test".to_owned(),
            tasks: vec![task_with_completion_tests(completion_tests)],
        }
    }

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

    #[test]
    fn plan_accepts_nested_bounded_completion_predicates() {
        let plan = plan_with_completion_tests(vec![CompletionTest {
            description: "successful output and retained evidence".to_owned(),
            mandatory: true,
            predicate: serde_json::json!({
                "op":"all",
                "predicates":[
                    {
                        "op":"json_pointer_equals",
                        "pointer":"/status~1code",
                        "value":200
                    },
                    {
                        "op":"any",
                        "predicates":[
                            {"op":"observation_count","minimum":1},
                            {"op":"artifact_count","minimum":1,"media_type":"application/xml"}
                        ]
                    }
                ]
            }),
        }]);
        plan.validate().unwrap();
    }

    #[test]
    fn plan_rejects_malformed_or_unbounded_completion_predicates() {
        for predicate in [
            serde_json::json!({"op":"json_pointer_exists","pointer":"not-a-pointer"}),
            serde_json::json!({"op":"json_pointer_exists","pointer":"/bad~2escape"}),
            serde_json::json!({"op":"observation_count","minimum":0}),
            serde_json::json!({"op":"all","predicates":[]}),
        ] {
            let plan = plan_with_completion_tests(vec![CompletionTest {
                description: "invalid".to_owned(),
                mandatory: true,
                predicate,
            }]);
            assert!(plan.validate().is_err());
        }

        let mut predicate = serde_json::json!({
            "op":"json_pointer_exists",
            "pointer":"/value"
        });
        for _ in 0..17 {
            predicate = serde_json::json!({"op":"not","predicate":predicate});
        }
        let plan = plan_with_completion_tests(vec![CompletionTest {
            description: "too deep".to_owned(),
            mandatory: true,
            predicate,
        }]);
        assert!(plan.validate().is_err());

        let tests = (0..65)
            .map(|index| CompletionTest {
                description: format!("criterion {index}"),
                mandatory: false,
                predicate: serde_json::json!({
                    "op":"json_pointer_exists",
                    "pointer":"/value"
                }),
            })
            .collect();
        assert!(plan_with_completion_tests(tests).validate().is_err());

        let plan = plan_with_completion_tests(vec![CompletionTest {
            description: "oversized expected JSON".to_owned(),
            mandatory: true,
            predicate: serde_json::json!({
                "op":"json_pointer_equals",
                "pointer":"/value",
                "value":"x".repeat(65 * 1024)
            }),
        }]);
        assert!(plan.validate().is_err());
    }

    #[test]
    fn older_task_projection_defaults_completion_to_none() {
        let projection = TaskProjection {
            task: task_with_completion_tests(Vec::new()),
            status: TaskStatus::Prepared,
            provider_id: None,
            execution: None,
            observations: Vec::new(),
            artifacts: Vec::new(),
            completion: None,
            last_sequence: 1,
        };
        let mut serialized = serde_json::to_value(&projection).unwrap();
        serialized.as_object_mut().unwrap().remove("completion");
        let replayed: TaskProjection = serde_json::from_value(serialized).unwrap();
        assert_eq!(replayed.completion, None);
    }
}
