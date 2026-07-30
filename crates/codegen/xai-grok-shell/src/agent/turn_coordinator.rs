use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use agent_client_protocol as acp;
use serde::{Deserialize, Serialize};
use xai_grok_sampling_types::ToolDefinition;

use super::config::ModelRouterConfig;
use super::turn_classifier::{Route, RouteDecision};

const META_MODE: &str = "x-grok-cooperation-mode";
const META_PLANNER: &str = "x-grok-planner-model";
const META_EXECUTOR: &str = "x-grok-executor-model";
const META_REVIEWER: &str = "x-grok-reviewer-model";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CooperationMode {
    #[default]
    Direct,
    PlanExecute,
    PlanExecuteReview,
}

impl CooperationMode {
    pub(crate) fn plans(self) -> bool {
        !matches!(self, Self::Direct)
    }

    pub(crate) fn reviews(self) -> bool {
        matches!(self, Self::PlanExecuteReview)
    }

    fn as_meta(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::PlanExecute => "plan_execute",
            Self::PlanExecuteReview => "plan_execute_review",
        }
    }

    fn from_meta(value: &str) -> Option<Self> {
        match value {
            "direct" => Some(Self::Direct),
            "plan_execute" => Some(Self::PlanExecute),
            "plan_execute_review" => Some(Self::PlanExecuteReview),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExecutionTask {
    pub task_id: String,
    pub objective: String,
    #[serde(default)]
    pub tool_families: Vec<String>,
    #[serde(default)]
    pub required_evidence: Vec<String>,
    #[serde(default)]
    pub completion_tests: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PlanPacket {
    pub objective: String,
    pub tasks: Vec<ExecutionTask>,
    #[serde(default)]
    pub required_tool_families: Vec<String>,
    #[serde(default)]
    pub required_evidence: Vec<String>,
    #[serde(default)]
    pub completion_tests: Vec<String>,
}

impl PlanPacket {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.objective.trim().is_empty() {
            return Err("planner returned an empty objective".to_string());
        }
        if self.tasks.is_empty() {
            return Err("planner returned no execution tasks".to_string());
        }
        if self.tasks.len() > 12 {
            return Err("planner returned more than 12 execution tasks".to_string());
        }
        if self
            .tasks
            .iter()
            .any(|task| task.task_id.trim().is_empty() || task.objective.trim().is_empty())
        {
            return Err(
                "planner returned an execution task without an id or objective".to_string(),
            );
        }
        Ok(())
    }

    pub(crate) fn tool_families(&self) -> Vec<String> {
        let mut values = self.required_tool_families.clone();
        for task in &self.tasks {
            values.extend(task.tool_families.iter().cloned());
        }
        values.sort_by_key(|value| value.to_ascii_lowercase());
        values.dedup_by(|left, right| left.eq_ignore_ascii_case(right));
        values
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "decision")]
pub(crate) enum ReviewDecision {
    Accept {
        final_response: String,
        #[serde(default)]
        unresolved: Vec<String>,
    },
    Correct {
        tasks: Vec<ExecutionTask>,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct TurnCooperation {
    pub mode: CooperationMode,
    pub planner_model: String,
    pub executor_model: String,
    pub reviewer_model: String,
    pub plan: Option<PlanPacket>,
    pub active_task_index: usize,
    pub reviewer_memory: Option<Vec<serde_json::Value>>,
    pub reviewer_memory_prefetch_started: bool,
    pub evidence: xai_grok_sampler::litert_lm::EvidenceBlackboard,
}

impl TurnCooperation {
    pub(crate) fn from_blocks(blocks: &[acp::ContentBlock]) -> Option<Self> {
        let meta = blocks.iter().find_map(|block| match block {
            acp::ContentBlock::Text(text) => text.meta.as_ref(),
            _ => None,
        })?;
        let mode = CooperationMode::from_meta(meta.get(META_MODE)?.as_str()?)?;
        Some(Self {
            mode,
            planner_model: meta.get(META_PLANNER)?.as_str()?.to_string(),
            executor_model: meta.get(META_EXECUTOR)?.as_str()?.to_string(),
            reviewer_model: meta.get(META_REVIEWER)?.as_str()?.to_string(),
            plan: None,
            active_task_index: 0,
            reviewer_memory: None,
            reviewer_memory_prefetch_started: false,
            evidence: xai_grok_sampler::litert_lm::EvidenceBlackboard::default(),
        })
    }

    pub(crate) fn current_task(&self) -> Option<&ExecutionTask> {
        self.plan.as_ref()?.tasks.get(self.active_task_index)
    }

    pub(crate) fn has_more_tasks(&self) -> bool {
        self.plan
            .as_ref()
            .is_some_and(|plan| self.active_task_index.saturating_add(1) < plan.tasks.len())
    }
}

pub(crate) fn take_cooperation_directive(
    blocks: &mut [acp::ContentBlock],
) -> Option<Option<CooperationMode>> {
    let text = blocks.iter_mut().find_map(|block| match block {
        acp::ContentBlock::Text(text) => Some(&mut text.text),
        _ => None,
    })?;
    let (first, rest) = text.split_once('\n').unwrap_or((text.as_str(), ""));
    let mode = match first.trim().to_ascii_lowercase().as_str() {
        "/cooperate direct" => Some(CooperationMode::Direct),
        "/cooperate plan" => Some(CooperationMode::PlanExecute),
        "/cooperate full" => Some(CooperationMode::PlanExecuteReview),
        "/cooperate auto" => None,
        _ => return None,
    };
    *text = rest.trim_start_matches(['\r', '\n']).to_owned();
    Some(mode)
}

pub(crate) fn mode_for(
    route: RouteDecision,
    explicit: Option<Option<CooperationMode>>,
) -> CooperationMode {
    if let Some(mode) = explicit {
        return mode.unwrap_or_else(|| mode_for(route.clone(), None));
    }
    match route.route {
        Some(Route::Chat) | None => CooperationMode::Direct,
        Some(Route::Execution) if !route.requires_planning => CooperationMode::Direct,
        Some(Route::Analysis | Route::Execution) => CooperationMode::PlanExecuteReview,
    }
}

pub(crate) fn model_roles(
    config: &ModelRouterConfig,
) -> Result<(String, String, String), &'static str> {
    let planner = config
        .planner_model
        .as_ref()
        .or(config.analysis_model.as_ref())
        .or(config.execution_model.as_ref())
        .or(config.chat_model.as_ref())
        .cloned()
        .ok_or("at least one model role is required")?;
    let executor = config
        .execution_model
        .as_ref()
        .or(config.chat_model.as_ref())
        .cloned()
        .ok_or("execution_model or chat_model is required")?;
    let reviewer = config
        .reviewer_model
        .as_ref()
        .or(config.planner_model.as_ref())
        .or(config.analysis_model.as_ref())
        .or(config.execution_model.as_ref())
        .or(config.chat_model.as_ref())
        .cloned()
        .ok_or("at least one model role is required")?;
    Ok((planner, executor, reviewer))
}

pub(crate) fn stamp(
    blocks: &mut [acp::ContentBlock],
    mode: CooperationMode,
    planner: &str,
    executor: &str,
    reviewer: &str,
) {
    let Some(text) = blocks.iter_mut().find_map(|block| match block {
        acp::ContentBlock::Text(text) => Some(text),
        _ => None,
    }) else {
        return;
    };
    let meta = text.meta.get_or_insert_with(Default::default);
    meta.insert(META_MODE.to_string(), mode.as_meta().into());
    meta.insert(META_PLANNER.to_string(), planner.into());
    meta.insert(META_EXECUTOR.to_string(), executor.into());
    meta.insert(META_REVIEWER.to_string(), reviewer.into());
}

fn registry() -> &'static Mutex<HashMap<String, TurnCooperation>> {
    static REGISTRY: OnceLock<Mutex<HashMap<String, TurnCooperation>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn key(session_id: &str, prompt_id: &str) -> String {
    format!("{session_id}\0{prompt_id}")
}

pub(crate) struct TurnCooperationGuard {
    key: String,
}

impl Drop for TurnCooperationGuard {
    fn drop(&mut self) {
        registry()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.key);
    }
}

pub(crate) fn register(
    session_id: &str,
    prompt_id: &str,
    cooperation: TurnCooperation,
) -> TurnCooperationGuard {
    let key = key(session_id, prompt_id);
    registry()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(key.clone(), cooperation);
    TurnCooperationGuard { key }
}

pub(crate) fn get(session_id: &str, prompt_id: &str) -> Option<TurnCooperation> {
    registry()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&key(session_id, prompt_id))
        .cloned()
}

pub(crate) fn set_plan(session_id: &str, prompt_id: &str, plan: PlanPacket) {
    if let Some(turn) = registry()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get_mut(&key(session_id, prompt_id))
    {
        turn.plan = Some(plan);
        turn.active_task_index = 0;
        turn.reviewer_memory = None;
        turn.reviewer_memory_prefetch_started = false;
    }
}

pub(crate) fn advance_task(
    session_id: &str,
    prompt_id: &str,
) -> Option<(ExecutionTask, usize, usize)> {
    let mut registry = registry()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let turn = registry.get_mut(&key(session_id, prompt_id))?;
    let plan = turn.plan.as_ref()?;
    let next = turn.active_task_index.saturating_add(1);
    let task = plan.tasks.get(next)?.clone();
    turn.active_task_index = next;
    Some((task, next, plan.tasks.len()))
}

pub(crate) fn begin_correction(
    session_id: &str,
    prompt_id: &str,
    tasks: Vec<ExecutionTask>,
) -> Option<(ExecutionTask, usize)> {
    let mut registry = registry()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let turn = registry.get_mut(&key(session_id, prompt_id))?;
    let first = tasks.first()?.clone();
    let count = tasks.len();
    turn.plan.as_mut()?.tasks = tasks;
    turn.active_task_index = 0;
    turn.reviewer_memory = None;
    turn.reviewer_memory_prefetch_started = false;
    Some((first, count))
}

pub(crate) fn begin_reviewer_memory_prefetch(session_id: &str, prompt_id: &str) -> bool {
    let mut registry = registry()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let Some(turn) = registry.get_mut(&key(session_id, prompt_id)) else {
        return false;
    };
    if turn.reviewer_memory_prefetch_started {
        return false;
    }
    turn.reviewer_memory_prefetch_started = true;
    true
}

pub(crate) fn set_reviewer_memory(
    session_id: &str,
    prompt_id: &str,
    evidence: Vec<serde_json::Value>,
) {
    if let Some(turn) = registry()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get_mut(&key(session_id, prompt_id))
    {
        turn.reviewer_memory = Some(evidence);
    }
}

pub(crate) fn take_reviewer_memory(
    session_id: &str,
    prompt_id: &str,
) -> Option<Vec<serde_json::Value>> {
    registry()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get_mut(&key(session_id, prompt_id))?
        .reviewer_memory
        .take()
}

pub(crate) fn degrade_to_direct(session_id: &str, prompt_id: &str) {
    if let Some(turn) = registry()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get_mut(&key(session_id, prompt_id))
    {
        turn.mode = CooperationMode::Direct;
        turn.plan = None;
        turn.active_task_index = 0;
        turn.reviewer_memory = None;
        turn.reviewer_memory_prefetch_started = false;
    }
}

pub(crate) fn select_tools_for_task(
    tools: Vec<ToolDefinition>,
    task: &ExecutionTask,
) -> Vec<ToolDefinition> {
    let families = task
        .tool_families
        .iter()
        .cloned()
        .into_iter()
        .map(|family| family.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let mut selected = tools
        .iter()
        .filter(|tool| {
            let name = tool.function.name.to_ascii_lowercase();
            families
                .iter()
                .any(|family| name.contains(family) || family.contains(&name))
        })
        .take(6)
        .cloned()
        .collect::<Vec<_>>();
    if selected.is_empty() {
        selected.extend(tools.into_iter().take(6));
    }
    selected
}

pub(crate) fn recent_evidence(
    cooperation: &TurnCooperation,
    limit: usize,
) -> Vec<xai_grok_sampler::litert_lm::EvidenceRecord> {
    let mut records = cooperation.evidence.snapshot();
    if records.len() > limit {
        records.drain(..records.len() - limit);
    }
    records
}

pub(crate) fn parse_json_payload<T: serde::de::DeserializeOwned>(text: &str) -> Result<T, String> {
    let trimmed = text.trim();
    let unfenced = trimmed
        .strip_prefix("```json")
        .or_else(|| trimmed.strip_prefix("```"))
        .and_then(|value| value.strip_suffix("```"))
        .map(str::trim)
        .unwrap_or(trimmed);
    serde_json::from_str(unfenced).or_else(|_| {
        let start = unfenced
            .find('{')
            .ok_or_else(|| "missing JSON object".to_string())?;
        let end = unfenced
            .rfind('}')
            .ok_or_else(|| "missing JSON object terminator".to_string())?;
        serde_json::from_str(&unfenced[start..=end]).map_err(|error| error.to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_directive_is_removed() {
        let mut blocks = vec![acp::ContentBlock::Text(acp::TextContent::new(
            "/cooperate full\ninspect the workspace".to_string(),
        ))];
        assert_eq!(
            take_cooperation_directive(&mut blocks),
            Some(Some(CooperationMode::PlanExecuteReview))
        );
        assert!(matches!(
            &blocks[0],
            acp::ContentBlock::Text(text) if text.text == "inspect the workspace"
        ));
    }

    #[test]
    fn tool_selection_is_bounded() {
        let tools = (0..10)
            .map(|index| {
                ToolDefinition::function(
                    format!("shell_{index}"),
                    None::<String>,
                    serde_json::json!({"type": "object"}),
                )
            })
            .collect();
        let plan = PlanPacket {
            objective: "test".to_string(),
            tasks: vec![ExecutionTask {
                task_id: "1".to_string(),
                objective: "run".to_string(),
                tool_families: vec!["shell".to_string()],
                required_evidence: Vec::new(),
                completion_tests: Vec::new(),
            }],
            required_tool_families: Vec::new(),
            required_evidence: Vec::new(),
            completion_tests: Vec::new(),
        };
        assert_eq!(select_tools_for_task(tools, &plan.tasks[0]).len(), 6);
    }

    #[test]
    fn cooperation_runs_plan_and_correction_tasks_one_at_a_time() {
        let session = uuid::Uuid::new_v4().to_string();
        let prompt = uuid::Uuid::new_v4().to_string();
        let _guard = register(
            &session,
            &prompt,
            TurnCooperation {
                mode: CooperationMode::PlanExecuteReview,
                planner_model: "vibe".to_string(),
                executor_model: "qwen".to_string(),
                reviewer_model: "vibe".to_string(),
                plan: None,
                active_task_index: 0,
                reviewer_memory: None,
                reviewer_memory_prefetch_started: false,
                evidence: xai_grok_sampler::litert_lm::EvidenceBlackboard::default(),
            },
        );
        let task = |id: &str| ExecutionTask {
            task_id: id.to_string(),
            objective: format!("objective-{id}"),
            tool_families: vec![format!("tool-{id}")],
            required_evidence: Vec::new(),
            completion_tests: Vec::new(),
        };
        set_plan(
            &session,
            &prompt,
            PlanPacket {
                objective: "complete work".to_string(),
                tasks: vec![task("one"), task("two")],
                required_tool_families: Vec::new(),
                required_evidence: Vec::new(),
                completion_tests: Vec::new(),
            },
        );
        let current = get(&session, &prompt).unwrap();
        assert_eq!(current.current_task().unwrap().task_id, "one");
        assert!(current.has_more_tasks());
        assert_eq!(advance_task(&session, &prompt).unwrap().0.task_id, "two");
        assert!(advance_task(&session, &prompt).is_none());

        let (first_correction, count) =
            begin_correction(&session, &prompt, vec![task("fix-a"), task("fix-b")]).unwrap();
        assert_eq!(first_correction.task_id, "fix-a");
        assert_eq!(count, 2);
        assert_eq!(advance_task(&session, &prompt).unwrap().0.task_id, "fix-b");
        assert!(advance_task(&session, &prompt).is_none());
    }

    #[test]
    fn direct_and_full_overrides_are_deterministic() {
        let route = RouteDecision {
            route: Some(Route::Analysis),
            reason: "test",
            requires_planning: true,
        };
        assert_eq!(
            mode_for(route.clone(), Some(Some(CooperationMode::Direct))),
            CooperationMode::Direct
        );
        assert_eq!(
            mode_for(
                route.clone(),
                Some(Some(CooperationMode::PlanExecuteReview))
            ),
            CooperationMode::PlanExecuteReview
        );
        assert_eq!(
            mode_for(route, Some(None)),
            CooperationMode::PlanExecuteReview
        );
    }

    #[test]
    fn short_single_tool_execution_stays_direct_but_complex_execution_plans() {
        let direct = RouteDecision {
            route: Some(Route::Execution),
            reason: "test",
            requires_planning: false,
        };
        let complex = RouteDecision {
            route: Some(Route::Execution),
            reason: "test",
            requires_planning: true,
        };
        assert_eq!(mode_for(direct, None), CooperationMode::Direct);
        assert_eq!(mode_for(complex, None), CooperationMode::PlanExecuteReview);
    }

    #[test]
    fn review_decisions_parse_without_reasoning_transfer() {
        let accepted: ReviewDecision =
            parse_json_payload(r#"{"decision":"accept","final_response":"done","unresolved":[]}"#)
                .unwrap();
        assert!(matches!(
            accepted,
            ReviewDecision::Accept { final_response, .. } if final_response == "done"
        ));
        let corrected: ReviewDecision = parse_json_payload(
            r#"{"decision":"correct","tasks":[{"task_id":"1","objective":"retry","tool_families":[],"required_evidence":[],"completion_tests":[]}]}"#,
        )
        .unwrap();
        assert!(matches!(
            corrected,
            ReviewDecision::Correct { tasks } if tasks.len() == 1
        ));
    }
}
