//! Session context — legacy name "ToolContext".
//!
//! This struct holds session-level state (cwd, gateway, filesystem, hunk tracker, etc.)
//! that the session actor needs for non-tool operations (ACP communication, git, rewind, etc.).
//!
//! Tool execution goes through the ToolBridge, which has its own SessionContext from
//! xai-grok-tools. This struct is NOT used for tool execution — it's session infrastructure.
//!
//! Note: Could be renamed to `SessionConfig` or flattened onto `SessionActor` in a future PR.
use crate::terminal::AsyncTerminalRunner;
use agent_client_protocol as acp;
use std::collections::HashMap;
use std::sync::Arc;
use xai_acp_lib::AcpAgentGatewaySender as GatewaySender;
use xai_grok_paths::AbsPathBuf;
use xai_grok_workspace::file_system::{AsyncFileSystem, AsyncFsWrapper};
use xai_grok_workspace::session::file_state::FileStateHandle;
use xai_hunk_tracker::HunkTrackerHandle;
use xai_tty_utils::ProcessScope;

#[derive(Clone, Debug)]
pub struct ToolOutputTurnBudget {
    config: crate::tools::config::ToolOutputBudgetConfig,
    /// Packed `(generation, reserved_or_spent_tokens)`.
    ///
    /// Reserving the allowance before an async result is filtered prevents
    /// parallel tool completions from each observing the same remaining
    /// budget. The generation makes resetting a turn safe even if a stale
    /// claim is dropped later.
    state: Arc<std::sync::atomic::AtomicU64>,
}

#[derive(Debug)]
pub struct ToolOutputBudgetClaim {
    state: Option<Arc<std::sync::atomic::AtomicU64>>,
    generation: u32,
    limit: u32,
    reserved: u32,
    settled: bool,
}

impl Default for ToolOutputTurnBudget {
    fn default() -> Self {
        Self::new(crate::tools::config::ToolOutputBudgetConfig::default())
    }
}

impl ToolOutputTurnBudget {
    pub fn new(config: crate::tools::config::ToolOutputBudgetConfig) -> Self {
        Self {
            config,
            state: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    pub fn reset_turn(&self) {
        use std::sync::atomic::Ordering;
        let mut observed = self.state.load(Ordering::Acquire);
        loop {
            let generation = generation_from_state(observed).wrapping_add(1);
            let reset = u64::from(generation) << 32;
            match self.state.compare_exchange_weak(
                observed,
                reset,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(actual) => observed = actual,
            }
        }
    }

    pub fn claim_next_result(&self) -> Option<ToolOutputBudgetClaim> {
        self.claim_next_result_with_cap(u32::MAX)
    }

    /// Claim a configured allowance capped by the live model-window runway.
    /// `Off` remains an explicit bypass. `Soft` and `Hard` never reserve more
    /// than can fit after completion and safety reserves.
    pub fn claim_next_result_with_cap(
        &self,
        available_tokens: u32,
    ) -> Option<ToolOutputBudgetClaim> {
        use crate::tools::config::ToolOutputBudgetMode;
        match self.config.mode {
            ToolOutputBudgetMode::Off => None,
            ToolOutputBudgetMode::Soft => Some(ToolOutputBudgetClaim::unreserved(
                self.config
                    .per_result_tokens
                    .unwrap_or(available_tokens)
                    .min(available_tokens),
            )),
            ToolOutputBudgetMode::Hard => {
                let Some(turn_limit) = self.config.per_turn_tokens else {
                    return Some(ToolOutputBudgetClaim::unreserved(
                        self.config
                            .per_result_tokens
                            .unwrap_or(available_tokens)
                            .min(available_tokens),
                    ));
                };
                use std::sync::atomic::Ordering;
                let mut observed = self.state.load(Ordering::Acquire);
                loop {
                    let generation = generation_from_state(observed);
                    let spent = spent_from_state(observed);
                    let remaining = turn_limit.saturating_sub(spent);
                    let reservation = self
                        .config
                        .per_result_tokens
                        .unwrap_or(remaining)
                        .min(remaining)
                        .min(available_tokens);
                    let updated = (u64::from(generation) << 32)
                        | u64::from(spent.saturating_add(reservation));
                    match self.state.compare_exchange_weak(
                        observed,
                        updated,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            return Some(ToolOutputBudgetClaim {
                                state: Some(Arc::clone(&self.state)),
                                generation,
                                limit: reservation,
                                reserved: reservation,
                                settled: false,
                            });
                        }
                        Err(actual) => observed = actual,
                    }
                }
            }
        }
    }

    #[cfg(test)]
    fn spent(&self) -> u64 {
        u64::from(spent_from_state(
            self.state.load(std::sync::atomic::Ordering::Acquire),
        ))
    }
}

impl ToolOutputBudgetClaim {
    fn unreserved(limit: u32) -> Self {
        Self {
            state: None,
            generation: 0,
            limit,
            reserved: 0,
            settled: false,
        }
    }

    pub fn limit(&self) -> u32 {
        self.limit
    }

    /// Commit the tokens actually retained and release any unused reservation.
    pub fn finish(mut self, retained_tokens: u32) {
        debug_assert!(
            retained_tokens <= self.limit,
            "retained tool output exceeded its token claim"
        );
        self.settle(retained_tokens.min(self.limit));
    }

    fn settle(&mut self, retained_tokens: u32) {
        if self.settled {
            return;
        }
        self.settled = true;
        let refund = self.reserved.saturating_sub(retained_tokens);
        let Some(state) = &self.state else {
            return;
        };
        if refund == 0 {
            return;
        }

        use std::sync::atomic::Ordering;
        let mut observed = state.load(Ordering::Acquire);
        loop {
            if generation_from_state(observed) != self.generation {
                return;
            }
            let spent = spent_from_state(observed);
            let updated =
                (u64::from(self.generation) << 32) | u64::from(spent.saturating_sub(refund));
            match state.compare_exchange_weak(
                observed,
                updated,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(actual) => observed = actual,
            }
        }
    }
}

impl Drop for ToolOutputBudgetClaim {
    fn drop(&mut self) {
        // A cancelled filter must not leak its reservation into the rest of
        // the turn. A claim from an older generation cannot alter a new turn.
        self.settle(0);
    }
}

fn generation_from_state(state: u64) -> u32 {
    (state >> 32) as u32
}

fn spent_from_state(state: u64) -> u32 {
    state as u32
}

#[derive(Debug, Clone, Default)]
pub struct TaskOutputTokenBudget {
    inner: Arc<parking_lot::Mutex<TaskOutputTokenBudgetState>>,
}
#[derive(Debug, Default)]
struct TaskOutputTokenBudgetState {
    total: Option<u64>,
    spent: u64,
    incomplete: bool,
}
impl TaskOutputTokenBudget {
    pub fn limited(total: u64) -> Self {
        debug_assert!(total > 0, "task output grant must be positive");
        Self {
            inner: Arc::new(parking_lot::Mutex::new(TaskOutputTokenBudgetState {
                total: Some(total),
                spent: 0,
                incomplete: false,
            })),
        }
    }
    pub fn remaining(&self) -> Option<u64> {
        let state = self.inner.lock();
        state.total.map(|total| total.saturating_sub(state.spent))
    }
    pub fn clamp_request(&self, configured: Option<u32>) -> Option<u32> {
        let remaining = self.remaining()?;
        if remaining == 0 {
            return Some(0);
        }
        let remaining = u32::try_from(remaining).unwrap_or(u32::MAX);
        Some(configured.map_or(remaining, |configured| configured.min(remaining)))
    }
    pub fn record_reported_output(&self, output_tokens: u64) {
        let mut state = self.inner.lock();
        state.spent = state.spent.saturating_add(output_tokens);
        if let Some(total) = state.total
            && state.spent > total
        {
            state.spent = total;
            state.incomplete = true;
        }
    }
    pub fn mark_incomplete_and_exhaust(&self) {
        let mut state = self.inner.lock();
        state.incomplete = true;
        if let Some(total) = state.total {
            state.spent = state.spent.max(total);
        }
    }
    pub fn usage(&self) -> (u64, bool) {
        let state = self.inner.lock();
        (state.spent, state.incomplete)
    }
    pub fn is_limited(&self) -> bool {
        self.inner.lock().total.is_some()
    }
}
pub struct BlockingWaitState(std::sync::Mutex<BlockingWaitInner>);
#[derive(Default)]
struct BlockingWaitInner {
    depth: usize,
    generation: u64,
}
impl BlockingWaitState {
    pub(crate) fn new() -> Self {
        Self(std::sync::Mutex::new(BlockingWaitInner::default()))
    }
    pub(crate) fn depth(&self) -> usize {
        self.0
            .lock()
            .expect("blocking wait state mutex poisoned")
            .depth
    }
    #[cfg(test)]
    pub(crate) fn set_depth_for_test(&self, depth: usize) {
        self.0
            .lock()
            .expect("blocking wait state mutex poisoned")
            .depth = depth;
    }
    pub(crate) fn reset(&self) {
        let mut state = self.0.lock().expect("blocking wait state mutex poisoned");
        state.generation = state.generation.wrapping_add(1);
        state.depth = 0;
    }
}
pub(crate) struct BlockingWaitGuard {
    state: Arc<BlockingWaitState>,
    generation: u64,
}
impl BlockingWaitGuard {
    pub(crate) fn enter(state: Arc<BlockingWaitState>) -> Self {
        let generation = {
            let mut inner = state.0.lock().expect("blocking wait state mutex poisoned");
            inner.depth = inner.depth.saturating_add(1);
            inner.generation
        };
        Self { state, generation }
    }
}
impl Drop for BlockingWaitGuard {
    fn drop(&mut self) {
        let mut inner = self
            .state
            .0
            .lock()
            .expect("blocking wait state mutex poisoned");
        if inner.generation == self.generation {
            inner.depth = inner.depth.saturating_sub(1);
        }
    }
}
pub(crate) fn subagent_foreground_wait(
    state: Arc<BlockingWaitState>,
) -> xai_grok_tools::implementations::grok_build::task::types::SubagentForegroundWait {
    xai_grok_tools::implementations::grok_build::task::types::SubagentForegroundWait::new(
        move || Box::new(BlockingWaitGuard::enter(Arc::clone(&state))),
    )
}
/// Session-level context. NOT used for tool execution (bridge handles that).
/// Holds ACP gateway, cwd, hunk tracker, etc. for session infrastructure.
#[derive(Clone)]
pub struct ToolContext {
    pub gateway: Option<GatewaySender>,
    pub session_id: Option<acp::SessionId>,
    pub fs: AsyncFsWrapper,
    pub terminal: Arc<dyn AsyncTerminalRunner>,
    pub cwd: AbsPathBuf,
    pub file_state_handle: Option<FileStateHandle>,
    pub session_env: Arc<HashMap<String, String>>,
    pub hunk_tracker_handle: HunkTrackerHandle,
    /// Whether hunk tracking is active for this session. `false` when the
    /// resolved mode is `off`/`disabled` — `hunk_tracker_handle` is then a
    /// `noop()` and the fs-notify loop skips forwarding to avoid per-event cost.
    pub hunk_tracking_enabled: bool,
    pub prompt_index: Arc<tokio::sync::Mutex<usize>>,
    /// Current subagent nesting depth for this session.
    /// Top-level sessions start at 0; child sessions are parent_depth + 1.
    pub subagent_depth: u32,
    /// Unified subagent event sender — carries spawn, query, cancel,
    /// list-active, completions, and outstanding messages to the coordinator.
    /// `None` if subagent support is not enabled.
    pub subagent_event_tx: Option<
        tokio::sync::mpsc::UnboundedSender<
            xai_grok_tools::implementations::grok_build::task::types::SubagentEvent,
        >,
    >,
    /// Shared LSP runtime — cloned cheaply (Arc) from parent to child.
    /// Same pattern as `fs` and `terminal`.
    pub lsp: Option<Arc<dyn xai_grok_tools::implementations::lsp::LspBackend>>,
    /// LSP server names snapshot from session creation (not updated mid-session).
    pub lsp_server_names: Vec<String>,
    /// Shared turn-active flag — set `true` at turn start, `false` at turn end.
    /// Used by the between-turn completion drain in `handle_prompt`.
    pub is_turn_active: Option<Arc<std::sync::atomic::AtomicBool>>,
    pub(crate) unattributed_background_usage: Arc<std::sync::atomic::AtomicBool>,
    /// Shared buffer for mid-turn monitor event notifications.
    /// Events pushed here are drained by the session turn loop
    /// (`inject_pending_monitor_events`) and surfaced as ONE hidden
    /// synthetic user message before the next sampling step.
    pub monitor_event_buffer:
        Option<xai_grok_tools::implementations::grok_build::monitor::types::MonitorEventBuffer>,
    pub task_completion_reservations:
        Option<xai_grok_tools::reminders::task_completion::TaskCompletionReservations>,
    pub task_wake_suppressed:
        Option<xai_grok_tools::reminders::task_completion::TaskWakeSuppressed>,
    /// Channel for requesting trace uploads for synthetic auto-wake turns.
    pub(crate) synthetic_trace_tx:
        Option<tokio::sync::mpsc::UnboundedSender<crate::upload::turn::SyntheticTurnTraceRequest>>,
    /// Shared slot for the synthetic trace channel. Populated by
    /// `start_subagent_coordinator` after the notification bridge is spawned.
    /// The notification bridge reads from this slot on each completion event.
    pub(crate) synthetic_trace_tx_shared: Option<
        std::sync::Arc<
            std::sync::Mutex<
                Option<
                    tokio::sync::mpsc::UnboundedSender<
                        crate::upload::turn::SyntheticTurnTraceRequest,
                    >,
                >,
            >,
        >,
    >,
    /// Resolved name of the `BackgroundTaskAction` tool in the current toolset.
    /// Used by auto-wake to format completion messages with the correct tool name.
    pub task_output_tool_name: String,
    /// Whether auto-wake is enabled. When `false`, background task and subagent
    /// completions fall back to the idle-gated notification drain.
    pub auto_wake_enabled: bool,
    /// When set, bash + subagent auto-wake synthetic prompts are suppressed.
    /// Shared `Arc` written at one chokepoint — see
    /// `SessionActor::set_goal_loop_active_resource` for the rationale.
    pub goal_loop_active_gate: Arc<std::sync::atomic::AtomicBool>,
    /// Count of interruptible blocking waits the running turn is parked in (via
    /// [`BlockingWaitGuard`]). `queue_input` reads it: a prompt arriving while
    /// non-zero takes the send-now path.
    pub blocking_wait_depth: Arc<BlockingWaitState>,
    pub task_output_token_budget: Option<TaskOutputTokenBudget>,
    pub tool_output_turn_budget: ToolOutputTurnBudget,
    pub(crate) sampler_retry_only_before_output: bool,
    /// This session's child-process reaper, set at session spawn; `None` for
    /// contexts without one (subagents, defaults). Spawn sites enroll children
    /// into it; enrolled children are killed when the session closes.
    pub process_scope: Option<ProcessScope>,
}
impl ToolContext {
    pub(crate) fn clamp_task_model_request(
        &self,
        configured: Option<u32>,
    ) -> Result<Option<u32>, &'static str> {
        match self.task_output_token_budget.as_ref() {
            Some(budget) => match budget.clamp_request(configured) {
                Some(0) => Err("workflow child output-token budget exhausted"),
                clamped => Ok(clamped),
            },
            None => Ok(configured),
        }
    }
    pub(crate) fn record_task_model_output(&self, output_tokens: u64) {
        if let Some(budget) = self.task_output_token_budget.as_ref() {
            budget.record_reported_output(output_tokens);
        }
    }
    pub(crate) fn fail_task_output_usage_closed(&self) {
        if let Some(budget) = self.task_output_token_budget.as_ref() {
            budget.mark_incomplete_and_exhaust();
        }
    }
    pub fn new(
        cwd: AbsPathBuf,
        gateway: Option<GatewaySender>,
        session_id: Option<acp::SessionId>,
        fs: Arc<dyn AsyncFileSystem>,
        terminal: Arc<dyn AsyncTerminalRunner>,
        hunk_tracker_handle: HunkTrackerHandle,
    ) -> Self {
        let session_env = xai_grok_workspace::envrc::load_envrc_or_empty_when_trusted(
            cwd.as_path(),
            crate::agent::folder_trust::project_scope_allowed(cwd.as_path()),
        );
        Self {
            gateway,
            session_id,
            fs: AsyncFsWrapper::new(fs),
            terminal,
            cwd,
            file_state_handle: None,
            session_env: Arc::new(session_env),
            hunk_tracker_handle,
            hunk_tracking_enabled: true,
            prompt_index: Arc::new(tokio::sync::Mutex::new(0)),
            subagent_depth: 0,
            subagent_event_tx: None,
            lsp: None,
            lsp_server_names: Vec::new(),
            is_turn_active: None,
            unattributed_background_usage: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            monitor_event_buffer: None,
            task_completion_reservations: None,
            task_wake_suppressed: None,
            synthetic_trace_tx: None,
            synthetic_trace_tx_shared: None,
            task_output_tool_name:
                xai_grok_tools::reminders::task_completion::DEFAULT_TASK_OUTPUT_TOOL.to_string(),
            auto_wake_enabled: true,
            goal_loop_active_gate: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            blocking_wait_depth: Arc::new(BlockingWaitState::new()),
            task_output_token_budget: None,
            tool_output_turn_budget: ToolOutputTurnBudget::default(),
            sampler_retry_only_before_output: false,
            process_scope: None,
        }
    }
    pub fn with_preloaded_env(
        cwd: AbsPathBuf,
        gateway: Option<GatewaySender>,
        session_id: Option<acp::SessionId>,
        fs: Arc<dyn AsyncFileSystem>,
        terminal: Arc<dyn AsyncTerminalRunner>,
        hunk_tracker_handle: HunkTrackerHandle,
        session_env: HashMap<String, String>,
    ) -> Self {
        Self {
            gateway,
            session_id,
            fs: AsyncFsWrapper::new(fs),
            terminal,
            cwd,
            file_state_handle: None,
            session_env: Arc::new(session_env),
            hunk_tracker_handle,
            hunk_tracking_enabled: true,
            prompt_index: Arc::new(tokio::sync::Mutex::new(0)),
            subagent_depth: 0,
            subagent_event_tx: None,
            lsp: None,
            lsp_server_names: Vec::new(),
            is_turn_active: None,
            unattributed_background_usage: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            monitor_event_buffer: None,
            task_completion_reservations: None,
            task_wake_suppressed: None,
            synthetic_trace_tx: None,
            synthetic_trace_tx_shared: None,
            task_output_tool_name:
                xai_grok_tools::reminders::task_completion::DEFAULT_TASK_OUTPUT_TOOL.to_string(),
            auto_wake_enabled: true,
            goal_loop_active_gate: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            blocking_wait_depth: Arc::new(BlockingWaitState::new()),
            task_output_token_budget: None,
            tool_output_turn_budget: ToolOutputTurnBudget::default(),
            sampler_retry_only_before_output: false,
            process_scope: None,
        }
    }
    pub fn with_file_state_handle(mut self, handle: FileStateHandle) -> Self {
        self.file_state_handle = Some(handle);
        self
    }
    pub fn with_prompt_index(mut self, prompt_index: Arc<tokio::sync::Mutex<usize>>) -> Self {
        self.prompt_index = prompt_index;
        self
    }
    /// Set whether hunk tracking is active. `false` pairs with a `noop()`
    /// `hunk_tracker_handle` so the fs-notify loop skips the per-event forward.
    pub fn with_hunk_tracking_enabled(mut self, enabled: bool) -> Self {
        self.hunk_tracking_enabled = enabled;
        self
    }
    pub fn with_tool_output_budget(
        mut self,
        config: crate::tools::config::ToolOutputBudgetConfig,
    ) -> Self {
        self.tool_output_turn_budget = ToolOutputTurnBudget::new(config);
        self
    }
}
#[cfg(test)]
mod output_budget_tests {
    use super::{TaskOutputTokenBudget, ToolOutputBudgetClaim, ToolOutputTurnBudget};
    use crate::tools::config::{ToolOutputBudgetConfig, ToolOutputBudgetMode};
    #[test]
    fn clamps_every_request_to_remaining_and_stops_at_zero() {
        let budget = TaskOutputTokenBudget::limited(10);
        assert_eq!(budget.clamp_request(None), Some(10));
        assert_eq!(budget.clamp_request(Some(7)), Some(7));
        budget.record_reported_output(6);
        assert_eq!(budget.clamp_request(None), Some(4));
        assert_eq!(budget.clamp_request(Some(9)), Some(4));
        budget.record_reported_output(4);
        assert_eq!(budget.clamp_request(None), Some(0));
    }
    #[test]
    fn provider_output_not_context_drives_spend() {
        let budget = TaskOutputTokenBudget::limited(100);
        let provider_prompt_tokens = 90_000u64;
        budget.record_reported_output(25);
        assert_eq!(budget.usage(), (25, false));
        assert_eq!(provider_prompt_tokens, 90_000);
        assert_eq!(budget.remaining(), Some(75));
    }
    #[test]
    fn unknown_usage_exhausts_grant_pessimistically() {
        let budget = TaskOutputTokenBudget::limited(50);
        budget.record_reported_output(7);
        budget.mark_incomplete_and_exhaust();
        assert_eq!(budget.usage(), (50, true));
        assert_eq!(budget.clamp_request(None), Some(0));
    }
    #[test]
    fn hard_tool_budget_is_cumulative_and_resettable() {
        let budget = ToolOutputTurnBudget::new(ToolOutputBudgetConfig {
            mode: ToolOutputBudgetMode::Hard,
            per_result_tokens: Some(2_000),
            per_turn_tokens: Some(5_000),
        });
        let first = budget.claim_next_result().unwrap();
        assert_eq!(first.limit(), 2_000);
        first.finish(2_000);
        budget.claim_next_result().unwrap().finish(2_000);
        let final_claim = budget.claim_next_result().unwrap();
        assert_eq!(final_claim.limit(), 1_000);
        final_claim.finish(1_000);
        assert_eq!(budget.claim_next_result().unwrap().limit(), 0);
        assert_eq!(budget.spent(), 5_000);
        budget.reset_turn();
        assert_eq!(budget.claim_next_result().unwrap().limit(), 2_000);
    }

    #[test]
    fn live_context_runway_caps_soft_and_hard_claims_but_not_off() {
        let soft = ToolOutputTurnBudget::new(ToolOutputBudgetConfig {
            mode: ToolOutputBudgetMode::Soft,
            per_result_tokens: Some(2_000),
            per_turn_tokens: None,
        });
        assert_eq!(soft.claim_next_result_with_cap(375).unwrap().limit(), 375);

        let hard = ToolOutputTurnBudget::new(ToolOutputBudgetConfig {
            mode: ToolOutputBudgetMode::Hard,
            per_result_tokens: Some(2_000),
            per_turn_tokens: Some(5_000),
        });
        let first = hard.claim_next_result_with_cap(400).unwrap();
        assert_eq!(first.limit(), 400);
        first.finish(400);
        assert_eq!(hard.claim_next_result_with_cap(250).unwrap().limit(), 250);

        let off = ToolOutputTurnBudget::new(ToolOutputBudgetConfig {
            mode: ToolOutputBudgetMode::Off,
            per_result_tokens: Some(1),
            per_turn_tokens: Some(1),
        });
        assert!(off.claim_next_result_with_cap(0).is_none());
    }

    #[test]
    fn parallel_tool_budget_claims_cannot_oversubscribe_turn() {
        let budget = ToolOutputTurnBudget::new(ToolOutputBudgetConfig {
            mode: ToolOutputBudgetMode::Hard,
            per_result_tokens: Some(2_000),
            per_turn_tokens: Some(5_000),
        });
        let claims = std::thread::scope(|scope| {
            let handles = (0..16)
                .map(|_| {
                    let budget = budget.clone();
                    scope.spawn(move || budget.claim_next_result().unwrap())
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });
        let reserved = claims.iter().map(ToolOutputBudgetClaim::limit).sum::<u32>();
        assert_eq!(reserved, 5_000);
        for claim in claims {
            let limit = claim.limit();
            claim.finish(limit);
        }
        assert_eq!(budget.spent(), 5_000);
    }

    #[test]
    fn stale_claim_drop_cannot_refund_new_turn() {
        let budget = ToolOutputTurnBudget::new(ToolOutputBudgetConfig {
            mode: ToolOutputBudgetMode::Hard,
            per_result_tokens: Some(2_000),
            per_turn_tokens: Some(5_000),
        });
        let stale = budget.claim_next_result().unwrap();
        budget.reset_turn();
        budget.claim_next_result().unwrap().finish(2_000);
        drop(stale);
        assert_eq!(budget.spent(), 2_000);
    }
}
#[cfg(test)]
mod tests {
    use super::BlockingWaitState;
    use crate::{terminal::AsyncTerminalRunner, tools::ToolContext};
    use std::collections::HashMap;
    use std::sync::Arc;
    use xai_grok_paths::AbsPathBuf;
    use xai_grok_workspace::file_system::{AsyncFileSystem, AsyncFsWrapper};
    use xai_hunk_tracker::HunkTrackerHandle;
    impl ToolContext {
        pub fn new_local_context(
            cwd: AbsPathBuf,
            fs: Arc<dyn AsyncFileSystem>,
            terminal: Arc<dyn AsyncTerminalRunner>,
        ) -> Self {
            Self {
                gateway: None,
                session_id: None,
                fs: AsyncFsWrapper::new(fs),
                terminal,
                cwd,
                file_state_handle: None,
                session_env: Arc::new(HashMap::new()),
                hunk_tracker_handle: HunkTrackerHandle::noop(),
                hunk_tracking_enabled: true,
                prompt_index: Arc::new(tokio::sync::Mutex::new(0)),
                subagent_depth: 0,
                subagent_event_tx: None,
                lsp: None,
                lsp_server_names: Vec::new(),
                is_turn_active: None,
                unattributed_background_usage: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                monitor_event_buffer: None,
                task_completion_reservations: None,
                task_wake_suppressed: None,
                synthetic_trace_tx: None,
                synthetic_trace_tx_shared: None,
                task_output_tool_name:
                    xai_grok_tools::reminders::task_completion::DEFAULT_TASK_OUTPUT_TOOL.to_string(),
                auto_wake_enabled: true,
                goal_loop_active_gate: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                blocking_wait_depth: Arc::new(BlockingWaitState::new()),
                task_output_token_budget: None,
                tool_output_turn_budget: super::ToolOutputTurnBudget::default(),
                sampler_retry_only_before_output: false,
                process_scope: None,
            }
        }
    }
}
