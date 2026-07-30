//! Deterministic, zero-inference-cost turn classification.
//!
//! This module does not own model execution or cooperation. `TurnCoordinator`
//! consumes its task features and owns the Direct/Plan/Execute/Review flow.

use agent_client_protocol as acp;

use super::config::ModelRouterConfig;

const BUILTIN_ANALYSIS_KEYWORDS: &[&str] = &[
    "analy",
    "architect",
    "compare",
    "explain why",
    "plan",
    "reason",
    "tradeoff",
    "threat model",
];

/// Terms that strongly imply the turn must inspect or mutate real state. These
/// route ahead of analysis because an analysis-only model can otherwise claim
/// success without ever emitting a tool call.
const BUILTIN_EXECUTION_KEYWORDS: &[&str] = &[
    "add ",
    "audit",
    "benchmark",
    "build",
    "change ",
    "command",
    "compile",
    "code",
    "debug",
    "diagnos",
    "edit ",
    "error",
    "execute",
    "file",
    "fix",
    "implement",
    "inspect",
    "investigate",
    "nmap",
    "optimiz",
    "port scan",
    "refactor",
    "repository",
    "review",
    "run ",
    "scan",
    "shell",
    "terminal",
    "test",
    "trace",
    "vulnerab",
    "workspace",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Route {
    Chat,
    Analysis,
    Execution,
}

impl Route {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Chat => "chat",
            Self::Analysis => "analysis",
            Self::Execution => "execution",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RouteDecision {
    pub(crate) route: Option<Route>,
    pub(crate) reason: &'static str,
    pub(crate) requires_planning: bool,
}

/// Remove an optional first-line routing directive.
///
/// `/route chat`, `/route analysis`, and `/route execution` force a route for
/// one turn.
/// `/route current` bypasses routing for one turn. The directive never enters
/// model history. Unknown `/route` values are left untouched for normal slash
/// command error handling.
pub(crate) fn take_directive(blocks: &mut [acp::ContentBlock]) -> Option<RouteDecision> {
    let text = blocks.iter_mut().find_map(|block| match block {
        acp::ContentBlock::Text(text) => Some(&mut text.text),
        _ => None,
    })?;
    let (first, rest) = text.split_once('\n').unwrap_or((text.as_str(), ""));
    let route = match first.trim().to_ascii_lowercase().as_str() {
        "/route chat" => Some(Route::Chat),
        "/route analysis" => Some(Route::Analysis),
        "/route execution" => Some(Route::Execution),
        "/route current" => None,
        _ => return None,
    };
    *text = rest.trim_start_matches(['\r', '\n']).to_owned();
    Some(RouteDecision {
        route,
        reason: "explicit_directive",
        requires_planning: matches!(route, Some(Route::Analysis)),
    })
}

pub(crate) fn prompt_text(blocks: &[acp::ContentBlock]) -> String {
    blocks
        .iter()
        .filter_map(|block| match block {
            acp::ContentBlock::Text(text) => Some(text.text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub(crate) fn decide(
    config: &ModelRouterConfig,
    text: &str,
    has_non_text_content: bool,
    has_output_schema: bool,
) -> RouteDecision {
    let trimmed = text.trim();
    if trimmed.is_empty() || trimmed.starts_with('!') {
        return RouteDecision {
            route: None,
            reason: "non_inference_input",
            requires_planning: false,
        };
    }
    let lowercase = trimmed.to_lowercase();
    if BUILTIN_EXECUTION_KEYWORDS
        .iter()
        .any(|keyword| lowercase.contains(keyword))
        || config
            .execution_keywords
            .iter()
            .any(|keyword| !keyword.is_empty() && lowercase.contains(&keyword.to_lowercase()))
    {
        return RouteDecision {
            route: Some(Route::Execution),
            reason: "execution_keyword",
            requires_planning: execution_requires_planning(
                trimmed,
                config.analysis_min_chars,
                &lowercase,
            ),
        };
    }
    if has_non_text_content {
        return RouteDecision {
            route: Some(Route::Analysis),
            reason: "multimodal_input",
            requires_planning: true,
        };
    }
    if has_output_schema {
        return RouteDecision {
            route: Some(Route::Analysis),
            reason: "structured_output",
            requires_planning: true,
        };
    }
    if trimmed.chars().count() >= config.analysis_min_chars {
        return RouteDecision {
            route: Some(Route::Analysis),
            reason: "length_threshold",
            requires_planning: true,
        };
    }
    if trimmed.contains("```") {
        return RouteDecision {
            route: Some(Route::Analysis),
            reason: "code_block",
            requires_planning: true,
        };
    }
    if BUILTIN_ANALYSIS_KEYWORDS
        .iter()
        .any(|keyword| lowercase.contains(keyword))
        || config
            .analysis_keywords
            .iter()
            .any(|keyword| !keyword.is_empty() && lowercase.contains(&keyword.to_lowercase()))
    {
        return RouteDecision {
            route: Some(Route::Analysis),
            reason: "analysis_keyword",
            requires_planning: true,
        };
    }
    RouteDecision {
        route: Some(Route::Chat),
        reason: "lightweight_default",
        requires_planning: false,
    }
}

fn execution_requires_planning(text: &str, analysis_min_chars: usize, lowercase: &str) -> bool {
    let structural_markers = [
        "\n- ",
        "\n* ",
        "\n1.",
        " and then ",
        " then ",
        " after that ",
        "multiple ",
        "several ",
        "all of ",
    ];
    text.chars().count() >= analysis_min_chars
        || text.contains("```")
        || structural_markers
            .iter()
            .any(|marker| lowercase.contains(marker))
}

pub(crate) fn model_for<'a>(config: &'a ModelRouterConfig, route: Route) -> Option<&'a str> {
    match route {
        Route::Chat => config.chat_model.as_deref(),
        Route::Analysis => config.analysis_model.as_deref(),
        // Backward compatible with two-model router profiles: the lightweight
        // chat model is the safest default executor because it already has a
        // tool-aware template in the supported local profile.
        Route::Execution => config
            .execution_model
            .as_deref()
            .or(config.chat_model.as_deref()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> ModelRouterConfig {
        ModelRouterConfig {
            enabled: true,
            chat_model: Some("chat".into()),
            analysis_model: Some("analysis".into()),
            execution_model: Some("execution".into()),
            planner_model: None,
            reviewer_model: None,
            analysis_min_chars: 40,
            analysis_keywords: vec!["threat model".into()],
            execution_keywords: vec!["packet capture".into()],
        }
    }

    #[test]
    fn routes_short_conversation_to_chat() {
        let decision = decide(&config(), "Hello, how are you?", false, false);
        assert_eq!(decision.route, Some(Route::Chat));
        assert_eq!(decision.reason, "lightweight_default");
    }

    #[test]
    fn separates_execution_and_analysis_intents() {
        let simple = decide(&config(), "Run nmap on localhost", false, false);
        assert_eq!(simple.route, Some(Route::Execution));
        assert!(!simple.requires_planning);
        let complex = decide(
            &config(),
            "Inspect the repository and then implement all required fixes",
            false,
            false,
        );
        assert_eq!(complex.route, Some(Route::Execution));
        assert!(complex.requires_planning);
        assert_eq!(
            decide(&config(), "Create a threat model", false, false).route,
            Some(Route::Analysis)
        );
    }

    #[test]
    fn execution_model_falls_back_to_chat_for_two_model_profiles() {
        let mut two_model = config();
        two_model.execution_model = None;
        assert_eq!(
            model_for(&two_model, Route::Execution),
            Some("chat"),
            "existing two-model profiles must remain executable"
        );
    }

    #[test]
    fn execution_route_precedes_length_and_analysis_keywords() {
        let prompt = format!(
            "Analyze the architecture, then run a command to inspect it. {}",
            "x".repeat(80)
        );
        let decision = decide(&config(), &prompt, false, false);
        assert_eq!(decision.route, Some(Route::Execution));
        assert_eq!(decision.reason, "execution_keyword");

        assert_eq!(
            decide(&config(), "Start a packet capture", false, false).route,
            Some(Route::Execution)
        );
    }

    #[test]
    fn routes_long_multimodal_and_structured_prompts_to_analysis() {
        assert_eq!(
            decide(&config(), &"x".repeat(40), false, false).route,
            Some(Route::Analysis)
        );
        assert_eq!(
            decide(&config(), "what is shown?", true, false).route,
            Some(Route::Analysis)
        );
        assert_eq!(
            decide(&config(), "say hello", false, true).route,
            Some(Route::Analysis)
        );
    }

    #[test]
    fn direct_shell_input_bypasses_router() {
        assert_eq!(decide(&config(), "!git status", false, false).route, None);
    }

    #[test]
    fn directive_is_removed_before_history() {
        let mut blocks = vec![acp::ContentBlock::Text(acp::TextContent::new(
            "/route analysis\nTell me a joke".to_string(),
        ))];
        let decision = take_directive(&mut blocks).expect("directive");
        assert_eq!(decision.route, Some(Route::Analysis));
        assert_eq!(prompt_text(&blocks), "Tell me a joke");
    }

    #[test]
    fn execution_directive_is_removed_before_history() {
        let mut blocks = vec![acp::ContentBlock::Text(acp::TextContent::new(
            "/route execution\nRun the probe".to_string(),
        ))];
        let decision = take_directive(&mut blocks).expect("directive");
        assert_eq!(decision.route, Some(Route::Execution));
        assert_eq!(prompt_text(&blocks), "Run the probe");
    }

    #[test]
    fn current_directive_bypasses_router() {
        let mut blocks = vec![acp::ContentBlock::Text(acp::TextContent::new(
            "/route current\nDo not switch".to_string(),
        ))];
        let decision = take_directive(&mut blocks).expect("directive");
        assert_eq!(decision.route, None);
        assert_eq!(prompt_text(&blocks), "Do not switch");
    }
}
