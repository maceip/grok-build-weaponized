use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The semantic role of one indivisible context component.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextComponentKind {
    System,
    CurrentUser,
    CurrentTask,
    RequiredToolSchema,
    ToolResultEnvelope,
    CurrentEvidence,
    RecentTurn,
    RetrievedMemory,
    OlderSummary,
    OptionalToolSchema,
}

impl ContextComponentKind {
    fn optional_priority(self) -> u8 {
        match self {
            Self::CurrentEvidence => 0,
            Self::RecentTurn => 1,
            Self::RetrievedMemory => 2,
            Self::OlderSummary => 3,
            Self::OptionalToolSchema => 4,
            Self::System
            | Self::CurrentUser
            | Self::CurrentTask
            | Self::RequiredToolSchema
            | Self::ToolResultEnvelope => 0,
        }
    }
}

/// One atomic candidate for context admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextComponent {
    pub id: String,
    pub kind: ContextComponentKind,
    pub tokens: u32,
    pub required: bool,
}

/// A component admitted into the final prompt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextAllocation {
    pub id: String,
    pub kind: ContextComponentKind,
    pub tokens: u32,
    pub required: bool,
}

/// An optional component omitted to keep the final request admissible.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DroppedContext {
    pub id: String,
    pub kind: ContextComponentKind,
    pub tokens: u32,
    pub reason: String,
}

/// Stage-specific completion requirements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageBudget {
    pub desired_completion_tokens: u32,
    pub minimum_completion_tokens: u32,
}

impl StageBudget {
    pub const fn planner() -> Self {
        Self {
            desired_completion_tokens: 768,
            minimum_completion_tokens: 768,
        }
    }

    pub const fn executor() -> Self {
        Self {
            desired_completion_tokens: 1_024,
            minimum_completion_tokens: 1_024,
        }
    }

    pub const fn reviewer() -> Self {
        Self {
            desired_completion_tokens: 768,
            minimum_completion_tokens: 768,
        }
    }

    pub const fn direct(maximum: u32) -> Self {
        Self {
            desired_completion_tokens: maximum,
            minimum_completion_tokens: maximum,
        }
    }
}

/// Complete, inspectable admission decision for one generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextPlan {
    pub context_window: u32,
    pub prompt_tokens: u32,
    pub completion_reserve: u32,
    pub safety_margin: u32,
    pub components: Vec<ContextAllocation>,
    pub dropped: Vec<DroppedContext>,
}

impl ContextPlan {
    pub fn remaining_tokens(&self) -> u32 {
        self.context_window.saturating_sub(
            self.prompt_tokens
                .saturating_add(self.completion_reserve)
                .saturating_add(self.safety_margin),
        )
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AdmissionError {
    #[error(
        "required context cannot fit: window={context_window}, required={required_tokens}, \
         minimum_completion={minimum_completion_tokens}, safety={safety_margin}; \
         components={components:?}"
    )]
    RequiredContextTooLarge {
        context_window: u32,
        required_tokens: u32,
        minimum_completion_tokens: u32,
        safety_margin: u32,
        components: Vec<(String, u32)>,
    },
    #[error(
        "native prompt measurement exceeds the admitted window: window={context_window}, \
         measured_prompt={measured_prompt_tokens}, completion={completion_reserve}, \
         safety={safety_margin}"
    )]
    NativeMeasurementOverflow {
        context_window: u32,
        measured_prompt_tokens: u32,
        completion_reserve: u32,
        safety_margin: u32,
    },
    #[error("context token count overflow")]
    TokenOverflow,
}

/// Deterministic, atomic context allocator.
#[derive(Debug, Default, Clone)]
pub struct ContextBudgetBroker;

impl ContextBudgetBroker {
    pub fn safety_margin(context_window: u32) -> u32 {
        let one_percent = context_window.saturating_add(99) / 100;
        one_percent.max(32)
    }

    pub fn admit(
        &self,
        context_window: u32,
        stage: StageBudget,
        components: impl IntoIterator<Item = ContextComponent>,
    ) -> Result<ContextPlan, AdmissionError> {
        let safety_margin = Self::safety_margin(context_window);
        let mut required = Vec::new();
        let mut optional = Vec::new();
        for (index, component) in components.into_iter().enumerate() {
            if component.required {
                required.push((index, component));
            } else {
                optional.push((index, component));
            }
        }

        let required_tokens = checked_sum(required.iter().map(|(_, item)| item.tokens))?;
        let minimum_total = required_tokens
            .checked_add(stage.minimum_completion_tokens)
            .and_then(|total| total.checked_add(safety_margin))
            .ok_or(AdmissionError::TokenOverflow)?;
        if minimum_total > context_window {
            return Err(AdmissionError::RequiredContextTooLarge {
                context_window,
                required_tokens,
                minimum_completion_tokens: stage.minimum_completion_tokens,
                safety_margin,
                components: required
                    .iter()
                    .map(|(_, item)| (item.id.clone(), item.tokens))
                    .collect(),
            });
        }

        let max_completion = context_window
            .saturating_sub(required_tokens)
            .saturating_sub(safety_margin);
        let completion_reserve = stage
            .desired_completion_tokens
            .min(max_completion)
            .max(stage.minimum_completion_tokens);
        let prompt_capacity = context_window
            .saturating_sub(completion_reserve)
            .saturating_sub(safety_margin);

        optional.sort_by_key(|(index, component)| (component.kind.optional_priority(), *index));

        let mut prompt_tokens = required_tokens;
        let mut admitted = required
            .into_iter()
            .map(|(index, component)| (index, allocation(component)))
            .collect::<Vec<_>>();
        let mut dropped = Vec::new();
        for (index, component) in optional {
            let fits = prompt_tokens
                .checked_add(component.tokens)
                .is_some_and(|total| total <= prompt_capacity);
            if fits {
                prompt_tokens += component.tokens;
                admitted.push((index, allocation(component)));
            } else {
                dropped.push(DroppedContext {
                    id: component.id,
                    kind: component.kind,
                    tokens: component.tokens,
                    reason: "context_budget".to_string(),
                });
            }
        }
        admitted.sort_by_key(|(index, _)| *index);

        Ok(ContextPlan {
            context_window,
            prompt_tokens,
            completion_reserve,
            safety_margin,
            components: admitted.into_iter().map(|(_, item)| item).collect(),
            dropped,
        })
    }

    /// Replace estimated prompt usage with the runtime's authoritative final
    /// template measurement. This is the last gate before generation.
    pub fn finalize_exact(
        &self,
        mut plan: ContextPlan,
        measured_prompt_tokens: u32,
    ) -> Result<ContextPlan, AdmissionError> {
        let total = measured_prompt_tokens
            .checked_add(plan.completion_reserve)
            .and_then(|value| value.checked_add(plan.safety_margin))
            .ok_or(AdmissionError::TokenOverflow)?;
        if total > plan.context_window {
            return Err(AdmissionError::NativeMeasurementOverflow {
                context_window: plan.context_window,
                measured_prompt_tokens,
                completion_reserve: plan.completion_reserve,
                safety_margin: plan.safety_margin,
            });
        }
        plan.prompt_tokens = measured_prompt_tokens;
        Ok(plan)
    }
}

fn checked_sum(mut values: impl Iterator<Item = u32>) -> Result<u32, AdmissionError> {
    values.try_fold(0_u32, |total, value| {
        total
            .checked_add(value)
            .ok_or(AdmissionError::TokenOverflow)
    })
}

fn allocation(component: ContextComponent) -> ContextAllocation {
    ContextAllocation {
        id: component.id,
        kind: component.kind,
        tokens: component.tokens,
        required: component.required,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn item(id: &str, kind: ContextComponentKind, tokens: u32, required: bool) -> ContextComponent {
        ContextComponent {
            id: id.to_string(),
            kind,
            tokens,
            required,
        }
    }

    #[test]
    fn admits_exact_window_and_rejects_window_plus_one() {
        let broker = ContextBudgetBroker;
        let margin = ContextBudgetBroker::safety_margin(4096);
        let exact = 4096 - margin - 768;
        let plan = broker
            .admit(
                4096,
                StageBudget::planner(),
                [item(
                    "required",
                    ContextComponentKind::CurrentUser,
                    exact,
                    true,
                )],
            )
            .unwrap();
        assert_eq!(plan.remaining_tokens(), 0);

        let error = broker
            .admit(
                4096,
                StageBudget::planner(),
                [item(
                    "required",
                    ContextComponentKind::CurrentUser,
                    exact + 513,
                    true,
                )],
            )
            .unwrap_err();
        assert!(matches!(
            error,
            AdmissionError::RequiredContextTooLarge { .. }
        ));
    }

    #[test]
    fn keeps_evidence_before_memory_and_optional_tools() {
        let broker = ContextBudgetBroker;
        let plan = broker
            .admit(
                1300,
                StageBudget {
                    desired_completion_tokens: 300,
                    minimum_completion_tokens: 300,
                },
                [
                    item("system", ContextComponentKind::System, 300, true),
                    item("tool", ContextComponentKind::OptionalToolSchema, 300, false),
                    item("memory", ContextComponentKind::RetrievedMemory, 300, false),
                    item(
                        "evidence",
                        ContextComponentKind::CurrentEvidence,
                        300,
                        false,
                    ),
                ],
            )
            .unwrap();
        assert!(plan.components.iter().any(|item| item.id == "evidence"));
        assert!(plan.components.iter().any(|item| item.id == "memory"));
        assert!(plan.dropped.iter().any(|item| item.id == "tool"));
    }

    #[test]
    fn exact_native_measurement_is_authoritative() {
        let broker = ContextBudgetBroker;
        let plan = broker
            .admit(
                1024,
                StageBudget {
                    desired_completion_tokens: 256,
                    minimum_completion_tokens: 256,
                },
                [item("system", ContextComponentKind::System, 100, true)],
            )
            .unwrap();
        assert!(broker.finalize_exact(plan.clone(), 700).is_ok());
        assert!(matches!(
            broker.finalize_exact(plan, 800),
            Err(AdmissionError::NativeMeasurementOverflow { .. })
        ));
    }

    #[test]
    fn exact_measurement_checks_window_minus_one_window_and_window_plus_one() {
        let broker = ContextBudgetBroker;
        let plan = broker
            .admit(
                4096,
                StageBudget::planner(),
                [item("system", ContextComponentKind::System, 32, true)],
            )
            .unwrap();
        let maximum_prompt = plan
            .context_window
            .saturating_sub(plan.completion_reserve)
            .saturating_sub(plan.safety_margin);
        let below = broker
            .finalize_exact(plan.clone(), maximum_prompt - 1)
            .unwrap();
        assert_eq!(below.remaining_tokens(), 1);
        let exact = broker.finalize_exact(plan.clone(), maximum_prompt).unwrap();
        assert_eq!(exact.remaining_tokens(), 0);
        assert!(matches!(
            broker.finalize_exact(plan, maximum_prompt + 1),
            Err(AdmissionError::NativeMeasurementOverflow { .. })
        ));
    }

    proptest! {
        #[test]
        fn arbitrary_atomic_components_never_overfill(
            context_window in 128_u32..65_536,
            minimum_completion in 1_u32..2_048,
            desired_extra in 0_u32..2_048,
            raw in prop::collection::vec((0_u8..10, 0_u32..4_096, any::<bool>()), 0..64),
        ) {
            let components = raw
                .into_iter()
                .enumerate()
                .map(|(index, (kind, tokens, required))| ContextComponent {
                    id: format!("component-{index}-\u{1f680}"),
                    kind: match kind {
                        0 => ContextComponentKind::System,
                        1 => ContextComponentKind::CurrentUser,
                        2 => ContextComponentKind::CurrentTask,
                        3 => ContextComponentKind::RequiredToolSchema,
                        4 => ContextComponentKind::ToolResultEnvelope,
                        5 => ContextComponentKind::CurrentEvidence,
                        6 => ContextComponentKind::RecentTurn,
                        7 => ContextComponentKind::RetrievedMemory,
                        8 => ContextComponentKind::OlderSummary,
                        _ => ContextComponentKind::OptionalToolSchema,
                    },
                    tokens,
                    required,
                })
                .collect::<Vec<_>>();
            let required_ids = components
                .iter()
                .filter(|component| component.required)
                .map(|component| component.id.clone())
                .collect::<Vec<_>>();
            let stage = StageBudget {
                desired_completion_tokens: minimum_completion.saturating_add(desired_extra),
                minimum_completion_tokens: minimum_completion,
            };
            if let Ok(plan) = ContextBudgetBroker.admit(context_window, stage, components) {
                prop_assert!(
                    plan.prompt_tokens
                        .saturating_add(plan.completion_reserve)
                        .saturating_add(plan.safety_margin)
                        <= context_window
                );
                for required_id in required_ids {
                    prop_assert!(plan.components.iter().any(|item| item.id == required_id));
                    prop_assert!(!plan.dropped.iter().any(|item| item.id == required_id));
                }
            }
        }
    }
}
