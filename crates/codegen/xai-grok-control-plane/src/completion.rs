use xai_grok_protocol::{
    CompletionPredicate, CompletionTest, CompletionTestResult, EvidenceObservation,
};

use crate::provider::ProviderArtifact;

pub(crate) struct CompletionEvaluation {
    pub mandatory_passed: bool,
    pub contract_valid: bool,
    pub results: Vec<CompletionTestResult>,
}

pub(crate) fn evaluate_completion_tests(
    tests: &[CompletionTest],
    output: &serde_json::Value,
    observations: &[EvidenceObservation],
    artifacts: &[ProviderArtifact],
) -> CompletionEvaluation {
    let mut mandatory_passed = true;
    let mut contract_valid = true;
    let results = tests
        .iter()
        .map(|test| {
            let evaluated = match CompletionPredicate::parse(&test.predicate) {
                Ok(predicate) => evaluate_predicate(&predicate, output, observations, artifacts),
                Err(error) => {
                    contract_valid = false;
                    PredicateResult {
                        passed: false,
                        detail: error,
                    }
                }
            };
            if test.mandatory && !evaluated.passed {
                mandatory_passed = false;
            }
            CompletionTestResult {
                description: test.description.clone(),
                mandatory: test.mandatory,
                passed: evaluated.passed,
                detail: evaluated.detail,
            }
        })
        .collect();
    CompletionEvaluation {
        mandatory_passed: mandatory_passed && contract_valid,
        contract_valid,
        results,
    }
}

struct PredicateResult {
    passed: bool,
    detail: String,
}

fn evaluate_predicate(
    predicate: &CompletionPredicate,
    output: &serde_json::Value,
    observations: &[EvidenceObservation],
    artifacts: &[ProviderArtifact],
) -> PredicateResult {
    match predicate {
        CompletionPredicate::JsonPointerExists { pointer } => {
            let passed = output.pointer(pointer).is_some();
            PredicateResult {
                passed,
                detail: format!(
                    "output pointer {pointer:?} {}",
                    if passed { "exists" } else { "is absent" }
                ),
            }
        }
        CompletionPredicate::JsonPointerEquals { pointer, value } => {
            let actual = output.pointer(pointer);
            let passed = actual == Some(value);
            PredicateResult {
                passed,
                detail: if actual.is_none() {
                    format!("output pointer {pointer:?} is absent")
                } else if passed {
                    format!("output pointer {pointer:?} equals the expected value")
                } else {
                    format!("output pointer {pointer:?} does not equal the expected value")
                },
            }
        }
        CompletionPredicate::ObservationCount {
            minimum,
            finding_contains,
            minimum_confidence,
        } => {
            let matched = observations
                .iter()
                .filter(|observation| {
                    finding_contains
                        .as_ref()
                        .is_none_or(|needle| observation.finding.contains(needle))
                        && minimum_confidence
                            .is_none_or(|minimum| observation.confidence >= minimum)
                })
                .count();
            let passed = matched >= *minimum as usize;
            PredicateResult {
                passed,
                detail: format!("matched {matched} observations; required at least {minimum}"),
            }
        }
        CompletionPredicate::ArtifactCount {
            minimum,
            media_type,
        } => {
            let matched = artifacts
                .iter()
                .filter(|artifact| {
                    media_type
                        .as_ref()
                        .is_none_or(|expected| artifact.media_type == *expected)
                })
                .count();
            let passed = matched >= *minimum as usize;
            PredicateResult {
                passed,
                detail: format!("matched {matched} artifacts; required at least {minimum}"),
            }
        }
        CompletionPredicate::All { predicates } => {
            let results = predicates
                .iter()
                .map(|predicate| evaluate_predicate(predicate, output, observations, artifacts))
                .collect::<Vec<_>>();
            let passed_count = results.iter().filter(|result| result.passed).count();
            PredicateResult {
                passed: passed_count == results.len(),
                detail: format!(
                    "all group passed {passed_count}/{} predicates",
                    results.len()
                ),
            }
        }
        CompletionPredicate::Any { predicates } => {
            let results = predicates
                .iter()
                .map(|predicate| evaluate_predicate(predicate, output, observations, artifacts))
                .collect::<Vec<_>>();
            let passed_count = results.iter().filter(|result| result.passed).count();
            PredicateResult {
                passed: passed_count > 0,
                detail: format!(
                    "any group passed {passed_count}/{} predicates",
                    results.len()
                ),
            }
        }
        CompletionPredicate::Not { predicate } => {
            let result = evaluate_predicate(predicate, output, observations, artifacts);
            PredicateResult {
                passed: !result.passed,
                detail: format!("negated predicate was {}", result.passed),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::ProviderArtifact;

    fn test(description: &str, mandatory: bool, predicate: serde_json::Value) -> CompletionTest {
        CompletionTest {
            description: description.to_owned(),
            predicate,
            mandatory,
        }
    }

    #[test]
    fn evaluates_output_observation_artifact_and_boolean_predicates() {
        let observations = vec![EvidenceObservation {
            finding: "tcp/443 is open: nginx".to_owned(),
            confidence: 0.95,
            artifact_id: None,
            attributes: serde_json::Map::new(),
        }];
        let artifacts = vec![ProviderArtifact::inline("application/xml", Vec::new())];
        let tests = vec![
            test(
                "exit succeeded",
                true,
                serde_json::json!({
                    "op":"json_pointer_equals",
                    "pointer":"/exit_code",
                    "value":0
                }),
            ),
            test(
                "open service observed",
                true,
                serde_json::json!({
                    "op":"observation_count",
                    "minimum":1,
                    "finding_contains":"443",
                    "minimum_confidence":0.9
                }),
            ),
            test(
                "XML retained",
                false,
                serde_json::json!({
                    "op":"artifact_count",
                    "minimum":1,
                    "media_type":"application/xml"
                }),
            ),
            test(
                "no error field",
                true,
                serde_json::json!({
                    "op":"not",
                    "predicate":{"op":"json_pointer_exists","pointer":"/error"}
                }),
            ),
            test(
                "nested evidence alternatives",
                true,
                serde_json::json!({
                    "op":"all",
                    "predicates":[
                        {"op":"json_pointer_exists","pointer":"/exit_code"},
                        {
                            "op":"any",
                            "predicates":[
                                {"op":"artifact_count","minimum":2},
                                {"op":"observation_count","minimum":1,"finding_contains":"nginx"}
                            ]
                        }
                    ]
                }),
            ),
        ];
        let evaluation = evaluate_completion_tests(
            &tests,
            &serde_json::json!({"exit_code":0}),
            &observations,
            &artifacts,
        );
        assert!(evaluation.contract_valid);
        assert!(evaluation.mandatory_passed);
        assert!(evaluation.results.iter().all(|result| result.passed));
    }

    #[test]
    fn invalid_or_failed_mandatory_predicates_fail_closed() {
        let tests = vec![
            test(
                "wrong exit",
                true,
                serde_json::json!({
                    "op":"json_pointer_equals",
                    "pointer":"/exit_code",
                    "value":0
                }),
            ),
            test(
                "unknown operation",
                false,
                serde_json::json!({"op":"unrecognized"}),
            ),
        ];
        let evaluation =
            evaluate_completion_tests(&tests, &serde_json::json!({"exit_code":7}), &[], &[]);
        assert!(!evaluation.contract_valid);
        assert!(!evaluation.mandatory_passed);
        assert!(evaluation.results.iter().all(|result| !result.passed));
    }
}
