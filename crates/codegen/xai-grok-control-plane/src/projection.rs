use std::collections::HashMap;

use tokio::sync::RwLock;
use xai_grok_protocol::{
    ArtifactId, ClientId, EngagementId, Event, EventEnvelope, EvidenceId, Exercise,
    ExerciseEvidence, ExerciseId, ExerciseRecord, Finding, FindingId, MessageId, OperationRun,
    OperationRunId, OperatorCatalog, OperatorSession, OperatorSessionId, Playbook, PlaybookId,
    ProjectionQuery, ProjectionSnapshot, ProviderId, ResourceClaimId, ServiceHealth, ServiceId,
    TaskArtifactProjection, TaskGraphProjection, TaskId, TaskObservationProjection, TaskProjection,
    TaskStatus, TeamId, TeamMessage, TeamPresence, TeamProjection, TeamResourceClaim, TeamWorkItem,
    TeamWorkItemId,
};

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ArtifactProjection {
    pub artifact_id: ArtifactId,
    pub task_id: Option<TaskId>,
    pub media_type: String,
    pub byte_size: u64,
    pub sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ProviderOutputProjection {
    pub task_id: TaskId,
    pub artifact_id: ArtifactId,
    pub sequence: u64,
}

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct EngagementProjection {
    pub engagement_id: EngagementId,
    pub workspace_id: Option<String>,
    pub session_id: Option<String>,
    pub exercise_id: Option<ExerciseId>,
    pub operation_run_id: Option<OperationRunId>,
    pub operator_session_id: Option<OperatorSessionId>,
    pub team_id: Option<TeamId>,
    pub client_id: Option<ClientId>,
    pub plan_revision: Option<u32>,
    pub task_count: u32,
    pub task_status: HashMap<TaskId, TaskStatus>,
    pub observations: u64,
    pub artifacts: u64,
    pub artifact_refs: Vec<ArtifactProjection>,
    pub provider_outputs: Vec<ProviderOutputProjection>,
    pub last_sequence: u64,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ProviderProjection {
    pub provider_id: ProviderId,
    pub service_id: ServiceId,
    pub generation: u64,
    pub health: ServiceHealth,
    pub last_sequence: u64,
}

#[derive(Default)]
struct ProjectionState {
    as_of_sequence: u64,
    engagements: HashMap<EngagementId, EngagementProjection>,
    task_graphs: HashMap<EngagementId, TaskGraphProjection>,
    exercises: HashMap<ExerciseId, Exercise>,
    operation_runs: HashMap<OperationRunId, OperationRun>,
    sessions: HashMap<OperatorSessionId, OperatorSession>,
    playbooks: HashMap<PlaybookId, Playbook>,
    evidence: HashMap<EvidenceId, ExerciseEvidence>,
    findings: HashMap<FindingId, Finding>,
    team_presence: HashMap<(TeamId, ClientId), TeamPresence>,
    team_work_items: HashMap<TeamWorkItemId, TeamWorkItem>,
    team_messages: HashMap<MessageId, TeamMessage>,
    team_resource_claims: HashMap<ResourceClaimId, TeamResourceClaim>,
    providers: HashMap<ProviderId, ProviderProjection>,
    overloads: HashMap<String, (u32, u32)>,
}

/// Rebuildable in-memory read models derived solely from the event journal.
pub struct ProjectionStore {
    state: RwLock<ProjectionState>,
}

impl ProjectionStore {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(ProjectionState::default()),
        }
    }

    pub async fn replay(&self, events: impl IntoIterator<Item = EventEnvelope>) {
        for event in events {
            self.apply(&event).await;
        }
    }

    pub async fn apply(&self, envelope: &EventEnvelope) {
        let mut state = self.state.write().await;
        if envelope.sequence <= state.as_of_sequence {
            return;
        }
        state.as_of_sequence = envelope.sequence;
        match &envelope.event {
            Event::ExerciseCreated { exercise } => {
                state
                    .exercises
                    .insert(exercise.exercise_id.clone(), exercise.clone());
            }
            Event::OperationRunCreated { operation_run } => {
                state.operation_runs.insert(
                    operation_run.operation_run_id.clone(),
                    operation_run.clone(),
                );
            }
            Event::OperatorSessionCreated { session } => {
                state
                    .sessions
                    .insert(session.session_id.clone(), session.clone());
            }
            Event::PlaybookCreated { playbook } => {
                state
                    .playbooks
                    .insert(playbook.playbook_id.clone(), playbook.clone());
            }
            Event::EvidenceRecorded { evidence } => {
                state
                    .evidence
                    .insert(evidence.evidence_id.clone(), evidence.clone());
            }
            Event::FindingCreated { finding } | Event::FindingStatusSet { finding } => {
                state
                    .findings
                    .insert(finding.finding_id.clone(), finding.clone());
            }
            Event::TeamPresenceSet { presence } => {
                state.team_presence.insert(
                    (
                        presence.client.team_id.clone(),
                        presence.client.client_id.clone(),
                    ),
                    presence.clone(),
                );
            }
            Event::TeamWorkItemCreated { work_item } | Event::TeamWorkItemUpdated { work_item } => {
                state
                    .team_work_items
                    .insert(work_item.work_item_id.clone(), work_item.clone());
            }
            Event::TeamMessagePosted { message } => {
                state
                    .team_messages
                    .insert(message.message_id.clone(), message.clone());
            }
            Event::TeamResourceClaimed { claim } | Event::TeamResourceReleased { claim } => {
                state
                    .team_resource_claims
                    .insert(claim.claim_id.clone(), claim.clone());
            }
            Event::EngagementAccepted {
                workspace_id,
                session_id,
                exercise_id,
                operation_run_id,
                operator_session_id,
                team_id,
                client_id,
            } => {
                if let Some(engagement_id) = &envelope.engagement_id {
                    let projection = engagement(&mut state, engagement_id);
                    projection.workspace_id = Some(workspace_id.clone());
                    projection.session_id = Some(session_id.clone());
                    projection.exercise_id.clone_from(exercise_id);
                    projection.operation_run_id.clone_from(operation_run_id);
                    projection
                        .operator_session_id
                        .clone_from(operator_session_id);
                    projection.team_id.clone_from(team_id);
                    projection.client_id.clone_from(client_id);
                    projection.last_sequence = envelope.sequence;
                }
            }
            Event::PlanAccepted {
                revision,
                task_count,
                plan,
            } => {
                if let Some(engagement_id) = &envelope.engagement_id {
                    let projection = engagement(&mut state, engagement_id);
                    projection.plan_revision = Some(*revision);
                    projection.task_count = *task_count;
                    projection.last_sequence = envelope.sequence;
                    if let Some(plan) = plan {
                        let prior = state.task_graphs.remove(engagement_id);
                        let tasks = plan
                            .tasks
                            .iter()
                            .cloned()
                            .map(|task| {
                                let previous = prior.as_ref().and_then(|graph| {
                                    graph
                                        .tasks
                                        .iter()
                                        .find(|node| node.task.task_id == task.task_id)
                                });
                                TaskProjection {
                                    task,
                                    status: previous
                                        .map_or(TaskStatus::Prepared, |node| node.status),
                                    provider_id: previous.and_then(|node| node.provider_id.clone()),
                                    observations: previous
                                        .map_or_else(Vec::new, |node| node.observations.clone()),
                                    artifacts: previous
                                        .map_or_else(Vec::new, |node| node.artifacts.clone()),
                                    last_sequence: envelope.sequence,
                                }
                            })
                            .collect();
                        state.task_graphs.insert(
                            engagement_id.clone(),
                            TaskGraphProjection {
                                engagement_id: engagement_id.clone(),
                                revision: *revision,
                                objective: plan.objective.clone(),
                                tasks,
                                last_sequence: envelope.sequence,
                            },
                        );
                    }
                }
            }
            Event::TaskStatus {
                task_id,
                status,
                provider_id,
            } => {
                if let Some(engagement_id) = &envelope.engagement_id {
                    let projection = engagement(&mut state, engagement_id);
                    projection.task_status.insert(task_id.clone(), *status);
                    projection.last_sequence = envelope.sequence;
                    if let Some(node) = state.task_graphs.get_mut(engagement_id).and_then(|graph| {
                        graph
                            .tasks
                            .iter_mut()
                            .find(|node| node.task.task_id == *task_id)
                    }) {
                        node.status = *status;
                        node.provider_id.clone_from(provider_id);
                        node.last_sequence = envelope.sequence;
                    }
                    if let Some(graph) = state.task_graphs.get_mut(engagement_id) {
                        graph.last_sequence = envelope.sequence;
                    }
                }
            }
            Event::Observation {
                task_id,
                observation,
            } => {
                if let Some(engagement_id) = &envelope.engagement_id {
                    let projection = engagement(&mut state, engagement_id);
                    projection.observations = projection.observations.saturating_add(1);
                    projection.last_sequence = envelope.sequence;
                    if let Some(graph) = state.task_graphs.get_mut(engagement_id) {
                        if let Some(node) = graph
                            .tasks
                            .iter_mut()
                            .find(|node| node.task.task_id == *task_id)
                        {
                            node.observations.push(TaskObservationProjection {
                                sequence: envelope.sequence,
                                observation: observation.clone(),
                            });
                            node.last_sequence = envelope.sequence;
                        }
                        graph.last_sequence = envelope.sequence;
                    }
                }
            }
            Event::ArtifactAvailable {
                artifact_id,
                task_id,
                media_type,
                byte_size,
            } => {
                if let Some(engagement_id) = &envelope.engagement_id {
                    let projection = engagement(&mut state, engagement_id);
                    projection.artifacts = projection.artifacts.saturating_add(1);
                    projection.artifact_refs.push(ArtifactProjection {
                        artifact_id: artifact_id.clone(),
                        task_id: task_id.clone(),
                        media_type: media_type.clone(),
                        byte_size: *byte_size,
                        sequence: envelope.sequence,
                    });
                    projection.last_sequence = envelope.sequence;
                    if let Some(task_id) = task_id
                        && let Some(graph) = state.task_graphs.get_mut(engagement_id)
                    {
                        if let Some(node) = graph
                            .tasks
                            .iter_mut()
                            .find(|node| node.task.task_id == *task_id)
                        {
                            node.artifacts.push(TaskArtifactProjection {
                                artifact_id: artifact_id.clone(),
                                media_type: media_type.clone(),
                                byte_size: *byte_size,
                                sequence: envelope.sequence,
                                provider_output: false,
                            });
                            node.last_sequence = envelope.sequence;
                        }
                        graph.last_sequence = envelope.sequence;
                    }
                }
            }
            Event::ProviderOutput {
                task_id,
                artifact_id,
            } => {
                if let Some(engagement_id) = &envelope.engagement_id {
                    let projection = engagement(&mut state, engagement_id);
                    projection.provider_outputs.push(ProviderOutputProjection {
                        task_id: task_id.clone(),
                        artifact_id: artifact_id.clone(),
                        sequence: envelope.sequence,
                    });
                    projection.last_sequence = envelope.sequence;
                    if let Some(graph) = state.task_graphs.get_mut(engagement_id) {
                        if let Some(node) = graph
                            .tasks
                            .iter_mut()
                            .find(|node| node.task.task_id == *task_id)
                        {
                            if let Some(artifact) = node
                                .artifacts
                                .iter_mut()
                                .find(|artifact| artifact.artifact_id == *artifact_id)
                            {
                                artifact.provider_output = true;
                            }
                            node.last_sequence = envelope.sequence;
                        }
                        graph.last_sequence = envelope.sequence;
                    }
                }
            }
            Event::ProviderState {
                provider_id,
                service_id,
                generation,
                health,
            } => {
                let stale = state
                    .providers
                    .get(provider_id)
                    .is_some_and(|current| *generation < current.generation);
                if !stale {
                    state.providers.insert(
                        provider_id.clone(),
                        ProviderProjection {
                            provider_id: provider_id.clone(),
                            service_id: service_id.clone(),
                            generation: *generation,
                            health: *health,
                            last_sequence: envelope.sequence,
                        },
                    );
                }
            }
            Event::Overload {
                component,
                queue_depth,
                capacity,
            } => {
                state
                    .overloads
                    .insert(component.clone(), (*queue_depth, *capacity));
            }
        }
    }

    pub async fn query(&self, query: ProjectionQuery) -> ProjectionSnapshot {
        let state = self.state.read().await;
        let value = match query {
            ProjectionQuery::Engagement { engagement_id } => {
                serde_json::to_value(state.engagements.get(&engagement_id))
                    .unwrap_or(serde_json::Value::Null)
            }
            ProjectionQuery::TaskGraph { engagement_id } => {
                serde_json::to_value(state.task_graphs.get(&engagement_id))
                    .unwrap_or(serde_json::Value::Null)
            }
            ProjectionQuery::OperatorCatalog { workspace_id } => {
                let mut exercises = state
                    .exercises
                    .values()
                    .filter(|exercise| {
                        workspace_id
                            .as_ref()
                            .is_none_or(|expected| &exercise.workspace_id == expected)
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                exercises.sort_by(|left, right| {
                    left.created_unix_ms
                        .cmp(&right.created_unix_ms)
                        .then_with(|| left.exercise_id.cmp(&right.exercise_id))
                });
                let exercise_ids = exercises
                    .iter()
                    .map(|exercise| exercise.exercise_id.clone())
                    .collect::<std::collections::HashSet<_>>();
                let mut operation_runs = state
                    .operation_runs
                    .values()
                    .filter(|run| exercise_ids.contains(&run.exercise_id))
                    .cloned()
                    .collect::<Vec<_>>();
                operation_runs.sort_by(|left, right| {
                    left.created_unix_ms
                        .cmp(&right.created_unix_ms)
                        .then_with(|| left.operation_run_id.cmp(&right.operation_run_id))
                });
                let mut sessions = state
                    .sessions
                    .values()
                    .filter(|session| exercise_ids.contains(&session.exercise_id))
                    .cloned()
                    .collect::<Vec<_>>();
                sessions.sort_by(|left, right| {
                    left.created_unix_ms
                        .cmp(&right.created_unix_ms)
                        .then_with(|| left.session_id.cmp(&right.session_id))
                });
                let mut playbooks = state
                    .playbooks
                    .values()
                    .filter(|playbook| {
                        workspace_id
                            .as_ref()
                            .is_none_or(|expected| &playbook.workspace_id == expected)
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                playbooks.sort_by(|left, right| {
                    left.name
                        .cmp(&right.name)
                        .then_with(|| left.revision.cmp(&right.revision))
                });
                serde_json::to_value(OperatorCatalog {
                    exercises,
                    operation_runs,
                    sessions,
                    playbooks,
                })
                .unwrap_or(serde_json::Value::Null)
            }
            ProjectionQuery::ExerciseRecord { exercise_id } => {
                let value = state.exercises.get(&exercise_id).map(|exercise| {
                    let mut operation_runs = state
                        .operation_runs
                        .values()
                        .filter(|run| run.exercise_id == exercise_id)
                        .cloned()
                        .collect::<Vec<_>>();
                    operation_runs.sort_by_key(|run| run.created_unix_ms);
                    let mut sessions = state
                        .sessions
                        .values()
                        .filter(|session| session.exercise_id == exercise_id)
                        .cloned()
                        .collect::<Vec<_>>();
                    sessions.sort_by_key(|session| session.created_unix_ms);
                    let mut evidence = state
                        .evidence
                        .values()
                        .filter(|record| record.exercise_id == exercise_id)
                        .cloned()
                        .collect::<Vec<_>>();
                    evidence.sort_by_key(|record| record.observed_unix_ms);
                    let mut findings = state
                        .findings
                        .values()
                        .filter(|finding| finding.exercise_id == exercise_id)
                        .cloned()
                        .collect::<Vec<_>>();
                    findings.sort_by_key(|finding| finding.created_unix_ms);
                    ExerciseRecord {
                        exercise: exercise.clone(),
                        operation_runs,
                        sessions,
                        evidence,
                        findings,
                    }
                });
                serde_json::to_value(value).unwrap_or(serde_json::Value::Null)
            }
            ProjectionQuery::Team { team_id } => {
                let mut presence = state
                    .team_presence
                    .values()
                    .filter(|presence| presence.client.team_id == team_id)
                    .cloned()
                    .collect::<Vec<_>>();
                presence.sort_by(|left, right| left.client.client_id.cmp(&right.client.client_id));
                let mut work_items = state
                    .team_work_items
                    .values()
                    .filter(|work_item| work_item.team_id == team_id)
                    .cloned()
                    .collect::<Vec<_>>();
                work_items.sort_by_key(|work_item| work_item.created_unix_ms);
                let mut messages = state
                    .team_messages
                    .values()
                    .filter(|message| message.team_id == team_id)
                    .cloned()
                    .collect::<Vec<_>>();
                messages.sort_by_key(|message| message.sent_unix_ms);
                if messages.len() > 500 {
                    messages.drain(..messages.len() - 500);
                }
                let mut resource_claims = state
                    .team_resource_claims
                    .values()
                    .filter(|claim| claim.team_id == team_id)
                    .cloned()
                    .collect::<Vec<_>>();
                resource_claims.sort_by_key(|claim| claim.claimed_unix_ms);
                serde_json::to_value(TeamProjection {
                    team_id,
                    presence,
                    work_items,
                    messages,
                    resource_claims,
                })
                .unwrap_or(serde_json::Value::Null)
            }
            ProjectionQuery::Providers => {
                let mut providers = state.providers.values().cloned().collect::<Vec<_>>();
                providers.sort_by(|left, right| left.provider_id.cmp(&right.provider_id));
                serde_json::to_value(providers).unwrap_or(serde_json::Value::Null)
            }
            ProjectionQuery::Capacity => {
                serde_json::to_value(&state.overloads).unwrap_or(serde_json::Value::Null)
            }
        };
        ProjectionSnapshot {
            as_of_sequence: state.as_of_sequence,
            value,
        }
    }

    pub async fn as_of_sequence(&self) -> u64 {
        self.state.read().await.as_of_sequence
    }

    pub async fn contains_exercise(&self, exercise_id: &ExerciseId) -> bool {
        self.state.read().await.exercises.contains_key(exercise_id)
    }

    pub async fn exercise(&self, exercise_id: &ExerciseId) -> Option<Exercise> {
        self.state.read().await.exercises.get(exercise_id).cloned()
    }

    pub async fn playbook(&self, playbook_id: &PlaybookId) -> Option<Playbook> {
        self.state.read().await.playbooks.get(playbook_id).cloned()
    }

    pub async fn evidence(&self, evidence_id: &EvidenceId) -> Option<ExerciseEvidence> {
        self.state.read().await.evidence.get(evidence_id).cloned()
    }

    pub async fn finding(&self, finding_id: &FindingId) -> Option<Finding> {
        self.state.read().await.findings.get(finding_id).cloned()
    }

    pub async fn team_member_exists(&self, team_id: &TeamId, client_id: &ClientId) -> bool {
        self.state
            .read()
            .await
            .team_presence
            .contains_key(&(team_id.clone(), client_id.clone()))
    }

    pub async fn team_work_item(&self, work_item_id: &TeamWorkItemId) -> Option<TeamWorkItem> {
        self.state
            .read()
            .await
            .team_work_items
            .get(work_item_id)
            .cloned()
    }

    pub async fn team_message(&self, message_id: &MessageId) -> Option<TeamMessage> {
        self.state
            .read()
            .await
            .team_messages
            .get(message_id)
            .cloned()
    }

    pub async fn team_resource_claim(
        &self,
        claim_id: &ResourceClaimId,
    ) -> Option<TeamResourceClaim> {
        self.state
            .read()
            .await
            .team_resource_claims
            .get(claim_id)
            .cloned()
    }

    pub async fn active_team_resource_claim(
        &self,
        team_id: &TeamId,
        resource_key: &str,
        now_unix_ms: u64,
    ) -> Option<TeamResourceClaim> {
        self.state
            .read()
            .await
            .team_resource_claims
            .values()
            .find(|claim| {
                &claim.team_id == team_id
                    && claim.resource_key == resource_key
                    && claim.released_unix_ms.is_none()
                    && claim.expires_unix_ms > now_unix_ms
            })
            .cloned()
    }

    pub async fn task_belongs_to_exercise(
        &self,
        task_id: &TaskId,
        exercise_id: &ExerciseId,
    ) -> bool {
        self.state
            .read()
            .await
            .engagements
            .values()
            .any(|engagement| {
                engagement.exercise_id.as_ref() == Some(exercise_id)
                    && engagement.task_status.contains_key(task_id)
            })
    }

    pub async fn operation_run(&self, operation_run_id: &OperationRunId) -> Option<OperationRun> {
        self.state
            .read()
            .await
            .operation_runs
            .get(operation_run_id)
            .cloned()
    }

    pub async fn operator_session(
        &self,
        session_id: &OperatorSessionId,
    ) -> Option<OperatorSession> {
        self.state.read().await.sessions.get(session_id).cloned()
    }

    pub async fn task_statuses(&self, engagement_id: &EngagementId) -> HashMap<TaskId, TaskStatus> {
        self.state
            .read()
            .await
            .engagements
            .get(engagement_id)
            .map(|engagement| engagement.task_status.clone())
            .unwrap_or_default()
    }
}

impl Default for ProjectionStore {
    fn default() -> Self {
        Self::new()
    }
}

fn engagement<'a>(
    state: &'a mut ProjectionState,
    engagement_id: &EngagementId,
) -> &'a mut EngagementProjection {
    state
        .engagements
        .entry(engagement_id.clone())
        .or_insert_with(|| EngagementProjection {
            engagement_id: engagement_id.clone(),
            ..EngagementProjection::default()
        })
}

#[cfg(test)]
mod tests {
    use xai_grok_protocol::{EventId, PROTOCOL_VERSION};

    use super::*;

    #[tokio::test]
    async fn stale_provider_generation_is_ignored() {
        let store = ProjectionStore::new();
        let provider_id = ProviderId::new();
        for (sequence, generation, health) in
            [(1, 2, ServiceHealth::Ready), (2, 1, ServiceHealth::Failed)]
        {
            store
                .apply(&EventEnvelope {
                    protocol_version: PROTOCOL_VERSION,
                    event_id: EventId::new(),
                    engagement_id: None,
                    sequence,
                    causation_id: None,
                    generation,
                    observed_unix_ms: 1,
                    event: Event::ProviderState {
                        provider_id: provider_id.clone(),
                        service_id: ServiceId::new(),
                        generation,
                        health,
                    },
                })
                .await;
        }
        let snapshot = store.query(ProjectionQuery::Providers).await;
        let providers: Vec<ProviderProjection> = serde_json::from_value(snapshot.value).unwrap();
        assert_eq!(providers[0].generation, 2);
        assert_eq!(providers[0].health, ServiceHealth::Ready);
    }
}
