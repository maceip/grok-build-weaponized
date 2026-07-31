use std::collections::HashMap;

use tokio::sync::RwLock;
use xai_grok_protocol::{
    ClientId, EngagementId, Event, EventEnvelope, ProjectionQuery, ProjectionSnapshot, ProviderId,
    ServiceHealth, ServiceId, TaskId, TaskStatus, TeamId,
};

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct EngagementProjection {
    pub engagement_id: EngagementId,
    pub workspace_id: Option<String>,
    pub session_id: Option<String>,
    pub team_id: Option<TeamId>,
    pub client_id: Option<ClientId>,
    pub plan_revision: Option<u32>,
    pub task_count: u32,
    pub task_status: HashMap<TaskId, TaskStatus>,
    pub observations: u64,
    pub artifacts: u64,
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
            Event::EngagementAccepted {
                workspace_id,
                session_id,
                team_id,
                client_id,
            } => {
                if let Some(engagement_id) = &envelope.engagement_id {
                    let projection = engagement(&mut state, engagement_id);
                    projection.workspace_id = Some(workspace_id.clone());
                    projection.session_id = Some(session_id.clone());
                    projection.team_id.clone_from(team_id);
                    projection.client_id.clone_from(client_id);
                    projection.last_sequence = envelope.sequence;
                }
            }
            Event::PlanAccepted {
                revision,
                task_count,
            } => {
                if let Some(engagement_id) = &envelope.engagement_id {
                    let projection = engagement(&mut state, engagement_id);
                    projection.plan_revision = Some(*revision);
                    projection.task_count = *task_count;
                    projection.last_sequence = envelope.sequence;
                }
            }
            Event::TaskStatus {
                task_id, status, ..
            } => {
                if let Some(engagement_id) = &envelope.engagement_id {
                    let projection = engagement(&mut state, engagement_id);
                    projection.task_status.insert(task_id.clone(), *status);
                    projection.last_sequence = envelope.sequence;
                }
            }
            Event::Observation { .. } => {
                if let Some(engagement_id) = &envelope.engagement_id {
                    let projection = engagement(&mut state, engagement_id);
                    projection.observations = projection.observations.saturating_add(1);
                    projection.last_sequence = envelope.sequence;
                }
            }
            Event::ArtifactAvailable { .. } => {
                if let Some(engagement_id) = &envelope.engagement_id {
                    let projection = engagement(&mut state, engagement_id);
                    projection.artifacts = projection.artifacts.saturating_add(1);
                    projection.last_sequence = envelope.sequence;
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
