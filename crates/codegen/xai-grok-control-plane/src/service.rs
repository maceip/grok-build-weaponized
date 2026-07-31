use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::RwLock;
use xai_grok_protocol::{
    CapabilityManifest, PROTOCOL_VERSION, ProtocolError, ProtocolErrorCode, ProviderId,
    ServiceHealth, ServiceId,
};

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ServiceRecord {
    pub service_id: ServiceId,
    pub provider_id: ProviderId,
    pub generation: u64,
    pub manifest_hash: String,
    pub manifest: CapabilityManifest,
    pub health: ServiceHealth,
    pub registered_unix_ms: u64,
    pub last_heartbeat_unix_ms: u64,
    pub restart_count: u64,
}

#[derive(Default)]
struct SupervisorState {
    services: HashMap<ProviderId, ServiceRecord>,
    generations: HashMap<ProviderId, u64>,
}

/// Generation-fenced registry for persistent local and external services.
///
/// A restarted provider receives a new generation. Heartbeats and state
/// changes from older processes are rejected instead of reviving stale work.
pub struct ServiceSupervisor {
    heartbeat_timeout_ms: u64,
    state: RwLock<SupervisorState>,
}

impl ServiceSupervisor {
    pub fn new(heartbeat_timeout_ms: u64) -> Self {
        Self {
            heartbeat_timeout_ms: heartbeat_timeout_ms.max(1),
            state: RwLock::new(SupervisorState::default()),
        }
    }

    pub async fn register(
        &self,
        manifest: CapabilityManifest,
    ) -> Result<ServiceRecord, ProtocolError> {
        manifest.validate()?;
        if !manifest.protocol.supports(PROTOCOL_VERSION) {
            return Err(ProtocolError::new(
                ProtocolErrorCode::IncompatibleVersion,
                format!(
                    "provider {} does not support control-plane protocol {}",
                    manifest.provider_id, PROTOCOL_VERSION
                ),
            ));
        }
        let now = now_unix_ms();
        let mut state = self.state.write().await;
        let generation = state
            .generations
            .get(&manifest.provider_id)
            .copied()
            .unwrap_or(0)
            .saturating_add(1);
        state
            .generations
            .insert(manifest.provider_id.clone(), generation);
        let restart_count = generation.saturating_sub(1);
        let record = ServiceRecord {
            service_id: ServiceId::new(),
            provider_id: manifest.provider_id.clone(),
            generation,
            manifest_hash: manifest.content_hash(),
            manifest,
            health: ServiceHealth::Starting,
            registered_unix_ms: now,
            last_heartbeat_unix_ms: now,
            restart_count,
        };
        state
            .services
            .insert(record.provider_id.clone(), record.clone());
        Ok(record)
    }

    pub async fn heartbeat(
        &self,
        provider_id: &ProviderId,
        generation: u64,
        health: ServiceHealth,
    ) -> Result<ServiceRecord, ProtocolError> {
        let mut state = self.state.write().await;
        let record = state.services.get_mut(provider_id).ok_or_else(|| {
            ProtocolError::new(
                ProtocolErrorCode::UnknownProvider,
                format!("service provider {provider_id} is not registered"),
            )
        })?;
        if record.generation != generation {
            return Err(ProtocolError::new(
                ProtocolErrorCode::Conflict,
                format!(
                    "stale provider generation {generation}; active generation is {}",
                    record.generation
                ),
            ));
        }
        record.health = health;
        record.last_heartbeat_unix_ms = now_unix_ms();
        Ok(record.clone())
    }

    pub async fn mark_failed(
        &self,
        provider_id: &ProviderId,
        generation: u64,
    ) -> Result<ServiceRecord, ProtocolError> {
        self.heartbeat(provider_id, generation, ServiceHealth::Failed)
            .await
    }

    pub async fn remove(&self, provider_id: &ProviderId, generation: u64) -> bool {
        let mut state = self.state.write().await;
        if state
            .services
            .get(provider_id)
            .is_some_and(|record| record.generation == generation)
        {
            state.services.remove(provider_id);
            true
        } else {
            false
        }
    }

    pub async fn sweep_stale(&self) -> Vec<ServiceRecord> {
        let now = now_unix_ms();
        let mut state = self.state.write().await;
        state
            .services
            .values_mut()
            .filter_map(|record| {
                let stale = matches!(
                    record.health,
                    ServiceHealth::Starting
                        | ServiceHealth::Ready
                        | ServiceHealth::Degraded
                        | ServiceHealth::Draining
                ) && now.saturating_sub(record.last_heartbeat_unix_ms)
                    > self.heartbeat_timeout_ms;
                if stale {
                    record.health = ServiceHealth::Failed;
                    Some(record.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub async fn get(&self, provider_id: &ProviderId) -> Option<ServiceRecord> {
        self.state.read().await.services.get(provider_id).cloned()
    }

    pub async fn snapshot(&self) -> Vec<ServiceRecord> {
        let mut records = self
            .state
            .read()
            .await
            .services
            .values()
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by(|left, right| left.provider_id.cmp(&right.provider_id));
        records
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use xai_grok_protocol::{
        ArtifactContract, CancellationSemantics, ConcurrencyProfile, OperationDescriptor,
        ProviderKind, RecoverySemantics, VersionRange,
    };

    use super::*;

    fn manifest() -> CapabilityManifest {
        CapabilityManifest {
            provider_id: "runtime".into(),
            provider_version: "1".to_owned(),
            protocol: VersionRange::exact(1),
            kind: ProviderKind::ModelRuntime,
            features: BTreeSet::new(),
            operations: vec![OperationDescriptor {
                operation_id: "generate".into(),
                display_name: "Generate".to_owned(),
                input_schema: serde_json::json!({}),
                output_schema: serde_json::json!({}),
                streaming: true,
                interactive: true,
                deferred: true,
            }],
            concurrency: ConcurrencyProfile {
                maximum_parallel: 1,
                queue_capacity: 8,
                exclusive_resource: Some("gpu".to_owned()),
            },
            cancellation: CancellationSemantics::Cooperative,
            recovery: RecoverySemantics::Restartable,
            artifacts: ArtifactContract::Optional,
            platforms: BTreeSet::new(),
            metadata: serde_json::Map::new(),
        }
    }

    #[tokio::test]
    async fn stale_generation_cannot_heartbeat() {
        let supervisor = ServiceSupervisor::new(1_000);
        let first = supervisor.register(manifest()).await.unwrap();
        let second = supervisor.register(manifest()).await.unwrap();
        assert!(second.generation > first.generation);
        let error = supervisor
            .heartbeat(&first.provider_id, first.generation, ServiceHealth::Ready)
            .await
            .unwrap_err();
        assert_eq!(error.code, ProtocolErrorCode::Conflict);
    }
}
