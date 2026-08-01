//! Thin reconnecting client for the local `grokd` protocol.
//!
//! This crate deliberately excludes daemon storage, execution providers,
//! model runtimes, GUI code, HTTP clients, and MCP dependencies.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::UnixStream;
use tokio::sync::Mutex;
use xai_grok_protocol::{
    ArtifactId, BuzzIngress, Command, CommandEnvelope, CommandId, EngagementId, Event, EventBatch,
    EventReadRequest, Hello, IngressEnvelope, IngressSource, MAX_CONTROL_FRAME_BYTES,
    PROTOCOL_VERSION, ProtocolError, ProtocolErrorCode, QmIngress, Response, ResponseEnvelope,
    TaskGraphProjection, TaskId, TeamClient,
};

#[derive(Clone, Debug)]
pub struct ClientIdentity {
    pub client_name: String,
    pub client_version: String,
    pub team: Option<TeamClient>,
}

impl ClientIdentity {
    pub fn new(client_name: impl Into<String>, client_version: impl Into<String>) -> Self {
        Self {
            client_name: client_name.into(),
            client_version: client_version.into(),
            team: None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("control-plane socket I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("control-plane frame encode: {0}")]
    Encode(#[from] rmp_serde::encode::Error),
    #[error("control-plane frame decode: {0}")]
    Decode(#[from] rmp_serde::decode::Error),
    #[error("control-plane frame is {actual} bytes; maximum is {maximum}")]
    FrameTooLarge { actual: usize, maximum: usize },
    #[error("control-plane protocol: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("control-plane returned an incompatible handshake")]
    InvalidHandshake,
    #[error("control-plane request timed out after {0} milliseconds; reconnect before retrying")]
    Timeout(u128),
    #[error("control-plane connection is no longer synchronized; reconnect before retrying")]
    ConnectionPoisoned,
    #[error("control-plane response id does not match the submitted command")]
    MismatchedResponse,
    #[error("control-plane returned {0} instead of an event batch")]
    ExpectedEvents(&'static str),
}

#[derive(Debug, thiserror::Error)]
pub enum SourceAdapterError {
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error("source adapter expected {expected}, received {actual}")]
    UnexpectedResponse {
        expected: &'static str,
        actual: &'static str,
    },
    #[error("source adapter projection decode: {0}")]
    Projection(#[from] serde_json::Error),
    #[error("invalid source delivery: {0}")]
    InvalidDelivery(String),
}

/// Durable acknowledgement returned to Buzz or QM after `grokd` accepts an
/// ingress delivery. Re-delivering the same source identifier returns the same
/// engagement ID with `accepted == false`.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SourceDeliveryReceipt {
    pub protocol_version: u32,
    pub source: IngressSource,
    pub source_event_id: String,
    pub engagement_id: EngagementId,
    pub accepted: bool,
}

/// Compact, source-neutral reference to one durable Grok event.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SourceEventReference {
    pub event_id: String,
    pub sequence: u64,
    pub generation: u64,
    pub observed_unix_ms: u64,
    pub event_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<TaskId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_id: Option<ArtifactId>,
}

/// Cursor-resumable progress page for a previously acknowledged source
/// delivery. `next_sequence` advances across unrelated daemon events as well,
/// so reconnecting adapters never rescan global traffic.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SourceProgressPage {
    pub protocol_version: u32,
    pub source: IngressSource,
    pub source_event_id: String,
    pub engagement_id: EngagementId,
    pub references: Vec<SourceEventReference>,
    pub high_watermark: u64,
    pub next_sequence: u64,
    pub caught_up: bool,
}

/// Versioned NDJSON frames emitted by `grokctl ingress ... --follow`.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "frame", rename_all = "snake_case")]
pub enum SourceAdapterFrame {
    Accepted {
        receipt: SourceDeliveryReceipt,
    },
    Progress {
        page: SourceProgressPage,
    },
    Complete {
        protocol_version: u32,
        source: IngressSource,
        source_event_id: String,
        engagement_id: EngagementId,
        next_sequence: u64,
    },
}

/// Executable Buzz/QM ingress boundary over the local versioned `grokd`
/// protocol. Source systems remain transport adapters; they never own or
/// invoke Grok execution state directly.
pub struct SourceIngressAdapter<'a> {
    control: &'a ControlPlaneClient,
}

impl<'a> SourceIngressAdapter<'a> {
    pub fn new(control: &'a ControlPlaneClient) -> Self {
        Self { control }
    }

    pub async fn deliver_buzz(
        &self,
        delivery: BuzzIngress,
    ) -> Result<SourceDeliveryReceipt, SourceAdapterError> {
        require_source_fields(&[
            ("event_id", &delivery.event_id),
            ("community", &delivery.community),
            ("channel_id", &delivery.channel_id),
            ("author", &delivery.author),
            ("content", &delivery.content),
        ])?;
        let source_event_id = delivery.event_id.clone();
        self.deliver(
            IngressSource::Buzz,
            source_event_id,
            delivery.into_envelope(),
        )
        .await
    }

    pub async fn deliver_qm(
        &self,
        delivery: QmIngress,
    ) -> Result<SourceDeliveryReceipt, SourceAdapterError> {
        require_source_fields(&[
            ("job_id", &delivery.job_id),
            ("scope_id", &delivery.scope_id),
            ("request", &delivery.request),
        ])?;
        if delivery
            .room_id
            .as_ref()
            .is_some_and(|room| room.trim().is_empty())
        {
            return Err(SourceAdapterError::InvalidDelivery(
                "room_id must not be empty when supplied".to_owned(),
            ));
        }
        let source_event_id = delivery.job_id.clone();
        self.deliver(IngressSource::Qm, source_event_id, delivery.into_envelope())
            .await
    }

    async fn deliver(
        &self,
        source: IngressSource,
        source_event_id: String,
        ingress: IngressEnvelope,
    ) -> Result<SourceDeliveryReceipt, SourceAdapterError> {
        let response = self
            .control
            .send(Command::SubmitIngress(ingress), Duration::from_secs(10))
            .await?;
        let Response::Accepted {
            engagement_id,
            accepted,
        } = response
        else {
            return Err(SourceAdapterError::UnexpectedResponse {
                expected: "accepted",
                actual: response_name(&response),
            });
        };
        Ok(SourceDeliveryReceipt {
            protocol_version: PROTOCOL_VERSION,
            source,
            source_event_id,
            engagement_id,
            accepted,
        })
    }

    pub async fn progress(
        &self,
        receipt: &SourceDeliveryReceipt,
        after_sequence: u64,
        maximum_events: u32,
        wait_ms: u32,
    ) -> Result<SourceProgressPage, SourceAdapterError> {
        let batch = self
            .control
            .read_events(EventReadRequest {
                after_sequence,
                maximum_events,
                wait_ms,
                engagement_id: Some(receipt.engagement_id.clone()),
            })
            .await?;
        Ok(SourceProgressPage {
            protocol_version: PROTOCOL_VERSION,
            source: receipt.source,
            source_event_id: receipt.source_event_id.clone(),
            engagement_id: receipt.engagement_id.clone(),
            references: batch.events.iter().map(event_reference).collect(),
            high_watermark: batch.high_watermark,
            next_sequence: batch.next_sequence,
            caught_up: batch.caught_up,
        })
    }

    pub async fn task_graph(
        &self,
        engagement_id: EngagementId,
    ) -> Result<Option<TaskGraphProjection>, SourceAdapterError> {
        let response = self
            .control
            .send(
                Command::QueryProjection(xai_grok_protocol::ProjectionQuery::TaskGraph {
                    engagement_id,
                }),
                Duration::from_secs(2),
            )
            .await?;
        let Response::Projection(snapshot) = response else {
            return Err(SourceAdapterError::UnexpectedResponse {
                expected: "projection",
                actual: response_name(&response),
            });
        };
        if snapshot.value.is_null() {
            return Ok(None);
        }
        Ok(Some(serde_json::from_value(snapshot.value)?))
    }

    pub async fn is_terminal(
        &self,
        engagement_id: EngagementId,
    ) -> Result<bool, SourceAdapterError> {
        let Some(graph) = self.task_graph(engagement_id).await? else {
            return Ok(false);
        };
        Ok(!graph.tasks.is_empty()
            && graph.tasks.iter().all(|task| {
                matches!(
                    task.status,
                    xai_grok_protocol::TaskStatus::Completed
                        | xai_grok_protocol::TaskStatus::Failed
                        | xai_grok_protocol::TaskStatus::Cancelled
                        | xai_grok_protocol::TaskStatus::Lost
                )
            }))
    }
}

fn require_source_fields(fields: &[(&str, &String)]) -> Result<(), SourceAdapterError> {
    for (name, value) in fields {
        if value.trim().is_empty() {
            return Err(SourceAdapterError::InvalidDelivery(format!(
                "{name} must not be empty"
            )));
        }
    }
    Ok(())
}

fn event_reference(envelope: &xai_grok_protocol::EventEnvelope) -> SourceEventReference {
    let (event_kind, task_id, artifact_id) = match &envelope.event {
        Event::ExerciseCreated { .. } => ("exercise_created", None, None),
        Event::OperationRunCreated { .. } => ("operation_run_created", None, None),
        Event::OperatorSessionCreated { .. } => ("operator_session_created", None, None),
        Event::PlaybookCreated { .. } => ("playbook_created", None, None),
        Event::EvidenceRecorded { .. } => ("evidence_recorded", None, None),
        Event::FindingCreated { .. } => ("finding_created", None, None),
        Event::FindingStatusSet { .. } => ("finding_status_set", None, None),
        Event::TeamPresenceSet { .. } => ("team_presence_set", None, None),
        Event::TeamWorkItemCreated { .. } => ("team_work_item_created", None, None),
        Event::TeamWorkItemUpdated { .. } => ("team_work_item_updated", None, None),
        Event::TeamMessagePosted { .. } => ("team_message_posted", None, None),
        Event::TeamResourceClaimed { .. } => ("team_resource_claimed", None, None),
        Event::TeamResourceReleased { .. } => ("team_resource_released", None, None),
        Event::EngagementAccepted { .. } => ("engagement_accepted", None, None),
        Event::PlanAccepted { .. } => ("plan_accepted", None, None),
        Event::TaskStatus { task_id, .. } => ("task_status", Some(task_id.clone()), None),
        Event::Observation { task_id, .. } => ("observation", Some(task_id.clone()), None),
        Event::ProviderState { .. } => ("provider_state", None, None),
        Event::ArtifactAvailable {
            artifact_id,
            task_id,
            ..
        } => (
            "artifact_available",
            task_id.clone(),
            Some(artifact_id.clone()),
        ),
        Event::ProviderOutput {
            task_id,
            artifact_id,
        } => (
            "provider_output",
            Some(task_id.clone()),
            Some(artifact_id.clone()),
        ),
        Event::CompletionEvaluated { task_id, .. } => {
            ("completion_evaluated", Some(task_id.clone()), None)
        }
        Event::Overload { .. } => ("overload", None, None),
    };
    SourceEventReference {
        event_id: envelope.event_id.to_string(),
        sequence: envelope.sequence,
        generation: envelope.generation,
        observed_unix_ms: envelope.observed_unix_ms,
        event_kind: event_kind.to_owned(),
        task_id,
        artifact_id,
    }
}

fn response_name(response: &Response) -> &'static str {
    match response {
        Response::Hello(_) => "hello",
        Response::Accepted { .. } => "accepted",
        Response::ExerciseCreated { .. } => "exercise_created",
        Response::OperationRunCreated { .. } => "operation_run_created",
        Response::OperatorSessionCreated { .. } => "operator_session_created",
        Response::PlaybookCreated { .. } => "playbook_created",
        Response::EvidenceRecorded { .. } => "evidence_recorded",
        Response::FindingCreated { .. } => "finding_created",
        Response::FindingStatusSet { .. } => "finding_status_set",
        Response::TeamPresenceSet { .. } => "team_presence_set",
        Response::TeamWorkItemCreated { .. } => "team_work_item_created",
        Response::TeamWorkItemUpdated { .. } => "team_work_item_updated",
        Response::TeamMessagePosted { .. } => "team_message_posted",
        Response::TeamResourceClaimed { .. } => "team_resource_claimed",
        Response::TeamResourceReleased { .. } => "team_resource_released",
        Response::ProviderInvoked { .. } => "provider_invoked",
        Response::PlanAccepted { .. } => "plan_accepted",
        Response::DispatchAccepted { .. } => "dispatch_accepted",
        Response::ProviderRegistered { .. } => "provider_registered",
        Response::ArtifactStored { .. } => "artifact_stored",
        Response::ArtifactUploadStarted { .. } => "artifact_upload_started",
        Response::ArtifactUploadState { .. } => "artifact_upload_state",
        Response::ArtifactUploadProgress { .. } => "artifact_upload_progress",
        Response::ArtifactUploadAborted { .. } => "artifact_upload_aborted",
        Response::ArtifactChunk { .. } => "artifact_chunk",
        Response::Projection(_) => "projection",
        Response::Events(_) => "events",
        Response::Ack => "ack",
    }
}

pub struct ControlPlaneClient {
    stream: Mutex<UnixStream>,
    maximum_frame_bytes: usize,
    team: Option<TeamClient>,
    poisoned: AtomicBool,
}

impl ControlPlaneClient {
    pub async fn connect(
        socket_path: impl AsRef<Path>,
        identity: ClientIdentity,
    ) -> Result<Self, ClientError> {
        if let Some(team) = &identity.team {
            team.validate()?;
        }
        let stream = UnixStream::connect(socket_path).await?;
        let team = identity.team.clone();
        let client = Self {
            stream: Mutex::new(stream),
            maximum_frame_bytes: MAX_CONTROL_FRAME_BYTES,
            team,
            poisoned: AtomicBool::new(false),
        };
        let response = client
            .send(
                Command::Hello(Hello {
                    protocol_version: PROTOCOL_VERSION,
                    client_name: identity.client_name,
                    client_version: identity.client_version,
                    team: identity.team,
                }),
                Duration::from_secs(5),
            )
            .await?;
        match response {
            Response::Hello(ack)
                if ack.protocol_version == PROTOCOL_VERSION && ack.server_name == "grokd" =>
            {
                Ok(client)
            }
            _ => Err(ClientError::InvalidHandshake),
        }
    }

    pub async fn send(
        &self,
        mut command: Command,
        timeout: Duration,
    ) -> Result<Response, ClientError> {
        if let Command::SubmitIngress(ingress) = &mut command
            && ingress.team.is_none()
        {
            ingress.team.clone_from(&self.team);
        }
        let envelope = CommandEnvelope {
            protocol_version: PROTOCOL_VERSION,
            command_id: CommandId::new(),
            causation_id: None,
            deadline_unix_ms: now_unix_ms()
                .saturating_add(u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX)),
            command,
        };
        let response = match tokio::time::timeout(timeout, self.send_envelope(envelope)).await {
            Ok(response) => response?,
            Err(_) => {
                self.poisoned.store(true, Ordering::Release);
                return Err(ClientError::Timeout(timeout.as_millis()));
            }
        };
        response.response.map_err(ClientError::Protocol)
    }

    pub async fn read_events(&self, request: EventReadRequest) -> Result<EventBatch, ClientError> {
        request.validate()?;
        let timeout = Duration::from_millis(u64::from(request.wait_ms) + 5_000);
        match self.send(Command::ReadEvents(request), timeout).await? {
            Response::Events(batch) => Ok(batch),
            Response::Hello(_) => Err(ClientError::ExpectedEvents("hello")),
            Response::Accepted { .. } => Err(ClientError::ExpectedEvents("accepted")),
            Response::ExerciseCreated { .. } => {
                Err(ClientError::ExpectedEvents("exercise_created"))
            }
            Response::OperationRunCreated { .. } => {
                Err(ClientError::ExpectedEvents("operation_run_created"))
            }
            Response::OperatorSessionCreated { .. } => {
                Err(ClientError::ExpectedEvents("operator_session_created"))
            }
            Response::PlaybookCreated { .. } => {
                Err(ClientError::ExpectedEvents("playbook_created"))
            }
            Response::EvidenceRecorded { .. } => {
                Err(ClientError::ExpectedEvents("evidence_recorded"))
            }
            Response::FindingCreated { .. } => Err(ClientError::ExpectedEvents("finding_created")),
            Response::FindingStatusSet { .. } => {
                Err(ClientError::ExpectedEvents("finding_status_set"))
            }
            Response::TeamPresenceSet { .. } => {
                Err(ClientError::ExpectedEvents("team_presence_set"))
            }
            Response::TeamWorkItemCreated { .. } => {
                Err(ClientError::ExpectedEvents("team_work_item_created"))
            }
            Response::TeamWorkItemUpdated { .. } => {
                Err(ClientError::ExpectedEvents("team_work_item_updated"))
            }
            Response::TeamMessagePosted { .. } => {
                Err(ClientError::ExpectedEvents("team_message_posted"))
            }
            Response::TeamResourceClaimed { .. } => {
                Err(ClientError::ExpectedEvents("team_resource_claimed"))
            }
            Response::TeamResourceReleased { .. } => {
                Err(ClientError::ExpectedEvents("team_resource_released"))
            }
            Response::ProviderInvoked { .. } => {
                Err(ClientError::ExpectedEvents("provider_invoked"))
            }
            Response::PlanAccepted { .. } => Err(ClientError::ExpectedEvents("plan_accepted")),
            Response::DispatchAccepted { .. } => {
                Err(ClientError::ExpectedEvents("dispatch_accepted"))
            }
            Response::ProviderRegistered { .. } => {
                Err(ClientError::ExpectedEvents("provider_registered"))
            }
            Response::ArtifactStored { .. } => Err(ClientError::ExpectedEvents("artifact_stored")),
            Response::ArtifactUploadStarted { .. } => {
                Err(ClientError::ExpectedEvents("artifact_upload_started"))
            }
            Response::ArtifactUploadState { .. } => {
                Err(ClientError::ExpectedEvents("artifact_upload_state"))
            }
            Response::ArtifactUploadProgress { .. } => {
                Err(ClientError::ExpectedEvents("artifact_upload_progress"))
            }
            Response::ArtifactUploadAborted { .. } => {
                Err(ClientError::ExpectedEvents("artifact_upload_aborted"))
            }
            Response::ArtifactChunk { .. } => Err(ClientError::ExpectedEvents("artifact_chunk")),
            Response::Projection(_) => Err(ClientError::ExpectedEvents("projection")),
            Response::Ack => Err(ClientError::ExpectedEvents("ack")),
        }
    }

    pub async fn send_envelope(
        &self,
        envelope: CommandEnvelope,
    ) -> Result<ResponseEnvelope, ClientError> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(ClientError::ConnectionPoisoned);
        }
        if envelope.protocol_version != PROTOCOL_VERSION {
            return Err(ClientError::Protocol(ProtocolError::new(
                ProtocolErrorCode::IncompatibleVersion,
                "client envelope protocol version is incompatible",
            )));
        }
        let mut stream = self.stream.lock().await;
        let command_id = envelope.command_id.clone();
        let response = async {
            write_frame(&mut stream, &envelope, self.maximum_frame_bytes).await?;
            read_frame(&mut stream, self.maximum_frame_bytes).await
        }
        .await;
        let response: ResponseEnvelope = match response {
            Ok(response) => response,
            Err(error) => {
                self.poisoned.store(true, Ordering::Release);
                return Err(error);
            }
        };
        if response.command_id != command_id {
            self.poisoned.store(true, Ordering::Release);
            return Err(ClientError::MismatchedResponse);
        }
        Ok(response)
    }
}

async fn read_frame<T: serde::de::DeserializeOwned>(
    stream: &mut UnixStream,
    maximum_frame_bytes: usize,
) -> Result<T, ClientError> {
    let length = stream.read_u32().await? as usize;
    if length > maximum_frame_bytes {
        return Err(ClientError::FrameTooLarge {
            actual: length,
            maximum: maximum_frame_bytes,
        });
    }
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes).await?;
    Ok(rmp_serde::from_slice(&bytes)?)
}

async fn write_frame<T: serde::Serialize>(
    stream: &mut UnixStream,
    value: &T,
    maximum_frame_bytes: usize,
) -> Result<(), ClientError> {
    let bytes = rmp_serde::to_vec_named(value)?;
    if bytes.len() > maximum_frame_bytes {
        return Err(ClientError::FrameTooLarge {
            actual: bytes.len(),
            maximum: maximum_frame_bytes,
        });
    }
    stream.write_u32(bytes.len() as u32).await?;
    stream.write_all(&bytes).await?;
    stream.flush().await?;
    Ok(())
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mismatched_response_id_is_rejected() {
        let (client_stream, mut server_stream) = UnixStream::pair().unwrap();
        let client = ControlPlaneClient {
            stream: Mutex::new(client_stream),
            maximum_frame_bytes: MAX_CONTROL_FRAME_BYTES,
            team: None,
            poisoned: AtomicBool::new(false),
        };
        let server = tokio::spawn(async move {
            let _: CommandEnvelope = read_frame(&mut server_stream, MAX_CONTROL_FRAME_BYTES)
                .await
                .unwrap();
            write_frame(
                &mut server_stream,
                &ResponseEnvelope {
                    protocol_version: PROTOCOL_VERSION,
                    command_id: CommandId::new(),
                    response: Ok(Response::Ack),
                },
                MAX_CONTROL_FRAME_BYTES,
            )
            .await
            .unwrap();
        });
        assert!(matches!(
            client.send(Command::Shutdown, Duration::from_secs(1)).await,
            Err(ClientError::MismatchedResponse)
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn timeout_poisons_the_stream_until_reconnect() {
        let (client_stream, _server_stream) = UnixStream::pair().unwrap();
        let client = ControlPlaneClient {
            stream: Mutex::new(client_stream),
            maximum_frame_bytes: MAX_CONTROL_FRAME_BYTES,
            team: None,
            poisoned: AtomicBool::new(false),
        };
        assert!(matches!(
            client
                .send(Command::Shutdown, Duration::from_millis(1))
                .await,
            Err(ClientError::Timeout(_))
        ));
        assert!(matches!(
            client.send(Command::Shutdown, Duration::from_secs(1)).await,
            Err(ClientError::ConnectionPoisoned)
        ));
    }
}
