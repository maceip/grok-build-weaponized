//! Read-only local runtime telemetry for daemon-supervised agent processes.

use agent_client_protocol as acp;

use super::{ExtResult, to_raw_response};
use crate::agent::MvpAgent;

pub async fn handle(agent: &MvpAgent, args: &acp::ExtRequest) -> ExtResult {
    if args.method.as_ref() != "x.ai/runtime/status" {
        return Err(acp::Error::method_not_found());
    }
    let runtime = xai_grok_sampler::litert_lm::local_runtime_status()
        .await
        .map_err(|error| acp::Error::internal_error().data(error.to_string()))?;
    to_raw_response(&serde_json::json!({
        "runtime": runtime,
        "agent_sessions": agent.sessions.borrow().len(),
        "memory_embedding_backfill": xai_grok_memory::embedding_backfill_status(),
    }))
}
