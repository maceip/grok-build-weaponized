//! Direct LiteRT-LM backend used by the supervised local-runtime worker.
//!
//! A model entry selects this transport with a `litert-lm://` base URL:
//!
//! ```text
//! litert-lm:///absolute/model.litertlm?library=/absolute/liblitert_lm_c.dylib&backend=cpu
//! ```
//!
//! The C ABI is loaded dynamically so the normal HTTP build has no link-time
//! dependency on LiteRT-LM. Engines are cached by configuration and guarded by
//! a single permit because the C API does not currently document concurrent
//! conversation safety. Conversation construction and model callbacks never
//! block Tokio's worker threads.

use std::collections::HashMap;
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::path::PathBuf;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver as SyncReceiver, SyncSender, sync_channel};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::{Duration, Instant};

use libloading::Library;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{Mutex as AsyncMutex, OnceCell, OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;
use url::Url;
use xai_grok_sampling_types::{
    AssistantItem, ContentPart, ConversationItem, ConversationRequest, ConversationResponse,
    SamplingError, StopReason, TokenUsage, ToolCall,
};

use crate::adapter::AdapterDescriptor;
use crate::events::{RuntimeChannel, RuntimeEvent};
use crate::metrics::InferenceLatencyStats;

const SCHEME: &str = "litert-lm";
/// Leave enough time for LiteRT-LM to unwind its callback thread while still
/// meeting the runtime's two-second native cancellation SLO.
const CANCEL_DRAIN_TIMEOUT: Duration = Duration::from_millis(1_500);
const CALLBACK_BUFFER_CAPACITY: usize = 256;
const SESSION_CACHE_CAPACITY: usize = 32;
const DEFAULT_MAX_RESIDENT_ADAPTERS: u32 = 8;
const DEFAULT_MAX_RESIDENT_SESSIONS: u32 = 8;
const DEFAULT_MAX_RESIDENT_CONTEXT_TOKENS: u32 = 32_768;
const DEFAULT_MIN_RECENT_TURNS: u32 = 4;
const BRIDGE_ABI_VERSION: u32 = 6;
const BRIDGE_CAP_STREAMING: u64 = 1 << 0;
const BRIDGE_CAP_TOKENIZE: u64 = 1 << 1;
const BRIDGE_CAP_LORA: u64 = 1 << 2;
const BRIDGE_CAP_ERROR_DETAIL: u64 = 1 << 3;
const BRIDGE_CAP_EXACT_PROMPT_MEASUREMENT: u64 = 1 << 4;
const BRIDGE_CAP_ADAPTER_UNLOAD: u64 = 1 << 5;
const BRIDGE_CAP_SUPPORTED_LORA_RANKS: u64 = 1 << 6;
const BRIDGE_CAP_JSON_SCHEMA: u64 = 1 << 7;
const REQUIRED_BRIDGE_CAPABILITIES: u64 = BRIDGE_CAP_STREAMING
    | BRIDGE_CAP_TOKENIZE
    | BRIDGE_CAP_LORA
    | BRIDGE_CAP_ERROR_DETAIL
    | BRIDGE_CAP_EXACT_PROMPT_MEASUREMENT
    | BRIDGE_CAP_ADAPTER_UNLOAD
    | BRIDGE_CAP_SUPPORTED_LORA_RANKS
    | BRIDGE_CAP_JSON_SCHEMA;

// These files are included into one private backend namespace so native
// handles and callbacks remain encapsulated while each lifecycle concern is
// independently reviewable. Public ownership stays with RuntimeManager.
include!("litert_lm/config.rs");
include!("litert_lm/abi.rs");
include!("litert_lm/engine.rs");
include!("litert_lm/session.rs");
include!("litert_lm/context.rs");
include!("litert_lm/streaming.rs");
include!("litert_lm/tests.rs");
