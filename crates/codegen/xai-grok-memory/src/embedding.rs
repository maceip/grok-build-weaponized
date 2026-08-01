//! Embedding provider abstraction for memory vector search.
//!
//! Defines the `EmbeddingProvider` trait and an API-based implementation
//! that calls an OpenAI-compatible embeddings API endpoint.
//!
//! Embeddings are cached in the sqlite-vec `chunks_vec` table — the vec0
//! virtual table IS the cache. No separate cache needed.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use async_trait::async_trait;
use fastembed::{EmbeddingModel, TextEmbedding, TextInitOptions};
use xai_grok_runtime::{ResourceClass, ResourceGovernor, ResourceLease};

/// Maximum retry attempts for transient API errors (429, 5xx).
const MAX_RETRIES: usize = 3;
/// Initial backoff delay in milliseconds (doubles on each retry: 1s, 2s, 4s).
const INITIAL_BACKOFF_MS: u64 = 1000;

/// Trait for generating text embeddings.
///
/// Implementations must be `Send + Sync` so they can be used in `Send`
/// futures (e.g., inside `tokio::spawn`). The `embed_batch` method is
/// async to support API-based providers.
#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    /// Embed a batch of texts, returning one vector per input text.
    async fn embed_batch(
        &self,
        texts: &[&str],
    ) -> Result<Vec<Vec<f32>>, Box<dyn std::error::Error + Send + Sync>>;

    /// Embed retrieval queries. Providers with asymmetric query/document
    /// prompts override this; symmetric providers use `embed_batch`.
    async fn embed_query(
        &self,
        query: &str,
    ) -> Result<Vec<f32>, Box<dyn std::error::Error + Send + Sync>> {
        self.embed_batch(&[query])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| "embedding provider returned no query vector".into())
    }

    /// The model name used for embeddings.
    fn model_name(&self) -> &str;

    /// The dimensionality of the embedding vectors.
    fn dimensions(&self) -> usize;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum LocalEmbeddingProfile {
    EmbeddingGemma,
    Qwen3,
}

impl LocalEmbeddingProfile {
    fn from_model(model: &str) -> Option<Self> {
        let model = model.to_ascii_lowercase();
        if model.contains("embeddinggemma") || model.contains("embedding-gemma") {
            Some(Self::EmbeddingGemma)
        } else if model.contains("qwen3") {
            Some(Self::Qwen3)
        } else {
            None
        }
    }

    fn model_name(self) -> &'static str {
        match self {
            Self::EmbeddingGemma => "embeddinggemma-300m-q4",
            Self::Qwen3 => "qwen3-embedding-0.6b-metal",
        }
    }

    fn maximum_dimensions(self) -> usize {
        match self {
            Self::EmbeddingGemma => 768,
            Self::Qwen3 => 1024,
        }
    }

    fn estimated_resident_bytes(self) -> u64 {
        const MIB: u64 = 1024 * 1024;
        match self {
            // Includes quantized weights, tokenizer state, and ORT workspaces.
            Self::EmbeddingGemma => 512 * MIB,
            // F16 weights plus Metal execution workspaces.
            Self::Qwen3 => 1536 * MIB,
        }
    }
}

enum LocalModel {
    EmbeddingGemma(TextEmbedding),
    #[cfg(feature = "high-recall-embeddings")]
    Qwen3(fastembed::Qwen3TextEmbedding),
}

impl LocalModel {
    fn embed(&mut self, texts: &[String]) -> Result<Vec<Vec<f32>>, String> {
        match self {
            Self::EmbeddingGemma(model) => model
                .embed(texts, Some(32))
                .map_err(|error| error.to_string()),
            #[cfg(feature = "high-recall-embeddings")]
            Self::Qwen3(model) => {
                let texts = texts.iter().map(String::as_str).collect::<Vec<_>>();
                model.embed(&texts).map_err(|error| error.to_string())
            }
        }
    }
}

enum LocalModelState {
    Loading,
    Ready {
        model: LocalModel,
        _lease: ResourceLease,
    },
    Failed(String),
}

struct LocalEmbeddingInner {
    profile: LocalEmbeddingProfile,
    dimensions: usize,
    state: Mutex<LocalModelState>,
    busy: AtomicBool,
}

/// Process-shared, non-blocking local embedding provider.
///
/// Model loading happens once in the background. Interactive retrieval never
/// waits for a load or another embedding batch: `Loading` and `Busy` are
/// returned immediately so the caller can continue with FTS5.
#[derive(Clone)]
pub struct LocalEmbeddingProvider {
    inner: Arc<LocalEmbeddingInner>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalEmbeddingStatus {
    Loading,
    Ready,
    Failed(String),
}

impl LocalEmbeddingProvider {
    pub fn from_config(config: &xai_grok_config_types::MemoryEmbeddingConfig) -> Option<Self> {
        let model = config.model.as_deref()?;
        let profile = LocalEmbeddingProfile::from_model(model)?;
        let dimensions = config.dimensions.min(profile.maximum_dimensions()).max(1);
        let key = (profile, dimensions);
        static REGISTRY: OnceLock<
            Mutex<HashMap<(LocalEmbeddingProfile, usize), Weak<LocalEmbeddingInner>>>,
        > = OnceLock::new();
        let registry = REGISTRY.get_or_init(|| Mutex::new(HashMap::new()));
        let mut registry = registry
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(inner) = registry.get(&key).and_then(Weak::upgrade) {
            return Some(Self { inner });
        }
        let inner = Arc::new(LocalEmbeddingInner {
            profile,
            dimensions,
            state: Mutex::new(LocalModelState::Loading),
            busy: AtomicBool::new(false),
        });
        let weak = Arc::downgrade(&inner);
        ResourceGovernor::global().register_reclaimer(
            ResourceClass::Embedding,
            Arc::new(move || {
                let Some(inner) = weak.upgrade() else {
                    return 0;
                };
                if inner.busy.load(Ordering::Acquire) {
                    return 0;
                }
                let mut state = inner
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let previous = std::mem::replace(
                    &mut *state,
                    LocalModelState::Failed(
                        "local embedding model was evicted under memory pressure".to_string(),
                    ),
                );
                match previous {
                    LocalModelState::Ready { _lease: lease, .. } => {
                        let bytes = lease.bytes();
                        drop(lease);
                        bytes
                    }
                    other => {
                        *state = other;
                        0
                    }
                }
            }),
        );
        registry.insert(key, Arc::downgrade(&inner));
        drop(registry);
        start_local_model_load(Arc::clone(&inner));
        Some(Self { inner })
    }

    pub fn is_ready(&self) -> bool {
        matches!(
            *self
                .inner
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            LocalModelState::Ready { .. }
        )
    }

    pub fn status(&self) -> LocalEmbeddingStatus {
        match &*self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
        {
            LocalModelState::Loading => LocalEmbeddingStatus::Loading,
            LocalModelState::Ready { .. } => LocalEmbeddingStatus::Ready,
            LocalModelState::Failed(error) => LocalEmbeddingStatus::Failed(error.clone()),
        }
    }

    /// Wait for an explicitly requested maintenance operation to materialize
    /// the local model. Interactive retrieval intentionally never calls this:
    /// it falls back to FTS immediately while the model is loading or busy.
    pub async fn wait_ready(&self, timeout: Duration) -> Result<(), String> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            match self.status() {
                LocalEmbeddingStatus::Ready => return Ok(()),
                LocalEmbeddingStatus::Failed(error) => return Err(error),
                LocalEmbeddingStatus::Loading => {}
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!(
                    "local embedding model did not become ready within {} seconds",
                    timeout.as_secs()
                ));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn prepare(&self, texts: &[&str], query: bool) -> Vec<String> {
        texts
            .iter()
            .map(|text| match (self.inner.profile, query) {
                (LocalEmbeddingProfile::EmbeddingGemma, true) => {
                    format!("task: search result | query: {text}")
                }
                (LocalEmbeddingProfile::EmbeddingGemma, false) => {
                    format!("title: none | text: {text}")
                }
                (LocalEmbeddingProfile::Qwen3, true) => {
                    format!("Instruct: Retrieve relevant workspace evidence\nQuery: {text}")
                }
                (LocalEmbeddingProfile::Qwen3, false) => (*text).to_string(),
            })
            .collect()
    }

    async fn embed_local(
        &self,
        texts: &[&str],
        query: bool,
    ) -> Result<Vec<Vec<f32>>, Box<dyn std::error::Error + Send + Sync>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        if self
            .inner
            .busy
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err("local embedding model is busy".into());
        }
        let prepared = self.prepare(texts, query);
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            struct BusyReset(Arc<LocalEmbeddingInner>);
            impl Drop for BusyReset {
                fn drop(&mut self) {
                    self.0.busy.store(false, Ordering::Release);
                }
            }
            let _reset = BusyReset(Arc::clone(&inner));
            let mut state = inner
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let model = match &mut *state {
                LocalModelState::Loading => {
                    return Err("local embedding model is still loading".to_string());
                }
                LocalModelState::Failed(error) => return Err(error.clone()),
                LocalModelState::Ready { model, .. } => model,
            };
            let mut embeddings = model.embed(&prepared)?;
            for embedding in &mut embeddings {
                truncate_and_normalize(embedding, inner.dimensions)?;
            }
            Ok(embeddings)
        })
        .await
        .map_err(|error| format!("local embedding task failed: {error}"))?
        .map_err(Into::into)
    }
}

/// Start the configured local embedding model load without waiting for model
/// materialization. Returns `false` for non-local/off profiles.
pub fn prewarm_local(config: &xai_grok_config_types::MemoryEmbeddingConfig) -> bool {
    LocalEmbeddingProvider::from_config(config).is_some()
}

fn start_local_model_load(inner: Arc<LocalEmbeddingInner>) {
    let load = move || {
        let governor = ResourceGovernor::global();
        let result = if governor.snapshot().soft_pressure {
            Err("local embedding load deferred by memory pressure".to_string())
        } else {
            governor
                .reserve(
                    ResourceClass::Embedding,
                    inner.profile.estimated_resident_bytes(),
                )
                .map_err(|error| error.to_string())
                .and_then(|lease| {
                    load_local_model(inner.profile).map(|model| LocalModelState::Ready {
                        model,
                        _lease: lease,
                    })
                })
        };
        let mut state = inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *state = match result {
            Ok(ready) => ready,
            Err(error) => LocalModelState::Failed(error),
        };
    };
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        runtime.spawn_blocking(load);
    } else {
        std::thread::Builder::new()
            .name("grok-local-embedding-load".to_string())
            .spawn(load)
            .ok();
    }
}

fn load_local_model(profile: LocalEmbeddingProfile) -> Result<LocalModel, String> {
    match profile {
        LocalEmbeddingProfile::EmbeddingGemma => {
            let options = TextInitOptions::new(EmbeddingModel::EmbeddingGemma300MQ4)
                .with_show_download_progress(false)
                .with_max_length(2048);
            TextEmbedding::try_new(options)
                .map(LocalModel::EmbeddingGemma)
                .map_err(|error| error.to_string())
        }
        LocalEmbeddingProfile::Qwen3 => load_qwen3_model(),
    }
}

#[cfg(feature = "high-recall-embeddings")]
fn load_qwen3_model() -> Result<LocalModel, String> {
    let device = candle_core::Device::new_metal(0).map_err(|error| error.to_string())?;
    fastembed::Qwen3TextEmbedding::from_hf(
        "Qwen/Qwen3-Embedding-0.6B",
        &device,
        candle_core::DType::F16,
        32_768,
    )
    .map(LocalModel::Qwen3)
    .map_err(|error| error.to_string())
}

#[cfg(not(feature = "high-recall-embeddings"))]
fn load_qwen3_model() -> Result<LocalModel, String> {
    Err("Qwen3 local embeddings require the high-recall-embeddings build feature".to_string())
}

fn truncate_and_normalize(embedding: &mut Vec<f32>, dimensions: usize) -> Result<(), String> {
    if embedding.len() < dimensions {
        return Err(format!(
            "embedding dimension mismatch: model returned {}, configured {dimensions}",
            embedding.len()
        ));
    }
    embedding.truncate(dimensions);
    let norm = embedding
        .iter()
        .map(|value| value * value)
        .sum::<f32>()
        .sqrt();
    if !norm.is_finite() || norm <= f32::EPSILON {
        return Err("embedding model returned a non-normalizable vector".to_string());
    }
    for value in embedding {
        *value /= norm;
    }
    Ok(())
}

#[async_trait]
impl EmbeddingProvider for LocalEmbeddingProvider {
    async fn embed_batch(
        &self,
        texts: &[&str],
    ) -> Result<Vec<Vec<f32>>, Box<dyn std::error::Error + Send + Sync>> {
        self.embed_local(texts, false).await
    }

    async fn embed_query(
        &self,
        query: &str,
    ) -> Result<Vec<f32>, Box<dyn std::error::Error + Send + Sync>> {
        self.embed_local(&[query], true)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| "local embedding model returned no query vector".into())
    }

    fn model_name(&self) -> &str {
        self.inner.profile.model_name()
    }

    fn dimensions(&self) -> usize {
        self.inner.dimensions
    }
}

/// API-based embedding provider using an OpenAI-compatible embeddings endpoint.
pub struct ApiEmbeddingProvider {
    api_base: String,
    model: String,
    dimensions: usize,
    client: reqwest_middleware::ClientWithMiddleware,
    max_batch_size: usize,
}

impl ApiEmbeddingProvider {
    pub fn new(
        api_base: String,
        model: String,
        dimensions: usize,
        client: reqwest_middleware::ClientWithMiddleware,
    ) -> Self {
        Self {
            api_base,
            model,
            dimensions,
            client,
            max_batch_size: 32,
        }
    }

    pub fn from_config(
        config: &xai_grok_config_types::MemoryEmbeddingConfig,
        api_base: String,
        client: reqwest_middleware::ClientWithMiddleware,
    ) -> Option<Self> {
        let model = config.model.clone().filter(|m| !m.is_empty())?;
        Some(Self::new(api_base, model, config.dimensions, client))
    }

    pub fn from_session(
        config: &xai_grok_config_types::MemoryEmbeddingConfig,
        proxy_base_url: String,
        auth_key: String,
    ) -> Option<Self> {
        let client = build_static_middleware_client(Some(auth_key));
        Self::from_config(config, proxy_base_url, client)
    }
}

pub(super) fn build_middleware_client(
    credentials: std::sync::Arc<dyn xai_grok_auth::AuthCredentialProvider>,
) -> reqwest_middleware::ClientWithMiddleware {
    xai_grok_http::with_auth_retry(xai_grok_http::shared_client(), credentials)
}

fn build_static_middleware_client(
    api_key: Option<String>,
) -> reqwest_middleware::ClientWithMiddleware {
    let provider: std::sync::Arc<dyn xai_grok_auth::AuthCredentialProvider> = std::sync::Arc::new(
        xai_grok_auth::StaticAuthCredentialProvider::new(Box::new(NoopHttpAuth), api_key),
    );
    build_middleware_client(provider)
}

struct NoopHttpAuth;

impl xai_grok_auth::HttpAuth for NoopHttpAuth {
    fn apply(&self, builder: reqwest::RequestBuilder, _base_url: &str) -> reqwest::RequestBuilder {
        builder
    }
}

#[async_trait]
impl EmbeddingProvider for ApiEmbeddingProvider {
    #[tracing::instrument(name = "memory.embed_batch", skip_all, fields(batch_size = texts.len()))]
    async fn embed_batch(
        &self,
        texts: &[&str],
    ) -> Result<Vec<Vec<f32>>, Box<dyn std::error::Error + Send + Sync>> {
        if texts.is_empty() {
            return Ok(vec![]);
        }

        let mut all_embeddings = Vec::with_capacity(texts.len());

        // Process in batches to respect API payload limits
        for batch in texts.chunks(self.max_batch_size) {
            let input: Vec<&str> = batch.to_vec();
            let body_json = serde_json::json!({
                "model": self.model,
                "input": input,
                "dimensions": self.dimensions,
            });

            // Retry with exponential backoff on transient errors (429, 5xx)
            let mut last_err = String::new();
            let mut success = false;
            for attempt in 0..MAX_RETRIES {
                if attempt > 0 {
                    let delay = INITIAL_BACKOFF_MS * 2u64.pow(attempt as u32 - 1);
                    tracing::warn!(
                        attempt,
                        delay_ms = delay,
                        "retrying embedding API call after transient error"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                }

                let request = xai_grok_http::shared_client()
                    .post(format!("{}/embeddings", self.api_base))
                    .json(&body_json)
                    .header("X-XAI-Token-Auth", "xai-grok-cli")
                    .header("x-grok-client-version", xai_grok_version::VERSION);

                let req = match request.build() {
                    Ok(r) => r,
                    Err(e) => {
                        return Err(format!("failed to build embedding request: {e}").into());
                    }
                };
                let response = match self.client.execute(req).await {
                    Ok(r) => r,
                    Err(e) => {
                        last_err = format!("request failed: {e}");
                        continue;
                    }
                };

                let status = response.status();
                if status.is_success() {
                    let body: serde_json::Value = response.json().await?;
                    let data = body
                        .get("data")
                        .and_then(|d| d.as_array())
                        .ok_or("embedding response missing 'data' array")?;

                    for item in data {
                        let embedding: Vec<f32> = item
                            .get("embedding")
                            .and_then(|e| e.as_array())
                            .ok_or("embedding item missing 'embedding' array")?
                            .iter()
                            .filter_map(|v| v.as_f64().map(|f| f as f32))
                            .collect();
                        all_embeddings.push(embedding);
                    }
                    success = true;
                    break;
                }

                // Retry on 429 (rate limit) or 5xx (server error)
                if status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
                    last_err = format!(
                        "HTTP {status}: {}",
                        response.text().await.unwrap_or_default()
                    );
                    continue;
                }

                // Non-retryable error (4xx other than 429)
                let body = response.text().await.unwrap_or_default();
                return Err(format!("embedding API error {status}: {body}").into());
            }

            if !success {
                return Err(format!(
                    "embedding API failed after {MAX_RETRIES} attempts: {last_err}"
                )
                .into());
            }
        }

        Ok(all_embeddings)
    }

    fn model_name(&self) -> &str {
        &self.model
    }

    fn dimensions(&self) -> usize {
        self.dimensions
    }
}

/// A mock embedding provider for testing that returns deterministic vectors.
/// Uses blake3 hash of text → float values for reproducible results.
#[cfg(any(test, feature = "test-support"))]
pub struct MockEmbeddingProvider {
    pub dimensions: usize,
}

#[cfg(any(test, feature = "test-support"))]
#[async_trait]
impl EmbeddingProvider for MockEmbeddingProvider {
    async fn embed_batch(
        &self,
        texts: &[&str],
    ) -> Result<Vec<Vec<f32>>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(texts
            .iter()
            .map(|text| {
                let hash = blake3::hash(text.as_bytes());
                let bytes = hash.as_bytes();
                (0..self.dimensions)
                    .map(|i| bytes[i % 32] as f32 / 255.0)
                    .collect()
            })
            .collect())
    }

    fn model_name(&self) -> &str {
        "mock-embedding"
    }

    fn dimensions(&self) -> usize {
        self.dimensions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_embedding_deterministic() {
        let provider = MockEmbeddingProvider { dimensions: 4 };
        let r1 = provider.embed_batch(&["hello"]).await.unwrap();
        let r2 = provider.embed_batch(&["hello"]).await.unwrap();
        assert_eq!(r1, r2);
    }

    #[tokio::test]
    async fn test_mock_embedding_different_texts() {
        let provider = MockEmbeddingProvider { dimensions: 4 };
        let results = provider.embed_batch(&["hello", "world"]).await.unwrap();
        assert_eq!(results.len(), 2);
        assert_ne!(results[0], results[1]);
    }

    #[tokio::test]
    async fn test_mock_embedding_empty_input() {
        let provider = MockEmbeddingProvider { dimensions: 4 };
        let results = provider.embed_batch(&[]).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_mock_embedding_correct_dimensions() {
        let provider = MockEmbeddingProvider { dimensions: 128 };
        let results = provider.embed_batch(&["test"]).await.unwrap();
        assert_eq!(results[0].len(), 128);
    }
}
