const SAMPLER_TOP_P: c_int = 2;

#[repr(C)]
struct LiteRtLmSamplerParams {
    sampler_type: c_int,
    top_k: c_int,
    top_p: f32,
    temperature: f32,
    seed: c_int,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct LoraAdapterConfig {
    pub path: PathBuf,
    /// Immutable adapter identity. Change this whenever the file contents
    /// change so resident weights and KV state can never be reused across
    /// adapter revisions.
    pub id: String,
}

impl LoraAdapterConfig {
    fn runtime_identity(&self) -> String {
        let path = self.path.to_string_lossy();
        format!("{}:{}{}:{}", self.id.len(), self.id, path.len(), path)
    }
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum ContextOverflowStrategy {
    #[default]
    Strict,
    Sliding,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct LiteRtLmConfig {
    pub model_path: PathBuf,
    pub library_path: PathBuf,
    pub backend: String,
    pub max_context_tokens: Option<u32>,
    /// LoRA ranks declared by the compiled GPU model artifact.
    pub supported_lora_ranks: Vec<u32>,
    /// Validated immutable adapter metadata. Requests bind this descriptor
    /// through `AdapterManager`; it is deliberately excluded from the base
    /// engine cache key.
    #[serde(default)]
    pub adapter_descriptor: Option<AdapterDescriptor>,
    pub lora_adapter: Option<LoraAdapterConfig>,
    pub max_resident_adapters: u32,
    pub max_resident_sessions: u32,
    pub max_resident_context_tokens: u32,
    pub context_strategy: ContextOverflowStrategy,
    pub min_recent_turns: u32,
}

impl LiteRtLmConfig {
    /// Parse a LiteRT-LM model URL. Returns `Ok(None)` for non-local schemes.
    pub fn from_base_url(base_url: &str) -> Result<Option<Self>, String> {
        let url = Url::parse(base_url).map_err(|error| error.to_string())?;
        if url.scheme() != SCHEME {
            return Ok(None);
        }
        if url.host_str().is_some_and(|host| !host.is_empty()) {
            return Err(
                "litert-lm URL must use an absolute local path (litert-lm:///...)".to_owned(),
            );
        }

        let decoded_path = percent_encoding::percent_decode_str(url.path())
            .decode_utf8()
            .map_err(|_| "litert-lm model path is not valid UTF-8".to_owned())?;
        let model_path = PathBuf::from(decoded_path.as_ref());
        if !model_path.is_absolute() {
            return Err("litert-lm URL must contain an absolute model path".to_owned());
        }
        let query: HashMap<_, _> = url.query_pairs().into_owned().collect();
        let library_path = query
            .get("library")
            .cloned()
            .or_else(|| std::env::var("LITERT_LM_LIBRARY").ok())
            .filter(|value| !value.trim().is_empty())
            .map(PathBuf::from)
            .ok_or_else(|| {
                "LiteRT-LM C library is required: set the `library` query parameter or \
                 LITERT_LM_LIBRARY"
                    .to_owned()
            })?;
        let backend = query
            .get("backend")
            .cloned()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| {
                if cfg!(target_os = "macos") {
                    "auto".to_owned()
                } else {
                    "cpu".to_owned()
                }
            });
        if !matches!(
            backend.as_str(),
            "auto" | "cpu" | "gpu" | "gpu_artisan"
        ) {
            return Err(format!(
                "invalid LiteRT-LM backend `{backend}`; expected auto, cpu, gpu, or gpu_artisan"
            ));
        }
        if query.contains_key("threads") {
            return Err(
                "LiteRT-LM's public C ABI does not expose thread-count configuration; \
                 remove the `threads` query parameter"
                    .to_owned(),
            );
        }
        let max_context_tokens = parse_optional_u32(&query, "max_context_tokens")?;
        let supported_lora_ranks = query
            .get("supported_lora_ranks")
            .map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(|value| {
                        value
                            .parse::<u32>()
                            .map_err(|_| format!("invalid `supported_lora_ranks` entry `{value}`"))
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?
            .unwrap_or_default();
        if !supported_lora_ranks.is_empty()
            && !matches!(backend.as_str(), "auto" | "gpu" | "gpu_artisan")
        {
            return Err(
                "`supported_lora_ranks` requires `backend=auto`, `backend=gpu`, or \
                 `backend=gpu_artisan`"
                    .to_owned(),
            );
        }
        let adapter_descriptor = query
            .get("adapter_manifest")
            .map(|path| {
                let path = PathBuf::from(path);
                if !path.is_absolute() {
                    return Err("`adapter_manifest` must be an absolute local path".to_owned());
                }
                let payload = std::fs::read(&path).map_err(|error| {
                    format!(
                        "failed to read adapter manifest {}: {error}",
                        path.display()
                    )
                })?;
                serde_json::from_slice::<AdapterDescriptor>(&payload).map_err(|error| {
                    format!("invalid adapter manifest {}: {error}", path.display())
                })
            })
            .transpose()?;
        if adapter_descriptor.is_some()
            && (query.contains_key("lora") || query.contains_key("lora_id"))
        {
            return Err(
                "`adapter_manifest` cannot be combined with legacy `lora`/`lora_id` parameters"
                    .to_owned(),
            );
        }
        let lora_adapter = match (query.get("lora"), query.get("lora_id")) {
            (Some(path), Some(id)) => {
                let path = PathBuf::from(path);
                if !path.is_absolute() {
                    return Err("`lora` must be an absolute local path".to_owned());
                }
                if id.trim().is_empty() {
                    return Err("`lora_id` must not be empty".to_owned());
                }
                Some(LoraAdapterConfig {
                    path,
                    id: id.clone(),
                })
            }
            (Some(_), None) => {
                return Err(
                    "`lora_id` is required with `lora`; use an immutable adapter revision"
                        .to_owned(),
                );
            }
            (None, Some(_)) => return Err("`lora_id` requires a `lora` path".to_owned()),
            (None, None) => None,
        };
        let max_resident_adapters = parse_optional_u32(&query, "max_resident_adapters")?
            .unwrap_or(DEFAULT_MAX_RESIDENT_ADAPTERS);
        let max_resident_sessions = parse_optional_u32(&query, "max_resident_sessions")?
            .unwrap_or(DEFAULT_MAX_RESIDENT_SESSIONS);
        let max_resident_context_tokens =
            parse_optional_u32(&query, "max_resident_context_tokens")?
                .unwrap_or(DEFAULT_MAX_RESIDENT_CONTEXT_TOKENS);
        let context_strategy = match query.get("context_strategy").map(String::as_str) {
            None | Some("strict") => ContextOverflowStrategy::Strict,
            Some("sliding") => ContextOverflowStrategy::Sliding,
            Some(value) => {
                return Err(format!(
                    "invalid `context_strategy` value `{value}`; expected `strict` or `sliding`"
                ));
            }
        };
        let min_recent_turns =
            parse_optional_u32(&query, "min_recent_turns")?.unwrap_or(DEFAULT_MIN_RECENT_TURNS);
        for key in query.keys() {
            if !matches!(
                key.as_str(),
                "library"
                    | "backend"
                    | "max_context_tokens"
                    | "supported_lora_ranks"
                    | "adapter_manifest"
                    | "lora"
                    | "lora_id"
                    | "max_resident_adapters"
                    | "max_resident_sessions"
                    | "max_resident_context_tokens"
                    | "context_strategy"
                    | "min_recent_turns"
            ) {
                return Err(format!("unknown LiteRT-LM URL query parameter `{key}`"));
            }
        }

        Ok(Some(Self {
            model_path,
            library_path,
            backend,
            max_context_tokens,
            supported_lora_ranks,
            adapter_descriptor,
            lora_adapter,
            max_resident_adapters,
            max_resident_sessions,
            max_resident_context_tokens,
            context_strategy,
            min_recent_turns,
        }))
    }

    pub fn runtime_key(&self) -> String {
        format!(
            "{}\0{}\0{}\0{:?}\0{:?}\0{}\0{}\0{}\0{:?}\0{}",
            self.library_path.display(),
            self.model_path.display(),
            self.backend,
            self.max_context_tokens,
            self.supported_lora_ranks,
            self.max_resident_adapters,
            self.max_resident_sessions,
            self.max_resident_context_tokens,
            self.context_strategy,
            self.min_recent_turns,
        )
    }
}

fn parse_optional_u32(query: &HashMap<String, String>, key: &str) -> Result<Option<u32>, String> {
    query
        .get(key)
        .map(|value| {
            value
                .parse::<u32>()
                .map_err(|_| format!("invalid `{key}` value `{value}`"))
                .and_then(|value| {
                    (value > 0)
                        .then_some(value)
                        .ok_or_else(|| format!("`{key}` must be greater than zero"))
                })
        })
        .transpose()
}
