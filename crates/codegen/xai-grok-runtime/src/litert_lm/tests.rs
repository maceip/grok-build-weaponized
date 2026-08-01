#[cfg(test)]
mod tests {
    use super::*;
    use xai_grok_sampling_types::{SystemItem, ToolSpec, UserItem};

    #[test]
    fn parses_local_transport_url() {
        let config = LiteRtLmConfig::from_base_url(
            "litert-lm:///tmp/model.litertlm?library=%2Ftmp%2Fliblitert_lm_c.dylib&\
             backend=cpu&max_context_tokens=8192",
        )
        .unwrap()
        .unwrap();

        assert_eq!(config.model_path, PathBuf::from("/tmp/model.litertlm"));
        assert_eq!(
            config.library_path,
            PathBuf::from("/tmp/liblitert_lm_c.dylib")
        );
        assert_eq!(config.backend, "cpu");
        assert_eq!(config.max_context_tokens, Some(8192));
        assert_eq!(config.adapter_descriptor, None);
        assert_eq!(config.lora_adapter, None);
        assert_eq!(config.max_resident_adapters, DEFAULT_MAX_RESIDENT_ADAPTERS);
        assert_eq!(config.max_resident_sessions, DEFAULT_MAX_RESIDENT_SESSIONS);
        assert_eq!(
            config.max_resident_context_tokens,
            DEFAULT_MAX_RESIDENT_CONTEXT_TOKENS
        );
        assert_eq!(config.context_strategy, ContextOverflowStrategy::Strict);
        assert_eq!(config.min_recent_turns, DEFAULT_MIN_RECENT_TURNS);
    }

    #[test]
    fn zero_temperature_uses_portable_top_one_sampling() {
        let sampler = sampler_params(Some(0.0), Some(1.0)).unwrap().unwrap();
        assert_eq!(sampler.sampler_type, SAMPLER_TOP_P);
        assert_eq!(sampler.top_k, 1);
        assert_eq!(sampler.temperature, 1.0);
    }

    #[test]
    fn parses_lora_capable_local_backends() {
        let artisan = LiteRtLmConfig::from_base_url(
            "litert-lm:///tmp/model.litertlm?library=/tmp/lib.dylib&\
             backend=gpu_artisan&supported_lora_ranks=8,16",
        )
        .unwrap()
        .unwrap();
        assert_eq!(artisan.backend, "gpu_artisan");
        assert_eq!(artisan.supported_lora_ranks, vec![8, 16]);

        let generic_gpu = LiteRtLmConfig::from_base_url(
            "litert-lm:///tmp/model.litertlm?library=/tmp/lib.dylib&\
             backend=gpu&supported_lora_ranks=8",
        )
        .unwrap()
        .unwrap();
        assert_eq!(generic_gpu.backend, "gpu");

        let cpu = LiteRtLmConfig::from_base_url(
            "litert-lm:///tmp/model.litertlm?library=/tmp/lib.dylib&\
             backend=cpu&supported_lora_ranks=8",
        )
        .unwrap()
        .unwrap();
        assert_eq!(cpu.backend, "cpu");
        assert_eq!(cpu.supported_lora_ranks, vec![8]);
    }

    #[test]
    fn parses_versioned_lora_adapter_and_residency_limit() {
        let config = LiteRtLmConfig::from_base_url(
            "litert-lm:///tmp/model.litertlm?library=/tmp/lib.dylib&\
             lora=/tmp/adapters/code.lora&lora_id=code-v7&max_resident_adapters=3&\
             max_resident_sessions=2&max_resident_context_tokens=12000&\
             context_strategy=sliding&min_recent_turns=2",
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            config.lora_adapter,
            Some(LoraAdapterConfig {
                path: PathBuf::from("/tmp/adapters/code.lora"),
                id: "code-v7".to_owned(),
            })
        );
        assert_eq!(config.max_resident_adapters, 3);
        assert_eq!(config.max_resident_sessions, 2);
        assert_eq!(config.max_resident_context_tokens, 12_000);
        assert_eq!(config.context_strategy, ContextOverflowStrategy::Sliding);
        assert_eq!(config.min_recent_turns, 2);
        assert!(!config.runtime_key().contains("code-v7"));
    }

    #[test]
    fn rejects_unversioned_lora_adapter() {
        let error = LiteRtLmConfig::from_base_url(
            "litert-lm:///tmp/model.litertlm?library=/tmp/lib.dylib&\
             lora=/tmp/adapters/code.lora",
        )
        .unwrap_err();
        assert!(error.contains("lora_id"));
    }

    #[test]
    fn parses_descriptor_based_adapter_without_static_lora_bypass() {
        let temp = tempfile::tempdir().unwrap();
        let adapter_path = temp.path().join("adapter.bin");
        std::fs::write(&adapter_path, b"adapter").unwrap();
        let descriptor = AdapterDescriptor {
            adapter_id: "ops".to_string(),
            revision: "r1".to_string(),
            content_hash: crate::adapter::hash_artifact(&adapter_path).unwrap(),
            base_model_hash: "base-hash".to_string(),
            path: adapter_path,
            rank: 8,
            dtype: crate::adapter::AdapterDtype::F16,
            byte_size: 7,
            tensor_names: vec!["layer.lora_A".to_string(), "layer.lora_B".to_string()],
        };
        let manifest = temp.path().join("adapter.json");
        std::fs::write(&manifest, serde_json::to_vec(&descriptor).unwrap()).unwrap();
        let mut url = Url::parse("litert-lm:///tmp/model.litertlm").unwrap();
        url.query_pairs_mut()
            .append_pair("library", "/tmp/lib.dylib")
            .append_pair("backend", "gpu")
            .append_pair("supported_lora_ranks", "8")
            .append_pair("adapter_manifest", manifest.to_str().unwrap());
        let config = LiteRtLmConfig::from_base_url(url.as_str())
            .unwrap()
            .unwrap();
        assert_eq!(config.adapter_descriptor, Some(descriptor));
        assert_eq!(config.lora_adapter, None);
    }

    #[test]
    fn adapter_identity_separates_kv_cache_lineages() {
        let first = LoraAdapterConfig {
            path: PathBuf::from("/tmp/first.lora"),
            id: "adapter@v1".to_string(),
        };
        let second = LoraAdapterConfig {
            path: PathBuf::from("/tmp/second.lora"),
            id: "adapter@v2".to_string(),
        };
        let first_key = conversation_cache_key("engine", "session", Some(&first));
        let second_key = conversation_cache_key("engine", "session", Some(&second));
        let base_key = conversation_cache_key("engine", "session", None);
        assert_ne!(first_key, second_key);
        assert_ne!(first_key, base_key);
        assert!(first_key.ends_with("\0session"));
    }

    #[test]
    fn sliding_context_evicts_whole_turns_and_keeps_recent_turns() {
        let mut messages = vec![
            json!({"role": "user", "content": "old request"}),
            json!({"role": "assistant", "tool_calls": [{"id": "a"}]}),
            json!({"role": "tool", "tool_call_id": "a", "content": "old result"}),
            json!({"role": "assistant", "content": "old answer"}),
            json!({"role": "user", "content": "recent request"}),
            json!({"role": "assistant", "content": "recent answer"}),
        ];

        assert_eq!(evict_oldest_complete_turn(&mut messages, 0, 1), Some(4));
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0]["content"], "recent request");
        assert_eq!(evict_oldest_complete_turn(&mut messages, 0, 1), None);
    }

    #[test]
    fn rejects_parameters_missing_from_the_public_c_abi() {
        let error = LiteRtLmConfig::from_base_url(
            "litert-lm:///tmp/model.litertlm?library=/tmp/lib.dylib&threads=4",
        )
        .unwrap_err();
        assert!(error.contains("does not expose thread-count"));
    }

    #[test]
    fn non_local_url_does_not_select_transport() {
        assert!(
            LiteRtLmConfig::from_base_url("https://api.x.ai/v1")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn separates_reasoning_tags_across_stream_boundaries() {
        let mut filter = ReasoningTagFilter::default();
        let mut output = Vec::new();
        output.extend(filter.push("prefix<th"));
        output.extend(filter.push("ink>private"));
        output.extend(filter.push(" chain</thi"));
        output.extend(filter.push("nk>answer<"));
        output.extend(filter.finish());

        assert_eq!(
            output,
            vec![
                FilteredLocalText {
                    channel: LocalTextChannel::Text,
                    text: "prefix".to_owned(),
                },
                FilteredLocalText {
                    channel: LocalTextChannel::Reasoning,
                    text: "private".to_owned(),
                },
                FilteredLocalText {
                    channel: LocalTextChannel::Reasoning,
                    text: " chain".to_owned(),
                },
                FilteredLocalText {
                    channel: LocalTextChannel::Text,
                    text: "answer".to_owned(),
                },
                FilteredLocalText {
                    channel: LocalTextChannel::Text,
                    text: "<".to_owned(),
                },
            ]
        );
    }

    #[test]
    fn prepares_messages_tools_and_current_turn() {
        let request = ConversationRequest {
            items: vec![
                ConversationItem::System(SystemItem {
                    content: Arc::<str>::from("Be concise."),
                }),
                ConversationItem::User(UserItem {
                    content: vec![ContentPart::Text {
                        text: Arc::<str>::from("Hello"),
                    }],
                    ..Default::default()
                }),
            ],
            tools: vec![ToolSpec {
                name: "status".to_owned(),
                description: Some("Read status".to_owned()),
                parameters: json!({"type": "object"}),
            }],
            max_output_tokens: Some(128),
            ..Default::default()
        };

        let prepared = prepare_conversation(request).unwrap();

        assert_eq!(prepared.system_message.as_deref(), Some("Be concise."));
        assert_eq!(prepared.messages, "[]");
        assert!(prepared.current_message.contains("\"role\":\"user\""));
        let tools: Value = serde_json::from_str(&prepared.tools).unwrap();
        assert_eq!(tools[0]["type"], "function");
        assert_eq!(tools[0]["function"]["name"], "status");
        assert_eq!(tools[0]["function"]["parameters"]["type"], "object");
        assert_eq!(prepared.max_output_tokens, Some(128));
    }

    #[test]
    fn prepares_native_json_schema_constraint() {
        let schema = json!({
            "type": "object",
            "properties": {"answer": {"type": "string"}},
            "required": ["answer"],
            "additionalProperties": false
        });
        let request = ConversationRequest {
            items: vec![ConversationItem::User(UserItem {
                content: vec![ContentPart::Text {
                    text: Arc::<str>::from("Return the answer"),
                }],
                ..Default::default()
            })],
            json_schema: Some(schema.clone()),
            ..Default::default()
        };

        let prepared = prepare_conversation(request).unwrap();

        let serialized = prepared.json_schema.as_deref().expect("serialized schema");
        assert_eq!(serde_json::from_str::<Value>(serialized).unwrap(), schema);
        assert_eq!(prepared.compatibility().json_schema, prepared.json_schema);
    }

    #[test]
    fn executor_stage_compacts_generic_base_system_and_retains_typed_task() {
        let request = ConversationRequest {
            items: vec![
                ConversationItem::System(SystemItem {
                    content: Arc::<str>::from(
                        "You are Grok released by xAI.\n\n<large-generic-policy>unused</large-generic-policy>",
                    ),
                }),
                ConversationItem::System(SystemItem {
                    content: Arc::<str>::from(
                        "<cooperation_task>{\"objective\":\"inspect\"}</cooperation_task>",
                    ),
                }),
                ConversationItem::User(UserItem {
                    content: vec![ContentPart::Text {
                        text: Arc::<str>::from("Inspect the workspace"),
                    }],
                    ..Default::default()
                }),
            ],
            x_grok_req_id: Some("grok-stage-executor:test:1".to_string()),
            ..Default::default()
        };

        let prepared = prepare_conversation(request).unwrap();
        let system = prepared.system_message.unwrap();
        assert!(system.contains("local execution stage"));
        assert!(system.contains("<cooperation_task>"));
        assert!(!system.contains("large-generic-policy"));
    }

    #[test]
    fn local_tool_admission_keeps_at_most_six_complete_relevant_schemas() {
        let request = ConversationRequest {
            items: vec![ConversationItem::User(UserItem {
                content: vec![ContentPart::Text {
                    text: Arc::<str>::from("Use special_scanner now"),
                }],
                ..Default::default()
            })],
            tools: (0..8)
                .map(|index| ToolSpec {
                    name: if index == 7 {
                        "special_scanner".to_string()
                    } else {
                        format!("unrelated_{index}")
                    },
                    description: None,
                    parameters: json!({
                        "type": "object",
                        "properties": {"target": {"type": "string"}}
                    }),
                })
                .collect(),
            ..Default::default()
        };
        let prepared = prepare_conversation(request).unwrap();
        let tools: Vec<Value> = serde_json::from_str(&prepared.tools).unwrap();
        assert_eq!(tools.len(), 6);
        assert!(
            tools
                .iter()
                .any(|tool| tool["function"]["name"] == "special_scanner")
        );
    }

    #[test]
    fn omits_redundant_discovery_catalogs_for_local_context() {
        let request = ConversationRequest {
            items: vec![
                ConversationItem::System(SystemItem {
                    content: Arc::<str>::from("Be concise."),
                }),
                ConversationItem::User(UserItem {
                    content: vec![ContentPart::Text {
                        text: Arc::<str>::from(
                            "<system-reminder>\nThe following skills are available\n\
                             - enormous catalog",
                        ),
                    }],
                    ..Default::default()
                }),
                ConversationItem::User(UserItem {
                    content: vec![ContentPart::Text {
                        text: Arc::<str>::from("Actual operator request"),
                    }],
                    ..Default::default()
                }),
            ],
            ..Default::default()
        };

        let prepared = prepare_conversation(request).unwrap();
        assert_eq!(prepared.messages, "[]");
        assert!(prepared.current_message.contains("Actual operator request"));
        assert!(!prepared.current_message.contains("enormous catalog"));
    }

    #[test]
    fn compacts_tool_schema_prose_without_losing_constraints() {
        let request = ConversationRequest {
            items: vec![ConversationItem::User(UserItem {
                content: vec![ContentPart::Text {
                    text: Arc::<str>::from("Use the status tool"),
                }],
                ..Default::default()
            })],
            tools: vec![ToolSpec {
                name: "status".to_owned(),
                description: Some("Long model-facing prose".to_owned()),
                parameters: json!({
                    "type": "object",
                    "title": "Status request",
                    "description": "Long redundant description",
                    "properties": {
                        "state": {
                            "type": "string",
                            "description": "Desired state",
                            "enum": ["ready", "blocked"],
                            "default": "ready"
                        }
                    },
                    "required": ["state"]
                }),
            }],
            ..Default::default()
        };

        let prepared = prepare_conversation(request).unwrap();
        let tools: Value = serde_json::from_str(&prepared.tools).unwrap();
        let schema = &tools[0]["function"]["parameters"];
        assert_eq!(schema["type"], "object");
        assert_eq!(
            schema["properties"]["state"]["enum"],
            json!(["ready", "blocked"])
        );
        assert_eq!(schema["required"], json!(["state"]));
        assert!(schema.get("title").is_none());
        assert!(schema.get("description").is_none());
        assert_eq!(
            schema["properties"]["state"]["description"],
            "Desired state"
        );
        assert!(schema["properties"]["state"].get("default").is_none());
    }

    #[test]
    fn compact_schema_keeps_only_bounded_required_property_descriptions() {
        let mut schema = json!({
            "type": "object",
            "properties": {
                "required_value": {
                    "type": "string",
                    "description": "r".repeat(REQUIRED_PROPERTY_DESCRIPTION_MAX_CHARS + 30)
                },
                "optional_value": {
                    "type": "string",
                    "description": "Optional prose"
                }
            },
            "required": ["required_value"]
        });

        compact_json_schema(&mut schema);

        let description = schema["properties"]["required_value"]["description"]
            .as_str()
            .expect("required description");
        assert_eq!(
            description.chars().count(),
            REQUIRED_PROPERTY_DESCRIPTION_MAX_CHARS
        );
        assert!(description.ends_with('…'));
        assert!(
            schema["properties"]["optional_value"]
                .get("description")
                .is_none()
        );
    }

    #[test]
    fn retrieved_memory_is_separated_from_pinned_system_instructions() {
        let (system, memory) = partition_retrieved_memory(vec![format!(
            "Pinned instruction.\n\n<system-reminder>\n<memory-context>\n{}\n</memory-context>\n</system-reminder>",
            "workspace evidence"
        )]);
        assert_eq!(system.as_deref(), Some("Pinned instruction."));
        assert_eq!(
            memory.as_deref(),
            Some("<memory-context>\nworkspace evidence\n</memory-context>")
        );
    }

    #[test]
    fn parses_text_and_tool_call_chunks() {
        let text = parse_response_chunk(
            r#"{"role":"assistant","content":[{"type":"text","text":"hello"}]}"#,
        )
        .unwrap();
        assert_eq!(text.text, vec!["hello"]);

        let tool = parse_response_chunk(
            r#"{"role":"assistant","tool_calls":[{"type":"function","function":{"name":"status","arguments":{"verbose":true}}}]}"#,
        )
        .unwrap();
        assert_eq!(tool.tool_calls.len(), 1);
        assert_eq!(tool.tool_calls[0].name, "status");
        assert_eq!(tool.tool_calls[0].arguments.as_ref(), r#"{"verbose":true}"#);
    }

    #[tokio::test]
    #[ignore = "requires a local C compiler capable of producing a shared library"]
    async fn live_dynamic_c_abi_streams_without_http() {
        let temp = tempfile::tempdir().unwrap();
        let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../xai-grok-sampler/tests/fixtures/litert_lm_stub.c");
        #[cfg(target_os = "macos")]
        let library = temp.path().join("liblitert_lm_stub.dylib");
        #[cfg(not(target_os = "macos"))]
        let library = temp.path().join("liblitert_lm_stub.so");
        let mut compiler = std::process::Command::new("cc");
        #[cfg(target_os = "macos")]
        compiler.arg("-dynamiclib");
        #[cfg(not(target_os = "macos"))]
        compiler.args(["-shared", "-fPIC"]);
        let status = compiler
            .arg(&source)
            .arg("-o")
            .arg(&library)
            .status()
            .unwrap();
        assert!(status.success(), "failed to compile LiteRT-LM ABI stub");

        let request = ConversationRequest {
            items: vec![ConversationItem::User(UserItem {
                content: vec![ContentPart::Text {
                    text: Arc::<str>::from("hello"),
                }],
                ..Default::default()
            })],
            max_output_tokens: Some(64),
            x_grok_session_id: Some("stub-session".to_owned()),
            ..Default::default()
        };
        let config = LiteRtLmConfig {
            model_path: temp.path().join("model.litertlm"),
            library_path: library,
            backend: "cpu".to_owned(),
            max_context_tokens: Some(4096),
            supported_lora_ranks: Vec::new(),
            adapter_descriptor: None,
            lora_adapter: None,
            max_resident_adapters: DEFAULT_MAX_RESIDENT_ADAPTERS,
            max_resident_sessions: DEFAULT_MAX_RESIDENT_SESSIONS,
            max_resident_context_tokens: DEFAULT_MAX_RESIDENT_CONTEXT_TOKENS,
            context_strategy: ContextOverflowStrategy::Strict,
            min_recent_turns: DEFAULT_MIN_RECENT_TURNS,
        };
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        let result = run_local_request(
            uuid::Uuid::new_v4().to_string(),
            request,
            config.clone(),
            "stub-model".to_owned(),
            &event_tx,
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        let LocalInferenceResult::Completed { response, .. } = result else {
            panic!("stub inference unexpectedly cancelled");
        };
        let ConversationItem::Assistant(assistant) = &response.items[0] else {
            panic!("expected assistant response");
        };
        assert_eq!(assistant.content.as_ref(), "stub response");
        assert_eq!(response.usage.as_ref().unwrap().total_tokens, 42);
        assert!(
            std::iter::from_fn(|| event_rx.try_recv().ok())
                .any(|event| matches!(event, RuntimeEvent::ChannelToken { text, .. } if text == "stub response"))
        );

        let continuation = ConversationRequest {
            items: vec![
                ConversationItem::User(UserItem {
                    content: vec![ContentPart::Text {
                        text: Arc::<str>::from("hello"),
                    }],
                    ..Default::default()
                }),
                response.items[0].clone(),
                ConversationItem::User(UserItem {
                    content: vec![ContentPart::Text {
                        text: Arc::<str>::from("continue"),
                    }],
                    ..Default::default()
                }),
            ],
            max_output_tokens: Some(64),
            x_grok_session_id: Some("stub-session".to_owned()),
            ..Default::default()
        };
        let continuation = run_local_request(
            uuid::Uuid::new_v4().to_string(),
            continuation,
            config.clone(),
            "stub-model".to_owned(),
            &event_tx,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        let LocalInferenceResult::Completed { response, .. } = continuation else {
            panic!("stub continuation unexpectedly cancelled");
        };
        let usage = response.usage.as_ref().unwrap();
        assert_eq!(usage.cached_prompt_tokens, 42);
        assert_eq!(usage.prompt_tokens, 77);
        assert_eq!(usage.completion_tokens, 7);
        assert_eq!(usage.total_tokens, 84);

        let base_url = format!(
            "litert-lm://{}?library={}&backend=cpu&max_context_tokens=4096",
            config.model_path.display(),
            config.library_path.display()
        );
        let oversized = "0123456789".repeat(40);
        let bounded = bound_text(&base_url, &oversized, 120)
            .await
            .unwrap()
            .expect("local token budget");
        assert!(bounded.truncated);
        assert_eq!(bounded.original_tokens, 400);
        assert!(bounded.retained_tokens <= 120);
        assert!(bounded.text.contains("middle truncated"));
    }

    #[test]
    #[ignore = "requires LITERT_LM_LIBRARY to point at the real Grok bridge"]
    fn live_real_bridge_exposes_lora_binding_and_reports_open_errors() {
        let library_path = std::env::var("LITERT_LM_LIBRARY").expect("LITERT_LM_LIBRARY");
        // SAFETY: this test explicitly loads the configured Grok bridge and
        // validates its versioned symbol table before invoking any setter.
        let library = unsafe { Library::new(library_path) }.unwrap();
        // SAFETY: Api::load checks every symbol required by this ABI version.
        let api = unsafe { Api::load(&library) }.unwrap();
        // SAFETY: bridge metadata functions take no pointers.
        assert_eq!(unsafe { (api.bridge_abi_version)() }, BRIDGE_ABI_VERSION);
        // SAFETY: constructor returns an owned opaque session config.
        let config = NonNull::new(unsafe { (api.session_config_create)() }).unwrap();
        let missing = CString::new("/definitely/missing/grok-adapter.lora").unwrap();
        let identity = CString::new("missing-adapter-v1").unwrap();
        let mut error_message = std::ptr::null();
        // SAFETY: config and both C strings remain live for this copying call.
        let status = unsafe {
            (api.session_config_set_lora)(
                config.as_ptr(),
                missing.as_ptr(),
                identity.as_ptr(),
                &mut error_message,
            )
        };
        assert_ne!(status, 0);
        assert!(!error_message.is_null());
        // SAFETY: config is destroyed exactly once.
        unsafe { (api.session_config_delete)(config.as_ptr()) };
    }

    #[tokio::test]
    #[ignore = "requires real LiteRT-LM bridge, base model, and LoRA fixture paths"]
    async fn live_real_bridge_refuses_silent_lora_fallback() {
        let library_path =
            PathBuf::from(std::env::var("LITERT_LM_LIBRARY").expect("LITERT_LM_LIBRARY"));
        let model_path =
            PathBuf::from(std::env::var("LITERT_LM_TEST_MODEL").expect("LITERT_LM_TEST_MODEL"));
        let lora_path =
            PathBuf::from(std::env::var("LITERT_LM_TEST_LORA").expect("LITERT_LM_TEST_LORA"));
        let config = LiteRtLmConfig {
            model_path,
            library_path,
            backend: "cpu".to_owned(),
            max_context_tokens: Some(256),
            supported_lora_ranks: vec![16],
            adapter_descriptor: None,
            lora_adapter: Some(LoraAdapterConfig {
                path: lora_path,
                id: "incompatible-fixture-v1".to_owned(),
            }),
            max_resident_adapters: 1,
            max_resident_sessions: 1,
            max_resident_context_tokens: 256,
            context_strategy: ContextOverflowStrategy::Strict,
            min_recent_turns: 1,
        };
        let request = ConversationRequest {
            items: vec![ConversationItem::User(UserItem {
                content: vec![ContentPart::Text {
                    text: Arc::<str>::from("hello"),
                }],
                ..Default::default()
            })],
            max_output_tokens: Some(1),
            ..Default::default()
        };
        let (event_tx, _) = mpsc::unbounded_channel();
        let error = match run_local_request(
            uuid::Uuid::new_v4().to_string(),
            request,
            config,
            "incompatible-lora-fixture".to_owned(),
            &event_tx,
            &CancellationToken::new(),
        )
        .await
        {
            Err(error) => error,
            Ok(_) => panic!("a base model without LoRA inputs must fail closed"),
        };
        assert!(
            error.to_string().contains("no LoRA input tensors"),
            "unexpected error: {error}"
        );
    }
}
