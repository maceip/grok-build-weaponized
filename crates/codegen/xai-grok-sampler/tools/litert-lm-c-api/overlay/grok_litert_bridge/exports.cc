#include <cstdint>

#include "c/engine.h"
#include "grok_litert_bridge/grok_litert_bridge.h"
#include "runtime/engine/engine_settings.h"

extern "C" const char* grok_litert_last_error_message();

namespace {
constexpr std::uint32_t kGrokLiteRtBridgeAbiVersion = 6;
constexpr std::uint64_t kGrokLiteRtBridgeStreaming = 1ULL << 0;
constexpr std::uint64_t kGrokLiteRtBridgeTokenize = 1ULL << 1;
constexpr std::uint64_t kGrokLiteRtBridgeLora = 1ULL << 2;
constexpr std::uint64_t kGrokLiteRtBridgeErrorDetail = 1ULL << 3;
constexpr std::uint64_t kGrokLiteRtBridgeExactPromptMeasurement = 1ULL << 4;
constexpr std::uint64_t kGrokLiteRtBridgeAdapterUnload = 1ULL << 5;
constexpr std::uint64_t kGrokLiteRtBridgeSupportedLoraRanks = 1ULL << 6;
constexpr std::uint64_t kGrokLiteRtBridgeJsonSchema = 1ULL << 7;
}  // namespace

extern "C" __attribute__((visibility("default"))) std::uint32_t
grok_litert_bridge_abi_version() {
  return kGrokLiteRtBridgeAbiVersion;
}

extern "C" __attribute__((visibility("default"))) std::uint64_t
grok_litert_bridge_capabilities() {
  return kGrokLiteRtBridgeStreaming | kGrokLiteRtBridgeTokenize |
         kGrokLiteRtBridgeLora | kGrokLiteRtBridgeErrorDetail |
         kGrokLiteRtBridgeExactPromptMeasurement |
         kGrokLiteRtBridgeAdapterUnload |
         kGrokLiteRtBridgeSupportedLoraRanks |
         kGrokLiteRtBridgeJsonSchema;
}

extern "C" __attribute__((visibility("default"))) const char*
grok_litert_bridge_build_id() {
  // This identifier describes the stable Grok-facing contract. The build
  // script records the exact LiteRT-LM source revision alongside the dylib.
  return "grok-litert-bridge-v6";
}

// `//c:engine` is a cc_library. This exported table creates concrete references
// to every symbol Grok loads so Bazel's linker cannot discard the C bridge when
// producing a standalone dylib.
extern "C" __attribute__((used, visibility("default")))
const std::uintptr_t grok_litert_lm_required_symbols[] = {
    reinterpret_cast<std::uintptr_t>(&grok_litert_last_error_message),
    reinterpret_cast<std::uintptr_t>(&litert_lm_engine_settings_create),
    reinterpret_cast<std::uintptr_t>(&litert_lm_engine_settings_delete),
    reinterpret_cast<std::uintptr_t>(
        &litert_lm_engine_settings_set_max_num_tokens),
    reinterpret_cast<std::uintptr_t>(
        &grok_litert_engine_settings_set_supported_lora_ranks),
    reinterpret_cast<std::uintptr_t>(
        &litert_lm_engine_settings_enable_benchmark),
    reinterpret_cast<std::uintptr_t>(&litert_lm_engine_create),
    reinterpret_cast<std::uintptr_t>(&litert_lm_engine_delete),
    reinterpret_cast<std::uintptr_t>(&grok_litert_engine_unload_lora),
    reinterpret_cast<std::uintptr_t>(&litert_lm_engine_create_session),
    reinterpret_cast<std::uintptr_t>(&litert_lm_session_delete),
    reinterpret_cast<std::uintptr_t>(&litert_lm_session_config_create),
    reinterpret_cast<std::uintptr_t>(&litert_lm_session_config_delete),
    reinterpret_cast<std::uintptr_t>(
        &litert_lm_session_config_set_max_output_tokens),
    reinterpret_cast<std::uintptr_t>(
        &litert_lm_session_config_set_sampler_params),
    reinterpret_cast<std::uintptr_t>(
        &grok_litert_session_config_set_lora_file),
    reinterpret_cast<std::uintptr_t>(&litert_lm_conversation_config_create),
    reinterpret_cast<std::uintptr_t>(&litert_lm_conversation_config_delete),
    reinterpret_cast<std::uintptr_t>(
        &litert_lm_conversation_config_set_session_config),
    reinterpret_cast<std::uintptr_t>(
        &litert_lm_conversation_config_set_system_message),
    reinterpret_cast<std::uintptr_t>(
        &litert_lm_conversation_config_set_tools),
    reinterpret_cast<std::uintptr_t>(
        &litert_lm_conversation_config_set_messages),
    reinterpret_cast<std::uintptr_t>(
        &litert_lm_conversation_config_set_enable_constrained_decoding),
    reinterpret_cast<std::uintptr_t>(
        &grok_litert_conversation_config_set_json_schema),
    reinterpret_cast<std::uintptr_t>(&litert_lm_conversation_create),
    reinterpret_cast<std::uintptr_t>(&litert_lm_conversation_delete),
    reinterpret_cast<std::uintptr_t>(
        &grok_litert_conversation_measure_prompt),
    reinterpret_cast<std::uintptr_t>(
        &litert_lm_conversation_send_message_stream),
    reinterpret_cast<std::uintptr_t>(
        &litert_lm_conversation_cancel_process),
    reinterpret_cast<std::uintptr_t>(
        &litert_lm_conversation_get_benchmark_info),
    reinterpret_cast<std::uintptr_t>(&litert_lm_benchmark_info_delete),
    reinterpret_cast<std::uintptr_t>(
        &litert_lm_benchmark_info_get_num_prefill_turns),
    reinterpret_cast<std::uintptr_t>(
        &litert_lm_benchmark_info_get_num_decode_turns),
    reinterpret_cast<std::uintptr_t>(
        &litert_lm_benchmark_info_get_prefill_token_count_at),
    reinterpret_cast<std::uintptr_t>(
        &litert_lm_benchmark_info_get_decode_token_count_at),
    reinterpret_cast<std::uintptr_t>(&litert_lm_engine_tokenize),
    reinterpret_cast<std::uintptr_t>(&litert_lm_tokenize_result_delete),
    reinterpret_cast<std::uintptr_t>(&litert_lm_tokenize_result_get_tokens),
    reinterpret_cast<std::uintptr_t>(
        &litert_lm_tokenize_result_get_num_tokens),
    reinterpret_cast<std::uintptr_t>(&litert_lm_engine_detokenize),
    reinterpret_cast<std::uintptr_t>(&litert_lm_detokenize_result_delete),
    reinterpret_cast<std::uintptr_t>(
        &litert_lm_detokenize_result_get_string),
};
