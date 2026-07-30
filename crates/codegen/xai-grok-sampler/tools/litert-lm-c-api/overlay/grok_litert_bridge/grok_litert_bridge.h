#ifndef GROK_LITERT_BRIDGE_H_
#define GROK_LITERT_BRIDGE_H_

#include <cstddef>
#include <cstdint>

#include "c/engine.h"

extern "C" {

std::uint32_t grok_litert_bridge_abi_version();
std::uint64_t grok_litert_bridge_capabilities();
const char* grok_litert_bridge_build_id();
int grok_litert_engine_settings_set_supported_lora_ranks(
    LiteRtLmEngineSettings* settings, const std::uint32_t* ranks,
    std::size_t num_ranks, const char** error_message);

}

#endif  // GROK_LITERT_BRIDGE_H_
