#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef void (*stream_callback)(void *, const char *, bool, const char *);

uint32_t grok_litert_bridge_abi_version(void) { return 5; }
uint64_t grok_litert_bridge_capabilities(void) {
  return (1ULL << 0) | (1ULL << 1) | (1ULL << 2) | (1ULL << 3) |
         (1ULL << 4) | (1ULL << 5) | (1ULL << 6);
}
const char *grok_litert_bridge_build_id(void) {
  return "grok-litert-test-bridge-v5";
}
const char *grok_litert_last_error_message(void) { return NULL; }

typedef struct {
  size_t count;
  int *tokens;
} tokenize_result;

typedef struct {
  char *text;
} detokenize_result;

typedef struct {
  int sends;
} conversation_state;

typedef struct {
  int sends;
} benchmark_info;

void *litert_lm_engine_settings_create(const char *model, const char *backend,
                                       const char *unused_a,
                                       const char *unused_b) {
  (void)model;
  (void)backend;
  (void)unused_a;
  (void)unused_b;
  return malloc(1);
}

void litert_lm_engine_settings_delete(void *settings) { free(settings); }
void litert_lm_engine_settings_set_max_num_tokens(void *settings, int value) {
  (void)settings;
  (void)value;
}
int grok_litert_engine_settings_set_supported_lora_ranks(
    void *settings, const uint32_t *ranks, size_t num_ranks,
    const char **error_message) {
  (void)settings;
  if (error_message != NULL) {
    *error_message = NULL;
  }
  return num_ranks > 0 && ranks == NULL ? 1 : 0;
}
void litert_lm_engine_settings_enable_benchmark(void *settings) {
  (void)settings;
}
void *litert_lm_engine_create(const void *settings) {
  (void)settings;
  return malloc(1);
}
void litert_lm_engine_delete(void *engine) { free(engine); }
void *litert_lm_engine_create_session(void *engine, void *config) {
  (void)engine;
  (void)config;
  return malloc(1);
}
void litert_lm_session_delete(void *session) { free(session); }
int grok_litert_engine_unload_lora(void *engine, const char *identity,
                                    const char **error_message) {
  (void)engine;
  if (error_message != NULL) {
    *error_message = NULL;
  }
  return identity == NULL || identity[0] == '\0' ? 1 : 0;
}
void *litert_lm_engine_tokenize(void *engine, const char *text) {
  (void)engine;
  tokenize_result *result = malloc(sizeof(tokenize_result));
  if (result == NULL) {
    return NULL;
  }
  size_t bytes = text == NULL ? 0 : strlen(text);
  result->count = bytes;
  result->tokens = bytes == 0 ? NULL : malloc(bytes * sizeof(int));
  if (bytes != 0 && result->tokens == NULL) {
    free(result);
    return NULL;
  }
  for (size_t index = 0; index < bytes; ++index) {
    result->tokens[index] = (unsigned char)text[index];
  }
  return result;
}
void litert_lm_tokenize_result_delete(void *opaque_result) {
  tokenize_result *result = opaque_result;
  if (result != NULL) {
    free(result->tokens);
  }
  free(result);
}
const int *litert_lm_tokenize_result_get_tokens(const void *opaque_result) {
  const tokenize_result *result = opaque_result;
  return result == NULL ? NULL : result->tokens;
}
size_t litert_lm_tokenize_result_get_num_tokens(const void *opaque_result) {
  const tokenize_result *result = opaque_result;
  return result == NULL ? 0 : result->count;
}
void *litert_lm_engine_detokenize(void *engine, const int *tokens,
                                 size_t num_tokens) {
  (void)engine;
  detokenize_result *result = malloc(sizeof(detokenize_result));
  if (result == NULL) {
    return NULL;
  }
  result->text = malloc(num_tokens + 1);
  if (result->text == NULL) {
    free(result);
    return NULL;
  }
  for (size_t index = 0; index < num_tokens; ++index) {
    result->text[index] = (char)tokens[index];
  }
  result->text[num_tokens] = '\0';
  return result;
}
void litert_lm_detokenize_result_delete(void *opaque_result) {
  detokenize_result *result = opaque_result;
  if (result != NULL) {
    free(result->text);
  }
  free(result);
}
const char *litert_lm_detokenize_result_get_string(const void *opaque_result) {
  const detokenize_result *result = opaque_result;
  return result == NULL ? NULL : result->text;
}

void *litert_lm_session_config_create(void) { return malloc(1); }
void litert_lm_session_config_delete(void *config) { free(config); }
void litert_lm_session_config_set_max_output_tokens(void *config, int value) {
  (void)config;
  (void)value;
}
void litert_lm_session_config_set_sampler_params(void *config,
                                                 const void *params) {
  (void)config;
  (void)params;
}
int grok_litert_session_config_set_lora_file(void *config, const char *path,
                                             const char *identity,
                                             const char **error_message) {
  (void)config;
  if (error_message != NULL) {
    *error_message = NULL;
  }
  return path == NULL || path[0] == '\0' || identity == NULL ||
                 identity[0] == '\0'
             ? 1
             : 0;
}

void *litert_lm_conversation_config_create(void) { return malloc(1); }
void litert_lm_conversation_config_delete(void *config) { free(config); }
void litert_lm_conversation_config_set_session_config(
    void *config, const void *session_config) {
  (void)config;
  (void)session_config;
}
void litert_lm_conversation_config_set_system_message(void *config,
                                                       const char *value) {
  (void)config;
  (void)value;
}
void litert_lm_conversation_config_set_tools(void *config, const char *value) {
  (void)config;
  (void)value;
}
void litert_lm_conversation_config_set_messages(void *config,
                                                const char *value) {
  (void)config;
  (void)value;
}
void litert_lm_conversation_config_set_enable_constrained_decoding(
    void *config, bool enabled) {
  (void)config;
  (void)enabled;
}

void *litert_lm_conversation_create(void *engine, void *config) {
  (void)engine;
  (void)config;
  conversation_state *conversation = calloc(1, sizeof(conversation_state));
  return conversation;
}
void litert_lm_conversation_delete(void *conversation) { free(conversation); }

int grok_litert_conversation_measure_prompt(
    void *engine, void *config, const char *message, size_t *token_count,
    const char **error_message) {
  (void)engine;
  (void)config;
  if (error_message != NULL) {
    *error_message = NULL;
  }
  if (message == NULL || token_count == NULL) {
    return 1;
  }
  *token_count = strlen(message);
  return 0;
}

int litert_lm_conversation_send_message_stream(
    void *conversation, const char *message, const char *extra_context,
    stream_callback callback, void *callback_data) {
  conversation_state *state = conversation;
  (void)extra_context;
  if (message != NULL &&
      strstr(message, "__GROK_TEST_NATIVE_CRASH__") != NULL) {
    abort();
  }
  if (message != NULL &&
      strstr(message, "__GROK_TEST_NATIVE_HANG__") != NULL) {
    const struct timespec delay = {.tv_sec = 1, .tv_nsec = 0};
    for (;;) {
      nanosleep(&delay, NULL);
    }
  }
  state->sends += 1;
  callback(callback_data,
           "{\"role\":\"assistant\",\"content\":[{\"type\":\"text\","
           "\"text\":\"stub response\"}]}",
           false, NULL);
  callback(callback_data, NULL, true, NULL);
  return 0;
}

void litert_lm_conversation_cancel_process(void *conversation) {
  (void)conversation;
}

void *litert_lm_conversation_get_benchmark_info(void *conversation) {
  const conversation_state *state = conversation;
  benchmark_info *info = malloc(sizeof(benchmark_info));
  if (info != NULL) {
    info->sends = state->sends;
  }
  return info;
}
void litert_lm_benchmark_info_delete(void *info) { free(info); }
int litert_lm_benchmark_info_get_num_prefill_turns(const void *info) {
  const benchmark_info *benchmark = info;
  return benchmark->sends;
}
int litert_lm_benchmark_info_get_num_decode_turns(const void *info) {
  const benchmark_info *benchmark = info;
  return benchmark->sends;
}
int litert_lm_benchmark_info_get_prefill_token_count_at(const void *info,
                                                        int index) {
  (void)info;
  (void)index;
  return 35;
}
int litert_lm_benchmark_info_get_decode_token_count_at(const void *info,
                                                       int index) {
  (void)info;
  (void)index;
  return 7;
}
