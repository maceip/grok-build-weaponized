#!/usr/bin/env bash
set -euo pipefail

cache_root="${GROK_LOCAL_MODEL_CACHE:-/Users/mac/.cache/grok-build/models}"
hf=(uvx --from huggingface_hub hf)

"${hf[@]}" download \
  litert-community/VibeThinker-3B \
  --local-dir "${cache_root}/vibethinker-3b"

"${hf[@]}" download \
  litert-community/Qwen2.5-1.5B-Instruct \
  Qwen2.5-1.5B-Instruct_multi-prefill-seq_q8_ekv4096.litertlm \
  README.md \
  --local-dir "${cache_root}/qwen2.5-1.5b-instruct"
