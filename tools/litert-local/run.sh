#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/../.." && pwd)"
binary="${repo_root}/target/release/xai-grok-pager"
runtime_dir="${repo_root}/.grok/runtime"

if [[ ! -f "${runtime_dir}/liblitert_lm_c.dylib" ]]; then
  "${repo_root}/crates/codegen/xai-grok-sampler/tools/litert-lm-c-api/build-macos.sh"
fi

if [[ "${GROK_LOCAL_SKIP_BUILD:-0}" != "1" ]]; then
  cargo build \
    --manifest-path "${repo_root}/Cargo.toml" \
    --release \
    -p xai-grok-pager-bin
fi

export GROK_HOME="${repo_root}/.grok/local-home"
export LITERT_LM_LIBRARY="${runtime_dir}/liblitert_lm_c.dylib"
export DYLD_LIBRARY_PATH="${runtime_dir}${DYLD_LIBRARY_PATH:+:${DYLD_LIBRARY_PATH}}"

exec "${binary}" "$@"
