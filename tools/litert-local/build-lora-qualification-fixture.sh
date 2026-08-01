#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 [--source PATH] [--output PATH] [--backend cpu|gpu|gpu_artisan]" >&2
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
repo_root="$(cd "${script_dir}/../.." && pwd -P)"
source_dir="${LITERT_LM_SOURCE:-}"
output_path="${repo_root}/.grok/runtime/test_lm_lora_cpu.litertlm"
backend="cpu"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source)
      [[ $# -ge 2 ]] || { usage; exit 2; }
      source_dir="$2"
      shift 2
      ;;
    --output)
      [[ $# -ge 2 ]] || { usage; exit 2; }
      output_path="$2"
      shift 2
      ;;
    --backend)
      [[ $# -ge 2 ]] || { usage; exit 2; }
      backend="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage
      exit 2
      ;;
  esac
done

[[ -n "${source_dir}" ]] || {
  echo "pass --source PATH or set LITERT_LM_SOURCE to the pinned LiteRT-LM checkout" >&2
  exit 1
}

case "${backend}" in
  cpu|gpu|gpu_artisan) ;;
  *)
    echo "unsupported fixture backend: ${backend}" >&2
    exit 2
    ;;
esac

task="${source_dir}/runtime/testdata/test_lm_lora.task"
metadata="${script_dir}/lora-fixture-metadata.textproto"
[[ -f "${task}" ]] || { echo "missing upstream LoRA task: ${task}" >&2; exit 1; }
[[ -f "${metadata}" ]] || { echo "missing fixture metadata: ${metadata}" >&2; exit 1; }
command -v bazelisk >/dev/null || { echo "bazelisk is required" >&2; exit 1; }
command -v unzip >/dev/null || { echo "unzip is required" >&2; exit 1; }

fixture_dir="$(mktemp -d)"
cleanup() {
  rm -r "${fixture_dir}"
}
trap cleanup EXIT
unzip -qq "${task}" -d "${fixture_dir}"
mkdir -p "$(dirname "${output_path}")"

(
  cd "${source_dir}"
  bazelisk run //python/litert_lm_builder:litertlm_builder_cli -- \
    llm_metadata --path "${metadata}" \
    sp_tokenizer --path "${fixture_dir}/TOKENIZER_MODEL" \
    tflite_model --path "${fixture_dir}/TF_LITE_PREFILL_DECODE" \
      --model_type prefill_decode --backend_constraint "${backend}" \
    output --path "${output_path}"
)

echo "${output_path}"
