#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 [--source PATH] [--output PATH]" >&2
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
repo_root="$(cd "${script_dir}/../../../../.." && pwd -P)"
source_dir="${LITERT_LM_SOURCE:-/Users/mac/LiteRT-DPM-main}"
output_dir="${repo_root}/.grok/runtime"
source "${script_dir}/toolchain.lock"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source)
      [[ $# -ge 2 ]] || { usage; exit 2; }
      source_dir="$2"
      shift 2
      ;;
    --output)
      [[ $# -ge 2 ]] || { usage; exit 2; }
      output_dir="$2"
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

[[ -f "${source_dir}/c/engine.h" ]] || {
  echo "LiteRT-LM source tree not found at ${source_dir}" >&2
  exit 1
}
command -v bazelisk >/dev/null || {
  echo "bazelisk is required" >&2
  exit 1
}
command -v xcrun >/dev/null || {
  echo "xcrun is required" >&2
  exit 1
}

clang_version_output="$(xcrun clang++ --version)"
actual_clang_version="${clang_version_output%%$'\n'*}"
[[ "${actual_clang_version}" == "${APPLE_CLANG_VERSION}" ]] || {
  echo "Apple clang mismatch" >&2
  echo "expected: ${APPLE_CLANG_VERSION}" >&2
  echo "actual:   ${actual_clang_version}" >&2
  exit 1
}

if git -C "${source_dir}" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  actual_source_revision="$(git -C "${source_dir}" rev-parse HEAD)"
  [[ "${actual_source_revision}" == "${LITERT_LM_SOURCE_REVISION}" ]] || {
    echo "LiteRT-LM source revision mismatch" >&2
    echo "expected: ${LITERT_LM_SOURCE_REVISION}" >&2
    echo "actual:   ${actual_source_revision}" >&2
    exit 1
  }
else
  (
    cd "${source_dir}"
    shasum -a 256 -c "${script_dir}/source.sha256"
  ) || {
    echo "LiteRT-LM source snapshot does not match the pinned revision" >&2
    exit 1
  }
fi

patches=(
  "${script_dir}/patches/0001-lora-identity-residency.patch"
  "${script_dir}/patches/0002-conversation-error-detail.patch"
  "${script_dir}/patches/0003-grok-safe-session-and-prompt-measurement.patch"
  "${script_dir}/patches/0004-native-adapter-unload.patch"
  "${script_dir}/patches/0005-supported-lora-ranks-setting.patch"
  "${script_dir}/patches/0006-legacy-webgpu-sampler-compatibility.patch"
  "${script_dir}/patches/0007-nonblocking-benchmark-warmup.patch"
)
patch_revision="$(
  shasum -a 256 "${patches[@]}" |
    shasum -a 256 |
    awk '{print $1}'
)"
applied_patches=()
restore_source_tree() {
  local index
  for ((index=${#applied_patches[@]} - 1; index >= 0; index--)); do
    git -C "${source_dir}" apply --reverse "${applied_patches[index]}" || {
      echo "failed to restore LiteRT-LM source patch ${applied_patches[index]}" >&2
    }
  done
}
trap restore_source_tree EXIT

for patch in "${patches[@]}"; do
  if git -C "${source_dir}" apply --check "${patch}"; then
    git -C "${source_dir}" apply "${patch}"
    applied_patches+=("${patch}")
  elif git -C "${source_dir}" apply --reverse --check "${patch}"; then
    echo "using already-applied LiteRT-LM patch $(basename "${patch}")" >&2
  else
    echo "LiteRT-LM source is incompatible with patch ${patch}" >&2
    exit 1
  fi
done

(
  cd "${source_dir}"
  bazelisk build \
    --package_path="${script_dir}/overlay:%workspace%" \
    --config=macos_arm64 \
    //grok_litert_bridge:liblitert_lm_c.dylib
)

artifact="${source_dir}/bazel-bin/grok_litert_bridge/liblitert_lm_c.dylib"
[[ -f "${artifact}" ]] || {
  echo "Bazel completed but did not produce ${artifact}" >&2
  exit 1
}

mkdir -p "${output_dir}"
install_runtime_file() {
  local source_file="$1"
  local destination="${output_dir}/$(basename "${source_file}")"
  if [[ -f "${destination}" ]] && cmp -s "${source_file}" "${destination}"; then
    return
  fi
  local staged
  staged="$(mktemp "${output_dir}/.$(basename "${source_file}").XXXXXX")"
  cp "${source_file}" "${staged}"
  chmod a-w "${staged}"
  mv -f "${staged}" "${destination}"
}

install_runtime_file "${artifact}"
for dependency in "${source_dir}"/prebuilt/macos_arm64/*.dylib; do
  install_runtime_file "${dependency}"
done
bridge_header_hash="$(
  shasum -a 256 \
    "${script_dir}/overlay/grok_litert_bridge/grok_litert_bridge.h" |
    awk '{print $1}'
)"
compiler_hash="$(printf '%s' "${APPLE_CLANG_VERSION}" | shasum -a 256 | awk '{print $1}')"
source_revision="${LITERT_LM_SOURCE_REVISION}+grok-patches.${patch_revision}+bridge.${bridge_header_hash}+clang.${compiler_hash}"
printf '%s\n' "${source_revision}" > "${output_dir}/litert_lm_source_revision"

echo "${output_dir}/liblitert_lm_c.dylib"
