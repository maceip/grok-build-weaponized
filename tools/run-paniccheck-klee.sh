#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
toolchain_file="$repo_root/verification/klee/toolchain.env"

# shellcheck source=../verification/klee/toolchain.env
source "$toolchain_file"

klee_bin="${KLEE_BIN:-klee}"
rust_toolchain="${PANICCHECK_RUST_TOOLCHAIN}"
output_root="${PANICCHECK_OUTPUT_DIR:-$repo_root/target/paniccheck-klee/$(date -u +%Y%m%dT%H%M%SZ)-$$}"
harness_root="$repo_root/verification/klee/harnesses"
target_root="$repo_root/crates/common/xai-grok-verification-core/src"

command -v rustup >/dev/null
command -v "$klee_bin" >/dev/null

rust_version="$(rustup run "$rust_toolchain" rustc --version --verbose)"
if ! grep -q "LLVM version: $PANICCHECK_LLVM_MAJOR" <<<"$rust_version"; then
    echo "paniccheck: Rust $rust_toolchain is not backed by LLVM $PANICCHECK_LLVM_MAJOR" >&2
    echo "$rust_version" >&2
    exit 2
fi

klee_version="$($klee_bin --version 2>&1)"
if ! grep -q "KLEE $PANICCHECK_KLEE_VERSION" <<<"$klee_version"; then
    echo "paniccheck: expected KLEE $PANICCHECK_KLEE_VERSION" >&2
    echo "$klee_version" >&2
    exit 2
fi

while IFS= read -r marker; do
    target_name="${marker##*: }"
    if [[ ! -f "$harness_root/$target_name.rs" ]]; then
        echo "paniccheck: annotated target '$target_name' has no matching harness" >&2
        exit 2
    fi
done < <(grep -Rho 'paniccheck-target: [a-zA-Z0-9_]*' "$target_root" | sort -u)

mkdir -p "$output_root"
verified=0

for harness in "$harness_root"/*.rs; do
    harness_name="$(basename "$harness" .rs)"
    bitcode="$output_root/$harness_name.bc"
    klee_output="$output_root/$harness_name"
    transcript="$output_root/$harness_name.log"

    rustup run "$rust_toolchain" rustc \
        --edition=2021 \
        --emit=llvm-bc \
        -C codegen-units=1 \
        -C debuginfo=2 \
        -C debug-assertions=yes \
        -C opt-level=0 \
        -C overflow-checks=yes \
        -C panic=abort \
        "$harness" \
        -o "$bitcode"

    set +e
    "$klee_bin" \
        --max-memory=1024 \
        --max-solver-time="$PANICCHECK_MAX_SOLVER_TIME" \
        --max-time="$PANICCHECK_MAX_TIME" \
        --optimize=false \
        --output-dir="$klee_output" \
        --search=dfs \
        "$bitcode" 2>&1 | tee "$transcript"
    klee_status=${PIPESTATUS[0]}
    set -e

    if [[ $klee_status -ne 0 ]]; then
        echo "paniccheck: KLEE crashed for $harness_name (status $klee_status)" >&2
        exit 1
    fi

    error_files="$(find "$klee_output" -maxdepth 1 -type f -name '*.err' -print)"
    if [[ -n "$error_files" ]]; then
        echo "paniccheck: symbolic counterexample found for $harness_name" >&2
        while IFS= read -r error_file; do
            echo "--- $error_file" >&2
            sed -n '1,120p' "$error_file" >&2
            ktest_file="${error_file%.*}.ktest"
            if [[ -f "$ktest_file" ]] && command -v ktest-tool >/dev/null; then
                ktest-tool "$ktest_file" >&2
            fi
        done <<<"$error_files"
        exit 1
    fi

    if grep -Eiq 'HaltTimer|halting execution|time[ -]?out|max-time' "$transcript"; then
        echo "paniccheck: verification did not complete for $harness_name" >&2
        exit 1
    fi

    if ! grep -Eq 'completed paths = [1-9][0-9]*' "$transcript"; then
        echo "paniccheck: KLEE completed no path for $harness_name" >&2
        exit 1
    fi

    if grep -Eq 'partially completed paths = [1-9][0-9]*' "$transcript"; then
        echo "paniccheck: KLEE left partial paths for $harness_name" >&2
        exit 1
    fi

    verified=$((verified + 1))
done

echo "paniccheck: verified $verified targets with KLEE $PANICCHECK_KLEE_VERSION"
echo "paniccheck: artifacts $output_root"
