#!/bin/sh
set -eu

target=${1:-x86_64-unknown-linux-musl}
case "$target" in
  x86_64-unknown-linux-musl|aarch64-unknown-linux-musl) ;;
  *)
    echo "static grokctl target must be x86_64-unknown-linux-musl or aarch64-unknown-linux-musl" >&2
    exit 2
    ;;
esac

rustc_bin=${RUSTC:-rustc}
cargo_bin=${CARGO_BIN:-cargo}
sysroot=$($rustc_bin --print sysroot)
host=$($rustc_bin -vV | sed -n 's/^host: //p')
linker=$sysroot/lib/rustlib/$host/bin/rust-lld
if [ ! -x "$linker" ]; then
  echo "Rust's bundled rust-lld was not found at $linker" >&2
  exit 1
fi

target_key=$(printf '%s' "$target" | tr '[:lower:]-' '[:upper:]_')
linker_variable=CARGO_TARGET_${target_key}_LINKER
cargo_target_root=${CARGO_TARGET_DIR:-target}

env "$linker_variable=$linker" "$cargo_bin" build --locked \
  --profile release-small \
  --target "$target" \
  -p xai-grok-control-client \
  --bin grokctl

artifact=$cargo_target_root/$target/release-small/grokctl
tools/check-compact-artifacts.sh "$artifact" 8388608
file "$artifact"
