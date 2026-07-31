#!/bin/sh
set -eu

target=${1:-}
profile=release-small
cargo_target_root=${CARGO_TARGET_DIR:-target}
output_root=$cargo_target_root/$profile

tools/check-compact-dependencies.sh
if [ -n "$target" ]; then
  output_root=$cargo_target_root/$target/$profile
fi

if [ -n "$target" ]; then
  cargo build --locked --profile "$profile" --target "$target" \
    -p xai-grok-control-client --bin grokctl \
    -p xai-grok-control-plane --bin grokd
else
  cargo build --locked --profile "$profile" \
    -p xai-grok-control-client --bin grokctl \
    -p xai-grok-control-plane --bin grokd
fi

suffix=
case "$target" in
  *windows*) suffix=.exe ;;
esac

tools/check-compact-artifacts.sh \
  "$output_root/grokctl$suffix" 8388608 \
  "$output_root/grokd$suffix" 16777216

if [ "${INCLUDE_GUI:-0}" = 1 ]; then
  case "$target" in
    *musl*)
      echo "grok-ui is a separate native GUI artifact and is not built for musl" >&2
      exit 2
      ;;
  esac
  if [ -n "$target" ]; then
    cargo build --locked --profile "$profile" --target "$target" \
      -p xai-grok-operator-ui --bin grok-ui
  else
    cargo build --locked --profile "$profile" \
      -p xai-grok-operator-ui --bin grok-ui
  fi
  tools/check-compact-artifacts.sh "$output_root/grok-ui$suffix" 33554432
fi
