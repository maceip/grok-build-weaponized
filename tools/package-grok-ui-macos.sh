#!/bin/sh
set -eu

binary=${1:-target/release-small/grok-ui}
bundle=${2:-target/release-small/Grok Operator.app}

if [ ! -x "$binary" ]; then
  echo "missing executable GUI binary: $binary" >&2
  exit 1
fi

install -d "$bundle/Contents/MacOS"
install -m 755 "$binary" "$bundle/Contents/MacOS/grok-ui"
install -m 644 tools/macos/GrokOperator-Info.plist "$bundle/Contents/Info.plist"
echo "$bundle"
