#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"

cd "${REPO_ROOT}"
cargo build --release -p xai-grok-mcp --bin grok-ops-mcp

echo "${REPO_ROOT}/target/release/grok-ops-mcp"
