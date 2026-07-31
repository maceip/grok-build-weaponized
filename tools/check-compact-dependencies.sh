#!/bin/sh
set -eu

reject_dependencies() {
  package=$1
  forbidden=$2
  matches=$(cargo tree --locked -p "$package" --edges normal --prefix none \
    | grep -E "^($forbidden) v" || true)
  if [ -n "$matches" ]; then
    echo "$package contains forbidden deployment dependencies:" >&2
    echo "$matches" >&2
    return 1
  fi
}

reject_dependencies xai-grok-control-client \
  'eframe|egui|winit|wgpu|reqwest|rmcp|rusqlite'
reject_dependencies xai-grok-control-plane \
  'eframe|egui|winit|wgpu|reqwest|rmcp'
reject_dependencies xai-grok-operator-ui 'wgpu'

echo "compact dependency boundaries passed"
