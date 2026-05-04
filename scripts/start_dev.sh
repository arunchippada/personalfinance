#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
env_file="$repo_root/.env.local"

cd "$repo_root"

if [[ -f "$env_file" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$env_file"
  set +a
fi

export NEXT_TEST_WASM_DIR="${NEXT_TEST_WASM_DIR:-./node_modules/@next/swc-wasm-nodejs}"

pnpm exec next dev
