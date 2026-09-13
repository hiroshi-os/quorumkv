#!/usr/bin/env bash
# Convenience wrapper: run the kill-leader demo against a live cluster.
# Works for local-cluster (8081-8083) or docker-compose published ports.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
ADDRS="${QUORUMKV_ADDRS:-http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083}"
if [[ ! -x "$ROOT/bin/chaos" ]]; then
  (cd "$ROOT" && mkdir -p bin && go build -o bin/chaos ./cmd/chaos)
fi
exec "$ROOT/bin/chaos" -addrs "$ADDRS"
