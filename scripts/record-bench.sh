#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
OUT="$ROOT/bench/RESULTS.md"
{
  echo "# quorumkv benches"
  echo
  echo "In-process 3-node Raft (MemoryNetwork, MemoryStorage, no HTTP, no fsync)."
  echo "This is the algorithm cost, not the compose+WAL cost."
  echo
  echo "## Hardware"
  echo
  echo "- date (UTC): $(date -u '+%Y-%m-%d %H:%M:%S %Z')"
  echo "- kernel: $(uname -srm)"
  echo "- cpu: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs) × $(nproc)"
  echo "- mem: $(awk '/MemTotal/ {printf \"%.1f GiB\", $2/1024/1024}' /proc/meminfo)"
  echo "- go: $(go version)"
  echo
  echo "## go test -bench"
  echo
  echo '```'
  (cd "$ROOT" && go test ./internal/raft ./internal/kv -bench=. -benchmem -count=3)
  echo '```'
} >"$OUT"
echo "wrote $OUT"
