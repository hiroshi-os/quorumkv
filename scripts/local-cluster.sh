#!/usr/bin/env bash
# Start / stop / chaos a 3-node quorumkv cluster on 127.0.0.1:8081-8083.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
BIN="${BIN:-$ROOT/bin/quorumkv}"
CHAOS="${CHAOS:-$ROOT/bin/chaos}"
DATA="$ROOT/.data"
PIDDIR="$ROOT/.pids"
LOGDIR="$ROOT/.logs"

start_one() {
  local id=$1 port=$2 peers=$3
  mkdir -p "$DATA/$id" "$PIDDIR" "$LOGDIR"
  "$BIN" \
    -id "$id" \
    -bind "127.0.0.1:$port" \
    -peers "$peers" \
    -data "$DATA/$id" \
    -log-level info \
    >"$LOGDIR/$id.log" 2>&1 &
  echo $! >"$PIDDIR/$id.pid"
  echo "started $id pid=$! :$port"
}

cmd_start() {
  if [[ ! -x "$BIN" ]]; then
    echo "building binaries..."
    (cd "$ROOT" && mkdir -p bin && go build -o bin/quorumkv ./cmd/quorumkv && go build -o bin/chaos ./cmd/chaos)
  fi
  cmd_stop >/dev/null 2>&1 || true
  start_one n1 8081 "n2=127.0.0.1:8082,n3=127.0.0.1:8083"
  start_one n2 8082 "n1=127.0.0.1:8081,n3=127.0.0.1:8083"
  start_one n3 8083 "n1=127.0.0.1:8081,n2=127.0.0.1:8082"
  echo "waiting for /health + unique leader ..."
  for p in 8081 8082 8083; do
    for _ in $(seq 1 50); do
      if curl -sf "http://127.0.0.1:$p/health" >/dev/null; then
        break
      fi
      sleep 0.1
    done
  done
  for _ in $(seq 1 80); do
    leaders=$(curl -sf http://127.0.0.1:8081/status http://127.0.0.1:8082/status http://127.0.0.1:8083/status \
      | grep -c '"role":"leader"' || true)
    if [[ "$leaders" == "1" ]]; then
      echo "cluster up: http://127.0.0.1:8081|8082|8083"
      cmd_status
      return
    fi
    sleep 0.05
  done
  echo "timed out waiting for a unique leader" >&2
  cmd_status
  return 1
}

cmd_fresh() {
  cmd_stop >/dev/null 2>&1 || true
  rm -rf "$DATA"
  cmd_start
}

cmd_stop() {
  if [[ -d "$PIDDIR" ]]; then
    for f in "$PIDDIR"/*.pid; do
      [[ -f "$f" ]] || continue
      kill "$(cat "$f")" >/dev/null 2>&1 || true
      rm -f "$f"
    done
  fi
}

cmd_status() {
  for p in 8081 8082 8083; do
    echo -n ":$p "
    curl -sf "http://127.0.0.1:$p/status" || echo "(down)"
  done
}

cmd_chaos() {
  if [[ ! -x "$CHAOS" ]]; then
    (cd "$ROOT" && mkdir -p bin && go build -o bin/chaos ./cmd/chaos)
  fi
  "$CHAOS" -addrs "http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083"
}

case "${1:-start}" in
  start) cmd_start ;;
  fresh) cmd_fresh ;;
  stop) cmd_stop ;;
  status) cmd_status ;;
  chaos) cmd_chaos ;;
  restart) cmd_stop; cmd_start ;;
  *) echo "usage: $0 start|fresh|stop|status|chaos|restart" >&2; exit 2 ;;
esac
