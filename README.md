# quorumkv

Real [Raft](https://raft.github.io/) in Go. Not a library wrapper. Not AI.

A 3-node replicated KV: `SET` goes through the leader and a majority;
`GET` is a local applied-state read (followers allowed; see caveats).

This complements a cache (ringcache). **Cache ≠ consensus.** Put
quorumkv behind the cache if you need a commit log; do not pretend the
cache elects a leader.

## What shipped

- From-scratch Raft: randomized election timeouts, RequestVote,
  AppendEntries, quorum commit, **conflictIndex** fast rollback,
  **current-term commit rule** (Figure 8)
- Heartbeats; kill-leader chaos demo with automatic election
- In-memory log for tests; JSON-lines WAL (`meta.json` + `log.jsonl`)
  for the compose/local demo
- 3-node `docker-compose`, benches with hardware/date, `DESIGN.md`
- **No** etcd/raft, hashicorp/raft, or any other consensus library

## Quick start (no Docker)

```bash
make cluster          # fresh n1:8081 n2:8082 n3:8083 + WAL under .data/
curl -s http://127.0.0.1:8081/status
curl -s -X PUT --data 'osaka' http://127.0.0.1:8081/kv/city
curl -s http://127.0.0.1:8082/kv/city    # follower GET is local
make chaos            # kill the leader, wait for election, SET/GET again
make stop
```

## Docker Compose

```bash
docker compose up --build -d
# host ports 8081/8082/8083 → n1/n2/n3
./scripts/chaos.sh
docker compose kill n1          # or POST /admin/crash
# remaining two still have quorum; a new leader appears
docker compose down -v
```

`restart: "no"` so a kill stays dead. Majority is 2.

## API

| Method | Path | Notes |
|---|---|---|
| `PUT` | `/kv/{key}` | body = value. Followers forward once to the leader. |
| `GET` | `/kv/{key}` | **Local** applied map. May be stale. |
| `GET` | `/status` | `id`, `role`, `term`, `leader`, commit/applied indexes |
| `GET` | `/health` | liveness |
| `POST` | `/raft/request_vote`, `/raft/append_entries` | Raft RPCs (JSON) |
| `POST` | `/admin/crash` | process exits — chaos only, unauthenticated |

```bash
curl -s http://127.0.0.1:8081/status | jq .
curl -s -X PUT --data '1' http://127.0.0.1:8082/kv/counter   # forwarded if needed
curl -s http://127.0.0.1:8083/kv/counter
```

## Read consistency (short)

- **SET that returns 200** is majority-committed under the current-term
  rule. Linearizable write.
- **GET is not linearizable.** No ReadIndex, no leader lease. A
  follower can lag; a partitioned ex-leader serves last committed
  state and misses later majority commits.
- Full write-up: [DESIGN.md](DESIGN.md).

## Layout

```
cmd/quorumkv     process (HTTP + Raft + WAL)
cmd/chaos        kill-leader demo client
internal/raft    the algorithm (no third-party Raft)
internal/kv      FSM
internal/server  HTTP
scripts/         local-cluster.sh, chaos.sh
bench/RESULTS.md measured numbers + hardware
```

## Tests and benches

```bash
make test
make bench          # also see bench/RESULTS.md
```

In-process cluster tests cover election, replication, kill-leader,
Figure 8, conflictIndex, up-to-date votes, WAL replay. HTTP tests
cover SET/GET and failover over the real JSON transport.

## Flags / env

| flag | env | default |
|---|---|---|
| `-id` | `QUORUMKV_ID` | `n1` |
| `-bind` | `QUORUMKV_BIND` | `:8080` |
| `-peers` | `QUORUMKV_PEERS` | `id=host:port,...` |
| `-data` | `QUORUMKV_DATA` | empty = memory |
| `-no-fsync` | `QUORUMKV_NO_FSYNC=1` | fsync on |
| `-election-min/max` | `QUORUMKV_ELECTION_MIN/MAX` | 250ms / 400ms |
| `-heartbeat` | `QUORUMKV_HEARTBEAT` | 50ms |

## Trade-offs we are not hiding

- JSON/HTTP RPCs instead of a binary mux — debuggable, slower.
- fsync on every persist — correct, not fast. Turn off only for benches.
- No snapshots, no membership change, no pre-vote, no linearizable GET.
- `/admin/crash` is unauthenticated. Do not expose it.

Measured numbers live in `bench/RESULTS.md` with CPU, RAM, kernel, and
date. In-process SET ≠ compose+WAL+fsync SET.
