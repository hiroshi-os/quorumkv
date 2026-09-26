# quorumkv

[![ci](https://github.com/hiroshi-os/quorumkv/actions/workflows/ci.yml/badge.svg)](https://github.com/hiroshi-os/quorumkv/actions/workflows/ci.yml)

Real [Raft](https://raft.github.io/) in Go. Not a library wrapper. Not AI.

A 3-node replicated KV: `SET` goes through the leader and a majority.
`GET ?consistent=true` is a Raft **ReadIndex** linearizable read on the
leader (followers forward once). Plain `GET` remains a local applied-state
read and can be stale — that path is kept so the porcupine checker can
prove the old behaviour is not linearizable under partition.

This complements a cache (ringcache). **Cache ≠ consensus.** Put
quorumkv behind the cache if you need a commit log; do not pretend the
cache elects a leader.

## What shipped

- From-scratch Raft: randomized election timeouts, RequestVote,
  AppendEntries, quorum commit, **conflictIndex** fast rollback,
  **current-term commit rule** (Figure 8), **ReadIndex** (§6.4)
- Heartbeats; kill-leader chaos demo with automatic election
- In-memory log for tests; JSON-lines WAL (`meta.json` + `log.jsonl`)
  for the compose/local demo
- Porcupine linearizability checker over randomized histories with
  leader kills and network partitions on the in-memory transport
- GitHub Actions CI: gofmt, `go vet`, `go test -race`, short porcupine
- 3-node `docker-compose`, benches with hardware/date, `DESIGN.md`
- **No** etcd/raft, hashicorp/raft, or any other consensus library

## Quick start (no Docker)

```bash
make cluster          # fresh n1:8081 n2:8082 n3:8083 + WAL under .data/
curl -s http://127.0.0.1:8081/status
curl -s -X PUT --data 'osaka' http://127.0.0.1:8081/kv/city
curl -s 'http://127.0.0.1:8082/kv/city?consistent=true'   # ReadIndex via leader
curl -s http://127.0.0.1:8082/kv/city                      # local GET (may be stale)
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
| `GET` | `/kv/{key}?consistent=true` | **ReadIndex**. Leader confirms quorum, waits for apply, then returns. Followers forward once. |
| `GET` | `/kv/{key}` | **Local** applied map. May be stale. |
| `GET` | `/status` | `id`, `role`, `term`, `leader`, commit/applied indexes |
| `GET` | `/health` | liveness |
| `POST` | `/raft/request_vote`, `/raft/append_entries` | Raft RPCs (JSON) |
| `POST` | `/admin/crash` | process exits — chaos only, unauthenticated |

```bash
curl -s http://127.0.0.1:8081/status | jq .
curl -s -X PUT --data '1' http://127.0.0.1:8082/kv/counter   # forwarded if needed
curl -s 'http://127.0.0.1:8083/kv/counter?consistent=true'
```

## Read consistency (short)

- **SET that returns 200** is majority-committed under the current-term
  rule. Linearizable write.
- **`GET ?consistent=true` is linearizable** when it returns 200.
  The leader waits until a current-term entry is committed, records
  `commitIndex`, confirms leadership with a fresh AppendEntries quorum
  round, then waits until `lastApplied` reaches that index before
  serving the local FSM value. A partitioned ex-leader fails the read
  instead of returning stale data. Followers forward once.
- **Plain `GET` is not linearizable.** No ReadIndex. A follower can
  lag; a partitioned ex-leader serves last committed state and misses
  later majority commits. Kept for demos and for the porcupine
  counter-example under partition.
- Full write-up: [DESIGN.md](DESIGN.md). Measured linearizability
  counts from a real run (see [bench/RESULTS.md](bench/RESULTS.md)):
  **500 / 500** ReadIndex histories linearizable; **88 / 100** plain
  local-GET histories under partition/chaos flagged non-linearizable
  (12 still passed by chance). In-process ReadIndex ≈ **3.3 µs** vs
  local GET ≈ **15 ns** on the same host.

## Layout

```
cmd/quorumkv     process (HTTP + Raft + WAL)
cmd/chaos        kill-leader demo client
internal/raft    the algorithm (no third-party Raft) + porcupine harness
internal/kv      FSM
internal/server  HTTP
scripts/         local-cluster.sh, chaos.sh
bench/RESULTS.md measured numbers + hardware
```

## Tests and benches

```bash
make test
make test-race
make porcupine    # 500 consistent histories + stale-path counter-examples
make bench        # also see bench/RESULTS.md
```

In-process cluster tests cover election, replication, kill-leader,
Figure 8, conflictIndex, up-to-date votes, WAL replay, ReadIndex, and
porcupine linearizability under chaos. HTTP tests cover SET/GET,
`?consistent=true`, and failover over the real JSON transport.

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
- No snapshots, no membership change, no pre-vote, no leader leases.
- Plain `GET` stays intentionally stale; use `?consistent=true` for
  linearizable reads (one quorum RTT of AppendEntries).
- `/admin/crash` is unauthenticated. Do not expose it.

Measured numbers live in `bench/RESULTS.md` with CPU, RAM, kernel,
commit SHA, date, and the exact command. In-process SET ≠
compose+WAL+fsync SET.
