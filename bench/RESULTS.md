# quorumkv benches

In-process 3-node Raft (MemoryNetwork, MemoryStorage, no HTTP, no fsync).
This is the algorithm cost, not the compose+WAL cost.

## Hardware

- date (UTC): 2026-09-26 14:30:07 UTC
- kernel / OS: Microsoft Windows 11 Home Single Language 10.0.26200 (windows/amd64)
- cpu: AMD Ryzen 5 7530U with Radeon Graphics × 12 logical processors
- mem: 23.3 GiB
- go: go version go1.25.5 windows/amd64
- commit: `db2acda0a5c0db9446d2de2f57a1220170c1f8b1` (tree measured on this branch; SHA stamped after commit)

## Linearizability (porcupine)

Exact command:

```
QUORUMKV_FULL_HISTORIES=1 go test ./internal/raft/ -count=1 -timeout 45m -v -run TestFullLinearizability
```

(PowerShell: `$env:QUORUMKV_FULL_HISTORIES='1'; go test ./internal/raft/ -count=1 -timeout 45m -v -run TestFullLinearizability`)

Result line from that run:

```
LIN_RESULT consistent_histories=500 consistent_pass=500 consistent_illegal=0 consistent_unknown=0 stale_histories=100 stale_pass=12 stale_violations=88 stale_unknown=0
```

| path | histories checked | pass | non-linearizable | checker unknown |
|---|---:|---:|---:|---:|
| `GET` via ReadIndex (`consistent=true`) under concurrent clients + leader kills + partitions | 500 | 500 | 0 | 0 |
| plain local `GET` under the same chaos (proves the checker) | 100 | 12 | **88** | 0 |

Wall time for the full measurement: 44.99s.

## go test -bench

Exact command:

```
go test -c -o bin/raft.test.exe ./internal/raft
./bin/raft.test.exe -test.bench=. -test.benchmem -test.count=3 -test.run=^$
go test -c -o bin/kv.test.exe ./internal/kv
./bin/kv.test.exe -test.bench=. -test.benchmem -test.count=3 -test.run=^$
```

```
goos: windows
goarch: amd64
pkg: github.com/hiroshi-os/quorumkv/internal/raft
cpu: AMD Ryzen 5 7530U with Radeon Graphics
BenchmarkElection3-12                  48         26540442 ns/op       10395 B/op         152 allocs/op
BenchmarkElection3-12                  40         29144072 ns/op       10068 B/op         151 allocs/op
BenchmarkElection3-12                  49         27549188 ns/op        9864 B/op         150 allocs/op
BenchmarkProposeCommitted-12        52353            28463 ns/op        3458 B/op          43 allocs/op
BenchmarkProposeCommitted-12        39346            44235 ns/op        3490 B/op          43 allocs/op
BenchmarkProposeCommitted-12        40363            35925 ns/op        3480 B/op          43 allocs/op
BenchmarkReadIndex-12              429524             3310 ns/op         602 B/op           7 allocs/op
BenchmarkReadIndex-12              513634             3255 ns/op         604 B/op           7 allocs/op
BenchmarkReadIndex-12              313154             3622 ns/op         612 B/op           7 allocs/op
BenchmarkLocalGet-12             86706454            15.03 ns/op           0 B/op           0 allocs/op
BenchmarkLocalGet-12             31254313            35.11 ns/op           0 B/op           0 allocs/op
BenchmarkLocalGet-12             68517769            16.22 ns/op           0 B/op           0 allocs/op
PASS
goos: windows
goarch: amd64
pkg: github.com/hiroshi-os/quorumkv/internal/kv
cpu: AMD Ryzen 5 7530U with Radeon Graphics
BenchmarkGet-12                 81619081            13.42 ns/op           0 B/op           0 allocs/op
BenchmarkGet-12                 99650392            16.38 ns/op           0 B/op           0 allocs/op
BenchmarkGet-12                 91197883            15.40 ns/op           0 B/op           0 allocs/op
PASS
```

## How to read this

| bench | typical | what it includes |
|---|---|---|
| `BenchmarkElection3` | ~28 ms | randomized election with 20–40 ms timeouts, in-process RPC |
| `BenchmarkProposeCommitted` | ~28–44 µs | majority commit + apply, memory log, no HTTP, no fsync |
| `BenchmarkReadIndex` | ~3.3 µs | quorum AppendEntries ack + wait for apply (in-process) |
| `BenchmarkLocalGet` / `kv.BenchmarkGet` | ~15 ns | one `sync.RWMutex` map read |

ReadIndex is ~200× a local GET in-process because it waits for a
fresh heartbeat round. Over HTTP+JSON that gap is dominated by the
network RTT either way. These are **not** docker-compose numbers.

## Local 3-node HTTP + WAL + fsync (prior host, 2026-09-13)

Process-per-node on `127.0.0.1:8081-8083`, `FileStorage` with fsync,
JSON-over-HTTP. 20 sequential curls after a leader existed (Linux Xeon
host from the original MVP run — kept for the demo envelope; not
re-measured on this Windows host):

| op | http | observed |
|---|---|---|
| `PUT /kv/bench` via leader | 200 | 1.4–2.4 ms (≈1.7 ms typical) |
| `GET /kv/city` on a follower | 200 | 0.15–0.50 ms (≈0.25 ms typical) |
