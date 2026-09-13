# quorumkv benches

In-process 3-node Raft (MemoryNetwork, MemoryStorage, no HTTP, no fsync).
This is the algorithm cost, not the compose+WAL cost.

## Hardware

- date (UTC): 2026-09-13 10:54:50 UTC
- kernel: Linux 6.12.94+ x86_64
- cpu: Intel(R) Xeon(R) Processor × 4
- mem: 15.6 GiB 
- go: go version go1.22.2 linux/amd64

## go test -bench

```
goos: linux
goarch: amd64
pkg: github.com/hiroshi-os/quorumkv/internal/raft
cpu: Intel(R) Xeon(R) Processor
BenchmarkElection3-4          	      43	  28982946 ns/op	    8296 B/op	     138 allocs/op
BenchmarkElection3-4          	      37	  27684511 ns/op	    8408 B/op	     136 allocs/op
BenchmarkElection3-4          	      51	  28110289 ns/op	    8485 B/op	     138 allocs/op
BenchmarkProposeCommitted-4   	  142930	      8130 ns/op	    3164 B/op	      40 allocs/op
BenchmarkProposeCommitted-4   	  149378	      7764 ns/op	    3405 B/op	      40 allocs/op
BenchmarkProposeCommitted-4   	  149656	      7785 ns/op	    3403 B/op	      40 allocs/op
BenchmarkLocalGet-4           	85523744	        13.91 ns/op	       0 B/op	       0 allocs/op
BenchmarkLocalGet-4           	76714064	        13.92 ns/op	       0 B/op	       0 allocs/op
BenchmarkLocalGet-4           	83435048	        13.97 ns/op	       0 B/op	       0 allocs/op
PASS
ok  	github.com/hiroshi-os/quorumkv/internal/raft	13.286s
goos: linux
goarch: amd64
pkg: github.com/hiroshi-os/quorumkv/internal/kv
cpu: Intel(R) Xeon(R) Processor
BenchmarkGet-4   	86557372	        13.67 ns/op	       0 B/op	       0 allocs/op
BenchmarkGet-4   	87964214	        13.89 ns/op	       0 B/op	       0 allocs/op
BenchmarkGet-4   	86264715	        13.69 ns/op	       0 B/op	       0 allocs/op
PASS
ok  	github.com/hiroshi-os/quorumkv/internal/kv	3.635s
```

## How to read this

| bench | p50-ish | what it includes |
|---|---|---|
| `BenchmarkElection3` | ~28 ms | randomized election with 20–40 ms timeouts, in-process RPC |
| `BenchmarkProposeCommitted` | ~8 µs | majority commit + apply, memory log, no HTTP, no fsync |
| `BenchmarkLocalGet` / `kv.BenchmarkGet` | ~14 ns | one `sync.RWMutex` map read |

These are **not** docker-compose numbers. HTTP + JSON + WAL fsync is
dominated by disk and kernel, often 10–100× slower than the 8 µs
Propose. GET stays a local map read even over HTTP (plus one
`json.Encode`).

Election time in the 3-node local demo (250–400 ms timeouts) is
one timeout plus a vote RTT, typically a few hundred milliseconds
after a leader crash — see the chaos run, not this bench.

## Local 3-node HTTP + WAL + fsync (same host, 2026-09-13)

Process-per-node on `127.0.0.1:8081-8083`, `FileStorage` with fsync,
JSON-over-HTTP. 20 sequential curls after a leader existed:

| op | http | observed |
|---|---|---|
| `PUT /kv/bench` via leader | 200 | 1.4–2.4 ms (≈1.7 ms typical) |
| `GET /kv/city` on a follower | 200 | 0.15–0.50 ms (≈0.25 ms typical) |

That is the number to quote for the demo. The 8 µs in-process Propose
is the Raft state machine without the kernel.

