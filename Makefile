.PHONY: build test bench cluster chaos stop fmt

build:
	mkdir -p bin
	go build -o bin/quorumkv ./cmd/quorumkv
	go build -o bin/chaos ./cmd/chaos

fmt:
	gofmt -w cmd internal

test:
	go test ./...

bench:
	go test ./internal/raft ./internal/kv -bench=. -benchmem -count=3

cluster: build
	./scripts/local-cluster.sh fresh

chaos: build
	./scripts/local-cluster.sh chaos

stop:
	./scripts/local-cluster.sh stop
