.PHONY: build test bench cluster chaos stop fmt fmt-check vet test-race porcupine

build:
	mkdir -p bin
	go build -o bin/quorumkv ./cmd/quorumkv
	go build -o bin/chaos ./cmd/chaos

fmt:
	gofmt -w cmd internal

fmt-check:
	@out=$$(gofmt -l cmd internal); \
	if [ -n "$$out" ]; then echo "$$out"; exit 1; fi

vet:
	go vet ./...

test:
	go test ./...

test-race:
	go test -race ./...

porcupine:
	QUORUMKV_FULL_HISTORIES=1 go test -count=1 -timeout 45m -v -run TestFullLinearizability ./internal/raft/

bench:
	go test ./internal/raft ./internal/kv -bench=. -benchmem -count=3

cluster: build
	./scripts/local-cluster.sh fresh

chaos: build
	./scripts/local-cluster.sh chaos

stop:
	./scripts/local-cluster.sh stop
