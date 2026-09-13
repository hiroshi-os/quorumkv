package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/hiroshi-os/quorumkv/internal/kv"
	"github.com/hiroshi-os/quorumkv/internal/raft"
	"github.com/hiroshi-os/quorumkv/internal/server"
)

func main() {
	id := flag.String("id", env("QUORUMKV_ID", "n1"), "node id")
	bind := flag.String("bind", env("QUORUMKV_BIND", ":8080"), "HTTP bind address")
	peers := flag.String("peers", env("QUORUMKV_PEERS", ""), "peer list: id=host:port,id=host:port")
	data := flag.String("data", env("QUORUMKV_DATA", ""), "WAL directory (empty = memory only)")
	noFsync := flag.Bool("no-fsync", env("QUORUMKV_NO_FSYNC", "") == "1", "skip WAL fsync (faster, unsafe)")
	electionMin := flag.Duration("election-min", durEnv("QUORUMKV_ELECTION_MIN", 250*time.Millisecond), "min election timeout")
	electionMax := flag.Duration("election-max", durEnv("QUORUMKV_ELECTION_MAX", 400*time.Millisecond), "max election timeout")
	heartbeat := flag.Duration("heartbeat", durEnv("QUORUMKV_HEARTBEAT", 50*time.Millisecond), "leader heartbeat interval")
	logLevel := flag.String("log-level", env("QUORUMKV_LOG_LEVEL", "info"), "debug|info|warn|error")
	flag.Parse()

	level := slog.LevelInfo
	switch strings.ToLower(*logLevel) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	peerURLs, peerIDs := parsePeers(*peers)
	store := kv.New()

	var storage raft.Storage = raft.NewMemoryStorage()
	if *data != "" {
		fs, err := raft.NewFileStorage(*data, !*noFsync)
		if err != nil {
			logger.Error("open wal", "err", err)
			os.Exit(1)
		}
		storage = fs
		logger.Info("wal enabled", "dir", *data, "fsync", !*noFsync)
	}

	trans := raft.NewHTTPTransport(peerURLs, 150*time.Millisecond)
	node := raft.New(raft.Config{
		ID:          *id,
		PeerIDs:     peerIDs,
		ElectionMin: *electionMin,
		ElectionMax: *electionMax,
		Heartbeat:   *heartbeat,
		Storage:     storage,
		Transport:   trans,
		Apply:       store.ApplyBytes,
		Logger:      logger,
	})
	if err := node.Start(); err != nil {
		logger.Error("raft start", "err", err)
		os.Exit(1)
	}

	srv := server.New(server.Config{
		Bind:        *bind,
		Node:        node,
		Store:       store,
		PeerURLs:    peerURLs,
		Logger:      logger,
		EnableCrash: true,
	})
	logger.Info("listening", "id", *id, "bind", *bind, "peers", peerIDs)

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errCh:
		if err != nil {
			logger.Error("http", "err", err)
			os.Exit(1)
		}
	case <-sig:
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func durEnv(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
	}
	return def
}

// parsePeers accepts "n2=127.0.0.1:8082,n3=n3:8080" and returns HTTP base URLs.
func parsePeers(s string) (map[string]string, []string) {
	urls := make(map[string]string)
	var ids []string
	if strings.TrimSpace(s) == "" {
		return urls, ids
	}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, addr, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		id, addr = strings.TrimSpace(id), strings.TrimSpace(addr)
		if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
			addr = "http://" + addr
		}
		urls[id] = addr
		ids = append(ids, id)
	}
	return urls, ids
}
