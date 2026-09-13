package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/hiroshi-os/quorumkv/internal/kv"
	"github.com/hiroshi-os/quorumkv/internal/raft"
)

// Server exposes Raft RPCs, the KV API, and a small admin surface.
type Server struct {
	node      *raft.Node
	store     *kv.Store
	peers     map[string]string // id -> base URL
	http      *http.Server
	logger    *slog.Logger
	proposeTO time.Duration
}

type Config struct {
	Bind        string
	Node        *raft.Node
	Store       *kv.Store
	PeerURLs    map[string]string
	Logger      *slog.Logger
	ProposeTO   time.Duration
	EnableCrash bool
}

func New(cfg Config) *Server {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.ProposeTO <= 0 {
		cfg.ProposeTO = 2 * time.Second
	}
	s := &Server{
		node:      cfg.Node,
		store:     cfg.Store,
		peers:     cfg.PeerURLs,
		logger:    cfg.Logger,
		proposeTO: cfg.ProposeTO,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /status", s.handleStatus)
	mux.HandleFunc("GET /kv/{key}", s.handleGet)
	mux.HandleFunc("PUT /kv/{key}", s.handlePut)
	mux.HandleFunc("POST /raft/request_vote", s.handleRequestVote)
	mux.HandleFunc("POST /raft/append_entries", s.handleAppendEntries)
	if cfg.EnableCrash {
		mux.HandleFunc("POST /admin/crash", s.handleCrash)
	}
	s.http = &http.Server{
		Addr:              cfg.Bind,
		Handler:           mux,
		ReadHeaderTimeout: 2 * time.Second,
	}
	return s
}

func (s *Server) Serve(lis net.Listener) error {
	s.http.Addr = lis.Addr().String()
	return s.http.Serve(lis)
}

func (s *Server) ListenAndServe() error {
	return s.http.ListenAndServe()
}

func (s *Server) Addr() string {
	return s.http.Addr
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.node.Stop()
	return s.http.Shutdown(ctx)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.node.Status())
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	val, ok := s.store.Get(key)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{
			"error":  "not_found",
			"key":    key,
			"stale":  true,
			"hint":   "GET is a local read of applied state; it may lag the leader",
			"status": s.node.Status(),
		})
		return
	}
	st := s.node.Status()
	writeJSON(w, http.StatusOK, map[string]any{
		"key":    key,
		"value":  val,
		"node":   st.ID,
		"role":   st.Role,
		"term":   st.Term,
		"leader": st.Leader,
		"note":   "local applied-state read; not a linearizable quorum read",
	})
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	value := strings.TrimSpace(string(body))
	if value == "" {
		var wrap struct {
			Value string `json:"value"`
		}
		if json.Unmarshal(body, &wrap) == nil {
			value = wrap.Value
		}
	}
	if value == "" {
		http.Error(w, "empty value", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.proposeTO)
	defer cancel()
	err = s.node.Propose(ctx, kv.EncodeSet(key, value))
	if err == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":    true,
			"key":   key,
			"value": value,
			"node":  s.node.ID(),
		})
		return
	}

	var nl raft.ErrNotLeader
	if errors.As(err, &nl) && r.Header.Get("X-QuorumKV-Forwarded") == "" && nl.LeaderID != "" {
		if url, ok := s.peers[nl.LeaderID]; ok {
			s.forwardPut(w, r, url, key, value)
			return
		}
	}
	st := http.StatusServiceUnavailable
	if errors.Is(err, context.DeadlineExceeded) {
		st = http.StatusGatewayTimeout
	}
	writeJSON(w, st, map[string]any{
		"error":  err.Error(),
		"leader": s.node.LeaderID(),
	})
}

func (s *Server) forwardPut(w http.ResponseWriter, r *http.Request, base, key, value string) {
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPut, strings.TrimRight(base, "/")+"/kv/"+key, strings.NewReader(value))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-QuorumKV-Forwarded", "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error(), "forwarded_to": base})
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-QuorumKV-Forwarded-To", base)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (s *Server) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	var args raft.RequestVoteArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, s.node.HandleRequestVote(args))
}

func (s *Server) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	var args raft.AppendEntriesArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, s.node.HandleAppendEntries(args))
}

func (s *Server) handleCrash(w http.ResponseWriter, _ *http.Request) {
	s.logger.Error("admin crash: exiting")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("crashing"))
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		os.Exit(1)
	}()
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
