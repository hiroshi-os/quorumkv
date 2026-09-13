package raft

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

var errUnreachable = errors.New("peer unreachable")

// Transport is the Raft RPC client. Implementations must not call back
// into the sender while holding the sender's mutex — Node releases the
// lock before every RPC.
type Transport interface {
	RequestVote(peerID string, args RequestVoteArgs) (RequestVoteReply, error)
	AppendEntries(peerID string, args AppendEntriesArgs) (AppendEntriesReply, error)
}

// MemoryNetwork routes RPCs in-process. Used by unit tests and benches.
// Isolate(id) drops all RPCs to/from that node (partition / kill).
type MemoryNetwork struct {
	mu    sync.RWMutex
	nodes map[string]*Node
	drop  map[string]bool
}

func NewMemoryNetwork() *MemoryNetwork {
	return &MemoryNetwork{
		nodes: make(map[string]*Node),
		drop:  make(map[string]bool),
	}
}

func (m *MemoryNetwork) Register(n *Node) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[n.id] = n
}

func (m *MemoryNetwork) Isolate(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.drop[id] = true
}

func (m *MemoryNetwork) Heal(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.drop, id)
}

func (m *MemoryNetwork) lookup(from, to string) (*Node, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.drop[from] || m.drop[to] {
		return nil, errUnreachable
	}
	n := m.nodes[to]
	if n == nil {
		return nil, errUnreachable
	}
	return n, nil
}

func (m *MemoryNetwork) RequestVote(peerID string, args RequestVoteArgs) (RequestVoteReply, error) {
	n, err := m.lookup(args.CandidateID, peerID)
	if err != nil {
		return RequestVoteReply{}, err
	}
	return n.HandleRequestVote(args), nil
}

func (m *MemoryNetwork) AppendEntries(peerID string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	n, err := m.lookup(args.LeaderID, peerID)
	if err != nil {
		return AppendEntriesReply{}, err
	}
	return n.HandleAppendEntries(args), nil
}

// HTTPTransport talks JSON over HTTP to /raft/request_vote and /raft/append_entries.
type HTTPTransport struct {
	peers  map[string]string // id -> base URL, e.g. "http://n2:8080"
	client *http.Client
}

func NewHTTPTransport(peers map[string]string, timeout time.Duration) *HTTPTransport {
	if timeout <= 0 {
		timeout = 150 * time.Millisecond
	}
	return &HTTPTransport{
		peers: peers,
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 4,
				IdleConnTimeout:     10 * time.Second,
			},
		},
	}
}

func (t *HTTPTransport) RequestVote(peerID string, args RequestVoteArgs) (RequestVoteReply, error) {
	var reply RequestVoteReply
	if err := t.post(peerID, "/raft/request_vote", args, &reply); err != nil {
		return RequestVoteReply{}, err
	}
	return reply, nil
}

func (t *HTTPTransport) AppendEntries(peerID string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	var reply AppendEntriesReply
	if err := t.post(peerID, "/raft/append_entries", args, &reply); err != nil {
		return AppendEntriesReply{}, err
	}
	return reply, nil
}

func (t *HTTPTransport) post(peerID, path string, args, reply any) error {
	base, ok := t.peers[peerID]
	if !ok {
		return fmt.Errorf("unknown peer %s", peerID)
	}
	body, err := json.Marshal(args)
	if err != nil {
		return err
	}
	resp, err := t.client.Post(base+path, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		slurp, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return fmt.Errorf("peer %s: %s %s", peerID, resp.Status, slurp)
	}
	return json.NewDecoder(resp.Body).Decode(reply)
}
