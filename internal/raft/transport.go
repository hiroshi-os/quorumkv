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
//
// Two failure modes:
//   - Isolate(id) drops every RPC to or from that node (a kill, or a
//     node cut off from the whole cluster).
//   - Partition / Cut drop specific pairs. Partition(group) severs every
//     link between group and the other registered nodes while leaving
//     links inside each side up, so both sides can still elect if they
//     hold a quorum.
type MemoryNetwork struct {
	mu    sync.RWMutex
	nodes map[string]*Node
	drop  map[string]bool
	cut   map[string]bool // canonical pair key -> link is partitioned
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

func pairKey(a, b string) string {
	if a > b {
		a, b = b, a
	}
	return a + "\x00" + b
}

// Cut drops RPCs between a and b in both directions.
func (m *MemoryNetwork) Cut(a, b string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cut == nil {
		m.cut = make(map[string]bool)
	}
	m.cut[pairKey(a, b)] = true
}

// HealCut restores RPCs between a and b.
func (m *MemoryNetwork) HealCut(a, b string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.cut, pairKey(a, b))
}

// Partition severs every link between group and nodes outside it.
// Links among group, and among the complement, stay up.
func (m *MemoryNetwork) Partition(group []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cut == nil {
		m.cut = make(map[string]bool)
	}
	in := make(map[string]bool, len(group))
	for _, id := range group {
		in[id] = true
	}
	for id := range m.nodes {
		if in[id] {
			continue
		}
		for g := range in {
			m.cut[pairKey(id, g)] = true
		}
	}
}

// HealPartitions clears pair cuts. Isolate flags are left alone.
func (m *MemoryNetwork) HealPartitions() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cut = make(map[string]bool)
}

func (m *MemoryNetwork) lookup(from, to string) (*Node, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.drop[from] || m.drop[to] || m.cut[pairKey(from, to)] {
		return nil, errUnreachable
	}
	n := m.nodes[to]
	if n == nil {
		return nil, errUnreachable
	}
	select {
	case <-n.stop:
		return nil, errUnreachable
	default:
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
