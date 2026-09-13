package raft

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testCfg(id string, peers []string, net *MemoryNetwork, store Storage, apply func([]byte)) Config {
	if store == nil {
		store = NewMemoryStorage()
	}
	return Config{
		ID:          id,
		PeerIDs:     peers,
		ElectionMin: 30 * time.Millisecond,
		ElectionMax: 60 * time.Millisecond,
		Heartbeat:   10 * time.Millisecond,
		Tick:        5 * time.Millisecond,
		Storage:     store,
		Transport:   net,
		Apply:       apply,
		Logger:      testLogger(),
	}
}

type cluster struct {
	net   *MemoryNetwork
	nodes []*Node
	fsms  []*memFSM
}

type memFSM struct {
	mu   sync.Mutex
	data map[string]string
}

func (m *memFSM) apply(cmd []byte) {
	// reuse EncodeSet JSON: {"op":"SET","key":...,"value":...}
	var kv struct {
		Op, Key, Value string
	}
	_ = json.Unmarshal(cmd, &kv)
	if kv.Op != "SET" {
		return
	}
	m.mu.Lock()
	if m.data == nil {
		m.data = map[string]string{}
	}
	m.data[kv.Key] = kv.Value
	m.mu.Unlock()
}

func (m *memFSM) get(k string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.data[k]
}

func startCluster(t *testing.T, n int) *cluster {
	t.Helper()
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = string(rune('a' + i))
	}
	net := NewMemoryNetwork()
	c := &cluster{net: net}
	for i := 0; i < n; i++ {
		var peers []string
		for j := 0; j < n; j++ {
			if i != j {
				peers = append(peers, ids[j])
			}
		}
		fsm := &memFSM{data: map[string]string{}}
		node := New(testCfg(ids[i], peers, net, nil, fsm.apply))
		net.Register(node)
		if err := node.Start(); err != nil {
			t.Fatal(err)
		}
		c.nodes = append(c.nodes, node)
		c.fsms = append(c.fsms, fsm)
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			n.Stop()
		}
	})
	return c
}

func (c *cluster) waitLeader(t *testing.T, timeout time.Duration) *Node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []*Node
		for _, n := range c.nodes {
			if n.Role() == Leader {
				leaders = append(leaders, n)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no unique leader (timeout %s)", timeout)
	return nil
}

func (c *cluster) living() []*Node {
	var out []*Node
	for _, n := range c.nodes {
		select {
		case <-n.stop:
		default:
			out = append(out, n)
		}
	}
	return out
}

func TestElectUniqueLeader(t *testing.T) {
	c := startCluster(t, 3)
	lead := c.waitLeader(t, 2*time.Second)
	term := lead.Term()
	time.Sleep(80 * time.Millisecond)
	var leaders int
	for _, n := range c.nodes {
		if n.Role() == Leader {
			leaders++
			if n.Term() != term && n.Term() < term {
				t.Fatalf("term went backwards")
			}
		}
	}
	if leaders != 1 {
		t.Fatalf("split brain: %d leaders", leaders)
	}
}

func TestProposeReplicates(t *testing.T) {
	c := startCluster(t, 3)
	lead := c.waitLeader(t, 2*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := lead.Propose(ctx, EncodeSet("k", "v")); err != nil {
		t.Fatal(err)
	}
	// Commit notification is pushed on the AE that advanced commitIndex;
	// followers should apply without waiting a heartbeat (~10ms here).
	deadline := time.Now().Add(80 * time.Millisecond)
	for time.Now().Before(deadline) {
		ok := true
		for _, fsm := range c.fsms {
			if fsm.get("k") != "v" {
				ok = false
			}
		}
		if ok {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("followers did not apply SET after commit push")
}

func TestKillLeaderElectsAndPreservesCommit(t *testing.T) {
	c := startCluster(t, 3)
	lead := c.waitLeader(t, 2*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := lead.Propose(ctx, EncodeSet("k", "before")); err != nil {
		t.Fatal(err)
	}

	c.net.Isolate(lead.id)
	lead.Stop()

	var survivor *Node
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var leaders []*Node
		for _, n := range c.nodes {
			if n.id == lead.id {
				continue
			}
			if n.Role() == Leader {
				leaders = append(leaders, n)
			}
		}
		if len(leaders) == 1 {
			survivor = leaders[0]
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if survivor == nil {
		t.Fatal("no new leader after kill")
	}
	if survivor.Term() <= lead.Term() {
		t.Fatalf("new term %d should exceed old %d", survivor.Term(), lead.Term())
	}

	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if c.fsms[indexOf(c, survivor)].get("k") == "before" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	if err := survivor.Propose(ctx2, EncodeSet("k", "after")); err != nil {
		t.Fatal(err)
	}
	if got := c.fsms[indexOf(c, survivor)].get("k"); got != "after" {
		t.Fatalf("got %q", got)
	}
}

func indexOf(c *cluster, n *Node) int {
	for i, x := range c.nodes {
		if x == n {
			return i
		}
	}
	return 0
}

func TestProposeRejectedOnFollower(t *testing.T) {
	c := startCluster(t, 3)
	lead := c.waitLeader(t, 2*time.Second)
	var fol *Node
	for _, n := range c.nodes {
		if n != lead {
			fol = n
			break
		}
	}
	err := fol.Propose(context.Background(), EncodeSet("k", "v"))
	var nl ErrNotLeader
	if err == nil {
		t.Fatal("expected not-leader")
	}
	if !asNotLeader(err, &nl) {
		t.Fatalf("want ErrNotLeader, got %v", err)
	}
	if nl.LeaderID != lead.id {
		t.Fatalf("leader hint %q want %q", nl.LeaderID, lead.id)
	}
}

func asNotLeader(err error, dest *ErrNotLeader) bool {
	e, ok := err.(ErrNotLeader)
	if !ok {
		return false
	}
	*dest = e
	return true
}

func TestCurrentTermCommitRule(t *testing.T) {
	net := NewMemoryNetwork()
	n := New(testCfg("a", []string{"b", "c"}, net, nil, nil))
	n.mu.Lock()
	n.role = Leader
	n.currentTerm = 4
	n.log.entries = []LogEntry{
		{},
		{Term: 2, Index: 1, Command: EncodeSet("x", "old")},
		{Term: 2, Index: 2, Command: EncodeSet("y", "old")},
	}
	n.matchIndex["b"] = 2
	n.matchIndex["c"] = 0
	n.maybeAdvanceCommitLocked()
	if n.commitIndex != 0 {
		t.Fatalf("must not commit previous-term entries (figure 8); commitIndex=%d", n.commitIndex)
	}
	n.log.Append(4, noopCommand)
	n.matchIndex["b"] = 3
	n.maybeAdvanceCommitLocked()
	if n.commitIndex != 3 {
		t.Fatalf("current-term majority should commit idx 3 (and prior); got %d", n.commitIndex)
	}
	n.mu.Unlock()
}

func TestConflictIndexFastRollback(t *testing.T) {
	// Follower log: 1,1,1,4,4   Leader log: 1,1,1,2,2,3
	// AE with prev=5 term=3 fails; conflictTerm=4 conflictIndex=4.
	// Leader has no term 4 → nextIndex = 4.
	fol := New(testCfg("f", []string{"l"}, NewMemoryNetwork(), nil, nil))
	fol.mu.Lock()
	fol.currentTerm = 4
	fol.log.entries = []LogEntry{
		{},
		{Term: 1, Index: 1},
		{Term: 1, Index: 2},
		{Term: 1, Index: 3},
		{Term: 4, Index: 4},
		{Term: 4, Index: 5},
	}
	fol.mu.Unlock()

	reply := fol.HandleAppendEntries(AppendEntriesArgs{
		Term:         5,
		LeaderID:     "l",
		PrevLogIndex: 5,
		PrevLogTerm:  3,
	})
	if reply.Success {
		t.Fatal("expected conflict")
	}
	if reply.ConflictTerm != 4 || reply.ConflictIndex != 4 {
		t.Fatalf("conflict term/index = %d/%d want 4/4", reply.ConflictTerm, reply.ConflictIndex)
	}

	lead := New(testCfg("l", []string{"f"}, NewMemoryNetwork(), nil, nil))
	lead.mu.Lock()
	lead.role = Leader
	lead.currentTerm = 5
	lead.log.entries = []LogEntry{
		{},
		{Term: 1, Index: 1},
		{Term: 1, Index: 2},
		{Term: 1, Index: 3},
		{Term: 2, Index: 4},
		{Term: 2, Index: 5},
		{Term: 3, Index: 6},
	}
	lead.nextIndex["f"] = 7
	lead.onAppendEntriesReplyLocked("f", AppendEntriesArgs{
		Term:         5,
		PrevLogIndex: 6,
		PrevLogTerm:  3,
	}, reply)
	if lead.nextIndex["f"] != 4 {
		t.Fatalf("nextIndex=%d want 4 (skip whole conflict term)", lead.nextIndex["f"])
	}
	lead.mu.Unlock()

	// Successful catch-up from prev=3.
	ok := fol.HandleAppendEntries(AppendEntriesArgs{
		Term:         5,
		LeaderID:     "l",
		PrevLogIndex: 3,
		PrevLogTerm:  1,
		Entries: []LogEntry{
			{Term: 2, Index: 4},
			{Term: 2, Index: 5},
			{Term: 3, Index: 6},
		},
	})
	if !ok.Success {
		t.Fatal("expected success after rewind")
	}
	fol.mu.Lock()
	if fol.log.LastIndex() != 6 || fol.log.TermAt(6) != 3 || fol.log.TermAt(4) != 2 {
		t.Fatalf("follower log not overwritten: last=%d t4=%d t6=%d",
			fol.log.LastIndex(), fol.log.TermAt(4), fol.log.TermAt(6))
	}
	fol.mu.Unlock()
}

func TestVoteRequiresUpToDateLog(t *testing.T) {
	n := New(testCfg("a", []string{"b"}, NewMemoryNetwork(), nil, nil))
	n.mu.Lock()
	n.currentTerm = 2
	n.log.entries = []LogEntry{{}, {Term: 2, Index: 1}}
	n.mu.Unlock()

	reply := n.HandleRequestVote(RequestVoteArgs{
		Term:         3,
		CandidateID:  "b",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if reply.VoteGranted {
		t.Fatal("stale log must not win the vote")
	}
	reply = n.HandleRequestVote(RequestVoteArgs{
		Term:         3,
		CandidateID:  "b",
		LastLogIndex: 1,
		LastLogTerm:  2,
	})
	if !reply.VoteGranted {
		t.Fatal("up-to-date candidate should get the vote")
	}
}

func TestWALRestartRecoversState(t *testing.T) {
	dir := t.TempDir()
	st, err := NewFileStorage(filepath.Join(dir, "n"), true)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SaveMeta(7, "n1"); err != nil {
		t.Fatal(err)
	}
	ents := []LogEntry{{Term: 7, Index: 1, Command: EncodeSet("a", "1")}}
	if err := st.Append(ents); err != nil {
		t.Fatal(err)
	}

	net := NewMemoryNetwork()
	n := New(testCfg("n1", []string{"n2"}, net, st, nil))
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	defer n.Stop()
	if n.Term() != 7 {
		t.Fatalf("term=%d", n.Term())
	}
	n.mu.Lock()
	if n.votedFor != "n1" || n.log.LastIndex() != 1 || n.log.TermAt(1) != 7 {
		t.Fatalf("wal replay failed: vote=%s last=%d term=%d", n.votedFor, n.log.LastIndex(), n.log.TermAt(1))
	}
	n.mu.Unlock()
}

func TestRejectStaleTermRPCs(t *testing.T) {
	n := New(testCfg("a", []string{"b"}, NewMemoryNetwork(), nil, nil))
	n.mu.Lock()
	n.currentTerm = 5
	n.mu.Unlock()
	vr := n.HandleRequestVote(RequestVoteArgs{Term: 4, CandidateID: "b"})
	if vr.VoteGranted || vr.Term != 5 {
		t.Fatalf("stale vote: %+v", vr)
	}
	ar := n.HandleAppendEntries(AppendEntriesArgs{Term: 4, LeaderID: "b"})
	if ar.Success || ar.Term != 5 {
		t.Fatalf("stale ae: %+v", ar)
	}
}
