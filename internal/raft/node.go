package raft

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

const maxEntriesPerRPC = 64

// noopCommand is appended when a node becomes leader so the current-term
// commit rule (Raft Figure 8) can commit leftover previous-term entries.
var noopCommand = []byte(`{"op":"NOOP"}`)

// Config tunes timeouts and wires storage, transport, and the FSM apply hook.
type Config struct {
	ID          string
	PeerIDs     []string
	ElectionMin time.Duration
	ElectionMax time.Duration
	Heartbeat   time.Duration
	Tick        time.Duration
	Storage     Storage
	Transport   Transport
	Apply       func(cmd []byte)
	Logger      *slog.Logger
}

func (c *Config) defaults() {
	if c.ElectionMin <= 0 {
		c.ElectionMin = 250 * time.Millisecond
	}
	if c.ElectionMax <= 0 {
		c.ElectionMax = 400 * time.Millisecond
	}
	if c.ElectionMax <= c.ElectionMin {
		c.ElectionMax = c.ElectionMin + 150*time.Millisecond
	}
	if c.Heartbeat <= 0 {
		c.Heartbeat = 50 * time.Millisecond
	}
	if c.Tick <= 0 {
		c.Tick = 10 * time.Millisecond
	}
	if c.Storage == nil {
		c.Storage = NewMemoryStorage()
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// Node is a single Raft server. All mutable state is guarded by mu.
// RPCs are issued without holding mu.
type Node struct {
	mu  sync.Mutex
	id  string
	cfg Config
	log *Log

	// persistent
	currentTerm uint64
	votedFor    string

	// volatile
	commitIndex uint64
	lastApplied uint64
	role        Role
	leaderID    string

	// leader volatile
	nextIndex  map[string]uint64
	matchIndex map[string]uint64
	aeInflight map[string]bool
	aePending  map[string]bool

	electionTimeout   time.Duration
	lastActivity      time.Time
	lastHeartbeatSent time.Time
	votes             map[string]bool
	waiters           map[uint64]chan error

	storage Storage
	trans   Transport
	apply   func([]byte)
	logger  *slog.Logger

	stop    chan struct{}
	stopped sync.Once
}

func New(cfg Config) *Node {
	cfg.defaults()
	return &Node{
		id:         cfg.ID,
		cfg:        cfg,
		log:        NewLog(),
		role:       Follower,
		nextIndex:  make(map[string]uint64),
		matchIndex: make(map[string]uint64),
		aeInflight: make(map[string]bool),
		aePending:  make(map[string]bool),
		waiters:    make(map[uint64]chan error),
		storage:    cfg.Storage,
		trans:      cfg.Transport,
		apply:      cfg.Apply,
		logger:     cfg.Logger.With("node", cfg.ID),
		stop:       make(chan struct{}),
	}
}

func (n *Node) ID() string { return n.id }

func (n *Node) Start() error {
	term, votedFor, entries, err := n.storage.Load()
	if err != nil {
		return err
	}
	n.mu.Lock()
	n.currentTerm = term
	n.votedFor = votedFor
	if len(entries) > 0 {
		n.log.Replace(entries)
	}
	n.resetElectionTimeoutLocked()
	n.lastActivity = time.Now()
	n.mu.Unlock()
	n.logger.Info("started", "term", term, "log_index", n.log.LastIndex())
	go n.loop()
	return nil
}

func (n *Node) Stop() {
	n.stopped.Do(func() { close(n.stop) })
}

func (n *Node) loop() {
	ticker := time.NewTicker(n.cfg.Tick)
	defer ticker.Stop()
	for {
		select {
		case <-n.stop:
			return
		case <-ticker.C:
			n.onTick()
		}
	}
}

func (n *Node) onTick() {
	n.mu.Lock()
	defer n.mu.Unlock()
	now := time.Now()
	switch n.role {
	case Leader:
		if now.Sub(n.lastHeartbeatSent) >= n.cfg.Heartbeat {
			n.broadcastAppendEntriesLocked()
			n.lastHeartbeatSent = now
		}
	case Follower, Candidate:
		if now.Sub(n.lastActivity) >= n.electionTimeout {
			n.startElectionLocked()
		}
	}
}

func (n *Node) resetElectionTimeoutLocked() {
	span := int64(n.cfg.ElectionMax - n.cfg.ElectionMin)
	n.electionTimeout = n.cfg.ElectionMin + time.Duration(rand.Int63n(span+1))
}

func (n *Node) quorum() int {
	return (len(n.cfg.PeerIDs)+1)/2 + 1
}

func (n *Node) startElectionLocked() {
	n.currentTerm++
	n.role = Candidate
	n.votedFor = n.id
	n.leaderID = ""
	n.votes = map[string]bool{n.id: true}
	n.lastActivity = time.Now()
	n.resetElectionTimeoutLocked()
	if err := n.storage.SaveMeta(n.currentTerm, n.votedFor); err != nil {
		n.logger.Error("persist meta", "err", err)
	}
	args := RequestVoteArgs{
		Term:         n.currentTerm,
		CandidateID:  n.id,
		LastLogIndex: n.log.LastIndex(),
		LastLogTerm:  n.log.LastTerm(),
	}
	n.logger.Info("election started", "term", n.currentTerm, "timeout", n.electionTimeout)
	if len(n.votes) >= n.quorum() {
		n.becomeLeaderLocked()
		return
	}
	for _, p := range n.cfg.PeerIDs {
		peer := p
		go n.sendRequestVote(peer, args)
	}
}

func (n *Node) sendRequestVote(peer string, args RequestVoteArgs) {
	reply, err := n.trans.RequestVote(peer, args)
	if err != nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if reply.Term > n.currentTerm {
		n.becomeFollowerLocked(reply.Term)
		return
	}
	if n.role != Candidate || args.Term != n.currentTerm {
		return
	}
	if reply.VoteGranted {
		n.votes[peer] = true
		if len(n.votes) >= n.quorum() {
			n.becomeLeaderLocked()
		}
	}
}

func (n *Node) becomeLeaderLocked() {
	n.role = Leader
	n.leaderID = n.id
	last := n.log.LastIndex()
	for _, p := range n.cfg.PeerIDs {
		n.nextIndex[p] = last + 1
		n.matchIndex[p] = 0
		n.aeInflight[p] = false
		n.aePending[p] = false
	}
	// Commit a current-term no-op so previous-term entries can be committed
	// (Raft Figure 8 / current-term commit rule).
	e := n.log.Append(n.currentTerm, noopCommand)
	if err := n.storage.Append([]LogEntry{e}); err != nil {
		n.logger.Error("persist noop", "err", err)
	}
	n.logger.Info("became leader", "term", n.currentTerm, "log_index", e.Index)
	n.lastHeartbeatSent = time.Time{}
	n.broadcastAppendEntriesLocked()
}

func (n *Node) becomeFollowerLocked(term uint64) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
		if err := n.storage.SaveMeta(n.currentTerm, n.votedFor); err != nil {
			n.logger.Error("persist meta", "err", err)
		}
	}
	if n.role == Leader {
		n.failWaitersLocked(ErrLostLeadership)
	}
	n.role = Follower
}

func (n *Node) failWaitersLocked(err error) {
	for idx, ch := range n.waiters {
		select {
		case ch <- err:
		default:
		}
		delete(n.waiters, idx)
	}
}

func (n *Node) broadcastAppendEntriesLocked() {
	for _, p := range n.cfg.PeerIDs {
		n.sendAppendEntriesLocked(p)
	}
}

func (n *Node) sendAppendEntriesLocked(peer string) {
	if n.role != Leader {
		return
	}
	if n.aeInflight[peer] {
		n.aePending[peer] = true
		return
	}
	n.aeInflight[peer] = true
	n.aePending[peer] = false
	next := n.nextIndex[peer]
	if next < 1 {
		next = 1
	}
	prevIdx := next - 1
	prevTerm := n.log.TermAt(prevIdx)
	end := n.log.LastIndex() + 1
	if end > next+maxEntriesPerRPC {
		end = next + maxEntriesPerRPC
	}
	args := AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      n.log.Slice(next, end),
		LeaderCommit: n.commitIndex,
	}
	go func() {
		reply, err := n.trans.AppendEntries(peer, args)
		n.mu.Lock()
		defer n.mu.Unlock()
		n.aeInflight[peer] = false
		if n.role != Leader {
			return
		}
		if err != nil {
			if n.aePending[peer] {
				n.sendAppendEntriesLocked(peer)
			}
			return
		}
		n.onAppendEntriesReplyLocked(peer, args, reply)
		if n.role == Leader && n.aePending[peer] {
			n.sendAppendEntriesLocked(peer)
		}
	}()
}

func (n *Node) onAppendEntriesReplyLocked(peer string, args AppendEntriesArgs, reply AppendEntriesReply) {
	if reply.Term > n.currentTerm {
		n.becomeFollowerLocked(reply.Term)
		return
	}
	if args.Term != n.currentTerm {
		return
	}
	if reply.Success {
		matched := args.PrevLogIndex + uint64(len(args.Entries))
		if matched > n.matchIndex[peer] {
			n.matchIndex[peer] = matched
		}
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		oldCommit := n.commitIndex
		n.maybeAdvanceCommitLocked()
		// Push commitIndex as soon as it moves, and catch up any peer
		// whose in-flight RPC still carried a stale LeaderCommit.
		if n.commitIndex > oldCommit {
			n.broadcastAppendEntriesLocked()
		} else if n.nextIndex[peer] <= n.log.LastIndex() || n.commitIndex > args.LeaderCommit {
			n.sendAppendEntriesLocked(peer)
		}
		return
	}

	// conflictIndex fast rollback (dissertation §5.3).
	if reply.ConflictTerm == 0 {
		n.nextIndex[peer] = max(1, reply.ConflictIndex)
	} else if last := n.log.LastIndexOfTerm(reply.ConflictTerm); last > 0 {
		n.nextIndex[peer] = last + 1
	} else {
		n.nextIndex[peer] = max(1, reply.ConflictIndex)
	}
	n.sendAppendEntriesLocked(peer)
}

// maybeAdvanceCommitLocked implements quorum commit plus the current-term
// commit rule: a leader only commits an entry if it is stored on a majority
// AND its term equals currentTerm. Earlier terms ride along once a
// current-term index is chosen (Raft Figure 8).
func (n *Node) maybeAdvanceCommitLocked() {
	if n.role != Leader {
		return
	}
	for idx := n.log.LastIndex(); idx > n.commitIndex; idx-- {
		if n.log.TermAt(idx) != n.currentTerm {
			continue
		}
		count := 1
		for _, p := range n.cfg.PeerIDs {
			if n.matchIndex[p] >= idx {
				count++
			}
		}
		if count >= n.quorum() {
			n.commitIndex = idx
			n.applyCommittedLocked()
			return
		}
	}
}

func (n *Node) applyCommittedLocked() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		e := n.log.At(n.lastApplied)
		if n.apply != nil && len(e.Command) > 0 {
			n.apply(e.Command)
		}
		if ch, ok := n.waiters[n.lastApplied]; ok {
			select {
			case ch <- nil:
			default:
			}
			delete(n.waiters, n.lastApplied)
		}
	}
}

// HandleRequestVote is the RequestVote RPC handler (Raft §5.2 + §5.4.1).
func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		return RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
	}
	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	}

	upToDate := args.LastLogTerm > n.log.LastTerm() ||
		(args.LastLogTerm == n.log.LastTerm() && args.LastLogIndex >= n.log.LastIndex())

	if (n.votedFor == "" || n.votedFor == args.CandidateID) && upToDate {
		n.votedFor = args.CandidateID
		if err := n.storage.SaveMeta(n.currentTerm, n.votedFor); err != nil {
			n.logger.Error("persist vote", "err", err)
		}
		n.lastActivity = time.Now()
		n.logger.Info("voted", "term", n.currentTerm, "for", args.CandidateID)
		return RequestVoteReply{Term: n.currentTerm, VoteGranted: true}
	}
	return RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
}

// HandleAppendEntries is the AppendEntries RPC handler (Raft §5.3 + conflictIndex).
func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		return AppendEntriesReply{Term: n.currentTerm, Success: false}
	}
	if args.Term > n.currentTerm || n.role == Candidate {
		n.becomeFollowerLocked(args.Term)
	}
	n.leaderID = args.LeaderID
	n.lastActivity = time.Now()

	lastIdx := n.log.LastIndex()
	if args.PrevLogIndex > lastIdx {
		return AppendEntriesReply{
			Term:          n.currentTerm,
			Success:       false,
			ConflictIndex: lastIdx + 1,
			ConflictTerm:  0,
		}
	}
	if args.PrevLogIndex > 0 && n.log.TermAt(args.PrevLogIndex) != args.PrevLogTerm {
		conflictTerm := n.log.TermAt(args.PrevLogIndex)
		conflictIndex := n.log.FirstIndexOfTerm(conflictTerm, args.PrevLogIndex)
		return AppendEntriesReply{
			Term:          n.currentTerm,
			Success:       false,
			ConflictIndex: conflictIndex,
			ConflictTerm:  conflictTerm,
		}
	}

	truncated := false
	for i, e := range args.Entries {
		idx := args.PrevLogIndex + uint64(i) + 1
		if idx <= n.log.LastIndex() {
			if n.log.TermAt(idx) != e.Term {
				n.log.TruncateFrom(idx)
				truncated = true
				n.log.AppendEntry(e)
			}
			continue
		}
		n.log.AppendEntry(e)
	}
	if truncated {
		if err := n.storage.ReplaceLog(n.log.RealEntries()); err != nil {
			n.logger.Error("rewrite log", "err", err)
		}
	} else if len(args.Entries) > 0 {
		// Persist only newly appended suffix.
		firstNew := args.PrevLogIndex + 1
		if firstNew <= n.log.LastIndex() {
			// Count how many we actually appended at the end this call.
			// Safer: persist any entry whose index was not already present
			// before this RPC. We track that with lastIdx captured above.
			var fresh []LogEntry
			for i, e := range args.Entries {
				idx := args.PrevLogIndex + uint64(i) + 1
				if idx > lastIdx {
					fresh = append(fresh, e)
				}
			}
			if len(fresh) > 0 {
				if err := n.storage.Append(fresh); err != nil {
					n.logger.Error("append log", "err", err)
				}
			}
		}
	}

	lastNew := args.PrevLogIndex + uint64(len(args.Entries))
	if args.LeaderCommit > n.commitIndex {
		n.commitIndex = min(args.LeaderCommit, lastNew)
		n.applyCommittedLocked()
	}
	return AppendEntriesReply{Term: n.currentTerm, Success: true}
}

// Propose appends a command on the leader and blocks until it is committed
// (applied locally) or ctx expires / leadership is lost.
func (n *Node) Propose(ctx context.Context, cmd []byte) error {
	n.mu.Lock()
	if n.role != Leader {
		leader := n.leaderID
		n.mu.Unlock()
		return ErrNotLeader{LeaderID: leader}
	}
	e := n.log.Append(n.currentTerm, cmd)
	if err := n.storage.Append([]LogEntry{e}); err != nil {
		n.mu.Unlock()
		return err
	}
	ch := make(chan error, 1)
	n.waiters[e.Index] = ch
	n.broadcastAppendEntriesLocked()
	n.lastHeartbeatSent = time.Now()
	n.mu.Unlock()

	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		n.mu.Lock()
		delete(n.waiters, e.Index)
		n.mu.Unlock()
		return ctx.Err()
	}
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID:          n.id,
		Role:        n.role.String(),
		Term:        n.currentTerm,
		Leader:      n.leaderID,
		VotedFor:    n.votedFor,
		CommitIndex: n.commitIndex,
		LastApplied: n.lastApplied,
		LastLogIdx:  n.log.LastIndex(),
		LastLogTerm: n.log.LastTerm(),
		Peers:       len(n.cfg.PeerIDs),
	}
}

func (n *Node) Role() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

func (n *Node) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

func (n *Node) Term() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

func (n *Node) CommitIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

// EncodeSet is a helper used by tests and the HTTP layer.
func EncodeSet(key, value string) []byte {
	b, _ := json.Marshal(map[string]string{"op": "SET", "key": key, "value": value})
	return b
}
