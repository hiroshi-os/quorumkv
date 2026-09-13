package raft

import "fmt"

// Role is the Raft server state machine.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// LogEntry is a single replicated command. Index 0 is a dummy sentinel
// (term 0) and is never applied to the FSM.
type LogEntry struct {
	Term    uint64 `json:"term"`
	Index   uint64 `json:"index"`
	Command []byte `json:"command"`
}

type RequestVoteArgs struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

type RequestVoteReply struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

type AppendEntriesArgs struct {
	Term         uint64     `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex uint64     `json:"prev_log_index"`
	PrevLogTerm  uint64     `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit uint64     `json:"leader_commit"`
}

// AppendEntriesReply includes the conflictIndex / conflictTerm optimization
// from Ongaro's dissertation §5.3 so a leader can skip a whole conflicting
// term instead of decrementing nextIndex one entry at a time.
type AppendEntriesReply struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	ConflictIndex uint64 `json:"conflict_index"`
	ConflictTerm  uint64 `json:"conflict_term"`
}

// Status is a snapshot of volatile + persistent Raft state for /status.
type Status struct {
	ID          string `json:"id"`
	Role        string `json:"role"`
	Term        uint64 `json:"term"`
	Leader      string `json:"leader"`
	VotedFor    string `json:"voted_for"`
	CommitIndex uint64 `json:"commit_index"`
	LastApplied uint64 `json:"last_applied"`
	LastLogIdx  uint64 `json:"last_log_index"`
	LastLogTerm uint64 `json:"last_log_term"`
	Peers       int    `json:"peers"`
}

// ErrNotLeader is returned by Propose when this node is not the leader.
type ErrNotLeader struct {
	LeaderID string
}

func (e ErrNotLeader) Error() string {
	if e.LeaderID == "" {
		return "not leader (leader unknown)"
	}
	return fmt.Sprintf("not leader (leader=%s)", e.LeaderID)
}

// ErrLostLeadership is returned to in-flight Propose calls after a step-down.
var ErrLostLeadership = fmt.Errorf("lost leadership before commit")
