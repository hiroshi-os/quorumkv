package raft

import (
	"context"
	"testing"
	"time"
)

func TestReadIndexSeesCommittedWrite(t *testing.T) {
	c := startCluster(t, 3)
	lead := c.waitLeader(t, 2*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := lead.Propose(ctx, EncodeSet("k", "v")); err != nil {
		t.Fatal(err)
	}
	idx, err := lead.ReadIndex(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if idx == 0 {
		t.Fatal("read index must be a committed log index")
	}
	st := lead.Status()
	if st.LastApplied < idx {
		t.Fatalf("applied %d < read index %d", st.LastApplied, idx)
	}
	if c.fsms[indexOf(c, lead)].get("k") != "v" {
		t.Fatalf("fsm = %q", c.fsms[indexOf(c, lead)].get("k"))
	}
}

func TestReadIndexRejectsFollower(t *testing.T) {
	c := startCluster(t, 3)
	lead := c.waitLeader(t, 2*time.Second)
	var fol *Node
	for _, n := range c.nodes {
		if n != lead {
			fol = n
			break
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := fol.ReadIndex(ctx)
	var nl ErrNotLeader
	if !asNotLeader(err, &nl) {
		t.Fatalf("got %v", err)
	}
	if nl.LeaderID != lead.ID() {
		t.Fatalf("leader hint %q want %q", nl.LeaderID, lead.ID())
	}
}

func TestReadIndexFailsWhenPartitionedFromQuorum(t *testing.T) {
	c := startCluster(t, 3)
	lead := c.waitLeader(t, 2*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := lead.Propose(ctx, EncodeSet("k", "v1")); err != nil {
		t.Fatal(err)
	}
	c.net.Partition([]string{lead.ID()})
	t.Cleanup(func() { c.net.HealPartitions() })

	rctx, rcancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer rcancel()
	if _, err := lead.ReadIndex(rctx); err == nil {
		t.Fatal("partitioned leader served ReadIndex without a quorum ack")
	}
	// The node still holds the old applied value; it must not have been returned
	// through ReadIndex. Local state remains readable for the non-linearizable path.
	if got := c.fsms[indexOf(c, lead)].get("k"); got != "v1" {
		t.Fatalf("leader fsm = %q", got)
	}
}

func TestCutDropsOnlyThatPair(t *testing.T) {
	c := startCluster(t, 3)
	c.net.Cut("a", "b")
	_, err := c.net.RequestVote("b", RequestVoteArgs{CandidateID: "a", Term: 1})
	if err == nil {
		t.Fatal("expected a—b cut to drop RequestVote")
	}
	_, err = c.net.RequestVote("c", RequestVoteArgs{CandidateID: "a", Term: 1})
	if err != nil {
		t.Fatalf("a—c should stay up: %v", err)
	}
	c.net.HealCut("a", "b")
	_, err = c.net.RequestVote("b", RequestVoteArgs{CandidateID: "a", Term: 1})
	if err != nil {
		t.Fatalf("healed link: %v", err)
	}
}

func TestPartitionElectsOnTheMajority(t *testing.T) {
	c := startCluster(t, 3)
	lead := c.waitLeader(t, 2*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := lead.Propose(ctx, EncodeSet("k", "before")); err != nil {
		t.Fatal(err)
	}
	c.net.Partition([]string{lead.ID()})
	t.Cleanup(func() { c.net.HealPartitions() })

	var neu *Node
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var leaders []*Node
		for _, n := range c.nodes {
			if n.ID() == lead.ID() {
				continue
			}
			if n.Role() == Leader {
				leaders = append(leaders, n)
			}
		}
		if len(leaders) == 1 {
			neu = leaders[0]
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if neu == nil {
		t.Fatal("majority did not elect under partition")
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	if err := neu.Propose(ctx2, EncodeSet("k", "after")); err != nil {
		t.Fatal(err)
	}
	if got := c.fsms[indexOf(c, neu)].get("k"); got != "after" {
		t.Fatalf("majority fsm = %q", got)
	}
	if got := c.fsms[indexOf(c, lead)].get("k"); got != "before" {
		t.Fatalf("partitioned leader fsm = %q, want stale before", got)
	}
}
