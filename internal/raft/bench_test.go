package raft

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func benchCluster(b *testing.B, n int) *cluster {
	b.Helper()
	// startCluster is in node_test.go (same package).
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
		node := New(Config{
			ID:          ids[i],
			PeerIDs:     peers,
			ElectionMin: 20 * time.Millisecond,
			ElectionMax: 40 * time.Millisecond,
			Heartbeat:   5 * time.Millisecond,
			Tick:        2 * time.Millisecond,
			Storage:     NewMemoryStorage(),
			Transport:   net,
			Apply:       fsm.apply,
			Logger:      testLogger(),
		})
		net.Register(node)
		if err := node.Start(); err != nil {
			b.Fatal(err)
		}
		c.nodes = append(c.nodes, node)
		c.fsms = append(c.fsms, fsm)
	}
	b.Cleanup(func() {
		for _, n := range c.nodes {
			n.Stop()
		}
	})
	return c
}

func waitLeaderB(b *testing.B, c *cluster) *Node {
	b.Helper()
	deadline := time.Now().Add(2 * time.Second)
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
		time.Sleep(2 * time.Millisecond)
	}
	b.Fatal("no leader")
	return nil
}

func BenchmarkElection3(b *testing.B) {
	for i := 0; i < b.N; i++ {
		c := benchCluster(b, 3)
		_ = waitLeaderB(b, c)
		for _, n := range c.nodes {
			n.Stop()
		}
	}
}

func BenchmarkProposeCommitted(b *testing.B) {
	c := benchCluster(b, 3)
	lead := waitLeaderB(b, c)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := lead.Propose(ctx, EncodeSet("k", fmt.Sprintf("%d", i))); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLocalGet(b *testing.B) {
	fsm := &memFSM{data: map[string]string{"k": "v"}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if fsm.get("k") != "v" {
			b.Fatal("miss")
		}
	}
}
