package server

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/hiroshi-os/quorumkv/internal/kv"
	"github.com/hiroshi-os/quorumkv/internal/raft"
)

type httpCluster struct {
	nodes []*raft.Node
	srvs  []*Server
	lns   []net.Listener
	urls  []string
	ids   []string
}

func startHTTPCluster(t *testing.T, n int) *httpCluster {
	t.Helper()
	ids := make([]string, n)
	lns := make([]net.Listener, n)
	urls := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("n%d", i+1)
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns[i] = ln
		urls[i] = "http://" + ln.Addr().String()
	}

	peerIDs := func(self int) []string {
		var p []string
		for i := 0; i < n; i++ {
			if i != self {
				p = append(p, ids[i])
			}
		}
		return p
	}
	peerURLs := func(self int) map[string]string {
		m := map[string]string{}
		for i := 0; i < n; i++ {
			if i != self {
				m[ids[i]] = urls[i]
			}
		}
		return m
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := &httpCluster{lns: lns, urls: urls, ids: ids}
	for i := 0; i < n; i++ {
		store := kv.New()
		node := raft.New(raft.Config{
			ID:          ids[i],
			PeerIDs:     peerIDs(i),
			ElectionMin: 40 * time.Millisecond,
			ElectionMax: 80 * time.Millisecond,
			Heartbeat:   15 * time.Millisecond,
			Tick:        5 * time.Millisecond,
			Storage:     raft.NewMemoryStorage(),
			Transport:   raft.NewHTTPTransport(peerURLs(i), 100*time.Millisecond),
			Apply:       store.ApplyBytes,
			Logger:      logger,
		})
		if err := node.Start(); err != nil {
			t.Fatal(err)
		}
		srv := New(Config{
			Bind:        lns[i].Addr().String(),
			Node:        node,
			Store:       store,
			PeerURLs:    peerURLs(i),
			Logger:      logger,
			ProposeTO:   3 * time.Second,
			EnableCrash: false,
		})
		go func(s *Server, ln net.Listener) { _ = s.Serve(ln) }(srv, lns[i])
		c.nodes = append(c.nodes, node)
		c.srvs = append(c.srvs, srv)
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			n.Stop()
		}
		for _, ln := range c.lns {
			_ = ln.Close()
		}
	})
	return c
}

func (c *httpCluster) waitLeader(t *testing.T) (int, raft.Status) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var found []int
		var st raft.Status
		for i, u := range c.urls {
			s, err := fetchStatus(u)
			if err != nil {
				continue
			}
			if s.Role == "leader" {
				found = append(found, i)
				st = s
			}
		}
		if len(found) == 1 {
			return found[0], st
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no unique HTTP leader")
	return 0, raft.Status{}
}

func fetchStatus(base string) (raft.Status, error) {
	resp, err := http.Get(base + "/status")
	if err != nil {
		return raft.Status{}, err
	}
	defer resp.Body.Close()
	var st raft.Status
	err = json.NewDecoder(resp.Body).Decode(&st)
	return st, err
}

func TestHTTPElectSetGet(t *testing.T) {
	c := startHTTPCluster(t, 3)
	li, st := c.waitLeader(t)
	if st.Term < 1 {
		t.Fatalf("bad term %+v", st)
	}

	req, _ := http.NewRequest(http.MethodPut, c.urls[li]+"/kv/city", strings.NewReader("osaka"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("put %s", resp.Status)
	}

	// GET on every node, including followers (local applied-state read).
	deadline := time.Now().Add(2 * time.Second)
	for _, u := range c.urls {
		ok := false
		for time.Now().Before(deadline) {
			r, err := http.Get(u + "/kv/city")
			if err != nil {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			r.Body.Close()
			if r.StatusCode == http.StatusOK && body["value"] == "osaka" {
				ok = true
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if !ok {
			t.Fatalf("GET %s never saw osaka", u)
		}
	}

	// Follower PUT should forward to leader.
	var fi int
	for i := range c.urls {
		if i != li {
			fi = i
			break
		}
	}
	req, _ = http.NewRequest(http.MethodPut, c.urls[fi]+"/kv/city", strings.NewReader("kyoto"))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("forwarded put %s", resp.Status)
	}
}

func TestHTTPKillLeader(t *testing.T) {
	c := startHTTPCluster(t, 3)
	li, old := c.waitLeader(t)

	req, _ := http.NewRequest(http.MethodPut, c.urls[li]+"/kv/k", strings.NewReader("v1"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatal(resp.Status)
	}

	c.nodes[li].Stop()
	_ = c.lns[li].Close()

	deadline := time.Now().Add(3 * time.Second)
	var newID string
	for time.Now().Before(deadline) {
		var leaders []string
		for i, u := range c.urls {
			if i == li {
				continue
			}
			s, err := fetchStatus(u)
			if err == nil && s.Role == "leader" {
				leaders = append(leaders, s.ID)
			}
		}
		if len(leaders) == 1 && leaders[0] != old.ID {
			newID = leaders[0]
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if newID == "" {
		t.Fatal("no successor leader")
	}

	var live int
	for i := range c.urls {
		if i != li {
			live = i
			break
		}
	}
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		r, err := http.Get(c.urls[live] + "/kv/k")
		if err == nil {
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			r.Body.Close()
			if r.StatusCode == 200 && body["value"] == "v1" {
				return
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatal("committed value lost after leader kill")
}
