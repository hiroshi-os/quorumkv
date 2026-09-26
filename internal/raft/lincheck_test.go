package raft

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

// kvIn / kvOut are the client-visible register operations checked by porcupine.
// Unknown puts may or may not have committed (timeout or lost leadership).
// Failed gets are omitted from histories: the client observed no value.
type kvIn struct {
	Op    string
	Key   string
	Value string
}

type kvOut struct {
	Value   string
	Ok      bool
	Unknown bool
}

func kvLinearizabilityModel() porcupine.Model {
	nm := porcupine.NondeterministicModel{
		Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
			m := make(map[string][]porcupine.Operation)
			for _, op := range history {
				k := op.Input.(kvIn).Key
				m[k] = append(m[k], op)
			}
			keys := make([]string, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			parts := make([][]porcupine.Operation, 0, len(keys))
			for _, k := range keys {
				parts = append(parts, m[k])
			}
			return parts
		},
		Init: func() []interface{} { return []interface{}{""} },
		Step: func(state, input, output interface{}) []interface{} {
			st := state.(string)
			in := input.(kvIn)
			out := output.(kvOut)
			switch in.Op {
			case "put":
				if out.Unknown {
					if st == in.Value {
						return []interface{}{st}
					}
					return []interface{}{st, in.Value}
				}
				if out.Ok {
					return []interface{}{in.Value}
				}
				return []interface{}{st}
			case "get":
				if out.Unknown || !out.Ok {
					return []interface{}{st}
				}
				if out.Value == st {
					return []interface{}{st}
				}
				return nil
			default:
				return nil
			}
		},
		Hash: func(state interface{}) uint64 {
			h := fnv.New64a()
			_, _ = h.Write([]byte(state.(string)))
			return h.Sum64()
		},
		DescribeOperation: func(input, output interface{}) string {
			in := input.(kvIn)
			out := output.(kvOut)
			if in.Op == "get" {
				if !out.Ok || out.Unknown {
					return fmt.Sprintf("get(%s) -> fail", in.Key)
				}
				return fmt.Sprintf("get(%s) -> %q", in.Key, out.Value)
			}
			switch {
			case out.Unknown:
				return fmt.Sprintf("put(%s,%q) -> unknown", in.Key, in.Value)
			case out.Ok:
				return fmt.Sprintf("put(%s,%q) -> ok", in.Key, in.Value)
			default:
				return fmt.Sprintf("put(%s,%q) -> fail", in.Key, in.Value)
			}
		},
	}
	return nm.ToModel()
}

func TestPorcupineFlagsStaleRegisterHistory(t *testing.T) {
	ops := []porcupine.Operation{
		{ClientId: 0, Input: kvIn{Op: "put", Key: "k", Value: "1"}, Call: 1, Output: kvOut{Ok: true}, Return: 2},
		{ClientId: 1, Input: kvIn{Op: "put", Key: "k", Value: "2"}, Call: 3, Output: kvOut{Ok: true}, Return: 4},
		{ClientId: 1, Input: kvIn{Op: "get", Key: "k"}, Call: 5, Output: kvOut{Ok: true, Value: "2"}, Return: 6},
		{ClientId: 0, Input: kvIn{Op: "get", Key: "k"}, Call: 7, Output: kvOut{Ok: true, Value: "1"}, Return: 8},
	}
	if porcupine.CheckOperations(kvLinearizabilityModel(), ops) {
		t.Fatal("hand-built stale read was accepted as linearizable")
	}
}

func TestPorcupineAcceptsLinearRegisterHistory(t *testing.T) {
	ops := []porcupine.Operation{
		{ClientId: 0, Input: kvIn{Op: "put", Key: "k", Value: "1"}, Call: 1, Output: kvOut{Ok: true}, Return: 2},
		{ClientId: 1, Input: kvIn{Op: "get", Key: "k"}, Call: 3, Output: kvOut{Ok: true, Value: "1"}, Return: 4},
		{ClientId: 0, Input: kvIn{Op: "put", Key: "k", Value: "2"}, Call: 5, Output: kvOut{Ok: true}, Return: 6},
		{ClientId: 1, Input: kvIn{Op: "get", Key: "k"}, Call: 7, Output: kvOut{Ok: true, Value: "2"}, Return: 8},
	}
	if !porcupine.CheckOperations(kvLinearizabilityModel(), ops) {
		t.Fatal("sequential register history was rejected")
	}
}

func TestLocalGetNotLinearizableUnderPartition(t *testing.T) {
	c := startCluster(t, 3)
	lead := c.waitLeader(t, 2*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var clk logicalClock
	call := clk.now()
	if err := lead.Propose(ctx, EncodeSet("k", "v1")); err != nil {
		t.Fatal(err)
	}
	ops := []porcupine.Operation{{
		ClientId: 0,
		Input:    kvIn{Op: "put", Key: "k", Value: "v1"},
		Output:   kvOut{Ok: true},
		Call:     call,
		Return:   clk.now(),
	}}
	waitKey(t, c, "k", "v1")

	c.net.Partition([]string{lead.ID()})
	t.Cleanup(c.net.HealPartitions)

	neu := waitOtherLeader(t, c, lead.ID(), 2*time.Second)
	call = clk.now()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	if err := neu.Propose(ctx2, EncodeSet("k", "v2")); err != nil {
		t.Fatal(err)
	}
	ops = append(ops, porcupine.Operation{
		ClientId: 1,
		Input:    kvIn{Op: "put", Key: "k", Value: "v2"},
		Output:   kvOut{Ok: true},
		Call:     call,
		Return:   clk.now(),
	})

	call = clk.now()
	gotNew := c.fsms[indexOf(c, neu)].get("k")
	ops = append(ops, porcupine.Operation{
		ClientId: 1,
		Input:    kvIn{Op: "get", Key: "k"},
		Output:   kvOut{Ok: true, Value: gotNew},
		Call:     call,
		Return:   clk.now(),
	})
	call = clk.now()
	gotOld := c.fsms[indexOf(c, lead)].get("k")
	ops = append(ops, porcupine.Operation{
		ClientId: 0,
		Input:    kvIn{Op: "get", Key: "k"},
		Output:   kvOut{Ok: true, Value: gotOld},
		Call:     call,
		Return:   clk.now(),
	})
	if gotNew != "v2" || gotOld != "v1" {
		t.Fatalf("want majority v2 and stale local v1, got %q and %q", gotNew, gotOld)
	}
	if porcupine.CheckOperations(kvLinearizabilityModel(), ops) {
		t.Fatal("old local GET under partition was linearizable; porcupine did not flag it")
	}
}

func TestReadIndexHistoryLinearizableUnderPartition(t *testing.T) {
	c := startCluster(t, 3)
	lead := c.waitLeader(t, 2*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var clk logicalClock
	call := clk.now()
	if err := lead.Propose(ctx, EncodeSet("k", "v1")); err != nil {
		t.Fatal(err)
	}
	ops := []porcupine.Operation{{
		ClientId: 0,
		Input:    kvIn{Op: "put", Key: "k", Value: "v1"},
		Output:   kvOut{Ok: true},
		Call:     call,
		Return:   clk.now(),
	}}
	waitKey(t, c, "k", "v1")
	c.net.Partition([]string{lead.ID()})
	t.Cleanup(c.net.HealPartitions)

	neu := waitOtherLeader(t, c, lead.ID(), 2*time.Second)
	call = clk.now()
	if err := neu.Propose(ctx, EncodeSet("k", "v2")); err != nil {
		t.Fatal(err)
	}
	ops = append(ops, porcupine.Operation{
		ClientId: 1,
		Input:    kvIn{Op: "put", Key: "k", Value: "v2"},
		Output:   kvOut{Ok: true},
		Call:     call,
		Return:   clk.now(),
	})

	rctx, rcancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	_, staleErr := lead.ReadIndex(rctx)
	rcancel()
	if staleErr == nil {
		t.Fatal("partitioned ex-leader completed ReadIndex")
	}

	call = clk.now()
	if _, err := neu.ReadIndex(ctx); err != nil {
		t.Fatal(err)
	}
	got := c.fsms[indexOf(c, neu)].get("k")
	ops = append(ops, porcupine.Operation{
		ClientId: 1,
		Input:    kvIn{Op: "get", Key: "k"},
		Output:   kvOut{Ok: true, Value: got},
		Call:     call,
		Return:   clk.now(),
	})
	if got != "v2" {
		t.Fatalf("consistent read = %q", got)
	}
	if !porcupine.CheckOperations(kvLinearizabilityModel(), ops) {
		t.Fatalf("ReadIndex history not linearizable:\n%s", formatOps(ops))
	}
}

func TestPorcupineChaosShort(t *testing.T) {
	n := getenvInt("QUORUMKV_PORCUPINE_HISTORIES", 4)
	ok, illegal, unknown := runConsistentHistories(t, n)
	if ok != n || illegal != 0 || unknown != 0 {
		t.Fatalf("short chaos: histories=%d pass=%d illegal=%d unknown=%d", n, ok, illegal, unknown)
	}
}

// TestFullLinearizability is the measurement run recorded in bench/RESULTS.md.
// It checks QUORUMKV_HISTORIES (default 500) randomized consistent-read
// histories and QUORUMKV_STALE_HISTORIES (default 100) local-GET histories.
func TestFullLinearizability(t *testing.T) {
	if os.Getenv("QUORUMKV_FULL_HISTORIES") == "" {
		t.Skip("set QUORUMKV_FULL_HISTORIES=1 to run the 500-history measurement")
	}
	n := getenvInt("QUORUMKV_HISTORIES", 500)
	staleN := getenvInt("QUORUMKV_STALE_HISTORIES", 100)
	if n < 500 {
		t.Fatalf("QUORUMKV_HISTORIES=%d, want at least 500", n)
	}
	ok, illegal, unknown := runConsistentHistories(t, n)
	sOK, sIllegal, sUnknown := runStaleHistories(t, staleN)
	fmt.Printf("LIN_RESULT consistent_histories=%d consistent_pass=%d consistent_illegal=%d consistent_unknown=%d stale_histories=%d stale_pass=%d stale_violations=%d stale_unknown=%d\n",
		n, ok, illegal, unknown, staleN, sOK, sIllegal, sUnknown)
	if ok != n || illegal != 0 || unknown != 0 {
		t.Fatalf("consistent reads: pass=%d illegal=%d unknown=%d", ok, illegal, unknown)
	}
	if sIllegal < 1 {
		t.Fatal("old local GET produced no non-linearizable history; checker was not shown a violation")
	}
	if sOK+sIllegal+sUnknown != staleN {
		t.Fatalf("stale counts do not add up: ok=%d illegal=%d unknown=%d", sOK, sIllegal, sUnknown)
	}
}

func runConsistentHistories(t *testing.T, n int) (ok, illegal, unknown int) {
	t.Helper()
	return runHistories(t, n, true)
}

func runStaleHistories(t *testing.T, n int) (ok, illegal, unknown int) {
	t.Helper()
	return runHistories(t, n, false)
}

func runHistories(t *testing.T, n int, consistent bool) (ok, illegal, unknown int) {
	t.Helper()
	model := kvLinearizabilityModel()
	for i := 0; i < n; i++ {
		seed := time.Now().UnixNano() + int64(i)*10007
		var ops []porcupine.Operation
		var used int64
		for attempt := 0; attempt < 4; attempt++ {
			used = seed + int64(attempt)*17
			ops = runRandomHistory(t, used, consistent)
			if historyUseful(ops) {
				break
			}
		}
		if !historyUseful(ops) {
			t.Fatalf("seed %d: history never recorded a successful put and get (consistent=%v, ops=%d)", used, consistent, len(ops))
		}
		res := porcupine.CheckOperationsTimeout(model, ops, 5*time.Second)
		switch res {
		case porcupine.Ok:
			ok++
		case porcupine.Illegal:
			illegal++
			if consistent {
				t.Fatalf("consistent history %d seed %d not linearizable\n%s", i, used, formatOps(ops))
			}
		default:
			unknown++
			if consistent {
				t.Fatalf("consistent history %d seed %d checker timed out (%d ops)", i, used, len(ops))
			}
		}
		if (i+1)%25 == 0 || i+1 == n {
			fmt.Printf("lincheck consistent=%v %d/%d pass=%d illegal=%d unknown=%d\n", consistent, i+1, n, ok, illegal, unknown)
		}
	}
	return ok, illegal, unknown
}

func historyUseful(ops []porcupine.Operation) bool {
	var puts, gets int
	for _, op := range ops {
		in := op.Input.(kvIn)
		out := op.Output.(kvOut)
		if in.Op == "put" && out.Ok {
			puts++
		}
		if in.Op == "get" && out.Ok {
			gets++
		}
	}
	return puts >= 1 && gets >= 1
}

type logicalClock struct{ n atomic.Int64 }

func (c *logicalClock) now() int64 { return c.n.Add(1) }

type recorder struct {
	mu  sync.Mutex
	ops []porcupine.Operation
}

func (r *recorder) add(op porcupine.Operation) {
	r.mu.Lock()
	r.ops = append(r.ops, op)
	r.mu.Unlock()
}

func (r *recorder) snapshot() []porcupine.Operation {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]porcupine.Operation, len(r.ops))
	copy(out, r.ops)
	return out
}

func startLinCluster(t *testing.T) *cluster {
	t.Helper()
	ids := []string{"a", "b", "c"}
	net := NewMemoryNetwork()
	c := &cluster{net: net}
	for i := range ids {
		var peers []string
		for j := range ids {
			if i != j {
				peers = append(peers, ids[j])
			}
		}
		fsm := &memFSM{data: map[string]string{}}
		node := New(Config{
			ID:          ids[i],
			PeerIDs:     peers,
			ElectionMin: 15 * time.Millisecond,
			ElectionMax: 30 * time.Millisecond,
			Heartbeat:   5 * time.Millisecond,
			Tick:        2 * time.Millisecond,
			Storage:     NewMemoryStorage(),
			Transport:   net,
			Apply:       fsm.apply,
			Logger:      testLogger(),
		})
		net.Register(node)
		if err := node.Start(); err != nil {
			t.Fatal(err)
		}
		c.nodes = append(c.nodes, node)
		c.fsms = append(c.fsms, fsm)
	}
	return c
}

func stopCluster(c *cluster) {
	for _, n := range c.nodes {
		n.Stop()
	}
}

func runRandomHistory(t *testing.T, seed int64, consistent bool) []porcupine.Operation {
	t.Helper()
	c := startLinCluster(t)
	defer stopCluster(c)
	_ = c.waitLeader(t, 2*time.Second)

	var rec recorder
	var clk logicalClock
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const clients = 4
	const opsEach = 8
	var wg sync.WaitGroup
	wg.Add(clients + 1)
	for id := 0; id < clients; id++ {
		rng := rand.New(rand.NewSource(seed + int64(id)*997))
		go func(id int, rng *rand.Rand) {
			defer wg.Done()
			runClient(c, &rec, &clk, rng, id, opsEach, consistent)
		}(id, rng)
	}
	go func() {
		defer wg.Done()
		runChaos(ctx, c, rand.New(rand.NewSource(seed^0x5a17)))
	}()
	wg.Wait()
	c.net.HealPartitions()
	return rec.snapshot()
}

func runClient(c *cluster, rec *recorder, clk *logicalClock, rng *rand.Rand, id, nops int, consistent bool) {
	keys := []string{"a", "b"}
	for i := 0; i < nops; i++ {
		key := keys[rng.Intn(len(keys))]
		if rng.Intn(2) == 0 {
			val := fmt.Sprintf("c%d-%d", id, i)
			opCtx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			call := clk.now()
			err := c.propose(opCtx, EncodeSet(key, val))
			ret := clk.now()
			cancel()
			out := kvOut{}
			switch classifyPut(err) {
			case putOK:
				out.Ok = true
			case putUnknown:
				out.Unknown = true
			default:
				continue
			}
			rec.add(porcupine.Operation{
				ClientId: id,
				Input:    kvIn{Op: "put", Key: key, Value: val},
				Output:   out,
				Call:     call,
				Return:   ret,
			})
			continue
		}
		if consistent {
			opCtx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			call := clk.now()
			node, err := c.consistentRead(opCtx)
			var out kvOut
			if err == nil {
				out.Ok = true
				out.Value = c.fsms[indexOf(c, node)].get(key)
			}
			ret := clk.now()
			cancel()
			if !out.Ok {
				continue
			}
			rec.add(porcupine.Operation{
				ClientId: id,
				Input:    kvIn{Op: "get", Key: key},
				Output:   out,
				Call:     call,
				Return:   ret,
			})
			continue
		}
		nodes := c.readable()
		if len(nodes) == 0 {
			continue
		}
		node := nodes[rng.Intn(len(nodes))]
		call := clk.now()
		val := c.fsms[indexOf(c, node)].get(key)
		ret := clk.now()
		rec.add(porcupine.Operation{
			ClientId: id,
			Input:    kvIn{Op: "get", Key: key},
			Output:   kvOut{Ok: true, Value: val},
			Call:     call,
			Return:   ret,
		})
	}
}

type putClass int

const (
	putOK putClass = iota
	putDefiniteFail
	putUnknown
)

func classifyPut(err error) putClass {
	if err == nil {
		return putOK
	}
	var nl ErrNotLeader
	if errors.As(err, &nl) || errors.Is(err, ErrStopped) {
		return putDefiniteFail
	}
	return putUnknown
}

var shuffleSeed atomic.Int64

func shuffleNodes(nodes []*Node) {
	rng := rand.New(rand.NewSource(shuffleSeed.Add(1)))
	rng.Shuffle(len(nodes), func(i, j int) { nodes[i], nodes[j] = nodes[j], nodes[i] })
}

func (c *cluster) propose(ctx context.Context, cmd []byte) error {
	nodes := c.living()
	shuffleNodes(nodes)
	var unknown error
	sawDefinite := false
	for _, n := range nodes {
		if ctx.Err() != nil {
			if unknown != nil {
				return unknown
			}
			return ctx.Err()
		}
		attempt, cancel := context.WithTimeout(ctx, 40*time.Millisecond)
		err := n.Propose(attempt, cmd)
		cancel()
		if err == nil {
			return nil
		}
		var nl ErrNotLeader
		if errors.As(err, &nl) || errors.Is(err, ErrStopped) {
			sawDefinite = true
			if errors.As(err, &nl) && nl.LeaderID != "" {
				if leader := c.livingByID(nl.LeaderID); leader != nil && leader != n {
					attempt2, cancel2 := context.WithTimeout(ctx, 40*time.Millisecond)
					err2 := leader.Propose(attempt2, cmd)
					cancel2()
					if err2 == nil {
						return nil
					}
					var nl2 ErrNotLeader
					if errors.As(err2, &nl2) || errors.Is(err2, ErrStopped) {
						continue
					}
					unknown = err2
				}
			}
			continue
		}
		unknown = err
	}
	if unknown != nil {
		return unknown
	}
	if sawDefinite {
		return ErrNotLeader{}
	}
	return ErrStopped
}

func (c *cluster) consistentRead(ctx context.Context) (*Node, error) {
	nodes := c.living()
	shuffleNodes(nodes)
	var last error = ErrNotLeader{}
	for _, n := range nodes {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if n.Role() != Leader {
			continue
		}
		attempt, cancel := context.WithTimeout(ctx, 40*time.Millisecond)
		_, err := n.ReadIndex(attempt)
		cancel()
		if err == nil {
			return n, nil
		}
		last = err
		var nl ErrNotLeader
		if errors.As(err, &nl) || errors.Is(err, ErrStopped) || errors.Is(err, ErrLostLeadership) {
			continue
		}
	}
	return nil, last
}

func (c *cluster) livingByID(id string) *Node {
	for _, n := range c.living() {
		if n.ID() == id {
			return n
		}
	}
	return nil
}

func (c *cluster) readable() []*Node {
	return c.living()
}

func runChaos(ctx context.Context, c *cluster, rng *rand.Rand) {
	if !sleepCtx(ctx, time.Duration(8+rng.Intn(12))*time.Millisecond) {
		return
	}
	var lead *Node
	for _, n := range c.living() {
		if n.Role() == Leader {
			lead = n
			break
		}
	}
	if lead == nil {
		return
	}
	switch rng.Intn(3) {
	case 0:
		c.net.Partition([]string{lead.ID()})
		sleepCtx(ctx, time.Duration(40+rng.Intn(25))*time.Millisecond)
		c.net.HealPartitions()
	case 1:
		c.net.Isolate(lead.ID())
		lead.Stop()
	default:
		peers := append([]string(nil), lead.cfg.PeerIDs...)
		if len(peers) > 0 {
			peer := peers[rng.Intn(len(peers))]
			c.net.Cut(lead.ID(), peer)
			sleepCtx(ctx, 15*time.Millisecond)
			c.net.HealCut(lead.ID(), peer)
		}
		c.net.Partition([]string{lead.ID()})
		sleepCtx(ctx, time.Duration(30+rng.Intn(20))*time.Millisecond)
		c.net.HealPartitions()
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func waitKey(t *testing.T, c *cluster, key, val string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, fsm := range c.fsms {
			if fsm.get(key) != val {
				ok = false
			}
		}
		if ok {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("cluster did not apply %s=%s", key, val)
}

func waitOtherLeader(t *testing.T, c *cluster, exclude string, timeout time.Duration) *Node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []*Node
		for _, n := range c.nodes {
			if n.ID() == exclude {
				continue
			}
			if n.Role() == Leader {
				leaders = append(leaders, n)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("no majority leader")
	return nil
}

func formatOps(ops []porcupine.Operation) string {
	var b string
	for _, op := range ops {
		b += fmt.Sprintf("  c%d [%d,%d] %s\n", op.ClientId, op.Call, op.Return, kvLinearizabilityModel().DescribeOperation(op.Input, op.Output))
	}
	return b
}

func getenvInt(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return n
}
