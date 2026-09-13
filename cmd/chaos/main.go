package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

type status struct {
	ID     string `json:"id"`
	Role   string `json:"role"`
	Term   uint64 `json:"term"`
	Leader string `json:"leader"`
}

type kvResp struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Error string `json:"error"`
	OK    bool   `json:"ok"`
}

func main() {
	addrs := flag.String("addrs", env("QUORUMKV_ADDRS", "http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083"), "comma-separated node base URLs")
	key := flag.String("key", "chaos", "key to write")
	timeout := flag.Duration("timeout", 15*time.Second, "overall deadline")
	flag.Parse()

	nodes := splitAddrs(*addrs)
	if len(nodes) < 2 {
		fatalf("need at least 2 node addrs")
	}
	deadline := time.Now().Add(*timeout)
	client := &http.Client{Timeout: 2 * time.Second}

	fmt.Println("== quorumkv chaos: wait for initial leader ==")
	leader, leaderURL := mustLeader(client, nodes, deadline)
	fmt.Printf("leader %s at %s term=%d\n", leader.ID, leaderURL, leader.Term)

	fmt.Println("== SET chaos=before via leader ==")
	mustPut(client, leaderURL, *key, "before")
	mustGetEverywhere(client, nodes, *key, "before", deadline)

	fmt.Println("== kill leader via POST /admin/crash ==")
	crash(client, leaderURL)
	killed := leader.ID
	fmt.Printf("crashed %s\n", killed)

	fmt.Println("== wait for automatic re-election ==")
	remaining := without(nodes, leaderURL)
	newLeader, newURL := mustLeader(client, remaining, deadline)
	if newLeader.ID == killed {
		fatalf("dead node still reporting as leader")
	}
	fmt.Printf("new leader %s at %s term=%d\n", newLeader.ID, newURL, newLeader.Term)

	fmt.Println("== GET after failover (committed prefix must survive) ==")
	mustGetEverywhere(client, remaining, *key, "before", deadline)

	fmt.Println("== SET chaos=after via new leader ==")
	mustPut(client, newURL, *key, "after")
	mustGetEverywhere(client, remaining, *key, "after", deadline)

	fmt.Println("OK: kill-leader recovered; SET/GET still consistent on survivors")
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func splitAddrs(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if !strings.HasPrefix(p, "http://") && !strings.HasPrefix(p, "https://") {
			p = "http://" + p
		}
		out = append(out, strings.TrimRight(p, "/"))
	}
	return out
}

func without(addrs []string, drop string) []string {
	var out []string
	for _, a := range addrs {
		if a != drop {
			out = append(out, a)
		}
	}
	return out
}

func mustLeader(c *http.Client, addrs []string, deadline time.Time) (status, string) {
	for time.Now().Before(deadline) {
		var leaders []struct {
			st  status
			url string
		}
		for _, u := range addrs {
			st, err := getStatus(c, u)
			if err != nil {
				continue
			}
			if st.Role == "leader" {
				leaders = append(leaders, struct {
					st  status
					url string
				}{st, u})
			}
		}
		if len(leaders) == 1 {
			return leaders[0].st, leaders[0].url
		}
		time.Sleep(50 * time.Millisecond)
	}
	fatalf("timed out waiting for a unique leader among %v", addrs)
	return status{}, ""
}

func getStatus(c *http.Client, base string) (status, error) {
	resp, err := c.Get(base + "/status")
	if err != nil {
		return status{}, err
	}
	defer resp.Body.Close()
	var st status
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return status{}, err
	}
	return st, nil
}

func mustPut(c *http.Client, base, key, value string) {
	req, err := http.NewRequest(http.MethodPut, base+"/kv/"+key, strings.NewReader(value))
	if err != nil {
		fatalf("put: %v", err)
	}
	resp, err := c.Do(req)
	if err != nil {
		fatalf("put %s: %v", base, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		fatalf("put %s: %s %s", base, resp.Status, body)
	}
	fmt.Printf("PUT %s/%s=%s -> %s\n", base, key, value, strings.TrimSpace(string(body)))
}

func mustGetEverywhere(c *http.Client, addrs []string, key, want string, deadline time.Time) {
	for _, u := range addrs {
		var last string
		ok := false
		for time.Now().Before(deadline) {
			val, err := getKV(c, u, key)
			if err == nil && val == want {
				fmt.Printf("GET %s/%s=%s\n", u, key, val)
				ok = true
				break
			}
			if err != nil {
				last = err.Error()
			} else {
				last = val
			}
			time.Sleep(20 * time.Millisecond)
		}
		if !ok {
			fatalf("GET %s/%s: want %q last=%s", u, key, want, last)
		}
	}
}

func getKV(c *http.Client, base, key string) (string, error) {
	resp, err := c.Get(base + "/kv/" + key)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var kv kvResp
	if err := json.NewDecoder(resp.Body).Decode(&kv); err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("%s %s", resp.Status, kv.Error)
	}
	return kv.Value, nil
}

func crash(c *http.Client, base string) {
	resp, err := c.Post(base+"/admin/crash", "text/plain", nil)
	if err != nil {
		// connection reset after crash is success
		fmt.Printf("crash %s: %v (expected if process died mid-response)\n", base, err)
		return
	}
	_ = resp.Body.Close()
}

func fatalf(f string, a ...any) {
	fmt.Fprintf(os.Stderr, "chaos FAIL: "+f+"\n", a...)
	os.Exit(1)
}
