package kv

import (
	"encoding/json"
	"sync"
)

// Command is the replicated FSM operation. GET is not a command — it is a
// local read of applied state (see DESIGN.md for consistency caveats).
type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Store is a thread-safe in-memory KV map driven by committed Raft entries.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

func New() *Store {
	return &Store{data: make(map[string]string)}
}

func (s *Store) ApplyBytes(cmd []byte) {
	var c Command
	if err := json.Unmarshal(cmd, &c); err != nil {
		return
	}
	s.Apply(c)
}

func (s *Store) Apply(c Command) {
	switch c.Op {
	case "SET":
		s.mu.Lock()
		s.data[c.Key] = c.Value
		s.mu.Unlock()
	case "NOOP", "":
		return
	}
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

func EncodeSet(key, value string) []byte {
	b, _ := json.Marshal(Command{Op: "SET", Key: key, Value: value})
	return b
}
