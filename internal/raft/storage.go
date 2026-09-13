package raft

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// Storage persists currentTerm, votedFor, and the log. MemoryStorage is
// correct for tests and the in-process bench. FileStorage is a JSON-lines
// WAL used by the 3-node demo so a crashed node can rejoin.
type Storage interface {
	Load() (term uint64, votedFor string, entries []LogEntry, err error)
	SaveMeta(term uint64, votedFor string) error
	Append(entries []LogEntry) error
	ReplaceLog(entries []LogEntry) error
}

// MemoryStorage keeps Raft state in process memory only.
type MemoryStorage struct {
	mu       sync.Mutex
	term     uint64
	votedFor string
	entries  []LogEntry
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{}
}

func (s *MemoryStorage) Load() (uint64, string, []LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]LogEntry, len(s.entries))
	copy(out, s.entries)
	return s.term, s.votedFor, out, nil
}

func (s *MemoryStorage) SaveMeta(term uint64, votedFor string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.term = term
	s.votedFor = votedFor
	return nil
}

func (s *MemoryStorage) Append(entries []LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, entries...)
	return nil
}

func (s *MemoryStorage) ReplaceLog(entries []LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append([]LogEntry(nil), entries...)
	return nil
}

type metaFile struct {
	Term     uint64 `json:"term"`
	VotedFor string `json:"voted_for"`
}

// FileStorage is a durable WAL: meta.json (term/vote) + log.jsonl (entries).
// Truncate rewrites the log file; appends are write+fsync. Honest trade-off:
// fsync on every persist is correct and slow; DisableSync is for benches.
type FileStorage struct {
	dir  string
	sync bool
}

func NewFileStorage(dir string, fsync bool) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FileStorage{dir: dir, sync: fsync}, nil
}

func (s *FileStorage) metaPath() string { return filepath.Join(s.dir, "meta.json") }
func (s *FileStorage) logPath() string  { return filepath.Join(s.dir, "log.jsonl") }

func (s *FileStorage) Load() (uint64, string, []LogEntry, error) {
	var term uint64
	var votedFor string
	if b, err := os.ReadFile(s.metaPath()); err == nil {
		var m metaFile
		if err := json.Unmarshal(b, &m); err != nil {
			return 0, "", nil, err
		}
		term, votedFor = m.Term, m.VotedFor
	} else if !os.IsNotExist(err) {
		return 0, "", nil, err
	}

	f, err := os.Open(s.logPath())
	if err != nil {
		if os.IsNotExist(err) {
			return term, votedFor, nil, nil
		}
		return 0, "", nil, err
	}
	defer f.Close()

	var entries []LogEntry
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var e LogEntry
		if err := json.Unmarshal(line, &e); err != nil {
			return 0, "", nil, err
		}
		entries = append(entries, e)
	}
	if err := sc.Err(); err != nil {
		return 0, "", nil, err
	}
	return term, votedFor, entries, nil
}

func (s *FileStorage) SaveMeta(term uint64, votedFor string) error {
	b, err := json.Marshal(metaFile{Term: term, VotedFor: votedFor})
	if err != nil {
		return err
	}
	return s.atomicWrite(s.metaPath(), b)
}

func (s *FileStorage) Append(entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	f, err := os.OpenFile(s.logPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for _, e := range entries {
		if err := enc.Encode(e); err != nil {
			return err
		}
	}
	if s.sync {
		return f.Sync()
	}
	return nil
}

func (s *FileStorage) ReplaceLog(entries []LogEntry) error {
	var buf []byte
	for _, e := range entries {
		b, err := json.Marshal(e)
		if err != nil {
			return err
		}
		buf = append(buf, b...)
		buf = append(buf, '\n')
	}
	return s.atomicWrite(s.logPath(), buf)
}

func (s *FileStorage) atomicWrite(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if s.sync {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
