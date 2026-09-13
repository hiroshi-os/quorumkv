package raft

// Log is an in-memory Raft log. entries[0] is a dummy {Term:0, Index:0}.
// Real entries occupy indices 1..len-1. The mutex lives on Node; Log is
// not safe for concurrent use on its own.
type Log struct {
	entries []LogEntry
}

func NewLog() *Log {
	return &Log{entries: []LogEntry{{Term: 0, Index: 0}}}
}

func (l *Log) LastIndex() uint64 {
	return uint64(len(l.entries) - 1)
}

func (l *Log) LastTerm() uint64 {
	return l.entries[len(l.entries)-1].Term
}

func (l *Log) TermAt(index uint64) uint64 {
	if index >= uint64(len(l.entries)) {
		return 0
	}
	return l.entries[index].Term
}

func (l *Log) At(index uint64) LogEntry {
	return l.entries[index]
}

func (l *Log) Append(term uint64, cmd []byte) LogEntry {
	e := LogEntry{
		Term:    term,
		Index:   l.LastIndex() + 1,
		Command: cmd,
	}
	l.entries = append(l.entries, e)
	return e
}

func (l *Log) AppendEntry(e LogEntry) {
	l.entries = append(l.entries, e)
}

// TruncateFrom drops entries at index and beyond (keeps 0..index-1).
func (l *Log) TruncateFrom(index uint64) {
	if index < 1 {
		index = 1
	}
	if index >= uint64(len(l.entries)) {
		return
	}
	l.entries = l.entries[:index]
}

// Slice returns a copy of entries in [from, to).
func (l *Log) Slice(from, to uint64) []LogEntry {
	n := uint64(len(l.entries))
	if from >= n || from >= to {
		return nil
	}
	if to > n {
		to = n
	}
	out := make([]LogEntry, to-from)
	copy(out, l.entries[from:to])
	return out
}

// LastIndexOfTerm returns the last index whose term equals t, or 0 if none.
func (l *Log) LastIndexOfTerm(t uint64) uint64 {
	for i := l.LastIndex(); i >= 1; i-- {
		term := l.entries[i].Term
		if term == t {
			return i
		}
		if term < t {
			return 0
		}
	}
	return 0
}

// FirstIndexOfTerm returns the first index of term t at or before hint.
func (l *Log) FirstIndexOfTerm(t, hint uint64) uint64 {
	if hint >= uint64(len(l.entries)) {
		hint = l.LastIndex()
	}
	i := hint
	for i > 1 && l.entries[i-1].Term == t {
		i--
	}
	return i
}

// RealEntries returns the log without the dummy sentinel (for persistence).
func (l *Log) RealEntries() []LogEntry {
	if len(l.entries) <= 1 {
		return nil
	}
	out := make([]LogEntry, len(l.entries)-1)
	copy(out, l.entries[1:])
	return out
}

func (l *Log) Replace(entries []LogEntry) {
	l.entries = []LogEntry{{Term: 0, Index: 0}}
	l.entries = append(l.entries, entries...)
}
