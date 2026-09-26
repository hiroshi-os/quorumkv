package raft

import (
	"context"
)

// pendingRead is one ReadIndex call waiting for a quorum of AppendEntries
// replies that were dispatched after the call began.
type pendingRead struct {
	term      uint64
	readIndex uint64
	barrier   uint64 // count a reply only when its send generation is greater
	acks      int
	got       map[string]struct{}
	ch        chan error
}

// applyWait is a caller blocked until lastApplied reaches index.
type applyWait struct {
	index uint64
	ch    chan struct{}
}

// alive reports whether Stop has been called. Safe without n.mu.
func (n *Node) alive() bool {
	select {
	case <-n.stop:
		return false
	default:
		return true
	}
}

func (n *Node) aliveLocked() bool { return n.alive() }

// ReadIndex implements Raft linearizable reads (Ongaro §6.4).
//
// The leader must already have committed an entry from its current term
// (the no-op appended on election). It records commitIndex, then exchanges
// a fresh AppendEntries round and waits for a quorum of acknowledgements
// so a partitioned ex-leader cannot serve the read. It then waits until
// lastApplied reaches the captured index. After a nil error, a local FSM
// read on this node is linearizable.
//
// The returned index is the commit index captured for this read.
func (n *Node) ReadIndex(ctx context.Context) (uint64, error) {
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		n.mu.Lock()
		if !n.aliveLocked() {
			n.mu.Unlock()
			return 0, ErrStopped
		}
		if n.role != Leader {
			leader := n.leaderID
			n.mu.Unlock()
			return 0, ErrNotLeader{LeaderID: leader}
		}
		if !n.currentTermCommittedLocked() {
			ch := make(chan struct{}, 1)
			n.commitWaiters = append(n.commitWaiters, ch)
			n.mu.Unlock()
			select {
			case <-ch:
				continue
			case <-ctx.Done():
				n.mu.Lock()
				n.removeCommitWaiterLocked(ch)
				n.mu.Unlock()
				return 0, ctx.Err()
			}
		}

		readIndex := n.commitIndex
		needPeers := n.quorum() - 1
		if needPeers <= 0 {
			n.mu.Unlock()
			if err := n.waitApplied(ctx, readIndex); err != nil {
				return 0, err
			}
			return readIndex, nil
		}

		pr := &pendingRead{
			term:      n.currentTerm,
			readIndex: readIndex,
			barrier:   n.aeGen,
			got:       make(map[string]struct{}),
			ch:        make(chan error, 1),
		}
		n.pendingReads = append(n.pendingReads, pr)
		// Dispatch a round strictly newer than pr.barrier. In-flight RPCs
		// sent at or before the barrier do not confirm leadership.
		n.broadcastAppendEntriesLocked()
		n.mu.Unlock()

		select {
		case err := <-pr.ch:
			if err != nil {
				return 0, err
			}
			if err := n.waitApplied(ctx, readIndex); err != nil {
				return 0, err
			}
			return readIndex, nil
		case <-ctx.Done():
			n.mu.Lock()
			select {
			case err := <-pr.ch:
				n.dropPendingReadLocked(pr)
				n.mu.Unlock()
				if err != nil {
					return 0, err
				}
				if err := n.waitApplied(ctx, readIndex); err != nil {
					return 0, err
				}
				return readIndex, nil
			default:
				n.dropPendingReadLocked(pr)
				n.mu.Unlock()
				return 0, ctx.Err()
			}
		}
	}
}

func (n *Node) currentTermCommittedLocked() bool {
	return n.commitIndex > 0 && n.log.TermAt(n.commitIndex) == n.currentTerm
}

func (n *Node) waitApplied(ctx context.Context, index uint64) error {
	n.mu.Lock()
	if n.lastApplied >= index {
		n.mu.Unlock()
		return nil
	}
	ch := make(chan struct{}, 1)
	n.applyWaiters = append(n.applyWaiters, applyWait{index: index, ch: ch})
	n.mu.Unlock()
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (n *Node) noteReadAckLocked(peer string, sendGen uint64) {
	if n.role != Leader {
		return
	}
	need := n.quorum() - 1
	rest := make([]*pendingRead, 0, len(n.pendingReads))
	for _, pr := range n.pendingReads {
		if pr.term != n.currentTerm {
			n.finishReadLocked(pr, ErrLostLeadership)
			continue
		}
		if sendGen > pr.barrier {
			if _, seen := pr.got[peer]; !seen {
				pr.got[peer] = struct{}{}
				pr.acks++
			}
		}
		if pr.acks >= need {
			n.finishReadLocked(pr, nil)
			continue
		}
		rest = append(rest, pr)
	}
	n.pendingReads = rest
}

func (n *Node) finishReadLocked(pr *pendingRead, err error) {
	select {
	case pr.ch <- err:
	default:
	}
}

func (n *Node) failPendingReadsLocked(err error) {
	for _, pr := range n.pendingReads {
		n.finishReadLocked(pr, err)
	}
	n.pendingReads = nil
}

func (n *Node) dropPendingReadLocked(pr *pendingRead) {
	rest := make([]*pendingRead, 0, len(n.pendingReads))
	for _, p := range n.pendingReads {
		if p != pr {
			rest = append(rest, p)
		}
	}
	n.pendingReads = rest
}

func (n *Node) signalCommitWaitersLocked() {
	waiters := n.commitWaiters
	n.commitWaiters = nil
	for _, ch := range waiters {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (n *Node) removeCommitWaiterLocked(ch chan struct{}) {
	rest := make([]chan struct{}, 0, len(n.commitWaiters))
	for _, c := range n.commitWaiters {
		if c != ch {
			rest = append(rest, c)
		}
	}
	n.commitWaiters = rest
}

func (n *Node) signalApplyWaitersLocked() {
	rest := make([]applyWait, 0, len(n.applyWaiters))
	for _, w := range n.applyWaiters {
		if n.lastApplied >= w.index {
			select {
			case w.ch <- struct{}{}:
			default:
			}
			continue
		}
		rest = append(rest, w)
	}
	n.applyWaiters = rest
}
