# quorumkv design

A from-scratch Raft key-value store. Not a wrapper around etcd/raft,
hashicorp/raft, or any other consensus library. Complements a cache
(ringcache): **cache is not consensus**. This process is the source of
truth; a cache in front of it is a performance layer with its own
invalidation problem.

## Safety properties (what “no split-brain” actually means)

Raft (Ongaro 2014) gives four interlocking guarantees. quorumkv
implements the rules that produce them; it does not add a fifth.

1. **Election safety.** At most one leader per term. A vote is granted
   at most once per term (`votedFor`), persisted before the RPC
   returns. A candidate needs a majority. Two majorities in a cluster
   of 2f+1 intersect, so two candidates cannot both win the same term.
2. **Leader append-only.** A leader never overwrites or deletes its own
   log. Conflicts are resolved by followers truncating *their* suffix
   when an AppendEntries prev-log check fails.
3. **Log matching.** If two logs have an entry with the same index and
   term, they are identical from that point back to the start. Enforced
   by the prevLogIndex/prevLogTerm check on every AppendEntries.
4. **Leader completeness / state-machine safety.** A committed entry is
   present on every future leader (up-to-date-log vote rule, §5.4.1)
   and is applied in the same order on every FSM.

**Split-brain is prevented by majority vote + terms, not by fencing
tokens or a lock service.** A partitioned minority can still *think* it
is leader until its election timer fires or it sees a higher term. It
cannot commit: `maybeAdvanceCommit` requires `matchIndex` on a quorum.
Client SETs on that minority time out. That is the honest picture —
“no split-brain writes,” not “no stale leader process.”

## Current-term commit rule (Figure 8)

A leader **must not** mark a previous-term entry committed just because
it is stored on a majority. The classic interleaving:

1. S1 (term 2) replicates an entry to itself only, then dies.
2. S5 (term 3) writes a different entry at the same index, then dies.
3. S1 returns, wins term 4, and replicates its *term-2* entry to a
   majority. Counting replicas would commit it.
4. S1 dies; S5 returns with a conflicting committed history.

quorumkv only advances `commitIndex` to an index `N` when
`log[N].term == currentTerm` **and** a quorum has `matchIndex >= N`.
Previous-term entries become committed only as a prefix of that `N`.

On becoming leader we append a `NOOP` in the new term so leftover
previous-term entries can be committed without waiting for a client
write. The FSM ignores `NOOP`.

## conflictIndex fast rollback

Blind `nextIndex--` on a failed AppendEntries is correct and slow.
Followers return:

- `conflictIndex = lastIndex+1`, `conflictTerm = 0` if their log is
  too short to contain `prevLogIndex`;
- otherwise `conflictTerm = log[prevLogIndex].term` and
  `conflictIndex` = first index of that term.

The leader then:

- if it has `conflictTerm`, set `nextIndex = lastIndexOf(conflictTerm)+1`;
- else set `nextIndex = conflictIndex`.

This skips a whole conflicting term in one RTT (dissertation §5.3).

## Replication and heartbeats

Leaders send AppendEntries on a heartbeat interval (default 50ms) and
immediately after a client Propose. Empty AppendEntries are heartbeats
and reset follower election timers. Election timeouts are randomized
in `[ElectionMin, ElectionMax]` (default 250–400ms) so split votes
usually dissolve on the next round.

RPC is JSON-over-HTTP. That is a deliberate MVP trade-off: easy to
debug with curl, easy to compose, higher latency than a binary
framing. The algorithm does not care.

## FSM and the KV API

`SET` is a Raft command. The leader appends, waits until the entry is
applied locally (which happens only after commit), then returns 200.
Followers apply when `leaderCommit` advances in AppendEntries.

`GET` is **not** a Raft command. It reads the local applied map on
whatever node received the request. Followers are allowed to serve
GET. This is documented, not accidental.

```
PUT /kv/{key}     SET via leader (followers forward once)
GET /kv/{key}     local applied-state read
GET /status       role, term, leader, commit/applied indexes
GET /health
POST /admin/crash chaos hook (process exits)
```

## Read consistency caveats (read this)

| Read | Linearizable? | What you can observe |
|---|---|---|
| GET on leader | **No** | No ReadIndex / leader lease. A partitioned ex-leader still
  serves last *committed* state. It will not serve uncommitted SETs
  (those never applied), but it can miss commits that happened on a
  new majority after the partition. |
| GET on follower | **No** | May lag the leader by one or more heartbeats. Can miss a SET that
  already returned 200 to a client. Can also be a partitioned node
  whose apply pointer is stale. |
| SET | **Yes, if it returns 200** | Majority-committed, current-term rule
  applied. Lost-leadership mid-wait returns an error; the entry may
  still commit later under a new leader or be overwritten if it never
  reached a quorum. |

What we did **not** implement, and why GET is not “safe”:

- **ReadIndex** (Raft §6.4): bounce a heartbeat, then serve a read at
  the confirmed commit index. Needed for linearizable leader reads.
- **Leader leases**: wall-clock bound so a stale leader refuses reads.
  Needs bounded clock drift, which we do not assume.
- **Read-your-writes from a follower**: would need a commit index in
  the SET response and a wait on the follower.

If you need linearizable reads, do not use GET as shipped. Sit a
lease/ReadIndex path in front, or read only through a SET-like
confirming RPC. A cache (ringcache) in front of GET makes this *worse*
unless you invalidate on commit.

## Persistence

Default docker-compose path: `FileStorage` under `/data`.

- `meta.json` — `currentTerm`, `votedFor` (temp+rename).
- `log.jsonl` — append-only entries; a conflict truncate rewrites the
  file.

`fsync` is on by default. That is the correct durability choice and
the main reason SET p50 is not “memory-map fast.” `-no-fsync` /
`QUORUMKV_NO_FSYNC=1` exists for benches; it will lose the tail of the
log on a host crash. In-memory storage (`-data` empty) is for tests.

No snapshots. Logs grow forever. Fine for an MVP; not fine for a
production cluster.

## Membership, snapshots, pre-vote — out of scope

Fixed 3-node (or N-node via `-peers`) configuration. No joint
consensus. No snapshot/installSnapshot. No pre-vote: a returning node
with a stale term can still bump the term and interrupt a stable
leader for one election. Chaos tests *kill* the process so this does
not fire; a flapping network would.

## Why not a library

The point of this repo is to *be* Raft: election timeouts, the two
RPCs, quorum commit, conflictIndex, Figure 8. Wrapping hashicorp/raft
would demo a KV API and hide every invariant listed above. Libraries
are the right call for a product that needs Raft; they are the wrong
call for a product whose job is to show Raft.

## Honest performance envelope

In-process 3-node benches (no HTTP, no fsync) measure the state
machine. HTTP+WAL+fsync on docker-compose is a different number, often
by 10–100×. See `bench/RESULTS.md`. Do not compare GET (atomic map
read) to SET (disk + majority RTT) and call it a win.
