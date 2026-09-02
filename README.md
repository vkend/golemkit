# golemkit

A local-first substrate for running a fleet of coding-agent sessions on one machine. Four small tools,
Python 3 stdlib only, no server and no network. Each is the smallest honest version of a system it
borrows from, built on the one below it.

| tool | borrows from | what it is |
|---|---|---|
| `golemlog` | Kafka | partitioned append-only log: consumer groups, compaction, torn-tail recovery, lock-free readers |
| `golemkv` | Redis | keyspace with strings, hashes, sets, TTL, CAS and pub/sub over a compact append-only file |
| `golemsched` | k8s leases | TTL-bounded named leases, fencing tokens minted inside one CAS |
| `golemledger` | a payments core | balanced-postings ledger with idempotency keys, budgets and statements |

## golemlog

Records append to a partition and read back by offset. Groups commit offsets and resume across restarts.
A topic is `delete` (retention by time) or `compact` (only the newest record per key survives). A
`kill -9` mid-append leaves a contiguous prefix and no hole: the torn tail is truncated on the next open
and offsets never restart. Reads take no locks, and compaction rewrites segments in place preserving
offsets, so a reader scanning through one sees no short read.

```
golemlog create t --partitions 4 --cleanup compact
golemlog produce t --key k1 hello
golemlog consume t --group readers --max 100
golemlog commit t --group readers --partition 1 --offset 1
```

## golemkv

The append-only file *is* a compacted `golemlog` topic and the state is its replay. Every mutation appends
the key's whole **new value**, not an operation to apply — that makes compaction lossless by construction,
where an op-log would let it drop the `hset` that created a field while keeping the one that set another.
One partition, one writer lock; reads and `sync()` take no lock. Pub/sub lives on a separate topic per
channel, never the file, since compaction drops superseded records a subscriber would then miss. A
snapshot keeps cold reads flat as the keyspace grows.

```
golemkv set user:1 alice --ex 300 && golemkv hset job:7 state running attempts 3
golemkv set counter 4 && golemkv cas counter 4 5
golemkv publish events '{"kind":"done"}'
```

## golemsched

A lease is one `golemkv` string holding holder, token, acquisition and expiry. The entry outlives the
lease: `release` CASes it to a free form so the token survives, and `acquire` is a **single** CAS from
the exact entry it read to one holding `token + 1`. Minting the token inside that CAS is the point — a
separate counter plus a CAS lets a stalled acquirer land a token *lower* than one already granted.
`hold` is acquire-or-renew for one identity, what a critical section spanning several processes needs.
Transitions append to `sched.events`: `acquire`, `renew`, `release`, `renew-rejected`,
`hold-refused`, `expire-observed`.

```
golemsched lease acquire build --ttl 900 --holder agent-3   # prints the token
golemsched lease hold build --ttl 900 --holder agent-3      # acquire-or-renew
golemsched lease who build && golemsched lease release build --holder agent-3 --token 7
```

Two limits are held as tests, not left to prose. **Fencing is real only where the resource checks the
token**: `golemkv` checks it, so a zombie holder's kv writes are rejected, but nothing here intercepts
an `open()` or a `bind()`. And **split-brain across the TTL is admitted, not solved** — a second holder
acquires once the TTL elapses while the first, merely stalled, still believes it holds.

## golemledger

A posting is a balanced set of entries summing to zero per unit; `post` refuses an unbalanced one, so
unbalanced state is unrepresentable. Balances are a fold over the journal, never the truth. Idempotency is
a three-step claim → append → fill, so a poster that dies between the append and the balance write is
recoverable rather than duplicated. The resume cursor is a vector of per-partition offsets, not a scalar,
so a resume cannot skip a partition. Balances are a cache written only by the holder of the `ledger`
lease through a guarded write; a caller without the lease leaves them stale, not raced.

```
golemledger open-budget tok 1000000
golemledger post --key run:42 --entry spend:tok:500 --entry equity:usage:tok:-500
golemledger balance spend && golemledger statement spend && golemledger headroom
```

## Composition and consumers

`golemkv` is a keyspace over a compacted `golemlog` topic. `golemsched` is leases over `golemkv`
strings, the fencing check made by `golemkv` under the same writer lock as the write it guards.
`golemledger` is a journal in `golemlog` with its cursor, balances and idempotency claims in `golemkv`,
single-writer by a `golemsched` lease. `sessionscan` alone knows the on-disk transcript layout.

- **A research pipeline whose hand-maintained count kept drifting** put its per-item census on a ledger
  balance. A named tally with a contra leg locates *where* a count diverged; a bare total cannot.
- **A multi-agent static-site build** serialized its edit → build → commit section behind one named
  lease, so agents sharing the repo stopped interleaving builds.

## Benchmarks — Apple M2 Pro, macOS, CPython 3.14, one partition, 200-byte values

| operation | measured |
|---|---|
| `golemlog` produce, unsynced / `fsync` per record | 76–82 k / ~3.8 k rec/s |
| `golemlog` consume; compaction of 26 743 records → 202 | 137 k rec/s; 0.085 s |
| `golemkv` in-process set / get / hset | 11.4 k / 27.4 k / 7.5 k per s |
| `golemkv` CAS wins, 1 / 2 / 4 processes | 8.9 k / 5.8 k / 589 per s |
| `golemkv` CLI `get` (30–1 000 keys), `golemsched` `lease who` | 46–50 ms, ~44 ms |

`fsync` on macOS flushes to the device but does not issue `F_FULLFSYNC`: these figures are durable against
process death, not sudden power loss. CLI latency is mostly interpreter start plus import (~35 ms);
in-process use does not pay it. CAS stays correct at 4-way contention, but throughput collapses.

## Status and tests

Early. Single machine, single writer per keyspace, Python 3 stdlib only. No network, no server, no
cluster, no replication. Read cost scales with the keyspace rather than the query, because there is no
shared in-memory state.

```
python3 -m unittest discover -s tests -q
```

205 tests. Contention and crash properties shell out to real processes: an in-process test can only pretend
to have them.

MIT licensed. See [LICENSE](LICENSE).
