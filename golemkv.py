#!/usr/bin/env python3
"""golemkv — a keyspace with TTL, CAS and pub/sub on top of golemlog.

What is borrowed from Redis: the AOF *is* the database (state is replay), expiry
travels with the data as an absolute `exp_ms`, and one command is one atomic
read-modify-write. What is deliberately **not** borrowed is the shared in-memory
keyspace: there is no server here, so every process builds its own view and read
cost scales with the keyspace rather than with the query.

Three breaks from the first design, each load-bearing:

1. **Full post-state entries, no op-application layer.** Every mutation appends
   the key's whole new value `{"t","v","exp_ms"}` under the writer lock; delete
   is a golemlog tombstone. Replay is last-write-wins per key, so log compaction
   (which keeps only the newest record per key) is lossless *by construction*.
   An op-record design would have let `golemlog compact` drop the `hset` that
   created a field while keeping the one that set another. The price is that an
   `hset` rewrites the whole hash; the consumers here hold ~6 fields.
2. **One partition, one lock.** `kv.aof` is 1 partition, `cleanup: compact`,
   256 KiB segments. A mutation takes `<root>/lock`, syncs the view to the log
   end, compares, produces (taking golemlog's own partition lock *inside* ours —
   the order is never reversed, so the two cannot deadlock), advances the cursor
   and releases. Reads and `sync()` take **no lock at all**: golemlog's
   positional read is lock-free and compaction rewrites segments in place
   preserving offsets, so a reader's cursor stays valid across a compaction.
3. **TTL is lazy on read and never appends.** An expired entry is filtered out of
   a read; it is not tombstoned there. A read that writes would make every `get`
   an exclusive lock acquisition and would fail on a read-only root. Tombstones
   for expired keys come only from `expire-sweep`. `exp_ms` is absolute
   wall-clock: if the system clock steps *backwards*, an expired key becomes
   visible again. That is documented, not defended against — a monotonic clock
   cannot survive the process exit that is this tool's whole access pattern.

Durability: an append is `write`+`flush`, so a `kill -9` cannot lose an
acknowledged write (the bytes are in the page cache). A machine crash can; pass
`durable=True` to a mutation to pay the `fsync`.

**Snapshot (S2.1).** The roadmap's pre-registered trigger fired — a cold `sync()`
over the real ~12 000-entry `kv.aof` costs 78–92 ms, against a 60 ms budget — so
`<root>/snapshot.json` now short-circuits the replay. It holds `upto` (an
exclusive AOF offset per partition), the raw `entries` map and a CRC32, written
atomically (`.tmp` → fsync → `os.replace`) **only on the writer path**: every
`SNAPSHOT_EVERY` offsets of AOF growth since the snapshot this process loaded,
and on `golemkv snapshot`. A read never writes one, so `get` still works on a
read-only root. `sync()` loads it, verifies the CRC, and tails the AOF from
`upto`; compaction rewrites segments in place preserving offsets, so a
snapshot's `upto` survives it. A corrupt, short, wrong-version or out-of-range
snapshot is ignored with one `note:` line and a full replay, and the next writer
overwrites it. Entries are stored raw — expiry is still filtered lazily on read.
Two writers racing produce two valid snapshots and `os.replace` gives readers
one or the other.
"""
import argparse
import fnmatch
import json
import math
import os
import sys
import time
import zlib

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import golemlog as gl                                          # noqa: E402

AOF = "kv.aof"
PUB_PREFIX = "kvpub."
SEGMENT_BYTES = 256 * 1024        # small: compaction needs sealed segments to work on
BATCH = 1000                      # records per read while tailing
DEFAULT_ROOT = ".golemkv"
SNAPSHOT = "snapshot.json"
SNAPSHOT_VERSION = 1
SNAPSHOT_EVERY = 500              # AOF offsets of growth before a writer re-snapshots

STR, HASH, SET = "str", "hash", "set"


def _note(msg):
    """One line on stderr for a recoverable oddity. Never raises, never a traceback."""
    sys.stderr.write("note: %s\n" % msg)


class _Any:
    """`expected=ANY` in `cas_guarded`: write whatever the target currently holds.

    Needed because a guarded *set* (S3's `guarded_set`) is unconditional on the
    target and conditional only on the guard. Reading the target first and
    passing it as `expected` would take two lock acquisitions and lose to a
    concurrent writer for no reason.
    """

    def __repr__(self):
        return "ANY"


ANY = _Any()


class KVError(Exception):
    pass


GolemKVError = KVError          # the name the CLI contract and docs use


class KVTypeError(KVError):
    """The key holds a different type — Redis's WRONGTYPE, raised not swallowed."""


def _now_ms():
    """Wall clock in ms. A single seam so tests can freeze time."""
    return int(time.time() * 1000)


def _channel_topic(channel):
    if not channel or "/" in channel or channel.startswith(".") or "\0" in channel:
        raise KVError("bad channel name %r" % channel)
    return PUB_PREFIX + channel


class KV:
    """A keyspace over one golemlog topic. Cheap to construct, cheap to drop."""

    def __init__(self, root=None, snapshot=None):
        self.root = os.path.abspath(
            root or os.environ.get("GOLEMKV_ROOT") or DEFAULT_ROOT)
        try:
            os.makedirs(self.root, exist_ok=True)
        except OSError as exc:
            # A read-only *parent*: reads cannot work either, since there is no
            # keyspace. One error, not a traceback.
            raise KVError("root not writable: %s (%s)" % (self.root, exc.strerror))
        self.log = gl.Log(os.path.join(self.root, "log"), segment_bytes=SEGMENT_BYTES)
        self.lock_path = os.path.join(self.root, "lock")
        self.snapshot_path = os.path.join(self.root, SNAPSHOT)
        self._view = {}                # key -> {"t","v","exp_ms"}
        self._cursor = None            # next AOF offset this view has not applied
        self._created = False
        # Snapshots off entirely: the bench's "before" arm, and an escape hatch.
        self._snapshot = (not os.environ.get("GOLEMKV_NO_SNAPSHOT")) \
            if snapshot is None else bool(snapshot)
        self._snap_every = int(os.environ.get("GOLEMKV_SNAPSHOT_EVERY")
                               or SNAPSHOT_EVERY)
        # The offset the on-disk snapshot covers, as far as this process knows.
        # The writer trigger is measured against the *log*, not against this
        # process's own append count: every CLI call is a fresh process, so a
        # per-process counter would never reach N and the snapshot would never
        # be written by the only writers this tool actually has.
        self._snap_base = None

    # ----------------------------------------------------------- plumbing ---
    def close(self):
        self.log.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def _ensure_topic(self, topic, cleanup):
        """Create a topic idempotently and **race-free**.

        `golemlog.create` is not atomic against a concurrent create of the same
        new topic: both processes write `meta.json.tmp`, one `os.replace` wins
        and the other raises ENOENT on a tmp file that no longer exists. Two
        fresh `golemkv` processes starting on an empty root hit that every time,
        so creation is serialised on the writer lock — the loser finds the topic
        already there and `create` returns its existing meta.
        """
        if os.path.exists(os.path.join(self.log.root, topic, "meta.json")):
            return
        with self._write():
            for attempt in (0, 1):
                try:
                    self.log.create(topic, partitions=1, cleanup=cleanup)
                    return
                except FileNotFoundError:
                    if attempt:
                        raise

    def _ensure(self):
        if not self._created:
            self._ensure_topic(AOF, "compact")
            self._created = True

    def _earliest(self):
        return self.log._pmeta(AOF, 0)["earliest_offset"]

    def sync(self):
        """Apply every AOF record this view has not seen. Takes no lock.

        Called at the top of every operation, which is what makes
        read-your-writes across processes a property of the log position rather
        than of a cache-invalidation protocol.
        """
        self._ensure()
        if self._cursor is None:
            self._start()
        while True:
            try:
                recs = self.log.read(AOF, 0, self._cursor, BATCH)
            except gl.OffsetOutOfRange:
                # Retention or a compaction that dropped our floor: rebuild.
                self._view.clear()
                self._cursor = self._snap_base = self._earliest()
                continue
            if not recs:
                return
            for r in recs:
                self._apply(r)
            self._cursor = recs[-1].offset + 1

    # ----------------------------------------------------------- snapshot ---
    def _start(self):
        """Seed the view for a cold `sync()`: from the snapshot if one verifies."""
        earliest = self._earliest()
        snap = self._read_snapshot() if self._snapshot else None
        if snap is not None:
            upto = snap["upto"]
            m = self.log._pmeta(AOF, 0)
            if upto < earliest:
                # Only reachable under a retention policy this topic does not
                # use, but a snapshot below the floor cannot be tailed from.
                _note("snapshot at %d is older than the log floor %d: full replay"
                      % (upto, earliest))
            elif upto > m["next_offset"]:
                _note("snapshot at %d is ahead of the log end %d: full replay"
                      % (upto, m["next_offset"]))
            else:
                self._view = snap["entries"]
                self._cursor = upto
                self._snap_base = upto
                return
        self._view.clear()
        self._cursor = earliest
        # No usable snapshot: the whole log counts as growth, so the next writer
        # here (or in any other process) writes one.
        self._snap_base = earliest

    def _read_snapshot(self):
        """Parse and CRC-check the snapshot. Any damage → one note and None.

        Format: a header line of JSON, then the entries JSON. Two parts rather
        than one object so the CRC covers exactly the entries bytes and those
        bytes are never re-escaped into a string.
        """
        try:
            with open(self.snapshot_path, "rb") as fh:
                blob = fh.read()
        except FileNotFoundError:
            return None
        except OSError as exc:
            _note("snapshot unreadable (%s): full replay" % exc.strerror)
            return None
        head, sep, body = blob.partition(b"\n")
        try:
            if not sep:
                raise ValueError("truncated before the entries")
            h = json.loads(head.decode("utf-8"))
            if h.get("version") != SNAPSHOT_VERSION:
                raise ValueError("version %r, want %d" % (h.get("version"),
                                                          SNAPSHOT_VERSION))
            if len(body) != h["bytes"]:
                raise ValueError("short: %d of %d bytes" % (len(body), h["bytes"]))
            if zlib.crc32(body) != h["crc32"]:
                raise ValueError("crc32 mismatch")
            entries = json.loads(body.decode("utf-8"))
            upto = int(h["upto"]["0"])
        except (ValueError, KeyError, TypeError, AttributeError,
                UnicodeDecodeError) as exc:
            _note("ignoring corrupt snapshot (%s): full replay" % exc)
            return None
        return {"upto": upto, "entries": entries}

    def _write_snapshot(self):
        """Write the snapshot atomically. Caller holds the writer lock."""
        body = json.dumps(self._view, separators=(",", ":")).encode("utf-8")
        head = json.dumps({"version": SNAPSHOT_VERSION,
                           "upto": {"0": self._cursor},
                           "bytes": len(body),
                           "crc32": zlib.crc32(body),
                           "keys": len(self._view)},
                          separators=(",", ":")).encode("utf-8")
        tmp = self.snapshot_path + ".tmp"
        with open(tmp, "wb") as fh:
            fh.write(head + b"\n" + body)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.snapshot_path)          # last writer wins; both valid
        gl._fsync_dir(self.root)
        self._snap_base = self._cursor
        return {"upto": self._cursor, "keys": len(self._view), "bytes": len(body)}

    def _maybe_snapshot(self):
        """The writer trigger. Called from `_put`/`_kill`, i.e. under the lock."""
        if not self._snapshot or self._cursor is None or self._snap_base is None:
            return
        if self._cursor - self._snap_base >= self._snap_every:
            try:
                self._write_snapshot()
            except OSError as exc:
                # A snapshot is an optimisation: never fail an acknowledged
                # write because it could not be written.
                _note("snapshot not written (%s)" % exc)
                self._snap_base = self._cursor

    def snapshot(self):
        """Write a snapshot now, whatever the trigger says. `golemkv snapshot`."""
        if not self._snapshot:
            raise KVError("snapshots are disabled (--no-snapshot / GOLEMKV_NO_SNAPSHOT)")
        with self._write():
            self.sync()
            return self._write_snapshot()

    def _apply(self, rec):
        key = rec.key.decode("utf-8", "replace")
        if rec.attrs & gl.ATTR_TOMB:
            self._view.pop(key, None)
        else:
            self._view[key] = json.loads(rec.value.decode("utf-8"))

    def _put(self, key, entry, durable=False):
        """Append a key's full post-state. Must be called under the writer lock."""
        _p, off = self.log.produce(AOF, key=key, value=json.dumps(entry),
                                   sync=durable)
        if self._cursor is not None and off == self._cursor:
            self._view[key] = entry
            self._cursor = off + 1
        else:
            self.sync()
        self._maybe_snapshot()

    def _kill(self, key, durable=False):
        _p, off = self.log.produce(AOF, key=key, tombstone=True, sync=durable)
        if self._cursor is not None and off == self._cursor:
            self._view.pop(key, None)
            self._cursor = off + 1
        else:
            self.sync()
        self._maybe_snapshot()

    def _write(self):
        """The writer lock. Always taken *outside* golemlog's own partition lock.

        Reads deliberately work on a read-only root (see the module docstring),
        so writability is checked *here* rather than in `__init__` — and checked
        up front, so a mutation fails with one `KVError` instead of a raw
        `PermissionError` traceback out of `os.open`.
        """
        if not os.access(self.root, os.W_OK | os.X_OK):
            raise KVError("root not writable: %s" % self.root)
        return gl._Lock(self.lock_path)

    def _live(self, key, now=None):
        """The entry for `key`, or None if missing or expired (never writes)."""
        e = self._view.get(key)
        if e is None:
            return None
        exp = e.get("exp_ms")
        if exp is not None and exp <= (now if now is not None else _now_ms()):
            return None
        return e

    @staticmethod
    def _typed(e, want, key):
        if e is not None and e["t"] != want:
            raise KVTypeError("%s holds a %s, not a %s" % (key, e["t"], want))
        return e

    def _exp(self, ex):
        return None if ex is None else _now_ms() + int(round(float(ex) * 1000))

    # -------------------------------------------------------------- reads ---
    def get(self, key):
        self.sync()
        e = self._typed(self._live(key), STR, key)
        return None if e is None else e["v"]

    def exists(self, key):
        self.sync()
        return self._live(key) is not None

    def dump(self, key):
        """The whole entry (type, value, expiry) — used by tests and `stats`."""
        self.sync()
        e = self._live(key)
        return None if e is None else dict(e)

    def ttl(self, key):
        """Remaining whole seconds; -1 no expiry; -2 no such key (Redis's codes)."""
        self.sync()
        e = self._live(key)
        if e is None:
            return -2
        if e.get("exp_ms") is None:
            return -1
        return int(math.ceil((e["exp_ms"] - _now_ms()) / 1000.0))

    def keys(self, pattern="*"):
        self.sync()
        now = _now_ms()
        return sorted(k for k in self._view
                      if self._live(k, now) is not None and fnmatch.fnmatchcase(k, pattern))

    def hget(self, key, field):
        self.sync()
        e = self._typed(self._live(key), HASH, key)
        return None if e is None else e["v"].get(field)

    def hmget(self, key, fields):
        self.sync()
        e = self._typed(self._live(key), HASH, key)
        v = {} if e is None else e["v"]
        return [v.get(f) for f in fields]

    def hgetall(self, key):
        self.sync()
        e = self._typed(self._live(key), HASH, key)
        return {} if e is None else dict(e["v"])

    def smembers(self, key):
        self.sync()
        e = self._typed(self._live(key), SET, key)
        return [] if e is None else sorted(e["v"])

    # --------------------------------------------------------- mutations ---
    def set(self, key, value, ex=None, durable=False):
        """Set a string. Like Redis, a plain `set` clears any existing TTL."""
        with self._write():
            self.sync()
            self._put(key, {"t": STR, "v": str(value), "exp_ms": self._exp(ex)}, durable)
        return True

    def delete(self, key, durable=False):
        with self._write():
            self.sync()
            if self._live(key) is None:
                return False
            self._kill(key, durable)
            return True

    def incr(self, key, by=1, durable=False):
        with self._write():
            self.sync()
            e = self._typed(self._live(key), STR, key)
            try:
                n = int(e["v"]) if e is not None else 0
            except ValueError:
                raise KVTypeError("%s does not hold an integer" % key)
            n += int(by)
            self._put(key, {"t": STR, "v": str(n),
                            "exp_ms": None if e is None else e.get("exp_ms")}, durable)
            return n

    def expire(self, key, secs, durable=False):
        with self._write():
            self.sync()
            e = self._live(key)
            if e is None:
                return False
            n = dict(e)
            n["exp_ms"] = self._exp(secs)
            self._put(key, n, durable)
            return True

    def persist(self, key, durable=False):
        """Drop a key's expiry. Returns False if there was none."""
        with self._write():
            self.sync()
            e = self._live(key)
            if e is None or e.get("exp_ms") is None:
                return False
            n = dict(e)
            n["exp_ms"] = None
            self._put(key, n, durable)
            return True

    def cas(self, key, expected, new, ex=None, durable=False):
        """Compare-and-set on a string. `expected=None` means "must not exist".

        This is the whole of WATCH/MULTI/EXEC for the single-key case, which is
        the only case any consumer here has, and unlike WATCH it survives the
        process boundary that every caller crosses.
        """
        with self._write():
            self.sync()
            e = self._typed(self._live(key), STR, key)
            cur = None if e is None else e["v"]
            if cur != (None if expected is None else str(expected)):
                return False
            self._put(key, {"t": STR, "v": str(new), "exp_ms": self._exp(ex)}, durable)
            return True

    def cas_guarded(self, guard_key, guard_value, key, expected, new, ex=None,
                    durable=False):
        """CAS on `key`, but only while `guard_key` still holds exactly `guard_value`.

        This is the whole of S3's fencing (see golemsched). A lease holder
        passes its own lease entry as the guard, so the moment the lease expires
        or changes hands the holder's writes stop landing — the *resource* does
        the checking, which is the only thing that makes a fencing token more
        than decoration. Guard and target are read and written under **one**
        writer lock, so nothing can slip between the check and the write.

        A guard that is absent, expired, or not a string is "not held" and
        returns False rather than raising: a guard is a predicate, not an
        access. A wrong-type *target* still raises, as everywhere else.
        """
        with self._write():
            self.sync()
            g = self._view.get(guard_key)
            if self._live(guard_key) is None or g["t"] != STR or \
                    g["v"] != str(guard_value):
                return False
            e = self._typed(self._live(key), STR, key)
            cur = None if e is None else e["v"]
            if expected is not ANY and cur != (None if expected is None else str(expected)):
                return False
            self._put(key, {"t": STR, "v": str(new), "exp_ms": self._exp(ex)}, durable)
            return True

    def delete_if(self, key, expected, durable=False):
        """Delete `key` only if it currently holds exactly `expected`.

        The CAS-guarded counterpart of `delete`, and how a lease is released
        (S3 DELTA 3): a real delete, so `who` reports nothing the instant the
        holder lets go — no sentinel value and no window in which the lease
        names a holder that no longer exists.
        """
        with self._write():
            self.sync()
            e = self._live(key)
            if e is None or e["t"] != STR or e["v"] != str(expected):
                return False
            self._kill(key, durable)
            return True

    def hset(self, key, mapping, durable=False):
        """Merge fields into a hash. Returns how many fields were new."""
        if not mapping:
            return 0
        with self._write():
            self.sync()
            e = self._typed(self._live(key), HASH, key)
            v = dict(e["v"]) if e is not None else {}
            added = sum(1 for f in mapping if f not in v)
            v.update({str(f): str(x) for f, x in mapping.items()})
            self._put(key, {"t": HASH, "v": v,
                            "exp_ms": None if e is None else e.get("exp_ms")}, durable)
            return added

    def hdel(self, key, fields, durable=False):
        """Remove fields. An emptied hash is deleted, as in Redis."""
        with self._write():
            self.sync()
            e = self._typed(self._live(key), HASH, key)
            if e is None:
                return 0
            v = dict(e["v"])
            gone = sum(1 for f in fields if v.pop(f, None) is not None)
            if not gone:
                return 0
            if v:
                self._put(key, {"t": HASH, "v": v, "exp_ms": e.get("exp_ms")}, durable)
            else:
                self._kill(key, durable)
            return gone

    def sadd(self, key, members, durable=False):
        if not members:
            return 0
        with self._write():
            self.sync()
            e = self._typed(self._live(key), SET, key)
            v = set(e["v"]) if e is not None else set()
            before = len(v)
            v.update(str(m) for m in members)
            if len(v) == before:
                return 0
            self._put(key, {"t": SET, "v": sorted(v),
                            "exp_ms": None if e is None else e.get("exp_ms")}, durable)
            return len(v) - before

    def srem(self, key, members, durable=False):
        with self._write():
            self.sync()
            e = self._typed(self._live(key), SET, key)
            if e is None:
                return 0
            v = set(e["v"])
            before = len(v)
            v.difference_update(str(m) for m in members)
            if len(v) == before:
                return 0
            if v:
                self._put(key, {"t": SET, "v": sorted(v), "exp_ms": e.get("exp_ms")},
                          durable)
            else:
                self._kill(key, durable)
            return before - len(v)

    def expire_sweep(self, durable=False):
        """Tombstone every expired key. The only place lazy expiry becomes a write."""
        with self._write():
            self.sync()
            now = _now_ms()
            dead = [k for k, e in self._view.items()
                    if e.get("exp_ms") is not None and e["exp_ms"] <= now]
            for k in dead:
                self._kill(k, durable)
            return len(dead)

    # ------------------------------------------------------------ pub/sub ---
    def publish(self, channel, message):
        """Append to the channel's own `delete`-policy topic — never to the AOF.

        A subscriber must not read `kv.aof`: compaction is *defined* to drop
        superseded records, so a subscriber reading it would silently miss
        messages that a compaction overtook.
        """
        topic = _channel_topic(channel)
        self._ensure_topic(topic, "delete")
        _p, off = self.log.produce(topic, key=channel, value=str(message))
        return off

    def subscribe(self, channel, group, max=100, commit=True):
        topic = _channel_topic(channel)
        self._ensure_topic(topic, "delete")
        recs = self.log.consume(topic, group, max)
        if commit and recs:
            self.log.commit(topic, group, 0, recs[-1].offset + 1)
        return [r.value.decode("utf-8", "replace") for r in recs]

    def channels(self):
        return sorted(t[len(PUB_PREFIX):] for t in self.log.topics()
                      if t.startswith(PUB_PREFIX))

    # -------------------------------------------------------------- stats ---
    def stats(self):
        self.sync()
        now = _now_ms()
        live = [e for k, e in self._view.items() if self._live(k, now) is not None]
        types = {}
        for e in live:
            types[e["t"]] = types.get(e["t"], 0) + 1
        m = self.log._pmeta(AOF, 0)
        return {"root": self.root, "keys": len(live),
                "expired_unswept": len(self._view) - len(live),
                "types": types, "aof_records": m["next_offset"],
                "aof_earliest": m["earliest_offset"], "cursor": self._cursor,
                "snapshot_upto": self._snap_base if self._snapshot else None,
                "channels": self.channels()}


# -------------------------------------------------------------------- CLI ---

def _p(out, x):
    out(x)


def run(argv=None, out=print, err=None):
    """Exit codes: 0 ok, 1 missing key / false CAS, 2 error."""
    err = err or (lambda s: sys.stderr.write(s + "\n"))
    ap = argparse.ArgumentParser(prog="golemkv", description=__doc__.split("\n")[0])
    ap.add_argument("--root", default=None,
                    help="keyspace root (default $GOLEMKV_ROOT or .golemkv)")
    ap.add_argument("--no-snapshot", action="store_true",
                    help="ignore and never write %s (full AOF replay)" % SNAPSHOT)
    sub = ap.add_subparsers(dest="cmd", required=True)

    def add(name, *args, **kw):
        c = sub.add_parser(name, **kw)
        for a in args:
            if a == "key":
                c.add_argument("key")
            elif a == "json":
                c.add_argument("--json", action="store_true")
        return c

    g = add("get", "key")
    add("exists", "key")
    s = add("set", "key")
    s.add_argument("value")
    s.add_argument("--ex", type=float, help="expire in N seconds")
    s.add_argument("--durable", action="store_true", help="fsync the append")
    add("del", "key")
    i = add("incr", "key")
    i.add_argument("--by", type=int, default=1)
    e = add("expire", "key")
    e.add_argument("secs", type=float)
    add("persist", "key")
    add("ttl", "key")
    k = add("keys", "json")
    k.add_argument("pattern", nargs="?", default="*")
    h = add("hset", "key")
    h.add_argument("pairs", nargs="+", metavar="FIELD VALUE")
    hg = add("hget", "key")
    hg.add_argument("field")
    hm = add("hmget", "key")
    hm.add_argument("fields", nargs="+")
    add("hgetall", "key", "json")
    hd = add("hdel", "key")
    hd.add_argument("fields", nargs="+")
    sa = add("sadd", "key")
    sa.add_argument("members", nargs="+")
    sr = add("srem", "key")
    sr.add_argument("members", nargs="+")
    add("smembers", "key", "json")
    c = add("cas", "key")
    c.add_argument("expected", help="the value it must currently hold, or '' for absent")
    c.add_argument("new")
    c.add_argument("--ex", type=float)
    cg = sub.add_parser("cas-guarded",
                        help="CAS KEY only while GUARD_KEY holds GUARD_VALUE")
    cg.add_argument("guard_key")
    cg.add_argument("guard_value")
    cg.add_argument("key")
    cg.add_argument("expected", help="the value KEY must hold, or '' for absent")
    cg.add_argument("new")
    cg.add_argument("--ex", type=float)
    di = add("del-if", "key", help="delete KEY only if it holds EXPECTED")
    di.add_argument("expected")
    pb = sub.add_parser("publish")
    pb.add_argument("channel")
    pb.add_argument("message")
    sb = sub.add_parser("subscribe")
    sb.add_argument("channel")
    sb.add_argument("--group", required=True)
    sb.add_argument("--max", type=int, default=100)
    sb.add_argument("--no-commit", action="store_true")
    sub.add_parser("expire-sweep")
    sub.add_parser("stats")
    sub.add_parser("snapshot", help="write %s now (writer path)" % SNAPSHOT)

    a = ap.parse_args(argv)
    # Construction itself can fail (an uncreatable root), so it is inside the
    # handler: every expected failure is one `error:` line and exit 2, never a
    # traceback and never exit 1 (which the contract reserves for "missing").
    try:
        kv = KV(a.root, snapshot=False if a.no_snapshot else None)
    except (KVError, gl.GolemLogError, OSError) as exc:
        err("error: %s" % exc)
        return 2
    try:
        return _dispatch(a, kv, out)
    except (KVError, gl.GolemLogError, OSError) as exc:
        err("error: %s" % exc)
        return 2
    finally:
        kv.close()


def _dispatch(a, kv, out):
    cmd = a.cmd
    if cmd == "get":
        v = kv.get(a.key)
        if v is None:
            return 1
        out(v)
    elif cmd == "exists":
        return 0 if kv.exists(a.key) else 1
    elif cmd == "set":
        kv.set(a.key, a.value, ex=a.ex, durable=a.durable)
    elif cmd == "del":
        return 0 if kv.delete(a.key) else 1
    elif cmd == "incr":
        out(kv.incr(a.key, a.by))
    elif cmd == "expire":
        return 0 if kv.expire(a.key, a.secs) else 1
    elif cmd == "persist":
        return 0 if kv.persist(a.key) else 1
    elif cmd == "ttl":
        t = kv.ttl(a.key)
        out(t)
        return 1 if t == -2 else 0
    elif cmd == "keys":
        ks = kv.keys(a.pattern)
        out(json.dumps(ks)) if a.json else [out(x) for x in ks]
    elif cmd == "hset":
        if len(a.pairs) % 2:
            raise KVError("hset takes FIELD VALUE pairs; got an odd number")
        pairs = dict(zip(a.pairs[0::2], a.pairs[1::2]))
        out(kv.hset(a.key, pairs))
    elif cmd == "hget":
        v = kv.hget(a.key, a.field)
        if v is None:
            return 1
        out(v)
    elif cmd == "hmget":
        # One value per line, empty for a missing field, so bash can `read -r`.
        # A missing *key* still prints those lines but exits 1, like
        # `hget`/`hgetall`/`get`: otherwise a caller cannot tell "the key is
        # gone" from "every field is unset".
        vals = kv.hmget(a.key, a.fields)      # raises on a wrong-type key first
        present = kv.exists(a.key)
        for v in vals:
            out("" if v is None else v)
        return 0 if present else 1
    elif cmd == "hgetall":
        d = kv.hgetall(a.key)
        if a.json:
            out(json.dumps(d))
        else:
            for f, v in sorted(d.items()):
                out("%s\t%s" % (f, v))
        return 0 if d else 1
    elif cmd == "hdel":
        n = kv.hdel(a.key, a.fields)
        out(n)
        return 0 if n else 1
    elif cmd == "sadd":
        out(kv.sadd(a.key, a.members))
    elif cmd == "srem":
        n = kv.srem(a.key, a.members)
        out(n)
        return 0 if n else 1
    elif cmd == "smembers":
        ms = kv.smembers(a.key)
        out(json.dumps(ms)) if a.json else [out(x) for x in ms]
        return 0 if ms else 1
    elif cmd == "cas":
        expected = None if a.expected == "" else a.expected
        return 0 if kv.cas(a.key, expected, a.new, ex=a.ex) else 1
    elif cmd == "cas-guarded":
        expected = None if a.expected == "" else a.expected
        return 0 if kv.cas_guarded(a.guard_key, a.guard_value, a.key, expected,
                                   a.new, ex=a.ex) else 1
    elif cmd == "del-if":
        return 0 if kv.delete_if(a.key, a.expected) else 1
    elif cmd == "publish":
        out(kv.publish(a.channel, a.message))
    elif cmd == "subscribe":
        for m in kv.subscribe(a.channel, a.group, a.max, commit=not a.no_commit):
            out(m)
    elif cmd == "expire-sweep":
        out(kv.expire_sweep())
    elif cmd == "stats":
        out(json.dumps(kv.stats(), indent=2))
    elif cmd == "snapshot":
        out(json.dumps(kv.snapshot()))
    return 0


if __name__ == "__main__":
    sys.exit(run())
