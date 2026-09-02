#!/usr/bin/env python3
"""golemlog — append-only partitioned log with consumer groups.

Local, stdlib only, no network. Offsets are positions and never mutated; the
reader owns its position; retention deletes whole segments, never the active one.
"""
import argparse
import base64
import collections
import errno
import fcntl
import glob
import heapq
import json
import math
import os
import struct
import sys
import threading
import time
import zlib

HDR = struct.Struct("<BBBIIIQ")   # magic, version, attrs, crc32, key_len, value_len, ts_ms
MAGIC = 0xA1
VERSION = 1
CRC_OFF = 7                       # crc covers header bytes from here on, plus key+value
OFF = struct.Struct("<Q")         # explicit absolute offset, compacted segments only
ATTR_TOMB = 0x01                  # tombstone: deletes the key
ATTR_OFFSET = 0x02                # record carries its absolute offset after the header
SEGMENT_BYTES = 8 * 1024 * 1024
DELETE_RETENTION_MS = 24 * 3600 * 1000
MAX_MAP_MB = 256
PLAN_NAME = "compact.plan"
META_EVERY = 512                  # persist meta.json at most this rarely; the segment is the truth
DEFAULT_ROOT = os.environ.get("GOLEMLOG_ROOT", ".golemlog")

Record = collections.namedtuple(
    "Record", "topic partition offset key value timestamp_ms attrs")
Record.__new__.__defaults__ = (0,)


class GolemLogError(Exception):
    pass


class UnknownTopic(GolemLogError):
    pass


class OffsetOutOfRange(GolemLogError):
    def __init__(self, earliest, offset=None):
        super().__init__(f"offset {offset} below earliest {earliest}")
        self.earliest = earliest
        self.offset = offset


class CommitOutOfRange(OffsetOutOfRange):
    def __init__(self, next_offset, offset=None):
        GolemLogError.__init__(
            self, f"commit offset {offset} is past next offset {next_offset}")
        self.earliest = next_offset
        self.next_offset = next_offset
        self.offset = offset


class CorruptRecord(GolemLogError):
    def __init__(self, offset, path):
        super().__init__(f"corrupt record at offset {offset} in {path}")
        self.offset = offset
        self.path = path


# ---------------------------------------------------------------- encoding ---

def _encode(key: bytes, value: bytes, ts_ms: int, attrs: int = 0, offset=None) -> bytes:
    """One record. `offset` set => ATTR_OFFSET and an absolute `<Q` inside the CRC."""
    if offset is None:
        extra, attrs = b"", attrs & ~ATTR_OFFSET
    else:
        extra, attrs = OFF.pack(offset), attrs | ATTR_OFFSET
    body = extra + key + value
    head = HDR.pack(MAGIC, VERSION, attrs, 0, len(key), len(value), ts_ms)
    crc = zlib.crc32(head[CRC_OFF:] + body) & 0xFFFFFFFF
    return HDR.pack(MAGIC, VERSION, attrs, crc, len(key), len(value), ts_ms) + body


def _parse(buf, base):
    """(records, end_pos, status, bad_offset) for one segment's bytes.

    status "ok"      — the whole buffer parsed.
    status "torn"    — short read / bad magic / a length longer than the bytes left:
                       an interrupted write. end_pos is the last good byte; a writer
                       truncates there. Lengths are bounded before any allocation.
    status "corrupt" — a complete record whose CRC does not match: real corruption,
                       raised as CorruptRecord rather than returned as data.
    """
    recs = []
    pos, n, off = 0, len(buf), base
    while pos < n:
        if n - pos < HDR.size:
            return recs, pos, "torn", off
        magic, ver, attrs, crc, klen, vlen, ts = HDR.unpack_from(buf, pos)
        if magic != MAGIC or ver != VERSION:
            return recs, pos, "torn", off
        xlen = OFF.size if attrs & ATTR_OFFSET else 0
        avail = n - pos - HDR.size
        if xlen > avail or klen > avail - xlen or vlen > avail - xlen - klen:
            return recs, pos, "torn", off
        s = pos + HDR.size
        body = buf[s:s + xlen + klen + vlen]
        if zlib.crc32(buf[pos + CRC_OFF:s] + body) & 0xFFFFFFFF != crc:
            return recs, pos, "corrupt", off
        if xlen:                                   # compacted: the offset is explicit
            off = OFF.unpack_from(body, 0)[0]
        recs.append((off, bytes(body[xlen:xlen + klen]),
                     bytes(body[xlen + klen:]), ts, attrs))
        pos = s + xlen + klen + vlen
        off += 1
    return recs, pos, "ok", off


def _as_bytes(x):
    if x is None:
        return b""
    return x if isinstance(x, bytes) else str(x).encode()


def _write_json(path, obj, sync=True):
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(obj, fh)
        if sync:
            fh.flush()
            os.fsync(fh.fileno())
    os.replace(tmp, path)
    if sync:
        _fsync_dir(os.path.dirname(path) or ".")


_ENTRY_COST = None


def _entry_cost(sample=2048):
    """Measured bytes of one `key -> (offset, tombstone, base)` map entry (delta 4).

    Amortized over a real dict of real objects rather than guessed: a one-entry
    dict overstates the slot cost by 5x because of the initial allocation. ~240 B
    on CPython 3.14/arm64. Cached; the scan's bound is derived from it.
    """
    global _ENTRY_COST
    if _ENTRY_COST is None:
        d = {}
        for i in range(sample):
            d[b"key-%016d" % i] = (i, False, 0)
        total = sys.getsizeof(d) + sum(
            sys.getsizeof(k) + sys.getsizeof(v) + sum(sys.getsizeof(x) for x in v)
            for k, v in d.items())
        _ENTRY_COST = max(120, int(total / sample))
    return _ENTRY_COST


def _fsync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class _Lock:
    """Per-partition advisory lock (`fcntl.flock`) held by **writers only**.

    `produce` takes it exclusive around an append; compaction takes it exclusive
    around the swap and the partition-meta read-modify-write. **Readers take no
    lock at all** (delta 5): they used to take it `LOCK_SH` for the whole scan,
    which put every producer on the partition behind every reader 1:1 — a reader
    stalled 3 s stalled the next append 2.98 s, and two writers × 5 000 appends
    went from 1.20 s to 8.38 s against one hot reader. Compaction is made safe
    for lock-free readers by rewriting each sealed segment **in place** (see
    `_rewrite`), so a reader holding an open fd keeps a consistent older view.

    The descriptor is cached per path so a hot producer pays two flock calls per
    append rather than an open/close pair as well, and acquisition is **depth
    counted** so a nested acquire (`_replay_plan` inside `_pmeta` inside
    `produce`) reuses the held lock instead of downgrading it.

    Thread safety: a `threading.RLock` per path guards the flock. flock is
    per-*process*, so without it a second thread asking for the exclusive lock
    while another thread held it was granted immediately and silently.

    Fork safety: a forked child inherits the cached fd, i.e. the same open file
    description, so its flock would be a no-op against the parent's and its
    release would drop the parent's lock. The pid is recorded with the fd and a
    mismatch reopens (the inherited fd is closed; the parent's own fd keeps the
    description, and with it the lock, alive).
    """
    _guard = threading.Lock()
    _state = {}

    def __init__(self, path, shared=False, nb=False):
        self.path = path
        self.shared = shared
        self.nb = nb                  # never wait: raise BlockingIOError instead
        self.upgraded = False
        self.st = None

    @classmethod
    def _state_for(cls, path):
        pid = os.getpid()
        with cls._guard:
            st = cls._state.get(path)
            if st is None or st["pid"] != pid:
                if st is not None and st["fd"] is not None:
                    try:
                        os.close(st["fd"])     # our copy only; the parent keeps its own
                    except OSError:
                        pass
                st = cls._state[path] = {"rlock": threading.RLock(), "fd": None,
                                         "pid": pid, "depth": 0, "shared": False}
            return st

    def __enter__(self):
        st = self.st = self._state_for(self.path)
        if not st["rlock"].acquire(not self.nb):
            raise BlockingIOError(errno.EWOULDBLOCK, "lock held by another thread",
                                  self.path)
        nbf = fcntl.LOCK_NB if self.nb else 0
        try:
            if st["fd"] is None:
                st["fd"] = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o644)
            if st["depth"]:
                if not self.shared and st["shared"]:
                    fcntl.flock(st["fd"], fcntl.LOCK_EX)   # upgrade, never downgrade
                    st["shared"] = False
                    self.upgraded = True
                st["depth"] += 1
            else:
                fcntl.flock(st["fd"],
                            (fcntl.LOCK_SH if self.shared else fcntl.LOCK_EX) | nbf)
                st["shared"] = self.shared
                st["depth"] = 1
        except BaseException:
            st["rlock"].release()
            raise
        return self

    def __exit__(self, *exc):
        st = self.st
        st["depth"] -= 1
        if not st["depth"]:
            fcntl.flock(st["fd"], fcntl.LOCK_UN)
        elif self.upgraded:
            fcntl.flock(st["fd"], fcntl.LOCK_SH)
            st["shared"] = True
        st["rlock"].release()


# -------------------------------------------------------------------- log ---

class Log:
    def __init__(self, root=DEFAULT_ROOT, segment_bytes=SEGMENT_BYTES):
        self.root = os.path.abspath(root)
        self.segment_bytes = segment_bytes
        self._rr = collections.Counter()      # round-robin cursor per topic (key=None)
        self._fh = {}                         # cached append handles, one per active segment
        self._pm = {}                         # cached partition meta, valid while sizes match
        self._dirty = {}                      # appends since meta.json was last persisted
        self._tm = {}                         # topic meta (partitions/cleanup) — immutable
        self._ready = set()                   # partition dirs known to exist

    def _append_handle(self, path):
        fh = self._fh.get(path)
        if fh is None:
            for old, h in list(self._fh.items()):     # only the active segment is written
                h.close()
                del self._fh[old]
            fh = self._fh[path] = open(path, "ab")
        return fh

    def close(self):
        for (topic, p), n in list(self._dirty.items()):
            if n and (topic, p) in self._pm:
                self._persist(topic, p, self._pm[(topic, p)])
        for h in self._fh.values():
            h.close()
        self._fh.clear()
        self._pm.clear()
        self._tm.clear()

    # --- paths ---
    def _tdir(self, topic):
        return os.path.join(self.root, topic)

    def _pdir(self, topic, p):
        return os.path.join(self._tdir(topic), str(p))

    def _seg(self, topic, p, base):
        return os.path.join(self._pdir(topic, p), "%020d.log" % base)

    def _segments(self, topic, p):
        return sorted(glob.glob(os.path.join(self._pdir(topic, p), "*.log")))

    @staticmethod
    def _base_of(path):
        return int(os.path.basename(path)[:-4])

    # --- topics ---
    def create(self, topic, partitions=1, cleanup="delete", retention_bytes=None,
               retention_secs=None):
        if cleanup not in ("delete", "none", "compact"):
            raise GolemLogError("cleanup must be 'delete', 'none' or 'compact'")
        if cleanup == "compact" and (retention_bytes or retention_secs):
            raise GolemLogError("cleanup 'compact' and byte/age retention are exclusive")
        tdir = self._tdir(topic)
        if os.path.exists(os.path.join(tdir, "meta.json")):
            m = self.meta(topic)
            want = {"cleanup": cleanup, "retention_bytes": retention_bytes,
                    "retention_secs": retention_secs}
            differs = [k for k, v in want.items() if m.get(k) != v]
            if differs:
                raise GolemLogError(
                    "topic %s already exists with %s; refusing to change its policy "
                    "(asked for %s). Recreate the topic under a new name, or delete "
                    "its directory." % (
                        topic,
                        ", ".join("%s=%r" % (k, m.get(k)) for k in sorted(want)),
                        ", ".join("%s -> %r" % (k, want[k]) for k in sorted(differs))))
            return m
        for p in range(partitions):
            os.makedirs(self._pdir(topic, p), exist_ok=True)
        os.makedirs(os.path.join(tdir, "groups"), exist_ok=True)
        meta = {"partitions": partitions, "cleanup": cleanup,
                "retention_bytes": retention_bytes, "retention_secs": retention_secs,
                "created_ms": int(time.time() * 1000)}
        _write_json(os.path.join(tdir, "meta.json"), meta)
        return meta

    def meta(self, topic):
        m = self._tm.get(topic)
        if m is not None:
            return m
        try:
            with open(os.path.join(self._tdir(topic), "meta.json")) as fh:
                m = self._tm[topic] = json.load(fh)
                return m
        except OSError:
            raise UnknownTopic(topic)

    def topics(self):
        return sorted(d for d in os.listdir(self.root)
                      if os.path.exists(os.path.join(self.root, d, "meta.json"))) \
            if os.path.isdir(self.root) else []

    # --- partition meta ---
    def _pmeta_path(self, topic, p):
        return os.path.join(self._pdir(topic, p), "meta.json")

    def _pmeta(self, topic, p, replay_block=True):
        """The partition meta, repaired (and any torn tail truncated) if stale.

        `replay_block=False` is the read path (delta 6): a pending compaction
        plan is replayed only if the partition lock can be taken *without
        waiting*, so a reader never blocks behind a compaction swap.

        The **segment listing wins**: the newest `*.log` on disk is the active
        segment, whatever `meta.json` claims. `meta.json` is a cache that only
        seeds `earliest_offset` and is then verified against a scan of that
        segment, so a producer killed between a roll and its meta persist cannot
        hide the newer segment from recovery (which used to make the next append
        re-roll behind an orphan record and hand back a wrong offset).

        A cached copy is trusted only while the active segment's name *and* size
        both still match, so a second process appending in between is never
        missed. When only the size grew, the scan starts at the last scanned size
        instead of rescanning the whole segment.
        """
        if os.path.exists(self._plan_path(topic, p)):
            # Finish an interrupted compaction (non-blocking on the read path).
            self._replay_plan(topic, p, block=replay_block)
        segs = self._segments(topic, p)
        if not segs:
            return self._rebuild(topic, p, segs)
        active = segs[-1]
        abase = self._base_of(active)
        try:
            size = os.path.getsize(active)
        except OSError:
            return self._rebuild(topic, p, self._segments(topic, p))
        cached = self._pm.get((topic, p))
        if cached is not None and cached["active_base"] == abase \
                and cached["end_pos"] == size:
            return cached
        if cached is not None and cached["active_base"] == abase \
                and size > cached["end_pos"]:
            # Only the tail is new: scan the delta, not the whole segment.
            m, start, sbase = cached, cached["end_pos"], cached["next_offset"]
            known_end = cached["end_pos"]
        else:
            try:
                with open(self._pmeta_path(topic, p)) as fh:
                    m = json.load(fh)
            except (OSError, ValueError):
                m = None
            if m is None or not {"active_base", "end_pos", "next_offset",
                                 "earliest_offset"} <= set(m):
                r = self._rebuild(topic, p, segs)
                for k in ("compacted", "clean_bytes"):    # keep compaction state
                    if m and k in m:
                        r[k] = m[k]
                return r
            if m["active_base"] == abase and m["end_pos"] == size:
                self._pm[(topic, p)] = m
                return m
            # meta.json is stale, or names an older segment: rescan the newest.
            start, sbase = 0, abase
            known_end = m["end_pos"] if m["active_base"] == abase else 0
        recs, end, status, _bad = self._read_segment(active, sbase, start)
        if status == "torn" and end < known_end:
            # Unparsable header inside bytes already known durable: real damage,
            # not an interrupted write. Never truncate over it.
            raise CorruptRecord(sbase + len(recs), active)
        if status == "torn" and size > end:
            self._truncate_locked(topic, p, active)
        m["active_base"] = abase
        m["next_offset"] = sbase + len(recs)
        m["end_pos"] = end
        self._pm[(topic, p)] = m
        return m

    def _rebuild(self, topic, p, segs):
        if not segs:
            os.makedirs(self._pdir(topic, p), exist_ok=True)
            m = {"next_offset": 0, "earliest_offset": 0, "active_base": 0, "end_pos": 0}
        else:
            active = segs[-1]
            base = self._base_of(active)
            recs, end, status, _ = self._read_segment(active, base)
            if status == "torn" and os.path.getsize(active) > end:
                self._truncate_locked(topic, p, active)
            m = {"next_offset": base + len(recs), "earliest_offset": self._base_of(segs[0]),
                 "active_base": base, "end_pos": end}
        self._pm[(topic, p)] = m
        return m

    def _truncate_locked(self, topic, p, path):
        """Drop a torn tail under the exclusive partition lock.

        Readers hold no lock (delta 5), so a reader that spots a half-written
        record must not truncate it from under the producer that is still
        writing it. Taking the writer lock and re-checking makes the recovery a
        no-op when the record completed in the meantime.
        """
        with self._plock(topic, p):
            recs, end, status, _bad = self._read_segment(path, self._base_of(path))
            if status == "torn" and os.path.getsize(path) > end:
                self._truncate(path, end)
            return recs, end

    @staticmethod
    def _truncate(path, end):
        with open(path, "r+b") as fh:
            fh.truncate(end)
            fh.flush()
            os.fsync(fh.fileno())
        print("golemlog: truncated torn tail of %s at %d" % (path, end), file=sys.stderr)

    @staticmethod
    def _read_segment(path, base, start=0):
        """Parse from byte `start`, where the record at `start` has offset `base`.

        OSError is **not** swallowed (delta 1): returning an empty "ok" parse for
        a segment that a compactor had just swapped away handed callers a short
        record list with no error, letting a consumer group commit past live
        records. `read` catches ENOENT and retries; everything else propagates.
        """
        with open(path, "rb") as fh:
            if start:
                fh.seek(start)
            buf = fh.read()
        recs, end, status, bad = _parse(buf, base)
        return recs, start + end, status, bad

    # --- produce ---
    def partition_for(self, topic, key, partitions):
        if key is None:
            n = self._rr[topic]
            self._rr[topic] = n + 1
            return n % partitions
        return (zlib.crc32(_as_bytes(key)) & 0xFFFFFFFF) % partitions

    def produce(self, topic, key=None, value=b"", sync=False, partition=None,
                tombstone=False):
        tmeta = self.meta(topic)
        p = partition if partition is not None else \
            self.partition_for(topic, key, tmeta["partitions"])
        kb, vb = _as_bytes(key), (b"" if tombstone else _as_bytes(value))
        if tombstone and not kb:
            # A tombstone deletes a key; a keyless record has no identity and is
            # never compacted, so a keyless tombstone deleted nothing and then
            # accumulated in the compacted segments for the life of the topic.
            raise GolemLogError("a tombstone needs a key: pass key=... to delete it")
        blob = _encode(kb, vb, int(time.time() * 1000),
                       attrs=ATTR_TOMB if tombstone else 0)
        pdir = self._pdir(topic, p)
        if pdir not in self._ready:
            os.makedirs(pdir, exist_ok=True)
            self._ready.add(pdir)
        with _Lock(os.path.join(pdir, "partition.lock")):
            m = self._pmeta(topic, p)
            base, end = m["active_base"], m["end_pos"]
            if end and end + len(blob) > self.segment_bytes:
                base, end = m["next_offset"], 0
                m["active_base"], m["end_pos"] = base, 0
            path = self._seg(topic, p, base)
            fh = self._append_handle(path)
            if end == 0:
                _fsync_dir(pdir)                       # the segment's name must be durable
            fh.write(blob)
            fh.flush()
            if sync:
                os.fsync(fh.fileno())
            offset = m["next_offset"]
            m["next_offset"] = offset + 1
            m["end_pos"] = end + len(blob)
            self._dirty[(topic, p)] = self._dirty.get((topic, p), 0) + 1
            if sync or end == 0 or self._dirty[(topic, p)] >= META_EVERY:
                self._persist(topic, p, m, sync, locked=True)
        return p, offset

    def _persist(self, topic, p, m, sync=True, locked=False):
        """Read-modify-write the partition meta under the partition lock (delta 3).

        The producer's lazy persist used to write its in-memory dict verbatim,
        which silently dropped compaction fields (`compacted`, `clean_bytes`) a
        compactor had written since this handle cached its copy. Disk-only keys
        are now preserved and folded back into the cached dict.
        """
        path = self._pmeta_path(topic, p)

        def go():
            try:
                with open(path) as fh:
                    disk = json.load(fh)
            except (OSError, ValueError):
                disk = {}
            for k, v in disk.items():
                if k not in m:
                    m[k] = v
            _write_json(path, m, sync=sync)
            self._dirty[(topic, p)] = 0

        if locked:
            go()
        else:
            with _Lock(os.path.join(self._pdir(topic, p), "partition.lock")):
                go()

    # --- read ---
    def _plock(self, topic, p, shared=False, nb=False):
        return _Lock(os.path.join(self._pdir(topic, p), "partition.lock"), shared, nb)

    def read(self, topic, partition, offset, max_records=100):
        """Positional read. Holes (compacted-away offsets) are skipped silently.

        The scan takes **no lock** (delta 5). Compaction rewrites each sealed
        segment in place with `os.replace`, so a segment this scan has already
        opened stays consistent and a segment it opens later is simply the newer
        version of the same offset range; no live record is in neither file. A
        scan that straddles a swap therefore sees a mix of old and new segments,
        which costs it only duplicate superseded records, never a live one.
        A pending `compact.plan` is *not* replayed on the read path unless the
        partition lock is free right now (delta 6): the reader tries `LOCK_NB`
        and, if a compactor holds it, proceeds on whatever segment files exist.
        Replaying it is the job of writers, the compactor, or an unobstructed
        reader.
        Belt and braces for a segment that vanishes anyway (retention): ENOENT
        drops the cached partition state and retries once from the same offset;
        any other OSError propagates (delta 1).
        """
        for attempt in (0, 1):
            try:
                return self._read_scan(topic, partition, offset, max_records)
            except FileNotFoundError:
                if attempt:
                    raise
                self._pm.pop((topic, partition), None)

    def _read_scan(self, topic, partition, offset, max_records):
        m = self._pmeta(topic, partition, replay_block=False)
        if offset < m["earliest_offset"]:
            raise OffsetOutOfRange(m["earliest_offset"], offset)
        if offset >= m["next_offset"]:
            return []
        out = []
        segs = self._segments(topic, partition)
        first = 0
        for i, s in enumerate(segs):                  # last segment whose base <= offset
            if self._base_of(s) <= offset:
                first = i
        for path in segs[first:]:
            recs, end, status, bad = self._read_segment(path, self._base_of(path))
            if status == "torn":
                if path == segs[-1] and end >= m["end_pos"]:
                    # A torn or over-long tail in the active segment: recover here
                    # rather than hand the writer an offset past records no reader
                    # can see.
                    recs = self._repair_active(topic, partition, path, end)
                else:
                    # A sealed segment, or bytes before the active segment's
                    # last-known end: an interrupted write cannot explain this.
                    status = "corrupt"
            for off, k, v, ts, at in recs:
                if off >= offset:
                    out.append(Record(topic, partition, off, k, v, ts, at))
                    if len(out) >= max_records:
                        return out
            if status == "corrupt" and bad >= offset:
                raise CorruptRecord(bad, path)
        return out

    def _repair_active(self, topic, p, path, end):
        """Truncate the active segment's torn tail and refresh the partition meta."""
        base = self._base_of(path)
        self._pm.pop((topic, p), None)
        self.close()
        with _Lock(os.path.join(self._pdir(topic, p), "partition.lock")):
            recs, end, status, _bad = self._read_segment(path, base)
            if status == "torn" and os.path.getsize(path) > end:
                self._truncate(path, end)
            m = self._pmeta(topic, p)
            m["active_base"], m["end_pos"] = base, end
            m["next_offset"] = base + len(recs)
            _write_json(self._pmeta_path(topic, p), m)
        return recs

    def tail(self, topic, n=10, partition=None):
        tmeta = self.meta(topic)
        parts = [partition] if partition is not None else range(tmeta["partitions"])
        got = []
        for p in parts:
            m = self._pmeta(topic, p)
            start = m["earliest_offset"] if n is None \
                else max(m["earliest_offset"], m["next_offset"] - n)
            if m["next_offset"] > start:
                got.extend(self.read(topic, p, start, m["next_offset"] - start))
        got.sort(key=lambda r: (r.timestamp_ms, r.partition, r.offset))
        return got[-n:] if n else got

    # --- consumer groups ---
    def _goff_path(self, topic, group):
        return os.path.join(self._tdir(topic), "groups", group + ".offsets")

    def offsets(self, topic, group):
        try:
            with open(self._goff_path(topic, group)) as fh:
                return {int(k): v for k, v in json.load(fh).items()}
        except (OSError, ValueError):
            return {}

    def groups(self, topic):
        d = os.path.join(self._tdir(topic), "groups")
        return sorted(f[:-8] for f in os.listdir(d)) if os.path.isdir(d) else []

    def drop_group(self, topic, group):
        """Forget a consumer group's committed offsets. Returns True if it existed.

        Committed offsets are not a GC floor any more (D-gc), so this is
        housekeeping rather than an unblocking ritual: it stops a dead group
        showing up in `groups` and reading from 0 if its name is reused.
        """
        try:
            os.remove(self._goff_path(topic, group))
            return True
        except FileNotFoundError:
            return False

    def commit(self, topic, group, partition, offset, rewind=False):
        os.makedirs(os.path.join(self._tdir(topic), "groups"), exist_ok=True)
        nxt = self._pmeta(topic, partition)["next_offset"]
        if offset > nxt:                       # == next_offset means "all consumed"
            raise CommitOutOfRange(nxt, offset)
        cur = self.offsets(topic, group)
        if not rewind and offset < cur.get(partition, -1):
            raise GolemLogError("commit would move %s/%s backwards" % (topic, group))
        cur[partition] = offset
        _write_json(self._goff_path(topic, group), {str(k): v for k, v in cur.items()})
        return offset

    def consume(self, topic, group, max_records=100, partition=None):
        tmeta = self.meta(topic)
        parts = [partition] if partition is not None else range(tmeta["partitions"])
        cur = self.offsets(topic, group)
        out = []
        for p in parts:
            if len(out) >= max_records:
                break
            m = self._pmeta(topic, p)
            start = cur.get(p, m["earliest_offset"])
            if start < m["earliest_offset"]:
                raise OffsetOutOfRange(m["earliest_offset"], start)
            out.extend(self.read(topic, p, start, max_records - len(out)))
        return out

    # --- retention ---
    def retention(self, topic, apply=False):
        tmeta = self.meta(topic)
        if tmeta.get("cleanup", "delete") in ("none", "compact"):
            return []                    # compact topics lose segments only to compact()
        rb, rs = tmeta.get("retention_bytes"), tmeta.get("retention_secs")
        now_ms = time.time() * 1000
        deleted = []
        for p in range(tmeta["partitions"]):
            m = self._pmeta(topic, p)
            segs = self._segments(topic, p)
            active = self._seg(topic, p, m["active_base"])
            old = [s for s in segs if s != active]           # never the active segment
            sizes = {s: os.path.getsize(s) for s in segs}
            total = sum(sizes.values())
            doomed = []
            for s in old:
                try:
                    recs, _e, _st, _b = self._read_segment(s, self._base_of(s))
                except FileNotFoundError:
                    continue
                aged = rs is not None and recs and (now_ms - recs[-1][3]) / 1000.0 >= rs
                over = rb is not None and total > rb
                if aged or over:
                    doomed.append(s)
                    total -= sizes[s]
            if not doomed:
                continue
            deleted.extend(doomed)
            if apply:
                for s in doomed:
                    os.remove(s)
                left = self._segments(topic, p)
                m["earliest_offset"] = self._base_of(left[0]) if left else m["next_offset"]
                _write_json(self._pmeta_path(topic, p), m)
        return deleted

    def stats(self, topic):
        tmeta = self.meta(topic)
        rows = []
        for p in range(tmeta["partitions"]):
            m = self._pmeta(topic, p)
            segs = self._segments(topic, p)
            sealed = sum(os.path.getsize(s) for s in self._sealed(topic, p, m))
            clean = m.get("clean_bytes", 0)
            rows.append({"partition": p, "earliest": m["earliest_offset"],
                         "next": m["next_offset"], "segments": len(segs),
                         "bytes": sum(os.path.getsize(s) for s in segs),
                         "sealed_bytes": sealed, "clean_bytes": clean,
                         "dirty_ratio": round((sealed - clean) / sealed, 4) if sealed else 0.0})
        return {"topic": topic, "cleanup": tmeta.get("cleanup", "delete"), "partitions": rows}

    # ---------------------------------------------------------- compaction ---
    def _plan_path(self, topic, p):
        return os.path.join(self._pdir(topic, p), PLAN_NAME)

    def _sealed(self, topic, p, m=None):
        m = m if m is not None else self._pmeta(topic, p)
        active = self._seg(topic, p, m["active_base"])
        return [s for s in self._segments(topic, p) if s != active]

    def _iter_sealed(self, sealed):
        """Every record of the sealed segments in offset order. A sealed segment
        that does not parse cleanly is corruption, never an interrupted write."""
        for path in sealed:
            recs, _end, status, bad = self._read_segment(path, self._base_of(path))
            if status != "ok":
                raise CorruptRecord(bad, path)
            for r in recs:
                yield path, r

    def _replay_plan(self, topic, p, block=True):
        """Finish an interrupted compaction. Writers block for the lock; readers
        pass `block=False` (delta 6) and simply skip when someone else holds it.

        A reader that skips leaves the plan for the next writer, compactor or
        idle reader. That is safe because the swap is in-place `os.replace` per
        segment: every offset stays present in either the old or the new file,
        so the mixed view a skipping reader sees costs it duplicate superseded
        records, never a live one.
        """
        path = self._plan_path(topic, p)
        try:
            with open(path) as fh:
                plan = json.load(fh)
        except (OSError, ValueError):
            return False
        lock = self._plock(topic, p, nb=not block)
        try:
            lock.__enter__()
        except OSError:
            if block:
                raise
            return False                           # busy: leave it for a writer
        try:
            if not os.path.exists(path):
                return False                       # another process finished it
            self._apply_plan(topic, p, plan)
        finally:
            lock.__exit__(None, None, None)
        return True

    def _apply_plan(self, topic, p, plan):
        """The swap. Idempotent, so recovery is simply "run it again".

        Renames land **before** unlinks, so no record is ever in neither file.
        Readers hold no lock and may scan straight through this half-state; the
        in-place `os.replace` keeps every offset present in either the old or
        the new file, so the worst they see is a duplicate superseded record.
        """
        pdir = self._pdir(topic, p)
        for tmp, final in plan["rename"]:
            if os.path.exists(tmp):
                os.replace(tmp, final)
        keep = {f for _t, f in plan["rename"]}
        for path in plan.get("unlink") or []:
            if path not in keep and os.path.exists(path):
                os.remove(path)
        _fsync_dir(pdir)
        mpath = self._pmeta_path(topic, p)
        try:
            with open(mpath) as fh:
                disk = json.load(fh)
        except (OSError, ValueError):
            disk = {}
        disk.update(plan["meta"])
        _write_json(mpath, disk)
        cached = self._pm.get((topic, p))
        if cached is not None:
            cached.update(plan["meta"])
        if os.path.exists(self._plan_path(topic, p)):
            os.remove(self._plan_path(topic, p))
        _fsync_dir(pdir)

    def _passes(self, sealed, max_entries):
        """Passes needed to keep the key map inside the memory bound (delta 4).

        Counts distinct keys into a bounded set that spills exactly the way the
        real scan does: on overflow, double `passes` and count bucket 0 only,
        until bucket 0 fits. Keys are never written to disk; only offsets are.
        """
        passes = 1
        while True:
            seen, overflow = set(), False
            for _path, (_off, k, _v, _ts, _at) in self._iter_sealed(sealed):
                if not k or zlib.crc32(k) % passes:
                    continue
                seen.add(k)
                if len(seen) > max_entries:
                    overflow = True
                    break
            if not overflow:
                distinct = len(seen) * passes
                return max(1, math.ceil(1.25 * distinct / max_entries))
            passes *= 2

    def _scan(self, sealed, passes, i, gcable):
        """key -> latest offset for hash bucket `i`; returns kept offsets sorted."""
        latest, extra = {}, []
        for path, (off, k, _v, _ts, at) in self._iter_sealed(sealed):
            if not k:
                if i == 0:
                    extra.append(off)              # no identity: never compacted
                continue
            if zlib.crc32(k) % passes != i:
                continue
            latest[k] = (off, at & ATTR_TOMB, self._base_of(path))
        keep = list(extra)
        for off, tomb, base in latest.values():
            if tomb and gcable(base):
                continue                           # tombstone past its retention
            keep.append(off)
        keep.sort()
        return keep

    def _rewrite(self, topic, p, sealed, merged, compacted, now):
        """Rewrite each sealed segment **in place**, keeping the offsets in
        `merged` (ascending), into `<base>.log.tmp` fsynced and ready to swap.

        Segments are never merged with each other and never unlinked (delta 5).
        A segment keeps its name and therefore its offset range, so:
          * `earliest_offset` can never rise — the first input keeps its base
            even when every record in it is dropped (it becomes a zero-record
            file that is just a name);
          * the swap is one `os.replace` per segment, atomic to a lock-free
            reader that has the old file open;
          * `compacted[base]`, the first-retained clock behind tombstone GC,
            needs no carry-forward across merged inputs — the base is stable.
        Merging adjacent small segments is an explicit non-goal.
        """
        renames, newmap, live = [], {}, 0
        want = next(merged, None)
        for path in sealed:
            base = self._base_of(path)
            recs, _end, status, bad = self._read_segment(path, base)
            if status != "ok":
                raise CorruptRecord(bad, path)
            tmp = path + ".tmp"
            with open(tmp, "wb") as fh:
                for off, k, v, ts, at in recs:
                    while want is not None and want < off:
                        want = next(merged, None)
                    if want != off:
                        continue
                    want = next(merged, None)
                    fh.write(_encode(k, v, ts, at, offset=off))
                    live += 1
                fh.flush()
                os.fsync(fh.fileno())
            renames.append([tmp, path])
            newmap[str(base)] = compacted.get(str(base), now)
        return renames, newmap, live

    def _compact_partition(self, topic, p, min_dirty_ratio, delete_retention_ms,
                           max_map_mb):
        t0 = time.time()
        self._replay_plan(topic, p)
        m = self._pmeta(topic, p)
        sealed = self._sealed(topic, p, m)
        row = {"partition": p, "compacted": False, "passes": 0}
        if not sealed:
            row["skipped"] = "no sealed segments"
            return row
        before = sum(os.path.getsize(s) for s in sealed)
        clean = m.get("clean_bytes", 0)
        ratio = (before - clean) / before if before else 0.0
        row["dirty_ratio"] = round(ratio, 4)
        row["bytes_before"] = before
        if ratio < min_dirty_ratio:
            row["skipped"] = "dirty ratio %.3f < %.3f" % (ratio, min_dirty_ratio)
            return row

        compacted = dict(m.get("compacted") or {})
        now = int(time.time() * 1000)

        def gcable(base):
            """Kafka's `delete.retention.ms`, and nothing else (D-gc).

            A tombstone is collected once `delete_retention_ms` has passed since
            the segment holding it was **first retained by a compaction** — an
            unseen segment records `now`, so a tombstone always survives its
            first pass. Consumer-group committed offsets are deliberately *not*
            a floor: as in Kafka, `delete_retention_ms` is the contract and it
            must exceed the worst consumer lag. The old min-committed floor made
            one group that had committed nothing on a partition (including a
            group that only ever touched a *different* partition) pin every
            tombstone on it forever, with no way to clear it but deleting the
            offsets file by hand.
            """
            first = compacted.get(str(base))
            return first is not None and now - first >= delete_retention_ms

        entry = _entry_cost()
        max_entries = max(1, int(max_map_mb * (1 << 20) // entry))
        passes = self._passes(sealed, max_entries)
        row.update(passes=passes, entry_bytes=entry, max_entries=max_entries)

        pdir = self._pdir(topic, p)
        files = []
        for i in range(passes):
            keep = self._scan(sealed, passes, i, gcable)
            fp = os.path.join(pdir, "compact.keep.%d" % i)
            with open(fp, "w") as fh:
                fh.write("".join("%d\n" % o for o in keep))
            files.append(fp)
        handles = [open(f) for f in files]
        try:
            merged = heapq.merge(*[(int(line) for line in h) for h in handles])
            renames, newmap, live = self._rewrite(
                topic, p, sealed, merged, compacted, now)
        finally:
            for h in handles:
                h.close()
            for f in files:
                os.remove(f)

        after = sum(os.path.getsize(t) for t, _f in renames)
        plan = {"rename": renames, "unlink": [],   # in place: nothing is unlinked
                "meta": {"compacted": newmap, "clean_bytes": after}}
        _write_json(self._plan_path(topic, p), plan)
        with self._plock(topic, p):
            self._apply_plan(topic, p, plan)
        row.update(compacted=True, live_records=live, bytes_after=after,
                   segments_after=len(renames), elapsed=round(time.time() - t0, 3),
                   skipped=None)
        return row

    def compact(self, topic, partition=None, min_dirty_ratio=0.5,
                delete_retention_ms=DELETE_RETENTION_MS, max_map_mb=MAX_MAP_MB):
        """Offline keyed compaction over the sealed segments of a compact topic."""
        tmeta = self.meta(topic)
        if tmeta.get("cleanup") != "compact":
            raise GolemLogError("topic %s is not cleanup: compact" % topic)
        parts = [partition] if partition is not None else range(tmeta["partitions"])
        return [self._compact_partition(topic, p, min_dirty_ratio,
                                        delete_retention_ms, max_map_mb)
                for p in parts]


# ------------------------------------------------------------ ingest-fleet ---

FLEET_TOPIC = "fleet.rows"
CURSOR_DIR = ".cursors"


def _load_session_scan():
    import importlib.util
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sessionscan.py")
    spec = importlib.util.spec_from_file_location("sessionscan", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def fleet_rows(es, repo, path, rows):
    """Eligible rows of one session, in file order: human turns and assistant final text.

    Uses sessionscan.indexable_row — the same predicate a consumer applies when
    walking transcripts directly, so a session indexed via the log matches one
    walked from disk exactly. This used to drop an assistant row that
    carried a tool_use even when it also had final text; it no longer does.

    Tags each record "kind": "main" | "subagent" (a subagent's "human" turn is its
    parent agent's prompt, not a person; consumers use this to keep the two apart). Always set; a consumer reading an
    older record that predates this field should treat a missing "kind" as "main"."""
    sid = os.path.basename(path)[:8]
    kind = es.path_kind(path)
    for r in rows:
        who, text = es.indexable_row(r)
        if who:
            yield {"repo": repo, "session": sid, "path": path, "kind": kind,
                   "ts": es.row_time(r), "who": who, "text": text}


def ingest_fleet(log, days=2, root=None, partitions=4, out=print, main_only=False):
    es = _load_session_scan()
    root = root or es.ROOT
    log.create(FLEET_TOPIC, partitions=partitions, cleanup="delete")
    cdir = os.path.join(log.root, CURSOR_DIR)
    os.makedirs(cdir, exist_ok=True)
    cpath = os.path.join(cdir, "ingest-fleet.json")
    try:
        with open(cpath) as fh:
            cursor = json.load(fh)
    except (OSError, ValueError):
        cursor = {}
    files = new = 0
    kinds = {}
    t0 = time.time()
    # Covers subagent transcripts too, not just depth-1 main sessions.
    for repo, path, rows in es.iter_sessions(root, days, None, main_only, kinds):
        files += 1
        try:
            st = os.stat(path)
        except OSError:
            continue
        c = cursor.get(path)
        if c and c["mtime"] == st.st_mtime and c["size"] == st.st_size:
            continue                                   # unchanged since last ingest
        done = c["n"] if c else 0
        n = 0
        for rec in fleet_rows(es, repo, path, rows):
            n += 1
            if n <= done:
                continue                               # already produced
            log.produce(FLEET_TOPIC, key=repo, value=json.dumps(rec))
            new += 1
        cursor[path] = {"mtime": st.st_mtime, "size": st.st_size, "n": max(n, done)}
    log.close()
    _write_json(cpath, cursor)
    out("ingest-fleet: files %d (main %d  subagent %d)  new records %d  elapsed %.1fs"
        % (files, kinds.get("main", 0), kinds.get("subagent", 0), new, time.time() - t0))
    return {"files": files, "main": kinds.get("main", 0), "subagent": kinds.get("subagent", 0),
            "new": new, "elapsed": time.time() - t0}


# -------------------------------------------------------------------- CLI ---

def _fmt(r):
    return "%d:%d %s %s" % (r.partition, r.offset, r.key.decode("utf-8", "replace"),
                            r.value.decode("utf-8", "replace"))


def _jsonable(b):
    """UTF-8 text where possible, `{"b64": ...}` where the bytes are not text."""
    try:
        return b.decode("utf-8")
    except UnicodeDecodeError:
        return {"b64": base64.b64encode(b).decode("ascii")}


def _json_line(r):
    """One record as one JSON object — the machine-readable form of `_fmt`.

    `_fmt` is "<p>:<off> <key> <value>", which loses a value containing a space
    or a newline, cannot round-trip non-UTF-8 bytes, and emits neither the
    timestamp nor the tombstone flag. Values that are themselves JSON (every
    `ingest-fleet` row is) stay the **raw string** in `value`; they are not
    parsed and re-nested, so `json.loads(row["value"])` is the caller's step.
    """
    return json.dumps({"topic": r.topic, "partition": r.partition,
                       "offset": r.offset, "ts_ms": r.timestamp_ms,
                       "key": _jsonable(r.key), "value": _jsonable(r.value),
                       "tombstone": bool(r.attrs & ATTR_TOMB)})


def run(argv=None, out=print):
    ap = argparse.ArgumentParser(prog="golemlog")
    ap.add_argument("--root", default=DEFAULT_ROOT)
    sub = ap.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("create")
    c.add_argument("topic")
    c.add_argument("--partitions", type=int, default=1)
    c.add_argument("--cleanup", choices=["delete", "none", "compact"], default="delete")
    c.add_argument("--retention-bytes", type=int)
    c.add_argument("--retention-hours", type=float)

    p = sub.add_parser("produce")
    p.add_argument("topic")
    p.add_argument("value", nargs="?")
    p.add_argument("--key")
    p.add_argument("--sync", action="store_true")
    p.add_argument("--tombstone", action="store_true",
                   help="delete the key (compact topics); the value is ignored")

    k = sub.add_parser("consume")
    k.add_argument("topic")
    k.add_argument("--group", required=True)
    k.add_argument("--max", type=int, default=100)
    k.add_argument("--commit", action="store_true")
    k.add_argument("--json", action="store_true",
                   help="one JSON object per line, nothing else on stdout")

    m = sub.add_parser("commit")
    m.add_argument("topic")
    m.add_argument("--group", required=True)
    m.add_argument("--partition", type=int, required=True)
    m.add_argument("--offset", type=int, required=True)

    t = sub.add_parser("tail")
    t.add_argument("topic")
    t.add_argument("-n", type=int, default=10)
    t.add_argument("--json", action="store_true",
                   help="one JSON object per line, nothing else on stdout")

    g = sub.add_parser("groups", help="consumer groups of a topic and their offsets")
    g.add_argument("topic")
    g.add_argument("--json", action="store_true")

    gd = sub.add_parser("group-drop", help="forget a consumer group's offsets")
    gd.add_argument("topic")
    gd.add_argument("--group", required=True)

    x = sub.add_parser("compact")
    x.add_argument("topic")
    x.add_argument("--partition", type=int)
    x.add_argument("--min-dirty-ratio", type=float, default=0.5)
    x.add_argument("--delete-retention-hours", type=float, default=24.0)
    x.add_argument("--max-map-mb", type=float, default=MAX_MAP_MB)
    x.add_argument("--force", action="store_true", help="compact whatever the dirty ratio")

    r = sub.add_parser("retention")
    r.add_argument("topic")
    r.add_argument("--apply", action="store_true")

    sub.add_parser("topics")
    s = sub.add_parser("stats")
    s.add_argument("topic")

    i = sub.add_parser("ingest-fleet")
    i.add_argument("--days", type=int, default=2)
    i.add_argument("--projects-root")
    i.add_argument("--partitions", type=int, default=4)
    i.add_argument("--main-only", action="store_true")

    a = ap.parse_args(argv)
    log = Log(a.root)
    try:
        return _dispatch(a, log, out)
    except GolemLogError as e:
        out("error: %s" % e)
        return 1
    finally:
        log.close()


def _dispatch(a, log, out):
    if a.cmd == "create":
        secs = int(a.retention_hours * 3600) if a.retention_hours else None
        m = log.create(a.topic, a.partitions, a.cleanup, a.retention_bytes, secs)
        # Report what is actually on disk, not what was asked for: `create` on an
        # existing topic used to print the requested policy while leaving the
        # stored one untouched.
        out("created %s partitions=%d cleanup=%s"
            % (a.topic, m["partitions"], m["cleanup"]))
    elif a.cmd == "produce":
        value = a.value if a.value is not None else sys.stdin.read()
        part, off = log.produce(a.topic, a.key, value, sync=a.sync,
                                tombstone=a.tombstone)
        out("%d:%d" % (part, off))
    elif a.cmd == "consume":
        recs = log.consume(a.topic, a.group, a.max)
        for rec in recs:
            out(_json_line(rec) if a.json else _fmt(rec))
        if a.commit:
            last = {}
            for rec in recs:
                last[rec.partition] = rec.offset
            for part, off in last.items():
                log.commit(a.topic, a.group, part, off + 1)
    elif a.cmd == "commit":
        log.commit(a.topic, a.group, a.partition, a.offset)
        out("committed %s/%s p%d @%d" % (a.topic, a.group, a.partition, a.offset))
    elif a.cmd == "tail":
        for rec in log.tail(a.topic, a.n):
            out(_json_line(rec) if a.json else _fmt(rec))
    elif a.cmd == "groups":
        rows = [{"group": g, "offsets": {str(k): v
                                         for k, v in sorted(log.offsets(a.topic, g).items())}}
                for g in log.groups(a.topic)]
        if a.json:
            out(json.dumps({"topic": a.topic, "groups": rows}))
        else:
            for row in rows:
                out("%s %s" % (row["group"],
                               " ".join("p%s@%s" % (k, v)
                                        for k, v in row["offsets"].items()) or "-"))
    elif a.cmd == "group-drop":
        out("dropped %s/%s" % (a.topic, a.group) if log.drop_group(a.topic, a.group)
            else "no such group %s/%s" % (a.topic, a.group))
    elif a.cmd == "compact":
        rows = log.compact(a.topic, a.partition,
                           0.0 if a.force else a.min_dirty_ratio,
                           int(a.delete_retention_hours * 3600 * 1000), a.max_map_mb)
        for row in rows:
            if row.get("skipped"):
                out("p%d skipped: %s" % (row["partition"], row["skipped"]))
            else:
                out("p%d live %d  bytes %d -> %d  segments %d  passes %d  %.3fs"
                    % (row["partition"], row["live_records"], row["bytes_before"],
                       row["bytes_after"], row["segments_after"], row["passes"],
                       row["elapsed"]))
    elif a.cmd == "retention":
        for s in log.retention(a.topic, a.apply):
            out(("deleted " if a.apply else "would delete ") + s)
    elif a.cmd == "topics":
        for name in log.topics():
            out(name)
    elif a.cmd == "stats":
        out(json.dumps(log.stats(a.topic), indent=2))
    elif a.cmd == "ingest-fleet":
        ingest_fleet(log, a.days, a.projects_root, a.partitions, out=out, main_only=a.main_only)
    return 0


if __name__ == "__main__":
    sys.exit(run())
