"""Acceptance tests for golemlog.py.

Everything runs in temp dirs; ingest-fleet is exercised on a synthetic sessions
root built under one, never on a real transcript corpus.
"""
import base64
import fcntl
import glob
import importlib.util
import json
import os
import random
import shutil
import signal
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest

KLEN_OFF, VLEN_OFF = 7, 11          # byte offsets inside the record header


class _Timeout(Exception):
    """Raised out of SIGALRM to cut short a lock acquisition that must block."""


def _alarm(_sig, _frame):
    raise _Timeout()

HERE = os.path.dirname(os.path.abspath(__file__))
TOOLS = os.path.join(HERE, "..")
SPEC = importlib.util.spec_from_file_location("golemlog", os.path.join(TOOLS, "golemlog.py"))
gl = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gl)

# A producer child for the crash and concurrency tests. argv:
#   root topic n sync segment_bytes
CHILD = r"""
import importlib.util, os, sys
spec = importlib.util.spec_from_file_location("golemlog", {tools!r} + "/golemlog.py")
gl = importlib.util.module_from_spec(spec); spec.loader.exec_module(gl)
root, topic, n, sync = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4] == "1"
log = gl.Log(root, segment_bytes=int(sys.argv[5]))
for i in range(n):
    log.produce(topic, key="k", value=("v%06d" % i) * 8, sync=sync)
    print(i, flush=True)
"""


class Base(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="golemlog-test-")
        self.log = gl.Log(self.dir)

    def tearDown(self):
        for log in [self.log] + getattr(self, "extra", []):
            log.close()
        shutil.rmtree(self.dir, ignore_errors=True)


class RoundTrip(Base):
    def test_order_and_gapless_over_4_partitions(self):
        self.log.create("t", partitions=4)
        sent = {}
        for i in range(10000):
            key = "k%d" % (i % 37)
            p, off = self.log.produce("t", key=key, value="v%05d" % i)
            sent.setdefault(p, []).append((off, b"v%05d" % i))
        self.assertEqual(sorted(sent), [0, 1, 2, 3])
        total = 0
        for p, expect in sent.items():
            got = self.log.read("t", p, 0, 100000)
            self.assertEqual([r.offset for r in got], list(range(len(expect))))
            self.assertEqual([r.value for r in got], [v for _o, v in expect])
            self.assertEqual([o for o, _v in expect], list(range(len(expect))))
            total += len(got)
        self.assertEqual(total, 10000)

    def test_same_key_same_partition(self):
        self.log.create("t", partitions=4)
        parts = {self.log.produce("t", key="stable", value=str(i))[0] for i in range(50)}
        self.assertEqual(len(parts), 1)


class CrashDurability(Base):
    def test_kill9_mid_write_truncates_tail_and_keeps_offsets_contiguous(self):
        self.log = gl.Log(self.dir, segment_bytes=8192)
        self.log.create("t", partitions=1)
        src = CHILD.format(tools=os.path.abspath(TOOLS))
        proc = subprocess.Popen([sys.executable, "-c", src, self.dir, "t", "4000", "1", "8192"],
                                stdout=subprocess.PIPE, text=True)
        acked = -1
        for line in proc.stdout:                      # kill after a real, acked append
            acked = int(line.strip())
            if acked >= 300:
                break
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait()
        proc.stdout.close()
        reopened = gl.Log(self.dir, segment_bytes=8192)
        self.extra = [reopened]
        recs = reopened.read("t", 0, 0, 100000)
        self.assertEqual([r.offset for r in recs], list(range(len(recs))),
                         "offsets must stay contiguous after a torn tail")
        self.assertGreaterEqual(len(recs), acked + 1, "an acked record was lost")
        for r in recs:                                # no CRC garbage returned as data
            self.assertTrue(r.value.startswith(b"v"))
        self.assertGreater(len(reopened._segments("t", 0)), 1, "segment never rolled")
        p, off = reopened.produce("t", key="k", value="after-crash")
        self.assertEqual(off, len(recs), "next offset must follow the truncated tail")


class Groups(Base):
    def test_commits_survive_restart_and_groups_are_independent(self):
        self.log.create("t", partitions=1)
        for i in range(20):
            self.log.produce("t", key="k", value=str(i))
        first = self.log.consume("t", "A", max_records=5)
        self.assertEqual([r.value for r in first], [b"0", b"1", b"2", b"3", b"4"])
        self.log.commit("t", "A", 0, first[-1].offset + 1)
        again = gl.Log(self.dir)                      # a fresh process's view
        self.extra = [again]
        self.assertEqual([r.value for r in again.consume("t", "A", max_records=3)],
                         [b"5", b"6", b"7"])
        self.assertEqual([r.value for r in again.consume("t", "B", max_records=3)],
                         [b"0", b"1", b"2"])
        self.assertEqual(again.groups("t"), ["A"])
        with self.assertRaises(gl.GolemLogError):
            again.commit("t", "A", 0, 1)              # backwards without rewind
        again.commit("t", "A", 0, 1, rewind=True)
        self.assertEqual(again.consume("t", "A", max_records=1)[0].value, b"1")


class Retention(Base):
    def _fill(self, topic, n=400, **kw):
        self.log = gl.Log(self.dir, segment_bytes=4096)
        self.log.create(topic, partitions=1, **kw)
        for i in range(n):
            self.log.produce(topic, key="k", value=("p%03d" % i) * 20)

    def test_deletes_whole_old_segments_never_the_active_one(self):
        self._fill("t", retention_bytes=8192)
        segs_before = len(self.log._segments("t", 0))
        self.assertGreater(segs_before, 3)
        active = self.log._seg("t", 0, self.log._pmeta("t", 0)["active_base"])
        deleted = self.log.retention("t", apply=True)
        self.assertTrue(deleted)
        self.assertNotIn(active, deleted)
        self.assertTrue(os.path.exists(active))
        m = self.log._pmeta("t", 0)
        self.assertGreater(m["earliest_offset"], 0)
        with self.assertRaises(gl.OffsetOutOfRange):
            self.log.read("t", 0, 0, 10)
        live = self.log.read("t", 0, m["earliest_offset"], 100000)
        self.assertEqual([r.offset for r in live],
                         list(range(m["earliest_offset"], m["next_offset"])))

    def test_offsets_never_restart_after_everything_expires(self):
        self._fill("t", retention_secs=0)
        self.log.retention("t", apply=True)
        before = self.log._pmeta("t", 0)["next_offset"]
        _p, off = self.log.produce("t", key="k", value="new")
        self.assertEqual(off, before)
        self.assertGreater(off, 300)

    def test_cleanup_none_never_deletes(self):
        self._fill("t", cleanup="none", retention_bytes=1024, retention_secs=0)
        n = self.log._pmeta("t", 0)["next_offset"]
        self.assertEqual(self.log.retention("t", apply=True), [])
        self.assertEqual(len(self.log.read("t", 0, 0, 100000)), n)
        self.assertEqual(self.log._pmeta("t", 0)["earliest_offset"], 0)


class Corruption(Base):
    def _one_segment(self, n=50):
        self.log.create("t", partitions=1)
        for i in range(n):
            self.log.produce("t", key="k", value="value-%03d" % i)
        return self.log._seg("t", 0, 0)

    def _flip(self, path, pos):
        with open(path, "r+b") as fh:
            fh.seek(pos)
            b = fh.read(1)
            fh.seek(pos)
            fh.write(bytes([b[0] ^ 0xFF]))

    def test_single_byte_flip_mid_segment_raises_and_keeps_earlier_records(self):
        path = self._one_segment()
        recs = self.log.read("t", 0, 0, 100000)
        self.assertEqual(len(recs), 50)
        rec_size = os.path.getsize(path) // 50
        target = 20
        self._flip(path, target * rec_size + gl.HDR.size + 3)   # inside record 20's payload
        with self.assertRaises(gl.CorruptRecord) as ctx:
            self.log.read("t", 0, 0, 100000)
        self.assertEqual(ctx.exception.offset, target)
        earlier = self.log.read("t", 0, 0, target)
        self.assertEqual([r.offset for r in earlier], list(range(target)))
        self.assertEqual(earlier[-1].value, b"value-019")

    def test_corrupt_length_field_before_the_known_end_is_reported_not_truncated(self):
        """A 4 GiB value_len must not allocate — and, since it sits before the
        active segment's last-known end, must be reported rather than silently
        truncated away along with every record after it."""
        path = self._one_segment()
        rec_size = os.path.getsize(path) // 50
        size_before = os.path.getsize(path)
        with open(path, "r+b") as fh:                  # value_len of record 30 -> 4 GiB
            fh.seek(30 * rec_size + 11)
            fh.write((0xFFFFFFF0).to_bytes(4, "little"))
        t0 = time.time()
        with self.assertRaises(gl.CorruptRecord) as ctx:
            self.log.read("t", 0, 0, 100000)           # must not allocate 4 GiB
        self.assertLess(time.time() - t0, 5)
        self.assertEqual(ctx.exception.offset, 30)
        earlier = self.log.read("t", 0, 0, 30)
        self.assertEqual([r.offset for r in earlier], list(range(30)))
        self.assertEqual(os.path.getsize(path), size_before, "records were truncated away")
        self.assertEqual(self.log._pmeta("t", 0)["next_offset"], 50)

    def test_torn_tail_of_the_active_segment_is_still_truncated(self):
        path = self._one_segment()
        with open(path, "ab") as fh:                   # a half-written record
            fh.write(b"\xa1\x01\x00partial")
        recs = self.log.read("t", 0, 0, 100000)
        self.assertEqual([r.offset for r in recs], list(range(50)))
        reopened = gl.Log(self.dir)
        self.extra = [reopened]
        _p, off = reopened.produce("t", key="k", value="after")
        self.assertEqual(off, 50)
        self.assertEqual(reopened.read("t", 0, 50, 5)[0].value, b"after")


class ConcurrentWriters(Base):
    def test_two_processes_5000_appends_each_are_gapless(self):
        self.log = gl.Log(self.dir, segment_bytes=65536)
        self.log.create("t", partitions=1)
        src = CHILD.format(tools=os.path.abspath(TOOLS))
        procs = [subprocess.Popen([sys.executable, "-c", src, self.dir, "t", "5000", "0", "65536"],
                                  stdout=subprocess.DEVNULL) for _ in range(2)]
        for p in procs:
            self.assertEqual(p.wait(), 0)
        recs = gl.Log(self.dir).read("t", 0, 0, 100000)
        self.assertEqual(len(recs), 10000)
        self.assertEqual([r.offset for r in recs], list(range(10000)))


class IngestFleet(Base):
    def _projects(self):
        proot = os.path.join(self.dir, "projects")
        d = os.path.join(proot, "synthetic-app")
        os.makedirs(d)
        self.session = os.path.join(d, "aaaabbbb-1111.jsonl")
        self._write(self.session, self._rows(3))
        return proot

    @staticmethod
    def _rows(n):
        ts = "2026-09-01T10:00:00.000Z"
        rows = []
        for i in range(n):
            rows.append({"type": "user", "timestamp": ts,
                         "message": {"role": "user", "content": "please fix bug %d" % i}})
            rows.append({"type": "assistant", "timestamp": ts,
                         "message": {"id": "m%d" % i, "role": "assistant",
                                     "content": [{"type": "text", "text": "fixed bug %d" % i}]}})
        return rows

    @staticmethod
    def _write(path, rows, mode="w"):
        with open(path, mode) as fh:
            for r in rows:
                fh.write(json.dumps(r) + "\n")

    def test_idempotent_and_incremental(self):
        proot = self._projects()
        first = gl.ingest_fleet(self.log, days=100000, root=proot, partitions=2, out=lambda *_: None)
        self.assertEqual(first["new"], 6)             # 3 human turns + 3 assistant finals
        second = gl.ingest_fleet(self.log, days=100000, root=proot, partitions=2, out=lambda *_: None)
        self.assertEqual(second["new"], 0, "a second run must produce nothing")
        total = sum(len(self.log.read(gl.FLEET_TOPIC, p, 0, 10000)) for p in range(2))
        self.assertEqual(total, 6)
        time.sleep(0.01)
        self._write(self.session, self._rows(1), mode="a")   # the file grows
        third = gl.ingest_fleet(self.log, days=100000, root=proot, partitions=2, out=lambda *_: None)
        self.assertEqual(third["new"], 2, "only the new rows may be produced")
        recs = [json.loads(r.value) for p in range(2)
                for r in self.log.read(gl.FLEET_TOPIC, p, 0, 10000)]
        self.assertEqual(len(recs), 8)
        self.assertEqual({r["repo"] for r in recs}, {"synthetic-app"})
        self.assertEqual(sorted({r["who"] for r in recs}), ["assistant", "human"])
        self.assertTrue(all(r["session"] == "aaaabbbb" for r in recs))

    def test_tags_kind_main_and_subagent(self):
        proot = self._projects()
        sub_dir = os.path.join(proot, "synthetic-app",
                                "aaaabbbb-1111", "subagents")
        os.makedirs(sub_dir)
        self._write(os.path.join(sub_dir, "agent-worker.jsonl"), self._rows(1))
        res = gl.ingest_fleet(self.log, days=100000, root=proot, partitions=2, out=lambda *_: None)
        self.assertEqual(res["main"], 1)
        self.assertEqual(res["subagent"], 1)
        recs = [json.loads(r.value) for p in range(2)
                for r in self.log.read(gl.FLEET_TOPIC, p, 0, 10000)]
        by_kind = {}
        for r in recs:
            by_kind[r["kind"]] = by_kind.get(r["kind"], 0) + 1
        self.assertEqual(by_kind, {"main": 6, "subagent": 2})

    def test_group_resumes_where_it_stopped(self):
        proot = self._projects()
        gl.ingest_fleet(self.log, days=100000, root=proot, partitions=1, out=lambda *_: None)
        batch = self.log.consume(gl.FLEET_TOPIC, "sessionscan", max_records=4)
        self.assertEqual(len(batch), 4)
        self.log.commit(gl.FLEET_TOPIC, "sessionscan", 0, batch[-1].offset + 1)
        rest = gl.Log(self.dir).consume(gl.FLEET_TOPIC, "sessionscan", max_records=100)
        self.assertEqual([r.offset for r in rest], [4, 5])


class Defects(unittest.TestCase):
    """Regression guards for the three defects the Stage 1 verifier found."""

    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="golemlog-defect-")
        self.addCleanup(shutil.rmtree, self.root, True)

    def fill(self, n=600, seg=4096, **kw):
        log = gl.Log(self.root, segment_bytes=seg)
        log.create("t", 1)
        for i in range(n):
            log.produce("t", key="k", value="v%05d" % i, partition=0, **kw)
        log.close()

    def segments(self):
        return sorted(glob.glob(os.path.join(self.root, "t", "0", "*.log")))

    def record_positions(self, path):
        buf = open(path, "rb").read()
        pos, p = [], 0
        while p < len(buf):
            _m, _v, _a, _c, klen, vlen, _ts = gl.HDR.unpack_from(buf, p)
            pos.append(p)
            p += gl.HDR.size + klen + vlen
        return pos

    def read_all(self, seg=4096):
        log = gl.Log(self.root, segment_bytes=seg)
        out, off = [], 0
        while True:
            batch = log.read("t", 0, off, 2000)
            if not batch:
                return out
            out.extend(batch)
            off = batch[-1].offset + 1

    # --- D1: header corruption inside a sealed segment must be reported ---
    def _corrupt_middle_segment(self, field_off, blob):
        self.fill()
        path = self.segments()[1]
        pos = self.record_positions(path)
        with open(path, "r+b") as fh:
            fh.seek(pos[len(pos) // 2] + field_off)
            fh.write(blob)

    def _assert_reported(self):
        try:
            recs = self.read_all()
        except gl.CorruptRecord:
            return
        offs = [r.offset for r in recs]
        gaps = [(a, b) for a, b in zip(offs, offs[1:]) if b != a + 1]
        self.fail("silent gap %s, %d records returned, no CorruptRecord raised"
                  % (gaps, len(recs)))

    def test_oversized_value_len_in_sealed_segment_is_reported(self):
        self._corrupt_middle_segment(VLEN_OFF, struct.pack("<I", 0xFFFFFFFF))
        self._assert_reported()

    def test_oversized_key_len_in_sealed_segment_is_reported(self):
        self._corrupt_middle_segment(KLEN_OFF, struct.pack("<I", 0x0FFFFFFF))
        self._assert_reported()

    def test_bad_magic_in_sealed_segment_is_reported(self):
        self.fill()
        path = self.segments()[-2]                   # last record of an old segment
        with open(path, "r+b") as fh:
            fh.seek(self.record_positions(path)[-1])
            fh.write(b"\x00")
        self._assert_reported()

    def test_records_before_the_damage_still_read(self):
        self._corrupt_middle_segment(VLEN_OFF, struct.pack("<I", 0xFFFFFFFF))
        log = gl.Log(self.root, segment_bytes=4096)
        with self.assertRaises(gl.CorruptRecord) as ctx:
            log.read("t", 0, 0, 2000)
        bad = ctx.exception.offset
        self.assertGreater(bad, 0)
        early = log.read("t", 0, 0, bad)
        self.assertEqual([r.offset for r in early], list(range(bad)))

    # --- D2: commit past next_offset must be rejected ---
    def test_commit_past_next_offset_is_rejected(self):
        self.fill(n=50, seg=8 * 1024 * 1024)
        log = gl.Log(self.root)
        nxt = log._pmeta("t", 0)["next_offset"]
        with self.assertRaises(gl.GolemLogError):
            log.commit("t", "g", 0, nxt + 1000)
        with self.assertRaises(gl.OffsetOutOfRange):
            log.commit("t", "g", 0, nxt + 1)
        self.assertEqual(log.commit("t", "g", 0, nxt), nxt)   # all consumed is fine
        log.close()

    # --- D3: recovery must see the newest segment on disk ---
    def test_produce_offset_matches_where_the_record_lands(self):
        real = gl._write_json
        hits = []

        class Crash(Exception):
            pass

        def hook(path, obj, sync=True):
            if path.endswith(os.path.join("0", "meta.json")) \
                    and obj.get("active_base", 0) > 0 and obj.get("end_pos", 0) < 200:
                hits.append(1)
                raise Crash()                        # die on the post-roll persist
            return real(path, obj, sync)

        gl._write_json = hook
        try:
            log = gl.Log(self.root, segment_bytes=4096)
            log.create("t", 1)
            try:
                for i in range(2000):
                    log.produce("t", key="k", value="v%05d" % i, partition=0, sync=True)
            except Crash:
                pass
        finally:
            gl._write_json = real
        self.assertTrue(hits, "fault injection never fired")

        log = gl.Log(self.root, segment_bytes=4096)
        _p, claimed = log.produce("t", key="k", value=b"marker", partition=0)
        found = [r.offset for r in self.read_all() if r.value == b"marker"]
        self.assertEqual(found, [claimed],
                         "produce returned offset %d, record is readable at %s"
                         % (claimed, found))

    def test_returned_offsets_read_back_across_three_rolls_with_sync(self):
        log = gl.Log(self.root, segment_bytes=2048)
        log.create("t", 1)
        claimed = {}
        for i in range(400):
            _p, off = log.produce("t", key="k", value="v%05d" % i, partition=0, sync=True)
            self.assertNotIn(off, claimed, "offset %d handed out twice" % off)
            claimed[off] = b"v%05d" % i
        log.close()
        self.assertGreaterEqual(len(self.segments()), 4, "fewer than 3 rolls")
        self.assertEqual(sorted(claimed), list(range(400)), "offset stream has a gap")
        back = gl.Log(self.root, segment_bytes=2048)
        for off, value in claimed.items():
            got = back.read("t", 0, off, 1)
            self.assertEqual([r.offset for r in got], [off])
            self.assertEqual(got[0].value, value)
        back.close()


class CLI(Base):
    def _run(self, *argv):
        out = []
        gl.run(["--root", self.dir] + list(argv), out=out.append)
        return "\n".join(str(x) for x in out)

    def test_create_produce_tail_consume_commit(self):
        self._run("create", "t", "--partitions", "2", "--cleanup", "none")
        self.assertTrue(self._run("produce", "t", "hello", "--key", "a").endswith(":0"))
        self._run("produce", "t", "world", "--key", "a")
        self.assertIn("world", self._run("tail", "t", "-n", "1"))
        self.assertEqual(self._run("topics"), "t")
        self.assertIn("hello", self._run("consume", "t", "--group", "g", "--commit"))
        self.assertEqual(self._run("consume", "t", "--group", "g"), "")
        self.assertIn('"cleanup": "none"', self._run("stats", "t"))


# ------------------------------------------------------------ S1.1 compaction ---

COMPACTOR = r"""
import importlib.util, os, sys, time
spec = importlib.util.spec_from_file_location("golemlog", {tools!r} + "/golemlog.py")
gl = importlib.util.module_from_spec(spec); spec.loader.exec_module(gl)
log = gl.Log(sys.argv[1], segment_bytes=int(sys.argv[2]))
print("GO", flush=True)
log.compact("t", min_dirty_ratio=0.0)
print("DONE", flush=True)
"""

SCANNER = r"""
import importlib.util, json, os, sys, time
spec = importlib.util.spec_from_file_location("golemlog", {tools!r} + "/golemlog.py")
gl = importlib.util.module_from_spec(spec); spec.loader.exec_module(gl)
log = gl.Log(sys.argv[1], segment_bytes=int(sys.argv[2]))
deadline = time.time() + float(sys.argv[3])
scans = []
print("GO", flush=True)
while time.time() < deadline:
    recs = log.read("t", 0, 0, 10 ** 6)          # one atomic scan per read call
    latest = {{}}
    offs = [r.offset for r in recs]
    for r in recs:
        latest[r.key.decode()] = r.value.decode()
    scans.append([len(recs), offs == sorted(set(offs)), latest])
print(json.dumps(scans), flush=True)
"""


class CompactBase(Base):
    """Shared fixture: a `cleanup: compact` topic with forced segment rolls."""

    PAD = b"_pad"

    def fill(self, keys=100, n=10000, seg=8192, topic="t", partitions=1):
        self.log = gl.Log(self.dir, segment_bytes=seg)
        self.log.create(topic, partitions=partitions, cleanup="compact")
        want = {}
        for i in range(n):
            k = "k%03d" % (i % keys)
            v = "v%06d" % i
            self.log.produce(topic, key=k, value=v, partition=0)
            want[k.encode()] = v.encode()
        want.update(self.seal(topic, seg))
        self.total = self.log._pmeta(topic, 0)["next_offset"]
        self.log.close()
        self.log = gl.Log(self.dir, segment_bytes=seg)
        return want

    def seal(self, topic="t", seg=8192):
        """Pad until the active segment holds nothing but padding.

        The active segment is never compacted, so without this every real key
        keeps a second, newer copy there and nothing collapses to one record.
        """
        start = len(self.log._segments(topic, 0))
        i = 0
        while len(self.log._segments(topic, 0)) < start + 2:
            self.log.produce(topic, key=self.PAD.decode(), value="pad%06d" % i,
                             partition=0)
            i += 1
        return {self.PAD: ("pad%06d" % (i - 1)).encode()}

    def after_count(self, seg):
        """Records a completed compaction of the current data would leave."""
        scratch = self.dir + "-expect"
        shutil.rmtree(scratch, ignore_errors=True)
        shutil.copytree(self.dir, scratch)
        self.addCleanup(shutil.rmtree, scratch, True)
        probe = gl.Log(scratch, segment_bytes=seg)
        probe.compact("t", min_dirty_ratio=0.0)
        n = len(probe.read("t", 0, 0, 10 ** 6))
        probe.close()
        shutil.rmtree(scratch, ignore_errors=True)
        return n

    def all_records(self, topic="t", p=0):
        return self.log.read(topic, p, 0, 10 ** 6)

    def live(self, topic="t", p=0):
        return {r.key: r.value for r in self.all_records(topic, p)}


class Compaction(CompactBase):
    def test_10k_updates_over_100_keys_collapse_to_the_live_set(self):
        want = self.fill(keys=100, n=10000, seg=8192)
        before = self.all_records()
        self.assertEqual(len(before), self.total)
        rows = self.log.compact("t", min_dirty_ratio=0.0)
        self.assertTrue(rows[0]["compacted"], rows)
        after = self.all_records()
        sealed = self.log._sealed("t", 0)
        sealed_recs = sum(len(self.log._read_segment(s, self.log._base_of(s))[0])
                          for s in sealed)
        self.assertLessEqual(sealed_recs, 101,
                             "sealed segments hold %d records" % sealed_recs)
        offs = [r.offset for r in after]
        self.assertEqual(offs, sorted(offs))
        self.assertEqual(len(offs), len(set(offs)), "an offset appeared twice")
        self.assertEqual({r.key: r.value for r in after}, want)
        for key in want:
            if key == self.PAD:                 # padding lives in the active segment
                continue
            hits = [r for r in after if r.key == key]
            self.assertEqual(len(hits), 1, "key %r survives %d times" % (key, len(hits)))
        self.assertLess(rows[0]["bytes_after"], rows[0]["bytes_before"])

    def test_offsets_and_earliest_survive_and_produce_continues(self):
        self.fill(keys=20, n=2000, seg=4096)
        m0 = dict(self.log._pmeta("t", 0))
        self.log.compact("t", min_dirty_ratio=0.0)
        m1 = self.log._pmeta("t", 0)
        self.assertEqual(m1["next_offset"], m0["next_offset"])
        self.assertEqual(m1["earliest_offset"], m0["earliest_offset"])
        _p, off = self.log.produce("t", key="k000", value="tail", partition=0)
        self.assertEqual(off, m0["next_offset"])
        self.assertEqual(self.log.read("t", 0, off, 1)[0].value, b"tail")

    def test_read_at_a_hole_consume_resumes_and_offset_out_of_range(self):
        self.fill(keys=10, n=800, seg=4096)
        self.log.compact("t", min_dirty_ratio=0.0)
        live = self.all_records()
        present = {r.offset for r in live}
        holes = [o for o in range(0, live[-1].offset) if o not in present]
        self.assertTrue(holes, "compaction left no holes to test")
        hole = holes[0]
        got = self.log.read("t", 0, hole, 5)
        self.assertTrue(got)
        self.assertGreater(got[0].offset, hole)          # skipped, not an error
        self.log.commit("t", "g", 0, hole)               # a commit landing in a hole
        resumed = self.log.consume("t", "g", max_records=3)
        self.assertEqual(resumed[0].offset, got[0].offset)
        self.assertEqual(self.log._pmeta("t", 0)["earliest_offset"], 0)
        self.assertEqual(self.log.read("t", 0, 0, 1)[0].offset, live[0].offset)
        with self.assertRaises(gl.OffsetOutOfRange) as ctx:
            self.log.read("t", 0, -1, 1)
        self.assertEqual(ctx.exception.earliest, 0)

    def test_compact_topics_are_immune_to_retention(self):
        want = self.fill(keys=10, n=1000, seg=4096)
        segs = len(self.log._segments("t", 0))
        self.assertGreater(segs, 3)
        self.assertEqual(self.log.retention("t", apply=True), [])
        self.assertEqual(len(self.log._segments("t", 0)), segs)
        self.assertEqual(len(self.all_records()), self.total)
        with self.assertRaises(gl.GolemLogError):
            self.log.create("u", cleanup="compact", retention_secs=60)
        self.log.compact("t", min_dirty_ratio=0.0)
        self.assertEqual(self.live(), want)

    def test_dirty_ratio_skips_a_clean_partition_and_force_overrides(self):
        self.fill(keys=10, n=1000, seg=4096)
        self.assertTrue(self.log.compact("t", min_dirty_ratio=0.0)[0]["compacted"])
        row = self.log.compact("t", min_dirty_ratio=0.5)[0]
        self.assertFalse(row["compacted"])
        self.assertIn("dirty ratio", row["skipped"])
        self.assertTrue(self.log.compact("t", min_dirty_ratio=0.0)[0]["compacted"])


class Tombstones(CompactBase):
    def _setup(self, victim=b"k003"):
        want = self.fill(keys=10, n=1000, seg=4096)
        self.log.produce("t", key=victim.decode(), partition=0, tombstone=True)
        want.update(self.seal("t", 4096))          # seal the tombstone's segment
        want.pop(victim)
        return want

    def test_tombstone_survives_one_pass_then_is_removed_after_the_window(self):
        want = self._setup()
        self.log.compact("t", min_dirty_ratio=0.0)
        recs = self.all_records()
        tombs = [r for r in recs if r.attrs & gl.ATTR_TOMB]
        self.assertEqual(len(tombs), 1, "tombstone was dropped on its first pass")
        self.assertEqual(tombs[0].key, b"k003")
        m = self.log._pmeta("t", 0)
        self.assertTrue(m.get("compacted"), "no first_retained_at recorded")

        real = gl.time.time
        gl.time.time = lambda: real() + 25 * 3600        # past the 24 h window
        try:
            self.log.compact("t", min_dirty_ratio=0.0)
        finally:
            gl.time.time = real
        recs = self.all_records()
        self.assertEqual([r for r in recs if r.attrs & gl.ATTR_TOMB], [])
        self.assertEqual({r.key: r.value for r in recs}, want)

    def _gc_after_the_window(self):
        real = gl.time.time
        gl.time.time = lambda: real() + 25 * 3600        # past the 24 h window
        try:
            self.log.compact("t", min_dirty_ratio=0.0)
        finally:
            gl.time.time = real
        return [r for r in self.all_records() if r.attrs & gl.ATTR_TOMB]

    def test_a_committed_offset_below_the_tombstone_is_not_a_gc_floor(self):
        """D-gc: Kafka semantics. `delete_retention_ms` is the only contract.

        golemlog used to hold a tombstone until every consumer group had
        committed past it, which meant a lagging group pinned it indefinitely.
        Kafka does not do this: the window is the guarantee, and it must be set
        wider than the worst consumer lag. The old floor is gone.
        """
        self._setup()
        self.log.compact("t", min_dirty_ratio=0.0)
        self.log.commit("t", "slow", 0, 0)               # committed below the tombstone
        self.assertEqual(self._gc_after_the_window(), [],
                         "a lagging group still pins the tombstone")
        self.assertNotIn("gc_floor", self.log.compact("t", min_dirty_ratio=0.0)[0])

    def test_a_group_that_never_committed_this_partition_does_not_block_gc(self):
        """D-gc: a group with no commit on this partition once froze GC forever.

        The floor was `-1` if any group's entry for the partition was missing,
        so a group that only ever committed a *different* partition — an ad-hoc
        probe, say — stopped every tombstone on this one being collected, with
        no CLI to clear it. `groups` and `group-drop` now exist for housekeeping,
        but neither is needed to make GC run.
        """
        self._setup()
        self.log.compact("t", min_dirty_ratio=0.0)
        self.log.commit("t", "other", 0, 0, rewind=True)
        gl._write_json(self.log._goff_path("t", "other"), {})   # a group with no commit
        self.assertEqual(self._gc_after_the_window(), [])

    def test_a_group_on_another_partition_does_not_block_gc_here(self):
        self.log = gl.Log(self.dir, segment_bytes=4096)
        self.log.create("t", partitions=2, cleanup="compact")
        for i in range(1000):
            self.log.produce("t", key="k%03d" % (i % 10), value="v%06d" % i, partition=0)
        self.log.produce("t", key="k003", partition=0, tombstone=True)
        self.seal("t", 4096)
        self.log.commit("t", "adhoc-probe", 1, 0)        # never touches partition 0
        self.log.compact("t", 0, min_dirty_ratio=0.0)
        self.assertEqual(self._gc_after_the_window(), [])

    def test_groups_and_group_drop(self):
        self.fill(keys=5, n=200, seg=4096)
        self.log.commit("t", "g1", 0, 3)
        self.log.close()
        out = []
        gl.run(["--root", self.dir, "groups", "t", "--json"], out=out.append)
        blob = json.loads(out[-1])
        self.assertEqual(blob["groups"], [{"group": "g1", "offsets": {"0": 3}}])
        gl.run(["--root", self.dir, "group-drop", "t", "--group", "g1"], out=out.append)
        self.assertIn("dropped", out[-1])
        self.assertEqual(gl.Log(self.dir).groups("t"), [])
        gl.run(["--root", self.dir, "group-drop", "t", "--group", "g1"], out=out.append)
        self.assertIn("no such group", out[-1])

    def test_a_keyless_tombstone_is_rejected(self):
        """D-gc: a keyless tombstone deleted nothing and was never collected.

        `_scan` treats any keyless record as "no identity: never compacted", so
        a tombstone with no key sat in the compacted segments for the life of
        the topic while deleting no key at all. It is refused at `produce`.
        """
        self.fill(keys=4, n=160, seg=4096)
        with self.assertRaises(gl.GolemLogError):
            self.log.produce("t", value="", partition=0, tombstone=True)
        with self.assertRaises(gl.GolemLogError):
            self.log.produce("t", key="", value="", partition=0, tombstone=True)
        self.log.produce("t", key="k000", partition=0, tombstone=True)   # still fine
        out = []
        gl.run(["--root", self.dir, "produce", "t", "x", "--tombstone"], out=out.append)
        self.assertIn("tombstone needs a key", out[-1])


class CompactionDurability(CompactBase):
    def test_kill9_during_compaction_leaves_old_or_new_never_neither(self):
        src = COMPACTOR.format(tools=os.path.abspath(TOOLS))
        landed = 0
        for trial in range(20):
            shutil.rmtree(self.dir, ignore_errors=True)
            os.makedirs(self.dir)
            want = self.fill(keys=40, n=1500, seg=4096)
            self.log.close()
            before = len(gl.Log(self.dir).read("t", 0, 0, 10 ** 6))
            after = self.after_count(4096)
            child = subprocess.Popen(
                [sys.executable, "-c", src, self.dir, "4096"],
                stdout=subprocess.PIPE, text=True)
            self.assertEqual(child.stdout.readline().strip(), "GO")
            time.sleep(random.uniform(0.0, 0.03))
            child.kill()
            child.wait()
            reopened = gl.Log(self.dir, segment_bytes=4096)
            recs = reopened.read("t", 0, 0, 10 ** 6)
            offs = [r.offset for r in recs]
            self.assertEqual(offs, sorted(set(offs)),
                             "trial %d: duplicate or unordered offsets" % trial)
            latest = {}
            for r in recs:
                latest[r.key] = r.value
            self.assertEqual(latest, want, "trial %d: lost or wrong live data" % trial)
            self.assertIn(len(recs), (before, after),
                          "trial %d: %d records is neither old (%d) nor new (%d)"
                          % (trial, len(recs), before, after))
            self.assertFalse(os.path.exists(reopened._plan_path("t", 0)),
                             "trial %d: plan not replayed on reopen" % trial)
            if len(recs) == after:
                landed += 1
            reopened.close()
        self.assertGreater(landed, 0, "no trial ever completed a compaction")

    def test_a_reader_scanning_through_a_compaction_never_sees_a_short_read(self):
        want = self.fill(keys=50, n=4000, seg=8192)
        self.log.close()
        before = len(gl.Log(self.dir).read("t", 0, 0, 10 ** 6))
        src = SCANNER.format(tools=os.path.abspath(TOOLS))
        reader = subprocess.Popen([sys.executable, "-c", src, self.dir, "8192", "2.0"],
                                  stdout=subprocess.PIPE, text=True)
        self.assertEqual(reader.stdout.readline().strip(), "GO")
        log = gl.Log(self.dir, segment_bytes=8192)
        log.compact("t", min_dirty_ratio=0.0)
        after = len(log.read("t", 0, 0, 10 ** 6))
        log.close()
        scans = json.loads(reader.stdout.readline())
        self.assertEqual(reader.wait(), 0)
        self.assertGreater(len(scans), 2, "reader made too few scans to prove anything")
        expect = {k.decode(): v.decode() for k, v in want.items()}
        seen_after = 0
        for i, (count, ascending, latest) in enumerate(scans):
            # The design permits a *mixed* view: a scan straddling the swap sees
            # some old and some new segments, so its record count is legitimately
            # neither `before` nor `after`. What it may never do is reorder or
            # repeat an offset, lose a live key, or raise. (D-s2-2: the old
            # `count in (before, after)` assertion contradicted the design.)
            self.assertTrue(ascending, "scan %d returned unordered/duplicate offsets" % i)
            self.assertGreaterEqual(count, after,
                                    "scan %d returned %d records, fewer than the "
                                    "compacted total %d" % (i, count, after))
            self.assertLessEqual(count, before,
                                 "scan %d returned %d records, more than the "
                                 "uncompacted total %d" % (i, count, before))
            self.assertEqual(latest, expect, "scan %d lost a live key" % i)
            seen_after += count == after
        self.assertGreater(seen_after, 0, "the compaction never landed during the scan")

    def test_read_retries_once_when_a_segment_vanishes_mid_read(self):
        want = self.fill(keys=20, n=1200, seg=4096)
        real = gl.Log._read_segment
        fired = []

        def flaky(path, base, start=0):
            if not fired and path.endswith(".log") and gl.Log._base_of(path) > 0:
                fired.append(path)
                raise FileNotFoundError(2, "No such file or directory", path)
            return real(path, base, start)

        gl.Log._read_segment = staticmethod(flaky)
        try:
            recs = self.log.read("t", 0, 0, 10 ** 6)
        finally:
            gl.Log._read_segment = staticmethod(real)
        self.assertTrue(fired, "the ENOENT was never injected")
        self.assertEqual(len(recs), self.total, "ENOENT produced a short read")
        self.assertEqual({r.key: r.value for r in recs}, want)

    def test_a_non_enoent_oserror_propagates(self):
        self.fill(keys=5, n=100, seg=4096)
        real = gl.Log._read_segment

        def boom(path, base, start=0):
            raise PermissionError(13, "denied", path)

        gl.Log._read_segment = staticmethod(boom)
        try:
            with self.assertRaises(PermissionError):
                self.log.read("t", 0, 0, 10)
        finally:
            gl.Log._read_segment = staticmethod(real)


class CompactionBounds(CompactBase):
    def test_multi_pass_by_key_hash_matches_the_single_pass_output(self):
        self.fill(keys=60, n=3000, seg=4096)
        self.log.close()
        single, multi = self.dir + "-one", self.dir + "-many"
        for d in (single, multi):
            shutil.copytree(self.dir, d)
            self.addCleanup(shutil.rmtree, d, True)
        one = gl.Log(single, segment_bytes=4096)
        many = gl.Log(multi, segment_bytes=4096)
        self.extra = [one, many]
        row1 = one.compact("t", min_dirty_ratio=0.0)[0]
        row2 = many.compact("t", min_dirty_ratio=0.0, max_map_mb=0.0005)[0]
        self.assertEqual(row1["passes"], 1)
        self.assertGreater(row2["passes"], 1, "the multi-pass path was never taken")
        self.assertLessEqual(row2["max_entries"], 5)
        a = [(r.offset, r.key, r.value, r.attrs) for r in one.read("t", 0, 0, 10 ** 6)]
        b = [(r.offset, r.key, r.value, r.attrs) for r in many.read("t", 0, 0, 10 ** 6)]
        self.assertEqual(a, b)
        self.assertEqual(row1["live_records"], row2["live_records"])

    def test_entry_cost_is_measured_and_plausible(self):
        cost = gl._entry_cost()
        self.assertGreater(cost, 120)
        self.assertLess(cost, 600)

    def test_null_key_records_are_never_compacted(self):
        self.log = gl.Log(self.dir, segment_bytes=4096)
        self.log.create("t", partitions=1, cleanup="compact")
        for i in range(400):
            self.log.produce("t", key=None, value="anon%04d" % i, partition=0)
            self.log.produce("t", key="k", value="v%04d" % i, partition=0)
        self.seal("t", 4096)
        self.log.compact("t", min_dirty_ratio=0.0)
        recs = self.all_records()
        anon = [r for r in recs if r.key == b""]
        self.assertEqual(len(anon), 400)
        self.assertEqual(len([r for r in recs if r.key == b"k"]), 1)


class CompactionMeta(CompactBase):
    def test_compaction_fields_survive_1000_later_producer_appends(self):
        self.fill(keys=20, n=1000, seg=4096)
        self.log.compact("t", min_dirty_ratio=0.0)
        disk = json.load(open(self.log._pmeta_path("t", 0)))
        self.assertIn("clean_bytes", disk)
        self.assertIn("compacted", disk)
        clean = disk["clean_bytes"]

        fresh = gl.Log(self.dir, segment_bytes=4096)     # a producer that cached nothing
        self.extra = [fresh]
        for i in range(1000):
            fresh.produce("t", key="k%03d" % (i % 20), value="late%05d" % i, partition=0)
        fresh.close()
        disk = json.load(open(self.log._pmeta_path("t", 0)))
        self.assertEqual(disk.get("clean_bytes"), clean,
                         "the producer's lazy meta persist dropped clean_bytes")
        self.assertIn("compacted", disk)
        self.assertGreaterEqual(disk["next_offset"], 2000)

    def test_rebuild_leaves_earliest_offset_at_the_segment_base(self):
        self.fill(keys=10, n=1000, seg=4096)
        self.log.compact("t", min_dirty_ratio=0.0)
        first = self.all_records()[0].offset
        self.assertGreater(first, 0, "compaction dropped nothing at the head")
        self.log.close()
        os.remove(os.path.join(self.dir, "t", "0", "meta.json"))
        rebuilt = gl.Log(self.dir, segment_bytes=4096)
        self.extra = [rebuilt]
        m = rebuilt._pmeta("t", 0)
        self.assertEqual(m["earliest_offset"], 0,
                         "_rebuild raised earliest_offset over a compacted hole")
        self.assertEqual(rebuilt.read("t", 0, 0, 1)[0].offset, first)

    def test_cli_compact_round_trip(self):
        out = []
        gl.run(["--root", self.dir, "create", "c", "--cleanup", "compact"], out=out.append)
        for i in range(3):
            gl.run(["--root", self.dir, "produce", "c", "v%d" % i, "--key", "k"],
                   out=out.append)
        gl.run(["--root", self.dir, "produce", "c", "x", "--key", "gone", "--tombstone"],
               out=out.append)
        gl.run(["--root", self.dir, "compact", "c", "--force"], out=out.append)
        text = "\n".join(str(x) for x in out)
        self.assertIn("skipped: no sealed segments", text)
        out2 = []
        gl.run(["--root", self.dir, "stats", "c"], out=out2.append)
        blob = json.loads("\n".join(str(x) for x in out2))
        self.assertEqual(blob["cleanup"], "compact")
        self.assertIn("dirty_ratio", blob["partitions"][0])


# ------------------------------------------------------- S1.1 locking (D-lock) ---

STALLED_READER = r"""
import importlib.util, os, sys, time
spec = importlib.util.spec_from_file_location("golemlog", {tools!r} + "/golemlog.py")
gl = importlib.util.module_from_spec(spec); spec.loader.exec_module(gl)
real, stall = gl.Log._read_segment, float(sys.argv[2])

def slow(path, base, start=0):
    out = real(path, base, start)
    if not hasattr(slow, "done"):        # stall once, mid-scan, holding the fd
        slow.done = True
        sys.stdout.write("READY\n"); sys.stdout.flush()
        time.sleep(stall)
    return out

gl.Log._read_segment = staticmethod(slow)
gl.Log(sys.argv[1]).read("t", 0, 0, 10 ** 6)
"""


class LockingBase(Base):
    def setUp(self):
        super().setUp()
        gl._Lock._state.clear()
        self.addCleanup(gl._Lock._state.clear)

    def lock_path(self, log):
        return os.path.join(log._pdir("t", 0), "partition.lock")

    def seeded(self):
        log = gl.Log(self.dir)
        log.create("t", 1, "compact")
        log.produce("t", key="seed", value="x", partition=0)
        self.extra = [log]
        return log


class ReadersTakeNoLock(LockingBase):
    """D-lock: a reader used to stall every producer on the partition 1:1.

    `read` took `partition.lock` LOCK_SH for the whole scan and `produce` takes
    it LOCK_EX per append, so a slow or hung consumer halted every producer:
    measured, a reader stalled 3.0 s delayed the next append by 2.98 s, and two
    writers × 5 000 appends went from 1.20 s to 8.38 s (7.0x) against one hot
    reader. Readers now take no lock; compaction is made safe for them by
    rewriting each sealed segment in place.
    """

    def test_a_stalled_reader_does_not_stall_the_producer(self):
        log = self.seeded()
        log.close()
        stall = 3.0
        src = STALLED_READER.format(tools=os.path.abspath(TOOLS))
        r = subprocess.Popen([sys.executable, "-u", "-c", src, self.dir, str(stall)],
                             stdout=subprocess.PIPE, text=True)
        self.addCleanup(r.kill)
        self.assertEqual(r.stdout.readline().strip(), "READY")
        fresh = gl.Log(self.dir)
        self.extra = [fresh]
        t0 = time.time()
        fresh.produce("t", key="b", value="y", partition=0)
        blocked = time.time() - t0
        self.assertLess(blocked, 0.05,
                        "producer blocked %.3fs by a reader stalled %.1fs mid-scan"
                        % (blocked, stall))

    def test_read_never_touches_the_partition_lock(self):
        log = self.seeded()
        for i in range(50):
            log.produce("t", key="k%d" % (i % 5), value="v%d" % i, partition=0)
        taken, real = [], gl._Lock.__enter__

        def spy(self):
            taken.append(self.path)
            return real(self)

        gl._Lock.__enter__ = spy
        try:
            self.assertEqual(len(log.read("t", 0, 0, 10 ** 6)), 51)
            self.assertEqual(log.consume("t", "g", 10 ** 6), log.read("t", 0, 0, 10 ** 6))
            self.assertTrue(log.tail("t", 5))
        finally:
            gl._Lock.__enter__ = real
        self.assertEqual(taken, [], "the read path took %d locks" % len(taken))


class LockExclusion(LockingBase):
    """D-lock: the lock was exclusive against neither threads nor forks.

    `_Lock` cached one fd per path in a class attribute and depth-counted
    acquisition, so (a) a second *thread* asking for the exclusive lock while
    another held it was granted immediately and silently, flock being
    per-process, and (b) a *forked* child inherited the cached fd — the same
    open file description — so its flock was a no-op against the parent's and
    its release dropped the parent's lock too.
    """

    def test_exclusive_waits_for_a_holder_in_another_thread(self):
        log = self.seeded()
        path, held, hold_for = self.lock_path(log), threading.Event(), 0.6

        def other():
            with gl._Lock(path, shared=True):
                held.set()
                time.sleep(hold_for)

        th = threading.Thread(target=other)
        th.start()
        self.addCleanup(th.join)
        self.assertTrue(held.wait(5))
        t0 = time.time()
        with gl._Lock(path):
            waited = time.time() - t0
        self.assertGreater(waited, hold_for / 2,
                           "exclusive lock granted after %.3fs while another thread "
                           "held it" % waited)

    def test_the_lock_is_still_reentrant_within_one_thread(self):
        log = self.seeded()
        path = self.lock_path(log)
        with gl._Lock(path):
            with gl._Lock(path, shared=True):        # nested acquire, never a downgrade
                self.assertFalse(gl._Lock._state[path]["shared"])
            with gl._Lock(path):
                pass
        self.assertEqual(gl._Lock._state[path]["depth"], 0)

    def test_a_forked_child_neither_takes_nor_releases_the_parents_lock(self):
        log = self.seeded()                      # populates the cached fd
        path = self.lock_path(log)
        with gl._Lock(path):
            pid = os.fork()
            if pid == 0:                         # child: must block, then give up
                code = 4
                try:
                    signal.signal(signal.SIGALRM, _alarm)
                    signal.setitimer(signal.ITIMER_REAL, 1.0)
                    with gl._Lock(path):
                        code = 1                 # acquired: it broke the parent's lock
                    signal.setitimer(signal.ITIMER_REAL, 0)
                except _Timeout:
                    code = 3                     # blocked, as it must be
                except BaseException:
                    code = 5
                os._exit(code)
            _pid, status = os.waitpid(pid, 0)
            # The child closed the fd it inherited; the parent's own fd keeps the
            # open file description, and with it the flock, alive. A second fd in
            # this process is a separate description and must still be refused.
            fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
            try:
                with self.assertRaises(BlockingIOError):
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally:
                os.close(fd)
        self.assertEqual(os.waitstatus_to_exitcode(status), 3,
                         "the forked child did not block on the parent's lock")


# --------------------------------------------------- S1.1 topic policy (D-policy) ---

class CleanupPolicySwitch(Base):
    """D-policy: re-creating a topic with a different cleanup was a silent no-op.

    `create` returned the stored meta untouched and the CLI printed the policy
    it had been *asked* for, so an operator was told the topic was now
    `cleanup=compact` when it was still `delete`, and only found out when a
    later `compact` failed with "topic t is not cleanup: compact".
    """

    def make(self, **kw):
        log = gl.Log(self.dir)
        log.create("t", 1, **kw)
        for i in range(5):
            log.produce("t", key="k", value="v%d" % i)
        log.close()

    def test_changing_cleanup_on_an_existing_topic_raises(self):
        self.make(cleanup="delete")
        with self.assertRaises(gl.GolemLogError) as ctx:
            gl.Log(self.dir).create("t", 1, "compact")
        self.assertIn("cleanup='delete'", str(ctx.exception))

    def test_changing_retention_on_an_existing_topic_raises(self):
        self.make(cleanup="delete", retention_secs=60)
        with self.assertRaises(gl.GolemLogError):
            gl.Log(self.dir).create("t", 1, "delete", retention_secs=120)
        self.assertEqual(gl.Log(self.dir).create("t", 1, "delete", retention_secs=60)
                         ["retention_secs"], 60)      # an identical create is a no-op

    def test_cli_create_reports_the_policy_on_disk_not_the_one_requested(self):
        self.make(cleanup="delete")
        said = []
        gl.run(["--root", self.dir, "create", "t", "--cleanup", "compact"],
               out=said.append)
        text = " ".join(str(x) for x in said)
        on_disk = json.load(open(os.path.join(self.dir, "t", "meta.json")))["cleanup"]
        self.assertEqual(on_disk, "delete")
        self.assertNotIn("cleanup=compact", text)
        self.assertIn("cleanup='delete'", text)
        said.clear()
        gl.run(["--root", self.dir, "create", "t", "--cleanup", "delete"],
               out=said.append)
        self.assertIn("cleanup=delete", " ".join(str(x) for x in said))


# ----------------------------------------------------- S1.1 JSON output (D-json) ---

class JsonOutput(Base):
    def _rows(self, *argv):
        lines = []
        gl.run(["--root", self.dir] + list(argv), out=lines.append)
        return [json.loads(x) for x in lines]

    def test_consume_json_is_one_object_per_line_and_round_trips(self):
        log = gl.Log(self.dir)
        log.create("t", 1, "none")
        log.produce("t", key="a b", value='{"x": 1}\nsecond line')
        log.produce("t", key="raw", value=b"\xff\xfe")
        log.close()
        rows = self._rows("consume", "t", "--group", "g", "--json")
        self.assertEqual(len(rows), 2, "a record spanned more than one line")
        self.assertEqual(set(rows[0]), {"topic", "partition", "offset", "ts_ms",
                                        "key", "value", "tombstone"})
        self.assertEqual(rows[0]["topic"], "t")
        self.assertEqual(rows[0]["key"], "a b")
        self.assertEqual(rows[0]["value"], '{"x": 1}\nsecond line')
        self.assertFalse(rows[0]["tombstone"])
        self.assertGreater(rows[0]["ts_ms"], 0)
        self.assertEqual(rows[1]["value"], {"b64": "//4="})
        self.assertEqual(base64.b64decode(rows[1]["value"]["b64"]), b"\xff\xfe")

    def test_tail_json_carries_the_tombstone_flag(self):
        log = gl.Log(self.dir)
        log.create("t", 1, "compact")
        log.produce("t", key="k", value="v", partition=0)
        log.produce("t", key="k", partition=0, tombstone=True)
        log.close()
        rows = self._rows("tail", "t", "-n", "2", "--json")
        self.assertEqual([r["tombstone"] for r in rows], [False, True])
        self.assertEqual([r["offset"] for r in rows], [0, 1])
        self.assertEqual(rows[1]["value"], "")

    def test_ingest_fleet_values_stay_raw_json_strings(self):
        log = gl.Log(self.dir)
        log.create("f", 1, "none")
        payload = {"repo": "demo-repo", "who": "human", "text": "hi"}
        log.produce("f", key="demo-repo", value=json.dumps(payload))
        log.close()
        row = self._rows("consume", "f", "--group", "g", "--json")[0]
        self.assertIsInstance(row["value"], str)      # not re-nested as an object
        self.assertEqual(json.loads(row["value"]), payload)


LOCK_FREE_READER = r"""
import importlib.util, json, sys, time
spec = importlib.util.spec_from_file_location("golemlog", %r)
gl = importlib.util.module_from_spec(spec); spec.loader.exec_module(gl)
log = gl.Log(sys.argv[1], segment_bytes=8192)
t = time.time()
recs = log.read("t", 0, 0, 10 ** 6)
ms = (time.time() - t) * 1000
offs = [r.offset for r in recs]
latest = {}
for r in recs:
    latest[r.key.decode()] = r.value.decode()
print(json.dumps([len(recs), ms, offs == sorted(set(offs)), latest]))
""" % (os.path.join(os.path.abspath(TOOLS), "golemlog.py"),)


class LockFreeRead(CompactBase):
    """D-s2-1: `Log.read` is documented lock-free but blocked on the swap.

    `read` -> `_read_scan` -> `_pmeta` replayed a pending `compact.plan`, and
    `_replay_plan` took the partition lock *exclusively*. While the compactor
    held that lock for its swap, every reader — including every `golemkv` get,
    which the S2 design states takes no lock — waited for the whole swap. The
    read path now tries the lock `LOCK_NB` and, when it is busy, scans whatever
    segment files exist: in-place `os.replace` keeps every offset in either the
    old or the new file, so a mixed view loses no live key.
    """

    def test_a_reader_does_not_block_behind_a_compaction_swap(self):
        want = self.fill(keys=50, n=4000, seg=8192)
        self.log.close()

        real = os.replace
        seen = []

        def slow(src, dst):
            out = real(src, dst)
            if str(src).endswith(".log.tmp") and len(seen) < 1:
                seen.append(src)
                seen.append(subprocess.run(
                    [sys.executable, "-c", LOCK_FREE_READER, self.dir],
                    capture_output=True, text=True, timeout=30))
            return out

        os.replace = slow
        try:
            writer = gl.Log(self.dir, segment_bytes=8192)
            writer.compact("t", min_dirty_ratio=0.0)
            writer.close()
        finally:
            os.replace = real

        self.assertEqual(len(seen), 2, "the swap never ran")
        child = seen[1]
        self.assertEqual(child.returncode, 0, child.stderr[-400:])
        n, ms, ascending, latest = json.loads(child.stdout)
        self.assertLess(ms, 200, "a lock-free read blocked %.0f ms on the swap" % ms)
        self.assertTrue(ascending, "the read returned unordered/duplicate offsets")
        self.assertGreater(n, 0)
        self.assertEqual(latest, {k.decode(): v.decode() for k, v in want.items()},
                         "the read during the swap lost a live key")


if __name__ == "__main__":
    unittest.main()
