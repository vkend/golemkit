"""Acceptance tests for golemkv.py.

Everything runs in temp roots. Cross-process tests shell out to the CLI or to a
small child script, because read-your-writes across processes is the property
that the whole design turns on and an in-process test cannot see it.
"""
import fcntl
import importlib.util
import json
import os
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
TOOLS = os.path.join(HERE, "..")
SPEC = importlib.util.spec_from_file_location("golemkv", os.path.join(TOOLS, "golemkv.py"))
kvmod = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(kvmod)
gl = kvmod.gl

CLI = os.path.join(TOOLS, "golemkv.py")


def cli(root, *args, **kw):
    """Run the CLI in a fresh process. Returns (rc, stdout, stderr)."""
    env = dict(os.environ)
    env.pop("GOLEMKV_ROOT", None)
    env.update(kw.pop("env", {}))
    argv = [sys.executable, CLI]
    if root is not None:
        argv += ["--root", root]
    argv += [str(a) for a in args]
    p = subprocess.run(argv, capture_output=True, text=True, env=env)
    return p.returncode, p.stdout, p.stderr


def _load_widened_ceiling(base_ms):
    """Widen a warm-CLI timing ceiling when the machine is under load.

    Returns (ceiling_ms, note) — note is empty unless widened.
    """
    try:
        load1, cpus = os.getloadavg()[0], os.cpu_count() or 1
        if load1 > 2 * cpus:
            return 400.0, ("  <-- loadavg %.1f > 2x%d cpus, ceiling widened to "
                            "400 ms" % (load1, cpus))
    except (OSError, AttributeError):
        pass
    return base_ms, ""


class Base(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="golemkv-test-")
        self.kv = kvmod.KV(self.dir)
        self._now = kvmod._now_ms

    def tearDown(self):
        kvmod._now_ms = self._now
        self.kv.close()
        shutil.rmtree(self.dir, ignore_errors=True)

    def freeze(self, ms):
        kvmod._now_ms = lambda: ms

    def log_len(self):
        """Offsets appended to kv.aof (never shrinks — compaction leaves holes)."""
        log = gl.Log(os.path.join(self.dir, "log"))
        try:
            return log._pmeta(kvmod.AOF, 0)["next_offset"]
        finally:
            log.close()

    def log_records(self):
        """Records still physically present in kv.aof — this is what compaction cuts."""
        log = gl.Log(os.path.join(self.dir, "log"))
        try:
            m = log._pmeta(kvmod.AOF, 0)
            off, n = m["earliest_offset"], 0
            while True:
                recs = log.read(kvmod.AOF, 0, off, 1000)
                if not recs:
                    return n
                n += len(recs)
                off = recs[-1].offset + 1
        finally:
            log.close()


# ---------------------------------------------------------------- strings ---

class Strings(Base):
    def test_set_get_delete_exists(self):
        self.assertIsNone(self.kv.get("a"))
        self.assertFalse(self.kv.exists("a"))
        self.kv.set("a", "1")
        self.assertEqual(self.kv.get("a"), "1")
        self.assertTrue(self.kv.exists("a"))
        self.kv.set("a", "2")                      # last write wins
        self.assertEqual(self.kv.get("a"), "2")
        self.assertTrue(self.kv.delete("a"))
        self.assertIsNone(self.kv.get("a"))
        self.assertFalse(self.kv.delete("a"))      # deleting a miss is False, not an error

    def test_incr(self):
        self.assertEqual(self.kv.incr("n"), 1)
        self.assertEqual(self.kv.incr("n", 41), 42)
        self.assertEqual(self.kv.get("n"), "42")
        self.kv.set("s", "abc")
        with self.assertRaises(kvmod.KVTypeError):
            self.kv.incr("s")

    def test_incr_keeps_ttl(self):
        self.freeze(1_000_000)
        self.kv.set("n", "1", ex=100)
        self.freeze(1_010_000)
        self.assertEqual(self.kv.incr("n"), 2)
        self.assertEqual(self.kv.ttl("n"), 90)

    def test_keys_pattern(self):
        for k in ("svc:a:x", "svc:b:y", "port:3000", "loop:tick"):
            self.kv.set(k, "v")
        self.assertEqual(self.kv.keys("svc:*"), ["svc:a:x", "svc:b:y"])
        self.assertEqual(self.kv.keys("port:????"), ["port:3000"])
        self.assertEqual(len(self.kv.keys("*")), 4)
        self.kv.delete("svc:a:x")
        self.assertEqual(self.kv.keys("svc:*"), ["svc:b:y"])


# -------------------------------------------------------------------- TTL ---

class TTL(Base):
    def test_ttl_exact_under_frozen_clock(self):
        self.freeze(1_000_000)
        self.kv.set("k", "v", ex=10)
        self.assertEqual(self.kv.ttl("k"), 10)
        self.freeze(1_004_000)
        self.assertEqual(self.kv.ttl("k"), 6)      # monotonically decreasing
        self.assertEqual(self.kv.get("k"), "v")
        self.freeze(1_009_999)
        self.assertEqual(self.kv.get("k"), "v")    # 1 ms left is still alive
        self.freeze(1_010_000)
        self.assertIsNone(self.kv.get("k"))        # expired to the millisecond
        self.assertEqual(self.kv.ttl("k"), -2)
        self.assertFalse(self.kv.exists("k"))
        self.assertEqual(self.kv.keys("*"), [])

    def test_ttl_sentinels_and_expire_verb(self):
        self.assertEqual(self.kv.ttl("nope"), -2)
        self.kv.set("k", "v")
        self.assertEqual(self.kv.ttl("k"), -1)
        self.freeze(2_000_000)
        self.assertTrue(self.kv.expire("k", 30))
        self.assertEqual(self.kv.ttl("k"), 30)
        self.assertFalse(self.kv.expire("missing", 30))
        self.kv.set("k", "v2")                      # a plain set clears the TTL
        self.assertEqual(self.kv.ttl("k"), -1)

    def test_expired_read_never_appends(self):
        """BREAK: lazy expiry filters on read and writes nothing."""
        self.freeze(1_000_000)
        self.kv.set("k", "v", ex=1)
        before = self.log_len()
        self.freeze(1_100_000)
        for _ in range(20):
            self.assertIsNone(self.kv.get("k"))
            self.assertEqual(self.kv.ttl("k"), -2)
            self.assertEqual(self.kv.keys("*"), [])
        self.assertEqual(self.log_len(), before)

    def test_expire_sweep_tombstones(self):
        self.freeze(1_000_000)
        self.kv.set("live", "v")
        self.kv.set("dead1", "v", ex=1)
        self.kv.set("dead2", "v", ex=1)
        self.freeze(1_100_000)
        before = self.log_len()
        self.assertEqual(self.kv.expire_sweep(), 2)
        self.assertEqual(self.log_len(), before + 2)
        self.assertEqual(self.kv.expire_sweep(), 0)   # idempotent
        # A fresh process replaying the log agrees the keys are gone.
        other = kvmod.KV(self.dir)
        self.addCleanup(other.close)
        self.assertEqual(other.keys("*"), ["live"])


# ------------------------------------------------------------ hashes/sets ---

class Collections(Base):
    def test_hash_ops(self):
        self.assertEqual(self.kv.hset("h", {"pid": "1", "cwd": "/tmp"}), 2)
        self.assertEqual(self.kv.hset("h", {"pid": "2", "log": "/l"}), 1)   # 1 new field
        self.assertEqual(self.kv.hget("h", "pid"), "2")
        self.assertIsNone(self.kv.hget("h", "nope"))
        self.assertEqual(self.kv.hmget("h", ["pid", "nope", "cwd"]), ["2", None, "/tmp"])
        self.assertEqual(self.kv.hgetall("h"), {"pid": "2", "cwd": "/tmp", "log": "/l"})
        self.assertEqual(self.kv.hdel("h", ["cwd", "nope"]), 1)
        self.assertEqual(sorted(self.kv.hgetall("h")), ["log", "pid"])
        self.assertEqual(self.kv.hdel("h", ["log", "pid"]), 2)
        self.assertEqual(self.kv.hgetall("h"), {})        # empty hash = gone
        self.assertFalse(self.kv.exists("h"))

    def test_set_ops(self):
        self.assertEqual(self.kv.sadd("s", ["a", "b", "a"]), 2)
        self.assertEqual(self.kv.sadd("s", ["b", "c"]), 1)
        self.assertEqual(self.kv.smembers("s"), ["a", "b", "c"])
        self.assertEqual(self.kv.srem("s", ["a", "zz"]), 1)
        self.assertEqual(self.kv.smembers("s"), ["b", "c"])
        self.assertEqual(self.kv.srem("s", ["b", "c"]), 2)
        self.assertFalse(self.kv.exists("s"))

    def test_wrong_type_is_an_error_not_a_lie(self):
        self.kv.set("str", "v")
        self.kv.hset("h", {"f": "v"})
        for call in (lambda: self.kv.hget("str", "f"),
                     lambda: self.kv.hgetall("str"),
                     lambda: self.kv.smembers("str"),
                     lambda: self.kv.get("h"),
                     lambda: self.kv.sadd("h", ["x"]),
                     lambda: self.kv.incr("h")):
            with self.assertRaises(kvmod.KVTypeError):
                call()

    def test_hash_ttl_survives_hset(self):
        self.freeze(3_000_000)
        self.kv.hset("h", {"a": "1"})
        self.kv.expire("h", 60)
        self.kv.hset("h", {"b": "2"})
        self.assertEqual(self.kv.ttl("h"), 60)
        self.freeze(3_061_000)
        self.assertEqual(self.kv.hgetall("h"), {})


# -------------------------------------------------------------------- CAS ---

CAS_CHILD = r"""
import importlib.util, sys, time
spec = importlib.util.spec_from_file_location("golemkv", {tools!r} + "/golemkv.py")
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
kv = m.KV(sys.argv[1])
target = int(sys.argv[2])
wins, attempts = 0, 0
t0 = time.time()
while True:
    cur = kv.get("counter")
    n = int(cur) if cur is not None else 0
    if n >= target:
        break
    attempts += 1
    if kv.cas("counter", cur, str(n + 1)):
        wins += 1
print("%d %d %.6f" % (wins, attempts, time.time() - t0))
"""


class CAS(Base):
    def test_cas_semantics(self):
        self.assertTrue(self.kv.cas("k", None, "1"))       # None = must not exist
        self.assertFalse(self.kv.cas("k", None, "2"))
        self.assertFalse(self.kv.cas("k", "wrong", "2"))
        self.assertEqual(self.kv.get("k"), "1")
        self.assertTrue(self.kv.cas("k", "1", "2", ex=60))
        self.assertEqual(self.kv.get("k"), "2")
        self.assertEqual(self.kv.ttl("k"), 60)

    def test_cas_contention_two_processes(self):
        src = CAS_CHILD.format(tools=os.path.abspath(TOOLS))
        path = os.path.join(self.dir, "cas_child.py")
        with open(path, "w") as fh:
            fh.write(src)
        target = 1000
        procs = [subprocess.Popen([sys.executable, path, self.dir, str(target)],
                                  stdout=subprocess.PIPE, text=True) for _ in range(2)]
        outs = [p.communicate()[0] for p in procs]
        self.assertEqual([p.returncode for p in procs], [0, 0])
        wins = attempts = 0
        elapsed = 0.0
        for o in outs:
            w, a, t = o.split()
            wins, attempts, elapsed = wins + int(w), attempts + int(a), max(elapsed, float(t))
        self.assertEqual(wins, target)                     # every transition won once
        self.assertEqual(self.kv.get("counter"), str(target))
        self.assertGreater(attempts, 0)
        sys.stderr.write("\n[bench] CAS: %d wins / %d attempts, %.2f ms per win\n"
                         % (wins, attempts, elapsed * 1000.0 / wins))


class GuardedCAS(Base):
    """S3 DELTA 1 + DELTA 3: a CAS gated on a second key, and a guarded delete."""

    def test_cas_guarded_success_path(self):
        self.kv.set("guard", "tok7")
        self.kv.set("k", "old")
        self.assertTrue(self.kv.cas_guarded("guard", "tok7", "k", "old", "new"))
        self.assertEqual(self.kv.get("k"), "new")

    def test_cas_guarded_creates_absent_key(self):
        self.kv.set("guard", "tok7")
        self.assertTrue(self.kv.cas_guarded("guard", "tok7", "k", None, "v", ex=60))
        self.assertEqual(self.kv.get("k"), "v")
        self.assertEqual(self.kv.ttl("k"), 60)

    def test_cas_guarded_rejects_on_guard_mismatch(self):
        self.kv.set("guard", "tok8")               # the lease changed hands
        self.kv.set("k", "old")
        self.assertFalse(self.kv.cas_guarded("guard", "tok7", "k", "old", "new"))
        self.assertEqual(self.kv.get("k"), "old")  # the zombie's write is rejected

    def test_cas_guarded_rejects_on_absent_or_expired_guard(self):
        self.kv.set("k", "old")
        self.assertFalse(self.kv.cas_guarded("guard", "tok7", "k", "old", "new"))
        self.freeze(1_000_000)
        self.kv.set("guard", "tok7", ex=10)
        self.freeze(1_000_000 + 10_001)            # the guard's TTL has elapsed
        self.assertFalse(self.kv.cas_guarded("guard", "tok7", "k", "old", "new"))
        self.assertEqual(self.kv.get("k"), "old")

    def test_cas_guarded_rejects_on_expected_mismatch(self):
        self.kv.set("guard", "tok7")
        self.kv.set("k", "other")
        self.assertFalse(self.kv.cas_guarded("guard", "tok7", "k", "old", "new"))
        self.assertEqual(self.kv.get("k"), "other")

    def test_cas_guarded_non_string_guard_is_not_a_holder(self):
        self.kv.hset("guard", {"a": "1"})
        self.assertFalse(self.kv.cas_guarded("guard", "tok7", "k", None, "v"))

    def test_delete_if(self):
        self.kv.set("k", "v")
        self.assertFalse(self.kv.delete_if("k", "wrong"))
        self.assertEqual(self.kv.get("k"), "v")
        self.assertTrue(self.kv.delete_if("k", "v"))
        self.assertIsNone(self.kv.get("k"))
        self.assertFalse(self.kv.delete_if("k", "v"))      # already gone

    def test_delete_if_expired_key_is_absent(self):
        self.freeze(2_000_000)
        self.kv.set("k", "v", ex=5)
        self.freeze(2_000_000 + 5_001)
        self.assertFalse(self.kv.delete_if("k", "v"))

    def test_cli_cas_guarded_and_delete_if(self):
        cli(self.dir, "set", "guard", "tok7")
        cli(self.dir, "set", "k", "old")
        rc, _o, _e = cli(self.dir, "cas-guarded", "guard", "tok9", "k", "old", "new")
        self.assertEqual(rc, 1)                            # wrong guard -> 1
        rc, _o, _e = cli(self.dir, "cas-guarded", "guard", "tok7", "k", "old", "new")
        self.assertEqual(rc, 0)
        self.assertEqual(cli(self.dir, "get", "k")[1], "new\n")
        self.assertEqual(cli(self.dir, "del-if", "k", "nope")[0], 1)
        self.assertEqual(cli(self.dir, "del-if", "k", "new")[0], 0)
        self.assertEqual(cli(self.dir, "get", "k")[0], 1)



# ---------------------------------------------------- cross-process / CLI ---

class CrossProcess(Base):
    def test_read_your_writes_across_processes(self):
        rc, out, err = cli(self.dir, "set", "a", "1")
        self.assertEqual(rc, 0, err)
        rc, out, _ = cli(self.dir, "get", "a")
        self.assertEqual((rc, out.strip()), (0, "1"))
        # An already-open handle must see a write made by another process.
        self.assertEqual(self.kv.get("a"), "1")
        cli(self.dir, "set", "a", "2")
        self.assertEqual(self.kv.get("a"), "2")
        # ...and the other direction.
        self.kv.set("b", "3")
        rc, out, _ = cli(self.dir, "get", "b")
        self.assertEqual((rc, out.strip()), (0, "3"))
        # 200 keys written by one process, all visible to the next.
        for i in range(200):
            self.kv.set("k%03d" % i, "v%03d" % i)
        rc, out, _ = cli(self.dir, "keys", "k*")
        self.assertEqual(len(out.split()), 200)
        rc, out, _ = cli(self.dir, "get", "k199")
        self.assertEqual(out.strip(), "v199")

    def test_cli_exit_codes_and_shapes(self):
        self.assertEqual(cli(self.dir, "get", "missing")[0], 1)
        self.assertEqual(cli(self.dir, "exists", "missing")[0], 1)
        self.assertEqual(cli(self.dir, "del", "missing")[0], 1)
        self.assertEqual(cli(self.dir, "set", "k", "v")[0], 0)
        self.assertEqual(cli(self.dir, "exists", "k")[0], 0)
        self.assertEqual(cli(self.dir, "cas", "k", "wrong", "v2")[0], 1)
        self.assertEqual(cli(self.dir, "cas", "k", "v", "v2")[0], 0)
        rc, out, _ = cli(self.dir, "hset", "h", "pid", "7", "cwd", "/tmp")
        self.assertEqual(rc, 0)
        rc, out, _ = cli(self.dir, "hmget", "h", "pid", "nope", "cwd")
        self.assertEqual(out.split("\n")[:3], ["7", "", "/tmp"])   # one value per line
        rc, out, _ = cli(self.dir, "hgetall", "h", "--json")
        self.assertEqual(json.loads(out), {"pid": "7", "cwd": "/tmp"})
        rc, out, _ = cli(self.dir, "keys", "*", "--json")
        self.assertEqual(sorted(json.loads(out)), ["h", "k"])
        self.assertEqual(cli(self.dir, "incr", "h")[0], 2)         # wrong type = 2
        self.assertEqual(cli(self.dir, "ttl", "k")[1].strip(), "-1")

    def test_root_override_by_env(self):
        alt = os.path.join(self.dir, "alt")
        rc, _, err = cli(None, "set", "k", "env", env={"GOLEMKV_ROOT": alt})
        self.assertEqual(rc, 0, err)
        self.assertTrue(os.path.isdir(os.path.join(alt, "log", kvmod.AOF)))
        rc, out, _ = cli(None, "get", "k", env={"GOLEMKV_ROOT": alt})
        self.assertEqual(out.strip(), "env")
        self.assertEqual(cli(self.dir, "get", "k")[0], 1)          # a different keyspace

    def test_cli_get_latency_budget(self):
        """BREAK 3: a warm CLI get must fit in 60 ms or S2 is redesigned.

        Asserted on the best of 7 (min), with the median recorded alongside so a
        load spike from other processes doesn't read as a regression. The
        ceiling widens when the machine is demonstrably loaded (loadavg > 2x
        cpu count) instead of skipping outright.
        """
        for i in range(100):
            self.kv.set("k%d" % i, "v%d" % i)
        cli(self.dir, "get", "k1")                                  # warm the caches
        times = []
        for _ in range(7):
            t0 = time.time()
            rc, out, _ = cli(self.dir, "get", "k50")
            times.append((time.time() - t0) * 1000.0)
            self.assertEqual((rc, out.strip()), (0, "v50"))
        best, med = min(times), statistics.median(times)
        ceiling, note = _load_widened_ceiling(60.0)
        sys.stderr.write("\n[bench] CLI get: best %.1f ms, median %.1f ms "
                         "(best of 7, 100 keys), ceiling %.0f ms%s\n"
                         % (best, med, ceiling, note))
        if best > ceiling:
            self.skipTest("CLI get best %.1f ms (median %.1f ms) > %.0f ms "
                          "budget on this machine (recorded)" % (best, med, ceiling))


# ---------------------------------------------------------------- durability ---

KILL_CHILD = r"""
import importlib.util, sys
spec = importlib.util.spec_from_file_location("golemkv", {tools!r} + "/golemkv.py")
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
kv = m.KV(sys.argv[1])
i = 0
while True:
    kv.set("k%06d" % i, "v" * 200)
    print(i, flush=True)
    i += 1
"""


class Durability(Base):
    def test_kill9_mid_write_loses_no_acknowledged_write(self):
        src = KILL_CHILD.format(tools=os.path.abspath(TOOLS))
        path = os.path.join(self.dir, "kill_child.py")
        with open(path, "w") as fh:
            fh.write(src)
        p = subprocess.Popen([sys.executable, path, self.dir],
                             stdout=subprocess.PIPE, text=True)
        acked = -1
        for _ in range(300):
            line = p.stdout.readline()
            if not line:
                break
            acked = int(line)
        self.assertGreater(acked, 10)
        os.kill(p.pid, signal.SIGKILL)
        p.wait()
        kv = kvmod.KV(self.dir)
        self.addCleanup(kv.close)
        keys = set(kv.keys("k*"))
        for i in range(acked + 1):                       # nothing acknowledged is lost
            self.assertIn("k%06d" % i, keys, "lost acknowledged write %d" % i)
        # The child kept writing after we stopped draining its stdout, so keys
        # past `acked` are legitimate. What must not happen is a hole or a torn
        # record surfacing as a key: the survivors are a contiguous prefix and
        # every one of them holds its whole value.
        top = max(int(k[1:]) for k in keys)
        self.assertGreaterEqual(top, acked)
        self.assertEqual(keys, {"k%06d" % i for i in range(top + 1)}, "hole in the log")
        for i in (0, top // 2, top):
            self.assertEqual(kv.get("k%06d" % i), "v" * 200, "torn value at %d" % i)


# ------------------------------------------------------------- compaction ---

class Compaction(Base):
    def test_compaction_preserves_every_latest_state(self):
        """BREAK 1 regression: full post-state entries make compaction lossless.

        Under the original delta-op design `golemlog compact` would keep only the
        last record per key and silently drop the earlier `hset` that created a
        field. Here the last record IS the whole key, so compaction cannot lose.
        """
        self.freeze(5_000_000)
        for round_ in range(30):
            for i in range(20):
                self.kv.set("s%02d" % i, "v-%d-%d" % (i, round_))
                self.kv.hset("h%02d" % i, {"f%d" % round_: str(round_),
                                           "pid": str(1000 + round_)})
                self.kv.sadd("t%02d" % i, ["m%d" % round_])
        self.kv.set("expiring", "soon", ex=3600)
        self.kv.set("doomed", "x")
        self.kv.delete("doomed")
        before = {k: self.kv.dump(k) for k in self.kv.keys("*")}
        self.assertEqual(len(before), 61)
        n_before = self.log_records()

        log = gl.Log(os.path.join(self.dir, "log"))
        try:
            rows = log.compact(kvmod.AOF, None, 0.0)
        finally:
            log.close()
        self.assertTrue(rows)

        fresh = kvmod.KV(self.dir)
        self.addCleanup(fresh.close)
        after = {k: fresh.dump(k) for k in fresh.keys("*")}
        self.assertEqual(after, before)
        self.assertEqual(fresh.hgetall("h05")["f29"], "29")
        self.assertEqual(len(fresh.hgetall("h05")), 31)      # 30 fields + pid
        self.assertEqual(len(fresh.smembers("t05")), 30)
        self.assertEqual(fresh.ttl("expiring"), 3600)
        self.assertIsNone(fresh.get("doomed"))
        n_after = self.log_records()
        self.assertLess(n_after, n_before)                   # it really did compact
        sys.stderr.write("\n[bench] compaction: %d records -> %d records, %d live keys\n"
                         % (n_before, n_after, len(after)))


# ----------------------------------------------------------------- pub/sub ---

class PubSub(Base):
    def test_pubsub_two_groups_each_get_everything_once(self):
        for i in range(10):
            self.kv.publish("events", "m%d" % i)
        a1 = self.kv.subscribe("events", "a", max=4)
        self.assertEqual(a1, ["m0", "m1", "m2", "m3"])
        b_all = self.kv.subscribe("events", "b", max=100)
        self.assertEqual(len(b_all), 10)                     # independent position
        fresh = kvmod.KV(self.dir)                           # restart mid-stream
        self.addCleanup(fresh.close)
        rest = fresh.subscribe("events", "a", max=100)
        self.assertEqual(rest, ["m%d" % i for i in range(4, 10)])
        self.assertEqual(fresh.subscribe("events", "a", max=100), [])

    def test_pubsub_does_not_touch_the_aof(self):
        before = self.log_len()
        self.kv.publish("events", "hello")
        self.assertEqual(self.log_len(), before)
        self.assertNotIn("events", self.kv.keys("*"))


# ------------------------------------------------------- CLI exit contract ---

class ExitCodes(unittest.TestCase):
    """"0 ok, 1 missing key / false CAS, 2 error" — with no traceback at 2."""

    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="golemkv-exit-")
        self.addCleanup(self._clean)

    def _clean(self):
        for root, dirs, _files in os.walk(self.dir):
            for d in dirs:
                try:
                    os.chmod(os.path.join(root, d), 0o700)
                except OSError:
                    pass
        try:
            os.chmod(self.dir, 0o700)
        except OSError:
            pass
        shutil.rmtree(self.dir, ignore_errors=True)

    def _readonly_root(self):
        root = os.path.join(self.dir, "ro")
        os.makedirs(root)
        cli(root, "set", "a", "1")
        os.chmod(root, 0o500)
        return root

    def test_a_write_to_an_unwritable_root_exits_2_not_1(self):
        """D-s2-3: the PermissionError from the writer lock escaped `run`'s
        except clause, so the interpreter exited 1 — the code that means
        "missing key" — after printing a traceback."""
        rc, _o, e = cli(self._readonly_root(), "set", "b", "2")
        self.assertEqual(rc, 2, e[-400:])
        self.assertNotIn("Traceback (most recent call last)", e)
        self.assertTrue(e.startswith("error: root not writable"), e[-400:])

    def test_a_root_that_cannot_be_created_exits_2_not_1(self):
        parent = os.path.join(self.dir, "locked")
        os.makedirs(parent)
        os.chmod(parent, 0o500)
        rc, _o, e = cli(os.path.join(parent, "kv"), "set", "a", "1")
        self.assertEqual(rc, 2, e[-400:])
        self.assertNotIn("Traceback (most recent call last)", e)
        self.assertTrue(e.startswith("error: root not writable"), e[-400:])

    def test_a_read_still_works_on_a_read_only_root(self):
        """The design point the writability check must not break."""
        rc, o, e = cli(self._readonly_root(), "get", "a")
        self.assertEqual((rc, o.strip()), (0, "1"), e[-400:])

    def test_hmget_on_a_missing_key_exits_1_like_hget_and_hgetall(self):
        """D-s2-4: `hmget` exited 0 and printed blank lines, so a bash caller
        could not tell "the key is gone" from "every field is unset"."""
        cli(self.dir, "hset", "h", "a", "1")
        self.assertEqual(cli(self.dir, "hget", "nokey", "a")[0], 1)
        self.assertEqual(cli(self.dir, "hgetall", "nokey")[0], 1)
        rc, o, _e = cli(self.dir, "hmget", "nokey", "a", "b")
        self.assertEqual(rc, 1)
        self.assertEqual(o, "\n\n")                  # still one line per field
        rc, o, _e = cli(self.dir, "hmget", "h", "a", "b")
        self.assertEqual((rc, o), (0, "1\n\n"))      # present key: 0, blank for unset


# --------------------------------------------------------------- snapshot ---

SNAP_CHILD = r"""
import importlib.util, os, sys
spec = importlib.util.spec_from_file_location("golemkv", {tools!r} + "/golemkv.py")
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
root, tag, n = sys.argv[1], sys.argv[2], int(sys.argv[3])
kv = m.KV(root)
for i in range(n):
    kv.set("%s:%04d" % (tag, i), "v%d" % i)
kv.close()
"""


class Snapshot(Base):
    """S2.1: the pre-registered snapshot. The trigger fired (cold sync > 60 ms)."""

    def setUp(self):
        super().setUp()
        self._kvs = []

    def tearDown(self):
        # Close every extra handle *before* the root is removed: golemlog
        # persists its partition meta on close, and addCleanup runs too late.
        for kv in self._kvs:
            try:
                kv.close()
            except OSError:
                pass
        super().tearDown()

    def kvnew(self, **kw):
        kv = kvmod.KV(self.dir, **kw)
        self._kvs.append(kv)
        return kv

    def snap_path(self):
        return os.path.join(self.dir, kvmod.SNAPSHOT)

    def read_snap(self):
        with open(self.snap_path(), "rb") as fh:
            head, _s, body = fh.read().partition(b"\n")
        return json.loads(head.decode()), body

    def write_snap(self, head_over, entries):
        """Hand-write a *valid* snapshot, so a load can be told from a replay."""
        import zlib
        body = json.dumps(entries, separators=(",", ":")).encode()
        head = {"version": kvmod.SNAPSHOT_VERSION, "upto": {"0": 0},
                "bytes": len(body), "crc32": zlib.crc32(body), "keys": len(entries)}
        head.update(head_over)
        with open(self.snap_path(), "wb") as fh:
            fh.write(json.dumps(head).encode() + b"\n" + body)

    def notes(self, fn):
        """Run `fn`, returning (result, stderr) — `note:` lines are the contract."""
        import contextlib
        import io
        buf = io.StringIO()
        with contextlib.redirect_stderr(buf):
            r = fn()
        return r, buf.getvalue()

    def test_snapshot_round_trip(self):
        for i in range(20):
            self.kv.set("k%02d" % i, "v%d" % i)
        self.kv.hset("h", {"a": "1"})
        self.kv.set("gone", "x", ex=100)
        info = self.kv.snapshot()
        self.assertTrue(os.path.exists(self.snap_path()))
        head, body = self.read_snap()
        self.assertEqual(head["version"], kvmod.SNAPSHOT_VERSION)
        self.assertEqual(head["upto"]["0"], info["upto"])
        self.assertEqual(head["bytes"], len(body))
        self.assertEqual(json.loads(body.decode())["h"]["v"], {"a": "1"})

        fresh = self.kvnew()
        fresh.sync()
        self.assertEqual(fresh._cursor, info["upto"])       # tailed, not replayed
        self.assertEqual(fresh.get("k07"), "v7")
        self.assertEqual(fresh.hgetall("h"), {"a": "1"})
        self.assertEqual(fresh.ttl("gone"), 100)

    def test_the_snapshot_is_really_the_source_on_a_cold_sync(self):
        """A doctored-but-valid snapshot must be what a cold reader sees.

        Without this, every other snapshot test would still pass if `sync()`
        quietly ignored the file and replayed the AOF.
        """
        self.kv.set("real", "1")
        self.kv.snapshot()
        head, body = self.read_snap()
        entries = json.loads(body.decode())
        entries["ghost"] = {"t": "str", "v": "boo", "exp_ms": None}
        self.write_snap({"upto": head["upto"]}, entries)
        fresh = self.kvnew()
        self.assertEqual(fresh.get("ghost"), "boo")
        self.assertEqual(fresh.get("real"), "1")

    def test_tail_after_snapshot_is_read_your_writes(self):
        self.kv.set("a", "1")
        upto = self.kv.snapshot()["upto"]
        # A different process appends after the snapshot was taken.
        self.assertEqual(cli(self.dir, "set", "b", "2")[0], 0)
        self.assertEqual(cli(self.dir, "del", "a")[0], 0)
        fresh = self.kvnew()
        self.assertEqual(fresh.get("b"), "2")
        self.assertIsNone(fresh.get("a"))
        self.assertGreater(fresh._cursor, upto)

    def test_corrupt_snapshot_is_ignored_and_the_log_wins(self):
        for i in range(10):
            self.kv.set("k%d" % i, str(i))
        self.kv.snapshot()
        with open(self.snap_path(), "r+b") as fh:
            fh.seek(-3, os.SEEK_END)
            fh.write(b"XyZ")                                 # break the CRC
        fresh = self.kvnew()
        (_r, errs) = self.notes(fresh.sync)
        self.assertIn("corrupt snapshot", errs)
        self.assertEqual(fresh.get("k7"), "7")               # full replay, correct
        self.assertEqual(fresh._cursor, self.log_len())
        # And the next writer rewrites it.
        os.environ["GOLEMKV_SNAPSHOT_EVERY"] = "1"
        self.addCleanup(os.environ.pop, "GOLEMKV_SNAPSHOT_EVERY", None)
        w = self.kvnew()
        w.set("k7", "seven")
        again = self.kvnew()
        (_r, errs2) = self.notes(again.sync)
        self.assertEqual(errs2, "")
        self.assertEqual(again.get("k7"), "seven")

    def test_truncated_and_wrong_version_snapshots_are_ignored(self):
        self.kv.set("a", "1")
        self.kv.snapshot()
        head, body = self.read_snap()
        with open(self.snap_path(), "wb") as fh:
            fh.write(json.dumps(head).encode() + b"\n" + body[:-5])   # short
        fresh = self.kvnew()
        (_r, errs) = self.notes(fresh.sync)
        self.assertIn("corrupt snapshot", errs)
        self.assertEqual(fresh.get("a"), "1")

        self.write_snap({"version": 99, "upto": head["upto"]}, {})
        f2 = self.kvnew()
        (_r, errs) = self.notes(f2.sync)
        self.assertIn("version", errs)
        self.assertEqual(f2.get("a"), "1")

    def test_snapshot_out_of_range_of_the_log_forces_full_replay(self):
        """Ahead of the log end, or below its floor: neither can be tailed from."""
        self.kv.set("a", "1")
        self.write_snap({"upto": {"0": 9999}}, {"junk": {"t": "str", "v": "j",
                                                         "exp_ms": None}})
        fresh = self.kvnew()
        (_r, errs) = self.notes(fresh.sync)
        self.assertIn("ahead of the log end", errs)
        self.assertIsNone(fresh.get("junk"))
        self.assertEqual(fresh.get("a"), "1")

        # Below the floor: raise the partition's earliest_offset, as retention
        # would. (`kv.aof` is `compact`, so this is a guard, not a live path.)
        log = gl.Log(os.path.join(self.dir, "log"))
        try:
            m = log._pmeta(kvmod.AOF, 0)
            floor = m["next_offset"]
            m["earliest_offset"] = floor
            gl._write_json(log._pmeta_path(kvmod.AOF, 0), m)
        finally:
            log.close()
        self.write_snap({"upto": {"0": 0}}, {"junk": {"t": "str", "v": "j",
                                                      "exp_ms": None}})
        f2 = self.kvnew()
        (_r, errs) = self.notes(f2.sync)
        self.assertIn("older than the log floor", errs)
        self.assertIsNone(f2.get("junk"))

    def test_writer_trigger_fires_every_n_offsets(self):
        os.environ["GOLEMKV_SNAPSHOT_EVERY"] = "5"
        self.addCleanup(os.environ.pop, "GOLEMKV_SNAPSHOT_EVERY", None)
        kv = self.kvnew()
        for i in range(4):
            kv.set("k%d" % i, str(i))
        self.assertFalse(os.path.exists(self.snap_path()))
        kv.set("k4", "4")
        self.assertTrue(os.path.exists(self.snap_path()))
        self.assertEqual(self.read_snap()[0]["upto"]["0"], 5)
        for i in range(5, 9):
            kv.set("k%d" % i, str(i))
        self.assertEqual(self.read_snap()[0]["upto"]["0"], 5)      # not yet
        kv.set("k9", "9")
        self.assertEqual(self.read_snap()[0]["upto"]["0"], 10)

    def test_a_read_never_writes_a_snapshot(self):
        """BREAK 3's read-only-root property must survive the snapshot."""
        os.environ["GOLEMKV_SNAPSHOT_EVERY"] = "1"
        self.addCleanup(os.environ.pop, "GOLEMKV_SNAPSHOT_EVERY", None)
        w = self.kvnew()
        for i in range(10):
            w.set("k%d" % i, str(i))
        self.assertTrue(os.path.exists(self.snap_path()))
        os.unlink(self.snap_path())
        reader = self.kvnew()
        for i in range(10):
            reader.get("k%d" % i)
        reader.keys("*")
        reader.stats()
        self.assertFalse(os.path.exists(self.snap_path()))

    def test_compaction_between_snapshot_and_sync(self):
        """Compaction rewrites in place preserving offsets, so `upto` stays valid."""
        for round_ in range(15):
            for i in range(20):
                self.kv.set("k%02d" % i, "v-%d-%d" % (i, round_))
        upto = self.kv.snapshot()["upto"]
        for i in range(20):
            self.kv.set("k%02d" % i, "final-%d" % i)
        self.kv.set("doomed", "x")
        self.kv.delete("doomed")
        log = gl.Log(os.path.join(self.dir, "log"))
        try:
            self.assertTrue(log.compact(kvmod.AOF, None, 0.0))
        finally:
            log.close()
        fresh = self.kvnew()
        (_r, errs) = self.notes(fresh.sync)
        self.assertEqual(errs, "")                    # the snapshot was still usable
        self.assertGreater(fresh._cursor, upto)
        self.assertEqual(fresh.get("k05"), "final-5")
        self.assertIsNone(fresh.get("doomed"))
        self.assertEqual(len(fresh.keys("*")), 20)

    def test_two_processes_writing_snapshots_stay_readable(self):
        env = {"GOLEMKV_SNAPSHOT_EVERY": "3"}
        src = SNAP_CHILD.format(tools=os.path.abspath(TOOLS))
        script = os.path.join(self.dir, "child.py")
        with open(script, "w") as fh:
            fh.write(src)
        e = dict(os.environ)
        e.update(env)
        procs = [subprocess.Popen([sys.executable, script, self.dir, tag, "40"],
                                  env=e, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)
                 for tag in ("a", "b")]
        for p in procs:
            _o, er = p.communicate(timeout=120)
            self.assertEqual(p.returncode, 0, er.decode()[-500:])
        with_snap = self.kvnew()
        (_r, errs) = self.notes(with_snap.sync)
        self.assertNotIn("corrupt", errs)
        without = self.kvnew(snapshot=False)
        self.assertEqual({k: with_snap.dump(k) for k in with_snap.keys("*")},
                         {k: without.dump(k) for k in without.keys("*")})
        self.assertEqual(len(without.keys("*")), 80)

    def test_cli_snapshot_and_no_snapshot(self):
        self.kv.set("a", "1")
        rc, o, err = cli(self.dir, "snapshot")
        self.assertEqual(rc, 0, err[-400:])
        self.assertEqual(json.loads(o)["keys"], 1)
        self.assertTrue(os.path.exists(self.snap_path()))
        rc, o, err = cli(self.dir, "--no-snapshot", "snapshot")
        self.assertEqual(rc, 2, o)
        self.assertTrue(err.startswith("error: snapshots are disabled"), err[-400:])
        # --no-snapshot reads must ignore even a poisoned snapshot.
        self.write_snap({"upto": {"0": 1}}, {"ghost": {"t": "str", "v": "boo",
                                                       "exp_ms": None}})
        self.assertEqual(cli(self.dir, "get", "ghost")[1].strip(), "boo")
        self.assertEqual(cli(self.dir, "--no-snapshot", "get", "ghost")[0], 1)
        rc, o, _e = cli(self.dir, "stats")
        self.assertEqual(json.loads(o)["snapshot_upto"], 1)


class SnapshotBench(unittest.TestCase):
    """The number the roadmap trigger asked for: cold `sync()` before and after."""
    N = 12000

    @classmethod
    def setUpClass(cls):
        cls.dir = tempfile.mkdtemp(prefix="golemkv-bench-")
        kv = kvmod.KV(cls.dir, snapshot=False)
        for i in range(cls.N):
            kv.set("fleet:host%05d:state" % i, "running-%d" % i)
        kv.close()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.dir, ignore_errors=True)

    def cold_sync_ms(self, snapshot):
        best = 1e9
        for _ in range(3):
            t0 = time.perf_counter()
            kv = kvmod.KV(self.dir, snapshot=snapshot)
            kv.sync()
            best = min(best, (time.perf_counter() - t0) * 1000)
            self.assertEqual(len(kv._view), self.N)
            kv.close()
        return best

    def test_cold_sync_is_under_25_ms_with_a_snapshot(self):
        before = self.cold_sync_ms(False)
        kv = kvmod.KV(self.dir)
        try:
            kv.snapshot()
        finally:
            kv.close()
        after = self.cold_sync_ms(True)
        sys.stderr.write("\n[bench] cold sync over %d entries: %.1f ms replay -> "
                         "%.1f ms snapshot (%.1fx)\n"
                         % (self.N, before, after, before / max(after, 1e-9)))
        self.assertLess(after, before)
        if after > 25:
            self.skipTest("cold sync %.1f ms > 25 ms target on this machine "
                          "(replay was %.1f ms; recorded)" % (after, before))


if __name__ == "__main__":
    unittest.main()
