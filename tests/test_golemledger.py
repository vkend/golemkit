"""Acceptance tests for golemledger.py.

Contention and crash tests shell out to real processes: "exactly one effective
posting under four retrying writers" and "a poster that died between the append
and the balance write" are cross-process properties, and an in-process test can
only pretend to have them.

Nothing here touches a real transcript corpus — `ingest-usage` is exercised
against a synthetic sessions root built in a temp dir.
"""
import datetime
import importlib.util
import json
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
TOOLS = os.path.abspath(os.path.join(HERE, ".."))
SPEC = importlib.util.spec_from_file_location(
    "golemledger", os.path.join(TOOLS, "golemledger.py"))
led = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(led)
gkv = led.gkv

CLI = os.path.join(TOOLS, "golemledger.py")


def cli(root, *args):
    env = dict(os.environ)
    for k in ("GOLEMKV_ROOT", "GOLEMLOG_ROOT", "GOLEMLEDGER_ROOT"):
        env.pop(k, None)
    argv = [sys.executable, CLI, "--root", root] + [str(a) for a in args]
    p = subprocess.run(argv, capture_output=True, text=True, env=env)
    return p.returncode, p.stdout, p.stderr


def child(root, body, *args):
    """Run `body` (a snippet) in a fresh interpreter with `L` bound to a Ledger."""
    src = ("import importlib.util,json,os,sys,time\n"
           "s=importlib.util.spec_from_file_location('gl',%r)\n"
           "m=importlib.util.module_from_spec(s); s.loader.exec_module(m)\n"
           "L=m.Ledger(%r)\n" % (CLI, root)) + body
    env = dict(os.environ)
    for k in ("GOLEMKV_ROOT", "GOLEMLOG_ROOT", "GOLEMLEDGER_ROOT"):
        env.pop(k, None)
    return subprocess.Popen([sys.executable, "-c", src] + [str(a) for a in args],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True, env=env)


def pair(account, unit, n, contra="equity:test"):
    return [[account, unit, n], [contra, unit, -n]]


class Base(unittest.TestCase):
    partitions = 1

    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="golemledger-test-")
        self.L = led.Ledger(self.dir, partitions=self.partitions)

    def tearDown(self):
        self.L.close()
        shutil.rmtree(self.dir, ignore_errors=True)

    def replayed(self):
        bal, _c, _s, _d, _b, _o = self.L._replay()
        return bal

    def records(self):
        return [r for _p, _o, r in self.L._iter()]


# --------------------------------------------------------------- postings ---
class Postings(Base):
    def test_unbalanced_is_refused_and_nothing_is_appended(self):
        before = len(self.records())
        with self.assertRaises(led.Unbalanced):
            self.L.post([["a", "tok", 5], ["b", "tok", -4]], key="k1")
        self.assertEqual(len(self.records()), before)
        self.assertFalse(self.L.kv.exists(led.IDEM + "k1"))

    def test_balanced_in_one_unit_but_not_another_is_refused(self):
        with self.assertRaises(led.Unbalanced) as cm:
            self.L.post([["a", "tok", 5], ["b", "tok", -5],
                         ["a", "usd_micro", 7], ["b", "usd_micro", -6]], key="k2")
        self.assertIn("usd_micro", str(cm.exception))
        self.assertEqual(self.records(), [])

    def test_a_float_amount_is_refused_before_the_balance_check(self):
        with self.assertRaises(led.LedgerError):
            self.L.post([["a", "tok", 5.0], ["b", "tok", -5.0]], key="k3")
        self.assertEqual(self.records(), [])

    def test_a_thousand_duplicate_keys_make_one_effective_posting(self):
        first = self.L.post(pair("a", "tok", 7), key="dup", advance=False)
        for _ in range(999):
            again = self.L.post(pair("a", "tok", 7), key="dup", advance=False)
            self.assertEqual((again["partition"], again["offset"]),
                             (first["partition"], first["offset"]))
        self.assertEqual(len(self.records()), 1)
        self.L.advance()
        self.assertEqual(self.L.balance("a"), 7)
        self.assertEqual(self.replayed()[("a", "tok")], 7)

    def test_a_live_claim_raises_Concurrent_rather_than_waiting(self):
        self.L.kv.cas(led.IDEM + "busy", None,
                      json.dumps({"state": "claim", "ts_ms": led._now_ms()},
                                 sort_keys=True))
        with self.assertRaises(led.Concurrent) as cm:
            self.L.post(pair("a", "tok", 1), key="busy")
        self.assertEqual(cm.exception.key, "busy")
        self.assertEqual(self.records(), [])

    def test_a_stale_claim_is_stolen_and_the_crashed_append_is_adopted(self):
        # A poster that appended and then died before filling its claim.
        rec = {"key": "orphan", "ts_ms": led._now_ms(),
               "entries": pair("a", "tok", 3), "meta": {}}
        self.L._ensure()
        self.L.log.produce(led.TOPIC, key="orphan", value=json.dumps(rec), partition=0)
        self.L.kv.set(led.IDEM + "orphan",
                      json.dumps({"state": "claim",
                                  "ts_ms": led._now_ms() - led.CLAIM_TTL_MS - 1000},
                                 sort_keys=True))
        got = self.L.post(pair("a", "tok", 3), key="orphan")
        self.assertEqual(len(self.records()), 1)          # adopted, not re-appended
        self.assertEqual(got["offset"], 0)
        self.assertEqual(json.loads(self.L.kv.get(led.IDEM + "orphan"))["state"], "done")
        self.assertEqual(self.L.balance("a"), 3)

    def test_statement_is_ordered_by_partition_then_offset(self):
        for i in range(6):
            self.L.post(pair("acct", "tok", i + 1), key="s%d" % i, advance=False)
        rows = self.L.statement("acct")
        self.assertEqual([(r["partition"], r["offset"]) for r in rows],
                         sorted((r["partition"], r["offset"]) for r in rows))
        self.assertEqual([r["key"] for r in rows], ["s%d" % i for i in range(6)])
        self.assertEqual(len(self.L.statement("acct", limit=2)), 2)
        self.assertEqual(self.L.statement("nobody"), [])


# ---------------------------------------------------------------- budgets ---
class Budgets(Base):
    def test_an_unopened_budget_is_unfunded_and_headroom_refuses(self):
        self.assertFalse(self.L.funded("tok"))
        self.assertEqual(self.L.headroom(led.BUDGET, "tok"),
                         {"state": "unfunded", "account": led.BUDGET, "unit": "tok"})
        rc, o, _e = cli(self.dir, "balance", led.BUDGET)
        self.assertEqual((rc, o.strip()), (1, "unfunded"))
        rc, o, _e = cli(self.dir, "headroom", led.BUDGET)
        self.assertEqual((rc, o.strip()), (1, "unfunded"))

    def test_open_budget_funds_it_and_crossing_zero_reports_over(self):
        self.L.open_budget("tok", 100)
        h = self.L.headroom(led.BUDGET, "tok")
        self.assertEqual((h["state"], h["balance"], h["headroom"]), ("ok", 100, 100))
        self.L.post([[led.BUDGET, "tok", -150], ["equity:usage", "tok", 150]],
                    key="spend")
        h = self.L.headroom(led.BUDGET, "tok")
        self.assertEqual((h["state"], h["balance"], h["headroom"]), ("over", -50, 0))
        rc, o, _e = cli(self.dir, "headroom", led.BUDGET)
        self.assertEqual((rc, o.split("\t")[0]), (0, "over"))

    def test_open_budget_is_per_unit(self):
        self.L.open_budget("tok", 10)
        self.assertTrue(self.L.funded("tok"))
        self.assertFalse(self.L.funded("usd_micro"))
        self.assertEqual(self.L.headroom(led.BUDGET, "usd_micro")["state"], "unfunded")

    # D2: the key carried `_now_ms()`, so two identical calls were two openings.
    def test_opening_the_same_budget_twice_does_not_double_the_cap(self):
        first = self.L.open_budget("tok", 1_000_000)
        again = self.L.open_budget("tok", 1_000_000)
        self.assertEqual((again["partition"], again["offset"]),
                         (first["partition"], first["offset"]))
        self.assertEqual(len(self.records()), 1)
        self.assertEqual(self.L.balance(led.BUDGET, "tok"), 1_000_000)
        self.assertEqual(self.replayed()[(led.BUDGET, "tok")], 1_000_000)

    def test_reopening_a_budget_at_a_different_amount_is_refused(self):
        self.L.open_budget("tok", 100)
        with self.assertRaises(led.LedgerError) as cm:
            self.L.open_budget("tok", 250)
        msg = str(cm.exception)
        self.assertIn("already opened at 100", msg)
        self.assertIn(led.BUDGET, msg)
        self.assertEqual(len(self.records()), 1)
        self.assertEqual(self.L.balance(led.BUDGET, "tok"), 100)
        rc, _o, e = cli(self.dir, "open-budget", "tok", "250")
        self.assertEqual(rc, 1)
        self.assertIn("already opened at 100", e)
        self.assertEqual(cli(self.dir, "open-budget", "tok", "100")[0], 0)

    # D3: `funded()` dropped its account argument and tested only the unit.
    def test_opening_another_account_does_not_fund_budget_fleet(self):
        self.L.open_budget("tok", 500, account="budget:other")
        self.assertTrue(self.L.funded("tok", "budget:other"))
        self.assertFalse(self.L.funded("tok", led.BUDGET))
        self.assertEqual(self.L.headroom(led.BUDGET, "tok")["state"], "unfunded")
        self.assertEqual(self.L.headroom("budget:other", "tok")["state"], "ok")
        rc, o, _e = cli(self.dir, "headroom", led.BUDGET)
        self.assertEqual((rc, o.strip()), (1, "unfunded"))
        rc, o, _e = cli(self.dir, "balance", led.BUDGET)
        self.assertEqual((rc, o.strip()), (1, "unfunded"))
        self.assertEqual(cli(self.dir, "headroom", "budget:other")[0], 0)

    def test_a_rebuilt_cache_still_knows_which_account_was_opened(self):
        self.L.open_budget("tok", 500, account="budget:other")
        for k in self.L.kv.keys(led.OPENED + "*"):
            self.L.kv.delete(k)
        self.L.rebuild()
        self.assertTrue(self.L.funded("tok", "budget:other"))
        self.assertFalse(self.L.funded("tok", led.BUDGET))

    def test_audit_catches_a_funded_flag_with_no_opening_posting(self):
        self.assertTrue(self.L.audit()["ok"])
        self.L.kv.set(led.OPENED + led.BUDGET + ":tok", "1")   # invented cap
        a = self.L.audit()
        self.assertFalse(a["ok"])
        self.assertEqual(a["opened_differences"],
                         [{"account": led.BUDGET, "unit": "tok",
                           "stored": True, "replay": False}])
        self.L.rebuild()
        self.assertTrue(self.L.audit()["ok"])
        self.assertEqual(self.L.headroom(led.BUDGET, "tok")["state"], "unfunded")


# -------------------------------------------------------- cache and audit ---
class Cache(Base):
    def test_a_crash_between_append_and_balance_write_leaves_a_stale_cache(self):
        for i in range(5):
            self.L.post(pair("a", "tok", 10), key="c%d" % i, advance=False)
        self.assertEqual(self.L.balance("a"), 0)          # never advanced: stale
        r = self.L.rebuild(verify=True)
        self.assertEqual(len(r["differences"]), 2)        # a and equity:test
        self.assertFalse(r["ok"])
        r = self.L.rebuild()
        self.assertTrue(r["ok"])
        self.assertEqual(self.L.balance("a"), 50)
        self.assertEqual(self.L.balance("a"), self.replayed()[("a", "tok")])
        self.assertTrue(self.L.rebuild(verify=True)["ok"])

    def test_the_cursor_is_a_vector_and_resumes_across_two_partitions(self):
        self.L.close()
        self.L = led.Ledger(self.dir, partitions=2)
        self.L._ensure()
        names = {}
        for i in range(200):
            n = "acct%d" % i
            p = self.L.log.partition_for(led.TOPIC, n, 2)
            names.setdefault(p, n)
            if len(names) == 2:
                break
        self.assertEqual(sorted(names), [0, 1], "need an account in each partition")
        self.L.post(pair(names[0], "tok", 5), key="p0a")
        cur = self.L._read_cursor()
        self.assertEqual(set(cur), {0, 1})
        self.L.post(pair(names[1], "tok", 7), key="p1a", advance=False)
        self.L.post(pair(names[0], "tok", 5), key="p0b", advance=False)
        self.L.advance()                                  # resumes both partitions
        self.assertEqual(self.L.balance(names[0]), 10)
        self.assertEqual(self.L.balance(names[1]), 7)
        self.assertEqual(self.L._read_cursor(), self.L._end_cursor())
        self.assertTrue(self.L.rebuild(verify=True)["ok"])

    # D1: the end cursor was read *after* the scan, so a posting appended in that
    # window was skipped by the fold and passed by the cursor — lost until rebuild.
    def test_a_posting_appended_during_a_fold_is_not_lost_from_the_cache(self):
        self.L.post(pair("x", "tok", 5), key="first")
        real_iter = self.L._iter

        def racy(start=None):
            for t in real_iter(start):
                yield t
            p = child(self.dir, "L.post([['x','tok',7],['equity:test','tok',-7]],"
                                "key='racer', advance=False)\n")
            self.assertEqual(p.wait(timeout=60), 0, p.stderr.read())

        self.L._iter = racy
        self.L.advance()                       # folds nothing; must not pass the racer
        self.L._iter = real_iter
        self.assertLessEqual(self.L._read_cursor()[0], 1)   # the cursor did not skip
        self.L.advance()                       # the next fold picks it up
        self.assertEqual(self.L.balance("x"), 12)
        self.assertEqual(self.replayed()[("x", "tok")], 12)
        self.assertEqual(self.L._read_cursor(), self.L._end_cursor())
        self.assertTrue(self.L.audit()["ok"])

    def test_every_cached_balance_matches_the_replay_under_a_racing_writer(self):
        self.L.post(pair("x", "tok", 1), key="seed")
        real_iter = self.L._iter
        procs = []

        def racy(start=None):
            for t in real_iter(start):
                yield t
            if len(procs) < 3:
                i = len(procs)
                p = child(self.dir, "import sys\n"
                                    "L.post([['x','tok',%d],['equity:test','tok',-%d]],"
                                    "key='r%d', advance=False)\n" % (i + 2, i + 2, i))
                procs.append(p)
                self.assertEqual(p.wait(timeout=60), 0, p.stderr.read())

        self.L._iter = racy
        for _ in range(4):
            self.L.advance()
        self.L._iter = real_iter
        self.L.advance()
        bal = self.replayed()
        self.assertEqual(bal[("x", "tok")], 1 + 2 + 3 + 4)
        for (account, unit), want in bal.items():
            self.assertEqual(self.L.balance(account, unit), want, account)
        self.assertTrue(self.L.rebuild(verify=True)["ok"])

    def test_audit_catches_an_injected_corrupt_balance_and_names_the_account(self):
        self.L.post(pair("a", "tok", 42), key="a1")
        self.assertTrue(self.L.audit()["ok"])
        self.L.kv.set(led.BAL + "a:tok", "999")           # hand-written corruption
        a = self.L.audit()
        self.assertFalse(a["ok"])
        self.assertEqual([d["account"] for d in a["balance_differences"]], ["a"])
        self.assertEqual(a["balance_differences"][0], {"account": "a", "unit": "tok",
                                                       "stored": 999, "replay": 42})
        self.L.rebuild()
        self.assertTrue(self.L.audit()["ok"])

    def test_audit_counts_a_benign_duplicate_and_the_fold_ignores_it(self):
        rec = self.L.post(pair("a", "tok", 4), key="d1")
        raw = json.dumps({"key": "d1", "ts_ms": rec["ts_ms"],
                          "entries": rec["entries"], "meta": {}}, sort_keys=True)
        self.L.log.produce(led.TOPIC, key="d1", value=raw, partition=0)
        self.L.advance()
        a = self.L.audit()
        self.assertEqual(a["benign_dup"], 1)
        self.assertEqual(a["postings"], 1)
        self.assertTrue(a["ok"], a)                        # inert, not a corruption
        self.assertEqual(self.L.balance("a"), 4)
        self.assertEqual(len(self.L.statement("a")), 1)

    def test_audit_reports_an_idempotency_key_pointing_at_nothing(self):
        self.L.post(pair("a", "tok", 1), key="ok1")
        self.L.kv.set(led.IDEM + "ghost",
                      json.dumps({"state": "done", "partition": 0, "offset": 999}))
        a = self.L.audit()
        self.assertFalse(a["ok"])
        self.assertEqual([o["key"] for o in a["idem_orphans"]], ["ghost"])


# -------------------------------------------------------------- contention ---
class Contention(Base):
    def test_four_processes_retrying_one_key_make_one_effective_posting(self):
        body = (
            "ok=0\n"
            "for _ in range(60):\n"
            "    try:\n"
            "        r=L.post([['a','tok',9],['equity:test','tok',-9]],key='hot',advance=False)\n"
            "        ok=1; print(json.dumps([r['partition'],r['offset']])); break\n"
            "    except m.Concurrent:\n"
            "        time.sleep(0.02)\n"
            "if not ok: print('CONCURRENT')\n")
        procs = [child(self.dir, body) for _ in range(4)]
        outs = [p.communicate(timeout=120) for p in procs]
        for p in procs:
            self.assertEqual(p.returncode, 0, outs)
        results = [o.strip() for o, _e in outs]
        self.assertNotIn("", results, outs)
        offs = {r for r in results if r != "CONCURRENT"}
        self.assertEqual(len(offs), 1, results)            # all four saw one posting
        recs = [r for r in self.records() if r["key"] == "hot"]
        self.assertEqual(len(recs), 1, "one effective posting in the journal")
        self.L.advance()
        self.assertEqual(self.L.balance("a"), 9)
        self.assertTrue(self.L.rebuild(verify=True)["ok"])

    def test_four_writers_contending_leave_balances_equal_to_a_replay(self):
        body = ("i=sys.argv[1]\n"
                "for j in range(15):\n"
                "    L.post([['a','tok',1],['equity:test','tok',-1]],key='w%s-%d'%(i,j))\n"
                "print('done')\n")
        procs = [child(self.dir, body, i) for i in range(4)]
        outs = [p.communicate(timeout=180) for p in procs]
        for p, o in zip(procs, outs):
            self.assertEqual(p.returncode, 0, o)
        self.assertEqual(len(self.records()), 60)
        self.L.advance()                                   # whoever lost the lease
        self.assertEqual(self.L.balance("a"), 60)
        self.assertEqual(self.L.balance("a"), self.replayed()[("a", "tok")])
        self.assertTrue(self.L.rebuild(verify=True)["ok"], self.L.rebuild(verify=True))


# ------------------------------------------------------------ usage ingest ---
def synth_row(mid, model, cls, when=None):
    ts = (when or datetime.datetime.now(datetime.timezone.utc)).isoformat()
    return json.dumps({
        "type": "assistant", "uuid": mid, "timestamp": ts,
        "message": {"id": mid, "model": model, "usage": {
            "input_tokens": cls["input"],
            "cache_creation": {
                "ephemeral_1h_input_tokens": cls["cache_create_1h"],
                "ephemeral_5m_input_tokens": cls["cache_create_5m"]},
            "cache_creation_input_tokens": cls["cache_create_1h"] + cls["cache_create_5m"],
            "cache_read_input_tokens": cls["cache_read"],
            "output_tokens": cls["output"]}}})


CLS = {"input": 1000, "cache_create_1h": 100, "cache_create_5m": 200,
       "cache_read": 5000, "output": 300}


class Ingest(Base):
    def setUp(self):
        super().setUp()
        self.projects = os.path.join(self.dir, "projects")
        proj = os.path.join(self.projects, "Demo")
        os.makedirs(proj)
        with open(os.path.join(proj, "sess-A.jsonl"), "w") as fh:
            fh.write(synth_row("msg_1", "claude-opus-5", CLS) + "\n")
            fh.write(synth_row("msg_1", "claude-opus-5", CLS) + "\n")   # continuation
            fh.write(synth_row("msg_2", "claude-haiku-4-5-20251001", CLS) + "\n")

    def test_ingest_is_idempotent_and_dedupes_continuation_rows(self):
        first = self.L.ingest_usage(days=7, projects_root=self.projects)
        self.assertEqual(first["rows"], 3)
        self.assertEqual((first["posted"], first["sessions"]), (2, 1))
        again = self.L.ingest_usage(days=7, projects_root=self.projects)
        self.assertEqual((again["posted"], again["skipped"]), (0, 3))
        self.assertEqual(len(self.records()), 2)
        self.assertTrue(self.L.rebuild(verify=True)["ok"])

    # D4: a call priced at zero was counted in `calls` but in neither `posted`
    # nor `skipped`, so `ingest-usage`'s own report did not add up.
    def test_a_zero_priced_call_is_counted_so_the_stats_add_up(self):
        zero = {"input": 10, "cache_create_1h": 0, "cache_create_5m": 0,
                "cache_read": 0, "output": 5}
        with open(os.path.join(self.projects, "Demo",
                               "sess-B.jsonl"), "w") as fh:
            fh.write(synth_row("msg_3", "<synthetic>", zero) + "\n")
        st = self.L.ingest_usage(days=7, projects_root=self.projects)
        self.assertEqual(st["zero_priced"], 1)
        self.assertEqual(st["unknown_model"], 0)          # `<synthetic>` is priced 0/0
        self.assertEqual((st["rows"], st["calls"], st["posted"], st["skipped"]),
                         (4, 3, 2, 1))
        self.assertEqual(st["calls"], st["posted"] + st["zero_priced"])
        self.assertEqual(st["rows"], st["calls"] + st["skipped"] + st["no_id"])
        self.assertEqual(len(self.records()), 2)          # the free call posts nothing

    def test_an_unpriced_model_is_counted_but_still_posts_at_the_default(self):
        with open(os.path.join(self.projects, "Demo",
                               "sess-C.jsonl"), "w") as fh:
            fh.write(synth_row("msg_4", "claude-not-in-the-table", CLS) + "\n")
        st = self.L.ingest_usage(days=7, projects_root=self.projects)
        self.assertEqual(st["unknown_model"], 1)
        self.assertEqual(st["zero_priced"], 0)            # unknown is never free
        self.assertEqual(st["posted"], 3)
        self.assertEqual(st["calls"], st["posted"] + st["zero_priced"])
        self.assertGreater(self.L.balance("model:claude-not-in-the-table"), 0)

    def test_the_ingest_stats_account_for_every_row_on_a_re_run(self):
        first = self.L.ingest_usage(days=7, projects_root=self.projects)
        self.assertEqual(first["calls"], first["posted"] + first["zero_priced"])
        self.assertEqual(first["rows"], first["calls"] + first["skipped"]
                         + first["no_id"])
        again = self.L.ingest_usage(days=7, projects_root=self.projects)
        self.assertEqual((again["calls"], again["posted"], again["skipped"]),
                         (0, 0, 3))
        self.assertEqual(again["calls"], again["posted"] + again["zero_priced"])
        self.assertEqual(again["rows"], again["calls"] + again["skipped"]
                         + again["no_id"])

    def test_the_five_classes_are_all_read_and_none_is_double_counted(self):
        s = self.L.ingest_usage(days=7, projects_root=self.projects)
        self.assertEqual(s["classes"], {k: v * 2 for k, v in CLS.items()})

    def test_weights_are_applied_per_model_so_equal_raw_counts_differ(self):
        self.L.ingest_usage(days=7, projects_root=self.projects)
        opus = self.L.balance("model:claude-opus-5")
        haiku = self.L.balance("model:claude-haiku-4-5-20251001")
        self.assertGreater(opus, 0)
        self.assertGreater(haiku, 0)
        self.assertNotEqual(opus, haiku)
        self.assertAlmostEqual(opus / float(haiku), 15.0, delta=0.01)
        self.assertEqual(self.L.balance(led.BUDGET), -(opus + haiku))
        self.assertEqual(self.L.balance("session:sess-A"), opus + haiku)

    def test_the_ledger_stays_balanced_and_the_budget_is_still_unfunded(self):
        self.L.ingest_usage(days=7, projects_root=self.projects)
        bal = self.replayed()
        for unit in ("tok", "usd_micro"):
            self.assertEqual(sum(v for (a, u), v in bal.items() if u == unit), 0)
        self.assertEqual(self.L.headroom(led.BUDGET, "tok")["state"], "unfunded")

    def test_cache_reads_are_not_weighted_like_input_tokens(self):
        cheap = {"input": 0, "cache_create_1h": 0, "cache_create_5m": 0,
                 "cache_read": 1000, "output": 0}
        rich = dict(cheap, cache_read=0, input=1000)
        a, _ = led.weigh("claude-opus-5", cheap, self.L.prices)
        b, _ = led.weigh("claude-opus-5", rich, self.L.prices)
        self.assertEqual(b, a * 10)                        # the ~10x the old fold lost

    def test_an_unknown_model_is_priced_as_the_default_never_free(self):
        tok, usd = led.weigh("some-model-that-does-not-exist", CLS, self.L.prices)
        self.assertGreater(tok, 0)
        self.assertGreater(usd, 0)


# ------------------------------------------------------- recon / reconcile ---
def series(slope, steps, window, t0, ratios=(1, 10), raw=1000):
    """Samples plus the usage that "caused" them, at a known slope.

    Consecutive calls alternate between a cheap and an expensive model with the
    *same raw class counts*, so the weighted `tok` and the unweighted `control`
    fold diverge exactly the way they do in real data — which is the only thing
    that lets the control fold be a real control rather than the same series
    under another name. `pct` tracks `tok`.
    """
    samples, usage, pct = [], [], 0.0
    for i in range(steps):
        samples.append({"ts_ms": t0 + i * 1000, "pct": pct, "resets_at": window})
        w = ratios[i % len(ratios)]
        usage.append({"ts_ms": t0 + i * 1000 + 500, "tok": raw * w, "control": raw})
        pct += slope * raw * w
    samples.append({"ts_ms": t0 + steps * 1000, "pct": pct, "resets_at": window})
    return samples, usage


class Reconcile(Base):
    def test_a_correct_weighting_gives_a_stable_slope_across_windows(self):
        s1, u1 = series(1e-4, 16, "w1", 1_000_000)
        s2, u2 = series(1e-4, 16, "w2", 2_000_000, raw=2000)
        r = self.L.reconcile(samples=s1 + s2, usage=u1 + u2)
        self.assertEqual(r["verdict"], "supported", r)
        self.assertEqual(r["windows"], 2)
        self.assertAlmostEqual(r["slope"], 1e-4, places=8)
        self.assertLess(r["stability"], led.STABILITY_LIMIT)
        self.assertGreater(r["control_residual"], r["residual"])

    def test_a_misweighted_control_that_fits_as_well_is_a_disproof(self):
        # ratios (1, 1): every call costs the same, so weighting does no work and
        # the deliberately wrong fold fits identically. That is the disproof.
        s1, u1 = series(1e-4, 16, "w1", 1_000_000, ratios=(1, 1))
        s2, u2 = series(1e-4, 16, "w2", 2_000_000, ratios=(1, 1))
        r = self.L.reconcile(samples=s1 + s2, usage=u1 + u2)
        self.assertEqual(r["verdict"], "disproved", r)
        self.assertIn("control fits as well", r["why"])

    def test_an_unstable_slope_across_windows_is_a_disproof(self):
        s1, u1 = series(1e-4, 16, "w1", 1_000_000)
        s2, u2 = series(1e-3, 16, "w2", 2_000_000)         # 10x different slope
        r = self.L.reconcile(samples=s1 + s2, usage=u1 + u2)
        self.assertEqual(r["verdict"], "disproved", r)
        self.assertIn("varies", r["why"])
        self.assertGreater(r["stability"], led.STABILITY_LIMIT)

    def test_pairs_straddling_a_reset_are_dropped_not_read_as_a_fall_to_zero(self):
        s1, u1 = series(1e-4, 16, "w1", 1_000_000)
        s2, u2 = series(1e-4, 16, "w2", 2_000_000)
        # Time-ordered, the last w1 sample is followed by the first w2 sample at
        # pct 0 — a naive walk reads that as a huge negative increment.
        r = self.L.reconcile(samples=s1 + s2, usage=u1 + u2)
        self.assertEqual(r["straddling_dropped"], 1)
        self.assertEqual(r["pairs"], len(s1) - 1 + len(s2) - 1)
        self.assertEqual(r["verdict"], "supported", r)
        self.assertAlmostEqual(r["slope"], 1e-4, places=8)

    def test_too_few_samples_is_not_yet_decidable_and_says_so(self):
        s1, u1 = series(1e-4, 3, "w1", 1_000_000)
        r = self.L.reconcile(samples=s1, usage=u1)
        self.assertEqual(r["verdict"], "not yet decidable")
        self.assertIn("untested", r["model_weighting"])

    def test_ingest_recon_says_so_when_the_capture_is_not_installed(self):
        missing = os.path.join(self.dir, "nope.jsonl")
        self.assertEqual(self.L.ingest_recon(missing)["state"], "absent")
        rc, o, _e = cli(self.dir, "ingest-recon", "--path", missing)
        self.assertEqual(rc, 0)
        self.assertIn("no independent source installed", o)

    def test_ingest_recon_posts_samples_when_the_file_is_there(self):
        p = os.path.join(self.dir, "ratelimits.jsonl")
        with open(p, "w") as fh:
            for i in range(4):
                fh.write(json.dumps({"ts_ms": 1_000_000 + i * 1000, "pct": 1.5 * i,
                                     "resets_at": "w1"}) + "\n")
        self.assertEqual(self.L.ingest_recon(p)["posted"], 4)
        self.assertEqual(self.L.ingest_recon(p)["posted"], 0)     # idempotent
        self.assertEqual(len(self.L.statement(led.RECON_ACCT)), 4)
        samples, _usage = self.L._recon_series()
        self.assertEqual([s["pct"] for s in samples], [0.0, 1.5, 3.0, 4.5])


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


# --------------------------------------------------------------- CLI/budget ---
class CommandLine(Base):
    def test_the_cli_mirrors_the_api_and_uses_the_documented_exit_codes(self):
        rc, o, e = cli(self.dir, "post", "--key", "k",
                       "--entry", "a:tok:5", "--entry", "equity:test:tok:-5")
        self.assertEqual(rc, 0, e)
        self.assertEqual(json.loads(o)["key"], "k")
        self.assertEqual(cli(self.dir, "balance", "a")[1].strip(), "5")
        rc, _o, e = cli(self.dir, "post", "--key", "bad", "--entry", "a:tok:5")
        self.assertEqual(rc, 2)
        self.assertIn("sum to zero", e)
        self.assertEqual(cli(self.dir, "audit")[0], 0)
        self.assertEqual(cli(self.dir, "rebuild", "--verify")[0], 0)
        self.assertEqual(cli(self.dir, "statement", "a")[0], 0)
        self.assertEqual(cli(self.dir, "statement", "nobody")[0], 1)
        self.assertEqual(cli(self.dir, "reconcile")[0], 0)

    def test_balance_stays_under_the_150ms_ceiling_warm(self):
        """60 ms is the target; 150 ms is the ceiling and a failure.

        This used to `skipTest` above 60 ms, which meant the one number the test
        exists to defend could regress without ever turning the suite red. The
        best of 7 runs is asserted against the ceiling (median is only recorded)
        so a load spike from other processes on the box doesn't flip this red —
        S2.1's snapshot is what is expected to bring it back under the 60 ms
        target. The ceiling itself widens when the machine is demonstrably
        loaded (loadavg > 2x cpu count) rather than skipping outright.
        """
        cli(self.dir, "post", "--key", "b0",
            "--entry", "a:tok:1", "--entry", "equity:test:tok:-1")
        times = []
        for _ in range(7):
            t0 = time.perf_counter()
            rc, _o, e = cli(self.dir, "balance", "a")
            times.append((time.perf_counter() - t0) * 1000.0)
            self.assertEqual(rc, 0, e)
        best, med = min(times), statistics.median(times)
        ceiling, note = _load_widened_ceiling(150.0)
        sys.stderr.write("\n[bench] golemledger CLI `balance` warm: best %.1f ms, "
                         "median %.1f ms (min %.1f, max %.1f), target 60 ms, "
                         "ceiling %.0f ms%s\n"
                         % (best, med, min(times), max(times), ceiling, note))
        self.assertLessEqual(best, ceiling,
                             "best-of-7 %.1f ms (median %.1f ms) is over the "
                             "%.0f ms ceiling (target 60 ms)" % (best, med, ceiling))


# ------------------------------------------------------------ count tally ---
class CountTally(Base):
    """S5: `post_count` — a monotonic tally that is still double-entry.

    The census case: a Bonferroni denominator is a *count of comparisons*, and
    the failure it has to survive is a silent +1 nobody can name. `post_count`
    gives every increment an idempotency key that is the (hypothesis, label)
    pair, so the denominator is not a number but a list of named units that
    happens to sum.
    """

    def test_a_re_post_of_the_same_pair_is_one_effective_posting(self):
        a = self.L.post_count("census.series", "series", 3, key="H-001:L126")
        b = self.L.post_count("census.series", "series", 3, key="H-001:L126")
        self.assertEqual((a["partition"], a["offset"]),
                         (b["partition"], b["offset"]))
        self.assertEqual(len(self.records()), 1)
        self.assertEqual(self.L.balance("census.series", "series"), 3)

    def test_the_contra_mirror_keeps_the_posting_balanced(self):
        self.L.post_count("census.series", "series", 4, key="H-002:B")
        rec = self.records()[0]
        self.assertEqual(rec["entries"],
                         [["census.series", "series", 4],
                          ["contra:census.series", "series", -4]])
        self.assertEqual(self.L.balance("contra:census.series", "series"), -4)
        self.assertTrue(self.L.audit()["ok"])

    def test_balance_is_the_sum_of_every_named_increment(self):
        rows = [("H-001:a", 3), ("H-002:b", 4), ("H-003:c", 5)]
        for k, n in rows:
            self.L.post_count("census.series", "series", n, key=k)
        self.assertEqual(self.L.balance("census.series", "series"),
                         sum(n for _k, n in rows))

    def test_a_correction_is_a_new_posting_and_never_an_edit(self):
        self.L.post_count("census.series", "series", 5, key="H-004:d")
        before = len(self.records())
        self.L.post_count("census.series", "series", -2, key="H-004:d/correction")
        self.assertEqual(len(self.records()), before + 1)
        self.assertEqual(self.L.balance("census.series", "series"), 3)
        keys = [r["key"] for r in self.records()]
        self.assertEqual(keys, ["H-004:d", "H-004:d/correction"])
        # the original posting is byte-for-byte what it was
        self.assertEqual(self.records()[0]["entries"],
                         [["census.series", "series", 5],
                          ["contra:census.series", "series", -5]])

    def test_zero_is_refused_because_a_tally_of_nothing_names_nothing(self):
        with self.assertRaises(led.LedgerError):
            self.L.post_count("census.series", "series", 0, key="H-005:e")

    def test_statement_per_unit_names_every_key_behind_the_denominator(self):
        self.L.post_count("census.series", "series", 3, key="H-001:a",
                          meta={"note": "H-001 three variants"})
        self.L.post_count("census.series", "series", 4, key="H-002:b")
        # an idempotency key is global, not per-unit: a second unit needs its
        # own key. Reusing the key under another unit raises KeyReused.
        self.L.post_count("census.series", "runs", 9, key="runs/H-001:a")
        rows = self.L.statement("census.series", unit="series")
        self.assertEqual([(r["key"], r["amount"]) for r in rows],
                         [("H-001:a", 3), ("H-002:b", 4)])
        self.assertEqual(rows[0]["meta"], {"note": "H-001 three variants"})
        self.assertIsInstance(rows[0]["ts_ms"], int)
        self.assertEqual(sum(r["amount"] for r in rows),
                         self.L.balance("census.series", "series"))
        other = self.L.statement("census.series", unit="runs")
        self.assertEqual([r["key"] for r in other], ["runs/H-001:a"])

    def test_statement_without_a_unit_is_unchanged(self):
        self.L.post_count("census.series", "series", 3, key="H-001:a")
        rows = self.L.statement("census.series")
        self.assertIn("entries", rows[0])
        self.assertNotIn("amount", rows[0])

    def test_audit_counts_contra_accounts_as_system_accounts(self):
        self.L.post_count("census.series", "series", 3, key="H-001:a")
        r = self.L.audit()
        self.assertTrue(r["ok"])
        self.assertEqual(r["system_accounts"], 1)
        self.assertEqual(r["accounts"], 1)

    def test_the_cli_posts_a_count_and_states_it(self):
        rc, o, e = cli(self.dir, "post-count", "census.series", "3",
                       "--unit", "series", "--key", "H-001:a",
                       "--meta", '{"note": "three"}')
        self.assertEqual(rc, 0, e)
        rc, o, e = cli(self.dir, "post-count", "census.series", "3",
                       "--unit", "series", "--key", "H-001:a")
        self.assertEqual(rc, 0, e)
        rc, o, e = cli(self.dir, "balance", "census.series", "--unit", "series")
        self.assertEqual((rc, o.strip()), (0, "3"), e)
        rc, o, e = cli(self.dir, "statement", "census.series",
                       "--unit", "series", "--json")
        self.assertEqual(rc, 0, e)
        rows = json.loads(o)
        self.assertEqual([(r["key"], r["amount"]) for r in rows], [("H-001:a", 3)])
        self.assertEqual(rows[0]["meta"], {"note": "three"})


class KeyReuse(Base):
    """An idempotency key is **global**, not scoped to (account, unit).

    Found by adversarial verification of the census-ledger consumer:
    `post_count` used to accept a key already spent on a different request,
    return the *original* posting, and credit 0. A tally that grew a second unit
    therefore recorded nothing while reporting success. Reuse under a different
    account, unit or amount is now `KeyReused`; an identical request is still
    idempotent, which is the contract that was worth keeping.
    """

    def test_the_same_key_in_a_second_unit_is_refused_not_silently_discarded(self):
        self.L.post_count("census.series", "series", 1, key="H-024:alpha")
        with self.assertRaises(led.KeyReused):
            self.L.post_count("census.series", "variant", 1, key="H-024:alpha")
        self.assertEqual(self.L.balance("census.series", "variant"), 0)
        self.assertEqual(len(self.records()), 1)

    def test_the_same_key_on_a_second_account_is_refused(self):
        self.L.post_count("census.series", "series", 1, key="H-024:alpha")
        with self.assertRaises(led.KeyReused):
            self.L.post_count("other.tally", "series", 1, key="H-024:alpha")
        self.assertEqual(self.L.balance("other.tally", "series"), 0)

    def test_the_same_key_with_a_different_count_is_refused(self):
        self.L.post_count("census.series", "series", 1, key="H-024:alpha")
        with self.assertRaises(led.KeyReused):
            self.L.post_count("census.series", "series", 3, key="H-024:alpha")
        self.assertEqual(self.L.balance("census.series", "series"), 1)

    def test_a_prefix_per_unit_is_the_documented_way_through(self):
        self.L.post_count("census.series", "series", 1, key="series/H-024:alpha")
        self.L.post_count("census.series", "variant", 1, key="variant/H-024:alpha")
        self.assertEqual(self.L.balance("census.series", "series"), 1)
        self.assertEqual(self.L.balance("census.series", "variant"), 1)

    def test_an_identical_request_is_still_idempotent(self):
        a = self.L.post_count("census.series", "series", 2, key="H-024:beta")
        b = self.L.post_count("census.series", "series", 2, key="H-024:beta")
        self.assertEqual((a["partition"], a["offset"]),
                         (b["partition"], b["offset"]))
        self.assertEqual(len(self.records()), 1)

    def test_KeyReused_is_a_LedgerError_and_names_the_key(self):
        self.L.post_count("census.series", "series", 1, key="H-024:gamma")
        with self.assertRaises(led.LedgerError) as ctx:
            self.L.post_count("census.series", "runs", 1, key="H-024:gamma")
        self.assertIsInstance(ctx.exception, led.KeyReused)
        self.assertEqual(ctx.exception.key, "H-024:gamma")
        self.assertIn("H-024:gamma", str(ctx.exception))

    def test_post_refuses_a_reused_key_with_different_entries(self):
        self.L.post(pair("a", "tok", 5), key="k")
        with self.assertRaises(led.KeyReused):
            self.L.post(pair("b", "tok", 5), key="k")
        self.assertEqual(self.L.balance("b"), 0)

    def test_a_stale_claim_stolen_by_another_poster_also_refuses(self):
        """The steal path returns a duplicate too, and must check it as well."""
        rec = self.L.post_count("census.series", "series", 1, key="H-024:delta")
        self.L.kv.set(led.IDEM + "H-024:delta",
                      json.dumps({"state": "claim",
                                  "ts_ms": led._now_ms() - led.CLAIM_TTL_MS - 1000},
                                 sort_keys=True))
        with self.assertRaises(led.KeyReused):
            self.L.post_count("census.series", "runs", 1, key="H-024:delta")
        again = self.L.post_count("census.series", "series", 1, key="H-024:delta")
        self.assertEqual((again["partition"], again["offset"]),
                         (rec["partition"], rec["offset"]))

    def test_the_cli_refuses_a_reused_key_with_one_line_and_exit_1(self):
        rc, o, e = cli(self.dir, "post-count", "census.series", "1",
                       "--unit", "series", "--key", "H-024:eps")
        self.assertEqual(rc, 0, e)
        rc, o, e = cli(self.dir, "post-count", "census.series", "1",
                       "--unit", "variant", "--key", "H-024:eps")
        self.assertEqual(rc, 1, (o, e))
        self.assertIn("H-024:eps", e)
        self.assertNotIn("Traceback", e)
        self.assertEqual(len(e.strip().splitlines()), 1, e)
        rc, o, e = cli(self.dir, "balance", "census.series", "--unit", "variant")
        self.assertEqual((rc, o.strip()), (0, "0"), e)


if __name__ == "__main__":
    unittest.main()
