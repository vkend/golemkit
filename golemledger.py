#!/usr/bin/env python3
"""golemledger — postings-as-truth with idempotency keys, over golemlog/golemkv/golemsched.

What is borrowed from a payments core is three properties and nothing else:

1. **Postings are the truth.** A posting is a *balanced set of entries* summing
   to zero **per unit**. Balances are a fold over the journal and are never the
   truth — they are a cache, and `rebuild` can always reproduce them. An
   unbalanced state is unrepresentable because `post` refuses to append one.
2. **Idempotency keys.** A retried `post` with the same key returns the original
   posting and appends nothing.
3. **The journal is a durable ordered stream** on a `cleanup: none` golemlog
   topic — retention never runs on it, so the ledger cannot forget.

Not borrowed: money movement, cards, a network API, PCI surface, payouts,
refunds, FX, webhooks, signatures.

**Honest framing, first rather than last.** Both sides of every
posting here are written by one process from one input, so the balanced-postings
invariant is this loop checking its own arithmetic. `rebuild --verify` passing is
the thing passing its own test, and is not evidence. S4's deliverable is the
**idempotency + guarded-single-writer primitive** and a **corrected usage
instrument**; the balance surface is expected to go unread and the roadmap says
so in advance.

Four design points that are load-bearing:

* **A budget is opened, never assumed.** `budget:fleet` starts *unfunded*: with
  no `open-budget` posting `balance` reports `unfunded` and `headroom` refuses
  rather than inventing a cap. Once opened, a balance that has crossed zero is
  `over`. `unfunded` / `ok` / `over` is the whole state surface.
* **A live claim is an error, not a wait.** `post` claims `ledger:idem:<key>` by
  CAS-from-absent, appends, then fills the claim with the record's
  `(partition, offset)`. A `post` arriving on a claim younger than
  `CLAIM_TTL_MS` raises `Concurrent` — a second writer of one key is a caller
  bug, and blocking on it is how a ledger acquires a phantom. An older claim is
  a crashed poster and is stealable; the stealer **scans the journal for the key
  first**, so the ordinary crash path appends no duplicate at all. Any duplicate
  that still slips through is **inert**: the fold keeps the **first occurrence
  by (offset, partition)** and `audit` counts the rest as `benign-dup`.
* **The cache cursor is a vector.** `ledger:upto` is `{partition: next_offset}`,
  so a resume cannot skip a partition the way a scalar cursor can. The fold is
  integer addition and therefore order-independent; ordering matters only where
  a human reads it, so `statement` sorts by (partition, offset).
* **Balances have one writer.** They are written only by the holder of the
  `ledger` golemsched lease, through `guarded_set` — S3's one honest guarantee
  (a holder that lost the lease has its golemkv writes rejected, not merely
  logged).

The usage instrument is a correction, not a restatement:
the naive fold omits `output_tokens` entirely and counts cache
reads at full weight, which the bill discounts about tenfold. Here the five real
classes are weighted per model from `golemledger_prices.json`, which is **marked
an estimate**. Nothing previously quoted from `usage_total` is re-quoted.
"""
import argparse
import collections
import importlib.util
import json
import math
import os
import socket
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))


def _load(name, filename):
    spec = importlib.util.spec_from_file_location(name, os.path.join(HERE, filename))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


if __package__ in (None, ""):
    gl = _load("golemlog", "golemlog.py")
    gkv = _load("golemkv", "golemkv.py")
    gsched = _load("golemsched", "golemsched.py")
else:                                                    # pragma: no cover
    from . import golemlog as gl, golemkv as gkv, golemsched as gsched

DEFAULT_ROOT = ".golemledger"
TOPIC = "ledger.postings"
IDEM = "ledger:idem:"
BAL = "ledger:bal:"
UPTO = "ledger:upto"
OPENED = "ledger:opened:"
LEASE = "ledger"
BUDGET = "budget:fleet"
CONTRA = "contra:"           # prefix of the mirror leg a `post_count` tally writes
EQ_OPENING = "equity:opening"
EQ_USAGE = "equity:usage"
EQ_RECON = "equity:recon"
RECON_ACCT = "recon:five_hour"
RECON_UNIT = "pct_milli"
RECON_PATH = os.environ.get("GOLEM_RECON_PATH", "~/.golem/ratelimits.jsonl")

CLAIM_TTL_MS = 60_000          # a claim older than this is a crashed poster
CLAIM_TRIES = 8
LEASE_TTL_S = 300.0            # S3 DELTA 4's floor: short leases are clock-step bait
LEASE_TRIES = 40               # ~2 s of contention before giving up and staying stale
LEASE_SLEEP = 0.05
READ_CHUNK = 500
PRICES_FILE = os.path.join(HERE, "golemledger_prices.json")
CLASSES = ("input", "cache_create_1h", "cache_create_5m", "cache_read", "output")
MIN_RECON_PAIRS = 20           # below this the verdict is "not yet decidable"
STABILITY_LIMIT = 2.0          # slope varying more than this across windows disproves


class LedgerError(Exception):
    pass


class Unbalanced(LedgerError):
    """Entries do not sum to zero in some unit. Nothing was appended."""


class Reopened(LedgerError):
    """A budget already opened at a different amount. Nothing was appended."""


class KeyReused(LedgerError):
    """An idempotency key already names a posting for a *different* request.

    Idempotency keys are **global**, not scoped to (account, unit): the key is
    the whole identity of a posting. Reusing one under another account, unit or
    amount used to return the original posting and move nothing, so a tally that
    grew a second unit silently recorded zero. Prefix the key per unit instead
    (`series/H-024:alpha`, `variant/H-024:alpha`).
    """

    def __init__(self, key, stored=None, requested=None):
        super().__init__(
            "idempotency key %r already names a different posting: stored %r, "
            "requested %r — keys are global, prefix per unit"
            % (key, stored, requested))
        self.key, self.stored, self.requested = key, stored, requested


class Concurrent(LedgerError):
    """Another poster holds a live claim on this idempotency key."""

    def __init__(self, key):
        super().__init__("a live claim already exists for key %r" % key)
        self.key = key


def _now_ms():
    return gkv._now_ms()


def _holder():
    return "%s@%d" % (socket.gethostname(), os.getpid())


# --------------------------------------------------------------- usage ------
def usage_classes(usage):
    """The five real billing classes of one `message.usage` block.

    Verified against a real transcript corpus at build time: every sampled usage
    block carries all five. `cache_creation_input_tokens` is the *sum* of the two
    ephemeral sub-keys, so it is used only as a fallback for a row that predates
    the split — counting both would double the cache writes.
    """
    if not isinstance(usage, dict):
        return {c: 0 for c in CLASSES}
    cc = usage.get("cache_creation")
    if isinstance(cc, dict) and (cc.get("ephemeral_1h_input_tokens") is not None
                                 or cc.get("ephemeral_5m_input_tokens") is not None):
        h1 = int(cc.get("ephemeral_1h_input_tokens") or 0)
        m5 = int(cc.get("ephemeral_5m_input_tokens") or 0)
    else:
        h1, m5 = 0, int(usage.get("cache_creation_input_tokens") or 0)
    return {"input": int(usage.get("input_tokens") or 0),
            "cache_create_1h": h1,
            "cache_create_5m": m5,
            "cache_read": int(usage.get("cache_read_input_tokens") or 0),
            "output": int(usage.get("output_tokens") or 0)}


def load_prices(path=PRICES_FILE):
    with open(path) as fh:
        return json.load(fh)


def weigh(model, classes, prices):
    """(tok, usd_micro) for one call's class counts under `model`'s prices.

    `tok` is a **billable-equivalent** token normalised to the table's single
    reference price, so the same raw counts on two models give different `tok`.
    That is the whole point of the unit: an unweighted token count is dominated
    by cache reads the bill discounts, which is the error this replaces.
    """
    models = prices["models"]
    m = models.get(model) or models["_default"]
    inp = float(m["input_usd_micro_per_token"])
    outp = float(m["output_usd_micro_per_token"])
    mult = prices["class_multipliers"]
    per = {"input": inp * float(mult["input"]),
           "cache_create_1h": inp * float(mult["cache_create_1h"]),
           "cache_create_5m": inp * float(mult["cache_create_5m"]),
           "cache_read": inp * float(mult["cache_read"]),
           "output": outp}
    usd = sum(int(classes.get(c) or 0) * per[c] for c in CLASSES)
    ref = float(prices["reference_usd_micro_per_token"]) or 1.0
    return int(round(usd / ref)), int(round(usd))


def control_tok(classes):
    """The *wrong* fold, kept deliberately: the naive token total as it was —
    input + cache create + cache read at equal weight, output omitted. `reconcile`
    fits this alongside the weighted fold; if it fits as well, the weighting is
    not doing any work and the roadmap's pre-registered disproof has fired."""
    return (int(classes.get("input") or 0)
            + int(classes.get("cache_create_1h") or 0)
            + int(classes.get("cache_create_5m") or 0)
            + int(classes.get("cache_read") or 0))


# ------------------------------------------------------------- ledger -------
class Ledger:
    """A journal plus a fold over it. Cheap to construct, cheap to drop.

    Root layout: `<root>/journal` (golemlog, `cleanup: none`) and `<root>/kv`
    (golemkv, which golemsched shares). Keeping both under one root is what lets
    a test be a temp dir and the real run be `.golemledger/`; point `kv_root` at
    the fleet keyspace to share it instead.
    """

    def __init__(self, root=None, partitions=1, kv_root=None, log_root=None,
                 prices=None):
        self.root = os.path.abspath(
            root or os.environ.get("GOLEMLEDGER_ROOT") or DEFAULT_ROOT)
        os.makedirs(self.root, exist_ok=True)
        self.sched = gsched.Sched(kv_root or os.path.join(self.root, "kv"))
        self.kv = self.sched.kv
        self.log = gl.Log(log_root or os.path.join(self.root, "journal"))
        self.partitions = int(partitions)
        self.prices = prices if prices is not None else load_prices()
        self._created = False
        self._lock_path = os.path.join(self.root, "ledger.lock")

    def close(self):
        self.log.close()
        self.sched.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ------------------------------------------------------- plumbing ---
    def _ensure(self):
        """Create the journal topic idempotently and race-free.

        `golemlog.create` is not atomic against a concurrent create of the same
        new topic (two `meta.json.tmp` writers, one `os.replace` winner), which
        two fresh processes on an empty root hit every time. Serialise it.
        """
        if self._created:
            return
        try:
            m = self.log.meta(TOPIC)
        except gl.GolemLogError:
            with gl._Lock(self._lock_path):
                try:
                    m = self.log.meta(TOPIC)
                except gl.GolemLogError:
                    m = self.log.create(TOPIC, partitions=self.partitions,
                                        cleanup="none")
        self.partitions = m["partitions"]
        self._created = True

    def _iter(self, start=None):
        """Yield (partition, offset, record) from `start` (a {p: offset} vector)."""
        self._ensure()
        for p in range(self.partitions):
            off = int((start or {}).get(p, (start or {}).get(str(p), 0)))
            while True:
                recs = self.log.read(TOPIC, p, off, READ_CHUNK)
                if not recs:
                    break
                for r in recs:
                    try:
                        yield p, r.offset, json.loads(r.value.decode("utf-8"))
                    except ValueError:
                        continue
                off = recs[-1].offset + 1

    def _end_cursor(self):
        self._ensure()
        return {p: self.log._pmeta(TOPIC, p)["next_offset"]
                for p in range(self.partitions)}

    def _read_cursor(self):
        raw = self.kv.get(UPTO)
        if not raw:
            return {}
        try:
            return {int(k): int(v) for k, v in json.loads(raw).items()}
        except (ValueError, AttributeError):
            return {}

    # -------------------------------------------------------- posting ---
    @staticmethod
    def _validate(entries):
        """Normalise entries and refuse anything that does not sum to zero per unit."""
        if not entries:
            raise LedgerError("a posting needs at least one entry")
        out, sums = [], collections.defaultdict(int)
        for e in entries:
            try:
                account, unit, amount = e
            except (TypeError, ValueError):
                raise LedgerError("entry must be [account, unit, amount]: %r" % (e,))
            account, unit = str(account), str(unit)
            if not account or not unit or "\0" in account or "\0" in unit:
                raise LedgerError("bad account/unit in entry %r" % (e,))
            if isinstance(amount, bool) or not isinstance(amount, int):
                raise LedgerError(
                    "amount must be an int (no floats in the journal): %r" % (e,))
            sums[unit] += amount
            out.append([account, unit, amount])
        bad = {u: s for u, s in sums.items() if s != 0}
        if bad:
            raise Unbalanced("entries do not sum to zero per unit: %s"
                             % ", ".join("%s=%+d" % (u, bad[u]) for u in sorted(bad)))
        return out

    def _posting_at(self, partition, offset):
        recs = self.log.read(TOPIC, int(partition), int(offset), 1)
        if not recs or recs[0].offset != int(offset):
            return None
        rec = json.loads(recs[0].value.decode("utf-8"))
        rec["partition"], rec["offset"] = int(partition), int(offset)
        return rec

    def _find_key(self, key):
        """(partition, offset) of the first record carrying `key`, or None.

        Only the steal path pays for this scan, and only after a crash — which is
        exactly the trade that keeps the ordinary crash path duplicate-free.
        """
        best = None
        for p, off, rec in self._iter():
            if rec.get("key") == key and (best is None or (off, p) < best):
                best = (off, p)
        return None if best is None else (best[1], best[0])

    @staticmethod
    def _same_request(got, entries, key):
        """Refuse a key already spent on a different posting.

        The idempotency contract is "re-posting *the same thing* is inert", and
        it was silently doing more than that: a key reused under another account
        or unit returned the original posting and credited nothing. Entries carry
        account, unit and amount, so comparing them is the whole check.
        """
        if got is not None and got.get("entries") != entries:
            raise KeyReused(key, got.get("entries"), entries)
        return got

    def post(self, entries, key, meta=None, ts_ms=None, advance=True, claim=True,
             sync=True):
        """Append one balanced posting under an idempotency key.

        `claim=False` skips the golemkv claim protocol and is for a **bulk,
        single-process** loader that has already established the key is absent by
        scanning the journal (`ingest_usage` does exactly that). It is not a
        shortcut: it is the acknowledgement that 60 000 idempotency keys whose
        only reader is the loader would make every later `balance` replay them.
        """
        entries = self._validate(entries)
        key = str(key)
        if not key or "\0" in key:
            raise LedgerError("an idempotency key is required")
        self._ensure()
        ts_ms = int(ts_ms if ts_ms is not None else _now_ms())
        ikey = IDEM + key
        mine = None

        if claim:
            for _ in range(CLAIM_TRIES):
                now = _now_ms()
                cand = json.dumps({"state": "claim", "ts_ms": now}, sort_keys=True)
                if self.kv.cas(ikey, None, cand):
                    mine = cand
                    break
                raw = self.kv.get(ikey)
                if raw is None:
                    continue                       # it vanished under us; retry
                try:
                    d = json.loads(raw)
                except ValueError:
                    raise LedgerError("corrupt idempotency entry for %r" % key)
                if d.get("state") == "done":
                    got = self._posting_at(d["partition"], d["offset"])
                    if got is not None:
                        self._same_request(got, entries, key)
                        if advance:
                            self.advance()
                        return got                 # the duplicate: nothing appended
                    raise LedgerError(
                        "idempotency key %r names a posting that is not there" % key)
                if now - int(d.get("ts_ms") or 0) < CLAIM_TTL_MS:
                    raise Concurrent(key)
                # Stale claim: a crashed poster. Steal it, then look before leaping.
                if self.kv.cas(ikey, raw, cand):
                    mine = cand
                    found = self._find_key(key)
                    if found is not None:
                        self.kv.cas(ikey, cand, json.dumps(
                            {"state": "done", "partition": found[0],
                             "offset": found[1]}, sort_keys=True))
                        got = self._posting_at(*found)
                        self._same_request(got, entries, key)
                        if advance:
                            self.advance()
                        return got
                    break
            if mine is None:
                raise Concurrent(key)

        rec = {"key": key, "ts_ms": ts_ms, "entries": entries,
               "meta": meta if meta is not None else {}}
        p = self.log.partition_for(TOPIC, entries[0][0], self.partitions)
        pp, off = self.log.produce(TOPIC, key=key,
                                   value=json.dumps(rec, sort_keys=True),
                                   partition=p, sync=sync)
        if claim:
            self.kv.cas(ikey, mine, json.dumps(
                {"state": "done", "partition": pp, "offset": off}, sort_keys=True))
        rec["partition"], rec["offset"] = pp, off
        if advance:
            self.advance()
        return rec

    def post_count(self, account, unit, n, key, meta=None, **kw):
        """One increment of a monotonic tally, still balanced.

        A tally — "how many comparisons has this repo paid for" — has no natural
        second leg: nothing is transferred, something is *counted*. Inventing a
        counterparty would be a lie, and dropping the second leg would give up
        the one invariant that makes the journal auditable. So the second leg is
        `contra:<account>`, the tally's mirror: it holds the negative of the
        count and means "this many increments were asserted by the source of
        truth" — for the census, by `series_census.py`'s fold over the evidence
        JSONs. `audit` reports `contra:*` separately as system accounts, because
        they are bookkeeping, not somewhere value lives.

        `n` is signed and non-zero. A **correction is a new posting** under a new
        key with a negative `n`, never an edit: re-posting the same key is inert
        (that is the idempotency contract), so the only way the number moves is
        another named line. That is the whole point — the balance is a sum whose
        every term can be named by `statement(account, unit=...)`.

        **Keys are global: prefix per unit.** The key is the whole identity of a
        posting, not a name within (account, unit). Reusing one for a different
        account, unit or `n` raises `KeyReused` rather than returning the
        original and counting nothing — so a tally that grows a second unit
        needs `series/H-024:alpha`, `variant/H-024:alpha`, not one key twice.
        """
        if isinstance(n, bool) or not isinstance(n, int):
            raise LedgerError("a count must be an int: %r" % (n,))
        if n == 0:
            raise LedgerError("a count of 0 names nothing; do not post it")
        account = str(account)
        if account.startswith(CONTRA):
            raise LedgerError("post_count writes the contra leg itself: %r" % account)
        return self.post([[account, unit, n], [CONTRA + account, unit, -n]],
                         key=key, meta=meta, **kw)

    def _opening_for(self, key):
        """The existing opening posting under `key`, or None.

        The idempotency store answers first; a journal scan is the fallback for a
        wiped or rebuilt kv, because "is this budget already open" must be
        answered from the journal — the only thing that is the truth.
        """
        raw = self.kv.get(IDEM + key)
        if raw:
            try:
                d = json.loads(raw)
            except ValueError:
                d = {}
            if d.get("state") == "done":
                got = self._posting_at(d.get("partition"), d.get("offset"))
                if got is not None:
                    return got
        found = self._find_key(key)
        return None if found is None else self._posting_at(*found)

    def open_budget(self, unit, amount, account=BUDGET, key=None, **kw):
        """Fund a budget account. Nothing else creates one: an account with no
        opening posting is `unfunded`, and `headroom` refuses on it.

        The key is `open-budget:<account>:<unit>` and carries no clock, so
        re-running an install or a loop script is idempotent: the same amount
        returns the original posting and appends nothing. A *different* amount is
        refused rather than stacked, because two openings of one budget is the
        one state this surface cannot represent honestly.
        """
        amount = int(amount)
        if amount <= 0:
            raise LedgerError("open-budget needs a positive amount")
        key = key or "open-budget:%s:%s" % (account, unit)
        prior = self._opening_for(key)
        if prior is not None:
            was = sum(int(a) for acct, u, a in prior.get("entries", [])
                      if acct == account and u == unit)
            if was != amount:
                raise Reopened(
                    "%s/%s is already opened at %d (posting %r at %d:%d); "
                    "refusing to reopen it at %d"
                    % (account, unit, was, key, prior.get("partition", -1),
                       prior.get("offset", -1), amount))
            return prior
        return self.post([[account, unit, amount], [EQ_OPENING, unit, -amount]],
                         key=key, meta={"kind": "open-budget"}, **kw)

    @staticmethod
    def _opened_in(rec):
        """The (account, unit) pairs an opening posting funds.

        Funding is per *account*: the opening posting credits one account against
        `equity:opening` in one unit, and only that pair becomes funded.
        """
        entries = rec.get("entries", [])
        units = {u for a, u, _n in entries if a == EQ_OPENING}
        return {(a, u) for a, u, n in entries
                if u in units and a != EQ_OPENING and int(n) > 0}

    # --------------------------------------------------------- balances ---
    def _replay(self):
        """The definition of a balance: fold the whole journal, first-key-wins.

        Returns (balances, cursor, seen, benign_dup, unbalanced). Deduplication
        keeps the **first occurrence by (offset, partition)** — which is why a
        duplicate record is inert rather than a corruption.
        """
        recs = list(self._iter())
        recs.sort(key=lambda t: (t[1], t[0]))
        bal = collections.defaultdict(int)
        seen, dups, bad = {}, 0, []
        opened = set()
        for p, off, rec in recs:
            k = rec.get("key")
            if k in seen:
                dups += 1
                continue
            seen[k] = (p, off)
            sums = collections.defaultdict(int)
            for account, unit, amount in rec.get("entries", []):
                bal[(account, unit)] += int(amount)
                sums[unit] += int(amount)
            opened |= self._opened_in(rec)
            if any(v for v in sums.values()):
                bad.append((p, off, k))
        return dict(bal), self._end_cursor(), seen, dups, bad, opened

    def _with_lease(self, fn):
        """Run `fn(holder, token)` while holding the `ledger` lease, or return None.

        Balances are a single-writer cache; a caller that cannot get the lease
        leaves them stale rather than racing, because a stale cache is repaired
        by the next writer and a raced one is not.
        """
        holder = _holder()
        for i in range(LEASE_TRIES):
            token = self.sched.acquire(LEASE, holder, LEASE_TTL_S)
            if token is not None:
                try:
                    return fn(holder, token)
                finally:
                    self.sched.release(LEASE, holder, token)
            time.sleep(LEASE_SLEEP)
        return None

    def _opened_key(self, account, unit):
        return "%s%s:%s" % (OPENED, account, unit)

    def _stored_opened(self):
        """The (account, unit) pairs the cache currently believes are funded."""
        out = set()
        for k in self.kv.keys(OPENED + "*"):
            if (self.kv.get(k) or "0") != "1":
                continue                       # a flag repaired to "0" is not funded
            account, _, unit = k[len(OPENED):].rpartition(":")
            if account and unit:
                out.add((account, unit))
        return out

    def _write_balances(self, holder, token, deltas, cursor, opened, absolute=False,
                        unopened=()):
        for (account, unit), amount in sorted(deltas.items()):
            k = "%s%s:%s" % (BAL, account, unit)
            cur = 0 if absolute else int(self.kv.get(k) or 0)
            self.sched.guarded_set(LEASE, holder, token, k, str(cur + amount))
        for account, unit in sorted(opened):
            self.sched.guarded_set(LEASE, holder, token,
                                   self._opened_key(account, unit), "1")
        for account, unit in sorted(unopened):   # a funded flag with no opening
            self.sched.guarded_set(LEASE, holder, token,
                                   self._opened_key(account, unit), "0")
        self.sched.guarded_set(LEASE, holder, token, UPTO,
                               json.dumps({str(k): v for k, v in cursor.items()},
                                          sort_keys=True))
        return len(deltas)

    def advance(self):
        """Fold the records the balance cache has not seen into it.

        Deduplication on this path consults the idempotency store: a record whose
        key is already `done` at a *different* offset is a duplicate and is
        skipped, which is the same first-wins rule `_replay` applies. `audit`
        exists to catch the case where the two disagree.

        The end of the window is read **before** the scan, not after it, and the
        fold covers exactly `[start, end)`. Reading it afterwards let a posting
        appended during the scan be missed by the fold *and* passed by the
        cursor, so no later `advance` would ever fold it — a silently wrong
        balance that only `rebuild` could repair.
        """
        self._ensure()

        def work(holder, token):
            start = self._read_cursor()
            end = self._end_cursor()
            deltas = collections.defaultdict(int)
            opened, n = set(), 0
            for p, off, rec in self._iter(start):
                if off >= int(end.get(p, end.get(str(p), 0))):
                    continue                        # appended after the window opened
                k = rec.get("key")
                raw = self.kv.get(IDEM + k) if k else None
                if raw:
                    try:
                        d = json.loads(raw)
                    except ValueError:
                        d = {}
                    if d.get("state") == "done" and \
                            (int(d.get("partition", -1)), int(d.get("offset", -1))) != (p, off):
                        continue                    # benign duplicate: inert
                for account, unit, amount in rec.get("entries", []):
                    deltas[(account, unit)] += int(amount)
                opened |= self._opened_in(rec)
                n += 1
            self._write_balances(holder, token, deltas, end, opened)
            return n
        return self._with_lease(work)

    def balance(self, account, unit="tok"):
        """The cached balance. A cache, not the truth — `rebuild` is the truth."""
        return int(self.kv.get("%s%s:%s" % (BAL, account, unit)) or 0)

    def funded(self, unit="tok", account=BUDGET):
        """Funding is per (account, unit): an opening on one account funds only
        that account. Opening `budget:other` must not make `budget:fleet` look
        capped — `ok, headroom 0` on an unopened account is an invented state."""
        return (self.kv.get(self._opened_key(account, unit)) or "0") == "1"

    def headroom(self, account=BUDGET, unit="tok"):
        """`unfunded` / `ok` / `over` — the whole state surface anything reads."""
        if str(account).startswith("budget:") and not self.funded(unit, account):
            return {"state": "unfunded", "account": account, "unit": unit}
        bal = self.balance(account, unit)
        return {"state": "over" if bal < 0 else "ok", "account": account,
                "unit": unit, "balance": bal, "headroom": max(bal, 0)}

    def statement(self, account, since=None, limit=None, unit=None):
        """Postings touching `account`, ordered by (partition, offset) — the
        journal's own order, not wall clock, and stable across runs.

        With `unit`, the shape changes to the one a *tally* needs: one row per
        posting that moves `account` in that unit, carrying `key`, `amount`,
        `meta` and `ts_ms`. That is what lets a denominator be **named** rather
        than merely large — `sum(r["amount"])` is the balance, and every term
        has the key that produced it, so a +1 nobody can account for shows up as
        a key on one side and not the other.
        """
        recs = sorted(self._iter(), key=lambda t: (t[1], t[0]))   # first-wins order
        out, seen = [], set()
        for p, off, rec in recs:
            if rec.get("key") in seen:
                continue
            seen.add(rec.get("key"))
            if since is not None and int(rec.get("ts_ms") or 0) < int(since):
                continue
            if not any(e[0] == account for e in rec.get("entries", [])):
                continue
            rec["partition"], rec["offset"] = p, off
            out.append(rec)
        out.sort(key=lambda r: (r["partition"], r["offset"]))
        if unit is not None:
            rows = []
            for rec in out:
                amt = sum(e[2] for e in rec.get("entries", [])
                          if e[0] == account and e[1] == unit)
                if not any(e[0] == account and e[1] == unit
                           for e in rec.get("entries", [])):
                    continue
                rows.append({"key": rec.get("key"), "amount": int(amt),
                             "meta": rec.get("meta") or {},
                             "ts_ms": int(rec.get("ts_ms") or 0),
                             "unit": unit, "account": account,
                             "partition": rec["partition"], "offset": rec["offset"]})
            out = rows
        return out[:limit] if limit else out

    def rebuild(self, verify=False):
        """Replay the journal, compare against the cache, and repair it.

        `verify=True` compares without writing. Passing is the ledger checking
        its own arithmetic and is explicitly **not** evidence that the numbers
        mean anything.
        """
        bal, cursor, seen, dups, bad, opened = self._replay()
        stored = {}
        for k in self.kv.keys(BAL + "*"):
            rest = k[len(BAL):]
            account, _, unit = rest.rpartition(":")
            stored[(account, unit)] = int(self.kv.get(k) or 0)
        diffs = []
        for kk in sorted(set(stored) | set(bal)):
            want, got = int(bal.get(kk, 0)), int(stored.get(kk, 0))
            if want != got:
                diffs.append({"account": kk[0], "unit": kk[1],
                              "stored": got, "replay": want})
        stored_opened = self._stored_opened()
        ofix = sorted(opened ^ stored_opened)
        odiffs = [{"account": a, "unit": u, "stored": (a, u) in stored_opened,
                   "replay": (a, u) in opened} for a, u in ofix]
        res = {"postings": len(seen), "benign_dup": dups,
               "unbalanced": [{"partition": p, "offset": o, "key": k} for p, o, k in bad],
               "differences": diffs, "opened_differences": odiffs,
               "repaired": 0, "verify": bool(verify)}
        if verify or not (diffs or odiffs):
            res["ok"] = not diffs and not odiffs and not bad
            return res
        wipe = {kk: 0 for kk in stored}
        wipe.update(bal)
        done = self._with_lease(
            lambda h, t: self._write_balances(h, t, wipe, cursor, opened, absolute=True,
                                              unopened=stored_opened - opened))
        res["repaired"] = (len(diffs) + len(odiffs)) if done is not None else 0
        res["ok"] = done is not None
        return res

    def audit(self):
        """Three invariants, and the count of duplicates that were inert anyway."""
        bal, _cursor, seen, dups, bad, _opened = self._replay()
        r = self.rebuild(verify=True)
        orphans = []
        for k in self.kv.keys(IDEM + "*"):
            key = k[len(IDEM):]
            try:
                d = json.loads(self.kv.get(k) or "{}")
            except ValueError:
                orphans.append({"key": key, "why": "corrupt"})
                continue
            if d.get("state") != "done":
                continue
            got = self._posting_at(d.get("partition"), d.get("offset"))
            if got is None:
                orphans.append({"key": key, "why": "missing posting"})
            elif got.get("key") != key:
                orphans.append({"key": key, "why": "points at key %r" % got.get("key")})
        # `contra:*` is bookkeeping — the mirror leg `post_count` writes so a
        # tally can be double-entry without inventing a counterparty. Counting
        # it as an account would double every tally in the account count.
        accts = {a for a, _u in bal}
        system = {a for a in accts if a.startswith(CONTRA)}
        out = {"postings": len(seen), "benign_dup": dups,
               "unbalanced": r["unbalanced"], "balance_differences": r["differences"],
               "opened_differences": r["opened_differences"],
               "idem_orphans": orphans, "accounts": len(accts - system),
               "system_accounts": len(system)}
        out["ok"] = not (bad or r["differences"] or r["opened_differences"] or orphans)
        return out

    # ---------------------------------------------------------- ingest ---
    def _known_keys(self):
        return {rec.get("key") for _p, _o, rec in self._iter()}

    def ingest_usage(self, days=7, main_only=False, projects_root=None, out=None):
        """Post one balanced posting per distinct model *call* in the window.

        Dedupe is by `message.id` (FL-048: one call is written as several
        assistant rows, so counting rows overcounts calls by ~45 %). The key is
        `usage:<message.id>`, so re-running over the same window posts nothing.

        Four entries per unit: `session:<sid>` +n against `budget:fleet` −n, and
        `model:<m>` +n against `equity:usage` −n. The model dimension therefore
        exists without unbalancing the posting.

        The stats account for every row, so the report can be checked against
        itself: `calls == posted + zero_priced` and
        `rows == calls + skipped + no_id`. `zero_priced` exists because a call
        priced at 0 (`<synthetic>`, 48 rows in the real 7-day window) is neither
        posted nor skipped. `unknown_model` is a diagnostic, not a term in either
        sum: an unpriced model is weighed as `_default` and still posts.
        """
        es = _load_session_scan()
        root = projects_root or es.ROOT
        self._ensure()
        seen = self._known_keys()
        stats = {"rows": 0, "calls": 0, "posted": 0, "skipped": 0,
                 "zero_priced": 0, "unknown_model": 0, "no_id": 0,
                 "sessions": set(), "models": collections.Counter(),
                 "classes": collections.Counter(), "tok": 0, "usd_micro": 0}
        for repo, path, rows in es.iter_sessions(root=root, days=days,
                                                 main_only=main_only):
            sid = os.path.basename(path)[:-6] if path.endswith(".jsonl") \
                else os.path.basename(path)
            for r in rows:
                if r.get("type") != "assistant":
                    continue
                msg = r.get("message") or {}
                u = msg.get("usage")
                if not isinstance(u, dict):
                    continue
                stats["rows"] += 1
                mid = msg.get("id") or r.get("uuid")
                if not mid:
                    stats["no_id"] += 1
                    continue
                key = "usage:%s" % mid
                if key in seen:
                    stats["skipped"] += 1
                    continue
                seen.add(key)
                stats["calls"] += 1
                cls = usage_classes(u)
                model = msg.get("model") or "_default"
                if model not in self.prices["models"]:
                    stats["unknown_model"] += 1     # priced as `_default`, never free
                tok, usd = weigh(model, cls, self.prices)
                if not tok and not usd:
                    stats["zero_priced"] += 1       # counted, so the report adds up
                    continue
                t = es.row_time(r)
                entries = []
                for unit, n in (("tok", tok), ("usd_micro", usd)):
                    if not n:
                        continue
                    entries += [["session:%s" % sid, unit, n],
                                [BUDGET, unit, -n],
                                ["model:%s" % model, unit, n],
                                [EQ_USAGE, unit, -n]]
                if not entries:                     # unreachable given the check above
                    stats["zero_priced"] += 1
                    continue
                self.post(entries, key=key,
                          meta={"model": model, "repo": repo, "session": sid,
                                "raw": cls, "control_tok": control_tok(cls)},
                          ts_ms=int(t * 1000) if t else None,
                          advance=False, claim=False, sync=False)
                stats["posted"] += 1
                stats["sessions"].add(sid)
                stats["models"][model] += tok
                for c in CLASSES:
                    stats["classes"][c] += cls[c]
                stats["tok"] += tok
                stats["usd_micro"] += usd
                if out and stats["posted"] % 5000 == 0:
                    out("  … %d posted" % stats["posted"])
        self.advance()
        stats["sessions"] = len(stats["sessions"])
        stats["models"] = dict(stats["models"])
        stats["classes"] = dict(stats["classes"])
        return stats

    def ingest_recon(self, path=None):
        """Post the *independent* samples, if the owner-gated capture exists.

        The server tells the statusline `rate_limits.five_hour.used_percentage`
        and nothing persists it. Capturing it is a one-line append to a global
        script, so it is filed as a request rather than installed unasked; with
        no file this prints and exits 0 rather than pretending.
        """
        p = os.path.expanduser(path or RECON_PATH)
        if not os.path.exists(p):
            return {"state": "absent", "path": p, "posted": 0}
        n, seen = 0, self._known_keys()
        with open(p, errors="replace") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                except ValueError:
                    continue
                ts = int(d.get("ts_ms") or (float(d.get("ts") or 0) * 1000))
                pct = d.get("pct", d.get("used_percentage"))
                if not ts or pct is None:
                    continue
                key = "recon:%d" % ts
                if key in seen:
                    continue
                seen.add(key)
                v = int(round(float(pct) * 1000))
                self.post([[RECON_ACCT, RECON_UNIT, v], [EQ_RECON, RECON_UNIT, -v]],
                          key=key, ts_ms=ts,
                          meta={"resets_at": d.get("resets_at"), "pct": pct},
                          advance=False, claim=False, sync=False)
                n += 1
        self.advance()
        return {"state": "ingested", "path": p, "posted": n}

    # ------------------------------------------------------- reconcile ---
    def _recon_series(self):
        samples, usage = [], []
        for _p, _o, rec in self._iter():
            meta = rec.get("meta") or {}
            accounts = {e[0] for e in rec.get("entries", [])}
            if RECON_ACCT in accounts:
                samples.append({"ts_ms": int(rec.get("ts_ms") or 0),
                                "pct": float(meta.get("pct") or 0.0),
                                "resets_at": meta.get("resets_at")})
            elif meta.get("raw"):
                tok = sum(a for acct, unit, a in rec["entries"]
                          if acct == BUDGET and unit == "tok")
                usage.append({"ts_ms": int(rec.get("ts_ms") or 0),
                              "tok": -tok,
                              "control": int(meta.get("control_tok") or 0)})
        return samples, usage

    def reconcile(self, samples=None, usage=None):
        """Regress Δpct on Δtok **through the origin**, inside one server window.

        Comparing a cumulative local fold against a percentage that resets is a
        comparison of two ramps and correlates at r≈0.9 whatever the weights are,
        which is why the levels test the provisional design proposed is struck.
        Increments do not have that defect. Pairs straddling a `resets_at`
        boundary are dropped rather than treated as a fall to zero.

        Pre-registered disproof: the slope varies more than `STABILITY_LIMIT`×
        across windows, **or** the deliberately mis-weighted control fold fits as
        well as the weighted one. Model-weighting is **untested** either way —
        this design cannot separate it, and the verdict says so.
        """
        if samples is None or usage is None:
            s, u = self._recon_series()
            samples = s if samples is None else samples
            usage = u if usage is None else usage
        usage = sorted(usage, key=lambda d: d["ts_ms"])
        smps = sorted(samples, key=lambda d: d["ts_ms"])

        def between(a, b, field):
            return sum(int(d[field]) for d in usage if a < d["ts_ms"] <= b)

        pairs, per_window, dropped = [], collections.defaultdict(list), 0
        for i in range(len(smps) - 1):
            a, b = smps[i], smps[i + 1]
            if a.get("resets_at") != b.get("resets_at"):
                dropped += 1               # a window boundary, not a fall to zero
                continue
            dtok = between(a["ts_ms"], b["ts_ms"], "tok")
            dctl = between(a["ts_ms"], b["ts_ms"], "control")
            dpct = float(b["pct"]) - float(a["pct"])
            if dtok <= 0 and dctl <= 0:
                continue
            per_window[a.get("resets_at")].append((dpct, dtok, dctl))
            pairs.append((dpct, dtok, dctl))

        def fit(ps, i):
            den = sum(float(p[i]) ** 2 for p in ps)
            if den <= 0:
                return None, None
            slope = sum(p[0] * float(p[i]) for p in ps) / den
            resid = math.sqrt(sum((p[0] - slope * float(p[i])) ** 2
                                  for p in ps) / len(ps))
            return slope, resid

        slope, resid = fit(pairs, 1)
        cslope, cresid = fit(pairs, 2)
        wslopes = {str(w): fit(ps, 1)[0] for w, ps in per_window.items()}
        good = [s for s in wslopes.values() if s and s > 0]
        stability = (max(good) / min(good)) if len(good) >= 2 else None
        out = {"pairs": len(pairs), "windows": len(per_window),
               "straddling_dropped": dropped, "slope": slope,
               "residual": resid, "window_slopes": wslopes,
               "stability": stability, "control_slope": cslope,
               "control_residual": cresid,
               "model_weighting": "untested — this design cannot separate it"}
        if len(pairs) < MIN_RECON_PAIRS or len(good) < 2:
            out["verdict"] = "not yet decidable"
            out["why"] = ("%d paired samples in %d windows; need >= %d pairs across "
                          ">= 2 windows" % (len(pairs), len(good), MIN_RECON_PAIRS))
            return out
        reasons = []
        if stability is not None and stability > STABILITY_LIMIT:
            reasons.append("slope varies %.2fx across windows (> %.1fx)"
                           % (stability, STABILITY_LIMIT))
        if resid is not None and cresid is not None and cresid <= resid:
            reasons.append("the mis-weighted control fits as well (%.4g <= %.4g)"
                           % (cresid, resid))
        out["verdict"] = "disproved" if reasons else "supported"
        out["why"] = "; ".join(reasons) if reasons else \
            "slope stable within %.1fx and the mis-weighted control fits worse" \
            % STABILITY_LIMIT
        return out


_ES = None


def _load_session_scan():
    global _ES
    if _ES is None:
        _ES = _load("sessionscan", "sessionscan.py")
    return _ES


# ------------------------------------------------------------------ CLI -----
def _p(out, x):
    out(json.dumps(x, sort_keys=True) if isinstance(x, (dict, list)) else x)


def run(argv=None, out=print, err=None):
    """Exit codes: 0 ok, 1 refused / absent / disproved, 2 error."""
    err = err or (lambda s: sys.stderr.write(s + "\n"))
    ap = argparse.ArgumentParser(prog="golemledger",
                                 description=__doc__.split("\n")[0])
    ap.add_argument("--root", default=None,
                    help="ledger root (default $GOLEMLEDGER_ROOT or .golemledger)")
    ap.add_argument("--partitions", type=int, default=1,
                    help="journal partitions, used only when the topic is created")
    sub = ap.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("post", help="append one balanced posting")
    c.add_argument("--key", required=True)
    c.add_argument("--entry", action="append", default=[], required=True,
                   metavar="ACCOUNT:UNIT:AMOUNT")
    c.add_argument("--meta", default=None, help="JSON object")

    c = sub.add_parser("post-count",
                       help="append one increment of a monotonic tally "
                            "(account + contra:account mirror)")
    c.add_argument("account")
    c.add_argument("n", type=int)
    c.add_argument("--unit", default="tok")
    c.add_argument("--key", required=True)
    c.add_argument("--meta", default=None, help="JSON object")

    c = sub.add_parser("balance")
    c.add_argument("account")
    c.add_argument("--unit", default="tok")

    c = sub.add_parser("headroom")
    c.add_argument("account", nargs="?", default=BUDGET)
    c.add_argument("--unit", default="tok")
    c.add_argument("--json", action="store_true")

    c = sub.add_parser("open-budget")
    c.add_argument("unit")
    c.add_argument("amount", type=int)
    c.add_argument("--account", default=BUDGET)

    c = sub.add_parser("statement")
    c.add_argument("account")
    c.add_argument("--since", type=int, default=None, help="ts_ms floor")
    c.add_argument("--limit", type=int, default=None)
    c.add_argument("--unit", default=None,
                   help="one row per posting in this unit: key, amount, meta, ts")
    c.add_argument("--json", action="store_true")

    c = sub.add_parser("rebuild")
    c.add_argument("--verify", action="store_true")

    sub.add_parser("audit")

    c = sub.add_parser("ingest-usage")
    c.add_argument("--days", type=int, default=7)
    c.add_argument("--main-only", action="store_true")
    c.add_argument("--projects-root", default=None)

    c = sub.add_parser("ingest-recon")
    c.add_argument("--path", default=None)

    sub.add_parser("reconcile")

    a = ap.parse_args(argv)
    try:
        led = Ledger(a.root, partitions=a.partitions)
    except Exception as exc:
        err("error: %s" % exc)
        return 2
    try:
        return _dispatch(a, led, out, err)
    except KeyReused as exc:
        err("refused: %s" % exc)                   # a spent key is not an error
        return 1
    except (LedgerError, gkv.KVError, gl.GolemLogError, OSError, ValueError) as exc:
        err("error: %s" % exc)
        return 2
    finally:
        led.close()


def _entry_arg(s):
    account, unit, amount = s.rsplit(":", 2)
    return [account, unit, int(amount)]


def _dispatch(a, led, out, err):
    if a.cmd == "post":
        meta = json.loads(a.meta) if a.meta else None
        rec = led.post([_entry_arg(e) for e in a.entry], key=a.key, meta=meta)
        _p(out, {"key": rec["key"], "partition": rec["partition"],
                 "offset": rec["offset"]})
        return 0
    if a.cmd == "post-count":
        meta = json.loads(a.meta) if a.meta else None
        rec = led.post_count(a.account, a.unit, a.n, key=a.key, meta=meta)
        _p(out, {"key": rec["key"], "partition": rec["partition"],
                 "offset": rec["offset"]})
        return 0
    if a.cmd == "balance":
        if str(a.account).startswith("budget:") and not led.funded(a.unit, a.account):
            out("unfunded")
            return 1
        out(led.balance(a.account, a.unit))
        return 0
    if a.cmd == "headroom":
        h = led.headroom(a.account, a.unit)
        if a.json:
            _p(out, h)
        else:
            out(h["state"] if h["state"] == "unfunded"
                else "%s\t%d" % (h["state"], h["balance"]))
        return 1 if h["state"] == "unfunded" else 0
    if a.cmd == "open-budget":
        try:
            rec = led.open_budget(a.unit, a.amount, account=a.account)
        except Reopened as exc:
            err("refused: %s" % exc)
            return 1
        _p(out, {"account": a.account, "unit": a.unit, "amount": a.amount,
                 "offset": rec["offset"]})
        return 0
    if a.cmd == "statement":
        rows = led.statement(a.account, since=a.since, limit=a.limit, unit=a.unit)
        if a.json:
            _p(out, rows)
        elif a.unit:
            for r in rows:
                out("%d:%d\t%s\t%+d %s" % (r["partition"], r["offset"],
                                           r["key"], r["amount"], r["unit"]))
        else:
            for r in rows:
                out("%d:%d\t%s\t%s" % (
                    r["partition"], r["offset"], r["key"],
                    " ".join("%s %s %+d" % tuple(e) for e in r["entries"])))
        return 0 if rows else 1
    if a.cmd == "rebuild":
        r = led.rebuild(verify=a.verify)
        _p(out, r)
        return 0 if r.get("ok") else 1
    if a.cmd == "audit":
        r = led.audit()
        _p(out, r)
        return 0 if r["ok"] else 1
    if a.cmd == "ingest-usage":
        t0 = time.time()
        s = led.ingest_usage(days=a.days, main_only=a.main_only,
                             projects_root=a.projects_root, out=err)
        s["elapsed_s"] = round(time.time() - t0, 2)
        _p(out, s)
        return 0
    if a.cmd == "ingest-recon":
        r = led.ingest_recon(a.path)
        if r["state"] == "absent":
            out("no independent source installed (%s)" % r["path"])
            return 0
        _p(out, r)
        return 0
    if a.cmd == "reconcile":
        r = led.reconcile()
        _p(out, r)
        return 1 if r["verdict"] == "disproved" else 0
    return 2


if __name__ == "__main__":
    sys.exit(run())
