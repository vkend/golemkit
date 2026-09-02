#!/usr/bin/env python3
"""golemsched — TTL-bounded leases with fencing tokens over golemkv.

What is borrowed from the k8s ecosystem is `coordination.k8s.io/Lease` and nothing
else: exclusive ownership of a *name*, held by an identity, bounded by a **TTL**
rather than by politeness. The holder renews by heartbeat; the lease is expired
**by time**, not by the holder being asked. A `kill -9`d holder cannot release
itself and must not be able to block. There is no daemon, no controller, no
queue and no job — the roadmap's cut order took the `job` subsystem before the
build, because it had no consumer.

Three things this does *not* claim, each of which the design turns on:

1. **Fencing is real only where the resource checks the token.** A token nobody
   validates is decoration. So the token is checked *by golemkv*: `guarded_set`
   goes through `golemkv.cas_guarded`, which verifies the holder's exact lease
   entry under the same writer lock as the write it guards, and `renew` and
   `release` are plain CASes on that same exact entry. A zombie holder whose
   lease changed hands has its **golemkv writes rejected**. For a file, a port, a GPU or a process, **no fencing is provided**
   — nothing here intercepts an `open()` or a `bind()`.
2. **Split-brain across the TTL is admitted.** Between a holder stalling and its
   TTL elapsing, a second holder acquires while the first still believes it
   holds. golemkv-backed state is protected by (1); everything else is not.
3. **The TTL is wall-clock.** A forward clock step expires a live lease early and
   permits a genuine double-hold (`ClockSteps` in the tests observes exactly
   that and does not assert it away). A backward step extends one. The
   mitigation is a floor rather than a fix: **use `--ttl` ≥ 300 s**, so a
   plausible step is small next to the lease.

**One entry, one atomic grant (D-S3-1).** The token is *not* a separate counter.
The lease entry outlives the lease: `release` CASes it to a free form
(`holder: null`) that carries the last token, and `acquire` is a **single**
`cas` from the exact entry it read to a new entry holding `token + 1`. Token and
lease therefore move together, so a token can only advance when the lease does
and grants are strictly ordered — an acquirer preempted mid-grant loses its CAS
and retries rather than landing a stale token behind a newer holder. A lost race
now spends nothing: tokens are consecutive per name, not merely increasing.

Because the entry must survive expiry to keep the token, it is stored **without
a golemkv TTL** and carries its own `exp_ms`, which golemsched evaluates. Two
consequences, both real:

* `guarded_set` is fenced by golemkv against a *change of holder* but no longer
  against the passage of time — golemkv sees a live entry where golemsched sees
  an expired one. golemsched checks `exp_ms` immediately before the guarded
  write, so the exposure is a TOCTOU window rather than the whole post-expiry
  period, and any new holder closes it by changing the entry.
* An expiry nobody looks at is not an event. The first `who`/`acquire` to see an
  expired entry CASes it to the free form and emits `expire-observed` with the
  dead token — exactly once per expiry, because only the CAS winner emits.

`who` reports nothing *immediately* after a clean release: the entry that
remains names no holder.
"""
import argparse
import json
import math
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import golemkv as gkv                                          # noqa: E402
import golemlog as gl                                          # noqa: E402

LEASE = "sched:lease:"
EVENTS = "sched.events"
MIN_SANE_TTL = 300          # DELTA 4: below this a clock step is not small
GRANT_TRIES = 8             # CAS retries before an acquire reports contention


class SchedError(Exception):
    pass


def _entry(holder, token, acq_ms, ttl, exp_ms):
    """The lease value. `sort_keys` so the string a holder re-reads is the string
    it wrote — the guard comparison is on the exact bytes."""
    return json.dumps({"acq_ms": acq_ms, "exp_ms": exp_ms, "holder": holder,
                       "token": token, "ttl": ttl}, sort_keys=True)


def _free(token):
    """The entry a released or reaped lease leaves behind: no holder, no expiry,
    and the last token issued — which is what makes the next grant `token + 1`."""
    return _entry(None, token, None, None, None)


def _parse_entry(raw):
    """Normalise a stored lease entry so no reader can ever raise on it.

    Every reading path (`who`, `acquire`, `renew`, `release`, `list`) goes
    through this. Three tolerances, each independent:

    - missing/invalid JSON -> a fresh free entry (holder=None, token=0), plus
      one `note:` line on stderr — best-effort, never fatal.
    - missing `token` -> 0.
    - missing `holder` -> None.
    - missing `exp_ms` (an older format) -> `0`, i.e. already-expired,
      *not* "no expiry" — `exp_ms: None` only ever means that when `_free`
      wrote it, and `_free` also nulls `holder`, so an entry with a holder and
      no `exp_ms` key is unambiguously the old format and must be treated as
      expired rather than held forever.
    """
    try:
        parsed = json.loads(raw) if raw else {}
        if not isinstance(parsed, dict):
            raise ValueError("lease entry is not a JSON object")
    except (ValueError, TypeError) as exc:
        sys.stderr.write("note: unreadable lease entry %r (%s); treating as free\n"
                          % (raw, exc))
        parsed = {}
    try:
        token = int(parsed.get("token") or 0)
    except (TypeError, ValueError):
        token = 0
    exp_ms = parsed.get("exp_ms", 0)
    return {
        "acq_ms": parsed.get("acq_ms"),
        "exp_ms": exp_ms,
        "holder": parsed.get("holder"),
        "token": token,
        "ttl": parsed.get("ttl"),
    }


class Sched:
    """Leases over one golemkv keyspace. Cheap to construct, cheap to drop."""

    def __init__(self, root=None):
        self.kv = gkv.KV(root)
        self.root = self.kv.root

    def close(self):
        self.kv.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # --------------------------------------------------------- plumbing ---
    @staticmethod
    def _check(name):
        if not name or "\0" in name or name.startswith("sched:"):
            raise SchedError("bad lease name %r" % name)
        return name

    def _read(self, name):
        """(exact stored string, parsed entry), or (None, None) if there is none.

        No expiry is applied: an expired entry is still *there*, and its token is
        the input to the next grant.
        """
        raw = self.kv.get(LEASE + self._check(name))
        return (None, None) if raw is None else (raw, _parse_entry(raw))

    @staticmethod
    def _held(e):
        """Is this entry a live hold? Expiry is evaluated here, not by golemkv."""
        if e is None or e.get("holder") is None:
            return False
        exp = e.get("exp_ms")
        return exp is None or exp > gkv._now_ms()

    def _reap(self, name, raw, e):
        """CAS an expired entry to its free form and report the expiry. Only the
        CAS winner emits, so `expire-observed` fires once per expiry however many
        readers trip over it. Best-effort: `who` must still work read-only."""
        try:
            if self.kv.cas(LEASE + name, raw, _free(int(e["token"]))):
                self._event("expire-observed", name, holder=e["holder"],
                            token=int(e["token"]), ttl=e.get("ttl"))
        except (gkv.KVError, gl.GolemLogError, OSError):
            pass

    def _mine(self, name, holder, token):
        """(raw, parsed) if this holder still holds with this token, else (None, None)."""
        raw, e = self._read(name)
        if not self._held(e):
            return None, None
        if e["holder"] != str(holder) or e["token"] != int(token):
            return None, None
        return raw, e

    def _event(self, kind, name, **kw):
        """Append to `sched.events`. Best-effort: an unwritable log must not turn a
        successful lease operation into a failure."""
        try:
            self.kv._ensure_topic(EVENTS, "delete")
            rec = {"kind": kind, "name": name, "ts_ms": gkv._now_ms()}
            rec.update(kw)
            self.kv.log.produce(EVENTS, key=name, value=json.dumps(rec, sort_keys=True))
        except (gl.GolemLogError, gkv.KVError, OSError):
            pass

    def note(self, kind, name, **kw):
        """Record a consumer-side fact in the lease's own history.

        A consumer that *bypasses* the lease (an escape-hatch flag, a manual
        override) leaves no trace in the lease table by definition, so the one
        place the bypass can be seen later is the event log it is bypassing.
        Best-effort and never raises, like every other event write.
        """
        self._event(str(kind), self._check(name), **kw)

    def events(self, group="golemsched", max=100, commit=True):
        """Drain `sched.events` as a consumer group. Used by tests and by eyeballs."""
        self.kv._ensure_topic(EVENTS, "delete")
        recs = self.kv.log.consume(EVENTS, group, max)
        if commit and recs:
            self.kv.log.commit(EVENTS, group, 0, recs[-1].offset + 1)
        return [json.loads(r.value.decode("utf-8")) for r in recs]

    # ----------------------------------------------------------- leases ---
    def acquire(self, name, holder, ttl_s):
        """Take the lease, or None if someone live holds it.

        One CAS from the entry as read to the entry as granted, so the token is
        assigned **inside** the grant (D-S3-1): there is no window in which a
        token exists without the lease that earned it. An expired entry and a
        released one are the same code path as a first acquisition — which is
        what makes a `kill -9`d holder harmless. A lost CAS means someone else
        moved the entry, so re-read and try again rather than spending a token.
        """
        self._check(name)
        ttl = float(ttl_s)
        if ttl <= 0:
            raise SchedError("ttl must be positive")
        for _ in range(GRANT_TRIES):
            raw, e = self._read(name)
            if self._held(e):
                return None
            token = 1 if e is None else int(e["token"]) + 1
            acq = gkv._now_ms()
            val = _entry(str(holder), token, acq, ttl,
                         acq + int(round(ttl * 1000)))
            if not self.kv.cas(LEASE + name, raw, val):
                continue                    # the entry moved under us: re-read
            if e is not None and e.get("holder") is not None:
                self._event("expire-observed", name, holder=e["holder"],
                            token=int(e["token"]), ttl=e.get("ttl"))
            self._event("acquire", name, holder=str(holder), token=token, ttl=ttl)
            return token
        return None                         # sustained contention, not a hold

    def renew(self, name, holder, token, ttl_s=None):
        """Extend a lease this holder still holds. A stale token cannot renew."""
        raw, e = self._mine(name, holder, token)
        if raw is None:
            self._event("renew-rejected", name, holder=str(holder), token=int(token))
            return False
        ttl = float(e["ttl"] if ttl_s is None else ttl_s)
        new = _entry(e["holder"], e["token"], e["acq_ms"], ttl,
                     gkv._now_ms() + int(round(ttl * 1000)))
        ok = self.kv.cas(LEASE + name, raw, new)
        self._event("renew" if ok else "renew-rejected", name,
                    holder=str(holder), token=int(token), ttl=ttl)
        return ok

    def release(self, name, holder, token):
        """Give the lease back: CAS to the free form, which names no holder but
        keeps the token so the next grant is strictly higher. `who` is None
        immediately after — the entry that survives is not a hold."""
        raw, e = self._mine(name, holder, token)
        if raw is None:
            self._event("release-rejected", name, holder=str(holder), token=int(token))
            return False
        ok = self.kv.cas(LEASE + name, raw, _free(int(e["token"])))
        self._event("release" if ok else "release-rejected", name,
                    holder=str(holder), token=int(token))
        return ok

    def hold(self, name, holder, ttl_s):
        """Acquire-or-renew for one identity: `(token, "acquired"|"renewed")`, or
        `None` if someone else holds it live.

        The primitive a *multi-step* critical section needs. A holder whose
        section spans several processes (edit, build, commit) cannot carry a
        token in a variable, and re-`acquire` would refuse its own live lease.
        So: read, and if the live holder is this identity, renew on that entry's
        token; otherwise acquire. Both branches are CASes on the exact entry
        read, so a lease that changes hands between the read and the write loses
        the race rather than being stolen — the caller sees `None` and refuses.

        Re-entrancy is by **name**, not by process: two agents that pass the same
        `holder` string share one lease and do not exclude each other. The holder
        string is the identity being serialised; pick it accordingly.
        """
        for _ in range(GRANT_TRIES):
            _raw, e = self._read(name)
            if self._held(e):
                if e["holder"] != str(holder):
                    return self._refused(name, holder, e)
                if self.renew(name, holder, e["token"], ttl_s):
                    return int(e["token"]), "renewed"
                continue                    # the entry moved under us: re-read
            tok = self.acquire(name, holder, ttl_s)
            if tok is not None:
                return tok, "acquired"
            # Acquire refused: someone took it between the read and the CAS.
            _raw, e = self._read(name)
            if self._held(e) and e["holder"] != str(holder):
                return self._refused(name, holder, e)
        return None

    def _refused(self, name, holder, e):
        """Log the refusal and return None.

        A refusal is the *observable* half of a lease: "the build was refused,
        naming its holder" is the falsifier for any consumer, and the consumer's
        own stderr is routinely redirected away. `acquire` deliberately stays
        silent (its None is ordinary CAS contention); `hold`'s None is always a
        live foreign holder, which is a real event.
        """
        self._event("hold-refused", name, holder=str(holder),
                    held_by=e["holder"], token=int(e["token"]))
        return None

    def guarded_set(self, name, holder, token, key, value, ex=None):
        """Write `key` in golemkv **only while still holding** `name`.

        This is S3's whole deliverable to S4/S5: the guard and the write happen
        under one golemkv writer lock, so a holder that lost the lease between
        its last renew and this call has the write **rejected**, not merely
        logged. `key` must not be a `sched:` key — a caller cannot use its lease
        to rewrite the lease table.

        Since D-S3-1 the entry carries its own `exp_ms` and golemkv holds no TTL
        on it, so golemkv fences a *change of holder* and `_mine` fences the
        clock. Time-based rejection is therefore golemsched's, checked here and
        not inside the writer lock (see the module docstring).
        """
        if str(key).startswith("sched:"):
            raise SchedError("guarded_set may not write the sched: keyspace")
        raw, _e = self._mine(name, holder, token)
        if raw is None:
            return False
        return self.kv.cas_guarded(LEASE + name, raw, key, gkv.ANY, value, ex=ex)

    def who(self, name):
        """The live holder, or None. Adds `remaining_s` from the entry's `exp_ms`.

        Seeing an expiry is what reports it: an expired entry is reaped to its
        free form here, which is also how `expire-observed` gets emitted once.
        """
        raw, e = self._read(name)
        if e is None or e.get("holder") is None:
            return None
        if not self._held(e):
            self._reap(name, raw, e)
            return None
        out = dict(e)
        out["remaining_s"] = max(
            0, int(math.ceil((e["exp_ms"] - gkv._now_ms()) / 1000.0)))
        return out

    def list(self):
        """Every live lease, by name."""
        out = {}
        for k in self.kv.keys(LEASE + "*"):
            name = k[len(LEASE):]
            e = self.who(name)
            if e is not None:
                out[name] = e
        return out


# -------------------------------------------------------------------- CLI ---

def run(argv=None, out=print, err=None):
    """Exit codes: 0 ok, 1 not held / lost the race, 2 error."""
    err = err or (lambda s: sys.stderr.write(s + "\n"))
    ap = argparse.ArgumentParser(prog="golemsched",
                                 description=__doc__.split("\n")[0])
    ap.add_argument("--root", default=None,
                    help="keyspace root (default $GOLEMKV_ROOT or .golemkv)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    lease = sub.add_parser("lease", help="TTL-bounded named leases")
    lsub = lease.add_subparsers(dest="verb", required=True)

    for verb in ("acquire", "hold"):
        c = lsub.add_parser(verb)
        c.add_argument("name")
        c.add_argument("--ttl", type=float, required=True)
        c.add_argument("--holder", required=True)
    for verb in ("renew", "release"):
        c = lsub.add_parser(verb)
        c.add_argument("name")
        c.add_argument("--holder", required=True)
        c.add_argument("--token", type=int, required=True)
        if verb == "renew":
            c.add_argument("--ttl", type=float, default=None)
    w = lsub.add_parser("who")
    w.add_argument("name")
    w.add_argument("--json", action="store_true")
    ls = lsub.add_parser("list")
    ls.add_argument("--json", action="store_true")

    a = ap.parse_args(argv)
    try:
        s = Sched(a.root)
    except Exception as exc:                      # CLI layer: never a traceback
        err("error: %s" % exc)
        return 2
    try:
        return _dispatch(a, s, out, err)
    except Exception as exc:                       # CLI layer: never a traceback
        err("error: %s" % exc)
        return 2
    finally:
        s.close()


def _fmt(name, e):
    return "%s\t%s\ttoken=%d\tremaining=%ds" % (
        name, e["holder"], e["token"], e["remaining_s"])


def _dispatch(a, s, out, err):
    v = a.verb
    if v == "hold":
        if a.ttl < MIN_SANE_TTL:
            err("warn: --ttl %g is under the recommended %d s floor; a forward "
                "clock step can double-hold a short lease" % (a.ttl, MIN_SANE_TTL))
        got = s.hold(a.name, a.holder, a.ttl)
        if got is None:
            cur = s.who(a.name)
            if cur is None:
                err("busy: %s was held and is now expired; retry" % a.name)
            else:
                err("busy: %s held by %s (token %d) for another %d s"
                    % (a.name, cur["holder"], cur["token"], cur["remaining_s"]))
            return 1
        tok, how = got
        err("%s %s token %d" % (how, a.name, tok))
        out(tok)
        return 0
    if v == "acquire":
        if a.ttl < MIN_SANE_TTL:
            err("warn: --ttl %g is under the recommended %d s floor; a forward "
                "clock step can double-hold a short lease" % (a.ttl, MIN_SANE_TTL))
        tok = s.acquire(a.name, a.holder, a.ttl)
        if tok is None:
            cur = s.who(a.name)
            if cur is None:
                # It expired between the failed CAS and this read. Say so rather
                # than printing a lease that is not there.
                err("busy: %s was held and is now expired; retry" % a.name)
            else:
                err("busy: %s held by %s (token %d) for another %d s"
                    % (a.name, cur["holder"], cur["token"], cur["remaining_s"]))
            return 1
        out(tok)
        return 0
    if v in ("renew", "release"):
        ok = (s.renew(a.name, a.holder, a.token, a.ttl) if v == "renew"
              else s.release(a.name, a.holder, a.token))
        if not ok:
            # A silent rc 1 here is how a forgotten release looks to a shell
            # step that does not check `$?`: say who holds it now instead.
            cur = s.who(a.name)
            err("%s rejected: %s is %s" % (
                v, a.name,
                "held by %s (token %d)" % (cur["holder"], cur["token"])
                if cur else "not held by anyone (expired or already released)"))
        return 0 if ok else 1
    if v == "who":
        e = s.who(a.name)
        if e is None:
            return 1
        out(json.dumps(e, sort_keys=True) if a.json else _fmt(a.name, e))
        return 0
    if v == "list":
        d = s.list()
        if a.json:
            out(json.dumps(d, sort_keys=True))
        else:
            for name in sorted(d):
                out(_fmt(name, d[name]))
        return 0 if d else 1
    return 2


if __name__ == "__main__":
    sys.exit(run())
