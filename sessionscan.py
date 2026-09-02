#!/usr/bin/env python3
"""sessionscan — a read-only walk over a coding agent's session transcripts.

This is the small adapter the substrate's ingest paths sit on: `golemlog`'s
`ingest-fleet` turns transcripts into `fleet.rows` records, and `golemledger`'s
`ingest-usage` folds their per-message token usage into balanced postings. It is
deliberately the *only* module that knows the on-disk transcript layout, so the
log, the kv, the lease service and the ledger stay free of it.

Layout it expects, under a sessions root:

    <root>/<project>/<session-id>.jsonl                                depth 1, main
    <root>/<project>/<sid>/subagents/<agent>.jsonl                     depth 3, subagent
    <root>/<project>/<sid>/subagents/workflows/<wf>/<agent>.jsonl      depth 5, subagent

Both kinds are attributed to `<root>/<project>`, so a project (repo) name keeps
its meaning for either. A nested `.jsonl` with no `subagents/` segment is not a
transcript in any layout observed here, and is skipped rather than
mis-attributed. A depth-1-only glob would see a small minority of the files.

Each line of a transcript is one JSON object; only `user` and `assistant` rows
are read. Configuration is by environment, so nothing here hardcodes a machine:

    GOLEM_SESSIONS_ROOT      the transcripts root (default ~/.agent-sessions)
    GOLEM_PROJECT_PREFIX     a path-encoding prefix stripped from project dir
                             names (many agents encode a project's absolute path
                             as its directory name, separators replaced by "-")
"""
import datetime
import json
import os
import re
import sys
import time

ROOT = os.path.expanduser(os.environ.get("GOLEM_SESSIONS_ROOT", "~/.agent-sessions"))
PREFIX = os.environ.get("GOLEM_PROJECT_PREFIX", "")
SNIP = 80

ROW_PREFILTER = re.compile(r'"type":\s*"(user|assistant)"')

# A user row is not necessarily a *human* turn. Tool results, hook feedback,
# teammate messages, task notifications, slash-command and skill boilerplate and
# image pastes all arrive as `user` rows. One shared filter excludes them, so
# every consumer counts the same thing.
SYNTHETIC_PREFIXES = (
    "<",                       # <command-*>, <local-command*>, <task-notification>, ...
    "[Request interrupted",
    "[Image:",
    "[image:",
    "Another session sent",
    "Stop hook feedback",
    "A session-scoped Stop hook",
    "Base directory for this skill",
    "# /loop",
    "This session is being continued",
    "Task notification",
    "Goal check-in:",
    "Review this change for security",
    "/",                       # bare slash command
)
SYNTHETIC_ANYWHERE = re.compile(
    r"(<command-|<local-command|Base directory for this skill|Another session sent|"
    r"Stop hook feedback|A session-scoped Stop hook|<task-notification|"
    r"This session is being continued|\[Request interrupted)"
)


def user_text(row):
    """Text of a user row, or None if the row is not a plain text user turn
    (tool_result rows and non-text content return None)."""
    if row.get("type") != "user":
        return None
    content = row.get("message", {}).get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        if any(isinstance(x, dict) and x.get("type") == "tool_result" for x in content):
            return None
        texts = [x.get("text", "") for x in content
                 if isinstance(x, dict) and x.get("type") == "text"]
        if not texts:
            return None
        return "\n".join(texts)
    return None


def is_human_turn(row):
    """True iff the row is a user turn typed by a person, not one of the many
    synthetic rows that share the `user` type (see SYNTHETIC_PREFIXES)."""
    if row.get("isMeta"):                       # injected: skill/loop prompts, check-ins
        return False
    if str(row.get("entrypoint", "cli")).startswith("sdk"):   # programmatic sessions
        return False
    origin = row.get("origin") or {}
    if isinstance(origin, dict) and origin.get("kind") not in (None, "human"):
        return False
    text = user_text(row)
    if text is None:
        return False
    t = text.strip()
    if not t:
        return False
    if t.startswith(SYNTHETIC_PREFIXES):
        return False
    if SYNTHETIC_ANYWHERE.search(t[:200]):
        return False
    return True


def assistant_parts(row):
    """(text, tool_uses) for an assistant row; tool_uses is a list of content dicts."""
    if row.get("type") != "assistant":
        return None, []
    content = row.get("message", {}).get("content")
    if isinstance(content, str):
        return content, []
    if not isinstance(content, list):
        return "", []
    texts = [x.get("text", "") for x in content
             if isinstance(x, dict) and x.get("type") == "text"]
    uses = [x for x in content if isinstance(x, dict) and x.get("type") == "tool_use"]
    return "\n".join(texts), uses


def indexable_row(row):
    """(who, text) for a row worth remembering, or (None, None).

    The one predicate for "is this row worth indexing". A consumer walking
    transcripts from disk and a consumer reading `fleet.rows` out of the log must
    agree exactly, so both go through this. A human row passes iff is_human_turn;
    an assistant row passes iff it has non-empty final text, tool_use parts and
    all — an assistant row that carries a tool_use *and* final text is a real
    answer, not a tool call to be dropped."""
    if row.get("type") == "user":
        if is_human_turn(row):
            return "human", user_text(row)
        return None, None
    if row.get("type") == "assistant":
        text, _uses = assistant_parts(row)
        if text and text.strip():
            return "assistant", text
    return None, None


def row_time(row):
    """Epoch seconds of a row's ISO-8601 `timestamp`, or None if absent/unparsable."""
    ts = row.get("timestamp")
    if not isinstance(ts, str):
        return None
    try:
        return datetime.datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def repo_name(project_dir):
    """Project name from a transcript project directory.

    Where an agent encodes a project's absolute path as the directory name, set
    GOLEM_PROJECT_PREFIX to the shared leading portion and the remainder is the
    name. Otherwise the directory name itself is the name."""
    base = os.path.basename(project_dir.rstrip("/"))
    if PREFIX and base.startswith(PREFIX):
        return base[len(PREFIX):] or base
    return base.lstrip("-") or base


def snip(text, n=SNIP):
    """One-line excerpt, whitespace collapsed, never more than SNIP characters."""
    s = " ".join(str(text).split())
    return s[: min(n, SNIP)]


def within_window(rows, cutoff):
    """Drop rows older than `cutoff`. A row without a timestamp is kept only if its
    immediate predecessor was kept, so tool_use/tool_result and assistant->human
    pairing across adjacent rows survives; a leading untimestamped row is kept
    (there is no predecessor to contradict it)."""
    out, prev_kept = [], True
    for r in rows:
        t = row_time(r)
        keep = prev_kept if t is None else t >= cutoff
        if keep:
            out.append(r)
        prev_kept = keep
    return out


def session_files(root=ROOT):
    """Yield (path, project_dir, kind, depth) for every transcript under `root`."""
    root = os.path.abspath(os.path.expanduser(root)).rstrip(os.sep)
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames.sort()
        rel = os.path.relpath(dirpath, root)
        if rel == os.curdir:                     # no transcripts sit directly in root
            continue
        parts = rel.split(os.sep)
        depth = len(parts)
        if depth > 1 and "subagents" not in parts[1:]:
            continue
        project_dir = os.path.join(root, parts[0])
        kind = "main" if depth == 1 else "subagent"
        for fn in sorted(filenames):
            if fn.endswith(".jsonl"):
                yield os.path.join(dirpath, fn), project_dir, kind, depth


def path_kind(path):
    """"main" or "subagent", from whether `path` has a `subagents` segment — for
    callers that hold only the path, not session_files' full tuple."""
    return "subagent" if "subagents" in path.split(os.sep) else "main"


def iter_sessions(root=ROOT, days=30, repo=None, main_only=False, stats=None):
    """Yield (repo, path, rows) per transcript modified within `days`; rows older
    than the window by their own timestamp are dropped (see within_window).

    Covers subagent transcripts as well as main sessions. `main_only` restricts to
    main sessions, and `stats` (a dict) is filled with the main/subagent file
    counts actually yielded."""
    cutoff = time.time() - days * 86400
    if stats is not None:
        stats.setdefault("main", 0)
        stats.setdefault("subagent", 0)
    for path, pdir, kind, _depth in sorted(session_files(root)):
        if main_only and kind != "main":
            continue
        try:
            if os.path.getmtime(path) < cutoff:
                continue
        except OSError:
            continue
        if repo and repo not in os.path.basename(pdir):
            continue
        if stats is not None:
            stats[kind] += 1
        rows = []
        try:
            fh = open(path, errors="replace")
        except OSError as e:
            print("WARN: skipping unreadable %s: %s" % (path, e.__class__.__name__),
                  file=sys.stderr)
            continue
        with fh:
            for line in fh:
                if not ROW_PREFILTER.search(line):   # cheap skip of attachment/system rows
                    continue
                try:
                    d = json.loads(line)
                except Exception:
                    continue
                if d.get("type") in ("user", "assistant"):
                    rows.append(d)
        yield repo_name(pdir), path, within_window(rows, cutoff)
