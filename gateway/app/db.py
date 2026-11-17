"""SQLite persistence for the edge gateway.

Tables:
  upstreams      - registered upstream targets (seeded from env, upserted on boot)
  routes         - single-row table holding the effective route and its version
  inflight       - one row per admitted proxied request; wiped on startup so a
                   restarted gateway never reports interrupted requests as in-flight
  probes         - audit log of candidate probes; a commit requires a fresh,
                   successful probe recorded against the same route version
  shadow_config  - single-row table holding the side-by-side comparison switch:
                   enabled flag, candidate target and the current comparison
                   round (bumped on every (re-)enable)
  shadow_records - one row per mirrored GET /work request, bound at admission
                   to the primary target, candidate target and round of that
                   moment; rows left incomplete across a restart are flagged
                   interrupted on boot
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from pathlib import Path

from . import compare

DB_PATH = Path(os.environ.get("GATEWAY_DB", "/data/gateway.db"))
PROBE_TTL_SECONDS = float(os.environ.get("PROBE_TTL_SECONDS", "60"))
SHADOW_RECORD_LIMIT = 50  # rows returned by get_state

_lock = threading.RLock()
_conn: sqlite3.Connection | None = None


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db(upstreams: dict[str, str], initial_target: str) -> dict:
    """Create tables, upsert the upstream registry, seed the route and the
    shadow config once, drop in-flight rows left behind by a previous (killed)
    process, and flag shadow records that never completed as interrupted."""
    global _conn
    if not upstreams:
        raise RuntimeError("no upstreams configured")
    if initial_target not in upstreams:
        raise RuntimeError(f"initial target {initial_target!r} not in upstreams")
    _conn = _connect()
    with _lock, _conn:
        _conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS upstreams (
                name TEXT PRIMARY KEY,
                url  TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS routes (
                id             INTEGER PRIMARY KEY CHECK (id = 1),
                current_target TEXT NOT NULL,
                version        INTEGER NOT NULL,
                updated_at     REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS inflight (
                id         TEXT PRIMARY KEY,
                target     TEXT NOT NULL,
                path       TEXT NOT NULL,
                started_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS probes (
                id           TEXT PRIMARY KEY,
                target       TEXT NOT NULL,
                base_version INTEGER NOT NULL,
                ok           INTEGER NOT NULL,
                detail       TEXT,
                created_at   REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS shadow_config (
                id         INTEGER PRIMARY KEY CHECK (id = 1),
                enabled    INTEGER NOT NULL,
                candidate  TEXT,
                round      INTEGER NOT NULL,
                updated_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS shadow_records (
                id               TEXT PRIMARY KEY,  -- same id as the admitted request
                round            INTEGER NOT NULL,
                primary_target   TEXT NOT NULL,
                candidate_target TEXT NOT NULL,
                path             TEXT NOT NULL,
                started_at       REAL NOT NULL,
                finished_at      REAL NOT NULL DEFAULT 0,
                interrupted      INTEGER NOT NULL DEFAULT 0,
                primary_status   INTEGER,
                primary_body     TEXT,
                primary_error    TEXT,
                cand_status      INTEGER,
                cand_body        TEXT,
                cand_error       TEXT
            );
            """
        )
        for name, url in upstreams.items():
            _conn.execute(
                "INSERT INTO upstreams (name, url) VALUES (?, ?) "
                "ON CONFLICT(name) DO UPDATE SET url = excluded.url",
                (name, url),
            )
        seeded = False
        if _conn.execute("SELECT COUNT(*) AS c FROM routes").fetchone()["c"] == 0:
            _conn.execute(
                "INSERT INTO routes (id, current_target, version, updated_at) "
                "VALUES (1, ?, 0, ?)",
                (initial_target, time.time()),
            )
            seeded = True
        if _conn.execute("SELECT COUNT(*) AS c FROM shadow_config").fetchone()["c"] == 0:
            _conn.execute(
                "INSERT INTO shadow_config (id, enabled, candidate, round, updated_at) "
                "VALUES (1, 0, NULL, 0, ?)",
                (time.time(),),
            )
        # Interrupted requests of a previous process must not linger as in-flight.
        cleared = _conn.execute("DELETE FROM inflight").rowcount
        # Shadow records whose primary or candidate side never completed belong
        # to the previous process; they can never finish now.
        interrupted = _conn.execute(
            "UPDATE shadow_records SET interrupted = 1 "
            "WHERE interrupted = 0 AND ("
            "  (primary_status IS NULL AND primary_error IS NULL) OR "
            "  (cand_status IS NULL AND cand_error IS NULL))"
        ).rowcount
    return {
        "seeded_route": seeded,
        "cleared_inflight": cleared,
        "interrupted_shadow": interrupted,
    }


def _db() -> sqlite3.Connection:
    assert _conn is not None, "db not initialised"
    return _conn


def get_route() -> dict:
    with _lock:
        row = _db().execute(
            "SELECT current_target, version, updated_at FROM routes WHERE id = 1"
        ).fetchone()
    return dict(row)


def upstream_url(name: str) -> str | None:
    with _lock:
        row = _db().execute(
            "SELECT url FROM upstreams WHERE name = ?", (name,)
        ).fetchone()
    return row["url"] if row else None


def admit(path: str) -> dict:
    """Atomically snapshot the effective route *and* the shadow config, and
    register the request as in-flight. The request keeps this target even if
    a switch commits while it is running, so it always completes against the
    original upstream.

    When comparison is enabled for a candidate other than the current target,
    a GET /work request additionally opens a shadow record bound to this
    moment's primary target, candidate and round; the candidate call itself
    is fired afterwards by the caller and never blocks the primary response.
    """
    rid = uuid.uuid4().hex
    with _lock, _db() as conn:
        route = conn.execute(
            "SELECT current_target, version FROM routes WHERE id = 1"
        ).fetchone()
        cfg = conn.execute(
            "SELECT enabled, candidate, round FROM shadow_config WHERE id = 1"
        ).fetchone()
        shadow = None
        if (
            cfg
            and cfg["enabled"]
            and cfg["candidate"]
            and path == "/work"
            and cfg["candidate"] != route["current_target"]
        ):
            url = conn.execute(
                "SELECT url FROM upstreams WHERE name = ?", (cfg["candidate"],)
            ).fetchone()
            if url is not None:
                shadow = {
                    "round": cfg["round"],
                    "candidate": cfg["candidate"],
                    "candidate_url": url["url"],
                }
        conn.execute(
            "INSERT INTO inflight (id, target, path, started_at) VALUES (?, ?, ?, ?)",
            (rid, route["current_target"], path, time.time()),
        )
        if shadow is not None:
            conn.execute(
                "INSERT INTO shadow_records "
                "(id, round, primary_target, candidate_target, path, started_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    rid,
                    shadow["round"],
                    route["current_target"],
                    shadow["candidate"],
                    path,
                    time.time(),
                ),
            )
    return {
        "id": rid,
        "target": route["current_target"],
        "version": route["version"],
        "shadow": shadow,
    }


def finish(request_id: str) -> None:
    with _lock, _db() as conn:
        conn.execute("DELETE FROM inflight WHERE id = ?", (request_id,))


def shadow_side_done(
    record_id: str, side: str, status: int | None, body: str | None, error: str | None
) -> None:
    """Record one side's outcome on the shadow record created at admission.
    Writes go to the record's own id, so a late candidate result still lands
    on its original round/targets no matter what changed since."""
    cols = {
        "primary": ("primary_status", "primary_body", "primary_error"),
        "candidate": ("cand_status", "cand_body", "cand_error"),
    }[side]
    if side == "candidate":
        sql = (
            f"UPDATE shadow_records SET {cols[0]} = ?, {cols[1]} = ?, {cols[2]} = ?, "
            "finished_at = ? WHERE id = ?"
        )
        params = (status, body, error, time.time(), record_id)
    else:
        sql = (
            f"UPDATE shadow_records SET {cols[0]} = ?, {cols[1]} = ?, {cols[2]} = ? "
            "WHERE id = ?"
        )
        params = (status, body, error, record_id)
    with _lock, _db() as conn:
        conn.execute(sql, params)


def set_shadow(enabled: bool, candidate: str | None = None) -> dict:
    """Enable comparison for a candidate (starting a new round) or disable it.
    Disabling keeps the round/candidate so late results of the old round still
    resolve against it; re-enabling always bumps the round."""
    with _lock, _db() as conn:
        if enabled:
            conn.execute(
                "UPDATE shadow_config SET enabled = 1, candidate = ?, "
                "round = round + 1, updated_at = ? WHERE id = 1",
                (candidate, time.time()),
            )
        else:
            conn.execute(
                "UPDATE shadow_config SET enabled = 0, updated_at = ? WHERE id = 1",
                (time.time(),),
            )
        row = conn.execute("SELECT * FROM shadow_config WHERE id = 1").fetchone()
    return _shadow_public(dict(row))


def get_shadow() -> dict:
    with _lock:
        row = _db().execute("SELECT * FROM shadow_config WHERE id = 1").fetchone()
    return _shadow_public(dict(row))


def _shadow_public(cfg: dict) -> dict:
    cfg["enabled"] = bool(cfg["enabled"])
    route = get_route()
    cfg["effective"] = (
        cfg["enabled"]
        and cfg["candidate"] is not None
        and cfg["candidate"] != route["current_target"]
    )
    return cfg


def _enrich_record(row: sqlite3.Row) -> dict:
    r = dict(row)
    p_done = r["primary_status"] is not None or r["primary_error"] is not None
    c_done = r["cand_status"] is not None or r["cand_error"] is not None
    if r["interrupted"]:
        status = "interrupted"
    elif p_done and c_done:
        status = "done"
    else:
        status = "pending"
    out = {
        "id": r["id"],
        "round": r["round"],
        "primary_target": r["primary_target"],
        "candidate_target": r["candidate_target"],
        "path": r["path"],
        "started_at": r["started_at"],
        "finished_at": r["finished_at"] or None,
        "status": status,
        "verdict": None,
        "reason": None,
        "diff_keys": [],
        "primary": None,
        "candidate": None,
    }
    if p_done:
        out["primary"] = {"status": r["primary_status"], "error": r["primary_error"]}
    if c_done:
        out["candidate"] = {"status": r["cand_status"], "error": r["cand_error"]}
    if status == "done":
        res = compare.compare(
            {
                "status": r["primary_status"],
                "raw": r["primary_body"],
                "error": r["primary_error"],
            },
            {"status": r["cand_status"], "raw": r["cand_body"], "error": r["cand_error"]},
        )
        out["verdict"] = res["verdict"]
        out["reason"] = res["reason"]
        out["diff_keys"] = res["diff_keys"]
        out["primary"].update(
            business=res["primary_business"], parseable=res["primary_parseable"]
        )
        out["candidate"].update(
            business=res["candidate_business"], parseable=res["candidate_parseable"]
        )
    return out


def record_probe(target: str, base_version: int, ok: bool, detail: dict) -> dict:
    """A probe never mutates the route; it only records an auditable result."""
    pid = uuid.uuid4().hex
    with _lock, _db() as conn:
        conn.execute(
            "INSERT INTO probes (id, target, base_version, ok, detail, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (pid, target, base_version, 1 if ok else 0, json.dumps(detail), time.time()),
        )
    return {"id": pid}


def commit_switch(target: str, base_version: int, probe_id: str) -> dict:
    """Compare-and-swap the route on its version. A commit carrying a stale
    version (e.g. a late probe from an interleaved switch attempt) loses and
    cannot overwrite the already-effective route."""
    with _lock, _db() as conn:
        route = conn.execute(
            "SELECT current_target, version FROM routes WHERE id = 1"
        ).fetchone()
        if route["version"] != base_version:
            return {
                "ok": False,
                "error": "version_conflict",
                "current_version": route["version"],
                "current_target": route["current_target"],
            }
        if route["current_target"] == target:
            return {
                "ok": False,
                "error": "target_is_current",
                "current_version": route["version"],
                "current_target": route["current_target"],
            }
        if upstream_url(target) is None:
            return {"ok": False, "error": "unknown_target"}
        probe = conn.execute(
            "SELECT * FROM probes WHERE id = ?", (probe_id,)
        ).fetchone()
        if (
            probe is None
            or not probe["ok"]
            or probe["target"] != target
            or probe["base_version"] != base_version
        ):
            return {"ok": False, "error": "probe_invalid"}
        if time.time() - probe["created_at"] > PROBE_TTL_SECONDS:
            return {"ok": False, "error": "probe_stale"}
        cur = conn.execute(
            "UPDATE routes SET current_target = ?, version = version + 1, updated_at = ? "
            "WHERE id = 1 AND version = ?",
            (target, time.time(), base_version),
        )
        if cur.rowcount != 1:  # guarded by _lock; belt and braces
            return {"ok": False, "error": "version_conflict"}
        return {"ok": True, "current_target": target, "version": base_version + 1}


def get_state() -> dict:
    with _lock:
        route = dict(_db().execute("SELECT * FROM routes WHERE id = 1").fetchone())
        upstreams = [
            dict(r) for r in _db().execute("SELECT name, url FROM upstreams ORDER BY name")
        ]
        inflight = [
            dict(r) for r in _db().execute("SELECT * FROM inflight ORDER BY started_at")
        ]
        probes = [
            dict(r)
            for r in _db().execute(
                "SELECT * FROM probes ORDER BY created_at DESC LIMIT 10"
            )
        ]
        shadow_rows = [
            r
            for r in _db().execute(
                "SELECT * FROM shadow_records ORDER BY started_at DESC LIMIT ?",
                (SHADOW_RECORD_LIMIT,),
            )
        ]
        shadow_cfg = dict(
            _db().execute("SELECT * FROM shadow_config WHERE id = 1").fetchone()
        )
    counts: dict[str, int] = {}
    for row in inflight:
        counts[row["target"]] = counts.get(row["target"], 0) + 1
    now = time.time()
    for u in upstreams:
        u["inflight"] = counts.get(u["name"], 0)
        u["is_current"] = u["name"] == route["current_target"]
        # A non-current target may only be taken offline once its in-flight
        # requests have fully drained.
        u["can_offline"] = (not u["is_current"]) and u["inflight"] == 0
    for row in inflight:
        row["age_ms"] = int((now - row["started_at"]) * 1000)
    for p in probes:
        p["detail"] = json.loads(p["detail"]) if p["detail"] else {}
    return {
        "now": now,
        "route": route,
        "upstreams": upstreams,
        "inflight": inflight,
        "recent_probes": probes,
        "shadow": _shadow_public(shadow_cfg),
        "shadow_records": [_enrich_record(r) for r in shadow_rows],
    }
