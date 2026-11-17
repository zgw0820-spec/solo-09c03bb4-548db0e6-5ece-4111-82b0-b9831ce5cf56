"""Business-JSON comparison for shadow (side-by-side) records.

Pure functions, no I/O: given the recorded outcome of the primary and the
candidate call, decide whether the two ends agree. Envelope fields added by
the demo upstream (``upstream``, ``ts``, ``delay_ms``) are ignored at the top
level of the JSON object. A side that failed to transmit, or whose body is
not parseable JSON, makes the record *incomparable* — it must never be
reported as consistent.
"""
from __future__ import annotations

import json

IGNORE_KEYS = frozenset({"upstream", "ts", "delay_ms"})

_MISSING = object()


def business_view(obj):
    """Strip envelope fields from a parsed JSON body (top level only)."""
    if isinstance(obj, dict):
        return {k: v for k, v in obj.items() if k not in IGNORE_KEYS}
    return obj


def parse_body(raw):
    """Return (ok, value); ok=False when the raw body is not valid JSON."""
    if raw is None:
        return False, None
    try:
        return True, json.loads(raw)
    except (ValueError, TypeError):
        return False, None


def diff_keys(a, b):
    """Top-level keys on which two business views differ (empty for non-dicts)."""
    if not (isinstance(a, dict) and isinstance(b, dict)):
        return []
    keys = set(a) | set(b)
    return sorted(k for k in keys if a.get(k, _MISSING) != b.get(k, _MISSING))


def compare(primary: dict, candidate: dict) -> dict:
    """Compare the two recorded sides of a shadow record.

    Each side is ``{"status": int|None, "raw": str|None, "error": str|None}``.
    Returns a dict with ``verdict`` (match | mismatch | incomparable),
    ``reason``, ``diff_keys`` and the per-side filtered business views.
    """
    p_err, c_err = primary.get("error"), candidate.get("error")
    p_ok, p_obj = (False, None) if p_err else parse_body(primary.get("raw"))
    c_ok, c_obj = (False, None) if c_err else parse_body(candidate.get("raw"))
    base = {
        "diff_keys": [],
        "primary_business": None,
        "candidate_business": None,
        "primary_parseable": p_ok,
        "candidate_parseable": c_ok,
    }
    if p_err or c_err:
        which = "both" if (p_err and c_err) else ("primary" if p_err else "candidate")
        return {**base, "verdict": "incomparable", "reason": f"{which}_transport_error"}
    if not (p_ok and c_ok):
        if not p_ok and not c_ok:
            which = "both"
        else:
            which = "primary" if not p_ok else "candidate"
        return {**base, "verdict": "incomparable", "reason": f"{which}_body_not_json"}
    p_biz, c_biz = business_view(p_obj), business_view(c_obj)
    dk = diff_keys(p_biz, c_biz)
    status_diff = primary.get("status") != candidate.get("status")
    if dk and status_diff:
        verdict, reason = "mismatch", "status_and_body_differ"
    elif dk:
        verdict, reason = "mismatch", "body_differs"
    elif status_diff:
        verdict, reason = "mismatch", "status_differs"
    else:
        verdict, reason = "match", None
    return {
        **base,
        "verdict": verdict,
        "reason": reason,
        "diff_keys": dk,
        "primary_business": p_biz,
        "candidate_business": c_biz,
    }
