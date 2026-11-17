#!/usr/bin/env python3
"""End-to-end smoke test for the edge gateway.

Usage:
  smoke_test.py BASE_URL SAVE_FILE phase1   # switch + shadow semantics, arms an
                                            # interrupted request/record
  smoke_test.py BASE_URL SAVE_FILE phase2   # post-restart recovery checks

Upstream admin URLs (for setting the demo business result) come from
ALPHA_URL / BETA_URL env vars, defaulting to the local harness ports.
"""
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
SAVE = sys.argv[2] if len(sys.argv) > 2 else "/tmp/edge-gw-phase1.json"
PHASE = sys.argv[3] if len(sys.argv) > 3 else "phase1"

ALPHA = os.environ.get("ALPHA_URL", "http://127.0.0.1:9001")
BETA = os.environ.get("BETA_URL", "http://127.0.0.1:9002")

FAILURES: list[str] = []


def api(method, path, body=None, timeout=70):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path, method=method, data=data,
        headers={"content-type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def up(base, method, path, body=None, timeout=10):
    """Talk to a demo upstream directly (admin hooks)."""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        base + path, method=method, data=data,
        headers={"content-type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def check(name, cond, extra=""):
    mark = "PASS" if cond else "FAIL"
    print(f"[{mark}] {name}" + (f"  ({extra})" if extra and not cond else ""))
    if not cond:
        FAILURES.append(name)


def inflight_of(state, name):
    for u in state["upstreams"]:
        if u["name"] == name:
            return u["inflight"]
    return 0


def upstream_of(state, name):
    return next(u for u in state["upstreams"] if u["name"] == name)


def wait_state(pred, what, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, st = api("GET", "/api/state")
        if pred(st):
            return st
        time.sleep(0.1)
    check(f"等待状态: {what}", False)
    return api("GET", "/api/state")[1]


def find_record(state, rid):
    return next((r for r in state["shadow_records"] if r["id"] == rid), None)


def wait_record(rid, pred, what, timeout=12):
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, st = api("GET", "/api/state")
        rec = find_record(st, rid)
        if rec and pred(rec):
            return rec
        time.sleep(0.1)
    check(f"等待对照记录: {what}", False)
    return {}


def unit_checks():
    """Pure comparison rules, imported straight from the gateway package."""
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "gateway"))
    from app import compare  # noqa: PLC0415

    def side(status, raw=None, error=None):
        return {"status": status, "raw": raw, "error": error}

    r = compare.compare(
        side(200, '{"upstream":"a","ts":1,"delay_ms":5,"result":"ok"}'),
        side(200, '{"upstream":"b","ts":2,"delay_ms":9,"result":"ok"}'),
    )
    check("单元:忽略 upstream/ts/delay_ms 后判一致", r["verdict"] == "match")

    r = compare.compare(side(200, '{"result":"ok"}'), side(200, '{"result":"changed"}'))
    check("单元:业务字段不同判不一致",
          r["verdict"] == "mismatch" and r["diff_keys"] == ["result"])

    r = compare.compare(side(200, '{"result":"ok"}'), side(200, "not-json{"))
    check("单元:候选回包无法解析判无法比较",
          r["verdict"] == "incomparable" and r["reason"] == "candidate_body_not_json")

    r = compare.compare(side(200, "<html>"), side(200, '{"result":"ok"}'))
    check("单元:主目标回包无法解析判无法比较",
          r["verdict"] == "incomparable" and r["reason"] == "primary_body_not_json")

    r = compare.compare(side(None, error="connect refused"), side(200, '{"result":"ok"}'))
    check("单元:主目标传输失败判无法比较",
          r["verdict"] == "incomparable" and r["reason"] == "primary_transport_error")

    r = compare.compare(side(200, '{"result":"ok"}'), side(500, '{"result":"ok"}'))
    check("单元:HTTP 状态不同判不一致",
          r["verdict"] == "mismatch" and r["reason"] == "status_differs")


def shadow_checks():
    """Side-by-side comparison semantics, including the key interleavings."""
    # S1) enable validation
    s, e = api("POST", "/api/shadow/enable", {"candidate": "beta"})  # beta is current
    check("候选为当前目标时开启被拒绝", s == 400 and e["error"] == "candidate_is_current")
    s, e = api("POST", "/api/shadow/enable", {"candidate": "ghost"})
    check("未知候选开启被拒绝", s == 404 and e["error"] == "unknown_target")
    s, e = api("POST", "/api/shadow/enable", {"candidate": "alpha"})
    check("开启旁路对照(候选 alpha, 轮次 1)",
          s == 200 and e["ok"] is True and e["shadow"]["round"] == 1
          and e["shadow"]["enabled"] is True)

    # S2) both ends agree -> match; record binds targets+round at admission
    s, r = api("POST", "/api/call", {"delay_ms": 50})
    check("对照中请求响应携带旁路信息",
          r.get("shadow") == {"round": 1, "candidate": "alpha"})
    rec = wait_record(r["request_id"], lambda x: x["status"] == "done", "首条对照记录完成")
    check("两端业务结果相同判一致", rec.get("verdict") == "match", json.dumps(rec))
    check("记录绑定接单时两端目标与轮次",
          rec.get("primary_target") == "beta" and rec.get("candidate_target") == "alpha"
          and rec.get("round") == 1)
    check("记录展示两端 HTTP 状态",
          rec.get("primary", {}).get("status") == 200
          and rec.get("candidate", {}).get("status") == 200)

    # S3) candidate business result differs -> mismatch on the result key
    up(ALPHA, "POST", "/admin/work_result", {"result": "changed"})
    s, r = api("POST", "/api/call", {"delay_ms": 50})
    rec = wait_record(r["request_id"], lambda x: x["status"] == "done", "不一致记录完成")
    check("候选业务结果不同判不一致",
          rec.get("verdict") == "mismatch" and "result" in rec.get("diff_keys", []),
          json.dumps(rec))
    up(ALPHA, "POST", "/admin/work_result", {"result": "ok"})

    # S4) candidate unreachable -> incomparable, never "match" (reopen = new round)
    s, e = api("POST", "/api/shadow/enable", {"candidate": "dead"})
    check("重开对照进入新轮次(轮次 2)", e["ok"] is True and e["shadow"]["round"] == 2)
    s, r = api("POST", "/api/call", {"delay_ms": 50})
    rec = wait_record(r["request_id"], lambda x: x["status"] == "done", "候选不可达记录完成")
    check("候选传输失败判无法比较且不得报一致",
          rec.get("verdict") == "incomparable"
          and rec.get("reason") == "candidate_transport_error", json.dumps(rec))
    check("候选失败时主端状态仍被记录", rec.get("primary", {}).get("status") == 200)

    # S5) late results belong to the original round; candidate-becomes-primary
    # stops mirroring new requests
    s, e = api("POST", "/api/shadow/enable", {"candidate": "alpha"})
    check("再次开启候选 alpha(轮次 3)", e["ok"] is True and e["shadow"]["round"] == 3)
    slow = {}

    def slow_call():
        try:
            ss, bb = api("POST", "/api/call", {"delay_ms": 2500})
            slow["status"], slow["body"] = ss, bb
        except Exception as exc:  # noqa: BLE001
            slow["error"] = str(exc)

    t = threading.Thread(target=slow_call, daemon=True)
    t.start()
    st = wait_state(lambda s2: any(x["round"] == 3 for x in s2["shadow_records"]),
                    "轮次 3 对照记录出现")
    slow_rec = next(x for x in st["shadow_records"] if x["round"] == 3)
    n_records = len(st["shadow_records"])
    # interleave a route switch to the candidate while the mirror is in flight
    s, p = api("POST", "/api/switch/probe", {"target": "alpha", "base_version": 3})
    s, c = api("POST", "/api/switch/commit",
               {"target": "alpha", "base_version": 3, "probe_id": p["probe_id"]})
    check("对照期间切换路由到候选(alpha@v4)", c.get("ok") is True and c.get("version") == 4)
    _, st = api("GET", "/api/state")
    check("候选成为主目标后对照标记为不生效", st["shadow"]["effective"] is False)
    s, r = api("POST", "/api/call", {"delay_ms": 50})
    check("候选成为主目标后新请求不再旁路",
          r.get("shadow") is None and r["target"] == "alpha")
    _, st = api("GET", "/api/state")
    check("不再旁路时未新增对照记录", len(st["shadow_records"]) == n_records)
    # reopen comparison (new round) while the slow mirror is still in flight
    s, e = api("POST", "/api/shadow/enable", {"candidate": "dead"})
    check("在途期间重开对照(轮次 4)", e["ok"] is True and e["shadow"]["round"] == 4)
    t.join(timeout=10)
    check("慢请求已完成", "body" in slow, slow.get("error", ""))
    rec = wait_record(slow_rec["id"], lambda x: x["status"] == "done", "慢请求对照记录完成")
    check("切换与重开后迟到结果仍归原轮次",
          rec.get("round") == 3 and rec.get("candidate_target") == "alpha"
          and rec.get("primary_target") == "beta", json.dumps(rec))
    check("迟到结果按原绑定判定(一致)", rec.get("verdict") == "match")

    # S6) switch back to beta so the restart checks see the original target
    s, p = api("POST", "/api/switch/probe", {"target": "beta", "base_version": 4})
    s, c = api("POST", "/api/switch/commit",
               {"target": "beta", "base_version": 4, "probe_id": p["probe_id"]})
    check("切回 beta(v5)", c.get("ok") is True and c.get("version") == 5)


def phase1():
    unit_checks()

    _, st = api("GET", "/api/state")
    check("初始当前目标为 alpha", st["route"]["current_target"] == "alpha")
    check("初始版号为 0", st["route"]["version"] == 0)
    check("初始无在途请求", len(st["inflight"]) == 0)
    check("初始对照关闭", st["shadow"]["enabled"] is False)

    # 1) slow request admitted to alpha, keeps running through the switch
    slow = {}

    def slow_call():
        try:
            s, b = api("POST", "/api/call", {"delay_ms": 4000})
            slow["status"], slow["body"] = s, b
        except Exception as e:  # noqa: BLE001
            slow["error"] = str(e)

    t = threading.Thread(target=slow_call, daemon=True)
    t.start()
    wait_state(lambda s: inflight_of(s, "alpha") == 1, "alpha 在途变为 1")

    # 2) before any switch, a fast call still goes to alpha
    s, r = api("POST", "/api/call", {"delay_ms": 50})
    check("切换前快请求走 alpha", r["target"] == "alpha" and r["upstream"]["upstream"] == "alpha")

    # 3) probe beta; while/after probing (before commit) traffic stays on alpha
    s, p = api("POST", "/api/switch/probe", {"target": "beta", "base_version": 0})
    check("探测 beta 成功", p["ok"] is True and p["version_matches"] is True)
    s, r = api("POST", "/api/call", {"delay_ms": 50})
    check("探测后未确认前请求仍走 alpha", r["target"] == "alpha")
    _, st = api("GET", "/api/state")
    check("探测不改变路由", st["route"]["current_target"] == "alpha" and st["route"]["version"] == 0)

    # 4) commit the switch
    s, c = api("POST", "/api/switch/commit",
               {"target": "beta", "base_version": 0, "probe_id": p["probe_id"]})
    check("确认切换成功，版号升为 1", c.get("ok") is True and c.get("version") == 1)

    # 5) new requests immediately go to beta, no waiting for the old one
    s, r = api("POST", "/api/call", {"delay_ms": 50})
    check("切换后新请求立即走 beta", r["target"] == "beta" and r["upstream"]["upstream"] == "beta")
    _, st = api("GET", "/api/state")
    check("旧目标 alpha 仍有 1 条在途（未等待其结束）", inflight_of(st, "alpha") == 1)
    check("alpha 在途未清零，不可下线", upstream_of(st, "alpha")["can_offline"] is False)

    # 6) stale commit (page version outdated) must not override the effective route
    s, p_stale = api("POST", "/api/switch/probe", {"target": "alpha", "base_version": 0})
    check("过期版号的探测被标记 version_matches=false",
          p_stale["ok"] is True and p_stale["version_matches"] is False)
    s, c_stale = api("POST", "/api/switch/commit",
                     {"target": "alpha", "base_version": 0, "probe_id": p_stale["probe_id"]})
    check("过期版号的确认被拒绝(409 version_conflict)",
          s == 409 and c_stale["error"] == "version_conflict")
    _, st = api("GET", "/api/state")
    check("路由未被迟到提交覆盖，仍为 beta@v1",
          st["route"]["current_target"] == "beta" and st["route"]["version"] == 1)

    # 7) failed probe keeps the original route
    s, pd = api("POST", "/api/switch/probe", {"target": "dead", "base_version": 1})
    check("探测不可达候选失败", pd["ok"] is False)
    _, st = api("GET", "/api/state")
    check("探测失败后维持原路由 beta@v1",
          st["route"]["current_target"] == "beta" and st["route"]["version"] == 1)
    s, cd = api("POST", "/api/switch/commit",
                {"target": "dead", "base_version": 1, "probe_id": pd["probe_id"]})
    check("失败探测对应的确认被拒绝", s == 400 and cd["error"] == "probe_invalid")

    # 8) two interleaved switches on the same base version: exactly one wins
    s, px = api("POST", "/api/switch/probe", {"target": "alpha", "base_version": 1})
    s, py = api("POST", "/api/switch/probe", {"target": "alpha", "base_version": 1})
    s, cx = api("POST", "/api/switch/commit",
                {"target": "alpha", "base_version": 1, "probe_id": px["probe_id"]})
    check("交错切换：先到的确认生效(v2)", cx.get("ok") is True and cx.get("version") == 2)
    s, cy = api("POST", "/api/switch/commit",
                {"target": "alpha", "base_version": 1, "probe_id": py["probe_id"]})
    check("交错切换：迟到的确认被版号拒绝", s == 409 and cy["error"] == "version_conflict")

    # 9) late probe must not overwrite an already-effective route
    s, pz = api("POST", "/api/switch/probe", {"target": "beta", "base_version": 2})
    check("探测 beta@v2 成功", pz["ok"] is True)
    s, pz2 = api("POST", "/api/switch/probe", {"target": "beta", "base_version": 2})
    s, cz2 = api("POST", "/api/switch/commit",
                 {"target": "beta", "base_version": 2, "probe_id": pz2["probe_id"]})
    check("另一值班员先完成切换 beta@v3", cz2.get("ok") is True and cz2.get("version") == 3)
    s, cz = api("POST", "/api/switch/commit",
                {"target": "beta", "base_version": 2, "probe_id": pz["probe_id"]})
    check("迟到探测的确认不得覆盖已生效路由", s == 409 and cz["error"] == "version_conflict")
    _, st = api("GET", "/api/state")
    check("路由保持 beta@v3", st["route"]["current_target"] == "beta" and st["route"]["version"] == 3)

    # 10) the slow pre-switch request completes with the full response from alpha
    t.join(timeout=15)
    check("慢请求已结束", "body" in slow, slow.get("error", ""))
    body = slow.get("body", {})
    check("慢请求从原上游 alpha 取回完整响应",
          body.get("ok") is True and body.get("upstream", {}).get("upstream") == "alpha"
          and body.get("upstream", {}).get("delay_ms") == 4000)

    # 11) alpha drains to zero -> only then may it be taken offline
    wait_state(lambda s: inflight_of(s, "alpha") == 0, "alpha 在途清零")
    _, st = api("GET", "/api/state")
    check("alpha 在途清零后提示可下线", upstream_of(st, "alpha")["can_offline"] is True)
    check("当前目标 beta 不标记可下线", upstream_of(st, "beta")["can_offline"] is False)

    # 12) shadow comparison semantics (route ends back at beta)
    shadow_checks()

    # 13) arm one in-flight request + one unfinished shadow record that the
    # harness will interrupt with kill -9
    s, e = api("POST", "/api/shadow/enable", {"candidate": "alpha"})
    check("为重启演练开启对照(轮次 5)", e["ok"] is True and e["shadow"]["round"] == 5)

    def interrupted():
        try:
            api("POST", "/api/call", {"delay_ms": 15000})
        except Exception:  # noqa: BLE001 - connection reset when gateway dies
            pass

    threading.Thread(target=interrupted, daemon=True).start()
    wait_state(lambda s: inflight_of(s, "beta") == 1, "重启前 beta 在途为 1")
    st = wait_state(lambda s: any(x["round"] == 5 for x in s["shadow_records"]),
                    "重启前对照记录已登记(轮次 5)")
    armed = next(x for x in st["shadow_records"] if x["round"] == 5)
    _, st = api("GET", "/api/state")
    with open(SAVE, "w") as f:
        json.dump({
            "target": st["route"]["current_target"],
            "version": st["route"]["version"],
            "shadow_record": armed["id"],
        }, f)
    print("PHASE1 READY: 已在 beta 上挂起 1 条在途请求和 1 条未完成对照，等待网关被杀")

    # keep the client connection open until the gateway is killed
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            api("GET", "/api/state", timeout=2)
            time.sleep(0.2)
        except Exception:  # noqa: BLE001
            print("PHASE1 OK: 网关已停止")
            return
    check("等待网关被杀", False)


def phase2():
    with open(SAVE) as f:
        saved = json.load(f)
    _, st = api("GET", "/api/state")
    check("重启后按已登记路由接流量(目标不变)", st["route"]["current_target"] == saved["target"])
    check("重启后路由版号保持", st["route"]["version"] == saved["version"])
    check("中断请求未留作在途", len(st["inflight"]) == 0)
    check("启动时清理了中断在途记录", st["boot"]["cleared_inflight"] >= 1)
    rec = find_record(st, saved["shadow_record"])
    check("重启前未完成的对照记录仍保留", rec is not None)
    if rec:
        check("未完成对照重启后显示中断", rec["status"] == "interrupted")
    check("启动时标记了中断对照", st["boot"].get("interrupted_shadow", 0) >= 1)
    check("对照配置随重启保留(仍开启, 候选 alpha)",
          st["shadow"]["enabled"] is True and st["shadow"]["candidate"] == "alpha")
    s, r = api("POST", "/api/call", {"delay_ms": 50})
    check("重启后新请求正常发往已登记目标",
          r["target"] == saved["target"] and r["ok"] is True)
    check("重启后对照继续生效(新请求恢复旁路)",
          r.get("shadow") is not None and r["shadow"]["candidate"] == "alpha")
    rec2 = wait_record(r["request_id"], lambda x: x["status"] == "done", "重启后对照记录完成")
    check("重启后对照判定一致", rec2.get("verdict") == "match", json.dumps(rec2))


if PHASE == "phase1":
    phase1()
else:
    phase2()

if FAILURES:
    print(f"\n{len(FAILURES)} 项检查失败:")
    for f in FAILURES:
        print(" -", f)
    sys.exit(1)
print(f"\n全部检查通过 ({PHASE})")
