#!/usr/bin/env python3
import inspect
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

MARKER = "STAT_TECH_LIVE_LOOP_V1_20260628"
SYS_PATH_MARKER = "STAT_TECH_LIVE_LOOP_SYS_PATH_FIX_20260628"

APP_ROOT = Path(__file__).resolve().parents[1]
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

ROOT = APP_ROOT
LOG_PATH = ROOT / "logs" / "stat_tech_live_bridge_events_v1.jsonl"
STATE_PATH = ROOT / "state" / "stat_tech_live_loop_state_v1.json"

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def env_bool(k, default=False):
    v = os.getenv(k)
    if v is None:
        return bool(default)
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")

def env_float(k, default):
    try:
        return float(os.getenv(k, str(default)))
    except Exception:
        return float(default)

def append_jsonl(path, row):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")

def load_state():
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"keys": {}, "pair_last": {}}

def save_state(st):
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(st, ensure_ascii=False, indent=2), encoding="utf-8")

def ts_now():
    return time.time()

def make_signal_key(r):
    return "|".join([
        str(r.get("symbol") or ""),
        str(r.get("direction") or ""),
        str(r.get("setup_type") or ""),
        str(round(float(r.get("entry") or 0), 8)),
        str(round(float(r.get("sl") or 0), 8)),
    ])

def call_bridge(payload):
    from app import main as main_app

    bridge = getattr(main_app, "_vps_execution_bridge", None)
    if not callable(bridge):
        return {
            "ok": False,
            "decision": "ERROR",
            "reason": "bridge_callable_missing",
        }

    res = bridge(payload)
    if inspect.iscoroutine(res):
        import asyncio
        res = asyncio.run(res)
    return res

def run_once():
    from app.stat_tech_signal_v1 import run_once as stat_run_once
    from app.live_entry_confluence_gate_v1 import evaluate_live_entry_confluence_gate_v1
    from app.live_rr12_plan_lock_v1 import apply_live_rr12_plan_lock_v1

    live_enabled = env_bool("STAT_TECH_LIVE_ENABLED", False)
    # STAT_TECH_NO_EXTRA_THROTTLE_PARITY_20260628
    cooldown_sec = env_float("STAT_TECH_COOLDOWN_MIN", 0.0) * 60.0
    now = ts_now()

    state = load_state()
    state["keys"] = {
        k: v for k, v in dict(state.get("keys") or {}).items()
        if now - float(v or 0) <= cooldown_sec
    }
    state["pair_last"] = {
        k: v for k, v in dict(state.get("pair_last") or {}).items()
        if now - float(v or 0) <= cooldown_sec
    }

    summary = stat_run_once(write_log=True)
    rows = summary.get("candidates") or []

    out = {
        "ok": True,
        "created_at_utc": utc_now_iso(),
        "marker": MARKER,
        "live_enabled": live_enabled,
        "total_candidates": len(rows),
        "sent": 0,
        "skipped": 0,
        "results": [],
    }

    for r in rows:
        symbol = str(r.get("symbol") or "").upper()
        key = make_signal_key(r)

        if key in state["keys"]:
            out["skipped"] += 1
            out["results"].append({"symbol": symbol, "skip": "duplicate_key_cooldown", "signal_key": key})
            continue

        if symbol in state["pair_last"]:
            out["skipped"] += 1
            out["results"].append({"symbol": symbol, "skip": "pair_cooldown", "signal_key": key})
            continue

        payload = dict(r)
        payload["signal_key"] = key
        payload["signal_id"] = key
        payload["source"] = "STAT_TECH_V1"
        payload["signal_source"] = "STAT_TECH_V1"
        payload["execution_owner"] = "STAT_TECH_PRIMARY"
        payload["status"] = "CANDIDATE"
        payload["rr"] = payload.get("rr") or 1.2
        payload["target_mode"] = "SINGLE_FULL"

        conf = evaluate_live_entry_confluence_gate_v1(payload)
        payload["live_entry_confluence_gate_v1"] = conf

        event = {
            "created_at_utc": utc_now_iso(),
            "marker": MARKER,
            "symbol": symbol,
            "direction": payload.get("direction"),
            "setup_type": payload.get("setup_type"),
            "signal_key": key,
            "technical_score": payload.get("technical_score"),
            "confluence_decision": conf.get("decision"),
            "confluence_reason": conf.get("reason"),
            "confluence_score": conf.get("confluence_score"),
            "confluence_components": conf.get("components"),
            "live_enabled": live_enabled,
        }

        if str(conf.get("decision") or "").upper() != "ALLOW":
            event["final_decision"] = "NO_TRADE"
            event["final_reason"] = conf.get("reason") or "confluence_block"
            append_jsonl(LOG_PATH, event)
            out["results"].append(event)
            state["keys"][key] = now
            state["pair_last"][symbol] = now
            continue

        rr12 = apply_live_rr12_plan_lock_v1(payload)
        payload["live_rr12_plan_lock_v1"] = rr12
        event["rr12_decision"] = rr12.get("decision")
        event["rr12_reason"] = rr12.get("reason")

        if str(rr12.get("decision") or "").upper() == "BLOCK":
            event["final_decision"] = "NO_TRADE"
            event["final_reason"] = rr12.get("reason") or "rr12_block"
            append_jsonl(LOG_PATH, event)
            out["results"].append(event)
            state["keys"][key] = now
            state["pair_last"][symbol] = now
            continue

        if isinstance(rr12.get("payload"), dict):
            payload.update(dict(rr12.get("payload") or {}))

        if not live_enabled:
            event["final_decision"] = "DRY_RUN"
            event["final_reason"] = "stat_tech_live_disabled"
            append_jsonl(LOG_PATH, event)
            out["results"].append(event)
            state["keys"][key] = now
            state["pair_last"][symbol] = now
            continue

        bridge_res = call_bridge(payload)
        event["bridge_decision"] = bridge_res.get("decision") if isinstance(bridge_res, dict) else None
        event["bridge_reason"] = bridge_res.get("reason") if isinstance(bridge_res, dict) else None
        event["bridge_ok"] = bridge_res.get("ok") if isinstance(bridge_res, dict) else None
        event["final_decision"] = event.get("bridge_decision")
        event["final_reason"] = event.get("bridge_reason")
        append_jsonl(LOG_PATH, event)

        out["sent"] += 1
        out["results"].append(event)
        state["keys"][key] = now
        state["pair_last"][symbol] = now

    save_state(state)
    append_jsonl(LOG_PATH, {
        "created_at_utc": utc_now_iso(),
        "marker": MARKER,
        "event": "SUMMARY",
        **out,
    })
    return out

if __name__ == "__main__":
    print(json.dumps(run_once(), ensure_ascii=False, indent=2))
