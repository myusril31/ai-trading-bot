#!/usr/bin/env python3
import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path

LOG_PATH = Path("logs/live_rr12_plan_lock_events_v1.jsonl")

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def to_float(x, default=None):
    try:
        if x is None or x == "":
            return default
        v = float(x)
        return v if math.isfinite(v) else default
    except Exception:
        return default

def append_log(row):
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
    except Exception:
        pass

def direction_of(p):
    d = str(p.get("direction") or p.get("dir") or p.get("side") or "").upper().strip()
    if d in ("BUY", "BULL", "LONG"):
        return "LONG"
    if d in ("SELL", "BEAR", "SHORT"):
        return "SHORT"
    return d

def entry_of(p):
    return to_float(
        p.get("entry_mid")
        or p.get("entry")
        or p.get("entry_price")
        or p.get("limit_entry")
        or p.get("price")
    )

def sl_of(p):
    return to_float(
        p.get("sl")
        or p.get("stop_loss")
        or p.get("invalid")
        or p.get("invalidation")
    )

def result(decision, reason, payload, details=None):
    row = {
        "ok": True,
        "decision": decision,
        "allow": decision == "ALLOW",
        "gate": "live_rr12_plan_lock_v1",
        "reason": reason,
        "symbol": payload.get("symbol"),
        "direction": direction_of(payload),
        "signal_key": payload.get("signal_key") or payload.get("signal_id"),
        "details": details or {},
        "created_at_utc": utc_now_iso(),
    }
    append_log(row)
    return row

def apply_live_rr12_plan_lock_v1(payload):
    """
    Live final plan lock:
    - SL must be structural SMC SL already present and valid.
    - TP is single full target @ RR_TARGET_R, default 1.2R.
    - Does not change sizing, leverage, margin, or SL distance.
    """
    p = dict(payload or {})
    direction = direction_of(p)
    entry = entry_of(p)
    sl = sl_of(p)

    rr = to_float(os.getenv("RR_TARGET_R"), 1.2)
    if rr is None or rr <= 0:
        rr = 1.2

    if direction not in ("LONG", "SHORT"):
        return result("BLOCK", "rr12_invalid_direction", p, {"direction": direction})

    if entry is None or sl is None:
        return result("BLOCK", "rr12_missing_entry_or_sl", p, {"entry": entry, "sl": sl})

    if direction == "LONG":
        if sl >= entry:
            return result("BLOCK", "rr12_invalid_sl_side_long", p, {"entry": entry, "sl": sl})
        risk = entry - sl
        tp = entry + rr * risk

    else:
        if sl <= entry:
            return result("BLOCK", "rr12_invalid_sl_side_short", p, {"entry": entry, "sl": sl})
        risk = sl - entry
        tp = entry - rr * risk

    if risk <= 0:
        return result("BLOCK", "rr12_invalid_risk", p, {"entry": entry, "sl": sl, "risk": risk})

    # Lock canonical fields.
    p["entry_mid"] = entry
    p["entry"] = p.get("entry") or entry
    p["sl"] = sl

    # Existing engine expects tp1/tp2/tp3 to exist in some paths.
    # Single target mode = all TP prices same, existing SINGLE_FULL logic handles qty/full target.
    p["raw_tp1"] = p.get("raw_tp1", p.get("tp1"))
    p["raw_tp2"] = p.get("raw_tp2", p.get("tp2"))
    p["raw_tp3"] = p.get("raw_tp3", p.get("tp3"))

    p["tp1"] = tp
    p["tp2"] = tp
    p["tp3"] = tp

    p["rr"] = rr
    p["rr_tp1"] = rr
    p["rr_tp2"] = rr
    p["rr_tp3"] = rr

    p["live_rr12_plan_locked"] = True
    p["live_rr12_plan_lock_reason"] = "single_full_tp_1p2r_from_structural_sl"
    p["tp_plan_mode"] = "SINGLE_FULL_1P2R"
    p["sl_plan_mode"] = "SMC_STRUCTURAL_VALIDATED"

    p["_live_rr12_plan_lock_result"] = result("ALLOW", "rr12_plan_locked", p, {
        "entry": entry,
        "sl": sl,
        "risk": risk,
        "rr": rr,
        "tp": tp,
        "tp_plan_mode": "SINGLE_FULL_1P2R",
    })

    return {
        "ok": True,
        "decision": "ALLOW",
        "allow": True,
        "payload": p,
        "reason": "rr12_plan_locked",
        "entry": entry,
        "sl": sl,
        "risk": risk,
        "rr": rr,
        "tp": tp,
    }
