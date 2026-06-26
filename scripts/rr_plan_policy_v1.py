#!/usr/bin/env python3
import csv, json, math, os
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import Counter

ROOT = Path(__file__).resolve().parents[1]
WIB = timezone(timedelta(hours=7))
UTC = timezone.utc

VERSION = "rr_plan_policy_v1_20260619"
MODE = os.getenv("RR_POLICY_MODE", "REPORT_ONLY")

IN_FILE = ROOT / "logs" / "execution_plans.jsonl"

OUT_JSON = ROOT / "reports" / "rr_plan_policy_v1.json"
OUT_CSV = ROOT / "reports" / "rr_plan_policy_v1.csv"
SNAP_JSONL = ROOT / "logs" / "rr_plan_policy_snapshots_v1.jsonl"
SNAP_CSV = ROOT / "logs" / "rr_plan_policy_snapshots_v1.csv"

MIN_TP1_R = float(os.getenv("RR_MIN_TP1_R", "1.0"))
MIN_TP2_R = float(os.getenv("RR_MIN_TP2_R", "1.5"))
MIN_TP3_R = float(os.getenv("RR_MIN_TP3_R", "2.0"))

def now_wib():
    return datetime.now(UTC).astimezone(WIB).strftime("%Y-%m-%d %H:%M:%S WIB")

def read_jsonl(path):
    rows = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                j = json.loads(line)
                if isinstance(j, dict):
                    rows.append(j)
            except Exception:
                pass
    return rows

def payload(r):
    p = r.get("payload")
    return p if isinstance(p, dict) else {}

def get(r, *keys):
    p = payload(r)
    for k in keys:
        if r.get(k) is not None:
            return r.get(k)
        if p.get(k) is not None:
            return p.get(k)
    return None

def num(x):
    try:
        v = float(x)
        if math.isfinite(v):
            return v
    except Exception:
        pass
    return None

def symbol_of(r):
    s = str(get(r, "symbol", "pair") or "")
    k = str(get(r, "signal_key") or "")
    if not s and "|" in k:
        s = k.split("|")[0]
    return s.replace("BINANCE:", "").replace(".P", "").replace("/", "").upper()

def direction_of(r):
    d = str(get(r, "direction", "dir", "side") or "").upper()
    k = str(get(r, "signal_key") or "")
    if not d and "|" in k:
        try:
            d = k.split("|")[1].upper()
        except Exception:
            pass
    if d == "BUY":
        d = "LONG"
    if d == "SELL":
        d = "SHORT"
    return d

def rr_for_plan(r):
    entry = num(get(r, "entry", "entry_mid", "entry_price"))
    sl = num(get(r, "sl", "stop_loss"))
    tp1 = num(get(r, "tp1", "raw_tp1"))
    tp2 = num(get(r, "tp2", "raw_tp2"))
    tp3 = num(get(r, "tp3", "raw_tp3"))
    d = direction_of(r)

    base = {
        "created_at_wib": now_wib(),
        "signal_key": get(r, "signal_key"),
        "plan_id": get(r, "plan_id"),
        "symbol": symbol_of(r),
        "direction": d,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "execution_mode": get(r, "execution_mode"),
        "execution_owner": get(r, "execution_owner"),
        "entry_type": get(r, "entry_type"),
    }

    if not entry or not sl or d not in ("LONG", "SHORT"):
        return {
            **base,
            "rr_status": "INVALID_PLAN_MISSING_FIELDS",
            "risk_pct": None,
            "rr_tp1": None,
            "rr_tp2": None,
            "rr_tp3": None,
            "rr_reasons": "missing_entry_sl_or_direction",
        }

    if d == "LONG":
        risk = entry - sl
        rr_tp1 = (tp1 - entry) / risk if tp1 and risk > 0 else None
        rr_tp2 = (tp2 - entry) / risk if tp2 and risk > 0 else None
        rr_tp3 = (tp3 - entry) / risk if tp3 and risk > 0 else None
    else:
        risk = sl - entry
        rr_tp1 = (entry - tp1) / risk if tp1 and risk > 0 else None
        rr_tp2 = (entry - tp2) / risk if tp2 and risk > 0 else None
        rr_tp3 = (entry - tp3) / risk if tp3 and risk > 0 else None

    if risk <= 0:
        return {
            **base,
            "rr_status": "INVALID_PLAN_BAD_SL",
            "risk_pct": None,
            "rr_tp1": None,
            "rr_tp2": None,
            "rr_tp3": None,
            "rr_reasons": "risk_not_positive",
        }

    reasons = []
    if rr_tp1 is None or rr_tp1 < MIN_TP1_R:
        reasons.append(f"tp1_rr_below_min:{rr_tp1}<{MIN_TP1_R}")
    if rr_tp2 is not None and rr_tp2 < MIN_TP2_R:
        reasons.append(f"tp2_rr_below_min:{rr_tp2}<{MIN_TP2_R}")
    if rr_tp3 is not None and rr_tp3 < MIN_TP3_R:
        reasons.append(f"tp3_rr_below_min:{rr_tp3}<{MIN_TP3_R}")

    status = "RR_OK" if not reasons else "RR_TOO_WEAK"

    return {
        **base,
        "risk_pct": round(abs(risk / entry) * 100, 5),
        "rr_tp1": round(rr_tp1, 4) if rr_tp1 is not None else None,
        "rr_tp2": round(rr_tp2, 4) if rr_tp2 is not None else None,
        "rr_tp3": round(rr_tp3, 4) if rr_tp3 is not None else None,
        "rr_status": status,
        "rr_reasons": "|".join(reasons),
    }

def write_jsonl_append(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False, sort_keys=True) + "\n")

def main():
    rows = read_jsonl(IN_FILE)
    plans = [rr_for_plan(r) for r in rows]

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)

    cols = [
        "created_at_wib", "signal_key", "plan_id", "symbol", "direction",
        "entry", "sl", "tp1", "tp2", "tp3",
        "risk_pct", "rr_tp1", "rr_tp2", "rr_tp3",
        "rr_status", "rr_reasons",
        "execution_mode", "execution_owner", "entry_type",
    ]

    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for p in plans:
            w.writerow({c: p.get(c) for c in cols})

    status_counts = Counter(p.get("rr_status") for p in plans)
    symbol_bad = Counter(p.get("symbol") for p in plans if p.get("rr_status") == "RR_TOO_WEAK")

    report = {
        "ok": True,
        "version": VERSION,
        "mode": MODE,
        "created_at_wib": now_wib(),
        "input_file": str(IN_FILE),
        "thresholds": {
            "min_tp1_r": MIN_TP1_R,
            "min_tp2_r": MIN_TP2_R,
            "min_tp3_r": MIN_TP3_R,
        },
        "total_plans": len(plans),
        "status_counts": dict(status_counts),
        "rr_too_weak_by_symbol": dict(symbol_bad.most_common()),
        "last_30": plans[-30:],
    }

    OUT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    snap = {
        "created_at_wib": report["created_at_wib"],
        "version": VERSION,
        "mode": MODE,
        "total_plans": len(plans),
        "status_counts": dict(status_counts),
    }

    write_jsonl_append(SNAP_JSONL, snap)

    exists = SNAP_CSV.exists()
    with SNAP_CSV.open("a", newline="", encoding="utf-8") as f:
        c = ["created_at_wib", "version", "mode", "total_plans", "status_counts"]
        w = csv.DictWriter(f, fieldnames=c)
        if not exists:
            w.writeheader()
        w.writerow({
            "created_at_wib": snap["created_at_wib"],
            "version": VERSION,
            "mode": MODE,
            "total_plans": snap["total_plans"],
            "status_counts": json.dumps(snap["status_counts"], sort_keys=True),
        })

    print(f"=== RR PLAN POLICY V1 | {MODE} ===")
    print("out_json:", OUT_JSON)
    print("out_csv :", OUT_CSV)
    print("total_plans:", len(plans))
    print("status_counts:", dict(status_counts))
    print("rr_too_weak_by_symbol:", dict(symbol_bad.most_common(20)))
    print("")
    print("=== last 10 ===")
    for p in plans[-10:]:
        print({
            "symbol": p.get("symbol"),
            "direction": p.get("direction"),
            "rr_status": p.get("rr_status"),
            "risk_pct": p.get("risk_pct"),
            "rr_tp1": p.get("rr_tp1"),
            "rr_tp2": p.get("rr_tp2"),
            "rr_tp3": p.get("rr_tp3"),
            "reason": p.get("rr_reasons"),
        })

if __name__ == "__main__":
    main()
