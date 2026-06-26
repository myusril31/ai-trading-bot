#!/usr/bin/env python3
import json
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import Counter

WIB = timezone(timedelta(hours=7))
HOURS = 2
cutoff = datetime.now(WIB) - timedelta(hours=HOURS)

FILES = {
    "decisions": Path("logs/decisions.jsonl"),
    "ml_predictions": Path("logs/ml_predictions.jsonl"),
    "execution_events": Path("logs/execution_events.jsonl"),
}

TIME_KEYS = [
    "decision_at_wib",
    "event_at_wib",
    "created_at_wib",
    "received_at_wib",
    "created_at_utc",
    "decision_at_utc",
    "event_at_utc",
]

def parse_time(v):
    if not v:
        return None
    s = str(v).replace(" WIB", "").replace("T", " ")
    if "+" in s:
        s = s.split("+")[0]
    if "Z" in s:
        s = s.replace("Z", "")
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(s[:26], fmt)
            return dt.replace(tzinfo=WIB)
        except Exception:
            pass
    return None

def row_time(r):
    for k in TIME_KEYS:
        dt = parse_time(r.get(k))
        if dt:
            return dt
    return None

def read_recent(path):
    out = []
    if not path.exists():
        return out
    for line in path.read_text(errors="ignore").splitlines():
        try:
            r = json.loads(line)
        except Exception:
            continue
        dt = row_time(r)
        if dt and dt >= cutoff:
            out.append(r)
    return out

print(f"=== SIGNAL FLOW LAST {HOURS}H ===")
print("cutoff_wib:", cutoff.strftime("%Y-%m-%d %H:%M:%S WIB"))

dec = read_recent(FILES["decisions"])
ml = read_recent(FILES["ml_predictions"])
exe = read_recent(FILES["execution_events"])

print("\n--- decisions ---")
print("count:", len(dec))
print("by_decision:", dict(Counter(str(r.get("decision")) for r in dec)))
print("by_reason:", dict(Counter(str(r.get("reason")) for r in dec).most_common(10)))
for r in dec[-20:]:
    print(r.get("decision"), r.get("reason"), r.get("gate"), r.get("signal_key"), r.get("score"))

print("\n--- ml_predictions ---")
print("count:", len(ml))
for r in ml[-20:]:
    pred = r.get("prediction") if isinstance(r.get("prediction"), dict) else r
    print(
        pred.get("model_version"),
        pred.get("decision"),
        pred.get("p_win") or pred.get("p_win_adj"),
        r.get("signal_key")
    )

print("\n--- execution_events ---")
print("count:", len(exe))
print("by_decision:", dict(Counter(str(r.get("decision") or r.get("action")) for r in exe)))
for r in exe[-20:]:
    print(r.get("decision") or r.get("action"), r.get("reason"), r.get("symbol"), r.get("signal_key"))
