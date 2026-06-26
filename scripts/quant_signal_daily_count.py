#!/usr/bin/env python3
import json, sys
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta

WIB = timezone(timedelta(hours=7))

DATE = sys.argv[1] if len(sys.argv) > 1 else "2026-05-01"

FILES = {
    "signals": Path("logs/signals.jsonl"),
    "decisions": Path("logs/decisions.jsonl"),
    "ml_dataset": Path("logs/ml_dataset_rows.jsonl"),
    "ml_predictions": Path("logs/ml_predictions.jsonl"),
    "execution_events": Path("logs/execution_events.jsonl"),
    "forward_outcomes": Path("logs/forward_outcomes.jsonl"),
}

TIME_KEYS = [
    "signal_time_wib",
    "confirmed_ts_wib",
    "decision_at_wib",
    "event_at_wib",
    "created_at_wib",
    "received_at_wib",
    "created_at_utc",
    "decision_at_utc",
    "event_at_utc",
]

def read_jsonl(path):
    out = []
    if not path.exists():
        return out
    for line in path.read_text(errors="ignore").splitlines():
        try:
            out.append(json.loads(line))
        except Exception:
            pass
    return out

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
    # Prefer event/decision receive time for counting actual system events,
    # fallback to signal_time if no log time exists.
    preferred = [
        "received_at_wib", "decision_at_wib", "event_at_wib",
        "created_at_wib", "created_at_utc", "decision_at_utc", "event_at_utc",
        "signal_time_wib", "confirmed_ts_wib",
    ]
    for k in preferred:
        dt = parse_time(r.get(k))
        if dt:
            return dt
    return None

def signal_key(r):
    return str(r.get("signal_key") or r.get("signal_id") or "")

def symbol_of(r):
    return str(r.get("symbol") or r.get("pair") or "").replace("BINANCE:", "").replace(".P", "")

def rows_on_date(path):
    rows = []
    for r in read_jsonl(path):
        dt = row_time(r)
        if dt and dt.strftime("%Y-%m-%d") == DATE:
            rows.append(r)
    return rows

def unique_by_key(rows):
    d = {}
    for r in rows:
        k = signal_key(r)
        if k:
            d[k] = r
    return list(d.values())

def print_counter(title, c):
    print(f"\n{title}")
    if not c:
        print("- none")
        return
    for k, v in c.most_common():
        print(f"- {k}: {v}")

print(f"=== QUANT SIGNAL DAILY COUNT | {DATE} WIB ===")

signals = rows_on_date(FILES["signals"])
decisions = rows_on_date(FILES["decisions"])
ml_rows = rows_on_date(FILES["ml_dataset"])
ml_preds = rows_on_date(FILES["ml_predictions"])
execs = rows_on_date(FILES["execution_events"])
outcomes = rows_on_date(FILES["forward_outcomes"])

signals_u = unique_by_key(signals)
decisions_u = unique_by_key(decisions)
ml_rows_u = unique_by_key(ml_rows)
ml_preds_u = unique_by_key(ml_preds)
execs_u = unique_by_key(execs)
outcomes_u = unique_by_key(outcomes)

print("\n--- totals ---")
print("raw signals rows:", len(signals), "| unique:", len(signals_u))
print("decision rows:", len(decisions), "| unique:", len(decisions_u))
print("ml dataset rows:", len(ml_rows), "| unique:", len(ml_rows_u))
print("ml prediction rows:", len(ml_preds), "| unique:", len(ml_preds_u))
print("execution event rows:", len(execs), "| unique:", len(execs_u))
print("outcome rows:", len(outcomes), "| unique:", len(outcomes_u))

print_counter("signals by source_mode", Counter(str(r.get("source_mode")) for r in signals))
print_counter("decisions by decision", Counter(str(r.get("decision")) for r in decisions))
print_counter("decisions by reason", Counter(str(r.get("reason")) for r in decisions))
print_counter("execution by decision/action", Counter(str(r.get("decision") or r.get("action")) for r in execs))

# ML prediction schema may vary
ml_decisions = Counter()
ml_models = Counter()
for r in ml_preds:
    p = r.get("prediction") if isinstance(r.get("prediction"), dict) else r
    ml_decisions[str(p.get("decision"))] += 1
    ml_models[str(p.get("model_version"))] += 1

print_counter("ML predictions by decision", ml_decisions)
print_counter("ML predictions by model", ml_models)

print_counter("unique signals by symbol", Counter(symbol_of(r) for r in signals_u))
print_counter("unique decisions by symbol", Counter(symbol_of(r) for r in decisions_u))
print_counter("execution events by symbol", Counter(symbol_of(r) for r in execs_u))

print("\n--- latest decisions ---")
for r in decisions[-20:]:
    print(r.get("decision"), r.get("reason"), r.get("gate"), signal_key(r), r.get("score"))

print("\n--- latest executions ---")
for r in execs[-20:]:
    print(r.get("decision") or r.get("action"), r.get("reason"), symbol_of(r), signal_key(r))
