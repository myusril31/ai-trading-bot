#!/usr/bin/env python3
import json, re
from pathlib import Path
from collections import defaultdict, Counter
from datetime import datetime, timezone, timedelta

WIB = timezone(timedelta(hours=7))

FILES = {
    "signals": Path("logs/signals.jsonl"),
    "decisions": Path("logs/decisions.jsonl"),
    "ml_dataset": Path("logs/ml_dataset_rows.jsonl"),
    "ml_predictions": Path("logs/ml_predictions.jsonl"),
    "execution_events": Path("logs/execution_events.jsonl"),
    "forward_outcomes": Path("logs/forward_outcomes.jsonl"),
}

TIME_KEYS = [
    "received_at_wib",
    "decision_at_wib",
    "event_at_wib",
    "created_at_wib",
    "signal_time_wib",
    "confirmed_ts_wib",
    "created_at_utc",
    "decision_at_utc",
    "event_at_utc",
]

def read_jsonl(path):
    if not path.exists():
        return []
    out = []
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
            return datetime.strptime(s[:26], fmt).replace(tzinfo=WIB)
        except Exception:
            pass
    return None

def key(r):
    return str(r.get("signal_key") or r.get("signal_id") or "")

def time_from_signal_key(k):
    # last pipe field often bucket_ms
    try:
        ms = int(str(k).split("|")[-1])
        return datetime.fromtimestamp(ms / 1000, timezone.utc).astimezone(WIB)
    except Exception:
        return None

def row_time(r):
    for k in TIME_KEYS:
        dt = parse_time(r.get(k))
        if dt:
            return dt
    return time_from_signal_key(key(r))

def norm_symbol(r):
    return str(r.get("symbol") or r.get("pair") or "").replace("BINANCE:", "").replace(".P", "")

def unique_count(rows):
    seen = set()
    for r in rows:
        k = key(r)
        if k:
            seen.add(k)
    return len(seen)

daily = defaultdict(lambda: {
    "signals": [],
    "decisions": [],
    "ml_dataset": [],
    "ml_predictions": [],
    "execution_events": [],
    "forward_outcomes": [],
})

for name, path in FILES.items():
    for r in read_jsonl(path):
        dt = row_time(r)
        if not dt:
            continue
        d = dt.strftime("%Y-%m-%d")
        daily[d][name].append(r)

print("=== QUANT SIGNAL CALENDAR ALL LOG DATES ===")
print("date         raw_sig dec accept backup reject ml_rows ml_pred exec live_order outcomes top_symbols")
print("-" * 130)

for d in sorted(daily.keys()):
    sig = daily[d]["signals"]
    dec = daily[d]["decisions"]
    mlr = daily[d]["ml_dataset"]
    mlp = daily[d]["ml_predictions"]
    exe = daily[d]["execution_events"]
    out = daily[d]["forward_outcomes"]

    dec_counter = Counter(str(r.get("decision")) for r in dec)
    exe_counter = Counter(str(r.get("decision") or r.get("action")) for r in exe)
    sym_counter = Counter(norm_symbol(r) for r in sig if norm_symbol(r))

    top_symbols = ",".join([f"{k}:{v}" for k, v in sym_counter.most_common(5)])

    print(
        f"{d}  "
        f"{unique_count(sig):7d} "
        f"{unique_count(dec):3d} "
        f"{dec_counter.get('ACCEPT',0):6d} "
        f"{dec_counter.get('BACKUP_ONLY',0):6d} "
        f"{dec_counter.get('REJECT',0):6d} "
        f"{unique_count(mlr):7d} "
        f"{unique_count(mlp):7d} "
        f"{unique_count(exe):4d} "
        f"{exe_counter.get('LIVE_ORDER_PLACED',0):10d} "
        f"{unique_count(out):8d} "
        f"{top_symbols}"
    )
