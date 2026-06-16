import json, csv, math
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import defaultdict, Counter

ROOT = Path(".")
OUTCOMES = ROOT / "logs" / "forward_outcomes.jsonl"
ML = ROOT / "logs" / "ml_dataset_rows.jsonl"
REPORTS = ROOT / "reports"
REPORTS.mkdir(exist_ok=True)

WIB = timezone(timedelta(hours=7))

TIME_FIELDS = [
    "hit_time_wib", "hit_at_wib", "outcome_time_wib", "resolved_at_wib",
    "resolved_time_wib", "label_time_wib", "first_touch_time_wib",
    "target_hit_time_wib", "close_time_wib", "end_time_wib",
]

BAR_FIELDS = [
    "bars_to_outcome", "bars_to_hit", "label_bars", "bars_elapsed",
    "bars_until_hit", "forward_bars_to_hit",
]

def parse_dt(x):
    if not x:
        return None
    s = str(x).strip()
    s = s.replace(" WIB", "")
    s = s.replace("Z", "+00:00")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z"):
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=WIB)
            return dt
        except Exception:
            pass
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=WIB)
        return dt
    except Exception:
        return None

def load_latest_by_key(path):
    latest = {}
    if not path.exists():
        return latest
    with path.open(errors="ignore") as f:
        for line in f:
            try:
                r = json.loads(line)
            except Exception:
                continue
            k = r.get("signal_key")
            if k:
                latest[k] = r
    return latest

def percentile(vals, q):
    vals = sorted([float(v) for v in vals if v is not None])
    if not vals:
        return None
    if len(vals) == 1:
        return vals[0]
    pos = (len(vals)-1) * q
    lo = int(math.floor(pos))
    hi = min(lo+1, len(vals)-1)
    frac = pos - lo
    return vals[lo]*(1-frac) + vals[hi]*frac

def avg(vals):
    vals = [float(v) for v in vals if v is not None]
    return sum(vals)/len(vals) if vals else None

outcomes = load_latest_by_key(OUTCOMES)
ml = load_latest_by_key(ML)

rows = []
source_counter = Counter()
missing_time = []

for key, out in outcomes.items():
    target = out.get("label_target") or out.get("target") or out.get("outcome_target")
    status = out.get("outcome_status")
    if target not in ("TP1", "TP2", "TP3", "SL"):
        continue

    d = ml.get(key, {})
    sig_time_raw = (
        d.get("signal_time_wib")
        or out.get("signal_time_wib")
        or d.get("event_at_wib")
        or out.get("event_at_wib")
    )
    sig_dt = parse_dt(sig_time_raw)

    hit_dt = None
    source = None

    for f in TIME_FIELDS:
        if out.get(f):
            hit_dt = parse_dt(out.get(f))
            source = f
            break

    minutes = None
    if sig_dt and hit_dt:
        minutes = (hit_dt - sig_dt).total_seconds() / 60.0
    else:
        for f in BAR_FIELDS:
            if out.get(f) is not None:
                try:
                    bars = float(out.get(f))
                    tf_min = float(out.get("timeframe_min") or out.get("tf_min") or 15)
                    minutes = bars * tf_min
                    source = f + "_x_tf"
                    break
                except Exception:
                    pass

    if minutes is None:
        missing_time.append({"signal_key": key, "outcome_keys": sorted(out.keys())})
        continue

    source_counter[source] += 1

    rows.append({
        "signal_key": key,
        "signal_time_wib": sig_time_raw or "",
        "symbol": d.get("symbol") or out.get("symbol") or "",
        "direction": d.get("direction") or out.get("direction") or "",
        "execution_decision": d.get("execution_decision") or "",
        "reject_gate": d.get("reject_gate") or "",
        "outcome_status": status or "",
        "label_target": target,
        "label_R": out.get("label_R") or "",
        "minutes_to_outcome": round(minutes, 3),
        "time_source": source or "",
    })

def group_summary(group_name, grouped_rows):
    vals = [r["minutes_to_outcome"] for r in grouped_rows]
    return {
        "group": group_name,
        "n": len(grouped_rows),
        "avg_min": round(avg(vals), 2) if avg(vals) is not None else None,
        "p50_min": round(percentile(vals, 0.50), 2) if percentile(vals, 0.50) is not None else None,
        "p75_min": round(percentile(vals, 0.75), 2) if percentile(vals, 0.75) is not None else None,
        "p80_min": round(percentile(vals, 0.80), 2) if percentile(vals, 0.80) is not None else None,
        "p90_min": round(percentile(vals, 0.90), 2) if percentile(vals, 0.90) is not None else None,
        "max_min": round(max(vals), 2) if vals else None,
    }

by_target = defaultdict(list)
by_symbol = defaultdict(list)
by_exec_target = defaultdict(list)

for r in rows:
    by_target[r["label_target"]].append(r)
    by_symbol[r["symbol"]].append(r)
    by_exec_target[(r["execution_decision"], r["label_target"])].append(r)

summary_target = [group_summary(k, v) for k, v in sorted(by_target.items())]
summary_symbol = [group_summary(k, v) for k, v in sorted(by_symbol.items())]
summary_exec_target = [
    group_summary(f"{k[0]}_{k[1]}", v)
    for k, v in sorted(by_exec_target.items())
]

tp1_vals = [r["minutes_to_outcome"] for r in rows if r["label_target"] == "TP1"]
sl_vals = [r["minutes_to_outcome"] for r in rows if r["label_target"] == "SL"]
all_vals = [r["minutes_to_outcome"] for r in rows]

# TTL suggestion untuk pending limit:
# pakai p75/p80 TP1 sebagai window utama, floor 15m, cap 90m.
tp1_p75 = percentile(tp1_vals, 0.75)
tp1_p80 = percentile(tp1_vals, 0.80)
all_p75 = percentile(all_vals, 0.75)

raw_ttl = tp1_p75 or all_p75 or 30
ttl_min = int(max(15, min(90, math.ceil(raw_ttl / 5) * 5)))

recommendation = {
    "limit_order_ttl_min_suggested": ttl_min,
    "cancel_pending_if_tp_or_sl_touched_before_fill": True,
    "tp1_p75_min": round(tp1_p75, 2) if tp1_p75 is not None else None,
    "tp1_p80_min": round(tp1_p80, 2) if tp1_p80 is not None else None,
    "sl_p50_min": round(percentile(sl_vals, 0.50), 2) if sl_vals else None,
    "all_p75_min": round(all_p75, 2) if all_p75 is not None else None,
}

with (REPORTS / "outcome_time_rows.csv").open("w", newline="") as f:
    if rows:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

with (REPORTS / "outcome_time_by_target.csv").open("w", newline="") as f:
    if summary_target:
        w = csv.DictWriter(f, fieldnames=list(summary_target[0].keys()))
        w.writeheader()
        w.writerows(summary_target)

with (REPORTS / "outcome_time_by_symbol.csv").open("w", newline="") as f:
    if summary_symbol:
        w = csv.DictWriter(f, fieldnames=list(summary_symbol[0].keys()))
        w.writeheader()
        w.writerows(summary_symbol)

summary = {
    "ok": True,
    "rows": len(rows),
    "missing_time_rows": len(missing_time),
    "time_source_counts": dict(source_counter),
    "recommendation": recommendation,
    "by_target": summary_target,
    "by_execution_target": summary_exec_target,
    "files": {
        "rows": "reports/outcome_time_rows.csv",
        "by_target": "reports/outcome_time_by_target.csv",
        "by_symbol": "reports/outcome_time_by_symbol.csv",
    }
}
(REPORTS / "outcome_time_summary.json").write_text(json.dumps(summary, indent=2))
print(json.dumps(summary, indent=2))
