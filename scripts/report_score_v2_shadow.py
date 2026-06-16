#!/usr/bin/env python3
import json, sys, math, re
from pathlib import Path
from collections import Counter, defaultdict
from statistics import mean, median
from datetime import datetime, timezone, timedelta

WIB = timezone(timedelta(hours=7))
UTC = timezone.utc

DATE = sys.argv[1] if len(sys.argv) > 1 else None

LOG_FILES = [
    Path("logs/signals.jsonl"),
    Path("logs/decisions.jsonl"),
    Path("logs/vps_smc_shadow_signals.jsonl"),  # VPS_SMC_SHADOW_LOG_SUPPORT_20260614
    Path("logs/ml_dataset_rows.jsonl"),
]

OUTCOME_FILE = Path("logs/forward_outcomes.jsonl")

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

def key_of(r):
    return str(r.get("signal_key") or r.get("signal_id") or "")

def symbol_of(r):
    k = key_of(r)
    s = str(r.get("symbol") or r.get("pair") or "")
    if not s and k:
        s = k.split("|")[0]
    return s.replace("BINANCE:", "").replace(".P", "")

def direction_of(r):
    k = key_of(r)
    d = str(r.get("direction") or r.get("dir") or "")
    if not d and "|" in k:
        try:
            d = k.split("|")[1]
        except Exception:
            pass
    return d.upper()

def parse_dt(v, is_utc=False):
    if not v:
        return None
    s = str(v).replace("T", " ").replace("Z", "").replace(" WIB", "")
    if "+" in s:
        s = s.split("+")[0]
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(s[:26], fmt)
            if is_utc:
                return dt.replace(tzinfo=UTC).astimezone(WIB)
            return dt.replace(tzinfo=WIB)
        except Exception:
            pass
    return None

def dt_from_key(k):
    try:
        ms = int(str(k).split("|")[-1])
        return datetime.fromtimestamp(ms / 1000, UTC).astimezone(WIB)
    except Exception:
        return None

def row_time(r):
    for k in ("created_at_wib","event_at_wib","decision_at_wib","signal_time_wib","received_at_wib"):
        dt = parse_dt(r.get(k), False)
        if dt:
            return dt
    for k in ("created_at_utc","event_at_utc","decision_at_utc","logged_at_utc","received_at_utc"):
        dt = parse_dt(r.get(k), True)
        if dt:
            return dt
    return dt_from_key(key_of(r))

def num(x):
    try:
        v = float(x)
        if math.isfinite(v):
            return v
    except Exception:
        pass
    return None

def get_sd(r):
    sd = r.get("score_detail")
    if isinstance(sd, dict):
        return sd

    # VPS shadow log stores score_v2 inside notes, because apparently structured data was too emotionally mature.
    notes = str(r.get("notes") or "")
    m = re.search(r"score_v2_shadow=([0-9]+(?:\.[0-9]+)?)", notes)
    if m:
        try:
            score_v2 = float(m.group(1))
        except Exception:
            score_v2 = None
        return {
            "score_version": "shadow_notes_vps_smc_20260614",
            "score_v1": r.get("score"),
            "score_v2": score_v2,
            "active_score": r.get("score"),
            "priority": r.get("priority"),
            "components": {},
            "reasons_v2": [notes],
        }

    if r.get("score_v2") is not None:
        return r

    return {}

def bucket(v):
    if v is None:
        return "NA"
    v = float(v)
    if v < 50:
        return "00-49"
    if v < 60:
        return "50-59"
    if v < 70:
        return "60-69"
    if v < 80:
        return "70-79"
    if v < 90:
        return "80-89"
    return "90-100"

def latest_by_key(rows):
    d = {}
    tmap = {}
    for r in rows:
        k = key_of(r)
        if not k:
            continue
        t = row_time(r) or datetime.min.replace(tzinfo=WIB)
        if k not in d or t >= tmap[k]:
            d[k] = r
            tmap[k] = t
    return d

rows = []
for path in LOG_FILES:
    for r in read_jsonl(path):
        dt = row_time(r)
        if DATE and (not dt or dt.strftime("%Y-%m-%d") != DATE):
            continue
        sd = get_sd(r)
        if sd.get("score_v2") is not None:
            r["_source_file"] = str(path)
            rows.append(r)

latest = latest_by_key(rows)

outcomes = latest_by_key(read_jsonl(OUTCOME_FILE))

records = []
for k, r in latest.items():
    sd = get_sd(r)
    o = outcomes.get(k)
    rec = {
        "signal_key": k,
        "source": r.get("_source_file"),
        "date": row_time(r).strftime("%Y-%m-%d") if row_time(r) else "",
        "symbol": symbol_of(r),
        "direction": direction_of(r),
        "score_v1": num(sd.get("score_v1")),
        "score_v2": num(sd.get("score_v2")),
        "active_score": num(sd.get("active_score")),
        "priority": sd.get("priority") or r.get("priority"),
        "components": sd.get("components") if isinstance(sd.get("components"), dict) else {},
        "label_win": num(o.get("label_win")) if o else None,
        "label_R": num(o.get("label_R")) if o else None,
        "label_target": o.get("label_target") if o else None,
    }
    records.append(rec)

scores_v2 = [r["score_v2"] for r in records if r["score_v2"] is not None]
scores_v1 = [r["score_v1"] for r in records if r["score_v1"] is not None]

print("=== SCORE V2 SHADOW AUDIT ===")
print("date_filter:", DATE or "ALL")
print("rows_unique:", len(records))

if not records:
    print("No score_v2 rows yet. Normal kalau belum ada signal baru setelah patch.")
    raise SystemExit(0)

print("")
print("--- headline ---")
print("score_v1 avg/min/max:", round(mean(scores_v1),2) if scores_v1 else "NA", min(scores_v1) if scores_v1 else "NA", max(scores_v1) if scores_v1 else "NA")
print("score_v2 avg/min/max:", round(mean(scores_v2),2), min(scores_v2), max(scores_v2))
print("score_v2 median:", round(median(scores_v2),2))
print("score_v2 buckets:", dict(Counter(bucket(v) for v in scores_v2)))

print("")
print("--- by symbol ---")
by_sym = defaultdict(list)
for r in records:
    by_sym[r["symbol"]].append(r)

for sym, arr in sorted(by_sym.items(), key=lambda x: len(x[1]), reverse=True):
    vals = [x["score_v2"] for x in arr if x["score_v2"] is not None]
    print(sym, "n=", len(arr), "avg_v2=", round(mean(vals),2), "min=", min(vals), "max=", max(vals))

print("")
print("--- by bucket + outcome if available ---")
by_bucket = defaultdict(list)
for r in records:
    by_bucket[bucket(r["score_v2"])].append(r)

for b in sorted(by_bucket.keys()):
    arr = by_bucket[b]
    wins = [x["label_win"] for x in arr if x["label_win"] is not None]
    rs = [x["label_R"] for x in arr if x["label_R"] is not None]
    wr = (sum(wins) / len(wins) * 100) if wins else None
    exp = mean(rs) if rs else None
    print(
        b,
        "n=", len(arr),
        "outcomes=", len(wins),
        "wr=", f"{wr:.2f}%" if wr is not None else "NA",
        "expR=", round(exp,4) if exp is not None else "NA"
    )

print("")
print("--- latest 30 ---")
for r in records[-30:]:
    print(
        r["date"],
        r["symbol"],
        r["direction"],
        "v1=", r["score_v1"],
        "v2=", r["score_v2"],
        "active=", r["active_score"],
        "prio=", r["priority"],
        "outcome=", r["label_target"],
        "R=", r["label_R"],
        "key=", r["signal_key"],
    )
