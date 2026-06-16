#!/usr/bin/env python3
import json, sys, math
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
from statistics import mean

WIB = timezone(timedelta(hours=7))
UTC = timezone.utc

HOURS = int(sys.argv[1]) if len(sys.argv) > 1 else 24

LOG = Path("logs/freqai_feature_store_v1.jsonl")
LATEST = Path("state/features/latest_freqai_features_v1.json")

FEATURE_FIELDS = [
    "latest_close",
    "ret_1m_5",
    "ret_5m_1",
    "ret_5m_3",
    "ret_5m_12",
    "ret_15m_1",
    "ret_15m_4",
    "ret_15m_16",
    "ret_4h_1",
    "ret_4h_6",
    "atr_pct_5m_14",
    "atr_pct_15m_14",
    "volume_z_5m_20",
    "volume_z_15m_20",
    "candle_range_pct_5m",
    "candle_body_ratio_5m",
    "upper_wick_ratio_5m",
    "lower_wick_ratio_5m",
    "close_pos_ratio_5m",
    "btc_residual_ret_15m_4",
    "eth_residual_ret_15m_4",
    "cross_rank_ret_15m_4",
    "cross_rank_atr_5m_14",
    "cross_rank_volume_z_5m_20",
]

def parse_dt(v):
    if not v:
        return None
    s = str(v).replace("T", " ").replace("Z", "").replace(" WIB", "")
    if "+" in s:
        s = s.split("+")[0]
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(s[:26], fmt)
            if "T" in str(v) or "+00:00" in str(v) or str(v).endswith("Z"):
                return dt.replace(tzinfo=UTC).astimezone(WIB)
            return dt.replace(tzinfo=WIB)
        except Exception:
            pass
    return None

def is_num(x):
    try:
        v = float(x)
        return math.isfinite(v)
    except Exception:
        return False

def read_jsonl(path):
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(errors="ignore").splitlines():
        try:
            rows.append(json.loads(line))
        except Exception:
            pass
    return rows

now = datetime.now(WIB)
cutoff = now - timedelta(hours=HOURS)

rows = []
for r in read_jsonl(LOG):
    dt = parse_dt(r.get("created_at_utc")) or parse_dt(r.get("created_at_wib"))
    if dt and dt >= cutoff:
        rows.append(r)

print(f"=== FEATURE STORE V1 HEALTH | last {HOURS}h ===")
print("log:", LOG)
print("rows:", len(rows))

if not rows:
    print("No rows in window.")
    raise SystemExit(0)

by_run = Counter(r.get("created_at_utc") or r.get("created_at_wib") for r in rows)
by_symbol = Counter(str(r.get("symbol")) for r in rows)

latest_by_symbol = {}
for r in rows:
    sym = str(r.get("symbol"))
    dt = parse_dt(r.get("created_at_utc")) or parse_dt(r.get("created_at_wib"))
    if sym not in latest_by_symbol or dt > latest_by_symbol[sym][0]:
        latest_by_symbol[sym] = (dt, r)

print("")
print("--- runs ---")
print("unique_runs:", len(by_run))
print("expected_rows_per_run:", max(by_run.values()) if by_run else 0)
print("last_run:", max(by_run.keys()) if by_run else "NA")

print("")
print("--- symbols ---")
print("symbols:", len(by_symbol))
for sym, cnt in by_symbol.most_common():
    dt, r = latest_by_symbol.get(sym, (None, {}))
    age_min = (now - dt).total_seconds() / 60 if dt else None
    print(
        sym,
        "rows=", cnt,
        "age_min=", round(age_min, 2) if age_min is not None else "NA",
        "close=", r.get("latest_close"),
        "ret15m4=", r.get("ret_15m_4"),
        "atr5m=", r.get("atr_pct_5m_14"),
        "volz5m=", r.get("volume_z_5m_20"),
    )

print("")
print("--- sanity ---")
sanity_bad = [r for r in rows if r.get("feature_sanity_ok") is False]
print("sanity_bad_rows:", len(sanity_bad), "/", len(rows))
if sanity_bad:
    by_sym_bad = Counter(str(r.get("symbol")) for r in sanity_bad)
    print("sanity_bad_by_symbol:", dict(by_sym_bad))
    for r in sanity_bad[-20:]:
        print(
            r.get("created_at_wib"),
            r.get("symbol"),
            r.get("latest_close"),
            "gap=", r.get("latest_vs_15m_close_gap_pct"),
            "warnings=", r.get("feature_sanity_warnings"),
        )

print("")
print("--- null rate by feature ---")
for f in FEATURE_FIELDS:
    nulls = sum(1 for r in rows if r.get(f) is None)
    pct = nulls / len(rows) * 100
    print(f"{f}: null={nulls}/{len(rows)} ({pct:.2f}%)")

print("")
print("--- quick numeric stats ---")
for f in ["ret_15m_4", "btc_residual_ret_15m_4", "atr_pct_5m_14", "volume_z_5m_20", "cross_rank_ret_15m_4"]:
    vals = [float(r[f]) for r in rows if is_num(r.get(f))]
    if not vals:
        print(f"{f}: NA")
        continue
    print(
        f"{f}:",
        "avg=", round(mean(vals), 8),
        "min=", round(min(vals), 8),
        "max=", round(max(vals), 8),
    )
