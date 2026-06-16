#!/usr/bin/env python3
import json, math
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timezone, timedelta

ROOT = Path(__file__).resolve().parents[1]
WIB = timezone(timedelta(hours=7))
UTC = timezone.utc

JOIN_FILE = ROOT / "logs" / "ml_dataset_v3_feature_join.jsonl"
FEATURE_FILE = ROOT / "logs" / "freqai_feature_store_v1.jsonl"
SHADOW_SIGNAL_FILE = ROOT / "logs" / "vps_smc_shadow_signals.jsonl"

OUT_JSON = ROOT / "reports" / "lookahead_recursive_audit_v1.json"

MAX_FEATURE_AGE_SEC = 20 * 60
CANDLE_FUTURE_TOLERANCE_SEC = 90

CRITICAL_PLAN_FIELDS = [
    "direction",
    "state",
    "entry_lo",
    "entry_hi",
    "entry_mid",
    "sl",
    "tp1",
    "tp2",
    "tp3",
    "invalid",
    "fvg_lo",
    "fvg_hi",
    "fvg_type",
    "rr_tp2",
    "plan_sanity_ok",
    "plan_invalid",
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

def parse_dt(v, default_tz=WIB):
    if not v:
        return None
    s = str(v).replace("T", " ").replace("Z", "").replace(" WIB", "")
    if "+" in s:
        s = s.split("+")[0]
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s[:26], fmt).replace(tzinfo=default_tz)
        except Exception:
            pass
    return None

def parse_utc(v):
    return parse_dt(v, UTC)

def key_of(r):
    return str(r.get("signal_key") or r.get("signal_id") or "")

def num(x):
    try:
        v = float(x)
        if math.isfinite(v):
            return v
    except Exception:
        pass
    return None

def comparable(v):
    n = num(v)
    if n is not None:
        return round(n, 10)
    if isinstance(v, bool):
        return v
    if v is None:
        return None
    return str(v)

def diff_critical(a, b):
    diffs = []
    for f in CRITICAL_PLAN_FIELDS:
        av = comparable(a.get(f))
        bv = comparable(b.get(f))
        if av != bv:
            diffs.append({"field": f, "from": av, "to": bv})
    return diffs

def main():
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)

    join_rows = read_jsonl(JOIN_FILE)
    feature_rows = read_jsonl(FEATURE_FILE)
    signal_rows = read_jsonl(SHADOW_SIGNAL_FILE)

    join_future_feature = []
    join_stale_feature = []
    join_bad_time = []

    for r in join_rows:
        sig_t = parse_dt(r.get("signal_time_wib"), WIB)
        feat_t = parse_dt(r.get("feature_time_wib"), WIB)
        age = num(r.get("feature_age_sec"))

        if not sig_t or not feat_t:
            join_bad_time.append({
                "signal_key": r.get("signal_key"),
                "symbol": r.get("symbol"),
                "reason": "missing_signal_or_feature_time",
            })
            continue

        if feat_t > sig_t:
            join_future_feature.append({
                "signal_key": r.get("signal_key"),
                "symbol": r.get("symbol"),
                "signal_time_wib": r.get("signal_time_wib"),
                "feature_time_wib": r.get("feature_time_wib"),
                "age_sec": age,
            })

        if age is not None and (age < -1 or age > MAX_FEATURE_AGE_SEC):
            join_stale_feature.append({
                "signal_key": r.get("signal_key"),
                "symbol": r.get("symbol"),
                "signal_time_wib": r.get("signal_time_wib"),
                "feature_time_wib": r.get("feature_time_wib"),
                "age_sec": age,
            })

    feature_future_candle = []
    feature_missing_time = 0

    for r in feature_rows:
        created = parse_utc(r.get("created_at_utc"))
        latest_ms = num(r.get("latest_close_time_ms"))

        if not created or latest_ms is None:
            feature_missing_time += 1
            continue

        created_ms = created.timestamp() * 1000.0
        if latest_ms > created_ms + (CANDLE_FUTURE_TOLERANCE_SEC * 1000):
            feature_future_candle.append({
                "symbol": r.get("symbol"),
                "created_at_utc": r.get("created_at_utc"),
                "latest_close_time_ms": latest_ms,
                "future_by_sec": round((latest_ms - created_ms) / 1000.0, 3),
            })

    signals_by_key = defaultdict(list)
    for r in signal_rows:
        k = key_of(r)
        if k:
            signals_by_key[k].append(r)

    duplicate_keys = {k: v for k, v in signals_by_key.items() if len(v) > 1}

    recursive_plan_drift = []
    for k, arr in duplicate_keys.items():
        # Keep original append order. We care if same signal_key later gets a different plan.
        first = arr[0]
        for idx, r in enumerate(arr[1:], start=1):
            diffs = diff_critical(first, r)
            if diffs:
                recursive_plan_drift.append({
                    "signal_key": k,
                    "symbol": first.get("symbol"),
                    "duplicates": len(arr),
                    "compare_index": idx,
                    "diffs": diffs[:10],
                })
                break

    fatal_count = len(join_future_feature) + len(feature_future_candle)
    warn_count = len(join_stale_feature) + len(join_bad_time) + len(recursive_plan_drift)

    if fatal_count > 0:
        status = "FAIL"
    elif warn_count > 0:
        status = "WARN"
    else:
        status = "OK"

    report = {
        "ok": status != "FAIL",
        "status": status,
        "audit_version": "lookahead_recursive_audit_v1_20260614",
        "created_at_wib": datetime.now(UTC).astimezone(WIB).strftime("%Y-%m-%d %H:%M:%S WIB"),

        "join_rows": len(join_rows),
        "feature_rows": len(feature_rows),
        "shadow_signal_rows": len(signal_rows),

        "join_future_feature_count": len(join_future_feature),
        "join_stale_feature_count": len(join_stale_feature),
        "join_bad_time_count": len(join_bad_time),

        "feature_future_candle_count": len(feature_future_candle),
        "feature_missing_time_count": feature_missing_time,

        "duplicate_signal_key_count": len(duplicate_keys),
        "recursive_plan_drift_count": len(recursive_plan_drift),

        "samples": {
            "join_future_feature": join_future_feature[:20],
            "join_stale_feature": join_stale_feature[:20],
            "join_bad_time": join_bad_time[:20],
            "feature_future_candle": feature_future_candle[:20],
            "recursive_plan_drift": recursive_plan_drift[:20],
        },

        "notes": [
            "FAIL means direct future feature/candle leakage detected.",
            "WARN on recursive_plan_drift means same signal_key emitted different critical plan fields across time.",
            "This is report-only. It does not modify engine, allowlist, ML, or execution.",
        ],
    }

    OUT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(json.dumps(report, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
