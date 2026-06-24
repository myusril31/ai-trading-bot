#!/usr/bin/env python3
import argparse
import collections
import csv
import json
import math
from datetime import datetime, timezone, timedelta
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "logs"
REPORT_DIR = ROOT / "reports"
REPORT_DIR.mkdir(exist_ok=True)

def parse_dt(x):
    try:
        return datetime.fromisoformat(str(x).replace("Z", "+00:00")) if x else None
    except Exception:
        return None

def read_jsonl(path: Path):
    if not path.exists():
        return []
    out = []
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            try:
                out.append(json.loads(line))
            except Exception:
                pass
    return out

def fl(x):
    try:
        if x is None or str(x).strip() == "" or str(x).lower() == "nan":
            return None
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None

def pct(num, den):
    num = fl(num)
    den = fl(den)
    if num is None or den is None or den == 0:
        return None
    return abs(num / den) * 100.0

def stats(xs):
    vals = [fl(x) for x in xs]
    vals = [x for x in vals if x is not None]
    if not vals:
        return {"n": 0}
    vals_sorted = sorted(vals)
    def q(p):
        if not vals_sorted:
            return None
        idx = int(round((len(vals_sorted) - 1) * p))
        idx = max(0, min(len(vals_sorted) - 1, idx))
        return vals_sorted[idx]
    return {
        "n": len(vals),
        "min": min(vals),
        "p25": q(0.25),
        "median": median(vals),
        "p75": q(0.75),
        "p90": q(0.90),
        "max": max(vals),
        "avg": sum(vals) / len(vals),
    }

def bucket_rr(rr):
    rr = fl(rr)
    if rr is None:
        return "NA"
    rr = rr + 1e-9
    if rr < 1.2:
        return "<1.2"
    if rr < 1.5:
        return "1.2-1.5"
    if rr < 2.0:
        return "1.5-2.0"
    if rr < 3.0:
        return "2.0-3.0"
    if rr < 5.0:
        return "3.0-5.0"
    return ">=5.0_OUTLIER"

ap = argparse.ArgumentParser()
ap.add_argument("--hours", type=float, default=24)
ap.add_argument("--rr-outlier", type=float, default=5.0)
ap.add_argument("--min-core-rr", type=float, default=1.2)
args = ap.parse_args()

src = LOG_DIR / "core_smc_shadow_candidates_v1.jsonl"
rows = []
cutoff = datetime.now(timezone.utc) - timedelta(hours=args.hours)

for r in read_jsonl(src):
    dt = parse_dt(r.get("created_at_utc"))
    if dt and dt >= cutoff:
        rows.append(r)

# Latest row per candidate_id
latest_by_id = {}
for r in rows:
    cid = r.get("candidate_id")
    if cid:
        latest_by_id[cid] = r

dedup = list(latest_by_id.values())

# enrich rows
enriched = []
for r in dedup:
    entry = fl(r.get("entry_mid"))
    sl = fl(r.get("sl"))
    tp1 = fl(r.get("tp1"))
    risk = fl(r.get("risk"))
    rr = fl(r.get("rr_at_emit"))

    risk_pct = pct(risk, entry)
    sl_distance_pct = pct((entry - sl) if entry is not None and sl is not None else None, entry)
    tp_distance_pct = pct((tp1 - entry) if entry is not None and tp1 is not None else None, entry)

    row = dict(r)
    row["risk_pct"] = risk_pct
    row["sl_distance_pct"] = sl_distance_pct
    row["tp_distance_pct"] = tp_distance_pct
    row["rr_bucket"] = bucket_rr(rr)
    row["rr_outlier_flag"] = bool(rr is not None and rr >= args.rr_outlier)
    row["core_rr_policy_ok"] = bool(rr is not None and rr + 1e-9 >= args.min_core_rr)
    enriched.append(row)

valid = [r for r in enriched if r.get("state") == "CORE_CANDIDATE"]
invalid = [r for r in enriched if r.get("state") != "CORE_CANDIDATE"]

def count_bool(rows_, key):
    total = len(rows_)
    true_count = sum(1 for r in rows_ if bool(r.get(key)))
    return {
        "true": true_count,
        "false": total - true_count,
        "rate": (true_count / total if total else None),
    }

report = {
    "created_at_utc": datetime.now(timezone.utc).isoformat(),
    "lookback_hours": args.hours,
    "source": str(src),
    "raw_rows": len(rows),
    "unique_candidates": len(enriched),
    "state_counts": dict(collections.Counter(r.get("state") for r in enriched).most_common()),
    "symbol_state_counts": [
        {"symbol": s, "state": st, "count": c}
        for (s, st), c in collections.Counter((r.get("symbol"), r.get("state")) for r in enriched).most_common()
    ],
    "valid_core_candidates": len(valid),
    "invalid_core_candidates": len(invalid),
    "valid_by_symbol": dict(collections.Counter(r.get("symbol") for r in valid).most_common()),
    "valid_by_direction": dict(collections.Counter(r.get("direction") for r in valid).most_common()),
    "invalid_reason_counts": dict(collections.Counter(
        r.get("strict_invalid_reason") or r.get("strict_confirm_reason") or "unknown"
        for r in invalid
    ).most_common()),
    "strict_state_for_valid_counts": dict(collections.Counter(r.get("strict_shadow_state") for r in valid).most_common()),
    "strict_reason_for_valid_counts": dict(collections.Counter(r.get("strict_confirm_reason") for r in valid).most_common()),
    "rr_stats_valid": stats([r.get("rr_at_emit") for r in valid]),
    "risk_pct_stats_valid": stats([r.get("risk_pct") for r in valid]),
    "sl_distance_pct_stats_valid": stats([r.get("sl_distance_pct") for r in valid]),
    "tp_distance_pct_stats_valid": stats([r.get("tp_distance_pct") for r in valid]),
    "rr_bucket_counts_valid": dict(collections.Counter(r.get("rr_bucket") for r in valid).most_common()),
    "rr_outlier_count_valid": sum(1 for r in valid if r.get("rr_outlier_flag")),
    "feature_rates_valid": {
        "fvg_present": count_bool(valid, "fvg_present"),
        "displacement_present": count_bool(valid, "displacement_present"),
        "htf_hard_extreme_block": count_bool(valid, "htf_hard_extreme_block"),
    },
    "htf_bias_counts_valid": dict(collections.Counter(r.get("htf_bias") for r in valid).most_common()),
    "structure_15m_counts_valid": dict(collections.Counter(r.get("structure_15m") for r in valid).most_common()),
    "latest_valid_candidates": [
        {
            "candidate_id": r.get("candidate_id"),
            "symbol": r.get("symbol"),
            "direction": r.get("direction"),
            "rr_at_emit": r.get("rr_at_emit"),
            "rr_bucket": r.get("rr_bucket"),
            "risk_pct": r.get("risk_pct"),
            "sl_distance_pct": r.get("sl_distance_pct"),
            "tp_distance_pct": r.get("tp_distance_pct"),
            "rr_outlier_flag": r.get("rr_outlier_flag"),
            "strict_shadow_state": r.get("strict_shadow_state"),
            "strict_confirm_reason": r.get("strict_confirm_reason"),
            "fvg_present": r.get("fvg_present"),
            "displacement_present": r.get("displacement_present"),
            "htf_bias": r.get("htf_bias"),
            "structure_15m": r.get("structure_15m"),
        }
        for r in valid[-50:]
    ],
}

json_path = REPORT_DIR / "core_smc_shadow_summary_v1.json"
csv_path = REPORT_DIR / "core_smc_shadow_summary_v1.csv"
json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")

csv_fields = [
    "created_at_utc", "candidate_id", "symbol", "direction", "state",
    "core_ok", "rr_ok", "min_rr_at_emit", "target_r", "rr_target_rewrite_expected",
    "entry_mid", "sl", "tp1", "risk", "rr_at_emit", "rr_bucket",
    "risk_pct", "sl_distance_pct", "tp_distance_pct", "rr_outlier_flag",
    "has_reclaim", "fvg_present", "displacement_present", "htf_bias",
    "htf_structure", "structure_15m", "strict_shadow_state",
    "strict_confirm_reason", "strict_invalid_reason",
]
with csv_path.open("w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=csv_fields)
    w.writeheader()
    for r in enriched:
        w.writerow({k: r.get(k) for k in csv_fields})

print(json.dumps({
    "raw_rows": report["raw_rows"],
    "unique_candidates": report["unique_candidates"],
    "state_counts": report["state_counts"],
    "valid_by_symbol": report["valid_by_symbol"],
    "rr_stats_valid": report["rr_stats_valid"],
    "rr_bucket_counts_valid": report["rr_bucket_counts_valid"],
    "rr_outlier_count_valid": report["rr_outlier_count_valid"],
    "strict_state_for_valid_counts": report["strict_state_for_valid_counts"],
    "strict_reason_for_valid_counts": report["strict_reason_for_valid_counts"],
}, indent=2, ensure_ascii=False, default=str))

print(f"[ok] wrote {json_path}")
print(f"[ok] wrote {csv_path}")
