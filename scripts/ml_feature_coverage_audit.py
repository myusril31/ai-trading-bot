#!/usr/bin/env python3
import json
from pathlib import Path
from collections import defaultdict

path = Path("logs/ml_dataset_rows.jsonl")

rows = []
for line in path.read_text(errors="ignore").splitlines():
    try:
        rows.append(json.loads(line))
    except Exception:
        pass

latest = {}
for r in rows:
    k = r.get("signal_key")
    if k:
        latest[k] = r

rows = list(latest.values())

FEATURES = [
    "symbol", "direction", "score", "score_total", "priority_score", "smc_score",
    "rr", "rr_tp1", "rr_to_tp1", "rrTp2", "rr_min",
    "entryDistPct", "entry_dist_pct", "entry_dist_from_price_pct", "distToZonePct",
    "sl_dist_pct", "tp1_dist_pct",
    "htfLoc", "htf_location", "htfBias", "htf_bias",
    "liqCtx", "liq_ctx",
    "sweep_type", "sweep_age_bars", "sweep_depth_pct",
    "reclaim_age_bars", "reclaim_strength",
    "fvg_age_bars", "fvg_size_pct", "fvg_size_atr", "fvg_fill_pct",
    "atr_pct_15m", "atr_pct_4h", "volume_zscore",
    "pre_entry_rr_to_tp1", "pre_entry_adverse_pct",
    "patch_phase",
]

def pick(r, key):
    objs = [r]
    for nested in ("payload", "plan", "meta", "htf", "liq"):
        if isinstance(r.get(nested), dict):
            objs.append(r[nested])
    for o in objs:
        if isinstance(o, dict) and key in o:
            return o.get(key)
    return None

print("=== ML FEATURE COVERAGE AUDIT ===")
print("unique_rows=", len(rows))
print("")

for k in FEATURES:
    total = len(rows)
    present = 0
    valid = 0
    for r in rows:
        v = pick(r, k)
        if v is not None:
            present += 1
        if v not in (None, "", "NA", "UNK", -1, -1.0):
            valid += 1
    print(f"{k:32s} present={present:4d}/{total} valid={valid:4d}/{total} valid_pct={(valid/total*100 if total else 0):6.2f}%")
