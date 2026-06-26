#!/usr/bin/env python3
import json
import csv
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime, timezone

ROOT = Path(".")
LOGS = ROOT / "logs"
REPORTS = ROOT / "reports"
REPORTS.mkdir(exist_ok=True)

OUTCOME = LOGS / "outcome_labels_v1.jsonl"
V4 = LOGS / "ml_dataset_v4_current14_candidate_join.jsonl"
V3 = LOGS / "ml_dataset_v3_feature_join.jsonl"

OUT_JSONL = LOGS / "ml_dataset_v4_outcome_join_v1.jsonl"
OUT_REPORT = REPORTS / "ml_dataset_v4_outcome_join_v1_report.json"
OUT_PAIR = REPORTS / "ml_dataset_v4_outcome_join_v1_by_pair.csv"

CURRENT14 = {
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "PAXGUSDT", "HYPEUSDT", "XRPUSDT", "ZECUSDT",
    "UNIUSDT", "ADAUSDT", "BCHUSDT", "LINKUSDT", "SUIUSDT", "LTCUSDT", "AVAXUSDT"
}

DROP_LEAK_KEYS = {
    "label_win", "label_R", "label_target", "first_hit", "outcome_status",
    "bars_to_hit", "exclude_label_reason", "future_hit", "future_result",
}

def read_jsonl(path):
    p = Path(path)
    if not p.exists():
        return
    with p.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue

def norm_symbol(x):
    return str(x or "").upper().replace("BINANCE:", "").replace(".P", "")

def norm_direction(x):
    d = str(x or "").upper()
    if d in ("BUY", "LONG"):
        return "LONG"
    if d in ("SELL", "SHORT"):
        return "SHORT"
    return d

def norm_key(k):
    if not k:
        return ""
    parts = str(k).replace("BINANCE:", "").replace(".P", "").split("|")
    if len(parts) >= 3:
        parts[0] = norm_symbol(parts[0])
        parts[1] = norm_direction(parts[1])
    return "|".join(parts)

def clean_features(r):
    out = {}
    for k, v in r.items():
        if k in DROP_LEAK_KEYS:
            continue
        if k.startswith("label_"):
            continue
        if k.startswith("future_"):
            continue
        out[k] = v
    return out

# Index V4 first, then V3 fallback.
feature_by_key = {}
feature_source_by_key = {}

for source_name, path in [("v4_current14", V4), ("v3_feature_join", V3)]:
    for r in read_jsonl(path):
        sym = norm_symbol(r.get("symbol"))
        if sym not in CURRENT14:
            continue
        key = norm_key(r.get("signal_key") or r.get("signal_id") or r.get("plan_id"))
        if not key:
            continue
        # Prefer v4 over v3.
        if key not in feature_by_key:
            feature_by_key[key] = clean_features(r)
            feature_source_by_key[key] = source_name

rows = []
unmatched = []

for lab in read_jsonl(OUTCOME):
    sym = norm_symbol(lab.get("symbol"))
    if sym not in CURRENT14:
        continue
    if not lab.get("matched_trade"):
        continue
    if not lab.get("trainable_label"):
        continue
    if lab.get("outcome_binary") not in (0, 1):
        continue

    key = norm_key(lab.get("signal_key"))
    feat = feature_by_key.get(key)

    if not feat:
        unmatched.append({
            "signal_key": lab.get("signal_key"),
            "symbol": sym,
            "closed_at_utc": lab.get("closed_at_utc"),
        })
        continue

    row = {
        **feat,
        "dataset_version": "ml_dataset_v4_outcome_join_v1",
        "feature_source": feature_source_by_key.get(key),
        "label_source": lab.get("label_source"),
        "label_version": lab.get("label_version"),
        "outcome_signal_key": lab.get("signal_key"),
        "client_prefix": lab.get("client_prefix"),
        "closed_at_utc": lab.get("closed_at_utc"),
        "outcome_binary": lab.get("outcome_binary"),
        "label_win": bool(lab.get("outcome_binary") == 1),
        "label_R": 1.0 if lab.get("outcome_binary") == 1 else -1.0,
        "label_target": "TP_INFERRED" if lab.get("outcome_binary") == 1 else "SL_INFERRED",
        "outcome_status": lab.get("outcome_status"),
        "exit_reason": lab.get("exit_reason"),
        "seconds_to_close": lab.get("seconds_to_close"),
        "bars_to_close_15m": lab.get("bars_to_close_15m"),
        "actual_entry_price": lab.get("actual_entry_price"),
        "matched_trade": True,
        "promotion_eligible": False,
    }
    rows.append(row)

rows.sort(key=lambda r: str(r.get("closed_at_utc") or ""))

with OUT_JSONL.open("w", encoding="utf-8") as f:
    for r in rows:
        f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")

pair = defaultdict(Counter)
feature_sources = Counter()
for r in rows:
    sym = norm_symbol(r.get("symbol"))
    pair[sym]["rows"] += 1
    pair[sym]["wins"] += int(r.get("outcome_binary") == 1)
    pair[sym]["losses"] += int(r.get("outcome_binary") == 0)
    feature_sources[r.get("feature_source")] += 1

report = {
    "ok": True,
    "version": "ml_dataset_v4_outcome_join_v1",
    "created_at_utc": datetime.now(timezone.utc).isoformat(),
    "out_jsonl": str(OUT_JSONL),
    "rows": len(rows),
    "wins": sum(1 for r in rows if r.get("outcome_binary") == 1),
    "losses": sum(1 for r in rows if r.get("outcome_binary") == 0),
    "symbols": sorted(pair.keys()),
    "symbol_count": len(pair),
    "feature_source_counts": dict(feature_sources.most_common()),
    "unmatched_outcome_labels": len(unmatched),
    "unmatched_sample": unmatched[:20],
    "promotion_ready": False,
    "promotion_blockers": [
        "below_500_matched_trainable_labels",
        "cleanup_inferred_labels_not_binance_order_history_confirmed",
        "walk_forward_validation_not_run",
    ],
    "note": "Outcome-joined dataset for report-only/challenger shadow. Do not promote live model from this file alone.",
}

OUT_REPORT.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

with OUT_PAIR.open("w", newline="", encoding="utf-8") as f:
    cols = ["symbol", "rows", "wins", "losses"]
    w = csv.DictWriter(f, fieldnames=cols)
    w.writeheader()
    for sym in sorted(pair):
        row = {"symbol": sym}
        row.update(pair[sym])
        w.writerow(row)

print(json.dumps(report, indent=2, ensure_ascii=False))
print(f"[ok] wrote {OUT_JSONL}")
print(f"[ok] wrote {OUT_REPORT}")
print(f"[ok] wrote {OUT_PAIR}")
