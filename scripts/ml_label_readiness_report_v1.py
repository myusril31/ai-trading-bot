#!/usr/bin/env python3
import csv
import json
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

ROOT = Path(".")
LOGS = ROOT / "logs"
REPORTS = ROOT / "reports"
REPORTS.mkdir(exist_ok=True)
LOGS.mkdir(exist_ok=True)

WIB = ZoneInfo("Asia/Jakarta")

OUT_JSON = REPORTS / "ml_label_readiness_report_v1.json"
OUT_CSV = REPORTS / "ml_label_readiness_report_v1.csv"
SNAP_JSONL = LOGS / "ml_label_readiness_snapshots_v1.jsonl"
SNAP_CSV = LOGS / "ml_label_readiness_snapshots_v1.csv"

LEGACY_V3 = LOGS / "ml_dataset_v3_feature_join.jsonl"
OUTCOME_V1 = LOGS / "outcome_labels_v1.jsonl"

CURRENT14 = {
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "PAXGUSDT", "HYPEUSDT", "XRPUSDT", "ZECUSDT",
    "UNIUSDT", "ADAUSDT", "BCHUSDT", "LINKUSDT", "SUIUSDT", "LTCUSDT", "AVAXUSDT"
}

THRESHOLDS = {
    "min_label_rows": 100,
    "min_symbols": 8,
    "min_wins": 25,
    "min_losses": 25,
    "max_age_hours": 6.0,

    # Promotion is intentionally much stricter.
    "promotion_min_matched_trainable": 500,
    "promotion_min_losses": 150,
}

def now_utc():
    return datetime.now(timezone.utc)

def parse_ts(x):
    if not x:
        return None
    try:
        return datetime.fromisoformat(str(x).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

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

def age_hours_from_latest(ts_list):
    vals = [t for t in ts_list if t]
    if not vals:
        return None
    return round((now_utc() - max(vals)).total_seconds() / 3600.0, 4)

def summarize_legacy_v3():
    rows_total = 0
    label_rows = 0
    wins = 0
    losses = 0
    symbols = Counter()
    target_counts = Counter()
    direction_counts = Counter()
    label_r_counts = Counter()
    latest_ts = []
    samples = []

    for r in read_jsonl(LEGACY_V3):
        sym = norm_symbol(r.get("symbol"))
        if sym not in CURRENT14:
            continue

        rows_total += 1
        latest_ts.append(parse_ts(r.get("created_at_utc")))
        label_win = r.get("label_win")
        target = r.get("label_target") or r.get("first_hit") or r.get("outcome_status")

        is_labeled = label_win in (True, False) or target in ("TP1", "TP2", "TP3", "SL")
        if not is_labeled:
            continue

        label_rows += 1
        symbols[sym] += 1
        direction_counts[norm_direction(r.get("direction"))] += 1

        if label_win is True or target in ("TP1", "TP2", "TP3"):
            wins += 1
        elif label_win is False or target == "SL":
            losses += 1

        if target:
            target_counts[str(target)] += 1
        if r.get("label_R") is not None:
            label_r_counts[str(r.get("label_R"))] += 1

        if len(samples) < 20:
            samples.append(r)

    return {
        "input_file": str(LEGACY_V3.resolve()),
        "rows_total": rows_total,
        "label_rows": label_rows,
        "wins": wins,
        "losses": losses,
        "label_symbols": len(symbols),
        "age_hours": age_hours_from_latest(latest_ts),
        "target_counts": dict(target_counts.most_common()),
        "direction_counts": dict(direction_counts.most_common()),
        "symbol_counts": dict(symbols.most_common()),
        "label_R_counts": dict(label_r_counts.most_common()),
        "sample_labels": samples,
    }

def summarize_outcome_v1():
    rows_total = 0
    trainable_all = 0
    matched_trainable = 0
    wins_all = 0
    losses_all = 0
    wins_matched = 0
    losses_matched = 0
    ambiguous = 0
    unmatched = 0
    symbols_all = Counter()
    symbols_matched = Counter()
    status_counts = Counter()
    direction_counts = Counter()
    latest_ts = []
    samples = []

    for r in read_jsonl(OUTCOME_V1):
        sym = norm_symbol(r.get("symbol"))
        if sym not in CURRENT14:
            continue

        rows_total += 1
        latest_ts.append(parse_ts(r.get("closed_at_utc")))
        status_counts[str(r.get("outcome_status"))] += 1
        direction_counts[norm_direction(r.get("direction"))] += 1

        if r.get("outcome_binary") is None:
            ambiguous += 1

        matched = bool(r.get("matched_trade"))
        trainable = bool(r.get("trainable_label")) and r.get("outcome_binary") in (0, 1)

        if not matched:
            unmatched += 1

        if trainable:
            trainable_all += 1
            symbols_all[sym] += 1
            if r.get("outcome_binary") == 1:
                wins_all += 1
            elif r.get("outcome_binary") == 0:
                losses_all += 1

        if trainable and matched:
            matched_trainable += 1
            symbols_matched[sym] += 1
            if r.get("outcome_binary") == 1:
                wins_matched += 1
            elif r.get("outcome_binary") == 0:
                losses_matched += 1
            if len(samples) < 20:
                samples.append(r)

    return {
        "input_file": str(OUTCOME_V1.resolve()),
        "rows_total": rows_total,
        "trainable_all": trainable_all,
        "matched_trainable": matched_trainable,
        "wins_all": wins_all,
        "losses_all": losses_all,
        "wins_matched": wins_matched,
        "losses_matched": losses_matched,
        "ambiguous": ambiguous,
        "unmatched": unmatched,
        "label_symbols_all": len(symbols_all),
        "label_symbols_matched": len(symbols_matched),
        "age_hours": age_hours_from_latest(latest_ts),
        "status_counts": dict(status_counts.most_common()),
        "direction_counts": dict(direction_counts.most_common()),
        "symbol_counts_all": dict(symbols_all.most_common()),
        "symbol_counts_matched": dict(symbols_matched.most_common()),
        "sample_matched_labels": samples,
    }

legacy = summarize_legacy_v3()
outcome = summarize_outcome_v1()

# Primary readiness should use feature-rich labels only.
primary = {
    "source": "outcome_labels_v1_matched_trade",
    "label_rows": outcome["matched_trainable"],
    "wins": outcome["wins_matched"],
    "losses": outcome["losses_matched"],
    "label_symbols": outcome["label_symbols_matched"],
    "age_hours": outcome["age_hours"],
}

reasons = []
if primary["label_rows"] < THRESHOLDS["min_label_rows"]:
    reasons.append(f"label_rows_below_min:{primary['label_rows']}<{THRESHOLDS['min_label_rows']}")
if primary["wins"] < THRESHOLDS["min_wins"]:
    reasons.append(f"wins_below_min:{primary['wins']}<{THRESHOLDS['min_wins']}")
if primary["losses"] < THRESHOLDS["min_losses"]:
    reasons.append(f"losses_below_min:{primary['losses']}<{THRESHOLDS['min_losses']}")
if primary["label_symbols"] < THRESHOLDS["min_symbols"]:
    reasons.append(f"symbols_below_min:{primary['label_symbols']}<{THRESHOLDS['min_symbols']}")
if primary["age_hours"] is not None and primary["age_hours"] > THRESHOLDS["max_age_hours"]:
    reasons.append(f"labels_too_old:{primary['age_hours']}>{THRESHOLDS['max_age_hours']}h")

promotion_blockers = []
if primary["label_rows"] < THRESHOLDS["promotion_min_matched_trainable"]:
    promotion_blockers.append(
        f"promotion_trainable_below_min:{primary['label_rows']}<{THRESHOLDS['promotion_min_matched_trainable']}"
    )
if primary["losses"] < THRESHOLDS["promotion_min_losses"]:
    promotion_blockers.append(
        f"promotion_losses_below_min:{primary['losses']}<{THRESHOLDS['promotion_min_losses']}"
    )
promotion_blockers.append("labels_are_cleanup_inferred_not_binance_order_history_confirmed")
promotion_blockers.append("walk_forward_validation_not_run")

if reasons:
    state = "ML_LABEL_NOT_READY_ACCUMULATE"
    recommendation = "WAIT_ACCUMULATE_LABELS_REPORT_ONLY"
else:
    state = "ML_LABEL_READY_REPORT_ONLY_INFERRED"
    recommendation = "USE_FOR_REPORT_AND_SHADOW_ONLY_DO_NOT_PROMOTE"

report = {
    "ok": True,
    "version": "ml_label_readiness_report_v1_20260625_outcome_v1",
    "mode": "REPORT_ONLY",
    "created_at_utc": now_utc().isoformat(),
    "created_at_wib": now_utc().astimezone(WIB).strftime("%Y-%m-%d %H:%M:%S WIB"),
    "state": state,
    "recommendation": recommendation,
    "reasons": reasons,
    "thresholds": THRESHOLDS,
    "primary_readiness_source": primary["source"],
    "rows_total": outcome["rows_total"],
    "label_rows": primary["label_rows"],
    "wins": primary["wins"],
    "losses": primary["losses"],
    "all_symbols": len(CURRENT14),
    "label_symbols": primary["label_symbols"],
    "age_hours": primary["age_hours"],
    "promotion_ready": False,
    "promotion_blockers": promotion_blockers,
    "outcome_v1": outcome,
    "legacy_v3": legacy,
}

OUT_JSON.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")

# CSV by pair, primary source only.
pair_rows = []
for sym in sorted(CURRENT14):
    all_n = outcome["symbol_counts_all"].get(sym, 0)
    matched_n = outcome["symbol_counts_matched"].get(sym, 0)
    pair_rows.append({
        "symbol": sym,
        "outcome_trainable_all": all_n,
        "outcome_matched_trainable": matched_n,
    })

with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
    cols = ["symbol", "outcome_trainable_all", "outcome_matched_trainable"]
    w = csv.DictWriter(f, fieldnames=cols)
    w.writeheader()
    for row in pair_rows:
        w.writerow(row)

snap = {
    "created_at_utc": report["created_at_utc"],
    "state": state,
    "label_rows": primary["label_rows"],
    "wins": primary["wins"],
    "losses": primary["losses"],
    "label_symbols": primary["label_symbols"],
    "age_hours": primary["age_hours"],
    "recommendation": recommendation,
}
with SNAP_JSONL.open("a", encoding="utf-8") as f:
    f.write(json.dumps(snap, ensure_ascii=False) + "\n")

write_header = not SNAP_CSV.exists()
with SNAP_CSV.open("a", newline="", encoding="utf-8") as f:
    cols = ["created_at_utc", "state", "label_rows", "wins", "losses", "label_symbols", "age_hours", "recommendation"]
    w = csv.DictWriter(f, fieldnames=cols)
    if write_header:
        w.writeheader()
    w.writerow(snap)

print(f"=== ML LABEL READINESS REPORT V1 | {state} ===")
print(f"out_json: {OUT_JSON.resolve()}")
print(f"out_csv : {OUT_CSV.resolve()}")
print(f"snap_jsonl: {SNAP_JSONL.resolve()}")
print(f"snap_csv : {SNAP_CSV.resolve()}")
print(f"primary_source: {primary['source']}")
print(f"recommendation: {recommendation}")
print(f"label_rows: {primary['label_rows']} wins: {primary['wins']} losses: {primary['losses']} symbols: {primary['label_symbols']}")
print(f"promotion_ready: False")
print(f"promotion_blockers: {promotion_blockers}")
