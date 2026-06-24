#!/usr/bin/env python3
import json
import csv
import hashlib
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime, timezone

ROOT = Path(".")
LOGS = ROOT / "logs"
REPORTS = ROOT / "reports"
REPORTS.mkdir(exist_ok=True)

OUT_JSONL = LOGS / "ml_dataset_v4_current14_execution_maturity_join.jsonl"
SUMMARY_JSON = REPORTS / "ml_dataset_v4_current14_execution_maturity_summary.json"
PAIR_CSV = REPORTS / "ml_dataset_v4_current14_execution_maturity_pair_summary.csv"
QUALITY_JSON = REPORTS / "ml_dataset_v4_current14_execution_maturity_quality.json"

CURRENT14 = {
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "PAXGUSDT", "HYPEUSDT", "XRPUSDT", "ZECUSDT",
    "UNIUSDT", "ADAUSDT", "BCHUSDT", "LINKUSDT", "SUIUSDT", "LTCUSDT", "AVAXUSDT"
}

def parse_time(x):
    if not x:
        return None
    try:
        return datetime.fromisoformat(str(x).replace("Z", "+00:00"))
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

def fnum(x, default=None):
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default

def norm_symbol(r):
    sym = r.get("symbol") or r.get("pair") or r.get("symbol_norm")
    sym = str(sym or "").upper().replace("BINANCE:", "").replace(".P", "")
    return sym

def norm_direction(r):
    d = str(r.get("direction") or r.get("side") or r.get("direction_norm") or "").upper()
    if d in ("LONG", "BUY"):
        return "LONG"
    if d in ("SHORT", "SELL"):
        return "SHORT"
    return d

def signal_key_of(r):
    return str(
        r.get("signal_key")
        or r.get("signal_id")
        or r.get("plan_id")
        or ""
    )

def event_ts(r):
    return (
        r.get("created_at_utc")
        or r.get("event_at_utc")
        or r.get("decision_at_utc")
        or r.get("closed_at_utc")
        or r.get("timestamp_utc")
    )

def stable_id(*parts):
    raw = "|".join(str(x) for x in parts)
    return hashlib.sha1(raw.encode()).hexdigest()[:16]

candidates = {}

# 1) CoreQuant signals
for r in read_jsonl(LOGS / "vps_smc_core_quant_signals.jsonl"):
    sym = norm_symbol(r)
    if sym not in CURRENT14:
        continue
    key = signal_key_of(r)
    if not key:
        key = f"{sym}|{norm_direction(r)}|COREQ|{stable_id(event_ts(r), r.get('entry_mid'), r.get('sl'))}"

    candidates[key] = {
        "dataset_version": "v4_current14",
        "row_source": "core_quant_signal",
        "signal_key": key,
        "symbol": sym,
        "direction": norm_direction(r),
        "created_at_utc": event_ts(r),
        "status": r.get("status") or r.get("state") or "CONFIRMED",
        "score": fnum(r.get("score")),
        "priority": r.get("priority"),
        "entry_mid": fnum(r.get("entry_mid") or r.get("entry")),
        "sl": fnum(r.get("sl")),
        "tp1": fnum(r.get("tp1")),
        "tp2": fnum(r.get("tp2")),
        "tp3": fnum(r.get("tp3")),
        "rr_at_emit": fnum(r.get("rr_at_emit") or r.get("rr_to_tp1") or r.get("rr_tp1")),
        "htf_dir": r.get("htf_dir"),
        "htf_bias": r.get("htf_bias"),
        "htf_location": r.get("htf_location"),
        "structure_15m": r.get("structure_15m"),
        "sweep_tag": r.get("sweep_tag"),
        "sweep_extreme": fnum(r.get("sweep_extreme")),
        "reclaim_level": fnum(r.get("reclaim_level")),
        "fvg_type": r.get("fvg_type"),
        "fvg_lo": fnum(r.get("fvg_lo")),
        "fvg_hi": fnum(r.get("fvg_hi")),
        "liq_dist_to_zone_pct": fnum(r.get("dist_to_zone_pct")),
        "raw": r,
    }

# 2) Existing execution plans
for r in read_jsonl(LOGS / "execution_plans.jsonl"):
    sym = norm_symbol(r)
    if sym not in CURRENT14:
        continue
    key = signal_key_of(r)
    if not key:
        continue
    base = candidates.get(key, {})
    payload = r.get("payload") or {}
    base.update({
        "dataset_version": "v4_current14",
        "row_source": base.get("row_source") or "execution_plan",
        "signal_key": key,
        "symbol": sym,
        "direction": norm_direction(r),
        "created_at_utc": base.get("created_at_utc") or event_ts(r),
        "status": base.get("status") or r.get("status"),
        "score": base.get("score") if base.get("score") is not None else fnum(payload.get("score") or r.get("score")),
        "priority": base.get("priority") or payload.get("priority") or r.get("priority"),
        "entry_mid": base.get("entry_mid") if base.get("entry_mid") is not None else fnum(r.get("entry_mid")),
        "sl": base.get("sl") if base.get("sl") is not None else fnum(r.get("sl")),
        "tp1": base.get("tp1") if base.get("tp1") is not None else fnum(r.get("tp1")),
        "tp2": base.get("tp2") if base.get("tp2") is not None else fnum(r.get("tp2")),
        "tp3": base.get("tp3") if base.get("tp3") is not None else fnum(r.get("tp3")),
        "rr_at_emit": base.get("rr_at_emit") if base.get("rr_at_emit") is not None else fnum(payload.get("rr_to_tp1") or payload.get("rr_tp1")),
        "htf_dir": base.get("htf_dir") or payload.get("htf_dir"),
        "htf_bias": base.get("htf_bias") or payload.get("htf_bias"),
        "htf_location": base.get("htf_location") or payload.get("htf_location"),
        "structure_15m": base.get("structure_15m") or payload.get("structure_15m"),
        "sweep_tag": base.get("sweep_tag") or payload.get("sweep_tag"),
        "sweep_extreme": base.get("sweep_extreme") if base.get("sweep_extreme") is not None else fnum(payload.get("sweep_extreme")),
        "reclaim_level": base.get("reclaim_level") if base.get("reclaim_level") is not None else fnum(payload.get("reclaim_level")),
        "fvg_type": base.get("fvg_type") or payload.get("fvg_type"),
        "fvg_lo": base.get("fvg_lo") if base.get("fvg_lo") is not None else fnum(payload.get("fvg_lo")),
        "fvg_hi": base.get("fvg_hi") if base.get("fvg_hi") is not None else fnum(payload.get("fvg_hi")),
        "liq_dist_to_zone_pct": base.get("liq_dist_to_zone_pct") if base.get("liq_dist_to_zone_pct") is not None else fnum(payload.get("dist_to_zone_pct")),
        "raw_plan": r,
    })
    candidates[key] = base

# 3) Bridge/result outcomes
bridge_by_key = defaultdict(list)
for r in read_jsonl(LOGS / "vps_smc_bridge_events.jsonl"):
    sym = norm_symbol(r)
    if sym not in CURRENT14:
        continue
    key = signal_key_of(r)
    if key:
        bridge_by_key[key].append(r)

# 4) Execution events
exec_by_key = defaultdict(list)
for r in read_jsonl(LOGS / "execution_events.jsonl"):
    sym = norm_symbol(r)
    if sym not in CURRENT14:
        continue
    key = signal_key_of(r)
    if key:
        exec_by_key[key].append(r)

# 5) Decisions
decision_by_key = defaultdict(list)
for r in read_jsonl(LOGS / "decisions.jsonl"):
    sym = norm_symbol(r)
    if sym not in CURRENT14:
        continue
    key = signal_key_of(r)
    if key:
        decision_by_key[key].append(r)

rows = []
for key, row in candidates.items():
    sym = row.get("symbol")
    if sym not in CURRENT14:
        continue

    bridges = bridge_by_key.get(key, [])
    execs = exec_by_key.get(key, [])
    decisions = decision_by_key.get(key, [])

    final_decision = None
    final_reason = None
    blocker_reason = None

    for r in bridges + decisions:
        d = r.get("decision") or r.get("bridge_decision")
        reason = r.get("reason") or r.get("bridge_reason") or r.get("block_reason")
        if d:
            final_decision = d
        if reason:
            final_reason = reason
            if str(d).upper() in ("REJECT", "NO_TRADE", "REFRESH_RECOMPUTE_DONE") or "BLOCK" in str(reason):
                blocker_reason = reason

    live_order_placed = any(
        ("LIVE_ORDER_PLACED" in json.dumps(r, ensure_ascii=False))
        for r in bridges + decisions + execs
    )
    position_confirmed = any(
        ("position_confirmed_after_entry" in json.dumps(r, ensure_ascii=False))
        for r in execs + bridges
    )
    protection_ok = any(
        ('"protection_ok": true' in json.dumps(r, ensure_ascii=False).lower())
        for r in execs + bridges
    )

    # Label v4 conservative:
    # 1 = protected live order was placed. Outcome PnL label needs future outcome_labeler, so mark trainable only if explicit closed outcome found.
    # For now this dataset supports candidate/gate maturity, not final model promotion unless closed labels exist.
    outcome_binary = None
    label_source = None

    # Try simple explicit closed labels if present in logs
    text_all = " ".join(json.dumps(r, ensure_ascii=False) for r in execs + bridges + decisions)
    if any(x in text_all for x in ["CLOSED_WIN", "TP_HIT", "TAKE_PROFIT_FILLED"]):
        outcome_binary = 1
        label_source = "explicit_close_win"
    elif any(x in text_all for x in ["CLOSED_LOSS", "SL_HIT", "STOP_LOSS_FILLED"]):
        outcome_binary = 0
        label_source = "explicit_close_loss"

    row.update({
        "bridge_event_count": len(bridges),
        "execution_event_count": len(execs),
        "decision_event_count": len(decisions),
        "final_decision": final_decision,
        "final_reason": final_reason,
        "blocker_reason": blocker_reason,
        "live_order_placed": live_order_placed,
        "position_confirmed": position_confirmed,
        "protection_ok": protection_ok,
        "outcome_binary": outcome_binary,
        "label_source": label_source,
        "trainable_label": outcome_binary in (0, 1),
    })

    # quality flags
    missing = []
    for f in ["symbol", "direction", "entry_mid", "sl", "tp1", "score"]:
        if row.get(f) in (None, "", []):
            missing.append(f)
    row["missing_required_fields"] = missing
    row["missing_required_n"] = len(missing)

    rows.append(row)

rows.sort(key=lambda r: str(r.get("created_at_utc") or ""))

with OUT_JSONL.open("w", encoding="utf-8") as f:
    for r in rows:
        # do not write giant raw fields into v4 output
        compact = {k: v for k, v in r.items() if k not in ("raw", "raw_plan")}
        f.write(json.dumps(compact, ensure_ascii=False, default=str) + "\n")

pair_stats = defaultdict(lambda: Counter())
reason_counts = Counter()
decision_counts = Counter()
missing_counts = Counter()

for r in rows:
    sym = r.get("symbol") or "UNK"
    pair_stats[sym]["rows"] += 1
    pair_stats[sym]["trainable"] += int(bool(r.get("trainable_label")))
    pair_stats[sym]["wins"] += int(r.get("outcome_binary") == 1)
    pair_stats[sym]["losses"] += int(r.get("outcome_binary") == 0)
    pair_stats[sym]["live_order_placed"] += int(bool(r.get("live_order_placed")))
    pair_stats[sym]["position_confirmed"] += int(bool(r.get("position_confirmed")))
    pair_stats[sym]["protection_ok"] += int(bool(r.get("protection_ok")))
    reason_counts[str(r.get("blocker_reason") or r.get("final_reason") or "NONE")] += 1
    decision_counts[str(r.get("final_decision") or "NONE")] += 1
    for m in r.get("missing_required_fields") or []:
        missing_counts[m] += 1

summary = {
    "dataset_version": "v4_current14",
    "created_at_utc": datetime.now(timezone.utc).isoformat(),
    "output": str(OUT_JSONL),
    "current14": sorted(CURRENT14),
    "rows": len(rows),
    "trainable_label_rows": sum(1 for r in rows if r.get("trainable_label")),
    "wins": sum(1 for r in rows if r.get("outcome_binary") == 1),
    "losses": sum(1 for r in rows if r.get("outcome_binary") == 0),
    "live_order_placed_rows": sum(1 for r in rows if r.get("live_order_placed")),
    "position_confirmed_rows": sum(1 for r in rows if r.get("position_confirmed")),
    "protection_ok_rows": sum(1 for r in rows if r.get("protection_ok")),
    "decision_counts": dict(decision_counts.most_common()),
    "reason_counts": dict(reason_counts.most_common(50)),
    "missing_required_counts": dict(missing_counts.most_common()),
    "duplicate_signal_keys": len(rows) - len(set(r.get("signal_key") for r in rows)),
    "note": "Dataset v4 candidate join. Closed trade labels require outcome_labeler for true model training maturity.",
}

SUMMARY_JSON.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

with PAIR_CSV.open("w", newline="", encoding="utf-8") as f:
    cols = ["symbol", "rows", "trainable", "wins", "losses", "live_order_placed", "position_confirmed", "protection_ok"]
    w = csv.DictWriter(f, fieldnames=cols)
    w.writeheader()
    for sym in sorted(pair_stats):
        d = {"symbol": sym}
        d.update(pair_stats[sym])
        w.writerow(d)

quality = {
    "ok": True,
    "warnings": [],
    "checks": {
        "has_rows": len(rows) > 0,
        "no_duplicate_signal_keys": summary["duplicate_signal_keys"] == 0,
        "missing_required_counts": summary["missing_required_counts"],
        "trainable_label_rows": summary["trainable_label_rows"],
        "losses": summary["losses"],
    }
}

if len(rows) == 0:
    quality["ok"] = False
    quality["warnings"].append("dataset_empty")
if summary["duplicate_signal_keys"] > 0:
    quality["warnings"].append("duplicate_signal_keys_present")
if summary["trainable_label_rows"] < 500:
    quality["warnings"].append("below_500_trainable_labels_for_model_promotion")
if summary["losses"] < 150:
    quality["warnings"].append("below_150_losses_for_reliable_hard_gate_training")
if missing_counts:
    quality["warnings"].append("missing_required_features_present")

QUALITY_JSON.write_text(json.dumps(quality, indent=2, ensure_ascii=False), encoding="utf-8")

print(json.dumps(summary, indent=2, ensure_ascii=False))
print(f"[ok] wrote {OUT_JSONL}")
print(f"[ok] wrote {SUMMARY_JSON}")
print(f"[ok] wrote {PAIR_CSV}")
print(f"[ok] wrote {QUALITY_JSON}")
