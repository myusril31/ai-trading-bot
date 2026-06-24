#!/usr/bin/env python3
import json
import csv
import re
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime, timezone

ROOT = Path(".")
LOGS = ROOT / "logs"
REPORTS = ROOT / "reports"
LOGS.mkdir(exist_ok=True)
REPORTS.mkdir(exist_ok=True)

OUT_JSONL = LOGS / "outcome_labels_v1.jsonl"
REPORT_JSON = REPORTS / "outcome_labeler_v1_report.json"
PAIR_CSV = REPORTS / "outcome_labeler_v1_by_pair.csv"

CURRENT14 = {
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "PAXGUSDT", "HYPEUSDT", "XRPUSDT", "ZECUSDT",
    "UNIUSDT", "ADAUSDT", "BCHUSDT", "LINKUSDT", "SUIUSDT", "LTCUSDT", "AVAXUSDT"
}

CLIENT_PREFIX_RE = re.compile(r"^(?P<prefix>[A-Z]+_\d+)(?:_(?:ENTRY|SL|TP1|TP2|TP3))?$")

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

def ts_parse(x):
    if not x:
        return None
    try:
        return datetime.fromisoformat(str(x).replace("Z", "+00:00"))
    except Exception:
        return None

def dt_seconds(a, b):
    da = ts_parse(a)
    db = ts_parse(b)
    if not da or not db:
        return None
    return (db - da).total_seconds()

def fnum(x, default=None):
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default

def norm_symbol(x):
    return str(x or "").upper().replace("BINANCE:", "").replace(".P", "")

def norm_direction(x):
    d = str(x or "").upper()
    if d in ("SHORT", "SELL"):
        return "SHORT"
    if d in ("LONG", "BUY"):
        return "LONG"
    return d

def client_prefix(client_id):
    if not client_id:
        return None
    m = CLIENT_PREFIX_RE.match(str(client_id))
    return m.group("prefix") if m else None

def compact_plan(plan):
    if not isinstance(plan, dict):
        return {}
    return {
        "plan_id": plan.get("plan_id"),
        "created_at_utc": plan.get("created_at_utc"),
        "signal_key": plan.get("signal_key"),
        "symbol": norm_symbol(plan.get("symbol")),
        "direction": norm_direction(plan.get("direction")),
        "entry_mid": fnum(plan.get("entry_mid")),
        "actual_entry_price": fnum(plan.get("actual_entry_price")),
        "sl": fnum(plan.get("sl")),
        "tp1": fnum(plan.get("tp1")),
        "tp2": fnum(plan.get("tp2")),
        "tp3": fnum(plan.get("tp3")),
        "quantity": fnum(plan.get("quantity")),
        "rr_tp1": fnum(plan.get("rr_tp1")),
        "rr_target_r": fnum(plan.get("rr_target_r")),
        "score": fnum((plan.get("payload") or {}).get("score") or plan.get("score")),
        "priority": (plan.get("payload") or {}).get("priority") or plan.get("priority"),
        "margin_type": plan.get("margin_type"),
        "entry_order_type": plan.get("entry_order_type"),
    }

# Map protective client prefix -> trade metadata from bridge accepted executions.
trade_by_prefix = {}
accepted_by_signal_key = {}

for r in read_jsonl(LOGS / "vps_smc_bridge_events.jsonl"):
    if r.get("bridge_decision") != "ACCEPT":
        continue

    raw = r.get("raw_bridge_result") or {}
    live = raw.get("live_execution") or {}
    plan = live.get("plan") or {}
    plan_c = compact_plan(plan)

    symbol = norm_symbol(r.get("symbol") or plan_c.get("symbol"))
    if symbol and symbol not in CURRENT14:
        continue

    signal_key = r.get("signal_key") or raw.get("signal_id") or plan_c.get("signal_key")
    entry_result = live.get("entry_result") or {}
    entry_body = entry_result.get("body") or {}
    entry_client_id = entry_body.get("clientOrderId")
    entry_prefix = client_prefix(entry_client_id)

    entry_fill = live.get("entry_fill_result") or {}
    position = entry_fill.get("position") or {}

    meta = {
        "signal_key": signal_key,
        "symbol": symbol,
        "direction": norm_direction(r.get("direction") or plan_c.get("direction")),
        "bridge_created_at_utc": r.get("created_at_utc"),
        "bridge_reason": r.get("bridge_reason"),
        "execution_mode": r.get("execution_mode"),
        "entry_client_order_id": entry_client_id,
        "entry_order_id": entry_body.get("orderId"),
        "entry_status": entry_body.get("status"),
        "position_confirm_reason": entry_fill.get("reason"),
        "position_amt": position.get("positionAmt"),
        "position_entry_price": fnum(position.get("entryPrice")),
        "position_margin_type": position.get("marginType"),
        "position_isolated": position.get("isolated"),
        **plan_c,
    }

    if signal_key:
        accepted_by_signal_key[signal_key] = meta

    prefixes = set()
    if entry_prefix:
        prefixes.add(entry_prefix)

    for pr in live.get("protective_results") or []:
        body = ((pr or {}).get("result") or {}).get("body") or {}
        cid = body.get("clientAlgoId") or body.get("clientOrderId")
        pref = client_prefix(cid)
        if pref:
            prefixes.add(pref)

    for pref in prefixes:
        trade_by_prefix[pref] = dict(meta, client_prefix=pref)


# === BROAD_TRADE_PREFIX_MATCHER_20260625 ===
def walk_values(obj):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield k, v
            yield from walk_values(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from walk_values(v)

def first_value(obj, keys):
    keys = set(keys)
    for k, v in walk_values(obj):
        if k in keys and v not in (None, "", []):
            return v
    return None

def first_dict_key(obj, key_name):
    for k, v in walk_values(obj):
        if k == key_name and isinstance(v, dict):
            return v
    return {}

def all_client_prefixes(obj):
    prefixes = set()
    for k, v in walk_values(obj):
        if k in ("clientOrderId", "clientAlgoId", "newClientOrderId", "newClientAlgoId"):
            pref = client_prefix(v)
            if pref:
                prefixes.add(pref)
    return prefixes

def merge_trade_meta(old, new):
    out = dict(old or {})
    for k, v in (new or {}).items():
        if out.get(k) in (None, "", [], {}):
            out[k] = v
    return out

def extract_generic_trade_meta(r, source_file):
    live = first_dict_key(r, "live_execution")
    plan = first_dict_key(r, "plan") or (live.get("plan") if isinstance(live, dict) else {}) or {}
    plan_c = compact_plan(plan)

    symbol = norm_symbol(
        r.get("symbol")
        or plan_c.get("symbol")
        or first_value(r, ("symbol", "pair", "symbol_norm"))
    )

    direction = norm_direction(
        r.get("direction")
        or plan_c.get("direction")
        or first_value(r, ("direction", "side", "direction_norm"))
    )

    signal_key = (
        r.get("signal_key")
        or r.get("signal_id")
        or plan_c.get("signal_key")
        or first_value(r, ("signal_key", "signal_id", "plan_id"))
    )

    position = first_dict_key(r, "position")
    entry_body = first_dict_key(r, "body")

    return {
        "matched_source": source_file,
        "signal_key": signal_key,
        "symbol": symbol,
        "direction": direction,
        "bridge_created_at_utc": r.get("created_at_utc") or r.get("event_at_utc") or r.get("decision_at_utc"),
        "entry_client_order_id": first_value(r, ("clientOrderId", "clientAlgoId")),
        "entry_order_id": first_value(r, ("orderId", "algoId")),
        "entry_status": first_value(r, ("status", "algoStatus")),
        "position_confirm_reason": first_value(r, ("reason",)),
        "position_amt": position.get("positionAmt") if isinstance(position, dict) else first_value(r, ("positionAmt",)),
        "position_entry_price": fnum((position or {}).get("entryPrice") if isinstance(position, dict) else first_value(r, ("entryPrice",))),
        "position_margin_type": (position or {}).get("marginType") if isinstance(position, dict) else first_value(r, ("marginType",)),
        **plan_c,
    }

# Broad fallback mapper: old/live orders may exist outside vps_smc_bridge_events.
for source_file in [
    "execution_events.jsonl",
    "decisions.jsonl",
    "execution_plans.jsonl",
    "vps_smc_bridge_events.jsonl",
]:
    for r in read_jsonl(LOGS / source_file):
        prefixes = all_client_prefixes(r)
        if not prefixes:
            continue

        meta = extract_generic_trade_meta(r, source_file)
        sym = norm_symbol(meta.get("symbol"))
        if sym and sym not in CURRENT14:
            continue

        for pref in prefixes:
            trade_by_prefix[pref] = merge_trade_meta(trade_by_prefix.get(pref), dict(meta, client_prefix=pref))
# === END BROAD_TRADE_PREFIX_MATCHER_20260625 ===


# Parse cleanup events. These are the best local proxy for closed outcome.
labels_by_prefix = {}

for r in read_jsonl(LOGS / "tp_lifecycle_events.jsonl"):
    if r.get("action") != "TP_LIFECYCLE_POSITION_CLOSED_CLEANUP":
        continue

    symbol = norm_symbol(r.get("symbol"))
    if symbol and symbol not in CURRENT14:
        continue

    cancel_results = r.get("cancel_results") or []
    client_ids = []
    order_types = []
    prefixes = set()

    for cr in cancel_results:
        cid = cr.get("clientAlgoId") or ((cr.get("result") or {}).get("body") or {}).get("clientAlgoId")
        ot = cr.get("orderType") or ((cr.get("result") or {}).get("body") or {}).get("orderType")
        if cid:
            client_ids.append(str(cid))
            pref = client_prefix(cid)
            if pref:
                prefixes.add(pref)
        if ot:
            order_types.append(str(ot).upper())

    if not prefixes:
        continue

    type_set = set(order_types)
    cid_text = " ".join(client_ids).upper()

    # Conservative inference:
    # If SL remains open and is canceled after position closed, TP likely filled => win.
    # If TP remains open and is canceled after position closed, SL likely filled => loss.
    # If both remain open, closure reason is ambiguous/manual/other.
    has_sl = ("STOP_MARKET" in type_set) or ("_SL" in cid_text)
    has_tp = ("TAKE_PROFIT_MARKET" in type_set) or ("_TP1" in cid_text) or ("_TP2" in cid_text) or ("_TP3" in cid_text)

    if has_sl and not has_tp:
        outcome_binary = 1
        outcome_status = "CLOSED_WIN"
        exit_reason = "TP_HIT_INFERRED_FROM_SL_CLEANUP"
        trainable = True
    elif has_tp and not has_sl:
        outcome_binary = 0
        outcome_status = "CLOSED_LOSS"
        exit_reason = "SL_HIT_INFERRED_FROM_TP_CLEANUP"
        trainable = True
    else:
        outcome_binary = None
        outcome_status = "CLOSED_AMBIGUOUS"
        exit_reason = "AMBIGUOUS_CLEANUP_BOTH_OR_UNKNOWN"
        trainable = False

    for pref in prefixes:
        prev = labels_by_prefix.get(pref)
        # Keep earliest cleanup for a prefix.
        if prev:
            old_t = ts_parse(prev.get("closed_at_utc"))
            new_t = ts_parse(r.get("event_at_utc"))
            if old_t and new_t and old_t <= new_t:
                continue

        labels_by_prefix[pref] = {
            "label_version": "outcome_labeler_v1",
            "client_prefix": pref,
            "symbol": symbol,
            "closed_at_utc": r.get("event_at_utc"),
            "outcome_binary": outcome_binary,
            "outcome_status": outcome_status,
            "exit_reason": exit_reason,
            "trainable_label": trainable,
            "label_source": "tp_lifecycle_cleanup_inference_v1",
            "canceled_order_types": sorted(type_set),
            "canceled_client_algo_ids": client_ids,
            "open_algo_before": r.get("open_algo_before"),
            "cleanup_raw": r,
        }

rows = []
for pref, lab in labels_by_prefix.items():
    trade = trade_by_prefix.get(pref, {})
    row = {
        **lab,
        "matched_trade": bool(trade),
        "signal_key": trade.get("signal_key"),
        "direction": trade.get("direction"),
        "bridge_created_at_utc": trade.get("bridge_created_at_utc"),
        "plan_created_at_utc": trade.get("created_at_utc"),
        "entry_mid": trade.get("entry_mid"),
        "actual_entry_price": trade.get("actual_entry_price") or trade.get("position_entry_price"),
        "sl": trade.get("sl"),
        "tp1": trade.get("tp1"),
        "tp2": trade.get("tp2"),
        "tp3": trade.get("tp3"),
        "quantity": trade.get("quantity"),
        "position_amt": trade.get("position_amt"),
        "position_margin_type": trade.get("position_margin_type"),
        "score": trade.get("score"),
        "priority": trade.get("priority"),
        "entry_order_type": trade.get("entry_order_type"),
        "bars_to_close_15m": None,
        "seconds_to_close": None,
        "mfe_r": None,
        "mae_r": None,
    }

    start_ts = row.get("plan_created_at_utc") or row.get("bridge_created_at_utc")
    sec = dt_seconds(start_ts, row.get("closed_at_utc"))
    if sec is not None:
        row["seconds_to_close"] = sec
        row["bars_to_close_15m"] = sec / 900.0

    rows.append(row)

rows.sort(key=lambda x: str(x.get("closed_at_utc") or ""))

with OUT_JSONL.open("w", encoding="utf-8") as f:
    for r in rows:
        compact = {k: v for k, v in r.items() if k != "cleanup_raw"}
        f.write(json.dumps(compact, ensure_ascii=False, default=str) + "\n")

pair_stats = defaultdict(Counter)
status_counts = Counter()
source_counts = Counter()

for r in rows:
    sym = r.get("symbol") or "UNK"
    pair_stats[sym]["rows"] += 1
    pair_stats[sym]["trainable"] += int(bool(r.get("trainable_label")))
    pair_stats[sym]["wins"] += int(r.get("outcome_binary") == 1)
    pair_stats[sym]["losses"] += int(r.get("outcome_binary") == 0)
    pair_stats[sym]["matched_trade"] += int(bool(r.get("matched_trade")))
    status_counts[r.get("outcome_status")] += 1
    source_counts[r.get("label_source")] += 1

summary = {
    "ok": True,
    "label_version": "outcome_labeler_v1",
    "created_at_utc": datetime.now(timezone.utc).isoformat(),
    "out_jsonl": str(OUT_JSONL),
    "rows": len(rows),
    "trainable_label_rows": sum(1 for r in rows if r.get("trainable_label")),
    "wins": sum(1 for r in rows if r.get("outcome_binary") == 1),
    "losses": sum(1 for r in rows if r.get("outcome_binary") == 0),
    "ambiguous": sum(1 for r in rows if r.get("outcome_binary") is None),
    "matched_trade_rows": sum(1 for r in rows if r.get("matched_trade")),
    "unmatched_rows": sum(1 for r in rows if not r.get("matched_trade")),
    "status_counts": dict(status_counts.most_common()),
    "source_counts": dict(source_counts.most_common()),
    "current14": sorted(CURRENT14),
    "note": "Conservative local inference from TP_LIFECYCLE_POSITION_CLOSED_CLEANUP. SL cleanup => TP hit win; TP cleanup => SL hit loss; both/unknown => not trainable.",
}

REPORT_JSON.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

with PAIR_CSV.open("w", newline="", encoding="utf-8") as f:
    cols = ["symbol", "rows", "trainable", "wins", "losses", "matched_trade"]
    w = csv.DictWriter(f, fieldnames=cols)
    w.writeheader()
    for sym in sorted(pair_stats):
        d = {"symbol": sym}
        d.update(pair_stats[sym])
        w.writerow(d)

print(json.dumps(summary, indent=2, ensure_ascii=False))
print(f"[ok] wrote {OUT_JSONL}")
print(f"[ok] wrote {REPORT_JSON}")
print(f"[ok] wrote {PAIR_CSV}")
