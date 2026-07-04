#!/usr/bin/env python3
# ML_OUTCOME_LABEL_FORWARDER_V1_20260705
import os
import json
import math
from pathlib import Path
from datetime import datetime, timezone

MARKER = "ML_OUTCOME_LABEL_FORWARDER_V1_20260705"

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def parse_ts(v):
    if not v:
        return None
    try:
        txt = str(v).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(txt)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def to_float(v, default=None):
    try:
        if v is None or v == "":
            return default
        x = float(v)
        if not math.isfinite(x):
            return default
        return x
    except Exception:
        return default

def norm_symbol(v):
    return str(v or "").strip().upper().replace("BINANCE:", "").replace(".P", "").replace("/", "")

def norm_dir(v):
    d = str(v or "").strip().upper()
    if d in ("BUY", "BULL", "LONG"):
        return "LONG"
    if d in ("SELL", "BEAR", "SHORT"):
        return "SHORT"
    return d

def read_jsonl(path, max_rows=None):
    p = Path(path)
    if not p.exists():
        return []
    lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()
    if max_rows:
        lines = lines[-max_rows:]
    rows = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                rows.append(obj)
        except Exception:
            pass
    return rows

def write_jsonl(path, rows):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n")

def row_ts(row):
    for k in (
        "created_at_utc",
        "signal_created_at_utc",
        "candidate_created_at_utc",
        "entry_created_at_utc",
        "exit_created_at_utc",
        "closed_at_utc",
        "event_time_utc",
        "timestamp_utc",
        "ts_utc",
        "time_utc",
    ):
        dt = parse_ts(row.get(k))
        if dt:
            return dt
    return None

def parse_label(row):
    for k in ("label_win", "outcome_binary", "win", "is_win", "outcome_win"):
        if k in row and row.get(k) not in (None, ""):
            v = row.get(k)
            if isinstance(v, bool):
                return v, "TP" if v else "SL"
            if isinstance(v, (int, float)):
                if int(v) == 1:
                    return True, "TP"
                if int(v) == 0:
                    return False, "SL"
            s = str(v).strip().upper()
            if s in ("1", "TRUE", "YES", "WIN", "TP", "TP1", "TP2", "TP3"):
                return True, "TP"
            if s in ("0", "FALSE", "NO", "LOSS", "SL", "STOP", "STOPLOSS"):
                return False, "SL"

    text = " ".join(str(row.get(k) or "") for k in (
        "outcome_status",
        "label_target",
        "first_hit",
        "target",
        "close_reason",
        "exit_reason",
        "reason",
        "final_reason",
        "status",
        "event",
        "type",
    )).upper()

    if any(x in text for x in ("TP1", "TP2", "TP3", "TAKE_PROFIT", "TAKE PROFIT", "PROFIT_TARGET", "TARGET_HIT")):
        return True, "TP"
    if any(x in text for x in ("STOP_LOSS", "STOPLOSS", "STOP LOSS", " SL", "SL_", "LIQUIDATION", "CUT_LOSS")):
        return False, "SL"

    pnl = to_float(
        row.get("net_pnl")
        or row.get("realized_pnl")
        or row.get("pnl")
        or row.get("profit")
        or row.get("income")
        or row.get("realizedProfit")
    )
    if pnl is not None:
        return pnl > 0, "PNL_POSITIVE" if pnl > 0 else "PNL_NEGATIVE"

    return None, None

def signal_keys(row):
    sym = norm_symbol(row.get("symbol") or row.get("pair"))
    direction = norm_dir(row.get("direction") or row.get("dir") or row.get("side"))
    setup = str(row.get("setup_type") or row.get("setup") or "UNKNOWN").upper()

    entry = to_float(row.get("entry") or row.get("entry_price") or row.get("limit_entry") or row.get("entry_limit"))
    sl = to_float(row.get("sl") or row.get("stop_loss") or row.get("stop"))
    tp = to_float(row.get("tp1") or row.get("tp") or row.get("target") or row.get("take_profit"))

    keys = []

    for k in ("signal_key", "signal_id", "client_order_id", "clientOrderId", "order_link_id"):
        v = row.get(k)
        if v not in (None, ""):
            keys.append(f"ID:{str(v)}")

    if sym and direction and setup and entry and sl:
        keys.append(f"PLAN:{sym}|{direction}|{setup}|{round(entry, 8)}|{round(sl, 8)}")

    if sym and direction and entry and sl:
        keys.append(f"ENTRY_SL:{sym}|{direction}|{round(entry, 8)}|{round(sl, 8)}")

    if sym and direction and setup:
        keys.append(f"SSD:{sym}|{direction}|{setup}")

    if sym and direction:
        keys.append(f"SD:{sym}|{direction}")

    if sym:
        keys.append(f"S:{sym}")

    return keys

def has_any_feature(row):
    return any(row.get(k) not in (None, "") for k in (
        "linear_quant_score",
        "barrier_score",
        "ou_score",
        "tp_sl_p_tp",
        "tp_sl_predictor_decision",
        "quant_score",
    ))

def has_label(row):
    lab, _ = parse_label(row)
    return lab is not None

def source_quality(row):
    q = 0
    if has_label(row):
        q += 100
    if row.get("signal_key"):
        q += 20
    if row.get("entry") not in (None, "") and row.get("sl") not in (None, ""):
        q += 15
    if row.get("net_pnl") not in (None, "") or row.get("realized_pnl") not in (None, ""):
        q += 10
    if row_ts(row):
        q += 5
    return q

def build_label_index(label_rows):
    exact = {}
    broad = {}

    for r in label_rows:
        lab, status = parse_label(r)
        if lab is None:
            continue

        label_obj = {
            "label_win": bool(lab),
            "outcome_binary": 1 if lab else 0,
            "outcome_status": status,
            "label_source_marker": MARKER,
            "label_source_file": r.get("_source_file"),
            "label_source_quality": source_quality(r),
            "label_outcome_ts_utc": (row_ts(r).isoformat().replace("+00:00", "Z") if row_ts(r) else None),
            "label_net_pnl": to_float(r.get("net_pnl") or r.get("realized_pnl") or r.get("pnl") or r.get("profit") or r.get("income")),
            "label_close_reason": r.get("close_reason") or r.get("exit_reason") or r.get("reason") or r.get("final_reason"),
            "label_raw_signal_key": r.get("signal_key") or r.get("signal_id"),
        }

        for k in signal_keys(r):
            target = exact if k.startswith(("ID:", "PLAN:", "ENTRY_SL:")) else broad
            old = target.get(k)
            if old is None or label_obj["label_source_quality"] >= old["label_source_quality"]:
                target[k] = label_obj

    return exact, broad

def forward_label(row, exact, broad):
    if has_label(row):
        out = dict(row)
        lab, status = parse_label(out)
        out["label_win"] = bool(lab)
        out["outcome_binary"] = 1 if lab else 0
        out["outcome_status"] = out.get("outcome_status") or status
        out["label_forwarded"] = False
        return out

    keys = signal_keys(row)

    for k in keys:
        if k in exact:
            out = dict(row)
            lab = exact[k]
            out.update(lab)
            out["label_forwarded"] = True
            out["label_forwarded_at_utc"] = utc_now_iso()
            out["label_match_key"] = k
            out["label_match_strength"] = "exact"
            return out

    allow_broad = str(os.getenv("ML_OUTCOME_FORWARD_ALLOW_BROAD_MATCH", "false")).strip().lower() in ("1", "true", "yes", "on")
    if allow_broad:
        for k in keys:
            if k in broad:
                out = dict(row)
                lab = broad[k]
                out.update(lab)
                out["label_forwarded"] = True
                out["label_forwarded_at_utc"] = utc_now_iso()
                out["label_match_key"] = k
                out["label_match_strength"] = "broad"
                return out

    out = dict(row)
    out["label_forwarded"] = False
    out["label_forward_reason"] = "no_label_match"
    return out

def dedupe_key(row):
    return str(
        row.get("signal_key")
        or row.get("signal_id")
        or "|".join([
            str(row.get("symbol") or row.get("pair") or ""),
            str(row.get("direction") or row.get("side") or ""),
            str(row.get("setup_type") or row.get("setup") or ""),
            str(row.get("entry") or ""),
            str(row.get("sl") or ""),
            str(row.get("created_at_utc") or ""),
        ])
    )

def row_quality(row):
    q = 0
    if has_label(row):
        q += 100
    if row.get("label_forwarded") is True:
        q += 20
    if has_any_feature(row):
        q += 10
    if row.get("ou_score") not in (None, ""):
        q += 6
    if row.get("barrier_score") not in (None, ""):
        q += 6
    if row.get("linear_quant_score") not in (None, ""):
        q += 6
    if row.get("tp_sl_p_tp") not in (None, ""):
        q += 8
    dt = row_ts(row)
    if dt:
        q += min(2, dt.timestamp() / 10**10)
    return q

def main():
    log_dir = Path(os.getenv("LOG_DIR", "logs"))
    report_dir = Path(os.getenv("REPORT_DIR", "reports"))
    log_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    max_rows = int(float(os.getenv("ML_OUTCOME_FORWARD_MAX_ROWS_PER_FILE", "80000")))

    base_paths = [
        log_dir / "ml_dataset_v4_ou_meanrev_join_v1.jsonl",
        log_dir / "ml_dataset_v4_stoch_barrier_join_v1.jsonl",
        log_dir / "ml_dataset_v4_linear_quant_join_v1.jsonl",
        log_dir / "stat_tech_live_bridge_events_v1.jsonl",
        log_dir / "ml_dataset_v4_current14_candidate_join.jsonl",
        log_dir / "ml_dataset_v4_candidate_join.jsonl",
    ]

    label_paths = [
        log_dir / "ml_dataset_v4_outcome_join_v1.jsonl",
        log_dir / "ml_dataset_v4_outcome_forwarded_v1.jsonl",
        log_dir / "tp_lifecycle_events.jsonl",
        log_dir / "execution_events.jsonl",
        log_dir / "decisions.jsonl",
        log_dir / "closed_trades.jsonl",
        log_dir / "trade_outcomes.jsonl",
        log_dir / "tp_sl_outcomes.jsonl",
    ]

    extra_base = os.getenv("ML_OUTCOME_FORWARD_BASE_PATHS", "").strip()
    if extra_base:
        base_paths = [Path(x.strip()) for x in extra_base.split(",") if x.strip()] + base_paths

    extra_label = os.getenv("ML_OUTCOME_FORWARD_LABEL_PATHS", "").strip()
    if extra_label:
        label_paths = [Path(x.strip()) for x in extra_label.split(",") if x.strip()] + label_paths

    base_rows = []
    label_rows = []
    base_counts = {}
    label_counts = {}

    for p in base_paths:
        rows = read_jsonl(p, max_rows=max_rows)
        if rows:
            base_counts[str(p)] = len(rows)
        for r in rows:
            rr = dict(r)
            rr["_outcome_forward_base_source_file"] = str(p)
            base_rows.append(rr)

    for p in label_paths:
        rows = read_jsonl(p, max_rows=max_rows)
        if rows:
            label_counts[str(p)] = len(rows)
        for r in rows:
            rr = dict(r)
            rr["_source_file"] = str(p)
            label_rows.append(rr)

    exact, broad = build_label_index(label_rows)

    forwarded = [forward_label(r, exact, broad) for r in base_rows]

    best = {}
    for r in forwarded:
        k = dedupe_key(r)
        if k not in best or row_quality(r) >= row_quality(best[k]):
            best[k] = r

    deduped = list(best.values())
    deduped.sort(key=lambda r: str(r.get("created_at_utc") or ""))

    output_path = Path(os.getenv("ML_OUTCOME_FORWARD_OUTPUT_PATH", str(log_dir / "ml_dataset_v4_outcome_forwarded_v1.jsonl")))
    write_jsonl(output_path, deduped)

    labeled = [r for r in deduped if has_label(r)]
    forwarded_labeled = [r for r in deduped if r.get("label_forwarded") is True]
    linear_label = [r for r in labeled if r.get("linear_quant_score") not in (None, "")]
    barrier_label = [r for r in labeled if r.get("barrier_score") not in (None, "")]
    ou_label = [r for r in labeled if r.get("ou_score") not in (None, "")]
    tp_sl_pred_label = [r for r in labeled if r.get("tp_sl_p_tp") not in (None, "")]
    all_math_label = [
        r for r in labeled
        if r.get("linear_quant_score") not in (None, "")
        and r.get("barrier_score") not in (None, "")
        and r.get("ou_score") not in (None, "")
    ]

    report = {
        "ok": True,
        "marker": MARKER,
        "created_at_utc": utc_now_iso(),
        "base_counts": base_counts,
        "label_counts": label_counts,
        "output_path": str(output_path),
        "base_rows": len(base_rows),
        "label_source_rows": len(label_rows),
        "exact_label_keys": len(exact),
        "broad_label_keys": len(broad),
        "deduped_rows": len(deduped),
        "labeled_rows": len(labeled),
        "forwarded_labeled_rows": len(forwarded_labeled),
        "trainable_linear_label_rows": len(linear_label),
        "trainable_barrier_label_rows": len(barrier_label),
        "trainable_ou_label_rows": len(ou_label),
        "trainable_tp_sl_predictor_label_rows": len(tp_sl_pred_label),
        "trainable_all_math_label_rows": len(all_math_label),
        "allow_broad_match": str(os.getenv("ML_OUTCOME_FORWARD_ALLOW_BROAD_MATCH", "false")),
    }

    report_path = report_dir / "ml_outcome_label_forwarder_v1_report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    main()
