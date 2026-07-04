#!/usr/bin/env python3
# ML_DATASET_V4_LINEAR_QUANT_JOIN_V1_20260704
import os
import json
import math
from pathlib import Path
from datetime import datetime, timezone

MARKER = "ML_DATASET_V4_LINEAR_QUANT_JOIN_V1_20260704"

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

def ts_to_iso(dt):
    if not dt:
        return None
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

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

def read_jsonl(path):
    rows = []
    p = Path(path)
    if not p.exists():
        return rows
    with p.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
                if isinstance(r, dict):
                    rows.append(r)
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
        "event_time_utc",
        "timestamp_utc",
        "ts_utc",
    ):
        dt = parse_ts(row.get(k))
        if dt:
            return dt
    return None

def has_label(row):
    for k in ("label_win", "outcome_binary", "win", "is_win", "outcome_win"):
        if k in row and row.get(k) not in (None, ""):
            return True
    txt = str(row.get("outcome_status") or row.get("label_target") or row.get("first_hit") or row.get("target") or "").upper()
    return any(x in txt for x in ("TP1", "TP2", "TP3", "WIN", "SL", "LOSS"))

def load_linear_store(path):
    rows = read_jsonl(path)
    by_symbol = {}
    for r in rows:
        sym = norm_symbol(r.get("symbol"))
        dt = parse_ts(r.get("created_at_utc"))
        if not sym:
            continue
        by_symbol.setdefault(sym, []).append((dt, r))
    for sym in by_symbol:
        by_symbol[sym].sort(key=lambda x: x[0] or datetime.min.replace(tzinfo=timezone.utc))
    return by_symbol, len(rows)

def find_linear(row, by_symbol, max_age_sec, future_tolerance_sec):
    sym = norm_symbol(row.get("symbol") or row.get("pair"))
    direction = norm_dir(row.get("direction") or row.get("dir") or row.get("side"))
    sig_ts = row_ts(row)

    if not sym:
        return None, "missing_symbol"
    arr = by_symbol.get(sym) or []
    if not arr:
        return None, "missing_linear_symbol"

    if sig_ts is None:
        dt, lr = arr[-1]
        return lr, "latest_no_signal_ts"

    best = None
    best_dt = None
    future_seen = False

    for dt, lr in arr:
        if dt is None:
            continue
        # no future leak: allow tiny tolerance for same-cycle write order only
        if dt <= sig_ts or (dt - sig_ts).total_seconds() <= future_tolerance_sec:
            best = lr
            best_dt = dt
        else:
            future_seen = True

    if best is None:
        return None, "future_blocked" if future_seen else "no_eligible_linear_before_signal"

    age = abs((sig_ts - best_dt).total_seconds()) if best_dt else None
    if age is not None and age > max_age_sec:
        return None, f"linear_stale:{round(age,1)}>{max_age_sec}"

    return best, "joined"

def flatten_linear(row, lr):
    out = dict(row)

    direction = norm_dir(out.get("direction") or out.get("dir") or out.get("side"))
    score_key = "la_score_long" if direction == "LONG" else "la_score_short"
    directional_score = to_float(lr.get(score_key))

    features = lr.get("linear_algebra_features") or {}

    out["linear_join_ok"] = True
    out["linear_join_source"] = "STAT_TECH_LINEAR_ALGEBRA_V1"
    out["linear_join_created_at_utc"] = lr.get("created_at_utc")
    out["linear_la_score_long"] = lr.get("la_score_long")
    out["linear_la_score_short"] = lr.get("la_score_short")
    out["linear_quant_score_joined"] = directional_score

    # Keep existing live-loop score if present. If missing, fill it.
    if out.get("linear_quant_score") in (None, ""):
        out["linear_quant_score"] = directional_score
    if out.get("linear_quant_source") in (None, ""):
        out["linear_quant_source"] = "STAT_TECH_LINEAR_ALGEBRA_V1"

    out["linear_corr_btc"] = features.get("corr_btc")
    out["linear_corr_eth"] = features.get("corr_eth")
    out["linear_beta_btc"] = features.get("beta_btc")
    out["linear_beta_eth"] = features.get("beta_eth")
    out["linear_cosine_btc"] = features.get("cosine_btc")
    out["linear_cosine_eth"] = features.get("cosine_eth")
    out["linear_ret_15m"] = features.get("ret_15m")
    out["linear_ret_1h"] = features.get("ret_1h")
    out["linear_ret_4h"] = features.get("ret_4h")
    out["linear_btc_ret_1h"] = features.get("btc_ret_1h")
    out["linear_eth_ret_1h"] = features.get("eth_ret_1h")
    out["linear_residual_btc_1h"] = features.get("residual_btc_1h")
    out["linear_avg_abs_corr_universe"] = features.get("avg_abs_corr_universe")

    # Dataset target value after live blend, if available.
    if out.get("quant_score_after_linear_blend") in (None, ""):
        out["quant_score_after_linear_blend"] = out.get("quant_score")

    out["linear_join_marker"] = MARKER
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
    score = 0
    if has_label(row):
        score += 100
    if row.get("linear_join_ok"):
        score += 20
    if row.get("linear_quant_score") not in (None, ""):
        score += 10
    if row.get("quant_score") not in (None, ""):
        score += 5
    if row.get("bridge_decision") not in (None, ""):
        score += 3
    dt = row_ts(row)
    if dt:
        score += min(2, dt.timestamp() / 10**10)
    return score

def main():
    log_dir = Path(os.getenv("LOG_DIR", "logs"))
    report_dir = Path(os.getenv("REPORT_DIR", "reports"))
    log_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    base_paths = []
    raw = os.getenv("ML_LINEAR_JOIN_BASE_PATHS", "").strip()
    if raw:
        base_paths.extend([x.strip() for x in raw.split(",") if x.strip()])

    base_paths.extend([
        str(log_dir / "ml_dataset_v4_current14_candidate_join.jsonl"),
        str(log_dir / "ml_dataset_v4_candidate_join.jsonl"),
        str(log_dir / "ml_dataset_v4_outcome_join_v1.jsonl"),
        str(log_dir / "stat_tech_live_bridge_events_v1.jsonl"),
    ])

    linear_path = os.getenv("ML_LINEAR_QUANT_STORE_PATH", str(log_dir / "stat_tech_linear_quant_store_v1.jsonl"))
    output_path = os.getenv("ML_LINEAR_JOIN_OUTPUT_PATH", str(log_dir / "ml_dataset_v4_linear_quant_join_v1.jsonl"))
    max_age_sec = float(os.getenv("ML_LINEAR_JOIN_MAX_AGE_MIN", "30") or 30) * 60
    future_tolerance_sec = float(os.getenv("ML_LINEAR_JOIN_FUTURE_TOLERANCE_SEC", "90") or 90)

    by_symbol, linear_rows_total = load_linear_store(linear_path)

    raw_rows = []
    source_counts = {}
    for path in base_paths:
        p = Path(path)
        if not p.exists():
            continue
        rows = read_jsonl(p)
        source_counts[str(p)] = len(rows)
        for r in rows:
            rr = dict(r)
            rr["_linear_join_base_source_file"] = str(p)
            raw_rows.append(rr)

    enriched = []
    counters = {
        "raw_rows": len(raw_rows),
        "linear_rows_total": linear_rows_total,
        "joined": 0,
        "already_had_linear": 0,
        "missing": 0,
        "future_blocked": 0,
        "stale": 0,
        "latest_no_signal_ts": 0,
        "label_rows": 0,
    }
    reason_counts = {}

    for r in raw_rows:
        if has_label(r):
            counters["label_rows"] += 1

        already = r.get("linear_quant_score") not in (None, "")
        if already:
            counters["already_had_linear"] += 1

        lr, reason = find_linear(r, by_symbol, max_age_sec=max_age_sec, future_tolerance_sec=future_tolerance_sec)
        reason_counts[reason] = reason_counts.get(reason, 0) + 1

        if lr:
            rr = flatten_linear(r, lr)
            rr["linear_join_reason"] = reason
            if reason == "joined":
                counters["joined"] += 1
            elif reason == "latest_no_signal_ts":
                counters["latest_no_signal_ts"] += 1
            enriched.append(rr)
        else:
            rr = dict(r)
            rr["linear_join_ok"] = False
            rr["linear_join_reason"] = reason
            rr["linear_join_marker"] = MARKER
            if "future" in reason:
                counters["future_blocked"] += 1
            elif "stale" in reason:
                counters["stale"] += 1
            else:
                counters["missing"] += 1
            enriched.append(rr)

    # Deduplicate, prefer labeled + linear-enriched rows.
    best = {}
    for r in enriched:
        k = dedupe_key(r)
        if k not in best or row_quality(r) >= row_quality(best[k]):
            best[k] = r

    deduped = list(best.values())
    deduped.sort(key=lambda r: str(r.get("created_at_utc") or ""))

    trainable_linear = sum(1 for r in deduped if has_label(r) and r.get("linear_join_ok"))
    linear_feature_rows = sum(1 for r in deduped if r.get("linear_quant_score") not in (None, ""))

    write_jsonl(output_path, deduped)

    report = {
        "ok": True,
        "marker": MARKER,
        "created_at_utc": utc_now_iso(),
        "base_source_counts": source_counts,
        "linear_store_path": linear_path,
        "output_path": output_path,
        "raw_rows": len(raw_rows),
        "deduped_rows": len(deduped),
        "linear_rows_total": linear_rows_total,
        "joined_rows": counters["joined"],
        "already_had_linear_rows": counters["already_had_linear"],
        "linear_feature_rows": linear_feature_rows,
        "label_rows_raw": counters["label_rows"],
        "trainable_linear_label_rows": trainable_linear,
        "missing_rows": counters["missing"],
        "future_blocked_rows": counters["future_blocked"],
        "stale_rows": counters["stale"],
        "latest_no_signal_ts_rows": counters["latest_no_signal_ts"],
        "reason_counts": reason_counts,
        "no_future_leak_ok": counters["future_blocked"] == 0,
        "max_age_sec": max_age_sec,
        "future_tolerance_sec": future_tolerance_sec,
    }

    report_path = report_dir / "ml_dataset_v4_linear_quant_join_v1_report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    main()
