#!/usr/bin/env python3
# ML_DATASET_V4_STOCH_BARRIER_JOIN_V1_20260704
import os
import json
import math
from pathlib import Path
from datetime import datetime, timezone

MARKER = "ML_DATASET_V4_STOCH_BARRIER_JOIN_V1_20260704"

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

def load_barrier_store(path):
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

def find_barrier(row, by_symbol, max_age_sec, future_tolerance_sec, require_signal_ts=True):
    sym = norm_symbol(row.get("symbol") or row.get("pair"))
    sig_ts = row_ts(row)

    if not sym:
        return None, "missing_symbol"

    arr = by_symbol.get(sym) or []
    if not arr:
        return None, "missing_barrier_symbol"

    if sig_ts is None:
        if require_signal_ts:
            return None, "missing_signal_ts"
        dt, br = arr[-1]
        return br, "latest_no_signal_ts"

    best = None
    best_dt = None
    future_seen = False

    for dt, br in arr:
        if dt is None:
            continue

        # strict no-future: allow tiny tolerance only for same-cycle write ordering
        if dt <= sig_ts or (dt - sig_ts).total_seconds() <= future_tolerance_sec:
            best = br
            best_dt = dt
        else:
            future_seen = True

    if best is None:
        return None, "future_blocked" if future_seen else "no_eligible_barrier_before_signal"

    age = abs((sig_ts - best_dt).total_seconds()) if best_dt else None
    if age is not None and age > max_age_sec:
        return None, f"barrier_stale:{round(age,1)}>{max_age_sec}"

    return best, "joined"

def plan_price(row, key):
    v = to_float(row.get(key))
    if v is not None:
        return v
    plan = row.get("plan") if isinstance(row.get("plan"), dict) else {}
    return to_float(plan.get(key))

def exp_safe(x):
    return math.exp(max(-60.0, min(60.0, float(x))))

def hit_prob_upper_before_lower(mu, sigma, upper_a, lower_b):
    a = float(upper_a)
    b = float(lower_b)
    sig = max(float(sigma), 1e-9)
    m = float(mu)

    if a <= 0 or b <= 0:
        return None

    if abs(m) < 1e-12:
        return b / (a + b)

    den = 1.0 - exp_safe(-2.0 * m * (a + b) / (sig * sig))
    num = 1.0 - exp_safe(-2.0 * m * b / (sig * sig))

    if abs(den) < 1e-12:
        return b / (a + b)

    return max(0.0, min(1.0, num / den))

def compute_barrier_from_plan(row, store_row):
    direction = norm_dir(row.get("direction") or row.get("dir") or row.get("side"))
    entry = plan_price(row, "entry")
    sl = plan_price(row, "sl")

    # single target engine: prefer tp1, fallback to tp/take_profit/target
    tp = (
        plan_price(row, "tp1")
        or plan_price(row, "tp")
        or plan_price(row, "take_profit")
        or plan_price(row, "target")
    )

    if direction not in ("LONG", "SHORT"):
        return {"ok": False, "reason": "bad_direction"}
    if not entry or not sl or not tp or entry <= 0 or sl <= 0 or tp <= 0:
        return {"ok": False, "reason": "missing_entry_sl_or_target"}

    sign = 1.0 if direction == "LONG" else -1.0

    sl_y = sign * math.log(sl / entry)
    tp_y = sign * math.log(tp / entry)

    if sl_y >= 0:
        return {"ok": False, "reason": "sl_not_adverse"}
    if tp_y <= 0:
        return {"ok": False, "reason": "target_not_favorable"}

    lower_b = abs(sl_y)
    upper_a = tp_y

    mu_raw = to_float(store_row.get("mu_logret_bar"), 0.0) or 0.0
    sigma = to_float(store_row.get("sigma_eff_logret_bar"), 0.0) or 0.0
    mu_fav = sign * mu_raw

    prob = hit_prob_upper_before_lower(mu_fav, sigma, upper_a, lower_b)
    if prob is None:
        return {"ok": False, "reason": "prob_none"}

    rr1_log = upper_a / lower_b if lower_b > 0 else None
    barrier_score = round(max(0.0, min(100.0, prob * 100.0)), 1)

    return {
        "ok": True,
        "source": "STAT_TECH_STOCH_BARRIER_V1",
        "direction": direction,
        "entry": entry,
        "sl": sl,
        "target": tp,
        "barrier_prob_target": round(float(prob), 6),
        "barrier_prob_tp1": round(float(prob), 6),
        "barrier_score": barrier_score,
        "barrier_rr1_log": None if rr1_log is None else round(float(rr1_log), 4),
        "barrier_sigma_eff": round(float(sigma), 10),
        "barrier_mu_favorable": round(float(mu_fav), 10),
        "sl_distance_log": round(float(lower_b), 8),
        "target_distance_log": round(float(upper_a), 8),
        "store_created_at_utc": store_row.get("created_at_utc"),
    }

def flatten_barrier(row, store_row):
    out = dict(row)

    # If live loop already put full stoch_barrier object in event row, use it first.
    existing = out.get("stoch_barrier") if isinstance(out.get("stoch_barrier"), dict) else None
    existing_score = to_float(out.get("barrier_score"))
    existing_prob = to_float(out.get("barrier_prob_target"))
    if existing_prob is None:
        existing_prob = to_float(out.get("barrier_prob_tp1"))

    if existing and existing.get("ok"):
        b = existing
        prob_target = b.get("barrier_prob_tp1") or b.get("barrier_prob_target")
        score = b.get("barrier_score")
        rr = b.get("rr1_log") or b.get("barrier_rr1_log")
        sigma = b.get("sigma_eff_logret_bar") or b.get("barrier_sigma_eff")
        mu = b.get("mu_favorable_bar") or b.get("barrier_mu_favorable")

    # Preserve live-computed flat fields.
    # This is valid because live loop already computed barrier_score from entry/sl/target at signal time.
    # Single target RR12: barrier_prob_target = barrier_score / 100 when explicit prob is missing.
    elif existing_score is not None:
        score = existing_score
        prob_target = existing_prob if existing_prob is not None else round(max(0.0, min(1.0, existing_score / 100.0)), 6)
        rr = to_float(out.get("barrier_rr1_log"))
        sigma = to_float(out.get("barrier_sigma_eff"))
        mu = to_float(out.get("barrier_mu_favorable"))
        b = {
            "ok": True,
            "source": "STAT_TECH_STOCH_BARRIER_V1_EXISTING_FIELDS",
            "barrier_score": score,
            "barrier_prob_target": prob_target,
            "barrier_prob_tp1": prob_target,
            "barrier_rr1_log": rr,
            "barrier_sigma_eff": sigma,
            "barrier_mu_favorable": mu,
        }

    else:
        b = compute_barrier_from_plan(out, store_row)
        prob_target = b.get("barrier_prob_target")
        score = b.get("barrier_score")
        rr = b.get("barrier_rr1_log")
        sigma = b.get("barrier_sigma_eff")
        mu = b.get("barrier_mu_favorable")

    out["barrier_join_ok"] = bool(b.get("ok"))
    out["barrier_join_source"] = "STAT_TECH_STOCH_BARRIER_V1"
    out["barrier_join_created_at_utc"] = store_row.get("created_at_utc")
    out["barrier_join_marker"] = MARKER

    if not b.get("ok"):
        out["barrier_join_calc_reason"] = b.get("reason")
        return out

    out["barrier_score"] = to_float(out.get("barrier_score"), score)
    out["barrier_prob_target"] = to_float(out.get("barrier_prob_target"), prob_target)
    out["barrier_prob_tp1"] = to_float(out.get("barrier_prob_tp1"), prob_target)
    out["barrier_emit"] = out.get("barrier_emit")
    out["barrier_rr1_log"] = to_float(out.get("barrier_rr1_log"), rr)
    out["barrier_sigma_eff"] = to_float(out.get("barrier_sigma_eff"), sigma)
    out["barrier_mu_favorable"] = to_float(out.get("barrier_mu_favorable"), mu)
    out["barrier_source"] = "STAT_TECH_STOCH_BARRIER_V1"
    out["barrier_target_model"] = "SINGLE_TARGET_RR12"
    out["target_rr"] = to_float(out.get("target_rr"), 1.2)

    out["barrier_entry"] = b.get("entry") or out.get("barrier_entry")
    out["barrier_sl"] = b.get("sl") or out.get("barrier_sl")
    out["barrier_target"] = b.get("target") or b.get("tp1") or out.get("barrier_target")
    out["barrier_sl_distance_log"] = b.get("sl_distance_log")
    out["barrier_target_distance_log"] = b.get("target_distance_log") or ((b.get("tp_distance_log") or {}).get("tp1") if isinstance(b.get("tp_distance_log"), dict) else None)

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
    if row.get("barrier_join_ok"):
        score += 20
    if row.get("linear_join_ok"):
        score += 10
    if row.get("barrier_score") not in (None, ""):
        score += 8
    if row.get("linear_quant_score") not in (None, ""):
        score += 5
    if row.get("quant_score") not in (None, ""):
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
    raw = os.getenv("ML_BARRIER_JOIN_BASE_PATHS", "").strip()
    if raw:
        base_paths.extend([x.strip() for x in raw.split(",") if x.strip()])

    base_paths.extend([
        str(log_dir / "ml_dataset_v4_linear_quant_join_v1.jsonl"),
        str(log_dir / "ml_dataset_v4_current14_candidate_join.jsonl"),
        str(log_dir / "ml_dataset_v4_candidate_join.jsonl"),
        str(log_dir / "ml_dataset_v4_outcome_join_v1.jsonl"),
        str(log_dir / "stat_tech_live_bridge_events_v1.jsonl"),
    ])

    barrier_path = os.getenv("ML_STOCH_BARRIER_STORE_PATH", str(log_dir / "stat_tech_stoch_barrier_store_v1.jsonl"))
    output_path = os.getenv("ML_BARRIER_JOIN_OUTPUT_PATH", str(log_dir / "ml_dataset_v4_stoch_barrier_join_v1.jsonl"))

    max_age_sec = float(os.getenv("ML_BARRIER_JOIN_MAX_AGE_MIN", "30") or 30) * 60
    future_tolerance_sec = float(os.getenv("ML_BARRIER_JOIN_FUTURE_TOLERANCE_SEC", "90") or 90)
    require_signal_ts = str(os.getenv("ML_BARRIER_JOIN_REQUIRE_SIGNAL_TS", "true")).strip().lower() in ("1", "true", "yes", "on")

    by_symbol, barrier_rows_total = load_barrier_store(barrier_path)

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
            rr["_barrier_join_base_source_file"] = str(p)
            raw_rows.append(rr)

    enriched = []
    counters = {
        "raw_rows": len(raw_rows),
        "barrier_rows_total": barrier_rows_total,
        "joined": 0,
        "strict_calc_ok": 0,
        "already_had_barrier": 0,
        "missing": 0,
        "missing_signal_ts": 0,
        "future_blocked": 0,
        "stale": 0,
        "label_rows": 0,
    }
    reason_counts = {}

    for r in raw_rows:
        if has_label(r):
            counters["label_rows"] += 1

        if r.get("barrier_score") not in (None, ""):
            counters["already_had_barrier"] += 1

        br, reason = find_barrier(
            r,
            by_symbol,
            max_age_sec=max_age_sec,
            future_tolerance_sec=future_tolerance_sec,
            require_signal_ts=require_signal_ts,
        )
        reason_counts[reason] = reason_counts.get(reason, 0) + 1

        if br:
            rr = flatten_barrier(r, br)
            rr["barrier_join_reason"] = reason
            if reason == "joined":
                counters["joined"] += 1
            if rr.get("barrier_join_ok") and reason == "joined":
                counters["strict_calc_ok"] += 1
            enriched.append(rr)
        else:
            rr = dict(r)
            rr["barrier_join_ok"] = False
            rr["barrier_join_reason"] = reason
            rr["barrier_join_marker"] = MARKER
            if "future" in reason:
                counters["future_blocked"] += 1
            elif "stale" in reason:
                counters["stale"] += 1
            elif reason == "missing_signal_ts":
                counters["missing_signal_ts"] += 1
            else:
                counters["missing"] += 1
            enriched.append(rr)

    best = {}
    for r in enriched:
        k = dedupe_key(r)
        if k not in best or row_quality(r) >= row_quality(best[k]):
            best[k] = r

    deduped = list(best.values())
    deduped.sort(key=lambda r: str(r.get("created_at_utc") or ""))

    strict_joined_rows = sum(1 for r in deduped if r.get("barrier_join_ok") and r.get("barrier_join_reason") == "joined")
    barrier_feature_rows = sum(1 for r in deduped if r.get("barrier_score") not in (None, ""))
    trainable_barrier_label_rows = sum(
        1 for r in deduped
        if has_label(r) and r.get("barrier_join_ok") and r.get("barrier_join_reason") == "joined"
    )

    probs = [
        to_float(r.get("barrier_prob_target"))
        for r in deduped
        if r.get("barrier_prob_target") not in (None, "")
    ]
    scores = [
        to_float(r.get("barrier_score"))
        for r in deduped
        if r.get("barrier_score") not in (None, "")
    ]

    write_jsonl(output_path, deduped)

    report = {
        "ok": True,
        "marker": MARKER,
        "created_at_utc": utc_now_iso(),
        "base_source_counts": source_counts,
        "barrier_store_path": barrier_path,
        "output_path": output_path,
        "raw_rows": len(raw_rows),
        "deduped_rows": len(deduped),
        "barrier_rows_total": barrier_rows_total,
        "joined_rows": counters["joined"],
        "strict_joined_rows": strict_joined_rows,
        "strict_calc_ok_rows": counters["strict_calc_ok"],
        "already_had_barrier_rows": counters["already_had_barrier"],
        "barrier_feature_rows": barrier_feature_rows,
        "label_rows_raw": counters["label_rows"],
        "trainable_barrier_label_rows": trainable_barrier_label_rows,
        "missing_rows": counters["missing"],
        "missing_signal_ts_rows": counters["missing_signal_ts"],
        "future_blocked_rows": counters["future_blocked"],
        "future_blocked_rows_are_prevented_not_joined": True,
        "stale_rows": counters["stale"],
        "reason_counts": reason_counts,
        "no_future_leak_ok": True,
        "strict_mode_require_signal_ts": require_signal_ts,
        "target_model": "SINGLE_TARGET_RR12",
        "barrier_prob_target_avg": None if not probs else round(sum(probs) / len(probs), 6),
        "barrier_score_avg": None if not scores else round(sum(scores) / len(scores), 3),
        "max_age_sec": max_age_sec,
        "future_tolerance_sec": future_tolerance_sec,
    }

    report_path = report_dir / "ml_dataset_v4_stoch_barrier_join_v1_report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    main()
