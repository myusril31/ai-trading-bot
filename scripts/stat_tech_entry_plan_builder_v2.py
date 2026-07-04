#!/usr/bin/env python3
# STAT_TECH_ENTRY_PLAN_BUILDER_V2_20260705
import os
import sys
import json
import math
import subprocess
from pathlib import Path
from datetime import datetime, timezone

MARKER = "STAT_TECH_ENTRY_PLAN_BUILDER_V2_20260705"

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def env_float(k, default):
    try:
        return float(os.getenv(k, str(default)))
    except Exception:
        return float(default)

def env_bool(k, default=False):
    v = os.getenv(k)
    if v is None:
        return bool(default)
    return str(v).strip().lower() in ("1", "true", "yes", "on")

def clamp(x, lo, hi):
    return max(lo, min(hi, x))

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

def first_float(payload, keys):
    for k in keys:
        v = to_float(payload.get(k))
        if v is not None:
            return v
    return None

def nested_get_float(payload, paths):
    for path in paths:
        cur = payload
        ok = True
        for part in path.split("."):
            if isinstance(cur, dict) and part in cur:
                cur = cur.get(part)
            else:
                ok = False
                break
        if ok:
            v = to_float(cur)
            if v is not None:
                return v
    return None

def resolve_raw_plan(payload):
    direction = norm_dir(payload.get("direction") or payload.get("dir") or payload.get("side"))
    entry = first_float(payload, [
        "entry", "entry_price", "limit_entry", "entry_limit",
        "planned_entry", "raw_entry", "final_entry"
    ])
    sl = first_float(payload, [
        "sl", "stop_loss", "stop", "planned_sl", "raw_sl", "final_sl"
    ])
    tp = first_float(payload, [
        "tp1", "tp", "target", "take_profit", "planned_tp", "raw_tp", "final_tp"
    ])

    rr = to_float(payload.get("target_rr") or payload.get("rr") or payload.get("rr_target") or payload.get("raw_rr"), 1.2)
    rr = clamp(rr, 0.3, 5.0)

    if entry is not None and sl is not None:
        risk = abs(entry - sl)
        if risk > 0:
            if tp is None:
                if direction == "LONG":
                    tp = entry + risk * rr
                elif direction == "SHORT":
                    tp = entry - risk * rr
            else:
                reward = abs(tp - entry)
                if reward > 0:
                    rr = reward / risk

    return {
        "direction": direction,
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "rr": rr,
    }

def resolve_swing(payload):
    high = first_float(payload, [
        "swing_high", "recent_swing_high", "range_high", "htf_swing_high",
        "fib_swing_high", "structure_high", "last_swing_high"
    ])
    low = first_float(payload, [
        "swing_low", "recent_swing_low", "range_low", "htf_swing_low",
        "fib_swing_low", "structure_low", "last_swing_low"
    ])

    if high is None:
        high = nested_get_float(payload, [
            "structure.swing_high",
            "htf.swing_high",
            "htf_summary.swing_high",
            "market_structure.swing_high",
            "fib.swing_high",
        ])
    if low is None:
        low = nested_get_float(payload, [
            "structure.swing_low",
            "htf.swing_low",
            "htf_summary.swing_low",
            "market_structure.swing_low",
            "fib.swing_low",
        ])

    if high is not None and low is not None and high > low:
        return high, low, "payload_swing"

    return None, None, "missing_swing"

def fib_geometry(payload, entry, sl, tp, direction):
    high, low, source = resolve_swing(payload)
    out = {
        "ok": False,
        "source": source,
        "swing_high": high,
        "swing_low": low,
        "fib_entry_retracement": None,
        "fib_tp_extension": None,
        "fib_target_quality": "UNKNOWN",
        "fib_entry_zone": "UNKNOWN",
        "fib_chasing_penalty": False,
        "fib_sl_quality": "UNKNOWN",
        "fib_geometry_score": 50.0,
        "fib_suggested_rr": None,
    }

    if high is None or low is None or entry is None or sl is None or tp is None:
        return out

    rng = high - low
    if rng <= 0:
        return out

    if direction == "LONG":
        # Pullback depth from high toward low. Higher retracement = deeper discount.
        entry_ret = (high - entry) / rng
        tp_ext = (tp - low) / rng
        sl_beyond_swing = sl < low
    elif direction == "SHORT":
        # Pullback depth from low toward high. Higher retracement = deeper premium.
        entry_ret = (entry - low) / rng
        tp_ext = (high - tp) / rng
        sl_beyond_swing = sl > high
    else:
        return out

    entry_ret = clamp(entry_ret, -1.0, 2.0)
    tp_ext = clamp(tp_ext, -1.0, 3.0)

    score = 50.0

    # Entry zone.
    if entry_ret < 0.236:
        zone = "CHASING"
        score -= 22
        chasing = True
    elif entry_ret < 0.382:
        zone = "SHALLOW"
        score -= 8
        chasing = False
    elif entry_ret <= 0.705:
        zone = "VALUE"
        score += 20
        chasing = False
    elif entry_ret <= 0.786:
        zone = "DEEP_VALUE"
        score += 12
        chasing = False
    elif entry_ret <= 1.0:
        zone = "TOO_DEEP"
        score -= 6
        chasing = False
    else:
        zone = "BROKEN_RANGE"
        score -= 18
        chasing = True

    # Target extension quality.
    if tp_ext < 0.80:
        target_quality = "TOO_CLOSE_INSIDE_RANGE"
        score -= 5
        suggested_rr = 0.8
    elif tp_ext <= 1.272:
        target_quality = "GOOD"
        score += 15
        suggested_rr = 1.2
    elif tp_ext <= 1.414:
        target_quality = "OK_STRETCHED"
        score += 8
        suggested_rr = 1.1
    elif tp_ext <= 1.618:
        target_quality = "STRETCHED"
        score -= 4
        suggested_rr = 1.0
    else:
        target_quality = "TOO_FAR"
        score -= 18
        suggested_rr = 0.9

    # SL quality.
    if sl_beyond_swing:
        sl_quality = "BEYOND_SWING"
        score += 10
    else:
        sl_quality = "INSIDE_SWING_NOISE"
        score -= 10

    out.update({
        "ok": True,
        "source": source,
        "fib_entry_retracement": round(entry_ret, 6),
        "fib_tp_extension": round(tp_ext, 6),
        "fib_target_quality": target_quality,
        "fib_entry_zone": zone,
        "fib_chasing_penalty": bool(chasing),
        "fib_sl_quality": sl_quality,
        "fib_geometry_score": round(clamp(score, 0.0, 100.0), 1),
        "fib_suggested_rr": round(clamp(suggested_rr, 0.8, 1.5), 2),
    })
    return out

def fee_r(entry, sl):
    default_fee_r = env_float("ENTRY_PLAN_BUILDER_DEFAULT_FEE_R", 0.06)
    if entry is None or sl is None or entry <= 0:
        return default_fee_r

    risk_pct = abs(entry - sl) / entry
    if risk_pct <= 1e-8:
        return default_fee_r

    roundtrip_fee_pct = env_float("ENTRY_PLAN_BUILDER_ROUNDTRIP_FEE_PCT", 0.0010)
    slippage_pct = env_float("ENTRY_PLAN_BUILDER_SLIPPAGE_PCT", 0.0002)
    fr = (roundtrip_fee_pct + slippage_pct) / risk_pct
    return clamp(fr, 0.0, env_float("ENTRY_PLAN_BUILDER_MAX_FEE_R", 0.35))

def p_from_payload(payload):
    for k in ("tp_sl_p_tp", "p_tp_before_sl", "barrier_prob_tp1", "barrier_prob_target"):
        p = to_float(payload.get(k))
        if p is not None:
            if p > 1:
                p = p / 100.0
            return clamp(p, 0.05, 0.95), f"payload:{k}"

    bs = to_float(payload.get("barrier_score"))
    if bs is not None:
        return clamp(bs / 100.0, 0.05, 0.95), "payload:barrier_score"

    q = to_float(payload.get("quant_score"))
    if q is not None:
        return clamp(0.50 + ((q - 60.0) / 180.0), 0.30, 0.70), "payload:quant_score"

    return 0.50, "fallback:neutral"

def expected_r(p_tp, rr, fr):
    p_tp = clamp(p_tp, 0.01, 0.99)
    p_sl = 1.0 - p_tp
    er = p_tp * rr - p_sl * 1.0
    return er, er - fr

def target_from_rr(entry, sl, direction, rr):
    risk = abs(entry - sl)
    if direction == "LONG":
        return entry + risk * rr
    if direction == "SHORT":
        return entry - risk * rr
    return None

def call_predictor(payload, entry, sl, tp, rr):
    script = os.getenv("ENTRY_PLAN_BUILDER_PREDICTOR_SCRIPT", "scripts/tp_sl_predictor_v1.py")
    if not Path(script).exists():
        return None

    pp = dict(payload)
    pp["signal_source"] = pp.get("signal_source") or "STAT_TECH_ENTRY_PLAN_BUILDER_V2"
    pp["entry"] = entry
    pp["sl"] = sl
    pp["tp1"] = tp
    pp["tp"] = tp
    pp["target"] = tp
    pp["target_rr"] = rr
    pp["rr"] = rr

    try:
        proc = subprocess.run(
            ["python", script],
            input=json.dumps(pp, ensure_ascii=False),
            text=True,
            capture_output=True,
            timeout=env_float("ENTRY_PLAN_BUILDER_PREDICTOR_TIMEOUT_SEC", 8),
        )
        if proc.returncode != 0:
            return {"ok": False, "reason": "predictor_nonzero", "stderr": (proc.stderr or "")[-500:]}
        out = json.loads((proc.stdout or "{}").strip() or "{}")
        return out if isinstance(out, dict) else None
    except Exception as e:
        return {"ok": False, "reason": f"predictor_exception:{type(e).__name__}:{e}"}

def choose_candidate_rr(payload, raw_rr, fib, p_raw):
    candidate = raw_rr
    reasons = []

    min_rr = env_float("ENTRY_PLAN_BUILDER_MIN_RR", 0.8)
    max_rr = env_float("ENTRY_PLAN_BUILDER_MAX_RR", 1.5)

    barrier_p = None
    for k in ("barrier_prob_tp1", "barrier_prob_target"):
        barrier_p = to_float(payload.get(k))
        if barrier_p is not None:
            if barrier_p > 1:
                barrier_p /= 100.0
            break

    linear = to_float(payload.get("linear_quant_score"))
    ou_score = to_float(payload.get("ou_score"))
    ou_z = to_float(payload.get("ou_zscore"))
    direction = norm_dir(payload.get("direction") or payload.get("dir") or payload.get("side"))

    ou_conflict = False
    if ou_z is not None:
        if direction == "LONG" and ou_z > 0:
            ou_conflict = True
        if direction == "SHORT" and ou_z < 0:
            ou_conflict = True

    if fib.get("ok"):
        sg = to_float(fib.get("fib_suggested_rr"))
        if sg is not None:
            if sg < candidate:
                candidate = sg
                reasons.append(f"fib_suggested_rr:{sg}")

        if fib.get("fib_chasing_penalty"):
            candidate = min(candidate, 0.9)
            reasons.append("fib_chasing_penalty")

        if fib.get("fib_target_quality") == "TOO_FAR":
            candidate = min(candidate, 0.9)
            reasons.append("fib_target_too_far")
        elif fib.get("fib_target_quality") == "STRETCHED":
            candidate = min(candidate, 1.0)
            reasons.append("fib_target_stretched")

    if ou_conflict:
        candidate = min(candidate, 1.0)
        reasons.append("ou_direction_conflict")
        if ou_z is not None and abs(ou_z) >= 1.5:
            candidate = min(candidate, 0.9)
            reasons.append("ou_conflict_severe")

    if barrier_p is not None and barrier_p < 0.54:
        candidate = min(candidate, 0.9)
        reasons.append("barrier_prob_weak")

    # Only consider extension if everything supports direction.
    if (
        barrier_p is not None and barrier_p >= 0.68
        and linear is not None and linear >= 72
        and not ou_conflict
        and fib.get("fib_chasing_penalty") is not True
        and str(fib.get("fib_target_quality")) in ("GOOD", "OK_STRETCHED", "UNKNOWN")
    ):
        candidate = max(candidate, min(raw_rr, 1.2))
        reasons.append("momentum_support_keep_rr")

    candidate = round(clamp(candidate, min_rr, max_rr), 4)
    if not reasons:
        reasons.append("keep_raw_rr")
    return candidate, reasons

def estimate_adjusted_probability(p_raw, raw_rr, candidate_rr, fib, payload):
    p = p_raw

    # Closer target should increase probability, farther target should reduce.
    if candidate_rr < raw_rr:
        p += (raw_rr - candidate_rr) * env_float("ENTRY_PLAN_BUILDER_CLOSE_TARGET_P_BOOST_PER_R", 0.35)
    elif candidate_rr > raw_rr:
        p -= (candidate_rr - raw_rr) * env_float("ENTRY_PLAN_BUILDER_FAR_TARGET_P_PENALTY_PER_R", 0.25)

    if fib.get("ok"):
        if fib.get("fib_entry_zone") in ("VALUE", "DEEP_VALUE"):
            p += 0.035
        if fib.get("fib_chasing_penalty"):
            p -= 0.065
        if fib.get("fib_sl_quality") == "BEYOND_SWING":
            p += 0.025
        if fib.get("fib_target_quality") == "GOOD":
            p += 0.030
        elif fib.get("fib_target_quality") == "TOO_FAR":
            p -= 0.050

    ou_z = to_float(payload.get("ou_zscore"))
    direction = norm_dir(payload.get("direction") or payload.get("dir") or payload.get("side"))
    if ou_z is not None:
        if direction == "LONG" and ou_z > 0:
            p -= min(0.10, abs(ou_z) * 0.025)
        if direction == "SHORT" and ou_z < 0:
            p -= min(0.10, abs(ou_z) * 0.025)
        if direction == "LONG" and ou_z < 0:
            p += min(0.07, abs(ou_z) * 0.02)
        if direction == "SHORT" and ou_z > 0:
            p += min(0.07, abs(ou_z) * 0.02)

    return round(clamp(p, 0.05, 0.90), 6)

def build_plan(payload):
    raw = resolve_raw_plan(payload)
    direction = raw["direction"]
    entry = raw["entry"]
    sl = raw["sl"]
    raw_tp = raw["tp"]
    raw_rr = raw["rr"]

    symbol = norm_symbol(payload.get("symbol") or payload.get("pair"))
    setup_type = payload.get("setup_type") or payload.get("setup")

    if direction not in ("LONG", "SHORT") or entry is None or sl is None:
        return {
            "ok": False,
            "marker": MARKER,
            "created_at_utc": utc_now_iso(),
            "symbol": symbol,
            "direction": direction,
            "setup_type": setup_type,
            "plan_decision": "INVALID",
            "plan_reason": "missing_direction_entry_or_sl",
        }

    if raw_tp is None:
        raw_tp = target_from_rr(entry, sl, direction, raw_rr)

    if raw_tp is None:
        return {
            "ok": False,
            "marker": MARKER,
            "created_at_utc": utc_now_iso(),
            "symbol": symbol,
            "direction": direction,
            "setup_type": setup_type,
            "plan_decision": "INVALID",
            "plan_reason": "missing_tp_and_cannot_compute",
        }

    # sanity direction
    if direction == "LONG" and not (sl < entry < raw_tp):
        return {
            "ok": False, "marker": MARKER, "created_at_utc": utc_now_iso(),
            "symbol": symbol, "direction": direction, "setup_type": setup_type,
            "plan_decision": "INVALID",
            "plan_reason": "long_plan_sanity_failed",
            "raw_entry": entry, "raw_sl": sl, "raw_tp": raw_tp,
        }

    if direction == "SHORT" and not (raw_tp < entry < sl):
        return {
            "ok": False, "marker": MARKER, "created_at_utc": utc_now_iso(),
            "symbol": symbol, "direction": direction, "setup_type": setup_type,
            "plan_decision": "INVALID",
            "plan_reason": "short_plan_sanity_failed",
            "raw_entry": entry, "raw_sl": sl, "raw_tp": raw_tp,
        }

    fib = fib_geometry(payload, entry, sl, raw_tp, direction)
    p_raw, p_source = p_from_payload(payload)
    raw_fr = fee_r(entry, sl)
    raw_er, raw_fee_er = expected_r(p_raw, raw_rr, raw_fr)

    raw_pred = call_predictor(payload, entry, sl, raw_tp, raw_rr)
    if raw_pred and raw_pred.get("ok"):
        pp = to_float(raw_pred.get("p_tp_before_sl"))
        if pp is not None:
            p_raw = pp
            p_source = "tp_sl_predictor_v1"
            raw_er = to_float(raw_pred.get("expected_R"), raw_er)
            raw_fee_er = to_float(raw_pred.get("fee_adjusted_expected_R"), raw_fee_er)

    candidate_rr, rr_reasons = choose_candidate_rr(payload, raw_rr, fib, p_raw)
    candidate_tp = target_from_rr(entry, sl, direction, candidate_rr)
    p_candidate = estimate_adjusted_probability(p_raw, raw_rr, candidate_rr, fib, payload)
    cand_fr = fee_r(entry, sl)
    cand_er, cand_fee_er = expected_r(p_candidate, candidate_rr, cand_fr)

    # Try predictor again with candidate TP; keep our adjusted p if predictor is unavailable.
    cand_pred = call_predictor(payload, entry, sl, candidate_tp, candidate_rr)
    if cand_pred and cand_pred.get("ok"):
        cp = to_float(cand_pred.get("p_tp_before_sl"))
        if cp is not None:
            # Blend predictor with geometry-adjusted estimate.
            p_candidate = round(clamp(0.55 * cp + 0.45 * p_candidate, 0.05, 0.90), 6)
            cand_er, cand_fee_er = expected_r(p_candidate, candidate_rr, cand_fr)

    min_improvement = env_float("ENTRY_PLAN_BUILDER_MIN_FEE_ADJ_R_IMPROVEMENT", 0.015)
    min_adjust_rr_diff = env_float("ENTRY_PLAN_BUILDER_MIN_ADJUST_RR_DIFF", 0.05)
    mode = os.getenv("ENTRY_PLAN_BUILDER_MODE", "AUTO_PRE_ENTRY").strip().upper()
    auto_adjust = mode in ("AUTO", "AUTO_PRE_ENTRY", "EXECUTE_PRE_ENTRY")

    target_adjusted = False
    final_rr = raw_rr
    final_tp = raw_tp
    final_p = p_raw
    final_er = raw_er
    final_fee_er = raw_fee_er
    adjustment_reason = "keep_raw_plan"

    if auto_adjust and abs(candidate_rr - raw_rr) >= min_adjust_rr_diff:
        better_edge = cand_fee_er >= raw_fee_er + min_improvement
        still_tradeable = candidate_rr >= env_float("ENTRY_PLAN_BUILDER_MIN_RR", 0.8)
        if better_edge and still_tradeable:
            target_adjusted = True
            final_rr = candidate_rr
            final_tp = candidate_tp
            final_p = p_candidate
            final_er = cand_er
            final_fee_er = cand_fee_er
            adjustment_reason = "auto_pre_entry_adjust:" + ",".join(rr_reasons)
        else:
            adjustment_reason = "candidate_rejected:not_enough_edge_improvement"

    p_sl = 1.0 - final_p

    if final_fee_er > env_float("ENTRY_PLAN_BUILDER_ALLOW_MIN_FEE_ADJ_R", 0.0) and final_p >= env_float("ENTRY_PLAN_BUILDER_ALLOW_MIN_P_TP", 0.52):
        decision = "ALLOW"
        reason = "positive_fee_adjusted_edge"
    elif final_fee_er > -0.05:
        decision = "WEAK_ALLOW"
        reason = "weak_or_near_flat_edge"
    else:
        decision = "NO_TRADE"
        reason = "negative_fee_adjusted_edge"

    return {
        "ok": True,
        "marker": MARKER,
        "created_at_utc": utc_now_iso(),
        "symbol": symbol,
        "direction": direction,
        "setup_type": setup_type,
        "signal_key": payload.get("signal_key") or payload.get("signal_id"),

        "raw_entry": round(entry, 12),
        "raw_sl": round(sl, 12),
        "raw_tp": round(raw_tp, 12),
        "raw_rr": round(raw_rr, 6),
        "raw_p_tp_before_sl": round(p_raw, 6),
        "raw_expected_R": round(raw_er, 6),
        "raw_fee_adjusted_expected_R": round(raw_fee_er, 6),

        "candidate_tp": round(candidate_tp, 12) if candidate_tp is not None else None,
        "candidate_rr": round(candidate_rr, 6),
        "candidate_p_tp_before_sl": round(p_candidate, 6),
        "candidate_expected_R": round(cand_er, 6),
        "candidate_fee_adjusted_expected_R": round(cand_fee_er, 6),
        "candidate_reasons": rr_reasons,

        "final_entry": round(entry, 12),
        "final_sl": round(sl, 12),
        "final_tp": round(final_tp, 12),
        "final_rr": round(final_rr, 6),
        "p_tp_before_sl": round(final_p, 6),
        "p_sl_before_tp": round(p_sl, 6),
        "expected_R": round(final_er, 6),
        "fee_adjusted_expected_R": round(final_fee_er, 6),
        "predictive_edge": round(final_fee_er, 6),

        "target_model": "SINGLE_TARGET_ADAPTIVE_RR",
        "target_adjusted": bool(target_adjusted),
        "target_adjustment_source": "ENTRY_PLAN_BUILDER_V2_FIB_GEOMETRY_PLUS_TP_SL_PREDICTOR",
        "target_adjustment_reason": adjustment_reason,

        "plan_decision": decision,
        "plan_reason": reason,
        "mode": mode,
        "p_source": p_source,

        "fib_geometry": fib,
        "fib_geometry_score": fib.get("fib_geometry_score"),
        "fib_entry_retracement": fib.get("fib_entry_retracement"),
        "fib_tp_extension": fib.get("fib_tp_extension"),
        "fib_target_quality": fib.get("fib_target_quality"),
        "fib_entry_zone": fib.get("fib_entry_zone"),
        "fib_chasing_penalty": fib.get("fib_chasing_penalty"),
        "fib_sl_quality": fib.get("fib_sl_quality"),
        "fib_suggested_rr": fib.get("fib_suggested_rr"),

        "linear_quant_score": payload.get("linear_quant_score"),
        "barrier_score": payload.get("barrier_score"),
        "barrier_prob_tp1": payload.get("barrier_prob_tp1"),
        "ou_score": payload.get("ou_score"),
        "ou_zscore": payload.get("ou_zscore"),
    }

def append_log_and_report(plan):
    log_dir = Path(os.getenv("LOG_DIR", "logs"))
    report_dir = Path(os.getenv("REPORT_DIR", "reports"))
    log_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    fp = log_dir / "stat_tech_entry_plan_builder_v2.jsonl"
    with fp.open("a", encoding="utf-8") as f:
        f.write(json.dumps(plan, ensure_ascii=False, separators=(",", ":")) + "\n")

    rows = []
    try:
        lines = fp.read_text(encoding="utf-8", errors="ignore").splitlines()[-1000:]
        for line in lines:
            try:
                rows.append(json.loads(line))
            except Exception:
                pass
    except Exception:
        rows = []

    report = {
        "ok": True,
        "marker": MARKER,
        "created_at_utc": utc_now_iso(),
        "rows_lookback": len(rows),
        "target_adjusted_rows": sum(1 for r in rows if r.get("target_adjusted") is True),
        "decision_counts": {},
        "target_quality_counts": {},
        "avg_final_rr": None,
        "avg_p_tp": None,
        "avg_fee_adjusted_expected_R": None,
        "latest": plan,
    }

    for r in rows:
        d = str(r.get("plan_decision") or "UNKNOWN")
        report["decision_counts"][d] = report["decision_counts"].get(d, 0) + 1
        q = str(r.get("fib_target_quality") or "UNKNOWN")
        report["target_quality_counts"][q] = report["target_quality_counts"].get(q, 0) + 1

    rr_vals = [to_float(r.get("final_rr")) for r in rows if to_float(r.get("final_rr")) is not None]
    p_vals = [to_float(r.get("p_tp_before_sl")) for r in rows if to_float(r.get("p_tp_before_sl")) is not None]
    edge_vals = [to_float(r.get("fee_adjusted_expected_R")) for r in rows if to_float(r.get("fee_adjusted_expected_R")) is not None]

    if rr_vals:
        report["avg_final_rr"] = round(sum(rr_vals) / len(rr_vals), 6)
    if p_vals:
        report["avg_p_tp"] = round(sum(p_vals) / len(p_vals), 6)
    if edge_vals:
        report["avg_fee_adjusted_expected_R"] = round(sum(edge_vals) / len(edge_vals), 6)

    (report_dir / "stat_tech_entry_plan_builder_v2_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")

def main():
    raw = sys.stdin.read().strip()
    if not raw:
        print(json.dumps({"ok": False, "marker": MARKER, "plan_decision": "ERROR", "plan_reason": "empty_stdin_payload"}))
        return

    try:
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise ValueError("payload_not_object")
        plan = build_plan(payload)
        append_log_and_report(plan)
        print(json.dumps(plan, ensure_ascii=False))
    except Exception as e:
        out = {
            "ok": False,
            "marker": MARKER,
            "created_at_utc": utc_now_iso(),
            "plan_decision": "ERROR",
            "plan_reason": f"{type(e).__name__}:{e}",
        }
        append_log_and_report(out)
        print(json.dumps(out, ensure_ascii=False))

if __name__ == "__main__":
    main()
