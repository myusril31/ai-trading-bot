#!/usr/bin/env python3
# TP_SL_PREDICTOR_V1_20260705
import os
import sys
import json
import math
from pathlib import Path
from datetime import datetime, timezone

MARKER = "TP_SL_PREDICTOR_V1_20260705"

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def env_float(k, default):
    try:
        return float(os.getenv(k, str(default)))
    except Exception:
        return float(default)

def env_int(k, default):
    try:
        return int(float(os.getenv(k, str(default))))
    except Exception:
        return int(default)

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

def read_jsonl(path, max_rows=None):
    p = Path(path)
    if not p.exists():
        return []
    lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()
    if max_rows:
        lines = lines[-max_rows:]
    rows = []
    for line in lines:
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                rows.append(obj)
        except Exception:
            pass
    return rows

def parse_boolish(v):
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        if v == 1:
            return True
        if v == 0:
            return False
    s = str(v or "").strip().upper()
    if s in ("1", "TRUE", "YES", "Y", "WIN", "TP", "TP1", "TP2", "TP3", "PROFIT"):
        return True
    if s in ("0", "FALSE", "NO", "N", "LOSS", "SL", "STOP", "STOPLOSS"):
        return False
    return None

def label_from_row(r):
    for k in ("label_win", "outcome_binary", "win", "is_win", "outcome_win"):
        if k in r and r.get(k) not in (None, ""):
            b = parse_boolish(r.get(k))
            if b is not None:
                return b

    txt = str(
        r.get("outcome_status")
        or r.get("label_target")
        or r.get("first_hit")
        or r.get("target")
        or r.get("close_reason")
        or ""
    ).upper()

    if any(x in txt for x in ("TP1", "TP2", "TP3", "TAKE_PROFIT", "WIN")):
        return True
    if any(x in txt for x in ("SL", "STOP", "STOPLOSS", "LOSS")):
        return False

    pnl = to_float(r.get("pnl") or r.get("realized_pnl") or r.get("net_pnl"))
    if pnl is not None:
        return pnl > 0

    return None

def beta_p(wins, total, alpha=2.0, beta=2.0):
    return (wins + alpha) / (total + alpha + beta)

def load_empirical_priors():
    log_dir = Path(os.getenv("LOG_DIR", "logs"))
    paths = [
        os.getenv("TP_SL_PREDICTOR_DATASET_PATH", str(log_dir / "ml_dataset_v4_outcome_forwarded_v1.jsonl")),
        str(log_dir / "ml_dataset_v4_ou_meanrev_join_v1.jsonl"),
        str(log_dir / "ml_dataset_v4_stoch_barrier_join_v1.jsonl"),
        str(log_dir / "ml_dataset_v4_linear_quant_join_v1.jsonl"),
        str(log_dir / "ml_dataset_v4_outcome_join_v1.jsonl"),
    ]
    max_rows = env_int("TP_SL_PREDICTOR_PRIOR_MAX_ROWS", 50000)

    buckets = {}
    total_seen = 0
    labeled = 0

    def add_bucket(key, win):
        w, n = buckets.get(key, (0, 0))
        buckets[key] = (w + (1 if win else 0), n + 1)

    for path in paths:
        for r in read_jsonl(path, max_rows=max_rows):
            total_seen += 1
            lab = label_from_row(r)
            if lab is None:
                continue
            labeled += 1

            sym = norm_symbol(r.get("symbol") or r.get("pair"))
            d = norm_dir(r.get("direction") or r.get("dir") or r.get("side"))
            setup = str(r.get("setup_type") or r.get("setup") or "UNKNOWN").upper()

            add_bucket(("GLOBAL",), lab)
            if sym:
                add_bucket(("SYMBOL", sym), lab)
            if d:
                add_bucket(("DIR", d), lab)
            if setup:
                add_bucket(("SETUP", setup), lab)
            if sym and d:
                add_bucket(("SYMBOL_DIR", sym, d), lab)
            if sym and d and setup:
                add_bucket(("COMBO", sym, d, setup), lab)

    return buckets, {"rows_seen": total_seen, "label_rows": labeled}

def empirical_prior(payload, buckets):
    sym = norm_symbol(payload.get("symbol") or payload.get("pair"))
    d = norm_dir(payload.get("direction") or payload.get("dir") or payload.get("side"))
    setup = str(payload.get("setup_type") or payload.get("setup") or "UNKNOWN").upper()

    min_n = env_int("TP_SL_PREDICTOR_MIN_BUCKET_N", 8)
    selected = []
    for key, weight in [
        (("COMBO", sym, d, setup), 0.42),
        (("SYMBOL_DIR", sym, d), 0.25),
        (("SYMBOL", sym), 0.13),
        (("DIR", d), 0.08),
        (("SETUP", setup), 0.07),
        (("GLOBAL",), 0.05),
    ]:
        if key in buckets:
            w, n = buckets[key]
            if n >= min_n or key == ("GLOBAL",):
                selected.append((beta_p(w, n), weight, w, n, "|".join(key)))

    if selected:
        sw = sum(x[1] for x in selected)
        p = sum(x[0] * x[1] for x in selected) / sw if sw > 0 else 0.5
        return clamp(p, 0.25, 0.80), selected

    q = (
        to_float(payload.get("stat_quant_score_raw"))
        or to_float(payload.get("quant_score"))
        or to_float(payload.get("core_quant_score"))
    )
    if q is not None:
        return clamp(0.50 + ((q - 60.0) / 200.0), 0.35, 0.68), [("fallback_quant", q)]

    return 0.50, [("fallback_neutral", 0)]

def score_to_prob(score, center=60.0, denom=180.0, lo=0.30, hi=0.72):
    x = to_float(score)
    if x is None:
        return None
    return clamp(0.50 + ((x - center) / denom), lo, hi)

def barrier_prob(payload):
    for k in ("barrier_prob_tp1", "barrier_prob_target", "prob_tp_before_sl"):
        p = to_float(payload.get(k))
        if p is not None:
            if p > 1.0:
                p = p / 100.0
            return clamp(p, 0.05, 0.95)
    bs = to_float(payload.get("barrier_score"))
    if bs is not None:
        return clamp(bs / 100.0, 0.05, 0.95)
    return None

def linear_prob(payload):
    return score_to_prob(payload.get("linear_quant_score"), center=60, denom=190, lo=0.32, hi=0.70)

def confluence_prob(payload):
    cs = to_float(payload.get("confluence_score"))
    if cs is not None:
        return score_to_prob(cs, center=60, denom=180, lo=0.35, hi=0.72)

    q = to_float(payload.get("quant_score"))
    tech = to_float(payload.get("technical_score"))
    if q is not None and tech is not None:
        raw = 0.55 * q + 0.45 * clamp(tech * 3.0, 0, 100)
        return score_to_prob(raw, center=60, denom=200, lo=0.35, hi=0.70)

    return None

def ou_prob(payload):
    score = to_float(payload.get("ou_score"))
    z = to_float(payload.get("ou_zscore"))
    direction = norm_dir(payload.get("direction") or payload.get("dir") or payload.get("side"))

    aligned = payload.get("ou_direction_aligned")
    if aligned is None and z is not None:
        if direction == "LONG":
            aligned = z < 0
        elif direction == "SHORT":
            aligned = z > 0

    if score is None and z is None:
        return None

    if score is None:
        score = 50.0

    p = score_to_prob(score, center=50, denom=220, lo=0.30, hi=0.70)

    if aligned is True:
        if z is not None:
            p += clamp(abs(z) / 8.0, 0.0, 0.08)
        return clamp(p, 0.40, 0.76)

    if aligned is False:
        penalty = 0.06
        if z is not None and abs(z) >= 1.0:
            penalty += 0.05
        if z is not None and abs(z) >= 2.0:
            penalty += 0.04
        return clamp(min(p, 0.49) - penalty, 0.20, 0.49)

    return clamp(p, 0.35, 0.65)

def resolve_entry_sl_tp(payload):
    entry = to_float(
        payload.get("entry")
        or payload.get("entry_price")
        or payload.get("limit_entry")
        or payload.get("entry_limit")
    )
    sl = to_float(payload.get("sl") or payload.get("stop_loss") or payload.get("stop"))
    tp = to_float(
        payload.get("tp1")
        or payload.get("tp")
        or payload.get("target")
        or payload.get("take_profit")
    )

    rr = to_float(payload.get("target_rr") or payload.get("rr") or payload.get("rr_target"), 1.2)

    if entry and sl and tp:
        risk = abs(entry - sl)
        reward = abs(tp - entry)
        if risk > 0:
            rr = reward / risk

    return entry, sl, tp, clamp(rr or 1.2, 0.3, 5.0)

def fee_r(payload, entry, sl):
    default_fee_r = env_float("TP_SL_PREDICTOR_DEFAULT_FEE_R", 0.06)
    if not entry or not sl or entry <= 0:
        return default_fee_r

    risk_pct = abs(entry - sl) / entry
    if risk_pct <= 1e-8:
        return default_fee_r

    roundtrip_fee_pct = env_float("TP_SL_PREDICTOR_ROUNDTRIP_FEE_PCT", 0.0010)
    slippage_pct = env_float("TP_SL_PREDICTOR_SLIPPAGE_PCT", 0.0002)
    fr = (roundtrip_fee_pct + slippage_pct) / risk_pct
    return clamp(fr, 0.0, env_float("TP_SL_PREDICTOR_MAX_FEE_R", 0.35))

def predict(payload):
    buckets, prior_meta = load_empirical_priors()

    p_hist, hist_detail = empirical_prior(payload, buckets)
    p_barrier = barrier_prob(payload)
    p_linear = linear_prob(payload)
    p_ou = ou_prob(payload)
    p_conf = confluence_prob(payload)

    components = {
        "hist": p_hist,
        "barrier": p_barrier,
        "linear": p_linear,
        "ou": p_ou,
        "confluence": p_conf,
    }

    base_weights = {
        "hist": env_float("TP_SL_PREDICTOR_W_HIST", 0.35),
        "barrier": env_float("TP_SL_PREDICTOR_W_BARRIER", 0.25),
        "linear": env_float("TP_SL_PREDICTOR_W_LINEAR", 0.18),
        "ou": env_float("TP_SL_PREDICTOR_W_OU", 0.12),
        "confluence": env_float("TP_SL_PREDICTOR_W_CONFLUENCE", 0.10),
    }

    active = {k: v for k, v in components.items() if v is not None}
    weight_sum = sum(base_weights[k] for k in active)
    if weight_sum <= 0:
        p_tp = 0.50
    else:
        p_tp = sum(active[k] * base_weights[k] for k in active) / weight_sum

    p_tp = clamp(p_tp, 0.01, 0.99)
    p_sl = 1.0 - p_tp

    entry, sl, tp, target_rr = resolve_entry_sl_tp(payload)
    fr = fee_r(payload, entry, sl)

    expected_r = p_tp * target_rr - p_sl * 1.0
    fee_adjusted_expected_r = expected_r - fr

    allow_p = env_float("TP_SL_PREDICTOR_ALLOW_P", 0.58)
    weak_p = env_float("TP_SL_PREDICTOR_WEAK_ALLOW_P", 0.52)
    min_fee_adj_r = env_float("TP_SL_PREDICTOR_MIN_FEE_ADJ_R", 0.00)

    if p_tp >= allow_p and fee_adjusted_expected_r > min_fee_adj_r:
        decision = "ALLOW"
        reason = "tp_probability_and_fee_adjusted_edge_ok"
    elif p_tp >= weak_p and fee_adjusted_expected_r > min_fee_adj_r:
        decision = "WEAK_ALLOW"
        reason = "weak_positive_edge"
    elif fee_adjusted_expected_r <= min_fee_adj_r:
        decision = "NO_TRADE"
        reason = "fee_adjusted_expected_r_not_positive"
    else:
        decision = "NO_TRADE"
        reason = "tp_probability_below_threshold"

    out = {
        "ok": True,
        "marker": MARKER,
        "created_at_utc": utc_now_iso(),
        "symbol": norm_symbol(payload.get("symbol") or payload.get("pair")),
        "direction": norm_dir(payload.get("direction") or payload.get("dir") or payload.get("side")),
        "setup_type": payload.get("setup_type") or payload.get("setup"),
        "signal_key": payload.get("signal_key") or payload.get("signal_id"),
        "p_tp_before_sl": round(p_tp, 6),
        "p_sl_before_tp": round(p_sl, 6),
        "target_rr": round(target_rr, 4),
        "fee_r": round(fr, 6),
        "expected_R": round(expected_r, 6),
        "fee_adjusted_expected_R": round(fee_adjusted_expected_r, 6),
        "predictive_edge": round(fee_adjusted_expected_r, 6),
        "decision": decision,
        "reason": reason,
        "components": {k: None if v is None else round(v, 6) for k, v in components.items()},
        "weights": {k: round(base_weights[k] / weight_sum, 6) for k in active} if weight_sum > 0 else {},
        "hist_detail": hist_detail[:6],
        "prior_meta": prior_meta,
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "source_payload_quant_score": payload.get("quant_score"),
        "source_barrier_score": payload.get("barrier_score"),
        "source_linear_quant_score": payload.get("linear_quant_score"),
        "source_ou_score": payload.get("ou_score"),
        "source_ou_zscore": payload.get("ou_zscore"),
        "mode": os.getenv("TP_SL_PREDICTOR_MODE", "ADVISORY"),
    }

    log_dir = Path(os.getenv("LOG_DIR", "logs"))
    log_dir.mkdir(parents=True, exist_ok=True)
    fp = Path(os.getenv("TP_SL_PREDICTOR_LOG_PATH", str(log_dir / "tp_sl_predictor_v1.jsonl")))
    with fp.open("a", encoding="utf-8") as f:
        f.write(json.dumps(out, ensure_ascii=False, separators=(",", ":")) + "\n")

    return out

def main():
    raw = sys.stdin.read().strip()
    if not raw:
        print(json.dumps({
            "ok": False,
            "marker": MARKER,
            "reason": "empty_stdin_payload",
            "usage": "echo '{...payload...}' | python scripts/tp_sl_predictor_v1.py"
        }, indent=2))
        return

    try:
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise ValueError("payload_not_object")
        out = predict(payload)
        print(json.dumps(out, ensure_ascii=False))
    except Exception as e:
        print(json.dumps({
            "ok": False,
            "marker": MARKER,
            "reason": f"{type(e).__name__}:{e}",
        }, ensure_ascii=False))

if __name__ == "__main__":
    main()
