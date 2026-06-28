#!/usr/bin/env python3
import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(".")
LOGS = ROOT / "logs"

DERIV_LOG = LOGS / "feature_store_binance_derivatives_v1.jsonl"
MACRO_LOG = LOGS / "feature_store_fred_macro_v1.jsonl"
GATE_LOG = LOGS / "live_entry_confluence_gate_events_v1.jsonl"

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def env_bool(k, default=False):
    v = os.getenv(k)
    if v is None:
        return bool(default)
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")

def env_float(k, default):
    try:
        return float(os.getenv(k, str(default)))
    except Exception:
        return float(default)

def to_float(x, default=None):
    try:
        if x is None or x == "":
            return default
        v = float(x)
        if math.isfinite(v):
            return v
        return default
    except Exception:
        return default

def parse_ts(x):
    try:
        if not x:
            return None
        return datetime.fromisoformat(str(x).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def age_seconds(ts):
    t = parse_ts(ts)
    if not t:
        return None
    return (datetime.now(timezone.utc) - t).total_seconds()

def append_jsonl(path, row):
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
    except Exception:
        pass

def read_latest_jsonl(path, symbol=None):
    if not path.exists():
        return None
    latest = None
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                if symbol is not None and str(r.get("symbol") or "").upper() != str(symbol).upper():
                    continue
                latest = r
    except Exception:
        return None
    return latest

def symbol_of(payload):
    return str(payload.get("symbol") or payload.get("pair") or "").upper().strip()

def direction_of(payload):
    d = (
        payload.get("direction")
        or payload.get("dir")
        or payload.get("side")
        or payload.get("trade_direction")
        or ""
    )
    d = str(d).upper().strip()
    if d in ("BUY", "BULL", "LONG"):
        return "LONG"
    if d in ("SELL", "BEAR", "SHORT"):
        return "SHORT"
    return d

def extract_quant_score(payload):
    for k in ("score", "priority", "quant_score", "core_quant_score", "smc_score"):
        v = to_float(payload.get(k))
        if v is not None:
            return v
    return None

def extract_pwin(payload):
    for k in ("p_win", "prob_win", "ml_p_win", "ml_prob_win", "probability_win"):
        v = to_float(payload.get(k))
        if v is not None:
            return v
    return None

def final_result(decision, reason, payload, deriv, macro, score, components, details=None):
    res = {
        "ok": True,
        "decision": decision,
        "allow": decision == "ALLOW",
        "gate": "live_entry_confluence_gate_v1",
        "reason": reason,
        "symbol": symbol_of(payload),
        "direction": direction_of(payload),
        "confluence_score": score,
        "components": components or {},
        "details": details or {},
        "created_at_utc": utc_now_iso(),
    }
    append_jsonl(GATE_LOG, {
        **res,
        "signal_key": payload.get("signal_key") or payload.get("signal_id"),
        "deriv_context": deriv,
        "macro_context": macro,
    })
    return res

def evaluate_live_entry_confluence_gate_v1(payload):
    payload = dict(payload or {})

    if not env_bool("LIVE_ENTRY_CONFLUENCE_GATE_ENABLED", False):
        return final_result("ALLOW", "gate_disabled", payload, None, None, None, {}, {})

    if str(os.getenv("LIVE_ENTRY_CONFLUENCE_MODE", "HARD_GATE")).upper() != "HARD_GATE":
        return final_result("ALLOW", "unsupported_mode_allow", payload, None, None, None, {}, {})

    fail_closed = env_bool("LIVE_ENTRY_CONFLUENCE_FAIL_CLOSED", True)
    symbol = symbol_of(payload)
    direction = direction_of(payload)

    if not symbol or direction not in ("LONG", "SHORT"):
        return final_result(
            "BLOCK" if fail_closed else "ALLOW",
            "missing_symbol_or_direction",
            payload, None, None, None, {}, {}
        )

    deriv = read_latest_jsonl(DERIV_LOG, symbol=symbol)
    macro = read_latest_jsonl(MACRO_LOG)

    max_deriv_age = env_float("LIVE_ENTRY_CONFLUENCE_MAX_DERIV_AGE_MIN", 30) * 60
    max_macro_age = env_float("LIVE_ENTRY_CONFLUENCE_MAX_MACRO_STORE_AGE_HOURS", 24) * 3600

    deriv_age = age_seconds((deriv or {}).get("created_at_utc"))
    macro_age = age_seconds((macro or {}).get("created_at_utc"))

    if not deriv or deriv_age is None or deriv_age > max_deriv_age:
        return final_result(
            "BLOCK" if fail_closed else "ALLOW",
            "deriv_missing_or_stale",
            payload, deriv, macro, None, {},
            {"deriv_age_sec": deriv_age, "max_deriv_age_sec": max_deriv_age}
        )

    if not macro or macro_age is None or macro_age > max_macro_age:
        return final_result(
            "BLOCK" if fail_closed else "ALLOW",
            "macro_missing_or_stale",
            payload, deriv, macro, None, {},
            {"macro_age_sec": macro_age, "max_macro_age_sec": max_macro_age}
        )

    quant_score = extract_quant_score(payload)
    pwin = extract_pwin(payload)

    min_quant = env_float("LIVE_ENTRY_CONFLUENCE_MIN_QUANT_SCORE", 60)
    if quant_score is not None and quant_score < min_quant:
        return final_result("BLOCK", "quant_score_below_min", payload, deriv, macro, None, {
            "quant_score": quant_score,
            "min_quant_score": min_quant,
        }, {})

    funding = to_float(deriv.get("deriv_funding_rate"), 0.0) or 0.0
    oi_chg = to_float(deriv.get("deriv_oi_chg_15m"), 0.0) or 0.0
    oi_z = to_float(deriv.get("deriv_oi_z_24h"), 0.0) or 0.0
    taker_imb = to_float(deriv.get("deriv_taker_imbalance_15m"), 0.0) or 0.0
    global_ls = to_float(deriv.get("deriv_global_long_short_ratio"), 1.0) or 1.0
    top_ls = to_float(deriv.get("deriv_top_pos_long_short_ratio"), 1.0) or 1.0
    liq_total = to_float(deriv.get("deriv_liq_total_notional_15m"), 0.0) or 0.0
    liq_imb = to_float(deriv.get("deriv_liq_imbalance_15m"), 0.0) or 0.0
    liq_ok = bool(deriv.get("deriv_liq_fetch_ok"))

    macro_risk_off = bool(macro.get("macro_regime_risk_off"))
    macro_liq_tight = bool(macro.get("macro_regime_liquidity_tight"))
    macro_stress = to_float(macro.get("macro_risk_stress_score_raw"), 0.0) or 0.0
    macro_liq_impulse = to_float(macro.get("macro_liquidity_impulse_raw"), 0.0) or 0.0

    # === LIVE_ENTRY_DIRECTION_BIAS_20260627 ===
    # Deriv + macro + quant ikut validasi arah.
    # Rule: jangan auto-flip direction. Kalau SMC LONG tapi context kuat SHORT, BLOCK.
    # Kalau mau SHORT, harus ada SMC SHORT setup sendiri. Bot bukan tukang salto.
    direction_bias_enabled = env_bool("LIVE_ENTRY_DIRECTION_BIAS_ENABLED", False)
    direction_conflict_block = env_bool("LIVE_ENTRY_DIRECTION_CONFLICT_BLOCK", True)
    direction_conflict_edge = env_float("LIVE_ENTRY_DIRECTION_CONFLICT_MIN_EDGE", 12)
    direction_min_bias_score = env_float("LIVE_ENTRY_DIRECTION_MIN_BIAS_SCORE", 55)

    long_bias_score = 50.0
    short_bias_score = 50.0
    direction_bias_components = {
        "long": {},
        "short": {},
    }

    # Macro bias.
    if macro_risk_off:
        short_bias_score += 6
        long_bias_score -= 6
        direction_bias_components["short"]["macro_risk_off"] = 6
        direction_bias_components["long"]["macro_risk_off"] = -6

    if macro_liq_tight:
        short_bias_score += 4
        long_bias_score -= 4
        direction_bias_components["short"]["macro_liquidity_tight"] = 4
        direction_bias_components["long"]["macro_liquidity_tight"] = -4

    if macro_stress <= 0 and not macro_liq_tight:
        long_bias_score += 4
        short_bias_score -= 2
        direction_bias_components["long"]["macro_stable"] = 4
        direction_bias_components["short"]["macro_stable"] = -2

    if macro_liq_impulse > 0:
        long_bias_score += 4
        direction_bias_components["long"]["macro_liquidity_impulse_positive"] = 4

    # Funding/crowding bias.
    if funding > 0.00005:
        short_bias_score += 7
        direction_bias_components["short"]["positive_funding"] = 7
    elif funding < -0.00005:
        long_bias_score += 7
        direction_bias_components["long"]["negative_funding"] = 7

    # Taker exhaustion / pressure bias.
    if taker_imb > 0.08:
        short_bias_score += 8
        direction_bias_components["short"]["taker_buy_pressure_fade"] = 8
    elif taker_imb < -0.08:
        long_bias_score += 8
        direction_bias_components["long"]["taker_sell_pressure_fade"] = 8
    else:
        long_bias_score += 2
        short_bias_score += 2
        direction_bias_components["long"]["taker_balanced"] = 2
        direction_bias_components["short"]["taker_balanced"] = 2

    # OI confirms intensity, not direction by itself. Add to both if expanding.
    if oi_chg > 0:
        long_bias_score += 2
        short_bias_score += 2
        direction_bias_components["long"]["oi_expansion"] = 2
        direction_bias_components["short"]["oi_expansion"] = 2

    # Long-short positioning bias.
    if global_ls >= 1.5:
        short_bias_score += 5
        direction_bias_components["short"]["global_ls_crowded_long"] = 5
    elif global_ls <= 0.85:
        long_bias_score += 5
        direction_bias_components["long"]["global_ls_crowded_short"] = 5

    if top_ls >= 1.5:
        short_bias_score += 5
        direction_bias_components["short"]["top_pos_crowded_long"] = 5
    elif top_ls <= 0.85:
        long_bias_score += 5
        direction_bias_components["long"]["top_pos_crowded_short"] = 5

    # Liquidation bias:
    # liq_imb < 0 means more long liquidation pressure/purge, can support LONG reclaim.
    # liq_imb > 0 means more short liquidation pressure/purge, can support SHORT reject.
    if liq_ok and liq_total > 0:
        if liq_imb < -0.35:
            long_bias_score += 8
            direction_bias_components["long"]["long_liq_purge"] = 8
        elif liq_imb > 0.35:
            short_bias_score += 8
            direction_bias_components["short"]["short_liq_purge"] = 8

    # Quant/p_win helps current SMC direction, not opposite direction.
    if quant_score is not None and quant_score >= 70:
        if direction == "LONG":
            long_bias_score += 5
            direction_bias_components["long"]["quant_supports_smc_direction"] = 5
        elif direction == "SHORT":
            short_bias_score += 5
            direction_bias_components["short"]["quant_supports_smc_direction"] = 5

    if pwin is not None and pwin >= 0.74:
        if direction == "LONG":
            long_bias_score += 5
            direction_bias_components["long"]["pwin_supports_smc_direction"] = 5
        elif direction == "SHORT":
            short_bias_score += 5
            direction_bias_components["short"]["pwin_supports_smc_direction"] = 5

    directional_bias = "LONG" if long_bias_score > short_bias_score else "SHORT" if short_bias_score > long_bias_score else "NEUTRAL"
    directional_edge = abs(long_bias_score - short_bias_score)

    if direction_bias_enabled and direction_conflict_block:
        if direction == "LONG" and short_bias_score - long_bias_score >= direction_conflict_edge:
            return final_result("BLOCK", "directional_conflict_short_bias", payload, deriv, macro, None, {
                "long_bias_score": long_bias_score,
                "short_bias_score": short_bias_score,
                "directional_bias": directional_bias,
                "directional_edge": directional_edge,
                "direction_bias_components": direction_bias_components,
            }, {})

        if direction == "SHORT" and long_bias_score - short_bias_score >= direction_conflict_edge:
            return final_result("BLOCK", "directional_conflict_long_bias", payload, deriv, macro, None, {
                "long_bias_score": long_bias_score,
                "short_bias_score": short_bias_score,
                "directional_bias": directional_bias,
                "directional_edge": directional_edge,
                "direction_bias_components": direction_bias_components,
            }, {})

        # Kalau bias terlalu lemah dan confluence minimal juga ketat, biarin score final yang mutusin.
        # Jadi tidak asal block kalau context netral.

    # HARD MACRO VETO: alt long pas risk stress + liquidity tight.
    if direction == "LONG" and symbol != "BTCUSDT":
        stress_block = env_float("LIVE_ENTRY_MACRO_ALT_LONG_STRESS_BLOCK", 0.50)
        if macro_risk_off and macro_liq_tight and macro_stress >= stress_block:
            return final_result("BLOCK", "macro_blocks_alt_long", payload, deriv, macro, None, {
                "macro_risk_off": macro_risk_off,
                "macro_liquidity_tight": macro_liq_tight,
                "macro_stress": macro_stress,
            }, {})

    # HARD DERIV CROWDING VETO.
    long_funding_block = env_float("LIVE_ENTRY_LONG_CROWDED_FUNDING_BLOCK", 0.00025)
    short_funding_block = env_float("LIVE_ENTRY_SHORT_CROWDED_FUNDING_BLOCK", -0.00025)
    oi_crowd = env_float("LIVE_ENTRY_OI_CHG_CROWD_BLOCK", 0.005)
    taker_crowd = env_float("LIVE_ENTRY_TAKER_IMB_CROWD_BLOCK", 0.15)
    global_ls_long = env_float("LIVE_ENTRY_GLOBAL_LS_LONG_CROWD", 2.00)
    top_ls_long = env_float("LIVE_ENTRY_TOP_LS_LONG_CROWD", 1.80)
    global_ls_short = env_float("LIVE_ENTRY_GLOBAL_LS_SHORT_CROWD", 0.75)
    top_ls_short = env_float("LIVE_ENTRY_TOP_LS_SHORT_CROWD", 0.75)

    if direction == "LONG":
        if funding >= long_funding_block and oi_chg >= oi_crowd and taker_imb >= taker_crowd and global_ls >= global_ls_long and top_ls >= top_ls_long:
            return final_result("BLOCK", "deriv_crowded_long_blocks_long", payload, deriv, macro, None, {
                "funding": funding, "oi_chg": oi_chg, "taker_imb": taker_imb, "global_ls": global_ls, "top_ls": top_ls
            }, {})

    if direction == "SHORT":
        if funding <= short_funding_block and oi_chg >= oi_crowd and taker_imb <= -abs(taker_crowd) and global_ls <= global_ls_short and top_ls <= top_ls_short:
            return final_result("BLOCK", "deriv_crowded_short_blocks_short", payload, deriv, macro, None, {
                "funding": funding, "oi_chg": oi_chg, "taker_imb": taker_imb, "global_ls": global_ls, "top_ls": top_ls
            }, {})

    score = 50.0
    components = {
        "base_smc_confirmed": _env_float("LIVE_ENTRY_CONFLUENCE_SMC_BASE_SCORE", 20.0),
        "deriv": 0.0,
        "macro": 0.0,
        "quant": 0.0,
        "liq": 0.0,
    }

    if quant_score is not None:
        if quant_score >= 80:
            components["quant"] += 12
        elif quant_score >= 70:
            components["quant"] += 8
        elif quant_score >= 60:
            components["quant"] += 3
        else:
            components["quant"] -= 10

    if pwin is not None:
        if pwin >= 0.74:
            components["quant"] += 10
        elif pwin >= 0.70:
            components["quant"] += 5
        elif pwin < 0.60:
            components["quant"] -= 8

    if direction == "LONG":
        if macro_risk_off:
            components["macro"] -= 8
        if macro_liq_tight:
            components["macro"] -= 6
        if macro_stress <= 0:
            components["macro"] += 4
        if macro_liq_impulse > 0:
            components["macro"] += 4

        if funding < -0.00005:
            components["deriv"] += 8
        elif funding <= 0.00015:
            components["deriv"] += 4
        elif funding > 0.00025:
            components["deriv"] -= 8

        if oi_chg > 0:
            components["deriv"] += 5
        if oi_z > 1.5:
            components["deriv"] += 3

        if taker_imb < -0.08:
            components["deriv"] += 8
        elif -0.08 <= taker_imb <= 0.08:
            components["deriv"] += 3
        elif taker_imb > 0.15:
            components["deriv"] -= 8

        if global_ls < 1.0:
            components["deriv"] += 6
        elif global_ls > 2.0:
            components["deriv"] -= 8

        if top_ls < 1.0:
            components["deriv"] += 5
        elif top_ls > 2.0:
            components["deriv"] -= 7

        if liq_ok and liq_total > 0 and liq_imb < -0.35:
            components["liq"] += 8

    if direction == "SHORT":
        if macro_risk_off:
            components["macro"] += 5
        if macro_liq_tight:
            components["macro"] += 3
        if macro_stress <= 0 and not macro_liq_tight:
            components["macro"] -= 3

        if funding > 0.00005:
            components["deriv"] += 8
        elif funding >= -0.00015:
            components["deriv"] += 3
        elif funding < -0.00025:
            components["deriv"] -= 8

        if oi_chg > 0:
            components["deriv"] += 5
        if oi_z > 1.5:
            components["deriv"] += 3

        if taker_imb > 0.08:
            components["deriv"] += 8
        elif -0.08 <= taker_imb <= 0.08:
            components["deriv"] += 3
        elif taker_imb < -0.15:
            components["deriv"] -= 8

        if global_ls > 1.5:
            components["deriv"] += 6
        elif global_ls < 0.75:
            components["deriv"] -= 8

        if top_ls > 1.5:
            components["deriv"] += 5
        elif top_ls < 0.75:
            components["deriv"] -= 7

        if liq_ok and liq_total > 0 and liq_imb > 0.35:
            components["liq"] += 8

    score += components["deriv"] + components["macro"] + components["quant"] + components["liq"]

    min_score = env_float("LIVE_ENTRY_CONFLUENCE_MIN_SCORE", 70)
    # === LIVE_ENTRY_CONFLUENCE_SMC_BASE20_REWEIGHT_20260628 ===
    # Minimal SMC is binary gate only. It must not dominate confluence scoring.
    # Existing deriv/quant components from legacy scale are boosted here so final score is
    # driven by deriv + macro + quant, not by SMC base.
    if _env_bool("LIVE_ENTRY_CONFLUENCE_REWEIGHT_MINIMAL_SMC", True):
        components["base_smc_confirmed"] = _env_float("LIVE_ENTRY_CONFLUENCE_SMC_BASE_SCORE", 20.0)

        deriv_mult = _env_float("LIVE_ENTRY_CONFLUENCE_DERIV_MULT", 1.45)
        quant_mult = _env_float("LIVE_ENTRY_CONFLUENCE_QUANT_MULT", 2.00)
        macro_mult = _env_float("LIVE_ENTRY_CONFLUENCE_MACRO_MULT", 1.00)
        liq_mult = _env_float("LIVE_ENTRY_CONFLUENCE_LIQ_MULT", 1.00)

        components["deriv"] = max(0.0, min(_env_float("LIVE_ENTRY_CONFLUENCE_DERIV_CAP", 35.0), float(components.get("deriv") or 0.0) * deriv_mult))
        components["quant"] = max(0.0, min(_env_float("LIVE_ENTRY_CONFLUENCE_QUANT_CAP", 30.0), float(components.get("quant") or 0.0) * quant_mult))
        components["macro"] = max(_env_float("LIVE_ENTRY_CONFLUENCE_MACRO_FLOOR", -15.0), min(_env_float("LIVE_ENTRY_CONFLUENCE_MACRO_CAP", 10.0), float(components.get("macro") or 0.0) * macro_mult))
        components["liq"] = max(0.0, min(_env_float("LIVE_ENTRY_CONFLUENCE_LIQ_CAP", 10.0), float(components.get("liq") or 0.0) * liq_mult))

        score = round(max(0.0, min(100.0, sum(float(v or 0.0) for v in components.values()))), 1)

    details = {
        "min_score": min_score,
        "deriv_age_sec": deriv_age,
        "macro_age_sec": macro_age,
        "quant_score": quant_score,
        "pwin": pwin,
        "funding": funding,
        "oi_chg_15m": oi_chg,
        "oi_z_24h": oi_z,
        "taker_imbalance_15m": taker_imb,
        "global_long_short_ratio": global_ls,
        "top_pos_long_short_ratio": top_ls,
        "liq_total_notional_15m": liq_total,
        "liq_imbalance_15m": liq_imb,
        "macro_risk_off": macro_risk_off,
        "macro_liquidity_tight": macro_liq_tight,
        "macro_risk_stress_score_raw": macro_stress,
        "macro_liquidity_impulse_raw": macro_liq_impulse,
        "long_bias_score": long_bias_score,
        "short_bias_score": short_bias_score,
        "directional_bias": directional_bias,
        "directional_edge": directional_edge,
        "direction_min_bias_score": direction_min_bias_score,
        "direction_bias_components": direction_bias_components,
    }

    if score < min_score:
        return final_result("BLOCK", "confluence_score_below_min", payload, deriv, macro, score, components, details)

    return final_result("ALLOW", "confluence_pass", payload, deriv, macro, score, components, details)
