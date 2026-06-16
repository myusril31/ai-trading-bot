import math
import statistics
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

WIB = ZoneInfo("Asia/Jakarta")

DEFAULT_SYMBOLS = [
    "BTCUSDT","ETHUSDT","SOLUSDT","PAXGUSDT","HYPEUSDT","XRPUSDT","UNIUSDT",
    "ADAUSDT","BCHUSDT","LINKUSDT","SUIUSDT","LTCUSDT","AVAXUSDT","DOTUSDT"
]

def _f(x: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        v = float(x)
        if math.isfinite(v):
            return v
    except Exception:
        pass
    return default

def _i(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default

def _closed_sorted(candles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for c in candles or []:
        if c.get("is_closed") is False:
            continue
        t = _i(c.get("open_time_ms"), 0)
        if t <= 0:
            continue
        if _f(c.get("close")) is None:
            continue
        out.append(c)
    return sorted(out, key=lambda x: _i(x.get("open_time_ms"), 0))

def _log(x: float) -> float:
    return math.log(max(float(x), 1e-12))

def _mean(xs: List[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0

def _stdev(xs: List[float]) -> float:
    if len(xs) < 2:
        return 0.0
    return statistics.pstdev(xs)

def _cov(a: List[float], b: List[float]) -> float:
    n = min(len(a), len(b))
    if n < 2:
        return 0.0
    aa = a[-n:]
    bb = b[-n:]
    ma = _mean(aa)
    mb = _mean(bb)
    return sum((x - ma) * (y - mb) for x, y in zip(aa, bb)) / n

def _returns(logs: List[float]) -> List[float]:
    return [logs[i] - logs[i - 1] for i in range(1, len(logs))]

def _z_last(xs: List[float]) -> Tuple[float, float, float]:
    if len(xs) < 10:
        return 0.0, 0.0, 0.0
    ref = xs[:-1] if len(xs) > 20 else xs
    mu = _mean(ref)
    sd = _stdev(ref)
    if sd <= 1e-12:
        return 0.0, mu, sd
    return (xs[-1] - mu) / sd, mu, sd

def _atr(candles: List[Dict[str, Any]], n: int = 14) -> Optional[float]:
    cs = _closed_sorted(candles)[-(n + 1):]
    if len(cs) < 8:
        return None
    trs = []
    prev_close = _f(cs[0].get("close"))
    for c in cs[1:]:
        hi = _f(c.get("high"))
        lo = _f(c.get("low"))
        close = _f(c.get("close"))
        if hi is None or lo is None or close is None:
            continue
        tr = max(hi - lo, abs(hi - prev_close), abs(lo - prev_close)) if prev_close is not None else hi - lo
        trs.append(max(0.0, tr))
        prev_close = close
    return _mean(trs[-n:]) if trs else None

def _volume_z(candles: List[Dict[str, Any]], n: int = 60) -> float:
    cs = _closed_sorted(candles)[-n:]
    vols = []
    for c in cs:
        v = _f(c.get("volume"), 0.0)
        if v is not None:
            vols.append(v)
    if len(vols) < 10:
        return 0.0
    sd = _stdev(vols[:-1])
    if sd <= 1e-12:
        return 0.0
    return (vols[-1] - _mean(vols[:-1])) / sd

def _wib_from_ms(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000.0, timezone.utc).astimezone(WIB).strftime("%Y-%m-%d %H:%M:%S WIB")

def _align_logs(pair_15, btc_15, eth_15, lookback: int):
    pd = {_i(c.get("open_time_ms")): _f(c.get("close")) for c in _closed_sorted(pair_15)}
    bd = {_i(c.get("open_time_ms")): _f(c.get("close")) for c in _closed_sorted(btc_15)}
    ed = {_i(c.get("open_time_ms")): _f(c.get("close")) for c in _closed_sorted(eth_15)}
    times = sorted(set(pd) & set(bd) & set(ed))[-lookback:]
    if len(times) < 80:
        return None

    y, x, clean_times = [], [], []
    for t in times:
        if pd[t] and bd[t] and ed[t]:
            y.append(_log(pd[t]))
            x.append((_log(bd[t]) + _log(ed[t])) / 2.0)
            clean_times.append(t)

    if len(y) < 80:
        return None
    return {"times": clean_times, "pair_log": y, "basket_log": x}

def _trend_4h(pair_4h, bars: int = 12) -> float:
    cs = _closed_sorted(pair_4h)
    if len(cs) < bars + 1:
        return 0.0
    p0 = _f(cs[-bars - 1].get("close"))
    p1 = _f(cs[-1].get("close"))
    if not p0 or not p1:
        return 0.0
    return _log(p1 / p0)

def _price_change(candles, bars: int) -> float:
    cs = _closed_sorted(candles)
    if len(cs) < bars + 1:
        return 0.0
    p0 = _f(cs[-bars - 1].get("close"))
    p1 = _f(cs[-1].get("close"))
    if not p0 or not p1:
        return 0.0
    return math.exp(_log(p1 / p0)) - 1.0

def _latest_price(candles_5m):
    cs = _closed_sorted(candles_5m)
    if not cs:
        return None, None, None
    last = cs[-1]
    return _f(last.get("close")), _i(last.get("open_time_ms")), _i(last.get("close_time_ms"))

def _build_plan(direction: str, entry: float, atr15: float, sl_mult: float):
    dist = max(entry * 0.0025, atr15 * sl_mult)
    if direction == "LONG":
        return {
            "entry": entry,
            "entry_mid": entry,
            "sl": entry - dist,
            "tp1": entry + dist * 0.75,
            "tp2": entry + dist * 1.25,
            "tp3": entry + dist * 1.75,
        }
    return {
        "entry": entry,
        "entry_mid": entry,
        "sl": entry + dist,
        "tp1": entry - dist * 0.75,
        "tp2": entry - dist * 1.25,
        "tp3": entry - dist * 1.75,
    }

def _score(abs_z, volume_z, z_slope, direction, trend4h, atr_pct, cfg):
    reasons = []
    score = 48.0

    score += min(30.0, max(0.0, abs_z - 1.5) * 18.0)
    reasons.append(f"residual_z_strength={abs_z:.2f}")

    if volume_z > 0:
        score += min(7.0, volume_z * 2.5)
        reasons.append(f"volume_confirm={volume_z:.2f}")

    if (direction == "SHORT" and z_slope < 0) or (direction == "LONG" and z_slope > 0):
        score += 6.0
        reasons.append("residual_turning_back")

    if (direction == "SHORT" and trend4h > 0.025) or (direction == "LONG" and trend4h < -0.025):
        score -= 8.0
        reasons.append("trend_4h_against_mean_reversion")

    if atr_pct <= 0:
        score -= 10.0
        reasons.append("atr_missing")
    elif atr_pct > float(cfg.get("max_atr_pct", 0.05)):
        score -= 12.0
        reasons.append("volatility_abnormal")
    else:
        score += 5.0
        reasons.append("volatility_normal")

    return int(max(0, min(100, round(score)))), reasons

def run_once(candles, symbols, cfg):
    model_version = str(cfg.get("model_version", "quant_v0.1"))
    family = str(cfg.get("model_family", "RESIDUAL_STAT_ARB"))
    lookback = int(cfg.get("lookback_15m", 160))
    min_z = float(cfg.get("min_abs_z", 2.0))
    min_score = int(cfg.get("min_score", 75))
    sl_atr_mult = float(cfg.get("sl_atr_mult", 1.20))

    btc15 = candles.get("BTCUSDT", {}).get("15m", [])
    eth15 = candles.get("ETHUSDT", {}).get("15m", [])

    signals = []
    diagnostics = []
    now = datetime.now(timezone.utc).isoformat()

    for sym in symbols:
        cset = candles.get(sym, {})
        pair15 = cset.get("15m", [])
        pair5 = cset.get("5m", [])
        pair4h = cset.get("4h", [])

        diag = {"symbol": sym, "status": "IDLE", "reason": None}
        aligned = _align_logs(pair15, btc15, eth15, lookback)
        entry, open_ms, close_ms = _latest_price(pair5)
        atr15 = _atr(pair15, 14)

        if aligned is None or entry is None or atr15 is None:
            diag.update({"status": "NO_DATA", "reason": "insufficient_aligned_candles_or_atr"})
            diagnostics.append(diag)
            continue

        y = aligned["pair_log"]
        x = aligned["basket_log"]
        ry = _returns(y)
        rx = _returns(x)

        varx = _cov(rx, rx)
        beta = _cov(ry, rx) / varx if varx > 1e-12 else 1.0

        spread = [yy - beta * xx for yy, xx in zip(y, x)]
        z, spread_mu, spread_sd = _z_last(spread)

        prev_window = spread[:-3] if len(spread) > 5 else spread[:-1]
        prev_z = _z_last(prev_window)[0] if len(prev_window) > 10 else 0.0
        z_slope = z - prev_z

        abs_z = abs(z)
        if abs_z < min_z:
            diag.update({"status": "IDLE", "reason": "residual_z_below_threshold", "residual_z": z})
            diagnostics.append(diag)
            continue

        direction = "SHORT" if z > 0 else "LONG"
        vol_z = _volume_z(pair15, 60)
        trend4h = _trend_4h(pair4h, 12)
        atr_pct = atr15 / entry if entry else 0.0

        score, score_reasons = _score(abs_z, vol_z, z_slope, direction, trend4h, atr_pct, cfg)
        if score < min_score:
            diag.update({"status": "WATCH", "reason": "score_below_min", "score": score, "residual_z": z})
            diagnostics.append(diag)
            continue

        plan = _build_plan(direction, entry, atr15, sl_atr_mult)
        signal_ms = int(close_ms or open_ms or 0)
        signal_key = f"QUANT|{sym}|{direction}|{signal_ms}"

        btc_ret_15m = _price_change(candles.get("BTCUSDT", {}).get("15m", []), 1)
        eth_ret_15m = _price_change(candles.get("ETHUSDT", {}).get("15m", []), 1)
        basket_ret_15m = (btc_ret_15m + eth_ret_15m) / 2.0

        features = {
            "residual_z": round(z, 6),
            "abs_residual_z": round(abs_z, 6),
            "residual_z_slope": round(z_slope, 6),
            "spread_mu": round(spread_mu, 8),
            "spread_sd": round(spread_sd, 8),
            "beta_basket": round(beta, 6),
            "volume_z": round(vol_z, 6),
            "atr15": round(atr15, 8),
            "atr_pct": round(atr_pct, 6),
            "trend_4h_12bar": round(trend4h, 6),
            "pair_ret_15m": round(_price_change(pair15, 1), 6),
            "pair_ret_1h": round(_price_change(pair15, 4), 6),
            "btc_ret_15m": round(btc_ret_15m, 6),
            "eth_ret_15m": round(eth_ret_15m, 6),
            "basket_ret_15m": round(basket_ret_15m, 6),
            "fee_slip_est": float(cfg.get("fee_slip_est", 0.0009)),
            "lookback_15m": lookback,
        }

        row = {
            "created_at_utc": now,
            "created_at_wib": datetime.now(WIB).isoformat(),
            "source_engine": "QUANT",
            "model_family": family,
            "model_version": model_version,
            "mode": str(cfg.get("mode", "SHADOW_ONLY")),
            "execution_enabled": False,
            "signal_key": signal_key,
            "symbol": sym,
            "pair": f"BINANCE:{sym}.P",
            "direction": direction,
            "status": "CONFIRMED",
            "score": score,
            "priority": "A" if score >= 85 else "B" if score >= 78 else "C",
            "signal_time_ms": signal_ms,
            "confirmed_bucket_ms": signal_ms,
            "signal_time_wib": _wib_from_ms(signal_ms),
            "eval_tf": str(cfg.get("eval_tf", "5m")),
            "max_forward_bars": int(cfg.get("max_forward_bars", 72)),
            **plan,
            "features": features,
            "score_reasons": score_reasons,
            "trigger": "RESIDUAL_Z_MEAN_REVERSION",
            "invalidation": "residual_z_expands_or_sl_hit_or_market_regime_flip",
        }

        signals.append(row)
        diag.update({"status": "CONFIRMED", "score": score, "direction": direction, "residual_z": z, "signal_key": signal_key})
        diagnostics.append(diag)

    return {
        "ok": True,
        "model_version": model_version,
        "model_family": family,
        "total_symbols": len(symbols),
        "confirmed_count": len(signals),
        "signals": signals,
        "diagnostics": diagnostics,
    }

def to_dataset_row(signal):
    row = dict(signal)
    row.update({
        "sample_type": "QUANT_SHADOW_SIGNAL",
        "include_ml": True,
        "include_ml_label": False,
        "execution_decision": "SHADOW_ONLY",
        "outcome_status": "PENDING",
        "label_target": None,
        "label_win": None,
        "label_R": None,
        "feature_source": "quant_engine_snapshot",
    })
    return row

def format_telegram(signal):
    f = signal.get("features") or {}
    lines = [
        "🧪 QUANT v0.1 SHADOW SIGNAL",
        str(signal.get("pair")),
        f"State: {signal.get('status')} | Dir: {signal.get('direction')} | Model: {signal.get('model_family')}",
        f"Priority: {signal.get('priority')} | Score: {signal.get('score')}",
        "Mode: SHADOW_ONLY | Execution: OFF",
        "",
        "Edge:",
        f"ResidualZ: {float(f.get('residual_z', 0)):+.2f}",
        f"BetaBasket: {float(f.get('beta_basket', 0)):.2f}",
        f"PairRet15m: {float(f.get('pair_ret_15m', 0))*100:+.2f}%",
        f"BasketRet15m: {float(f.get('basket_ret_15m', 0))*100:+.2f}%",
        f"VolumeZ: {float(f.get('volume_z', 0)):+.2f}",
        f"ATR%: {float(f.get('atr_pct', 0))*100:.2f}%",
        "Reason: residual/statistical mispricing + regime/cost screen pass.",
        "",
        f"ENTRY: {float(signal.get('entry_mid')):.8g}",
        f"SL: {float(signal.get('sl')):.8g}",
        f"TP1: {float(signal.get('tp1')):.8g}",
        f"TP2: {float(signal.get('tp2')):.8g}",
        f"TP3: {float(signal.get('tp3')):.8g}",
        "",
        "Forward Test:",
        "Outcome: PENDING",
        f"OutcomeKey: {signal.get('signal_key')}",
        f"EvalTF: {signal.get('eval_tf', '5m')}",
        f"MaxBars: {signal.get('max_forward_bars', 72)}",
    ]
    return "\n".join(lines)
