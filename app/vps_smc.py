import json
import os
from datetime import datetime, timedelta, timezone
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

EXECUTION_BRIDGE_HANDLER = None


def register_execution_bridge_handler(handler) -> None:
    global EXECUTION_BRIDGE_HANDLER
    EXECUTION_BRIDGE_HANDLER = handler


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return int(str(raw).strip())
    except Exception:
        return default


def _normalize_symbol(raw: Any) -> str:
    s = str(raw or "").strip().upper()
    if ":" in s:
        s = s.split(":", 1)[1]
    s = s.replace(".P", "")
    s = s.replace("/", "")
    s = s.replace("-", "")
    return s


def _csv_set(name: str) -> set[str]:
    raw = str(os.getenv(name, "")).strip()
    out: set[str] = set()
    for item in raw.replace(";", ",").split(","):
        sym = _normalize_symbol(item)
        if sym:
            out.add(sym)
    return out


def _log_dir() -> Path:
    return Path(os.getenv("LOG_DIR", "logs"))


def _state_dir() -> Path:
    return Path(os.getenv("STATE_DIR", "state"))


def _market_data_dir() -> Path:
    raw = str(os.getenv("BINANCE_CANDLE_STORE_DIR") or "").strip()
    return Path(raw) if raw else _state_dir() / "market_data"


def _ensure_dirs() -> None:
    _log_dir().mkdir(parents=True, exist_ok=True)
    (_state_dir() / "vps_smc").mkdir(parents=True, exist_ok=True)


def _append_jsonl(path: Path, row: Dict[str, Any]) -> None:
    _ensure_dirs()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    out: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except Exception:
                continue
            if isinstance(obj, dict):
                out.append(obj)
    return out


def _state_file() -> Path:
    return _state_dir() / "vps_smc" / "state.json"


def _default_state() -> Dict[str, Any]:
    return {
        "last_run_utc": None,
        "last_signal_count": 0,
        "last_error": None,
        "last_symbols": [],
        "last_compare_utc": None,
        "last_compare_counts": {},
        "last_gsheet_mirror_utc": None,
        "last_gsheet_mirror_counts": {},
        "last_scheduler_run_utc": None,
        "last_scheduler_status": "IDLE",
        "last_scheduler_error": None,
        "gsheet_mirrored_keys": {"shadow": {}, "compare": {}},
        "gsheet_headers_written": {"shadow": False, "compare": False},
        "scheduler_running": False,
        "logged_signal_keys": {},
        "updated_at_utc": _utc_now_iso(),
    }


def _load_state() -> Dict[str, Any]:
    path = _state_file()
    if not path.exists():
        return _default_state()
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return _default_state()
        base = _default_state()
        base.update(data)
        return base
    except Exception:
        return _default_state()


def _save_state(state: Dict[str, Any]) -> None:
    _ensure_dirs()
    path = _state_file()
    state["updated_at_utc"] = _utc_now_iso()
    tmp = path.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def _intervals() -> Dict[str, str]:
    return {
        "htf": str(os.getenv("VPS_SMC_INTERVAL_HTF", "4h")).strip() or "4h",
        "entry": str(os.getenv("VPS_SMC_INTERVAL_ENTRY", "15m")).strip() or "15m",
        "stageb": str(os.getenv("VPS_SMC_INTERVAL_STAGEB", "5m")).strip() or "5m",
    }


def _symbols_fallback() -> List[str]:
    allow = sorted(_csv_set("PAIR_ALLOWLIST"))
    return allow or ["BTCUSDT", "ETHUSDT", "UNIUSDT"]


def _parse_iso_utc(raw: Any) -> Optional[datetime]:
    if raw is None:
        return None
    txt = str(raw).strip()
    if not txt:
        return None
    try:
        if txt.endswith("Z"):
            txt = txt[:-1] + "+00:00"
        dt = datetime.fromisoformat(txt)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _normalize_direction(raw: Any) -> Optional[str]:
    d = str(raw or "").strip().upper()
    if d in ("LONG", "BUY"):
        return "LONG"
    if d in ("SHORT", "SELL"):
        return "SHORT"
    return None


def _candles_file(symbol: str, interval: str) -> Path:
    return _market_data_dir() / f"{symbol}_{interval}.jsonl"


def _load_internal_candles(symbol: str, interval: str) -> List[Dict[str, Any]]:
    rows = _read_jsonl(_candles_file(symbol, interval))
    out: List[Dict[str, Any]] = []
    use_closed_only = _env_bool("VPS_SMC_USE_CLOSED_CANDLES_ONLY", True)
    for row in rows:
        try:
            if use_closed_only and not bool(row.get("is_closed", False)):
                continue
            open_time_ms = int(row.get("open_time_ms"))
            close_time_ms = int(row.get("close_time_ms"))
            out.append({
                "t": close_time_ms,
                "tBucketMs": open_time_ms,
                "o": float(row.get("open")),
                "h": float(row.get("high")),
                "l": float(row.get("low")),
                "c": float(row.get("close")),
                "v": float(row.get("volume")),
            })
        except Exception:
            continue
    out.sort(key=lambda x: int(x.get("tBucketMs") or 0))
    return out



def detect_pivots(candles: List[Dict[str, Any]], left: int = 2, right: int = 2) -> Dict[str, List[Dict[str, Any]]]:
    swings_h: List[Dict[str, Any]] = []
    swings_l: List[Dict[str, Any]] = []
    n = len(candles)
    if n < (left + right + 1):
        return {"swing_highs": swings_h, "swing_lows": swings_l}
    for i in range(left, n - right):
        h = float(candles[i]["h"])
        l = float(candles[i]["l"])
        left_h = [float(candles[j]["h"]) for j in range(i - left, i)]
        right_h = [float(candles[j]["h"]) for j in range(i + 1, i + 1 + right)]
        left_l = [float(candles[j]["l"]) for j in range(i - left, i)]
        right_l = [float(candles[j]["l"]) for j in range(i + 1, i + 1 + right)]
        if all(h > x for x in left_h + right_h):
            swings_h.append({"idx": i, "t": candles[i]["t"], "price": h, "type": "SWING_HIGH"})
        if all(l < x for x in left_l + right_l):
            swings_l.append({"idx": i, "t": candles[i]["t"], "price": l, "type": "SWING_LOW"})
    return {"swing_highs": swings_h, "swing_lows": swings_l}


def build_swing_summary(candles: List[Dict[str, Any]]) -> Dict[str, Any]:
    pivots = detect_pivots(candles)
    last_h = pivots["swing_highs"][-1] if pivots["swing_highs"] else None
    last_l = pivots["swing_lows"][-1] if pivots["swing_lows"] else None
    return {
        "last_swing_high": (last_h or {}).get("price"),
        "last_swing_high_t": (last_h or {}).get("t"),
        "last_swing_low": (last_l or {}).get("price"),
        "last_swing_low_t": (last_l or {}).get("t"),
        "swing_high_count": len(pivots["swing_highs"]),
        "swing_low_count": len(pivots["swing_lows"]),
    }


def detect_equal_high_low(candles: List[Dict[str, Any]], pivots: Dict[str, List[Dict[str, Any]]], band_pct: float = 0.15) -> Dict[str, List[Dict[str, Any]]]:
    recent_start = max(0, len(candles) - 50)
    highs = [x for x in pivots.get("swing_highs", []) if int(x.get("idx", -1)) >= recent_start]
    lows = [x for x in pivots.get("swing_lows", []) if int(x.get("idx", -1)) >= recent_start]

    def _group(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        groups: List[Dict[str, Any]] = []
        for item in items:
            price = float(item["price"])
            found = None
            for g in groups:
                mid = float(g["price"])
                tol = abs(mid) * (band_pct / 100.0)
                if tol == 0:
                    tol = 1e-12
                if abs(price - mid) <= tol:
                    found = g
                    break
            if found is None:
                groups.append({"price": price, "count": 1, "t": item["t"], "band_pct": band_pct})
            else:
                found["count"] += 1
                found["price"] = (float(found["price"]) * (found["count"] - 1) + price) / found["count"]
                found["t"] = item["t"]
        return [g for g in groups if int(g["count"]) >= 2]

    return {"eqh": _group(highs), "eql": _group(lows)}


def detect_sweep(candles: List[Dict[str, Any]], swing_summary: Dict[str, Any], scan_n: int = 20) -> Dict[str, Any]:
    out = {
        "bullish_sweep": False,
        "bearish_sweep": False,
        "sweep_level": None,
        "sweep_extreme": None,
        "sweep_t": None,
        "sweep_tag": None,
    }
    if not candles:
        return out
    scan = candles[-max(1, scan_n):]
    last_low = swing_summary.get("last_swing_low")
    last_high = swing_summary.get("last_swing_high")
    if last_low is not None:
        for c in reversed(scan):
            if float(c["l"]) < float(last_low) and float(c["c"]) > float(last_low):
                out.update({"bullish_sweep": True, "sweep_level": float(last_low), "sweep_extreme": float(c["l"]), "sweep_t": c["t"], "sweep_tag": "SWEEP_LOW"})
                break
    if last_high is not None:
        for c in reversed(scan):
            if float(c["h"]) > float(last_high) and float(c["c"]) < float(last_high):
                if not out["bullish_sweep"]:
                    out.update({"sweep_level": float(last_high), "sweep_extreme": float(c["h"]), "sweep_t": c["t"], "sweep_tag": "SWEEP_HIGH"})
                out["bearish_sweep"] = True
                break
    return out


def detect_fvg(candles: List[Dict[str, Any]], lookback: int = 35) -> Dict[str, Any]:
    out = {"bullish_fvg": None, "bearish_fvg": None, "count_bullish": 0, "count_bearish": 0}
    if len(candles) < 3:
        return out
    start = max(2, len(candles) - max(3, lookback))
    for i in range(start, len(candles)):
        c0 = candles[i - 2]
        c2 = candles[i]
        if float(c0["h"]) < float(c2["l"]):
            out["count_bullish"] += 1
            out["bullish_fvg"] = {"lo": float(c0["h"]), "hi": float(c2["l"]), "t": c2["t"], "idx": i}
        if float(c0["l"]) > float(c2["h"]):
            out["count_bearish"] += 1
            out["bearish_fvg"] = {"lo": float(c2["h"]), "hi": float(c0["l"]), "t": c2["t"], "idx": i}
    return out


def calc_atr(candles: List[Dict[str, Any]], length: int = 10) -> Optional[float]:
    if len(candles) < (length + 1):
        return None
    trs: List[float] = []
    start = len(candles) - length
    for i in range(start, len(candles)):
        c = candles[i]
        prev = candles[i - 1]
        high = float(c["h"])
        low = float(c["l"])
        prev_close = float(prev["c"])
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    if not trs:
        return None
    return sum(trs) / len(trs)


def detect_displacement(candles: List[Dict[str, Any]], direction: str, atr_len: int, atr_mult: float, min_body_pct: float, lookback: int) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "has_displacement": False, "direction": None, "displacement_t": None, "displacement_idx": None,
        "body_pct": None, "range": None, "atr": None, "reason": None,
    }
    if direction not in ("LONG", "SHORT"):
        out["reason"] = "invalid_direction"
        return out
    scan = candles[-max(1, lookback):] if candles else []
    for offset in range(len(scan) - 1, -1, -1):
        c = scan[offset]
        full_idx = len(candles) - len(scan) + offset
        atr_window_end = full_idx + 1
        atr_window = candles[:atr_window_end]
        atr = calc_atr(atr_window, atr_len)
        if atr is None:
            continue
        o = float(c["o"])
        h = float(c["h"])
        l = float(c["l"])
        close = float(c["c"])
        crange = h - l
        if crange <= 0:
            continue
        body = close - o
        body_abs = abs(body)
        body_pct = (body_abs / crange) * 100.0
        directional_ok = (direction == "LONG" and body > 0) or (direction == "SHORT" and body < 0)
        range_ok = crange >= (atr * atr_mult)
        body_ok = body_pct >= min_body_pct
        if directional_ok and range_ok and body_ok:
            out.update({
                "has_displacement": True,
                "direction": direction,
                "displacement_t": c.get("t"),
                "displacement_idx": full_idx,
                "body_pct": body_pct,
                "range": crange,
                "atr": atr,
                "reason": "ok",
            })
            return out
    out["reason"] = "not_found"
    return out


def detect_reclaim(stageb_candles: List[Dict[str, Any]], direction: str, reclaim_level: Optional[float], lookback: int) -> Dict[str, Any]:
    out: Dict[str, Any] = {"has_reclaim": False, "reclaim_level": reclaim_level, "reclaim_t": None, "reclaim_idx": None, "reason": None}
    if reclaim_level is None:
        out["reason"] = "missing_reclaim_level"
        return out
    if direction not in ("LONG", "SHORT"):
        out["reason"] = "invalid_direction"
        return out
    scan = stageb_candles[-max(1, lookback):] if stageb_candles else []
    for offset in range(len(scan) - 1, -1, -1):
        c = scan[offset]
        full_idx = len(stageb_candles) - len(scan) + offset
        low = float(c["l"])
        high = float(c["h"])
        close = float(c["c"])
        if direction == "LONG" and low < float(reclaim_level) and close > float(reclaim_level):
            out.update({"has_reclaim": True, "reclaim_t": c.get("t"), "reclaim_idx": full_idx, "reason": "ok"})
            return out
        if direction == "SHORT" and high > float(reclaim_level) and close < float(reclaim_level):
            out.update({"has_reclaim": True, "reclaim_t": c.get("t"), "reclaim_idx": full_idx, "reason": "ok"})
            return out
    out["reason"] = "not_found"
    return out


def select_fvg_poi(stageb_fvg: Dict[str, Any], direction: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {"has_fvg": False, "fvg_type": None, "fvg_lo": None, "fvg_hi": None, "fvg_t": None, "reason": None}
    if direction == "LONG":
        fvg = (stageb_fvg or {}).get("bullish_fvg")
        if fvg:
            out.update({"has_fvg": True, "fvg_type": "BULLISH", "fvg_lo": fvg.get("lo"), "fvg_hi": fvg.get("hi"), "fvg_t": fvg.get("t"), "reason": "ok"})
            return out
        out["reason"] = "missing_bullish_fvg"
        return out
    if direction == "SHORT":
        fvg = (stageb_fvg or {}).get("bearish_fvg")
        if fvg:
            out.update({"has_fvg": True, "fvg_type": "BEARISH", "fvg_lo": fvg.get("lo"), "fvg_hi": fvg.get("hi"), "fvg_t": fvg.get("t"), "reason": "ok"})
            return out
        out["reason"] = "missing_bearish_fvg"
        return out
    out["reason"] = "invalid_direction"
    return out


def derive_stageb_direction(context: Dict[str, Any]) -> Dict[str, Any]:
    sweep = (context or {}).get("entry_sweep") or {}
    bullish = bool(sweep.get("bullish_sweep"))
    bearish = bool(sweep.get("bearish_sweep"))
    direction = "NONE"
    reason = "no_sweep_direction"
    if bullish and bearish:
        direction = "MIXED"
        reason = "mixed_sweep_direction"
    elif bullish:
        direction = "LONG"
        reason = "bullish_sweep"
    elif bearish:
        direction = "SHORT"
        reason = "bearish_sweep"
    return {"stageb_direction": direction, "direction_reason": reason}


def build_stageb_confirmation(result: Dict[str, Any], stageb_candles: List[Dict[str, Any]]) -> Dict[str, Any]:
    direction_info = derive_stageb_direction(result)
    direction = direction_info["stageb_direction"]
    context_status = str(result.get("context_status") or "ERROR")
    liq_gate_status = str(result.get("liq_gate_status") or "BLOCK")

    out: Dict[str, Any] = {
        "stageb_status": "INVALID",
        "stageb_direction": direction,
        "stageb_reclaim": {"has_reclaim": False, "reclaim_level": None, "reclaim_t": None, "reclaim_idx": None, "reason": "not_built"},
        "stageb_displacement": {"has_displacement": False, "direction": None, "displacement_t": None, "displacement_idx": None, "body_pct": None, "range": None, "atr": None, "reason": "not_built"},
        "stageb_fvg_poi": {"has_fvg": False, "fvg_type": None, "fvg_lo": None, "fvg_hi": None, "fvg_t": None, "reason": "not_built"},
        "stageb_confirm_reason": None,
        "stageb_invalid_reason": None,
    }

    if context_status in ("DATA_GAP", "HTF_DATA_GAP", "ERROR"):
        out["stageb_invalid_reason"] = "context_not_ready"
        return out
    if direction == "MIXED":
        out["stageb_invalid_reason"] = "mixed_sweep_direction"
        return out
    if direction == "NONE":
        out["stageb_status"] = "IDLE"
        out["stageb_confirm_reason"] = "no_sweep_direction"
        out["stageb_invalid_reason"] = None
        return out

    reclaim_level = (result.get("liq_ctx") or {}).get("sweep_level")
    if reclaim_level is None:
        reclaim_level = (result.get("liq_ctx") or {}).get("nearest_liq_price")

    out["stageb_reclaim"] = detect_reclaim(stageb_candles, direction, reclaim_level, _env_int("VPS_SMC_RECLAIM_LOOKBACK_BARS", 20))
    out["stageb_displacement"] = detect_displacement(
        stageb_candles,
        direction,
        _env_int("VPS_SMC_DISPLACEMENT_ATR_LEN", 10),
        _env_float("VPS_SMC_DISPLACEMENT_ATR_MULT", 1.15),
        _env_float("VPS_SMC_DISPLACEMENT_MIN_BODY_PCT", 55.0),
        _env_int("VPS_SMC_STAGEB_LOOKBACK_BARS", 60),
    )
    out["stageb_fvg_poi"] = select_fvg_poi((result.get("stageb_fvg") or {}), direction)

    require_reclaim = _env_bool("VPS_SMC_REQUIRE_RECLAIM", True)
    require_displacement = _env_bool("VPS_SMC_REQUIRE_DISPLACEMENT", True)
    require_fvg = _env_bool("VPS_SMC_REQUIRE_FVG_FOR_CONFIRM", True)

    reclaim_ok = bool(out["stageb_reclaim"].get("has_reclaim")) or (not require_reclaim)
    displacement_ok = bool(out["stageb_displacement"].get("has_displacement")) or (not require_displacement)
    fvg_ok = bool(out["stageb_fvg_poi"].get("has_fvg")) or (not require_fvg)

    out["stageb_status"] = "WATCH"
    reasons: List[str] = []
    if liq_gate_status == "BLOCK":
        reasons.append("liq_gate_blocked")
    if reclaim_ok and displacement_ok and fvg_ok and context_status == "READY" and liq_gate_status != "BLOCK":
        out["stageb_status"] = "CONFIRMED"
        reasons.append("all_requirements_passed")
    else:
        if not reclaim_ok:
            reasons.append("reclaim_missing")
        if not displacement_ok:
            reasons.append("displacement_missing")
        if not fvg_ok:
            reasons.append("fvg_missing")
    out["stageb_confirm_reason"] = ",".join(reasons) if reasons else "watch"
    return out
def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return float(str(raw).strip())
    except Exception:
        return default


def build_htf_gate(htf: List[Dict[str, Any]], htf_swing_summary: Dict[str, Any]) -> Dict[str, Any]:
    min_candles = _env_int("VPS_SMC_HTF_MIN_CANDLES", 30)
    eq_band_pct = _env_float("VPS_SMC_HTF_EQ_BAND_PCT", 0.30)
    if len(htf) < min_candles:
        return {
            "htf_gate_status": "HTF_DATA_GAP", "htf_dir": "NEUTRAL", "htf_bias": "MIXED", "htf_structure": "UNKNOWN",
            "htf_location": "UNKNOWN", "htf_close": None, "htf_last_swing_high": None, "htf_last_swing_low": None,
            "htf_mid": None, "htf_reason": "not_enough_4h_candles",
        }

    close = float(htf[-1]["c"])
    last_high = htf_swing_summary.get("last_swing_high")
    last_low = htf_swing_summary.get("last_swing_low")
    htf_mid = None
    location = "UNKNOWN"
    structure = "RANGE"
    direction = "NEUTRAL"
    bias = "MIXED"
    if last_high is not None and last_low is not None:
        last_high = float(last_high)
        last_low = float(last_low)
        htf_mid = (last_high + last_low) / 2.0
        band = abs(htf_mid) * (eq_band_pct / 100.0)
        if band == 0:
            band = 1e-12
        if abs(close - htf_mid) <= band:
            location = "EQUILIBRIUM"
        elif close > htf_mid:
            location = "PREMIUM"
        else:
            location = "DISCOUNT"
        if close > last_high:
            structure, direction, bias = "BOS_UP", "LONG", "BULLISH"
        elif close < last_low:
            structure, direction, bias = "BOS_DOWN", "SHORT", "BEARISH"
    return {
        "htf_gate_status": "PASS",
        "htf_dir": direction,
        "htf_bias": bias,
        "htf_structure": structure,
        "htf_location": location,
        "htf_close": close,
        "htf_last_swing_high": last_high,
        "htf_last_swing_low": last_low,
        "htf_mid": htf_mid,
        "htf_reason": None,
    }


def build_liquidity_context(entry: List[Dict[str, Any]], entry_swing_summary: Dict[str, Any], entry_eq: Dict[str, Any], entry_sweep: Dict[str, Any]) -> Dict[str, Any]:
    near_liq_pct = _env_float("VPS_SMC_NEAR_LIQ_PCT", 0.35)
    require_near = _env_bool("VPS_SMC_REQUIRE_AT_OR_NEAR_LIQ", True)
    close = float(entry[-1]["c"]) if entry else None
    last_high = entry_swing_summary.get("last_swing_high")
    last_low = entry_swing_summary.get("last_swing_low")
    eqh = entry_eq.get("eqh") or []
    eql = entry_eq.get("eql") or []
    zones: List[Dict[str, Any]] = []
    if last_high is not None:
        zones.append({"type": "SWING_HIGH", "price": float(last_high)})
    if last_low is not None:
        zones.append({"type": "SWING_LOW", "price": float(last_low)})
    for z in eqh:
        if z.get("price") is not None:
            zones.append({"type": "EQH", "price": float(z["price"])})
    for z in eql:
        if z.get("price") is not None:
            zones.append({"type": "EQL", "price": float(z["price"])})

    nearest = None
    if close is not None and close != 0 and zones:
        nearest = min(zones, key=lambda z: abs(close - float(z["price"])))
    dist_pct = None
    at_or_near = False
    if nearest is not None and close is not None and close != 0:
        dist_pct = abs(close - float(nearest["price"])) / close * 100.0
        at_or_near = dist_pct <= near_liq_pct
    liq_gate_status = "PASS"
    liq_reason = None
    if require_near and not at_or_near:
        liq_gate_status = "BLOCK"
        liq_reason = "not_near_liquidity"
    liq_ctx = {
        "last_buy_side_liq": float(last_high) if last_high is not None else None,
        "last_sell_side_liq": float(last_low) if last_low is not None else None,
        "eqh_count": len(eqh),
        "eql_count": len(eql),
        "nearest_liq_type": (nearest or {}).get("type"),
        "nearest_liq_price": (nearest or {}).get("price"),
        "dist_to_zone_pct": dist_pct,
        "at_or_near_liq": at_or_near,
        "sweep_tag": entry_sweep.get("sweep_tag"),
        "sweep_level": entry_sweep.get("sweep_level"),
        "sweep_extreme": entry_sweep.get("sweep_extreme"),
    }
    return {"liq_ctx": liq_ctx, "liq_gate_status": liq_gate_status, "liq_reason": liq_reason}


def build_structure_15m(entry: List[Dict[str, Any]]) -> Dict[str, Any]:
    pivots = detect_pivots(entry)
    highs = pivots.get("swing_highs", [])
    lows = pivots.get("swing_lows", [])
    if len(highs) < 2 or len(lows) < 2:
        return {"structure_15m": "UNKNOWN", "structure_reason": "not_enough_swings"}
    h1, h2 = float(highs[-2]["price"]), float(highs[-1]["price"])
    l1, l2 = float(lows[-2]["price"]), float(lows[-1]["price"])
    if h2 > h1 and l2 > l1:
        return {"structure_15m": "UP", "structure_reason": None}
    if h2 < h1 and l2 < l1:
        return {"structure_15m": "DOWN", "structure_reason": None}
    return {"structure_15m": "RANGE", "structure_reason": None}


def _bucket_ms(ts_ms: Optional[Any], bucket_min: int) -> Optional[int]:
    if ts_ms is None:
        return None
    try:
        ts = int(ts_ms)
    except Exception:
        return None
    bucket = max(1, int(bucket_min)) * 60 * 1000
    return (ts // bucket) * bucket


def _build_plan_and_score(result: Dict[str, Any]) -> tuple[str, Optional[Dict[str, Any]], Dict[str, Any], Optional[str]]:
    direction = str(((result.get("stageb_confirmation") or {}).get("stageb_direction") or "NONE")).upper()
    if direction not in ("LONG", "SHORT"):
        return "INVALID_PLAN", None, {"score": 0, "priority": "C", "risk_mult": 0.5, "reasons": ["invalid_direction"]}, "invalid_plan"
    fvg = ((result.get("stageb_confirmation") or {}).get("stageb_fvg_poi") or {})
    fvg_lo = fvg.get("fvg_lo")
    fvg_hi = fvg.get("fvg_hi")
    if fvg_lo is None or fvg_hi is None:
        return "INVALID_PLAN", None, {"score": 0, "priority": "C", "risk_mult": 0.5, "reasons": ["missing_fvg"]}, "invalid_plan"
    entry_lo = min(float(fvg_lo), float(fvg_hi))
    entry_hi = max(float(fvg_lo), float(fvg_hi))
    entry_mid = (entry_lo + entry_hi) / 2.0
    liq_ctx = result.get("liq_ctx") or {}
    htf_gate = result.get("htf_gate") or {}
    entry_sw = result.get("entry_swing_summary") or {}
    htf_sw = result.get("htf_swing_summary") or {}
    sweep_extreme = liq_ctx.get("sweep_extreme")
    buffer_pct = _env_float("VPS_SMC_INVALID_BUFFER_PCT", 0.08)
    buffer_mult = buffer_pct / 100.0
    if sweep_extreme is None:
        sweep_extreme = entry_sw.get("last_swing_low") if direction == "LONG" else entry_sw.get("last_swing_high")
    if sweep_extreme is None:
        return "INVALID_PLAN", None, {"score": 0, "priority": "C", "risk_mult": 0.5, "reasons": ["missing_sweep_extreme"]}, "invalid_plan"
    sweep_extreme = float(sweep_extreme)
    sl = sweep_extreme * (1 - buffer_mult) if direction == "LONG" else sweep_extreme * (1 + buffer_mult)
    risk = (entry_mid - sl) if direction == "LONG" else (sl - entry_mid)
    if risk <= 0:
        return "INVALID_PLAN", None, {"score": 0, "priority": "C", "risk_mult": 0.5, "reasons": ["invalid_risk"]}, "invalid_plan"
    if direction == "LONG":
        raw_tp1 = liq_ctx.get("last_buy_side_liq") or entry_sw.get("last_swing_high")
        tp1 = (entry_mid + (1.0 * risk)) if (raw_tp1 is None or float(raw_tp1) <= entry_mid) else float(raw_tp1)

        raw_tp2 = htf_sw.get("last_swing_high")
        tp2 = (entry_mid + (1.5 * risk)) if (raw_tp2 is None or float(raw_tp2) <= entry_mid) else float(raw_tp2)

        tp3 = (entry_mid + (2.5 * risk)) if _env_bool("VPS_SMC_TP3_ENABLED", True) else None
        rr_tp2 = (tp2 - entry_mid) / risk
        sane = sl < entry_mid and tp2 > entry_mid
    else:
        raw_tp1 = liq_ctx.get("last_sell_side_liq") or entry_sw.get("last_swing_low")
        tp1 = (entry_mid - (1.0 * risk)) if (raw_tp1 is None or float(raw_tp1) >= entry_mid) else float(raw_tp1)

        raw_tp2 = htf_sw.get("last_swing_low")
        tp2 = (entry_mid - (1.5 * risk)) if (raw_tp2 is None or float(raw_tp2) >= entry_mid) else float(raw_tp2)

        tp3 = (entry_mid - (2.5 * risk)) if _env_bool("VPS_SMC_TP3_ENABLED", True) else None
        rr_tp2 = (entry_mid - tp2) / risk
        sane = sl > entry_mid and tp2 < entry_mid
    if not sane:
        return "INVALID_PLAN", None, {"score": 0, "priority": "C", "risk_mult": 0.5, "reasons": ["sanity_check_failed"]}, "invalid_plan"
    if rr_tp2 < _env_float("VPS_SMC_RR_MIN_TP2", 0.95):
        return "INVALID_PLAN", None, {"score": 0, "priority": "C", "risk_mult": 0.5, "reasons": ["rr_below_min"]}, "invalid_plan"
    plan = {"entry_lo": entry_lo, "entry_hi": entry_hi, "entry_mid": entry_mid, "sl": sl, "invalid": sl, "tp1": float(tp1), "tp2": float(tp2), "tp3": (float(tp3) if tp3 is not None else None), "rr_tp2": rr_tp2}
    score = 60
    reasons: List[str] = ["base_60"]
    htf_bias = str(htf_gate.get("htf_bias") or "MIXED").upper()
    htf_structure = str(htf_gate.get("htf_structure") or "UNKNOWN").upper()
    if str(htf_gate.get("htf_gate_status") or "") == "PASS":
        score += 10; reasons.append("+10_htf_pass")
    if (direction == "LONG" and htf_bias == "BULLISH") or (direction == "SHORT" and htf_bias == "BEARISH"):
        score += 5; reasons.append("+5_htf_bias_align")
    elif htf_bias == "MIXED" and htf_structure == "RANGE":
        score += 5; reasons.append("+5_mixed_range")
    elif (direction == "LONG" and htf_bias == "BEARISH") or (direction == "SHORT" and htf_bias == "BULLISH"):
        score -= 10; reasons.append("-10_htf_bias_conflict")
    if str(result.get("liq_gate_status") or "") == "PASS":
        score += 10; reasons.append("+10_liq_pass")
    elif str(result.get("liq_gate_status") or "") == "BLOCK":
        score -= 10; reasons.append("-10_liq_block")
    if ((result.get("stageb_confirmation") or {}).get("stageb_reclaim") or {}).get("has_reclaim"):
        score += 10; reasons.append("+10_reclaim")
    if ((result.get("stageb_confirmation") or {}).get("stageb_displacement") or {}).get("has_displacement"):
        score += 10; reasons.append("+10_displacement")
    if ((result.get("stageb_confirmation") or {}).get("stageb_fvg_poi") or {}).get("has_fvg"):
        score += 10; reasons.append("+10_fvg")
    if rr_tp2 >= 1.5:
        score += 5; reasons.append("+5_rr_ge_1_5")
    if rr_tp2 < 1.0:
        score -= 10; reasons.append("-10_rr_lt_1_0")
    s15 = str(result.get("structure_15m") or "UNKNOWN").upper()
    if (direction == "LONG" and s15 == "UP") or (direction == "SHORT" and s15 == "DOWN"):
        score += 5; reasons.append("+5_structure_align")
    score = max(0, min(100, score))
    a_min = _env_float("VPS_SMC_PRIORITY_A_MIN", 80.0)
    b_min = _env_float("VPS_SMC_PRIORITY_B_MIN", 70.0)
    priority = "A" if score >= a_min else ("B" if score >= b_min else "C")
    risk_mult = 1.0 if priority == "A" else (0.75 if priority == "B" else 0.5)
    return "VALID", plan, {"score": score, "priority": priority, "risk_mult": risk_mult, "reasons": reasons}, None

def vps_smc_status() -> Dict[str, Any]:
    state = _load_state()
    return {
        "ok": True,
        "enabled": _env_bool("VPS_SMC_COMPETITOR_ENABLED", True),
        "mode": str(os.getenv("VPS_SMC_COMPETITOR_MODE", "SHADOW_ONLY")).strip().upper() or "SHADOW_ONLY",
        "data_source": str(os.getenv("VPS_SMC_DATA_SOURCE", "BINANCE_CANDLE_STORE")).strip() or "BINANCE_CANDLE_STORE",
        "intervals": _intervals(),
        "symbols": _symbols_fallback(),
        "last_run_utc": state.get("last_run_utc"),
        "last_signal_count": int(state.get("last_signal_count") or 0),
        "last_error": state.get("last_error"),
        "last_compare_utc": state.get("last_compare_utc"),
        "last_compare_counts": state.get("last_compare_counts") or {},
        "scheduler_enabled": _env_bool("VPS_SMC_SCHEDULER_ENABLED", False),
    }


def vps_smc_compare(lookback_minutes: Optional[int] = 180, symbols: Optional[List[str]] = None, run_vps_first: bool = False) -> Dict[str, Any]:
    lookback = int(lookback_minutes or 180)
    lookback = max(1, min(lookback, 7 * 24 * 60))
    requested = [_normalize_symbol(s) for s in (symbols or []) if _normalize_symbol(s)]
    target_symbols = requested or _symbols_fallback()
    target_set = set(target_symbols)

    if run_vps_first:
        vps_smc_run_once(target_symbols)

    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(minutes=lookback)
    created_at_utc = now_utc.isoformat()
    notes: List[str] = []

    app_rows_raw: List[Dict[str, Any]] = []
    ml_dataset_path = _log_dir() / "ml_dataset_rows.jsonl"
    signals_path = _log_dir() / "signals.jsonl"
    if ml_dataset_path.exists():
        app_rows_raw = _read_jsonl(ml_dataset_path)
    elif signals_path.exists():
        app_rows_raw = _read_jsonl(signals_path)
    else:
        notes.append("missing_apps_logs")

    vps_path = _log_dir() / "vps_smc_shadow_signals.jsonl"
    vps_rows_raw = _read_jsonl(vps_path) if vps_path.exists() else []
    if not vps_path.exists():
        notes.append("missing_vps_shadow_logs")

    app_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    for row in app_rows_raw:
        sample_type = str(row.get("sample_type") or "").strip().upper()
        if sample_type == "VALIDATION_SAMPLE":
            continue
        if sample_type and sample_type != "FORWARD_SHADOW_PAPER":
            continue
        decision = str(row.get("execution_decision") or row.get("decision") or "").strip().upper()
        if decision not in ("ACCEPT", "REJECT"):
            continue
        symbol = _normalize_symbol(row.get("symbol") or row.get("pair"))
        if not symbol or symbol not in target_set:
            continue
        dt = _parse_iso_utc(row.get("created_at_utc") or row.get("event_at_utc") or row.get("timestamp_utc"))
        if dt is None or dt < cutoff:
            continue
        app_by_symbol.setdefault(symbol, []).append({
            "signal_key": row.get("signal_key") or row.get("signal_id"),
            "direction_raw": row.get("direction") or row.get("dir"),
            "direction": _normalize_direction(row.get("direction") or row.get("dir")),
            "decision": decision,
            "created_at_utc": dt.isoformat(),
            "ts": dt.timestamp(),
        })

    vps_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    for row in vps_rows_raw:
        if str(row.get("source_mode") or "").strip().upper() != "SHADOW_ONLY":
            continue
        if bool(row.get("candidate_only")) is not True:
            continue
        if str(row.get("state") or "").strip().upper() != "CONFIRMED":
            continue
        symbol = _normalize_symbol(row.get("symbol") or row.get("pair"))
        if not symbol or symbol not in target_set:
            continue
        dt = _parse_iso_utc(row.get("created_at_utc") or row.get("event_at_utc") or row.get("timestamp_utc"))
        if dt is None or dt < cutoff:
            continue
        vps_by_symbol.setdefault(symbol, []).append({
            "signal_key": row.get("signal_key"),
            "direction": _normalize_direction(row.get("direction")),
            "score": row.get("score"),
            "priority": row.get("priority"),
            "created_at_utc": dt.isoformat(),
            "ts": dt.timestamp(),
        })

    rows: List[Dict[str, Any]] = []
    seen_keys: set[str] = set()
    for symbol in sorted(target_set):
        app_items = sorted(app_by_symbol.get(symbol, []), key=lambda x: float(x.get("ts") or 0), reverse=True)
        vps_items = sorted(vps_by_symbol.get(symbol, []), key=lambda x: float(x.get("ts") or 0), reverse=True)
        app = app_items[0] if app_items else None
        vps = vps_items[0] if vps_items else None

        classification = "OVERLAP_AGREE"
        reason = "direction_match"
        if app and vps:
            if (app.get("direction") or "") != (vps.get("direction") or ""):
                classification = "CONFLICT"
                reason = "direction_conflict"
        elif app and not vps:
            classification = "APP_ONLY"
            reason = "no_vps_signal"
        elif vps and not app:
            classification = "VPS_ONLY"
            reason = "no_app_signal"
        else:
            continue

        compare_key = "|".join([
            symbol,
            classification,
            str((app or {}).get("signal_key") or ""),
            str((vps or {}).get("signal_key") or ""),
            str(lookback),
        ])
        if compare_key in seen_keys:
            continue
        seen_keys.add(compare_key)
        row_out = {
            "created_at_utc": created_at_utc,
            "lookback_minutes": lookback,
            "symbol": symbol,
            "classification": classification,
            "app_signal_key": (app or {}).get("signal_key"),
            "app_direction": (app or {}).get("direction_raw"),
            "app_decision": (app or {}).get("decision"),
            "app_created_at_utc": (app or {}).get("created_at_utc"),
            "vps_signal_key": (vps or {}).get("signal_key"),
            "vps_direction": (vps or {}).get("direction"),
            "vps_score": (vps or {}).get("score"),
            "vps_priority": (vps or {}).get("priority"),
            "vps_created_at_utc": (vps or {}).get("created_at_utc"),
            "reason": reason,
            "compare_key": compare_key,
        }
        rows.append(row_out)

    counts = {
        "total": len(rows),
        "overlap_agree": len([r for r in rows if r.get("classification") == "OVERLAP_AGREE"]),
        "app_only": len([r for r in rows if r.get("classification") == "APP_ONLY"]),
        "vps_only": len([r for r in rows if r.get("classification") == "VPS_ONLY"]),
        "conflict": len([r for r in rows if r.get("classification") == "CONFLICT"]),
    }
    for row in rows:
        _append_jsonl(_log_dir() / "vps_smc_compare.jsonl", row)

    state = _load_state()
    state["last_compare_utc"] = created_at_utc
    state["last_compare_counts"] = counts
    _save_state(state)

    out: Dict[str, Any] = {
        "ok": True,
        "mode": "SHADOW_ONLY_COMPARE",
        "lookback_minutes": lookback,
        "symbols": sorted(target_set),
        "counts": counts,
        "rows": rows,
    }
    if notes:
        out["reason"] = ",".join(notes)
    return out


def vps_smc_run_once(symbols: Optional[List[str]]) -> Dict[str, Any]:
    requested_raw = symbols or []
    requested = [_normalize_symbol(s) for s in requested_raw if _normalize_symbol(s)]
    if not requested:
        requested = _symbols_fallback()
    max_symbols = _env_int("VPS_SMC_MAX_SYMBOLS_PER_RUN", 4)
    evaluated = requested[:max_symbols]
    intervals = _intervals()
    results: List[Dict[str, Any]] = []
    last_error: Optional[str] = None

    for symbol in evaluated:
        try:
            htf = _load_internal_candles(symbol, intervals["htf"])
            entry = _load_internal_candles(symbol, intervals["entry"])
            stageb = _load_internal_candles(symbol, intervals["stageb"])

            status = "READY"
            reason = None
            if len(htf) < 1:
                status = "DATA_GAP"
                reason = f"missing_{intervals['htf']}_candles"
            elif len(entry) < 1:
                status = "DATA_GAP"
                reason = f"missing_{intervals['entry']}_candles"
            elif len(stageb) < 1:
                status = "DATA_GAP"
                reason = f"missing_{intervals['stageb']}_candles"

            primitive_status = "SKIPPED_DATA_GAP"
            entry_swing_summary: Dict[str, Any] = {}
            entry_eq: Dict[str, Any] = {"eqh": [], "eql": []}
            entry_sweep: Dict[str, Any] = {
                "bullish_sweep": False,
                "bearish_sweep": False,
                "sweep_level": None,
                "sweep_extreme": None,
                "sweep_t": None,
                "sweep_tag": None,
            }
            stageb_fvg: Dict[str, Any] = {"bullish_fvg": None, "bearish_fvg": None, "count_bullish": 0, "count_bearish": 0}
            htf_swing_summary: Dict[str, Any] = {}
            if status == "READY":
                try:
                    entry_pivots = detect_pivots(entry)
                    entry_swing_summary = build_swing_summary(entry)
                    entry_eq = detect_equal_high_low(entry, entry_pivots)
                    entry_sweep = detect_sweep(entry, entry_swing_summary)
                    stageb_fvg = detect_fvg(stageb)
                    htf_swing_summary = build_swing_summary(htf)
                    primitive_status = "READY"
                except Exception as pexc:
                    primitive_status = "ERROR"
                    _append_jsonl(_log_dir() / "vps_smc_errors.jsonl", {
                        "created_at_utc": _utc_now_iso(),
                        "symbol": symbol,
                        "error": f"primitive:{pexc}",
                    })
            htf_gate = {"htf_gate_status": "HTF_DATA_GAP", "htf_reason": "not_enough_4h_candles"}
            liq_ctx: Dict[str, Any] = {}
            liq_gate_status = "BLOCK"
            liq_reason = "context_not_built"
            structure_15m = "UNKNOWN"
            structure_reason = "context_not_built"
            context_status = "DATA_GAP" if status == "DATA_GAP" else "ERROR"
            if primitive_status == "READY":
                try:
                    htf_gate = build_htf_gate(htf, htf_swing_summary)
                    liq_out = build_liquidity_context(entry, entry_swing_summary, entry_eq, entry_sweep)
                    liq_ctx = liq_out["liq_ctx"]
                    liq_gate_status = liq_out["liq_gate_status"]
                    liq_reason = liq_out["liq_reason"]
                    s15 = build_structure_15m(entry)
                    structure_15m = s15["structure_15m"]
                    structure_reason = s15["structure_reason"]
                    htf_gate_status = htf_gate.get("htf_gate_status")
                    if htf_gate_status == "HTF_DATA_GAP":
                        context_status = "HTF_DATA_GAP"
                    elif htf_gate_status == "PASS" and liq_gate_status == "PASS":
                        context_status = "READY"
                    elif htf_gate_status == "BLOCK" or liq_gate_status == "BLOCK":
                        context_status = "BLOCKED"
                    else:
                        context_status = "ERROR"
                except Exception as cexc:
                    context_status = "ERROR"
                    _append_jsonl(_log_dir() / "vps_smc_errors.jsonl", {
                        "created_at_utc": _utc_now_iso(),
                        "symbol": symbol,
                        "error": f"context:{cexc}",
                    })
            stageb_confirmation = {
                "stageb_status": "INVALID",
                "stageb_direction": "NONE",
                "stageb_reclaim": {},
                "stageb_displacement": {},
                "stageb_fvg_poi": {},
                "stageb_confirm_reason": None,
                "stageb_invalid_reason": "not_built",
            }
            shadow_state = "INVALID"
            if _env_bool("VPS_SMC_STAGEB_ENABLED", True):
                try:
                    stageb_confirmation = build_stageb_confirmation({
                        "context_status": context_status,
                        "liq_gate_status": liq_gate_status,
                        "liq_ctx": liq_ctx,
                        "entry_sweep": entry_sweep,
                        "stageb_fvg": stageb_fvg,
                    }, stageb)
                    shadow_state = stageb_confirmation.get("stageb_status") or "INVALID"
                except Exception as sexc:
                    stageb_confirmation = {
                        "stageb_status": "INVALID",
                        "stageb_direction": "NONE",
                        "stageb_reclaim": {},
                        "stageb_displacement": {},
                        "stageb_fvg_poi": {},
                        "stageb_confirm_reason": None,
                        "stageb_invalid_reason": f"stageb_error:{sexc}",
                    }
                    shadow_state = "INVALID"
                    _append_jsonl(_log_dir() / "vps_smc_errors.jsonl", {
                        "created_at_utc": _utc_now_iso(),
                        "symbol": symbol,
                        "error": f"stageb:{sexc}",
                    })
            results.append({
                "symbol": symbol,
                "status": status,
                "primitive_status": primitive_status,
                "htf_count": len(htf),
                "entry_count": len(entry),
                "stageb_count": len(stageb),
                "latest_htf_close_time_ms": htf[-1]["t"] if htf else None,
                "latest_entry_close_time_ms": entry[-1]["t"] if entry else None,
                "latest_stageb_close_time_ms": stageb[-1]["t"] if stageb else None,
                "entry_swing_summary": entry_swing_summary,
                "entry_eq": entry_eq,
                "entry_sweep": entry_sweep,
                "stageb_fvg": stageb_fvg,
                "htf_swing_summary": htf_swing_summary,
                "htf_gate": htf_gate,
                "liq_ctx": liq_ctx,
                "liq_gate_status": liq_gate_status,
                "liq_reason": liq_reason,
                "structure_15m": structure_15m,
                "structure_reason": structure_reason,
                "context_status": context_status,
                "stageb_confirmation": stageb_confirmation,
                "shadow_state": shadow_state,
                "reason": reason,
            })
        except Exception as exc:
            last_error = f"{symbol}:{exc}"
            _append_jsonl(_log_dir() / "vps_smc_errors.jsonl", {
                "created_at_utc": _utc_now_iso(),
                "symbol": symbol,
                "error": str(exc),
            })

    signal_count = 0
    state = _load_state()
    logged_signal_keys = state.get("logged_signal_keys") if isinstance(state.get("logged_signal_keys"), dict) else {}
    for result in results:
        result.setdefault("htf_gate", {"htf_gate_status": "HTF_DATA_GAP"})
        result.setdefault("liq_ctx", {})
        result.setdefault("liq_gate_status", "BLOCK")
        result.setdefault("liq_reason", "context_not_built")
        result.setdefault("structure_15m", "UNKNOWN")
        result.setdefault("structure_reason", "context_not_built")
        if result.get("status") == "DATA_GAP":
            result.setdefault("context_status", "DATA_GAP")
        else:
            result.setdefault("context_status", "ERROR")
        result.setdefault("stageb_confirmation", {
            "stageb_status": "INVALID",
            "stageb_direction": "NONE",
            "stageb_reclaim": {},
            "stageb_displacement": {},
            "stageb_fvg_poi": {},
            "stageb_confirm_reason": None,
            "stageb_invalid_reason": "not_built",
        })
        result.setdefault("shadow_state", (result.get("stageb_confirmation") or {}).get("stageb_status") or "INVALID")
        result["plan_status"] = "INVALID_PLAN"
        result["plan"] = None
        result["score_detail"] = {"score": 0, "priority": "C", "risk_mult": 0.5, "reasons": ["not_confirmed"]}
        result["signal_logged"] = False
        result["signal_key"] = None
        result["signal_skip_reason"] = "not_confirmed"
        if str(result.get("shadow_state") or "") != "CONFIRMED":
            continue
        if not _env_bool("VPS_SMC_PLAN_ENABLED", True):
            result["signal_skip_reason"] = "invalid_plan"
            continue
        plan_status, plan, score_detail, skip_reason = _build_plan_and_score(result)
        result["plan_status"] = plan_status
        result["plan"] = plan
        result["score_detail"] = score_detail
        result["signal_skip_reason"] = skip_reason if plan_status != "VALID" else None
        if plan_status != "VALID":
            continue
        if _env_bool("VPS_SMC_SCORE_ENABLED", True) and float(score_detail.get("score") or 0.0) < _env_float("VPS_SMC_SCORE_MIN", 70.0):
            result["signal_skip_reason"] = "score_below_min"
            continue
        bucket_ms = _bucket_ms(result.get("latest_stageb_close_time_ms"), _env_int("VPS_SMC_DEDUP_BUCKET_MIN", 15))
        direction = str(((result.get("stageb_confirmation") or {}).get("stageb_direction") or "NONE")).upper()
        signal_key = f"{result.get('symbol')}|{direction}|{bucket_ms}"
        result["signal_key"] = signal_key
        if _env_bool("VPS_SMC_DEDUP_ENABLED", True) and signal_key in logged_signal_keys:
            result["signal_skip_reason"] = "duplicate"
            continue
        if not _env_bool("VPS_SMC_SHADOW_SIGNAL_LOG_ENABLED", True):
            result["signal_skip_reason"] = "signal_log_disabled"
            continue
        htf_gate = result.get("htf_gate") or {}
        stageb = result.get("stageb_confirmation") or {}
        poi = stageb.get("stageb_fvg_poi") or {}
        reclaim = stageb.get("stageb_reclaim") or {}
        liq_ctx = result.get("liq_ctx") or {}
        signal_row = {
            "signal_key": signal_key, "signal_source": "VPS_SMC", "source_mode": "SHADOW_ONLY", "candidate_only": True,
            "symbol": result.get("symbol"), "pair": f"BINANCE:{str(result.get('symbol') or '').upper()}.P", "direction": direction, "state": "CONFIRMED", "mode": "SHADOW_ONLY",
            "score": score_detail.get("score"), "priority": score_detail.get("priority"), "risk_mult": score_detail.get("risk_mult"),
            "signal_time_wib": None, "confirmed_bucket_ms": bucket_ms, "htf_dir": htf_gate.get("htf_dir"), "htf_bias": htf_gate.get("htf_bias"),
            "htf_location": htf_gate.get("htf_location"), "htf_structure": htf_gate.get("htf_structure"), "liq_ctx": result.get("liq_ctx"),
            "dist_to_zone_pct": liq_ctx.get("dist_to_zone_pct"), "structure_15m": result.get("structure_15m"), "sweep_tag": liq_ctx.get("sweep_tag"),
            "sweep_extreme": liq_ctx.get("sweep_extreme"), "reclaim_level": reclaim.get("reclaim_level"), "fvg_type": poi.get("fvg_type"),
            "fvg_lo": poi.get("fvg_lo"), "fvg_hi": poi.get("fvg_hi"), "entry_lo": plan.get("entry_lo"), "entry_hi": plan.get("entry_hi"),
            "entry_mid": plan.get("entry_mid"), "sl": plan.get("sl"), "invalid": plan.get("invalid"), "tp1": plan.get("tp1"),
            "tp2": plan.get("tp2"), "tp3": plan.get("tp3"), "rr_tp2": plan.get("rr_tp2"), "notes": ",".join(score_detail.get("reasons") or []),
            "created_at_utc": _utc_now_iso(),
        }
        _append_jsonl(_log_dir() / "vps_smc_shadow_signals.jsonl", signal_row)
        logged_signal_keys[signal_key] = signal_row["created_at_utc"]
        result["signal_logged"] = True
        result["signal_skip_reason"] = None
        source_mode = str(os.getenv("SIGNAL_SOURCE_MODE", "APPS_SCRIPT_ONLY")).strip().upper() or "APPS_SCRIPT_ONLY"
        vps_exec_enabled = _env_bool("VPS_SMC_EXECUTION_ENABLED", False)
        competitor_mode = str(os.getenv("VPS_SMC_COMPETITOR_MODE", "SHADOW_ONLY")).strip().upper() or "SHADOW_ONLY"
        if (
            EXECUTION_BRIDGE_HANDLER is not None
            and source_mode == "VPS_SMC_PRIMARY"
            and vps_exec_enabled
            and competitor_mode == "PRODUCTION_SIGNAL"
        ):
            bridge_payload = {
                "source": "VPS_SMC",
                "signal_source": "VPS_SMC",
                "event_type": "SIGNAL_CONFIRMED",
                "source_mode": "VPS_SMC_PRIMARY",
                "execution_owner": "VPS_SMC",
                "status": "CONFIRMED",
                "state": "CONFIRMED",
                "signal_key": signal_key,
                "signal_id": signal_key,
                "symbol": result.get("symbol"),
                "pair": f"BINANCE:{str(result.get('symbol') or '').upper()}.P",
                "direction": direction,
                "entry": plan.get("entry_mid"),
                "entry_mid": plan.get("entry_mid"),
                "entry_lo": plan.get("entry_lo"),
                "entry_hi": plan.get("entry_hi"),
                "sl": plan.get("sl"),
                "tp1": plan.get("tp1"),
                "tp2": plan.get("tp2"),
                "tp3": plan.get("tp3"),
                "score": score_detail.get("score"),
                "priority": score_detail.get("priority"),
                "confirmed_bucket_ms": bucket_ms,
                "signal_time_wib": None,
                "htf_dir": htf_gate.get("htf_dir"),
                "htf_bias": htf_gate.get("htf_bias"),
                "htf_location": htf_gate.get("htf_location"),
                "htf_structure": htf_gate.get("htf_structure"),
                "liq_ctx": result.get("liq_ctx"),
                "dist_to_zone_pct": liq_ctx.get("dist_to_zone_pct"),
                "structure_15m": result.get("structure_15m"),
                "sweep_tag": liq_ctx.get("sweep_tag"),
                "sweep_extreme": liq_ctx.get("sweep_extreme"),
                "reclaim_level": reclaim.get("reclaim_level"),
                "fvg_type": poi.get("fvg_type"),
                "fvg_lo": poi.get("fvg_lo"),
                "fvg_hi": poi.get("fvg_hi"),
                "notes": ",".join(score_detail.get("reasons") or []),
            }
            try:
                result["execution_bridge"] = EXECUTION_BRIDGE_HANDLER(bridge_payload)
            except Exception as exc:
                result["execution_bridge"] = {"ok": False, "error": str(exc)}
                _append_jsonl(_log_dir() / "vps_smc_errors.jsonl", {
                    "created_at_utc": _utc_now_iso(),
                    "event_type": "VPS_SMC_EXECUTION_BRIDGE_ERROR",
                    "symbol": result.get("symbol"),
                    "signal_key": signal_key,
                    "error": str(exc),
                })
        signal_count += 1

    log_row = {
        "event_type": "VPS_SMC_RUN_ONCE",
        "created_at_utc": _utc_now_iso(),
        "mode": str(os.getenv("VPS_SMC_COMPETITOR_MODE", "SHADOW_ONLY")).strip().upper() or "SHADOW_ONLY",
        "symbols_requested": requested,
        "symbols_evaluated": [r.get("symbol") for r in results],
        "signal_count": signal_count,
        "results_summary": {
            "ready": len([r for r in results if r.get("status") == "READY"]),
            "data_gap": len([r for r in results if r.get("status") == "DATA_GAP"]),
            "primitive_ready": len([r for r in results if r.get("primitive_status") == "READY"]),
            "primitive_error": len([r for r in results if r.get("primitive_status") == "ERROR"]),
            "primitive_skipped": len([r for r in results if r.get("primitive_status") == "SKIPPED_DATA_GAP"]),
            "bullish_sweep_count": len([r for r in results if (r.get("entry_sweep") or {}).get("bullish_sweep")]),
            "bearish_sweep_count": len([r for r in results if (r.get("entry_sweep") or {}).get("bearish_sweep")]),
            "bullish_fvg_count": sum(int(((r.get("stageb_fvg") or {}).get("count_bullish") or 0)) for r in results),
            "bearish_fvg_count": sum(int(((r.get("stageb_fvg") or {}).get("count_bearish") or 0)) for r in results),
            "htf_pass": len([r for r in results if ((r.get("htf_gate") or {}).get("htf_gate_status") == "PASS")]),
            "htf_block": len([r for r in results if ((r.get("htf_gate") or {}).get("htf_gate_status") == "BLOCK")]),
            "htf_data_gap": len([r for r in results if ((r.get("htf_gate") or {}).get("htf_gate_status") == "HTF_DATA_GAP")]),
            "liq_pass": len([r for r in results if r.get("liq_gate_status") == "PASS"]),
            "liq_block": len([r for r in results if r.get("liq_gate_status") == "BLOCK"]),
            "context_ready": len([r for r in results if r.get("context_status") == "READY"]),
            "context_blocked": len([r for r in results if r.get("context_status") == "BLOCKED"]),
            "stageb_idle": len([r for r in results if ((r.get("stageb_confirmation") or {}).get("stageb_status") == "IDLE")]),
            "stageb_watch": len([r for r in results if ((r.get("stageb_confirmation") or {}).get("stageb_status") == "WATCH")]),
            "stageb_confirmed": len([r for r in results if ((r.get("stageb_confirmation") or {}).get("stageb_status") == "CONFIRMED")]),
            "stageb_invalid": len([r for r in results if ((r.get("stageb_confirmation") or {}).get("stageb_status") == "INVALID")]),
            "reclaim_pass": len([r for r in results if ((r.get("stageb_confirmation") or {}).get("stageb_reclaim") or {}).get("has_reclaim")]),
            "displacement_pass": len([r for r in results if ((r.get("stageb_confirmation") or {}).get("stageb_displacement") or {}).get("has_displacement")]),
            "fvg_poi_pass": len([r for r in results if ((r.get("stageb_confirmation") or {}).get("stageb_fvg_poi") or {}).get("has_fvg")]),
            "long_candidates": len([r for r in results if ((r.get("stageb_confirmation") or {}).get("stageb_direction") == "LONG")]),
            "short_candidates": len([r for r in results if ((r.get("stageb_confirmation") or {}).get("stageb_direction") == "SHORT")]),
            "structure_up": len([r for r in results if r.get("structure_15m") == "UP"]),
            "structure_down": len([r for r in results if r.get("structure_15m") == "DOWN"]),
            "structure_range": len([r for r in results if r.get("structure_15m") == "RANGE"]),
            "plan_valid": len([r for r in results if r.get("plan_status") == "VALID"]),
            "plan_invalid": len([r for r in results if r.get("plan_status") == "INVALID_PLAN"]),
            "shadow_signals_logged": len([r for r in results if r.get("signal_logged") is True]),
            "shadow_signals_skipped": len([r for r in results if r.get("signal_logged") is False]),
            "score_a": len([r for r in results if ((r.get("score_detail") or {}).get("priority") == "A")]),
            "score_b": len([r for r in results if ((r.get("score_detail") or {}).get("priority") == "B")]),
            "score_c": len([r for r in results if ((r.get("score_detail") or {}).get("priority") == "C")]),
        },
        "error": last_error,
    }
    _append_jsonl(_log_dir() / "vps_smc_systemlog.jsonl", log_row)

    state["last_run_utc"] = log_row["created_at_utc"]
    state["last_signal_count"] = signal_count
    state["last_error"] = last_error
    state["last_symbols"] = [r.get("symbol") for r in results]
    state["logged_signal_keys"] = logged_signal_keys
    _save_state(state)

    return {
        "ok": True,
        "mode": log_row["mode"],
        "symbols_requested": requested,
        "symbols_evaluated": state["last_symbols"],
        "signal_count": signal_count,
        "results": results,
    }




def _env_str(name: str, default: str = "") -> str:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip() or default


def _bool_str(v: Any) -> str:
    return "true" if bool(v) else "false"


def _gspreadsheet_id() -> str:
    return _env_str("VPS_SMC_GSHEET_SPREADSHEET_ID") or _env_str("MACRO_EVENT_SPREADSHEET_ID")


def _gservice_account_file() -> str:
    return _env_str("VPS_SMC_GSHEET_SERVICE_ACCOUNT_FILE", "state/private/google_service_account.json") or _env_str("MACRO_EVENT_SERVICE_ACCOUNT_FILE")


def _vps_scheduler_lock() -> threading.Lock:
    if not hasattr(_vps_scheduler_lock, "_lock"):
        setattr(_vps_scheduler_lock, "_lock", threading.Lock())
    return getattr(_vps_scheduler_lock, "_lock")


def _read_latest(path: Path, limit: int) -> List[Dict[str, Any]]:
    rows = _read_jsonl(path)
    take = max(1, min(int(limit or 100), _env_int("VPS_SMC_GSHEET_MAX_ROWS_PER_RUN", 100)))
    return rows[-take:]


def _sheet_append(spreadsheet_id: str, service_account_file: str, sheet_name: str, header: List[str], rows: List[List[Any]], create_header: bool, header_already_written: bool) -> bool:
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(service_account_file, scopes=scopes)
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)

    header_now_written = bool(header_already_written)
    if create_header and not header_now_written:
        if rows:
            service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": [header]},
            ).execute()
            header_now_written = True
        else:
            probe = service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1:Z1",
            ).execute()
            existing = probe.get("values") or []
            if not existing:
                service.spreadsheets().values().append(
                    spreadsheetId=spreadsheet_id,
                    range=f"{sheet_name}!A1",
                    valueInputOption="RAW",
                    insertDataOption="INSERT_ROWS",
                    body={"values": [header]},
                ).execute()
            header_now_written = True

    if rows:
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": rows},
        ).execute()
    return header_now_written


def vps_smc_mirror_gsheet(target: str = "ALL", limit: int = 100, force: bool = False) -> Dict[str, Any]:
    target_u = str(target or "ALL").strip().upper()
    enabled = _env_bool("VPS_SMC_GSHEET_MIRROR_ENABLED", False)
    counts = {"shadow_candidates_seen": 0, "shadow_candidates_written": 0, "compare_rows_seen": 0, "compare_rows_written": 0, "skipped_duplicate": 0, "errors": 0}
    state = _load_state()
    mirrored = state.get("gsheet_mirrored_keys") if isinstance(state.get("gsheet_mirrored_keys"), dict) else {"shadow": {}, "compare": {}}
    mirrored.setdefault("shadow", {})
    mirrored.setdefault("compare", {})
    headers_written = state.get("gsheet_headers_written") if isinstance(state.get("gsheet_headers_written"), dict) else {"shadow": False, "compare": False}
    headers_written.setdefault("shadow", False)
    headers_written.setdefault("compare", False)
    spreadsheet_id = _gspreadsheet_id()

    if not enabled:
        return {"ok": True, "enabled": False, "target": target_u, "spreadsheet_id": spreadsheet_id or None, "counts": counts, "reason": "gsheet_mirror_disabled"}

    service_account_file = _gservice_account_file()
    if not spreadsheet_id or not service_account_file:
        reason = "credentials_missing"
        _append_jsonl(_log_dir() / "vps_smc_errors.jsonl", {"created_at_utc": _utc_now_iso(), "error": reason})
        return {"ok": _env_bool("VPS_SMC_GSHEET_FAIL_OPEN", True), "enabled": True, "target": target_u, "spreadsheet_id": spreadsheet_id or None, "counts": counts, "reason": reason}

    shadow_header = ["created_at_utc","signal_key","symbol","direction","state","score","priority","risk_mult","source_mode","candidate_only","htf_bias","htf_location","htf_structure","structure_15m","sweep_tag","entry_lo","entry_hi","entry_mid","sl","tp1","tp2","tp3","rr_tp2","notes"]
    compare_header = ["created_at_utc","lookback_minutes","symbol","classification","app_signal_key","app_direction","app_decision","app_created_at_utc","vps_signal_key","vps_direction","vps_score","vps_priority","vps_created_at_utc","reason","compare_key"]
    shadow_rows=[]; compare_rows=[]
    try:
        if target_u in ("ALL","SHADOW_SIGNALS"):
            for r in _read_latest(_log_dir()/"vps_smc_shadow_signals.jsonl", limit):
                if str(r.get("source_mode") or "").upper() != "SHADOW_ONLY" or bool(r.get("candidate_only")) is not True:
                    continue
                counts["shadow_candidates_seen"] += 1
                key = str(r.get("signal_key") or "")
                if key and (not force) and key in mirrored["shadow"]:
                    counts["skipped_duplicate"] += 1; continue
                shadow_rows.append([r.get(k) for k in shadow_header])
                if key: mirrored["shadow"][key]=_utc_now_iso()
        if target_u in ("ALL","COMPARE"):
            for r in _read_latest(_log_dir()/"vps_smc_compare.jsonl", limit):
                counts["compare_rows_seen"] += 1
                key = str(r.get("compare_key") or "")
                if key and (not force) and key in mirrored["compare"]:
                    counts["skipped_duplicate"] += 1; continue
                compare_rows.append([r.get(k) for k in compare_header])
                if key: mirrored["compare"][key]=_utc_now_iso()

        if target_u in ("ALL", "SHADOW_SIGNALS"):
            headers_written["shadow"] = _sheet_append(
                spreadsheet_id,
                service_account_file,
                _env_str("VPS_SMC_GSHEET_SHADOW_SHEET_NAME", "VPS_SMC_SHADOW_SIGNALS"),
                shadow_header,
                shadow_rows,
                _env_bool("VPS_SMC_GSHEET_CREATE_HEADER", True),
                bool(headers_written.get("shadow", False)),
            )
        if target_u in ("ALL", "COMPARE"):
            headers_written["compare"] = _sheet_append(
                spreadsheet_id,
                service_account_file,
                _env_str("VPS_SMC_GSHEET_COMPARE_SHEET_NAME", "VPS_SMC_COMPARE"),
                compare_header,
                compare_rows,
                _env_bool("VPS_SMC_GSHEET_CREATE_HEADER", True),
                bool(headers_written.get("compare", False)),
            )
        counts["shadow_candidates_written"] = len(shadow_rows)
        counts["compare_rows_written"] = len(compare_rows)
        state["gsheet_mirrored_keys"] = mirrored
        state["gsheet_headers_written"] = headers_written
        state["last_gsheet_mirror_utc"] = _utc_now_iso()
        state["last_gsheet_mirror_counts"] = counts
        _save_state(state)
        _append_jsonl(_log_dir() / "vps_smc_gsheet_mirror.jsonl", {"created_at_utc": _utc_now_iso(), "target": target_u, "counts": counts})
        return {"ok": True, "enabled": True, "target": target_u, "spreadsheet_id": spreadsheet_id, "counts": counts, "reason": None}
    except Exception as exc:
        counts["errors"] += 1
        reason = f"gsheet_error:{exc}"
        _append_jsonl(_log_dir() / "vps_smc_errors.jsonl", {"created_at_utc": _utc_now_iso(), "error": reason})
        return {"ok": _env_bool("VPS_SMC_GSHEET_FAIL_OPEN", True), "enabled": True, "target": target_u, "spreadsheet_id": spreadsheet_id, "counts": counts, "reason": "gsheet_api_error"}


def vps_smc_scheduler_status() -> Dict[str, Any]:
    state = _load_state()
    return {"ok": True, "enabled": _env_bool("VPS_SMC_SCHEDULER_ENABLED", False), "interval_sec": _env_int("VPS_SMC_SCHEDULER_INTERVAL_SEC", 300), "last_scheduler_run_utc": state.get("last_scheduler_run_utc"), "last_scheduler_status": state.get("last_scheduler_status") or "IDLE", "last_error": state.get("last_scheduler_error"), "running": bool(state.get("scheduler_running", False))}


def scheduler_status() -> Dict[str, Any]:
    """Backward-compatible alias used by /operator/status payload."""
    return vps_smc_scheduler_status()


def vps_smc_scheduler_run_once(symbols: Optional[List[str]] = None, run_vps: bool = True, run_compare: bool = True, mirror_gsheet: bool = False, lookback_minutes: int = 360) -> Dict[str, Any]:
    lock = _vps_scheduler_lock()
    if not lock.acquire(blocking=False):
        return {"ok": True, "mode": "SHADOW_ONLY_SCHEDULER_RUN_ONCE", "run_vps_result": None, "compare_result": None, "mirror_result": None, "error": "scheduler_already_running"}
    state = _load_state(); state["scheduler_running"] = True; _save_state(state)
    rv=None; cr=None; mr=None; err=None
    try:
        if run_vps:
            rv = vps_smc_run_once(symbols)
        if run_compare:
            cr = vps_smc_compare(lookback_minutes=lookback_minutes, symbols=symbols, run_vps_first=False)
        if mirror_gsheet:
            mr = vps_smc_mirror_gsheet(target="ALL", limit=_env_int("VPS_SMC_GSHEET_MAX_ROWS_PER_RUN", 100), force=False)
        state = _load_state(); state["last_scheduler_status"] = "OK"
    except Exception as exc:
        err = str(exc)
        state = _load_state(); state["last_scheduler_status"] = "ERROR"; state["last_scheduler_error"] = err
    state["scheduler_running"] = False
    state["last_scheduler_run_utc"] = _utc_now_iso()
    _save_state(state)
    _append_jsonl(_log_dir()/"vps_smc_scheduler.jsonl", {"created_at_utc": _utc_now_iso(), "ok": err is None, "error": err})
    lock.release()
    return {"ok": err is None or _env_bool("VPS_SMC_SCHEDULER_FAIL_OPEN", True), "mode": "SHADOW_ONLY_SCHEDULER_RUN_ONCE", "run_vps_result": rv, "compare_result": cr, "mirror_result": mr, "error": err}
def vps_smc_latest_signals(symbol: str, limit: int) -> Dict[str, Any]:
    symbol_norm = _normalize_symbol(symbol)
    rows = _read_jsonl(_log_dir() / "vps_smc_shadow_signals.jsonl")
    filtered = [r for r in rows if _normalize_symbol(r.get("symbol")) == symbol_norm]
    take = max(1, min(int(limit or 10), 100))
    latest = filtered[-take:]
    latest.reverse()
    return {
        "ok": True,
        "symbol": symbol_norm,
        "count": len(latest),
        "signals": latest,
    }
