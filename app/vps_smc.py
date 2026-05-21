import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


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
        "scheduler_enabled": _env_bool("VPS_SMC_SCHEDULER_ENABLED", False),
    }


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
            "structure_up": len([r for r in results if r.get("structure_15m") == "UP"]),
            "structure_down": len([r for r in results if r.get("structure_15m") == "DOWN"]),
            "structure_range": len([r for r in results if r.get("structure_15m") == "RANGE"]),
        },
        "error": last_error,
    }
    _append_jsonl(_log_dir() / "vps_smc_systemlog.jsonl", log_row)

    state = _load_state()
    state["last_run_utc"] = log_row["created_at_utc"]
    state["last_signal_count"] = signal_count
    state["last_error"] = last_error
    state["last_symbols"] = [r.get("symbol") for r in results]
    _save_state(state)

    return {
        "ok": True,
        "mode": log_row["mode"],
        "symbols_requested": requested,
        "symbols_evaluated": state["last_symbols"],
        "signal_count": signal_count,
        "results": results,
    }


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
