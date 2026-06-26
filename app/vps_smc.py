import json
import os
from datetime import datetime, timedelta, timezone
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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
        "gsheet_mirrored_keys": {"shadow": {}, "compare": {}, "outcomes": {}},
        "gsheet_headers_written": {"shadow": False, "compare": False, "outcomes": False},
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


WIB_TZ = timezone(timedelta(hours=7))


def _parse_wib_time(raw: Any) -> Optional[datetime]:
    if raw is None:
        return None
    txt = str(raw).strip()
    if not txt:
        return None
    cleaned = txt.replace("WIB", "").strip()
    candidates = [cleaned]
    if "T" not in cleaned and len(cleaned) == 16:
        candidates.append(cleaned + ":00")
    for candidate in candidates:
        try:
            dt = datetime.fromisoformat(candidate)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=WIB_TZ)
            return dt.astimezone(timezone.utc)
        except Exception:
            pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M"):
        try:
            dt = datetime.strptime(cleaned, fmt).replace(tzinfo=WIB_TZ)
            return dt.astimezone(timezone.utc)
        except Exception:
            continue
    return None


def _parse_apps_signal_time(row: Dict[str, Any]) -> Optional[datetime]:
    for key in ("created_at_utc", "event_at_utc", "timestamp_utc"):
        dt = _parse_iso_utc(row.get(key))
        if dt is not None:
            return dt
    for key in ("signal_time_wib", "confirmed_ts_wib", "run_ts_wib"):
        dt = _parse_wib_time(row.get(key))
        if dt is not None:
            return dt
    return None


def _bucket_ms_to_wib_text(bucket_ms: Any) -> Optional[str]:
    try:
        ms = int(bucket_ms)
    except Exception:
        return None
    dt = datetime.fromtimestamp(ms / 1000.0, timezone.utc).astimezone(timezone(timedelta(hours=7)))
    return dt.strftime("%Y-%m-%d %H:%M:%S WIB")

def _normalize_direction(raw: Any) -> Optional[str]:
    d = str(raw or "").strip().upper()
    if d in ("LONG", "BUY"):
        return "LONG"
    if d in ("SHORT", "SELL"):
        return "SHORT"
    return None


def _normalize_tps(direction: str, entry_mid: Any, sl: Any, tp1: Any, tp2: Any, tp3: Any) -> Dict[str, Any]:
    d = str(direction or "").strip().upper()
    if d not in ("LONG", "SHORT"):
        return {"ok": False, "reason": "invalid_direction", "plan_invalid": True}
    try:
        e = float(entry_mid)
        s = float(sl)
        t1 = float(tp1)
        t2 = float(tp2)
        t3 = float(tp3)
    except Exception:
        return {"ok": False, "reason": "missing_entry_or_sl_or_tp", "plan_invalid": True}
    if d == "LONG":
        if s >= e:
            return {"ok": False, "reason": "invalid_sl_side", "plan_invalid": True}
        sorted_tps = sorted([x for x in [t1, t2, t3] if x > e])
    else:
        if s <= e:
            return {"ok": False, "reason": "invalid_sl_side", "plan_invalid": True}
        sorted_tps = sorted([x for x in [t1, t2, t3] if x < e], reverse=True)
    if len(sorted_tps) < 3:
        return {"ok": False, "reason": "not_enough_valid_tps_after_normalization", "plan_invalid": True}
    was_normalized = not (t1 == sorted_tps[0] and t2 == sorted_tps[1] and t3 == sorted_tps[2])
    return {"ok": True, "reason": "ok", "plan_invalid": False, "tp1": sorted_tps[0], "tp2": sorted_tps[1], "tp3": sorted_tps[2], "raw_tp1": t1, "raw_tp2": t2, "raw_tp3": t3, "tp_normalized": was_normalized, "tp_normalize_reason": ("tp_order_resequenced" if was_normalized else None)}


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
    if interval == (str(os.getenv("VPS_SMC_INTERVAL_HTF", "4h")).strip() or "4h"):
        htf_limit = max(50, min(_env_int("VPS_SMC_HTF_CANDLE_LIMIT", 220), 1000))
        if len(out) < htf_limit:
            fetched = _fetch_binance_closed_klines(symbol, interval, htf_limit)
            if fetched:
                merged: Dict[int, Dict[str, Any]] = {int(x.get("tBucketMs") or 0): x for x in out}
                for row in fetched:
                    merged[int(row.get("tBucketMs") or 0)] = row
                out = sorted(merged.values(), key=lambda x: int(x.get("tBucketMs") or 0))
        out = out[-htf_limit:]
    return out


def _fetch_binance_closed_klines(symbol: str, interval: str, limit: int) -> List[Dict[str, Any]]:
    try:
        base = str(os.getenv("BINANCE_FAPI_BASE_URL", "https://fapi.binance.com")).strip() or "https://fapi.binance.com"
        q = urllib.parse.urlencode({"symbol": str(symbol).upper(), "interval": interval, "limit": int(limit)})
        url = f"{base}/fapi/v1/klines?{q}"
        req = urllib.request.Request(url, headers={"User-Agent": "vps-smc/1.0"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        now_ms = int(time.time() * 1000)
        out: List[Dict[str, Any]] = []
        for k in data if isinstance(data, list) else []:
            try:
                ot = int(k[0]); ct = int(k[6])
                if ct >= now_ms:
                    continue
                out.append({
                    "t": ct, "tBucketMs": ot,
                    "o": float(k[1]), "h": float(k[2]), "l": float(k[3]), "c": float(k[4]), "v": float(k[5]),
                })
            except Exception:
                continue
        return out
    except Exception:
        return []



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


def _atr_wilder(candles: List[Dict[str, Any]], period: int) -> List[Optional[float]]:
    if period <= 1 or len(candles) < 2:
        return [None for _ in candles]
    trs: List[float] = []
    for i, c in enumerate(candles):
        h = float(c["h"]); l = float(c["l"])
        if i == 0:
            trs.append(h - l)
            continue
        pc = float(candles[i - 1]["c"])
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    out: List[Optional[float]] = [None for _ in candles]
    if len(trs) < period:
        return out
    atr = sum(trs[:period]) / float(period)
    out[period - 1] = atr
    for i in range(period, len(trs)):
        atr = ((atr * (period - 1)) + trs[i]) / float(period)
        out[i] = atr
    return out


def _atr_sma_tr_available(candles: List[Dict[str, Any]], period: int) -> List[Optional[float]]:
    if period <= 0 or not candles:
        return [None for _ in candles]
    trs: List[float] = []
    for i, c in enumerate(candles):
        h = float(c["h"]); l = float(c["l"])
        if i == 0:
            tr = h - l
        else:
            pc = float(candles[i - 1]["c"])
            tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    out: List[Optional[float]] = []
    for i in range(len(trs)):
        n = min(period, i + 1)
        seg = trs[i + 1 - n:i + 1]
        out.append((sum(seg) / float(n)) if seg else None)
    return out


def detect_sweep(candles: List[Dict[str, Any]], swing_summary: Dict[str, Any], scan_n: int = 20) -> Dict[str, Any]:
    out = {
        "bullish_sweep": False,
        "bearish_sweep": False,
        "sweep_level": None,
        "sweep_extreme": None,
        "sweep_t": None,
        "sweep_tag": None,
        "bullish_sweep_level": None,
        "bullish_sweep_extreme": None,
        "bullish_sweep_t": None,
        "bullish_sweep_tag": None,
        "bearish_sweep_level": None,
        "bearish_sweep_extreme": None,
        "bearish_sweep_t": None,
        "bearish_sweep_tag": None,
        "mixed_sweep_detected": False,
        "selected_direction": "NONE",
        "selected_direction_reason": "no_sweep_direction",
        "selected_sweep_level": None,
        "selected_sweep_extreme": None,
        "selected_sweep_t": None,
        "selected_sweep_tag": None,
    }
    if not candles:
        return out
    scan = candles[-max(1, scan_n):]
    last_low = swing_summary.get("last_swing_low")
    last_high = swing_summary.get("last_swing_high")
    bullish_hit: Optional[Dict[str, Any]] = None
    bearish_hit: Optional[Dict[str, Any]] = None
    if last_low is not None:
        for c in reversed(scan):
            if float(c["l"]) < float(last_low) and float(c["c"]) > float(last_low):
                bullish_hit = {"direction": "LONG", "sweep_level": float(last_low), "sweep_extreme": float(c["l"]), "sweep_t": c["t"], "sweep_tag": "SWEEP_LOW"}
                out.update({"bullish_sweep": True, "sweep_level": float(last_low), "sweep_extreme": float(c["l"]), "sweep_t": c["t"], "sweep_tag": "SWEEP_LOW"})
                out.update({"bullish_sweep_level": float(last_low), "bullish_sweep_extreme": float(c["l"]), "bullish_sweep_t": c["t"], "bullish_sweep_tag": "SWEEP_LOW"})
                break
    if last_high is not None:
        for c in reversed(scan):
            if float(c["h"]) > float(last_high) and float(c["c"]) < float(last_high):
                if not out["bullish_sweep"]:
                    out.update({"sweep_level": float(last_high), "sweep_extreme": float(c["h"]), "sweep_t": c["t"], "sweep_tag": "SWEEP_HIGH"})
                out["bearish_sweep"] = True
                bearish_hit = {"direction": "SHORT", "sweep_level": float(last_high), "sweep_extreme": float(c["h"]), "sweep_t": c["t"], "sweep_tag": "SWEEP_HIGH"}
                out.update({"bearish_sweep_level": float(last_high), "bearish_sweep_extreme": float(c["h"]), "bearish_sweep_t": c["t"], "bearish_sweep_tag": "SWEEP_HIGH"})
                break
    out["mixed_sweep_detected"] = bool(out["bullish_sweep"] and out["bearish_sweep"])
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



# ===== FULL SEMI APPS SCRIPT PARITY STATE MACHINE =====
# Persisted per-symbol StageB lifecycle:
# IDLE -> WAIT_DISPLACEMENT -> WAIT_RETEST -> CONFIRMED -> EXPIRED/INVALID
def _semi_state_file() -> Path:
    return _state_dir() / "vps_smc" / "semi_stageb_state.json"


def _load_semi_states() -> Dict[str, Any]:
    path = _semi_state_file()
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_semi_states(states: Dict[str, Any]) -> None:
    _ensure_dirs()
    path = _semi_state_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(states, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def _semi_key(symbol: Any) -> str:
    return _normalize_symbol(symbol) or "UNKNOWN"


def _semi_bar_t(c: Dict[str, Any]) -> Optional[int]:
    try:
        return int(c.get("t"))
    except Exception:
        try:
            return int(c.get("tBucketMs"))
        except Exception:
            return None


def _semi_bars_since(candles: List[Dict[str, Any]], from_t: Any) -> int:
    try:
        t0 = int(from_t)
    except Exception:
        return 999999
    return len([c for c in (candles or []) if (_semi_bar_t(c) or 0) > t0])


def _semi_last_close(candles: List[Dict[str, Any]]) -> Optional[float]:
    try:
        return float((candles or [])[-1]["c"])
    except Exception:
        return None


def _semi_bar_debug(c: Dict[str, Any], idx: Optional[int] = None) -> Dict[str, Any]:
    t = _semi_bar_t(c)
    out = {
        "o": c.get("o"),
        "h": c.get("h"),
        "l": c.get("l"),
        "c": c.get("c"),
        "t": t,
        "t_wib": _bucket_ms_to_wib_text(t) if t is not None else None,
    }
    if idx is not None:
        out["idx"] = idx
    return out


def _semi_invalidation_debug_defaults() -> Dict[str, Any]:
    return {
        "invalidation_checked_from_t": None,
        "invalidation_checked_from_wib": None,
        "invalidation_threshold": None,
        "invalidation_hit_t": None,
        "invalidation_hit_wib": None,
        "invalidation_hit_price": None,
        "invalidation_hit_bar": None,
        "invalidation_rule": None,
        "invalidation_buffer_pct": None,
    }


def _semi_invalidated(direction: str, candles: List[Dict[str, Any]], sweep_extreme: Any, checked_from_t: Any = None) -> Dict[str, Any]:
    debug = _semi_invalidation_debug_defaults()
    if sweep_extreme is None:
        debug["invalidation_rule"] = "missing_sweep_extreme"
        return {"invalidated": False, **debug}
    try:
        ext = float(sweep_extreme)
    except Exception:
        debug["invalidation_rule"] = "invalid_sweep_extreme"
        return {"invalidated": False, **debug}

    try:
        start_t = int(checked_from_t)
    except Exception:
        start_t = None
    if start_t is None:
        debug["invalidation_rule"] = "missing_selected_sweep_t"
        return {"invalidated": False, **debug}

    # Same idea as Apps Script invalid beyond sweep + buffer.  The window is
    # intentionally anchored to the selected sweep (or caller-provided start)
    # so older candles cannot invalidate a newly selected Stage-B sequence.
    buffer_pct = _env_float("VPS_SMC_INVALID_BUFFER_PCT", 0.08) + _env_float("VPS_SMC_FEES_BUFFER_PCT", 0.03)
    buf = buffer_pct / 100.0
    d = str(direction or "").upper()

    debug["invalidation_checked_from_t"] = start_t
    debug["invalidation_checked_from_wib"] = _bucket_ms_to_wib_text(start_t) if start_t is not None else None
    debug["invalidation_buffer_pct"] = buffer_pct

    if d == "LONG":
        threshold = ext * (1 - buf)
        debug["invalidation_threshold"] = threshold
        debug["invalidation_rule"] = "LONG: post_selected_sweep_low_or_close < sweep_extreme * (1 - buffer)"
    elif d == "SHORT":
        threshold = ext * (1 + buf)
        debug["invalidation_threshold"] = threshold
        debug["invalidation_rule"] = "SHORT: post_selected_sweep_high_or_close > sweep_extreme * (1 + buffer)"
    else:
        debug["invalidation_rule"] = "unsupported_direction"
        return {"invalidated": False, **debug}

    for c in candles or []:
        t = _semi_bar_t(c)
        if start_t is not None and (t is None or t < start_t):
            continue
        try:
            high = float(c["h"])
            low = float(c["l"])
            close = float(c["c"])
        except Exception:
            continue

        hit = False
        hit_price = None
        if d == "SHORT" and (high > threshold or close > threshold):
            hit = True
            hit_price = max(high, close)
        elif d == "LONG" and (low < threshold or close < threshold):
            hit = True
            hit_price = min(low, close)

        if hit:
            debug["invalidation_hit_t"] = t
            debug["invalidation_hit_wib"] = _bucket_ms_to_wib_text(t) if t is not None else None
            debug["invalidation_hit_price"] = hit_price
            debug["invalidation_hit_bar"] = _semi_bar_debug(c)
            return {"invalidated": True, **debug}

    return {"invalidated": False, **debug}


def _semi_find_reclaim_after(
    candles: List[Dict[str, Any]],
    direction: str,
    reclaim_level: Any,
    sweep_t: Any,
    max_bars: int,
) -> Dict[str, Any]:
    out = {"has_reclaim": False, "reclaim_level": reclaim_level, "reclaim_t": None, "reclaim_t_wib": None, "reclaim_idx": None, "reclaim_candle": None, "reason": "not_found", "mode": None}
    try:
        level = float(reclaim_level)
        st = int(sweep_t)
    except Exception:
        out["reason"] = "missing_reclaim_level_or_sweep_t"
        return out

    d = str(direction or "").upper()
    bars = candles or []
    after = [(i, c) for i, c in enumerate(bars) if (_semi_bar_t(c) or 0) >= st]
    after = after[: max(1, int(max_bars or 8)) + 1]

    for i, c in after:
        try:
            low = float(c["l"])
            high = float(c["h"])
            close = float(c["c"])
            t = _semi_bar_t(c)
        except Exception:
            continue

        if d == "LONG" and low < level and close > level:
            out.update({"has_reclaim": True, "reclaim_t": t, "reclaim_t_wib": _bucket_ms_to_wib_text(t) if t is not None else None, "reclaim_idx": i, "reclaim_candle": _semi_bar_debug(c, i), "reason": "ok", "mode": "STRICT"})
            return out
        if d == "SHORT" and high > level and close < level:
            out.update({"has_reclaim": True, "reclaim_t": t, "reclaim_t_wib": _bucket_ms_to_wib_text(t) if t is not None else None, "reclaim_idx": i, "reclaim_candle": _semi_bar_debug(c, i), "reason": "ok", "mode": "STRICT"})
            return out

    # Relax mode: allow close back through level without requiring wick cross on that exact 5m candle.
    if _env_bool("VPS_SMC_RECLAIM_RELAX_MODE", True):
        for i, c in after:
            try:
                close = float(c["c"])
                t = _semi_bar_t(c)
            except Exception:
                continue
            if d == "LONG" and close > level:
                out.update({"has_reclaim": True, "reclaim_t": t, "reclaim_t_wib": _bucket_ms_to_wib_text(t) if t is not None else None, "reclaim_idx": i, "reclaim_candle": _semi_bar_debug(c, i), "reason": "ok", "mode": "RELAX"})
                return out
            if d == "SHORT" and close < level:
                out.update({"has_reclaim": True, "reclaim_t": t, "reclaim_t_wib": _bucket_ms_to_wib_text(t) if t is not None else None, "reclaim_idx": i, "reclaim_candle": _semi_bar_debug(c, i), "reason": "ok", "mode": "RELAX"})
                return out

    return out


def _semi_find_displacement_after(
    candles: List[Dict[str, Any]],
    direction: str,
    after_t: Any,
    max_bars: int,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "has_displacement": False,
        "direction": direction,
        "displacement_t": None,
        "displacement_t_wib": None,
        "displacement_idx": None,
        "displacement_candle": None,
        "body_pct": None,
        "range": None,
        "atr": None,
        "reason": "not_found",
    }
    try:
        t0 = int(after_t)
    except Exception:
        out["reason"] = "missing_after_t"
        return out

    d = str(direction or "").upper()
    bars = candles or []
    candidates = [(i, c) for i, c in enumerate(bars) if (_semi_bar_t(c) or 0) > t0]
    candidates = candidates[: max(1, int(max_bars or 10))]

    atr_len = _env_int("VPS_SMC_DISPLACEMENT_ATR_LEN", 14)
    atr_mult = _env_float("VPS_SMC_DISPLACEMENT_ATR_MULT", 1.15)
    min_body_pct = _env_float("VPS_SMC_DISPLACEMENT_MIN_BODY_PCT", 55.0)

    for i, c in candidates:
        try:
            o = float(c["o"])
            h = float(c["h"])
            l = float(c["l"])
            close = float(c["c"])
            crange = h - l
            if crange <= 0:
                continue
            body = close - o
            body_pct = abs(body) / crange * 100.0
            atr = calc_atr(bars[: i + 1], atr_len)
            if atr is None:
                continue
            directional_ok = (d == "LONG" and body > 0) or (d == "SHORT" and body < 0)
            range_ok = crange >= (atr * atr_mult)
            body_ok = body_pct >= min_body_pct
            if directional_ok and range_ok and body_ok:
                out.update({
                    "has_displacement": True,
                    "displacement_t": _semi_bar_t(c),
                    "displacement_t_wib": _bucket_ms_to_wib_text(_semi_bar_t(c)) if _semi_bar_t(c) is not None else None,
                    "displacement_idx": i,
                    "displacement_candle": _semi_bar_debug(c, i),
                    "body_pct": body_pct,
                    "range": crange,
                    "atr": atr,
                    "reason": "ok",
                })
                return out
        except Exception:
            continue

    return out


def _semi_fvg_candidate_debug(bars: List[Dict[str, Any]], i: int, fvg_type: str, lo: float, hi: float, anchor_idx: Optional[int]) -> Dict[str, Any]:
    c0 = bars[i - 2]
    c1 = bars[i - 1]
    c2 = bars[i]
    t2 = _semi_bar_t(c2)
    relation = None
    if anchor_idx is not None:
        if i < anchor_idx:
            relation = "BEFORE_DISPLACEMENT"
        elif i == anchor_idx:
            relation = "AT_DISPLACEMENT"
        else:
            relation = "AFTER_DISPLACEMENT"
    return {
        "type": fvg_type,
        "lo": min(float(lo), float(hi)),
        "hi": max(float(lo), float(hi)),
        "mid": (float(lo) + float(hi)) / 2.0,
        "t": t2,
        "t_wib": _bucket_ms_to_wib_text(t2) if t2 is not None else None,
        "idx": i,
        "relation_to_displacement": relation,
        "gap_size": abs(float(hi) - float(lo)),
        "c0": _semi_bar_debug(c0, i - 2),
        "c1": _semi_bar_debug(c1, i - 1),
        "c2": _semi_bar_debug(c2, i),
    }


def _semi_collect_fvg_candidates(candles: List[Dict[str, Any]], after_t: Any, lookback: int) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "bullish_fvg_candidates": [],
        "bearish_fvg_candidates": [],
        "anchor_t": None,
        "anchor_t_wib": None,
        "anchor_idx": None,
        "scan_start_idx": None,
        "scan_end_idx": None,
        "reason": "ok",
    }
    bars = candles or []
    if len(bars) < 3:
        out["reason"] = "not_enough_candles"
        return out

    anchor_idx: Optional[int] = None
    anchor_t: Optional[int] = None
    try:
        t0 = int(after_t)
        anchor_t = t0
        for i, c in enumerate(bars):
            if (_semi_bar_t(c) or 0) >= t0:
                anchor_idx = i
                break
    except Exception:
        t0 = None

    # Apps-style Stage-B FVG belongs to the displacement sequence, not merely
    # the most recent global lookback window.  Scan from the displacement
    # anchor with a small pre-window so a gap formed by/around the displacement
    # candle is not missed when VPS selected the next displacement candle.
    pre_bars = max(0, _env_int("VPS_SMC_FVG_PRE_DISPLACEMENT_BARS_5M", 2))
    lb = max(3, int(lookback or 35))
    if anchor_idx is None:
        scan_start = max(2, len(bars) - lb)
        scan_end = len(bars) - 1
    else:
        scan_start = max(2, anchor_idx - pre_bars)
        scan_end = min(len(bars) - 1, anchor_idx + lb)

    out.update({
        "anchor_t": anchor_t,
        "anchor_t_wib": _bucket_ms_to_wib_text(anchor_t) if anchor_t is not None else None,
        "anchor_idx": anchor_idx,
        "scan_start_idx": scan_start,
        "scan_end_idx": scan_end,
    })

    for i in range(scan_start, scan_end + 1):
        try:
            c0 = bars[i - 2]
            c2 = bars[i]
            c0_h = float(c0["h"])
            c0_l = float(c0["l"])
            c2_h = float(c2["h"])
            c2_l = float(c2["l"])
        except Exception:
            continue
        if c0_h < c2_l:
            out["bullish_fvg_candidates"].append(_semi_fvg_candidate_debug(bars, i, "BULLISH", c0_h, c2_l, anchor_idx))
        if c0_l > c2_h:
            out["bearish_fvg_candidates"].append(_semi_fvg_candidate_debug(bars, i, "BEARISH", c2_h, c0_l, anchor_idx))
    return out


def _semi_select_directional_fvg(candidates: Dict[str, Any], direction: str) -> Tuple[Optional[Dict[str, Any]], str]:
    d = str(direction or "").upper()
    key = "bullish_fvg_candidates" if d == "LONG" else "bearish_fvg_candidates" if d == "SHORT" else None
    if key is None:
        return None, "invalid_direction"
    items = list((candidates or {}).get(key) or [])
    if not items:
        return None, f"missing_{'bullish' if d == 'LONG' else 'bearish'}_fvg"

    anchor_idx = (candidates or {}).get("anchor_idx")
    if anchor_idx is None:
        return items[-1], "ok_latest_in_lookback"

    at_or_after = [x for x in items if int(x.get("idx") or 0) >= int(anchor_idx)]
    if at_or_after:
        return at_or_after[0], "ok_first_at_or_after_displacement"
    # If the VPS displacement candle is one/two bars later than Apps, use the
    # nearest pre-displacement gap rather than falling back to OB.
    return items[-1], "ok_nearest_pre_displacement"


def _semi_mitigation_retest_probe(
    candles: List[Dict[str, Any]],
    direction: str,
    candidate: Dict[str, Any],
    max_bars: int,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "has_retest": False,
        "has_rejection_close": False,
        "retest_t": None,
        "retest_t_wib": None,
        "retest_idx": None,
        "retest_candle": None,
        "reason": "not_found",
        "scan_bars": [],
    }
    try:
        zlo = min(float(candidate.get("lo")), float(candidate.get("hi")))
        zhi = max(float(candidate.get("lo")), float(candidate.get("hi")))
        c_idx = int(candidate.get("idx"))
    except Exception:
        out["reason"] = "invalid_candidate"
        return out

    d = str(direction or "").upper()
    if d not in ("LONG", "SHORT"):
        out["reason"] = "invalid_direction"
        return out

    zone_size = max(abs(zhi - zlo), 1e-12)
    near_pad = max(zone_size * _env_float("VPS_SMC_FVG_MITIGATION_NEAR_ZONE_MULT", 0.75), abs((zhi + zlo) / 2.0) * 0.0001)
    probe_lo = zlo - near_pad
    probe_hi = zhi + near_pad
    mid = (zlo + zhi) / 2.0
    bars = candles or []
    touched = False
    for i, c in [(j, b) for j, b in enumerate(bars) if j > c_idx][: max(1, int(max_bars or 18))]:
        try:
            low = float(c["l"])
            high = float(c["h"])
            close = float(c["c"])
            o = float(c["o"])
            t = _semi_bar_t(c)
        except Exception:
            continue

        overlaps = low <= probe_hi and high >= probe_lo
        bearish_reject = d == "SHORT" and close < o and close <= mid
        bullish_reject = d == "LONG" and close > o and close >= mid
        rejection_ok = bool(overlaps and (bearish_reject or bullish_reject))
        bar_debug = _semi_bar_debug(c, i)
        bar_debug.update({
            "candidate_lo": zlo,
            "candidate_hi": zhi,
            "probe_lo": probe_lo,
            "probe_hi": probe_hi,
            "touched_candidate_or_near_zone": overlaps,
            "has_rejection_close": rejection_ok,
            "result": "mitigation_retest_rejection" if rejection_ok else "touched_no_rejection_close" if overlaps else "no_touch",
        })
        out["scan_bars"].append(bar_debug)
        if not overlaps:
            continue
        touched = True
        if rejection_ok:
            close_distance = 0.0 if zlo <= close <= zhi else min(abs(close - zlo), abs(close - zhi))
            out.update({
                "has_retest": True,
                "has_rejection_close": True,
                "retest_t": t,
                "retest_t_wib": _bucket_ms_to_wib_text(t) if t is not None else None,
                "retest_idx": i,
                "retest_candle": _semi_bar_debug(c, i),
                "close_distance_to_zone": close_distance,
                "reason": "ok",
            })
            return out

    out["reason"] = "touched_mitigation_fvg_no_rejection_close" if touched else "waiting_mitigation_fvg_retest"
    return out


def _semi_select_mitigation_fvg(
    candles: List[Dict[str, Any]],
    candidates: Dict[str, Any],
    direction: str,
    max_bars: int,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], str]:
    d = str(direction or "").upper()
    key = "bearish_fvg_candidates" if d == "LONG" else "bullish_fvg_candidates" if d == "SHORT" else None
    if key is None:
        return None, None, "invalid_direction"

    anchor_idx = (candidates or {}).get("anchor_idx")
    items = list((candidates or {}).get(key) or [])
    if anchor_idx is not None:
        items = [x for x in items if int(x.get("idx") or 0) >= int(anchor_idx)]
    if not items:
        return None, None, f"missing_mitigation_{'bearish' if d == 'LONG' else 'bullish'}_fvg"

    rejected: List[Tuple[float, int, Dict[str, Any], Dict[str, Any]]] = []
    last_reason = "waiting_mitigation_fvg_retest"
    for candidate in items:
        probe = _semi_mitigation_retest_probe(candles, d, candidate, max_bars)
        last_reason = probe.get("reason") or last_reason
        if probe.get("has_retest") and probe.get("has_rejection_close"):
            distance = float(probe.get("close_distance_to_zone") or 0.0)
            rejected.append((distance, -int(candidate.get("idx") or 0), candidate, probe))

    if not rejected:
        return None, None, last_reason

    rejected.sort(key=lambda x: (x[0], x[1]))
    _, _, selected, probe = rejected[0]
    return selected, probe, "ok_mitigation_retrace_retest_rejection"


def _semi_find_fvg_after(candles: List[Dict[str, Any]], direction: str, after_t: Any, lookback: int) -> Dict[str, Any]:
    out = {
        "has_fvg": False,
        "fvg_type": None,
        "fvg_lo": None,
        "fvg_hi": None,
        "fvg_t": None,
        "fvg_idx": None,
        "reason": "not_found",
        "selected_fvg_reason": "not_found",
        "selected_fvg_polarity": None,
        "selected_fvg_mode": None,
        "selected_fvg_candidate": None,
        "selected_fvg_retest_probe": None,
        "fvg_rejection_reason": None,
        "bullish_fvg_candidates": [],
        "bearish_fvg_candidates": [],
    }
    try:
        int(after_t)
    except Exception:
        out["reason"] = "missing_after_t"
        out["selected_fvg_reason"] = "missing_after_t"
        return out

    candidates = _semi_collect_fvg_candidates(candles, after_t, lookback)
    out.update({
        "bullish_fvg_candidates": candidates.get("bullish_fvg_candidates") or [],
        "bearish_fvg_candidates": candidates.get("bearish_fvg_candidates") or [],
        "fvg_scan": {k: candidates.get(k) for k in ("anchor_t", "anchor_t_wib", "anchor_idx", "scan_start_idx", "scan_end_idx", "reason")},
    })
    if candidates.get("reason") != "ok":
        out["reason"] = candidates.get("reason") or "not_found"
        out["selected_fvg_reason"] = out["reason"]
        return out

    selected, reason = _semi_select_directional_fvg(candidates, direction)
    out["selected_fvg_reason"] = reason
    mode = "DIRECTIONAL"
    probe = None
    if not selected:
        mitigation, mitigation_probe, mitigation_reason = _semi_select_mitigation_fvg(
            candles,
            candidates,
            direction,
            _env_int("VPS_SMC_RETEST_MAX_AGE_BARS_5M", 18),
        )
        out["fvg_rejection_reason"] = mitigation_reason
        if not mitigation:
            out["reason"] = reason
            out["selected_fvg_reason"] = reason
            return out
        selected = mitigation
        probe = mitigation_probe
        reason = mitigation_reason
        mode = "MITIGATION_RETRACE"
        out["selected_fvg_reason"] = reason

    out.update({
        "has_fvg": True,
        "fvg_type": selected.get("type"),
        "fvg_lo": selected.get("lo"),
        "fvg_hi": selected.get("hi"),
        "fvg_t": selected.get("t"),
        "fvg_idx": selected.get("idx"),
        "fvg_t_wib": selected.get("t_wib"),
        "selected_fvg": selected,
        "selected_fvg_polarity": selected.get("type"),
        "selected_fvg_mode": mode,
        "selected_fvg_candidate": selected,
        "selected_fvg_retest_probe": probe,
        "fvg_rejection_reason": (probe or {}).get("reason") if probe else None,
        "reason": "ok",
    })
    return out

def _semi_find_ob_before_displacement(
    candles: List[Dict[str, Any]],
    direction: str,
    displacement_idx: Any,
    lookback: int,
    min_body_atr: float,
) -> Dict[str, Any]:
    out = {"has_ob": False, "ob_type": None, "ob_lo": None, "ob_hi": None, "ob_t": None, "ob_idx": None, "reason": "not_found"}
    try:
        d_idx = int(displacement_idx)
    except Exception:
        out["reason"] = "missing_displacement_idx"
        return out
    d = str(direction or "").upper()
    if d not in ("LONG", "SHORT"):
        out["reason"] = "invalid_direction"
        return out
    bars = candles or []
    if d_idx <= 0 or d_idx >= len(bars):
        out["reason"] = "invalid_displacement_idx"
        return out
    start = max(0, d_idx - max(1, int(lookback or 12)))
    for i in range(d_idx - 1, start - 1, -1):
        c = bars[i] or {}
        try:
            o = float(c["o"]); close = float(c["c"]); low = float(c["l"]); high = float(c["h"])
        except Exception:
            continue
        directional_candle = (d == "LONG" and close < o) or (d == "SHORT" and close > o)
        if not directional_candle:
            continue
        if float(min_body_atr or 0.0) > 0.0:
            atr = calc_atr(bars[: i + 1], _env_int("VPS_SMC_DISPLACEMENT_ATR_LEN", 14))
            if atr is None or atr <= 0:
                continue
            if abs(close - o) < (float(min_body_atr) * atr):
                continue
        out.update({
            "has_ob": True,
            "ob_type": ("BULL_OB" if d == "LONG" else "BEAR_OB"),
            "ob_lo": low,
            "ob_hi": high,
            "ob_t": _semi_bar_t(c),
            "ob_idx": i,
            "reason": "ok",
        })
        return out
    return out


def _semi_select_poi(fvg: Dict[str, Any], ob: Dict[str, Any]) -> Dict[str, Any]:
    has_fvg = bool((fvg or {}).get("has_fvg"))
    has_ob = bool((ob or {}).get("has_ob"))
    out = {
        "selected_poi_type": None,
        "selected_poi_lo": None,
        "selected_poi_hi": None,
        "selected_poi_mid": None,
        "selected_poi_t": None,
        "selected_poi_t_wib": None,
        "selected_poi_reason": "no_valid_poi",
        "selected_fvg_reason": (fvg or {}).get("selected_fvg_reason") or (fvg or {}).get("reason") or "not_checked",
        "selected_fvg_polarity": (fvg or {}).get("selected_fvg_polarity") or (fvg or {}).get("fvg_type"),
        "selected_fvg_mode": (fvg or {}).get("selected_fvg_mode"),
        "selected_fvg_candidate": (fvg or {}).get("selected_fvg_candidate") or (fvg or {}).get("selected_fvg"),
        "selected_fvg_retest_probe": (fvg or {}).get("selected_fvg_retest_probe"),
        "fvg_rejection_reason": (fvg or {}).get("fvg_rejection_reason"),
        "selected_ob_reason": (ob or {}).get("reason") or "not_checked",
        "fvg_available": has_fvg,
        "ob_available": has_ob,
    }
    if has_fvg:
        lo = float(fvg.get("fvg_lo")); hi = float(fvg.get("fvg_hi"))
        t = fvg.get("fvg_t")
        out.update({
            "selected_poi_type": "FVG",
            "selected_poi_lo": min(lo, hi),
            "selected_poi_hi": max(lo, hi),
            "selected_poi_mid": (lo + hi) / 2.0,
            "selected_poi_t": t,
            "selected_poi_t_wib": _bucket_ms_to_wib_text(t) if t is not None else None,
            "selected_poi_reason": "fvg_preferred",
        })
        return out
    if has_ob:
        lo = float(ob.get("ob_lo")); hi = float(ob.get("ob_hi"))
        t = ob.get("ob_t")
        out.update({
            "selected_poi_type": "OB",
            "selected_poi_lo": min(lo, hi),
            "selected_poi_hi": max(lo, hi),
            "selected_poi_mid": (lo + hi) / 2.0,
            "selected_poi_t": t,
            "selected_poi_t_wib": _bucket_ms_to_wib_text(t) if t is not None else None,
            "selected_poi_reason": "ob_fallback_no_fvg",
        })
    return out

def _semi_find_retest_rejection(
    candles: List[Dict[str, Any]],
    direction: str,
    poi: Dict[str, Any],
    after_t: Any,
    max_bars: int,
) -> Dict[str, Any]:
    out = {"has_retest": False, "has_rejection_close": False, "retest_t": None, "retest_t_wib": None, "retest_idx": None, "retest_candle": None, "reason": "not_found", "scan_bars": []}
    try:
        zlo = min(float(poi.get("selected_poi_lo")), float(poi.get("selected_poi_hi")))
        zhi = max(float(poi.get("selected_poi_lo")), float(poi.get("selected_poi_hi")))
        t0 = int(after_t)
    except Exception:
        out["reason"] = "missing_poi_or_after_t"
        return out

    d = str(direction or "").upper()
    bars = candles or []
    candidates = [(i, c) for i, c in enumerate(bars) if (_semi_bar_t(c) or 0) > t0]
    candidates = candidates[: max(1, int(max_bars or 18))]

    touched = False
    for i, c in candidates:
        try:
            low = float(c["l"])
            high = float(c["h"])
            close = float(c["c"])
            o = float(c["o"])
            t = _semi_bar_t(c)
        except Exception:
            continue

        overlaps = low <= zhi and high >= zlo
        rejection_ok = bool(overlaps and ((d == "LONG" and close > zhi and close > o) or (d == "SHORT" and close < zlo and close < o)))
        bar_debug = _semi_bar_debug(c, i)
        bar_debug.update({
            "poi_lo": zlo,
            "poi_hi": zhi,
            "touched_poi": overlaps,
            "has_rejection_close": rejection_ok,
            "result": "retest_rejection" if rejection_ok else "touched_no_rejection_close" if overlaps else "no_touch",
        })
        out["scan_bars"].append(bar_debug)

        if not overlaps:
            continue

        touched = True
        if rejection_ok:
            out.update({"has_retest": True, "has_rejection_close": True, "retest_t": t, "retest_t_wib": _bucket_ms_to_wib_text(t) if t is not None else None, "retest_idx": i, "retest_candle": _semi_bar_debug(c, i), "reason": "ok"})
            return out

    if touched:
        out["reason"] = "touched_poi_no_rejection_close"
    else:
        out["reason"] = "waiting_poi_retest"
    return out

def _semi_stageb_base(direction: str) -> Dict[str, Any]:
    return {
        "stageb_status": "INVALID",
        "stageb_direction": direction,
        "stageb_reclaim": {"has_reclaim": False, "reclaim_level": None, "reclaim_t": None, "reclaim_idx": None, "reason": "not_built"},
        "stageb_displacement": {"has_displacement": False, "direction": direction, "displacement_t": None, "displacement_idx": None, "body_pct": None, "range": None, "atr": None, "reason": "not_built"},
        "stageb_fvg_poi": {"has_fvg": False, "fvg_type": None, "fvg_lo": None, "fvg_hi": None, "fvg_t": None, "reason": "not_built", "selected_fvg_reason": "not_built", "selected_fvg_polarity": None, "selected_fvg_mode": None, "selected_fvg_candidate": None, "selected_fvg_retest_probe": None, "fvg_rejection_reason": None, "bullish_fvg_candidates": [], "bearish_fvg_candidates": []},
        "stageb_ob_poi": {"has_ob": False, "ob_type": None, "ob_lo": None, "ob_hi": None, "ob_t": None, "reason": "not_built"},
        "stageb_retest": {"has_retest": False, "has_rejection_close": False, "retest_t": None, "retest_idx": None, "reason": "not_built"},
        "selected_poi_type": None, "selected_poi_lo": None, "selected_poi_hi": None, "selected_poi_mid": None, "selected_poi_t": None, "selected_poi_t_wib": None, "selected_poi_reason": "no_valid_poi",
        "fvg_available": False, "ob_available": False,
        "stageb_confirm_reason": None,
        "stageb_invalid_reason": None,
        "stageb_state_machine": "IDLE",
        "selected_sweep_t_wib": None,
        "selected_fvg_reason": "not_built",
        "selected_fvg_polarity": None,
        "selected_fvg_mode": None,
        "selected_fvg_candidate": None,
        "selected_fvg_retest_probe": None,
        "fvg_rejection_reason": None,
        "selected_ob_reason": "not_built",
        "confirmed_t": None,
        "confirmed_t_wib": None,
        **_semi_invalidation_debug_defaults(),
    }


def derive_stageb_direction(context: Dict[str, Any]) -> Dict[str, Any]:
    sweep = (context or {}).get("entry_sweep") or {}
    htf_gate = (context or {}).get("htf_gate") or {}
    htf_appstyle_ready = bool(htf_gate.get("htf_appstyle_ready"))
    if htf_appstyle_ready:
        htf_bias_appstyle = str(htf_gate.get("htf_bias_appstyle") or "").upper()
        swing_bias_num = int(htf_gate.get("htf_swing_bias_num") or 0)
        last_swing_dir = str(((htf_gate.get("htf_last_swing_event") or {}).get("dir") or "")).upper()
        if htf_bias_appstyle in ("BULLISH", "BULL") or swing_bias_num == 1 or last_swing_dir == "LONG":
            return {"stageb_direction": "LONG", "direction_reason": "htf_appstyle_bullish"}
        if htf_bias_appstyle in ("BEARISH", "BEAR") or swing_bias_num == -1 or last_swing_dir == "SHORT":
            return {"stageb_direction": "SHORT", "direction_reason": "htf_appstyle_bearish"}
    htf_bias = str(htf_gate.get("htf_bias") or htf_gate.get("htf_dir") or "").upper()
    if htf_bias in ("BULLISH", "BULL", "LONG"):
        return {"stageb_direction": "LONG", "direction_reason": "htf_bias_bullish"}
    if htf_bias in ("BEARISH", "BEAR", "SHORT"):
        return {"stageb_direction": "SHORT", "direction_reason": "htf_bias_bearish"}
    bullish = bool(sweep.get("bullish_sweep"))
    bearish = bool(sweep.get("bearish_sweep"))
    direction = "NONE"; reason = "no_sweep_direction"
    if bullish and bearish:
        try:
            b_t = int(sweep.get("bullish_sweep_t"))
            s_t = int(sweep.get("bearish_sweep_t"))
        except Exception:
            b_t, s_t = 0, 0
        direction = "LONG" if b_t >= s_t else "SHORT"
        reason = "latest_directional_sweep_fallback"
    elif bullish:
        direction = "LONG"; reason = "bullish_sweep"
    elif bearish:
        direction = "SHORT"; reason = "bearish_sweep"
    return {"stageb_direction": direction, "direction_reason": reason}


def build_stageb_confirmation(
    result: Dict[str, Any],
    stageb_candles: List[Dict[str, Any]],
    dry_run: bool = False,
    no_persist: Optional[bool] = None,
    ignore_persisted_state: bool = False,
) -> Dict[str, Any]:
    symbol = _semi_key(result.get("symbol"))
    direction_info = derive_stageb_direction(result)
    detected_direction = direction_info["stageb_direction"]

    states = {} if ignore_persisted_state else _load_semi_states()
    persist_enabled = not bool(dry_run or no_persist or ignore_persisted_state)

    def _persist_semi_states() -> None:
        if persist_enabled:
            _save_semi_states(states)

    st = states.get(symbol) if isinstance(states.get(symbol), dict) else {}

    active_state = str(st.get("state") or "IDLE").upper()
    active_direction = str(st.get("direction") or "").upper()
    direction = active_direction if active_state in ("WAIT_DISPLACEMENT", "WAIT_RETEST", "CONFIRMED") and active_direction in ("LONG", "SHORT") else detected_direction

    context_status = str(result.get("context_status") or "ERROR")
    liq_gate_status = str(result.get("liq_gate_status") or "BLOCK")
    out = _semi_stageb_base(direction)
    out["selected_direction"] = direction
    out["selected_direction_reason"] = direction_info.get("direction_reason")
    out["selected_sweep_tag"] = None
    out["selected_sweep_level"] = None
    out["selected_sweep_extreme"] = None
    out["selected_sweep_t"] = None
    entry_sweep = result.get("entry_sweep") or {}
    if direction == "LONG":
        out["selected_sweep_tag"] = "SWEEP_LOW"
        out["selected_sweep_level"] = entry_sweep.get("bullish_sweep_level")
        out["selected_sweep_extreme"] = entry_sweep.get("bullish_sweep_extreme")
        out["selected_sweep_t"] = entry_sweep.get("bullish_sweep_t")
    elif direction == "SHORT":
        out["selected_sweep_tag"] = "SWEEP_HIGH"
        out["selected_sweep_level"] = entry_sweep.get("bearish_sweep_level")
        out["selected_sweep_extreme"] = entry_sweep.get("bearish_sweep_extreme")
        out["selected_sweep_t"] = entry_sweep.get("bearish_sweep_t")
    out["selected_sweep_t_wib"] = _bucket_ms_to_wib_text(out.get("selected_sweep_t")) if out.get("selected_sweep_t") is not None else None

    if context_status in ("DATA_GAP", "HTF_DATA_GAP", "ERROR"):
        out["stageb_invalid_reason"] = "context_not_ready"
        out["stageb_confirm_reason"] = "context_not_ready"
        return out

    if direction == "NONE":
        out["stageb_status"] = "IDLE"
        out["stageb_state_machine"] = active_state if active_state in ("WAIT_DISPLACEMENT", "WAIT_RETEST") else "IDLE"
        out["stageb_confirm_reason"] = "no_sweep_direction"
        out["stageb_invalid_reason"] = None
        return out

    if liq_gate_status == "BLOCK" and active_state not in ("WAIT_DISPLACEMENT", "WAIT_RETEST", "CONFIRMED"):
        out["stageb_status"] = "WATCH"
        out["stageb_confirm_reason"] = "liq_gate_blocked"
        return out

    invalidation_start_t = st.get("sweep_t") or out.get("selected_sweep_t")
    invalidation_extreme = st.get("sweep_extreme") or out.get("selected_sweep_extreme") or (result.get("liq_ctx") or {}).get("sweep_extreme")
    if st.get("sweep_t") and not out.get("selected_sweep_t"):
        out["selected_sweep_t"] = st.get("sweep_t")
        out["selected_sweep_level"] = st.get("sweep_level")
        out["selected_sweep_extreme"] = st.get("sweep_extreme")
        out["selected_sweep_t_wib"] = _bucket_ms_to_wib_text(out.get("selected_sweep_t")) if out.get("selected_sweep_t") is not None else None
    invalidation = _semi_invalidated(direction, stageb_candles, invalidation_extreme, invalidation_start_t)
    out.update({k: v for k, v in invalidation.items() if k != "invalidated"})
    if invalidation.get("invalidated"):
        states.pop(symbol, None)
        _persist_semi_states()
        out["stageb_status"] = "INVALID"
        out["stageb_state_machine"] = "INVALID"
        out["stageb_invalid_reason"] = "invalidated_beyond_sweep_extreme_buffer"
        out["stageb_confirm_reason"] = "invalidated_beyond_sweep_extreme_buffer"
        return out

    max_reclaim = _env_int("VPS_SMC_MAX_RECLAIM_AGE_BARS_5M", 8)
    max_disp = _env_int("VPS_SMC_SWEEP_MAX_AGE_BARS_5M", 10)
    max_retest = _env_int("VPS_SMC_RETEST_MAX_AGE_BARS_5M", 18)
    fvg_lookback = _env_int("VPS_SMC_FVG_LOOKBACK_BARS_5M", 35)
    ob_enabled = _env_bool("VPS_SMC_OB_ENABLED", True)
    ob_lookback = _env_int("VPS_SMC_OB_LOOKBACK", 12)
    ob_min_body_atr = _env_float("VPS_SMC_OB_MIN_BODY_ATR", 0.0)

    liq_ctx = result.get("liq_ctx") or {}

    # Existing confirmed state: keep stable, dedup will use confirmed_t.
    if active_state == "CONFIRMED":
        confirmed_t = st.get("confirmed_t")
        if confirmed_t is not None and _semi_bars_since(stageb_candles, confirmed_t) > max_retest:
            states.pop(symbol, None)
            _persist_semi_states()
            out["stageb_status"] = "EXPIRED"
            out["stageb_state_machine"] = "EXPIRED"
            out["stageb_confirm_reason"] = "confirmed_ttl_expired"
            out["stageb_invalid_reason"] = "confirmed_ttl_expired"
            return out

        out["stageb_status"] = "CONFIRMED"
        out["stageb_state_machine"] = "CONFIRMED"
        out["stageb_reclaim"] = st.get("reclaim") or out["stageb_reclaim"]
        out["stageb_displacement"] = st.get("displacement") or out["stageb_displacement"]
        out["stageb_fvg_poi"] = st.get("poi") or out["stageb_fvg_poi"]
        out["stageb_ob_poi"] = st.get("ob") or out["stageb_ob_poi"]
        out.update(st.get("selected_poi") or {})
        out["stageb_retest"] = st.get("retest") or out["stageb_retest"]
        out["confirmed_t"] = confirmed_t
        out["confirmed_t_wib"] = _bucket_ms_to_wib_text(confirmed_t) if confirmed_t else None
        out["stageb_confirm_reason"] = "already_confirmed"
        return out

    # Create WAIT_DISPLACEMENT from a fresh sweep->reclaim.
    if active_state not in ("WAIT_DISPLACEMENT", "WAIT_RETEST"):
        if direction in ("LONG", "SHORT"):
            sweep_t = out.get("selected_sweep_t")
            sweep_level = out.get("selected_sweep_level")
            sweep_extreme = out.get("selected_sweep_extreme")
            if sweep_t is None or sweep_level is None:
                out["stageb_status"] = "WATCH"
                out["stageb_state_machine"] = "IDLE"
                out["stageb_confirm_reason"] = "selected_sweep_missing_for_direction"
                out["stageb_invalid_reason"] = "selected_sweep_missing_for_direction"
                return out
        else:
            sweep_t = entry_sweep.get("sweep_t")
            sweep_level = entry_sweep.get("sweep_level") or liq_ctx.get("sweep_level") or liq_ctx.get("nearest_liq_price")
            sweep_extreme = entry_sweep.get("sweep_extreme") or liq_ctx.get("sweep_extreme")

        if sweep_t is None or sweep_level is None:
            out["stageb_status"] = "IDLE"
            out["stageb_state_machine"] = "IDLE"
            out["stageb_confirm_reason"] = "no_valid_sweep"
            return out

        reclaim = _semi_find_reclaim_after(stageb_candles, direction, sweep_level, sweep_t, max_reclaim)
        out["stageb_reclaim"] = reclaim

        if not reclaim.get("has_reclaim"):
            out["stageb_status"] = "WATCH"
            out["stageb_state_machine"] = "IDLE"
            out["stageb_confirm_reason"] = "reclaim_missing"
            return out

        st = {
            "state": "WAIT_DISPLACEMENT",
            "direction": direction,
            "sweep_t": sweep_t,
            "sweep_level": sweep_level,
            "sweep_extreme": sweep_extreme,
            "reclaim": reclaim,
            "reclaim_t": reclaim.get("reclaim_t"),
            "created_at_utc": _utc_now_iso(),
            "updated_at_utc": _utc_now_iso(),
        }
        states[symbol] = st
        _persist_semi_states()
        active_state = "WAIT_DISPLACEMENT"

    # WAIT_DISPLACEMENT: wait max 10 bars after reclaim.
    if active_state == "WAIT_DISPLACEMENT":
        reclaim = st.get("reclaim") or {}
        reclaim_t = st.get("reclaim_t") or reclaim.get("reclaim_t")
        out["stageb_reclaim"] = reclaim or out["stageb_reclaim"]

        waited = _semi_bars_since(stageb_candles, reclaim_t)
        if waited > max_disp:
            states.pop(symbol, None)
            _persist_semi_states()
            out["stageb_status"] = "EXPIRED"
            out["stageb_state_machine"] = "EXPIRED"
            out["stageb_confirm_reason"] = f"no_displacement_within_{max_disp}_bars"
            out["stageb_invalid_reason"] = f"no_displacement_within_{max_disp}_bars"
            return out

        disp = st.get("displacement") if isinstance(st.get("displacement"), dict) and st.get("displacement", {}).get("has_displacement") else _semi_find_displacement_after(stageb_candles, direction, reclaim_t, max_disp)
        out["stageb_displacement"] = disp

        if not disp.get("has_displacement"):
            out["stageb_status"] = "WATCH"
            out["stageb_state_machine"] = "WAIT_DISPLACEMENT"
            out["stageb_confirm_reason"] = f"wait_displacement_{waited}/{max_disp}"
            st["updated_at_utc"] = _utc_now_iso()
            states[symbol] = st
            _persist_semi_states()
            return out

        poi = st.get("poi") if isinstance(st.get("poi"), dict) and st.get("poi", {}).get("has_fvg") else _semi_find_fvg_after(stageb_candles, direction, disp.get("displacement_t"), fvg_lookback)
        ob = st.get("ob") if isinstance(st.get("ob"), dict) else {"has_ob": False}
        if not ob.get("has_ob") and ob_enabled:
            ob = _semi_find_ob_before_displacement(stageb_candles, direction, disp.get("displacement_idx"), ob_lookback, ob_min_body_atr)
        selected_poi = _semi_select_poi(poi, ob)
        out["stageb_fvg_poi"] = poi
        out["stageb_ob_poi"] = ob
        out.update(selected_poi)

        if not selected_poi.get("selected_poi_type"):
            out["stageb_status"] = "WATCH"
            out["stageb_state_machine"] = "WAIT_DISPLACEMENT"
            out["stageb_confirm_reason"] = "displacement_ok_wait_poi"
            st["displacement"] = disp
            st["poi"] = poi
            st["ob"] = ob
            st["selected_poi"] = selected_poi
            st["updated_at_utc"] = _utc_now_iso()
            states[symbol] = st
            _persist_semi_states()
            return out

        st["state"] = "WAIT_RETEST"
        st["displacement"] = disp
        st["poi"] = poi
        st["ob"] = ob
        st["selected_poi"] = selected_poi
        st["displacement_t"] = disp.get("displacement_t")
        st["poi_t"] = selected_poi.get("selected_poi_t")
        st["updated_at_utc"] = _utc_now_iso()
        states[symbol] = st
        _persist_semi_states()
        active_state = "WAIT_RETEST"

    # WAIT_RETEST: wait max 18 bars after displacement/POI, then require retest + rejection close.
    if active_state == "WAIT_RETEST":
        reclaim = st.get("reclaim") or {}
        disp = st.get("displacement") or {}
        poi = st.get("poi") or {}
        selected_poi = st.get("selected_poi") or {}

        ob = st.get("ob") or out["stageb_ob_poi"]
        # If an earlier build fell back to OB, re-scan the displacement sequence
        # for an Apps-style FVG and prefer it before testing retest/rejection.
        if disp.get("has_displacement") and selected_poi.get("selected_poi_type") != "FVG":
            refreshed_poi = _semi_find_fvg_after(stageb_candles, direction, disp.get("displacement_t"), fvg_lookback)
            if refreshed_poi.get("has_fvg"):
                poi = refreshed_poi
                selected_poi = _semi_select_poi(poi, ob)
                st["poi"] = poi
                st["selected_poi"] = selected_poi
                st["poi_t"] = selected_poi.get("selected_poi_t")

        out["stageb_reclaim"] = reclaim or out["stageb_reclaim"]
        out["stageb_displacement"] = disp or out["stageb_displacement"]
        out["stageb_fvg_poi"] = poi or out["stageb_fvg_poi"]
        out["stageb_ob_poi"] = ob
        out.update(selected_poi)

        after_t = max(int(disp.get("displacement_t") or 0), int(selected_poi.get("selected_poi_t") or 0))
        waited = _semi_bars_since(stageb_candles, after_t)

        if waited > max_retest:
            states.pop(symbol, None)
            _persist_semi_states()
            out["stageb_status"] = "EXPIRED"
            out["stageb_state_machine"] = "EXPIRED"
            out["stageb_confirm_reason"] = f"no_poi_retest_within_{max_retest}_bars"
            out["stageb_invalid_reason"] = f"no_poi_retest_within_{max_retest}_bars"
            return out

        # Apps-style mitigation FVGs already run a stricter near-zone retest probe
        # during POI selection; use only that probe for this mitigation path and
        # leave the generic POI retest unchanged for directional FVG/OB setups.
        mitigation_probe = None
        if (
            selected_poi.get("selected_poi_type") == "FVG"
            and selected_poi.get("selected_fvg_mode") == "MITIGATION_RETRACE"
        ):
            probe = selected_poi.get("selected_fvg_retest_probe") or (poi or {}).get(
                "selected_fvg_retest_probe"
            )
            if (
                isinstance(probe, dict)
                and probe.get("has_retest")
                and probe.get("has_rejection_close")
            ):
                mitigation_probe = dict(probe)
                mitigation_probe.setdefault("reason", "ok")
                mitigation_probe["source"] = "selected_fvg_retest_probe"

        retest = mitigation_probe or _semi_find_retest_rejection(
            stageb_candles,
            direction,
            selected_poi,
            after_t,
            max_retest,
        )
        out["stageb_retest"] = retest

        if not (retest.get("has_retest") and retest.get("has_rejection_close")):
            out["stageb_status"] = "WATCH"
            out["stageb_state_machine"] = "WAIT_RETEST"
            out["stageb_confirm_reason"] = f"wait_retest_rejection:{retest.get('reason')}:{waited}/{max_retest}"
            st["updated_at_utc"] = _utc_now_iso()
            states[symbol] = st
            _persist_semi_states()
            return out

        confirmed_t = retest.get("retest_t") or after_t
        st["state"] = "CONFIRMED"
        st["retest"] = retest
        st["confirmed_t"] = confirmed_t
        st["updated_at_utc"] = _utc_now_iso()
        states[symbol] = st
        _persist_semi_states()

        out["stageb_status"] = "CONFIRMED"
        out["stageb_state_machine"] = "CONFIRMED"
        out["stageb_retest"] = retest
        out["confirmed_t"] = confirmed_t
        out["confirmed_t_wib"] = _bucket_ms_to_wib_text(confirmed_t) if confirmed_t else None
        out["stageb_confirm_reason"] = (
            "mitigation_fvg_retest_rejection_close"
            if mitigation_probe
            else "poi_retest_rejection_close"
        )
        return out

    out["stageb_status"] = "WATCH"
    out["stageb_state_machine"] = active_state
    out["stageb_confirm_reason"] = "watch"
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
    appstyle_min_candles = _env_int("VPS_SMC_HTF_APPSTYLE_MIN_CANDLES", 180)
    eq_band_pct = _env_float("VPS_SMC_HTF_EQ_BAND_PCT", 0.30)
    if len(htf) < min_candles:
        return {
            "htf_gate_status": "HTF_DATA_GAP", "htf_dir": "NEUTRAL", "htf_bias": "MIXED", "htf_structure": "UNKNOWN",
            "htf_location": "UNKNOWN", "htf_close": None, "htf_last_swing_high": None, "htf_last_swing_low": None,
            "htf_mid": None, "htf_reason": "not_enough_4h_candles",
            "htf_bias_appstyle": "MIXED", "htf_dir_appstyle": "NEUTRAL", "htf_structure_appstyle": "UNKNOWN",
            "htf_location_appstyle": "UNKNOWN", "htf_eq_appstyle": "NONE", "htf_swing_bias_num": 0,
            "htf_internal_bias_num": 0, "htf_last_swing_event": None, "htf_last_internal_event": None, "htf_pd_appstyle": "UNKNOWN",
            "htf_appstyle_ready": False, "htf_appstyle_reason": "not_enough_4h_candles_for_gate",
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
    appstyle_ready = len(htf) >= appstyle_min_candles
    appstyle_reason = "ok" if appstyle_ready else f"need_{appstyle_min_candles}_4h_candles_got_{len(htf)}"
    try:
        app = _build_htf_appstyle(htf) if appstyle_ready else {
            "htf_bias_appstyle": "MIXED", "htf_dir_appstyle": "NEUTRAL", "htf_structure_appstyle": "RANGE",
            "htf_location_appstyle": "UNKNOWN", "htf_eq_appstyle": "NONE", "htf_swing_bias_num": 0,
            "htf_internal_bias_num": 0, "htf_last_swing_event": None, "htf_last_internal_event": None, "htf_pd_appstyle": "UNKNOWN",
        }
    except Exception:
        appstyle_ready = False
        appstyle_reason = "appstyle_exception_fallback"
        app = {
            "htf_bias_appstyle": "MIXED",
            "htf_dir_appstyle": "NEUTRAL",
            "htf_structure_appstyle": "UNKNOWN",
            "htf_location_appstyle": "UNKNOWN",
            "htf_eq_appstyle": "NONE",
            "htf_swing_bias_num": 0,
            "htf_internal_bias_num": 0,
            "htf_last_swing_event": None,
            "htf_last_internal_event": None,
            "htf_pd_appstyle": "UNKNOWN",
        }
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
        "htf_bias_appstyle": app.get("htf_bias_appstyle"),
        "htf_dir_appstyle": app.get("htf_dir_appstyle"),
        "htf_structure_appstyle": app.get("htf_structure_appstyle"),
        "htf_location_appstyle": app.get("htf_location_appstyle"),
        "htf_eq_appstyle": app.get("htf_eq_appstyle"),
        "htf_swing_bias_num": app.get("htf_swing_bias_num"),
        "htf_internal_bias_num": app.get("htf_internal_bias_num"),
        "htf_last_swing_event": app.get("htf_last_swing_event"),
        "htf_last_internal_event": app.get("htf_last_internal_event"),
        "htf_pd_appstyle": app.get("htf_pd_appstyle"),
        "htf_appstyle_ready": appstyle_ready,
        "htf_appstyle_reason": appstyle_reason,
    }


def _build_htf_appstyle(htf: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not htf:
        return {
            "htf_bias_appstyle": "MIXED", "htf_dir_appstyle": "NEUTRAL", "htf_structure_appstyle": "RANGE",
            "htf_location_appstyle": "UNKNOWN", "htf_eq_appstyle": "NONE", "htf_swing_bias_num": 0, "htf_internal_bias_num": 0,
            "htf_last_swing_event": None, "htf_last_internal_event": None, "htf_pd_appstyle": "UNKNOWN", "htf_eq_appstyle_obj": None,
        }
    eq_lookback = 6
    atr_vals = _atr_sma_tr_available(htf, 100)
    swing_len = 20
    internal_len = 5
    state: Dict[str, Any] = {
        "swingBias": 0, "internalBias": 0,
        "swingHigh": None, "swingLow": None, "internalHigh": None, "internalLow": None,
        "swingHighHist": [], "swingLowHist": [],
        "lastSwingEvent": None, "lastInternalEvent": None,
        "lastEQ": None, "eqAbsThrLast": None,
    }

    def _confirm_pivot(i: int, ln: int) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        pidx = i - ln
        if pidx - ln < 0 or pidx + ln >= len(htf):
            return None, None
        p = htf[pidx]
        ph = float(p["h"]); pl = float(p["l"])
        left_h = max(float(htf[j]["h"]) for j in range(pidx - ln, pidx))
        right_h = max(float(htf[j]["h"]) for j in range(pidx + 1, pidx + 1 + ln))
        left_l = min(float(htf[j]["l"]) for j in range(pidx - ln, pidx))
        right_l = min(float(htf[j]["l"]) for j in range(pidx + 1, pidx + 1 + ln))
        hi = {"level": ph, "t": p.get("t"), "idx": pidx} if (ph > left_h and ph >= right_h) else None
        lo = {"level": pl, "t": p.get("t"), "idx": pidx} if (pl < left_l and pl <= right_l) else None
        return hi, lo

    for i in range(1, len(htf)):
        s_hi, s_lo = _confirm_pivot(i, swing_len)
        if s_hi is not None:
            state["swingHigh"] = s_hi
            state["swingHighHist"].append(s_hi)
            hist = state["swingHighHist"][-(eq_lookback + 1):-1]
            thr = atr_vals[i] * 0.15 if atr_vals[i] is not None else None
            state["eqAbsThrLast"] = thr
            if thr is not None and hist:
                for p in hist:
                    if abs(float(s_hi["level"]) - float(p["level"])) <= thr:
                        state["lastEQ"] = {
                            "type": "EQH",
                            "level": float(s_hi["level"]),
                            "t": s_hi.get("t"),
                            "refT": p.get("t"),
                            "thr_abs": thr,
                        }
                        break
        if s_lo is not None:
            state["swingLow"] = s_lo
            state["swingLowHist"].append(s_lo)
            hist = state["swingLowHist"][-(eq_lookback + 1):-1]
            thr = atr_vals[i] * 0.15 if atr_vals[i] is not None else None
            state["eqAbsThrLast"] = thr
            if thr is not None and hist:
                for p in hist:
                    if abs(float(s_lo["level"]) - float(p["level"])) <= thr:
                        state["lastEQ"] = {
                            "type": "EQL",
                            "level": float(s_lo["level"]),
                            "t": s_lo.get("t"),
                            "refT": p.get("t"),
                            "thr_abs": thr,
                        }
                        break

        i_hi, i_lo = _confirm_pivot(i, internal_len)
        if i_hi is not None:
            state["internalHigh"] = i_hi
        if i_lo is not None:
            state["internalLow"] = i_lo

        prev_close = float(htf[i - 1]["c"]); close = float(htf[i]["c"])
        if state["swingHigh"] is not None and prev_close <= float(state["swingHigh"]["level"]) and close > float(state["swingHigh"]["level"]):
            et = "CHOCH" if state["swingBias"] == -1 else "BOS"
            state["swingBias"] = 1
            state["lastSwingEvent"] = {"type": et, "dir": "LONG", "level": float(state["swingHigh"]["level"]), "t": htf[i].get("t"), "mode": "swing"}
        elif state["swingLow"] is not None and prev_close >= float(state["swingLow"]["level"]) and close < float(state["swingLow"]["level"]):
            et = "CHOCH" if state["swingBias"] == 1 else "BOS"
            state["swingBias"] = -1
            state["lastSwingEvent"] = {"type": et, "dir": "SHORT", "level": float(state["swingLow"]["level"]), "t": htf[i].get("t"), "mode": "swing"}

        if state["internalHigh"] is not None and prev_close <= float(state["internalHigh"]["level"]) and close > float(state["internalHigh"]["level"]):
            et = "CHOCH" if state["internalBias"] == -1 else "BOS"
            state["internalBias"] = 1
            state["lastInternalEvent"] = {"type": et, "dir": "LONG", "level": float(state["internalHigh"]["level"]), "t": htf[i].get("t"), "mode": "internal"}
        elif state["internalLow"] is not None and prev_close >= float(state["internalLow"]["level"]) and close < float(state["internalLow"]["level"]):
            et = "CHOCH" if state["internalBias"] == 1 else "BOS"
            state["internalBias"] = -1
            state["lastInternalEvent"] = {"type": et, "dir": "SHORT", "level": float(state["internalLow"]["level"]), "t": htf[i].get("t"), "mode": "internal"}

    swing_high = state["swingHigh"]
    swing_low = state["swingLow"]
    close_now = float(htf[-1]["c"])
    loc = "UNKNOWN"
    pd = "EQUILIBRIUM"
    if swing_high and swing_low and close_now is not None:
        sh = float(swing_high["level"]); sl = float(swing_low["level"])
        mid = (sh + sl) / 2.0
        rng = abs(sh - sl)
        band = 0.05 * rng
        if close_now > (mid + band):
            loc = "PREMIUM"; pd = "PREMIUM"
        elif close_now < (mid - band):
            loc = "DISCOUNT"; pd = "DISCOUNT"
        else:
            loc = "EQUILIBRIUM"; pd = "EQUILIBRIUM"

    eq_obj = state["lastEQ"]
    eq_state = str((eq_obj or {}).get("type") or "NONE")

    dir_app = "NEUTRAL"
    bias_app = "MIXED"
    swing_event = state["lastSwingEvent"]
    if str((swing_event or {}).get("dir") or "").upper() == "LONG" or int(state["swingBias"] or 0) == 1:
        dir_app = "LONG"; bias_app = "BULLISH"
    elif str((swing_event or {}).get("dir") or "").upper() == "SHORT" or int(state["swingBias"] or 0) == -1:
        dir_app = "SHORT"; bias_app = "BEARISH"
    return {
        "htf_bias_appstyle": bias_app,
        "htf_dir_appstyle": dir_app,
        "htf_structure_appstyle": ((swing_event or {}).get("type") or "RANGE"),
        "htf_location_appstyle": loc,
        "htf_eq_appstyle": eq_state,
        "htf_swing_bias_num": int(state["swingBias"] or 0),
        "htf_internal_bias_num": int(state["internalBias"] or 0),
        "htf_last_swing_event": swing_event,
        "htf_last_internal_event": state["lastInternalEvent"],
        "htf_pd_appstyle": pd,
        "htf_eq_appstyle_obj": eq_obj,
    }


def build_liquidity_context(entry: List[Dict[str, Any]], entry_swing_summary: Dict[str, Any], entry_eq: Dict[str, Any], entry_sweep: Dict[str, Any]) -> Dict[str, Any]:
    near_liq_pct = _env_float("VPS_SMC_NEAR_LIQ_PCT", 0.35)
    candidate_max_dist_pct = _env_float("VPS_SMC_CANDIDATE_MAX_DIST_PCT", 0.35)
    candidate_between_near_pct = _env_float("VPS_SMC_CANDIDATE_BETWEEN_NEAR_PCT", 0.18)
    require_near = _env_bool("VPS_SMC_REQUIRE_AT_OR_NEAR_LIQ", True)
    close = float(entry[-1]["c"]) if entry else None
    last_high = entry_swing_summary.get("last_swing_high")
    last_low = entry_swing_summary.get("last_swing_low")
    eqh = entry_eq.get("eqh") or []
    eql = entry_eq.get("eql") or []

    def _candidate_distance_pct(candidate: Dict[str, Any]) -> float:
        if close is None or close == 0 or candidate.get("price") is None:
            return float("inf")
        return abs(close - float(candidate["price"])) / abs(close) * 100.0

    def _candidate_t(candidate: Dict[str, Any]) -> int:
        try:
            return int(candidate.get("t") or 0)
        except Exception:
            return 0

    def _candidate_to_debug(candidate: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(candidate)
        out["dist_pct"] = _candidate_distance_pct(candidate) if close is not None else None
        return out

    def _cluster_candidate(cluster: Dict[str, Any], source: str) -> Optional[Dict[str, Any]]:
        if cluster.get("price") is None:
            return None
        return {
            "source": source,
            "price": float(cluster["price"]),
            "touches": int(cluster.get("count") or cluster.get("touches") or 0),
            "t": _candidate_t(cluster),
            "band_pct": float(cluster.get("band_pct") or 0.15),
        }

    def _level_candidate(price: Any, source: str, touches: int = 1, t: Any = None, extra: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        if price is None:
            return None
        candidate = {
            "source": source,
            "price": float(price),
            "touches": int(touches),
            "t": _candidate_t({"t": t}),
            "band_pct": 0.15,
        }
        if extra:
            candidate.update(extra)
        return candidate

    def _active_sweep_candidate(side: str) -> Optional[Dict[str, Any]]:
        if side == "BSL" and bool(entry_sweep.get("bearish_sweep")):
            return _level_candidate(
                entry_sweep.get("bearish_sweep_level"),
                "ACTIVE_BEARISH_SWEEP",
                touches=3,
                t=entry_sweep.get("bearish_sweep_t"),
                extra={
                    "sweep_extreme": entry_sweep.get("bearish_sweep_extreme"),
                    "sweep_tag": entry_sweep.get("bearish_sweep_tag") or "SWEEP_HIGH",
                },
            )
        if side == "SSL" and bool(entry_sweep.get("bullish_sweep")):
            return _level_candidate(
                entry_sweep.get("bullish_sweep_level"),
                "ACTIVE_BULLISH_SWEEP",
                touches=3,
                t=entry_sweep.get("bullish_sweep_t"),
                extra={
                    "sweep_extreme": entry_sweep.get("bullish_sweep_extreme"),
                    "sweep_tag": entry_sweep.get("bullish_sweep_tag") or "SWEEP_LOW",
                },
            )
        return None

    def _zone_from_candidate(candidate: Optional[Dict[str, Any]], zone_type: str) -> Optional[Dict[str, Any]]:
        if candidate is None or candidate.get("price") is None:
            return None
        mid = float(candidate["price"])
        band_pct = float(candidate.get("band_pct") or 0.15)
        half = abs(mid) * (band_pct / 100.0)
        if half == 0:
            half = 1e-12
        z_from = mid - half
        z_to = mid + half
        zone = {
            "from": min(z_from, z_to),
            "to": max(z_from, z_to),
            "mid": mid,
            "touches": int(candidate.get("touches") or 0),
            "type": zone_type,
            "source": candidate.get("source"),
        }
        if candidate.get("sweep_extreme") is not None:
            zone["sweep_extreme"] = float(candidate["sweep_extreme"])
        if candidate.get("sweep_tag") is not None:
            zone["sweep_tag"] = candidate.get("sweep_tag")
        return zone

    def _candidate_is_close(candidate: Dict[str, Any]) -> bool:
        if close is None or close == 0 or candidate.get("price") is None:
            return False
        zone = _zone_from_candidate(candidate, "TMP")
        if zone is not None and float(zone["from"]) <= close <= float(zone["to"]):
            return True
        return _candidate_distance_pct(candidate) <= near_liq_pct

    def _build_candidates(clusters: List[Dict[str, Any]], side: str) -> List[Dict[str, Any]]:
        source = "EQH_CLUSTER" if side == "BSL" else "EQL_CLUSTER"
        candidates: List[Dict[str, Any]] = []
        for cluster in clusters:
            candidate = _cluster_candidate(cluster, source)
            if candidate is not None:
                candidates.append(candidate)
        active = _active_sweep_candidate(side)
        if active is not None:
            candidates.append(active)
        if side == "BSL":
            fallback = _level_candidate(last_high, "LAST_SWING_HIGH", touches=1, t=entry_swing_summary.get("last_swing_high_t"))
        else:
            fallback = _level_candidate(last_low, "LAST_SWING_LOW", touches=1, t=entry_swing_summary.get("last_swing_low_t"))
        if fallback is not None:
            candidates.append(fallback)
        return candidates

    def _pick_relevant_candidate(candidates: List[Dict[str, Any]], active_source: str) -> Tuple[Optional[Dict[str, Any]], str]:
        if not candidates:
            return None, "no_candidates"
        active = [c for c in candidates if c.get("source") == active_source]
        active_near = [c for c in active if _candidate_is_close(c)]
        if active_near:
            active_near.sort(key=lambda c: (_candidate_distance_pct(c), -_candidate_t(c)))
            return active_near[0], "active_sweep_close_preferred"

        ranked = list(candidates)
        ranked.sort(
            key=lambda c: (
                0 if _candidate_is_close(c) else 1,
                _candidate_distance_pct(c),
                -int(c.get("touches") or 0),
                -_candidate_t(c),
            )
        )
        selected = ranked[0]
        if _candidate_is_close(selected):
            return selected, "nearest_close_candidate"
        if str(selected.get("source") or "").endswith("CLUSTER"):
            return selected, "nearest_cluster_candidate"
        return selected, "fallback_last_swing"

    bsl_candidates_raw = _build_candidates(eqh, "BSL")
    ssl_candidates_raw = _build_candidates(eql, "SSL")
    selected_bsl, selected_bsl_reason = _pick_relevant_candidate(bsl_candidates_raw, "ACTIVE_BEARISH_SWEEP")
    selected_ssl, selected_ssl_reason = _pick_relevant_candidate(ssl_candidates_raw, "ACTIVE_BULLISH_SWEEP")

    bsl_zone = _zone_from_candidate(selected_bsl, "BSL")
    ssl_zone = _zone_from_candidate(selected_ssl, "SSL")

    nearest = None
    dist_pct = None
    dist_to_bsl_pct = None
    dist_to_ssl_pct = None
    liq_ctx_appstyle = "NONE"
    if close is not None and close != 0:
        zone_prices: List[Dict[str, Any]] = []
        if bsl_zone is not None:
            zone_prices.append({"type": "BSL", "price": float(bsl_zone["mid"])})
            if float(bsl_zone["from"]) <= close <= float(bsl_zone["to"]):
                liq_ctx_appstyle = "AT_BSL"
            dist_to_bsl_pct = abs(close - float(bsl_zone["mid"])) / close * 100.0
        if ssl_zone is not None:
            zone_prices.append({"type": "SSL", "price": float(ssl_zone["mid"])})
            if float(ssl_zone["from"]) <= close <= float(ssl_zone["to"]):
                liq_ctx_appstyle = "AT_SSL"
            dist_to_ssl_pct = abs(close - float(ssl_zone["mid"])) / close * 100.0
        if zone_prices:
            nearest = min(zone_prices, key=lambda z: abs(close - float(z["price"])))
            dist_pct = abs(close - float(nearest["price"])) / close * 100.0

        if liq_ctx_appstyle == "NONE":
            if dist_to_bsl_pct is not None and dist_to_bsl_pct <= near_liq_pct:
                liq_ctx_appstyle = "NEAR_BSL"
            elif dist_to_ssl_pct is not None and dist_to_ssl_pct <= near_liq_pct:
                liq_ctx_appstyle = "NEAR_SSL"
            elif bsl_zone is not None and ssl_zone is not None:
                bsl_mid = float(bsl_zone["mid"])
                ssl_mid = float(ssl_zone["mid"])
                upper = max(bsl_mid, ssl_mid)
                lower = min(bsl_mid, ssl_mid)
                if lower < close < upper:
                    liq_ctx_appstyle = "BETWEEN_BSL_SSL"
                elif close > upper:
                    liq_ctx_appstyle = "ABOVE_BSL"
                elif close < lower:
                    liq_ctx_appstyle = "BELOW_SSL"

    at_or_near = liq_ctx_appstyle in ("AT_BSL", "NEAR_BSL", "AT_SSL", "NEAR_SSL")
    liq_gate_status = "PASS"
    liq_reason = None
    candidate_reason_appstyle = "pass_at_or_near"
    if liq_ctx_appstyle == "BETWEEN_BSL_SSL":
        if dist_pct is not None and dist_pct <= candidate_between_near_pct:
            candidate_reason_appstyle = "pass_between_near"
        else:
            candidate_reason_appstyle = "block_between_far"
            liq_gate_status = "BLOCK"
            liq_reason = "between_bsl_ssl_too_far"
    elif not at_or_near:
        candidate_reason_appstyle = "block_not_at_or_near"
        liq_gate_status = "BLOCK"
        liq_reason = "not_at_or_near_bsl_ssl"
    if dist_pct is not None and dist_pct > candidate_max_dist_pct and liq_gate_status == "PASS":
        candidate_reason_appstyle = "block_candidate_too_far"
        liq_gate_status = "BLOCK"
        liq_reason = "candidate_too_far"
    if require_near and liq_gate_status != "PASS":
        liq_gate_status = "BLOCK"
    elif not require_near:
        liq_gate_status = "PASS"
        liq_reason = None
        candidate_reason_appstyle = "liquidity_gate_disabled"

    liq_ctx = {
        "last_buy_side_liq": float(last_high) if last_high is not None else None,
        "last_sell_side_liq": float(last_low) if last_low is not None else None,
        "eqh_count": len(eqh),
        "eql_count": len(eql),
        "bsl_candidates": [_candidate_to_debug(c) for c in bsl_candidates_raw],
        "ssl_candidates": [_candidate_to_debug(c) for c in ssl_candidates_raw],
        "selected_bsl_reason": selected_bsl_reason,
        "selected_ssl_reason": selected_ssl_reason,
        "nearest_liq_type": (nearest or {}).get("type"),
        "nearest_liq_price": (nearest or {}).get("price"),
        "dist_to_zone_pct": dist_pct,
        "at_or_near_liq": at_or_near,
        "liq_ctx_appstyle": liq_ctx_appstyle,
        "bsl_zone": bsl_zone,
        "ssl_zone": ssl_zone,
        "dist_to_bsl_pct": dist_to_bsl_pct,
        "dist_to_ssl_pct": dist_to_ssl_pct,
        "candidate_reason_appstyle": candidate_reason_appstyle,
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
    stageb = (result.get("stageb_confirmation") or {})
    poi_lo = stageb.get("selected_poi_lo")
    poi_hi = stageb.get("selected_poi_hi")
    if poi_lo is None or poi_hi is None:
        return "INVALID_PLAN", None, {"score": 0, "priority": "C", "risk_mult": 0.5, "reasons": ["missing_selected_poi"]}, "invalid_plan"
    entry_lo = min(float(poi_lo), float(poi_hi))
    entry_hi = max(float(poi_lo), float(poi_hi))
    entry_mid = (entry_lo + entry_hi) / 2.0
    liq_ctx = result.get("liq_ctx") or {}
    htf_gate = result.get("htf_gate") or {}
    entry_sw = result.get("entry_swing_summary") or {}
    htf_sw = result.get("htf_swing_summary") or {}
    sweep_extreme = stageb.get("selected_sweep_extreme") or liq_ctx.get("sweep_extreme")
    buffer_pct = _env_float("VPS_SMC_INVALID_BUFFER_PCT", 0.08) + _env_float("VPS_SMC_FEES_BUFFER_PCT", 0.03)
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
        tp1 = (entry_mid + (_env_float("VPS_SMC_TP1_R", 1.0) * risk)) if (raw_tp1 is None or float(raw_tp1) <= entry_mid) else float(raw_tp1)

        raw_tp2 = htf_sw.get("last_swing_high")
        tp2 = (entry_mid + (_env_float("VPS_SMC_TP2_R", 1.5) * risk)) if (raw_tp2 is None or float(raw_tp2) <= entry_mid) else float(raw_tp2)

        tp3 = (entry_mid + (_env_float("VPS_SMC_TP3_R", 2.5) * risk)) if _env_bool("VPS_SMC_TP3_ENABLED", True) else None
        rr_tp2 = (tp2 - entry_mid) / risk
        sane = sl < entry_mid and tp2 > entry_mid
    else:
        raw_tp1 = liq_ctx.get("last_sell_side_liq") or entry_sw.get("last_swing_low")
        tp1 = (entry_mid - (_env_float("VPS_SMC_TP1_R", 1.0) * risk)) if (raw_tp1 is None or float(raw_tp1) >= entry_mid) else float(raw_tp1)

        raw_tp2 = htf_sw.get("last_swing_low")
        tp2 = (entry_mid - (_env_float("VPS_SMC_TP2_R", 1.5) * risk)) if (raw_tp2 is None or float(raw_tp2) >= entry_mid) else float(raw_tp2)

        tp3 = (entry_mid - (_env_float("VPS_SMC_TP3_R", 2.5) * risk)) if _env_bool("VPS_SMC_TP3_ENABLED", True) else None
        rr_tp2 = (entry_mid - tp2) / risk
        sane = sl > entry_mid and tp2 < entry_mid
    if not sane:
        return "INVALID_PLAN", None, {"score": 0, "priority": "C", "risk_mult": 0.5, "reasons": ["sanity_check_failed"]}, "invalid_plan"
    if rr_tp2 < _env_float("VPS_SMC_RR_MIN_TP2", 1.50):
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

    # === SMC_SCORE_V2_SHADOW_FREQTRADE_ADOPT_20260614 ===
    # Freqtrade/FreqAI-inspired scoring:
    # keep legacy score active for safety, but compute score_v2 + components for audit.
    components = {}
    score_v2_reasons = ["score_v2_shadow_freqtrade_adopt"]

    # === SCORE_V2_SAFE_LOCALS_20260614 ===
    # Shadow score must never break scanner. Missing internals become neutral/default.
    _score_v2_htf_gate = str(locals().get("htf_gate") or "")
    _score_v2_htf_bias = str(locals().get("htf_bias") or "")
    _score_v2_direction = str(locals().get("direction") or "")
    _score_v2_liq_decision = str(
        locals().get("liq_decision")
        or locals().get("liquidity_decision")
        or locals().get("liq_status")
        or locals().get("liq_gate")
        or ""
    )
    _score_v2_reclaim_ok = bool(locals().get("reclaim_ok", False))
    _score_v2_displacement_ok = bool(locals().get("displacement_ok", False))
    _score_v2_fvg_ok = bool(locals().get("fvg_ok", False))
    _score_v2_rr_tp2 = locals().get("rr_tp2", 0)
    _score_v2_structure_ctx = locals().get("structure_ctx") or {}
    _score_v2_entry_mid = locals().get("entry_mid", None)
    _score_v2_entry_hi = locals().get("entry_hi", None)
    _score_v2_entry_lo = locals().get("entry_lo", None)
    _score_v2_risk = locals().get("risk", None)

    def _add_comp(name, value, reason):
        try:
            value = float(value)
        except Exception:
            value = 0.0
        components[name] = components.get(name, 0.0) + value
        score_v2_reasons.append(f"{reason}:{value:+.1f}")

    # --- HTF component, max-ish 20 ---
    if _score_v2_htf_gate == "PASS":
        _add_comp("htf", 10, "htf_pass")
    else:
        _add_comp("penalty", -12, "htf_not_pass")

    try:
        if str(_score_v2_htf_bias).lower() == str(_score_v2_direction).lower():
            _add_comp("htf", 8, "htf_bias_align")
        elif str(_score_v2_htf_bias).lower() in ("long", "short"):
            _add_comp("penalty", -10, "htf_bias_conflict")
    except Exception:
        pass

    # === SCORE_V2_FIX_HTF_LOC_NAMEERROR_20260614 ===
    _score_v2_htf_loc = str(
        locals().get("htf_loc")
        or locals().get("htf_location")
        or locals().get("htf_loc_tag")
        or ""
    )

    if _score_v2_htf_loc == "MIXED_RANGE":
        _add_comp("htf", 3, "htf_mixed_range")

    # --- Liquidity component, max-ish 15 ---
    if _score_v2_liq_decision == "PASS":
        _add_comp("liquidity", 12, "liq_pass")
    elif _score_v2_liq_decision == "BLOCK":
        _add_comp("penalty", -12, "liq_block")
    else:
        _add_comp("liquidity", 4, "liq_neutral")

    # --- Trigger component, max-ish 25 ---
    if _score_v2_reclaim_ok:
        _add_comp("trigger", 7, "reclaim_ok")
    else:
        _add_comp("penalty", -4, "reclaim_missing")

    if _score_v2_displacement_ok:
        _add_comp("trigger", 7, "displacement_ok")
    else:
        _add_comp("penalty", -3, "displacement_missing")

    if _score_v2_fvg_ok:
        _add_comp("trigger", 8, "fvg_ok")
    else:
        _add_comp("penalty", -5, "fvg_missing")

    # --- RR component, max-ish 15 ---
    try:
        rr2 = float(_score_v2_rr_tp2)
    except Exception:
        rr2 = 0.0

    if rr2 >= 2.0:
        _add_comp("rr", 15, "rr_tp2_ge_2")
    elif rr2 >= 1.5:
        _add_comp("rr", 11, "rr_tp2_ge_1_5")
    elif rr2 >= 1.2:
        _add_comp("rr", 7, "rr_tp2_ge_1_2")
    elif rr2 >= 1.0:
        _add_comp("rr", 3, "rr_tp2_ge_1")
    else:
        _add_comp("penalty", -12, "rr_tp2_bad")

    # --- Structure component, max-ish 10 ---
    try:
        structure_bias = str((_score_v2_structure_ctx or {}).get("bias") or "").lower()
        if structure_bias == str(_score_v2_direction).lower():
            _add_comp("structure", 8, "structure_align")
        elif structure_bias in ("long", "short"):
            _add_comp("penalty", -8, "structure_conflict")
    except Exception:
        pass

    # --- Quality component, max-ish 10 ---
    try:
        entry_mid_safe = float(_score_v2_entry_mid)
        zone_width_pct = abs(float(_score_v2_entry_hi) - float(_score_v2_entry_lo)) / entry_mid_safe * 100.0 if entry_mid_safe else 999.0
        sl_dist_pct = abs(float(_score_v2_risk)) / entry_mid_safe * 100.0 if entry_mid_safe else 999.0

        if zone_width_pct <= 0.20:
            _add_comp("quality", 5, "tight_entry_zone")
        elif zone_width_pct <= 0.50:
            _add_comp("quality", 3, "ok_entry_zone")
        else:
            _add_comp("penalty", -6, "wide_entry_zone")

        if sl_dist_pct <= 1.50:
            _add_comp("quality", 5, "compact_sl")
        elif sl_dist_pct <= 3.00:
            _add_comp("quality", 2, "medium_sl")
        else:
            _add_comp("penalty", -5, "wide_sl")

    except Exception as e:
        score_v2_reasons.append(f"quality_calc_error:{type(e).__name__}")

    score_v2_raw = sum(components.values())
    score_v2 = int(round(max(0.0, min(100.0, score_v2_raw))))

    # Legacy score remains active for now.
    score_v1 = score
    active_score = score_v1

    score_detail_v2 = {
        "score_version": "shadow_v2_freqtrade_adopt_20260614",
        "score_v1": score_v1,
        "score_v2": score_v2,
        "active_score": active_score,
        "components": components,
        "reasons_v2": score_v2_reasons,
    }

    reasons.append(f"score_v2_shadow={score_v2}")

    score_detail = {"score": score, "priority": priority, "risk_mult": risk_mult, "reasons": reasons, **score_detail_v2}
    return "VALID", plan, score_detail, None


def _vps_smc_log_core_candidate_shadow_v1(result: Dict[str, Any]) -> None:
    """Emit relaxed Core SMC candidate telemetry in SHADOW ONLY mode.

    This does not route anything to the execution bridge.  It exists to measure
    whether relaxed SMC core candidates appear earlier than current strict
    CONFIRMED signals and whether RR decays before pre-entry.
    """
    if not _env_bool("VPS_SMC_CORE_SHADOW_ENABLED", True):
        return
    try:
        symbol = str(result.get("symbol") or "").upper().strip()
        if not symbol:
            return
        stageb = result.get("stageb_confirmation") or {}
        entry_sweep = result.get("entry_sweep") or {}
        liq_ctx = result.get("liq_ctx") or {}
        htf_gate = result.get("htf_gate") or {}
        reclaim = stageb.get("stageb_reclaim") or {}

        direction = str(
            stageb.get("selected_direction")
            or stageb.get("stageb_direction")
            or entry_sweep.get("selected_direction")
            or entry_sweep.get("selected_direction_appstyle")
            or "NONE"
        ).upper()
        if direction not in ("LONG", "SHORT"):
            return

        sweep_t = (
            stageb.get("selected_sweep_t")
            or entry_sweep.get("selected_sweep_t")
            or entry_sweep.get("sweep_t")
            or liq_ctx.get("sweep_t")
        )
        sweep_level = (
            stageb.get("selected_sweep_level")
            or entry_sweep.get("selected_sweep_level")
            or entry_sweep.get("sweep_level")
            or liq_ctx.get("sweep_level")
            or liq_ctx.get("nearest_liq_price")
        )
        sweep_extreme = (
            stageb.get("selected_sweep_extreme")
            or entry_sweep.get("selected_sweep_extreme")
            or entry_sweep.get("sweep_extreme")
            or liq_ctx.get("sweep_extreme")
        )
        reclaim_t = reclaim.get("reclaim_t") or stageb.get("reclaim_t")
        reclaim_level = reclaim.get("reclaim_level") or sweep_level
        has_reclaim = bool(reclaim.get("has_reclaim") or reclaim_t)

        latest_close = result.get("close_15m_used")
        try:
            entry_mid = float(latest_close)
            sweep_extreme_f = float(sweep_extreme)
        except Exception:
            entry_mid = None
            sweep_extreme_f = None

        buffer_pct = _env_float("VPS_SMC_INVALID_BUFFER_PCT", 0.08) + _env_float("VPS_SMC_FEES_BUFFER_PCT", 0.03)
        buffer_mult = buffer_pct / 100.0
        sl = None
        risk = None
        target_r = _env_float("CORE_SMC_TARGET_R", _env_float("RR_TARGET_R", 1.2))
        if entry_mid is not None and sweep_extreme_f is not None:
            sl = sweep_extreme_f * (1 - buffer_mult) if direction == "LONG" else sweep_extreme_f * (1 + buffer_mult)
            risk = (entry_mid - sl) if direction == "LONG" else (sl - entry_mid)
        rr_at_emit = None
        tp1 = None
        if risk is not None and risk > 0:
            if direction == "LONG":
                raw_tp1 = liq_ctx.get("last_buy_side_liq")
                try:
                    tp1 = float(raw_tp1) if raw_tp1 is not None and float(raw_tp1) > entry_mid else entry_mid + target_r * risk
                except Exception:
                    tp1 = entry_mid + target_r * risk
                rr_at_emit = (tp1 - entry_mid) / risk
            else:
                raw_tp1 = liq_ctx.get("last_sell_side_liq")
                try:
                    tp1 = float(raw_tp1) if raw_tp1 is not None and float(raw_tp1) < entry_mid else entry_mid - target_r * risk
                except Exception:
                    tp1 = entry_mid - target_r * risk
                rr_at_emit = (entry_mid - tp1) / risk

        htf_bias = str(htf_gate.get("htf_bias") or "UNKNOWN").upper()
        htf_structure = str(htf_gate.get("htf_structure") or "UNKNOWN").upper()
        htf_hard_extreme_block = bool(
            (direction == "LONG" and htf_bias in ("HARD_BEARISH", "HARD_BEARISH_EXTREME"))
            or (direction == "SHORT" and htf_bias in ("HARD_BULLISH", "HARD_BULLISH_EXTREME"))
        )
        geometry_valid = bool(entry_mid is not None and sl is not None and tp1 is not None and risk is not None and risk > 0)
        core_ok = bool(sweep_t is not None and sweep_level is not None and has_reclaim and geometry_valid and not htf_hard_extreme_block)
        min_rr = _env_float("CORE_SMC_MIN_RR_AT_EMIT", 1.2)
        rr_ok = bool(rr_at_emit is not None and (float(rr_at_emit) + 1e-9) >= min_rr)
        state = "CORE_CANDIDATE" if core_ok and rr_ok else "CORE_INVALID"

        bucket_src = reclaim_t or sweep_t or result.get("latest_entry_close_time_ms")
        bucket_ms = _bucket_ms(bucket_src, _env_int("VPS_SMC_DEDUP_BUCKET_MIN", 15))
        candidate_id = f"{symbol}|{direction}|CORE|{bucket_ms or sweep_t or result.get('latest_entry_close_time_ms')}"
        poi = stageb.get("stageb_fvg_poi") or {}
        disp = stageb.get("stageb_displacement") or {}
        row = {
            "created_at_utc": _utc_now_iso(),
            "event_type": "SMC_CORE_CANDIDATE_SHADOW_V1",
            "source": "VPS_SMC",
            "mode": "SHADOW_ONLY",
            "execution_allowed": False,
            "candidate_id": candidate_id,
            "symbol": symbol,
            "direction": direction,
            "state": state,
            "core_ok": core_ok,
            "rr_ok": rr_ok,
            "min_rr_at_emit": min_rr,
            "target_r": target_r,
            "rr_target_rewrite_expected": _env_float("RR_TARGET_R", 1.2),
            "entry_mid": entry_mid,
            "sl": sl,
            "tp1": tp1,
            "risk": risk,
            "rr_at_emit": rr_at_emit,
            "sweep_t": sweep_t,
            "sweep_t_wib": _bucket_ms_to_wib_text(sweep_t) if sweep_t is not None else None,
            "sweep_level": sweep_level,
            "sweep_extreme": sweep_extreme,
            "reclaim_t": reclaim_t,
            "reclaim_t_wib": _bucket_ms_to_wib_text(reclaim_t) if reclaim_t is not None else None,
            "reclaim_level": reclaim_level,
            "has_reclaim": has_reclaim,
            "htf_bias": htf_bias,
            "htf_structure": htf_structure,
            "htf_hard_extreme_block": htf_hard_extreme_block,
            "structure_15m": result.get("structure_15m"),
            "fvg_present": bool(poi.get("has_fvg") or poi.get("fvg_lo") is not None or poi.get("fvg_hi") is not None),
            "fvg_type": poi.get("fvg_type"),
            "fvg_lo": poi.get("fvg_lo"),
            "fvg_hi": poi.get("fvg_hi"),
            "displacement_present": bool(disp.get("has_displacement")),
            "liq_dist_to_zone_pct": liq_ctx.get("dist_to_zone_pct"),
            "strict_shadow_state": result.get("shadow_state"),
            "strict_confirm_reason": stageb.get("stageb_confirm_reason"),
            "strict_invalid_reason": stageb.get("stageb_invalid_reason"),
        }
        _append_jsonl(_log_dir() / "core_smc_shadow_candidates_v1.jsonl", row)
        return row
    except Exception as exc:
        _append_jsonl(_log_dir() / "vps_smc_errors.jsonl", {
            "created_at_utc": _utc_now_iso(),
            "event_type": "CORE_SMC_SHADOW_LOG_ERROR",
            "symbol": result.get("symbol"),
            "error": str(exc),
        })


def _vps_smc_route_core_quant_v1(result: Dict[str, Any], core_row: Optional[Dict[str, Any]], logged_signal_keys: Dict[str, Any]) -> bool:
    """Route Core SMC candidate to existing production bridge with quant guard.

    This does NOT bypass live safety:
    - RR target remains 1.2R
    - ML hard gate remains in app/main.py bridge
    - cost gate/orderbook/pre-entry refresh remain in app/main.py bridge
    - cooldown/max trades/max position remain in bridge/risk layer
    """
    if not _env_bool("VPS_SMC_CORE_QUANT_PRODUCTION_ENABLED", True):
        return False
    if not isinstance(core_row, dict):
        return False
    core_state = str(core_row.get("state") or "")
    if core_state != "CORE_CANDIDATE":
        result["core_quant_skip_reason"] = (
            f"core_state_not_candidate:{core_state}:"
            f"core_ok={bool(core_row.get('core_ok'))}:"
            f"rr_ok={bool(core_row.get('rr_ok'))}:"
            f"strict={core_row.get('strict_shadow_state')}:"
            f"reason={core_row.get('strict_confirm_reason') or core_row.get('strict_invalid_reason')}"
        )
        return False
    if not bool(core_row.get("core_ok")):
        result["core_quant_skip_reason"] = "core_ok_false"
        return False
    if not bool(core_row.get("rr_ok")):
        result["core_quant_skip_reason"] = f"rr_ok_false:rr={core_row.get('rr_at_emit')}:min={core_row.get('min_rr_at_emit')}"
        return False
    if bool(core_row.get("htf_hard_extreme_block")):
        result["core_quant_skip_reason"] = "htf_hard_extreme_block"
        return False

    source_mode = str(os.getenv("SIGNAL_SOURCE_MODE", "APPS_SCRIPT_ONLY")).strip().upper() or "APPS_SCRIPT_ONLY"
    vps_exec_enabled = _env_bool("VPS_SMC_EXECUTION_ENABLED", False)
    competitor_mode = str(os.getenv("VPS_SMC_COMPETITOR_MODE", "SHADOW_ONLY")).strip().upper() or "SHADOW_ONLY"
    if not (
        EXECUTION_BRIDGE_HANDLER is not None
        and source_mode == "VPS_SMC_PRIMARY"
        and vps_exec_enabled
        and competitor_mode == "PRODUCTION_SIGNAL"
    ):
        result["core_quant_skip_reason"] = "execution_bridge_not_enabled"
        return False

    symbol = str(core_row.get("symbol") or result.get("symbol") or "").upper().strip()
    direction = str(core_row.get("direction") or "").upper().strip()
    if direction not in ("LONG", "SHORT") or not symbol:
        result["core_quant_skip_reason"] = "invalid_symbol_or_direction"
        return False

    try:
        entry = float(core_row.get("entry_mid"))
        sl = float(core_row.get("sl"))
        risk = float(core_row.get("risk"))
    except Exception:
        result["core_quant_skip_reason"] = "missing_entry_sl_risk"
        return False

    if risk <= 0:
        result["core_quant_skip_reason"] = "invalid_risk"
        return False

    target_r = _env_float("RR_TARGET_R", 1.2)
    if direction == "LONG":
        tp = entry + (target_r * risk)
        if not (sl < entry < tp):
            result["core_quant_skip_reason"] = "plan_sanity_failed"
            return False
    else:
        tp = entry - (target_r * risk)
        if not (tp < entry < sl):
            result["core_quant_skip_reason"] = "plan_sanity_failed"
            return False

    rr_emit = float(core_row.get("rr_at_emit") or 0.0)
    min_rr = _env_float("CORE_QUANT_MIN_RR_AT_EMIT", _env_float("CORE_SMC_MIN_RR_AT_EMIT", 1.2))
    if rr_emit + 1e-9 < min_rr:
        result["core_quant_skip_reason"] = "rr_below_core_quant_min"
        return False

    # Quant score: simple, transparent v1. ML gate still runs later in main bridge.
    score = 60.0
    reasons = ["core_base_60"]

    if bool(core_row.get("has_reclaim")):
        score += 10.0
        reasons.append("+10_reclaim")
    if bool(core_row.get("fvg_present")):
        score += 5.0
        reasons.append("+5_fvg_feature")
    if bool(core_row.get("displacement_present")):
        score += 5.0
        reasons.append("+5_displacement_feature")
    if rr_emit >= 1.5:
        score += 5.0
        reasons.append("+5_rr_ge_1_5")
    if rr_emit >= 2.0:
        score += 5.0
        reasons.append("+5_rr_ge_2_0")
    if rr_emit >= _env_float("CORE_QUANT_RR_OUTLIER_WARN", 5.0):
        score -= 5.0
        reasons.append("-5_rr_outlier_warn")

    htf_bias = str(core_row.get("htf_bias") or "UNKNOWN").upper()
    htf_structure = str(core_row.get("htf_structure") or "UNKNOWN").upper()
    if htf_bias == "MIXED" and htf_structure == "RANGE":
        score += 5.0
        reasons.append("+5_htf_mixed_range")
    elif (direction == "LONG" and htf_bias == "BULLISH") or (direction == "SHORT" and htf_bias == "BEARISH"):
        score += 5.0
        reasons.append("+5_htf_bias_align")
    elif (direction == "LONG" and htf_bias == "BEARISH") or (direction == "SHORT" and htf_bias == "BULLISH"):
        score -= 10.0
        reasons.append("-10_htf_bias_conflict")

    s15 = str(core_row.get("structure_15m") or "UNKNOWN").upper()
    if (direction == "LONG" and s15 == "UP") or (direction == "SHORT" and s15 == "DOWN"):
        score += 5.0
        reasons.append("+5_structure_align")
    elif s15 in ("UP", "DOWN"):
        score -= 5.0
        reasons.append("-5_structure_conflict")

    score = max(0.0, min(100.0, score))
    min_score = _env_float("CORE_QUANT_MIN_SCORE", 70.0)
    result["core_quant_score"] = score
    result["core_quant_min_score"] = min_score
    if score < min_score:
        result["core_quant_skip_reason"] = f"score_below_min:{score:.1f}<{min_score:.1f}"
        return False

    priority = "A" if score >= 85 else ("B" if score >= 70 else "C")
    risk_mult = 1.0 if priority == "A" else (0.75 if priority == "B" else 0.5)

    bucket_src = core_row.get("reclaim_t") or core_row.get("sweep_t") or result.get("latest_entry_close_time_ms")
    bucket_ms = _bucket_ms(bucket_src, _env_int("CORE_QUANT_DEDUP_BUCKET_MIN", _env_int("VPS_SMC_DEDUP_BUCKET_MIN", 15)))
    signal_key = f"{symbol}|{direction}|COREQ|{bucket_ms or core_row.get('candidate_id') or _utc_now_iso()}"

    if _env_bool("CORE_QUANT_DEDUP_ENABLED", True) and signal_key in logged_signal_keys:
        result["core_quant_skip_reason"] = "duplicate"
        return False

    bridge_payload = {
        "source": "VPS_SMC_CORE_QUANT",
        "signal_source": "VPS_SMC_CORE_QUANT",
        "event_type": "CORE_QUANT_CONFIRMED",
        "source_mode": "VPS_SMC_PRIMARY",
        "execution_owner": "VPS_SMC_CORE_QUANT",
        "status": "CONFIRMED",
        "state": "CONFIRMED",
        "signal_key": signal_key,
        "signal_id": signal_key,
        "candidate_id": core_row.get("candidate_id"),
        "symbol": symbol,
        "pair": f"BINANCE:{symbol}.P",
        "direction": direction,

        # Market/current core plan. Main bridge still refreshes and can reject stale plan.
        "entry": entry,
        "entry_mid": entry,
        "entry_lo": entry,
        "entry_hi": entry,
        "sl": sl,
        "tp1": tp,
        "tp2": tp,
        "tp3": tp,
        "raw_tp1": core_row.get("tp1"),
        "raw_tp2": None,
        "raw_tp3": None,
        "tp_normalized": True,
        "tp_normalize_reason": "core_quant_single_target_1_2r",
        "plan_sanity_ok": True,
        "plan_sanity_reason": "core_quant_geometry_ok",
        "plan_invalid": sl,

        "score": score,
        "priority": priority,
        "risk_mult": risk_mult,
        "confirmed_bucket_ms": bucket_ms,
        "signal_time_wib": _bucket_ms_to_wib_text(bucket_ms),

        "htf_bias": core_row.get("htf_bias"),
        "htf_structure": core_row.get("htf_structure"),
        "structure_15m": core_row.get("structure_15m"),
        "sweep_extreme": core_row.get("sweep_extreme"),
        "reclaim_level": core_row.get("reclaim_level"),
        "fvg_type": core_row.get("fvg_type"),
        "fvg_lo": core_row.get("fvg_lo"),
        "fvg_hi": core_row.get("fvg_hi"),
        "dist_to_zone_pct": core_row.get("liq_dist_to_zone_pct"),
        "notes": ",".join(reasons),
        "core_quant": {
            "enabled": True,
            "score": score,
            "min_score": min_score,
            "rr_at_emit": rr_emit,
            "min_rr": min_rr,
            "target_r": target_r,
            "strict_shadow_state": core_row.get("strict_shadow_state"),
            "strict_confirm_reason": core_row.get("strict_confirm_reason"),
            "strict_invalid_reason": core_row.get("strict_invalid_reason"),
            "fvg_present": core_row.get("fvg_present"),
            "displacement_present": core_row.get("displacement_present"),
        },
    }

    signal_row = dict(bridge_payload)
    signal_row.update({
        "created_at_utc": _utc_now_iso(),
        "mode": "PRODUCTION_SIGNAL_CORE_QUANT",
        "execution_allowed": True,
    })
    _append_jsonl(_log_dir() / "vps_smc_core_quant_signals.jsonl", signal_row)

    try:
        result["execution_bridge"] = EXECUTION_BRIDGE_HANDLER(bridge_payload)
    except Exception as exc:
        result["execution_bridge"] = {"ok": False, "error": str(exc)}
        _append_jsonl(_log_dir() / "vps_smc_errors.jsonl", {
            "created_at_utc": _utc_now_iso(),
            "event_type": "CORE_QUANT_EXECUTION_BRIDGE_ERROR",
            "symbol": symbol,
            "signal_key": signal_key,
            "error": str(exc),
            "traceback": __import__("traceback").format_exc(),
        })
        return False

    # === CORE_QUANT_DEDUP_LOCK_ONLY_ON_ACCEPT_20260623 ===
    # Do NOT lock CoreQuant dedup just because bridge was attempted.
    # Lock only when bridge looks accepted/order-ish, or when explicitly configured to lock rejects.
    bridge_res = result.get("execution_bridge")
    bridge_blob = str(bridge_res)

    bridge_decision = ""
    bridge_reason = ""
    if isinstance(bridge_res, dict):
        bridge_decision = str(
            bridge_res.get("decision")
            or bridge_res.get("bridge_decision")
            or bridge_res.get("status")
            or bridge_res.get("state")
            or ""
        )
        bridge_reason = str(
            bridge_res.get("reason")
            or bridge_res.get("bridge_reason")
            or bridge_res.get("error")
            or ""
        )

    rejectish = bridge_decision.upper() in {
        "NO_TRADE",
        "REJECT",
        "REFRESH_RECOMPUTE_DONE",
        "INVALID",
        "CANCELED",
        "CANCELLED",
        "ERROR",
    }

    orderish = any(tok in bridge_blob for tok in (
        "ORDER_PLACED",
        "LIVE_SMALL_CAPITAL_ENTRY",
        "POSITION_CONFIRMED",
        "PROTECTION",
        "entry_and_protection_processed",
        "orderId",
        "order_id",
        "clientOrderId",
    ))

    ok_accepted = bool(isinstance(bridge_res, dict) and bridge_res.get("ok") is True and not rejectish)
    lock_on_reject = _env_bool("CORE_QUANT_DEDUP_LOCK_ON_REJECT", False)

    # === CORE_QUANT_LOCK_MARGIN_INSUFFICIENT_20260627 ===
    # Keep normal no-trade/RR reject retry behavior, but do not spam Binance
    # when entry failed because account margin is insufficient.
    try:
        _bridge_txt = json.dumps(bridge_result, ensure_ascii=False)
    except Exception:
        _bridge_txt = str(bridge_result)
    margin_insufficient_reject = bool(
        rejectish
        and (
            "-2019" in _bridge_txt
            or "Margin is insufficient" in _bridge_txt
            or "margin is insufficient" in _bridge_txt.lower()
        )
    )

# === CORE_QUANT_DEDUP_ORDERISH_ONLY_20260623 ===
    should_lock_dedup = bool(orderish or margin_insufficient_reject or (lock_on_reject and rejectish))

    if should_lock_dedup:
        logged_signal_keys[signal_key] = signal_row["created_at_utc"]
        result["core_quant_dedup_locked"] = True
    else:
        result["core_quant_dedup_locked"] = False
        result["core_quant_dedup_not_locked"] = True
        _append_jsonl(_log_dir() / "vps_smc_core_quant_dedup_not_locked.jsonl", {
            "created_at_utc": _utc_now_iso(),
            "event_type": "CORE_QUANT_DEDUP_NOT_LOCKED",
            "symbol": symbol,
            "direction": direction,
            "signal_key": signal_key,
            "bridge_decision": bridge_decision,
            "bridge_reason": bridge_reason,
            "orderish": orderish,
            "ok_accepted": ok_accepted,
            "lock_on_reject": lock_on_reject,
        })
    result["signal_logged"] = True
    result["signal_key"] = signal_key
    result["plan_status"] = "VALID"
    result["plan"] = {
        "entry_lo": entry,
        "entry_hi": entry,
        "entry_mid": entry,
        "sl": sl,
        "invalid": sl,
        "tp1": tp,
        "tp2": tp,
        "tp3": tp,
        "rr_tp2": target_r,
    }
    result["score_detail"] = {"score": score, "priority": priority, "risk_mult": risk_mult, "reasons": reasons}
    result["signal_skip_reason"] = None
    result["core_quant_routed"] = True
    return True

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

    tolerance_pct = _env_float("VPS_SMC_COMPARE_PRICE_TOLERANCE_PCT", 0.05)

    def _to_float(value: Any) -> Optional[float]:
        try:
            if value is None or str(value).strip() == "":
                return None
            return float(value)
        except Exception:
            return None

    def _is_price_mismatch(a: Any, b: Any) -> bool:
        av = _to_float(a)
        bv = _to_float(b)
        if av is None or bv is None:
            return False
        denom = max(abs(av), abs(bv), 1e-12)
        rel_diff_pct = abs(av - bv) / denom * 100.0
        return rel_diff_pct > float(tolerance_pct)

    latest_diag_by_symbol: Dict[str, Dict[str, Any]] = {}
    diag_rows = _read_jsonl(_log_dir() / "vps_smc_gate_diagnostics.jsonl")
    if diag_rows:
        latest = diag_rows[-1] if isinstance(diag_rows[-1], dict) else {}
        for d in (latest.get("diagnostics") or []):
            if not isinstance(d, dict):
                continue
            sym = _normalize_symbol(d.get("symbol"))
            if sym:
                latest_diag_by_symbol[sym] = d

    app_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    for row in app_rows_raw:
        sample_type = str(row.get("sample_type") or "").strip().upper()
        if sample_type == "VALIDATION_SAMPLE":
            continue
        if sample_type and sample_type != "FORWARD_SHADOW_PAPER":
            continue
        decision = str(row.get("execution_decision") or row.get("decision") or "").strip().upper()
        if decision not in ("ACCEPT", "REJECT", "BACKUP_ONLY"):
            continue
        symbol = _normalize_symbol(row.get("symbol") or row.get("pair"))
        if not symbol or symbol not in target_set:
            continue
        dt = _parse_apps_signal_time(row)
        if dt is None or dt < cutoff:
            continue
        app_by_symbol.setdefault(symbol, []).append({"signal_key": row.get("signal_key") or row.get("signal_id"), "direction_raw": row.get("direction") or row.get("dir"), "direction": _normalize_direction(row.get("direction") or row.get("dir"),), "decision": decision, "status": row.get("state") or row.get("status") or decision, "entry": row.get("entry") or row.get("entry_mid"), "sl": row.get("sl"), "tp1": row.get("tp1"), "tp2": row.get("tp2"), "tp3": row.get("tp3"), "created_at_utc": dt.isoformat(), "ts": dt.timestamp(),})

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
        vps_by_symbol.setdefault(symbol, []).append({"signal_key": row.get("signal_key"), "direction": _normalize_direction(row.get("direction")), "status": row.get("state") or row.get("status"), "entry": row.get("entry") or row.get("entry_mid"), "sl": row.get("sl"), "tp1": row.get("tp1"), "tp2": row.get("tp2"), "tp3": row.get("tp3"), "score": row.get("score"), "priority": row.get("priority"), "created_at_utc": dt.isoformat(), "ts": dt.timestamp(),})

    rows: List[Dict[str, Any]] = []
    seen_keys: set[str] = set()
    for symbol in sorted(target_set):
        app_items = sorted(app_by_symbol.get(symbol, []), key=lambda x: float(x.get("ts") or 0), reverse=True)
        vps_items = sorted(vps_by_symbol.get(symbol, []), key=lambda x: float(x.get("ts") or 0), reverse=True)
        app = app_items[0] if app_items else None
        vps = vps_items[0] if vps_items else None

        classification = "NO_SIGNAL_BOTH"
        reason = "no_signal_both"
        if app and vps:
            if (app.get("direction") or "") != (vps.get("direction") or ""):
                classification = "CONFLICT"; reason = "direction_mismatch"
            elif any(_is_price_mismatch(app.get(k), vps.get(k)) for k in ("entry", "sl", "tp1", "tp2", "tp3")):
                classification = "CONFLICT"; reason = "price_plan_mismatch"
            else:
                classification = "OVERLAP_AGREE"; reason = "direction_match"
        elif app and not vps:
            classification = "APP_ONLY"
            blocker = str((latest_diag_by_symbol.get(symbol) or {}).get("final_blocker") or "").strip()
            reason = f"vps_blocker:{blocker}" if blocker else "app_only_no_vps_confirm"
        elif vps and not app:
            classification = "VPS_ONLY"
            reason = "vps_only_no_app_signal"

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
            "app_direction": (app or {}).get("direction_raw"), "app_status": (app or {}).get("status"), "app_entry": (app or {}).get("entry"), "app_sl": (app or {}).get("sl"), "app_tp1": (app or {}).get("tp1"), "app_tp2": (app or {}).get("tp2"), "app_tp3": (app or {}).get("tp3"),
            "app_decision": (app or {}).get("decision"),
            "app_created_at_utc": (app or {}).get("created_at_utc"),
            "vps_signal_key": (vps or {}).get("signal_key"),
            "vps_direction": (vps or {}).get("direction"), "vps_status": (vps or {}).get("status"), "vps_entry": (vps or {}).get("entry"), "vps_sl": (vps or {}).get("sl"), "vps_tp1": (vps or {}).get("tp1"), "vps_tp2": (vps or {}).get("tp2"), "vps_tp3": (vps or {}).get("tp3"),
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
        "no_signal_both": len([r for r in rows if r.get("classification") == "NO_SIGNAL_BOTH"]),
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




def _classify_final_blocker(result: Dict[str, Any]) -> str:
    if str(result.get("status") or "") == "DATA_GAP":
        return "DATA_GAP"
    htf_gate = result.get("htf_gate") or {}
    liq_gate_status = str(result.get("liq_gate_status") or "").upper()
    stageb = result.get("stageb_confirmation") or {}
    state_machine_raw = stageb.get("stageb_state_machine")
    state_machine = str(state_machine_raw or "")
    status = str(stageb.get("stageb_status") or "")
    confirm_reason = str(stageb.get("stageb_confirm_reason") or "")
    if str(htf_gate.get("htf_gate_status") or "").upper() == "BLOCK":
        return "HTF_BLOCKED"
    if liq_gate_status == "BLOCK" and (confirm_reason == "liq_gate_blocked" or state_machine in ("", "IDLE")):
        return "LIQ_BLOCKED"
    selected_direction = str(stageb.get("selected_direction") or "NONE").upper()
    if selected_direction not in ("LONG", "SHORT"):
        return "NO_SWEEP_DIRECTION"
    if not stageb.get("selected_sweep_tag"):
        return "SELECTED_SWEEP_MISSING"
    if str(stageb.get("stageb_invalid_reason") or "") == "invalidated_beyond_sweep_extreme_buffer":
        return "INVALIDATED_BEYOND_SWEEP"
    if state_machine == "WAIT_DISPLACEMENT":
        return "WAIT_DISPLACEMENT"
    if "no_displacement_within_" in confirm_reason:
        return "NO_DISPLACEMENT_EXPIRED"
    if state_machine == "WAIT_RETEST":
        return "WAIT_RETEST"
    if "no_poi_retest_within_" in confirm_reason:
        return "WAIT_RETEST"
    if "selected_sweep_missing_for_direction" in confirm_reason:
        return "SELECTED_SWEEP_MISSING"
    if "reclaim_missing" in confirm_reason:
        return "WAIT_RECLAIM"
    if "wait_poi" in confirm_reason:
        return "WAIT_POI"
    if status != "CONFIRMED":
        return "PLAN_INVALID"
    if str(result.get("plan_status") or "") != "VALID":
        return "PLAN_INVALID"
    if str(result.get("signal_skip_reason") or "") == "score_below_min":
        return "SCORE_BELOW_MIN"
    if result.get("signal_logged"):
        return "CONFIRMED"
    if str(result.get("signal_skip_reason") or "") in ("duplicate","signal_log_disabled"):
        return "CONFIRMED"
    return "UNKNOWN"


def _build_diagnostics(results: List[Dict[str, Any]], signal_count: int) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    diags=[]
    by_final_blocker={}
    by_liq={}
    by_stage={}
    by_dir={}
    by_poi={}
    for r in results:
        htf=r.get("htf_gate") or {}
        liq=r.get("liq_ctx") or {}
        st=r.get("stageb_confirmation") or {}
        reclaim=st.get("stageb_reclaim") or {}
        disp=st.get("stageb_displacement") or {}
        ret=st.get("stageb_retest") or {}
        entry_sweep=r.get("entry_sweep") or {}
        final_blocker=_classify_final_blocker(r)
        d={
            "symbol":r.get("symbol"),"status":r.get("status"),"context_status":r.get("context_status"),"htf_count":r.get("htf_count"),"entry_count":r.get("entry_count"),"stageb_count":r.get("stageb_count"),
            "htf_gate_status":htf.get("htf_gate_status"),"htf_bias":htf.get("htf_bias"),"htf_bias_appstyle":htf.get("htf_bias_appstyle"),"htf_appstyle_ready":htf.get("htf_appstyle_ready"),"htf_dir_appstyle":htf.get("htf_dir_appstyle"),
            "liq_gate_status":r.get("liq_gate_status"),"liq_ctx_appstyle":liq.get("liq_ctx_appstyle"),"liq_reason":r.get("liq_reason"),"dist_to_bsl_pct":liq.get("dist_to_bsl_pct"),"dist_to_ssl_pct":liq.get("dist_to_ssl_pct"),"candidate_reason_appstyle":liq.get("candidate_reason_appstyle"),
            "bsl_zone":liq.get("bsl_zone"),"ssl_zone":liq.get("ssl_zone"),"close_15m_used":r.get("close_15m_used"),"nearest_liq_price":liq.get("nearest_liq_price"),"nearest_liq_type":liq.get("nearest_liq_type"),
            "selected_direction":st.get("selected_direction"),"selected_direction_reason":st.get("selected_direction_reason"),
            "bullish_sweep":entry_sweep.get("bullish_sweep"),"bearish_sweep":entry_sweep.get("bearish_sweep"),"mixed_sweep_detected":entry_sweep.get("mixed_sweep_detected"),
            "bullish_sweep_level":entry_sweep.get("bullish_sweep_level"),"bullish_sweep_extreme":entry_sweep.get("bullish_sweep_extreme"),"bullish_sweep_t":entry_sweep.get("bullish_sweep_t"),"bullish_sweep_tag":entry_sweep.get("bullish_sweep_tag"),
            "bearish_sweep_level":entry_sweep.get("bearish_sweep_level"),"bearish_sweep_extreme":entry_sweep.get("bearish_sweep_extreme"),"bearish_sweep_t":entry_sweep.get("bearish_sweep_t"),"bearish_sweep_tag":entry_sweep.get("bearish_sweep_tag"),
            "selected_sweep_tag":st.get("selected_sweep_tag"),"selected_sweep_level":st.get("selected_sweep_level"),"selected_sweep_extreme":st.get("selected_sweep_extreme"),"selected_sweep_t":st.get("selected_sweep_t"),
            "stageb_state_machine":st.get("stageb_state_machine"),"stageb_status":st.get("stageb_status"),"stageb_confirm_reason":st.get("stageb_confirm_reason"),"stageb_invalid_reason":st.get("stageb_invalid_reason"),
            "reclaim_ok":bool(reclaim.get("has_reclaim")),"reclaim_reason":reclaim.get("reason"),"displacement_ok":bool(disp.get("has_displacement")),"displacement_reason":disp.get("reason"),"fvg_available":st.get("fvg_available"),"ob_available":st.get("ob_available"),"selected_poi_type":st.get("selected_poi_type"),"selected_poi_reason":st.get("selected_poi_reason"),"retest_ok":bool(ret.get("has_retest")),"retest_reason":ret.get("reason"),
            "plan_status":r.get("plan_status"),"signal_skip_reason":r.get("signal_skip_reason"),"final_blocker":final_blocker,
        }
        diags.append(d)
        for m,k in ((by_final_blocker,final_blocker),(by_liq,d.get("liq_ctx_appstyle") or "NONE"),(by_stage,d.get("stageb_state_machine") or "NONE"),(by_dir,d.get("selected_direction") or "NONE"),(by_poi,d.get("selected_poi_type") or "NONE")):
            m[k]=m.get(k,0)+1
    by_plan={}
    by_context={}
    by_stage_status={}
    by_stage_reason={}
    for d in diags:
        for m,k in (
            (by_plan,d.get("plan_status") or "NONE"),
            (by_context,d.get("context_status") or "NONE"),
            (by_stage_status,d.get("stageb_status") or "NONE"),
            (by_stage_reason,d.get("stageb_invalid_reason") or d.get("stageb_confirm_reason") or "NONE"),
        ):
            m[k]=m.get(k,0)+1
    summary={"total_symbols":len(results),"ready_count":sum(1 for r in results if r.get("context_status")=="READY"),"blocked_count":sum(1 for r in results if r.get("context_status") in ("BLOCKED","DATA_GAP","HTF_DATA_GAP")),"confirmed_count":sum(1 for d in diags if d.get("final_blocker")=="CONFIRMED"),"signal_count":signal_count,"by_final_blocker":by_final_blocker,"by_liq_ctx_appstyle":by_liq,"by_stageb_state":by_stage,"by_selected_direction":by_dir,"by_selected_poi_type":by_poi,"by_plan_status":by_plan,"by_context_status":by_context,"by_stageb_status":by_stage_status,"by_stageb_reason":by_stage_reason}
    return diags,summary



def _filter_candles_at(candles: List[Dict[str, Any]], as_of_ms: Optional[int]) -> List[Dict[str, Any]]:
    if as_of_ms is None:
        return list(candles)
    out: List[Dict[str, Any]] = []
    for candle in candles:
        try:
            if int(candle.get("t") or candle.get("close_time_ms") or 0) <= as_of_ms:
                out.append(candle)
        except Exception:
            continue
    return out


def _empty_entry_sweep() -> Dict[str, Any]:
    return {
        "bullish_sweep": False, "bearish_sweep": False, "sweep_level": None, "sweep_extreme": None, "sweep_t": None, "sweep_tag": None,
        "bullish_sweep_level": None, "bullish_sweep_extreme": None, "bullish_sweep_t": None, "bullish_sweep_tag": None,
        "bearish_sweep_level": None, "bearish_sweep_extreme": None, "bearish_sweep_t": None, "bearish_sweep_tag": None,
        "mixed_sweep_detected": False, "selected_direction": "NONE", "selected_direction_reason": "no_sweep_direction",
        "selected_sweep_level": None, "selected_sweep_extreme": None, "selected_sweep_t": None, "selected_sweep_tag": None,
    }


def _initial_stageb_confirmation() -> Dict[str, Any]:
    return {
        "stageb_status": "INVALID", "stageb_direction": "NONE", "stageb_reclaim": {}, "stageb_displacement": {},
        "stageb_fvg_poi": {}, "stageb_ob_poi": {}, "selected_poi_type": None, "selected_poi_lo": None,
        "selected_poi_hi": None, "selected_poi_mid": None, "selected_poi_t": None, "selected_poi_t_wib": None, "selected_poi_reason": "no_valid_poi",
        "selected_fvg_reason": "not_built", "selected_ob_reason": "not_built",
        "fvg_available": False, "ob_available": False, "stageb_confirm_reason": None, "stageb_invalid_reason": "not_built",
    }


def _evaluate_vps_smc_symbol(symbol: str, as_of_utc: Optional[datetime] = None, dry_run: bool = False) -> Dict[str, Any]:
    symbol = _normalize_symbol(symbol)
    intervals = _intervals()
    as_of_ms = int(as_of_utc.timestamp() * 1000) if as_of_utc is not None else None
    htf = _filter_candles_at(_load_internal_candles(symbol, intervals["htf"]), as_of_ms)
    entry = _filter_candles_at(_load_internal_candles(symbol, intervals["entry"]), as_of_ms)
    stageb = _filter_candles_at(_load_internal_candles(symbol, intervals["stageb"]), as_of_ms)

    status = "READY"
    reason = None
    if len(htf) < 1:
        status = "DATA_GAP"; reason = f"missing_{intervals['htf']}_candles"
    elif len(entry) < 1:
        status = "DATA_GAP"; reason = f"missing_{intervals['entry']}_candles"
    elif len(stageb) < 1:
        status = "DATA_GAP"; reason = f"missing_{intervals['stageb']}_candles"

    primitive_status = "SKIPPED_DATA_GAP"
    entry_swing_summary: Dict[str, Any] = {}
    entry_eq: Dict[str, Any] = {"eqh": [], "eql": []}
    entry_sweep: Dict[str, Any] = _empty_entry_sweep()
    stageb_fvg: Dict[str, Any] = {"bullish_fvg": None, "bearish_fvg": None, "count_bullish": 0, "count_bearish": 0}
    htf_swing_summary: Dict[str, Any] = {}
    primitive_error: Optional[str] = None
    if status == "READY":
        try:
            entry_pivots = detect_pivots(entry)
            entry_swing_summary = build_swing_summary(entry)
            entry_eq = detect_equal_high_low(entry, entry_pivots)
            entry_sweep = detect_sweep(entry, entry_swing_summary, _env_int("VPS_SMC_SWEEP_LOOKBACK_BARS_5M", 20))
            stageb_fvg = detect_fvg(stageb, _env_int("VPS_SMC_FVG_LOOKBACK_BARS_5M", 35))
            htf_swing_summary = build_swing_summary(htf)
            primitive_status = "READY"
        except Exception as exc:
            primitive_status = "ERROR"
            primitive_error = f"primitive:{exc}"

    htf_gate = {"htf_gate_status": "HTF_DATA_GAP", "htf_reason": "not_enough_4h_candles"}
    liq_ctx: Dict[str, Any] = {}
    liq_gate_status = "BLOCK"
    liq_reason = "context_not_built"
    structure_15m = "UNKNOWN"
    structure_reason = "context_not_built"
    context_status = "DATA_GAP" if status == "DATA_GAP" else "ERROR"
    context_error: Optional[str] = None
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
        except Exception as exc:
            context_status = "ERROR"
            context_error = f"context:{exc}"

    stageb_confirmation = _initial_stageb_confirmation()
    shadow_state = "INVALID"
    stageb_error: Optional[str] = None
    if _env_bool("VPS_SMC_STAGEB_ENABLED", True):
        try:
            stageb_confirmation = build_stageb_confirmation({
                "symbol": symbol, "context_status": context_status, "liq_gate_status": liq_gate_status, "liq_ctx": liq_ctx,
                "entry_sweep": entry_sweep, "stageb_fvg": stageb_fvg, "htf_gate": htf_gate, "structure_15m": structure_15m,
            }, stageb, dry_run=dry_run, no_persist=dry_run, ignore_persisted_state=dry_run)
            shadow_state = stageb_confirmation.get("stageb_status") or "INVALID"
        except Exception as exc:
            stageb_error = f"stageb:{exc}"
            stageb_confirmation = _initial_stageb_confirmation()
            stageb_confirmation["stageb_invalid_reason"] = stageb_error
            shadow_state = "INVALID"

    entry_sweep["selected_direction"] = (stageb_confirmation.get("selected_direction") or "NONE")
    entry_sweep["selected_direction_reason"] = (stageb_confirmation.get("selected_direction_reason") or "no_sweep_direction")
    entry_sweep["selected_sweep_tag"] = stageb_confirmation.get("selected_sweep_tag")
    entry_sweep["selected_sweep_level"] = stageb_confirmation.get("selected_sweep_level")
    entry_sweep["selected_sweep_extreme"] = stageb_confirmation.get("selected_sweep_extreme")
    entry_sweep["selected_sweep_t"] = stageb_confirmation.get("selected_sweep_t")

    result: Dict[str, Any] = {
        "symbol": symbol, "status": status, "primitive_status": primitive_status, "htf_count": len(htf), "entry_count": len(entry),
        "stageb_count": len(stageb), "latest_htf_close_time_ms": htf[-1]["t"] if htf else None,
        "latest_entry_close_time_ms": entry[-1]["t"] if entry else None, "latest_stageb_close_time_ms": stageb[-1]["t"] if stageb else None,
        "close_15m_used": entry[-1]["c"] if entry else None,
        "entry_swing_summary": entry_swing_summary, "entry_eq": entry_eq, "entry_sweep": entry_sweep, "stageb_fvg": stageb_fvg,
        "htf_swing_summary": htf_swing_summary, "htf_gate": htf_gate, "liq_ctx": liq_ctx, "liq_gate_status": liq_gate_status,
        "liq_reason": liq_reason, "structure_15m": structure_15m, "structure_reason": structure_reason, "context_status": context_status,
        "stageb_confirmation": stageb_confirmation, "shadow_state": shadow_state, "reason": reason,
    }
    errors = [e for e in (primitive_error, context_error, stageb_error) if e]
    if errors:
        result["debug_errors"] = errors
    return result



def _stageb_sequence_debug(stageb: Dict[str, Any]) -> Dict[str, Any]:
    fvg = (stageb or {}).get("stageb_fvg_poi") or {}
    return {
        "selected_sweep_t": (stageb or {}).get("selected_sweep_t"),
        "selected_sweep_t_wib": (stageb or {}).get("selected_sweep_t_wib"),
        "selected_sweep_level": (stageb or {}).get("selected_sweep_level"),
        "selected_sweep_extreme": (stageb or {}).get("selected_sweep_extreme"),
        "reclaim": (stageb or {}).get("stageb_reclaim"),
        "reclaim_candle": ((stageb or {}).get("stageb_reclaim") or {}).get("reclaim_candle"),
        "displacement": (stageb or {}).get("stageb_displacement"),
        "displacement_candle": ((stageb or {}).get("stageb_displacement") or {}).get("displacement_candle"),
        "bullish_fvg_candidates": fvg.get("bullish_fvg_candidates") or [],
        "bearish_fvg_candidates": fvg.get("bearish_fvg_candidates") or [],
        "selected_fvg_reason": (stageb or {}).get("selected_fvg_reason") or fvg.get("selected_fvg_reason") or fvg.get("reason"),
        "selected_fvg_polarity": (stageb or {}).get("selected_fvg_polarity") or fvg.get("selected_fvg_polarity") or fvg.get("fvg_type"),
        "selected_fvg_mode": (stageb or {}).get("selected_fvg_mode") or fvg.get("selected_fvg_mode"),
        "selected_fvg_candidate": (stageb or {}).get("selected_fvg_candidate") or fvg.get("selected_fvg_candidate") or fvg.get("selected_fvg"),
        "selected_fvg_retest_probe": (stageb or {}).get("selected_fvg_retest_probe") or fvg.get("selected_fvg_retest_probe"),
        "fvg_rejection_reason": (stageb or {}).get("fvg_rejection_reason") or fvg.get("fvg_rejection_reason"),
        "selected_ob_reason": (stageb or {}).get("selected_ob_reason") or (((stageb or {}).get("stageb_ob_poi") or {}).get("reason")),
        "selected_poi_reason": (stageb or {}).get("selected_poi_reason"),
        "selected_poi_type": (stageb or {}).get("selected_poi_type"),
        "selected_poi_lo": (stageb or {}).get("selected_poi_lo"),
        "selected_poi_hi": (stageb or {}).get("selected_poi_hi"),
        "selected_poi_t": (stageb or {}).get("selected_poi_t"),
        "selected_poi_t_wib": (stageb or {}).get("selected_poi_t_wib"),
        "retest": (stageb or {}).get("stageb_retest"),
        "retest_scan_bars": (((stageb or {}).get("stageb_retest") or {}).get("scan_bars") or []),
    }

def vps_smc_debug_replay(symbol: str, as_of_wib: Optional[str] = None, as_of_utc: Optional[str] = None) -> Dict[str, Any]:
    parsed_utc = _parse_iso_utc(as_of_utc) if as_of_utc else _parse_wib_time(as_of_wib)
    if parsed_utc is None:
        return {"ok": False, "mode": "DEBUG_REPLAY", "error": "invalid_replay_time", "symbol": _normalize_symbol(symbol), "as_of_wib": as_of_wib, "as_of_utc": as_of_utc}

    state_path = _semi_state_file()
    before = state_path.read_bytes() if state_path.exists() else None
    symbol_norm = _normalize_symbol(symbol)
    try:
        fake_result = _evaluate_vps_smc_symbol(symbol_norm, parsed_utc, dry_run=True)
    finally:
        if before is None:
            try:
                state_path.unlink()
            except FileNotFoundError:
                pass
        else:
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_bytes(before)

    plan_status, plan, score_detail, skip_reason = _build_plan_and_score(fake_result)
    fake_result["plan_status"] = plan_status
    fake_result["plan"] = plan
    fake_result["score_detail"] = score_detail
    fake_result["signal_skip_reason"] = skip_reason if plan_status != "VALID" else None
    diagnostics, diagnostic_summary = _build_diagnostics([fake_result], 1 if plan_status == "VALID" else 0)
    stageb = fake_result.get("stageb_confirmation") or {}
    invalidation_debug = {k: stageb.get(k) for k in _semi_invalidation_debug_defaults().keys()}
    return {
        "ok": True, "mode": "DEBUG_REPLAY", "read_only": True, "symbol": symbol_norm,
        "as_of_utc": parsed_utc.isoformat(), "as_of_wib": parsed_utc.astimezone(WIB_TZ).strftime("%Y-%m-%d %H:%M:%S WIB"),
        "plan_status": plan_status, "plan": plan, "entry": (plan or {}).get("entry_mid"), "sl": (plan or {}).get("sl"),
        "tp1": (plan or {}).get("tp1"), "tp2": (plan or {}).get("tp2"), "tp3": (plan or {}).get("tp3"), "rr": (plan or {}).get("rr_tp2"),
        "score_detail": score_detail, "skip_reason": fake_result.get("signal_skip_reason"),
        "stageb_status": stageb.get("stageb_status"), "stageb_state_machine": stageb.get("stageb_state_machine"),
        "stageb_confirm_reason": stageb.get("stageb_confirm_reason"), "stageb_invalid_reason": stageb.get("stageb_invalid_reason"),
        "stageb_sequence_debug": _stageb_sequence_debug(stageb),
        **invalidation_debug,
        "result": fake_result, "diagnostics": diagnostics, "diagnostic_summary": diagnostic_summary,
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
                "bullish_sweep_level": None,
                "bullish_sweep_extreme": None,
                "bullish_sweep_t": None,
                "bullish_sweep_tag": None,
                "bearish_sweep_level": None,
                "bearish_sweep_extreme": None,
                "bearish_sweep_t": None,
                "bearish_sweep_tag": None,
                "mixed_sweep_detected": False,
                "selected_direction": "NONE",
                "selected_direction_reason": "no_sweep_direction",
                "selected_sweep_level": None,
                "selected_sweep_extreme": None,
                "selected_sweep_t": None,
                "selected_sweep_tag": None,
            }
            stageb_fvg: Dict[str, Any] = {"bullish_fvg": None, "bearish_fvg": None, "count_bullish": 0, "count_bearish": 0}
            htf_swing_summary: Dict[str, Any] = {}
            if status == "READY":
                try:
                    entry_pivots = detect_pivots(entry)
                    entry_swing_summary = build_swing_summary(entry)
                    entry_eq = detect_equal_high_low(entry, entry_pivots)
                    entry_sweep = detect_sweep(entry, entry_swing_summary, _env_int("VPS_SMC_SWEEP_LOOKBACK_BARS_5M", 20))
                    stageb_fvg = detect_fvg(stageb, _env_int("VPS_SMC_FVG_LOOKBACK_BARS_5M", 35))
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
                "stageb_ob_poi": {},
                "selected_poi_type": None,
                "selected_poi_lo": None,
                "selected_poi_hi": None,
                "selected_poi_mid": None,
                "selected_poi_t": None,
                "selected_poi_reason": "no_valid_poi",
                "fvg_available": False,
                "ob_available": False,
                "stageb_confirm_reason": None,
                "stageb_invalid_reason": "not_built",
            }
            shadow_state = "INVALID"
            if _env_bool("VPS_SMC_STAGEB_ENABLED", True):
                try:
                    stageb_confirmation = build_stageb_confirmation({
                        "symbol": symbol,
                        "context_status": context_status,
                        "liq_gate_status": liq_gate_status,
                        "liq_ctx": liq_ctx,
                        "entry_sweep": entry_sweep,
                        "stageb_fvg": stageb_fvg,
                        "htf_gate": htf_gate,
                        "structure_15m": structure_15m,
                    }, stageb)
                    shadow_state = stageb_confirmation.get("stageb_status") or "INVALID"
                except Exception as sexc:
                    stageb_confirmation = {
                        "stageb_status": "INVALID",
                        "stageb_direction": "NONE",
                        "stageb_reclaim": {},
                        "stageb_displacement": {},
                        "stageb_fvg_poi": {},
                        "stageb_ob_poi": {},
                        "stageb_confirm_reason": None,
                        "stageb_invalid_reason": f"stageb_error:{sexc}",
                    }
                    shadow_state = "INVALID"
                    _append_jsonl(_log_dir() / "vps_smc_errors.jsonl", {
                        "created_at_utc": _utc_now_iso(),
                        "symbol": symbol,
                        "error": f"stageb:{sexc}",
                    })
            entry_sweep["selected_direction"] = (stageb_confirmation.get("selected_direction") or "NONE")
            entry_sweep["selected_direction_reason"] = (stageb_confirmation.get("selected_direction_reason") or "no_sweep_direction")
            entry_sweep["selected_sweep_tag"] = stageb_confirmation.get("selected_sweep_tag")
            entry_sweep["selected_sweep_level"] = stageb_confirmation.get("selected_sweep_level")
            entry_sweep["selected_sweep_extreme"] = stageb_confirmation.get("selected_sweep_extreme")
            entry_sweep["selected_sweep_t"] = stageb_confirmation.get("selected_sweep_t")
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
                "close_15m_used": entry[-1]["c"] if entry else None,
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
            "stageb_ob_poi": {},
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
        core_row = None
        try:
            core_row = _vps_smc_log_core_candidate_shadow_v1(result)
        except Exception as exc:
            _append_jsonl(_log_dir() / "vps_smc_errors.jsonl", {
                "created_at_utc": _utc_now_iso(),
                "event_type": "CORE_SMC_SHADOW_LOG_ERROR",
                "symbol": result.get("symbol"),
                "error": str(exc),
            })
        if str(result.get("shadow_state") or "") != "CONFIRMED":
            try:
                if _vps_smc_route_core_quant_v1(result, core_row, logged_signal_keys):
                    signal_count += 1
                    continue
            except Exception as exc:
                result["core_quant_skip_reason"] = "router_error"
                _append_jsonl(_log_dir() / "vps_smc_errors.jsonl", {
                    "created_at_utc": _utc_now_iso(),
                    "event_type": "CORE_QUANT_ROUTER_ERROR",
                    "symbol": result.get("symbol"),
                    "error": str(exc),
                })
            if isinstance(core_row, dict):
                try:
                    _append_jsonl(_log_dir() / "vps_smc_core_quant_skips.jsonl", {
                        "created_at_utc": _utc_now_iso(),
                        "event_type": "CORE_QUANT_SKIP",
                        "symbol": core_row.get("symbol") or result.get("symbol"),
                        "direction": core_row.get("direction"),
                        "candidate_id": core_row.get("candidate_id"),
                        "core_state": core_row.get("state"),
                        "core_ok": core_row.get("core_ok"),
                        "rr_ok": core_row.get("rr_ok"),
                        "rr_at_emit": core_row.get("rr_at_emit"),
                        "min_rr_at_emit": core_row.get("min_rr_at_emit"),
                        "target_r": core_row.get("target_r"),
                        "strict_shadow_state": core_row.get("strict_shadow_state"),
                        "strict_confirm_reason": core_row.get("strict_confirm_reason"),
                        "strict_invalid_reason": core_row.get("strict_invalid_reason"),
                        "fvg_present": core_row.get("fvg_present"),
                        "displacement_present": core_row.get("displacement_present"),
                        "htf_bias": core_row.get("htf_bias"),
                        "structure_15m": core_row.get("structure_15m"),
                        "score": result.get("core_quant_score"),
                        "min_score": result.get("core_quant_min_score"),
                        "skip_reason": result.get("core_quant_skip_reason") or "not_routed_unknown",
                    })
                except Exception:
                    pass
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
        bucket_src_ms = ((result.get("stageb_confirmation") or {}).get("confirmed_t") or result.get("latest_stageb_close_time_ms"))
        bucket_ms = _bucket_ms(bucket_src_ms, _env_int("VPS_SMC_DEDUP_BUCKET_MIN", 15))
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
        tp_norm = _normalize_tps(
            direction,
            plan.get("entry_mid"),
            plan.get("sl"),
            plan.get("tp1"),
            plan.get("tp2"),
            plan.get("tp3"),
        )
        if bool(tp_norm.get("ok")):
            plan["tp1"] = tp_norm.get("tp1")
            plan["tp2"] = tp_norm.get("tp2")
            plan["tp3"] = tp_norm.get("tp3")
        signal_row = {
            "signal_key": signal_key, "source": "VPS_SMC", "signal_source": "VPS_SMC", "source_mode": "SHADOW_ONLY", "candidate_only": True,
            "symbol": result.get("symbol"), "pair": f"BINANCE:{str(result.get('symbol') or '').upper()}.P", "direction": direction, "state": "CONFIRMED", "mode": "SHADOW_ONLY",
            "score": score_detail.get("score"), "priority": score_detail.get("priority"), "risk_mult": score_detail.get("risk_mult"),
            "signal_time_wib": _bucket_ms_to_wib_text(bucket_ms), "confirmed_bucket_ms": bucket_ms, "htf_dir": htf_gate.get("htf_dir"), "htf_bias": htf_gate.get("htf_bias"),
            "htf_location": htf_gate.get("htf_location"), "htf_structure": htf_gate.get("htf_structure"), "liq_ctx": result.get("liq_ctx"),
            "dist_to_zone_pct": liq_ctx.get("dist_to_zone_pct"), "structure_15m": result.get("structure_15m"), "sweep_tag": liq_ctx.get("sweep_tag"),
            "sweep_extreme": liq_ctx.get("sweep_extreme"), "reclaim_level": reclaim.get("reclaim_level"), "fvg_type": poi.get("fvg_type"),
            "fvg_lo": poi.get("fvg_lo"), "fvg_hi": poi.get("fvg_hi"), "entry_lo": plan.get("entry_lo"), "entry_hi": plan.get("entry_hi"),
            "entry_mid": plan.get("entry_mid"), "sl": plan.get("sl"), "invalid": plan.get("invalid"), "tp1": plan.get("tp1"),
            "tp2": plan.get("tp2"), "tp3": plan.get("tp3"), "rr_tp2": plan.get("rr_tp2"), "notes": ",".join(score_detail.get("reasons") or []),
            "raw_tp1": tp_norm.get("raw_tp1"), "raw_tp2": tp_norm.get("raw_tp2"), "raw_tp3": tp_norm.get("raw_tp3"),
            "tp_normalized": tp_norm.get("tp_normalized"), "tp_normalize_reason": tp_norm.get("tp_normalize_reason"),
            "plan_sanity_ok": tp_norm.get("ok"), "plan_sanity_reason": tp_norm.get("reason"), "plan_invalid": tp_norm.get("plan_invalid"),
            "stageb_state_machine": stageb.get("stageb_state_machine"),
            "stageb_retest": stageb.get("stageb_retest"),
            "confirmed_t": stageb.get("confirmed_t"),
            "confirmed_t_wib": stageb.get("confirmed_t_wib"),
            "created_at_utc": _utc_now_iso(),
        }
        _append_jsonl(_log_dir() / "vps_smc_shadow_signals.jsonl", signal_row)
        logged_signal_keys[signal_key] = signal_row["created_at_utc"]
        result["signal_logged"] = True
        result["signal_skip_reason"] = None
        source_mode = str(os.getenv("SIGNAL_SOURCE_MODE", "APPS_SCRIPT_ONLY")).strip().upper() or "APPS_SCRIPT_ONLY"
        vps_exec_enabled = _env_bool("VPS_SMC_EXECUTION_ENABLED", False)
        competitor_mode = str(os.getenv("VPS_SMC_COMPETITOR_MODE", "SHADOW_ONLY")).strip().upper() or "SHADOW_ONLY"

        # VPS_SMC_BRIDGE_DECISION_LOG_20260621
        bridge_condition_ok = (
            EXECUTION_BRIDGE_HANDLER is not None
            and source_mode == "VPS_SMC_PRIMARY"
            and vps_exec_enabled
            and competitor_mode == "PRODUCTION_SIGNAL"
        )
        try:
            _append_jsonl(_log_dir() / "vps_smc_bridge_events.jsonl", {
                "created_at_utc": _utc_now_iso(),
                "event_type": "VPS_SMC_BRIDGE_PRECHECK",
                "symbol": result.get("symbol"),
                "signal_key": signal_key,
                "direction": direction,
                "source_mode": source_mode,
                "vps_exec_enabled": vps_exec_enabled,
                "competitor_mode": competitor_mode,
                "handler_is_none": EXECUTION_BRIDGE_HANDLER is None,
                "bridge_condition_ok": bridge_condition_ok,
                "status": "CONFIRMED",
            })
        except Exception:
            pass

        if bridge_condition_ok:
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
                "raw_tp1": tp_norm.get("raw_tp1"),
                "raw_tp2": tp_norm.get("raw_tp2"),
                "raw_tp3": tp_norm.get("raw_tp3"),
                "tp_normalized": tp_norm.get("tp_normalized"),
                "tp_normalize_reason": tp_norm.get("tp_normalize_reason"),
                "plan_sanity_ok": tp_norm.get("ok"),
                "plan_sanity_reason": tp_norm.get("reason"),
                "plan_invalid": tp_norm.get("plan_invalid"),
                "score": score_detail.get("score"),
                "priority": score_detail.get("priority"),
                "confirmed_bucket_ms": bucket_ms,
                "signal_time_wib": _bucket_ms_to_wib_text(bucket_ms),
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
                _append_jsonl(_log_dir() / "vps_smc_bridge_events.jsonl", {
                    "created_at_utc": _utc_now_iso(),
                    "event_type": "VPS_SMC_BRIDGE_ATTEMPT",
                    "symbol": result.get("symbol"),
                    "signal_key": signal_key,
                    "direction": direction,
                    "status": "CONFIRMED",
                    "source_mode": source_mode,
                    "competitor_mode": competitor_mode,
                })
                result["execution_bridge"] = EXECUTION_BRIDGE_HANDLER(bridge_payload)
                _bridge_res = result.get("execution_bridge") if isinstance(result.get("execution_bridge"), dict) else {}
                _append_jsonl(_log_dir() / "vps_smc_bridge_events.jsonl", {
                    "created_at_utc": _utc_now_iso(),
                    "event_type": "VPS_SMC_BRIDGE_RESULT",
                    "symbol": result.get("symbol"),
                    "signal_key": signal_key,
                    "direction": direction,
                    "status": "CONFIRMED",
                    "bridge_ok": _bridge_res.get("ok"),
                    "bridge_decision": _bridge_res.get("decision"),
                    "bridge_reason": _bridge_res.get("reason"),
                    "bridge_gate": _bridge_res.get("gate"),
                    "execution_mode": _bridge_res.get("execution_mode"),
                    "cost_gate_pass": _bridge_res.get("cost_gate_pass"),
                    "cost_gate_reason": _bridge_res.get("cost_gate_reason"),
                    "raw_bridge_result": _bridge_res,
                })
            except Exception as exc:
                result["execution_bridge"] = {"ok": False, "error": str(exc)}
                _append_jsonl(_log_dir() / "vps_smc_bridge_events.jsonl", {
                    "created_at_utc": _utc_now_iso(),
                    "event_type": "VPS_SMC_BRIDGE_ERROR",
                    "symbol": result.get("symbol"),
                    "signal_key": signal_key,
                    "direction": direction,
                    "error": str(exc),
                    "source_mode": source_mode,
                    "vps_exec_enabled": vps_exec_enabled,
                    "competitor_mode": competitor_mode,
                    "handler_is_none": EXECUTION_BRIDGE_HANDLER is None,
                    "bridge_condition_ok": bridge_condition_ok,
                })
                _append_jsonl(_log_dir() / "vps_smc_errors.jsonl", {
                    "created_at_utc": _utc_now_iso(),
                    "event_type": "VPS_SMC_EXECUTION_BRIDGE_ERROR",
                    "symbol": result.get("symbol"),
                    "signal_key": signal_key,
                    "error": str(exc),
                })
        signal_count += 1

    diagnostics, diagnostic_summary = _build_diagnostics(results, signal_count)
    for result in results:
        entry_sweep = result.get("entry_sweep") or {}
        stageb = result.get("stageb_confirmation") or {}
        htf_gate = result.get("htf_gate") or {}
        result["run_once_debug"] = {
            "symbol": result.get("symbol"),
            "htf_count": result.get("htf_count"),
            "htf_appstyle_ready": htf_gate.get("htf_appstyle_ready"),
            "htf_appstyle_reason": htf_gate.get("htf_appstyle_reason"),
            "htf_bias": htf_gate.get("htf_bias"),
            "htf_bias_appstyle": htf_gate.get("htf_bias_appstyle"),
            "htf_dir_appstyle": htf_gate.get("htf_dir_appstyle"),
            "htf_structure_appstyle": htf_gate.get("htf_structure_appstyle"),
            "htf_location_appstyle": htf_gate.get("htf_location_appstyle"),
            "htf_swing_bias_num": htf_gate.get("htf_swing_bias_num"),
            "htf_internal_bias_num": htf_gate.get("htf_internal_bias_num"),
            "bullish_sweep": entry_sweep.get("bullish_sweep"),
            "bearish_sweep": entry_sweep.get("bearish_sweep"),
            "mixed_sweep_detected": entry_sweep.get("mixed_sweep_detected"),
            "selected_direction": stageb.get("selected_direction"),
            "selected_direction_reason": stageb.get("selected_direction_reason"),
            "selected_sweep_tag": stageb.get("selected_sweep_tag"),
            "selected_sweep_level": stageb.get("selected_sweep_level"),
            "selected_sweep_extreme": stageb.get("selected_sweep_extreme"),
            "selected_sweep_t": stageb.get("selected_sweep_t"),
            "stageb_state_machine": stageb.get("stageb_state_machine"),
            "stageb_confirm_reason": stageb.get("stageb_confirm_reason"),
            "fvg_available": stageb.get("fvg_available"),
            "ob_available": stageb.get("ob_available"),
            "selected_poi_type": stageb.get("selected_poi_type"),
            "selected_poi_reason": stageb.get("selected_poi_reason"),
            "selected_poi_lo": stageb.get("selected_poi_lo"),
            "selected_poi_hi": stageb.get("selected_poi_hi"),
            "selected_poi_mid": stageb.get("selected_poi_mid"),
            "selected_poi_t": stageb.get("selected_poi_t"),
            "ob_type": ((stageb.get("stageb_ob_poi") or {}).get("ob_type")),
            "ob_lo": ((stageb.get("stageb_ob_poi") or {}).get("ob_lo")),
            "ob_hi": ((stageb.get("stageb_ob_poi") or {}).get("ob_hi")),
            "ob_t": ((stageb.get("stageb_ob_poi") or {}).get("ob_t")),
            "plan_status": result.get("plan_status"),
            "signal_skip_reason": result.get("signal_skip_reason"),
        }

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
    _append_jsonl(_log_dir() / "vps_smc_gate_diagnostics.jsonl", {"created_at_utc": log_row["created_at_utc"], "mode": log_row["mode"], "symbols_evaluated": log_row["symbols_evaluated"], "summary": diagnostic_summary, "diagnostics": diagnostics})

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
        "diagnostics": diagnostics,
        "diagnostic_summary": diagnostic_summary,
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



def _canonical_symbol_from_row(row: Dict[str, Any]) -> Any:
    raw_symbol = str(row.get("symbol") or "").strip()
    if raw_symbol:
        return raw_symbol.replace(".P", "").split(":")[-1].upper()

    raw_pair = str(row.get("pair") or "").strip()
    if raw_pair:
        x = raw_pair.split("|", 1)[0].split(":", 1)[-1].replace(".P", "")
        if x:
            return x.upper()

    raw_key = str(row.get("signal_key") or "").strip()
    if raw_key:
        first = raw_key.split("|", 1)[0]
        x = first.split(":", 1)[-1].replace(".P", "")
        if x:
            return x.upper()

    return None


def _canonical_pair_from_row(row: Dict[str, Any]) -> Any:
    raw_pair = str(row.get("pair") or "").strip()
    if raw_pair:
        if raw_pair.startswith("BINANCE:") and raw_pair.endswith(".P"):
            return raw_pair
        # If old rows used bare symbol as pair.
        if ":" not in raw_pair and "|" not in raw_pair:
            return f"BINANCE:{raw_pair.replace('.P', '').upper()}.P"

    raw_key = str(row.get("signal_key") or "").strip()
    if raw_key.startswith("BINANCE:"):
        first = raw_key.split("|", 1)[0]
        if first.endswith(".P"):
            return first
        return f"{first}.P"

    sym = _canonical_symbol_from_row(row)
    if sym:
        return f"BINANCE:{sym}.P"

    return raw_pair or None


def _canonical_direction_from_row(row: Dict[str, Any]) -> Any:
    d = str(row.get("direction") or row.get("dir") or "").strip().upper()
    if d in ("BUY", "LONG"):
        return "LONG"
    if d in ("SELL", "SHORT"):
        return "SHORT"
    return d or None


def _canonical_display_row(row: Dict[str, Any], default_source: str = "VPS_SMC") -> Dict[str, Any]:
    out = dict(row or {})
    out["source"] = out.get("source") or out.get("signal_source") or default_source
    out["signal_source"] = out.get("signal_source") or default_source
    out["pair"] = _canonical_pair_from_row(out)
    out["symbol"] = _canonical_symbol_from_row(out)
    out["direction"] = _canonical_direction_from_row(out)

    # Derive signal_time_wib for old rows if bucket exists.
    if not out.get("signal_time_wib"):
        try:
            ms = out.get("confirmed_bucket_ms")
            if ms is None:
                sk = str(out.get("signal_key") or "")
                parts = sk.split("|")
                if len(parts) >= 3:
                    ms = parts[-1]
            if ms is not None:
                dt = datetime.fromtimestamp(int(float(ms)) / 1000, timezone.utc).astimezone(_WIB)
                out["signal_time_wib"] = dt.strftime("%Y-%m-%d %H:%M:%S WIB")
        except Exception:
            pass
    return out


def _row_timestamp_value(row: Dict[str, Any]) -> tuple:
    for key in ("updated_at_utc", "evaluated_at_utc", "created_at_utc"):
        dt = _parse_iso_utc(row.get(key))
        if dt is not None:
            return (1, dt.timestamp())
    return (0, 0.0)


def _read_latest_outcomes(path: Path, limit: int) -> List[Dict[str, Any]]:
    rows = _read_jsonl(path)
    latest_by_key: Dict[str, Dict[str, Any]] = {}
    latest_rank: Dict[str, tuple] = {}
    for idx, row in enumerate(rows):
        key = str(row.get("signal_key") or "").strip()
        if not key:
            continue
        rank = (*_row_timestamp_value(row), idx)
        if key not in latest_rank or rank >= latest_rank[key]:
            latest_by_key[key] = row
            latest_rank[key] = rank
    deduped = sorted(latest_by_key.values(), key=lambda r: (*_row_timestamp_value(r), str(r.get("signal_key") or "")))
    take = max(1, min(int(limit or 100), _env_int("VPS_SMC_GSHEET_MAX_ROWS_PER_RUN", 100)))
    return deduped[-take:]


def _sheet_append(spreadsheet_id: str, service_account_file: str, sheet_name: str, header: List[str], rows: List[List[Any]], create_header: bool, header_already_written: bool, clear_on_header_change: bool = False) -> bool:
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(service_account_file, scopes=scopes)
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)

    header_now_written = bool(header_already_written)
    if create_header:
        probe = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1:ZZ1",
        ).execute()
        existing = probe.get("values") or []
        existing_header = existing[0] if existing else []
        if not existing_header:
            service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": [header]},
            ).execute()
            header_now_written = True
        elif existing_header != header:
            if clear_on_header_change:
                service.spreadsheets().values().clear(
                    spreadsheetId=spreadsheet_id,
                    range=f"{sheet_name}!A:ZZ",
                    body={},
                ).execute()
                service.spreadsheets().values().append(
                    spreadsheetId=spreadsheet_id,
                    range=f"{sheet_name}!A1",
                    valueInputOption="RAW",
                    insertDataOption="INSERT_ROWS",
                    body={"values": [header]},
                ).execute()
            else:
                service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=f"{sheet_name}!A1",
                    valueInputOption="RAW",
                    body={"values": [header]},
                ).execute()
            header_now_written = True
        else:
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


def _sheet_header_matches(spreadsheet_id: str, service_account_file: str, sheet_name: str, header: List[str]) -> bool:
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(service_account_file, scopes=scopes)
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    probe = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!A1:ZZ1",
    ).execute()
    existing = probe.get("values") or []
    existing_header = existing[0] if existing else []
    if not existing_header:
        return True
    return existing_header == header


def vps_smc_mirror_gsheet(target: str = "ALL", limit: int = 100, force: bool = False) -> Dict[str, Any]:
    target_u = str(target or "ALL").strip().upper()
    enabled = _env_bool("VPS_SMC_GSHEET_MIRROR_ENABLED", False)
    counts = {"shadow_candidates_seen": 0, "shadow_candidates_written": 0, "compare_rows_seen": 0, "compare_rows_written": 0, "outcomes_seen": 0, "outcomes_latest": 0, "outcomes_written": 0, "skipped_duplicate": 0, "errors": 0}
    state = _load_state()
    mirrored = state.get("gsheet_mirrored_keys") if isinstance(state.get("gsheet_mirrored_keys"), dict) else {"shadow": {}, "compare": {}, "outcomes": {}}
    mirrored.setdefault("shadow", {})
    mirrored.setdefault("compare", {})
    mirrored.setdefault("outcomes", {})
    headers_written = state.get("gsheet_headers_written") if isinstance(state.get("gsheet_headers_written"), dict) else {"shadow": False, "compare": False, "outcomes": False}
    headers_written.setdefault("shadow", False)
    headers_written.setdefault("compare", False)
    headers_written.setdefault("outcomes", False)
    spreadsheet_id = _gspreadsheet_id()

    if not enabled:
        return {"ok": True, "enabled": False, "target": target_u, "spreadsheet_id": spreadsheet_id or None, "counts": counts, "reason": "gsheet_mirror_disabled"}

    service_account_file = _gservice_account_file()
    if not spreadsheet_id or not service_account_file:
        reason = "credentials_missing"
        _append_jsonl(_log_dir() / "vps_smc_errors.jsonl", {"created_at_utc": _utc_now_iso(), "error": reason})
        return {"ok": _env_bool("VPS_SMC_GSHEET_FAIL_OPEN", True), "enabled": True, "target": target_u, "spreadsheet_id": spreadsheet_id or None, "counts": counts, "reason": reason}

    shadow_header = ["created_at_utc","source","signal_source","source_mode","signal_key","pair","symbol","direction","state","score","priority","risk_mult","candidate_only","signal_time_wib","confirmed_bucket_ms","htf_bias","htf_location","htf_structure","structure_15m","sweep_tag","entry_lo","entry_hi","entry_mid","sl","raw_tp1","raw_tp2","raw_tp3","tp1","tp2","tp3","tp_normalized","tp_normalize_reason","rr_tp2","plan_sanity_ok","plan_sanity_reason","plan_invalid","notes"]
    compare_header = ["created_at_utc","lookback_minutes","symbol","classification","app_signal_key","app_direction","app_decision","app_created_at_utc","vps_signal_key","vps_direction","vps_score","vps_priority","vps_created_at_utc","reason","compare_key"]
    outcome_header = ["created_at_utc","evaluated_at_utc","updated_at_utc","signal_key","pair","symbol","direction","signal_time_wib","entry","sl","tp1","tp2","tp3","outcome_status","label_target","label_win","label_R","include_ml_label","exclude_label_reason","first_hit","hit_time_utc","hit_time_wib","bars_to_hit","max_favorable_r","max_adverse_r","data_gap","open_end","notes"]
    shadow_rows=[]; compare_rows=[]; outcome_rows=[]
    try:
        if target_u in ("ALL","SHADOW_SIGNALS"):
            for r in _read_latest(_log_dir()/"vps_smc_shadow_signals.jsonl", limit):
                if str(r.get("source_mode") or "").upper() != "SHADOW_ONLY" or bool(r.get("candidate_only")) is not True:
                    continue
                counts["shadow_candidates_seen"] += 1
                key = str(r.get("signal_key") or "")
                if key and (not force) and key in mirrored["shadow"]:
                    counts["skipped_duplicate"] += 1; continue
                display_row = _canonical_display_row(r, default_source="VPS_SMC")
                shadow_rows.append([display_row.get(k) for k in shadow_header])
                if key: mirrored["shadow"][key]=_utc_now_iso()
        if target_u in ("ALL","COMPARE"):
            for r in _read_latest(_log_dir()/"vps_smc_compare.jsonl", limit):
                counts["compare_rows_seen"] += 1
                key = str(r.get("compare_key") or "")
                if key and (not force) and key in mirrored["compare"]:
                    counts["skipped_duplicate"] += 1; continue
                compare_rows.append([r.get(k) for k in compare_header])
                if key: mirrored["compare"][key]=_utc_now_iso()
        if target_u in ("ALL", "OUTCOMES"):
            raw_outcomes = _read_jsonl(_log_dir()/"forward_outcomes.jsonl")
            counts["outcomes_seen"] = len(raw_outcomes)
            latest_rows = _read_latest_outcomes(_log_dir()/"forward_outcomes.jsonl", limit)
            counts["outcomes_latest"] = len(latest_rows)
            for r in latest_rows:
                signal_key = str(r.get("signal_key") or "").strip()
                ts = str(r.get("updated_at_utc") or r.get("evaluated_at_utc") or r.get("created_at_utc") or "").strip()
                key = f"{signal_key}|{ts}" if signal_key else ""
                if key and (not force) and key in mirrored["outcomes"]:
                    counts["skipped_duplicate"] += 1
                    continue
                display_row = _canonical_display_row(r, default_source=(r.get("signal_source") or "UNKNOWN"))
                outcome_rows.append([display_row.get(k) for k in outcome_header])
                if key:
                    mirrored["outcomes"][key] = _utc_now_iso()

        if target_u in ("ALL", "SHADOW_SIGNALS"):
            shadow_sheet_name = _env_str("VPS_SMC_GSHEET_SHADOW_SHEET_NAME", "VPS_SMC_SHADOW_SIGNALS")
            if _env_bool("VPS_SMC_GSHEET_CREATE_HEADER", True) and not _sheet_header_matches(spreadsheet_id, service_account_file, shadow_sheet_name, shadow_header):
                mirrored["shadow"] = {}
            headers_written["shadow"] = _sheet_append(
                spreadsheet_id,
                service_account_file,
                shadow_sheet_name,
                shadow_header,
                shadow_rows,
                _env_bool("VPS_SMC_GSHEET_CREATE_HEADER", True),
                bool(headers_written.get("shadow", False)),
                clear_on_header_change=True,
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
        if target_u in ("ALL", "OUTCOMES"):
            outcome_sheet_name = _env_str("VPS_SMC_GSHEET_OUTCOME_SHEET_NAME", "FORWARD_OUTCOMES")
            if _env_bool("VPS_SMC_GSHEET_CREATE_HEADER", True) and not _sheet_header_matches(spreadsheet_id, service_account_file, outcome_sheet_name, outcome_header):
                mirrored["outcomes"] = {}
            headers_written["outcomes"] = _sheet_append(
                spreadsheet_id,
                service_account_file,
                outcome_sheet_name,
                outcome_header,
                outcome_rows,
                _env_bool("VPS_SMC_GSHEET_CREATE_HEADER", True),
                bool(headers_written.get("outcomes", False)),
                clear_on_header_change=True,
            )
        counts["shadow_candidates_written"] = len(shadow_rows)
        counts["compare_rows_written"] = len(compare_rows)
        counts["outcomes_written"] = len(outcome_rows)
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


def vps_smc_diagnostics_latest(limit: int = 1) -> Dict[str, Any]:
    rows = _read_jsonl(_log_dir() / "vps_smc_gate_diagnostics.jsonl")
    lim = max(1, min(int(limit or 1), 20))
    latest = rows[-lim:] if rows else []
    return {"ok": True, "count": len(latest), "rows": latest}
