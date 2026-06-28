# === SMC_FINAL_PURGE_STAT_TECH_PRIMARY_20260628 ===
import json
import os
import time
import hmac
import hashlib
import pickle
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from pathlib import Path
from decimal import Decimal, ROUND_DOWN
from threading import Lock, Thread
from typing import Any, Dict, Optional, List, Tuple
from zoneinfo import ZoneInfo

try:
    from websocket import WebSocketApp
except Exception:
    WebSocketApp = None
from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, ConfigDict
# === SMC_RUNTIME_REMOVED_STAT_TECH_PRIMARY_20260628 ===
class _RemovedVpsSmc:
    def __getattr__(self, name):
        def _disabled(*args, **kwargs):
            if name in ("scheduler_status", "vps_smc_scheduler_status"):
                return {
                    "ok": True,
                    "enabled": False,
                    "disabled": True,
                    "reason": "smc_runtime_removed_stat_tech_primary",
                    "replacement": "STAT_TECH_V1",
                }
            if "latest" in name or "signals" in name:
                return []
            return {
                "ok": False,
                "disabled": True,
                "reason": "smc_runtime_removed_stat_tech_primary",
                "replacement": "STAT_TECH_V1",
                "function": name,
            }
        return _disabled

vps_smc = _RemovedVpsSmc()

import app.quant_engine as quant_engine
from app.position_manager import PositionManagerDeps, create_router as create_position_manager_router


APP_VERSION = "v0.25-p0-vps-smc-primary-execution-bridge"

LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
STATE_DIR = Path(os.getenv("STATE_DIR", "state"))

SIGNALS_LOG = LOG_DIR / "signals.jsonl"
DECISIONS_LOG = LOG_DIR / "decisions.jsonl"
PAPER_STATE_FILE = STATE_DIR / "paper_state.json"
PAPER_EVENTS_LOG = LOG_DIR / "paper_events.jsonl"
PAPER_PERFORMANCE_LOG = LOG_DIR / "paper_performance.jsonl"
EXECUTION_PLANS_LOG = LOG_DIR / "execution_plans.jsonl"
EXECUTION_EVENTS_LOG = LOG_DIR / "execution_events.jsonl"
MANAGER_DECISIONS_LOG = LOG_DIR / "manager_decisions.jsonl"
MANAGER_ACTIONS_LOG = LOG_DIR / "manager_actions.jsonl"
MANAGER_ERRORS_LOG = LOG_DIR / "manager_errors.jsonl"
MANAGER_HOURLY_REPORTS_LOG = LOG_DIR / "manager_hourly_reports.jsonl"
EXECUTION_SUMMARY_LOG = LOG_DIR / "execution_summary.jsonl"
TP_LIFECYCLE_STATE_FILE = STATE_DIR / "tp_lifecycle_state.json"
ML_SHADOW_SIGNALS_LOG = LOG_DIR / "ml_shadow_signals.jsonl"
ML_CONTEXT_SNAPSHOTS_LOG = LOG_DIR / "ml_context_snapshots.jsonl"
ML_PREDICTIONS_LOG = LOG_DIR / "ml_predictions.jsonl"
ML_DATASET_ROWS_LOG = LOG_DIR / "ml_dataset_rows.jsonl"
FORWARD_OUTCOMES_LOG = LOG_DIR / "forward_outcomes.jsonl"
FORWARD_OUTCOME_ERRORS_LOG = LOG_DIR / "forward_outcome_errors.jsonl"
ML_CONTEXT_ERRORS_LOG = LOG_DIR / "ml_context_errors.jsonl"
ML_CONTEXT_CACHE_FILE = STATE_DIR / "ml_context_cache.json"
FORWARD_OUTCOME_STATE_FILE = STATE_DIR / "forward_outcome_state.json"
MARKET_DATA_DEFAULT_DIR = STATE_DIR / "market_data"
MARKET_DATA_DEFAULT_AUDIT_LOG = LOG_DIR / "market_candles.jsonl"

WIB = ZoneInfo("Asia/Jakarta")
LOCK = Lock()
MARKET_DATA_LOCK = Lock()
MARKET_DATA_THREAD: Optional[Thread] = None
MARKET_WS_CONNECTED = False
MARKET_BOOTSTRAP_DONE = False
MARKET_LAST_ERROR = ""
MARKET_LAST_CLOSED: Dict[str, Dict[str, Any]] = {}

app = FastAPI(title="AI Trading VPS Bot", version=APP_VERSION)
try:
    from app.dashboard import router as dashboard_router
    app.include_router(dashboard_router)
    print("[dashboard] enabled")
except Exception as e:
    print(f"[dashboard] load failed: {e}")

class SignalPayload(BaseModel):
    model_config = ConfigDict(extra="allow")

    signal_id: Optional[str] = None
    signal_key: Optional[str] = None
    key_type: Optional[str] = None

    symbol: Optional[str] = None
    pair: Optional[str] = None

    direction: Optional[str] = None
    dir: Optional[str] = None

    status: Optional[str] = None
    state: Optional[str] = None

    priority: Optional[str] = None
    score: Optional[float] = None

    entry_lo: Optional[float] = None
    entry_hi: Optional[float] = None
    entry_mid: Optional[float] = None

    signal_time_wib: Optional[str] = None
    run_ts_wib: Optional[str] = None
    confirmed_ts_wib: Optional[str] = None

class PaperClosePayload(BaseModel):
    model_config = ConfigDict(extra="allow")

    signal_key: str
    outcome: Optional[str] = None
    close_reason: Optional[str] = "MANUAL_CLOSE"
    close_price: Optional[float] = None
    notes: Optional[str] = None


class PaperCloseAllPayload(BaseModel):
    model_config = ConfigDict(extra="allow")

    outcome: Optional[str] = None
    close_reason: Optional[str] = "MANUAL_CLOSE_ALL"
    close_price: Optional[float] = None
    notes: Optional[str] = None


class OperatorSymbolPayload(BaseModel):
    model_config = ConfigDict(extra="allow")

    symbol: Optional[str] = None
    date_wib: Optional[str] = None
    date_utc: Optional[str] = None
    signal_key: Optional[str] = None


class MlContextPayload(BaseModel):
    model_config = ConfigDict(extra="allow")
    symbol: Optional[str] = None
    pair: Optional[str] = None
    direction: Optional[str] = None
    signal_key: Optional[str] = None
    signal_time_wib: Optional[str] = None


class ForwardOutcomeEvaluatePayload(BaseModel):
    model_config = ConfigDict(extra="allow")
    limit: Optional[int] = None
    max_rows: Optional[int] = None
    force: Optional[bool] = False


class MlDatasetReclassifyPayload(BaseModel):
    model_config = ConfigDict(extra="allow")
    dry_run: Optional[bool] = True
    backup: Optional[bool] = True
    limit: Optional[int] = None
    force: Optional[bool] = False




class MlTrainLogisticPayload(BaseModel):
    model_config = ConfigDict(extra="allow")
    model_version: Optional[str] = "logistic_v1"
    force: Optional[bool] = True
    min_rows: Optional[int] = 30
    min_loss: Optional[int] = 5
    mode: Optional[str] = "SMOKE_TRAIN"


class MlPredictionScorePayload(BaseModel):
    model_config = ConfigDict(extra="allow")
    signal_key: Optional[str] = None
    payload: Optional[Dict[str, Any]] = None

class VpsSmcRunOncePayload(BaseModel):
    model_config = ConfigDict(extra="allow")
    symbols: Optional[List[str]] = None


class VpsSmcComparePayload(BaseModel):
    model_config = ConfigDict(extra="allow")
    lookback_minutes: Optional[int] = 180
    symbols: Optional[List[str]] = None
    run_vps_first: Optional[bool] = False


class VpsSmcDebugReplayPayload(BaseModel):
    model_config = ConfigDict(extra="allow")
    symbol: str
    as_of_wib: Optional[str] = None
    as_of_utc: Optional[str] = None
    replay_at_wib: Optional[str] = None
    replay_at_utc: Optional[str] = None


class VpsSmcMirrorPayload(BaseModel):
    model_config = ConfigDict(extra="allow")
    target: Optional[str] = "ALL"
    limit: Optional[int] = 100
    force: Optional[bool] = False


class VpsSmcSchedulerRunOncePayload(BaseModel):
    model_config = ConfigDict(extra="allow")
    symbols: Optional[List[str]] = None
    run_vps: Optional[bool] = True
    run_compare: Optional[bool] = True
    mirror_gsheet: Optional[bool] = False
    lookback_minutes: Optional[int] = 360

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def wib_now_iso() -> str:
    return datetime.now(WIB).isoformat()


def ensure_dirs() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    market_storage_dir().mkdir(parents=True, exist_ok=True)
    market_audit_log_path().parent.mkdir(parents=True, exist_ok=True)


def append_jsonl(path: Path, obj: Dict[str, Any]) -> None:
    ensure_dirs()
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n")


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return int(str(raw).strip())
    except Exception:
        return default


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return float(str(raw).strip())
    except Exception:
        return default


def market_storage_dir() -> Path:
    raw = str(os.getenv("BINANCE_CANDLE_STORE_DIR") or "").strip()
    return Path(raw) if raw else MARKET_DATA_DEFAULT_DIR


def market_audit_log_path() -> Path:
    raw = str(os.getenv("BINANCE_CANDLE_AUDIT_LOG") or "").strip()
    return Path(raw) if raw else MARKET_DATA_DEFAULT_AUDIT_LOG


def market_enabled() -> bool:
    primary = env_bool("BINANCE_CANDLE_STORE_ENABLED", True)
    alias = env_bool("BINANCE_CANDLE_COLLECTOR_ENABLED", primary)
    return primary and alias


def market_intervals() -> List[str]:
    raw = str(os.getenv("BINANCE_CANDLE_INTERVALS") or "1m,5m,15m,4h").strip()
    allowed = {"1m", "5m", "15m", "4h"}
    intervals = [x.strip() for x in raw.split(",") if x.strip()]
    clean = [x for x in intervals if x in allowed]
    return clean or ["1m", "5m", "15m", "4h"]


def market_allowlist_symbols() -> List[str]:
    allow = sorted(csv_set("PAIR_ALLOWLIST"))
    if allow:
        return [v010_normalize_symbol(s) for s in allow if v010_normalize_symbol(s)]
    return ["BTCUSDT", "ETHUSDT", "UNIUSDT"]


def retention_cutoff_ms(retention_days: int) -> int:
    return int((utc_now() - timedelta(days=retention_days)).timestamp() * 1000)


def market_data_file(symbol: str, interval: str) -> Path:
    return market_storage_dir() / f"{symbol.upper()}_{interval}.jsonl"


def market_load_candles(symbol: str, interval: str) -> List[Dict[str, Any]]:
    ensure_dirs()
    path = market_data_file(symbol, interval)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
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
                rows.append(obj)
    return rows


def market_write_candles(symbol: str, interval: str, rows: List[Dict[str, Any]]) -> None:
    ensure_dirs()
    path = market_data_file(symbol, interval)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")


def market_upsert_candles(rows: List[Dict[str, Any]], retention_days: int) -> Dict[str, Any]:
    global MARKET_LAST_CLOSED

    def canonical_compare(row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "close_time_ms": int(row.get("close_time_ms") or 0),
            "open": str(row.get("open") or ""),
            "high": str(row.get("high") or ""),
            "low": str(row.get("low") or ""),
            "close": str(row.get("close") or ""),
            "volume": str(row.get("volume") or ""),
            "quote_volume": str(row.get("quote_volume") or ""),
            "trade_count": int(row.get("trade_count") or 0),
            "source": str(row.get("source") or ""),
            "is_closed": bool(row.get("is_closed")),
        }

    with MARKET_DATA_LOCK:
        cutoff_ms = retention_cutoff_ms(retention_days)
        grouped: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
        for item in rows:
            symbol = str(item.get("symbol") or "").upper()
            interval = str(item.get("interval") or "1m")
            if not symbol or interval not in market_intervals():
                continue
            grouped.setdefault((symbol, interval), []).append(item)
        total_written = 0
        for (symbol, interval), incoming in grouped.items():
            existing = market_load_candles(symbol, interval)
            merged: Dict[tuple[str, str, int], Dict[str, Any]] = {}
            existing_map: Dict[tuple[str, str, int], Dict[str, Any]] = {}
            audit_append_rows: List[Dict[str, Any]] = []
            for item in existing:
                open_time_ms = int(item.get("open_time_ms") or 0)
                if open_time_ms <= 0 or open_time_ms < cutoff_ms:
                    continue
                key = (symbol, interval, open_time_ms)
                merged[key] = item
                existing_map[key] = item
            for row in incoming:
                open_time_ms = int(row.get("open_time_ms") or 0)
                if open_time_ms <= 0 or open_time_ms < cutoff_ms:
                    continue
                key = (symbol, interval, open_time_ms)
                prev = merged.get(key)
                if prev is not None and canonical_compare(prev) == canonical_compare(row):
                    merged[key] = prev
                    continue
                merged[key] = row
                if existing_map.get(key) is None or canonical_compare(existing_map.get(key) or {}) != canonical_compare(row):
                    audit_append_rows.append(row)
            clean_rows = sorted(merged.values(), key=lambda x: int(x.get("open_time_ms") or 0))
            if clean_rows:
                MARKET_LAST_CLOSED.setdefault(symbol, {})[interval] = clean_rows[-1]
            market_write_candles(symbol, interval, clean_rows)
            total_written += len(clean_rows)
            if audit_append_rows:
                with market_audit_log_path().open("a", encoding="utf-8") as af:
                    for row in audit_append_rows:
                        af.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
        return {"written": total_written, "upserted": len(rows), "cutoff_ms": cutoff_ms}


def market_build_row_from_kline(symbol: str, interval: str, kline: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        return {
            "symbol": str(symbol or "").upper(),
            "interval": str(interval or "1m"),
            "open_time_ms": int(kline.get("t")),
            "close_time_ms": int(kline.get("T")),
            "open": str(kline.get("o")),
            "high": str(kline.get("h")),
            "low": str(kline.get("l")),
            "close": str(kline.get("c")),
            "volume": str(kline.get("v")),
            "quote_volume": str(kline.get("q")),
            "trade_count": int(kline.get("n") or 0),
            "source": "BINANCE_FUTURES",
            "is_closed": bool(kline.get("x")),
            "received_at_utc": utc_now_iso(),
        }
    except Exception:
        return None


def market_build_row_from_rest(symbol: str, interval: str, row: List[Any]) -> Optional[Dict[str, Any]]:
    try:
        now_ms = int(time.time() * 1000)
        close_time_ms = int(row[6])
        if close_time_ms > now_ms:
            return None
        return {
            "symbol": str(symbol or "").upper(),
            "interval": str(interval or "1m"),
            "open_time_ms": int(row[0]),
            "close_time_ms": close_time_ms,
            "open": str(row[1]),
            "high": str(row[2]),
            "low": str(row[3]),
            "close": str(row[4]),
            "volume": str(row[5]),
            "quote_volume": str(row[7]),
            "trade_count": int(row[8]),
            "source": "BINANCE_FUTURES",
            "is_closed": True,
            "received_at_utc": utc_now_iso(),
        }
    except Exception:
        return None


def market_rest_bootstrap(symbols: List[str], intervals: List[str], limit: int = 300) -> Dict[str, Any]:
    base = "https://fapi.binance.com/fapi/v1/klines"
    inserted_rows: List[Dict[str, Any]] = []
    failures: List[str] = []
    for symbol in symbols:
        for interval in intervals:
            try:
                query = urllib.parse.urlencode({"symbol": symbol, "interval": interval, "limit": int(limit)})
                rows = http_get_json(f"{base}?{query}")
                if not isinstance(rows, list):
                    failures.append(f"{symbol}:{interval}:invalid_response")
                    continue
                for r in rows:
                    if not isinstance(r, list) or len(r) < 9:
                        continue
                    obj = market_build_row_from_rest(symbol, interval, r)
                    if obj:
                        inserted_rows.append(obj)
            except Exception as e:
                failures.append(f"{symbol}:{interval}:{e}")
    retention_days = env_int("BINANCE_CANDLE_RETENTION_DAYS", 7)
    write_res = market_upsert_candles(inserted_rows, retention_days)
    return {"ok": True, "symbols": symbols, "intervals": intervals, "limit": limit, "rows_ingested": len(inserted_rows), "failures": failures, "write": write_res}




def paper_notional_usdt_default() -> float:
    return env_float("PAPER_NOTIONAL_USDT", 50.0)


def paper_fee_rate() -> float:
    return env_float("PAPER_FEE_RATE", 0.0005)


def paper_fee_buffer_mult() -> float:
    return env_float("PAPER_FEE_BUFFER_MULT", 1.2)


def paper_slippage_buffer_rate() -> float:
    return env_float("PAPER_SLIPPAGE_BUFFER_RATE", 0.0002)


def paper_net_pnl_enabled() -> bool:
    return env_bool("PAPER_NET_PNL_ENABLED", True)


# =========================
# LIVE SMALL CAPITAL EXECUTOR
# =========================

def live_fapi_base_url() -> str:
    return str(os.getenv("BINANCE_FAPI_BASE_URL") or "https://fapi.binance.com").rstrip("/")

def live_api_credentials() -> dict:
    api_key = (
        os.getenv("BINANCE_LIVE_API_KEY")
        or os.getenv("BINANCE_API_KEY")
        or os.getenv("BINANCE_FUTURES_LIVE_API_KEY")
        or ""
    )
    api_secret = (
        os.getenv("BINANCE_LIVE_API_SECRET")
        or os.getenv("BINANCE_API_SECRET")
        or os.getenv("BINANCE_FUTURES_LIVE_API_SECRET")
        or ""
    )
    return {
        "ok": bool(api_key and api_secret),
        "api_key": api_key,
        "api_secret": api_secret,
        "reason": None if api_key and api_secret else "missing_live_api_credentials",
    }

def live_signed_request(method: str, path: str, params: dict | None = None) -> dict:
    creds = live_api_credentials()
    if not creds.get("ok"):
        return {"ok": False, "reason": creds.get("reason"), "http_status": None, "body": None, "path": path}

    params = dict(params or {})
    params["timestamp"] = int(time.time() * 1000)
    params.setdefault("recvWindow", 5000)

    query = urllib.parse.urlencode(params, doseq=True)
    sig = hmac.new(
        str(creds["api_secret"]).encode("utf-8"),
        query.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

    url = live_fapi_base_url() + path + "?" + query + "&signature=" + sig
    req = urllib.request.Request(
        url,
        headers={"X-MBX-APIKEY": str(creds["api_key"])},
        method=method.upper()
    )

    if method.upper() in ("POST", "PUT", "DELETE"):
        req.data = b""

    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            raw = r.read().decode("utf-8")
            try:
                body = json.loads(raw)
            except Exception:
                body = raw
            return {
                "ok": 200 <= int(r.status) < 300,
                "http_status": int(r.status),
                "body": body,
                "reason": None,
                "path": path,
                "method": method.upper(),
            }
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        try:
            body = json.loads(raw)
        except Exception:
            body = raw
        return {
            "ok": False,
            "http_status": int(e.code),
            "body": body,
            "reason": "binance_http_error",
            "path": path,
            "method": method.upper(),
        }
    except Exception as e:
        return {
            "ok": False,
            "http_status": None,
            "body": None,
            "reason": f"live_request_exception:{type(e).__name__}:{e}",
            "path": path,
            "method": method.upper(),
        }

def _live_client_order_id(prefix: str, signal_key: str, label: str) -> str:
    raw = f"{prefix}_{abs(hash(signal_key)) % 999999999}_{label}"
    return "".join(ch for ch in raw if ch.isalnum() or ch in ("_", "-"))[:36]

def live_place_order(params: dict) -> dict:
    return live_signed_request("POST", "/fapi/v1/order", params)


def live_place_algo_order(params: dict) -> dict:
    """
    Binance USD-M Futures conditional orders.
    Auto-normalize per-symbol:
    - /fapi/v1/algoOrder
    - algoType=CONDITIONAL
    - stopPrice -> triggerPrice
    - price rounded by PRICE_FILTER.tickSize
    - quantity rounded by LOT_SIZE.stepSize
    """
    import json as _json
    import urllib.request as _urlreq
    from decimal import Decimal, ROUND_DOWN, ROUND_UP

    q = dict(params or {})

    if "newClientOrderId" in q and "clientAlgoId" not in q:
        q["clientAlgoId"] = q.pop("newClientOrderId")

    if "stopPrice" in q and "triggerPrice" not in q:
        q["triggerPrice"] = q.pop("stopPrice")

    q.setdefault("algoType", "CONDITIONAL")
    q.setdefault("positionSide", "BOTH")
    q.setdefault("workingType", "CONTRACT_PRICE")
    q.setdefault("priceProtect", "false")

    if str(q.get("closePosition", "")).lower() == "true":
        q.pop("quantity", None)
        q.pop("reduceOnly", None)
    else:
        q.pop("closePosition", None)

    def _fmt_decimal(x):
        return format(Decimal(str(x)).normalize(), "f")

    def _round_to_step(value, step, mode="down"):
        v = Decimal(str(value))
        st = Decimal(str(step))
        if st == 0:
            return _fmt_decimal(v)
        rounding = ROUND_UP if mode == "up" else ROUND_DOWN
        n = (v / st).to_integral_value(rounding=rounding)
        return _fmt_decimal(n * st)

    def _filters(symbol):
        symbol = str(symbol or "").upper().strip()
        cache = getattr(live_place_algo_order, "_filters_cache", {})
        if symbol in cache:
            return cache[symbol]

        url = live_fapi_base_url() + "/fapi/v1/exchangeInfo"
        with _urlreq.urlopen(url, timeout=15) as r:
            data = _json.loads(r.read().decode("utf-8"))

        for item in data.get("symbols", []):
            if item.get("symbol") == symbol:
                fs = {f.get("filterType"): f for f in item.get("filters", [])}
                out = {
                    "tickSize": fs.get("PRICE_FILTER", {}).get("tickSize", "0.0001"),
                    "stepSize": fs.get("LOT_SIZE", {}).get("stepSize", "0.1"),
                    "minQty": fs.get("LOT_SIZE", {}).get("minQty", "0"),
                    "minNotional": fs.get("MIN_NOTIONAL", {}).get("notional", "0"),
                }
                cache[symbol] = out
                setattr(live_place_algo_order, "_filters_cache", cache)
                return out

        out = {"tickSize": "0.0001", "stepSize": "0.1", "minQty": "0", "minNotional": "0"}
        cache[symbol] = out
        setattr(live_place_algo_order, "_filters_cache", cache)
        return out

    symbol = str(q.get("symbol") or "").upper().strip()
    if symbol:
        f = _filters(symbol)
        tick = f.get("tickSize", "0.0001")
        step = f.get("stepSize", "0.1")

        side = str(q.get("side") or "").upper()
        typ = str(q.get("type") or "").upper()

        # Final anti -2021 guard:
        # STOP/TP trigger must be on the correct side of live current price
        # right before POST /fapi/v1/algoOrder.
        side_guard_enabled = str(os.getenv("PROTECTION_SIDE_GUARD_ENABLED", "true")).strip().lower() in ("1", "true", "yes", "y", "on")
        if side_guard_enabled and "triggerPrice" in q and q.get("triggerPrice") not in (None, ""):
            try:
                ref = live_bad_fill_current_price(symbol)
            except Exception:
                ref = Decimal("0")

            try:
                trig = Decimal(str(q.get("triggerPrice")))
            except Exception:
                trig = Decimal("0")

            if ref > 0 and trig > 0:
                side_buf = Decimal(str(os.getenv("PROTECTION_SIDE_GUARD_BUFFER_PCT", os.getenv("RANGE_SIDE_GUARD_BUFFER_PCT", "0.05")))) / Decimal("100")
                tp_buf = Decimal(str(os.getenv("PROTECTION_TP_MIN_PROFIT_PCT", os.getenv("RANGE_TP1_MIN_PROFIT_PCT", "0.20")))) / Decimal("100")
                old_trig = trig
                adjusted_reason = None

                if typ == "STOP_MARKET":
                    # LONG exit SL = SELL stop below current
                    if side == "SELL" and trig >= ref:
                        trig = ref * (Decimal("1") - side_buf)
                        adjusted_reason = "LONG_SL_trigger_not_below_ref"
                    # SHORT exit SL = BUY stop above current
                    elif side == "BUY" and trig <= ref:
                        trig = ref * (Decimal("1") + side_buf)
                        adjusted_reason = "SHORT_SL_trigger_not_above_ref"

                elif typ == "TAKE_PROFIT_MARKET":
                    # LONG exit TP = SELL take-profit above current
                    if side == "SELL" and trig <= ref:
                        trig = ref * (Decimal("1") + tp_buf)
                        adjusted_reason = "LONG_TP_trigger_not_above_ref"
                    # SHORT exit TP = BUY take-profit below current
                    elif side == "BUY" and trig >= ref:
                        trig = ref * (Decimal("1") - tp_buf)
                        adjusted_reason = "SHORT_TP_trigger_not_below_ref"

                if adjusted_reason:
                    q["side_guard_meta"] = {
                        "adjusted": True,
                        "reason": adjusted_reason,
                        "ref_price": _fmt_decimal(ref),
                        "old_triggerPrice": _fmt_decimal(old_trig),
                        "new_triggerPrice": _fmt_decimal(trig),
                        "type": typ,
                        "side": side,
                    }
                    q["triggerPrice"] = _fmt_decimal(trig)

        if "quantity" in q and q.get("quantity") not in (None, ""):
            q["quantity"] = _round_to_step(q["quantity"], step, "down")

        if "triggerPrice" in q and q.get("triggerPrice") not in (None, ""):
            # Preserve trigger direction:
            # SHORT exit = BUY: SL STOP round up, TP round down.
            # LONG exit  = SELL: SL STOP round down, TP round up.
            if typ == "STOP_MARKET":
                mode = "up" if side == "BUY" else "down"
            elif typ == "TAKE_PROFIT_MARKET":
                mode = "down" if side == "BUY" else "up"
            else:
                mode = "down"

            q["triggerPrice"] = _round_to_step(q["triggerPrice"], tick, mode)

        q["filter_meta"] = f

    # Jangan kirim metadata internal ke Binance.
    request_meta = dict(q)
    q.pop("filter_meta", None)
    q.pop("side_guard_meta", None)

    res = live_signed_request("POST", "/fapi/v1/algoOrder", q)
    res["request_params"] = q
    res["filter_meta"] = request_meta.get("filter_meta")
    res["side_guard_meta"] = request_meta.get("side_guard_meta")
    return res


def live_futures_position_risk(symbol: str) -> dict:
    symbol = str(symbol or "").upper().strip()
    if not symbol:
        return {"ok": False, "reason": "symbol_required_for_position_risk", "body": [], "positions": [], "raw": None}

    res = live_signed_request("GET", "/fapi/v2/positionRisk", {"symbol": symbol})
    body = res.get("body")
    positions = body if isinstance(body, list) else []
    return {
        "ok": bool(res.get("ok")),
        "reason": None if bool(res.get("ok")) else (res.get("reason") or "live_position_risk_failed"),
        "http_status": res.get("http_status"),
        "body": body,
        "positions": positions,
        "raw": res,
    }


def live_fetch_open_orders(symbol: str) -> dict:
    symbol = str(symbol or "").upper().strip()
    if not symbol:
        return {
            "ok": False,
            "reason": "symbol_required_for_open_orders",
            "orders": [],
            "normal_orders": [],
            "algo_orders": [],
            "raw": None,
        }

    normal_res = live_signed_request("GET", "/fapi/v1/openOrders", {"symbol": symbol})
    algo_res = live_signed_request("GET", "/fapi/v1/openAlgoOrders", {"symbol": symbol})

    normal_body = normal_res.get("body")
    algo_body = algo_res.get("body")

    normal_orders = normal_body if isinstance(normal_body, list) else []
    algo_orders = algo_body if isinstance(algo_body, list) else []

    combined = []

    for row in normal_orders:
        if isinstance(row, dict):
            x = dict(row)
            x["_source_endpoint"] = "openOrders"
            combined.append(x)

    for row in algo_orders:
        if isinstance(row, dict):
            x = dict(row)
            x["_source_endpoint"] = "openAlgoOrders"
            combined.append(x)

    ok = bool(normal_res.get("ok")) and bool(algo_res.get("ok"))

    return {
        "ok": ok,
        "reason": None if ok else (
            normal_res.get("reason")
            or algo_res.get("reason")
            or "live_open_orders_or_algo_orders_failed"
        ),
        "orders": combined,
        "normal_orders": normal_orders,
        "algo_orders": algo_orders,
        "normal_open_count": len(normal_orders),
        "algo_open_count": len(algo_orders),
        "raw": {
            "openOrders": normal_res,
            "openAlgoOrders": algo_res,
        },
    }


def live_try_set_margin_and_leverage(plan: Dict[str, Any]) -> dict:
    if not isinstance(plan, dict):
        plan = {}
    p = plan
    # === ML_HARD_GATE_PRE_LIVE_20260620 ===
    # Hard-block live execution BEFORE margin/leverage, entry_build, and live_place_order.
    if env_bool("ML_PREDICTION_ENABLED", True) and env_bool("ML_GATE_ENABLED", False) and ml_gate_mode() == "HARD_GATE":
        try:
            score_res = score_ml_prediction_internal(signal_key_of(p), p)
        except Exception as e:
            score_res = {
                "ok": False,
                "ml_decision": "REJECT_BY_ML_GATE" if not env_bool("ML_GATE_FAIL_OPEN", False) else "ML_GATE_ERROR_FAIL_OPEN",
                "reason": "ml_gate_exception_fail_closed" if not env_bool("ML_GATE_FAIL_OPEN", False) else "ml_gate_exception_fail_open",
                "error": f"{type(e).__name__}: {e}",
                "ml_gate_mode": ml_gate_mode(),
                "model_version": os.getenv("ML_GATE_MODEL", "unknown"),
            }

        raw_ml_decision = str(score_res.get("ml_decision") or score_res.get("decision") or "").strip().upper()
        ml_reason = score_res.get("reason") or score_res.get("decision") or "ml_hard_gate"
        ml_prob = (
            score_res.get("probability_win")
            if score_res.get("probability_win") is not None
            else score_res.get("p_win")
            if score_res.get("p_win") is not None
            else score_res.get("p_win_adj")
        )

        ml_fields = {
            "ml_gate_mode": score_res.get("ml_gate_mode") or ml_gate_mode(),
            "ml_gate_decision": score_res.get("ml_decision") or score_res.get("decision"),
            "ml_gate_reason": ml_reason,
            "ml_probability_win": ml_prob,
            "ml_prob": ml_prob,
            "ml_score": score_res.get("ml_score"),
            "ml_confidence": score_res.get("confidence") or score_res.get("ml_confidence"),
            "ml_model_version": score_res.get("model_version"),
        }

        for k, v in ml_fields.items():
            p[k] = v
            plan[k] = v
            base_event[k] = v

        hard_reject_decisions = {
            "REJECT_BY_ML_GATE",
            "ML_BLOCK_LOW_PWIN",
            "ML_BLOCK_LOW_CONFIDENCE",
            "ML_MODEL_ERROR_FAIL_CLOSED",
            "ML_GATE_ERROR_FAIL_CLOSED",
        }

        if raw_ml_decision in hard_reject_decisions:
            event = dict(base_event)
            event.update({
                "action": "LIVE_SMALL_CAPITAL_ML_GATE",
                "decision": "REJECT",
                "reason": ml_reason,
                "gate": "ml_gate",
                "plan": plan,
                "ml_score_result": score_res,
            })
            append_jsonl(EXECUTION_EVENTS_LOG, event)
            return {
                "ok": False,
                "decision": "REJECT",
                "reason": ml_reason,
                "gate": "ml_gate",
                "plan": plan,
                "ml_score_result": score_res,
                **ml_fields,
            }

    symbol = str(plan.get("symbol") or "").upper()
    lev = int(plan.get("leverage") or env_int("DEFAULT_LEVERAGE", 2))
    # === NORMALIZE_MARGIN_TYPE_20260624 ===
    raw_margin_type = os.getenv("MARGIN_TYPE") or plan.get("margin_type") or "CROSSED"
    margin_type = str(raw_margin_type).strip().upper()
    if margin_type in ("CROSS", "CROSSED"):
        margin_type = "CROSSED"
    elif margin_type == "ISOLATED":
        margin_type = "ISOLATED"
    else:
        margin_type = "CROSSED"

    margin_res = live_signed_request("POST", "/fapi/v1/marginType", {
        "symbol": symbol,
        "marginType": margin_type,
    })

    # Binance returns error if margin type already set. Treat that as non-fatal.
    margin_ok = bool(margin_res.get("ok"))
    body = margin_res.get("body")
    body_text = json.dumps(body, ensure_ascii=False) if isinstance(body, (dict, list)) else str(body)
    if (not margin_ok) and ("No need to change margin type" in body_text or "-4046" in body_text):
        margin_ok = True

    lev_res = live_signed_request("POST", "/fapi/v1/leverage", {
        "symbol": symbol,
        "leverage": lev,
    })

    return {
        "ok": margin_ok and bool(lev_res.get("ok")),
        "margin": margin_res,
        "leverage": lev_res,
        "margin_ok": margin_ok,
        "leverage_ok": bool(lev_res.get("ok")),
    }


def _live_plain_decimal(x: Decimal) -> str:
    out = format(Decimal(str(x)).normalize(), "f")
    if "." in out:
        out = out.rstrip("0").rstrip(".")
    return out or "0"


def live_get_position_snapshot(symbol: str) -> dict:
    symbol = str(symbol or "").upper().strip()

    fn = globals().get("live_futures_position_risk")
    if callable(fn):
        res = fn(symbol)
    else:
        res = live_signed_request("GET", "/fapi/v2/positionRisk", {"symbol": symbol})

    body = res.get("positions") or res.get("body")
    if isinstance(body, dict):
        body = body.get("positions") or body.get("body") or []

    positions = body if isinstance(body, list) else []
    pos = None
    for row in positions:
        if str(row.get("symbol") or "").upper() == symbol:
            pos = row
            break

    return {
        "ok": bool(res.get("ok")),
        "reason": None if bool(res.get("ok")) else (res.get("reason") or "position_risk_failed"),
        "raw": res,
        "position": pos,
    }


def live_wait_position_after_entry(symbol: str, direction: str, signal_key: str = "") -> dict:
    symbol = str(symbol or "").upper().strip()
    d = str(direction or "").upper()
    attempts = env_int("LIVE_WAIT_POSITION_ATTEMPTS", 14)
    sleep_sec = env_float("LIVE_WAIT_POSITION_SLEEP_SEC", 0.5)

    last = None

    for i in range(max(1, attempts)):
        snap = live_get_position_snapshot(symbol)
        last = snap

        pos = snap.get("position") or {}
        try:
            amt = Decimal(str(pos.get("positionAmt") or "0"))
            entry_price = Decimal(str(pos.get("entryPrice") or "0"))
        except Exception:
            amt = Decimal("0")
            entry_price = Decimal("0")

        sign_ok = (d == "LONG" and amt > 0) or (d == "SHORT" and amt < 0)

        if bool(snap.get("ok")) and sign_ok and abs(amt) > 0:
            return {
                "ok": True,
                "reason": "position_confirmed_after_entry",
                "attempt": i + 1,
                "symbol": symbol,
                "direction": direction,
                "positionAmt": _live_plain_decimal(amt),
                "positionQty": _live_plain_decimal(abs(amt)),
                "entryPrice": _live_plain_decimal(entry_price),
                "position": pos,
                "raw": snap.get("raw"),
            }

        time.sleep(max(0.1, sleep_sec))

    return {
        "ok": False,
        "reason": "position_not_confirmed_after_entry",
        "symbol": symbol,
        "direction": direction,
        "attempts": attempts,
        "last_snapshot": last,
    }


def live_apply_actual_position_to_plan(plan: Dict[str, Any], fill_res: dict) -> Dict[str, Any]:
    out = dict(plan or {})
    pos = (fill_res or {}).get("position") or {}

    try:
        amt = Decimal(str(pos.get("positionAmt") or fill_res.get("positionAmt") or "0"))
    except Exception:
        amt = Decimal("0")

    qty = abs(amt)
    if qty <= 0:
        return out

    policy = str(os.getenv("LIVE_TP_POLICY") or os.getenv("TP_POLICY") or "").strip().upper()
    if policy in ("FULL_TP1", "TP1_FULL", "100_TP1", "ALL_TP1"):
        q1 = qty
        q2 = Decimal("0")
        q3 = Decimal("0")
        split_mode = "FULL_TP1_actual_position_qty"
    else:
        q1 = qty * Decimal("0.40")
        q2 = qty * Decimal("0.35")
        q3 = qty - q1 - q2
        split_mode = "40_35_25_actual_position_qty"

    out["quantity"] = _live_plain_decimal(qty)
    out["tp1_qty"] = _live_plain_decimal(q1)
    out["tp2_qty"] = _live_plain_decimal(q2)
    out["tp3_qty"] = _live_plain_decimal(q3)
    out["tp_split_mode"] = split_mode
    out["protection_qty_source"] = "binance_positionRisk_after_entry"

    try:
        ep = Decimal(str(pos.get("entryPrice") or fill_res.get("entryPrice") or "0"))
        if ep > 0:
            out["actual_entry_price"] = _live_plain_decimal(ep)
    except Exception:
        pass

    return out




def live_bad_fill_decimal_str(x) -> str:
    from decimal import Decimal
    out = format(Decimal(str(x)).normalize(), "f")
    if "." in out:
        out = out.rstrip("0").rstrip(".")
    return out or "0"


def live_bad_fill_current_price(symbol: str) -> Decimal:
    import json as _json
    import urllib.request as _urlreq
    try:
        url = live_fapi_base_url().rstrip("/") + "/fapi/v1/ticker/price?symbol=" + str(symbol).upper().strip()
        with _urlreq.urlopen(url, timeout=5) as r:
            data = _json.loads(r.read().decode("utf-8"))
        return Decimal(str(data.get("price") or "0"))
    except Exception:
        return Decimal("0")


def live_apply_bad_fill_tight_sl(plan: Dict[str, Any], entry_fill_res: dict) -> Dict[str, Any]:
    """
    BAD_FILL_ACTION=TIGHT_SL:
    if actual entry is too adverse vs plan entry, keep trade alive but tighten SL.

    LONG adverse  = actual_entry > plan_entry
    SHORT adverse = actual_entry < plan_entry
    """
    out = dict(plan or {})

    guard_enabled = str(os.getenv("BAD_FILL_GUARD_ENABLED", "false")).strip().lower() in ("1", "true", "yes", "y", "on")
    if not guard_enabled:
        out["bad_fill_guard_enabled"] = False
        return out

    out["bad_fill_guard_enabled"] = True

    action = str(os.getenv("BAD_FILL_ACTION", "")).strip().upper()
    if action not in ("TIGHT_SL", "TIGHT_STOP", "TIGHT"):
        return out

    try:
        direction = str(out.get("direction") or out.get("payload", {}).get("direction") or "").upper()
        if direction.startswith("LONG"):
            direction = "LONG"
        elif direction.startswith("SHORT"):
            direction = "SHORT"
        else:
            return out

        plan_entry = Decimal(str(out.get("entry_mid") or out.get("entry") or out.get("payload", {}).get("entry_mid") or out.get("payload", {}).get("entry") or "0"))
        actual_entry = Decimal(str(out.get("actual_entry_price") or entry_fill_res.get("entryPrice") or "0"))
        if plan_entry <= 0 or actual_entry <= 0:
            return out

        if direction == "LONG":
            signed = (actual_entry - plan_entry) / plan_entry * Decimal("100")
        else:
            signed = (plan_entry - actual_entry) / plan_entry * Decimal("100")

        adverse = signed if signed > 0 else Decimal("0")
        max_bad = Decimal(str(os.getenv("MAX_ADVERSE_SLIPPAGE_PCT", "0.60")))

        out["actual_entry_price"] = live_bad_fill_decimal_str(actual_entry)
        out["adverse_slippage_pct"] = live_bad_fill_decimal_str(adverse)
        out["bad_fill_threshold_pct"] = live_bad_fill_decimal_str(max_bad)

        if adverse <= max_bad:
            out["bad_fill"] = False
            return out

        symbol = str(out.get("symbol") or "").upper().strip()
        current = live_bad_fill_current_price(symbol)
        ref = current if current > 0 else actual_entry

        sl_buf = Decimal(str(os.getenv("BAD_FILL_TIGHT_SL_BUFFER_PCT", "0.20"))) / Decimal("100")
        tp_buf = Decimal(str(os.getenv("BAD_FILL_TP1_MIN_PROFIT_PCT", "0.20"))) / Decimal("100")

        old_sl = Decimal(str(out.get("sl") or "0"))
        old_tp1 = Decimal(str(out.get("tp1") or "0"))

        if direction == "LONG":
            tight_sl = ref * (Decimal("1") - sl_buf)
            min_tp1 = ref * (Decimal("1") + tp_buf)
            new_tp1 = max(old_tp1, min_tp1) if old_tp1 > 0 else min_tp1
        else:
            tight_sl = ref * (Decimal("1") + sl_buf)
            min_tp1 = ref * (Decimal("1") - tp_buf)
            new_tp1 = min(old_tp1, min_tp1) if old_tp1 > 0 else min_tp1

        out["bad_fill"] = True
        out["bad_fill_action"] = "TIGHT_SL"
        out["bad_fill_reason"] = "adverse_slippage_gt_threshold"
        out["bad_fill_plan_entry"] = live_bad_fill_decimal_str(plan_entry)
        out["bad_fill_actual_entry"] = live_bad_fill_decimal_str(actual_entry)
        out["bad_fill_current_ref"] = live_bad_fill_decimal_str(ref)
        out["bad_fill_old_sl"] = live_bad_fill_decimal_str(old_sl)
        out["bad_fill_old_tp1"] = live_bad_fill_decimal_str(old_tp1)
        out["sl"] = live_bad_fill_decimal_str(tight_sl)
        out["tp1"] = live_bad_fill_decimal_str(new_tp1)
        out["tp2_qty"] = "0"
        out["tp3_qty"] = "0"
        out["tp_split_mode"] = "BAD_FILL_TIGHT_SL_FULL_TP1"

        return out

    except Exception as e:
        out["bad_fill_check_error"] = f"{type(e).__name__}:{e}"
        return out




def live_range_decimal_str(x) -> str:
    out = format(Decimal(str(x)).normalize(), "f")
    if "." in out:
        out = out.rstrip("0").rstrip(".")
    return out or "0"


def live_apply_range_plan_rebase(plan: Dict[str, Any], entry_fill_res: dict) -> Dict[str, Any]:
    """
    Range Plan Rebase:
    - Build entry range from plan_entry +/- ENTRY_RANGE_BUFFER_PCT.
    - If actual entry is still inside valid adverse range:
        SL/TP1 are rebased from actual entry using original plan risk/reward distance.
    - If actual entry is outside adverse range:
        only tag range_entry_in_range=false; BAD_FILL guard handles tight SL after this.
    """
    out = dict(plan or {})

    enabled = str(os.getenv("LIVE_RANGE_PLAN_ENABLED", "false")).strip().lower() in ("1", "true", "yes", "y", "on")
    rebase_enabled = str(os.getenv("RANGE_REBASE_SL_TP", "true")).strip().lower() in ("1", "true", "yes", "y", "on")
    preserve_rr = str(os.getenv("RANGE_PRESERVE_RR", "true")).strip().lower() in ("1", "true", "yes", "y", "on")
    no_worse_sl = str(os.getenv("RANGE_SL_NO_WORSE_THAN_PLAN", "true")).strip().lower() in ("1", "true", "yes", "y", "on")

    out["range_plan_enabled"] = bool(enabled)

    if not enabled:
        return out

    try:
        direction = str(out.get("direction") or out.get("payload", {}).get("direction") or "").upper()
        if direction.startswith("LONG"):
            direction = "LONG"
        elif direction.startswith("SHORT"):
            direction = "SHORT"
        else:
            out["range_plan_error"] = "direction_unknown"
            return out

        plan_entry = Decimal(str(out.get("entry_mid") or out.get("entry") or out.get("payload", {}).get("entry_mid") or out.get("payload", {}).get("entry") or "0"))
        actual_entry = Decimal(str(out.get("actual_entry_price") or entry_fill_res.get("entryPrice") or "0"))
        old_sl = Decimal(str(out.get("sl") or "0"))
        old_tp1 = Decimal(str(out.get("tp1") or "0"))

        if plan_entry <= 0 or actual_entry <= 0 or old_sl <= 0 or old_tp1 <= 0:
            out["range_plan_error"] = "missing_price"
            return out

        buf_pct = Decimal(str(os.getenv("ENTRY_RANGE_BUFFER_PCT", os.getenv("MAX_ADVERSE_SLIPPAGE_PCT", "0.60"))))
        buf = buf_pct / Decimal("100")

        # Adverse-valid range.
        # LONG bad if actual too high. SHORT bad if actual too low.
        if direction == "LONG":
            entry_range_lo = plan_entry
            entry_range_hi = plan_entry * (Decimal("1") + buf)
            adverse_slip = (actual_entry - plan_entry) / plan_entry * Decimal("100")
            entry_in_range = actual_entry <= entry_range_hi
        else:
            entry_range_lo = plan_entry * (Decimal("1") - buf)
            entry_range_hi = plan_entry
            adverse_slip = (plan_entry - actual_entry) / plan_entry * Decimal("100")
            entry_in_range = actual_entry >= entry_range_lo

        adverse_slip = adverse_slip if adverse_slip > 0 else Decimal("0")

        out["range_entry_buffer_pct"] = live_range_decimal_str(buf_pct)
        out["range_entry_lo"] = live_range_decimal_str(entry_range_lo)
        out["range_entry_hi"] = live_range_decimal_str(entry_range_hi)
        out["range_plan_entry"] = live_range_decimal_str(plan_entry)
        out["range_actual_entry"] = live_range_decimal_str(actual_entry)
        out["range_adverse_slippage_pct"] = live_range_decimal_str(adverse_slip)
        out["range_entry_in_range"] = bool(entry_in_range)

        if not entry_in_range:
            out["range_decision"] = "OUT_OF_RANGE_BAD_FILL_CANDIDATE"
            return out

        out["range_decision"] = "IN_RANGE_REBASE_ALLOWED"

        if not rebase_enabled:
            return out

        risk_dist = abs(old_sl - plan_entry)
        rr_target_r = Decimal(str(os.getenv("RR_TARGET_R", "1.2")))
        actual_risk_dist = abs(actual_entry - old_sl)
        reward_dist = actual_risk_dist * rr_target_r if actual_risk_dist > 0 else abs(plan_entry - old_tp1)
        out["post_fill_rr_rebase_applied"] = True
        out["post_fill_rr_target_r"] = str(rr_target_r)
        out["post_fill_actual_risk_dist"] = str(actual_risk_dist)

        if risk_dist <= 0 or reward_dist <= 0:
            out["range_plan_error"] = "bad_risk_reward_distance"
            return out

        symbol = str(out.get("symbol") or "").upper().strip()
        current_ref = live_bad_fill_current_price(symbol) if symbol else Decimal("0")
        ref_price = current_ref if current_ref > 0 else actual_entry

        side_guard = Decimal(str(os.getenv("RANGE_SIDE_GUARD_BUFFER_PCT", "0.05"))) / Decimal("100")
        tp_min_profit = Decimal(str(os.getenv("RANGE_TP1_MIN_PROFIT_PCT", os.getenv("BAD_FILL_TP1_MIN_PROFIT_PCT", "0.20")))) / Decimal("100")

        if direction == "LONG":
            rebase_sl = actual_entry - risk_dist
            rebase_tp1 = actual_entry + reward_dist

            # LONG SL must stay below current/ref price.
            old_sl_valid_side = old_sl < ref_price
            final_sl = max(rebase_sl, old_sl) if (no_worse_sl and old_sl_valid_side) else rebase_sl

            max_valid_sl = ref_price * (Decimal("1") - side_guard)
            if final_sl >= max_valid_sl:
                final_sl = max_valid_sl

            final_tp1 = rebase_tp1
            min_valid_tp1 = ref_price * (Decimal("1") + tp_min_profit)
            if final_tp1 <= min_valid_tp1:
                final_tp1 = min_valid_tp1

        else:
            rebase_sl = actual_entry + risk_dist
            rebase_tp1 = actual_entry - reward_dist

            # SHORT SL must stay above current/ref price.
            old_sl_valid_side = old_sl > ref_price
            final_sl = min(rebase_sl, old_sl) if (no_worse_sl and old_sl_valid_side) else rebase_sl

            min_valid_sl = ref_price * (Decimal("1") + side_guard)
            if final_sl <= min_valid_sl:
                final_sl = min_valid_sl

            final_tp1 = rebase_tp1
            max_valid_tp1 = ref_price * (Decimal("1") - tp_min_profit)
            if final_tp1 >= max_valid_tp1:
                final_tp1 = max_valid_tp1

        out["range_ref_price"] = live_range_decimal_str(ref_price)
        out["range_old_sl_valid_side"] = bool(old_sl_valid_side)

        out["range_old_sl"] = live_range_decimal_str(old_sl)
        out["range_old_tp1"] = live_range_decimal_str(old_tp1)
        out["range_risk_dist"] = live_range_decimal_str(risk_dist)
        out["range_reward_dist"] = live_range_decimal_str(reward_dist)
        out["range_rebase_sl"] = live_range_decimal_str(rebase_sl)
        out["range_rebase_tp1"] = live_range_decimal_str(rebase_tp1)
        out["range_final_sl"] = live_range_decimal_str(final_sl)
        out["range_final_tp1"] = live_range_decimal_str(final_tp1)

        out["sl"] = live_range_decimal_str(final_sl)
        out["tp1"] = live_range_decimal_str(final_tp1)
        out["tp2_qty"] = "0"
        out["tp3_qty"] = "0"
        out["tp_split_mode"] = "RANGE_REBASE_FULL_TP1"

        return out

    except Exception as e:
        out["range_plan_error"] = f"{type(e).__name__}:{e}"
        return out




def live_entry_plain_decimal(x) -> str:
    from decimal import Decimal
    out = format(Decimal(str(x)).normalize(), "f")
    if "." in out:
        out = out.rstrip("0").rstrip(".")
    return out or "0"


def live_entry_round_to_step(value, step, mode="down") -> str:
    from decimal import Decimal, ROUND_DOWN, ROUND_UP
    v = Decimal(str(value))
    st = Decimal(str(step))
    if st <= 0:
        return live_entry_plain_decimal(v)
    rounding = ROUND_UP if str(mode).lower() == "up" else ROUND_DOWN
    n = (v / st).to_integral_value(rounding=rounding)
    return live_entry_plain_decimal(n * st)


def live_entry_symbol_filters(symbol: str) -> dict:
    import json as _json
    import urllib.request as _urlreq

    symbol = str(symbol or "").upper().strip()
    cache = getattr(live_entry_symbol_filters, "_cache", {})
    if symbol in cache:
        return cache[symbol]

    out = {
        "tickSize": "0.0001",
        "stepSize": "0.1",
        "minQty": "0",
        "minNotional": "0",
    }

    try:
        url = live_fapi_base_url().rstrip("/") + "/fapi/v1/exchangeInfo"
        with _urlreq.urlopen(url, timeout=15) as r:
            data = _json.loads(r.read().decode("utf-8"))

        for item in data.get("symbols", []):
            if item.get("symbol") == symbol:
                fs = {f.get("filterType"): f for f in item.get("filters", [])}
                out = {
                    "tickSize": fs.get("PRICE_FILTER", {}).get("tickSize", "0.0001"),
                    "stepSize": fs.get("LOT_SIZE", {}).get("stepSize", "0.1"),
                    "minQty": fs.get("LOT_SIZE", {}).get("minQty", "0"),
                    "minNotional": fs.get("MIN_NOTIONAL", {}).get("notional", "0"),
                }
                break
    except Exception as e:
        out["filter_error"] = f"{type(e).__name__}:{e}"

    cache[symbol] = out
    setattr(live_entry_symbol_filters, "_cache", cache)
    return out


def live_plan_entry_price(plan: Dict[str, Any]) -> str:
    payload = plan.get("payload") if isinstance(plan.get("payload"), dict) else {}
    v = (
        plan.get("entry_mid")
        or plan.get("entry")
        or plan.get("entry_price")
        or payload.get("entry_mid")
        or payload.get("entry")
        or payload.get("entry_price")
    )
    return str(v or "")



# === ORDERBOOK_EXISTING_SHADOW_BRIDGE_20260620 ===
def live_read_latest_orderbook_shadow_guard(symbol: str) -> dict:
    """
    Read latest REPORT_ONLY orderbook shadow guard result for a symbol.
    Source: logs/orderbook_shadow_guard_snapshots_v1.jsonl
    This does not fetch Binance. It bridges existing orderbook chain into live execution.
    """
    import json
    from datetime import datetime, timedelta

    symbol = str(symbol or "").upper().replace("BINANCE:", "").replace(".P", "").strip()
    path = LOG_DIR / "orderbook_shadow_guard_snapshots_v1.jsonl"

    if not path.exists():
        return {
            "ok": False,
            "reason": "orderbook_shadow_guard_log_missing",
            "symbol": symbol,
            "path": str(path),
        }

    latest = None
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                if str(row.get("symbol") or "").upper().strip() == symbol:
                    latest = row

        if not latest:
            return {
                "ok": False,
                "reason": "orderbook_shadow_guard_symbol_missing",
                "symbol": symbol,
                "path": str(path),
            }

        created_at_wib = str(latest.get("created_at_wib") or "").replace(" WIB", "").strip()
        age_min = None
        if created_at_wib:
            try:
                ts = datetime.strptime(created_at_wib, "%Y-%m-%d %H:%M:%S")
                now_wib = datetime.utcnow() + timedelta(hours=7)
                age_min = (now_wib - ts).total_seconds() / 60.0
            except Exception:
                age_min = None

        max_age_min = env_float("ORDERBOOK_BRIDGE_MAX_AGE_MIN", 30)
        stale = bool(age_min is not None and age_min > max_age_min)

        return {
            "ok": not stale,
            "reason": "orderbook_shadow_guard_stale" if stale else "orderbook_shadow_guard_ok",
            "symbol": symbol,
            "age_min": age_min,
            "max_age_min": max_age_min,
            "row": latest,
            "path": str(path),
        }
    except Exception as e:
        return {
            "ok": False,
            "reason": "orderbook_shadow_guard_read_exception",
            "error": f"{type(e).__name__}: {e}",
            "symbol": symbol,
            "path": str(path),
        }


def live_orderbook_existing_bridge_guard(symbol: str, plan: Dict[str, Any]) -> dict:
    """
    Bridge existing orderbook shadow guard into live execution.
    Default is SHADOW, so it logs would-block but does not block order.
    LIVE_BLOCK can be enabled later after validation.
    """
    if not env_bool("ORDERBOOK_BRIDGE_ENABLED", True):
        return {
            "ok": True,
            "enabled": False,
            "mode": "DISABLED",
            "decision": "ORDERBOOK_BRIDGE_DISABLED",
            "reason": "orderbook_bridge_disabled",
        }

    mode = str(os.getenv("ORDERBOOK_BRIDGE_MODE", "SHADOW")).strip().upper()
    res = live_read_latest_orderbook_shadow_guard(symbol)

    row = res.get("row") or {}
    guard_state = str(row.get("guard_state") or "UNKNOWN").upper()
    severity = str(row.get("severity") or "UNKNOWN").upper()
    shadow_action = str(row.get("shadow_action") or "UNKNOWN").upper()
    would_block = bool(row.get("would_block_if_live")) or guard_state == "WOULD_BLOCK_IF_LIVE" or "BLOCK" in severity

    if not bool(res.get("ok")):
        decision = "ORDERBOOK_BRIDGE_REJECT" if mode == "LIVE_BLOCK" else "ORDERBOOK_BRIDGE_SHADOW_DATA_BAD"
        return {
            "ok": mode != "LIVE_BLOCK",
            "enabled": True,
            "mode": mode,
            "decision": decision,
            "reason": res.get("reason"),
            "symbol": symbol,
            "source_ok": False,
            "source_age_min": res.get("age_min"),
            "guard_state": guard_state,
            "severity": severity,
            "shadow_action": shadow_action,
            "would_block_if_live": would_block,
            "row": row,
        }

    if would_block and mode == "LIVE_BLOCK":
        decision = "ORDERBOOK_REJECT"
        ok = False
    elif would_block:
        decision = "ORDERBOOK_SHADOW_WOULD_BLOCK"
        ok = True
    else:
        decision = "ORDERBOOK_ALLOW"
        ok = True

    return {
        "ok": ok,
        "enabled": True,
        "mode": mode,
        "decision": decision,
        "reason": "|".join(row.get("hard_reasons") or row.get("soft_reasons") or []) or row.get("shadow_action") or "orderbook_bridge_ok",
        "symbol": symbol,
        "source_ok": True,
        "source_age_min": res.get("age_min"),
        "guard_state": guard_state,
        "severity": severity,
        "shadow_action": shadow_action,
        "would_block_if_live": would_block,
        "grade24": row.get("grade24"),
        "grade7": row.get("grade7"),
        "latest_pricing_status": row.get("latest_pricing_status"),
        "ok_share24": row.get("ok_share24"),
        "sample24": row.get("sample24"),
        "spread_bps_p95": row.get("spread_bps_p95"),
        "worst_slip_50_p95": row.get("worst_slip_50_p95"),
        "min_depth10_p10": row.get("min_depth10_p10"),
        "latest_spread_bps": row.get("latest_spread_bps"),
        "latest_depth10_bid_usdt": row.get("latest_depth10_bid_usdt"),
        "latest_depth10_ask_usdt": row.get("latest_depth10_ask_usdt"),
        "hard_reasons": row.get("hard_reasons"),
        "soft_reasons": row.get("soft_reasons"),
        "created_at_wib": row.get("created_at_wib"),
    }


def live_build_entry_order_params(symbol: str, entry_side: str, qty: str, signal_key: str, plan: Dict[str, Any]) -> dict:
    """
    Entry order builder.

    MARKET:
      current old behavior.

    LIMIT_IOC:
      pure signal plan entry, no pending order numpuk.
      BUY  price rounded DOWN to tick.
      SELL price rounded UP to tick.
      If not immediately fill, Binance cancels it.
    """
    symbol = str(symbol or "").upper().strip()
    side = str(entry_side or "").upper().strip()
    mode = str(os.getenv("LIVE_ENTRY_ORDER_TYPE", "MARKET")).strip().upper()

    filters = live_entry_symbol_filters(symbol)
    step = filters.get("stepSize", "0.1")
    tick = filters.get("tickSize", "0.0001")

    qty2 = live_entry_round_to_step(qty, step, "down")

    if mode in ("LIMIT_IOC", "IOC_LIMIT", "LIMIT_TTL", "LIMIT_GTC", "GTC_LIMIT"):
        raw_price = live_plan_entry_price(plan)
        if not raw_price:
            return {
                "ok": False,
                "reason": "missing_plan_entry_for_limit_ioc",
                "params": None,
                "meta": {"mode": mode, "filters": filters},
            }

        # Pure plan, tapi rounding dibuat tidak lebih buruk.
        # LONG BUY: round down. SHORT SELL: round up.
        price_mode = "down" if side == "BUY" else "up"
        price = live_entry_round_to_step(raw_price, tick, price_mode)

        tif = "IOC" if mode in ("LIMIT_IOC", "IOC_LIMIT") else "GTC"
        entry_mode_label = "LIMIT_IOC" if tif == "IOC" else "LIMIT_TTL"

        params = {
            "symbol": symbol,
            "side": side,
            "type": "LIMIT",
            "timeInForce": tif,
            "quantity": qty2,
            "price": price,
            "newClientOrderId": _live_client_order_id("SMC", signal_key, "ENTRY"),
        }

        return {
            "ok": True,
            "reason": None,
            "params": params,
            "meta": {
                "mode": entry_mode_label,
                "timeInForce": tif,
                "ttl_sec": str(os.getenv("LIVE_ENTRY_TTL_SEC", "5400")),
                "raw_plan_entry": str(raw_price),
                "rounded_price": price,
                "price_round_mode": price_mode,
                "raw_qty": str(qty),
                "rounded_qty": qty2,
                "filters": filters,
                "market_fallback": str(os.getenv("LIVE_MARKET_FALLBACK", "false")).lower(),
            },
        }

    params = {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quantity": qty2,
        "newClientOrderId": _live_client_order_id("SMC", signal_key, "ENTRY"),
    }

    return {
        "ok": True,
        "reason": None,
        "params": params,
        "meta": {
            "mode": "MARKET",
            "raw_qty": str(qty),
            "rounded_qty": qty2,
            "filters": filters,
        },
    }


def handle_live_small_capital_execution(p: Dict[str, Any], safety: Dict[str, Any] | None = None) -> Dict[str, Any]:
    signal_key = signal_key_of(p)
    plan = build_execution_plan(p)
    plan["execution_mode"] = "LIVE_SMALL_CAPITAL"
    plan["binance_env"] = "LIVE"

    ok, reason = shared_validate_plan_cost_gate(plan, require_quantity=True)

    base_event = {
        "event_at_utc": utc_now_iso(),
        "event_at_wib": wib_now_iso(),
        "app_version": APP_VERSION,
        "execution_mode": execution_mode(),
        "binance_env": binance_env(),
        "signal_key": signal_key,
        "pair": pair_of(p),
        "symbol": plan.get("symbol"),
        "source": p.get("source"),
        "signal_source": p.get("signal_source"),
        "execution_owner": p.get("execution_owner"),
        "plan": plan,
        "safety_summary": safety,
    }

    append_jsonl(EXECUTION_PLANS_LOG, plan)

    if not ok:
        event = dict(base_event)
        event.update({
            "action": "LIVE_SMALL_CAPITAL_EXECUTE",
            "decision": "REJECT",
            "reason": reason,
            "gate": "live_plan_cost_gate",
        })
        append_jsonl(EXECUTION_EVENTS_LOG, event)
        return {"ok": False, "decision": "REJECT", "reason": reason, "gate": "live_plan_cost_gate", "plan": plan}

    if binance_env() != "LIVE" or execution_mode() != "LIVE_SMALL_CAPITAL":
        event = dict(base_event)
        event.update({
            "action": "LIVE_SMALL_CAPITAL_EXECUTE",
            "decision": "REJECT",
            "reason": "live_env_or_execution_mode_mismatch",
            "gate": "live_env_gate",
        })
        append_jsonl(EXECUTION_EVENTS_LOG, event)
        return {"ok": False, "decision": "REJECT", "reason": "live_env_or_execution_mode_mismatch", "gate": "live_env_gate", "plan": plan}

    if env_bool("KILL_SWITCH", False) or env_bool("EMERGENCY_CLOSE_ENABLED", False):
        event = dict(base_event)
        event.update({
            "action": "LIVE_SMALL_CAPITAL_EXECUTE",
            "decision": "REJECT",
            "reason": "kill_switch_or_emergency_close_active",
            "gate": "live_kill_switch_gate",
        })
        append_jsonl(EXECUTION_EVENTS_LOG, event)
        return {"ok": False, "decision": "REJECT", "reason": "kill_switch_or_emergency_close_active", "gate": "live_kill_switch_gate", "plan": plan}

    symbol = str(plan.get("symbol") or "").upper()
    entry_side = str(plan.get("entry_side") or "").upper()
    exit_side = str(plan.get("exit_side") or "").upper()
    qty = str(plan.get("quantity") or "").strip()

    orderbook_bridge = live_orderbook_existing_bridge_guard(symbol, plan)

    orderbook_bridge_fields = {
        "orderbook_bridge_enabled": orderbook_bridge.get("enabled"),
        "orderbook_bridge_mode": orderbook_bridge.get("mode"),
        "orderbook_bridge_decision": orderbook_bridge.get("decision"),
        "orderbook_bridge_reason": orderbook_bridge.get("reason"),
        "orderbook_guard_state": orderbook_bridge.get("guard_state"),
        "orderbook_severity": orderbook_bridge.get("severity"),
        "orderbook_shadow_action": orderbook_bridge.get("shadow_action"),
        "orderbook_would_block_if_live": orderbook_bridge.get("would_block_if_live"),
        "orderbook_grade24": orderbook_bridge.get("grade24"),
        "orderbook_grade7": orderbook_bridge.get("grade7"),
        "orderbook_ok_share24": orderbook_bridge.get("ok_share24"),
        "orderbook_sample24": orderbook_bridge.get("sample24"),
        "orderbook_spread_bps_p95": orderbook_bridge.get("spread_bps_p95"),
        "orderbook_worst_slip_50_p95": orderbook_bridge.get("worst_slip_50_p95"),
        "orderbook_min_depth10_p10": orderbook_bridge.get("min_depth10_p10"),
        "orderbook_latest_spread_bps": orderbook_bridge.get("latest_spread_bps"),
        "orderbook_latest_depth10_bid_usdt": orderbook_bridge.get("latest_depth10_bid_usdt"),
        "orderbook_latest_depth10_ask_usdt": orderbook_bridge.get("latest_depth10_ask_usdt"),
        "orderbook_source_age_min": orderbook_bridge.get("source_age_min"),
        "orderbook_source_created_at_wib": orderbook_bridge.get("created_at_wib"),
    }

    plan.update(orderbook_bridge_fields)
    base_event.update(orderbook_bridge_fields)

    if not bool(orderbook_bridge.get("ok")):
        event = dict(base_event)
        event.update({
            "action": "LIVE_SMALL_CAPITAL_ORDERBOOK_BRIDGE",
            "decision": "REJECT",
            "reason": orderbook_bridge.get("reason") or "orderbook_bridge_reject",
            "gate": "orderbook_bridge_gate",
            "plan": plan,
            "orderbook_bridge": orderbook_bridge,
        })
        append_jsonl(EXECUTION_EVENTS_LOG, event)
        return {
            "ok": False,
            "decision": "REJECT",
            "reason": orderbook_bridge.get("reason") or "orderbook_bridge_reject",
            "gate": "orderbook_bridge_gate",
            "plan": plan,
            "orderbook_bridge": orderbook_bridge,
        }

    setup_res = live_try_set_margin_and_leverage(plan)

    entry_build = live_build_entry_order_params(symbol, entry_side, qty, signal_key, plan)
    if not bool(entry_build.get("ok")):
        event = dict(base_event)
        event.update({
            "action": "LIVE_SMALL_CAPITAL_ENTRY",
            "decision": "LIVE_ENTRY_BUILD_FAILED",
            "reason": entry_build.get("reason"),
            "setup_result": setup_res,
            "entry_build": entry_build,
        })
        append_jsonl(EXECUTION_EVENTS_LOG, event)
        return {"ok": False, "decision": "LIVE_ENTRY_BUILD_FAILED", "reason": entry_build.get("reason"), "plan": plan, "entry_build": entry_build}

    entry_params = entry_build.get("params")
    plan["entry_order_type"] = (entry_build.get("meta") or {}).get("mode")
    plan["entry_order_meta"] = entry_build.get("meta")

    entry_res = live_place_order(entry_params)

    if not bool(entry_res.get("ok")):
        event = dict(base_event)
        event.update({
            "action": "LIVE_SMALL_CAPITAL_ENTRY",
            "decision": "LIVE_ENTRY_FAILED",
            "reason": entry_res.get("reason"),
            "setup_result": setup_res,
            "entry_params": entry_params,
            "entry_result": entry_res,
        })
        append_jsonl(EXECUTION_EVENTS_LOG, event)
        return {"ok": False, "decision": "LIVE_ENTRY_FAILED", "reason": entry_res.get("reason"), "plan": plan, "entry_result": entry_res}

    # Jangan pasang TP/SL sebelum MARKET entry bener-bener kebaca sebagai posisi live.
    ensure_tp_split(plan)

    entry_fill_res = live_wait_position_after_entry(symbol, direction_of(p), signal_key)
    if not bool(entry_fill_res.get("ok")):
        event = dict(base_event)
        event.update({
            "action": "LIVE_SMALL_CAPITAL_EXECUTE",
            "decision": "LIVE_LIMIT_IOC_NOT_FILLED_OR_POSITION_NOT_CONFIRMED",
            "reason": entry_fill_res.get("reason"),
            "setup_result": setup_res,
            "entry_params": entry_params,
            "entry_result": entry_res,
            "entry_fill_result": entry_fill_res,
            "protection_ok": False,
            "protective_results": [],
        })
        append_jsonl(EXECUTION_EVENTS_LOG, event)
        return {
            "ok": False,
            "decision": "LIVE_LIMIT_IOC_NOT_FILLED_OR_POSITION_NOT_CONFIRMED",
            "reason": entry_fill_res.get("reason"),
            "plan": plan,
            "entry_result": entry_res,
            "entry_fill_result": entry_fill_res,
        }

    plan = live_apply_actual_position_to_plan(plan, entry_fill_res)
    plan = live_apply_range_plan_rebase(plan, entry_fill_res)
    plan = live_apply_bad_fill_tight_sl(plan, entry_fill_res)
    qty = str(plan.get("quantity") or qty)

    protective_results = []

    sl_params = {
        "symbol": symbol,
        "side": exit_side,
        "type": "STOP_MARKET",
        "stopPrice": str(plan.get("sl")),
        "quantity": qty,
        "reduceOnly": "true",
        "workingType": "CONTRACT_PRICE",
        "newClientOrderId": _live_client_order_id("SMC", signal_key, "SL"),
    }
    sl_res = live_place_algo_order(sl_params)
    protective_results.append({"label": "SL", "params": sl_params, "result": sl_res})

    for label, tp_key, qty_key in [
        ("TP1", "tp1", "tp1_qty"),
        ("TP2", "tp2", "tp2_qty"),
        ("TP3", "tp3", "tp3_qty"),
    ]:
        tp_qty = plan.get(qty_key)
        try:
            tp_qty_f = float(tp_qty or 0)
        except Exception:
            tp_qty_f = 0.0
        if tp_qty_f <= 0:
            continue

        tp_params = {
            "symbol": symbol,
            "side": exit_side,
            "type": "TAKE_PROFIT_MARKET",
            "stopPrice": str(plan.get(tp_key)),
            "quantity": str(tp_qty),
            "reduceOnly": "true",
            "workingType": "CONTRACT_PRICE",
            "newClientOrderId": _live_client_order_id("SMC", signal_key, label),
        }
        tp_res = live_place_algo_order(tp_params)
        protective_results.append({"label": label, "params": tp_params, "result": tp_res})

    protection_ok = all(bool(x.get("result", {}).get("ok")) for x in protective_results)

    final_decision = "LIVE_ORDER_PLACED" if protection_ok else "LIVE_ENTRY_PLACED_PROTECTION_PARTIAL_OR_FAILED"

    event = dict(base_event)
    event.update({
        "action": "LIVE_SMALL_CAPITAL_EXECUTE",
        "decision": final_decision,
        "reason": "entry_and_protection_processed",
        "setup_result": setup_res,
        "entry_params": entry_params,
        "entry_result": entry_res,
        "entry_fill_result": entry_fill_res,
        "plan": plan,
        "protective_results": protective_results,
        "protection_ok": protection_ok,
    })
    append_jsonl(EXECUTION_EVENTS_LOG, event)

    v014_execution_summary_write(signal_key, {
        "pair": pair_of(p),
        "symbol": symbol,
        "direction": direction_of(p),
        "lifecycle_state": final_decision,
        "entry_order_id": (entry_res.get("body") or {}).get("orderId") if isinstance(entry_res.get("body"), dict) else None,
        "notes": "live_small_capital_execute_on_confirmed",
    })

    return {
        "ok": protection_ok,
        "decision": final_decision,
        "reason": "entry_and_protection_processed",
        "plan": plan,
        "setup_result": setup_res,
        "entry_result": entry_res,
        "entry_fill_result": entry_fill_res,
        "protective_results": protective_results,
    }


def send_telegram_message(text: str) -> Dict[str, Any]:
    if not env_bool("TELEGRAM_ENABLED", False):
        return {"ok": True, "sent": False, "skipped": True, "reason": "telegram_disabled"}
    if not env_bool("TELEGRAM_REPORTING_ENABLED", True):
        return {"ok": True, "sent": False, "skipped": True, "reason": "telegram_reporting_disabled"}

    token = str(os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    chat_id = str(os.getenv("TELEGRAM_CHAT_ID") or "").strip()
    if not token or not chat_id:
        return {"ok": False, "sent": False, "skipped": True, "reason": "telegram_credentials_missing"}

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = urllib.parse.urlencode({"chat_id": chat_id, "text": text})
    req = urllib.request.Request(url, data=payload.encode("utf-8"), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=2.5) as resp:
            ok = 200 <= int(resp.getcode() or 0) < 300
            return {"ok": ok, "sent": ok, "skipped": False, "reason": "sent" if ok else "telegram_http_non_2xx"}
    except Exception as e:
        print(f"[telegram] send failed: {e}")
        return {"ok": False, "sent": False, "skipped": False, "reason": "telegram_send_failed", "error": str(e)}


def fire_and_forget_telegram(text: str) -> None:
    def _run() -> None:
        try:
            send_telegram_message(text)
        except Exception as e:
            print(f"[telegram] unexpected failure: {e}")

    Thread(target=_run, daemon=True).start()


def notify_signal_decision_async(p: Dict[str, Any], decision: Dict[str, Any]) -> None:
    try:
        msg = format_signal_decision_message(p, decision)
    except Exception as e:
        print(f"[telegram] decision message format failed: {e}")
        return
    try:
        fire_and_forget_telegram(msg)
    except Exception as e:
        print(f"[telegram] decision async notify failed: {e}")


def market_ws_run_forever(symbols: List[str], intervals: List[str]) -> None:
    global MARKET_WS_CONNECTED, MARKET_LAST_ERROR
    if WebSocketApp is None:
        MARKET_WS_CONNECTED = False
        MARKET_LAST_ERROR = "websocket_client_missing"
        print("[market_data] websocket-client missing; websocket collector disabled")
        return
    streams = "/".join([f"{s.lower()}@kline_{i}" for s in symbols for i in intervals])
    ws_url = f"wss://fstream.binance.com/stream?streams={streams}"
    retention_days = env_int("BINANCE_CANDLE_RETENTION_DAYS", 7)

    def on_message(ws: WebSocketApp, message: str) -> None:
        global MARKET_LAST_ERROR
        try:
            payload = json.loads(message)
            data = payload.get("data") if isinstance(payload, dict) else None
            kline = data.get("k") if isinstance(data, dict) else None
            if not isinstance(kline, dict) or not bool(kline.get("x")):
                return
            symbol = str(kline.get("s") or "").upper()
            interval = str(kline.get("i") or "")
            row = market_build_row_from_kline(symbol, interval, kline)
            if row:
                market_upsert_candles([row], retention_days)
        except Exception as e:
            MARKET_LAST_ERROR = str(e)
            print(f"[market_data] ws message error: {e}")

    def on_error(ws: WebSocketApp, error: Any) -> None:
        global MARKET_WS_CONNECTED, MARKET_LAST_ERROR
        MARKET_WS_CONNECTED = False
        MARKET_LAST_ERROR = str(error)
        print(f"[market_data] ws error: {error}")

    def on_close(ws: WebSocketApp, status_code: Any, msg: Any) -> None:
        global MARKET_WS_CONNECTED
        MARKET_WS_CONNECTED = False
        print(f"[market_data] ws closed: {status_code} {msg}")

    while True:
        try:
            ws = WebSocketApp(ws_url, on_message=on_message, on_error=on_error, on_close=on_close)
            MARKET_WS_CONNECTED = True
            ws.run_forever(ping_interval=15, ping_timeout=10)
        except Exception as e:
            MARKET_WS_CONNECTED = False
            MARKET_LAST_ERROR = str(e)
            print(f"[market_data] ws crash: {e}")
        if not env_bool("BINANCE_CANDLE_RECONNECT_ENABLED", True):
            break
        time.sleep(3)


def market_data_start_background() -> Dict[str, Any]:
    global MARKET_DATA_THREAD
    if not market_enabled():
        return {"ok": True, "started": False, "reason": "collector_disabled"}
    if MARKET_DATA_THREAD and MARKET_DATA_THREAD.is_alive():
        return {"ok": True, "started": False, "reason": "already_running"}
    symbols = market_allowlist_symbols()
    intervals = market_intervals()
    MARKET_DATA_THREAD = Thread(target=market_collector_main, args=(symbols, intervals), daemon=True)
    MARKET_DATA_THREAD.start()
    return {"ok": True, "started": True, "symbols": symbols, "intervals": intervals}


def market_collector_main(symbols: List[str], intervals: List[str]) -> None:
    global MARKET_BOOTSTRAP_DONE, MARKET_LAST_ERROR
    try:
        if env_bool("BINANCE_CANDLE_REST_BOOTSTRAP_ENABLED", True):
            limit = env_int("BINANCE_CANDLE_BOOTSTRAP_LIMIT", 300)
            market_rest_bootstrap(symbols, intervals, limit=limit)
        MARKET_BOOTSTRAP_DONE = True
    except Exception as e:
        MARKET_LAST_ERROR = str(e)
        print(f"[market_data] bootstrap error (non-fatal): {e}")
    if str(os.getenv("BINANCE_CANDLE_SOURCE") or "WEBSOCKET").upper() == "WEBSOCKET":
        market_ws_run_forever(symbols, intervals)


def operator_status_payload(symbol: str = "") -> Dict[str, Any]:
    safety = v014_safety_summary(symbol)
    scheduler_status = vps_smc.scheduler_status()
    verdict = "GO" if bool(safety.get("safe_to_continue")) and execution_mode() == "LIVE_SMALL_CAPITAL" else "NO_GO"
    return {
        "ok": safety.get("ok"),
        "mode": get_mode(),
        "execution_mode": safety.get("execution_mode"),
        "binance_env": safety.get("binance_env"),
        "signal_source_mode": signal_source_mode(),
        "apps_script_signal_mode": apps_script_signal_mode(),
        "stat_tech_live_enabled": env_bool("STAT_TECH_LIVE_ENABLED", False),
        "live_trading_enabled": env_bool("LIVE_TRADING_ENABLED", False),
        "live_go_confirm": env_bool("LIVE_GO_CONFIRM", False),
        "safe_to_continue": safety.get("safe_to_continue"),
        "mismatch_state": safety.get("mismatch_state"),
        "open_paper_positions": safety.get("open_paper_positions"),
        "symbol": safety.get("symbol"),
        "positionAmt": safety.get("positionAmt"),
        "open_algo_count": safety.get("open_algo_count"),
        "scheduler_status": scheduler_status,
        "candle_websocket_status": {"connected": MARKET_WS_CONNECTED, "bootstrap_done": MARKET_BOOTSTRAP_DONE, "last_error": MARKET_LAST_ERROR},
        "last_removed_smc_error": scheduler_status.get("last_error"),
        "final_verdict": verdict,
        "reasons": safety.get("reasons") or [],
        "timestamp_utc": safety.get("timestamp_utc"),
    }


def format_safety_summary_message(s: Dict[str, Any]) -> str:
    reasons = s.get("reasons") or []
    return "\n".join([
        "🛡️ SAFETY SUMMARY",
        f"Mode: {get_mode()}",
        f"Execution: {s.get('execution_mode')}",
        f"Env: {s.get('binance_env')}",
        f"Safe: {str(bool(s.get('safe_to_continue'))).lower()}",
        f"Mismatch: {s.get('mismatch_state')}",
        f"Paper Open: {s.get('open_paper_positions')}",
        f"Symbol: {s.get('symbol') or '-'}",
        f"PositionAmt: {s.get('positionAmt')}",
        f"Open Algo: {s.get('open_algo_count')}",
        f"Reasons: {'; '.join(reasons) if reasons else '-'}",
    ])


def decision_do_not_queue(decision: Dict[str, Any]) -> bool:
    if str(decision.get("decision") or "").upper() != "REJECT":
        return False
    text = f"{decision.get('reason','')}|{decision.get('gate','')}".lower()
    keys = ["max_open", "stale", "duplicate", "unsafe", "cooldown", "daily", "stop"]
    return any(k in text for k in keys)


def format_signal_decision_message(p: Dict[str, Any], decision: Dict[str, Any]) -> str:
    score = p.get("score")
    priority = p.get("priority")
    if str(p.get("signal_source") or "").upper() == "VPS_SMC":
        return "\n".join([
            "📡 VPS SMC REALTIME SIGNAL",
            "Source: VPS_BINANCE_REALTIME",
            f"Execution Owner: {p.get('execution_owner') or '-'}",
            f"Mode: {execution_mode()}",
            f"Decision: {decision.get('decision')}",
            f"Gate: {decision.get('gate')}",
            f"Reason: {decision.get('reason')}",
            f"Pair: {pair_of(p) or '-'}",
            f"Dir: {direction_of(p) or '-'}",
            f"Entry: {p.get('entry') or p.get('entry_mid') or '-'}",
            f"SL: {p.get('sl') or '-'}",
            f"TP1/TP2/TP3: {p.get('tp1') or '-'} / {p.get('tp2') or '-'} / {p.get('tp3') or '-'}",
        ])
    return "\n".join([
        "📡 SIGNAL DECISION",
        f"Pair: {pair_of(p) or '-'}",
        f"Symbol: {v010_normalize_symbol(p.get('symbol') or p.get('pair') or '') or '-'}",
        f"Dir: {direction_of(p) or '-'}",
        f"Status: {status_of(p) or '-'}",
        f"Score: {score if score is not None else 'pending'}",
        f"Priority: {priority if priority not in (None, '') else 'pending'}",
        "",
        f"Decision: {decision.get('decision')}",
        f"Reason: {decision.get('reason')}",
        f"Gate: {decision.get('gate')}",
        f"Do Not Queue: {str(decision_do_not_queue(decision)).lower()}",
        f"Execution: {execution_mode()}",
    ])
def normalize_pair(x: Any) -> str:
    return str(x or "").strip().upper()


def csv_set(name: str) -> set[str]:
    raw = os.getenv(name, "")
    return {
        normalize_pair(x)
        for x in raw.split(",")
        if normalize_pair(x)
    }


def get_mode() -> str:
    mode = (os.getenv("BOT_MODE") or os.getenv("MODE") or "RECEIVED_ONLY").strip().upper()
    allowed = (
        "RECEIVED_ONLY",
        "RECEIVER_ONLY",
        "PAPER",
        "TESTNET",
        "TESTNET_MARKET",
        "LIVE_SMALL_CAPITAL",
    )
    if mode not in allowed:
        return "RECEIVED_ONLY"
    if mode == "RECEIVER_ONLY":
        return "RECEIVED_ONLY"
    return mode


def signal_source_mode() -> str:
    return str(os.getenv("SIGNAL_SOURCE_MODE", "APPS_SCRIPT_ONLY")).strip().upper() or "APPS_SCRIPT_ONLY"


def apps_script_signal_mode() -> str:
    return str(os.getenv("APPS_SCRIPT_SIGNAL_MODE", "PRIMARY_EXECUTION")).strip().upper() or "PRIMARY_EXECUTION"

def canonical_pair_from_symbol(symbol: str) -> str:
    sym = v010_normalize_symbol(symbol)
    return f"BINANCE:{sym}.P" if sym else ""


def pair_allowlist_candidates(pair: str) -> list[str]:
    p = normalize_pair(pair)
    sym = v010_normalize_symbol(p)
    cands = []
    if p:
        cands.append(p)
    cp = canonical_pair_from_symbol(sym)
    if cp and cp not in cands:
        cands.append(cp)
    if sym and sym not in cands:
        cands.append(sym)
    return cands


def vps_smc_bridge_enabled_for_mode(mode: str) -> bool:
    mode = str(mode or "").upper()
    if mode in ("DISABLED", "PAPER", "TESTNET", "TESTNET_DRY_RUN", "TESTNET_ORDER_TEST", "TESTNET_MARKET", "LIVE_SMALL_CAPITAL"):
        return True
    return False


def verify_secret(x_signal_secret: Optional[str], x_webhook_secret: Optional[str]) -> None:
    expected = (os.getenv("WEBHOOK_SECRET") or "").strip()
    if not expected:
        return

    got = (x_signal_secret or x_webhook_secret or "").strip()
    if got != expected:
        raise HTTPException(status_code=401, detail="invalid webhook secret")


def payload_to_dict(payload: SignalPayload) -> Dict[str, Any]:
    return payload.model_dump(mode="json")


def signal_key_of(p: Dict[str, Any]) -> str:
    return str(
        p.get("signal_key")
        or p.get("signal_id")
        or ""
    ).strip()


def pair_of(p: Dict[str, Any]) -> str:
    return normalize_pair(p.get("pair") or p.get("symbol"))


def direction_of(p: Dict[str, Any]) -> str:
    raw_direction = str(p.get("direction") or p.get("dir") or "").strip().upper()
    if raw_direction in ("LONG", "BUY"):
        return "Long"
    if raw_direction in ("SHORT", "SELL"):
        return "Short"
    return ""


def status_of(p: Dict[str, Any]) -> str:
    return str(p.get("status") or p.get("state") or "").strip().upper()


def default_state() -> Dict[str, Any]:
    return {
        "seen_signal_keys": [],
        "accepted_by_pair": {},
        "open_paper_positions": [],
	"closed_paper_positions": [],
        "daily_stop_active": False,
	"daily_counters": {
	    "date_wib": "",
	    "accepted_count": 0,
	    "rejected_count": 0,
	    "accepted_by_pair": {},
	    "rejected_by_gate": {},
	},
        "updated_at_utc": utc_now_iso(),
    }


def load_state() -> Dict[str, Any]:
    ensure_dirs()
    if not PAPER_STATE_FILE.exists():
        return default_state()

    try:
        with PAPER_STATE_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return default_state()
        base = default_state()
        base.update(data)
        return base
    except Exception:
        return default_state()


def save_state(state: Dict[str, Any]) -> None:
    ensure_dirs()
    state["updated_at_utc"] = utc_now_iso()

    # avoid unlimited growth
    keys = state.get("seen_signal_keys") or []
    if len(keys) > 5000:
        state["seen_signal_keys"] = keys[-5000:]

    tmp = PAPER_STATE_FILE.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    tmp.replace(PAPER_STATE_FILE)


def parse_iso_utc(s: Any) -> Optional[datetime]:
    if not s:
        return None
    try:
        text = str(s).replace("Z", "+00:00")
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def parse_wib_time(s: str) -> Optional[datetime]:
    s = str(s or "").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=WIB)
        except Exception:
            pass
    return None


def parse_wib_flexible(s: Any) -> Optional[datetime]:
    raw = str(s or "").replace("WIB", "").strip()
    dt = parse_wib_time(raw)
    if dt:
        return dt
    try:
        x = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if x.tzinfo is None:
            x = x.replace(tzinfo=WIB)
        return x.astimezone(WIB)
    except Exception:
        return None


def http_get_json(url: str, timeout: float = 2.0) -> Any:
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def load_json_file(path: Path, default: Any) -> Any:
    try:
        if not path.exists():
            return default
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json_file(path: Path, data: Any) -> None:
    ensure_dirs()
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def read_context_cache() -> Dict[str, Any]:
    data = load_json_file(ML_CONTEXT_CACHE_FILE, {})
    return data if isinstance(data, dict) else {}


def write_context_cache(data: Dict[str, Any]) -> None:
    save_json_file(ML_CONTEXT_CACHE_FILE, data)


def cache_get(cache: Dict[str, Any], key: str, ttl_sec: int) -> Optional[Any]:
    item = cache.get(key)
    if not isinstance(item, dict):
        return None
    ts = parse_iso_utc(item.get("cached_at_utc"))
    if not ts:
        return None
    if (utc_now() - ts).total_seconds() > max(0, ttl_sec):
        return None
    return item.get("value")


def cache_set(cache: Dict[str, Any], key: str, value: Any) -> None:
    cache[key] = {"cached_at_utc": utc_now_iso(), "value": value}


def ml_enabled() -> bool:
    return env_bool("ML_DATA_COLLECTION_ENABLED", True)


def _extract_signal_key_bucket_ms(signal_key: Any) -> Optional[int]:
    parts = str(signal_key or "").strip().split("|")
    if len(parts) < 3:
        return None
    tail = str(parts[-1]).strip()
    if tail.isdigit():
        return int(tail)
    return None


def _is_explicit_validation_sample(row: Dict[str, Any]) -> bool:
    markers = ("SMOKE", "TEST", "VALIDATION", "MANUAL")
    signal_key = str(row.get("signal_key") or row.get("signal_id") or "").upper()
    source = str(row.get("source") or row.get("signal_source") or "").upper()
    mode = str(row.get("mode") or row.get("source_mode") or "").upper()
    if any(m in signal_key for m in markers):
        return True
    if any(m in source for m in markers):
        return True
    if "SMOKE_TEST" in mode:
        return True
    return False


def _is_production_apps_script_signal(row: Dict[str, Any]) -> bool:
    source = str(row.get("source") or row.get("signal_source") or "").strip().lower()
    engine = str(row.get("engine") or "").strip().upper()
    event_type = str(row.get("event_type") or "").strip().upper()
    symbol = v010_normalize_symbol(row.get("symbol") or row.get("pair") or "")
    signal_key = str(row.get("signal_key") or row.get("signal_id") or "")
    direction = str(row.get("direction") or row.get("dir") or "").strip().upper()
    entry = to_float_or_none(row.get("entry") or row.get("entry_mid") or row.get("entry_lo") or row.get("entry_price"))
    sl = to_float_or_none(row.get("sl"))
    tp1 = to_float_or_none(row.get("tp1"))
    key_head = signal_key.split("|")[0].upper() if signal_key else ""
    source_ok = source == "apps_script_inst" or engine == "INST"
    event_ok = event_type == "SIGNAL_CONFIRMED"
    symbol_ok = key_head.startswith("BINANCE:") and key_head.endswith("USDT.P")
    if not symbol_ok:
        symbol_ok = symbol.endswith("USDT")
    direction_ok = direction in ("LONG", "SHORT")
    plan_ok = entry is not None and sl is not None and tp1 is not None
    if source_ok and event_ok and symbol_ok and direction_ok and plan_ok:
        return True

    # Legacy dataset fallback for backfill rows missing source/engine/event_type.
    # Explicit validation markers are handled first in ml_classify_dataset_row().
    decision = str(row.get("execution_decision") or "").strip().upper()
    sample_type = str(row.get("sample_type") or "").strip().upper()
    decision_or_sample_ok = decision in ("ACCEPT", "REJECT") or sample_type in ("VALIDATION_SAMPLE", "FORWARD_SHADOW_PAPER")
    legacy_symbol_ok = key_head.startswith("BINANCE:") and key_head.endswith("USDT.P")
    legacy_symbol_ok = legacy_symbol_ok and symbol.endswith("USDT")
    return bool(legacy_symbol_ok and direction_ok and plan_ok and decision_or_sample_ok)


def ml_classify_dataset_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row or {})
    bucket_ms = _extract_signal_key_bucket_ms(out.get("signal_key") or out.get("signal_id"))
    if bucket_ms is not None:
        out["confirmed_bucket_ms"] = bucket_ms
        out["time_source"] = "signal_key_bucket_ms"
        if not out.get("signal_time_wib"):
            out["signal_time_wib"] = datetime.fromtimestamp(bucket_ms / 1000.0, timezone.utc).astimezone(WIB).isoformat()
    if _is_explicit_validation_sample(out):
        out["sample_type"] = "VALIDATION_SAMPLE"
        out["include_ml"] = False
        out["include_reason"] = None
        out["exclude_label_reason"] = "validation_sample"
        return out
    if _is_production_apps_script_signal(out):
        out["sample_type"] = "FORWARD_SHADOW_PAPER"
        out["include_ml"] = True
        out["include_reason"] = "production_signal"
        out["exclude_label_reason"] = None
        return out
    out["sample_type"] = str(out.get("sample_type") or "FORWARD_SHADOW_PAPER")
    out["include_ml"] = bool(out.get("include_ml", True))
    if out["include_ml"] and not out.get("include_reason"):
        out["include_reason"] = "forward_shadow_paper"
    if not out["include_ml"] and not out.get("exclude_label_reason"):
        out["exclude_label_reason"] = "include_ml_false"
    return out


def build_binance_context(symbol: str) -> Dict[str, Any]:
    base = os.getenv("BINANCE_MARKET_CONTEXT_BASE", "https://fapi.binance.com").rstrip("/")
    out: Dict[str, Any] = {"pair_derivatives_available": False, "btc_derivatives_available": False}
    try:
        btc_ticker = http_get_json(f"{base}/fapi/v1/ticker/24hr?symbol=BTCUSDT")
        btc_change = float(btc_ticker.get("priceChangePercent", 0.0))
        out["btc_change_24h_pct"] = btc_change
        out["btc_regime"] = "BULL" if btc_change > 1 else ("BEAR" if btc_change < -1 else "RANGE")
        out["btc_derivatives_available"] = True
    except Exception:
        out["btc_change_24h_pct"] = None
        out["btc_regime"] = "UNKNOWN"
    try:
        fr = http_get_json(f"{base}/fapi/v1/fundingRate?symbol={symbol}&limit=1")
        fval = float((fr or [{}])[-1].get("fundingRate", 0.0))
        out["funding_rate"] = fval
        out["funding_status"] = "EXTREME_POSITIVE" if fval >= 0.0008 else "EXTREME_NEGATIVE" if fval <= -0.0008 else "POSITIVE" if fval > 0 else "NEGATIVE" if fval < 0 else "NEUTRAL"
        out["pair_derivatives_available"] = True
    except Exception:
        out["funding_rate"] = None
        out["funding_status"] = "UNKNOWN"
    return out


def build_fred_context() -> Dict[str, Any]:
    api_key = str(os.getenv("FRED_API_KEY") or "").strip()
    if not api_key:
        return {"macro_regime": "UNKNOWN", "equity_risk": "UNKNOWN", "yield_pressure": "UNKNOWN", "usd_pressure": "UNKNOWN", "vol_status": "UNKNOWN", "fred_context_quality": "API_FAILED"}
    base = os.getenv("FRED_BASE", "https://api.stlouisfed.org/fred").rstrip("/")
    def last_two(series: str) -> tuple[Optional[float], Optional[float]]:
        data = http_get_json(f"{base}/series/observations?series_id={series}&api_key={api_key}&file_type=json&sort_order=desc&limit=6", timeout=3.0)
        vals = [float(x["value"]) for x in data.get("observations", []) if x.get("value") not in (".", None, "")]
        return (vals[0], vals[-1]) if vals else (None, None)
    try:
        us10, us10_old = last_two("DGS10")
        vix, _ = last_two("VIXCLS")
        spx, spx_old = last_two("SP500")
        dxy, dxy_old = last_two("DTWEXBGS")
        return {
            "us10y_latest": us10, "us10y_change_5d": (us10 - us10_old) if us10 is not None and us10_old is not None else None,
            "vix_latest": vix, "sp500_change_5d": (spx - spx_old) if spx is not None and spx_old is not None else None,
            "usd_index_latest": dxy, "usd_change_5d": (dxy - dxy_old) if dxy is not None and dxy_old is not None else None,
            "yield_pressure": "RISING" if (us10 is not None and us10_old is not None and us10 > us10_old) else "FALLING" if (us10 is not None and us10_old is not None and us10 < us10_old) else "NEUTRAL",
            "vol_status": "STRESS" if (vix or 0) >= 30 else "ELEVATED" if (vix or 0) >= 20 else "NORMAL" if (vix or 0) >= 14 else "CALM",
            "equity_risk": "RISK_ON" if (spx is not None and spx_old is not None and spx > spx_old) else "RISK_OFF" if (spx is not None and spx_old is not None and spx < spx_old) else "NEUTRAL",
            "usd_pressure": "USD_UP" if (dxy is not None and dxy_old is not None and dxy > dxy_old) else "USD_DOWN" if (dxy is not None and dxy_old is not None and dxy < dxy_old) else "NEUTRAL",
            "macro_regime": "MIXED", "fred_context_quality": "FULL",
        }
    except Exception:
        return {"macro_regime": "UNKNOWN", "equity_risk": "UNKNOWN", "yield_pressure": "UNKNOWN", "usd_pressure": "UNKNOWN", "vol_status": "UNKNOWN", "fred_context_quality": "API_FAILED"}



def is_calendar_blackout_active() -> tuple[bool, str]:
    if not env_bool("CALENDAR_GATE_ENABLED", False):
        return False, "calendar_gate_disabled"

    raw = os.getenv("CALENDAR_BLACKOUT_WIB", "").strip()
    if not raw:
        return False, "no_blackout_window_configured"

    now = datetime.now(WIB)

    # Format:
    # 2026-05-14 19:00..2026-05-14 21:00;2026-05-15 19:30..2026-05-15 20:30
    windows = [x.strip() for x in raw.split(";") if x.strip()]
    for w in windows:
        if ".." in w:
            a, b = w.split("..", 1)
        elif " - " in w:
            a, b = w.split(" - ", 1)
        else:
            continue

        start = parse_wib_time(a)
        end = parse_wib_time(b)

        if not start or not end:
            continue

        if start <= now <= end:
            return True, f"calendar_blackout_active:{start.isoformat()}..{end.isoformat()}"

    return False, "calendar_clear"


def build_macro_event_context(signal_time_wib: Any, events: Optional[List[Dict[str, Any]]] = None, quality: str = "API_FAILED") -> Dict[str, Any]:
    fallback = events if isinstance(events, list) else []
    now = parse_wib_flexible(signal_time_wib) or datetime.now(WIB)
    active_ids: List[str] = []
    in_blackout = False
    risk = "NONE"
    next_event = None
    for e in fallback if isinstance(fallback, list) else []:
        if str(e.get("status", "")).upper() != "ACTIVE":
            continue
        et = parse_wib_flexible(e.get("event_time_wib"))
        if not et:
            continue
        pre = int(e.get("blackout_before_min") or 0)
        post = int(e.get("blackout_after_min") or 0)
        a = et - timedelta(minutes=pre)
        b = et + timedelta(minutes=post)
        if a <= now <= b:
            in_blackout = True
            impact = str(e.get("impact", "UNKNOWN")).upper()
            risk = "HIGH" if impact == "HIGH" else "MEDIUM" if impact == "MEDIUM" else "LOW" if impact == "LOW" else "UNKNOWN"
            active_ids.append(str(e.get("event_id") or ""))
        if et >= now and (next_event is None or et < next_event[0]):
            next_event = (et, e)
    return {
        "is_blackout_active": in_blackout,
        "event_risk_level": risk if in_blackout else "NONE",
        "active_event_ids": active_ids,
        "next_event_id": (next_event[1].get("event_id") if next_event else None),
        "next_event_name": (next_event[1].get("event_name") if next_event else None),
        "next_event_impact": (next_event[1].get("impact") if next_event else None),
        "next_event_currency": (next_event[1].get("currency") if next_event else None),
        "minutes_to_next_high_impact": int((next_event[0] - now).total_seconds() / 60) if next_event and str(next_event[1].get("impact", "")).upper() == "HIGH" else None,
        "minutes_since_last_high_impact": None,
        "macro_event_context_quality": quality if fallback else "API_FAILED",
    }




def _norm_macro_header(h: Any) -> str:
    key = str(h or "").strip().upper()
    mapping = {
        "EVENT_ID": "event_id",
        "EVENT_NAME": "event_name",
        "CURRENCY": "currency",
        "IMPACT": "impact",
        "EVENT_TS_WIB": "event_time_wib",
        "PRE_BLACKOUT_MIN": "blackout_before_min",
        "POST_BLACKOUT_MIN": "blackout_after_min",
        "STATUS": "status",
        "NOTES": "notes",
    }
    return mapping.get(key, str(h or "").strip().lower())


def _normalize_macro_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row or {})
    st = str(out.get("status") or "").upper().strip()
    out["status"] = st
    imp = str(out.get("impact") or "UNKNOWN").upper().strip()
    out["impact"] = imp if imp in ("HIGH", "MEDIUM", "LOW") else "UNKNOWN"
    for k in ("blackout_before_min", "blackout_after_min"):
        try:
            out[k] = int(out.get(k) or 0)
        except Exception:
            out[k] = 0
    return out
def load_macro_events_from_gsheet_if_configured() -> tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
    source = str(os.getenv("MACRO_EVENT_SOURCE", "GSHEET")).upper().strip()
    if source != "GSHEET":
        return None, "gsheet_source_disabled"
    sheet_id = str(os.getenv("MACRO_EVENT_SPREADSHEET_ID") or os.getenv("INST_MACRO_EVENTS_SPREADSHEET_ID") or os.getenv("MACRO_EVENTS_SPREADSHEET_ID") or "").strip()
    sheet_name = str(os.getenv("MACRO_EVENT_SHEET_NAME") or os.getenv("INST_MACRO_EVENTS_SHEET_NAME") or "INST_MACRO_EVENTS").strip()
    sheet_range = f"{sheet_name}!A:Z"
    creds = str(os.getenv("MACRO_EVENT_SERVICE_ACCOUNT_FILE") or os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or os.getenv("GSHEET_CREDENTIALS_FILE") or "").strip()
    if not creds:
        return None, "gsheet_credentials_missing"
    if not sheet_id:
        return None, "gsheet_sheet_id_missing"
    try:
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
    except Exception:
        return None, "gsheet_provider_unavailable"
    try:
        c = Credentials.from_service_account_file(creds, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
        svc = build("sheets", "v4", credentials=c, cache_discovery=False)
        vals = svc.spreadsheets().values().get(spreadsheetId=sheet_id, range=sheet_range).execute().get("values", [])
        if not vals:
            return [], None
        headers = [_norm_macro_header(x) for x in vals[0]]
        rows: List[Dict[str, Any]] = []
        for r in vals[1:]:
            raw = {headers[i]: r[i] if i < len(r) else "" for i in range(len(headers))}
            row = _normalize_macro_row(raw)
            rows.append(row)
        return rows, None
    except Exception as e:
        return None, f"gsheet_read_failed:{e}"


def daily_stop_active(state: Dict[str, Any]) -> tuple[bool, str]:
    if env_bool("DAILY_STOP_ACTIVE", False):
        return True, "daily_stop_active_env"

    if bool(state.get("daily_stop_active")):
        return True, "daily_stop_active_state"

    return False, "daily_stop_clear"


def open_paper_count(state: Dict[str, Any]) -> int:
    positions = state.get("open_paper_positions") or []
    return sum(1 for p in positions if str(p.get("status", "OPEN")).upper() == "OPEN")


def cooldown_active(state: Dict[str, Any], pair: str) -> tuple[bool, str]:
    cd_min = env_int("COOLDOWN_PAIR_MIN", 90)
    accepted_by_pair = state.get("accepted_by_pair") or {}

    last_iso = accepted_by_pair.get(pair)
    last_dt = parse_iso_utc(last_iso)

    if not last_dt:
        return False, "cooldown_clear_no_prior_accept"

    elapsed = utc_now() - last_dt
    cd = timedelta(minutes=cd_min)

    if elapsed < cd:
        remain = cd - elapsed
        remain_min = max(1, int(remain.total_seconds() // 60))
        return True, f"cooldown_active_pair_{cd_min}m_remaining_{remain_min}m"

    return False, "cooldown_clear"


def make_reject(reason: str, gate: str) -> Dict[str, Any]:
    return {
        "decision": "REJECT",
        "reason": reason,
        "gate": gate,
    }

def current_wib_date() -> str:
    return datetime.now(WIB).strftime("%Y-%m-%d")


def ensure_daily_counters(state: Dict[str, Any]) -> Dict[str, Any]:
    today = current_wib_date()
    dc = state.get("daily_counters") or {}

    if dc.get("date_wib") != today:
        dc = {
            "date_wib": today,
            "accepted_count": 0,
            "rejected_count": 0,
            "accepted_by_pair": {},
            "rejected_by_gate": {},
        }

    state["daily_counters"] = dc
    return dc


def kill_switch_active() -> tuple[bool, str]:
    if env_bool("KILL_SWITCH", False):
        return True, "kill_switch_active"
    return False, "kill_switch_clear"


def daily_max_trades_active(state: Dict[str, Any]) -> tuple[bool, str]:
    dc = ensure_daily_counters(state)
    max_trades = env_int("MAX_TRADES_PER_DAY", 5)
    accepted = int(dc.get("accepted_count") or 0)

    if max_trades > 0 and accepted >= max_trades:
        return True, f"daily_max_trades_reached:{accepted}/{max_trades}"

    return False, "daily_max_trades_clear"


def increment_accept_counter(state: Dict[str, Any], pair: str) -> None:
    dc = ensure_daily_counters(state)

    dc["accepted_count"] = int(dc.get("accepted_count") or 0) + 1

    by_pair = dc.get("accepted_by_pair") or {}
    by_pair[pair] = int(by_pair.get(pair) or 0) + 1
    dc["accepted_by_pair"] = by_pair

    state["daily_counters"] = dc


def increment_reject_counter(state: Dict[str, Any], gate: str) -> None:
    dc = ensure_daily_counters(state)

    dc["rejected_count"] = int(dc.get("rejected_count") or 0) + 1

    by_gate = dc.get("rejected_by_gate") or {}
    gate_key = str(gate or "unknown_gate")
    by_gate[gate_key] = int(by_gate.get(gate_key) or 0) + 1
    dc["rejected_by_gate"] = by_gate

    state["daily_counters"] = dc


def paper_decide(p: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
    signal_key = signal_key_of(p)
    pair = pair_of(p)
    status = status_of(p)

    seen = set(state.get("seen_signal_keys") or [])

    # 1) duplicate signal_key
    if not signal_key:
        return make_reject("missing_signal_key", "duplicate_check")

    if signal_key in seen:
        return make_reject("duplicate_signal_key", "duplicate_check")

    # 2) status must be CONFIRMED
    if status != "CONFIRMED":
        return make_reject(f"status_not_confirmed:{status or 'EMPTY'}", "status_gate")

    # 3) allowlist
    allowlist_enabled = env_bool("PAIR_ALLOWLIST_ENABLED", True)
    allowlist = csv_set("PAIR_ALLOWLIST")
    if allowlist_enabled:
        if not allowlist:
            return make_reject("pair_allowlist_empty", "allowlist_gate")
        if not any(c in allowlist for c in pair_allowlist_candidates(pair)):
            return make_reject(f"pair_not_allowlisted:{pair}", "allowlist_gate")

    # 4A) kill switch
    ks_on, ks_reason = kill_switch_active()
    if ks_on:
        return make_reject(ks_reason, "kill_switch_gate")

    # 4B) daily max trades
    dm_on, dm_reason = daily_max_trades_active(state)
    if dm_on:
        return make_reject(dm_reason, "daily_max_trades_gate")

    # 4) cooldown pair
    cd_on, cd_reason = cooldown_active(state, pair)
    if cd_on:
        return make_reject(cd_reason, "cooldown_gate")

    # 5) max open paper position
    max_open = env_int("MAX_OPEN_PAPER_POSITIONS", 1)
    cur_open = open_paper_count(state)
    if cur_open >= max_open:
        return make_reject(f"max_open_paper_positions_reached:{cur_open}/{max_open}", "max_open_gate")

    # 6) calendar blackout
    cal_on, cal_reason = is_calendar_blackout_active()
    if cal_on:
        return make_reject(cal_reason, "calendar_gate")

    # 7) daily stop
    ds_on, ds_reason = daily_stop_active(state)
    if ds_on:
        return make_reject(ds_reason, "daily_stop_gate")

    # 8) accept
    return {
        "decision": "ACCEPT",
        "reason": "all_paper_gates_pass",
        "gate": "paper_gate",
    }


def apply_decision_to_state(p: Dict[str, Any], decision: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
    signal_key = signal_key_of(p)
    pair = pair_of(p)

    if signal_key:
        keys = state.get("seen_signal_keys") or []
        if signal_key not in keys:
            keys.append(signal_key)
        state["seen_signal_keys"] = keys

    if decision.get("decision") == "REJECT":
        increment_reject_counter(state, decision.get("gate") or "unknown_gate")

    if decision.get("decision") == "ACCEPT":
        now_iso = utc_now_iso()
        increment_accept_counter(state, pair)
        accepted_by_pair = state.get("accepted_by_pair") or {}
        accepted_by_pair[pair] = now_iso
        state["accepted_by_pair"] = accepted_by_pair

        entry_price = to_float_or_none(p.get("entry_mid") or p.get("entry_price"))
        plan_qty = to_float_or_none(p.get("quantity"))
        if plan_qty is None:
            plan = build_execution_plan(p)
            plan_qty = to_float_or_none(plan.get("quantity"))
        notional_usdt = paper_notional_usdt_default()
        qty = plan_qty
        if qty is None and entry_price is not None and entry_price > 0:
            qty = notional_usdt / entry_price

        positions = state.get("open_paper_positions") or []
        positions.append({
            "signal_key": signal_key,
            "pair": pair,
            "symbol": v010_normalize_symbol(p.get("symbol") or p.get("pair") or ""),
            "direction": direction_of(p),
            "status": "OPEN",
            "accepted_at_utc": now_iso,
            "entry_mid": p.get("entry_mid"),
            "entry_price": entry_price,
            "quantity": qty,
            "notional_usdt": notional_usdt,
            "sl": p.get("sl") or p.get("invalid"),
            "tp1": p.get("tp1"),
            "tp2": p.get("tp2"),
            "tp3": p.get("tp3"),
        })
        state["open_paper_positions"] = positions

    return state


def build_signal_log(p: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "received_at_utc": utc_now_iso(),
        "received_at_wib": wib_now_iso(),
        "app_version": APP_VERSION,
        "mode": get_mode(),
        "decision": "RECEIVED_ONLY" if get_mode() == "RECEIVED_ONLY" else "PAPER_EVAL",
        "reason": "raw_signal_logged",
        "signal_id": p.get("signal_id") or p.get("signal_key"),
        "signal_key": p.get("signal_key") or p.get("signal_id"),
        "pair": p.get("pair") or p.get("symbol"),
        "direction": p.get("direction") or p.get("dir"),
        "status": p.get("status") or p.get("state"),
        "source": p.get("source"),
        "signal_source": p.get("signal_source"),
        "source_mode": p.get("source_mode"),
        "execution_owner": p.get("execution_owner"),
        "plan_sanity_ok": p.get("plan_sanity_ok"),
        "plan_sanity_reason": p.get("plan_sanity_reason"),
        "plan_invalid": p.get("plan_invalid"),
        "raw_tp1": p.get("raw_tp1"),
        "raw_tp2": p.get("raw_tp2"),
        "raw_tp3": p.get("raw_tp3"),
        "tp_normalized": p.get("tp_normalized"),
        "tp_normalize_reason": p.get("tp_normalize_reason"),
        "payload": p,
    }

def binance_testnet_keys_present() -> tuple[bool, str]:
    api_key = str(os.getenv("BINANCE_TESTNET_API_KEY", "")).strip()
    api_secret = str(os.getenv("BINANCE_TESTNET_API_SECRET", "")).strip()

    if not api_key:
        return False, "missing_binance_testnet_api_key"
    if not api_secret:
        return False, "missing_binance_testnet_api_secret"

    return True, "binance_testnet_keys_present"


def binance_testnet_signed_request(method: str, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    if binance_env() != "TESTNET":
        return {"ok": False, "reason": "binance_env_not_testnet"}

    api_key = str(os.getenv("BINANCE_TESTNET_API_KEY", "")).strip()
    api_secret = str(os.getenv("BINANCE_TESTNET_API_SECRET", "")).strip()
    base_url = str(os.getenv("BINANCE_FUTURES_TESTNET_BASE_URL", "https://demo-fapi.binance.com")).strip()

    if not api_key:
        return {"ok": False, "reason": "missing_binance_testnet_api_key"}
    if not api_secret:
        return {"ok": False, "reason": "missing_binance_testnet_api_secret"}

    payload = dict(params or {})
    payload["timestamp"] = int(time.time() * 1000)
    payload["recvWindow"] = int(payload.get("recvWindow") or 5000)

    query = urllib.parse.urlencode(payload, doseq=True)
    signature = hmac.new(
        api_secret.encode("utf-8"),
        query.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    signed_query = query + "&signature=" + signature
    url = base_url.rstrip("/") + path

    headers = {
        "X-MBX-APIKEY": api_key,
        "Content-Type": "application/x-www-form-urlencoded",
    }

    try:
        if method.upper() == "POST":
            data = signed_query.encode("utf-8")
            req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        elif method.upper() == "GET":
            req = urllib.request.Request(url + "?" + signed_query, headers=headers, method="GET")
        else:
            return {"ok": False, "reason": f"unsupported_method:{method}"}

        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read().decode("utf-8")
            status = resp.status

        try:
            body = json.loads(raw) if raw else {}
        except Exception:
            body = {"raw": raw}

        return {
            "ok": 200 <= status < 300,
            "http_status": status,
            "body": body,
            "path": path,
            "method": method.upper(),
        }

    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8")
        try:
            body = json.loads(raw) if raw else {}
        except Exception:
            body = {"raw": raw}

        return {
            "ok": False,
            "http_status": e.code,
            "body": body,
            "path": path,
            "method": method.upper(),
        }

    except Exception as e:
        return {
            "ok": False,
            "reason": "binance_request_exception",
            "error": str(e),
            "path": path,
            "method": method.upper(),
        }


def decimal_plain(x: Decimal) -> str:
    s = format(x.normalize(), "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s if s else "0"


def decimal_value(v: Any, default: str = "0") -> Decimal:
    if v is None:
        return Decimal(default)
    return Decimal(str(v))


def floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def binance_testnet_public_get(path: str, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    if binance_env() != "TESTNET":
        return {"ok": False, "reason": "binance_env_not_testnet"}

    base_url = str(os.getenv("BINANCE_FUTURES_TESTNET_BASE_URL", "https://demo-fapi.binance.com")).strip()
    query = urllib.parse.urlencode(params or {}, doseq=True)
    url = base_url.rstrip("/") + path + (("?" + query) if query else "")

    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read().decode("utf-8")
            status = resp.status

        try:
            body = json.loads(raw) if raw else {}
        except Exception:
            body = {"raw": raw}

        return {
            "ok": 200 <= status < 300,
            "http_status": status,
            "body": body,
            "path": path,
            "method": "GET",
        }

    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8")
        try:
            body = json.loads(raw) if raw else {}
        except Exception:
            body = {"raw": raw}

        return {
            "ok": False,
            "http_status": e.code,
            "body": body,
            "path": path,
            "method": "GET",
        }

    except Exception as e:
        return {
            "ok": False,
            "reason": "binance_public_get_exception",
            "error": str(e),
            "path": path,
            "method": "GET",
        }


def exchange_info_cache_file() -> Path:
    return Path(os.getenv("EXCHANGE_INFO_CACHE_PATH", "state/exchange_info_testnet.json"))


def fetch_exchange_info(force: bool = False) -> Dict[str, Any]:
    cache_path = exchange_info_cache_file()

    if not force and cache_path.exists():
        try:
            cached = json.loads(cache_path.read_text())
            if cached.get("symbols"):
                return {"ok": True, "source": "cache", "body": cached}
        except Exception:
            pass

    res = binance_testnet_public_get("/fapi/v1/exchangeInfo")
    if not res.get("ok"):
        return res

    body = res.get("body") or {}
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(body, ensure_ascii=False, separators=(",", ":")))

    return {"ok": True, "source": "remote", "body": body}


def symbol_exchange_info(symbol: str, force: bool = False) -> Dict[str, Any]:
    symbol = str(symbol or "").strip().upper()
    info = fetch_exchange_info(force=force)

    if not info.get("ok"):
        return {"ok": False, "reason": "exchange_info_fetch_failed", "exchange_info_result": info}

    for s in (info.get("body") or {}).get("symbols", []):
        if str(s.get("symbol") or "").upper() == symbol:
            return {"ok": True, "source": info.get("source"), "symbol_info": s}

    return {"ok": False, "reason": f"symbol_not_found_in_exchange_info:{symbol}"}


def parse_symbol_filters(symbol_info: Dict[str, Any]) -> Dict[str, Any]:
    filters = {f.get("filterType"): f for f in symbol_info.get("filters", []) if f.get("filterType")}

    lot = filters.get("MARKET_LOT_SIZE") or filters.get("LOT_SIZE") or {}
    price_filter = filters.get("PRICE_FILTER") or {}
    min_notional_filter = filters.get("MIN_NOTIONAL") or filters.get("NOTIONAL") or {}

    return {
        "symbol": symbol_info.get("symbol"),
        "status": symbol_info.get("status"),
        "price_precision": symbol_info.get("pricePrecision"),
        "quantity_precision": symbol_info.get("quantityPrecision"),
        "base_asset": symbol_info.get("baseAsset"),
        "quote_asset": symbol_info.get("quoteAsset"),
        "tick_size": price_filter.get("tickSize"),
        "min_price": price_filter.get("minPrice"),
        "max_price": price_filter.get("maxPrice"),
        "step_size": lot.get("stepSize"),
        "min_qty": lot.get("minQty"),
        "max_qty": lot.get("maxQty"),
        "min_notional": min_notional_filter.get("notional") or min_notional_filter.get("minNotional"),
        "raw_filter_types": sorted(list(filters.keys())),
    }


def calculate_order_quantity(plan: Dict[str, Any], force_exchange_info: bool = False) -> Dict[str, Any]:
    symbol = str(plan.get("symbol") or "").strip().upper()
    entry = decimal_value(plan.get("entry_mid"))
    sl = decimal_value(plan.get("sl"))
    risk_usdt = decimal_value(plan.get("risk_usdt") or env_int("TESTNET_RISK_USDT_PER_TRADE", 5))
    max_notional = decimal_value(plan.get("notional_usdt_cap") or env_int("TESTNET_MAX_NOTIONAL_USDT", 50))

    if not symbol:
        return {"ok": False, "reason": "missing_symbol"}
    if entry <= 0:
        return {"ok": False, "reason": "invalid_entry_mid"}
    if sl <= 0:
        return {"ok": False, "reason": "invalid_sl"}

    stop_distance = abs(entry - sl)
    if stop_distance <= 0:
        return {"ok": False, "reason": "invalid_stop_distance"}

    si = symbol_exchange_info(symbol, force=force_exchange_info)
    if not si.get("ok"):
        return si

    filters = parse_symbol_filters(si.get("symbol_info") or {})

    step = decimal_value(filters.get("step_size"), "0")
    min_qty = decimal_value(filters.get("min_qty"), "0")
    max_qty = decimal_value(filters.get("max_qty"), "999999999")
    min_notional = decimal_value(filters.get("min_notional"), "0")

    risk_qty = risk_usdt / stop_distance
    cap_qty = max_notional / entry
    raw_qty = min(risk_qty, cap_qty)
    qty = floor_to_step(raw_qty, step)

    notional = qty * entry

    if qty <= 0:
        return {"ok": False, "reason": "qty_rounded_to_zero", "filters": filters}
    if min_qty > 0 and qty < min_qty:
        return {
            "ok": False,
            "reason": f"qty_below_min_qty:{decimal_plain(qty)}<{decimal_plain(min_qty)}",
            "filters": filters,
            "raw_qty": decimal_plain(raw_qty),
            "rounded_qty": decimal_plain(qty),
        }
    if max_qty > 0 and qty > max_qty:
        return {
            "ok": False,
            "reason": f"qty_above_max_qty:{decimal_plain(qty)}>{decimal_plain(max_qty)}",
            "filters": filters,
            "raw_qty": decimal_plain(raw_qty),
            "rounded_qty": decimal_plain(qty),
        }
    if min_notional > 0 and notional < min_notional:
        return {
            "ok": False,
            "reason": f"notional_below_min_notional:{decimal_plain(notional)}<{decimal_plain(min_notional)}",
            "filters": filters,
            "rounded_qty": decimal_plain(qty),
            "notional": decimal_plain(notional),
        }
    if max_notional > 0 and notional > max_notional:
        return {
            "ok": False,
            "reason": f"notional_above_cap:{decimal_plain(notional)}>{decimal_plain(max_notional)}",
            "filters": filters,
            "rounded_qty": decimal_plain(qty),
            "notional": decimal_plain(notional),
        }

    return {
        "ok": True,
        "source": si.get("source"),
        "symbol": symbol,
        "filters": filters,
        "entry_mid": decimal_plain(entry),
        "sl": decimal_plain(sl),
        "risk_usdt": decimal_plain(risk_usdt),
        "max_notional": decimal_plain(max_notional),
        "stop_distance": decimal_plain(stop_distance),
        "risk_qty": decimal_plain(risk_qty),
        "cap_qty": decimal_plain(cap_qty),
        "raw_qty": decimal_plain(raw_qty),
        "rounded_qty": decimal_plain(qty),
        "notional": decimal_plain(notional),
    }


def enrich_plan_with_quantity(plan: Dict[str, Any], force_exchange_info: bool = False) -> tuple[bool, str, Dict[str, Any]]:
    qty_res = calculate_order_quantity(plan, force_exchange_info=force_exchange_info)

    plan["quantity_sizing"] = qty_res

    if not qty_res.get("ok"):
        plan["quantity"] = None
        return False, qty_res.get("reason") or "quantity_sizing_failed", qty_res

    plan["quantity"] = qty_res.get("rounded_qty")
    plan["quantity_float"] = float(plan["quantity"] or 0.0)
    plan["notional_usdt"] = qty_res.get("notional")
    return True, "quantity_sizing_valid", qty_res


def ensure_tp_split(plan: Dict[str, Any]) -> None:
    def _sf(v: Any, default: float = 0.0) -> float:
        try:
            return float(v)
        except Exception:
            return default

    total_qty = _sf(plan.get("quantity_float") or plan.get("quantity"), 0.0)
    if total_qty <= 0:
        plan["tp1_qty"] = 0.0
        plan["tp2_qty"] = 0.0
        plan["tp3_qty"] = 0.0
        return

    policy = str(os.getenv("LIVE_TP_POLICY") or os.getenv("TP_POLICY") or "").strip().upper()
    if policy in ("FULL_TP1", "TP1_FULL", "100_TP1", "ALL_TP1"):
        plan["tp1_qty"] = total_qty
        plan["tp2_qty"] = 0.0
        plan["tp3_qty"] = 0.0
        plan["tp_split_mode"] = "FULL_TP1"
        return

    p1 = _sf(plan.get("tp1_qty"), 0.0)
    p2 = _sf(plan.get("tp2_qty"), 0.0)
    p3 = _sf(plan.get("tp3_qty"), 0.0)
    if p1 > 0 and p2 >= 0 and p3 >= 0:
        plan["tp1_qty"], plan["tp2_qty"], plan["tp3_qty"] = p1, p2, p3
        return

    filters = ((plan.get("quantity_sizing") or {}).get("filters") or {})
    split_res = v011_build_tp_quantities(Decimal(str(total_qty)), filters, {})
    if split_res.get("ok"):
        tp_qtys = split_res.get("tp_qtys_str") or []
        if len(tp_qtys) == 3:
            plan["tp1_qty"] = _sf(tp_qtys[0], 0.0)
            plan["tp2_qty"] = _sf(tp_qtys[1], 0.0)
            plan["tp3_qty"] = _sf(tp_qtys[2], 0.0)
            return

    plan["tp1_qty"] = 0.0
    plan["tp2_qty"] = 0.0
    plan["tp3_qty"] = 0.0


def binance_order_test(plan: Dict[str, Any]) -> Dict[str, Any]:
    if binance_env() != "TESTNET":
        return {"ok": False, "reason": "binance_env_not_testnet"}

    if not env_bool("ORDER_TEST_ENDPOINT_ONLY", True):
        return {"ok": False, "reason": "order_test_endpoint_only_required"}

    symbol = str(plan.get("symbol") or "").strip().upper()
    side = str(plan.get("entry_side") or "").strip().upper()

    # v0.7 uses tiny fixed quantity only for /order/test parameter validation.
    # It does NOT submit to matching engine.
    quantity = str(plan.get("quantity") or "0.001")

    if not symbol:
        return {"ok": False, "reason": "missing_symbol"}
    if side not in ("BUY", "SELL"):
        return {"ok": False, "reason": f"invalid_side:{side}"}

    params = {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quantity": quantity,
    }

    return binance_testnet_signed_request("POST", "/fapi/v1/order/test", params)

def execution_mode() -> str:
    return str(os.getenv("EXECUTION_MODE", "DISABLED")).strip().upper()


def binance_env() -> str:
    return str(os.getenv("BINANCE_ENV", "TESTNET")).strip().upper()


def pair_to_binance_symbol(pair: str) -> str:
    s = str(pair or "").strip().upper()
    s = s.replace("BINANCE:", "")
    s = s.replace(".P", "")
    return s


def testnet_allowed_symbols() -> set[str]:
    raw = os.getenv("TESTNET_ALLOWED_SYMBOLS", "")
    return {x.strip().upper() for x in raw.split(",") if x.strip()}


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return float(str(raw).strip())
    except Exception:
        return default


def compute_cost_gate(plan: Dict[str, Any]) -> Dict[str, Any]:
    cost_gate_enabled = env_bool("COST_GATE_ENABLED", True)

    breakdown: Dict[str, Any] = {
        "cost_gate_enabled": cost_gate_enabled,
        "cost_gate_pass": True,
        "cost_gate_reason": "cost_gate_pass",
    }
    try:
        direction = str(plan.get("direction") or "")
        if direction not in ("Long", "Short"):
            breakdown["cost_gate_pass"] = False
            breakdown["cost_gate_reason"] = "invalid_direction_for_cost_gate"
            return breakdown
        entry_price = float(plan.get("entry_mid") or 0.0)
        tp1 = float(plan.get("tp1") or 0.0)
        tp2 = float(plan.get("tp2") or 0.0)
        tp3 = float(plan.get("tp3") or 0.0)
        tp1_qty = float(plan.get("tp1_qty") or 0.0)
        tp2_qty = float(plan.get("tp2_qty") or 0.0)
        tp3_qty = float(plan.get("tp3_qty") or 0.0)
        total_qty = float(plan.get("quantity_float") or 0.0)
        taker_fee_rate = env_float("TAKER_FEE_RATE", 0.0005)
        fee_buffer_mult = env_float("FEE_BUFFER_MULT", 1.2)
        funding_rate_buffer = env_float("FUNDING_RATE_BUFFER", 0.0005) if env_bool("FUNDING_GATE_ENABLED", True) else 0.0
        funding_buffer_cycles = env_int("FUNDING_BUFFER_CYCLES", 1)
        slippage_buffer_rate = env_float("SLIPPAGE_BUFFER_RATE", 0.0002)
        min_net_tp1_usdt = env_float("MIN_NET_TP1_USDT", 0.01)
        min_net_tp1_pct_notional = env_float("MIN_NET_TP1_PCT_NOTIONAL", 0.0002)
        max_abs_funding_rate = env_float("MAX_ABS_FUNDING_RATE", 0.001)
        reject_if_net_tp1_negative = env_bool("REJECT_IF_NET_TP1_NEGATIVE", True)
        breakdown.update({
            "taker_fee_rate": taker_fee_rate,
            "fee_buffer_mult": fee_buffer_mult,
            "funding_rate_buffer": funding_rate_buffer,
            "funding_buffer_cycles": funding_buffer_cycles,
            "slippage_buffer_rate": slippage_buffer_rate,
            "tp1_qty": tp1_qty,
            "tp2_qty": tp2_qty,
            "tp3_qty": tp3_qty,
            "min_net_tp1_usdt": min_net_tp1_usdt,
            "min_net_tp1_pct_notional": min_net_tp1_pct_notional,
        })
        if not cost_gate_enabled:
            breakdown["cost_gate_reason"] = "cost_gate_disabled"
        if abs(funding_rate_buffer) > max_abs_funding_rate:
            breakdown["cost_gate_pass"] = False
            breakdown["cost_gate_reason"] = "funding_cost_too_high"
            return breakdown
        if tp1_qty <= 0 or total_qty <= 0:
            breakdown["cost_gate_pass"] = False
            breakdown["cost_gate_reason"] = "invalid_qty_for_cost_gate"
            return breakdown
        if direction == "Long" and tp1 <= entry_price:
            breakdown["cost_gate_pass"] = False
            breakdown["cost_gate_reason"] = "invalid_tp1_direction_for_cost_gate"
            return breakdown
        if direction == "Short" and tp1 >= entry_price:
            breakdown["cost_gate_pass"] = False
            breakdown["cost_gate_reason"] = "invalid_tp1_direction_for_cost_gate"
            return breakdown

        entry_notional = entry_price * total_qty
        entry_fee_total = entry_notional * taker_fee_rate * fee_buffer_mult
        funding_buffer_total = entry_notional * abs(funding_rate_buffer) * funding_buffer_cycles

        def gross(tp_price: float, tp_qty: float) -> float:
            if direction == "Long":
                return (tp_price - entry_price) * tp_qty
            return (entry_price - tp_price) * tp_qty

        gross_profit_tp1 = gross(tp1, tp1_qty)
        gross_profit_tp2 = gross(tp2, tp2_qty)
        gross_profit_tp3 = gross(tp3, tp3_qty)

        entry_fee_alloc_tp1 = entry_fee_total * (tp1_qty / total_qty)
        entry_fee_alloc_tp2 = entry_fee_total * (tp2_qty / total_qty)
        entry_fee_alloc_tp3 = entry_fee_total * (tp3_qty / total_qty)
        exit_fee_tp1 = tp1 * tp1_qty * taker_fee_rate * fee_buffer_mult
        exit_fee_tp2 = tp2 * tp2_qty * taker_fee_rate * fee_buffer_mult
        exit_fee_tp3 = tp3 * tp3_qty * taker_fee_rate * fee_buffer_mult
        funding_buffer_alloc_tp1 = funding_buffer_total * (tp1_qty / total_qty)
        funding_buffer_alloc_tp2 = funding_buffer_total * (tp2_qty / total_qty)
        funding_buffer_alloc_tp3 = funding_buffer_total * (tp3_qty / total_qty)
        slippage_buffer_tp1 = entry_price * tp1_qty * slippage_buffer_rate
        slippage_buffer_tp2 = entry_price * tp2_qty * slippage_buffer_rate
        slippage_buffer_tp3 = entry_price * tp3_qty * slippage_buffer_rate
        net_profit_tp1 = gross_profit_tp1 - entry_fee_alloc_tp1 - exit_fee_tp1 - funding_buffer_alloc_tp1 - slippage_buffer_tp1
        net_profit_tp2 = gross_profit_tp2 - entry_fee_alloc_tp2 - exit_fee_tp2 - funding_buffer_alloc_tp2 - slippage_buffer_tp2
        net_profit_tp3 = gross_profit_tp3 - entry_fee_alloc_tp3 - exit_fee_tp3 - funding_buffer_alloc_tp3 - slippage_buffer_tp3
        full_plan_net = net_profit_tp1 + net_profit_tp2 + net_profit_tp3
        net_tp1_pct = (net_profit_tp1 / entry_notional) if entry_notional > 0 else 0.0

        breakdown.update({
            "entry_notional": entry_notional,
            "entry_fee_total": entry_fee_total,
            "funding_buffer_total": funding_buffer_total,
            "gross_profit_tp1": gross_profit_tp1,
            "gross_profit_tp2": gross_profit_tp2,
            "gross_profit_tp3": gross_profit_tp3,
            "entry_fee_alloc_tp1": entry_fee_alloc_tp1,
            "entry_fee_alloc_tp2": entry_fee_alloc_tp2,
            "entry_fee_alloc_tp3": entry_fee_alloc_tp3,
            "exit_fee_tp1": exit_fee_tp1,
            "exit_fee_tp2": exit_fee_tp2,
            "exit_fee_tp3": exit_fee_tp3,
            "funding_buffer_alloc_tp1": funding_buffer_alloc_tp1,
            "funding_buffer_alloc_tp2": funding_buffer_alloc_tp2,
            "funding_buffer_alloc_tp3": funding_buffer_alloc_tp3,
            "slippage_buffer_tp1": slippage_buffer_tp1,
            "slippage_buffer_tp2": slippage_buffer_tp2,
            "slippage_buffer_tp3": slippage_buffer_tp3,
            "net_profit_tp1": net_profit_tp1,
            "net_profit_tp2": net_profit_tp2,
            "net_profit_tp3": net_profit_tp3,
            "full_plan_net": full_plan_net,
        })

        if reject_if_net_tp1_negative and net_profit_tp1 <= 0:
            breakdown["cost_gate_pass"] = False
            breakdown["cost_gate_reason"] = "net_tp1_after_cost_negative"
        elif net_profit_tp1 < min_net_tp1_usdt:
            breakdown["cost_gate_pass"] = False
            breakdown["cost_gate_reason"] = "net_tp1_after_cost_below_min_usdt"
        elif net_tp1_pct < min_net_tp1_pct_notional:
            breakdown["cost_gate_pass"] = False
            breakdown["cost_gate_reason"] = "net_tp1_after_cost_below_min_pct"
        elif full_plan_net <= 0:
            breakdown["cost_gate_pass"] = False
            breakdown["cost_gate_reason"] = "full_plan_net_after_cost_negative"
        return breakdown
    except Exception:
        breakdown["cost_gate_pass"] = False
        breakdown["cost_gate_reason"] = "cost_gate_error"
        return breakdown


def build_execution_plan(p: Dict[str, Any]) -> Dict[str, Any]:
    normalize_tp_plan(p)
    pair = pair_of(p)
    symbol = pair_to_binance_symbol(pair)
    direction = direction_of(p)

    entry_mid = p.get("entry_mid")
    sl = p.get("sl") or p.get("invalid")
    tp1 = p.get("tp1")
    tp2 = p.get("tp2")
    tp3 = p.get("tp3")

    side = "BUY" if direction == "Long" else ("SELL" if direction == "Short" else "")
    exit_side = "SELL" if direction == "Long" else ("BUY" if direction == "Short" else "")

    return {
        "plan_id": f"PLAN|{signal_key_of(p)}",
        "created_at_utc": utc_now_iso(),
        "app_version": APP_VERSION,
        "execution_mode": execution_mode(),
        "binance_env": binance_env(),
        "signal_key": signal_key_of(p),
        "pair": pair,
        "symbol": symbol,
        "direction": direction,
        "entry_type": "MARKET",
        "entry_side": side,
        "exit_side": exit_side,
        "entry_mid": entry_mid,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "raw_tp1": p.get("raw_tp1"),
        "raw_tp2": p.get("raw_tp2"),
        "raw_tp3": p.get("raw_tp3"),
        "tp_normalized": p.get("tp_normalized"),
        "tp_normalize_reason": p.get("tp_normalize_reason"),
        "leverage": env_int("DEFAULT_LEVERAGE", 2),
        # === PLAN_MARGIN_TYPE_FROM_ENV_20260624 ===
        # === PLAN_MARGIN_TYPE_FROM_ENV_SAFE_20260624 ===
        "margin_type": (
            "CROSSED"
            if str(os.getenv("MARGIN_TYPE") or "CROSSED").strip().upper() in ("CROSS", "CROSSED")
            else "ISOLATED"
            if str(os.getenv("MARGIN_TYPE") or "CROSSED").strip().upper() == "ISOLATED"
            else "CROSSED"
        ),
        "quantity": None,
        "notional_usdt_cap": env_int("TESTNET_MAX_NOTIONAL_USDT", 50),
        "risk_usdt": env_int("TESTNET_RISK_USDT_PER_TRADE", 5),
        "payload": p,
    }




def shared_validate_plan_cost_gate(plan: Dict[str, Any], require_quantity: bool = True) -> tuple[bool, str]:
    direction = str(plan.get("direction") or "").strip().upper()
    entry_side = str(plan.get("entry_side") or "").strip().upper()
    exit_side = str(plan.get("exit_side") or "").strip().upper()

    if direction not in ("LONG", "SHORT"):
        return False, "invalid_direction"
    if direction == "LONG" and (entry_side != "BUY" or exit_side != "SELL"):
        return False, "plan_sanity_invalid_direction_side_map"
    if direction == "SHORT" and (entry_side != "SELL" or exit_side != "BUY"):
        return False, "plan_sanity_invalid_direction_side_map"

    if env_bool("ORDER_REQUIRE_SL", True) and not plan.get("sl"):
        return False, "missing_sl"
    if env_bool("ORDER_REQUIRE_TP", True) and not (plan.get("tp1") or plan.get("tp2") or plan.get("tp3")):
        return False, "missing_tp"

    entry_mid = to_float_or_none(plan.get("entry_mid"))
    sl = to_float_or_none(plan.get("sl"))
    tp1 = to_float_or_none(plan.get("tp1"))
    if entry_mid is None:
        return False, "missing_entry_mid"
    if sl is None:
        return False, "missing_sl"
    if tp1 is None:
        return False, "missing_tp1"

    if direction == "LONG":
        if sl >= entry_mid:
            return False, "invalid_sl_side"
        if tp1 <= entry_mid:
            return False, "invalid_tp1_side"
    else:
        if sl <= entry_mid:
            return False, "invalid_sl_side"
        if tp1 >= entry_mid:
            return False, "invalid_tp1_side"

    if require_quantity:
        qty_ok, qty_reason, _qty_res = enrich_plan_with_quantity(plan)
        if not qty_ok:
            return False, qty_reason
    ensure_tp_split(plan)
    cost = compute_cost_gate(plan)
    plan["cost_breakdown"] = cost
    plan.update(cost)
    if env_bool("COST_GATE_ENABLED", True) and not bool(cost.get("cost_gate_pass")):
        return False, f"cost_gate_failed:{cost.get('cost_gate_reason')}"

    return True, "shared_plan_cost_gate_valid"

def validate_execution_plan(plan: Dict[str, Any]) -> tuple[bool, str]:
    mode = execution_mode()

    if mode == "DISABLED":
        return False, "execution_mode_disabled"

    if binance_env() != "TESTNET":
        return False, "binance_env_not_testnet"

    if env_bool("TESTNET_KILL_SWITCH", False):
        return False, "testnet_kill_switch_active"

    if mode == "TESTNET_ORDER_TEST":
        if not env_bool("ORDER_TEST_ENDPOINT_ONLY", True):
            return False, "order_test_endpoint_only_required"

    elif mode != "TESTNET_DRY_RUN":
        if not env_bool("ENABLE_TESTNET_ORDERS", False):
            return False, "enable_testnet_orders_false"

    allowed = testnet_allowed_symbols()
    if allowed and plan.get("symbol") not in allowed:
        return False, f"symbol_not_allowed_for_testnet:{plan.get('symbol')}"

    if env_bool("ISOLATED_MARGIN_ONLY", True) and plan.get("margin_type") != "ISOLATED":
        return False, "isolated_margin_required"

    ok, reason = shared_validate_plan_cost_gate(plan, require_quantity=True)
    if not ok:
        return False, reason

    return True, "execution_plan_valid"



def apply_rr_single_target_rewrite(plan: Dict[str, Any]) -> Dict[str, Any]:
    """
    RR Single Target Rewrite V1

    Immediate live behavior:
    - rewrite TP plan to a single full target at RR_TARGET_R
    - preserve raw TP fields for audit
    - keep tp1/tp2/tp3 schema compatible
    - force tp2/tp3 qty to 0 when qty fields exist
    """
    enabled = env_bool("RR_TARGET_REWRITE_ENABLED", True)
    target_r = env_float("RR_TARGET_R", 1.2)
    mode = str(os.getenv("RR_TARGET_MODE", "SINGLE_FULL")).strip().upper() or "SINGLE_FULL"

    base = {
        "rr_target_rewrite_enabled": enabled,
        "rr_target_mode": mode,
        "rr_target_r": target_r,
        "rr_rewrite_applied": False,
        "rr_rewrite_reason": "",
        "rr_old_tp1_r": None,
        "rr_old_tp2_r": None,
        "rr_old_tp3_r": None,
        "rr_new_tp_r": None,
        "rr_single_target_price": None,
    }

    if not enabled:
        base["rr_rewrite_reason"] = "rr_target_rewrite_disabled"
        plan.update(base)
        return {**base, "ok": True}

    entry = to_float_or_none(plan.get("entry_mid") or plan.get("entry") or plan.get("entry_price"))
    sl = to_float_or_none(plan.get("sl") or plan.get("stop_loss") or plan.get("invalid"))
    old_tp1 = to_float_or_none(plan.get("tp1"))
    old_tp2 = to_float_or_none(plan.get("tp2"))
    old_tp3 = to_float_or_none(plan.get("tp3"))

    direction_raw = str(plan.get("direction") or plan.get("dir") or "").strip().upper()
    try:
        d2 = str(direction_of(plan) or "").strip().upper()
        if d2:
            direction_raw = d2
    except Exception:
        pass

    is_long = direction_raw in ("LONG", "BUY")
    is_short = direction_raw in ("SHORT", "SELL")

    if entry is None or sl is None or entry <= 0 or sl <= 0:
        base["rr_rewrite_reason"] = "missing_entry_or_sl"
        plan.update(base)
        return {**base, "ok": False, "reason": "rr_missing_entry_or_sl"}

    if not is_long and not is_short:
        base["rr_rewrite_reason"] = f"invalid_direction:{direction_raw or 'EMPTY'}"
        plan.update(base)
        return {**base, "ok": False, "reason": "rr_invalid_direction"}

    if target_r <= 0:
        base["rr_rewrite_reason"] = f"invalid_target_r:{target_r}"
        plan.update(base)
        return {**base, "ok": False, "reason": "rr_invalid_target_r"}

    if is_long:
        risk = entry - sl
        if risk <= 0:
            base["rr_rewrite_reason"] = "invalid_long_risk"
            plan.update(base)
            return {**base, "ok": False, "reason": "rr_invalid_long_risk"}

        def rr(tp):
            return ((tp - entry) / risk) if tp is not None else None

        target = entry + (risk * target_r)

    else:
        risk = sl - entry
        if risk <= 0:
            base["rr_rewrite_reason"] = "invalid_short_risk"
            plan.update(base)
            return {**base, "ok": False, "reason": "rr_invalid_short_risk"}

        def rr(tp):
            return ((entry - tp) / risk) if tp is not None else None

        target = entry - (risk * target_r)

    # Preserve raw targets once.
    if plan.get("raw_tp1") is None:
        plan["raw_tp1"] = old_tp1
    if plan.get("raw_tp2") is None:
        plan["raw_tp2"] = old_tp2
    if plan.get("raw_tp3") is None:
        plan["raw_tp3"] = old_tp3

    old_rr1 = rr(old_tp1)
    old_rr2 = rr(old_tp2)
    old_rr3 = rr(old_tp3)

    # Keep schema compatible. Execution qty will make it a single target.
    plan["tp1"] = target
    plan["tp2"] = target
    plan["tp3"] = target

    # Force single full target if quantities are already known.
    q1 = to_float_or_none(plan.get("tp1_qty"))
    q2 = to_float_or_none(plan.get("tp2_qty"))
    q3 = to_float_or_none(plan.get("tp3_qty"))
    q_total = None

    known_qs = [x for x in [q1, q2, q3] if x is not None and x > 0]
    if known_qs:
        q_total = sum(known_qs)
    else:
        q_total = (
            to_float_or_none(plan.get("quantity"))
            or to_float_or_none(plan.get("qty"))
            or to_float_or_none(plan.get("order_qty"))
            or to_float_or_none(plan.get("position_qty"))
        )

    if q_total is not None and q_total > 0:
        plan["tp1_qty"] = q_total
        plan["tp2_qty"] = 0.0
        plan["tp3_qty"] = 0.0
    else:
        # Still block partials when downstream respects zero qty.
        plan["tp2_qty"] = 0.0
        plan["tp3_qty"] = 0.0

    plan["rr_target_rewrite_enabled"] = True
    plan["rr_target_mode"] = mode
    plan["rr_target_r"] = target_r
    plan["rr_rewrite_applied"] = True
    plan["rr_rewrite_reason"] = "single_full_target_1r_rewrite"
    plan["rr_single_target_price"] = target
    plan["rr_old_tp1_r"] = round(old_rr1, 6) if old_rr1 is not None else None
    plan["rr_old_tp2_r"] = round(old_rr2, 6) if old_rr2 is not None else None
    plan["rr_old_tp3_r"] = round(old_rr3, 6) if old_rr3 is not None else None
    plan["rr_new_tp_r"] = target_r
    plan["rr_tp1"] = target_r
    plan["rr_tp2"] = target_r
    plan["rr_tp3"] = target_r
    plan["tp_single_full_exit"] = True
    plan["tp_normalize_reason"] = "rr_single_target_rewrite"

    out = {
        **base,
        "ok": True,
        "rr_rewrite_applied": True,
        "rr_rewrite_reason": "single_full_target_1r_rewrite",
        "rr_old_tp1_r": plan["rr_old_tp1_r"],
        "rr_old_tp2_r": plan["rr_old_tp2_r"],
        "rr_old_tp3_r": plan["rr_old_tp3_r"],
        "rr_new_tp_r": target_r,
        "rr_single_target_price": target,
    }
    return out

def normalize_tp_plan(payload: Dict[str, Any]) -> Dict[str, Any]:
    direction = str(payload.get("direction") or payload.get("dir") or "").strip().upper()
    if direction in ("BUY",):
        direction = "LONG"
    if direction in ("SELL",):
        direction = "SHORT"
    if direction not in ("LONG", "SHORT"):
        return {"ok": False, "reason": "invalid_direction", "plan_invalid": True}

    entry_mid = to_float_or_none(payload.get("entry_mid") or payload.get("entry") or payload.get("entry_price"))
    sl = to_float_or_none(payload.get("sl") or payload.get("invalid"))
    tp1 = to_float_or_none(payload.get("tp1"))
    tp2 = to_float_or_none(payload.get("tp2"))
    tp3 = to_float_or_none(payload.get("tp3"))
    raw_tp1 = to_float_or_none(payload.get("raw_tp1"))
    raw_tp2 = to_float_or_none(payload.get("raw_tp2"))
    raw_tp3 = to_float_or_none(payload.get("raw_tp3"))
    payload["raw_tp1"] = raw_tp1 if raw_tp1 is not None else tp1
    payload["raw_tp2"] = raw_tp2 if raw_tp2 is not None else tp2
    payload["raw_tp3"] = raw_tp3 if raw_tp3 is not None else tp3
    if entry_mid is None or sl is None or tp1 is None or tp2 is None or tp3 is None:
        return {"ok": False, "reason": "missing_entry_or_sl_or_tp", "plan_invalid": True}

    if direction == "LONG":
        if sl >= entry_mid:
            return {"ok": False, "reason": "invalid_sl_side", "plan_invalid": True}
        valid_tps = sorted([x for x in [tp1, tp2, tp3] if x > entry_mid])
        if len(valid_tps) < 3:
            return {"ok": False, "reason": "not_enough_valid_tps_after_normalization", "plan_invalid": True}
        normalized = [valid_tps[0], valid_tps[1], valid_tps[2]]
    else:
        if sl <= entry_mid:
            return {"ok": False, "reason": "invalid_sl_side", "plan_invalid": True}
        valid_tps = sorted([x for x in [tp1, tp2, tp3] if x < entry_mid], reverse=True)
        if len(valid_tps) < 3:
            return {"ok": False, "reason": "not_enough_valid_tps_after_normalization", "plan_invalid": True}
        normalized = [valid_tps[0], valid_tps[1], valid_tps[2]]

    payload["tp1"] = normalized[0]
    payload["tp2"] = normalized[1]
    payload["tp3"] = normalized[2]
    was_normalized = not (tp1 == payload["tp1"] and tp2 == payload["tp2"] and tp3 == payload["tp3"])
    previous_normalized = bool(payload.get("tp_normalized"))
    payload["tp_normalized"] = bool(previous_normalized or was_normalized)
    payload["tp_normalize_reason"] = "tp_order_resequenced" if payload["tp_normalized"] else None
    return {"ok": True, "reason": "ok", "plan_invalid": False}


def live_binance_key_detected() -> bool:
    live_key_names = [
        "BINANCE_API_KEY",
        "BINANCE_API_SECRET",
        "BINANCE_LIVE_API_KEY",
        "BINANCE_LIVE_API_SECRET",
        "BINANCE_MAINNET_API_KEY",
        "BINANCE_MAINNET_API_SECRET",
    ]
    return any(str(os.getenv(name, "")).strip() for name in live_key_names)


def safe_client_order_id(prefix: str, signal_key: str) -> str:
    raw = "".join(ch if ch.isalnum() or ch in ["_", "-"] else "_" for ch in str(signal_key or "NO_KEY"))
    raw = raw[:24]
    suffix = str(int(time.time() * 1000))[-8:]
    return f"{prefix}_{raw}_{suffix}"[:36]


def binance_market_order(plan: Dict[str, Any]) -> Dict[str, Any]:
    if binance_env() != "TESTNET":
        return {"ok": False, "reason": "binance_env_not_testnet"}

    if live_binance_key_detected():
        return {"ok": False, "reason": "live_binance_key_detected_abort"}

    if not env_bool("ENABLE_TESTNET_ORDERS", False):
        return {"ok": False, "reason": "enable_testnet_orders_false"}

    if env_bool("ORDER_TEST_ENDPOINT_ONLY", True):
        return {"ok": False, "reason": "order_test_endpoint_only_true"}

    if env_bool("TESTNET_KILL_SWITCH", False):
        return {"ok": False, "reason": "testnet_kill_switch_true"}

    symbol = str(plan.get("symbol") or "").strip().upper()
    side = str(plan.get("entry_side") or "").strip().upper()
    quantity = str(plan.get("quantity") or "").strip()
    signal_key = str(plan.get("signal_key") or plan.get("plan_id") or symbol or "UNKNOWN")

    if not symbol:
        return {"ok": False, "reason": "missing_symbol"}
    if side not in ["BUY", "SELL"]:
        return {"ok": False, "reason": f"invalid_entry_side:{side}"}
    if not quantity:
        return {"ok": False, "reason": "missing_quantity"}

    params = {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quantity": quantity,
        "newClientOrderId": safe_client_order_id("V09MKT", signal_key),
        "newOrderRespType": "RESULT",
    }

    return binance_testnet_signed_request("POST", "/fapi/v1/order", params)


def handle_execution_after_accept(p: Dict[str, Any]) -> Dict[str, Any]:
    plan = build_execution_plan(p)
    signal_key = signal_key_of(p)
    ok, reason = validate_execution_plan(plan)

    event = {
        "event_at_utc": utc_now_iso(),
        "event_at_wib": wib_now_iso(),
        "app_version": APP_VERSION,
        "execution_mode": execution_mode(),
        "binance_env": binance_env(),
        "signal_key": signal_key,
        "pair": pair_of(p),
        "symbol": plan.get("symbol"),
        "decision": "BUILT_ONLY" if ok else ("EXECUTION_SKIPPED" if str(reason).startswith("cost_gate_failed:") else "SKIPPED"),
        "reason": "cost_gate_failed" if str(reason).startswith("cost_gate_failed:") else reason,
        "cost_gate_reason": (str(reason).split(":", 1)[1] if str(reason).startswith("cost_gate_failed:") else (plan.get("cost_gate_reason") or None)),
        "plan": plan,
    }

    append_jsonl(EXECUTION_EVENTS_LOG, event)

    v014_execution_summary_write(signal_key, {
        "pair": pair_of(p),
        "symbol": plan.get("symbol"),
        "direction": direction_of(p),
        "paper_decision": "ACCEPT",
        "paper_reason": "all_paper_gates_pass",
        "lifecycle_state": "PLAN_VALID" if ok else "PLAN_REJECTED",
        "notes": reason,
    })

    if ok or str(reason).startswith("cost_gate_failed:"):
        append_jsonl(EXECUTION_PLANS_LOG, plan)

    if ok:

        if execution_mode() == "TESTNET_ORDER_TEST":
            order_test_res = binance_order_test(plan)

            order_test_event = {
                "event_at_utc": utc_now_iso(),
                "event_at_wib": wib_now_iso(),
                "app_version": APP_VERSION,
                "execution_mode": execution_mode(),
                "binance_env": binance_env(),
                "signal_key": signal_key_of(p),
                "pair": pair_of(p),
                "symbol": plan.get("symbol"),
                "decision": "ORDER_TEST_SENT",
                "reason": "binance_order_test_endpoint_called",
                "order_test_result": order_test_res,
                "plan": plan,
            }

            append_jsonl(EXECUTION_EVENTS_LOG, order_test_event)
            event = order_test_event

        elif execution_mode() in ("TESTNET", "TESTNET_MARKET"):
            force = str((p.get("force_test") or p.get("force") or "")).strip().lower() in ("1", "true", "yes", "y", "on")
            session_guard = assert_controlled_test_session_clean(plan.get("symbol") or "", force=force, ignore_signal_key=signal_key)
            if not session_guard.get("ok"):
                blocked_event = {
                    "event_at_utc": utc_now_iso(),
                    "event_at_wib": wib_now_iso(),
                    "app_version": APP_VERSION,
                    "execution_mode": execution_mode(),
                    "binance_env": binance_env(),
                    "signal_key": signal_key,
                    "pair": pair_of(p),
                    "symbol": plan.get("symbol"),
                    "decision": "CONTROLLED_TEST_BLOCKED",
                    "reason": session_guard.get("reason"),
                    "safety_summary": session_guard.get("safety_summary"),
                    "forced": session_guard.get("forced"),
                    "plan": plan,
                }
                append_jsonl(EXECUTION_EVENTS_LOG, blocked_event)
                v014_execution_summary_write(signal_key, {
                    "entry_order_result": "CONTROLLED_TEST_BLOCKED",
                    "lifecycle_state": "CONTROLLED_TEST_BLOCKED",
                    "notes": session_guard.get("reason"),
                })
                return blocked_event
            market_res = binance_market_order(plan)

            market_event = {
                "event_at_utc": utc_now_iso(),
                "event_at_wib": wib_now_iso(),
                "app_version": APP_VERSION,
                "execution_mode": execution_mode(),
                "binance_env": binance_env(),
                "signal_key": signal_key_of(p),
                "pair": pair_of(p),
                "symbol": plan.get("symbol"),
                "decision": "TESTNET_MARKET_SENT" if market_res.get("ok") else "TESTNET_MARKET_REJECTED",
                "reason": "binance_testnet_market_order_called" if market_res.get("ok") else (market_res.get("reason") or "binance_testnet_market_order_failed"),
                "market_order_result": market_res,
                "plan": plan,
            }

            append_jsonl(EXECUTION_EVENTS_LOG, market_event)
            event = market_event
            order_body = market_res.get("body") if isinstance(market_res.get("body"), dict) else {}
            v014_execution_summary_write(signal_key, {
                "entry_order_result": "FILLED_OR_ACCEPTED" if market_res.get("ok") else "FAILED",
                "entry_order_id": order_body.get("orderId"),
                "entry_client_order_id": order_body.get("clientOrderId") or order_body.get("clientOrderID"),
                "quantity": plan.get("quantity"),
                "entry_fill_price": order_body.get("avgPrice") or order_body.get("price"),
                "lifecycle_state": "ENTRY_SENT" if market_res.get("ok") else "ENTRY_FAILED",
                "notes": market_event.get("reason"),
            })

    return event


def build_decision_log(p: Dict[str, Any], decision: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "decision_at_utc": utc_now_iso(),
        "decision_at_wib": wib_now_iso(),
        "app_version": APP_VERSION,
        "mode": get_mode(),

        "decision": decision.get("decision"),
        "reason": decision.get("reason"),
        "gate": decision.get("gate"),

        "signal_key": signal_key_of(p),
        "signal_id": p.get("signal_id") or p.get("signal_key"),
        "pair": pair_of(p),
        "direction": direction_of(p),
        "status": status_of(p),
        "priority": p.get("priority"),
        "score": p.get("score"),

        "entry_mid": p.get("entry_mid"),
        "signal_time_wib": p.get("signal_time_wib"),
        "run_ts_wib": p.get("run_ts_wib"),
        "confirmed_ts_wib": p.get("confirmed_ts_wib"),
        "source": p.get("source"),
        "signal_source": p.get("signal_source"),
        "source_mode": p.get("source_mode"),
        "execution_owner": p.get("execution_owner"),
        "plan_sanity_ok": p.get("plan_sanity_ok"),
        "plan_sanity_reason": p.get("plan_sanity_reason"),
        "plan_invalid": p.get("plan_invalid"),
        "raw_tp1": p.get("raw_tp1"),
        "raw_tp2": p.get("raw_tp2"),
        "raw_tp3": p.get("raw_tp3"),
        "tp_normalized": p.get("tp_normalized"),
        "tp_normalize_reason": p.get("tp_normalize_reason"),
        "cost_gate_pass": decision.get("cost_gate_pass") if ("cost_gate_pass" in decision) else p.get("cost_gate_pass"),
        "cost_gate_reason": decision.get("cost_gate_reason") or p.get("cost_gate_reason"),

        "state_snapshot": {
            "open_paper_positions": open_paper_count(state),
            "seen_signal_keys_count": len(state.get("seen_signal_keys") or []),
        },

        "payload": p,
    }



def normalize_close_status(outcome: Any) -> str:
    raw = str(outcome or "").strip().upper()

    mapping = {
        "TP1": "CLOSED_TP1",
        "TP2": "CLOSED_TP2",
        "TP3": "CLOSED_TP3",
        "SL": "CLOSED_SL",
        "BE": "CLOSED_BE",
        "MANUAL": "CLOSED_MANUAL",
        "CLOSE": "CLOSED_MANUAL",
        "CLOSED": "CLOSED_MANUAL",
        "EXPIRE": "EXPIRED",
        "EXPIRED": "EXPIRED",
    }

    if raw in mapping:
        return mapping[raw]

    allowed = {
        "OPEN",
        "CLOSED_TP1",
        "CLOSED_TP2",
        "CLOSED_TP3",
        "CLOSED_SL",
        "CLOSED_BE",
        "CLOSED_MANUAL",
        "EXPIRED",
    }

    if raw in allowed:
        return raw

    return "CLOSED_MANUAL"


def append_paper_event(event_type: str, position: Dict[str, Any], extra: Optional[Dict[str, Any]] = None) -> None:
    extra = extra or {}
    event = {
        "event_at_utc": utc_now_iso(),
        "event_at_wib": wib_now_iso(),
        "app_version": APP_VERSION,
        "mode": get_mode(),
        "event_type": event_type,
        "signal_key": position.get("signal_key"),
        "pair": position.get("pair"),
        "direction": position.get("direction"),
        "status": position.get("status"),
        "entry_mid": position.get("entry_mid"),
        "sl": position.get("sl"),
        "tp1": position.get("tp1"),
        "tp2": position.get("tp2"),
        "tp3": position.get("tp3"),
        "position": position,
    }
    event.update(extra)
    append_jsonl(PAPER_EVENTS_LOG, event)

def paper_win_loss_be(outcome: str, include_performance: bool, needs_review: bool) -> str:
    o = str(outcome or "").upper()
    if needs_review and not include_performance:
        return "PENDING"
    if o in {"TP1", "TP2", "TP3", "MANUAL_PROFIT"}:
        return "WIN"
    if o in {"SL", "MANUAL_LOSS"}:
        return "LOSS"
    if o == "BE":
        return "BE"
    return "REVIEW"


def to_float_or_none(v: Any) -> Optional[float]:
    try:
        if v is None or str(v).strip() == "":
            return None
        return float(v)
    except Exception:
        return None


def classify_paper_outcome(position: Dict[str, Any], close_price: Optional[float], r_realized: Optional[float]) -> str:
    direction = str(position.get("direction") or "").upper()
    sl = to_float_or_none(position.get("sl"))
    tp1 = to_float_or_none(position.get("tp1"))
    tp2 = to_float_or_none(position.get("tp2"))
    tp3 = to_float_or_none(position.get("tp3"))
    entry = to_float_or_none(position.get("entry_mid") or position.get("entry_price"))
    if close_price is None:
        return "CLOSED_MANUAL"

    if direction == "LONG":
        if sl is not None and close_price <= sl:
            return "SL"
        if tp3 is not None and close_price >= tp3:
            return "TP3"
        if tp2 is not None and close_price >= tp2:
            return "TP2"
        if tp1 is not None and close_price >= tp1:
            return "TP1"
        if r_realized is not None and abs(r_realized) <= env_float("PAPER_BE_TOLERANCE_R", 0.05):
            return "BE"
        if entry is not None and close_price > entry:
            return "MANUAL_PROFIT"
        if entry is not None and close_price < entry:
            return "MANUAL_LOSS"
    elif direction == "SHORT":
        if sl is not None and close_price >= sl:
            return "SL"
        if tp3 is not None and close_price <= tp3:
            return "TP3"
        if tp2 is not None and close_price <= tp2:
            return "TP2"
        if tp1 is not None and close_price <= tp1:
            return "TP1"
        if r_realized is not None and abs(r_realized) <= env_float("PAPER_BE_TOLERANCE_R", 0.05):
            return "BE"
        if entry is not None and close_price < entry:
            return "MANUAL_PROFIT"
        if entry is not None and close_price > entry:
            return "MANUAL_LOSS"
    return "CLOSED_MANUAL"


def compute_r_realized(position: Dict[str, Any], close_price: Optional[float]) -> Optional[float]:
    entry = to_float_or_none(position.get("entry_mid") or position.get("entry_price"))
    sl = to_float_or_none(position.get("sl"))
    direction = str(position.get("direction") or "").upper()
    if entry is None or sl is None or close_price is None:
        return None
    risk_per_unit = abs(entry - sl)
    if risk_per_unit <= 0:
        return None
    if direction == "LONG":
        return (close_price - entry) / risk_per_unit
    if direction == "SHORT":
        return (entry - close_price) / risk_per_unit
    return None


def compute_gross_pnl(position: Dict[str, Any], close_price: Optional[float]) -> Any:
    entry = to_float_or_none(position.get("entry_mid") or position.get("entry_price"))
    qty = to_float_or_none(position.get("quantity"))
    direction = str(position.get("direction") or "").upper()
    if close_price is None or entry is None:
        return "pending"
    if qty is None:
        return "pending"
    if direction == "LONG":
        return (close_price - entry) * qty
    if direction == "SHORT":
        return (entry - close_price) * qty
    return "pending"


def compute_paper_estimated_pnl(position: Dict[str, Any], close_price: Optional[float]) -> Dict[str, Any]:
    entry = to_float_or_none(position.get("entry_mid") or position.get("entry_price"))
    qty = to_float_or_none(position.get("quantity"))
    gross = compute_gross_pnl(position, close_price)
    if (not paper_net_pnl_enabled()) or entry is None or qty is None or close_price is None or not isinstance(gross, (int, float)):
        return {
            "gross_pnl_usdt": gross if isinstance(gross, (int, float)) else "pending",
            "estimated_entry_fee_usdt": "pending",
            "estimated_exit_fee_usdt": "pending",
            "estimated_slippage_usdt": "pending",
            "estimated_net_pnl_usdt": "pending",
            "total_fees_estimated": "pending",
            "pnl_source": "PAPER_ESTIMATE",
            "include_pnl": False,
        }

    entry_fee = entry * qty * paper_fee_rate() * paper_fee_buffer_mult()
    exit_fee = close_price * qty * paper_fee_rate() * paper_fee_buffer_mult()
    slippage = entry * qty * paper_slippage_buffer_rate()
    total_fees = entry_fee + exit_fee + slippage
    est_net = gross - total_fees
    return {
        "gross_pnl_usdt": gross,
        "estimated_entry_fee_usdt": entry_fee,
        "estimated_exit_fee_usdt": exit_fee,
        "estimated_slippage_usdt": slippage,
        "estimated_net_pnl_usdt": est_net,
        "total_fees_estimated": total_fees,
        "pnl_source": "PAPER_ESTIMATE",
        "include_pnl": True,
    }


def append_paper_performance(position: Dict[str, Any], payload_outcome: Optional[str], close_price: Optional[float]) -> Dict[str, Any]:
    include_performance = True
    needs_review = False
    reason = ""
    try:
        r_realized = compute_r_realized(position, close_price)
        if payload_outcome:
            outcome = str(payload_outcome).strip().upper()
        elif close_price is not None:
            outcome = classify_paper_outcome(position, close_price, r_realized)
        else:
            outcome = "CLOSED_MANUAL"
            include_performance = False
            needs_review = True
            reason = "missing_close_price"

        pnl_metrics = compute_paper_estimated_pnl(position, close_price)
        rec = {
            "signal_key": position.get("signal_key"),
            "pair": position.get("pair"),
            "symbol": position.get("symbol"),
            "direction": position.get("direction"),
            "entry_price": to_float_or_none(position.get("entry_mid") or position.get("entry_price")),
            "sl": to_float_or_none(position.get("sl")),
            "tp1": to_float_or_none(position.get("tp1")),
            "tp2": to_float_or_none(position.get("tp2")),
            "tp3": to_float_or_none(position.get("tp3")),
            "close_price": close_price,
            "outcome": outcome,
            "win_loss_be": paper_win_loss_be(outcome, include_performance, needs_review),
            "r_realized": r_realized,
            "quantity": to_float_or_none(position.get("quantity")),
            "notional_usdt": to_float_or_none(position.get("notional_usdt")),
            "gross_pnl_usdt": pnl_metrics.get("gross_pnl_usdt"),
            "estimated_entry_fee_usdt": pnl_metrics.get("estimated_entry_fee_usdt"),
            "estimated_exit_fee_usdt": pnl_metrics.get("estimated_exit_fee_usdt"),
            "estimated_slippage_usdt": pnl_metrics.get("estimated_slippage_usdt"),
            "estimated_net_pnl_usdt": pnl_metrics.get("estimated_net_pnl_usdt"),
            "total_fees_estimated": pnl_metrics.get("total_fees_estimated"),
            "pnl_source": pnl_metrics.get("pnl_source"),
            "include_pnl": pnl_metrics.get("include_pnl"),
            "include_performance": include_performance,
            "needs_review": needs_review,
            "reason": reason,
            "opened_at_wib": position.get("opened_at_wib"),
            "closed_at_wib": position.get("closed_at_wib"),
            "created_at_utc": utc_now_iso(),
        }
        append_jsonl(PAPER_PERFORMANCE_LOG, rec)
        return rec
    except Exception as e:
        rec = {
            "signal_key": position.get("signal_key"),
            "pair": position.get("pair"),
            "symbol": position.get("symbol"),
            "direction": position.get("direction"),
            "entry_price": to_float_or_none(position.get("entry_mid") or position.get("entry_price")),
            "sl": to_float_or_none(position.get("sl")),
            "tp1": to_float_or_none(position.get("tp1")),
            "tp2": to_float_or_none(position.get("tp2")),
            "tp3": to_float_or_none(position.get("tp3")),
            "close_price": close_price,
            "quantity": to_float_or_none(position.get("quantity")),
            "outcome": str(payload_outcome or "CLOSED_MANUAL").upper(),
            "win_loss_be": "REVIEW",
            "r_realized": None,
            "gross_pnl_usdt": "pending",
            "estimated_net_pnl_usdt": "pending",
            "total_fees_estimated": "pending",
            "include_performance": False,
            "needs_review": True,
            "reason": f"performance_error:{e}",
            "opened_at_wib": position.get("opened_at_wib"),
            "closed_at_wib": position.get("closed_at_wib"),
            "created_at_utc": utc_now_iso(),
        }
        append_jsonl(PAPER_PERFORMANCE_LOG, rec)
        return rec


def close_one_position_in_state(
    state: Dict[str, Any],
    signal_key: str,
    outcome: str = "CLOSED_MANUAL",
    close_reason: str = "MANUAL_CLOSE",
    close_price: Optional[float] = None,
    notes: Optional[str] = None,
) -> Dict[str, Any]:
    signal_key = str(signal_key or "").strip()
    if not signal_key:
        return {"ok": False, "reason": "missing_signal_key"}

    positions = state.get("open_paper_positions") or []
    remaining = []
    target = None

    for pos in positions:
        if (
            not target
            and str(pos.get("signal_key") or "").strip() == signal_key
            and str(pos.get("status", "OPEN")).upper() == "OPEN"
        ):
            target = dict(pos)
        else:
            remaining.append(pos)

    if not target:
        return {
            "ok": False,
            "reason": "open_position_not_found",
            "signal_key": signal_key,
        }

    closed_at_utc = utc_now_iso()
    target["closed_at_utc"] = closed_at_utc
    target["closed_at_wib"] = datetime.now(WIB).isoformat()
    target["close_reason"] = close_reason or "MANUAL_CLOSE"
    target["close_price"] = close_price
    target["notes"] = notes or ""

    perf = append_paper_performance(target, outcome, close_price)
    close_outcome = str((perf or {}).get("outcome") or outcome or "CLOSED_MANUAL").upper()
    close_status = normalize_close_status(close_outcome)

    target["status"] = close_status
    target["close_outcome"] = close_outcome

    state["open_paper_positions"] = remaining

    closed = state.get("closed_paper_positions") or []
    closed.append(target)
    if len(closed) > 1000:
        closed = closed[-1000:]
    state["closed_paper_positions"] = closed

    append_paper_event(
        "PAPER_POSITION_CLOSED",
        target,
        {
            "close_reason": target["close_reason"],
            "close_outcome": target["close_outcome"],
            "close_price": close_price,
            "performance": perf,
        },
    )

    return {
        "ok": True,
        "reason": "paper_position_closed",
        "closed_position": target,
        "open_paper_positions": open_paper_count(state),
        "closed_paper_positions": len(state.get("closed_paper_positions") or []),
    }

def close_all_positions_in_state(
    state: Dict[str, Any],
    outcome: str = "CLOSED_MANUAL",
    close_reason: str = "MANUAL_CLOSE_ALL",
    close_price: Optional[float] = None,
    notes: Optional[str] = None,
) -> Dict[str, Any]:
    positions = state.get("open_paper_positions") or []
    if not positions:
        return {
            "ok": True,
            "reason": "no_open_positions",
            "closed_count": 0,
            "open_paper_positions": 0,
        }

    closed_now = []
    remaining = []

    for pos0 in positions:
        pos = dict(pos0)
        if str(pos.get("status", "OPEN")).upper() != "OPEN":
            remaining.append(pos)
            continue

        pos["closed_at_utc"] = utc_now_iso()
        pos["closed_at_wib"] = datetime.now(WIB).isoformat()
        pos["close_reason"] = close_reason or "MANUAL_CLOSE_ALL"
        pos["close_price"] = close_price
        pos["notes"] = notes or ""

        perf = append_paper_performance(pos, outcome, close_price)
        close_outcome = str((perf or {}).get("outcome") or outcome or "CLOSED_MANUAL").upper()
        close_status = normalize_close_status(close_outcome)

        pos["status"] = close_status
        pos["close_outcome"] = close_outcome

        closed_now.append(pos)

        append_paper_event(
            "PAPER_POSITION_CLOSED",
            pos,
            {
                "close_reason": pos["close_reason"],
                "close_outcome": pos["close_outcome"],
                "close_price": close_price,
                "performance": perf,
            },
        )

    closed = state.get("closed_paper_positions") or []
    closed.extend(closed_now)
    if len(closed) > 1000:
        closed = closed[-1000:]

    state["open_paper_positions"] = remaining
    state["closed_paper_positions"] = closed

    return {
        "ok": True,
        "reason": "paper_positions_closed_all",
        "closed_count": len(closed_now),
        "open_paper_positions": open_paper_count(state),
        "closed_positions": closed_now,
    }



def load_ml_model_meta() -> Dict[str, Any]:
    meta_path = Path(os.getenv("ML_MODEL_META_PATH", "state/ml_models/logistic_v1_meta.json"))
    data = load_json_file(meta_path, {})
    return data if isinstance(data, dict) else {}




def ml_gate_mode() -> str:
    mode = str(os.getenv("ML_GATE_MODE", "SHADOW_ONLY")).strip().upper()
    allowed = {"OFF", "SHADOW_ONLY", "ADVISORY", "SOFT_GATE", "HARD_GATE"}
    return mode if mode in allowed else "SHADOW_ONLY"


def _latest_rows_by_signal(path: Path) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in _read_jsonl(path):
        k = str(row.get("signal_key") or "").strip()
        if k:
            out[k] = row
    return out


def _ml_feature_names() -> List[str]:
    names = ["direction", "score", "priority", "entry", "sl", "tp1", "tp2", "tp3", "sl_distance_pct", "tp1_distance_pct", "tp2_distance_pct", "tp3_distance_pct", "rr", "rr_tp1", "rr_tp2", "rr_tp3", "tp_normalized", "plan_sanity_ok"]
    if env_bool("ML_FEATURE_INCLUDE_SYMBOL", True):
        names.extend(["symbol", "pair"])
    if env_bool("ML_FEATURE_INCLUDE_SIGNAL_SOURCE", True):
        names.extend(["source", "signal_source"])
    if env_bool("ML_FEATURE_INCLUDE_SOURCE_MODE", False):
        names.append("source_mode")
    return names


def _time_for_row(row: Dict[str, Any]) -> Tuple[int, str]:
    ms, src = _signal_time_and_source(row)
    if ms is not None:
        return ms, src
    return 0, "missing"


def build_logistic_feature_row(payload_or_dataset_row: Dict[str, Any], feature_names: Optional[List[str]] = None) -> Dict[str, Any]:
    row = payload_or_dataset_row or {}
    entry = to_float_or_none(row.get("entry") if row.get("entry") is not None else (row.get("entry_mid") if row.get("entry_mid") is not None else row.get("entry_lo")))
    sl = to_float_or_none(row.get("sl")); tp1 = to_float_or_none(row.get("tp1")); tp2 = to_float_or_none(row.get("tp2")); tp3 = to_float_or_none(row.get("tp3"))
    def _pct(v):
        try:
            return (abs(float(v) - float(entry)) / abs(float(entry))) if (entry is not None and entry != 0 and v is not None) else None
        except Exception:
            return None
    direction = str(row.get("direction") or row.get("dir") or "").upper()
    base = {
        "symbol": row.get("symbol"), "pair": row.get("pair"), "direction": direction,
        "score": row.get("score"), "priority": row.get("priority"), "entry": entry,
        "sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3,
        "sl_distance_pct": _pct(sl), "tp1_distance_pct": _pct(tp1), "tp2_distance_pct": _pct(tp2), "tp3_distance_pct": _pct(tp3),
        "rr": row.get("rr"), "rr_tp1": row.get("rr_tp1"), "rr_tp2": row.get("rr_tp2"), "rr_tp3": row.get("rr_tp3"),
        "tp_normalized": row.get("tp_normalized"), "plan_sanity_ok": row.get("plan_sanity_ok"),
        "source": row.get("source"), "signal_source": row.get("signal_source"), "source_mode": row.get("source_mode"),
    }
    names = feature_names or _ml_feature_names()
    return {k: base.get(k) for k in names}


def _build_training_frame() -> Tuple[list, list, List[str], Dict[str, Any], List[int]]:
    outcomes = _latest_rows_by_signal(FORWARD_OUTCOMES_LOG)
    dataset = _latest_rows_by_signal(ML_DATASET_ROWS_LOG)
    feats, labels, times = [], [], []
    feature_names = _ml_feature_names()
    win = loss = 0
    for sk, out in outcomes.items():
        if not bool(out.get("include_ml_label")):
            continue
        lw = out.get("label_win")
        if lw not in (0, 1, 0.0, 1.0):
            continue
        drow = dataset.get(sk)
        if not isinstance(drow, dict):
            continue
        tms, _ = _time_for_row(drow)
        feats.append(build_logistic_feature_row(drow, feature_names=feature_names))
        y = int(float(lw)); labels.append(y); times.append(tms)
        if y == 1: win += 1
        else: loss += 1
    return feats, labels, feature_names, {"train_rows_total": len(labels), "win": win, "loss": loss}, times

def score_ml_prediction_internal(signal_key: str, payload: Dict[str, Any], context: Optional[Dict[str, Any]] = None, response: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    model_version = os.getenv("ML_MODEL_VERSION", "logistic_v1")
    model_path = Path(os.getenv("ML_MODEL_PATH", "state/ml_models/logistic_v1.pkl"))
    meta = load_ml_model_meta()
    mode = ml_gate_mode()
    if not model_path.exists():
        return {"ok": False, "signal_key": signal_key, "model_version": model_version, "ml_gate_mode": mode, "production_gate_ready": False, "reason": "model_not_found"}
    try:
        import pandas as pd
        model = pickle.loads(model_path.read_bytes())
        feature_columns = list((meta or {}).get("feature_columns") or _ml_feature_names())
        X = pd.DataFrame([build_logistic_feature_row(payload or {}, feature_columns)])
        p_win = None
        if hasattr(model, "predict_proba"):
            proba = model.predict_proba(X)
            p_win = float(proba[0][1] if len(proba[0]) > 1 else proba[0][0])
        elif hasattr(model, "predict"):
            raw = model.predict(X)
            p_win = float(raw[0])
        if p_win is None:
            return {"ok": False, "signal_key": signal_key, "model_version": model_version, "ml_gate_mode": mode, "production_gate_ready": bool(meta.get("production_gate_ready")), "reason": "prediction_output_empty"}
        p_win = max(0.0, min(1.0, float(p_win)))
    except Exception as e:
        return {"ok": False, "signal_key": signal_key, "model_version": model_version, "ml_gate_mode": mode, "production_gate_ready": bool(meta.get("production_gate_ready")), "reason": f"predict_failed:{e}"}

    prod_ready = bool(meta.get("production_gate_ready"))
    ml_decision = "LOG_ONLY"
    reason = "shadow_mode"
    if mode == "HARD_GATE" and env_bool("ML_GATE_ENABLED", False) and (not env_bool("ML_GATE_REQUIRE_PRODUCTION_READY", True) or prod_ready):
        th = env_float("ML_GATE_MIN_PROB_WIN", 0.60)
        if p_win < th:
            ml_decision = "REJECT_BY_ML_GATE"
            reason = "hard_gate_threshold"

    return {
        "ok": True,
        "signal_key": signal_key,
        "model_version": model_version,
        "probability_win": p_win,
        "probability_loss": 1.0 - p_win,
        "ml_decision": ml_decision,
        "ml_gate_mode": mode,
        "production_gate_ready": prod_ready,
        "reason": reason,
        "features_used": list((meta or {}).get("feature_columns") or _ml_feature_names()),
        "signal_source": (payload or {}).get("signal_source"),
        "source_mode": (payload or {}).get("source_mode"),
        "execution_mode": execution_mode(),
    }


def run_shadow_prediction(p: Dict[str, Any], context: Dict[str, Any], response: Dict[str, Any], now: str) -> Dict[str, Any]:
    score = score_ml_prediction_internal(signal_key_of(p), p, context=context, response=response)
    pred = {"signal_key": signal_key_of(p), "created_at_utc": now, "p_win": score.get("probability_win"), "ml_score": None, "ml_confidence": "NONE", "model_version": score.get("model_version", os.getenv("ML_MODEL_VERSION", "logistic_v1")), "decision_effect": "SHADOW_ONLY"}
    if not score.get("ok"):
        pred["ml_action"] = "MODEL_ERROR"
        pred["reason"] = score.get("reason")
        return pred
    p_win = float(score.get("probability_win"))
    pred["ml_score"] = p_win * 100.0
    pred["ml_confidence"] = "HIGH" if (p_win >= 0.70 or p_win < 0.45) else ("MEDIUM" if (0.60 <= p_win < 0.70 or 0.45 <= p_win < 0.52) else "LOW")
    pred["ml_action"] = "BOOST" if p_win >= 0.70 else "HOLD" if p_win >= 0.60 else "DOWNGRADE" if p_win >= 0.52 else "AVOID"
    return pred

def build_context_snapshot(p: Dict[str, Any]) -> Dict[str, Any]:
    if not env_bool("ML_CONTEXT_ENABLED", True):
        return {"context_quality": "DISABLED", "context_errors": [], "macro_event_context_quality": "DISABLED"}
    symbol = v010_normalize_symbol(p.get("symbol") or p.get("pair") or "BTCUSDT")
    cache = read_context_cache()
    providers_enabled, ok_cnt, fail_cnt = 0, 0, 0
    err: List[str] = []
    ctx: Dict[str, Any] = {}
    if env_bool("BINANCE_MARKET_CONTEXT_ENABLED", True):
        providers_enabled += 1
        try:
            key = f"binance:{symbol}"
            b = cache_get(cache, key, env_int("BINANCE_MARKET_CONTEXT_CACHE_TTL_SEC", 300))
            if not isinstance(b, dict):
                b = build_binance_context(symbol)
                cache_set(cache, key, b)
            ctx.update(b)
            binance_ok = bool(b.get("btc_derivatives_available")) or bool(b.get("pair_derivatives_available"))
            if (str(b.get("btc_regime")) == "UNKNOWN" and str(b.get("funding_status")) == "UNKNOWN"):
                binance_ok = False
            if binance_ok:
                ok_cnt += 1
            else:
                fail_cnt += 1
                err.append("binance:missing_or_unavailable")
        except Exception as e:
            fail_cnt += 1; err.append(f"binance:{e}")
    if env_bool("FRED_CONTEXT_ENABLED", True):
        providers_enabled += 1
        try:
            key = "fred:macro"
            f = cache_get(cache, key, env_int("FRED_CACHE_TTL_SEC", 21600))
            if not isinstance(f, dict):
                f = build_fred_context()
                cache_set(cache, key, f)
            ctx.update(f)
            fred_ok = not (
                str(f.get("macro_regime")) == "UNKNOWN"
                and str(f.get("equity_risk")) == "UNKNOWN"
                and str(f.get("yield_pressure")) == "UNKNOWN"
                and str(f.get("usd_pressure")) == "UNKNOWN"
                and str(f.get("vol_status")) == "UNKNOWN"
            )
            if not fred_ok:
                fail_cnt += 1; err.append("fred:missing_or_unavailable")
            else:
                ok_cnt += 1
        except Exception as e:
            fail_cnt += 1; err.append(f"fred:{e}")
    if env_bool("MACRO_EVENT_CALENDAR_ENABLED", True):
        providers_enabled += 1
        try:
            key = "macro:events:list"
            rows = cache_get(cache, key, env_int("MACRO_EVENT_CACHE_TTL_SEC", 300))
            quality = "API_FAILED"
            gerr = None
            if not isinstance(rows, list):
                rows, gerr = load_macro_events_from_gsheet_if_configured()
                if isinstance(rows, list) and rows:
                    quality = "FULL"
                    save_json_file(Path(os.getenv("MACRO_EVENT_CALENDAR_FILE", "state/macro_events.json")), rows)
                    cache_set(cache, key, rows)
                else:
                    local_rows = load_json_file(Path(os.getenv("MACRO_EVENT_CALENDAR_FILE", "state/macro_events.json")), [])
                    if isinstance(local_rows, list) and local_rows:
                        rows = local_rows
                        quality = "PARTIAL"
                    else:
                        rows = []
                        quality = "API_FAILED"
            else:
                quality = "PARTIAL"
            m = build_macro_event_context(p.get("signal_time_wib") or wib_now_iso(), events=rows, quality=quality)
            if quality == "API_FAILED":
                m["event_risk_level"] = "UNKNOWN"
                m["active_event_ids"] = []
            if gerr:
                err.append(f"macro:{gerr}")
            ctx.update(m)
            if str(ctx.get("macro_event_context_quality")) in ("FULL", "PARTIAL"):
                ok_cnt += 1
            else:
                fail_cnt += 1
        except Exception as e:
            fail_cnt += 1; err.append(f"macro:{e}")
    else:
        ctx["macro_event_context_quality"] = "DISABLED"
    ctx["context_quality"] = "FULL" if providers_enabled > 0 and ok_cnt == providers_enabled else "PARTIAL" if ok_cnt > 0 else "API_FAILED"
    ctx["context_errors"] = err
    ctx["btc_context"] = "TAILWIND" if ctx.get("btc_regime") == "BULL" else "HEADWIND" if ctx.get("btc_regime") == "BEAR" else "MIXED" if ctx.get("btc_regime") == "RANGE" else "UNKNOWN"
    write_context_cache(cache)
    if err:
        append_jsonl(ML_CONTEXT_ERRORS_LOG, {"created_at_utc": utc_now_iso(), "signal_key": signal_key_of(p), "errors": err})
    return ctx


def safe_ml_shadow_log(p: Dict[str, Any], decision: Dict[str, Any], response: Dict[str, Any], state_snapshot: Optional[Dict[str, Any]] = None) -> None:
    if not ml_enabled():
        return
    try:
        now = utc_now_iso()
        append_jsonl(ML_SHADOW_SIGNALS_LOG, {
            "created_at_utc": now, "signal_key": signal_key_of(p), "pair": pair_of(p), "symbol": v010_normalize_symbol(p.get("symbol") or p.get("pair") or ""),
            "direction": direction_of(p), "status": status_of(p), "score": p.get("score"), "priority": p.get("priority"), "mode": get_mode(),
            "execution_decision": decision.get("decision"), "reject_gate": decision.get("gate"), "reject_reason": decision.get("reason"),
            "payload_summary": {"has_entry": p.get("entry_lo") is not None or p.get("entry_mid") is not None},
        })
        context = build_context_snapshot(p) if env_bool("ML_CONTEXT_FETCH_ON_SIGNAL", True) else {"context_quality": "DISABLED", "macro_event_context_quality": "DISABLED", "context_errors": []}
        if context:
            append_jsonl(ML_CONTEXT_SNAPSHOTS_LOG, {"created_at_utc": now, "signal_key": signal_key_of(p), **context})
        sample = ml_classify_dataset_row({
            "signal_key": signal_key_of(p),
            "source": p.get("source"),
            "engine": p.get("engine"),
            "event_type": p.get("event_type"),
            "signal_source": p.get("signal_source"),
            "mode": get_mode(),
            "symbol": v010_normalize_symbol(p.get("symbol") or p.get("pair") or ""),
            "pair": pair_of(p),
            "direction": direction_of(p),
            "entry": p.get("entry_mid") or p.get("entry_lo"),
            "sl": p.get("sl"),
            "tp1": p.get("tp1"),
            "signal_time_wib": p.get("signal_time_wib"),
        })
        paper_qty = None
        paper_notional = None
        if str(decision.get("decision")) == "ACCEPT":
            for pos in reversed((state_snapshot or {}).get("open_paper_positions") or []):
                if str(pos.get("signal_key") or "") == str(signal_key_of(p)):
                    paper_qty = pos.get("quantity")
                    paper_notional = pos.get("notional_usdt")
                    break
            if paper_qty is None:
                entry_price = to_float_or_none(p.get("entry_mid") or p.get("entry_price"))
                if entry_price and entry_price > 0:
                    paper_notional = paper_notional_usdt_default()
                    paper_qty = paper_notional / entry_price
        append_jsonl(ML_DATASET_ROWS_LOG, {
            "signal_key": signal_key_of(p), "sample_type": sample.get("sample_type"), "include_ml": sample.get("include_ml"), "include_reason": sample.get("include_reason"),
            "pair": pair_of(p), "symbol": v010_normalize_symbol(p.get("symbol") or p.get("pair") or ""), "direction": direction_of(p),
            "signal_time_wib": sample.get("signal_time_wib") or p.get("signal_time_wib"), "created_at_utc": now, "score": p.get("score"), "priority": p.get("priority"), "mode": get_mode(),
            "setup_type": p.get("setup_type"), "risk_profile": p.get("risk_profile"), "config_version": p.get("config_version"), "source_mode": p.get("source_mode"),
            "signal_source": p.get("signal_source"), "source": p.get("source"), "engine": p.get("engine"), "event_type": p.get("event_type"), "execution_owner": p.get("execution_owner"),
            "plan_sanity_ok": p.get("plan_sanity_ok"), "plan_sanity_reason": p.get("plan_sanity_reason"), "plan_invalid": p.get("plan_invalid"),
            "raw_tp1": p.get("raw_tp1"), "raw_tp2": p.get("raw_tp2"), "raw_tp3": p.get("raw_tp3"),
            "tp_normalized": p.get("tp_normalized"), "tp_normalize_reason": p.get("tp_normalize_reason"),
            "execution_decision": decision.get("decision"), "reject_gate": decision.get("gate"), "reject_reason": decision.get("reason"),
            "do_not_queue": decision_do_not_queue(decision), "entry": p.get("entry_mid") or p.get("entry_lo"), "sl": p.get("sl"), "tp1": p.get("tp1"), "tp2": p.get("tp2"), "tp3": p.get("tp3"),
            "paper_quantity": paper_qty, "paper_notional": paper_notional, "cost_gate_pass": response.get("cost_gate_pass"), "cost_gate_reason": response.get("cost_gate_reason"),
            "net_tp1_after_cost": response.get("net_tp1_after_cost"), "label_win": None, "label_target": None, "label_R": None, "outcome_status": "PENDING", **context,
            "exclude_label_reason": sample.get("exclude_label_reason"), "confirmed_bucket_ms": sample.get("confirmed_bucket_ms"), "time_source": sample.get("time_source"),
        })
        if env_bool("ML_PREDICTION_ENABLED", True) and str(os.getenv("ML_PREDICTION_MODE", "SHADOW_ONLY")).upper() == "SHADOW_ONLY":
            pred = run_shadow_prediction(p, context if isinstance(context, dict) else {}, response, now)
            append_jsonl(ML_PREDICTIONS_LOG, pred)
    except Exception as e:
        append_jsonl(ML_CONTEXT_ERRORS_LOG, {"created_at_utc": utc_now_iso(), "signal_key": signal_key_of(p), "error": str(e)})


def fire_and_forget_ml_shadow_log(p: Dict[str, Any], decision: Dict[str, Any], response: Dict[str, Any], state_snapshot: Optional[Dict[str, Any]] = None) -> None:
    p0, d0, r0 = dict(p or {}), dict(decision or {}), dict(response or {})
    s0 = dict(state_snapshot or {})
    def _run() -> None:
        try:
            safe_ml_shadow_log(p0, d0, r0, s0)
        except Exception:
            pass
    Thread(target=_run, daemon=True).start()


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "ok": True,
        "app_version": APP_VERSION,
        "mode": get_mode(),
        "time_utc": utc_now_iso(),
    }


@app.post("/ml/context-snapshot")
def ml_context_snapshot(
    payload: MlContextPayload,
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)
    p = payload.model_dump(mode="json")
    ctx = build_context_snapshot(p)
    row = {"created_at_utc": utc_now_iso(), "signal_key": p.get("signal_key"), **ctx}
    append_jsonl(ML_CONTEXT_SNAPSHOTS_LOG, row)
    return {"ok": True, "context": row}


def _latest_by_signal(path: Path, signal_key: str) -> Dict[str, Any]:
    if not path.exists():
        return {}
    last = {}
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if str(obj.get("signal_key") or "") == signal_key:
                last = obj
    return last


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
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
                rows.append(obj)
    return rows


def _dataset_summary(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    out = {"validation_sample": 0, "forward_shadow_paper": 0, "include_ml_true": 0, "include_ml_false": 0}
    for row in rows:
        sample_type = str(row.get("sample_type") or "").upper()
        if sample_type == "VALIDATION_SAMPLE":
            out["validation_sample"] += 1
        if sample_type == "FORWARD_SHADOW_PAPER":
            out["forward_shadow_paper"] += 1
        if bool(row.get("include_ml")):
            out["include_ml_true"] += 1
        else:
            out["include_ml_false"] += 1
    return out


def reclassify_ml_dataset_rows(dry_run: bool = True, backup: bool = True, limit: Optional[int] = None) -> Dict[str, Any]:
    rows = _read_jsonl(ML_DATASET_ROWS_LOG)
    before = _dataset_summary(rows)
    out_rows: List[Dict[str, Any]] = []
    changed_rows = 0
    changed_examples: List[Dict[str, Any]] = []
    max_rows = max(0, int(limit)) if limit is not None else None
    for idx, row in enumerate(rows):
        if max_rows is not None and idx >= max_rows:
            out_rows.append(dict(row))
            continue
        old_row = dict(row)
        new_row = ml_classify_dataset_row(row)
        if old_row != new_row:
            changed_rows += 1
            if len(changed_examples) < 10:
                changed_examples.append({
                    "signal_key": new_row.get("signal_key"),
                    "old_sample_type": old_row.get("sample_type"),
                    "new_sample_type": new_row.get("sample_type"),
                    "old_include_ml": old_row.get("include_ml"),
                    "new_include_ml": new_row.get("include_ml"),
                    "reason": new_row.get("include_reason") or new_row.get("exclude_label_reason"),
                })
        out_rows.append(new_row)
    after = _dataset_summary(out_rows)
    backup_path = None
    if not dry_run:
        ensure_dirs()
        if backup and ML_DATASET_ROWS_LOG.exists():
            stamp = utc_now().strftime("%Y%m%d%H%M%S")
            backup_file = LOG_DIR / f"ml_dataset_rows.jsonl.bak_{stamp}"
            ML_DATASET_ROWS_LOG.replace(backup_file)
            backup_path = str(backup_file)
        with ML_DATASET_ROWS_LOG.open("w", encoding="utf-8") as f:
            for row in out_rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
    return {"ok": True, "dry_run": dry_run, "total_rows": len(rows), "changed_rows": changed_rows, "before": before, "after": after, "changed_examples": changed_examples, "backup_path": backup_path}


def _forward_window_hours() -> int:
    hrs = env_int("FORWARD_OUTCOME_WINDOW_HOURS", 24)
    if hrs <= 0:
        mins_fallback = env_int("FORWARD_OUTCOME_WINDOW_MINUTES", 180)
        hrs = max(1, mins_fallback // 60)
    return max(1, hrs)


def _signal_time_and_source(row: Dict[str, Any]) -> (Optional[int], str):
    bucket = row.get("confirmed_bucket_ms")
    if bucket is not None:
        try:
            return int(bucket), "signal_key_bucket_ms"
        except Exception:
            pass
    key_bucket = _extract_signal_key_bucket_ms(row.get("signal_key") or row.get("signal_id"))
    if key_bucket is not None:
        return key_bucket, "signal_key_bucket_ms"
    dt_wib = parse_wib_flexible(row.get("signal_time_wib"))
    if dt_wib:
        return int(dt_wib.astimezone(timezone.utc).timestamp() * 1000), "signal_time_wib"
    dt_utc = parse_iso_utc(row.get("created_at_utc"))
    if dt_utc:
        return int(dt_utc.timestamp() * 1000), "created_at_utc_fallback"
    return None, "missing"


def _eval_forward_outcome_row(row: Dict[str, Any], interval: str, window_ms: int, window_hours: int, skip_validation: bool) -> Dict[str, Any]:
    now_utc = utc_now()
    now = now_utc.isoformat()
    signal_key = str(row.get("signal_key") or "")
    symbol = v010_normalize_symbol(row.get("symbol") or row.get("pair") or "")
    direction = str(row.get("direction") or "").upper()
    sample_type = str(row.get("sample_type") or "")
    include_ml = bool(row.get("include_ml"))
    base = {
        "signal_key": signal_key,
        "symbol": symbol,
        "direction": direction,
        "signal_time_wib": row.get("signal_time_wib"),
        "evaluated_at_utc": now,
        "outcome_status": "PENDING",
        "label_win": None,
        "label_target": None,
        "label_R": None,
        "outcome_ts_wib": None,
        "bars_to_outcome": None,
        "candles_checked": 0,
        "hit_tp1": False,
        "hit_tp2": False,
        "hit_tp3": False,
        "hit_sl": False,
        "first_hit": None,
        "same_candle_conflict": False,
        "same_candle_policy": str(os.getenv("FORWARD_OUTCOME_SAME_CANDLE_POLICY", "CONSERVATIVE_SL")).strip() or "CONSERVATIVE_SL",
        "evaluation_window_hours": window_hours,
        "candle_interval": interval,
        "include_ml_label": False,
        "exclude_label_reason": None,
        "time_source": "missing",
        "execution_decision": row.get("execution_decision"),
        "sample_type": sample_type,
    }
    if sample_type == "VALIDATION_SAMPLE":
        base["outcome_status"] = "SKIPPED"
        base["exclude_label_reason"] = "validation_sample"
        if skip_validation:
            return base
    if not include_ml:
        base["outcome_status"] = "SKIPPED"
        base["exclude_label_reason"] = "include_ml_false"
        return base
    signal_ms, time_source = _signal_time_and_source(row)
    base["time_source"] = time_source
    if signal_ms is None:
        return {**base, "outcome_status": "DATA_GAP", "exclude_label_reason": "missing_signal_time"}
    entry = to_float_or_none(row.get("entry"))
    sl = to_float_or_none(row.get("sl"))
    tp1 = to_float_or_none(row.get("tp1"))
    tp2 = to_float_or_none(row.get("tp2"))
    tp3 = to_float_or_none(row.get("tp3"))
    if entry is None or sl is None or tp1 is None or direction not in ("LONG", "SHORT"):
        return {**base, "outcome_status": "INVALID_PLAN", "exclude_label_reason": "missing_plan_fields"}
    candles = market_load_candles(symbol, interval)
    if not candles:
        return {**base, "outcome_status": "DATA_GAP", "exclude_label_reason": "missing_candles"}
    horizon_ms = signal_ms + window_ms
    selected = []
    for c in candles:
        o = int(c.get("open_time_ms") or 0)
        cl = int(c.get("close_time_ms") or 0)
        if cl > signal_ms and o <= horizon_ms:
            selected.append(c)
    if not selected:
        if int(now_utc.timestamp() * 1000) < horizon_ms:
            return {**base, "outcome_status": "PENDING", "exclude_label_reason": "awaiting_candles"}
        return {**base, "outcome_status": "DATA_GAP", "exclude_label_reason": "no_candles_after_signal"}
    hit_target = None
    hit_ts_wib = None
    same_conflict = False
    checked = 0
    latest_close_ms = 0
    for c in selected:
        checked += 1
        latest_close_ms = max(latest_close_ms, int(c.get("close_time_ms") or 0))
        hi = to_float_or_none(c.get("high"))
        lo = to_float_or_none(c.get("low"))
        if hi is None or lo is None:
            continue
        if direction == "LONG":
            sl_hit = lo <= sl
            tp1_hit = hi >= tp1
            tp2_hit = tp2 is not None and hi >= tp2
            tp3_hit = tp3 is not None and hi >= tp3
        else:
            sl_hit = hi >= sl
            tp1_hit = lo <= tp1
            tp2_hit = tp2 is not None and lo <= tp2
            tp3_hit = tp3 is not None and lo <= tp3
        any_tp_hit = bool(tp1_hit or tp2_hit or tp3_hit)
        if sl_hit and any_tp_hit:
            hit_target = "SL"
            same_conflict = True
            hit_ts_wib = datetime.fromtimestamp((int(c.get("close_time_ms") or 0)) / 1000.0, timezone.utc).astimezone(WIB).isoformat()
            break
        if sl_hit:
            hit_target = "SL"
            hit_ts_wib = datetime.fromtimestamp((int(c.get("close_time_ms") or 0)) / 1000.0, timezone.utc).astimezone(WIB).isoformat()
            break
        if tp3_hit:
            hit_target = "TP3"
            hit_ts_wib = datetime.fromtimestamp((int(c.get("close_time_ms") or 0)) / 1000.0, timezone.utc).astimezone(WIB).isoformat()
            break
        if tp2_hit:
            hit_target = "TP2"
            hit_ts_wib = datetime.fromtimestamp((int(c.get("close_time_ms") or 0)) / 1000.0, timezone.utc).astimezone(WIB).isoformat()
            break
        if tp1_hit:
            hit_target = "TP1"
            hit_ts_wib = datetime.fromtimestamp((int(c.get("close_time_ms") or 0)) / 1000.0, timezone.utc).astimezone(WIB).isoformat()
            break
    out = dict(base)
    out["candles_checked"] = checked
    out["bars_to_outcome"] = checked if hit_target else None
    out["outcome_ts_wib"] = hit_ts_wib
    out["same_candle_conflict"] = same_conflict
    if hit_target == "SL":
        out.update({"outcome_status": "RESOLVED", "include_ml_label": True, "label_target": "SL", "label_win": 0, "label_R": -1.0, "first_hit": "SL", "hit_sl": True})
        return out
    if hit_target in ("TP1", "TP2", "TP3"):
        rr = {"TP1": 1.0, "TP2": 1.5, "TP3": 2.5}
        out.update({"outcome_status": "RESOLVED", "include_ml_label": True, "label_target": hit_target, "label_win": 1, "label_R": rr[hit_target], "first_hit": hit_target})
        out["hit_tp1"] = hit_target == "TP1"
        out["hit_tp2"] = hit_target == "TP2"
        out["hit_tp3"] = hit_target == "TP3"
        return out
    if latest_close_ms >= horizon_ms:
        out.update({"outcome_status": "OPEN_END", "exclude_label_reason": "window_end_no_hit"})
        return out
    out.update({"outcome_status": "PENDING", "exclude_label_reason": "awaiting_window_end"})
    return out


def evaluate_forward_outcomes(limit: int = 200, force: bool = False) -> Dict[str, Any]:
    if not env_bool("FORWARD_OUTCOME_ENABLED", True):
        return {"ok": True, "skipped": True, "reason": "forward_outcome_disabled"}
    interval = str(os.getenv("FORWARD_OUTCOME_INTERVAL", "1m")).strip() or "1m"
    include_rejected = env_bool("FORWARD_OUTCOME_INCLUDE_REJECTED", True)
    include_accepted = env_bool("FORWARD_OUTCOME_INCLUDE_ACCEPTED", True)
    skip_validation = env_bool("FORWARD_OUTCOME_SKIP_VALIDATION", True)
    max_rows = env_int("FORWARD_OUTCOME_MAX_ROWS_PER_RUN", 200)
    limit = max(1, min(limit, max_rows))
    window_hours = _forward_window_hours()
    window_ms = window_hours * 60 * 60 * 1000
    rows = _read_jsonl(ML_DATASET_ROWS_LOG)
    st = load_json_file(FORWARD_OUTCOME_STATE_FILE, {})
    final_keys = st.get("final_signal_keys") if isinstance(st, dict) else {}
    if not isinstance(final_keys, dict):
        final_keys = {}
    processed = 0
    written = 0
    for row in reversed(rows):
        if processed >= limit:
            break
        signal_key = str(row.get("signal_key") or "")
        decision = str(row.get("execution_decision") or "").upper()
        if decision == "REJECT" and not include_rejected:
            continue
        if decision == "ACCEPT" and not include_accepted:
            continue
        if not signal_key or (signal_key in final_keys and not force):
            continue
        try:
            result = _eval_forward_outcome_row(row, interval, window_ms, window_hours, skip_validation)
        except Exception as e:
            append_jsonl(FORWARD_OUTCOME_ERRORS_LOG, {"evaluated_at_utc": utc_now_iso(), "signal_key": signal_key, "error": str(e)})
            continue
        processed += 1
        append_jsonl(FORWARD_OUTCOMES_LOG, result)
        written += 1
        if result.get("outcome_status") in ("RESOLVED", "OPEN_END", "DATA_GAP", "INVALID_PLAN", "SKIPPED"):
            final_keys[signal_key] = {"status": result.get("outcome_status"), "updated_at_utc": utc_now_iso()}
    save_json_file(FORWARD_OUTCOME_STATE_FILE, {"updated_at_utc": utc_now_iso(), "final_signal_keys": final_keys})
    return {"ok": True, "evaluated": processed, "written": written, "interval": interval, "window_ms": window_ms, "window_hours": window_hours, "force": force}


@app.get("/ml/context-latest")
def ml_context_latest(
    signal_key: str,
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)
    return {"ok": True, "row": _latest_by_signal(ML_CONTEXT_SNAPSHOTS_LOG, signal_key)}


@app.get("/ml/dataset/latest")
def ml_dataset_latest(
    signal_key: str,
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)
    return {"ok": True, "row": _latest_by_signal(ML_DATASET_ROWS_LOG, signal_key)}


@app.get("/ml/prediction/latest")
def ml_prediction_latest(
    signal_key: str,
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)
    return {"ok": True, "row": _latest_by_signal(ML_PREDICTIONS_LOG, signal_key)}


@app.post("/ml/outcome/evaluate")
def ml_outcome_evaluate(
    payload: Optional[ForwardOutcomeEvaluatePayload] = None,
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)
    req = payload.model_dump(mode="json") if payload else {}
    max_rows = req.get("max_rows")
    limit = req.get("limit")
    force = bool(req.get("force") is True)
    raw_lim = max_rows if str(max_rows or "").strip() else limit
    lim = int(raw_lim) if str(raw_lim or "").strip() else env_int("FORWARD_OUTCOME_MAX_ROWS_PER_RUN", 200)
    try:
        return evaluate_forward_outcomes(limit=lim, force=force)
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/ml/dataset/reclassify")
def ml_dataset_reclassify(
    payload: Optional[MlDatasetReclassifyPayload] = None,
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)
    req = payload.model_dump(mode="json") if payload else {}
    return reclassify_ml_dataset_rows(
        dry_run=bool(req.get("dry_run", True)),
        backup=bool(req.get("backup", True)),
        limit=req.get("limit"),
    )


@app.get("/ml/outcome/latest")
def ml_outcome_latest(
    signal_key: str,
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)
    return {"ok": True, "row": _latest_by_signal(FORWARD_OUTCOMES_LOG, signal_key)}


@app.get("/ml/outcome/summary")
def ml_outcome_summary(
    date_wib: Optional[str] = None,
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)
    rows = _read_jsonl(FORWARD_OUTCOMES_LOG)
    counts: Dict[str, int] = {}
    filtered: List[Dict[str, Any]] = []
    for r in rows:
        if date_wib:
            ev = str(r.get("evaluated_at_utc") or "").strip()
            dt = parse_iso_utc(ev)
            if not dt or dt.astimezone(WIB).date().isoformat() != str(date_wib):
                continue
        filtered.append(r)
        k = str(r.get("outcome_status") or "UNKNOWN")
        counts[k] = counts.get(k, 0) + 1
    labeled = 0
    win = 0
    loss = 0
    tp1 = tp2 = tp3 = sl = 0
    for r in filtered:
        if bool(r.get("include_ml_label")):
            labeled += 1
        tgt = str(r.get("label_target") or "")
        if tgt == "TP1":
            tp1 += 1; win += 1
        elif tgt == "TP2":
            tp2 += 1; win += 1
        elif tgt == "TP3":
            tp3 += 1; win += 1
        elif tgt == "SL":
            sl += 1; loss += 1
    return {
        "ok": True,
        "date_wib": date_wib,
        "total": len(filtered),
        "labeled": labeled,
        "win": win,
        "loss": loss,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "sl": sl,
        "pending": counts.get("PENDING", 0),
        "skipped": counts.get("SKIPPED", 0),
        "open_end": counts.get("OPEN_END", 0),
        "data_gap": counts.get("DATA_GAP", 0),
        "invalid_plan": counts.get("INVALID_PLAN", 0),
    }



def train_logistic_model(req: Dict[str, Any]) -> Dict[str, Any]:
    try:
        import pandas as pd
        from sklearn.compose import ColumnTransformer
        from sklearn.impute import SimpleImputer
        from sklearn.linear_model import LogisticRegression
        from sklearn.metrics import accuracy_score, roc_auc_score, brier_score_loss, precision_score, recall_score
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import OneHotEncoder
    except Exception as e:
        return {"ok": False, "reason": f"missing_training_dependencies:{e}"}
    feats, labels, feature_names, stats, times = _build_training_frame()
    min_rows = int(req.get("min_rows") or 30)
    min_loss = int(req.get("min_loss") or 5)
    if len(labels) < min_rows or stats["loss"] < min_loss:
        return {"ok": False, "reason": "insufficient_data", **stats, "min_rows": min_rows, "min_loss": min_loss}
    model_version = str(req.get("model_version") or "logistic_v1")
    model_path = Path(os.getenv("ML_MODEL_PATH", f"state/ml_models/{model_version}.pkl"))
    meta_path = Path(os.getenv("ML_MODEL_META_PATH", f"state/ml_models/{model_version}_meta.json"))
    model_path.parent.mkdir(parents=True, exist_ok=True)
    if model_path.exists() and not bool(req.get("force") is True):
        return {"ok": True, "reason": "model_exists", "model_path": str(model_path), "meta_path": str(meta_path), "model_exists": True}

    rows = sorted(list(zip(times, feats, labels)), key=lambda x: x[0])
    split_idx = max(1, int(len(rows) * 0.7))
    train_rows, val_rows = rows[:split_idx], rows[split_idx:]
    if not val_rows:
        train_rows, val_rows = rows, []
    X_train = pd.DataFrame([r[1] for r in train_rows]); y_train = pd.Series([r[2] for r in train_rows])

    validation_reason_override = None
    y_all = pd.Series(labels)
    if len(set(int(v) for v in y_train.tolist())) < 2:
        if len(set(int(v) for v in y_all.tolist())) >= 2:
            train_rows = rows
            val_rows = []
            X_train = pd.DataFrame([r[1] for r in train_rows]); y_train = pd.Series([r[2] for r in train_rows])
            validation_reason_override = "train_split_single_class_used_full_train"
        else:
            return {"ok": False, "reason": "single_class_training_data", **stats}

    num_cols = [c for c in X_train.columns if pd.api.types.is_numeric_dtype(X_train[c])]
    cat_cols = [c for c in X_train.columns if c not in num_cols]
    pre = ColumnTransformer([("num", Pipeline([("imp", SimpleImputer(strategy="median"))]), num_cols), ("cat", Pipeline([("imp", SimpleImputer(strategy="most_frequent")), ("oh", OneHotEncoder(handle_unknown="ignore"))]), cat_cols)])
    model = Pipeline([("pre", pre), ("clf", LogisticRegression(max_iter=1000, class_weight="balanced"))])
    model.fit(X_train, y_train)
    model_path.write_bytes(pickle.dumps(model))

    def calc_metrics(Xdf, yser):
        pred = model.predict(Xdf)
        proba = model.predict_proba(Xdf)[:,1] if hasattr(model,'predict_proba') else None
        m = {"accuracy": float(accuracy_score(yser,pred)) if len(yser) else None, "roc_auc": None, "brier_score": None, "precision_loss_avoidance": None, "recall_loss_avoidance": None}
        if proba is not None:
            try: m["brier_score"] = float(brier_score_loss(yser, proba))
            except: pass
            try:
                if len(set(yser.tolist())) >= 2: m["roc_auc"] = float(roc_auc_score(yser, proba))
            except: pass
        try:
            loss_true = [1-int(v) for v in yser.tolist()]
            loss_pred = [1-int(v) for v in pred.tolist()]
            m["precision_loss_avoidance"] = float(precision_score(loss_true, loss_pred, zero_division=0))
            m["recall_loss_avoidance"] = float(recall_score(loss_true, loss_pred, zero_division=0))
        except: pass
        return m

    in_sample_metrics = calc_metrics(pd.DataFrame(feats), pd.Series(labels))
    validation_ready = False
    validation_reason = validation_reason_override
    validation_metrics = None
    validation_win = validation_loss = 0
    validation_class_coverage_ok = False
    val_start = val_end = None
    if val_rows:
        X_val = pd.DataFrame([r[1] for r in val_rows]); y_val = pd.Series([r[2] for r in val_rows])
        validation_win = int(sum(1 for v in y_val.tolist() if v == 1)); validation_loss = int(sum(1 for v in y_val.tolist() if v == 0))
        val_start = datetime.fromtimestamp(val_rows[0][0]/1000, timezone.utc).isoformat() if val_rows[0][0] else None
        val_end = datetime.fromtimestamp(val_rows[-1][0]/1000, timezone.utc).isoformat() if val_rows[-1][0] else None
        validation_class_coverage_ok = validation_win >= 1 and validation_loss >= 1
        if len(val_rows) < 10:
            validation_reason = validation_reason or "insufficient_rows"
        elif not validation_class_coverage_ok:
            if validation_win == 0 and validation_loss == 0:
                validation_reason = validation_reason or "validation_single_class"
            elif validation_win == 0:
                validation_reason = validation_reason or "validation_no_win"
            elif validation_loss == 0:
                validation_reason = validation_reason or "validation_no_loss"
            else:
                validation_reason = validation_reason or "validation_single_class"
        else:
            validation_ready = True
            validation_reason = None if validation_reason_override is None else validation_reason_override
            validation_metrics = calc_metrics(X_val, y_val)
    else:
        validation_reason = validation_reason or "insufficient_rows"

    train_rows_total = int(stats["train_rows_total"]); win = int(stats["win"]); loss = int(stats["loss"])
    train_win = int(sum(1 for v in y_train.tolist() if int(v) == 1))
    train_loss = int(sum(1 for v in y_train.tolist() if int(v) == 0))
    train_period_start = datetime.fromtimestamp(train_rows[0][0]/1000, timezone.utc).isoformat() if train_rows and train_rows[0][0] else None
    train_period_end = datetime.fromtimestamp(train_rows[-1][0]/1000, timezone.utc).isoformat() if train_rows and train_rows[-1][0] else None

    smoke_ready = train_rows_total >= 30 and win >= 10 and loss >= 5
    baseline_ready = train_rows_total >= 100 and loss >= 10
    min_brier = env_float("ML_VALIDATION_MAX_BRIER", 0.25)
    days_cov = ((max(times)-min(times))/86400000.0) if times and min(times) > 0 else 0.0
    severe_leak = False
    prod = bool(train_rows_total >= 500 and loss >= 100 and validation_ready and validation_loss >= 30 and validation_metrics and (validation_metrics.get("roc_auc") or 0) >= 0.60 and (validation_metrics.get("brier_score") is not None and validation_metrics.get("brier_score") <= min_brier) and (not severe_leak) and days_cov >= 30.0)

    df_all = pd.DataFrame(feats)
    feature_missing_rate = {c: float(df_all[c].isna().mean()) for c in df_all.columns}
    categorical_unique_count = {c: int(df_all[c].nunique(dropna=True)) for c in cat_cols}
    dropped_features = ["cost_gate_pass", "execution_decision", "reject_reason", "label_win", "label_R", "label_target", "outcome_status"]
    leak_warn = ["source_mode_excluded_by_default", "cost_gate_pass_excluded_by_default"]
    symbol_warn = ("symbol_small_sample_high_cardinality" if ("symbol" in df_all.columns and len(df_all) < 200 and df_all["symbol"].nunique(dropna=True) > 3) else None)

    meta = {
        "model_version": model_version, "created_at_utc": utc_now_iso(), "mode": str(req.get("mode") or "SMOKE_TRAIN"),
        "train_rows_total": train_rows_total, "win": win, "loss": loss, "train_rows_used": len(train_rows), "train_win": train_win, "train_loss": train_loss, "train_period_start": train_period_start, "train_period_end": train_period_end,
        "class_balance": {"win_ratio": (win/train_rows_total) if train_rows_total else 0.0, "loss_ratio": (loss/train_rows_total) if train_rows_total else 0.0},
        "features": feature_names, "feature_columns": feature_names,
        "in_sample_metrics": in_sample_metrics, "validation_metrics": validation_metrics, "validation_ready": validation_ready, "validation_reason": validation_reason,
        "validation_rows": len(val_rows), "validation_win": validation_win, "validation_loss": validation_loss, "validation_class_coverage_ok": validation_class_coverage_ok,
        "validation_period_start": val_start, "validation_period_end": val_end,
        "feature_missing_rate": feature_missing_rate, "categorical_unique_count": categorical_unique_count,
        "dropped_features": dropped_features, "leakage_risk_warnings": leak_warn,
        "small_sample_warning": train_rows_total < 100, "symbol_overfit_warning": symbol_warn,
        "smoke_ready": smoke_ready, "baseline_ready": baseline_ready,
        "production_gate_ready": prod, "production_gate_ready_reason": "requires_large_oos_validated_sample" if not prod else "ready",
        "recommended_ml_gate_mode": "SHADOW_ONLY" if not prod else "ADVISORY", "decision_effect": "SHADOW_ONLY_NOT_GATE" if not prod else "GATE_ELIGIBLE",
    }
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return {"ok": True, "model_path": str(model_path), "meta_path": str(meta_path), "meta": meta}


@app.post("/ml/model/train-logistic")
def ml_model_train_logistic(payload: MlTrainLogisticPayload, x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"), x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret")) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)
    return train_logistic_model(payload.model_dump(mode="json"))


@app.post("/ml/model/evaluate")
def ml_model_evaluate(x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"), x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret")) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)
    meta = load_ml_model_meta()
    feats, labels, _, stats, _ = _build_training_frame()
    out = {"ok": True, "dataset": stats, "meta": meta, "production_gate_ready": bool(meta.get("production_gate_ready")), "recommended_ml_gate_mode": meta.get("recommended_ml_gate_mode", "SHADOW_ONLY")}
    if not feats:
        return out
    try:
        import pandas as pd
        model_path = Path(os.getenv("ML_MODEL_PATH", "state/ml_models/logistic_v1.pkl"))
        model = pickle.loads(model_path.read_bytes())
        X = pd.DataFrame(feats)
        y = labels
        pred = model.predict(X)
        out["metrics"] = {"accuracy": float(sum(int(a==b) for a,b in zip(pred,y))/len(y)) if y else None}
    except Exception as e:
        out["metrics_error"] = str(e)
    return out


@app.post("/ml/prediction/score")
def ml_prediction_score(payload: MlPredictionScorePayload, x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"), x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret")) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)
    p = payload.payload or {}
    signal_key = payload.signal_key or signal_key_of(p)
    score = score_ml_prediction_internal(signal_key, p)
    row = {"created_at_utc": utc_now_iso(), **score}
    append_jsonl(ML_PREDICTIONS_LOG, row)
    return score

@app.get("/ml/model/status")
def ml_model_status(
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)
    model_path = Path(os.getenv("ML_MODEL_PATH", "state/ml_models/logistic_v1.pkl"))
    meta = load_ml_model_meta()
    return {"ok": True, "model_version": os.getenv("ML_MODEL_VERSION", "logistic_v1"), "model_exists": model_path.exists(), "model_path": str(model_path), "meta": meta if meta else None, "production_gate_ready": bool((meta or {}).get("production_gate_ready")), "recommended_ml_gate_mode": (meta or {}).get("recommended_ml_gate_mode", "SHADOW_ONLY")}



@app.post("/webhook/signal")
def webhook_signal(
    payload: SignalPayload,
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)
    p = payload_to_dict(payload)
    p["signal_source"] = "APPS_SCRIPT"
    p["event_type"] = "SIGNAL_CONFIRMED"
    if signal_source_mode() == "STAT_TECH_PRIMARY" and apps_script_signal_mode() == "BACKUP_COMPARE_ONLY":
        p["source"] = p.get("source") or "apps_script_inst"
        p["engine"] = p.get("engine") or "INST"
        p["source_mode"] = "BACKUP_COMPARE_ONLY"
        p["execution_owner"] = "NONE"
        decision = {"decision": "BACKUP_ONLY", "reason": "SOURCE_NOT_PRIMARY", "gate": "source_gate"}
        with LOCK:
            append_jsonl(SIGNALS_LOG, build_signal_log(p))
            state = load_state()
            append_jsonl(DECISIONS_LOG, build_decision_log(p, decision, state))
            response = {"ok": True, "decision": decision["decision"], "reason": decision["reason"], "gate": decision["gate"], "execution_owner": "NONE", "source_mode": "BACKUP_COMPARE_ONLY"}
            fire_and_forget_ml_shadow_log(p, decision, response, state)
        return response
    p["source"] = p.get("source") or "APPS_SCRIPT"
    return _process_signal_pipeline(p)


def _process_signal_pipeline(p: Dict[str, Any]) -> Dict[str, Any]:
    mode = get_mode()
    with LOCK:
        append_jsonl(SIGNALS_LOG, build_signal_log(p))
        state = load_state()
        source = str(p.get("source") or "").strip().upper()
        execution_owner = str(p.get("execution_owner") or "").strip().upper()
        if execution_mode() == "DISABLED" and (source == "VPS_SMC" or execution_owner == "VPS_SMC"):
            decision = {"decision": "REJECT", "reason": "execution_mode_disabled", "gate": "execution_mode_gate"}
            append_jsonl(DECISIONS_LOG, build_decision_log(p, decision, state))
            notify_signal_decision_async(p, decision)
            response = {
                "ok": True,
                "decision": decision["decision"],
                "reason": decision["reason"],
                "gate": decision["gate"],
                "signal_id": p.get("signal_id") or p.get("signal_key"),
                "execution_mode": execution_mode(),
            }
            fire_and_forget_ml_shadow_log(p, decision, response, state)
            return response
        if (source == "VPS_SMC" or execution_owner == "VPS_SMC") and not vps_smc_bridge_enabled_for_mode(execution_mode()):
            decision = {"decision": "REJECT", "reason": "vps_smc_bridge_mode_not_enabled", "gate": "vps_smc_bridge_mode_gate"}
            append_jsonl(DECISIONS_LOG, build_decision_log(p, decision, state))
            notify_signal_decision_async(p, decision)
            response = {
                "ok": True,
                "decision": decision["decision"],
                "reason": decision["reason"],
                "gate": decision["gate"],
                "signal_id": p.get("signal_id") or p.get("signal_key"),
                "execution_mode": execution_mode(),
            }
            fire_and_forget_ml_shadow_log(p, decision, response, state)
            return response
        if mode == "RECEIVED_ONLY":
            decision = {"decision": "RECEIVED_ONLY", "reason": "receiver_only_logger_mode_no_paper_gate_no_execution", "gate": "mode_gate"}
            append_jsonl(DECISIONS_LOG, build_decision_log(p, decision, state))
            notify_signal_decision_async(p, decision)
            fire_and_forget_ml_shadow_log(p, decision, {"ok": True, "decision": "RECEIVED_ONLY"}, state)
            return {"ok": True, "decision": "RECEIVED_ONLY", "reason": "v0.3 logger mode, no execution", "signal_id": p.get("signal_id") or p.get("signal_key")}
        current_execution_mode = execution_mode()

        # === VPS_SMC_PROCESS_PIPELINE_CONFLUENCE_FIRST_20260628 ===
        # Safety net: force VPS SMC / minimal SMC candidates through confluence
        # before legacy refresh/recompute/ML guards can block them silently.
        _src_txt = str(
            p.get("source")
            or p.get("signal_source")
            or p.get("execution_owner")
            or p.get("owner")
            or p.get("engine")
            or p.get("strategy")
            or ""
        ).upper()

        _stageb_txt = str(
            p.get("stageb_state_machine")
            or p.get("stageb_status")
            or p.get("shadow_state")
            or p.get("status")
            or ""
        ).upper()

        _reason_txt = str(
            p.get("stageb_confirm_reason")
            or p.get("confirm_reason")
            or p.get("reason")
            or ""
        )

        _is_vps_smc_candidate = bool(
            "VPS_SMC" in _src_txt
            or "SMC" in _src_txt
            or p.get("smc_minimal_required_enabled")
            or p.get("smc_minimal_required")
            or p.get("stageb_confirmation")
            or _stageb_txt == "CONFIRMED"
            or _reason_txt == "minimal_sweep_reclaim_structural_sl_entry_sane"
            or str(p.get("selected_poi_type") or "").upper() == "SMC_MINIMAL"
        )

        if (
            _is_vps_smc_candidate
            and env_bool("LIVE_ENTRY_CONFLUENCE_GATE_ENABLED", False)
            and not isinstance(p.get("live_entry_confluence_gate_v1"), dict)
        ):
            try:
                from app.live_entry_confluence_gate_v1 import evaluate_live_entry_confluence_gate_v1
                _entry_conf = evaluate_live_entry_confluence_gate_v1(p)
            except Exception as _entry_e:
                _entry_conf = {
                    "ok": False,
                    "decision": "BLOCK",
                    "allow": False,
                    "gate": "live_entry_confluence_gate_v1",
                    "reason": "live_entry_confluence_error",
                    "error": str(_entry_e)[:240],
                }

            p["live_entry_confluence_gate_v1"] = _entry_conf

            if str((_entry_conf or {}).get("decision") or "").upper() == "BLOCK":
                decision = {
                    "decision": "NO_TRADE",
                    "reason": (_entry_conf or {}).get("reason") or "live_entry_confluence_block",
                    "gate": "live_entry_confluence_gate_v1",
                }
                try:
                    append_jsonl(DECISIONS_LOG, build_decision_log(p, decision, state))
                except Exception:
                    pass

                response = {
                    "ok": True,
                    "decision": "NO_TRADE",
                    "reason": decision["reason"],
                    "gate": "live_entry_confluence_gate_v1",
                    "signal_id": p.get("signal_id") or p.get("signal_key"),
                    "symbol": p.get("symbol"),
                    "direction": p.get("direction") or p.get("dir") or p.get("side"),
                    "execution_mode": current_execution_mode,
                    "live_entry_confluence_gate_v1": _entry_conf,
                }
                try:
                    fire_and_forget_ml_shadow_log(p, decision, response, state)
                except Exception:
                    pass
                return response

            if (
                env_bool("LIVE_RR12_PLAN_LOCK_ENABLED", True)
                and not isinstance(p.get("live_rr12_plan_lock_v1"), dict)
            ):
                try:
                    from app.live_rr12_plan_lock_v1 import apply_live_rr12_plan_lock_v1
                    _rr12_lock = apply_live_rr12_plan_lock_v1(p)
                except Exception as _rr12_e:
                    _rr12_lock = {
                        "ok": False,
                        "decision": "BLOCK",
                        "allow": False,
                        "gate": "live_rr12_plan_lock_v1",
                        "reason": "rr12_plan_lock_error",
                        "error": str(_rr12_e)[:240],
                    }

                p["live_rr12_plan_lock_v1"] = _rr12_lock

                if str((_rr12_lock or {}).get("decision") or "").upper() == "BLOCK":
                    decision = {
                        "decision": "NO_TRADE",
                        "reason": (_rr12_lock or {}).get("reason") or "live_rr12_plan_lock_block",
                        "gate": "live_rr12_plan_lock_v1",
                    }
                    try:
                        append_jsonl(DECISIONS_LOG, build_decision_log(p, decision, state))
                    except Exception:
                        pass

                    response = {
                        "ok": True,
                        "decision": "NO_TRADE",
                        "reason": decision["reason"],
                        "gate": "live_rr12_plan_lock_v1",
                        "signal_id": p.get("signal_id") or p.get("signal_key"),
                        "symbol": p.get("symbol"),
                        "direction": p.get("direction") or p.get("dir") or p.get("side"),
                        "execution_mode": current_execution_mode,
                        "live_entry_confluence_gate_v1": _entry_conf,
                        "live_rr12_plan_lock_v1": _rr12_lock,
                    }
                    try:
                        fire_and_forget_ml_shadow_log(p, decision, response, state)
                    except Exception:
                        pass
                    return response

                if isinstance((_rr12_lock or {}).get("payload"), dict):
                    p.update(dict((_rr12_lock or {}).get("payload") or {}))

        if current_execution_mode == "DISABLED":
            decision = {"decision": "REJECT", "reason": "execution_mode_disabled", "gate": "execution_mode_gate"}
            append_jsonl(DECISIONS_LOG, build_decision_log(p, decision, state))
            notify_signal_decision_async(p, decision)
            response = {"ok": True, "decision": decision["decision"], "reason": decision["reason"], "gate": decision["gate"], "signal_id": p.get("signal_id") or p.get("signal_key"), "execution_mode": current_execution_mode}
            fire_and_forget_ml_shadow_log(p, decision, response, state)
            return response

        normalize_res = normalize_tp_plan(p)
        p["plan_sanity_ok"] = bool(normalize_res.get("ok"))
        p["plan_sanity_reason"] = normalize_res.get("reason")
        p["plan_invalid"] = bool(normalize_res.get("plan_invalid"))

        rr_rewrite_res = apply_rr_single_target_rewrite(p)
        p["rr_rewrite_ok"] = bool(rr_rewrite_res.get("ok"))
        p["rr_rewrite_reason"] = rr_rewrite_res.get("rr_rewrite_reason") or rr_rewrite_res.get("reason")
        p["rr_target_mode"] = rr_rewrite_res.get("rr_target_mode") or p.get("rr_target_mode")
        p["rr_target_r"] = rr_rewrite_res.get("rr_target_r") or p.get("rr_target_r")
        p["rr_single_target_price"] = rr_rewrite_res.get("rr_single_target_price") or p.get("rr_single_target_price")

        if bool(rr_rewrite_res.get("rr_rewrite_applied")):
            normalize_res = normalize_tp_plan(p)
            p["plan_sanity_ok"] = bool(normalize_res.get("ok"))
            p["plan_sanity_reason"] = normalize_res.get("reason")
            p["plan_invalid"] = bool(normalize_res.get("plan_invalid"))

        if not bool(rr_rewrite_res.get("ok")):
            normalize_res = {"ok": False, "reason": rr_rewrite_res.get("reason") or rr_rewrite_res.get("rr_rewrite_reason") or "rr_rewrite_failed", "plan_invalid": True}
            p["plan_sanity_ok"] = False
            p["plan_sanity_reason"] = normalize_res.get("reason")
            p["plan_invalid"] = True

        if not bool(normalize_res.get("ok")):
            decision = {"decision": "REJECT", "reason": normalize_res.get("reason"), "gate": "plan_sanity_gate"}
            append_jsonl(DECISIONS_LOG, build_decision_log(p, decision, state))
            notify_signal_decision_async(p, decision)
            response = {"ok": True, "decision": decision["decision"], "reason": decision["reason"], "gate": decision["gate"], "signal_id": p.get("signal_id") or p.get("signal_key"), "execution_mode": current_execution_mode, "plan_sanity_ok": False, "plan_sanity_reason": normalize_res.get("reason"), "plan_invalid": True}
            fire_and_forget_ml_shadow_log(p, decision, response, state)
            return response

        if current_execution_mode == "LIVE_SMALL_CAPITAL":
            safety = v014_safety_summary(symbol=pair_to_binance_symbol(pair_of(p)), ignore_signal_key=signal_key_of(p))
            if not bool(safety.get("safe_to_continue")):
                decision = {"decision": "REJECT", "reason": "live_preflight_failed", "gate": "live_preflight_gate"}
                append_jsonl(DECISIONS_LOG, build_decision_log(p, decision, state))
                notify_signal_decision_async(p, decision)
                response = {"ok": True, "decision": decision["decision"], "reason": decision["reason"], "gate": decision["gate"], "signal_id": p.get("signal_id") or p.get("signal_key"), "execution_mode": current_execution_mode, "safety_summary": safety}
                fire_and_forget_ml_shadow_log(p, decision, response, state)
                return response

            live_res = handle_live_small_capital_execution(p, safety=safety)
            decision = {
                "decision": "ACCEPT" if str(live_res.get("decision")) == "LIVE_ORDER_PLACED" else "REJECT",
                "reason": live_res.get("decision") or live_res.get("reason"),
                "gate": "live_small_capital_execute_on_confirmed",
            }
            append_jsonl(DECISIONS_LOG, build_decision_log(p, decision, state))
            notify_signal_decision_async(p, decision)
            response = {
                "ok": bool(live_res.get("ok")),
                "decision": decision["decision"],
                "reason": decision["reason"],
                "gate": decision["gate"],
                "signal_id": p.get("signal_id") or p.get("signal_key"),
                "execution_mode": current_execution_mode,
                "live_execution": live_res,
            }
            fire_and_forget_ml_shadow_log(p, decision, response, state)
            return response

        if current_execution_mode == "PAPER":
            if env_bool("COST_GATE_ENABLED", True):
                plan = build_execution_plan(p)
                ok, reason = shared_validate_plan_cost_gate(plan, require_quantity=True)
                p["cost_gate_pass"] = bool(ok)
                p["cost_gate_reason"] = (None if ok else str(reason).split(":", 1)[1] if str(reason).startswith("cost_gate_failed:") else reason)
                if not ok:
                    gate = "cost_gate" if str(reason).startswith("cost_gate_failed:") else "plan_cost_gate"
                    decision = {"decision": "REJECT", "reason": str(reason), "gate": gate, "cost_gate_pass": False, "cost_gate_reason": p.get("cost_gate_reason")}
                    state = apply_decision_to_state(p, decision, state)
                    save_state(state)
                    append_jsonl(DECISIONS_LOG, build_decision_log(p, decision, state))
                    notify_signal_decision_async(p, decision)
                    response = {"ok": True, "decision": decision["decision"], "reason": decision["reason"], "gate": decision["gate"], "signal_id": p.get("signal_id") or p.get("signal_key"), "execution_mode": current_execution_mode, "cost_gate_pass": False, "cost_gate_reason": p.get("cost_gate_reason")}
                    fire_and_forget_ml_shadow_log(p, decision, response, state)
                    return response
            decision = paper_decide(p, state)
            if env_bool("ML_PREDICTION_ENABLED", True):
                score_res = score_ml_prediction_internal(signal_key_of(p), p)
                p["ml_probability_win"] = score_res.get("probability_win")
                p["ml_gate_mode"] = score_res.get("ml_gate_mode")
                p["ml_gate_decision"] = score_res.get("ml_decision")
                p["ml_gate_reason"] = score_res.get("reason")
                p["ml_model_version"] = score_res.get("model_version")
                if decision.get("decision") == "ACCEPT" and score_res.get("ml_decision") == "REJECT_BY_ML_GATE":
                    decision = {"decision": "REJECT", "reason": "ml_hard_gate_reject", "gate": "ml_gate"}
            state = apply_decision_to_state(p, decision, state)
            save_state(state)
            append_jsonl(DECISIONS_LOG, build_decision_log(p, decision, state))
            notify_signal_decision_async(p, decision)
            response = {"ok": True, "decision": decision["decision"], "reason": decision["reason"], "gate": decision["gate"], "signal_id": p.get("signal_id") or p.get("signal_key"), "execution_mode": current_execution_mode}
            fire_and_forget_ml_shadow_log(p, decision, response, state)
            return response

        decision = {"decision": "ACCEPT", "reason": "execution_mode_testnet_path", "gate": "execution_mode_gate"}
        if env_bool("ML_PREDICTION_ENABLED", True):
            score_res = score_ml_prediction_internal(signal_key_of(p), p)
            p["ml_probability_win"] = score_res.get("probability_win")
            p["ml_gate_mode"] = score_res.get("ml_gate_mode")
            p["ml_gate_decision"] = score_res.get("ml_decision")
            p["ml_gate_reason"] = score_res.get("reason")
            p["ml_model_version"] = score_res.get("model_version")
            if score_res.get("ml_decision") == "REJECT_BY_ML_GATE":
                decision = {"decision": "REJECT", "reason": "ml_hard_gate_reject", "gate": "ml_gate"}
                append_jsonl(DECISIONS_LOG, build_decision_log(p, decision, state))
                notify_signal_decision_async(p, decision)
                response = {"ok": True, "decision": decision["decision"], "reason": decision["reason"], "gate": decision["gate"], "signal_id": p.get("signal_id") or p.get("signal_key"), "execution_mode": current_execution_mode}
                fire_and_forget_ml_shadow_log(p, decision, response, state)
                return response
        append_jsonl(DECISIONS_LOG, build_decision_log(p, decision, state))
        notify_signal_decision_async(p, decision)
        response = {"ok": True, "decision": decision["decision"], "reason": decision["reason"], "gate": decision["gate"], "signal_id": p.get("signal_id") or p.get("signal_key"), "execution_mode": current_execution_mode}
        execution_event = handle_execution_after_accept(p)
        response["cost_gate_pass"] = execution_event.get("plan", {}).get("cost_gate_pass")
        response["cost_gate_reason"] = execution_event.get("plan", {}).get("cost_gate_reason")
        if execution_event.get("reason") == "cost_gate_failed":
            response["execution_skipped_reason"] = "cost_gate_failed"
        if current_execution_mode in ("TESTNET", "TESTNET_MARKET"):
            market_res = execution_event.get("market_order_result")
            response["market_order_result"] = market_res
            if not (market_res or {}).get("ok"):
                response["ok"] = False
                response["execution_error_reason"] = execution_event.get("reason")
        elif current_execution_mode == "TESTNET_ORDER_TEST":
            response["testnet_order_result"] = execution_event.get("order_test_result")
        fire_and_forget_ml_shadow_log(p, decision, response, state)
        return response


def _vps_execution_bridge(payload: Dict[str, Any]) -> Dict[str, Any]:
    p = dict(payload or {})
    p["signal_source"] = "VPS_SMC"
    p["source"] = "VPS_SMC"
    p["source_mode"] = "STAT_TECH_PRIMARY"
    p["execution_owner"] = "VPS_SMC"
    # === LIVE_ENTRY_CONFLUENCE_BRIDGE_FIRST_20260628 ===
    # VPS SMC minimal is binary only.
    # Final context scoring must happen here before legacy refresh/recompute/ML blocks.
    # Flow: SMC minimal PASS -> confluence deriv/macro/quant -> RR12 lock -> legacy execution guards.
    if env_bool("LIVE_ENTRY_CONFLUENCE_GATE_ENABLED", False):
        try:
            from app.live_entry_confluence_gate_v1 import evaluate_live_entry_confluence_gate_v1
            _entry_conf = evaluate_live_entry_confluence_gate_v1(p)
        except Exception as _entry_e:
            _entry_conf = {
                "ok": False,
                "decision": "BLOCK",
                "allow": False,
                "gate": "live_entry_confluence_gate_v1",
                "reason": "live_entry_confluence_error",
                "error": str(_entry_e)[:240],
            }

        p["live_entry_confluence_gate_v1"] = _entry_conf

        if str((_entry_conf or {}).get("decision") or "").upper() == "BLOCK":
            return {
                "ok": True,
                "decision": "NO_TRADE",
                "reason": (_entry_conf or {}).get("reason") or "live_entry_confluence_block",
                "gate": "live_entry_confluence_gate_v1",
                "signal_id": p.get("signal_id") or p.get("signal_key"),
                "symbol": p.get("symbol"),
                "direction": p.get("direction") or p.get("dir") or p.get("side"),
                "execution_mode": execution_mode(),
                "live_entry_confluence_gate_v1": _entry_conf,
            }

        if env_bool("LIVE_RR12_PLAN_LOCK_ENABLED", True):
            try:
                from app.live_rr12_plan_lock_v1 import apply_live_rr12_plan_lock_v1
                _rr12_lock = apply_live_rr12_plan_lock_v1(p)
            except Exception as _rr12_e:
                _rr12_lock = {
                    "ok": False,
                    "decision": "BLOCK",
                    "allow": False,
                    "gate": "live_rr12_plan_lock_v1",
                    "reason": "rr12_plan_lock_error",
                    "error": str(_rr12_e)[:240],
                }

            p["live_rr12_plan_lock_v1"] = _rr12_lock

            if str((_rr12_lock or {}).get("decision") or "").upper() == "BLOCK":
                return {
                    "ok": True,
                    "decision": "NO_TRADE",
                    "reason": (_rr12_lock or {}).get("reason") or "live_rr12_plan_lock_block",
                    "gate": "live_rr12_plan_lock_v1",
                    "signal_id": p.get("signal_id") or p.get("signal_key"),
                    "symbol": p.get("symbol"),
                    "direction": p.get("direction") or p.get("dir") or p.get("side"),
                    "execution_mode": execution_mode(),
                    "live_entry_confluence_gate_v1": _entry_conf,
                    "live_rr12_plan_lock_v1": _rr12_lock,
                }

            if isinstance((_rr12_lock or {}).get("payload"), dict):
                p = dict((_rr12_lock or {}).get("payload") or p)

    if not (
        ("plan_sanity_ok" in p)
        and ("tp_normalized" in p)
        and ("raw_tp1" in p or "raw_tp2" in p or "raw_tp3" in p)
    ):
        normalize_tp_plan(p)
    return _process_signal_pipeline(p)
# SMC_RUNTIME_REMOVED_STAT_TECH_PRIMARY_20260628: SMC bridge registration removed. STAT_TECH_V1 owns live signal path.
@app.get("/paper/state")
def paper_state(
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)

    state = load_state()
    return {
        "ok": True,
        "mode": get_mode(),
        "open_paper_positions": open_paper_count(state),
        "seen_signal_keys_count": len(state.get("seen_signal_keys") or []),
        "state": state,
    }


@app.post("/paper/reset")
def paper_reset(
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)

    state = default_state()
    save_state(state)

    return {
        "ok": True,
        "reason": "paper_state_reset",
        "mode": get_mode(),
    }

@app.get("/paper/positions")
def paper_positions(
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)

    state = load_state()
    open_positions = [
        p for p in (state.get("open_paper_positions") or [])
        if str(p.get("status", "OPEN")).upper() == "OPEN"
    ]
    closed_positions = state.get("closed_paper_positions") or []

    return {
        "ok": True,
        "app_version": APP_VERSION,
        "mode": get_mode(),
        "open_paper_positions": len(open_positions),
        "closed_paper_positions": len(closed_positions),
        "positions": open_positions,
        "closed_recent": closed_positions[-20:],
    }

@app.post("/paper/close")
def paper_close(
    req: PaperClosePayload,
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)

    with LOCK:
        state = load_state()
        res = close_one_position_in_state(
            state=state,
            signal_key=req.signal_key,
            outcome=req.outcome,
            close_reason=req.close_reason or "MANUAL_CLOSE",
            close_price=req.close_price,
            notes=req.notes,
        )

        if res.get("ok"):
            save_state(state)

        return res

@app.post("/paper/close-all")
def paper_close_all(
    req: Optional[PaperCloseAllPayload] = None,
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)

    req = req or PaperCloseAllPayload()

    with LOCK:
        state = load_state()
        res = close_all_positions_in_state(
            state=state,
            outcome=req.outcome,
            close_reason=req.close_reason or "MANUAL_CLOSE_ALL",
            close_price=req.close_price,
            notes=req.notes,
        )
        save_state(state)
        return res

@app.get("/risk/daily")
def risk_daily(
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)

    state = load_state()
    dc = ensure_daily_counters(state)

    max_trades = env_int("MAX_TRADES_PER_DAY", 5)
    accepted = int(dc.get("accepted_count") or 0)
    remaining = max(0, max_trades - accepted) if max_trades > 0 else 999999

    ds_on, ds_reason = daily_stop_active(state)
    ks_on, ks_reason = kill_switch_active()

    return {
        "ok": True,
        "app_version": APP_VERSION,
        "mode": get_mode(),
        "date_wib": dc.get("date_wib"),
        "max_trades_per_day": max_trades,
        "accepted_count": accepted,
        "rejected_count": int(dc.get("rejected_count") or 0),
        "remaining_trades": remaining,
        "accepted_by_pair": dc.get("accepted_by_pair") or {},
        "rejected_by_gate": dc.get("rejected_by_gate") or {},
        "kill_switch": ks_on,
        "kill_switch_reason": ks_reason,
        "daily_stop_active": ds_on,
        "daily_stop_reason": ds_reason,
    }

@app.post("/risk/reset-daily")
def risk_reset_daily(
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)

    with LOCK:
        state = load_state()
        today = current_wib_date()

        state["daily_counters"] = {
            "date_wib": today,
            "accepted_count": 0,
            "rejected_count": 0,
            "accepted_by_pair": {},
            "rejected_by_gate": {},
        }

        save_state(state)

        return {
            "ok": True,
            "reason": "daily_counters_reset",
            "mode": get_mode(),
            "date_wib": today,
            "daily_counters": state["daily_counters"],
        }

@app.get("/exchange/filters")
def exchange_filters(
    symbol: str = "BTCUSDT",
    force: bool = False,
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)

    symbol = str(symbol or "").strip().upper()
    si = symbol_exchange_info(symbol, force=force)

    if not si.get("ok"):
        return {
            "ok": False,
            "app_version": APP_VERSION,
            "mode": get_mode(),
            "binance_env": binance_env(),
            "symbol": symbol,
            "reason": si.get("reason"),
            "detail": si,
        }

    filters = parse_symbol_filters(si.get("symbol_info") or {})

    return {
        "ok": True,
        "app_version": APP_VERSION,
        "mode": get_mode(),
        "binance_env": binance_env(),
        "source": si.get("source"),
        "symbol": symbol,
        "filters": filters,
    }


@app.get("/exchange/qty-test")
def exchange_qty_test(
    symbol: str = "BTCUSDT",
    entry: float = 100000.0,
    sl: float = 99000.0,
    force: bool = False,
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)

    symbol = str(symbol or "").strip().upper()
    side = "BUY"

    plan = {
        "symbol": symbol,
        "entry_side": side,
        "entry_mid": entry,
        "sl": sl,
        "risk_usdt": env_int("TESTNET_RISK_USDT_PER_TRADE", 5),
        "notional_usdt_cap": env_int("TESTNET_MAX_NOTIONAL_USDT", 50),
    }

    qty_res = calculate_order_quantity(plan, force_exchange_info=force)

    return {
        "ok": bool(qty_res.get("ok")),
        "app_version": APP_VERSION,
        "mode": get_mode(),
        "binance_env": binance_env(),
        "symbol": symbol,
        "qty_result": qty_res,
    }


@app.post("/testnet/cost-check")
async def v016_testnet_cost_check_endpoint(request: Request):
    if not v010_auth_ok(request):
        return {"ok": False, "decision": "REJECT", "reason": "unauthorized"}
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    plan = {
        "symbol": str(payload.get("symbol") or "").strip().upper(),
        "direction": str(payload.get("direction") or "Long").strip().title(),
        "entry_mid": payload.get("entry_price"),
        "quantity_float": payload.get("quantity"),
        "tp1": payload.get("tp1"),
        "tp2": payload.get("tp2"),
        "tp3": payload.get("tp3"),
        "tp1_qty": payload.get("tp1_qty"),
        "tp2_qty": payload.get("tp2_qty"),
        "tp3_qty": payload.get("tp3_qty"),
    }
    if not plan.get("tp1_qty") and not plan.get("tp2_qty") and not plan.get("tp3_qty"):
        ensure_tp_split(plan)
    breakdown = compute_cost_gate(plan)
    return {"ok": True, "cost_gate_pass": breakdown.get("cost_gate_pass"), "cost_gate_reason": breakdown.get("cost_gate_reason"), "cost_breakdown": breakdown}


# =========================
# v0.10 TESTNET close / reduce-only skeleton
# =========================

def v010_secret_value() -> str:
    env_secret = str(os.getenv("WEBHOOK_SECRET", "")).strip()
    if env_secret:
        return env_secret

    secret_path = Path("state/private/webhook_secret_current.txt")
    if secret_path.exists():
        return secret_path.read_text().strip()

    return ""


def v010_auth_ok(request: Request) -> bool:
    expected = v010_secret_value()
    got = str(request.headers.get("X-Webhook-Secret", "")).strip()
    return bool(expected) and got == expected


def v010_normalize_symbol(raw: Any) -> str:
    s = str(raw or "").strip().upper()
    if ":" in s:
        s = s.split(":", 1)[1]
    s = s.replace(".P", "")
    s = s.replace("/", "")
    s = s.replace("-", "")
    return s


def v010_testnet_allowed_symbol(symbol: str) -> bool:
    raw = str(os.getenv("TESTNET_ALLOWED_SYMBOLS", "")).strip()
    if not raw:
        return False
    allowed = [x.strip().upper() for x in raw.replace(";", ",").split(",") if x.strip()]
    return str(symbol or "").upper() in allowed


def v010_base_event(symbol: str, action: str, reason: str = "") -> Dict[str, Any]:
    return {
        "event_at_utc": utc_now_iso(),
        "event_at_wib": wib_now_iso(),
        "app_version": APP_VERSION,
        "execution_mode": execution_mode(),
        "binance_env": binance_env(),
        "action": action,
        "symbol": symbol,
        "reason": reason,
    }


def binance_testnet_position_risk(symbol: str = "") -> Dict[str, Any]:
    if binance_env() != "TESTNET":
        return {"ok": False, "reason": "binance_env_not_testnet"}

    if live_binance_key_detected():
        return {"ok": False, "reason": "live_binance_key_detected_abort"}

    params = {}
    symbol = v010_normalize_symbol(symbol)
    if symbol:
        params["symbol"] = symbol

    res = binance_testnet_signed_request("GET", "/fapi/v2/positionRisk", params)

    # Normalize response body for easier validation
    body = res.get("body")
    positions = []
    if isinstance(body, list):
        positions = body
    elif isinstance(body, dict) and isinstance(body.get("positions"), list):
        positions = body.get("positions")
    elif isinstance(body, dict) and body.get("symbol"):
        positions = [body]

    return {
        "ok": bool(res.get("ok")),
        "reason": res.get("reason"),
        "http_status": res.get("http_status"),
        "body": body,
        "positions": positions,
        "raw": res,
    }


def v010_find_open_position(position_res: Dict[str, Any], symbol: str) -> Dict[str, Any]:
    symbol = v010_normalize_symbol(symbol)
    for pos in position_res.get("positions") or []:
        if str(pos.get("symbol", "")).upper() != symbol:
            continue
        try:
            amt = Decimal(str(pos.get("positionAmt", "0")))
        except Exception:
            amt = Decimal("0")
        if amt != 0:
            return {"ok": True, "position": pos, "position_amt": str(amt)}
    return {"ok": False, "reason": "NO_POSITION", "position": None, "position_amt": "0"}


def binance_testnet_close_position_reduce_only(symbol: str) -> Dict[str, Any]:
    symbol = v010_normalize_symbol(symbol)

    if not symbol:
        return {"ok": False, "decision": "TESTNET_CLOSE_REJECTED", "reason": "missing_symbol"}

    if binance_env() != "TESTNET":
        return {"ok": False, "decision": "TESTNET_CLOSE_REJECTED", "reason": "binance_env_not_testnet"}

    if live_binance_key_detected():
        return {"ok": False, "decision": "TESTNET_CLOSE_REJECTED", "reason": "live_binance_key_detected_abort"}

    if not v010_testnet_allowed_symbol(symbol):
        return {"ok": False, "decision": "TESTNET_CLOSE_REJECTED", "reason": f"symbol_not_allowed_for_testnet:{symbol}"}

    if not env_bool("ENABLE_TESTNET_ORDERS", False):
        return {"ok": False, "decision": "TESTNET_CLOSE_REJECTED", "reason": "enable_testnet_orders_false"}

    if env_bool("ORDER_TEST_ENDPOINT_ONLY", True):
        return {"ok": False, "decision": "TESTNET_CLOSE_REJECTED", "reason": "order_test_endpoint_only_true"}

    if env_bool("TESTNET_KILL_SWITCH", False):
        return {"ok": False, "decision": "TESTNET_CLOSE_REJECTED", "reason": "testnet_kill_switch_true"}

    pos_res = binance_testnet_position_risk(symbol)
    if not pos_res.get("ok"):
        return {
            "ok": False,
            "decision": "TESTNET_CLOSE_REJECTED",
            "reason": pos_res.get("reason") or "position_risk_fetch_failed",
            "position_risk_result": pos_res,
        }

    open_pos = v010_find_open_position(pos_res, symbol)
    if not open_pos.get("ok"):
        return {
            "ok": True,
            "decision": "TESTNET_NO_POSITION",
            "reason": "NO_POSITION",
            "position_risk_result": pos_res,
        }

    pos = open_pos["position"]
    amt = Decimal(str(open_pos["position_amt"]))
    close_side = "SELL" if amt > 0 else "BUY"
    qty = str(abs(amt).normalize())

    params = {
        "symbol": symbol,
        "side": close_side,
        "type": "MARKET",
        "quantity": qty,
        "reduceOnly": "true",
        "newClientOrderId": safe_client_order_id("V10CLOSE", symbol),
        "newOrderRespType": "RESULT",
    }

    close_res = binance_testnet_signed_request("POST", "/fapi/v1/order", params)

    return {
        "ok": bool(close_res.get("ok")),
        "decision": "TESTNET_CLOSE_SENT" if close_res.get("ok") else "TESTNET_CLOSE_REJECTED",
        "reason": "binance_testnet_reduce_only_close_called" if close_res.get("ok") else (close_res.get("reason") or "binance_testnet_reduce_only_close_failed"),
        "symbol": symbol,
        "position_amt_before": str(amt),
        "close_side": close_side,
        "close_quantity": qty,
        "position_risk_result": pos_res,
        "close_order_result": close_res,
    }


@app.post("/testnet/position-risk")
async def v010_testnet_position_risk_endpoint(request: Request):
    if not v010_auth_ok(request):
        return {"ok": False, "decision": "REJECT", "reason": "unauthorized"}

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    symbol = v010_normalize_symbol(payload.get("symbol") or payload.get("pair") or "")
    res = binance_testnet_position_risk(symbol)

    event = v010_base_event(symbol, "TESTNET_POSITION_RISK", res.get("reason") or "position_risk_requested")
    event.update({
        "decision": "POSITION_RISK_OK" if res.get("ok") else "POSITION_RISK_FAILED",
        "position_risk_result": res,
    })
    append_jsonl(EXECUTION_EVENTS_LOG, event)

    return {
        "ok": bool(res.get("ok")),
        "symbol": symbol,
        "reason": res.get("reason"),
        "http_status": res.get("http_status"),
        "positions": res.get("positions"),
    }


@app.post("/testnet/close-position")
async def v010_testnet_close_position_endpoint(request: Request):
    if not v010_auth_ok(request):
        event = v010_base_event("", "TESTNET_CLOSE_POSITION", "unauthorized")
        event.update({"decision": "TESTNET_CLOSE_REJECTED"})
        append_jsonl(EXECUTION_EVENTS_LOG, event)
        return {"ok": False, "decision": "REJECT", "reason": "unauthorized"}

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    symbol = v010_normalize_symbol(payload.get("symbol") or payload.get("pair") or "")

    close_res = binance_testnet_close_position_reduce_only(symbol)

    event = v010_base_event(symbol, "TESTNET_CLOSE_POSITION", close_res.get("reason") or "")
    event.update({
        "decision": close_res.get("decision"),
        "position_amt_before": close_res.get("position_amt_before"),
        "close_side": close_res.get("close_side"),
        "close_quantity": close_res.get("close_quantity"),
        "position_risk_result": close_res.get("position_risk_result"),
        "close_order_result": close_res.get("close_order_result"),
        "close_result": close_res,
    })
    append_jsonl(EXECUTION_EVENTS_LOG, event)

    return close_res


# =========================
# v0.11 Protective SL/TP Planning Skeleton
# Planning only: no actual SL/TP orders are sent.
# =========================

EXECUTION_PLANS_LOG = Path("logs/execution_plans.jsonl")


def v011_d(x: Any, default: str = "0") -> Decimal:
    try:
        if x is None or x == "":
            return Decimal(default)
        return Decimal(str(x))
    except Exception:
        return Decimal(default)


def v011_decimal_str(x: Decimal) -> str:
    try:
        s = format(x.normalize(), "f")
        if "." in s:
            s = s.rstrip("0").rstrip(".")
        return s if s else "0"
    except Exception:
        return str(x)


def v011_floor_to_step(qty: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return qty
    return (qty / step).to_integral_value(rounding=ROUND_DOWN) * step


def v011_testnet_base_url() -> str:
    raw = (
        os.getenv("BINANCE_TESTNET_BASE_URL")
        or os.getenv("BINANCE_FUTURES_TESTNET_BASE_URL")
        or "https://testnet.binancefuture.com"
    )
    return str(raw).rstrip("/")


def v011_fetch_exchange_filters(symbol: str) -> Dict[str, Any]:
    import json
    import urllib.request

    symbol = v010_normalize_symbol(symbol)
    url = v011_testnet_base_url() + "/fapi/v1/exchangeInfo"

    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            body = json.loads(r.read().decode("utf-8"))
    except Exception as e:
        return {"ok": False, "reason": f"exchange_info_fetch_failed:{type(e).__name__}:{e}"}

    sym = None
    for item in body.get("symbols", []):
        if str(item.get("symbol", "")).upper() == symbol:
            sym = item
            break

    if not sym:
        return {"ok": False, "reason": f"symbol_not_found_in_exchange_info:{symbol}"}

    filters_raw = sym.get("filters", [])
    by_type = {str(f.get("filterType", "")): f for f in filters_raw}

    lot = by_type.get("LOT_SIZE", {}) or {}
    market_lot = by_type.get("MARKET_LOT_SIZE", {}) or {}
    price_filter = by_type.get("PRICE_FILTER", {}) or {}
    min_notional_filter = by_type.get("MIN_NOTIONAL", {}) or {}

    step_size = (
        market_lot.get("stepSize")
        or lot.get("stepSize")
        or "0"
    )
    min_qty = (
        market_lot.get("minQty")
        or lot.get("minQty")
        or "0"
    )
    max_qty = (
        market_lot.get("maxQty")
        or lot.get("maxQty")
        or "0"
    )
    min_notional = (
        min_notional_filter.get("notional")
        or min_notional_filter.get("minNotional")
        or "0"
    )

    return {
        "ok": True,
        "symbol": symbol,
        "status": sym.get("status"),
        "step_size": str(step_size),
        "min_qty": str(min_qty),
        "max_qty": str(max_qty),
        "tick_size": str(price_filter.get("tickSize", "0")),
        "min_notional": str(min_notional),
        "quantity_precision": sym.get("quantityPrecision"),
        "price_precision": sym.get("pricePrecision"),
        "raw_filter_types": [str(f.get("filterType")) for f in filters_raw],
    }


def v011_validate_single_qty(symbol: str, qty: Decimal, price: Decimal, filters: Dict[str, Any], label: str) -> Dict[str, Any]:
    step = v011_d(filters.get("step_size"))
    min_qty = v011_d(filters.get("min_qty"))
    max_qty = v011_d(filters.get("max_qty"))
    min_notional = v011_d(filters.get("min_notional"))

    if qty <= 0:
        return {"ok": False, "reason": f"{label}_qty_not_positive:{v011_decimal_str(qty)}"}

    if min_qty > 0 and qty < min_qty:
        return {
            "ok": False,
            "reason": f"{label}_qty_below_min_qty:{v011_decimal_str(qty)}<{v011_decimal_str(min_qty)}",
        }

    if max_qty > 0 and qty > max_qty:
        return {
            "ok": False,
            "reason": f"{label}_qty_above_max_qty:{v011_decimal_str(qty)}>{v011_decimal_str(max_qty)}",
        }

    if step > 0:
        floored = v011_floor_to_step(qty, step)
        if floored != qty:
            return {
                "ok": False,
                "reason": f"{label}_qty_not_step_aligned:{v011_decimal_str(qty)} step={v011_decimal_str(step)}",
            }

    notional = qty * price
    if min_notional > 0 and notional < min_notional:
        return {
            "ok": False,
            "reason": f"{label}_notional_below_min:{v011_decimal_str(notional)}<{v011_decimal_str(min_notional)}",
        }

    return {
        "ok": True,
        "label": label,
        "qty": v011_decimal_str(qty),
        "price": v011_decimal_str(price),
        "notional": v011_decimal_str(notional),
    }


def v011_build_tp_quantities(position_qty: Decimal, filters: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    step = v011_d(filters.get("step_size"))
    if step <= 0:
        step = Decimal("0.00000001")

    explicit = payload.get("tp_qtys") or payload.get("tp_quantities")
    if isinstance(explicit, list) and len(explicit) == 3:
        q1 = v011_floor_to_step(v011_d(explicit[0]), step)
        q2 = v011_floor_to_step(v011_d(explicit[1]), step)
        q3 = v011_floor_to_step(v011_d(explicit[2]), step)
    else:
        pct = payload.get("tp_split_pct") or payload.get("tp_splits_pct") or [33, 33, 34]
        if not isinstance(pct, list) or len(pct) != 3:
            return {"ok": False, "reason": "tp_split_pct_must_be_list_of_3"}

        p1 = v011_d(pct[0]) / Decimal("100")
        p2 = v011_d(pct[1]) / Decimal("100")

        q1 = v011_floor_to_step(position_qty * p1, step)
        q2 = v011_floor_to_step(position_qty * p2, step)
        q3 = v011_floor_to_step(position_qty - q1 - q2, step)

    total = q1 + q2 + q3

    if q1 <= 0 or q2 <= 0 or q3 <= 0:
        return {
            "ok": False,
            "reason": f"tp_split_qty_not_positive:{v011_decimal_str(q1)},{v011_decimal_str(q2)},{v011_decimal_str(q3)}",
        }

    if total > position_qty:
        return {
            "ok": False,
            "reason": f"tp_split_qty_exceeds_position:{v011_decimal_str(total)}>{v011_decimal_str(position_qty)}",
        }

    return {
        "ok": True,
        "tp_qtys": [q1, q2, q3],
        "tp_qtys_str": [v011_decimal_str(q1), v011_decimal_str(q2), v011_decimal_str(q3)],
        "tp_total_qty": v011_decimal_str(total),
    }


def v011_reference_prices(payload: Dict[str, Any], position: Dict[str, Any]) -> Dict[str, Any]:
    refs = []

    for k in ["entry_mid", "entry_price", "fill_price", "avgPrice"]:
        val = v011_d(payload.get(k))
        if val > 0:
            refs.append({"label": k, "price": val})

    for k in ["entryPrice", "markPrice"]:
        val = v011_d(position.get(k))
        if val > 0:
            refs.append({"label": k, "price": val})

    if not refs:
        return {"ok": False, "reason": "no_valid_reference_price"}

    return {
        "ok": True,
        "refs": refs,
        "max_ref": max(r["price"] for r in refs),
        "min_ref": min(r["price"] for r in refs),
        "refs_out": [{"label": r["label"], "price": v011_decimal_str(r["price"])} for r in refs],
    }


def v011_validate_prices(direction: str, sl: Decimal, tps: list, refs: Dict[str, Any]) -> Dict[str, Any]:
    max_ref = refs["max_ref"]
    min_ref = refs["min_ref"]

    if direction == "LONG":
        if not (sl < min_ref):
            return {
                "ok": False,
                "reason": f"long_sl_not_below_reference:sl={v011_decimal_str(sl)} min_ref={v011_decimal_str(min_ref)}",
            }
        for i, tp in enumerate(tps, 1):
            if not (tp > max_ref):
                return {
                    "ok": False,
                    "reason": f"long_tp{i}_not_above_reference:tp={v011_decimal_str(tp)} max_ref={v011_decimal_str(max_ref)}",
                }
        return {"ok": True}

    if direction == "SHORT":
        if not (sl > max_ref):
            return {
                "ok": False,
                "reason": f"short_sl_not_above_reference:sl={v011_decimal_str(sl)} max_ref={v011_decimal_str(max_ref)}",
            }
        for i, tp in enumerate(tps, 1):
            if not (tp < min_ref):
                return {
                    "ok": False,
                    "reason": f"short_tp{i}_not_below_reference:tp={v011_decimal_str(tp)} min_ref={v011_decimal_str(min_ref)}",
                }
        return {"ok": True}

    return {"ok": False, "reason": f"unknown_direction:{direction}"}


def build_v011_protection_plan(payload: Dict[str, Any]) -> Dict[str, Any]:
    symbol = v010_normalize_symbol(payload.get("symbol") or payload.get("pair") or "")
    mock = bool(payload.get("mock") or payload.get("mock_position"))

    if not symbol:
        return {"ok": False, "decision": "PROTECTION_PLAN_REJECTED", "reason": "missing_symbol"}

    if binance_env() != "TESTNET":
        return {"ok": False, "decision": "PROTECTION_PLAN_REJECTED", "reason": "binance_env_not_testnet", "symbol": symbol}

    if live_binance_key_detected():
        return {"ok": False, "decision": "PROTECTION_PLAN_REJECTED", "reason": "live_binance_key_detected_abort", "symbol": symbol}

    if not v010_testnet_allowed_symbol(symbol):
        return {"ok": False, "decision": "PROTECTION_PLAN_REJECTED", "reason": f"symbol_not_allowed_for_testnet:{symbol}", "symbol": symbol}

    sl = v011_d(payload.get("sl") or payload.get("stop_loss") or payload.get("invalid"))
    tp1 = v011_d(payload.get("tp1") or payload.get("tp_1") or payload.get("take_profit_1"))
    tp2 = v011_d(payload.get("tp2") or payload.get("tp_2") or payload.get("take_profit_2"))
    # Hotfix: allow v0.13 lifecycle/protection payload with only tp1+tp2.
    # If tp3 is not provided, mirror tp2 for a valid 3-leg protective plan.
    tp3 = v011_d(payload.get("tp3") or payload.get("tp_3") or payload.get("take_profit_3") or payload.get("tp2"))

    if sl <= 0 or tp1 <= 0 or tp2 <= 0 or tp3 <= 0:
        return {"ok": False, "decision": "PROTECTION_PLAN_REJECTED", "reason": "missing_or_invalid_sl_tp", "symbol": symbol}

    filters = v011_fetch_exchange_filters(symbol)
    if not filters.get("ok"):
        return {"ok": False, "decision": "PROTECTION_PLAN_REJECTED", "reason": filters.get("reason"), "symbol": symbol}

    position_risk_result = None

    if mock:
        position_amt = v011_d(payload.get("mock_position_amt") or payload.get("position_amt") or payload.get("quantity"))
        position = {
            "symbol": symbol,
            "positionAmt": v011_decimal_str(position_amt),
            "entryPrice": str(payload.get("entry_mid") or payload.get("entry_price") or "0"),
            "markPrice": str(payload.get("mark_price") or payload.get("entry_mid") or payload.get("entry_price") or "0"),
            "positionSide": "BOTH",
            "source": "MOCK_ONLY",
        }
    else:
        position_risk_result = binance_testnet_position_risk(symbol)
        if not position_risk_result.get("ok"):
            return {
                "ok": False,
                "decision": "PROTECTION_PLAN_REJECTED",
                "reason": position_risk_result.get("reason") or "position_risk_failed",
                "symbol": symbol,
                "position_risk_result": position_risk_result,
            }

        open_pos = v010_find_open_position(position_risk_result, symbol)
        if not open_pos.get("ok"):
            return {
                "ok": True,
                "decision": "NO_POSITION",
                "reason": "NO_POSITION",
                "symbol": symbol,
                "position_risk_result": position_risk_result,
            }

        position = open_pos["position"]
        position_amt = v011_d(open_pos.get("position_amt"))

    if position_amt == 0:
        return {"ok": True, "decision": "NO_POSITION", "reason": "NO_POSITION", "symbol": symbol}

    direction = "LONG" if position_amt > 0 else "SHORT"
    position_qty = abs(position_amt)

    side = "SELL" if direction == "LONG" else "BUY"

    refs = v011_reference_prices(payload, position)
    if not refs.get("ok"):
        return {"ok": False, "decision": "PROTECTION_PLAN_REJECTED", "reason": refs.get("reason"), "symbol": symbol}

    price_validation = v011_validate_prices(direction, sl, [tp1, tp2, tp3], refs)
    if not price_validation.get("ok"):
        return {
            "ok": False,
            "decision": "PROTECTION_PLAN_REJECTED",
            "reason": price_validation.get("reason"),
            "symbol": symbol,
            "direction": direction,
            "reference_prices": refs.get("refs_out"),
        }

    tp_qtys = v011_build_tp_quantities(position_qty, filters, payload)
    if not tp_qtys.get("ok"):
        return {
            "ok": False,
            "decision": "PROTECTION_PLAN_REJECTED",
            "reason": tp_qtys.get("reason"),
            "symbol": symbol,
            "direction": direction,
        }

    qty_checks = []
    qty_checks.append(v011_validate_single_qty(symbol, position_qty, sl, filters, "sl_full"))

    for i, (qty, price) in enumerate(zip(tp_qtys["tp_qtys"], [tp1, tp2, tp3]), 1):
        qty_checks.append(v011_validate_single_qty(symbol, qty, price, filters, f"tp{i}"))

    bad = [x for x in qty_checks if not x.get("ok")]
    if bad:
        return {
            "ok": False,
            "decision": "PROTECTION_PLAN_REJECTED",
            "reason": bad[0].get("reason"),
            "symbol": symbol,
            "direction": direction,
            "qty_checks": qty_checks,
        }

    plan = {
        "plan_type": "PROTECTIVE_SL_TP_PLANNING_ONLY",
        "no_actual_orders": True,
        "symbol": symbol,
        "source": "MOCK_ONLY" if mock else "BINANCE_TESTNET_POSITION_RISK",
        "direction": direction,
        "position_amt": v011_decimal_str(position_amt),
        "position_qty": v011_decimal_str(position_qty),
        "reference_prices": refs.get("refs_out"),
        "filters": filters,
        "sl_plan": {
            "type": "STOP_MARKET",
            "side": side,
            "stop_price": v011_decimal_str(sl),
            "quantity": v011_decimal_str(position_qty),
            "reduceOnly": True,
            "workingType": "CONTRACT_PRICE",
            "send_to_binance": False,
        },
        "tp_plans": [
            {
                "label": "TP1",
                "type": "TAKE_PROFIT_MARKET",
                "side": side,
                "stop_price": v011_decimal_str(tp1),
                "quantity": tp_qtys["tp_qtys_str"][0],
                "reduceOnly": True,
                "workingType": "CONTRACT_PRICE",
                "send_to_binance": False,
            },
            {
                "label": "TP2",
                "type": "TAKE_PROFIT_MARKET",
                "side": side,
                "stop_price": v011_decimal_str(tp2),
                "quantity": tp_qtys["tp_qtys_str"][1],
                "reduceOnly": True,
                "workingType": "CONTRACT_PRICE",
                "send_to_binance": False,
            },
            {
                "label": "TP3",
                "type": "TAKE_PROFIT_MARKET",
                "side": side,
                "stop_price": v011_decimal_str(tp3),
                "quantity": tp_qtys["tp_qtys_str"][2],
                "reduceOnly": True,
                "workingType": "CONTRACT_PRICE",
                "send_to_binance": False,
            },
        ],
        "qty_checks": qty_checks,
    }

    return {
        "ok": True,
        "decision": "PROTECTION_PLAN_BUILT",
        "reason": "protection_plan_planning_only",
        "symbol": symbol,
        "direction": direction,
        "plan": plan,
        "position_risk_result": position_risk_result,
    }


@app.post("/testnet/protection-plan")
async def v011_testnet_protection_plan_endpoint(request: Request):
    if not v010_auth_ok(request):
        event = {
            "event_at_utc": utc_now_iso(),
            "event_at_wib": wib_now_iso(),
            "app_version": APP_VERSION,
            "action": "TESTNET_PROTECTION_PLAN",
            "decision": "PROTECTION_PLAN_REJECTED",
            "reason": "unauthorized",
        }
        append_jsonl(EXECUTION_EVENTS_LOG, event)
        return {"ok": False, "decision": "REJECT", "reason": "unauthorized"}

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    res = build_v011_protection_plan(payload)

    event = {
        "event_at_utc": utc_now_iso(),
        "event_at_wib": wib_now_iso(),
        "app_version": APP_VERSION,
        "action": "TESTNET_PROTECTION_PLAN",
        "binance_env": binance_env(),
        "execution_mode": execution_mode(),
        "symbol": res.get("symbol") or v010_normalize_symbol(payload.get("symbol") or payload.get("pair") or ""),
        "decision": res.get("decision"),
        "reason": res.get("reason"),
        "direction": res.get("direction"),
        "no_actual_orders": True,
        "plan": res.get("plan"),
    }

    append_jsonl(EXECUTION_EVENTS_LOG, event)

    if res.get("decision") == "PROTECTION_PLAN_BUILT":
        append_jsonl(EXECUTION_PLANS_LOG, event)

    return res


# =========================
# v0.12 TESTNET Protective SL/TP Order Placement Skeleton
# Actual Binance Futures TESTNET protective orders only.
# Depends on v0.11 planner. Apps Script still never places orders.
# =========================

import json as v012_json
import os as v012_os
import time as v012_time
import hmac as v012_hmac
import hashlib as v012_hashlib
import urllib.parse as v012_urlparse
import urllib.request as v012_urlrequest
import urllib.error as v012_urlerror
from pathlib import Path as V012Path

V012_PROTECTION_STATE_PATH = V012Path("state/testnet_protection_orders.json")
PROTECTION_STORE = "TESTNET_ALGO_PROTECTION"


def v012_env_bool(name: str, default: bool = False) -> bool:
    val = str(v012_os.getenv(name, "")).strip().lower()
    if val in ("1", "true", "yes", "y", "on"):
        return True
    if val in ("0", "false", "no", "n", "off"):
        return False
    return default


def v012_get_execution_mode() -> str:
    try:
        return str(execution_mode()).upper()
    except Exception:
        return str(v012_os.getenv("EXECUTION_MODE", "")).upper()


def v012_get_binance_env() -> str:
    try:
        return str(binance_env()).upper()
    except Exception:
        return str(v012_os.getenv("BINANCE_ENV", "")).upper()


def v012_live_key_detected() -> bool:
    try:
        return bool(live_binance_key_detected())
    except Exception:
        # defensive fallback: any non-testnet/live-looking key env should block
        suspicious = [
            "BINANCE_API_KEY",
            "BINANCE_API_SECRET",
            "BINANCE_LIVE_API_KEY",
            "BINANCE_LIVE_API_SECRET",
        ]
        return any(bool(v012_os.getenv(k)) for k in suspicious)


def v012_testnet_allowed_symbol(symbol: str) -> bool:
    try:
        return bool(v010_testnet_allowed_symbol(symbol))
    except Exception:
        raw = v012_os.getenv("TESTNET_ALLOWED_SYMBOLS", "")
        allow = [x.strip().upper() for x in raw.split(",") if x.strip()]
        return symbol.upper() in allow


def v012_testnet_base_url() -> str:
    return str(
        v012_os.getenv("BINANCE_TESTNET_BASE_URL")
        or v012_os.getenv("BINANCE_FUTURES_TESTNET_BASE_URL")
        or "https://testnet.binancefuture.com"
    ).rstrip("/")


def v012_api_credentials() -> dict:
    api_key = (
        v012_os.getenv("BINANCE_TESTNET_API_KEY")
        or v012_os.getenv("TESTNET_BINANCE_API_KEY")
        or v012_os.getenv("BINANCE_FUTURES_TESTNET_API_KEY")
        or ""
    )
    api_secret = (
        v012_os.getenv("BINANCE_TESTNET_API_SECRET")
        or v012_os.getenv("TESTNET_BINANCE_API_SECRET")
        or v012_os.getenv("BINANCE_FUTURES_TESTNET_API_SECRET")
        or ""
    )
    return {
        "ok": bool(api_key and api_secret),
        "api_key": api_key,
        "api_secret": api_secret,
        "reason": None if api_key and api_secret else "missing_testnet_api_credentials",
    }


def v012_signed_request(method: str, path: str, params: dict | None = None) -> dict:
    v012_block_legacy_protection_cancel(path, params or {})
    creds = v012_api_credentials()
    if not creds.get("ok"):
        return {"ok": False, "reason": creds.get("reason"), "http_status": None, "body": None}

    params = dict(params or {})
    params["timestamp"] = int(v012_time.time() * 1000)
    params.setdefault("recvWindow", 5000)

    query = v012_urlparse.urlencode(params, doseq=True)
    sig = v012_hmac.new(
        creds["api_secret"].encode("utf-8"),
        query.encode("utf-8"),
        v012_hashlib.sha256
    ).hexdigest()

    full_query = query + "&signature=" + sig
    url = v012_testnet_base_url() + path + "?" + full_query

    req = v012_urlrequest.Request(
        url,
        headers={"X-MBX-APIKEY": creds["api_key"]},
        method=method.upper()
    )

    if method.upper() in ("POST", "PUT", "DELETE"):
        req.data = b""

    try:
        with v012_urlrequest.urlopen(req, timeout=15) as r:
            raw = r.read().decode("utf-8")
            try:
                body = v012_json.loads(raw)
            except Exception:
                body = raw
            return {
                "ok": 200 <= int(r.status) < 300,
                "http_status": int(r.status),
                "body": body,
                "reason": None,
                "path": path,
                "method": method.upper(),
            }
    except v012_urlerror.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        try:
            body = v012_json.loads(raw)
        except Exception:
            body = raw
        return {
            "ok": False,
            "http_status": int(e.code),
            "body": body,
            "reason": "binance_http_error",
            "path": path,
            "method": method.upper(),
        }
    except Exception as e:
        return {
            "ok": False,
            "http_status": None,
            "body": None,
            "reason": f"binance_request_exception:{type(e).__name__}:{e}",
            "path": path,
            "method": method.upper(),
        }


def v012_load_protection_state() -> dict:
    try:
        if not V012_PROTECTION_STATE_PATH.exists():
            return {"symbols": {}}
        data = v012_json.loads(V012_PROTECTION_STATE_PATH.read_text())
        if not isinstance(data, dict):
            return {"symbols": {}}
        data.setdefault("symbols", {})
        return data
    except Exception:
        return {"symbols": {}}


def v012_save_protection_state(state: dict) -> None:
    V012_PROTECTION_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    V012_PROTECTION_STATE_PATH.write_text(v012_json.dumps(state, indent=2, sort_keys=True))


def v012_store_protection_orders(symbol: str, record: dict) -> None:
    state = v012_load_protection_state()
    state.setdefault("symbols", {})
    state["symbols"].setdefault(symbol, [])
    state["symbols"][symbol].append(record)
    v012_save_protection_state(state)


def v012_known_orders(symbol: str) -> list:
    state = v012_load_protection_state()
    rows = state.get("symbols", {}).get(symbol, [])
    if isinstance(rows, list):
        return rows
    return []


def v012_mark_cancel_result(symbol: str, cancel_results: list) -> None:
    state = v012_load_protection_state()
    rows = state.get("symbols", {}).get(symbol, [])
    canceled_ids = set()
    for r in cancel_results:
        body = r.get("response", {}).get("body") or {}
        oid = str(body.get("algoId") or r.get("algoId") or "")
        if oid:
            canceled_ids.add(oid)

    for order in rows:
        oid = str(order.get("algoId") or "")
        if oid in canceled_ids:
            order["status"] = "CANCEL_REQUESTED"
            order["canceled_at"] = utc_now_iso()

    state.setdefault("symbols", {})[symbol] = rows
    v012_save_protection_state(state)


def v012_require_place_mode(symbol: str) -> dict:
    if v012_get_binance_env() != "TESTNET":
        return {"ok": False, "reason": "binance_env_not_testnet"}

    if v012_live_key_detected():
        return {"ok": False, "reason": "live_binance_key_detected_abort"}

    if not v012_testnet_allowed_symbol(symbol):
        return {"ok": False, "reason": f"symbol_not_allowed_for_testnet:{symbol}"}

    if not v012_env_bool("ENABLE_TESTNET_ORDERS", False):
        return {"ok": False, "reason": "enable_testnet_orders_false"}

    if v012_env_bool("ORDER_TEST_ENDPOINT_ONLY", True):
        return {"ok": False, "reason": "order_test_endpoint_only_true"}

    if v012_env_bool("TESTNET_KILL_SWITCH", False):
        return {"ok": False, "reason": "testnet_kill_switch_active"}

    if v012_env_bool("KILL_SWITCH", False):
        return {"ok": False, "reason": "global_kill_switch_active"}

    if v012_get_execution_mode() != "TESTNET_MARKET":
        return {"ok": False, "reason": f"execution_mode_not_testnet_market:{v012_get_execution_mode()}"}

    creds = v012_api_credentials()
    if not creds.get("ok"):
        return {"ok": False, "reason": creds.get("reason")}

    return {"ok": True, "reason": "place_mode_ok"}


def v012_require_cancel_mode(symbol: str) -> dict:
    # Cancel is cleanup/safety path. It must stay TESTNET-only, but can run even after EXECUTION_MODE restored.
    if v012_get_binance_env() != "TESTNET":
        return {"ok": False, "reason": "binance_env_not_testnet"}

    if v012_live_key_detected():
        return {"ok": False, "reason": "live_binance_key_detected_abort"}

    if not v012_testnet_allowed_symbol(symbol):
        return {"ok": False, "reason": f"symbol_not_allowed_for_testnet:{symbol}"}

    if v012_env_bool("TESTNET_KILL_SWITCH", False):
        return {"ok": False, "reason": "testnet_kill_switch_active"}

    if v012_env_bool("KILL_SWITCH", False):
        return {"ok": False, "reason": "global_kill_switch_active"}

    creds = v012_api_credentials()
    if not creds.get("ok"):
        return {"ok": False, "reason": creds.get("reason")}

    return {"ok": True, "reason": "cancel_mode_ok"}


def v012_place_protective_order(symbol: str, label: str, plan_item: dict, client_prefix: str) -> dict:
    side = str(plan_item.get("side", "")).upper()
    order_type = str(plan_item.get("type", "")).upper()
    qty = str(plan_item.get("quantity", ""))
    stop_price = str(plan_item.get("stop_price", ""))
    working_type = str(v012_os.getenv("TESTNET_PROTECTION_WORKING_TYPE", plan_item.get("workingType") or "MARK_PRICE")).upper()

    client_order_id = f"{client_prefix}_{label}_{int(v012_time.time() * 1000)}"[:36]

    params = {
        "algoType": "CONDITIONAL",
        "symbol": symbol,
        "side": side,
        "type": order_type,
        "quantity": qty,
        "triggerPrice": stop_price,
        "reduceOnly": "true",
        "workingType": working_type,
        "clientAlgoId": client_order_id,
    }

    response = v012_signed_request("POST", "/fapi/v1/algoOrder", params)

    event = {
        "event_at_utc": utc_now_iso(),
        "event_at_wib": wib_now_iso(),
        "app_version": APP_VERSION,
        "action": "TESTNET_PROTECTION_ORDER_PLACE",
        "symbol": symbol,
        "label": label,
        "request": {
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "quantity": qty,
            "stopPrice": stop_price,
            "reduceOnly": True,
            "workingType": working_type,
            "newClientOrderId": client_order_id,
        },
        "response": response,
        "ok": response.get("ok"),
        "http_status": response.get("http_status"),
        "reason": response.get("reason"),
    }
    append_jsonl(EXECUTION_EVENTS_LOG, event)

    return {
        "ok": bool(response.get("ok")),
        "label": label,
        "clientOrderId": client_order_id,
        "request": event["request"],
        "response": response,
        "orderId": (response.get("body") or {}).get("algoId") if isinstance(response.get("body"), dict) else None,
        "algoId": (response.get("body") or {}).get("algoId") if isinstance(response.get("body"), dict) else None,
    }




def v012_build_protection_store_record(symbol: str, signal_key: str, order: dict) -> dict:
    response_body = (order.get("response") or {}).get("body") or {}
    return {
        "symbol": symbol,
        "signal_key": signal_key,
        "algoId": response_body.get("algoId") or order.get("algoId") or order.get("orderId"),
        "clientAlgoId": response_body.get("clientAlgoId") or order.get("clientAlgoId") or order.get("clientOrderId"),
        "type": (order.get("request") or {}).get("type"),
        "side": (order.get("request") or {}).get("side"),
        "stopPrice": (order.get("request") or {}).get("stopPrice"),
        "quantity": (order.get("request") or {}).get("quantity"),
        "status": "PLACED" if order.get("ok") else "FAILED",
        "created_at": utc_now_iso(),
        "canceled_at": None,
        "source": PROTECTION_STORE,
    }


def v012_store_protection_records(symbol: str, signal_key: str, orders: list) -> None:
    for order in orders:
        v012_store_protection_orders(symbol, v012_build_protection_store_record(symbol, signal_key, order))


def binance_get_open_algo_orders(symbol: str) -> dict:
    params = {"symbol": symbol}
    return v012_signed_request("GET", "/fapi/v1/openAlgoOrders", params)


def v012_clean_algo_order_row(symbol: str, row: dict) -> dict:
    return {
        "symbol": symbol,
        "algoId": row.get("algoId"),
        "clientAlgoId": row.get("clientAlgoId"),
        "type": row.get("type"),
        "side": row.get("side"),
        "stopPrice": row.get("stopPrice") or row.get("triggerPrice"),
        "quantity": row.get("quantity") or row.get("origQty"),
        "status": row.get("status"),
    }


def v012_block_legacy_protection_cancel(path: str, params: dict) -> None:
    if path != "/fapi/v1/order":
        return
    p = params or {}
    protection_marker = str(p.get("_protection_cancel", "")).lower() == "true" or str(p.get("algoType", "")).upper() == "CONDITIONAL" or bool(p.get("algoId") or p.get("clientAlgoId"))
    if protection_marker:
        raise RuntimeError("legacy_order_cancel_for_protection_blocked")

def v012_cancel_order(symbol: str, order_id=None, orig_client_order_id=None) -> dict:
    """
    v0.12a management fix:
    Protection orders are Binance Algo Orders.
    NEVER cancel protection via /fapi/v1/order.
    order_id param is treated as algoId for backward compatibility.
    orig_client_order_id is treated as clientAlgoId.
    """
    params = {"symbol": symbol}

    if order_id:
        params["algoId"] = order_id
    elif orig_client_order_id:
        params["clientAlgoId"] = orig_client_order_id
    else:
        return {"ok": False, "reason": "missing_algo_id_or_client_algo_id"}

    response = v012_signed_request("DELETE", "/fapi/v1/algoOrder", params)

    event = {
        "event_at_utc": utc_now_iso(),
        "event_at_wib": wib_now_iso(),
        "app_version": APP_VERSION,
        "action": "TESTNET_PROTECTION_ALGO_CANCEL",
        "symbol": symbol,
        "algoId": order_id,
        "clientAlgoId": orig_client_order_id,
        "response": response,
        "ok": response.get("ok"),
        "http_status": response.get("http_status"),
        "reason": response.get("reason"),
    }
    append_jsonl(EXECUTION_EVENTS_LOG, event)

    return {
        "ok": bool(response.get("ok")),
        "algoId": order_id,
        "clientAlgoId": orig_client_order_id,
        "response": response,
    }

@app.post("/testnet/place-protection")
async def v012_place_protection_endpoint(request: Request):
    if not v010_auth_ok(request):
        event = {
            "event_at_utc": utc_now_iso(),
            "event_at_wib": wib_now_iso(),
            "app_version": APP_VERSION,
            "action": "TESTNET_PLACE_PROTECTION",
            "decision": "REJECT",
            "reason": "unauthorized",
        }
        append_jsonl(EXECUTION_EVENTS_LOG, event)
        return {"ok": False, "decision": "REJECT", "reason": "unauthorized"}

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    symbol = v010_normalize_symbol(payload.get("symbol") or payload.get("pair") or "")
    if not symbol:
        return {"ok": False, "decision": "PROTECTION_REJECTED", "reason": "missing_symbol"}

    guard = v012_require_place_mode(symbol)
    if not guard.get("ok"):
        event = {
            "event_at_utc": utc_now_iso(),
            "event_at_wib": wib_now_iso(),
            "app_version": APP_VERSION,
            "action": "TESTNET_PLACE_PROTECTION",
            "symbol": symbol,
            "decision": "PROTECTION_REJECTED",
            "reason": guard.get("reason"),
        }
        append_jsonl(EXECUTION_EVENTS_LOG, event)
        return {"ok": False, "decision": "PROTECTION_REJECTED", "reason": guard.get("reason"), "symbol": symbol}

    plan_res = build_v011_protection_plan(payload)
    if plan_res.get("decision") != "PROTECTION_PLAN_BUILT":
        event = {
            "event_at_utc": utc_now_iso(),
            "event_at_wib": wib_now_iso(),
            "app_version": APP_VERSION,
            "action": "TESTNET_PLACE_PROTECTION",
            "symbol": symbol,
            "decision": "PROTECTION_REJECTED",
            "reason": plan_res.get("reason"),
            "planner_decision": plan_res.get("decision"),
        }
        append_jsonl(EXECUTION_EVENTS_LOG, event)
        return {
            "ok": False,
            "decision": "PROTECTION_REJECTED",
            "reason": plan_res.get("reason"),
            "planner_decision": plan_res.get("decision"),
            "symbol": symbol,
        }

    plan = plan_res.get("plan") or {}
    signal_key = str(payload.get("signal_key") or payload.get("signal_id") or f"V012_{symbol}_{int(v012_time.time())}")
    client_prefix = ("V012_" + symbol + "_" + str(abs(hash(signal_key)) % 999999))[:18]

    placed = []

    # Hard rule: place SL first. If SL fails, do not place TPs.
    sl_result = v012_place_protective_order(symbol, "SL", plan.get("sl_plan") or {}, client_prefix)
    placed.append(sl_result)

    if not sl_result.get("ok"):
        decision = "PROTECTION_SL_FAILED"
        record = {
            "created_at_utc": utc_now_iso(),
            "created_at_wib": wib_now_iso(),
            "signal_key": signal_key,
            "decision": decision,
            "symbol": symbol,
            "direction": plan.get("direction"),
            "orders": placed,
            "plan": plan,
        }
        v012_store_protection_records(symbol, signal_key, placed)
        sl_failed_event = {
            "event_at_utc": utc_now_iso(),
            "event_at_wib": wib_now_iso(),
            "app_version": APP_VERSION,
            "action": "TESTNET_PLACE_PROTECTION",
            "symbol": symbol,
            "decision": decision,
            "reason": "sl_failed_tp_skipped",
            "orders": placed,
        }
        append_jsonl(EXECUTION_EVENTS_LOG, sl_failed_event)
        v014_execution_summary_write(signal_key, {
            "symbol": symbol,
            "sl_algo_id": sl_result.get("algoId"),
            "lifecycle_state": "PROTECTION_SL_FAILED",
            "notes": "sl_failed_tp_skipped",
        })
        return {
            "ok": False,
            "decision": decision,
            "reason": "sl_failed_tp_skipped",
            "symbol": symbol,
            "orders": placed,
        }

    for tp in plan.get("tp_plans") or []:
        label = str(tp.get("label") or "TP")
        placed.append(v012_place_protective_order(symbol, label, tp, client_prefix))

    failed = [x for x in placed if not x.get("ok")]
    decision = "PROTECTION_PLACED" if not failed else "PARTIAL_PROTECTION"

    record = {
        "created_at_utc": utc_now_iso(),
        "created_at_wib": wib_now_iso(),
        "signal_key": signal_key,
        "decision": decision,
        "symbol": symbol,
        "direction": plan.get("direction"),
        "orders": placed,
        "plan": plan,
    }
    v012_store_protection_records(symbol, signal_key, placed)

    event = {
        "event_at_utc": utc_now_iso(),
        "event_at_wib": wib_now_iso(),
        "app_version": APP_VERSION,
        "action": "TESTNET_PLACE_PROTECTION",
        "symbol": symbol,
        "decision": decision,
        "reason": "all_protection_orders_placed" if decision == "PROTECTION_PLACED" else "some_protection_orders_failed_cleanup_required",
        "orders": placed,
    }
    append_jsonl(EXECUTION_EVENTS_LOG, event)
    latest_by_label = {str(o.get("label") or "").upper(): o for o in placed}
    v014_execution_summary_write(signal_key, {
        "symbol": symbol,
        "sl_algo_id": (latest_by_label.get("SL") or {}).get("algoId"),
        "tp1_algo_id": (latest_by_label.get("TP1") or {}).get("algoId"),
        "tp2_algo_id": (latest_by_label.get("TP2") or {}).get("algoId"),
        "tp3_algo_id": (latest_by_label.get("TP3") or {}).get("algoId"),
        "lifecycle_state": decision,
        "notes": event.get("reason"),
    })

    tp_map = {str(t.get("label") or "").upper(): t for t in (plan.get("tp_plans") or [])}
    sl_plan = plan.get("sl_plan") or {}
    initial_qty_fallback = v017_d(plan.get("initial_qty") or "0")
    if initial_qty_fallback <= Decimal("0"):
        initial_qty_fallback = v017_d(plan.get("quantity") or "0")
    if initial_qty_fallback <= Decimal("0"):
        initial_qty_fallback = sum(abs(v017_d(tp.get("quantity") or "0")) for tp in (plan.get("tp_plans") or []))
    sl_algo_id = (latest_by_label.get("SL") or {}).get("algoId")
    lifecycle_stage_seed = "ENTRY_PROTECTED" if (decision == "PROTECTION_PLACED" and sl_algo_id) else "NEEDS_REVIEW"
    v017_upsert_tp_lifecycle(signal_key, {
        "symbol": symbol,
        "direction": plan.get("direction"),
        "entry_price": plan.get("entry_price"),
        "initial_qty": str(initial_qty_fallback),
        "current_position_qty": str(initial_qty_fallback),
        "tp1_price": (tp_map.get("TP1") or {}).get("stop_price"),
        "tp2_price": (tp_map.get("TP2") or {}).get("stop_price"),
        "tp3_price": (tp_map.get("TP3") or {}).get("stop_price"),
        "tp1_qty": (tp_map.get("TP1") or {}).get("quantity"),
        "tp2_qty": (tp_map.get("TP2") or {}).get("quantity"),
        "tp3_qty": (tp_map.get("TP3") or {}).get("quantity"),
        "initial_sl_price": sl_plan.get("stop_price"),
        "current_sl_algo_id": sl_algo_id,
        "current_sl_client_algo_id": (latest_by_label.get("SL") or {}).get("clientOrderId"),
        "tp1_algo_id": (latest_by_label.get("TP1") or {}).get("algoId"),
        "tp2_algo_id": (latest_by_label.get("TP2") or {}).get("algoId"),
        "tp3_algo_id": (latest_by_label.get("TP3") or {}).get("algoId"),
        "tp1_processed": False,
        "tp2_processed": False,
        "be_moved": False,
        "lock_profit_moved": False,
        "lifecycle_stage": lifecycle_stage_seed,
    })

    return {
        "ok": decision == "PROTECTION_PLACED",
        "decision": decision,
        "reason": event["reason"],
        "symbol": symbol,
        "orders": placed,
        "cleanup_required": decision == "PARTIAL_PROTECTION",
    }


@app.api_route("/testnet/algo-open-orders", methods=["GET", "POST"])
async def v012_algo_open_orders_endpoint(request: Request):
    if not v010_auth_ok(request):
        return {"ok": False, "decision": "REJECT", "reason": "unauthorized"}

    payload = {}
    if request.method.upper() == "POST":
        try:
            payload = await request.json()
        except Exception:
            payload = {}

    symbol = v010_normalize_symbol(
        request.query_params.get("symbol")
        or payload.get("symbol")
        or payload.get("pair")
        or ""
    )
    if not symbol:
        return {"ok": False, "reason": "missing_symbol"}

    res = binance_get_open_algo_orders(symbol)
    body = res.get("body")
    raw_rows = body if isinstance(body, list) else (body.get("orders") if isinstance(body, dict) else [])
    rows = [v012_clean_algo_order_row(symbol, r) for r in (raw_rows or []) if isinstance(r, dict)]
    return {
        "ok": bool(res.get("ok")),
        "symbol": symbol,
        "orders": rows,
        "http_status": res.get("http_status"),
        "reason": res.get("reason"),
    }


@app.post("/testnet/cancel-protection")
async def v012_cancel_protection_endpoint(request: Request):
    if not v010_auth_ok(request):
        event = {
            "event_at_utc": utc_now_iso(),
            "event_at_wib": wib_now_iso(),
            "app_version": APP_VERSION,
            "action": "TESTNET_CANCEL_PROTECTION",
            "decision": "REJECT",
            "reason": "unauthorized",
        }
        append_jsonl(EXECUTION_EVENTS_LOG, event)
        return {"ok": False, "decision": "REJECT", "reason": "unauthorized"}

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    symbol = v010_normalize_symbol(payload.get("symbol") or payload.get("pair") or "")
    if not symbol:
        return {"ok": False, "decision": "CANCEL_REJECTED", "reason": "missing_symbol"}

    guard = v012_require_cancel_mode(symbol)
    if not guard.get("ok"):
        event = {
            "event_at_utc": utc_now_iso(),
            "event_at_wib": wib_now_iso(),
            "app_version": APP_VERSION,
            "action": "TESTNET_CANCEL_PROTECTION",
            "symbol": symbol,
            "decision": "CANCEL_REJECTED",
            "reason": guard.get("reason"),
        }
        append_jsonl(EXECUTION_EVENTS_LOG, event)
        return {"ok": False, "decision": "CANCEL_REJECTED", "reason": guard.get("reason"), "symbol": symbol}

    explicit_order_ids = payload.get("order_ids") or payload.get("orderIds") or []
    explicit_client_ids = payload.get("client_order_ids") or payload.get("clientOrderIds") or []

    cancel_targets = []

    for oid in explicit_order_ids:
        cancel_targets.append({"orderId": oid, "origClientOrderId": None})

    for cid in explicit_client_ids:
        cancel_targets.append({"orderId": None, "origClientOrderId": cid})

    if not cancel_targets:
        for order in v012_known_orders(symbol):
            oid = order.get("algoId")
            cid = order.get("clientAlgoId")
            if oid or cid:
                cancel_targets.append({"orderId": oid, "origClientOrderId": cid})

    # de-dupe
    seen = set()
    unique_targets = []
    for t in cancel_targets:
        key = str(t.get("orderId") or "") + "|" + str(t.get("origClientOrderId") or "")
        if key not in seen and key != "|":
            seen.add(key)
            unique_targets.append(t)

    results = []
    for t in unique_targets:
        results.append(v012_cancel_order(symbol, t.get("orderId"), t.get("origClientOrderId")))

    v012_mark_cancel_result(symbol, results)

    decision = "PROTECTION_CANCEL_DONE" if results else "NO_KNOWN_PROTECTION_ORDERS"
    event = {
        "event_at_utc": utc_now_iso(),
        "event_at_wib": wib_now_iso(),
        "app_version": APP_VERSION,
        "action": "TESTNET_CANCEL_PROTECTION",
        "symbol": symbol,
        "decision": decision,
        "cancel_count": len(results),
        "results": results,
    }
    append_jsonl(EXECUTION_EVENTS_LOG, event)

    return {
        "ok": True,
        "decision": decision,
        "symbol": symbol,
        "cancel_count": len(results),
        "results": results,
    }


def v013_env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, str(default))).strip().lower()
    return raw in ("1", "true", "yes", "on")


def v013_lifecycle_guard(symbol: str) -> Dict[str, Any]:
    env = str(binance_env() or "").upper()
    if env != "TESTNET":
        return {"ok": False, "decision": "REJECT", "reason": "binance_env_not_testnet"}
    if v013_env_bool("TESTNET_KILL_SWITCH", False):
        return {"ok": False, "decision": "REJECT", "reason": "testnet_kill_switch_enabled"}
    if v013_env_bool("KILL_SWITCH", False):
        return {"ok": False, "decision": "REJECT", "reason": "kill_switch_enabled"}
    if live_binance_key_detected():
        return {"ok": False, "decision": "REJECT", "reason": "live_key_detected"}
    if not v010_testnet_allowed_symbol(symbol):
        return {"ok": False, "decision": "REJECT", "reason": "symbol_not_allowed"}
    return {"ok": True}


def v013_extract_position_amt(position_res: Dict[str, Any], symbol: str) -> str:
    open_pos = v010_find_open_position(position_res, symbol) or {}
    # v010_find_open_position returns key: position_amt (not positionAmt).
    return str(open_pos.get("position_amt") or open_pos.get("positionAmt") or "0")


def v013_fetch_open_algo_orders(symbol: str) -> Dict[str, Any]:
    res = binance_get_open_algo_orders(symbol)
    body = res.get("body")
    raw_rows = body if isinstance(body, list) else (body.get("orders") if isinstance(body, dict) else [])
    rows = [v012_clean_algo_order_row(symbol, row) for row in (raw_rows or []) if isinstance(row, dict)]
    return {"ok": bool(res.get("ok")), "orders": rows, "raw": res}


def v013_detect_lifecycle_state(position_amt: str, open_algo_orders_count: int, fetch_ok: bool) -> str:
    if not fetch_ok:
        return "POSITION_UNKNOWN"
    try:
        has_position = abs(float(position_amt)) > 0.0
    except Exception:
        has_position = str(position_amt).strip() not in ("", "0", "0.0", "0.00")
    if has_position and open_algo_orders_count > 0:
        return "POSITION_OPEN_PROTECTED"
    if has_position and open_algo_orders_count == 0:
        return "POSITION_OPEN_UNPROTECTED"
    if (not has_position) and open_algo_orders_count == 0:
        return "POSITION_CLOSED_CLEAN"
    return "POSITION_CLOSED_STALE_ALGO"


def v013_cancel_stale_algo_orders(symbol: str, open_algo_orders: list) -> list:
    cleanup_results = []
    for order in open_algo_orders or []:
        if not isinstance(order, dict):
            continue
        algo_id = order.get("algoId") or order.get("orderId")
        client_algo_id = order.get("clientAlgoId") or order.get("origClientOrderId")
        cleanup_results.append(v012_cancel_order(symbol, algo_id, client_algo_id))
    return cleanup_results


def v013_log_lifecycle_event(event: Dict[str, Any]) -> None:
    append_jsonl(EXECUTION_EVENTS_LOG, event)


def v014_is_nonzero_position_amt(position_amt: Any) -> bool:
    try:
        return abs(float(str(position_amt or "0").strip())) > 0.0
    except Exception:
        return str(position_amt).strip() not in ("", "0", "0.0", "0.00")


def v014_open_paper_positions_for_symbol(symbol: str, ignore_signal_key: str = "") -> list:
    state = load_state()
    symbol = v010_normalize_symbol(symbol)
    ignore_signal_key = str(ignore_signal_key or "").strip()
    rows = []
    for row in (state.get("open_paper_positions") or []):
        if str(row.get("status", "OPEN")).upper() != "OPEN":
            continue
        if ignore_signal_key and str(row.get("signal_key") or "").strip() == ignore_signal_key:
            continue
        row_symbol = v010_normalize_symbol(row.get("pair") or row.get("symbol") or "")
        if row_symbol == symbol:
            rows.append(row)
    return rows


def v014_reconcile_state(symbol: str = "", signal_key: str = "", ignore_signal_key: str = "") -> Dict[str, Any]:
    symbol = v010_normalize_symbol(symbol or "")
    paper_rows = v014_open_paper_positions_for_symbol(symbol, ignore_signal_key=ignore_signal_key) if symbol else (load_state().get("open_paper_positions") or [])
    paper_open = any(str(r.get("status", "OPEN")).upper() == "OPEN" for r in paper_rows)

    if symbol:
        if execution_mode() == "LIVE_SMALL_CAPITAL" and binance_env() == "LIVE":
            position_res = live_futures_position_risk(symbol)
            algo_res = live_fetch_open_orders(symbol)
        else:
            position_res = binance_testnet_position_risk(symbol)
            algo_res = v013_fetch_open_algo_orders(symbol)
    else:
        position_res = {"ok": False, "reason": "symbol_required_for_position_risk"}
        algo_res = {"ok": False, "orders": [], "raw": {"reason": "symbol_required_for_algo_orders"}}

    position_amt = "0"
    if symbol and position_res.get("ok"):
        position_amt = v013_extract_position_amt(position_res, symbol)
    open_orders = algo_res.get("orders") or []
    open_algo_count = len(open_orders)

    has_position = v014_is_nonzero_position_amt(position_amt)
    bot_state_detected = paper_open
    reasons = []
    mismatch_state = "UNKNOWN"

    if symbol and (not position_res.get("ok") or not algo_res.get("ok")):
        mismatch_state = "UNKNOWN"
        reasons.append(position_res.get("reason") or (algo_res.get("raw") or {}).get("reason") or "exchange_fetch_failed")
    elif has_position and open_algo_count > 0:
        mismatch_state = "POSITION_OPEN_PROTECTED"
        if not bot_state_detected:
            reasons.append("binance_position_open_protected_but_bot_state_not_open")
    elif has_position and open_algo_count == 0:
        mismatch_state = "UNPROTECTED_POSITION"
        reasons.append("position_open_without_protection_orders")
    elif bot_state_detected and (not has_position):
        mismatch_state = "STATE_OPEN_NO_POSITION"
        reasons.append("bot_or_paper_state_open_but_binance_position_closed")
    elif (not has_position) and open_algo_count > 0:
        mismatch_state = "STALE_ALGO_NO_POSITION"
        reasons.append("open_algo_orders_exist_without_open_position")
    elif (not has_position) and open_algo_count == 0 and (not paper_open):
        mismatch_state = "CLEAN"
    else:
        mismatch_state = "UNKNOWN"
        reasons.append("unable_to_classify_state_conservatively")

    ok = mismatch_state != "UNKNOWN"
    return {
        "ok": ok,
        "symbol": symbol or None,
        "signal_key": signal_key or None,
        "bot_state_detected": bot_state_detected,
        "paper_position_detected": paper_open,
        "binance_position_amt": position_amt,
        "open_algo_count": open_algo_count,
        "open_algo_orders": open_orders,
        "mismatch_state": mismatch_state,
        "reasons": reasons,
        "cleanup_required": mismatch_state in ("STALE_ALGO_NO_POSITION", "UNPROTECTED_POSITION"),
        "timestamp_utc": utc_now_iso(),
    }


@app.on_event("startup")
def startup_market_data_collector() -> None:
    try:
        res = market_data_start_background()
        print(f"[market_data] startup={res}")
    except Exception as e:
        print(f"[market_data] startup failed (non-fatal): {e}")




@app.get("/vps-smc/status")
def vps_smc_status_endpoint(
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)
    return vps_smc.vps_smc_status()


@app.post("/vps-smc/run-once")
def vps_smc_run_once_endpoint(
    payload: Optional[VpsSmcRunOncePayload] = None,
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)
    symbols = payload.symbols if payload else None
    return vps_smc.vps_smc_run_once(symbols)


@app.get("/vps-smc/signals/latest")
def vps_smc_signals_latest_endpoint(
    symbol: str,
    limit: int = 10,
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)
    return vps_smc.vps_smc_latest_signals(symbol=symbol, limit=limit)


@app.post("/vps-smc/compare")
def vps_smc_compare_endpoint(
    payload: Optional[VpsSmcComparePayload] = None,
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)
    req = payload or VpsSmcComparePayload()
    return vps_smc.vps_smc_compare(
        lookback_minutes=req.lookback_minutes,
        symbols=req.symbols,
        run_vps_first=bool(req.run_vps_first),
    )


@app.post("/vps-smc/debug/replay")
def vps_smc_debug_replay_endpoint(
    payload: VpsSmcDebugReplayPayload,
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)
    return vps_smc.vps_smc_debug_replay(
        symbol=payload.symbol,
        as_of_wib=payload.as_of_wib or payload.replay_at_wib,
        as_of_utc=payload.as_of_utc or payload.replay_at_utc,
    )


@app.post("/vps-smc/mirror-gsheet")
def vps_smc_mirror_gsheet_endpoint(
    payload: Optional[VpsSmcMirrorPayload] = None,
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)
    req = payload or VpsSmcMirrorPayload()
    return vps_smc.vps_smc_mirror_gsheet(target=req.target or "ALL", limit=int(req.limit or 100), force=bool(req.force))


@app.get("/vps-smc/scheduler/status")
def vps_smc_scheduler_status_endpoint(
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)
    return vps_smc.vps_smc_scheduler_status()




@app.get("/vps-smc/diagnostics/latest")
def vps_smc_diagnostics_latest_endpoint(
    limit: Optional[int] = 1,
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
):
    verify_secret(x_signal_secret, x_webhook_secret)
    return vps_smc.vps_smc_diagnostics_latest(limit=limit or 1)

@app.post("/vps-smc/scheduler/run-once")
def vps_smc_scheduler_run_once_endpoint(
    payload: Optional[VpsSmcSchedulerRunOncePayload] = None,
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)
    req = payload or VpsSmcSchedulerRunOncePayload()
    return vps_smc.vps_smc_scheduler_run_once(
        symbols=req.symbols,
        run_vps=bool(req.run_vps),
        run_compare=bool(req.run_compare),
        mirror_gsheet=bool(req.mirror_gsheet),
        lookback_minutes=int(req.lookback_minutes or 360),
    )

@app.get("/market/candles/status")
def market_candles_status(
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)
    symbols = market_allowlist_symbols()
    intervals = market_intervals()
    last_closed_candle: Dict[str, Dict[str, Any]] = {}
    for symbol in symbols:
        last_closed_candle[symbol] = {}
        for interval in intervals:
            cached = (MARKET_LAST_CLOSED.get(symbol) or {}).get(interval)
            if isinstance(cached, dict) and cached:
                last_closed_candle[symbol][interval] = cached
                continue
            rows = market_load_candles(symbol, interval)
            if rows:
                latest = sorted(rows, key=lambda x: int(x.get("open_time_ms") or 0))[-1]
                last_closed_candle[symbol][interval] = latest
    return {
        "ok": True,
        "enabled": market_enabled(),
        "symbols": symbols,
        "intervals": intervals,
        "last_closed_candle": last_closed_candle,
        "websocket_connected": MARKET_WS_CONNECTED,
        "bootstrap_done": MARKET_BOOTSTRAP_DONE,
        "storage_dir": str(market_storage_dir()),
        "audit_log": str(market_audit_log_path()),
        "last_error": MARKET_LAST_ERROR or None,
        "timestamp_utc": utc_now_iso(),
    }


@app.get("/market/candles/latest")
def market_candles_latest(
    symbol: str = "UNIUSDT",
    interval: str = "1m",
    limit: int = 10,
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)
    symbol = v010_normalize_symbol(symbol)
    interval = str(interval or "1m").strip()
    if interval not in market_intervals():
        return {"ok": False, "reason": f"unsupported_interval:{interval}", "allowed_intervals": market_intervals()}
    limit = max(1, min(int(limit or 10), 500))
    rows = market_load_candles(symbol, interval)
    items_sorted = sorted(rows, key=lambda x: int(x.get("open_time_ms") or 0), reverse=True)
    return {"ok": True, "symbol": symbol, "interval": interval, "count": len(items_sorted[:limit]), "candles": items_sorted[:limit], "timestamp_utc": utc_now_iso()}


@app.post("/market/candles/bootstrap")
def market_candles_bootstrap(
    limit: int = 300,
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)
    symbols = market_allowlist_symbols()
    intervals = market_intervals()
    res = market_rest_bootstrap(symbols, intervals, limit=max(10, min(int(limit or 300), 1000)))
    res["storage_dir"] = str(market_storage_dir())
    return res


def v014_execution_summary_write(signal_key: str, updates: Dict[str, Any]) -> Dict[str, Any]:
    latest = v014_execution_summary_latest(signal_key) or {}
    created = latest.get("created_at_utc") or utc_now_iso()
    row = dict(latest)
    row.update({k: v for k, v in (updates or {}).items() if v is not None})
    row["signal_key"] = signal_key
    row["created_at_utc"] = created
    row["updated_at_utc"] = utc_now_iso()
    append_jsonl(EXECUTION_SUMMARY_LOG, row)
    return row


def v014_execution_summary_latest(signal_key: str) -> Optional[Dict[str, Any]]:
    if not signal_key or not EXECUTION_SUMMARY_LOG.exists():
        return None
    latest = None
    try:
        with EXECUTION_SUMMARY_LOG.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                if str(row.get("signal_key") or "") == str(signal_key):
                    latest = row
    except Exception:
        return None
    return latest


def v014_safety_summary(symbol: str = "", ignore_signal_key: str = "") -> Dict[str, Any]:
    symbol = v010_normalize_symbol(symbol or "")
    recon = v014_reconcile_state(symbol=symbol, ignore_signal_key=ignore_signal_key)
    open_paper_positions = len(v014_open_paper_positions_for_symbol(symbol, ignore_signal_key=ignore_signal_key)) if symbol else open_paper_count(load_state())
    reasons = list(recon.get("reasons") or [])

    mode = execution_mode()
    env = binance_env()
    live_key_detected = live_binance_key_detected()
    signal_mode = signal_source_mode()
    app_mode = apps_script_signal_mode()
    vps_exec_enabled = env_bool("STAT_TECH_LIVE_ENABLED", False)
    competitor_mode = str(os.getenv("VPS_SMC_COMPETITOR_MODE", "")).strip().upper()

    if mode in ("LIVE", "PROD", "MAINNET"):
        reasons.append(f"execution_mode_not_allowed:{mode}")

    if mode == "LIVE_SMALL_CAPITAL":
        if env != "LIVE":
            reasons.append("binance_env_not_live")
        if not env_bool("LIVE_GO_CONFIRM", False):
            reasons.append("live_go_confirm_missing")
        if not env_bool("LIVE_TRADING_ENABLED", False):
            reasons.append("live_trading_not_enabled")
        if not live_key_detected:
            reasons.append("live_key_missing")
        if signal_mode != "STAT_TECH_PRIMARY":
            reasons.append("signal_source_mode_not_stat_tech_primary")
        if app_mode != "BACKUP_COMPARE_ONLY":
            reasons.append("apps_script_signal_mode_not_backup_compare_only")
        if not vps_exec_enabled:
            reasons.append("stat_tech_live_disabled")
        if competitor_mode != "PRODUCTION_SIGNAL":
            reasons.append("stat_tech_primary_mode_required")
        if env_bool("TESTNET_KILL_SWITCH", False):
            reasons.append("testnet_kill_switch_active")
        if env_bool("EMERGENCY_CLOSE_ENABLED", False):
            reasons.append("emergency_close_enabled")
        if env_bool("KILL_SWITCH", False):
            reasons.append("kill_switch_active")

        safe_to_continue = (
            env == "LIVE"
            and mode == "LIVE_SMALL_CAPITAL"
            and env_bool("LIVE_TRADING_ENABLED", False)
            and env_bool("LIVE_GO_CONFIRM", False)
            and live_key_detected
            and signal_mode == "STAT_TECH_PRIMARY"
            and app_mode == "BACKUP_COMPARE_ONLY"
            and vps_exec_enabled
            and competitor_mode == "PRODUCTION_SIGNAL"
            and not env_bool("TESTNET_KILL_SWITCH", False)
            and not env_bool("EMERGENCY_CLOSE_ENABLED", False)
            and not env_bool("KILL_SWITCH", False)
            and recon.get("mismatch_state") == "CLEAN"
            and open_paper_positions == 0
        )
    else:
        if env != "TESTNET":
            reasons.append("binance_env_not_testnet")
        if live_key_detected:
            reasons.append("live_key_detected")
        if env_bool("TESTNET_KILL_SWITCH", False):
            reasons.append("testnet_kill_switch_active")

        safe_to_continue = (
            env == "TESTNET"
            and not live_key_detected
            and mode not in ("LIVE", "PROD", "MAINNET")
            and not env_bool("TESTNET_KILL_SWITCH", False)
            and recon.get("mismatch_state") == "CLEAN"
            and open_paper_positions == 0
        )

    return {
        "ok": bool(recon.get("ok")),
        "execution_mode": mode,
        "binance_env": env,
        "enable_testnet_orders": env_bool("ENABLE_TESTNET_ORDERS", False),
        "order_test_endpoint_only": env_bool("ORDER_TEST_ENDPOINT_ONLY", True),
        "testnet_kill_switch": env_bool("TESTNET_KILL_SWITCH", False),
        "emergency_close_enabled": env_bool("EMERGENCY_CLOSE_ENABLED", False),
        "live_key_detected": live_key_detected,
        "open_paper_positions": open_paper_positions,
        "symbol": symbol or None,
        "positionAmt": recon.get("binance_position_amt"),
        "open_algo_count": recon.get("open_algo_count"),
        "mismatch_state": recon.get("mismatch_state"),
        "safe_to_continue": safe_to_continue,
        "reasons": reasons,
        "timestamp_utc": utc_now_iso(),
    }




def v017_d(val: Any) -> Decimal:
    try:
        return Decimal(str(val))
    except Exception:
        return Decimal("0")


def v017_load_tp_state() -> dict:
    try:
        if not TP_LIFECYCLE_STATE_FILE.exists():
            return {"signals": {}}
        obj = json.loads(TP_LIFECYCLE_STATE_FILE.read_text(encoding="utf-8"))
        if not isinstance(obj, dict):
            return {"signals": {}}
        obj.setdefault("signals", {})
        return obj
    except Exception:
        return {"signals": {}}


def v017_save_tp_state(state: dict) -> None:
    ensure_dirs()
    TP_LIFECYCLE_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    TP_LIFECYCLE_STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def v017_get_tp_lifecycle(signal_key: str) -> dict:
    state = v017_load_tp_state()
    return (state.get("signals") or {}).get(signal_key) or {}


def v017_upsert_tp_lifecycle(signal_key: str, patch: dict) -> dict:
    state = v017_load_tp_state()
    state.setdefault("signals", {})
    cur = dict((state.get("signals") or {}).get(signal_key) or {})
    cur.update(patch or {})
    cur["signal_key"] = signal_key
    cur["updated_at"] = utc_now_iso()
    state["signals"][signal_key] = cur
    v017_save_tp_state(state)
    return cur


def v017_cost_buffer_price(entry_price: Decimal) -> Decimal:
    taker = v017_d(os.getenv("TAKER_FEE_RATE", "0.0004"))
    fee_mult = v017_d(os.getenv("FEE_BUFFER_MULT", "1.0"))
    slippage = v017_d(os.getenv("SLIPPAGE_BUFFER_RATE", "0.0005"))
    return entry_price * (taker * fee_mult * Decimal("2") + slippage)


def v017_is_valid_stop(price: Decimal) -> bool:
    return price > Decimal("0")

def assert_controlled_test_session_clean(symbol: str, force: bool = False, ignore_signal_key: str = "") -> Dict[str, Any]:
    safety = v014_safety_summary(symbol, ignore_signal_key=ignore_signal_key)
    if force:
        if live_binance_key_detected():
            return {"ok": False, "decision": "CONTROLLED_TEST_BLOCKED", "reason": "force_not_allowed_with_live_key_detected", "forced": True, "safety_summary": safety}
        if binance_env() != "TESTNET":
            return {"ok": False, "decision": "CONTROLLED_TEST_BLOCKED", "reason": "force_not_allowed_outside_testnet", "forced": True, "safety_summary": safety}
        return {"ok": True, "decision": "CONTROLLED_TEST_FORCED", "reason": "force_override_in_testnet", "forced": True, "safety_summary": safety}
    if not safety.get("safe_to_continue"):
        reason = ";".join(safety.get("reasons") or ["unsafe_previous_test_session_state"])
        return {"ok": False, "decision": "CONTROLLED_TEST_BLOCKED", "reason": reason, "forced": False, "safety_summary": safety}
    return {"ok": True, "decision": "CONTROLLED_TEST_ALLOWED", "reason": "session_clean", "forced": False, "safety_summary": safety}


@app.post("/testnet/lifecycle-check")
async def v013_testnet_lifecycle_check(request: Request):
    if not v010_auth_ok(request):
        return {"ok": False, "decision": "REJECT", "reason": "unauthorized"}

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    symbol = v010_normalize_symbol(payload.get("symbol") or payload.get("pair") or "")
    if not symbol:
        return {"ok": False, "decision": "REJECT", "reason": "missing_symbol"}

    guard = v013_lifecycle_guard(symbol)
    if not guard.get("ok"):
        return {"ok": False, "decision": guard.get("decision"), "reason": guard.get("reason"), "symbol": symbol}

    position_res = binance_testnet_position_risk(symbol)
    open_algo_res = v013_fetch_open_algo_orders(symbol)
    position_ok = bool(position_res.get("ok"))
    orders_ok = bool(open_algo_res.get("ok"))
    fetch_ok = position_ok and orders_ok

    position_amt = v013_extract_position_amt(position_res, symbol) if position_ok else "0"
    open_algo_orders = open_algo_res.get("orders") or []
    open_algo_orders_count = len(open_algo_orders)
    lifecycle_state = v013_detect_lifecycle_state(position_amt, open_algo_orders_count, fetch_ok)

    known_fn = globals().get("v012_known_orders")
    known_protection_records_count = len(known_fn(symbol)) if callable(known_fn) else 0

    cleanup_results = []
    open_algo_orders_after_cleanup = open_algo_orders
    if lifecycle_state == "POSITION_CLOSED_STALE_ALGO" and str(position_amt).strip() in ("0", "0.0", "0.00"):
        cleanup_results = v013_cancel_stale_algo_orders(symbol, open_algo_orders)
        if cleanup_results:
            v012_mark_cancel_result(symbol, cleanup_results)
        refreshed = v013_fetch_open_algo_orders(symbol)
        open_algo_orders_after_cleanup = refreshed.get("orders") or []

    emergency_close_enabled = v013_env_bool("EMERGENCY_CLOSE_ENABLED", False)
    emergency_close_result = None
    if lifecycle_state == "POSITION_OPEN_UNPROTECTED":
        if emergency_close_enabled:
            emergency_close_result = binance_testnet_close_position_reduce_only(symbol)
        else:
            emergency_close_result = {
                "ok": False,
                "decision": "ALERT_ONLY",
                "reason": "position_open_without_protection_alert_only",
            }

    event = {
        "event_at_utc": utc_now_iso(),
        "event_at_wib": wib_now_iso(),
        "app_version": APP_VERSION,
        "action": "TESTNET_LIFECYCLE_CHECK",
        "execution_mode": execution_mode(),
        "binance_env": binance_env(),
        "symbol": symbol,
        "decision": "LIFECYCLE_CHECK_DONE" if fetch_ok else "LIFECYCLE_CHECK_PARTIAL",
        "lifecycle_state": lifecycle_state,
        "position_amt": position_amt,
        "open_algo_orders_count": open_algo_orders_count,
        "known_protection_records_count": known_protection_records_count,
        "cleanup_count": len(cleanup_results),
        "cleanup_results": cleanup_results,
        "emergency_close_enabled": emergency_close_enabled,
        "emergency_close_result": emergency_close_result,
        "position_risk_result": position_res,
        "open_algo_orders": open_algo_orders,
        "open_algo_orders_after_cleanup": open_algo_orders_after_cleanup,
    }
    v013_log_lifecycle_event(event)
    signal_key = str(payload.get("signal_key") or payload.get("signal_id") or "").strip()
    if signal_key:
        v014_execution_summary_write(signal_key, {
            "symbol": symbol,
            "lifecycle_state": lifecycle_state,
            "cleanup_count": len(cleanup_results),
            "final_position_amt": position_amt,
            "final_open_algo_count": len(open_algo_orders_after_cleanup),
            "safe_restored": lifecycle_state in ("POSITION_CLOSED_CLEAN", "POSITION_CLOSED_STALE_ALGO") and len(open_algo_orders_after_cleanup) == 0 and not v014_is_nonzero_position_amt(position_amt),
            "notes": event.get("decision"),
        })

    return {
        "ok": fetch_ok,
        "decision": event["decision"],
        "symbol": symbol,
        "lifecycle_state": lifecycle_state,
        "position_amt": position_amt,
        "open_algo_orders_count": open_algo_orders_count,
        "known_protection_records_count": known_protection_records_count,
        "cleanup_count": len(cleanup_results),
        "cleanup_results": cleanup_results,
        "emergency_close_enabled": emergency_close_enabled,
        "emergency_close_result": emergency_close_result,
        "open_algo_orders": open_algo_orders,
        "open_algo_orders_after_cleanup": open_algo_orders_after_cleanup,
    }




def v017_tp_lifecycle_check_core(symbol: str, signal_key: str) -> Dict[str, Any]:
    symbol = v010_normalize_symbol(symbol)
    signal_key = str(signal_key or "").strip()
    if not symbol or not signal_key:
        return {"ok": False, "reason": "missing_symbol_or_signal_key"}

    guard = v013_lifecycle_guard(symbol)
    if not guard.get("ok"):
        return {"ok": False, "symbol": symbol, "signal_key": signal_key, "reason": guard.get("reason")}

    row = v017_get_tp_lifecycle(signal_key)
    if not row:
        return {"ok": False, "symbol": symbol, "signal_key": signal_key, "reason": "lifecycle_state_not_found"}
    if str(row.get("lifecycle_stage") or "") == "NEEDS_REVIEW":
        return {"ok": False, "symbol": symbol, "signal_key": signal_key, "reason": "lifecycle_needs_manual_review"}

    pos_res = binance_testnet_position_risk(symbol)
    if not pos_res.get("ok"):
        return {"ok": False, "symbol": symbol, "signal_key": signal_key, "reason": pos_res.get("reason")}
    position_amt = v013_extract_position_amt(pos_res, symbol)
    abs_pos = abs(v017_d(position_amt))

    open_algo_res = v013_fetch_open_algo_orders(symbol)
    open_algo_orders = open_algo_res.get("orders") or []

    initial_qty = abs(v017_d(row.get("initial_qty")))
    tp1_qty = abs(v017_d(row.get("tp1_qty")))
    tp2_qty = abs(v017_d(row.get("tp2_qty")))
    entry_price = v017_d(row.get("entry_price"))
    tp1_price = v017_d(row.get("tp1_price"))
    direction = str(row.get("direction") or "").upper()

    detected_event = "NONE"
    action_taken = "NONE"
    reason = ""
    old_sl_cancel_ok = None
    new_sl_algo_id = None
    new_sl_client_algo_id = None
    new_sl_price = None
    stage = str(row.get("lifecycle_stage") or "ENTRY_PROTECTED")

    cleanup_results = []
    if not open_algo_res.get("ok"):
        reason = "open_algo_fetch_failed"
        stage = "NEEDS_REVIEW"
        row = v017_upsert_tp_lifecycle(signal_key, {"lifecycle_stage": stage})
    elif abs_pos == Decimal("0"):
        cleanup_results = v013_cancel_stale_algo_orders(symbol, open_algo_orders)
        cleanup_failed = any(not bool(r.get("ok")) for r in (cleanup_results or []) if isinstance(r, dict))
        if cleanup_failed:
            action_taken = "CLEANUP_LEFTOVER_ALGO_FAILED"
            reason = "leftover_algo_cleanup_failed"
            stage = "NEEDS_REVIEW"
        else:
            action_taken = "CLEANUP_LEFTOVER_ALGO"
            stage = "POSITION_CLOSED_CLEAN"
        row = v017_upsert_tp_lifecycle(signal_key, {"current_position_qty": "0", "lifecycle_stage": stage})
    else:
        remaining_qty = abs_pos
        current_sl_algo_id = row.get("current_sl_algo_id")
        current_sl_client_algo_id = row.get("current_sl_client_algo_id")
        if direction not in ("LONG", "SHORT"):
            reason = "invalid_direction"
            stage = "NEEDS_REVIEW"
            row = v017_upsert_tp_lifecycle(signal_key, {"lifecycle_stage": stage})
        elif initial_qty <= Decimal("0"):
            reason = "invalid_initial_qty"
            stage = "NEEDS_REVIEW"
            row = v017_upsert_tp_lifecycle(signal_key, {"lifecycle_stage": stage})
        elif remaining_qty <= Decimal("0"):
            reason = "remaining_qty_unknown"
            stage = "NEEDS_REVIEW"
            row = v017_upsert_tp_lifecycle(signal_key, {"lifecycle_stage": stage})
        elif not (current_sl_algo_id or current_sl_client_algo_id):
            reason = "missing_current_sl_reference"
            stage = "NEEDS_REVIEW"
            row = v017_upsert_tp_lifecycle(signal_key, {"lifecycle_stage": stage})
        elif abs_pos <= (initial_qty - tp1_qty - tp2_qty) and not bool(row.get("tp2_processed")):
            detected_event = "TP2_HIT"
            cost_buf = v017_cost_buffer_price(entry_price)
            lock_price = tp1_price + cost_buf if direction == "LONG" else tp1_price - cost_buf
            if not v017_is_valid_stop(lock_price):
                reason = "invalid_lock_profit_price"
                stage = "NEEDS_REVIEW"
                row = v017_upsert_tp_lifecycle(signal_key, {"lifecycle_stage": stage})
            else:
                cancel_res = v012_cancel_order(symbol, current_sl_algo_id, current_sl_client_algo_id)
                old_sl_cancel_ok = bool(cancel_res.get("ok"))
                if not old_sl_cancel_ok:
                    reason = "old_sl_cancel_failed"
                    stage = "NEEDS_REVIEW"
                    row = v017_upsert_tp_lifecycle(signal_key, {"lifecycle_stage": stage})
                else:
                    plan = {"side": "SELL" if direction == "LONG" else "BUY", "type": "STOP_MARKET", "quantity": str(remaining_qty), "stop_price": str(lock_price), "reduceOnly": True}
                    new_sl = v012_place_protective_order(symbol, "SL_LOCK", plan, "V017")
                    if (not new_sl.get("ok")) or (not new_sl.get("algoId")):
                        reason = "new_sl_place_failed"
                        stage = "NEEDS_REVIEW"
                        row = v017_upsert_tp_lifecycle(signal_key, {"lifecycle_stage": stage})
                    else:
                        new_sl_algo_id = new_sl.get("algoId")
                        new_sl_client_algo_id = (new_sl.get("response") or {}).get("body", {}).get("clientAlgoId") if isinstance(new_sl.get("response"), dict) else None
                        new_sl_price = str(lock_price)
                        action_taken = "MOVE_SL_TO_LOCK_PROFIT_NET"
                        stage = "TP2_HIT_LOCK_PROFIT_MOVED"
                        row = v017_upsert_tp_lifecycle(signal_key, {"tp1_processed": True, "be_moved": True, "tp2_processed": True, "lock_profit_moved": True, "current_sl_algo_id": new_sl_algo_id, "current_sl_client_algo_id": new_sl_client_algo_id, "current_position_qty": str(remaining_qty), "lifecycle_stage": stage})
        elif abs_pos <= (initial_qty - tp1_qty) and not bool(row.get("tp1_processed")):
            detected_event = "TP1_HIT"
            cost_buf = v017_cost_buffer_price(entry_price)
            be_price = entry_price + cost_buf if direction == "LONG" else entry_price - cost_buf
            if not v017_is_valid_stop(be_price):
                reason = "invalid_be_net_price"
                stage = "NEEDS_REVIEW"
                row = v017_upsert_tp_lifecycle(signal_key, {"lifecycle_stage": stage})
            else:
                cancel_res = v012_cancel_order(symbol, current_sl_algo_id, current_sl_client_algo_id)
                old_sl_cancel_ok = bool(cancel_res.get("ok"))
                if not old_sl_cancel_ok:
                    reason = "old_sl_cancel_failed"
                    stage = "NEEDS_REVIEW"
                    row = v017_upsert_tp_lifecycle(signal_key, {"lifecycle_stage": stage})
                else:
                    plan = {"side": "SELL" if direction == "LONG" else "BUY", "type": "STOP_MARKET", "quantity": str(remaining_qty), "stop_price": str(be_price), "reduceOnly": True}
                    new_sl = v012_place_protective_order(symbol, "SL_BE", plan, "V017")
                    if (not new_sl.get("ok")) or (not new_sl.get("algoId")):
                        reason = "new_sl_place_failed"
                        stage = "NEEDS_REVIEW"
                        row = v017_upsert_tp_lifecycle(signal_key, {"lifecycle_stage": stage})
                    else:
                        new_sl_algo_id = new_sl.get("algoId")
                        new_sl_client_algo_id = (new_sl.get("response") or {}).get("body", {}).get("clientAlgoId") if isinstance(new_sl.get("response"), dict) else None
                        new_sl_price = str(be_price)
                        action_taken = "MOVE_SL_TO_BE_NET"
                        stage = "TP1_HIT_BE_MOVED"
                        row = v017_upsert_tp_lifecycle(signal_key, {"tp1_processed": True, "be_moved": True, "current_sl_algo_id": new_sl_algo_id, "current_sl_client_algo_id": new_sl_client_algo_id, "current_position_qty": str(remaining_qty), "lifecycle_stage": stage})
        elif abs_pos <= (initial_qty - tp1_qty) and bool(row.get("tp1_processed")):
            reason = "already_processed"

    event = {"event_at_utc": utc_now_iso(), "event_at_wib": wib_now_iso(), "app_version": APP_VERSION, "action": "TESTNET_TP_LIFECYCLE_CHECK", "symbol": symbol, "signal_key": signal_key, "positionAmt": str(position_amt), "detected_event": detected_event, "action_taken": action_taken, "reason": reason or None, "lifecycle_stage": stage, "cleanup_count": len(cleanup_results), "cleanup_results": cleanup_results, "funding_buffer": "pending"}
    append_jsonl(EXECUTION_EVENTS_LOG, event)
    v014_execution_summary_write(signal_key, {"symbol": symbol, "lifecycle_state": str(row.get("lifecycle_stage") or stage), "tp_lifecycle_stage": str(row.get("lifecycle_stage") or stage), "notes": action_taken if action_taken != "NONE" else (reason or "NONE")})

    return {"ok": True, "symbol": symbol, "signal_key": signal_key, "positionAmt": str(position_amt), "initial_qty": str(row.get("initial_qty") or ""), "remaining_qty": str(abs_pos), "detected_event": detected_event, "action_taken": action_taken, "reason": reason or None, "old_sl_cancel_ok": old_sl_cancel_ok, "new_sl_algo_id": new_sl_algo_id, "new_sl_client_algo_id": new_sl_client_algo_id, "new_sl_price": new_sl_price, "cleanup_count": len(cleanup_results), "cleanup_results": cleanup_results, "lifecycle_stage": str(row.get("lifecycle_stage") or stage)}



def _pm_live_algo_orders(symbol: str) -> Dict[str, Any]:
    symbol = v010_normalize_symbol(symbol)
    res = live_signed_request("GET", "/fapi/v1/openAlgoOrders", {"symbol": symbol})
    body = res.get("body") if isinstance(res.get("body"), list) else []
    return {
        "ok": bool(res.get("ok")),
        "reason": res.get("reason"),
        "symbol": symbol,
        "orders": body,
        "raw": res,
    }


def _pm_live_position_amt(symbol: str) -> Dict[str, Any]:
    symbol = v010_normalize_symbol(symbol)
    res = live_signed_request("GET", "/fapi/v2/positionRisk", {"symbol": symbol})
    if not res.get("ok"):
        return {"ok": False, "reason": res.get("reason") or "position_risk_failed", "symbol": symbol, "raw": res}

    rows = res.get("body") if isinstance(res.get("body"), list) else []
    row = next((x for x in rows if str(x.get("symbol") or "").upper() == symbol), None)
    if not row:
        return {"ok": False, "reason": "position_row_not_found", "symbol": symbol, "raw": res}

    try:
        amt = Decimal(str(row.get("positionAmt") or "0"))
    except Exception:
        return {"ok": False, "reason": "invalid_position_amt", "symbol": symbol, "position": row}

    return {"ok": True, "symbol": symbol, "positionAmt": str(amt), "abs_qty": str(abs(amt)), "position": row}


def _pm_order_type(order: Dict[str, Any]) -> str:
    return str(order.get("orderType") or order.get("type") or "").upper()


def _pm_order_qty(order: Dict[str, Any]) -> Decimal:
    try:
        return abs(Decimal(str(order.get("quantity") or order.get("origQty") or "0")))
    except Exception:
        return Decimal("0")


def _pm_order_reduce_only(order: Dict[str, Any]) -> bool:
    raw = order.get("reduceOnly")
    return raw is True or str(raw).strip().lower() == "true"


def _pm_order_client_id(order: Dict[str, Any]) -> str:
    return str(order.get("clientAlgoId") or order.get("clientOrderId") or "")


def _pm_plain_decimal(value: Decimal) -> str:
    try:
        return _live_plain_decimal(value)
    except Exception:
        s = format(value.normalize(), "f")
        return s.rstrip("0").rstrip(".") if "." in s else s


def _pm_symbol_filters(symbol: str) -> Dict[str, Any]:
    try:
        return live_entry_symbol_filters(symbol)
    except Exception:
        return {}


def _pm_round_qty_to_step(symbol: str, qty: Decimal) -> Decimal:
    try:
        filters = _pm_symbol_filters(symbol)
        step = Decimal(str(filters.get("stepSize") or filters.get("step_size") or "0"))
        if step > 0:
            return (qty / step).to_integral_value(rounding=ROUND_DOWN) * step
    except Exception:
        pass
    return qty


def _pm_round_price_to_tick(symbol: str, price: Any) -> str:
    try:
        p = Decimal(str(price))
        filters = _pm_symbol_filters(symbol)
        tick = Decimal(str(filters.get("tickSize") or filters.get("tick_size") or "0"))
        if tick > 0:
            p = (p / tick).to_integral_value(rounding=ROUND_DOWN) * tick
        return _pm_plain_decimal(p)
    except Exception:
        return str(price)


def _pm_plan_price(bot_plan: Dict[str, Any], key: str) -> str:
    plan = bot_plan.get("plan") if isinstance(bot_plan.get("plan"), dict) else {}
    value = (
        bot_plan.get(key)
        or plan.get(key)
        or ((plan.get("payload") or {}).get(key) if isinstance(plan.get("payload"), dict) else None)
    )
    return str(value or "").strip()


def _pm_valid_reduce_order_exists(orders: list, order_type: str, qty: Decimal) -> bool:
    order_type = str(order_type or "").upper()
    for order in orders or []:
        if not isinstance(order, dict):
            continue
        if _pm_order_type(order) != order_type:
            continue
        if not _pm_order_reduce_only(order):
            continue
        if _pm_order_qty(order) == qty:
            return True
    return False


def _pm_live_cancel_algo_order(symbol: str, order: Dict[str, Any]) -> Dict[str, Any]:
    symbol = v010_normalize_symbol(symbol)
    algo_id = order.get("algoId")
    client_id = order.get("clientAlgoId") or order.get("clientOrderId")

    if algo_id:
        res = live_signed_request("DELETE", "/fapi/v1/algoOrder", {"symbol": symbol, "algoId": algo_id})
        if res.get("ok"):
            return {"ok": True, "symbol": symbol, "cancel_by": "algoId", "algoId": algo_id, "clientAlgoId": client_id, "raw": res}

    if client_id:
        res = live_signed_request("DELETE", "/fapi/v1/algoOrder", {"symbol": symbol, "clientAlgoId": client_id})
        return {"ok": bool(res.get("ok")), "symbol": symbol, "cancel_by": "clientAlgoId", "algoId": algo_id, "clientAlgoId": client_id, "raw": res}

    return {"ok": False, "reason": "missing_algo_id_and_client_algo_id", "symbol": symbol, "order": order}


def _pm_live_place_reduce_algo(symbol: str, label: str, side: str, order_type: str, qty: Decimal, trigger_price: str) -> Dict[str, Any]:
    import time as _time

    symbol = v010_normalize_symbol(symbol)
    qty_s = _pm_plain_decimal(qty)
    client_id = f"PMSYNC_{int(_time.time() * 1000) % 1000000000}_{label}"[:36]

    params = {
        "symbol": symbol,
        "side": str(side or "").upper(),
        "type": str(order_type or "").upper(),
        "quantity": qty_s,
        "reduceOnly": "true",
        "workingType": "CONTRACT_PRICE",
        "clientAlgoId": client_id,
        "triggerPrice": str(trigger_price),
        "algoType": "CONDITIONAL",
        "positionSide": "BOTH",
        "priceProtect": "false",
    }

    res = live_signed_request("POST", "/fapi/v1/algoOrder", params)
    return {
        "ok": bool(res.get("ok")),
        "symbol": symbol,
        "label": label,
        "clientAlgoId": client_id,
        "request": params,
        "raw": res,
        "reason": res.get("reason"),
    }


def pm_sync_protection_after_live_reduce(symbol: str, action: str, signal_key: str = "") -> Dict[str, Any]:
    import os as _os
    import time as _time

    symbol = v010_normalize_symbol(symbol)
    action = str(action or "").strip().upper()
    signal_key = str(signal_key or "").strip()

    if action != "REDUCE_50":
        return {"ok": True, "skipped": True, "reason": "sync_only_after_reduce_50", "symbol": symbol, "action": action}

    enabled = str(_os.getenv("PM_SYNC_PROTECTION_AFTER_REDUCE", "true")).strip().lower() in ("1", "true", "yes", "y", "on")
    if not enabled:
        return {"ok": True, "skipped": True, "reason": "pm_sync_protection_after_reduce_disabled", "symbol": symbol, "action": action}

    try:
        delay = float(str(_os.getenv("PM_POST_REDUCE_SYNC_DELAY_SEC", "2.0")).strip() or "2.0")
    except Exception:
        delay = 2.0
    if delay > 0:
        _time.sleep(min(max(delay, 0.0), 10.0))

    pos = _pm_live_position_amt(symbol)
    if not pos.get("ok"):
        return {"ok": False, "reason": "position_after_reduce_fetch_failed", "symbol": symbol, "position_result": pos}

    position_amt = Decimal(str(pos.get("positionAmt") or "0"))
    remaining_qty = abs(position_amt)

    if remaining_qty == Decimal("0"):
        return {"ok": True, "symbol": symbol, "positionAmt": str(position_amt), "remaining_qty": "0", "reason": "position_closed_no_protection_sync_needed"}

    remaining_qty = _pm_round_qty_to_step(symbol, remaining_qty)
    if remaining_qty <= Decimal("0"):
        return {"ok": False, "reason": "remaining_qty_below_step", "symbol": symbol, "positionAmt": str(position_amt)}

    side = "SELL" if position_amt > 0 else "BUY"
    bot_plan = pm_find_bot_plan(symbol)
    sl_price_raw = _pm_plan_price(bot_plan, "sl")
    tp1_price_raw = _pm_plan_price(bot_plan, "tp1")

    sl_price = _pm_round_price_to_tick(symbol, sl_price_raw) if sl_price_raw else ""
    tp1_price = _pm_round_price_to_tick(symbol, tp1_price_raw) if tp1_price_raw else ""

    before = _pm_live_algo_orders(symbol)
    if not before.get("ok"):
        return {"ok": False, "reason": "open_algo_before_fetch_failed", "symbol": symbol, "open_algo_result": before}

    orders_before = before.get("orders") or []
    placed = []
    canceled = []
    skipped_cancel = []

    has_stop = _pm_valid_reduce_order_exists(orders_before, "STOP_MARKET", remaining_qty)
    has_tp = _pm_valid_reduce_order_exists(orders_before, "TAKE_PROFIT_MARKET", remaining_qty)

    # Place replacement before canceling old oversize orders. Being naked even for a second is a very human kind of bad idea.
    if not has_stop and sl_price:
        placed_stop = _pm_live_place_reduce_algo(symbol, "SL_SYNC", side, "STOP_MARKET", remaining_qty, sl_price)
        placed.append(placed_stop)

    if not has_tp and tp1_price:
        placed_tp = _pm_live_place_reduce_algo(symbol, "TP1_SYNC", side, "TAKE_PROFIT_MARKET", remaining_qty, tp1_price)
        placed.append(placed_tp)

    _time.sleep(0.7)

    after_place = _pm_live_algo_orders(symbol)
    if not after_place.get("ok"):
        return {
            "ok": False,
            "reason": "open_algo_after_place_fetch_failed",
            "symbol": symbol,
            "positionAmt": str(position_amt),
            "remaining_qty": _pm_plain_decimal(remaining_qty),
            "placed": placed,
            "open_algo_result": after_place,
        }

    orders_after_place = after_place.get("orders") or []
    valid_stop_after = _pm_valid_reduce_order_exists(orders_after_place, "STOP_MARKET", remaining_qty)
    valid_tp_after = _pm_valid_reduce_order_exists(orders_after_place, "TAKE_PROFIT_MARKET", remaining_qty)

    for order in orders_after_place:
        if not isinstance(order, dict):
            continue
        typ = _pm_order_type(order)
        if typ not in ("STOP_MARKET", "TAKE_PROFIT_MARKET"):
            continue
        if not _pm_order_reduce_only(order):
            continue

        qty = _pm_order_qty(order)
        if qty <= remaining_qty:
            continue

        can_cancel = (typ == "STOP_MARKET" and valid_stop_after) or (typ == "TAKE_PROFIT_MARKET" and valid_tp_after)
        if not can_cancel:
            skipped_cancel.append({
                "clientAlgoId": _pm_order_client_id(order),
                "algoId": order.get("algoId"),
                "orderType": typ,
                "quantity": str(qty),
                "reason": "no_valid_replacement_order_same_type",
            })
            continue

        canceled.append(_pm_live_cancel_algo_order(symbol, order))

    _time.sleep(0.7)

    final = _pm_live_algo_orders(symbol)
    if not final.get("ok"):
        return {
            "ok": False,
            "reason": "open_algo_final_fetch_failed",
            "symbol": symbol,
            "positionAmt": str(position_amt),
            "remaining_qty": _pm_plain_decimal(remaining_qty),
            "placed": placed,
            "canceled": canceled,
            "skipped_cancel": skipped_cancel,
            "open_algo_result": final,
        }

    final_orders = final.get("orders") or []
    oversized = []
    final_has_stop = _pm_valid_reduce_order_exists(final_orders, "STOP_MARKET", remaining_qty)
    final_has_tp = _pm_valid_reduce_order_exists(final_orders, "TAKE_PROFIT_MARKET", remaining_qty)

    for order in final_orders:
        if not isinstance(order, dict):
            continue
        typ = _pm_order_type(order)
        if typ not in ("STOP_MARKET", "TAKE_PROFIT_MARKET"):
            continue
        if _pm_order_reduce_only(order) and _pm_order_qty(order) > remaining_qty:
            oversized.append({
                "clientAlgoId": _pm_order_client_id(order),
                "algoId": order.get("algoId"),
                "orderType": typ,
                "quantity": str(_pm_order_qty(order)),
                "triggerPrice": order.get("triggerPrice"),
            })

    ok = bool(final_has_stop and final_has_tp and not oversized and not any(not x.get("ok") for x in placed + canceled if isinstance(x, dict)))

    result = {
        "ok": ok,
        "symbol": symbol,
        "action": action,
        "signal_key": signal_key or None,
        "positionAmt": str(position_amt),
        "remaining_qty": _pm_plain_decimal(remaining_qty),
        "side": side,
        "bot_plan_ok": bool(bot_plan.get("ok")),
        "sl_price": sl_price or None,
        "tp1_price": tp1_price or None,
        "placed": placed,
        "canceled": canceled,
        "skipped_cancel": skipped_cancel,
        "final_has_stop": final_has_stop,
        "final_has_tp": final_has_tp,
        "oversized_orders": oversized,
        "final_open_algo_count": len(final_orders),
    }

    if not ok:
        result["reason"] = "post_reduce_protection_sync_incomplete"
    return result


def pm_live_guarded_reduce_only_action(symbol: str, action: str, signal_key: str = "") -> Dict[str, Any]:
    symbol = v010_normalize_symbol(symbol)
    action = str(action or "").strip().upper()
    signal_key = str(signal_key or symbol or "PM").strip()
    if action not in ("REDUCE_50", "CLOSE_FULL", "EMERGENCY_CLOSE"):
        return {"ok": False, "reason": f"unsupported_pm_action:{action}", "symbol": symbol, "action": action}
    if execution_mode() != "LIVE_SMALL_CAPITAL" or binance_env() != "LIVE":
        return {"ok": False, "reason": "live_guarded_action_requires_live_small_capital_live_env", "symbol": symbol, "action": action}

    snap = live_get_position_snapshot(symbol)
    if not snap.get("ok"):
        return {"ok": False, "reason": snap.get("reason") or "position_snapshot_failed", "symbol": symbol, "action": action, "snapshot": snap}
    position = snap.get("position") or {}
    try:
        position_amt = Decimal(str(position.get("positionAmt") or "0"))
    except Exception:
        return {"ok": False, "reason": "invalid_position_amt", "symbol": symbol, "action": action, "position": position}
    if position_amt == Decimal("0"):
        return {"ok": False, "reason": "no_open_position", "symbol": symbol, "action": action, "positionAmt": str(position_amt)}

    abs_qty = abs(position_amt)
    reduce_qty = abs_qty / Decimal("2") if action == "REDUCE_50" else abs_qty
    if reduce_qty <= Decimal("0"):
        return {"ok": False, "reason": "invalid_reduce_qty", "symbol": symbol, "action": action, "positionAmt": str(position_amt)}
    try:
        filters = live_entry_symbol_filters(symbol)
        step = Decimal(str(filters.get("stepSize") or filters.get("step_size") or "0"))
        if step > 0:
            reduce_qty = (reduce_qty / step).to_integral_value(rounding=ROUND_DOWN) * step
    except Exception:
        filters = {}
    if reduce_qty <= Decimal("0"):
        return {"ok": False, "reason": "reduce_qty_below_step_size", "symbol": symbol, "action": action, "positionAmt": str(position_amt)}

    side = "SELL" if position_amt > 0 else "BUY"
    params = {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quantity": _live_plain_decimal(reduce_qty),
        "reduceOnly": "true",
        "newClientOrderId": _live_client_order_id("PMV027", signal_key, action),
    }
    res = live_place_order(params)

    post_reduce_sync_result: Dict[str, Any] = {}
    if bool(res.get("ok")) and action == "REDUCE_50":
        try:
            post_reduce_sync_result = pm_sync_protection_after_live_reduce(symbol, action, signal_key)
        except Exception as e:
            post_reduce_sync_result = {
                "ok": False,
                "reason": f"post_reduce_sync_exception:{type(e).__name__}:{e}",
                "symbol": symbol,
                "action": action,
            }

    event = {
        "event_at_utc": utc_now_iso(),
        "event_at_wib": wib_now_iso(),
        "action": "POSITION_MANAGER_LIVE_GUARDED_ACTION",
        "symbol": symbol,
        "signal_key": signal_key or None,
        "manager_action": action,
        "positionAmt": str(position_amt),
        "side": side,
        "quantity": params["quantity"],
        "order_ok": bool(res.get("ok")),
        "order_reason": res.get("reason"),
        "post_reduce_sync_result": post_reduce_sync_result or None,
    }
    append_jsonl(EXECUTION_EVENTS_LOG, event)
    append_jsonl(MANAGER_ACTIONS_LOG if res.get("ok") else MANAGER_ERRORS_LOG, event)
    if res.get("ok") and post_reduce_sync_result and not post_reduce_sync_result.get("ok", True):
        append_jsonl(MANAGER_ERRORS_LOG, event)

    return {
        "ok": bool(res.get("ok")),
        "reason": res.get("reason"),
        "symbol": symbol,
        "action": action,
        "positionAmt": str(position_amt),
        "side": side,
        "quantity": params["quantity"],
        "order": res,
        "post_reduce_sync_result": post_reduce_sync_result or None,
    }


PM_ACTION_STATE_FILE = STATE_DIR / "position_manager_actions.json"


def pm_live_open_positions() -> Dict[str, Any]:
    if execution_mode() != "LIVE_SMALL_CAPITAL" or binance_env() != "LIVE":
        return {"ok": True, "reason": "not_live_guarded_env", "positions": []}
    res = live_signed_request("GET", "/fapi/v2/positionRisk", {})
    if not res.get("ok"):
        return {"ok": False, "reason": res.get("reason") or "live_position_risk_failed", "positions": [], "raw": res}
    rows = res.get("body") if isinstance(res.get("body"), list) else []
    positions = []
    for row in rows:
        try:
            amt = Decimal(str(row.get("positionAmt") or "0"))
        except Exception:
            amt = Decimal("0")
        if amt != Decimal("0"):
            positions.append({"symbol": str(row.get("symbol") or "").upper(), "positionAmt": str(amt), "entryPrice": row.get("entryPrice"), "raw": row})
    return {"ok": True, "positions": positions, "raw": res}



def _pm_norm_plan_direction(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if raw in ("BUY", "LONG"):
        return "LONG"
    if raw in ("SELL", "SHORT"):
        return "SHORT"
    return ""


def _pm_smc_client_prefix(client_id: Any) -> str:
    s = str(client_id or "").strip()
    if not s.startswith("SMC_"):
        return ""
    parts = s.split("_")
    if len(parts) >= 2 and parts[1]:
        return f"SMC_{parts[1]}"
    return ""


def _pm_event_client_ids(event: Dict[str, Any]) -> List[str]:
    ids: List[str] = []

    entry_params = event.get("entry_params") if isinstance(event.get("entry_params"), dict) else {}
    ids.append(str(entry_params.get("newClientOrderId") or entry_params.get("clientOrderId") or ""))

    entry_result = event.get("entry_result") if isinstance(event.get("entry_result"), dict) else {}
    entry_body = entry_result.get("body") if isinstance(entry_result.get("body"), dict) else {}
    ids.append(str(entry_body.get("clientOrderId") or ""))

    for item in event.get("protective_results") or []:
        if not isinstance(item, dict):
            continue
        params = item.get("params") if isinstance(item.get("params"), dict) else {}
        result = item.get("result") if isinstance(item.get("result"), dict) else {}
        body = result.get("body") if isinstance(result.get("body"), dict) else {}
        req = result.get("request_params") if isinstance(result.get("request_params"), dict) else {}

        ids.append(str(params.get("newClientOrderId") or params.get("clientAlgoId") or ""))
        ids.append(str(body.get("clientAlgoId") or body.get("clientOrderId") or ""))
        ids.append(str(req.get("clientAlgoId") or req.get("newClientOrderId") or ""))

    return [x for x in ids if x]


def _pm_recent_jsonl(path: Path, max_lines: int = 5000) -> List[Dict[str, Any]]:
    import json
    from collections import deque

    rows: List[Dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as fh:
            lines = deque(fh, maxlen=max_lines)
    except Exception:
        return rows

    for line in reversed(lines):
        line = str(line or "").strip()
        if not line:
            continue
        try:
            item = json.loads(line)
        except Exception:
            continue
        if isinstance(item, dict):
            rows.append(item)

    return rows


def _pm_active_smc_prefixes(symbol: str) -> List[str]:
    prefixes = set()
    try:
        res = v013_fetch_open_algo_orders(symbol)
        orders = res.get("orders") or []
    except Exception:
        orders = []

    for order in orders:
        if not isinstance(order, dict):
            continue
        for key in ("clientAlgoId", "clientOrderId", "origClientOrderId"):
            prefix = _pm_smc_client_prefix(order.get(key))
            if prefix:
                prefixes.add(prefix)

    return sorted(prefixes)


def _pm_find_bot_plan_from_execution_events(symbol: str) -> Dict[str, Any]:
    symbol = v010_normalize_symbol(symbol)
    active_prefixes = set(_pm_active_smc_prefixes(symbol))

    for event in _pm_recent_jsonl(EXECUTION_EVENTS_LOG, max_lines=8000):
        event_symbol = v010_normalize_symbol(
            event.get("symbol")
            or (event.get("plan") or {}).get("symbol")
            or ((event.get("plan") or {}).get("payload") or {}).get("symbol")
            or ""
        )
        if event_symbol != symbol:
            continue

        decision = str(event.get("decision") or "").upper()
        action = str(event.get("action") or "").upper()
        if decision != "LIVE_ORDER_PLACED" and action != "LIVE_SMALL_CAPITAL_EXECUTE":
            continue

        if event.get("protection_ok") is False:
            continue

        event_prefixes = {_pm_smc_client_prefix(x) for x in _pm_event_client_ids(event)}
        event_prefixes.discard("")

        # If current open protective orders expose SMC_xxx, require the execution event to match it.
        # This avoids adopting an old closed trade just because the symbol matches. Humanity survives one more if-statement.
        if active_prefixes and not (active_prefixes & event_prefixes):
            continue

        plan = event.get("plan") if isinstance(event.get("plan"), dict) else {}
        payload = plan.get("payload") if isinstance(plan.get("payload"), dict) else {}

        direction = _pm_norm_plan_direction(
            plan.get("direction")
            or payload.get("direction")
            or event.get("direction")
        )
        if direction not in ("LONG", "SHORT"):
            continue

        signal_key = str(
            event.get("signal_key")
            or plan.get("signal_key")
            or payload.get("signal_key")
            or payload.get("signal_id")
            or ""
        ).strip()

        entry = (
            plan.get("actual_entry_price")
            or plan.get("entry_price")
            or plan.get("entry_mid")
            or payload.get("entry")
            or payload.get("entry_mid")
            or ((event.get("entry_fill_result") or {}).get("entryPrice") if isinstance(event.get("entry_fill_result"), dict) else None)
        )
        sl = plan.get("sl") or payload.get("sl")
        tp1 = plan.get("tp1") or payload.get("tp1")
        qty = (
            plan.get("quantity")
            or plan.get("tp1_qty")
            or payload.get("quantity")
            or ((event.get("entry_params") or {}).get("quantity") if isinstance(event.get("entry_params"), dict) else None)
        )

        adopted_plan = dict(plan)
        adopted_plan.update({
            "symbol": symbol,
            "direction": direction,
            "signal_key": signal_key,
            "entry_price": entry,
            "entry": entry,
            "entry_mid": entry,
            "sl": sl,
            "tp1": tp1,
            "quantity": qty,
            "source": "execution_events_log",
            "source_event_at_utc": event.get("event_at_utc"),
            "source_decision": decision or action,
            "active_smc_prefixes": sorted(active_prefixes),
            "matched_event_prefixes": sorted(event_prefixes),
        })

        return {
            "ok": True,
            "source": "execution_events_log",
            "symbol": symbol,
            "direction": direction,
            "signal_key": signal_key,
            "entry_price": entry,
            "sl": sl,
            "tp1": tp1,
            "quantity": qty,
            "plan": adopted_plan,
        }

    return {
        "ok": False,
        "reason": "matching_execution_event_plan_not_found",
        "symbol": symbol,
        "active_smc_prefixes": sorted(active_prefixes),
    }


def pm_find_bot_plan(symbol: str) -> Dict[str, Any]:
    symbol = v010_normalize_symbol(symbol)

    rows = v014_open_paper_positions_for_symbol(symbol)
    for row in rows:
        direction = _pm_norm_plan_direction(row.get("direction") or row.get("side"))
        if direction in ("LONG", "SHORT"):
            return {
                "ok": True,
                "source": "open_paper_positions",
                "symbol": symbol,
                "direction": direction,
                "signal_key": row.get("signal_key"),
                "plan": row,
            }

    state = v017_load_tp_state()
    for signal_key, row in (state.get("signals") or {}).items():
        if v010_normalize_symbol(row.get("symbol") or "") != symbol:
            continue
        stage = str(row.get("lifecycle_stage") or "").upper()
        direction = _pm_norm_plan_direction(row.get("direction"))
        if direction in ("LONG", "SHORT") and stage not in ("", "POSITION_CLOSED_CLEAN", "NEEDS_REVIEW", "ENTRY_FAILED"):
            return {
                "ok": True,
                "source": "tp_lifecycle_state",
                "symbol": symbol,
                "direction": direction,
                "signal_key": signal_key,
                "plan": row,
            }

    fallback = _pm_find_bot_plan_from_execution_events(symbol)
    if fallback.get("ok"):
        return fallback

    return {
        "ok": False,
        "reason": "matching_open_bot_plan_not_found",
        "symbol": symbol,
        "fallback": fallback,
    }


def _pm_float(x: Any) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None


def _pm_closes(symbol: str, interval: str = "1m", limit: int = 260) -> List[float]:
    rows = _pm_rows(symbol, interval, limit)
    return [float(x.get("close")) for x in rows if _pm_float(x.get("close")) is not None]


def _pm_rows(symbol: str, interval: str = "1m", limit: int = 260) -> List[Dict[str, Any]]:
    rows = market_load_candles(v010_normalize_symbol(symbol), interval)
    return sorted(rows, key=lambda x: int(x.get("open_time_ms") or 0))[-limit:]


def _pm_ema(values: List[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    k = 2.0 / (period + 1.0)
    ema = sum(values[:period]) / period
    for val in values[period:]:
        ema = val * k + ema * (1.0 - k)
    return ema


def _pm_rsi(values: List[float], period: int = 14) -> Optional[float]:
    if len(values) <= period:
        return None
    gains = []
    losses = []
    for prev, cur in zip(values[-period - 1:-1], values[-period:]):
        diff = cur - prev
        gains.append(max(diff, 0.0))
        losses.append(abs(min(diff, 0.0)))
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    return 100.0 - (100.0 / (1.0 + ((sum(gains) / period) / avg_loss)))


def pm_manager_score(symbol: str, recon: Dict[str, Any], bot_plan: Dict[str, Any]) -> Dict[str, Any]:
    symbol = v010_normalize_symbol(symbol)
    rows = _pm_rows(symbol, "1m", 260)
    closes = [float(x.get("close")) for x in rows if _pm_float(x.get("close")) is not None]
    highs = [float(x.get("high")) for x in rows if _pm_float(x.get("high")) is not None]
    lows = [float(x.get("low")) for x in rows if _pm_float(x.get("low")) is not None]
    volumes = [float(x.get("volume") or x.get("quote_volume") or 0.0) for x in rows if _pm_float(x.get("volume") or x.get("quote_volume") or 0.0) is not None]
    btc = _pm_closes("BTCUSDT", "1m", 260)
    score = 0.0
    missing = []
    details: Dict[str, Any] = {}
    if len(closes) < 60:
        missing.append("candles")
    else:
        ema20 = _pm_ema(closes, 20); ema50 = _pm_ema(closes, 50); ema200 = _pm_ema(closes, 200)
        rsi = _pm_rsi(closes)
        macd_fast = _pm_ema(closes, 12); macd_slow = _pm_ema(closes, 26)
        macd_hist = (macd_fast - macd_slow) if macd_fast is not None and macd_slow is not None else None
        returns = [(b / a - 1.0) for a, b in zip(closes[-31:-1], closes[-30:]) if a]
        ret_now = (closes[-1] / closes[-21] - 1.0) if len(closes) >= 21 and closes[-21] else 0.0
        mean = sum(returns) / len(returns) if returns else 0.0
        var = sum((x - mean) ** 2 for x in returns) / len(returns) if returns else 0.0
        z = (ret_now - mean) / (var ** 0.5) if var > 0 else 0.0
        btc_ret_1h = (btc[-1] / btc[-61] - 1.0) if len(btc) >= 61 and btc[-61] else None
        pair_ret_1h = (closes[-1] / closes[-61] - 1.0) if len(closes) >= 61 and closes[-61] else None
        rel_btc = (pair_ret_1h - btc_ret_1h) if pair_ret_1h is not None and btc_ret_1h is not None else None
        tr_values = []
        for i in range(max(1, len(closes) - 20), len(closes)):
            prev_close = closes[i - 1]
            high = highs[i] if i < len(highs) else closes[i]
            low = lows[i] if i < len(lows) else closes[i]
            tr_values.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
        atr = (sum(tr_values) / len(tr_values)) if tr_values else None
        atr_counter_move = abs(closes[-1] - max(closes[-20:])) / atr if atr and closes[-1] < max(closes[-20:]) else 0.0
        vol_sma20 = (sum(volumes[-20:]) / min(20, len(volumes))) if volumes else None
        volume_ratio = (volumes[-1] / vol_sma20) if vol_sma20 else None
        entry = _pm_float((bot_plan.get("plan") or {}).get("entry_price") or (bot_plan.get("plan") or {}).get("entry") or (bot_plan.get("plan") or {}).get("entry_mid"))
        mfe_giveback = None
        if entry and len(closes) >= 20:
            if str(bot_plan.get("direction") or "").upper() == "SHORT":
                mfe = max(0.0, entry - min(closes[-60:]))
                giveback = closes[-1] - min(closes[-60:])
            else:
                mfe = max(0.0, max(closes[-60:]) - entry)
                giveback = max(closes[-60:]) - closes[-1]
            mfe_giveback = (giveback / mfe) if mfe > 0 else None
        alt_returns = []
        for alt in ("ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT"):
            alt_closes = _pm_closes(alt, "1m", 70)
            if len(alt_closes) >= 61 and alt_closes[-61]:
                alt_returns.append(alt_closes[-1] / alt_closes[-61] - 1.0)
        alt_avg = (sum(alt_returns) / len(alt_returns)) if alt_returns else None
        alt_breadth = (len([x for x in alt_returns if x > 0]) / len(alt_returns)) if alt_returns else None
        rel_alt = (pair_ret_1h - alt_avg) if pair_ret_1h is not None and alt_avg is not None else None
        btcdom = _pm_closes("BTCDOMUSDT", "1m", 70)
        btcdom_ret_1h = (btcdom[-1] / btcdom[-61] - 1.0) if len(btcdom) >= 61 and btcdom[-61] else None
        direction = str(bot_plan.get("direction") or "").upper()
        adverse_long = 0.0
        adverse_short = 0.0
        if ema20 and ema50 and ema200:
            adverse_long += 15.0 if closes[-1] < ema20 else 0.0
            adverse_long += 15.0 if ema20 < ema50 else 0.0
            adverse_long += 10.0 if ema50 < ema200 else 0.0
            adverse_short += 15.0 if closes[-1] > ema20 else 0.0
            adverse_short += 15.0 if ema20 > ema50 else 0.0
            adverse_short += 10.0 if ema50 > ema200 else 0.0
        else:
            missing.append("ema200")
        if rsi is not None:
            adverse_long += 10.0 if rsi < 45 else 0.0
            adverse_long += 10.0 if rsi < 35 else 0.0
            adverse_short += 10.0 if rsi > 55 else 0.0
            adverse_short += 10.0 if rsi > 65 else 0.0
        else:
            missing.append("rsi")
        if macd_hist is not None:
            adverse_long += 10.0 if macd_hist < 0 else 0.0
            adverse_short += 10.0 if macd_hist > 0 else 0.0
        else:
            missing.append("macd")
        adverse_long += min(20.0, abs(min(z, 0.0)) * 8.0)
        adverse_short += min(20.0, max(z, 0.0) * 8.0)
        if rel_btc is not None:
            adverse_long += min(10.0, abs(rel_btc) * 500.0) if rel_btc < 0 else 0.0
            adverse_short += min(10.0, rel_btc * 500.0) if rel_btc > 0 else 0.0
        if rel_alt is not None:
            adverse_long += min(8.0, abs(rel_alt) * 400.0) if rel_alt < 0 else 0.0
            adverse_short += min(8.0, rel_alt * 400.0) if rel_alt > 0 else 0.0
        if volume_ratio is not None and volume_ratio > 1.5:
            adverse_long += 5.0 if ret_now < 0 else 0.0
            adverse_short += 5.0 if ret_now > 0 else 0.0
        if mfe_giveback is not None and mfe_giveback > 0.5:
            if direction == "SHORT":
                adverse_short += 8.0
            else:
                adverse_long += 8.0
        score = max(0.0, min(100.0, adverse_short if direction == "SHORT" else adverse_long))
        if not alt_returns:
            missing.append("alt_basket")
        if btcdom_ret_1h is None:
            missing.append("btcdom")
        details.update({"ema20": ema20, "ema50": ema50, "ema200": ema200, "rsi": rsi, "macd_histogram": macd_hist, "return_zscore": z, "btc_return_1h": btc_ret_1h, "btc_return_4h": ((btc[-1] / btc[-241] - 1.0) if len(btc) >= 241 and btc[-241] else None), "pair_relative_strength_vs_btc": rel_btc, "vwap_approx": (sum(closes[-60:]) / min(60, len(closes))), "session_vwap_approx": (sum(closes[-240:]) / min(240, len(closes))), "atr_counter_move": atr_counter_move, "volume_ratio_sma20": volume_ratio, "mfe_giveback": mfe_giveback, "alt_basket_breadth": alt_breadth, "pair_relative_strength_vs_alt_basket": rel_alt, "btcdom_return_1h": btcdom_ret_1h, "funding_rate": None, "open_interest": None, "taker_ratio": None, "fred_context": None})
    confidence = max(0.0, 1.0 - (len(missing) * 0.08))
    protect_th = env_float("PM_PROTECT_THRESHOLD", env_float("PM_PROTECT_SCORE", 50.0))
    reduce_th = env_float("PM_REDUCE_THRESHOLD", env_float("PM_REDUCE_SCORE", 72.0))
    close_th = env_float("PM_CLOSE_FULL_THRESHOLD", env_float("PM_CLOSE_SCORE", 85.0)) + (10.0 if missing else 0.0)
    emergency_th = env_float("PM_EMERGENCY_THRESHOLD", env_float("PM_EMERGENCY_SCORE", 92.0)) + (10.0 if missing else 0.0)
    if score >= emergency_th:
        verdict = "EMERGENCY_CLOSE"
    elif score >= close_th:
        verdict = "CLOSE_FULL"
    elif score >= reduce_th:
        verdict = "REDUCE_50"
    elif score >= protect_th:
        verdict = "PROTECT_ONLY"
    else:
        verdict = "HOLD_PLAN"
    return {"ok": True, "symbol": symbol, "score": round(score, 4), "confidence": round(confidence, 4), "missing_context": missing, "verdict": verdict, "verdict_action": verdict, "thresholds": {"protect": protect_th, "reduce": reduce_th, "close": close_th, "emergency": emergency_th}, "features": details}


def pm_action_budget_guard(symbol: str, action: str) -> Dict[str, Any]:
    state = load_json_file(PM_ACTION_STATE_FILE, {})
    today = utc_now_iso()[:10]
    symbol_key = v010_normalize_symbol(symbol)
    rows = [r for r in (state.get("actions") or []) if isinstance(r, dict)]
    per_pos = [r for r in rows if r.get("symbol") == symbol_key]
    today_rows = [r for r in rows if str(r.get("event_at_utc") or "")[:10] == today]
    cooldown_min = env_float("PM_ACTION_COOLDOWN_PER_POSITION_MIN", 60.0)
    if per_pos:
        last = str(per_pos[-1].get("event_at_utc") or "")
        try:
            elapsed = (datetime.now(timezone.utc) - datetime.fromisoformat(last.replace("Z", "+00:00"))).total_seconds() / 60.0
            if elapsed < cooldown_min:
                return {"ok": False, "reason": "pm_action_cooldown_active", "cooldown_min": cooldown_min}
        except Exception:
            pass
    if len(per_pos) >= env_int("PM_MAX_ACTIONS_PER_POSITION", 2):
        return {"ok": False, "reason": "pm_max_actions_per_position_reached"}
    if action in ("CLOSE_FULL", "EMERGENCY_CLOSE") and len([r for r in today_rows if r.get("action") in ("CLOSE_FULL", "EMERGENCY_CLOSE")]) >= env_int("PM_MAX_MANAGER_CLOSES_PER_DAY", 2):
        return {"ok": False, "reason": "pm_max_manager_closes_per_day_reached"}
    if action == "REDUCE_50" and len([r for r in today_rows if r.get("action") == "REDUCE_50"]) >= env_int("PM_MAX_REDUCES_PER_DAY", 3):
        return {"ok": False, "reason": "pm_max_reduces_per_day_reached"}
    return {"ok": True}


def pm_action_budget_record(symbol: str, action: str, result: Dict[str, Any]) -> None:
    state = load_json_file(PM_ACTION_STATE_FILE, {})
    rows = state.get("actions") if isinstance(state.get("actions"), list) else []
    rows.append({"event_at_utc": utc_now_iso(), "symbol": v010_normalize_symbol(symbol), "action": action, "ok": bool((result.get("live_action_result") or {}).get("ok"))})
    save_json_file(PM_ACTION_STATE_FILE, {"actions": rows[-1000:]})
    append_jsonl(MANAGER_ACTIONS_LOG, rows[-1])


def pm_record_manager_decision(decision: Dict[str, Any]) -> None:
    append_jsonl(MANAGER_DECISIONS_LOG, {"event_at_utc": utc_now_iso(), **(decision or {})})


def pm_send_report(report: Dict[str, Any]) -> Dict[str, Any]:
    fn = globals().get("send_telegram_message")
    if not callable(fn) or not (env_bool("PM_REPORT_TELEGRAM_ENABLED", False) or env_bool("PM_SEND_REPORT_ENABLED", False)):
        result = {"ok": True, "decision": "REPORT_PREVIEW_ONLY", "reason": "telegram_disabled_or_unavailable"}
        append_jsonl(MANAGER_HOURLY_REPORTS_LOG, {"event_at_utc": utc_now_iso(), "report": report, "send_result": result})
        return result
    text = f"PM report {report.get('symbol') or '-'} decision={(report.get('reconcile') or {}).get('mismatch_state')} safe={(report.get('safety_summary') or {}).get('safe_to_continue')}"
    res = fn(text)
    result = {"ok": True, "decision": "REPORT_SENT", "telegram_result": res}
    append_jsonl(MANAGER_HOURLY_REPORTS_LOG, {"event_at_utc": utc_now_iso(), "report": report, "send_result": result})
    return result

@app.post("/testnet/tp-lifecycle-check")
async def v017_testnet_tp_lifecycle_check(request: Request):
    if not v010_auth_ok(request):
        return {"ok": False, "decision": "REJECT", "reason": "unauthorized"}

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    symbol = v010_normalize_symbol(payload.get("symbol") or payload.get("pair") or "")
    signal_key = str(payload.get("signal_key") or payload.get("signal_id") or "").strip()
    return v017_tp_lifecycle_check_core(symbol, signal_key)

@app.get("/testnet/execution-summary")
def v014_execution_summary(request: Request, signal_key: str = ""):
    if not v010_auth_ok(request):
        return {"ok": False, "decision": "REJECT", "reason": "unauthorized"}
    row = v014_execution_summary_latest(signal_key)
    if not row:
        return {"ok": False, "signal_key": signal_key, "reason": "not_found"}
    return {"ok": True, "signal_key": signal_key, "summary": row}


@app.get("/testnet/safety-summary")
def v014_safety_summary_endpoint(request: Request, symbol: str = ""):
    if not v010_auth_ok(request):
        return {"ok": False, "decision": "REJECT", "reason": "unauthorized"}
    return v014_safety_summary(symbol)


def _extract_event_day(row: Dict[str, Any], primary_fields: list[str], fallback_fields: Optional[list[str]] = None) -> str:
    for field in primary_fields:
        ts = str(row.get(field) or "").strip()
        if ts and len(ts) >= 10:
            return ts[:10]
    for field in (fallback_fields or []):
        ts = str(row.get(field) or "").strip()
        if ts and len(ts) >= 10:
            dt = parse_iso_utc(ts)
            if dt:
                return dt.astimezone(WIB).date().isoformat()
            return ts[:10]
    return ""


def count_events_today(path: Path, day: str, timestamp_fields: list[str], filter_fn, fallback_timestamp_fields: Optional[list[str]] = None) -> Optional[int]:
    if not path.exists():
        return None
    c = 0
    seen = 0
    try:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                row_day = _extract_event_day(row, timestamp_fields, fallback_timestamp_fields)
                if row_day == day:
                    seen += 1
                    if filter_fn(row):
                        c += 1
    except Exception:
        return None
    if seen == 0:
        return None
    return c


def sum_events_today(path: Path, day: str, timestamp_fields: list[str], field: str, filter_fn, fallback_timestamp_fields: Optional[list[str]] = None) -> Optional[int]:
    if not path.exists():
        return None
    total = 0
    seen = 0
    try:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                row_day = _extract_event_day(row, timestamp_fields, fallback_timestamp_fields)
                if row_day == day:
                    seen += 1
                    if not filter_fn(row):
                        continue
                    try:
                        val = int(row.get(field) or 0)
                    except Exception:
                        continue
                    if val > 0:
                        total += val
    except Exception:
        return None
    if seen == 0:
        return None
    return total


def _safe_metric_text(v: Optional[int]) -> str:
    if v is None:
        return "pending"
    return str(v)


def _parse_date_ymd(raw: str) -> str:
    t = str(raw or "").strip()
    if not t:
        return ""
    if len(t) >= 10:
        return t[:10]
    return ""


def _resolve_report_date_wib(payload: Optional[OperatorSymbolPayload]) -> str:
    if payload:
        d_wib = _parse_date_ymd(payload.date_wib or "")
        if d_wib:
            return d_wib
        d_utc = _parse_date_ymd(payload.date_utc or "")
        if d_utc:
            try:
                dt = datetime.fromisoformat(f"{d_utc}T00:00:00+00:00").astimezone(WIB)
                return dt.date().isoformat()
            except Exception:
                return d_utc
    return datetime.now(WIB).date().isoformat()


def paper_performance_daily_stats(date_wib: str) -> Dict[str, Any]:
    closed_trades = win = loss = be = 0
    tp1_count = tp2_count = tp3_count = sl_count = 0
    manual_count = needs_review_count = 0
    r_vals = []
    pnl_sum = 0.0
    pnl_count = 0
    est_net_sum = 0.0
    est_net_count = 0
    fees_sum = 0.0
    fees_count = 0

    if not PAPER_PERFORMANCE_LOG.exists():
        return {
            "ok": True,
            "date_wib": date_wib,
            "closed_trades": 0,
            "win": 0,
            "loss": 0,
            "be": 0,
            "win_loss_be": "0/0/0",
            "avg_r": "pending",
            "gross_pnl_usdt": "pending",
            "estimated_net_pnl_usdt": "pending",
            "total_fees_estimated": "pending",
            "tp1_count": 0,
            "tp2_count": 0,
            "tp3_count": 0,
            "sl_count": 0,
            "manual_count": 0,
            "needs_review_count": 0,
        }

    for line in PAPER_PERFORMANCE_LOG.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        closed = str(row.get("closed_at_wib") or "")[:10]
        if closed != date_wib:
            continue
        closed_trades += 1
        wl = str(row.get("win_loss_be") or "").upper()
        if wl == "WIN": win += 1
        elif wl == "LOSS": loss += 1
        elif wl == "BE": be += 1
        if bool(row.get("needs_review")): needs_review_count += 1
        out = str(row.get("outcome") or "").upper()
        if out == "TP1": tp1_count += 1
        if out == "TP2": tp2_count += 1
        if out == "TP3": tp3_count += 1
        if out == "SL": sl_count += 1
        if out in {"MANUAL_PROFIT","MANUAL_LOSS","CLOSED_MANUAL"}: manual_count += 1
        if bool(row.get("include_performance")):
            rv = to_float_or_none(row.get("r_realized"))
            if rv is not None: r_vals.append(rv)
        pv = to_float_or_none(row.get("gross_pnl_usdt"))
        if pv is not None:
            pnl_sum += pv
            pnl_count += 1
        nv = to_float_or_none(row.get("estimated_net_pnl_usdt"))
        if nv is not None:
            est_net_sum += nv
            est_net_count += 1
        fv = to_float_or_none(row.get("total_fees_estimated"))
        if fv is not None:
            fees_sum += fv
            fees_count += 1

    return {
        "ok": True,
        "date_wib": date_wib,
        "closed_trades": closed_trades,
        "win": win,
        "loss": loss,
        "be": be,
        "win_loss_be": f"{win}/{loss}/{be}",
        "avg_r": (sum(r_vals)/len(r_vals)) if r_vals else "pending",
        "gross_pnl_usdt": pnl_sum if pnl_count > 0 else "pending",
        "estimated_net_pnl_usdt": est_net_sum if est_net_count > 0 else "pending",
        "total_fees_estimated": fees_sum if fees_count > 0 else "pending",
        "tp1_count": tp1_count,
        "tp2_count": tp2_count,
        "tp3_count": tp3_count,
        "sl_count": sl_count,
        "manual_count": manual_count,
        "needs_review_count": needs_review_count,
    }


@app.get("/paper/performance/daily")
def paper_performance_daily(date_wib: str, x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"), x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret")) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)
    d = _parse_date_ymd(date_wib)
    if not d:
        raise HTTPException(status_code=400, detail="invalid_date_wib")
    return paper_performance_daily_stats(d)






# =========================
# Operator Candle Health
# =========================

def _market_interval_ms(interval: str) -> Optional[int]:
    m = {
        "1m": 60_000,
        "5m": 5 * 60_000,
        "15m": 15 * 60_000,
        "4h": 4 * 60 * 60_000,
    }
    return m.get(str(interval or "").strip())


def _parse_csv_list(raw: Any) -> List[str]:
    txt = str(raw or "").strip()
    if not txt:
        return []
    return [x.strip() for x in txt.replace(";", ",").split(",") if x.strip()]


def market_candle_health(symbols: Optional[List[str]] = None, intervals: Optional[List[str]] = None) -> Dict[str, Any]:
    now_ms = int(time.time() * 1000)

    clean_symbols = [
        v010_normalize_symbol(s)
        for s in (symbols or market_allowlist_symbols())
        if v010_normalize_symbol(s)
    ]

    clean_intervals = [
        str(x).strip()
        for x in (intervals or ["5m", "15m", "4h"])
        if str(x).strip() in {"1m", "5m", "15m", "4h"}
    ] or ["5m", "15m", "4h"]

    grace_mult = max(0.0, env_float("CANDLE_HEALTH_GRACE_MULT", 0.20))
    min_grace_sec = max(0, env_int("CANDLE_HEALTH_MIN_GRACE_SEC", 120))

    rows: List[Dict[str, Any]] = []
    missing_count = 0
    stale_count = 0
    invalid_count = 0

    for symbol in clean_symbols:
        for interval in clean_intervals:
            expected_ms = _market_interval_ms(interval)
            if not expected_ms:
                invalid_count += 1
                rows.append({
                    "symbol": symbol,
                    "interval": interval,
                    "ok": False,
                    "status": "INVALID_INTERVAL",
                    "reason": "unsupported_interval",
                    "count": 0,
                    "last_close_time_ms": None,
                    "last_close_wib": None,
                    "age_min": None,
                    "max_age_min": None,
                })
                continue

            candles = market_load_candles(symbol, interval)
            closed = [r for r in candles if bool(r.get("is_closed", True))]
            count = len(closed)

            if not closed:
                missing_count += 1
                rows.append({
                    "symbol": symbol,
                    "interval": interval,
                    "ok": False,
                    "status": "MISSING",
                    "reason": "no_closed_candles",
                    "count": 0,
                    "last_close_time_ms": None,
                    "last_close_wib": None,
                    "age_min": None,
                    "max_age_min": round((expected_ms + max(min_grace_sec * 1000, int(expected_ms * grace_mult))) / 60000.0, 2),
                })
                continue

            last = closed[-1]
            try:
                last_close_ms = int(last.get("close_time_ms") or last.get("t") or 0)
            except Exception:
                last_close_ms = 0

            if last_close_ms <= 0:
                missing_count += 1
                rows.append({
                    "symbol": symbol,
                    "interval": interval,
                    "ok": False,
                    "status": "MISSING",
                    "reason": "invalid_last_close_time",
                    "count": count,
                    "last_close_time_ms": None,
                    "last_close_wib": None,
                    "age_min": None,
                    "max_age_min": round((expected_ms + max(min_grace_sec * 1000, int(expected_ms * grace_mult))) / 60000.0, 2),
                })
                continue

            grace_ms = max(min_grace_sec * 1000, int(expected_ms * grace_mult))
            max_age_ms = expected_ms + grace_ms
            age_ms = max(0, now_ms - last_close_ms)
            stale = age_ms > max_age_ms

            if stale:
                stale_count += 1

            last_close_wib = datetime.fromtimestamp(last_close_ms / 1000.0, timezone.utc).astimezone(WIB).strftime("%Y-%m-%d %H:%M:%S WIB")

            rows.append({
                "symbol": symbol,
                "interval": interval,
                "ok": not stale,
                "status": "OK" if not stale else "STALE",
                "reason": "ok" if not stale else "stale_candle",
                "count": count,
                "last_close_time_ms": last_close_ms,
                "last_close_wib": last_close_wib,
                "age_min": round(age_ms / 60000.0, 2),
                "max_age_min": round(max_age_ms / 60000.0, 2),
            })

    total = len(rows)
    ok_count = sum(1 for r in rows if r.get("ok"))
    bad_count = total - ok_count
    coverage_pct = round((ok_count / total) * 100.0, 2) if total else 0.0
    status = "OK" if bad_count == 0 else "WARN"

    return {
        "ok": bad_count == 0,
        "status": status,
        "coverage_pct": coverage_pct,
        "total_checks": total,
        "ok_count": ok_count,
        "bad_count": bad_count,
        "missing_count": missing_count,
        "stale_count": stale_count,
        "invalid_count": invalid_count,
        "symbols": clean_symbols,
        "intervals": clean_intervals,
        "rows": rows,
        "timestamp_utc": utc_now_iso(),
        "timestamp_wib": wib_now_iso(),
    }


def format_market_candle_health_message(h: Dict[str, Any]) -> str:
    rows = h.get("rows") or []
    bad = [r for r in rows if not r.get("ok")]

    lines = [
        "🕯️ CANDLE HEALTH",
        f"Status: {h.get('status')}",
        f"Time: {h.get('timestamp_wib')}",
        f"Coverage: {h.get('coverage_pct')}%",
        f"OK: {h.get('ok_count')}/{h.get('total_checks')}",
        f"Missing: {h.get('missing_count')}",
        f"Stale: {h.get('stale_count')}",
        "",
        "Bad rows:",
    ]

    if not bad:
        lines.append("- none")
    else:
        for r in bad[:25]:
            lines.append(
                f"- {r.get('symbol')} {r.get('interval')} | {r.get('status')} | age={r.get('age_min')}m | max={r.get('max_age_min')}m | last={r.get('last_close_wib')}"
            )

    if len(bad) > 25:
        lines.append(f"- ...and {len(bad) - 25} more")

    return "\n".join(lines)


@app.api_route("/operator/candle-health", methods=["GET", "POST"])
def operator_candle_health(request: Request, symbol: str = "", intervals: str = "5m,15m,4h"):
    if not v010_auth_ok(request):
        return {"ok": False, "decision": "REJECT", "reason": "unauthorized"}

    symbols = _parse_csv_list(symbol)
    health = market_candle_health(
        symbols=symbols or None,
        intervals=_parse_csv_list(intervals) or ["5m", "15m", "4h"],
    )
    return health


@app.post("/operator/candle-health/telegram")
def operator_candle_health_telegram(request: Request, symbol: str = "", intervals: str = "5m,15m,4h"):
    if not v010_auth_ok(request):
        return {"ok": False, "decision": "REJECT", "reason": "unauthorized"}

    symbols = _parse_csv_list(symbol)
    health = market_candle_health(
        symbols=symbols or None,
        intervals=_parse_csv_list(intervals) or ["5m", "15m", "4h"],
    )
    msg = format_market_candle_health_message(health)
    telegram = send_telegram_message(msg)

    return {
        "ok": bool(health.get("ok")) and bool(telegram.get("ok")),
        "health": health,
        "telegram": telegram,
    }





# =========================
# Operator Candle Auto-Heal
# =========================

def market_candle_health_auto_heal(
    symbols: Optional[List[str]] = None,
    intervals: Optional[List[str]] = None,
    limit: int = 800,
    dry_run: bool = False,
) -> Dict[str, Any]:
    before = market_candle_health(symbols=symbols, intervals=intervals)
    bad_rows = [r for r in (before.get("rows") or []) if not r.get("ok")]

    heal_symbols = sorted({str(r.get("symbol") or "").upper() for r in bad_rows if r.get("symbol")})
    heal_intervals = sorted({str(r.get("interval") or "") for r in bad_rows if r.get("interval")})

    if not bad_rows:
        return {
            "ok": True,
            "action": "NOOP",
            "reason": "candle_health_ok",
            "before": before,
            "bootstrap": None,
            "after": before,
            "timestamp_utc": utc_now_iso(),
            "timestamp_wib": wib_now_iso(),
        }

    plan = {
        "symbols": heal_symbols,
        "intervals": heal_intervals,
        "bad_count": len(bad_rows),
        "bad_rows": bad_rows,
        "limit": int(limit or 800),
    }

    if dry_run:
        return {
            "ok": False,
            "action": "DRY_RUN",
            "reason": "stale_or_missing_detected",
            "plan": plan,
            "before": before,
            "bootstrap": None,
            "after": before,
            "timestamp_utc": utc_now_iso(),
            "timestamp_wib": wib_now_iso(),
        }

    bootstrap = market_rest_bootstrap(heal_symbols, heal_intervals, limit=int(limit or 800))
    after = market_candle_health(symbols=symbols, intervals=intervals)
    repaired = bool(after.get("ok"))

    return {
        "ok": repaired,
        "action": "BOOTSTRAP",
        "reason": "auto_heal_completed" if repaired else "auto_heal_incomplete",
        "plan": plan,
        "before": before,
        "bootstrap": bootstrap,
        "after": after,
        "timestamp_utc": utc_now_iso(),
        "timestamp_wib": wib_now_iso(),
    }


def format_market_candle_auto_heal_message(res: Dict[str, Any]) -> str:
    action = res.get("action")
    before = res.get("before") or {}
    after = res.get("after") or {}
    plan = res.get("plan") or {}

    lines = [
        "🛠️ CANDLE AUTO-HEAL",
        f"Action: {action}",
        f"Result: {'OK' if res.get('ok') else 'WARN'}",
        f"Time: {res.get('timestamp_wib')}",
        "",
        "Before:",
        f"- status={before.get('status')}",
        f"- coverage={before.get('coverage_pct')}%",
        f"- stale={before.get('stale_count')}",
        f"- missing={before.get('missing_count')}",
        "",
        "Plan:",
        f"- symbols={','.join(plan.get('symbols') or []) or '-'}",
        f"- intervals={','.join(plan.get('intervals') or []) or '-'}",
        f"- bad_count={plan.get('bad_count', 0)}",
        "",
        "After:",
        f"- status={after.get('status')}",
        f"- coverage={after.get('coverage_pct')}%",
        f"- stale={after.get('stale_count')}",
        f"- missing={after.get('missing_count')}",
    ]

    bootstrap = res.get("bootstrap")
    if isinstance(bootstrap, dict):
        lines.extend([
            "",
            "Bootstrap:",
            f"- ok={bootstrap.get('ok')}",
            f"- rows_ingested={bootstrap.get('rows_ingested')}",
            f"- failures={len(bootstrap.get('failures') or [])}",
        ])

    return "\n".join(lines)


@app.post("/operator/candle-health/auto-heal")
def operator_candle_health_auto_heal(
    request: Request,
    symbol: str = "",
    intervals: str = "5m,15m,4h",
    limit: int = 800,
    dry_run: bool = False,
):
    if not v010_auth_ok(request):
        return {"ok": False, "decision": "REJECT", "reason": "unauthorized"}

    symbols = _parse_csv_list(symbol)
    res = market_candle_health_auto_heal(
        symbols=symbols or None,
        intervals=_parse_csv_list(intervals) or ["5m", "15m", "4h"],
        limit=limit,
        dry_run=dry_run,
    )
    return res


@app.post("/operator/candle-health/auto-heal/telegram")
def operator_candle_health_auto_heal_telegram(
    request: Request,
    symbol: str = "",
    intervals: str = "5m,15m,4h",
    limit: int = 800,
    dry_run: bool = False,
):
    if not v010_auth_ok(request):
        return {"ok": False, "decision": "REJECT", "reason": "unauthorized"}

    symbols = _parse_csv_list(symbol)
    res = market_candle_health_auto_heal(
        symbols=symbols or None,
        intervals=_parse_csv_list(intervals) or ["5m", "15m", "4h"],
        limit=limit,
        dry_run=dry_run,
    )
    msg = format_market_candle_auto_heal_message(res)
    telegram = send_telegram_message(msg)

    return {
        "ok": bool(res.get("ok")) and bool(telegram.get("ok")),
        "auto_heal": res,
        "telegram": telegram,
    }



@app.api_route("/operator/status", methods=["GET", "POST"])
def operator_status(request: Request, payload: Optional[OperatorSymbolPayload] = None, symbol: str = ""):
    if not v010_auth_ok(request):
        return {"ok": False, "decision": "REJECT", "reason": "unauthorized"}
    req_symbol = symbol or ((payload.symbol if payload else "") or "")
    return operator_status_payload(req_symbol)


@app.post("/operator/send-safety-summary")
def operator_send_safety_summary(request: Request, payload: Optional[OperatorSymbolPayload] = None, symbol: str = ""):
    if not v010_auth_ok(request):
        return {"ok": False, "decision": "REJECT", "reason": "unauthorized"}
    req_symbol = symbol or ((payload.symbol if payload else "") or "")
    summary = operator_status_payload(req_symbol)
    msg = format_safety_summary_message(summary)
    telegram = send_telegram_message(msg)
    return {"ok": True, "safety_summary": summary, "telegram": telegram}


def format_execution_summary_message(signal_key: str, row: Dict[str, Any]) -> str:
    return "\n".join([
        "📄 EXECUTION SUMMARY",
        f"Signal: {signal_key}",
        f"Pair: {row.get('pair') or '-'}",
        f"Symbol: {row.get('symbol') or '-'}",
        f"Dir: {row.get('direction') or row.get('dir') or '-'}",
        f"Paper: {row.get('paper_decision') or '-'} / {row.get('paper_reason') or '-'}",
        f"Lifecycle: {row.get('lifecycle_state') or '-'}",
        f"TP Lifecycle: {row.get('tp_lifecycle_stage') or '-'}",
        f"Entry: {row.get('entry_status') or row.get('entry_result') or '-'}",
        f"Protection: {row.get('protection_status') or row.get('protection_summary') or '-'}",
        f"Notes: {row.get('notes') or '-'}",
        f"Safe: {row.get('safe_restored') if row.get('safe_restored') is not None else '-'}",
    ])


@app.post("/operator/send-execution-summary")
def operator_send_execution_summary(request: Request, payload: Optional[OperatorSymbolPayload] = None, signal_key: str = ""):
    if not v010_auth_ok(request):
        return {"ok": False, "decision": "REJECT", "reason": "unauthorized"}
    key = str(signal_key or (payload.signal_key if payload else "") or "").strip()
    if not key:
        return {"ok": False, "reason": "signal_key_required"}
    row = v014_execution_summary_latest(key)
    if not row:
        return {"ok": False, "reason": "not_found", "signal_key": key}
    msg = format_execution_summary_message(key, row)
    telegram = send_telegram_message(msg)
    return {"ok": True, "signal_key": key, "summary": row, "telegram": telegram}


@app.post("/operator/daily-report")
def operator_daily_report(request: Request, payload: Optional[OperatorSymbolPayload] = None, symbol: str = ""):
    if not v010_auth_ok(request):
        return {"ok": False, "decision": "REJECT", "reason": "unauthorized"}

    req_symbol = symbol or ((payload.symbol if payload else "") or "")
    summary = operator_status_payload(req_symbol)
    state = load_state()
    dc = state.get("daily_counters") or {}
    report_date_wib = _resolve_report_date_wib(payload)
    accept = int(dc.get("accepted_count") or 0)
    reject = int(dc.get("rejected_count") or 0)
    total = accept + reject
    reject_by_gate = dc.get("rejected_by_gate") or {}

    wib_ts_fields = [
        "ts_wib", "timestamp_wib", "created_at_wib", "event_at_wib",
        "decision_at_wib", "accepted_at_wib", "closed_at_wib",
        "run_ts_wib", "confirmed_ts_wib", "signal_time_wib", "received_at_wib",
    ]
    utc_ts_fields = [
        "ts_utc", "timestamp_utc", "created_at_utc", "event_at_utc",
        "decision_at_utc", "accepted_at_utc", "closed_at_utc", "received_at_utc",
        "received_at", "ts", "created_at",
    ]
    signals_received = count_events_today(
        SIGNALS_LOG,
        report_date_wib,
        wib_ts_fields,
        lambda _row: True,
        fallback_timestamp_fields=utc_ts_fields,
    )
    entries_today = count_events_today(
        EXECUTION_PLANS_LOG,
        report_date_wib,
        wib_ts_fields,
        lambda _row: True,
        fallback_timestamp_fields=utc_ts_fields,
    )
    protection_today = count_events_today(
        EXECUTION_EVENTS_LOG,
        report_date_wib,
        wib_ts_fields,
        lambda row: (
            str(row.get("action") or "") == "TESTNET_PLACE_PROTECTION"
            or str(row.get("decision") or "") == "PROTECTION_PLACED"
            or str(row.get("reason") or "") == "all_protection_orders_placed"
        ),
        fallback_timestamp_fields=utc_ts_fields,
    )
    stale_cleanup = sum_events_today(
        EXECUTION_EVENTS_LOG,
        report_date_wib,
        wib_ts_fields,
        "cleanup_count",
        lambda row: int(row.get("cleanup_count") or 0) > 0
        or (
            str(row.get("lifecycle_state") or "") == "POSITION_CLOSED_STALE_ALGO"
            and int(row.get("cleanup_count") or 0) > 0
        ),
        fallback_timestamp_fields=utc_ts_fields,
    )

    max_trades = env_int("MAX_TRADES_PER_DAY", 0)
    max_open = env_int("MAX_OPEN_POSITIONS", 0)
    loss_cap = env_int("MAX_CONSECUTIVE_LOSSES", 0)

    partial_data = signals_received is None
    report_status = "BLOCKED" if not summary.get("safe_to_continue") else ("WARN" if partial_data else "OK")

    reject_lines = []
    for gate, cnt in sorted(reject_by_gate.items()):
        reject_lines.append(f"- {gate}: {int(cnt or 0)}")
    known_reject_total = sum(int(v or 0) for v in reject_by_gate.values())
    reject_lines.append(f"- other: {max(0, reject - known_reject_total)}")

    perf = paper_performance_daily_stats(report_date_wib)

    report_lines = [
        "📊 DAILY BOT REPORT",
        "",
        "SYSTEM",
        f"Date WIB: {report_date_wib}",
        f"Mode: {summary.get('mode')}",
        f"Execution: {summary.get('execution_mode')}",
        f"Env: {summary.get('binance_env')}",
        f"Safe: {str(bool(summary.get('safe_to_continue'))).lower()}",
        f"Mismatch: {summary.get('mismatch_state')}",
        f"Paper Open: {summary.get('open_paper_positions')}",
        f"PositionAmt: {summary.get('positionAmt')}",
        f"Open Algo: {summary.get('open_algo_count')}",
        "",
        "SIGNALS",
        f"Total Received: {_safe_metric_text(signals_received)}",
        f"ACCEPT: {accept}",
        f"REJECT: {reject}",
        "Reject Breakdown:",
        "",
        "EXECUTION",
        f"Entries Today: {_safe_metric_text(entries_today)}",
        f"Protection Placed: {_safe_metric_text(protection_today)}",
        "Lifecycle Clean: pending",
        "TP Lifecycle Stage: pending",
        f"Stale Cleanup: {_safe_metric_text(stale_cleanup)}",
        f"Open Count: {summary.get('open_paper_positions')}",
        "",
        "PERFORMANCE",
        f"Closed Trades: {perf.get('closed_trades')}",
        f"Win/Loss/BE: {perf.get('win_loss_be')}",
        f"Gross PnL: {perf.get('gross_pnl_usdt')}",
        f"Est. Net PnL: {perf.get('estimated_net_pnl_usdt')}",
        f"Fees Estimate: {perf.get('total_fees_estimated')}",
        "Net PnL: pending",
        f"Avg R: {perf.get('avg_r')}",
        f"TP1/TP2/TP3/SL: {perf.get('tp1_count')}/{perf.get('tp2_count')}/{perf.get('tp3_count')}/{perf.get('sl_count')}",
        f"Needs Review: {perf.get('needs_review_count')}",
        "",
        "DISCIPLINE",
        f"Daily Trades Used / MAX_TRADES_PER_DAY: {accept}/{max_trades if max_trades else 'pending'}",
        f"Loss Streak / MAX_CONSECUTIVE_LOSSES: pending/{loss_cap if loss_cap else 'pending'}",
        f"Max Open Position Used / MAX_OPEN_POSITIONS: {summary.get('open_paper_positions')}/{max_open if max_open else 'pending'}",
        "No Queue Rule: ON",
        f"Status: {report_status}",
    ]
    idx = report_lines.index("Reject Breakdown:") + 1
    report_lines[idx:idx] = reject_lines
    report = "\n".join(report_lines)
    telegram = send_telegram_message(report)
    if report_status == "OK" and not telegram.get("ok") and not telegram.get("skipped"):
        report_status = "WARN"
        report = report.replace("Status: OK", "Status: WARN")
    return {"ok": True, "report": report, "telegram": telegram}



# =========================
# QUANT v0.1 SHADOW ENGINE
# =========================

QUANT_SIGNALS_LOG = LOG_DIR / "quant_signals.jsonl"
QUANT_DIAGNOSTICS_LOG = LOG_DIR / "quant_diagnostics.jsonl"
QUANT_STATE_FILE = STATE_DIR / "quant_state.json"

class QuantRunOncePayload(BaseModel):
    model_config = ConfigDict(extra="allow")
    symbols: Optional[List[str]] = None
    force: Optional[bool] = False

def _quant_symbols_default() -> List[str]:
    raw = str(os.getenv("QUANT_SYMBOLS") or "").strip()
    if raw:
        return [x.strip().upper() for x in raw.split(",") if x.strip()]
    return list(quant_engine.DEFAULT_SYMBOLS)

def _quant_cfg() -> Dict[str, Any]:
    return {
        "model_family": str(os.getenv("QUANT_MODEL_FAMILY", "RESIDUAL_STAT_ARB")),
        "model_version": str(os.getenv("QUANT_MODEL_VERSION", "quant_v0.1")),
        "mode": str(os.getenv("QUANT_MODE", "SHADOW_ONLY")),
        "lookback_15m": env_int("QUANT_LOOKBACK_15M", 160),
        "min_abs_z": env_float("QUANT_MIN_ABS_Z", 2.0),
        "min_score": env_int("QUANT_MIN_SCORE_TELEGRAM", 75),
        "sl_atr_mult": env_float("QUANT_SL_ATR_MULT", 1.20),
        "max_atr_pct": env_float("QUANT_MAX_ATR_PCT", 0.05),
        "fee_slip_est": env_float("QUANT_FEE_SLIP_EST", 0.0009),
        "eval_tf": str(os.getenv("QUANT_EVAL_TF", "5m")),
        "max_forward_bars": env_int("QUANT_MAX_FORWARD_BARS", 72),
    }

def _quant_load_candles(symbols: List[str]) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    out: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    required = sorted(set(list(symbols) + ["BTCUSDT", "ETHUSDT"]))
    for sym in required:
        out[sym] = {
            "5m": market_load_candles(sym, "5m"),
            "15m": market_load_candles(sym, "15m"),
            "4h": market_load_candles(sym, "4h"),
        }
    return out

def _quant_should_send(sig: Dict[str, Any], state: Dict[str, Any], force: bool = False) -> Tuple[bool, str]:
    if force:
        return True, "force"
    now_ms = int(time.time() * 1000)
    cooldown_min = env_int("QUANT_COOLDOWN_PAIR_MIN", 90)
    max_daily = env_int("QUANT_MAX_TELEGRAM_PER_DAY", 10)
    key = f"{sig.get('symbol')}|{sig.get('direction')}|{sig.get('model_version')}"
    alerts = state.get("last_alert_ms")
    if not isinstance(alerts, dict):
        alerts = {}
        state["last_alert_ms"] = alerts
    last = int(alerts.get(key) or 0)
    if last and now_ms - last < cooldown_min * 60 * 1000:
        return False, "cooldown"

    day = datetime.now(WIB).strftime("%Y-%m-%d")
    day_counts = state.get("day_counts")
    if not isinstance(day_counts, dict):
        day_counts = {}
        state["day_counts"] = day_counts
    if int(day_counts.get(day) or 0) >= max_daily:
        return False, "daily_limit"

    return True, "ok"

def _quant_mark_sent(sig: Dict[str, Any], state: Dict[str, Any]) -> None:
    now_ms = int(time.time() * 1000)
    key = f"{sig.get('symbol')}|{sig.get('direction')}|{sig.get('model_version')}"
    state.setdefault("last_alert_ms", {})[key] = now_ms
    day = datetime.now(WIB).strftime("%Y-%m-%d")
    state.setdefault("day_counts", {})[day] = int(state.setdefault("day_counts", {}).get(day) or 0) + 1
    state["updated_at_utc"] = utc_now_iso()

@app.get("/quant/status")
def quant_status(
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)
    st = load_json_file(QUANT_STATE_FILE, {})
    return {
        "ok": True,
        "enabled": env_bool("QUANT_ENGINE_ENABLED", False),
        "mode": str(os.getenv("QUANT_MODE", "SHADOW_ONLY")),
        "telegram_enabled": env_bool("QUANT_TELEGRAM_ENABLED", True),
        "execution_enabled": env_bool("QUANT_EXECUTION_ENABLED", False),
        "model_family": str(os.getenv("QUANT_MODEL_FAMILY", "RESIDUAL_STAT_ARB")),
        "model_version": str(os.getenv("QUANT_MODEL_VERSION", "quant_v0.1")),
        "min_score": env_int("QUANT_MIN_SCORE_TELEGRAM", 75),
        "min_abs_z": env_float("QUANT_MIN_ABS_Z", 2.0),
        "symbols": _quant_symbols_default(),
        "state": st,
    }

@app.get("/quant/latest")
def quant_latest(
    limit: int = 20,
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)
    rows = _read_jsonl(QUANT_SIGNALS_LOG)
    return {"ok": True, "rows": rows[-max(1, min(limit, 200)):]}

@app.post("/quant/run-once")
def quant_run_once(
    payload: Optional[QuantRunOncePayload] = None,
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)

    if not env_bool("QUANT_ENGINE_ENABLED", False):
        return {"ok": True, "skipped": True, "reason": "quant_engine_disabled"}

    if env_bool("QUANT_EXECUTION_ENABLED", False):
        return {"ok": False, "error": "QUANT_EXECUTION_ENABLED_MUST_STAY_FALSE_FOR_V0_1"}

    req = payload.model_dump(mode="json") if payload else {}
    symbols = [str(x).upper().strip() for x in (req.get("symbols") or _quant_symbols_default()) if str(x).strip()]
    force = bool(req.get("force") is True)

    cfg = _quant_cfg()
    candles = _quant_load_candles(symbols)
    result = quant_engine.run_once(candles, symbols, cfg)

    append_jsonl(QUANT_DIAGNOSTICS_LOG, {
        "created_at_utc": utc_now_iso(),
        "created_at_wib": wib_now_iso(),
        "result_summary": {
            "confirmed_count": result.get("confirmed_count"),
            "total_symbols": result.get("total_symbols"),
            "model_version": result.get("model_version"),
        },
        "diagnostics": result.get("diagnostics", []),
    })

    state = load_json_file(QUANT_STATE_FILE, {})
    sent = []
    skipped = []

    for sig in result.get("signals", []):
        can_send, reason = _quant_should_send(sig, state, force=force)
        if not can_send:
            skipped.append({"signal_key": sig.get("signal_key"), "reason": reason})
            continue

        append_jsonl(QUANT_SIGNALS_LOG, sig)

        dataset_row = quant_engine.to_dataset_row(sig)
        append_jsonl(ML_DATASET_ROWS_LOG, dataset_row)

        outcome_eval = None
        if env_bool("QUANT_OUTCOME_EVALUATE_AFTER_SIGNAL", True):
            try:
                outcome_eval = evaluate_forward_outcomes(limit=env_int("QUANT_OUTCOME_EVAL_LIMIT", 50), force=False)
            except Exception as e:
                outcome_eval = {"ok": False, "error": str(e)}

        telegram = {"ok": True, "sent": False, "skipped": True, "reason": "quant_telegram_disabled"}
        if env_bool("QUANT_TELEGRAM_ENABLED", True):
            telegram = send_telegram_message(quant_engine.format_telegram(sig))

        _quant_mark_sent(sig, state)

        sent.append({
            "signal_key": sig.get("signal_key"),
            "symbol": sig.get("symbol"),
            "direction": sig.get("direction"),
            "score": sig.get("score"),
            "telegram": telegram,
            "outcome_eval": outcome_eval,
        })

    save_json_file(QUANT_STATE_FILE, state)

    return {
        "ok": True,
        "mode": cfg.get("mode"),
        "execution_enabled": False,
        "confirmed_count": result.get("confirmed_count"),
        "sent_count": len(sent),
        "skipped_count": len(skipped),
        "sent": sent,
        "skipped": skipped,
        "diagnostic_summary": {
            "total_symbols": result.get("total_symbols"),
            "confirmed_count": result.get("confirmed_count"),
            "by_status": {
                k: sum(1 for d in result.get("diagnostics", []) if d.get("status") == k)
                for k in sorted(set(d.get("status") for d in result.get("diagnostics", [])))
            },
        },
    }


# ============================================================
# LATENCY AUDIT PATCH v0.1
# Non-invasive: logs Binance signed request latency + Telegram latency.
# Does not change signal/entry/TP/SL logic.
# ============================================================
try:
    import os as _lat_os
    import json as _lat_json
    import time as _lat_time
    from pathlib import Path as _LatPath
    from datetime import datetime as _lat_datetime, timezone as _lat_timezone, timedelta as _lat_timedelta

    _LAT_WIB = _lat_timezone(_lat_timedelta(hours=7))
    _LAT_ROOT = _LatPath("/app") if _LatPath("/app").exists() else _LatPath(".")
    _LAT_LOG = _LAT_ROOT / "logs" / "latency_audit.jsonl"

    def _lat_now_ms_():
        return int(_lat_time.time() * 1000)

    def _lat_now_wib_():
        return _lat_datetime.now(_LAT_WIB).isoformat(timespec="milliseconds")

    def _lat_enabled_():
        return str(_lat_os.getenv("LATENCY_AUDIT_ENABLED", "true")).lower() in ("1", "true", "yes", "on")

    def _lat_safe_json_(x):
        try:
            _lat_json.dumps(x)
            return x
        except Exception:
            return str(x)

    def _lat_append_(row):
        if not _lat_enabled_():
            return
        try:
            _LAT_LOG.parent.mkdir(parents=True, exist_ok=True)
            row = dict(row or {})
            row.setdefault("event_at_wib", _lat_now_wib_())
            with _LAT_LOG.open("a", encoding="utf-8") as f:
                f.write(_lat_json.dumps(row, ensure_ascii=False, default=str) + "\n")
        except Exception as e:
            try:
                print(f"[latency_audit] append failed: {e}")
            except Exception:
                pass

    if "live_signed_request" in globals() and not globals().get("_LATENCY_AUDIT_LIVE_SIGNED_PATCHED"):
        _LATENCY_AUDIT_ORIG_LIVE_SIGNED_REQUEST = live_signed_request

        def live_signed_request(method, path, params=None, *args, **kwargs):
            _t0 = _lat_time.perf_counter()
            _ms0 = _lat_now_ms_()
            _err = None
            _res = None
            try:
                _res = _LATENCY_AUDIT_ORIG_LIVE_SIGNED_REQUEST(method, path, params, *args, **kwargs)
                return _res
            except Exception as e:
                _err = e
                raise
            finally:
                try:
                    _dt = int((_lat_time.perf_counter() - _t0) * 1000)
                    _params = params if isinstance(params, dict) else {}
                    _body = _res.get("body") if isinstance(_res, dict) else None
                    _row = {
                        "kind": "BINANCE_SIGNED_REQUEST",
                        "started_ms": _ms0,
                        "duration_ms": _dt,
                        "method": str(method),
                        "path": str(path),
                        "symbol": str(_params.get("symbol") or ""),
                        "side": str(_params.get("side") or ""),
                        "type": str(_params.get("type") or _params.get("algoType") or ""),
                        "client_order_id": str(_params.get("newClientOrderId") or _params.get("clientOrderId") or _params.get("clientAlgoId") or ""),
                        "has_error": bool(_err),
                        "error": str(_err)[:500] if _err else "",
                    }

                    if isinstance(_res, dict):
                        _row["res_ok"] = _res.get("ok")
                        _row["http_code"] = _res.get("httpCode") or _res.get("status_code") or _res.get("code")

                    if isinstance(_body, dict):
                        _row["binance_code"] = _body.get("code")
                        _row["binance_msg"] = str(_body.get("msg") or "")[:300]
                        _row["order_id"] = _body.get("orderId") or _body.get("algoId")
                        _row["status"] = _body.get("status")
                    elif isinstance(_body, list):
                        _row["body_len"] = len(_body)

                    # Fokus endpoint yang penting buat latency eksekusi
                    if str(path) in (
                        "/fapi/v1/order",
                        "/fapi/v1/algoOrder",
                        "/fapi/v1/openOrders",
                        "/fapi/v1/positionRisk",
                        "/fapi/v1/ticker/price",
                    ):
                        _lat_append_(_row)
                except Exception as e:
                    try:
                        print(f"[latency_audit] live_signed wrapper log failed: {e}")
                    except Exception:
                        pass

        globals()["_LATENCY_AUDIT_LIVE_SIGNED_PATCHED"] = True
        print("[latency_audit] live_signed_request patched")

    if "send_telegram_message" in globals() and not globals().get("_LATENCY_AUDIT_TELEGRAM_PATCHED"):
        _LATENCY_AUDIT_ORIG_SEND_TELEGRAM_MESSAGE = send_telegram_message

        def send_telegram_message(text, *args, **kwargs):
            _t0 = _lat_time.perf_counter()
            _ms0 = _lat_now_ms_()
            _err = None
            _res = None
            try:
                _res = _LATENCY_AUDIT_ORIG_SEND_TELEGRAM_MESSAGE(text, *args, **kwargs)
                return _res
            except Exception as e:
                _err = e
                raise
            finally:
                try:
                    _lat_append_({
                        "kind": "TELEGRAM_SEND",
                        "started_ms": _ms0,
                        "duration_ms": int((_lat_time.perf_counter() - _t0) * 1000),
                        "text_len": len(str(text or "")),
                        "has_error": bool(_err),
                        "error": str(_err)[:500] if _err else "",
                        "res": _lat_safe_json_(_res),
                    })
                except Exception as e:
                    try:
                        print(f"[latency_audit] telegram wrapper log failed: {e}")
                    except Exception:
                        pass

        globals()["_LATENCY_AUDIT_TELEGRAM_PATCHED"] = True
        print("[latency_audit] send_telegram_message patched")

except Exception as _lat_patch_e:
    try:
        print(f"[latency_audit] patch init failed: {_lat_patch_e}")
    except Exception:
        pass



# === NIGHT_ACCURACY_FIX_V20260611 ===
# Fix:
# 1) Old/stale VPS_SMC bridge plan is never executed as-is.
# 2) If stale/opportunity missed -> quick candle refresh -> recompute SMC -> execute only fresh plan if still valid.
# 3) Pre-entry live price guard before pipeline execution.
# 4) Does NOT rebase/adjust plan levels.
_NIGHT_REFRESH_INFLIGHT = set()

def _night_bool(name, default=False):
    try:
        raw = os.getenv(name)
        if raw is None:
            return bool(default)
        return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")
    except Exception:
        return bool(default)

def _night_float(name, default):
    try:
        raw = os.getenv(name)
        if raw is None or str(raw).strip() == "":
            return float(default)
        return float(str(raw).strip())
    except Exception:
        return float(default)

def _night_int(name, default):
    try:
        raw = os.getenv(name)
        if raw is None or str(raw).strip() == "":
            return int(default)
        return int(str(raw).strip())
    except Exception:
        return int(default)

def _night_dec(x):
    try:
        if x is None or str(x).strip() == "":
            return None
        return Decimal(str(x))
    except Exception:
        return None

def _night_symbol(p):
    return v010_normalize_symbol((p or {}).get("symbol") or (p or {}).get("pair") or "")

def _night_now_ms():
    return int(time.time() * 1000)

def _night_age_sec(ms):
    try:
        if ms is None or str(ms).strip() == "":
            return None
        return max(0.0, (_night_now_ms() - float(ms)) / 1000.0)
    except Exception:
        return None

def _night_latest_closed_age(symbol, interval):
    try:
        rows = market_load_candles(symbol, interval)
        if not rows:
            return {"ok": False, "reason": "NO_CANDLES", "symbol": symbol, "interval": interval}
        latest = sorted(rows, key=lambda r: int(r.get("close_time_ms") or r.get("open_time_ms") or 0))[-1]
        close_ms = int(latest.get("close_time_ms") or 0)
        age = _night_age_sec(close_ms)
        return {
            "ok": close_ms > 0 and age is not None,
            "symbol": symbol,
            "interval": interval,
            "close_time_ms": close_ms,
            "age_sec": age,
            "close": latest.get("close"),
        }
    except Exception as e:
        return {"ok": False, "reason": f"latest_age_error:{e}", "symbol": symbol, "interval": interval}

def _night_live_price(symbol):
    try:
        import urllib.parse as _night_urlparse
        url = "https://fapi.binance.com/fapi/v1/ticker/price?symbol=" + _night_urlparse.quote(symbol)
        data = http_get_json(url)
        px = _night_dec((data or {}).get("price") if isinstance(data, dict) else None)
        if px is None:
            return {"ok": False, "reason": "live_price_missing", "raw": data}
        return {"ok": True, "symbol": symbol, "price": str(px)}
    except Exception as e:
        return {"ok": False, "reason": f"live_price_error:{e}"}

def _night_payload_direction(p):
    raw = str(
        (p or {}).get("direction")
        or (p or {}).get("dir")
        or ((p or {}).get("stageb_confirmation") or {}).get("stageb_direction")
        or ""
    ).upper()
    if raw.startswith("LONG") or raw == "BUY":
        return "LONG"
    if raw.startswith("SHORT") or raw == "SELL":
        return "SHORT"
    return raw or "UNKNOWN"

def _night_pick_num(p, *keys):
    for k in keys:
        d = _night_dec((p or {}).get(k))
        if d is not None and d > 0:
            return d
    payload = (p or {}).get("payload") if isinstance((p or {}).get("payload"), dict) else {}
    for k in keys:
        d = _night_dec(payload.get(k))
        if d is not None and d > 0:
            return d
    return None

def _night_guard_payload(p):
    symbol = _night_symbol(p)
    direction = _night_payload_direction(p)

    guard = {
        "ok": True,
        "action": "PASS",
        "reason": "fresh_and_executable",
        "symbol": symbol,
        "direction": direction,
        "checked_at_utc": utc_now_iso(),
    }

    if not symbol:
        guard.update({"ok": False, "action": "NO_TRADE", "reason": "MISSING_SYMBOL"})
        return guard

    # Freshness check from local candle store.
    stageb_5m = _night_latest_closed_age(symbol, "5m")
    entry_15m = _night_latest_closed_age(symbol, "15m")
    htf_4h = _night_latest_closed_age(symbol, "4h")

    guard["freshness"] = {
        "stageb_5m": stageb_5m,
        "entry_15m": entry_15m,
        "htf_4h": htf_4h,
        "max_stageb_5m_sec": _night_int("VPS_SMC_STAGEB_5M_MAX_AGE_SEC", 420),
        "max_entry_15m_sec": _night_int("VPS_SMC_ENTRY_15M_MAX_AGE_SEC", 1080),
        "max_htf_4h_sec": _night_int("VPS_SMC_HTF_4H_MAX_AGE_SEC", 15600),
    }

    def _refresh(reason):
        guard.update({"ok": False, "action": "REFRESH_RECOMPUTE", "reason": reason})
        return guard

    if not stageb_5m.get("ok"):
        return _refresh("STALE_PLAN_STAGEB_5M_NOT_AVAILABLE")
    if not entry_15m.get("ok"):
        return _refresh("STALE_PLAN_ENTRY_15M_NOT_AVAILABLE")
    if not htf_4h.get("ok"):
        return _refresh("STALE_PLAN_HTF_4H_NOT_AVAILABLE")

    if float(stageb_5m.get("age_sec") or 999999) > _night_int("VPS_SMC_STAGEB_5M_MAX_AGE_SEC", 420):
        return _refresh("STALE_PLAN_STAGEB_5M_TOO_OLD")
    if float(entry_15m.get("age_sec") or 999999) > _night_int("VPS_SMC_ENTRY_15M_MAX_AGE_SEC", 1080):
        return _refresh("STALE_PLAN_ENTRY_15M_TOO_OLD")
    if float(htf_4h.get("age_sec") or 999999) > _night_int("VPS_SMC_HTF_4H_MAX_AGE_SEC", 15600):
        return _refresh("STALE_PLAN_HTF_4H_TOO_OLD")

    # Pre-entry live price accuracy guard.
    if not _night_bool("PRE_ENTRY_PLAN_GUARD_ENABLED", True):
        guard["pre_entry"] = {"ok": True, "reason": "disabled"}
        return guard

    tp1 = _night_pick_num(p, "tp1", "raw_tp1")
    sl = _night_pick_num(p, "sl", "invalid")
    entry_mid = _night_pick_num(p, "entry_mid", "entry", "entry_price")

    if tp1 is None or sl is None:
        guard.update({"ok": False, "action": "NO_TRADE", "reason": "PRE_ENTRY_MISSING_TP1_OR_SL"})
        guard["pre_entry"] = {"tp1": str(tp1), "sl": str(sl)}
        return guard

    live = _night_live_price(symbol)
    guard["pre_entry"] = {"live_price_result": live, "tp1": str(tp1), "sl": str(sl), "entry_mid": str(entry_mid) if entry_mid is not None else None}

    if not live.get("ok"):
        return _refresh("PRE_ENTRY_LIVE_PRICE_UNAVAILABLE")

    live_price = _night_dec(live.get("price"))
    if live_price is None:
        return _refresh("PRE_ENTRY_LIVE_PRICE_BAD")

    min_rr = Decimal(str(_night_float("MIN_ENTRY_RR_TO_TP1", 0.70)))
    max_adverse_pct = Decimal(str(_night_float("MAX_ENTRY_ADVERSE_SLIPPAGE_PCT", 0.30)))

    if direction == "LONG":
        if _night_bool("REJECT_IF_TP1_TOUCHED_BEFORE_ENTRY", True) and live_price >= tp1:
            return _refresh("TP1_ALREADY_TOUCHED_BEFORE_ENTRY_REFRESH_RECOMPUTE")
        if _night_bool("REJECT_IF_SL_TOUCHED_BEFORE_ENTRY", True) and live_price <= sl:
            guard.update({"ok": False, "action": "NO_TRADE", "reason": "SL_ALREADY_TOUCHED_BEFORE_ENTRY_INVALID"})
            return guard

        risk = live_price - sl
        reward = tp1 - live_price
        if risk <= 0 or reward <= 0:
            return _refresh("PRE_ENTRY_INVALID_LIVE_RR_GEOMETRY")

        rr = reward / risk
        guard["pre_entry"]["rr_to_tp1"] = str(rr)
        if rr < min_rr:
            return _refresh("LIVE_RR_TOO_LOW_REFRESH_RECOMPUTE")

        if entry_mid is not None and live_price > entry_mid:
            adverse_pct = ((live_price - entry_mid) / entry_mid) * Decimal("100")
            guard["pre_entry"]["adverse_pct"] = str(adverse_pct)
            if adverse_pct > max_adverse_pct:
                return _refresh("ENTRY_TOO_FAR_FROM_PLAN_REFRESH_RECOMPUTE")

    elif direction == "SHORT":
        if _night_bool("REJECT_IF_TP1_TOUCHED_BEFORE_ENTRY", True) and live_price <= tp1:
            return _refresh("TP1_ALREADY_TOUCHED_BEFORE_ENTRY_REFRESH_RECOMPUTE")
        if _night_bool("REJECT_IF_SL_TOUCHED_BEFORE_ENTRY", True) and live_price >= sl:
            guard.update({"ok": False, "action": "NO_TRADE", "reason": "SL_ALREADY_TOUCHED_BEFORE_ENTRY_INVALID"})
            return guard

        risk = sl - live_price
        reward = live_price - tp1
        if risk <= 0 or reward <= 0:
            return _refresh("PRE_ENTRY_INVALID_LIVE_RR_GEOMETRY")

        rr = reward / risk
        guard["pre_entry"]["rr_to_tp1"] = str(rr)
        if rr < min_rr:
            return _refresh("LIVE_RR_TOO_LOW_REFRESH_RECOMPUTE")

        if entry_mid is not None and live_price < entry_mid:
            adverse_pct = ((entry_mid - live_price) / entry_mid) * Decimal("100")
            guard["pre_entry"]["adverse_pct"] = str(adverse_pct)
            if adverse_pct > max_adverse_pct:
                return _refresh("ENTRY_TOO_FAR_FROM_PLAN_REFRESH_RECOMPUTE")

    else:
        guard.update({"ok": False, "action": "NO_TRADE", "reason": "PRE_ENTRY_UNKNOWN_DIRECTION"})
        return guard

    return guard

def _night_append_event(row):
    try:
        row = dict(row or {})
        row["created_at_utc"] = utc_now_iso()
        append_jsonl(LOG_DIR / "night_accuracy_fix_events.jsonl", row)
    except Exception:
        pass

def _night_refresh_recompute(old_payload, reason, guard):
    symbol = _night_symbol(old_payload)
    if not symbol:
        return {"ok": False, "decision": "NO_TRADE", "reason": "refresh_recompute_missing_symbol", "guard": guard}

    if symbol in _NIGHT_REFRESH_INFLIGHT:
        _night_append_event({
            "event_type": "NIGHT_REFRESH_RECOMPUTE_BLOCKED_INFLIGHT",
            "symbol": symbol,
            "reason": reason,
            "old_signal_key": (old_payload or {}).get("signal_key"),
            "guard": guard,
        })
        return {
            "ok": False,
            "decision": "NO_TRADE",
            "reason": "FRESH_RECOMPUTE_STILL_BLOCKED",
            "symbol": symbol,
            "guard": guard,
        }

    _NIGHT_REFRESH_INFLIGHT.add(symbol)
    try:
        sync_res = None
        try:
            sync_res = market_rest_bootstrap([symbol], market_intervals(), limit=_night_int("VPS_SMC_REFRESH_BOOTSTRAP_LIMIT", 20))
        except Exception as e:
            sync_res = {"ok": False, "reason": f"quick_sync_error:{e}"}

        old_dedup = os.environ.get("VPS_SMC_DEDUP_ENABLED")
        os.environ["VPS_SMC_DEDUP_ENABLED"] = "false"
        try:
            fresh_run = vps_smc.vps_smc_run_once([symbol])
        finally:
            if old_dedup is None:
                try:
                    del os.environ["VPS_SMC_DEDUP_ENABLED"]
                except Exception:
                    pass
            else:
                os.environ["VPS_SMC_DEDUP_ENABLED"] = old_dedup

        out = {
            "ok": True,
            "decision": "REFRESH_RECOMPUTE_DONE",
            "reason": reason,
            "symbol": symbol,
            "old_signal_key": (old_payload or {}).get("signal_key"),
            "quick_sync": sync_res,
            "fresh_run_summary": {
                "ok": fresh_run.get("ok") if isinstance(fresh_run, dict) else None,
                "signal_count": fresh_run.get("signal_count") if isinstance(fresh_run, dict) else None,
                "summary": fresh_run.get("summary") if isinstance(fresh_run, dict) else None,
            },
            "guard": guard,
        }
        _night_append_event({"event_type": "NIGHT_REFRESH_RECOMPUTE_DONE", **out})
        return out
    finally:
        try:
            _NIGHT_REFRESH_INFLIGHT.remove(symbol)
        except Exception:
            pass

try:
    _NIGHT_ORIGINAL_VPS_EXECUTION_BRIDGE = _vps_execution_bridge

    def _night_guarded_vps_execution_bridge(payload):
        p = dict(payload or {})
        if not _night_bool("NIGHT_ACCURACY_GUARD_ENABLED", True):
            return _NIGHT_ORIGINAL_VPS_EXECUTION_BRIDGE(p)

        p["night_accuracy_patch"] = {
            "enabled": True,
            "version": "V20260611",
            "checked_at_utc": utc_now_iso(),
        }

        guard = _night_guard_payload(p)
        p["night_accuracy_guard"] = guard

        if guard.get("action") == "PASS":
            _night_append_event({
                "event_type": "NIGHT_GUARD_PASS",
                "symbol": _night_symbol(p),
                "signal_key": p.get("signal_key"),
                "guard": guard,
            })
            return _NIGHT_ORIGINAL_VPS_EXECUTION_BRIDGE(p)

        if guard.get("action") == "REFRESH_RECOMPUTE" and _night_bool("STALE_PLAN_REFRESH_ENABLED", True):
            _night_append_event({
                "event_type": "NIGHT_OLD_PLAN_DISCARDED_REFRESH_RECOMPUTE",
                "symbol": _night_symbol(p),
                "signal_key": p.get("signal_key"),
                "reason": guard.get("reason"),
                "guard": guard,
            })
            return _night_refresh_recompute(p, guard.get("reason"), guard)

        _night_append_event({
            "event_type": "NIGHT_GUARD_NO_TRADE",
            "symbol": _night_symbol(p),
            "signal_key": p.get("signal_key"),
            "reason": guard.get("reason"),
            "guard": guard,
        })
        return {
            "ok": False,
            "decision": "NO_TRADE",
            "reason": guard.get("reason"),
            "guard": guard,
            "symbol": _night_symbol(p),
            "signal_key": p.get("signal_key"),
        }
# SMC_FINAL_PURGE_STAT_TECH_PRIMARY_20260628: removed legacy SMC bridge registration
    print("[night_accuracy_fix] guarded VPS execution bridge registered")
except Exception as _night_exc:
    print(f"[night_accuracy_fix] failed to register guarded bridge: {_night_exc}")
# === END_NIGHT_ACCURACY_FIX_V20260611 ===



# === HARD_PRE_ENTRY_GUARD_V20260613 ===
# Hard layer on top of NIGHT_ACCURACY_FIX:
# - Enforce final live RR/adverse threshold from env.
# - Does not rebase SL/TP.
# - Does not create order.
# - If final check fails, plan must refresh/recompute instead of executing as-is.
try:
    import os as _hpeg_os
    from decimal import Decimal as _hpeg_Decimal

    def _hpeg_bool(name, default=True):
        try:
            raw = _hpeg_os.getenv(name)
            if raw is None:
                return bool(default)
            return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")
        except Exception:
            return bool(default)

    def _hpeg_dec(x, default=None):
        try:
            if x is None or str(x).strip() == "":
                return default
            return _hpeg_Decimal(str(x))
        except Exception:
            return default

    def _hpeg_float_env(name, default):
        try:
            raw = _hpeg_os.getenv(name)
            if raw is None or str(raw).strip() == "":
                return _hpeg_Decimal(str(default))
            return _hpeg_Decimal(str(raw).strip())
        except Exception:
            return _hpeg_Decimal(str(default))

    def _hpeg_block(guard, reason):
        g = dict(guard or {})
        g.update({
            "ok": False,
            "action": str(_hpeg_os.getenv("PRE_ENTRY_GUARD_ACTION", "REFRESH_RECOMPUTE")),
            "reason": reason,
            "hard_pre_entry_guard": True,
        })
        try:
            if "_night_append_event" in globals():
                _night_append_event({
                    "event_type": "PRE_ENTRY_HARD_GUARD_BLOCK",
                    "reason": reason,
                    "symbol": g.get("symbol"),
                    "direction": g.get("direction"),
                    "pre_entry": g.get("pre_entry"),
                    "guard": g,
                })
        except Exception:
            pass
        return g

    if "_night_guard_payload" in globals() and not globals().get("_HARD_PRE_ENTRY_GUARD_PATCHED"):
        _HARD_PRE_ENTRY_ORIG_GUARD_PAYLOAD = _night_guard_payload

        def _night_guard_payload(p):
            g = _HARD_PRE_ENTRY_ORIG_GUARD_PAYLOAD(p)

            if not _hpeg_bool("PRE_ENTRY_HARD_GUARD_ENABLED", True):
                return g

            if not isinstance(g, dict):
                return g

            # Kalau base guard sudah reject/recompute, jangan override.
            if not bool(g.get("ok")):
                try:
                    g["hard_pre_entry_guard_checked"] = True
                    g["hard_pre_entry_guard_base_failed"] = True
                except Exception:
                    pass
                return g

            pre = g.get("pre_entry") if isinstance(g.get("pre_entry"), dict) else {}

            min_rr = _hpeg_float_env("MIN_ENTRY_RR_TO_TP1", "0.75")
            max_adv = _hpeg_float_env("MAX_ENTRY_ADVERSE_SLIPPAGE_PCT", "0.25")

            rr = _hpeg_dec(pre.get("rr_to_tp1"))
            adv = _hpeg_dec(pre.get("adverse_pct"), _hpeg_Decimal("0"))

            try:
                pre["hard_min_rr"] = str(min_rr)
                pre["hard_max_adverse_pct"] = str(max_adv)
                pre["hard_rr_to_tp1"] = str(rr) if rr is not None else None
                pre["hard_adverse_pct"] = str(adv) if adv is not None else None
                g["pre_entry"] = pre
                g["hard_pre_entry_guard_checked"] = True
            except Exception:
                pass

            # Missing RR = unsafe. Jangan execute market tanpa geometry final.
            if rr is None:
                return _hpeg_block(g, "HARD_PRE_ENTRY_MISSING_RR_NO_EXECUTE")

            if rr < min_rr:
                return _hpeg_block(g, "HARD_LIVE_RR_TOO_LOW_REFRESH_RECOMPUTE")

            if adv is not None and adv > max_adv:
                return _hpeg_block(g, "HARD_ENTRY_TOO_FAR_FROM_PLAN_REFRESH_RECOMPUTE")

            return g

        globals()["_HARD_PRE_ENTRY_GUARD_PATCHED"] = True
        print("[hard_pre_entry_guard] patched")

except Exception as _hpeg_e:
    try:
        print(f"[hard_pre_entry_guard] patch init failed: {_hpeg_e}")
    except Exception:
        pass



# === ML_GATE_SCORECARD_V20260613 ===
# ML Gate v0:
# - Learns from logs/ml_dataset_rows.jsonl + logs/forward_outcomes.jsonl
# - Predicts probability that TP1 hits before SL
# - Blocks execution if p_win_adj < ML_GATE_MIN_P_WIN
# - Hooked after freshness/pre-entry/hard guard by wrapping _night_guard_payload
try:
    import os as _mlg_os
    import json as _mlg_json
    import math as _mlg_math
    import time as _mlg_time
    from pathlib import Path as _mlg_Path
    from datetime import datetime as _mlg_datetime, timezone as _mlg_timezone, timedelta as _mlg_timedelta
    from collections import defaultdict as _mlg_defaultdict

    _MLG_WIB = _mlg_timedelta(hours=7)
    _MLG_CACHE = {
        "loaded_at": 0,
        "model": None,
    }

    def _mlg_bool(name, default=False):
        try:
            raw = _mlg_os.getenv(name)
            if raw is None:
                return bool(default)
            return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")
        except Exception:
            return bool(default)

    def _mlg_float_env(name, default):
        try:
            raw = _mlg_os.getenv(name)
            if raw is None or str(raw).strip() == "":
                return float(default)
            return float(str(raw).strip())
        except Exception:
            return float(default)

    def _mlg_int_env(name, default):
        try:
            raw = _mlg_os.getenv(name)
            if raw is None or str(raw).strip() == "":
                return int(default)
            return int(str(raw).strip())
        except Exception:
            return int(default)

    def _mlg_safe_float(x):
        try:
            if x is None or str(x).strip() == "":
                return None
            v = float(str(x))
            return v if _mlg_math.isfinite(v) else None
        except Exception:
            return None

    def _mlg_path(global_name, fallback):
        try:
            g = globals().get(global_name)
            if g is not None:
                return _mlg_Path(g)
        except Exception:
            pass
        return _mlg_Path(fallback)

    def _mlg_read_jsonl(path):
        out = []
        try:
            path = _mlg_Path(path)
            if not path.exists():
                return out
            with path.open(errors="ignore") as f:
                for line in f:
                    try:
                        out.append(_mlg_json.loads(line))
                    except Exception:
                        pass
        except Exception:
            return out
        return out

    def _mlg_signal_key(row):
        return str((row or {}).get("signal_key") or (row or {}).get("signal_id") or "")

    def _mlg_norm_symbol(x):
        try:
            if "v010_normalize_symbol" in globals():
                return v010_normalize_symbol(x)
        except Exception:
            pass
        return str(x or "").upper().replace("BINANCE:", "").replace(".P", "")

    def _mlg_direction(row):
        d = str((row or {}).get("direction") or (row or {}).get("dir") or "").strip().upper()
        if d in ("LONG", "BUY"):
            return "LONG"
        if d in ("SHORT", "SELL"):
            return "SHORT"
        return d or "UNK"

    def _mlg_parse_time_wib(s):
        if not s:
            return None
        ss = str(s).replace(" WIB", "").replace("T", " ").split("+")[0]
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d"):
            try:
                return _mlg_datetime.strptime(ss[:26], fmt).replace(tzinfo=_mlg_timezone(_MLG_WIB))
            except Exception:
                pass
        return None

    def _mlg_hour(row):
        for k in ("signal_time_wib", "confirmed_ts_wib", "run_ts_wib", "event_time_wib", "created_at_wib"):
            dt = _mlg_parse_time_wib((row or {}).get(k))
            if dt:
                return int(dt.hour)
        try:
            key = _mlg_signal_key(row)
            ms = int(key.split("|")[-1])
            dt = _mlg_datetime.fromtimestamp(ms / 1000.0, _mlg_timezone.utc).astimezone(_mlg_timezone(_MLG_WIB))
            return int(dt.hour)
        except Exception:
            return None

    def _mlg_session(h):
        if h is None:
            return "UNK"
        if 0 <= h < 7:
            return "ASIA_EARLY"
        if 7 <= h < 13:
            return "ASIA_DAY"
        if 13 <= h < 19:
            return "LONDON"
        return "NY_LATE"

    def _mlg_bin_num(v, bins):
        x = _mlg_safe_float(v)
        if x is None:
            return "NA"
        for name, lo, hi in bins:
            if lo <= x < hi:
                return name
        return bins[-1][0] if bins else "NA"

    def _mlg_pick(row, *keys):
        row = row or {}
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        plan = row.get("plan") if isinstance(row.get("plan"), dict) else {}
        meta = row.get("meta") if isinstance(row.get("meta"), dict) else {}
        htf = row.get("htf") if isinstance(row.get("htf"), dict) else {}
        liq = row.get("liq") if isinstance(row.get("liq"), dict) else {}
        for k in keys:
            for obj in (row, payload, plan, meta, htf, liq):
                try:
                    if k in obj and obj.get(k) not in (None, ""):
                        return obj.get(k)
                except Exception:
                    pass
        return None

    def _mlg_features(row):
        h = _mlg_hour(row)
        symbol = _mlg_norm_symbol(_mlg_pick(row, "symbol", "pair"))
        direction = _mlg_direction(row)

        score = _mlg_pick(row, "score", "score_total", "priority_score", "smc_score")
        rr = _mlg_pick(row, "rr", "rr_tp1", "rr_to_tp1", "rrTp2", "rr_min")
        entry_dist = _mlg_pick(row, "entryDistPct", "entry_dist_pct", "entry_dist_from_price_pct", "distToZonePct")
        htf_loc = _mlg_pick(row, "htfLoc", "htf_location", "location")
        htf_bias = _mlg_pick(row, "htfBias", "htf_bias", "bias")
        liq_ctx = _mlg_pick(row, "liqCtx", "liq_ctx", "ctx")
        mode = _mlg_pick(row, "mode", "state", "status")

        return {
            "symbol": symbol or "UNK",
            "direction": direction or "UNK",
            "hour": str(h) if h is not None else "UNK",
            "session": _mlg_session(h),
            "score_bin": _mlg_bin_num(score, [
                ("score<60", -999, 60),
                ("60-67", 60, 68),
                ("68-69", 68, 70),
                ("70-74", 70, 75),
                ("75-79", 75, 80),
                ("80+", 80, 999),
            ]),
            "rr_bin": _mlg_bin_num(rr, [
                ("rr<0.7", -999, 0.7),
                ("0.7-0.9", 0.7, 0.9),
                ("0.9-1.2", 0.9, 1.2),
                ("1.2-1.8", 1.2, 1.8),
                ("1.8+", 1.8, 999),
            ]),
            "entry_dist_bin": _mlg_bin_num(entry_dist, [
                ("dist<0.15", -999, 0.15),
                ("0.15-0.35", 0.15, 0.35),
                ("0.35-0.60", 0.35, 0.60),
                ("0.60-1.00", 0.60, 1.00),
                ("1.00+", 1.00, 999),
            ]),
            "htf_loc": str(htf_loc or "UNK"),
            "htf_bias": str(htf_bias or "UNK"),
            "liq_ctx": str(liq_ctx or "UNK"),
            "mode": str(mode or "UNK"),
        }

    def _mlg_latest_outcomes():
        path = _mlg_path("FORWARD_OUTCOMES_LOG", "logs/forward_outcomes.jsonl")
        latest = {}
        for r in _mlg_read_jsonl(path):
            k = _mlg_signal_key(r)
            if k:
                latest[k] = r
        return latest

    def _mlg_label(row, out):
        try:
            if out:
                lw = out.get("label_win")
                if lw in (0, 1):
                    return int(lw)
                target = str(out.get("label_target") or "").upper()
                if target in ("TP1", "TP2", "TP3"):
                    return 1
                if target == "SL":
                    return 0
            lw = (row or {}).get("label_win")
            if lw in (0, 1):
                return int(lw)
        except Exception:
            pass
        return None

    def _mlg_include_train(row, out):
        try:
            if (row or {}).get("include_ml") is False:
                return False
            if out and out.get("include_ml_label") is False:
                return False
            if _mlg_bool("ML_GATE_TRAIN_ACCEPT_ONLY", False):
                dec = str((row or {}).get("execution_decision") or "").upper()
                if dec != "ACCEPT":
                    return False
            return _mlg_label(row, out) in (0, 1)
        except Exception:
            return False

    def _mlg_build_model():
        dataset_path = _mlg_path("ML_DATASET_ROWS_LOG", "logs/ml_dataset_rows.jsonl")
        rows_raw = _mlg_read_jsonl(dataset_path)
        outcomes = _mlg_latest_outcomes()

        latest_rows = {}
        for r in rows_raw:
            k = _mlg_signal_key(r)
            if k:
                latest_rows[k] = r

        rows = list(latest_rows.values())

        global_w = 0
        global_n = 0
        stats = _mlg_defaultdict(lambda: [0, 0])

        for row in rows:
            out = outcomes.get(_mlg_signal_key(row))
            if not _mlg_include_train(row, out):
                continue
            y = _mlg_label(row, out)
            if y not in (0, 1):
                continue

            global_n += 1
            global_w += int(y)

            fm = _mlg_features(row)
            for k, v in fm.items():
                kk = f"{k}={v}"
                stats[kk][0] += int(y)
                stats[kk][1] += 1

        return {
            "built_at_utc": globals().get("utc_now_iso", lambda: _mlg_datetime.now(_mlg_timezone.utc).isoformat())(),
            "global_w": global_w,
            "global_n": global_n,
            "stats": dict(stats),
        }

    def _mlg_model():
        ttl = _mlg_int_env("ML_GATE_CACHE_TTL_SEC", 300)
        now = _mlg_time.time()
        if _MLG_CACHE.get("model") and (now - float(_MLG_CACHE.get("loaded_at") or 0)) < ttl:
            return _MLG_CACHE["model"]
        model = _mlg_build_model()
        _MLG_CACHE["model"] = model
        _MLG_CACHE["loaded_at"] = now
        return model

    def _mlg_predict(row):
        model = _mlg_model()
        global_n = int(model.get("global_n") or 0)
        global_w = int(model.get("global_w") or 0)
        gp = (global_w / global_n) if global_n > 0 else 0.50

        min_train = _mlg_int_env("ML_GATE_MIN_TRAIN_ROWS", 120)
        min_feature_n = _mlg_int_env("ML_GATE_MIN_FEATURE_N", 5)

        if global_n < min_train:
            return {
                "ok": bool(_mlg_bool("ML_GATE_FAIL_OPEN", True)),
                "decision": "ML_PASS_NOT_ENOUGH_DATA" if _mlg_bool("ML_GATE_FAIL_OPEN", True) else "ML_BLOCK_NOT_ENOUGH_DATA",
                "reason": "ml_gate_not_enough_train_rows",
                "p_win": round(gp, 4),
                "p_win_adj": round(gp, 4),
                "confidence": 0.0,
                "train_rows": global_n,
                "global_wr": round(gp, 4),
                "reasons": [],
            }

        fm = _mlg_features(row)
        stats = model.get("stats") or {}

        weights = {
            "symbol": 0.26,
            "direction": 0.08,
            "hour": 0.08,
            "session": 0.08,
            "score_bin": 0.13,
            "rr_bin": 0.11,
            "entry_dist_bin": 0.11,
            "htf_loc": 0.06,
            "htf_bias": 0.04,
            "liq_ctx": 0.03,
            "mode": 0.02,
        }

        weighted_sum = gp * 0.34
        weight_total = 0.34
        evidence_n = 0
        feature_hits = 0
        reasons = []

        for k, v in fm.items():
            kk = f"{k}={v}"
            w, n = stats.get(kk, [0, 0])
            w, n = int(w or 0), int(n or 0)
            if n < min_feature_n:
                continue

            alpha = 12
            p_smooth = (w + alpha * gp) / (n + alpha)
            conf = min(1.0, n / 45.0)
            weight = float(weights.get(k, 0.03)) * conf

            weighted_sum += p_smooth * weight
            weight_total += weight
            evidence_n += n
            feature_hits += 1

            reasons.append({
                "feature": kk,
                "win": w,
                "n": n,
                "wr": round(w / n, 4) if n else None,
                "p_smooth": round(p_smooth, 4),
                "weight": round(weight, 4),
            })

        p_win = weighted_sum / weight_total if weight_total else gp
        confidence = min(1.0, evidence_n / 180.0)
        p_adj = p_win - (0.06 * (1.0 - confidence))

        min_p = _mlg_float_env("ML_GATE_MIN_P_WIN", 0.74)
        min_conf = _mlg_float_env("ML_GATE_MIN_CONFIDENCE", 0.20)
        block_low_conf = _mlg_bool("ML_GATE_BLOCK_LOW_CONFIDENCE", False)

        if confidence < min_conf and block_low_conf:
            ok = False
            decision = "ML_BLOCK_LOW_CONFIDENCE"
            reason = "ml_gate_low_confidence"
        elif p_adj < min_p:
            ok = False
            decision = "ML_BLOCK_LOW_PWIN"
            reason = "ml_gate_pwin_below_threshold"
        else:
            ok = True
            decision = "ML_PASS"
            reason = "ml_gate_pass"

        return {
            "ok": ok,
            "decision": decision,
            "reason": reason,
            "p_win": round(p_win, 4),
            "p_win_adj": round(p_adj, 4),
            "confidence": round(confidence, 4),
            "min_p_win": min_p,
            "min_confidence": min_conf,
            "train_rows": global_n,
            "global_win": global_w,
            "global_loss": global_n - global_w,
            "global_wr": round(gp, 4),
            "feature_hits": feature_hits,
            "features": fm,
            "reasons": sorted(reasons, key=lambda x: x.get("weight", 0), reverse=True)[:8],
            "model_built_at_utc": model.get("built_at_utc"),
        }

    def _mlg_append_prediction(payload, pred):
        try:
            rec = {
                "event_type": "ML_GATE_PREDICTION_V0",
                "created_at_utc": globals().get("utc_now_iso", lambda: _mlg_datetime.now(_mlg_timezone.utc).isoformat())(),
                "signal_key": _mlg_signal_key(payload),
                "symbol": _mlg_norm_symbol(_mlg_pick(payload, "symbol", "pair")),
                "direction": _mlg_direction(payload),
                "prediction": pred,
            }
            path = _mlg_path("ML_PREDICTIONS_LOG", "logs/ml_predictions.jsonl")
            if "append_jsonl" in globals():
                append_jsonl(path, rec)
            else:
                with _mlg_Path(path).open("a") as f:
                    f.write(_mlg_json.dumps(rec, default=str) + "\n")
        except Exception:
            pass

    def _mlg_block_guard(guard, pred):
        g = dict(guard or {})
        g["ok"] = False
        g["action"] = str(_mlg_os.getenv("ML_GATE_ACTION", "NO_TRADE"))
        g["reason"] = str(pred.get("decision") or "ML_GATE_BLOCK")
        g["ml_gate"] = pred
        try:
            if "_night_append_event" in globals():
                _night_append_event({
                    "event_type": "ML_GATE_BLOCK",
                    "reason": g.get("reason"),
                    "symbol": g.get("symbol"),
                    "direction": g.get("direction"),
                    "ml_gate": pred,
                })
        except Exception:
            pass
        return g

    if "_night_guard_payload" in globals() and not globals().get("_ML_GATE_PATCHED"):
        _ML_GATE_ORIG_NIGHT_GUARD_PAYLOAD = _night_guard_payload

        def _night_guard_payload(p):
            g = _ML_GATE_ORIG_NIGHT_GUARD_PAYLOAD(p)

            if not _mlg_bool("ML_GATE_ENABLED", False):
                return g

            if not isinstance(g, dict):
                return g

            # Kalau freshness/hard-pre-entry guard sudah fail, jangan override.
            if not bool(g.get("ok")):
                try:
                    g["ml_gate_checked"] = False
                    g["ml_gate_skip_reason"] = "base_guard_failed"
                except Exception:
                    pass
                return g

            pred = _mlg_predict(p or {})
            try:
                g["ml_gate"] = pred
                g["ml_gate_checked"] = True
            except Exception:
                pass

            _mlg_append_prediction(p or {}, pred)

            mode = str(_mlg_os.getenv("ML_GATE_MODE", "LIVE_BLOCK")).strip().upper()
            if mode == "SHADOW":
                return g

            if not bool(pred.get("ok")):
                return _mlg_block_guard(g, pred)

            return g

        globals()["_ML_GATE_PATCHED"] = True
        print("[ml_gate] scorecard gate patched")
except Exception as _mlg_e:
    try:
        print("[ml_gate] patch failed", str(_mlg_e))
    except Exception:
        pass



# === ML_GATE_SKLEARN_V1_PATCH_20260613 ===
# Prefer sklearn predict_proba model for ML Gate.
# Falls back to scorecard gate if model/dependency is unavailable.
try:
    import os as _sk_os
    import math as _sk_math
    from pathlib import Path as _sk_Path
    from datetime import datetime as _sk_datetime, timezone as _sk_timezone

    _SK_MODEL_CACHE = {
        "loaded_at": 0,
        "obj": None,
        "path": None,
    }

    def _sk_safe_float(x, default=-1.0):
        try:
            if x is None or str(x).strip() == "":
                return default
            v = float(str(x))
            return v if _sk_math.isfinite(v) else default
        except Exception:
            return default

    def _sk_features(row):
        # Reuse helper functions from scorecard patch when available.
        h = _mlg_hour(row) if "_mlg_hour" in globals() else -1

        score = _mlg_pick(row, "score", "score_total", "priority_score", "smc_score") if "_mlg_pick" in globals() else (row or {}).get("score")
        rr = _mlg_pick(row, "rr", "rr_tp1", "rr_to_tp1", "rrTp2", "rr_min") if "_mlg_pick" in globals() else (row or {}).get("rr")
        entry_dist = _mlg_pick(row, "entryDistPct", "entry_dist_pct", "entry_dist_from_price_pct", "distToZonePct") if "_mlg_pick" in globals() else (row or {}).get("entry_dist_pct")
        htf_loc = _mlg_pick(row, "htfLoc", "htf_location", "location") if "_mlg_pick" in globals() else None
        htf_bias = _mlg_pick(row, "htfBias", "htf_bias", "bias") if "_mlg_pick" in globals() else None
        liq_ctx = _mlg_pick(row, "liqCtx", "liq_ctx", "ctx") if "_mlg_pick" in globals() else None
        mode = _mlg_pick(row, "mode", "state", "status") if "_mlg_pick" in globals() else None

        symbol = _mlg_norm_symbol(_mlg_pick(row, "symbol", "pair")) if "_mlg_norm_symbol" in globals() and "_mlg_pick" in globals() else str((row or {}).get("symbol") or "").upper()
        direction = _mlg_direction(row) if "_mlg_direction" in globals() else str((row or {}).get("direction") or "").upper()
        session = _mlg_session(h) if "_mlg_session" in globals() else "UNK"

        return {
            "symbol": symbol or "UNK",
            "direction": direction or "UNK",
            "hour": float(h if h is not None else -1),
            "session": session,
            "score": _sk_safe_float(score, -1.0),
            "rr": _sk_safe_float(rr, -1.0),
            "entry_dist": _sk_safe_float(entry_dist, -1.0),
            "htf_loc": str(htf_loc or "UNK"),
            "htf_bias": str(htf_bias or "UNK"),
            "liq_ctx": str(liq_ctx or "UNK"),
            "mode": str(mode or "UNK"),
        }

    def _sk_model_obj():
        import time as _sk_time
        import joblib as _sk_joblib

        ttl = int(_sk_os.getenv("ML_GATE_SKLEARN_CACHE_TTL_SEC", "300"))
        path = _sk_Path(_sk_os.getenv("ML_GATE_MODEL_PATH", "artifacts/ml_gate_sklearn_v1.joblib"))

        now = _sk_time.time()
        if (
            _SK_MODEL_CACHE.get("obj") is not None
            and _SK_MODEL_CACHE.get("path") == str(path)
            and now - float(_SK_MODEL_CACHE.get("loaded_at") or 0) < ttl
        ):
            return _SK_MODEL_CACHE["obj"]

        obj = _sk_joblib.load(path)
        _SK_MODEL_CACHE["obj"] = obj
        _SK_MODEL_CACHE["path"] = str(path)
        _SK_MODEL_CACHE["loaded_at"] = now
        return obj

    def _sk_predict(row):
        obj = _sk_model_obj()
        model = obj.get("model")
        version = obj.get("model_version", "sklearn_unknown")
        metrics = obj.get("metrics", {})

        x = _sk_features(row)
        probs = model.predict_proba([x])[0]
        classes = list(model.named_steps["clf"].classes_)
        pos_idx = classes.index(1)
        p_win = float(probs[pos_idx])

        min_p = float(_sk_os.getenv("ML_GATE_MIN_P_WIN", str(obj.get("threshold", 0.74))))
        confidence = round(abs(p_win - 0.50) * 2.0, 4)

        if p_win >= min_p:
            ok = True
            decision = "ML_PASS"
            reason = "sklearn_gate_pass"
        else:
            ok = False
            decision = "ML_BLOCK_LOW_PWIN"
            reason = "sklearn_gate_pwin_below_threshold"

        return {
            "ok": ok,
            "decision": decision,
            "reason": reason,
            "model_version": version,
            "p_win": round(p_win, 4),
            "p_win_adj": round(p_win, 4),
            "confidence": confidence,
            "min_p_win": min_p,
            "features": x,
            "metrics": metrics,
        }

    if "_mlg_predict" in globals() and not globals().get("_ML_GATE_SKLEARN_PATCHED"):
        _MLG_SCORECARD_PREDICT_ORIG = _mlg_predict

        def _mlg_predict(row):
            model_mode = str(_sk_os.getenv("ML_GATE_MODEL", "SCORECARD_V0")).upper().strip()
            if model_mode == "SKLEARN_V1":
                try:
                    return _sk_predict(row or {})
                except Exception as e:
                    # fail open to scorecard, not to order directly
                    try:
                        print("[ml_gate_sklearn] fallback_to_scorecard", str(e))
                    except Exception:
                        pass
                    return _MLG_SCORECARD_PREDICT_ORIG(row or {})
            return _MLG_SCORECARD_PREDICT_ORIG(row or {})

        globals()["_ML_GATE_SKLEARN_PATCHED"] = True
        print("[ml_gate_sklearn] sklearn v1 gate patched")
except Exception as _sk_e:
    try:
        print("[ml_gate_sklearn] patch failed", str(_sk_e))
    except Exception:
        pass



# === ML_GATE_SKLEARN_V2_LIVE_FEATURES_20260613 ===
try:
    import os as _v2_os
    import re as _v2_re
    import math as _v2_math
    from datetime import datetime as _v2_datetime, timezone as _v2_timezone, timedelta as _v2_timedelta

    _V2_WIB = _v2_timezone(_v2_timedelta(hours=7))

    def _v2_fnum(x, default=-1.0):
        try:
            if x is None:
                return default
            if isinstance(x, (int, float)):
                v = float(x)
                return v if _v2_math.isfinite(v) else default
            ss = str(x).strip()
            if not ss:
                return default
            nums = _v2_re.findall(r'-?\d+(?:\.\d+)?', ss)
            if len(nums) >= 2 and "-" in ss:
                vals = [float(a) for a in nums[:2]]
                return sum(vals) / len(vals)
            v = float(ss.replace(",", ""))
            return v if _v2_math.isfinite(v) else default
        except Exception:
            return default

    def _v2_range_lo_hi(x):
        try:
            if x is None:
                return None, None
            if isinstance(x, (int, float)):
                v = float(x)
                return v, v
            ss = str(x).strip()
            nums = _v2_re.findall(r'-?\d+(?:\.\d+)?', ss)
            if len(nums) >= 2:
                return float(nums[0]), float(nums[1])
            if len(nums) == 1:
                v = float(nums[0])
                return v, v
        except Exception:
            pass
        return None, None

    def _v2_norm_symbol(x):
        return str(x or "").upper().replace("BINANCE:", "").replace(".P", "")

    def _v2_norm_dir(x):
        d = str(x or "").upper()
        if d in ("LONG", "BUY"):
            return "LONG"
        if d in ("SHORT", "SELL"):
            return "SHORT"
        return d or "UNK"

    def _v2_cat(x):
        if x in (None, "", -1, -1.0):
            return "UNK"
        return str(x)

    def _v2_parse_wib(s):
        if not s:
            return None
        ss = str(s).replace(" WIB", "").replace("T", " ").split("+")[0]
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
            try:
                return _v2_datetime.strptime(ss[:26], fmt).replace(tzinfo=_V2_WIB)
            except Exception:
                pass
        return None

    def _v2_patch_phase(signal_time):
        dt = _v2_parse_wib(signal_time)
        if not dt:
            return "UNKNOWN"
        phases = [
            ("PRE_LATENCY_PATCH", "2026-06-10 22:49:27"),
            ("PRE_NIGHT_GUARD", "2026-06-11 00:36:16"),
            ("PRE_HARD_GUARD", "2026-06-13 16:42:59"),
            ("PRE_SKLEARN_GATE", "2026-06-13 21:28:04"),
        ]
        for name, cutoff in phases:
            c = _v2_parse_wib(cutoff)
            if c and dt < c:
                return name
        return "AFTER_SKLEARN_GATE"

    def _v2_age_bars(signal_time, event_time, tf_min=5.0):
        a = _v2_parse_wib(signal_time)
        b = _v2_parse_wib(event_time)
        if not a or not b:
            return -1.0
        return round(max(0.0, (a - b).total_seconds() / 60.0) / tf_min, 3)

    def _v2_pick(row, *keys):
        if row is None or not isinstance(row, dict):
            return None
        objs = [row]
        for nested in ("payload", "plan", "locked_plan", "trade_plan", "meta", "htf", "liq"):
            if isinstance(row.get(nested), dict):
                objs.append(row[nested])
        if isinstance(row.get("plan"), dict):
            objs.append(row["plan"])

        for k in keys:
            for obj in objs:
                if isinstance(obj, dict) and obj.get(k) not in (None, ""):
                    return obj.get(k)
        return None

    def _v2_payload(row):
        if isinstance(row, dict) and isinstance(row.get("payload"), dict):
            return row["payload"]
        if isinstance(row, dict):
            return row
        return {}

    def _v2_smc_features(row):
        p = _v2_payload(row)

        htf = p.get("htf_summary") if isinstance(p.get("htf_summary"), dict) else {}
        liq = p.get("liquidity") if isinstance(p.get("liquidity"), dict) else {}
        smc = p.get("smc_context") if isinstance(p.get("smc_context"), dict) else {}

        sweep = smc.get("sweep") if isinstance(smc.get("sweep"), dict) else {}
        reclaim = smc.get("reclaim") if isinstance(smc.get("reclaim"), dict) else {}
        fvg = smc.get("fvg") if isinstance(smc.get("fvg"), dict) else {}
        poi = smc.get("poi") if isinstance(smc.get("poi"), dict) else {}
        ob = smc.get("ob") if isinstance(smc.get("ob"), dict) else {}

        direction = _v2_norm_dir(_v2_pick(row, "direction", "dir"))
        symbol = _v2_norm_symbol(_v2_pick(row, "symbol", "pair"))

        entry = _v2_fnum(_v2_pick(row, "entry_mid", "entry"), None)
        entry_lo = _v2_fnum(_v2_pick(row, "entry_lo"), None)
        entry_hi = _v2_fnum(_v2_pick(row, "entry_hi"), None)

        if entry is None:
            lo, hi = _v2_range_lo_hi(_v2_pick(row, "entry_zone", "entry"))
            if lo is not None and hi is not None:
                entry = (lo + hi) / 2.0
                entry_lo, entry_hi = lo, hi

        sl = _v2_fnum(_v2_pick(row, "sl", "invalid"), None)
        tp1 = _v2_fnum(_v2_pick(row, "tp1", "raw_tp1"), None)
        tp2 = _v2_fnum(_v2_pick(row, "tp2", "raw_tp2"), None)
        tp3 = _v2_fnum(_v2_pick(row, "tp3", "raw_tp3"), None)

        x = {}

        x["symbol_norm"] = symbol or "UNK"
        x["direction_norm"] = direction or "UNK"
        x["priority"] = _v2_cat(_v2_pick(row, "priority"))
        x["mode"] = _v2_cat(_v2_pick(row, "mode"))
        x["source_mode"] = _v2_cat(_v2_pick(row, "source_mode"))
        x["execution_decision"] = _v2_cat(_v2_pick(row, "execution_decision"))
        x["reject_gate"] = _v2_cat(_v2_pick(row, "reject_gate"))

        x["htf_dir"] = _v2_cat(htf.get("htf_dir") or htf.get("dir"))
        x["htf_bias"] = _v2_cat(htf.get("bias"))
        x["htf_location"] = _v2_cat(htf.get("location"))
        x["htf_dol"] = _v2_cat(htf.get("dol"))
        x["liq_ctx"] = _v2_cat(liq.get("ctx"))

        x["sweep_tag"] = _v2_cat(_v2_pick(row, "sweep_tag") or sweep.get("tag"))
        x["reclaim_mode"] = _v2_cat(_v2_pick(row, "reclaim_mode") or reclaim.get("mode"))
        x["fvg_type"] = _v2_cat(fvg.get("type") or poi.get("type"))
        x["ob_type"] = _v2_cat(ob.get("type"))

        signal_time = _v2_pick(row, "signal_time_wib", "confirmed_ts_wib")
        x["patch_phase"] = _v2_patch_phase(signal_time)

        x["score"] = _v2_fnum(_v2_pick(row, "score"), -1.0)

        if entry and sl and tp1 and abs(sl - entry) > 0:
            risk = abs(sl - entry)
            x["rr_to_tp1"] = round(abs(tp1 - entry) / risk, 6)
            x["sl_dist_pct"] = round(risk / entry * 100.0, 6)
            x["tp1_dist_pct"] = round(abs(tp1 - entry) / entry * 100.0, 6)
            x["rr_to_tp2"] = round(abs(tp2 - entry) / risk, 6) if tp2 else -1.0
            x["tp2_dist_pct"] = round(abs(tp2 - entry) / entry * 100.0, 6) if tp2 else -1.0
            x["rr_to_tp3"] = round(abs(tp3 - entry) / risk, 6) if tp3 else -1.0
            x["tp3_dist_pct"] = round(abs(tp3 - entry) / entry * 100.0, 6) if tp3 else -1.0
        else:
            x["rr_to_tp1"] = _v2_fnum(_v2_pick(row, "rr_to_tp1", "rr", "rr_tp1"), -1.0)
            x["rr_to_tp2"] = _v2_fnum(_v2_pick(row, "rr_to_tp2", "rrTp2"), -1.0)
            x["rr_to_tp3"] = _v2_fnum(_v2_pick(row, "rr_to_tp3"), -1.0)
            x["sl_dist_pct"] = _v2_fnum(_v2_pick(row, "sl_dist_pct"), -1.0)
            x["tp1_dist_pct"] = _v2_fnum(_v2_pick(row, "tp1_dist_pct"), -1.0)
            x["tp2_dist_pct"] = _v2_fnum(_v2_pick(row, "tp2_dist_pct"), -1.0)
            x["tp3_dist_pct"] = _v2_fnum(_v2_pick(row, "tp3_dist_pct"), -1.0)

        x["entry_zone_width_pct"] = (
            round(abs(entry_hi - entry_lo) / entry * 100.0, 6)
            if entry and entry_lo is not None and entry_hi is not None
            else _v2_fnum(_v2_pick(row, "entry_zone_width_pct"), -1.0)
        )

        x["liq_dist_to_zone_pct"] = _v2_fnum(liq.get("dist_to_zone_pct") or _v2_pick(row, "liq_dist_to_zone_pct"), -1.0)
        x["sweep_age_bars_5m"] = _v2_age_bars(signal_time, _v2_pick(row, "sweep_ts_wib") or sweep.get("ts_wib"))
        x["reclaim_age_bars_5m"] = _v2_age_bars(signal_time, _v2_pick(row, "reclaim_ts_wib") or reclaim.get("ts_wib"))
        x["fvg_age_bars_5m"] = _v2_age_bars(signal_time, _v2_pick(row, "fvg_ts_wib"))

        fvg_lo = _v2_fnum(_v2_pick(row, "fvg_lo") or fvg.get("bot") or poi.get("lo"), None)
        fvg_hi = _v2_fnum(_v2_pick(row, "fvg_hi") or fvg.get("top") or poi.get("hi"), None)

        x["fvg_size_pct"] = (
            round(abs(fvg_hi - fvg_lo) / entry * 100.0, 6)
            if entry and fvg_lo is not None and fvg_hi is not None
            else _v2_fnum(_v2_pick(row, "fvg_size_pct"), -1.0)
        )

        ob_lo = _v2_fnum(ob.get("lo"), None)
        ob_hi = _v2_fnum(ob.get("hi"), None)

        x["ob_size_pct"] = (
            round(abs(ob_hi - ob_lo) / entry * 100.0, 6)
            if entry and ob_lo is not None and ob_hi is not None
            else _v2_fnum(_v2_pick(row, "ob_size_pct"), -1.0)
        )

        rr = x.get("rr_to_tp1", -1.0)
        slp = x.get("sl_dist_pct", -1.0)
        fvgp = x.get("fvg_size_pct", -1.0)

        x["rr_bucket"] = "rr<0.7" if rr < 0.7 else "0.7-0.9" if rr < 0.9 else "0.9-1.2" if rr < 1.2 else "1.2+"
        x["sl_bucket"] = "sl<0.15" if slp < 0.15 else "0.15-0.35" if slp < 0.35 else "0.35-0.70" if slp < 0.70 else "0.70+"
        x["fvg_bucket"] = "fvgNA" if fvgp < 0 else "fvg<0.10" if fvgp < 0.10 else "0.10-0.30" if fvgp < 0.30 else "0.30+"

        return x

    if "_sk_predict" in globals():
        def _sk_features(row):
            return _v2_smc_features(row or {})

    if "_mlg_predict" in globals() and "_sk_predict" in globals() and not globals().get("_ML_GATE_SKLEARN_V2_ALIAS_PATCHED"):
        _MLG_PREDICT_BEFORE_V2_ALIAS = _mlg_predict

        def _mlg_predict(row):
            mode = str(_v2_os.getenv("ML_GATE_MODEL", "SCORECARD_V0")).upper().strip()
            if mode in ("SKLEARN", "SKLEARN_V1", "SKLEARN_V2"):
                try:
                    return _sk_predict(row or {})
                except Exception as e:
                    try:
                        print("[ml_gate_sklearn_v2] fallback", str(e))
                    except Exception:
                        pass
                    return _MLG_PREDICT_BEFORE_V2_ALIAS(row or {})
            return _MLG_PREDICT_BEFORE_V2_ALIAS(row or {})

        globals()["_ML_GATE_SKLEARN_V2_ALIAS_PATCHED"] = True
        print("[ml_gate_sklearn_v2] live features v2 patched")
except Exception as _v2_e:
    try:
        print("[ml_gate_sklearn_v2] patch failed", str(_v2_e))
    except Exception:
        pass


# === FIX_BASE_EVENT_ALIAS_20260623 ===
# Some patched live/bridge code calls base_event(), while older helper is named v010_base_event().
# Keep this alias global so runtime bridge logging does not crash before order decision.
try:
    base_event
except NameError:
    try:
        base_event = v010_base_event
    except NameError:
        def base_event(symbol: str = "", action: str = "", reason: str = "") -> dict:
            from datetime import datetime, timezone
            return {
                "created_at_utc": datetime.now(timezone.utc).isoformat(),
                "symbol": str(symbol or ""),
                "event_type": str(action or "BASE_EVENT"),
                "reason": str(reason or ""),
            }


# === FIX_BASE_EVENT_COMPAT_DICT_CALLABLE_20260623 ===
# Compatibility shim:
# - some code calls base_event(symbol, action, reason)
# - some code mutates base_event["key"] = value
# This object supports both so bridge logging cannot kill execution.
class _BaseEventCompat(dict):
    def __call__(self, symbol: str = "", action: str = "", reason: str = "") -> dict:
        try:
            return v010_base_event(symbol, action, reason)
        except Exception:
            from datetime import datetime, timezone
            return {
                "created_at_utc": datetime.now(timezone.utc).isoformat(),
                "symbol": str(symbol or ""),
                "event_type": str(action or "BASE_EVENT"),
                "reason": str(reason or ""),
            }

    def __setitem__(self, key, value):
        try:
            if "created_at_utc" not in self:
                from datetime import datetime, timezone
                dict.__setitem__(self, "created_at_utc", datetime.now(timezone.utc).isoformat())
        except Exception:
            pass
        dict.__setitem__(self, key, value)

try:
    base_event = _BaseEventCompat()
except Exception:
    pass




# === POSITION_MANAGER_V026 ===
# Position-management endpoints live in app/position_manager.py so they can be
# maintained without changing the VPS SMC entry strategy.
app.include_router(create_position_manager_router(PositionManagerDeps(
    auth_ok=v010_auth_ok,
    normalize_symbol=v010_normalize_symbol,
    utc_now_iso=utc_now_iso,
    binance_env=binance_env,
    execution_mode=execution_mode,
    safety_summary=v014_safety_summary,
    reconcile_state=v014_reconcile_state,
    append_event=lambda event: append_jsonl(EXECUTION_EVENTS_LOG, event),
    cancel_stale_algo_orders=v013_cancel_stale_algo_orders,
    tp_lifecycle_tick=v017_tp_lifecycle_check_core,
    live_guarded_action=pm_live_guarded_reduce_only_action,
    send_report=pm_send_report,
    list_open_positions=pm_live_open_positions,
    find_bot_plan=pm_find_bot_plan,
    manager_score=pm_manager_score,
    record_decision=pm_record_manager_decision,
    action_budget_guard=pm_action_budget_guard,
    action_budget_record=pm_action_budget_record,
)))
