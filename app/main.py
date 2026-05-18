import json
import os
import time
import hmac
import hashlib
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from pathlib import Path
from decimal import Decimal, ROUND_DOWN
from threading import Lock
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, ConfigDict


APP_VERSION = "v0.12-testnet-protective-order-placement-skeleton"

LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
STATE_DIR = Path(os.getenv("STATE_DIR", "state"))

SIGNALS_LOG = LOG_DIR / "signals.jsonl"
DECISIONS_LOG = LOG_DIR / "decisions.jsonl"
PAPER_STATE_FILE = STATE_DIR / "paper_state.json"
PAPER_EVENTS_LOG = LOG_DIR / "paper_events.jsonl"
EXECUTION_PLANS_LOG = LOG_DIR / "execution_plans.jsonl"
EXECUTION_EVENTS_LOG = LOG_DIR / "execution_events.jsonl"

WIB = ZoneInfo("Asia/Jakarta")
LOCK = Lock()

app = FastAPI(title="AI Trading VPS Bot", version=APP_VERSION)


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
    outcome: Optional[str] = "CLOSED_MANUAL"
    close_reason: Optional[str] = "MANUAL_CLOSE"
    close_price: Optional[float] = None
    notes: Optional[str] = None


class PaperCloseAllPayload(BaseModel):
    model_config = ConfigDict(extra="allow")

    outcome: Optional[str] = "CLOSED_MANUAL"
    close_reason: Optional[str] = "MANUAL_CLOSE_ALL"
    close_price: Optional[float] = None
    notes: Optional[str] = None

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def wib_now_iso() -> str:
    return datetime.now(WIB).isoformat()


def ensure_dirs() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)


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
    if mode not in ("RECEIVED_ONLY", "RECEIVER_ONLY", "PAPER"):
        return "RECEIVED_ONLY"
    if mode == "RECEIVER_ONLY":
        return "RECEIVED_ONLY"
    return mode


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
    return str(p.get("direction") or p.get("dir") or "").strip()


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
        if pair not in allowlist:
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

        positions = state.get("open_paper_positions") or []
        positions.append({
            "signal_key": signal_key,
            "pair": pair,
            "direction": direction_of(p),
            "status": "OPEN",
            "accepted_at_utc": now_iso,
            "entry_mid": p.get("entry_mid"),
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
    plan["notional_usdt"] = qty_res.get("notional")
    return True, "quantity_sizing_valid", qty_res


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


def build_execution_plan(p: Dict[str, Any]) -> Dict[str, Any]:
    pair = pair_of(p)
    symbol = pair_to_binance_symbol(pair)
    direction = direction_of(p)

    entry_mid = p.get("entry_mid")
    sl = p.get("sl") or p.get("invalid")
    tp1 = p.get("tp1")
    tp2 = p.get("tp2")
    tp3 = p.get("tp3")

    side = "BUY" if direction == "Long" else "SELL"
    exit_side = "SELL" if direction == "Long" else "BUY"

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
        "leverage": env_int("DEFAULT_LEVERAGE", 2),
        "margin_type": "ISOLATED",
        "quantity": None,
        "notional_usdt_cap": env_int("TESTNET_MAX_NOTIONAL_USDT", 50),
        "risk_usdt": env_int("TESTNET_RISK_USDT_PER_TRADE", 5),
        "payload": p,
    }


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

    if env_bool("ORDER_REQUIRE_SL", True) and not plan.get("sl"):
        return False, "missing_sl"

    if env_bool("ORDER_REQUIRE_TP", True) and not (plan.get("tp1") or plan.get("tp2") or plan.get("tp3")):
        return False, "missing_tp"

    allowed = testnet_allowed_symbols()
    if allowed and plan.get("symbol") not in allowed:
        return False, f"symbol_not_allowed_for_testnet:{plan.get('symbol')}"

    if env_bool("ISOLATED_MARGIN_ONLY", True) and plan.get("margin_type") != "ISOLATED":
        return False, "isolated_margin_required"

    qty_ok, qty_reason, _qty_res = enrich_plan_with_quantity(plan)
    if not qty_ok:
        return False, qty_reason

    return True, "execution_plan_valid"


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
    ok, reason = validate_execution_plan(plan)

    event = {
        "event_at_utc": utc_now_iso(),
        "event_at_wib": wib_now_iso(),
        "app_version": APP_VERSION,
        "execution_mode": execution_mode(),
        "binance_env": binance_env(),
        "signal_key": signal_key_of(p),
        "pair": pair_of(p),
        "symbol": plan.get("symbol"),
        "decision": "BUILT_ONLY" if ok else "SKIPPED",
        "reason": reason,
        "plan": plan,
    }

    append_jsonl(EXECUTION_EVENTS_LOG, event)

    if ok:
        append_jsonl(EXECUTION_PLANS_LOG, plan)

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

        elif execution_mode() == "TESTNET_MARKET":
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
    close_status = normalize_close_status(outcome)

    target["status"] = close_status
    target["closed_at_utc"] = closed_at_utc
    target["closed_at_wib"] = datetime.now(WIB).isoformat()
    target["close_reason"] = close_reason or "MANUAL_CLOSE"
    target["close_outcome"] = outcome or close_status
    target["close_price"] = close_price
    target["notes"] = notes or ""

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

        close_status = normalize_close_status(outcome)

        pos["status"] = close_status
        pos["closed_at_utc"] = utc_now_iso()
        pos["closed_at_wib"] = datetime.now(WIB).isoformat()
        pos["close_reason"] = close_reason or "MANUAL_CLOSE_ALL"
        pos["close_outcome"] = outcome or close_status
        pos["close_price"] = close_price
        pos["notes"] = notes or ""

        closed_now.append(pos)

        append_paper_event(
            "PAPER_POSITION_CLOSED",
            pos,
            {
                "close_reason": pos["close_reason"],
                "close_outcome": pos["close_outcome"],
                "close_price": close_price,
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


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "ok": True,
        "app_version": APP_VERSION,
        "mode": get_mode(),
        "time_utc": utc_now_iso(),
    }


@app.post("/webhook/signal")
def webhook_signal(
    payload: SignalPayload,
    x_signal_secret: Optional[str] = Header(default=None, alias="X-Signal-Secret"),
    x_webhook_secret: Optional[str] = Header(default=None, alias="X-Webhook-Secret"),
) -> Dict[str, Any]:
    verify_secret(x_signal_secret, x_webhook_secret)

    p = payload_to_dict(payload)
    mode = get_mode()

    with LOCK:
        append_jsonl(SIGNALS_LOG, build_signal_log(p))

        state = load_state()

        if mode == "RECEIVED_ONLY":
            decision = {
                "decision": "RECEIVED_ONLY",
                "reason": "receiver_only_logger_mode_no_paper_gate_no_execution",
                "gate": "mode_gate",
            }
            append_jsonl(DECISIONS_LOG, build_decision_log(p, decision, state))

            return {
                "ok": True,
                "decision": "RECEIVED_ONLY",
                "reason": "v0.3 logger mode, no execution",
                "signal_id": p.get("signal_id") or p.get("signal_key"),
            }

        # PAPER mode: accept/reject only. No Binance execution.
        decision = paper_decide(p, state)
        state = apply_decision_to_state(p, decision, state)
        save_state(state)

        append_jsonl(DECISIONS_LOG, build_decision_log(p, decision, state))

        response = {
            "ok": True,
            "decision": decision["decision"],
            "reason": decision["reason"],
            "gate": decision["gate"],
            "signal_id": p.get("signal_id") or p.get("signal_key"),
            "execution_mode": execution_mode(),
        }

        if decision.get("decision") == "ACCEPT":
            execution_event = handle_execution_after_accept(p)
            if execution_mode() == "TESTNET_MARKET":
                market_res = execution_event.get("market_order_result")
                response["market_order_result"] = market_res
                if not (market_res or {}).get("ok"):
                    response["ok"] = False
                    response["execution_error_reason"] = execution_event.get("reason")
            elif execution_mode() == "TESTNET_ORDER_TEST":
                response["testnet_order_result"] = execution_event.get("order_test_result")

        return response


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
            outcome=req.outcome or "CLOSED_MANUAL",
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
            outcome=req.outcome or "CLOSED_MANUAL",
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
        append_jsonl(EXECUTION_EVENTS_LOG, {
            "event_at_utc": utc_now_iso(),
            "event_at_wib": wib_now_iso(),
            "app_version": APP_VERSION,
            "action": "TESTNET_PLACE_PROTECTION",
            "symbol": symbol,
            "decision": decision,
            "reason": "sl_failed_tp_skipped",
            "orders": placed,
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
