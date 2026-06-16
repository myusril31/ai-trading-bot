import sys, json, time
sys.path.insert(0, "/app")

from pathlib import Path
from decimal import Decimal
import app.main as m

ROOT = Path("/app")
EXEC = ROOT / "logs" / "execution_events.jsonl"
LOG = ROOT / "logs" / "limit_entry_ttl_events.jsonl"
STATE = ROOT / "state" / "limit_entry_ttl_state.json"

def D(x):
    try:
        if x is None or x == "":
            return Decimal("0")
        return Decimal(str(x))
    except Exception:
        return Decimal("0")

def append(row):
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with LOG.open("a") as f:
        f.write(json.dumps(row, separators=(",",":")) + "\n")

def load_state():
    try:
        if STATE.exists():
            return json.loads(STATE.read_text())
    except Exception:
        pass
    return {"orders": {}}

def save_state(st):
    STATE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE.with_suffix(".tmp")
    tmp.write_text(json.dumps(st, indent=2))
    tmp.replace(STATE)

def now_ms():
    return int(time.time() * 1000)

def side_direction(plan, entry_side):
    d = str(plan.get("direction") or plan.get("payload", {}).get("direction") or "").upper()
    if d.startswith("LONG"):
        return "LONG"
    if d.startswith("SHORT"):
        return "SHORT"
    return "LONG" if str(entry_side).upper() == "BUY" else "SHORT"

def current_price(symbol):
    try:
        return m.live_bad_fill_current_price(symbol)
    except Exception:
        return Decimal("0")

def latest_limit_entries():
    out = {}
    if not EXEC.exists():
        return out

    for line in EXEC.read_text(errors="ignore").splitlines():
        try:
            r = json.loads(line)
        except Exception:
            continue

        plan = r.get("plan") if isinstance(r.get("plan"), dict) else {}
        entry_res = r.get("entry_result") if isinstance(r.get("entry_result"), dict) else {}
        body = entry_res.get("body") if isinstance(entry_res.get("body"), dict) else {}

        symbol = str(r.get("symbol") or body.get("symbol") or plan.get("symbol") or "").upper()
        order_id = body.get("orderId")
        client_id = body.get("clientOrderId")
        if not symbol or not order_id:
            continue

        entry_type = str(plan.get("entry_order_type") or "").upper()
        order_type = str(body.get("type") or "").upper()
        tif = str(body.get("timeInForce") or "").upper()

        if entry_type not in ("LIMIT_TTL", "LIMIT_GTC") and not (order_type == "LIMIT" and tif == "GTC"):
            continue

        key = f"{symbol}|{order_id}"
        out[key] = {
            "key": key,
            "symbol": symbol,
            "orderId": order_id,
            "clientOrderId": client_id,
            "signal_key": r.get("signal_key"),
            "event_at_wib": r.get("event_at_wib"),
            "plan": plan,
            "entry_side": body.get("side") or ("BUY" if str(plan.get("direction","")).upper().startswith("LONG") else "SELL"),
            "created_ms": int(body.get("time") or body.get("updateTime") or now_ms()),
        }
    return out

def get_order(symbol, order_id):
    return m.live_signed_request("GET", "/fapi/v1/order", {"symbol": symbol, "orderId": order_id})

def cancel_order(symbol, order_id):
    return m.live_signed_request("DELETE", "/fapi/v1/order", {"symbol": symbol, "orderId": order_id})

def get_position(symbol):
    res = m.live_signed_request("GET", "/fapi/v2/positionRisk", {"symbol": symbol})
    rows = res.get("body") if isinstance(res.get("body"), list) else []
    for p in rows:
        if str(p.get("symbol") or "").upper() == symbol:
            return p
    return None

def open_algo(symbol):
    res = m.live_signed_request("GET", "/fapi/v1/openAlgoOrders", {"symbol": symbol})
    return res.get("body") if isinstance(res.get("body"), list) else []

def has_sl_tp(symbol):
    rows = open_algo(symbol)
    has_sl = any(str(x.get("orderType") or "").upper() == "STOP_MARKET" for x in rows)
    has_tp = any(str(x.get("orderType") or "").upper() == "TAKE_PROFIT_MARKET" for x in rows)
    return has_sl, has_tp, rows

def touched_before_fill(symbol, direction, plan):
    px = current_price(symbol)
    tp1 = D(plan.get("tp1"))
    sl = D(plan.get("sl"))

    cancel_tp = str(m.os.getenv("LIMIT_CANCEL_IF_TP1_TOUCHED", "true")).lower() in ("1","true","yes","y","on")
    cancel_sl = str(m.os.getenv("LIMIT_CANCEL_IF_SL_TOUCHED", "true")).lower() in ("1","true","yes","y","on")

    if px <= 0:
        return False, None, px

    if direction == "LONG":
        if cancel_tp and tp1 > 0 and px >= tp1:
            return True, "TP1_TOUCHED_BEFORE_ENTRY_FILL", px
        if cancel_sl and sl > 0 and px <= sl:
            return True, "SL_TOUCHED_BEFORE_ENTRY_FILL", px

    if direction == "SHORT":
        if cancel_tp and tp1 > 0 and px <= tp1:
            return True, "TP1_TOUCHED_BEFORE_ENTRY_FILL", px
        if cancel_sl and sl > 0 and px >= sl:
            return True, "SL_TOUCHED_BEFORE_ENTRY_FILL", px

    return False, None, px

def protect_position(symbol, direction, qty, plan, signal_key):
    exit_side = "SELL" if direction == "LONG" else "BUY"
    suffix = abs(hash(str(signal_key) + symbol)) % 999999999

    results = []

    sl_params = {
        "symbol": symbol,
        "side": exit_side,
        "type": "STOP_MARKET",
        "stopPrice": str(plan["sl"]),
        "quantity": str(qty),
        "reduceOnly": "true",
        "workingType": "CONTRACT_PRICE",
        "newClientOrderId": f"LTTL_{suffix}_SL"[:36],
    }
    results.append({"label": "SL", "result": m.live_place_algo_order(sl_params)})

    tp_params = {
        "symbol": symbol,
        "side": exit_side,
        "type": "TAKE_PROFIT_MARKET",
        "stopPrice": str(plan["tp1"]),
        "quantity": str(qty),
        "reduceOnly": "true",
        "workingType": "CONTRACT_PRICE",
        "newClientOrderId": f"LTTL_{suffix}_TP1"[:36],
    }
    results.append({"label": "TP1", "result": m.live_place_algo_order(tp_params)})

    return results

def run_once():
    st = load_state()
    st.setdefault("orders", {})

    ttl_sec = int(float(m.os.getenv("LIVE_ENTRY_TTL_SEC", "5400")))
    entries = latest_limit_entries()
    events = []

    for key, e in entries.items():
        prev = st["orders"].get(key, {})
        if prev.get("status") in ("CANCELED", "PROTECTED", "NO_POSITION_CLOSED", "EXPIRED"):
            continue

        symbol = e["symbol"]
        plan = dict(e.get("plan") or {})
        direction = side_direction(plan, e.get("entry_side"))
        order_id = e["orderId"]

        ord_res = get_order(symbol, order_id)
        ord_body = ord_res.get("body") if isinstance(ord_res.get("body"), dict) else {}
        status = str(ord_body.get("status") or "").upper()
        executed = D(ord_body.get("executedQty") or "0")
        created_ms = int(ord_body.get("time") or e.get("created_ms") or now_ms())
        age_sec = max(0, (now_ms() - created_ms) / 1000)

        should_cancel, cancel_reason, px = touched_before_fill(symbol, direction, plan)

        if status in ("NEW", "PARTIALLY_FILLED") and executed <= 0:
            if age_sec >= ttl_sec:
                cancel_reason = "TTL_EXPIRED"
                should_cancel = True

            if should_cancel:
                cancel_res = cancel_order(symbol, order_id)
                row = {
                    "event_at_utc": m.utc_now_iso(),
                    "action": "LIMIT_TTL_ENTRY_CANCELED",
                    "symbol": symbol,
                    "orderId": order_id,
                    "signal_key": e.get("signal_key"),
                    "reason": cancel_reason,
                    "age_sec": age_sec,
                    "current_price": str(px),
                    "order_status": status,
                    "cancel_result": cancel_res,
                }
                st["orders"][key] = {"status": "CANCELED", "reason": cancel_reason, "updated_at": m.utc_now_iso()}
                append(row); events.append(row)
                continue

            row = {
                "event_at_utc": m.utc_now_iso(),
                "action": "LIMIT_TTL_ENTRY_PENDING",
                "symbol": symbol,
                "orderId": order_id,
                "signal_key": e.get("signal_key"),
                "age_sec": age_sec,
                "ttl_sec": ttl_sec,
                "current_price": str(px),
                "order_status": status,
            }
            st["orders"][key] = {"status": "PENDING", "updated_at": m.utc_now_iso()}
            append(row); events.append(row)
            continue

        # Partial filled: cancel rest, protect filled position.
        if status == "PARTIALLY_FILLED" and executed > 0:
            cancel_order(symbol, order_id)

        if status in ("FILLED", "PARTIALLY_FILLED") or executed > 0:
            pos = get_position(symbol)
            amt = D((pos or {}).get("positionAmt") or "0")
            if amt == 0:
                row = {
                    "event_at_utc": m.utc_now_iso(),
                    "action": "LIMIT_TTL_FILLED_BUT_NO_POSITION",
                    "symbol": symbol,
                    "orderId": order_id,
                    "status": status,
                    "executedQty": str(executed),
                }
                st["orders"][key] = {"status": "NO_POSITION_CLOSED", "updated_at": m.utc_now_iso()}
                append(row); events.append(row)
                continue

            qty = abs(amt)
            has_sl, has_tp, algos = has_sl_tp(symbol)
            if has_sl and has_tp:
                row = {
                    "event_at_utc": m.utc_now_iso(),
                    "action": "LIMIT_TTL_ALREADY_PROTECTED",
                    "symbol": symbol,
                    "orderId": order_id,
                    "positionAmt": str(amt),
                    "open_algo_count": len(algos),
                }
                st["orders"][key] = {"status": "PROTECTED", "updated_at": m.utc_now_iso()}
                append(row); events.append(row)
                continue

            results = protect_position(symbol, direction, qty, plan, e.get("signal_key"))
            ok = all(bool((x.get("result") or {}).get("ok")) for x in results)

            row = {
                "event_at_utc": m.utc_now_iso(),
                "action": "LIMIT_TTL_FILLED_PROTECTION_PLACED" if ok else "LIMIT_TTL_FILLED_PROTECTION_FAILED",
                "symbol": symbol,
                "orderId": order_id,
                "signal_key": e.get("signal_key"),
                "direction": direction,
                "positionAmt": str(amt),
                "qty": str(qty),
                "results": results,
            }
            st["orders"][key] = {"status": "PROTECTED" if ok else "PROTECTION_FAILED", "updated_at": m.utc_now_iso()}
            append(row); events.append(row)
            continue

        if status in ("CANCELED", "EXPIRED", "REJECTED"):
            row = {
                "event_at_utc": m.utc_now_iso(),
                "action": "LIMIT_TTL_ENTRY_TERMINAL",
                "symbol": symbol,
                "orderId": order_id,
                "status": status,
            }
            st["orders"][key] = {"status": status, "updated_at": m.utc_now_iso()}
            append(row); events.append(row)

    save_state(st)
    return {"ok": True, "events": len(events)}

if __name__ == "__main__":
    print(json.dumps(run_once(), indent=2))
