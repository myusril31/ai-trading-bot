import sys, json, os, time
from pathlib import Path
from decimal import Decimal

sys.path.insert(0, "/app")
import app.main as m

ROOT = Path("/app")
EXEC_LOG = ROOT / "logs" / "execution_events.jsonl"
STATE = ROOT / "state" / "protection_fail_autoclose_state.json"
OUT_LOG = ROOT / "logs" / "protection_fail_autoclose.jsonl"

def enabled(name, default="true"):
    return str(os.getenv(name, default)).strip().lower() in ("1", "true", "yes", "on")

def D(x):
    try:
        return Decimal(str(x))
    except Exception:
        return Decimal("0")

def append(row):
    OUT_LOG.parent.mkdir(parents=True, exist_ok=True)
    row["ts"] = time.time()
    OUT_LOG.open("a").write(json.dumps(row, default=str) + "\n")

def load_state():
    try:
        return json.loads(STATE.read_text())
    except Exception:
        return {"processed_keys": []}

def save_state(st):
    STATE.parent.mkdir(parents=True, exist_ok=True)
    st["processed_keys"] = list(dict.fromkeys(st.get("processed_keys", [])))[-500:]
    STATE.write_text(json.dumps(st, indent=2))

def signed(method, path, params):
    return m.live_signed_request(method, path, params)

def round_qty(symbol, qty):
    q = abs(D(qty))
    try:
        if hasattr(m, "live_entry_symbol_filters") and hasattr(m, "live_entry_round_to_step"):
            fs = m.live_entry_symbol_filters(symbol)
            return m.live_entry_round_to_step(str(q), fs.get("stepSize", "0.001"), "down")
    except Exception:
        pass
    return format(q, "f").rstrip("0").rstrip(".") or "0"

def get_position(symbol):
    res = signed("GET", "/fapi/v2/positionRisk", {})
    body = res.get("body") if isinstance(res, dict) else []
    for p in body or []:
        if str(p.get("symbol")) == symbol:
            amt = D(p.get("positionAmt"))
            if amt != 0:
                return p
    return None

def close_symbol(symbol, reason):
    pos = get_position(symbol)
    if not pos:
        return {"ok": True, "reason": "no_open_position", "symbol": symbol}

    amt = D(pos.get("positionAmt"))
    side = "SELL" if amt > 0 else "BUY"
    qty = round_qty(symbol, amt)

    params = {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quantity": qty,
        "reduceOnly": "true",
        "newClientOrderId": f"SMC_PROTFAIL_CLOSE_{symbol}_{int(time.time())}"[:36],
    }

    if hasattr(m, "live_place_order"):
        res = m.live_place_order(params)
    else:
        res = signed("POST", "/fapi/v1/order", params)

    try:
        if hasattr(m, "send_telegram_message"):
            m.send_telegram_message(
                "🚨 PROTECTION FAIL AUTO-CLOSE\n"
                f"Symbol: {symbol}\n"
                f"Reason: {reason}\n"
                f"Close side: {side}\n"
                f"Qty: {qty}\n"
                f"OK: {bool(res.get('ok')) if isinstance(res, dict) else False}"
            )
    except Exception:
        pass

    return {
        "ok": bool(res.get("ok")) if isinstance(res, dict) else False,
        "symbol": symbol,
        "reason": reason,
        "positionAmt": str(amt),
        "close_params": params,
        "close_result": res,
    }

def main():
    if not enabled("PROTECTION_FAIL_AUTO_CLOSE_ENABLED", "true"):
        print(json.dumps({"ok": True, "disabled": True}))
        return

    st = load_state()
    processed = set(st.get("processed_keys") or [])

    if not EXEC_LOG.exists():
        print(json.dumps({"ok": True, "reason": "no_exec_log"}))
        return

    lines = EXEC_LOG.read_text(errors="ignore").splitlines()[-500:]
    actions = []

    for line in lines:
        try:
            r = json.loads(line)
        except Exception:
            continue

        decision = str(r.get("decision") or "")
        reason = str(r.get("reason") or "")
        txt = decision + "|" + reason

        if "PROTECTION_PARTIAL_OR_FAILED" not in txt and "protection" not in txt.lower():
            continue
        if "FAILED" not in txt.upper() and "PARTIAL" not in txt.upper():
            continue

        symbol = str(r.get("symbol") or r.get("pair") or "").replace("BINANCE:", "").replace(".P", "").upper()
        if not symbol:
            continue

        key = str(r.get("signal_key") or "") + "|" + symbol + "|" + str(r.get("event_at_wib") or r.get("event_at_utc") or "")
        if key in processed:
            continue

        result = close_symbol(symbol, txt)
        actions.append(result)
        processed.add(key)
        append({"event": "PROTECTION_FAIL_AUTO_CLOSE", "source_event": r, "result": result})

    st["processed_keys"] = list(processed)
    save_state(st)
    print(json.dumps({"ok": True, "actions": actions}, indent=2, default=str))

if __name__ == "__main__":
    main()
