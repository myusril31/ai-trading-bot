import json, time
import sys
sys.path.insert(0, '/app')
from pathlib import Path
from decimal import Decimal

import app.main as m

ROOT = Path("/app")
LOG = ROOT / "logs" / "protection_repair_now.jsonl"
EXEC = ROOT / "logs" / "execution_events.jsonl"

def append(row):
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with LOG.open("a") as f:
        f.write(json.dumps(row, separators=(",",":")) + "\n")

def d(x):
    return Decimal(str(x))

def latest_plan(symbol):
    best = None
    if not EXEC.exists():
        return None
    for line in EXEC.read_text(errors="ignore").splitlines():
        try:
            r = json.loads(line)
        except Exception:
            continue
        if str(r.get("symbol") or "").upper() != symbol:
            continue
        plan = r.get("plan") if isinstance(r.get("plan"), dict) else None
        if not plan:
            continue
        if plan.get("sl") and plan.get("tp1"):
            best = r
    return best

def has_protection(symbol):
    normal = m.live_signed_request("GET", "/fapi/v1/openOrders", {"symbol": symbol})
    algo = m.live_signed_request("GET", "/fapi/v1/openAlgoOrders", {"symbol": symbol})
    normal_body = normal.get("body") if isinstance(normal.get("body"), list) else []
    algo_body = algo.get("body") if isinstance(algo.get("body"), list) else []

    rows = []
    for x in normal_body:
        if isinstance(x, dict):
            rows.append(x)
    for x in algo_body:
        if isinstance(x, dict):
            rows.append(x)

    has_sl = any(str(x.get("type") or x.get("orderType") or "").upper() == "STOP_MARKET" for x in rows)
    has_tp = any(str(x.get("type") or x.get("orderType") or "").upper() == "TAKE_PROFIT_MARKET" for x in rows)
    return {"has_sl": has_sl, "has_tp": has_tp, "count": len(rows), "normal": normal_body, "algo": algo_body}

pos_res = m.live_signed_request("GET", "/fapi/v2/positionRisk", {})
positions = pos_res.get("body") if isinstance(pos_res.get("body"), list) else []

for pos in positions:
    symbol = str(pos.get("symbol") or "").upper()
    amt = d(pos.get("positionAmt") or "0")
    if amt == 0:
        continue

    direction = "SHORT" if amt < 0 else "LONG"
    exit_side = "BUY" if direction == "SHORT" else "SELL"
    qty = abs(amt)

    existing = has_protection(symbol)
    if existing["has_sl"] and existing["has_tp"]:
        out = {
            "event_at_utc": m.utc_now_iso(),
            "action": "REPAIR_SKIP_ALREADY_PROTECTED",
            "symbol": symbol,
            "positionAmt": str(amt),
            "open_protection_count": existing["count"],
        }
        append(out)
        print(json.dumps(out, indent=2))
        continue

    row = latest_plan(symbol)
    if not row:
        out = {
            "event_at_utc": m.utc_now_iso(),
            "action": "REPAIR_FAIL_NO_PLAN",
            "symbol": symbol,
            "positionAmt": str(amt),
            "existing": existing,
        }
        append(out)
        print(json.dumps(out, indent=2))
        continue

    plan = dict(row.get("plan") or {})
    signal_key = str(row.get("signal_key") or f"REPAIR_{symbol}_{int(time.time())}")
    suffix = abs(hash(signal_key)) % 999999999

    qty_str = str(qty)
    q1 = qty * Decimal("0.40")
    q2 = qty * Decimal("0.35")
    q3 = qty - q1 - q2

    orders = []

    sl_params = {
        "symbol": symbol,
        "side": exit_side,
        "type": "STOP_MARKET",
        "stopPrice": str(plan["sl"]),
        "quantity": qty_str,
        "reduceOnly": "true",
        "workingType": "CONTRACT_PRICE",
        "newClientOrderId": f"REPAIR_{suffix}_SL"[:36],
    }
    orders.append({"label": "SL", "params": sl_params, "result": m.live_place_algo_order(sl_params)})

    for label, tp_key, q in [
        ("TP1", "tp1", q1),
        ("TP2", "tp2", q2),
        ("TP3", "tp3", q3),
    ]:
        if not plan.get(tp_key) or q <= 0:
            continue
        params = {
            "symbol": symbol,
            "side": exit_side,
            "type": "TAKE_PROFIT_MARKET",
            "stopPrice": str(plan[tp_key]),
            "quantity": str(q),
            "reduceOnly": "true",
            "workingType": "CONTRACT_PRICE",
            "newClientOrderId": f"REPAIR_{suffix}_{label}"[:36],
        }
        orders.append({"label": label, "params": params, "result": m.live_place_algo_order(params)})

    out = {
        "event_at_utc": m.utc_now_iso(),
        "action": "REPAIR_OPEN_POSITION_PROTECTION",
        "symbol": symbol,
        "direction": direction,
        "positionAmt": str(amt),
        "signal_key": signal_key,
        "results": orders,
    }
    append(out)
    print(json.dumps(out, indent=2))
