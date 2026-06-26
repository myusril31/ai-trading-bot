import sys, json, time
sys.path.insert(0, "/app")

from pathlib import Path
from decimal import Decimal
import app.main as m

SYMBOL = "AAVEUSDT"
ROOT = Path("/app")
EXEC = ROOT / "logs" / "execution_events.jsonl"
LOG = ROOT / "logs" / "force_repair_aave.jsonl"

def append(x):
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with LOG.open("a") as f:
        f.write(json.dumps(x, separators=(",",":")) + "\n")

def latest_plan(symbol):
    best = None
    for line in EXEC.read_text(errors="ignore").splitlines():
        try:
            r = json.loads(line)
        except Exception:
            continue
        if str(r.get("symbol") or "").upper() != symbol:
            continue
        plan = r.get("plan") if isinstance(r.get("plan"), dict) else None
        if plan and plan.get("sl") and plan.get("tp1"):
            best = r
    return best

def get_position(symbol):
    res = m.live_signed_request("GET", "/fapi/v2/positionRisk", {"symbol": symbol})
    body = res.get("body")
    rows = body if isinstance(body, list) else []
    for x in rows:
        if str(x.get("symbol") or "").upper() == symbol:
            return x
    return None

def cancel_existing(symbol):
    results = []

    # cancel normal open orders
    oo = m.live_signed_request("GET", "/fapi/v1/openOrders", {"symbol": symbol})
    for o in (oo.get("body") if isinstance(oo.get("body"), list) else []):
        oid = o.get("orderId")
        if oid:
            results.append({
                "source": "openOrders",
                "orderId": oid,
                "cancel": m.live_signed_request("DELETE", "/fapi/v1/order", {"symbol": symbol, "orderId": oid})
            })

    # cancel algo orders
    ao = m.live_signed_request("GET", "/fapi/v1/openAlgoOrders", {"symbol": symbol})
    for o in (ao.get("body") if isinstance(ao.get("body"), list) else []):
        algo_id = o.get("algoId")
        client_id = o.get("clientAlgoId")
        params = {"symbol": symbol}
        if algo_id:
            params["algoId"] = algo_id
        elif client_id:
            params["clientAlgoId"] = client_id
        else:
            continue
        results.append({
            "source": "openAlgoOrders",
            "algoId": algo_id,
            "clientAlgoId": client_id,
            "cancel": m.live_signed_request("DELETE", "/fapi/v1/algoOrder", params)
        })

    return results

pos = get_position(SYMBOL)
if not pos:
    out = {"action":"FORCE_REPAIR_FAIL", "symbol":SYMBOL, "reason":"position_not_found"}
    print(json.dumps(out, indent=2)); append(out); raise SystemExit(1)

amt = Decimal(str(pos.get("positionAmt") or "0"))
if amt == 0:
    out = {"action":"FORCE_REPAIR_SKIP", "symbol":SYMBOL, "reason":"no_open_position", "position":pos}
    print(json.dumps(out, indent=2)); append(out); raise SystemExit(0)

direction = "SHORT" if amt < 0 else "LONG"
exit_side = "BUY" if direction == "SHORT" else "SELL"
qty = abs(amt)

row = latest_plan(SYMBOL)
if not row:
    out = {"action":"FORCE_REPAIR_FAIL", "symbol":SYMBOL, "reason":"no_latest_plan", "position":pos}
    print(json.dumps(out, indent=2)); append(out); raise SystemExit(1)

plan = row["plan"]
signal_key = str(row.get("signal_key") or f"REPAIR_{SYMBOL}_{int(time.time())}")
suffix = abs(hash(signal_key)) % 999999999

cancel_results = cancel_existing(SYMBOL)
time.sleep(0.5)

q1 = qty * Decimal("0.40")
q2 = qty * Decimal("0.35")
q3 = qty - q1 - q2

results = []

sl_params = {
    "symbol": SYMBOL,
    "side": exit_side,
    "type": "STOP_MARKET",
    "stopPrice": str(plan["sl"]),
    "quantity": str(qty),
    "reduceOnly": "true",
    "workingType": "CONTRACT_PRICE",
    "newClientOrderId": f"FR_{suffix}_SL"[:36],
}
results.append({"label":"SL", "params":sl_params, "result":m.live_place_algo_order(sl_params)})

for label, key, q in [
    ("TP1","tp1",q1),
    ("TP2","tp2",q2),
    ("TP3","tp3",q3),
]:
    if not plan.get(key) or q <= 0:
        continue
    params = {
        "symbol": SYMBOL,
        "side": exit_side,
        "type": "TAKE_PROFIT_MARKET",
        "stopPrice": str(plan[key]),
        "quantity": str(q),
        "reduceOnly": "true",
        "workingType": "CONTRACT_PRICE",
        "newClientOrderId": f"FR_{suffix}_{label}"[:36],
    }
    results.append({"label":label, "params":params, "result":m.live_place_algo_order(params)})

verify_algo = m.live_signed_request("GET", "/fapi/v1/openAlgoOrders", {"symbol": SYMBOL})
verify_normal = m.live_signed_request("GET", "/fapi/v1/openOrders", {"symbol": SYMBOL})

out = {
    "event_at_utc": m.utc_now_iso(),
    "action": "FORCE_REPAIR_AAVE_PROTECTION",
    "symbol": SYMBOL,
    "direction": direction,
    "positionAmt": str(amt),
    "qty": str(qty),
    "signal_key": signal_key,
    "cancel_results": cancel_results,
    "place_results": results,
    "verify_openAlgoOrders_count": len(verify_algo.get("body") if isinstance(verify_algo.get("body"), list) else []),
    "verify_openOrders_count": len(verify_normal.get("body") if isinstance(verify_normal.get("body"), list) else []),
    "verify_openAlgoOrders_sample": (verify_algo.get("body") if isinstance(verify_algo.get("body"), list) else [])[:5],
}
print(json.dumps(out, indent=2))
append(out)
