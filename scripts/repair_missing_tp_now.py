import sys, json, time
sys.path.insert(0, "/app")

from pathlib import Path
from decimal import Decimal
import app.main as m

SYMBOLS = ["AAVEUSDT", "AVAXUSDT"]
ROOT = Path("/app")
EXEC = ROOT / "logs" / "execution_events.jsonl"
LOG = ROOT / "logs" / "repair_missing_tp_now.jsonl"

def D(x):
    try: return Decimal(str(x))
    except Exception: return Decimal("0")

def append(x):
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with LOG.open("a") as f:
        f.write(json.dumps(x, separators=(",",":")) + "\n")

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
        if plan and plan.get("sl") and plan.get("tp1"):
            best = r
    return best

def get_pos(symbol):
    res = m.live_signed_request("GET", "/fapi/v2/positionRisk", {"symbol": symbol})
    rows = res.get("body") if isinstance(res.get("body"), list) else []
    for p in rows:
        if str(p.get("symbol") or "").upper() == symbol:
            return p
    return None

def open_algo(symbol):
    res = m.live_signed_request("GET", "/fapi/v1/openAlgoOrders", {"symbol": symbol})
    return res.get("body") if isinstance(res.get("body"), list) else []

for sym in SYMBOLS:
    pos = get_pos(sym)
    amt = D((pos or {}).get("positionAmt") or "0")
    if amt == 0:
        out = {"event_at_utc": m.utc_now_iso(), "symbol": sym, "action": "SKIP_NO_POSITION"}
        print(json.dumps(out, indent=2)); append(out); continue

    direction = "LONG" if amt > 0 else "SHORT"
    exit_side = "SELL" if direction == "LONG" else "BUY"
    qty = abs(amt)

    algos = open_algo(sym)
    has_sl = any(str(o.get("orderType") or "").upper() == "STOP_MARKET" for o in algos)
    has_tp = any(str(o.get("orderType") or "").upper() == "TAKE_PROFIT_MARKET" for o in algos)

    row = latest_plan(sym)
    if not row:
        out = {"event_at_utc": m.utc_now_iso(), "symbol": sym, "action": "FAIL_NO_PLAN", "positionAmt": str(amt)}
        print(json.dumps(out, indent=2)); append(out); continue

    plan = dict(row.get("plan") or {})
    signal_key = str(row.get("signal_key") or f"REPAIR_{sym}_{int(time.time())}")
    suffix = abs(hash(signal_key)) % 999999999

    results = []

    if not has_sl:
        sl_params = {
            "symbol": sym,
            "side": exit_side,
            "type": "STOP_MARKET",
            "stopPrice": str(plan["sl"]),
            "quantity": str(qty),
            "reduceOnly": "true",
            "workingType": "CONTRACT_PRICE",
            "newClientOrderId": f"RTP_{suffix}_SL"[:36],
        }
        results.append({"label": "SL", "result": m.live_place_algo_order(sl_params)})

    if not has_tp:
        tp_params = {
            "symbol": sym,
            "side": exit_side,
            "type": "TAKE_PROFIT_MARKET",
            "stopPrice": str(plan["tp1"]),
            "quantity": str(qty),
            "reduceOnly": "true",
            "workingType": "CONTRACT_PRICE",
            "newClientOrderId": f"RTP_{suffix}_TP1"[:36],
        }
        results.append({"label": "TP1", "result": m.live_place_algo_order(tp_params)})

    verify = open_algo(sym)
    out = {
        "event_at_utc": m.utc_now_iso(),
        "symbol": sym,
        "action": "REPAIR_MISSING_PROTECTION",
        "positionAmt": str(amt),
        "had_sl": has_sl,
        "had_tp": has_tp,
        "results": results,
        "verify_openAlgoOrders_count": len(verify),
        "verify_orderTypes": [o.get("orderType") for o in verify],
    }
    print(json.dumps(out, indent=2))
    append(out)
