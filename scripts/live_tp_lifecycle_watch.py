import sys, json, time, argparse
sys.path.insert(0, "/app")

from pathlib import Path
from decimal import Decimal
import app.main as m

ROOT = Path("/app")
STATE_FILE = ROOT / "state" / "tp_lifecycle_state.json"
EXEC_LOG = ROOT / "logs" / "execution_events.jsonl"
LIFE_LOG = ROOT / "logs" / "tp_lifecycle_events.jsonl"

def D(x, default="0"):
    try:
        return Decimal(str(x))
    except Exception:
        return Decimal(default)

def plain(x):
    out = format(Decimal(str(x)).normalize(), "f")
    if "." in out:
        out = out.rstrip("0").rstrip(".")
    return out or "0"

def append_log(row):
    LIFE_LOG.parent.mkdir(parents=True, exist_ok=True)
    with LIFE_LOG.open("a") as f:
        f.write(json.dumps(row, separators=(",",":")) + "\n")

def load_state():
    try:
        if STATE_FILE.exists():
            return json.loads(STATE_FILE.read_text())
    except Exception:
        pass
    return {"symbols": {}, "updated_at_utc": None}

def save_state(st):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    st["updated_at_utc"] = m.utc_now_iso()
    tmp = STATE_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(st, indent=2))
    tmp.replace(STATE_FILE)

def latest_plan(symbol):
    best = None
    if not EXEC_LOG.exists():
        return None
    lines = EXEC_LOG.read_text(errors="ignore").splitlines()
    for line in reversed(lines):
        try:
            r = json.loads(line)
        except Exception:
            continue
        if str(r.get("symbol") or "").upper() != symbol:
            continue
        plan = r.get("plan") if isinstance(r.get("plan"), dict) else None
        if not plan:
            continue
        if plan.get("sl") and plan.get("tp1") and plan.get("quantity"):
            best = r
            break
    return best

def all_positions():
    res = m.live_signed_request("GET", "/fapi/v2/positionRisk", {})
    rows = res.get("body") if isinstance(res.get("body"), list) else []
    out = []
    for p in rows:
        amt = D(p.get("positionAmt") or "0")
        if amt != 0:
            out.append(p)
    return out

def open_algo(symbol):
    res = m.live_signed_request("GET", "/fapi/v1/openAlgoOrders", {"symbol": symbol})
    body = res.get("body") if isinstance(res.get("body"), list) else []
    return body

def current_price(symbol):
    try:
        base = m.live_fapi_base_url()
    except Exception:
        base = "https://fapi.binance.com"
    try:
        data = m.http_get_json(f"{base.rstrip()}/fapi/v1/ticker/price?symbol={symbol}", timeout=5.0)
        return D(data.get("price"))
    except Exception:
        return Decimal("0")

def near_price(a, b):
    a = D(a); b = D(b)
    if a <= 0 or b <= 0:
        return False
    diff_pct = abs(a - b) / b * Decimal("100")
    return diff_pct <= Decimal("0.08")

def tp_presence(algo_orders, plan):
    tps = [x for x in algo_orders if str(x.get("orderType") or x.get("type") or "").upper() == "TAKE_PROFIT_MARKET"]
    triggers = [D(x.get("triggerPrice") or 0) for x in tps]
    return {
        "tp1": any(near_price(x, plan.get("tp1")) for x in triggers),
        "tp2": any(near_price(x, plan.get("tp2")) for x in triggers),
        "tp3": any(near_price(x, plan.get("tp3")) for x in triggers),
        "tp_count": len(tps),
        "triggers": [plain(x) for x in triggers],
    }

def stop_orders(algo_orders):
    return [x for x in algo_orders if str(x.get("orderType") or x.get("type") or "").upper() == "STOP_MARKET"]

def sl_good_enough(stops, direction, desired):
    desired = D(desired)
    if desired <= 0:
        return False
    vals = [D(x.get("triggerPrice") or 0) for x in stops]
    if not vals:
        return False
    if direction == "LONG":
        # higher stop is better for long
        return max(vals) >= desired * Decimal("0.999")
    if direction == "SHORT":
        # lower stop is better for short
        return min(vals) <= desired * Decimal("1.001")
    return False

def cancel_stop_orders(symbol, stops):
    out = []
    for o in stops:
        algo_id = o.get("algoId")
        client_id = o.get("clientAlgoId")
        params = {"symbol": symbol}
        if algo_id:
            params["algoId"] = algo_id
        elif client_id:
            params["clientAlgoId"] = client_id
        else:
            continue
        out.append({
            "algoId": algo_id,
            "clientAlgoId": client_id,
            "result": m.live_signed_request("DELETE", "/fapi/v1/algoOrder", params),
        })
    return out

def cancel_all_algo(symbol, algo_orders):
    out = []
    for o in algo_orders:
        algo_id = o.get("algoId")
        client_id = o.get("clientAlgoId")
        params = {"symbol": symbol}
        if algo_id:
            params["algoId"] = algo_id
        elif client_id:
            params["clientAlgoId"] = client_id
        else:
            continue
        out.append({
            "algoId": algo_id,
            "clientAlgoId": client_id,
            "orderType": o.get("orderType"),
            "result": m.live_signed_request("DELETE", "/fapi/v1/algoOrder", params),
        })
    return out

def place_sl(symbol, direction, qty, price, tag):
    exit_side = "SELL" if direction == "LONG" else "BUY"
    cid = f"LIFE_{abs(hash(symbol + tag)) % 999999999}_{tag}_{int(time.time())}"[:36]
    params = {
        "symbol": symbol,
        "side": exit_side,
        "type": "STOP_MARKET",
        "stopPrice": plain(price),
        "quantity": plain(qty),
        "reduceOnly": "true",
        "workingType": "CONTRACT_PRICE",
        "newClientOrderId": cid,
    }
    return {"params": params, "result": m.live_place_algo_order(params)}

def valid_stop_side(direction, desired_sl, last_price):
    desired_sl = D(desired_sl); last_price = D(last_price)
    if last_price <= 0:
        return True, "last_price_unavailable"
    if direction == "LONG":
        return desired_sl < last_price, f"LONG stop must be below last price: sl={desired_sl} last={last_price}"
    if direction == "SHORT":
        return desired_sl > last_price, f"SHORT stop must be above last price: sl={desired_sl} last={last_price}"
    return False, "invalid_direction"


def fallback_stop_after_invalid_be(direction, last_price):
    """
    Emergency tight stop when BE/TP1 lock cannot be placed because it would immediately trigger.
    LONG  fallback stop = slightly below current price.
    SHORT fallback stop = slightly above current price.
    """
    last_price = D(last_price)
    if last_price <= 0:
        return Decimal("0")

    buf_pct = D(getattr(m, "os").getenv("LIVE_TP_LIFECYCLE_FALLBACK_BUFFER_PCT", "0.12"))
    buf = buf_pct / Decimal("100")

    if direction == "LONG":
        return last_price * (Decimal("1") - buf)
    if direction == "SHORT":
        return last_price * (Decimal("1") + buf)
    return Decimal("0")

def handle_symbol(pos, state):
    symbol = str(pos.get("symbol") or "").upper()
    amt = D(pos.get("positionAmt") or "0")
    if amt == 0:
        return None

    direction = "LONG" if amt > 0 else "SHORT"
    qty_now = abs(amt)
    entry_price = D(pos.get("entryPrice") or "0")

    row = latest_plan(symbol)
    if not row:
        ev = {"event_at_utc": m.utc_now_iso(), "action": "TP_LIFECYCLE_NO_PLAN", "symbol": symbol, "positionAmt": plain(amt)}
        append_log(ev)
        return ev

    plan = dict(row.get("plan") or {})
    signal_key = str(row.get("signal_key") or plan.get("signal_key") or "")
    initial_qty = D(plan.get("quantity") or qty_now)
    if initial_qty <= 0:
        initial_qty = qty_now

    actual_entry = D(plan.get("actual_entry_price") or row.get("entry_fill_result", {}).get("entryPrice") or entry_price)
    if actual_entry <= 0:
        actual_entry = entry_price

    sym_state = state["symbols"].get(symbol, {})
    if sym_state.get("signal_key") != signal_key:
        sym_state = {
            "signal_key": signal_key,
            "symbol": symbol,
            "direction": direction,
            "initial_qty": plain(initial_qty),
            "actual_entry": plain(actual_entry),
            "stage": "INIT",
            "created_at_utc": m.utc_now_iso(),
        }

    stage = str(sym_state.get("stage") or "INIT").upper()

    algo = open_algo(symbol)
    stops = stop_orders(algo)
    tp = tp_presence(algo, plan)

    # TP hit detection:
    qty_tp1_hit = qty_now <= initial_qty * Decimal("0.70")
    qty_tp2_hit = qty_now <= initial_qty * Decimal("0.40")

    missing_tp1_hit = (not tp["tp1"]) and (tp["tp2"] or tp["tp3"]) and qty_now < initial_qty
    missing_tp2_hit = (not tp["tp2"]) and tp["tp3"] and qty_now < initial_qty

    tp1_hit = bool(qty_tp1_hit or missing_tp1_hit)
    tp2_hit = bool(qty_tp2_hit or missing_tp2_hit)

    be_buffer_pct = D(getattr(m, "os").getenv("LIVE_TP_LIFECYCLE_BE_BUFFER_PCT", "0.06"))
    be_buf = be_buffer_pct / Decimal("100")

    if direction == "LONG":
        be_sl = actual_entry * (Decimal("1") + be_buf)
        tp1_lock_sl = D(plan.get("tp1"))
    else:
        be_sl = actual_entry * (Decimal("1") - be_buf)
        tp1_lock_sl = D(plan.get("tp1"))

    target_stage = None
    target_price = None

    if tp2_hit:
        target_stage = "TP2_LOCK_TP1"
        target_price = tp1_lock_sl
    elif tp1_hit:
        target_stage = "TP1_BE"
        target_price = be_sl

    base = {
        "event_at_utc": m.utc_now_iso(),
        "action": "TP_LIFECYCLE_CHECK",
        "symbol": symbol,
        "signal_key": signal_key,
        "direction": direction,
        "positionAmt": plain(amt),
        "qty_now": plain(qty_now),
        "initial_qty": plain(initial_qty),
        "actual_entry": plain(actual_entry),
        "stage_before": stage,
        "tp_presence": tp,
        "open_algo_count": len(algo),
        "stop_count": len(stops),
        "tp1_hit": tp1_hit,
        "tp2_hit": tp2_hit,
    }

    if not target_stage:
        sym_state.update({
            "stage": stage,
            "last_check_utc": m.utc_now_iso(),
            "qty_now": plain(qty_now),
            "tp_presence": tp,
        })
        state["symbols"][symbol] = sym_state
        ev = dict(base)
        ev["decision"] = "NO_TP_LIFECYCLE_ACTION"
        append_log(ev)
        return ev

    # Kalau stage sudah sama dan SL sudah cukup bagus, jangan spam cancel/place.
    if stage == target_stage and sl_good_enough(stops, direction, target_price):
        sym_state.update({
            "stage": target_stage,
            "last_check_utc": m.utc_now_iso(),
            "qty_now": plain(qty_now),
            "managed_sl": plain(target_price),
        })
        state["symbols"][symbol] = sym_state
        ev = dict(base)
        ev.update({"decision": "ALREADY_MANAGED", "target_stage": target_stage, "target_sl": plain(target_price)})
        append_log(ev)
        return ev

    last = current_price(symbol)
    ok_side, side_reason = valid_stop_side(direction, target_price, last)
    if not ok_side:
        fallback_enabled = str(getattr(m, "os").getenv("LIVE_TP_LIFECYCLE_FALLBACK_ON_INVALID_BE", "true")).strip().lower() in ("1", "true", "yes", "y", "on")
        fallback_price = fallback_stop_after_invalid_be(direction, last) if fallback_enabled else Decimal("0")
        fallback_ok, fallback_reason = valid_stop_side(direction, fallback_price, last)

        if fallback_enabled and fallback_price > 0 and fallback_ok:
            # Safer sequence: place tighter fallback SL first, then cancel old SL.
            # This avoids leaving position naked if new SL placement fails.
            place_res = place_sl(symbol, direction, qty_now, fallback_price, target_stage + "_FALLBACK")
            place_ok = bool((place_res.get("result") or {}).get("ok"))
            cancel_res = cancel_stop_orders(symbol, stops) if place_ok else []

            sym_state.update({
                "stage": target_stage + "_FALLBACK" if place_ok else stage,
                "last_check_utc": m.utc_now_iso(),
                "qty_now": plain(qty_now),
                "managed_sl": plain(fallback_price) if place_ok else sym_state.get("managed_sl"),
                "last_action": "MOVE_SL_FALLBACK",
                "last_action_ok": place_ok,
            })
            state["symbols"][symbol] = sym_state

            ev = dict(base)
            ev.update({
                "decision": "FALLBACK_MOVE_SL_PLACED" if place_ok else "FALLBACK_MOVE_SL_FAILED",
                "target_stage": target_stage,
                "target_sl": plain(target_price),
                "fallback_sl": plain(fallback_price),
                "last_price": plain(last),
                "reason": side_reason,
                "fallback_reason": fallback_reason,
                "place_sl": place_res,
                "cancel_stop_results": cancel_res,
            })
            append_log(ev)
            return ev

        ev = dict(base)
        ev.update({
            "decision": "SKIP_MOVE_SL_WOULD_TRIGGER",
            "target_stage": target_stage,
            "target_sl": plain(target_price),
            "fallback_sl": plain(fallback_price),
            "last_price": plain(last),
            "reason": side_reason,
            "fallback_reason": fallback_reason,
        })
        append_log(ev)
        return ev

    cancel_res = cancel_stop_orders(symbol, stops)
    time.sleep(0.25)
    place_res = place_sl(symbol, direction, qty_now, target_price, target_stage)

    place_ok = bool((place_res.get("result") or {}).get("ok"))
    sym_state.update({
        "stage": target_stage if place_ok else stage,
        "last_check_utc": m.utc_now_iso(),
        "qty_now": plain(qty_now),
        "managed_sl": plain(target_price) if place_ok else sym_state.get("managed_sl"),
        "last_action": "MOVE_SL",
        "last_action_ok": place_ok,
    })
    state["symbols"][symbol] = sym_state

    ev = dict(base)
    ev.update({
        "decision": "MOVE_SL_PLACED" if place_ok else "MOVE_SL_FAILED",
        "target_stage": target_stage,
        "target_sl": plain(target_price),
        "last_price": plain(last),
        "cancel_stop_results": cancel_res,
        "place_sl": place_res,
    })
    append_log(ev)
    return ev

def cleanup_closed_symbols(state):
    events = []
    live_syms = set()
    for p in all_positions():
        live_syms.add(str(p.get("symbol") or "").upper())

    for symbol, st in list((state.get("symbols") or {}).items()):
        if symbol in live_syms:
            continue
        algo = open_algo(symbol)
        cancel_res = cancel_all_algo(symbol, algo) if algo else []
        st["stage"] = "CLOSED"
        st["closed_at_utc"] = m.utc_now_iso()
        st["cleanup_count"] = len(cancel_res)
        state["symbols"][symbol] = st

        # === SKIP_EMPTY_TP_CLEANUP_LOG_20260627 ===
        # Skip useless cleanup rows when there is no open algo and no cancel result.
        # This prevents NO_PREFIX spam from polluting tp_lifecycle_events.jsonl.
        if len(algo) <= 0 and not cancel_res:
            continue

        ev = {
            "event_at_utc": m.utc_now_iso(),
            "action": "TP_LIFECYCLE_POSITION_CLOSED_CLEANUP",
            "symbol": symbol,
            "open_algo_before": len(algo),
            "cancel_results": cancel_res,
        }
        append_log(ev)
        events.append(ev)
    return events

def run_once():
    state = load_state()
    state.setdefault("symbols", {})
    events = []

    for pos in all_positions():
        ev = handle_symbol(pos, state)
        if ev:
            events.append(ev)

    events.extend(cleanup_closed_symbols(state))
    save_state(state)
    return {"ok": True, "events": events, "event_count": len(events)}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--loop", action="store_true")
    ap.add_argument("--sleep", type=float, default=20.0)
    args = ap.parse_args()

    if args.loop:
        while True:
            try:
                print(json.dumps(run_once(), separators=(",",":")))
            except Exception as e:
                ev = {"event_at_utc": m.utc_now_iso(), "action": "TP_LIFECYCLE_CRASH", "error": str(e)}
                append_log(ev)
                print(json.dumps(ev, separators=(",",":")))
            time.sleep(max(5.0, args.sleep))
    else:
        print(json.dumps(run_once(), indent=2))

if __name__ == "__main__":
    main()
