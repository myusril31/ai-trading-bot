#!/usr/bin/env python3
import sys, json, math, time, csv, os
from pathlib import Path
from datetime import datetime, timezone, timedelta
from decimal import Decimal

sys.path.insert(0, "/app")
import app.main as m

ROOT = Path("/app")
LOGS = ROOT / "logs"
REPORTS = ROOT / "reports"
REPORTS.mkdir(exist_ok=True)

EXEC = LOGS / "execution_events.jsonl"
WIB = timezone(timedelta(hours=7))

def _audit_since_ms_from_env():
    raw = os.getenv("AUDIT_SINCE_WIB", "").strip()
    if not raw:
        return None

    s = raw.replace(" WIB", "").replace("T", " ").strip()

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(s, fmt).replace(tzinfo=WIB)
            return int(dt.timestamp() * 1000)
        except Exception:
            pass

    raise SystemExit(f"BAD_AUDIT_SINCE_WIB={raw}; use: YYYY-MM-DD HH:MM:SS")

SINCE_MS = _audit_since_ms_from_env()


HOURS = 96
MAX_AFTER_SEC = 1800
MIN_BEFORE_SEC = 60

def now_tag():
    return datetime.now(WIB).strftime("%Y%m%d_%H%M%S")

def D(x):
    try:
        if x is None or x == "":
            return None
        v = float(str(x))
        if math.isfinite(v):
            return v
    except Exception:
        pass
    return None

def walk(x):
    if isinstance(x, dict):
        yield x
        for v in x.values():
            yield from walk(v)
    elif isinstance(x, list):
        for v in x:
            yield from walk(v)

def pick_num(obj, keys):
    keys = set(keys)
    for d in walk(obj):
        for k, v in d.items():
            if k in keys:
                n = D(v)
                if n and n > 0:
                    return n
    return None

def pick_str(obj, keys):
    keys = set(keys)
    for d in walk(obj):
        for k, v in d.items():
            if k in keys and v not in (None, ""):
                return str(v)
    return ""

def parse_ms(row):
    for k in ("event_at_ms","created_ms","started_ms","time","transactTime","updateTime"):
        n = D(row.get(k))
        if n:
            return int(n)
    for k in ("event_at_wib","created_at_wib","created_at_utc","event_at_utc"):
        s = row.get(k)
        if not s:
            continue
        try:
            ss = str(s).replace(" WIB","").replace("Z","+00:00")
            dt = datetime.fromisoformat(ss)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=WIB)
            return int(dt.timestamp() * 1000)
        except Exception:
            pass
    return None

def find_plan(row):
    for d in walk(row):
        p = d.get("plan")
        if isinstance(p, dict):
            if pick_num(p, ("entry_mid","entry","entry_price")) and pick_num(p, ("tp1","raw_tp1")) and pick_num(p, ("sl","stop_loss","invalid")):
                return p
    return None

def direction(row, plan):
    raw = (
        pick_str(row, ("direction","dir"))
        or pick_str(plan, ("direction","dir"))
        or ""
    ).upper()
    if raw in ("LONG","BUY"):
        return "LONG"
    if raw in ("SHORT","SELL"):
        return "SHORT"
    return raw or "UNKNOWN"

def side_for_dir(d):
    return "BUY" if d == "LONG" else "SELL" if d == "SHORT" else ""

def symbol(row, plan):
    s = (pick_str(row, ("symbol","pair")) or pick_str(plan, ("symbol","pair"))).upper()
    return s.replace("BINANCE:","").replace(".P","")

def signal_key(row, plan):
    return pick_str(row, ("signal_key","signalKey")) or pick_str(plan, ("signal_key","signalKey"))

def load_plans():
    cutoff = (SINCE_MS or int((time.time() - HOURS * 3600) * 1000))
    out = []
    if not EXEC.exists():
        return out

    for line in EXEC.read_text(errors="ignore").splitlines():
        try:
            r = json.loads(line)
        except Exception:
            continue
        ms = parse_ms(r)
        if ms and ms < cutoff:
            continue
        p = find_plan(r)
        if not p:
            continue

        sym = symbol(r, p)
        direc = direction(r, p)
        if not sym or direc not in ("LONG","SHORT"):
            continue

        out.append({
            "signal_key": signal_key(r, p),
            "symbol": sym,
            "direction": direc,
            "side": side_for_dir(direc),
            "plan_ms": ms,
            "plan_entry": pick_num(p, ("entry_mid","entry","entry_price")),
            "tp1": pick_num(p, ("tp1","raw_tp1")),
            "sl": pick_num(p, ("sl","stop_loss","invalid")),
        })

    # dedupe kasar
    seen = set()
    dedup = []
    for p in out:
        k = (p["signal_key"], p["symbol"], p["direction"], p["plan_entry"], p["tp1"], p["sl"], p["plan_ms"])
        if k in seen:
            continue
        seen.add(k)
        dedup.append(p)
    return dedup

def signed(method, path, params):
    res = m.live_signed_request(method, path, params)
    body = res.get("body") if isinstance(res, dict) else res
    return body

def query_orders(symbols, start_ms):
    all_orders = []
    errors = []
    for sym in symbols:
        try:
            body = signed("GET", "/fapi/v1/allOrders", {
                "symbol": sym,
                "startTime": start_ms,
                "limit": 1000,
            })
            if isinstance(body, dict) and body.get("code"):
                errors.append({"symbol": sym, "error": body})
                continue
            if isinstance(body, list):
                for o in body:
                    o["_symbol_query"] = sym
                    all_orders.append(o)
        except Exception as e:
            errors.append({"symbol": sym, "error": f"{type(e).__name__}:{e}"})
        time.sleep(0.15)
    return all_orders, errors

def order_fill_price(o):
    avg = D(o.get("avgPrice"))
    if avg and avg > 0:
        return avg, "avgPrice"

    cq = D(o.get("cumQuote") or o.get("cummulativeQuoteQty"))
    eq = D(o.get("executedQty"))
    if cq and eq and eq > 0:
        return cq / eq, "cumQuote/executedQty"

    px = D(o.get("price"))
    if px and px > 0:
        return px, "price"

    return None, ""

def is_entry_order(o, expected_side):
    status = str(o.get("status") or "").upper()
    typ = str(o.get("type") or "").upper()
    side = str(o.get("side") or "").upper()
    reduce_only = str(o.get("reduceOnly") or "").lower() == "true"
    cid = str(o.get("clientOrderId") or "")

    if status not in ("FILLED", "PARTIALLY_FILLED"):
        return False
    if typ != "MARKET":
        return False
    if side != expected_side:
        return False
    if reduce_only:
        return False
    if "PROTFAIL" in cid or "TP" in cid or "SL" in cid:
        return False
    return True

def adverse_slip(direction, entry, fill):
    if not entry or not fill:
        return None
    if direction == "LONG":
        return (fill - entry) / entry * 100
    if direction == "SHORT":
        return (entry - fill) / entry * 100
    return abs(fill - entry) / entry * 100

def rr_from_fill(direction, fill, tp1, sl):
    if not fill or not tp1 or not sl:
        return None
    if direction == "LONG":
        risk = fill - sl
        reward = tp1 - fill
    else:
        risk = sl - fill
        reward = fill - tp1
    if risk <= 0:
        return None
    return reward / risk

def main():
    plans = load_plans()
    if not plans:
        print("NO_PLANS_FOUND")
        return

    start_ms = (SINCE_MS or int((time.time() - HOURS * 3600) * 1000))
    symbols = sorted(set(p["symbol"] for p in plans))
    orders, errors = query_orders(symbols, start_ms)

    filled_orders = []
    for o in orders:
        px, px_src = order_fill_price(o)
        if px:
            o["_fill_price"] = px
            o["_fill_src"] = px_src
            o["_time_ms"] = int(o.get("time") or o.get("updateTime") or 0)
            filled_orders.append(o)

    used = set()
    rows = []

    for p in plans:
        best = None
        best_score = None
        for idx, o in enumerate(filled_orders):
            if idx in used:
                continue
            if str(o.get("symbol") or "") != p["symbol"]:
                continue
            if not is_entry_order(o, p["side"]):
                continue

            ot = int(o.get("_time_ms") or 0)
            if not p.get("plan_ms") or not ot:
                continue

            dt_sec = (ot - p["plan_ms"]) / 1000
            if dt_sec < -MIN_BEFORE_SEC or dt_sec > MAX_AFTER_SEC:
                continue

            score = abs(dt_sec)
            cid = str(o.get("clientOrderId") or "")
            if p.get("signal_key") and p["signal_key"] in cid:
                score -= 999999

            if best is None or score < best_score:
                best = (idx, o, dt_sec)
                best_score = score

        row = dict(p)
        if best:
            idx, o, dt_sec = best
            used.add(idx)
            fill = o["_fill_price"]
            adv = adverse_slip(p["direction"], p["plan_entry"], fill)
            rr = rr_from_fill(p["direction"], fill, p["tp1"], p["sl"])
            flags = []
            if adv is not None and adv > 0.30:
                flags.append("ADVERSE_GT_0.30")
            if rr is not None and rr < 0.70:
                flags.append("RR_LT_0.70")
            if rr is None:
                flags.append("RR_NA_OR_BAD_GEOMETRY")

            row.update({
                "matched": True,
                "matched_by": "client_id_or_nearest_time",
                "order_id": o.get("orderId"),
                "client_order_id": o.get("clientOrderId"),
                "order_time_ms": o.get("_time_ms"),
                "dt_plan_to_order_sec": round(dt_sec, 3),
                "fill_price": fill,
                "fill_source": o["_fill_src"],
                "executed_qty": o.get("executedQty"),
                "cum_quote": o.get("cumQuote") or o.get("cummulativeQuoteQty"),
                "status": o.get("status"),
                "type": o.get("type"),
                "abs_slippage_pct": round(abs(fill - p["plan_entry"]) / p["plan_entry"] * 100, 5) if p["plan_entry"] else None,
                "adverse_slippage_pct": round(adv, 5) if adv is not None else None,
                "rr_to_tp1_from_fill": round(rr, 5) if rr is not None else None,
                "flag": "|".join(flags) if flags else "OK",
            })
        else:
            row.update({
                "matched": False,
                "matched_by": "",
                "order_id": "",
                "client_order_id": "",
                "order_time_ms": "",
                "dt_plan_to_order_sec": "",
                "fill_price": "",
                "fill_source": "",
                "executed_qty": "",
                "cum_quote": "",
                "status": "",
                "type": "",
                "abs_slippage_pct": "",
                "adverse_slippage_pct": "",
                "rr_to_tp1_from_fill": "",
                "flag": "NO_EXCHANGE_ORDER_MATCH",
            })

        rows.append(row)

    tag = now_tag()
    csv_path = REPORTS / f"execution_plan_exchange_audit_{tag}.csv"

    fields = [
        "signal_key","symbol","direction","side","plan_entry","fill_price","tp1","sl",
        "abs_slippage_pct","adverse_slippage_pct","rr_to_tp1_from_fill",
        "dt_plan_to_order_sec","order_id","client_order_id","executed_qty",
        "cum_quote","status","type","matched","matched_by","flag"
    ]

    with csv_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k:r.get(k,"") for k in fields})

    matched = [r for r in rows if r.get("matched")]
    adv = [r["adverse_slippage_pct"] for r in matched if isinstance(r.get("adverse_slippage_pct"), (int,float))]
    rr = [r["rr_to_tp1_from_fill"] for r in matched if isinstance(r.get("rr_to_tp1_from_fill"), (int,float))]
    flags = [r for r in rows if r.get("flag") != "OK"]

    def pctl(vals, q):
        vals = sorted(vals)
        if not vals:
            return None
        pos = (len(vals)-1)*q
        lo = int(pos)
        hi = min(lo+1, len(vals)-1)
        frac = pos-lo
        return vals[lo]*(1-frac)+vals[hi]*frac

    print("=== EXECUTION VS PLAN EXCHANGE AUDIT ===")
    print(f"hours={HOURS}")
    print(f"since_wib={os.getenv('AUDIT_SINCE_WIB', '') or 'auto_hours_window'}")
    print(f"plans_found={len(plans)}")
    print(f"symbols_queried={len(symbols)}")
    print(f"orders_seen={len(orders)}")
    print(f"filled_orders_seen={len(filled_orders)}")
    print(f"matched={len(matched)}")
    print(f"unmatched={len(rows)-len(matched)}")
    print(f"errors={len(errors)}")
    print("")
    print("Slippage adverse %:")
    print(f"- p50={pctl(adv,0.50)}")
    print(f"- p90={pctl(adv,0.90)}")
    print(f"- max={max(adv) if adv else None}")
    print(f"- adverse_gt_0.30={len([x for x in adv if x > 0.30])}")
    print("")
    print("RR to TP1 from fill:")
    print(f"- p50={pctl(rr,0.50)}")
    print(f"- min={min(rr) if rr else None}")
    print(f"- rr_lt_0.70={len([x for x in rr if x < 0.70])}")
    print("")
    print("Last matched rows:")
    for r in matched[-20:]:
        print(f"- {r['symbol']} {r['direction']} plan={r['plan_entry']} fill={r['fill_price']} adv={r['adverse_slippage_pct']} rr={r['rr_to_tp1_from_fill']} dt={r['dt_plan_to_order_sec']}s flag={r['flag']}")
    print("")
    print("Sample unmatched:")
    for r in [x for x in rows if not x.get("matched")][-15:]:
        print(f"- {r['symbol']} {r['direction']} plan={r['plan_entry']} flag={r['flag']}")
    print("")
    if errors:
        print("Errors:")
        for e in errors[:8]:
            print(json.dumps(e, default=str)[:500])
    print(f"CSV={csv_path}")

if __name__ == "__main__":
    main()
