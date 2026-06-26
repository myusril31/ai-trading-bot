#!/usr/bin/env python3
import argparse, csv, json, math, re, time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from statistics import median

ROOT = Path(".")
LOGS = ROOT / "logs"
REPORTS = ROOT / "reports"
REPORTS.mkdir(exist_ok=True)
WIB = timezone(timedelta(hours=7))

CANDIDATE_LOGS = [
    LOGS / "execution_events.jsonl",
    LOGS / "night_accuracy_fix_events.jsonl",
    LOGS / "latency_audit.jsonl",
    LOGS / "repair_missing_tp_now.jsonl",
]

def now_tag():
    return datetime.now(WIB).strftime("%Y%m%d_%H%M%S")

def parse_dt(x):
    if not x:
        return None
    s = str(x).strip().replace(" WIB", "").replace("Z", "+00:00")
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=WIB)
            return dt
        except Exception:
            pass
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=WIB)
        return dt
    except Exception:
        return None

def event_ms(r):
    for k in ("event_at_ms","created_ms","started_ms","time","transactTime","updateTime"):
        try:
            v = r.get(k)
            if v is not None and str(v).strip():
                return int(float(v))
        except Exception:
            pass
    for k in ("event_at_wib","created_at_wib","created_at_utc","event_at_utc","checked_at_utc"):
        dt = parse_dt(r.get(k))
        if dt:
            return int(dt.timestamp() * 1000)
    return None

def D(x):
    try:
        if x is None:
            return None
        s = str(x).strip()
        if not s or s.lower() in ("none","nan","null"):
            return None
        v = float(s)
        if not math.isfinite(v):
            return None
        return v
    except Exception:
        return None

def walk(obj):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from walk(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from walk(v)

def deep_first(obj, keys):
    keys = set(keys)
    for d in walk(obj):
        for k, v in d.items():
            if k in keys:
                return v
    return None

def deep_nums(obj, keys):
    out = []
    keys = set(keys)
    for d in walk(obj):
        for k, v in d.items():
            if k in keys:
                n = D(v)
                if n is not None and n > 0:
                    out.append((k, n))
    return out

def pick_num(obj, keys):
    vals = deep_nums(obj, keys)
    return vals[0][1] if vals else None

def deep_str(obj, keys):
    v = deep_first(obj, keys)
    return str(v).strip() if v is not None else ""

def find_plan(row):
    # direct plan
    for k in ("plan", "locked_plan", "trade_plan"):
        if isinstance(row.get(k), dict):
            p = row[k]
            if pick_num(p, ("entry_mid","entry","entry_price")) and pick_num(p, ("sl","stop_loss","invalid")) and pick_num(p, ("tp1","raw_tp1")):
                return p

    # nested plan
    for d in walk(row):
        for k in ("plan", "locked_plan", "trade_plan"):
            if isinstance(d.get(k), dict):
                p = d[k]
                if pick_num(p, ("entry_mid","entry","entry_price")) and pick_num(p, ("sl","stop_loss","invalid")) and pick_num(p, ("tp1","raw_tp1")):
                    return p

    return None

def direction_from(row, plan):
    raw = (
        deep_str(row, ("direction","dir","side_direction"))
        or deep_str(plan or {}, ("direction","dir","side_direction"))
        or deep_str(row, ("entry_side","side"))
    ).upper()

    if raw in ("BUY","LONG"):
        return "LONG"
    if raw in ("SELL","SHORT"):
        return "SHORT"
    if raw.startswith("LONG"):
        return "LONG"
    if raw.startswith("SHORT"):
        return "SHORT"
    return raw or "UNKNOWN"

def symbol_from(row, plan):
    s = (
        deep_str(row, ("symbol","pair"))
        or deep_str(plan or {}, ("symbol","pair"))
    ).upper()
    return s.replace("BINANCE:", "").replace(".P", "")

def signal_key_from(row, plan):
    return (
        deep_str(row, ("signal_key","signalKey","client_signal_key"))
        or deep_str(plan or {}, ("signal_key","signalKey"))
        or ""
    )

def extract_fill_price(row):
    # Prefer explicit fill avg fields.
    for keys in [
        ("fill_price","filled_price","avg_fill_price","avgFillPrice","entry_fill_price"),
        ("avgPrice","averagePrice"),
    ]:
        v = pick_num(row, keys)
        if v and v > 0:
            return v, keys[0]

    # Binance order sometimes: cumQuote / executedQty.
    cq = pick_num(row, ("cumQuote","cummulativeQuoteQty"))
    eq = pick_num(row, ("executedQty","origQty","quantity"))
    if cq and eq and eq > 0:
        return cq / eq, "cumQuote/executedQty"

    # Last fallback: price, but avoid zero.
    v = pick_num(row, ("price","entry_price_executed"))
    if v and v > 0:
        return v, "price"

    return None, ""

def is_entry_fill_row(row):
    txt = json.dumps(row, default=str).upper()
    action = str(row.get("action") or row.get("event_type") or "").upper()
    status = deep_str(row, ("status","orderStatus")).upper()
    typ = deep_str(row, ("type","orderType")).upper()

    if "PROTECTION" in action or "ALGOORDER" in txt or "TAKE_PROFIT" in txt or "STOP_MARKET" in txt:
        return False

    if "ENTRY" in action and ("FILLED" in action or status == "FILLED"):
        return True

    if typ == "MARKET" and status in ("FILLED","PARTIALLY_FILLED"):
        return True

    if "/FAPI/V1/ORDER" in txt and status in ("FILLED","PARTIALLY_FILLED"):
        return True

    return False

def load_rows(hours):
    cutoff = int((time.time() - hours * 3600) * 1000)
    rows = []
    for path in CANDIDATE_LOGS:
        if not path.exists():
            continue
        with path.open(errors="ignore") as f:
            for line in f:
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                ms = event_ms(r)
                if ms is not None and ms < cutoff:
                    continue
                r["_source"] = str(path)
                r["_event_ms"] = ms
                rows.append(r)
    rows.sort(key=lambda x: x.get("_event_ms") or 0)
    return rows

def pct(vals, q):
    vals = sorted([v for v in vals if v is not None])
    if not vals:
        return None
    if len(vals) == 1:
        return vals[0]
    pos = (len(vals)-1) * q
    lo = int(math.floor(pos))
    hi = min(lo+1, len(vals)-1)
    frac = pos - lo
    return vals[lo]*(1-frac) + vals[hi]*frac

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=72)
    ap.add_argument("--last", type=int, default=30)
    args = ap.parse_args()

    rows = load_rows(args.hours)

    plans = {}
    fills = {}

    for r in rows:
        p = find_plan(r)
        if p:
            key = signal_key_from(r, p)
            sym = symbol_from(r, p)
            direction = direction_from(r, p)
            if not key:
                # fallback key biar tetap kebaca
                key = f"{sym}|{direction}|{pick_num(p, ('entry_mid','entry','entry_price'))}|{pick_num(p, ('tp1','raw_tp1'))}|{pick_num(p, ('sl','stop_loss','invalid'))}"

            if key not in plans:
                plans[key] = {
                    "signal_key": key,
                    "symbol": sym,
                    "direction": direction,
                    "plan_entry": pick_num(p, ("entry_mid","entry","entry_price")),
                    "tp1": pick_num(p, ("tp1","raw_tp1")),
                    "sl": pick_num(p, ("sl","stop_loss","invalid")),
                    "plan_ms": r.get("_event_ms"),
                    "plan_source": r.get("_source"),
                }

        key = signal_key_from(r, None)
        fill_price, fill_source = extract_fill_price(r)

        if key and fill_price and is_entry_fill_row(r):
            if key not in fills:
                fills[key] = {
                    "fill_price": fill_price,
                    "fill_source": fill_source,
                    "fill_ms": r.get("_event_ms"),
                    "fill_row_source": r.get("_source"),
                    "order_id": deep_str(r, ("orderId","order_id")),
                    "status": deep_str(r, ("status","orderStatus")),
                }

    audit = []
    for key, p in plans.items():
        f = fills.get(key)
        fill = f.get("fill_price") if f else None
        entry = p.get("plan_entry")
        tp1 = p.get("tp1")
        sl = p.get("sl")
        direction = p.get("direction")

        row = dict(p)
        row.update({
            "has_fill": bool(f),
            "fill_price": fill,
            "fill_source": f.get("fill_source") if f else "",
            "order_id": f.get("order_id") if f else "",
            "status": f.get("status") if f else "",
            "plan_to_fill_sec": round(((f.get("fill_ms") or 0) - (p.get("plan_ms") or 0))/1000, 3) if f and p.get("plan_ms") and f.get("fill_ms") else "",
            "abs_slippage_pct": "",
            "adverse_slippage_pct": "",
            "rr_to_tp1_from_fill": "",
            "flag": "",
        })

        flags = []

        if fill and entry and entry > 0:
            abs_slip = abs(fill - entry) / entry * 100
            if direction == "LONG":
                adverse = (fill - entry) / entry * 100
            elif direction == "SHORT":
                adverse = (entry - fill) / entry * 100
            else:
                adverse = abs_slip

            row["abs_slippage_pct"] = round(abs_slip, 5)
            row["adverse_slippage_pct"] = round(adverse, 5)

            if adverse > 0.30:
                flags.append("ADVERSE_GT_0.30")

        if fill and tp1 and sl:
            if direction == "LONG":
                risk = fill - sl
                reward = tp1 - fill
            elif direction == "SHORT":
                risk = sl - fill
                reward = fill - tp1
            else:
                risk = reward = None

            if risk and reward and risk > 0:
                rr = reward / risk
                row["rr_to_tp1_from_fill"] = round(rr, 5)
                if rr < 0.70:
                    flags.append("RR_LT_0.70")
            else:
                flags.append("BAD_RR_GEOMETRY_AT_FILL")

        if not f:
            flags.append("FILL_NOT_FOUND_IN_LOG")

        if not entry or not tp1 or not sl:
            flags.append("PLAN_LEVEL_MISSING")

        row["flag"] = "|".join(flags) if flags else "OK"
        audit.append(row)

    # Latency summary
    lat_rows = [r for r in rows if r.get("kind") == "BINANCE_SIGNED_REQUEST"]
    lat_by_path = {}
    for r in lat_rows:
        path = r.get("path") or ""
        lat_by_path.setdefault(path, []).append(D(r.get("duration_ms")))

    tag = now_tag()
    csv_path = REPORTS / f"execution_plan_accuracy_{tag}.csv"
    json_path = REPORTS / f"execution_plan_accuracy_{tag}.json"

    fields = [
        "signal_key","symbol","direction","plan_entry","fill_price","tp1","sl",
        "abs_slippage_pct","adverse_slippage_pct","rr_to_tp1_from_fill",
        "plan_to_fill_sec","has_fill","fill_source","order_id","status","flag",
        "plan_source","fill_row_source"
    ]

    with csv_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in audit:
            w.writerow({k:r.get(k,"") for k in fields})

    with json_path.open("w") as f:
        json.dump(audit, f, indent=2, default=str)

    with_fill = [r for r in audit if r.get("has_fill")]
    adverse = [r.get("adverse_slippage_pct") for r in with_fill if isinstance(r.get("adverse_slippage_pct"), (int,float))]
    rr = [r.get("rr_to_tp1_from_fill") for r in with_fill if isinstance(r.get("rr_to_tp1_from_fill"), (int,float))]
    bad_flags = [r for r in audit if r.get("flag") != "OK"]

    print("=== EXECUTION VS PLAN AUDIT ===")
    print(f"hours={args.hours}")
    print(f"raw_rows={len(rows)}")
    print(f"plans_found={len(plans)}")
    print(f"fills_matched={len(with_fill)}")
    print(f"missing_fill={len([r for r in audit if not r.get('has_fill')])}")
    print(f"flagged={len(bad_flags)}")
    print("")
    print("Slippage adverse %:")
    print(f"- p50={pct(adverse,0.50)}")
    print(f"- p90={pct(adverse,0.90)}")
    print(f"- max={max(adverse) if adverse else None}")
    print(f"- adverse_gt_0.30={len([x for x in adverse if x > 0.30])}")
    print("")
    print("RR to TP1 from fill:")
    print(f"- p50={pct(rr,0.50)}")
    print(f"- min={min(rr) if rr else None}")
    print(f"- rr_lt_0.70={len([x for x in rr if x < 0.70])}")
    print("")
    print("Latency audit:")
    for path, vals in sorted(lat_by_path.items()):
        vals = [v for v in vals if v is not None]
        if not vals:
            continue
        print(f"- {path}: n={len(vals)} p50={pct(vals,0.50)}ms p90={pct(vals,0.90)}ms max={max(vals)}ms")
    print("")
    print("Last rows:")
    for r in audit[-args.last:]:
        print(f"- {r.get('symbol')} {r.get('direction')} entry={r.get('plan_entry')} fill={r.get('fill_price')} adverse={r.get('adverse_slippage_pct')} rr={r.get('rr_to_tp1_from_fill')} flag={r.get('flag')}")
    print("")
    print(f"CSV={csv_path}")
    print(f"JSON={json_path}")

if __name__ == "__main__":
    main()
