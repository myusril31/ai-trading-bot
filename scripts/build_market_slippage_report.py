import sys, json, csv, argparse
sys.path.insert(0, "/app")

from pathlib import Path
from decimal import Decimal, ROUND_HALF_UP
import app.main as m

ROOT = Path("/app")
EXEC_LOG = ROOT / "logs" / "execution_events.jsonl"
REPORT_DIR = ROOT / "reports"

def D(x, default="0"):
    try:
        if x is None or x == "":
            return Decimal(default)
        return Decimal(str(x))
    except Exception:
        return Decimal(default)

def plain(x, n=8):
    q = Decimal("1." + ("0" * n))
    return str(D(x).quantize(q, rounding=ROUND_HALF_UP)).rstrip("0").rstrip(".")

def pct(x):
    return plain(x, 6)

def norm_dir(x):
    s = str(x or "").upper()
    if s.startswith("LONG"):
        return "LONG"
    if s.startswith("SHORT"):
        return "SHORT"
    if s == "BUY":
        return "LONG"
    if s == "SELL":
        return "SHORT"
    return s

def row_plan(row):
    plan = row.get("plan") if isinstance(row.get("plan"), dict) else {}
    payload = plan.get("payload") if isinstance(plan.get("payload"), dict) else {}
    direction = norm_dir(plan.get("direction") or payload.get("direction") or row.get("direction"))

    entry = (
        plan.get("entry_mid")
        or plan.get("entry")
        or payload.get("entry_mid")
        or payload.get("entry")
    )

    return plan, direction, D(entry)

def actual_entry_from_log(row):
    fill = row.get("entry_fill_result") if isinstance(row.get("entry_fill_result"), dict) else {}
    plan = row.get("plan") if isinstance(row.get("plan"), dict) else {}
    entry_res = row.get("entry_result") if isinstance(row.get("entry_result"), dict) else {}
    body = entry_res.get("body") if isinstance(entry_res.get("body"), dict) else {}

    candidates = [
        fill.get("entryPrice"),
        plan.get("actual_entry_price"),
        body.get("avgPrice"),
    ]
    for c in candidates:
        v = D(c)
        if v > 0:
            return v, "log"
    return Decimal("0"), None

def fetch_entry_trades(symbol, order_id):
    if not order_id:
        return {"ok": False, "reason": "no_order_id", "trades": []}
    try:
        oid = int(order_id)
    except Exception:
        return {"ok": False, "reason": "bad_order_id", "trades": []}

    res = m.live_signed_request("GET", "/fapi/v1/userTrades", {
        "symbol": symbol,
        "orderId": oid,
    })
    body = res.get("body")
    trades = body if isinstance(body, list) else []
    return {
        "ok": bool(res.get("ok")),
        "reason": res.get("reason"),
        "trades": trades,
        "raw": res,
    }

def weighted_entry_from_trades(trades):
    qty_sum = Decimal("0")
    px_qty = Decimal("0")
    fee_sum = Decimal("0")
    realized = Decimal("0")

    for t in trades:
        qty = D(t.get("qty"))
        price = D(t.get("price"))
        fee = D(t.get("commission"))
        rpnl = D(t.get("realizedPnl"))
        if qty > 0 and price > 0:
            qty_sum += qty
            px_qty += price * qty
        fee_sum += fee
        realized += rpnl

    if qty_sum <= 0:
        return Decimal("0"), qty_sum, fee_sum, realized

    return px_qty / qty_sum, qty_sum, fee_sum, realized

def slippage(direction, plan_entry, actual_entry):
    if plan_entry <= 0 or actual_entry <= 0:
        return Decimal("0"), Decimal("0"), Decimal("0")

    # signed positive = bad/adverse, negative = favorable
    if direction == "LONG":
        signed = (actual_entry - plan_entry) / plan_entry * Decimal("100")
    elif direction == "SHORT":
        signed = (plan_entry - actual_entry) / plan_entry * Decimal("100")
    else:
        signed = Decimal("0")

    adverse = signed if signed > 0 else Decimal("0")
    favorable = abs(signed) if signed < 0 else Decimal("0")
    return signed, adverse, favorable

def percentile(vals, q):
    vals = sorted([D(x) for x in vals])
    if not vals:
        return Decimal("0")
    if len(vals) == 1:
        return vals[0]
    pos = (Decimal(len(vals) - 1) * Decimal(str(q)))
    lo = int(pos)
    hi = min(lo + 1, len(vals) - 1)
    frac = pos - Decimal(lo)
    return vals[lo] * (Decimal("1") - frac) + vals[hi] * frac

def make_stats(rows, key):
    groups = {}
    for r in rows:
        k = r[key] if key else "ALL"
        groups.setdefault(k, []).append(r)

    out = []
    for k, items in sorted(groups.items()):
        adv = [D(x["adverse_slip_pct"]) for x in items]
        fav = [D(x["favorable_slip_pct"]) for x in items]
        signed = [D(x["signed_slip_pct"]) for x in items]

        n = len(items)
        p50 = percentile(adv, "0.50")
        p75 = percentile(adv, "0.75")
        p90 = percentile(adv, "0.90")
        p95 = percentile(adv, "0.95")
        mx = max(adv) if adv else Decimal("0")
        mean_adv = sum(adv, Decimal("0")) / Decimal(n) if n else Decimal("0")
        mean_fav = sum(fav, Decimal("0")) / Decimal(n) if n else Decimal("0")
        mean_signed = sum(signed, Decimal("0")) / Decimal(n) if n else Decimal("0")

        # buffer buat allowed market fill range. Conservative pakai p90 * 1.2.
        raw_buf = p90 * Decimal("1.20")
        min_buf = D("0.08")     # 0.08% floor
        max_buf = D("0.60")     # 0.60% cap sementara
        rec_buf = max(min_buf, min(max_buf, raw_buf))

        out.append({
            "group": k,
            "n": n,
            "mean_signed_slip_pct": pct(mean_signed),
            "mean_adverse_slip_pct": pct(mean_adv),
            "mean_favorable_slip_pct": pct(mean_fav),
            "p50_adverse_pct": pct(p50),
            "p75_adverse_pct": pct(p75),
            "p90_adverse_pct": pct(p90),
            "p95_adverse_pct": pct(p95),
            "max_adverse_pct": pct(mx),
            "recommended_entry_buffer_pct": pct(rec_buf),
            "sample_quality": "OK" if n >= 5 else "LOW_SAMPLE",
        })
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--use-binance", action="store_true", default=True)
    args = ap.parse_args()

    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    rows = []
    if not EXEC_LOG.exists():
        raise SystemExit(f"missing {EXEC_LOG}")

    for line in EXEC_LOG.read_text(errors="ignore").splitlines():
        try:
            row = json.loads(line)
        except Exception:
            continue

        if row.get("action") != "LIVE_SMALL_CAPITAL_EXECUTE":
            continue

        symbol = str(row.get("symbol") or "").upper()
        if not symbol:
            continue

        plan, direction, plan_entry = row_plan(row)
        if plan_entry <= 0 or direction not in ("LONG", "SHORT"):
            continue

        entry_res = row.get("entry_result") if isinstance(row.get("entry_result"), dict) else {}
        entry_body = entry_res.get("body") if isinstance(entry_res.get("body"), dict) else {}
        order_id = entry_body.get("orderId")
        client_id = entry_body.get("clientOrderId")

        actual_entry, source = actual_entry_from_log(row)
        qty = Decimal("0")
        fee = Decimal("0")
        realized = Decimal("0")
        trades_count = 0

        if actual_entry <= 0 and args.use_binance and order_id:
            tr = fetch_entry_trades(symbol, order_id)
            trades = tr.get("trades") or []
            trades_count = len(trades)
            actual_entry, qty, fee, realized = weighted_entry_from_trades(trades)
            if actual_entry > 0:
                source = "binance_userTrades"

        if actual_entry <= 0:
            continue

        signed, adverse, favorable = slippage(direction, plan_entry, actual_entry)

        rows.append({
            "event_at_wib": row.get("event_at_wib") or "",
            "symbol": symbol,
            "signal_key": row.get("signal_key") or plan.get("signal_key") or "",
            "direction": direction,
            "decision": row.get("decision") or "",
            "plan_entry": plain(plan_entry, 8),
            "actual_entry": plain(actual_entry, 8),
            "signed_slip_pct": pct(signed),
            "adverse_slip_pct": pct(adverse),
            "favorable_slip_pct": pct(favorable),
            "entry_order_id": str(order_id or ""),
            "entry_client_id": str(client_id or ""),
            "actual_source": source or "",
            "entry_qty_from_trades": plain(qty, 8),
            "entry_fee_from_trades": plain(fee, 8),
            "entry_realized_pnl_from_trades": plain(realized, 8),
            "trades_count": trades_count,
        })

    report_csv = REPORT_DIR / "slippage_report.csv"
    by_symbol_csv = REPORT_DIR / "slippage_by_symbol.csv"
    rec_json = REPORT_DIR / "slippage_range_recommendations.json"

    if rows:
        with report_csv.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)

    stats_symbol = make_stats(rows, "symbol")
    stats_all = make_stats(rows, None)

    if stats_symbol:
        with by_symbol_csv.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(stats_symbol[0].keys()))
            w.writeheader()
            w.writerows(stats_symbol)

    recommendations = {
        "generated_at_utc": m.utc_now_iso(),
        "logic": {
            "signed_slip_pct": "positive = adverse/bad fill, negative = favorable fill",
            "long_adverse": "actual_entry > plan_entry",
            "short_adverse": "actual_entry < plan_entry",
            "entry_range_rule": {
                "LONG": "valid actual entry <= plan_entry * (1 + buffer_pct/100)",
                "SHORT": "valid actual entry >= plan_entry * (1 - buffer_pct/100)"
            },
            "recommended_buffer": "max(0.08%, min(0.60%, p90_adverse_pct * 1.20))"
        },
        "all": stats_all[0] if stats_all else {},
        "by_symbol": stats_symbol,
    }
    rec_json.write_text(json.dumps(recommendations, indent=2))

    print(json.dumps({
        "ok": True,
        "rows": len(rows),
        "files": {
            "report_csv": str(report_csv),
            "by_symbol_csv": str(by_symbol_csv),
            "recommendations_json": str(rec_json),
        },
        "summary_all": stats_all[0] if stats_all else {},
        "top_symbols": stats_symbol[:10],
    }, indent=2))

if __name__ == "__main__":
    main()
