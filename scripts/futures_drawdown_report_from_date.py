#!/usr/bin/env python3
import sys, json, time, math, argparse
from datetime import datetime, timezone, timedelta
from collections import defaultdict

sys.path.insert(0, "/app")
import app.main as m

WIB = timezone(timedelta(hours=7))

def ms(dt):
    return int(dt.timestamp() * 1000)

def wib(msv):
    return datetime.fromtimestamp(msv / 1000, timezone.utc).astimezone(WIB)

def signed(method, path, params=None):
    params = params or {}
    res = m.live_signed_request(method, path, params)
    body = res.get("body") if isinstance(res, dict) else res
    return body

def f(x, default=0.0):
    try:
        if x is None or x == "":
            return default
        v = float(str(x))
        return v if math.isfinite(v) else default
    except Exception:
        return default

def parse_start_date(s):
    # input contoh: 2026-06-01 atau 2026-06-01 00:00:00
    s = str(s).strip()
    if len(s) == 10:
        s += " 00:00:00"
    dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=WIB)
    return dt.astimezone(timezone.utc)

def get_account():
    body = signed("GET", "/fapi/v2/account", {})
    if not isinstance(body, dict):
        raise SystemExit(f"BAD_ACCOUNT_RESPONSE={body}")
    return body

def get_income(start_ms, end_ms):
    out = []
    types = ["REALIZED_PNL", "COMMISSION", "FUNDING_FEE"]
    for typ in types:
        cursor = start_ms
        while True:
            body = signed("GET", "/fapi/v1/income", {
                "incomeType": typ,
                "startTime": cursor,
                "endTime": end_ms,
                "limit": 1000,
            })
            if not isinstance(body, list):
                print(f"WARN income {typ} bad response: {body}")
                break
            if not body:
                break

            for r in body:
                r["_incomeType"] = typ
                out.append(r)

            if len(body) < 1000:
                break

            last_t = max(int(x.get("time", 0)) for x in body)
            if last_t <= cursor:
                break
            cursor = last_t + 1
            time.sleep(0.15)
    return out

def max_drawdown(points):
    peak = None
    peak_t = None
    max_dd = 0.0
    max_dd_pct = 0.0
    trough_t = None

    for t, eq in points:
        if peak is None or eq > peak:
            peak = eq
            peak_t = t

        dd = peak - eq
        dd_pct = (dd / peak * 100) if peak and peak > 0 else 0.0

        if dd > max_dd:
            max_dd = dd
            max_dd_pct = dd_pct
            trough_t = t

    return max_dd, max_dd_pct, peak_t, trough_t

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="WIB date, example: 2026-06-01")
    args = ap.parse_args()

    start = parse_start_date(args.start)
    now = datetime.now(timezone.utc)

    account = get_account()
    current_wallet = f(account.get("totalWalletBalance"))
    current_margin_balance = f(account.get("totalMarginBalance"))
    current_unrealized = f(account.get("totalUnrealizedProfit"))

    income = get_income(ms(start), ms(now))
    income.sort(key=lambda r: int(r.get("time", 0)))

    total_realized = 0.0
    total_commission = 0.0
    total_funding = 0.0
    total_net = 0.0

    by_day = defaultdict(lambda: {
        "REALIZED_PNL": 0.0,
        "COMMISSION": 0.0,
        "FUNDING_FEE": 0.0,
        "NET": 0.0,
        "events": 0,
    })

    for r in income:
        typ = r.get("incomeType") or r.get("_incomeType")
        amt = f(r.get("income"))
        t = int(r.get("time", 0))
        day = wib(t).strftime("%Y-%m-%d")

        by_day[day][typ] += amt
        by_day[day]["NET"] += amt
        by_day[day]["events"] += 1

        total_net += amt
        if typ == "REALIZED_PNL":
            total_realized += amt
        elif typ == "COMMISSION":
            total_commission += amt
        elif typ == "FUNDING_FEE":
            total_funding += amt

    # Approx starting wallet = current wallet - realized net since start.
    # Ini realized equity curve, bukan full historical mark-to-market.
    start_wallet = current_wallet - total_net

    curve = []
    eq = start_wallet
    curve.append((ms(start), eq))

    for r in income:
        eq += f(r.get("income"))
        curve.append((int(r.get("time", 0)), eq))

    curve.append((ms(now), current_wallet))

    dd_usdt, dd_pct_peak, peak_t, trough_t = max_drawdown(curve)
    dd_pct_start = (dd_usdt / start_wallet * 100) if start_wallet > 0 else 0.0

    worst_day = None
    best_day = None
    for d, v in by_day.items():
        if worst_day is None or v["NET"] < by_day[worst_day]["NET"]:
            worst_day = d
        if best_day is None or v["NET"] > by_day[best_day]["NET"]:
            best_day = d

    print("=== FUTURES DRAWDOWN REPORT FROM DATE ===")
    print(f"start_wib={start.astimezone(WIB).strftime('%Y-%m-%d %H:%M:%S WIB')}")
    print(f"end_wib={now.astimezone(WIB).strftime('%Y-%m-%d %H:%M:%S WIB')}")
    print("")
    print("Account now:")
    print(f"- totalWalletBalance={current_wallet:.4f} USDT")
    print(f"- totalMarginBalance={current_margin_balance:.4f} USDT")
    print(f"- totalUnrealizedProfit={current_unrealized:.4f} USDT")
    print("")
    print("Period PnL:")
    print(f"- realized_pnl={total_realized:.4f} USDT")
    print(f"- commission={total_commission:.4f} USDT")
    print(f"- funding={total_funding:.4f} USDT")
    print(f"- net={total_net:.4f} USDT")
    print(f"- events={len(income)}")
    print("")
    print("Realized equity curve approximation:")
    print(f"- approx_start_wallet={start_wallet:.4f} USDT")
    print(f"- max_drawdown={dd_usdt:.4f} USDT")
    print(f"- max_drawdown_pct_start={dd_pct_start:.2f}%")
    print(f"- max_drawdown_pct_peak={dd_pct_peak:.2f}%")
    print(f"- peak_time={wib(peak_t).strftime('%Y-%m-%d %H:%M:%S WIB') if peak_t else '-'}")
    print(f"- trough_time={wib(trough_t).strftime('%Y-%m-%d %H:%M:%S WIB') if trough_t else '-'}")
    print("")
    print("Daily net:")
    for d in sorted(by_day):
        v = by_day[d]
        print(
            f"- {d}: net={v['NET']:.4f} realized={v['REALIZED_PNL']:.4f} "
            f"commission={v['COMMISSION']:.4f} funding={v['FUNDING_FEE']:.4f} events={v['events']}"
        )
    print("")
    if best_day:
        print(f"best_day={best_day} net={by_day[best_day]['NET']:.4f} USDT")
    if worst_day:
        print(f"worst_day={worst_day} net={by_day[worst_day]['NET']:.4f} USDT")
    print("")
    print("Risk read:")
    if dd_pct_start < 1.0:
        print("- DD_STATUS=GOOD")
    elif dd_pct_start < 2.5:
        print("- DD_STATUS=WATCH")
    elif dd_pct_start < 4.0:
        print("- DD_STATUS=DEFENSIVE_MODE")
    else:
        print("- DD_STATUS=KILL_SWITCH_RECOMMENDED")

if __name__ == "__main__":
    main()
