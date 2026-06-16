#!/usr/bin/env python3
import json, csv, os, time, urllib.request, urllib.parse
from pathlib import Path
from datetime import datetime, timezone, timedelta

ROOT = Path(__file__).resolve().parents[1]
WIB = timezone(timedelta(hours=7))
UTC = timezone.utc

OUT_JSON = ROOT / "reports" / "orderbook_pricing_sim_v1.json"
OUT_CSV = ROOT / "reports" / "orderbook_pricing_sim_v1.csv"

DEFAULT_ALLOWLIST = [
    "BTCUSDT","ETHUSDT","SOLUSDT","PAXGUSDT","HYPEUSDT","XRPUSDT","ZECUSDT",
    "UNIUSDT","ADAUSDT","BCHUSDT","LINKUSDT","SUIUSDT","LTCUSDT","AVAXUSDT",
]

BASE_URL = os.getenv("BINANCE_FAPI_BASE_URL", "https://fapi.binance.com")
DEPTH_LIMIT = int(os.getenv("ORDERBOOK_SIM_DEPTH_LIMIT", "100"))
TIMEOUT_SEC = float(os.getenv("ORDERBOOK_SIM_TIMEOUT_SEC", "6"))
SLEEP_SEC = float(os.getenv("ORDERBOOK_SIM_SLEEP_SEC", "0.20"))

NOTIONALS = [
    float(x.strip())
    for x in os.getenv("ORDERBOOK_SIM_NOTIONALS_USDT", "10,25,50").split(",")
    if x.strip()
]

def load_allowlist():
    env = ROOT / ".env"
    if not env.exists():
        return DEFAULT_ALLOWLIST

    txt = env.read_text(errors="ignore")
    for line in txt.splitlines():
        line = line.strip()
        if line.startswith("PAIR_ALLOWLIST="):
            raw = line.split("=", 1)[1].strip().strip('"').strip("'")
            arr = [x.strip().replace(".P", "").replace("BINANCE:", "").upper() for x in raw.split(",") if x.strip()]
            return arr or DEFAULT_ALLOWLIST

    return DEFAULT_ALLOWLIST

def fetch_depth(symbol):
    qs = urllib.parse.urlencode({
        "symbol": symbol,
        "limit": DEPTH_LIMIT,
    })
    url = f"{BASE_URL}/fapi/v1/depth?{qs}"
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "ai-trading-vps-bot-orderbook-sim-v1",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=TIMEOUT_SEC) as resp:
        return json.loads(resp.read().decode("utf-8"))

def levels_float(levels):
    out = []
    for px, qty in levels:
        try:
            p = float(px)
            q = float(qty)
            if p > 0 and q > 0:
                out.append((p, q))
        except Exception:
            pass
    return out

def depth_within_bps(levels, mid, side, bps):
    if mid <= 0:
        return 0.0

    total_quote = 0.0
    for px, qty in levels:
        if side == "ask":
            dist_bps = (px - mid) / mid * 10000.0
        else:
            dist_bps = (mid - px) / mid * 10000.0

        if dist_bps <= bps:
            total_quote += px * qty

    return round(total_quote, 4)

def simulate_market_fill(levels, mid, notional, side):
    # side buy consumes asks. side sell consumes bids.
    remaining = float(notional)
    quote_spent = 0.0
    base_filled = 0.0

    for px, qty in levels:
        level_quote = px * qty
        use_quote = min(remaining, level_quote)
        if use_quote <= 0:
            continue

        base = use_quote / px
        quote_spent += use_quote
        base_filled += base
        remaining -= use_quote

        if remaining <= 1e-9:
            break

    if remaining > 1e-6 or base_filled <= 0:
        return {
            "ok": False,
            "avg_price": None,
            "slippage_bps": None,
            "filled_quote": round(quote_spent, 4),
            "missing_quote": round(remaining, 4),
        }

    avg_price = quote_spent / base_filled

    if side == "buy":
        slip = (avg_price - mid) / mid * 10000.0
    else:
        slip = (mid - avg_price) / mid * 10000.0

    return {
        "ok": True,
        "avg_price": round(avg_price, 10),
        "slippage_bps": round(max(0.0, slip), 6),
        "filled_quote": round(quote_spent, 4),
        "missing_quote": 0.0,
    }

def classify(spread_bps, slip_25_buy, slip_25_sell, depth10_bid, depth10_ask):
    vals = [x for x in (slip_25_buy, slip_25_sell) if x is not None]
    max_slip = max(vals) if vals else 999.0
    min_depth10 = min(depth10_bid or 0.0, depth10_ask or 0.0)

    if spread_bps <= 3.0 and max_slip <= 5.0 and min_depth10 >= 250.0:
        return "OK"
    if spread_bps <= 8.0 and max_slip <= 15.0 and min_depth10 >= 100.0:
        return "WATCH"
    return "THIN"

def main():
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)

    symbols = load_allowlist()
    now = datetime.now(UTC)
    rows = []
    errors = []

    for idx, symbol in enumerate(symbols):
        if idx > 0:
            time.sleep(SLEEP_SEC)

        try:
            d = fetch_depth(symbol)
            bids = levels_float(d.get("bids") or [])
            asks = levels_float(d.get("asks") or [])

            if not bids or not asks:
                raise RuntimeError("empty_orderbook")

            best_bid = bids[0][0]
            best_ask = asks[0][0]
            mid = (best_bid + best_ask) / 2.0
            spread_bps = (best_ask - best_bid) / mid * 10000.0

            depth5_bid = depth_within_bps(bids, mid, "bid", 5)
            depth5_ask = depth_within_bps(asks, mid, "ask", 5)
            depth10_bid = depth_within_bps(bids, mid, "bid", 10)
            depth10_ask = depth_within_bps(asks, mid, "ask", 10)

            sim = {}
            for n in NOTIONALS:
                buy = simulate_market_fill(asks, mid, n, "buy")
                sell = simulate_market_fill(bids, mid, n, "sell")
                key = str(int(n)) if float(n).is_integer() else str(n)
                sim[f"buy_slip_bps_{key}"] = buy["slippage_bps"]
                sim[f"sell_slip_bps_{key}"] = sell["slippage_bps"]
                sim[f"buy_fill_ok_{key}"] = buy["ok"]
                sim[f"sell_fill_ok_{key}"] = sell["ok"]

            # Use 25 USDT if available, else first notional.
            ref = 25.0 if 25.0 in NOTIONALS else NOTIONALS[0]
            ref_key = str(int(ref)) if float(ref).is_integer() else str(ref)

            status = classify(
                spread_bps,
                sim.get(f"buy_slip_bps_{ref_key}"),
                sim.get(f"sell_slip_bps_{ref_key}"),
                depth10_bid,
                depth10_ask,
            )

            rows.append({
                "symbol": symbol,
                "pricing_status": status,
                "best_bid": round(best_bid, 10),
                "best_ask": round(best_ask, 10),
                "mid": round(mid, 10),
                "spread_bps": round(spread_bps, 6),
                "depth5_bid_usdt": depth5_bid,
                "depth5_ask_usdt": depth5_ask,
                "depth10_bid_usdt": depth10_bid,
                "depth10_ask_usdt": depth10_ask,
                **sim,
                "event_time": d.get("E"),
                "transaction_time": d.get("T"),
            })

        except Exception as e:
            errors.append({
                "symbol": symbol,
                "error": repr(e),
            })
            rows.append({
                "symbol": symbol,
                "pricing_status": "ERROR",
                "error": repr(e),
            })

    counts = {}
    for r in rows:
        s = r.get("pricing_status")
        counts[s] = counts.get(s, 0) + 1

    report = {
        "ok": len(errors) == 0,
        "report_version": "orderbook_pricing_sim_v1_20260615",
        "mode": "REPORT_ONLY",
        "created_at_wib": now.astimezone(WIB).strftime("%Y-%m-%d %H:%M:%S WIB"),
        "symbols": len(symbols),
        "status_counts": counts,
        "notionals_usdt": NOTIONALS,
        "depth_limit": DEPTH_LIMIT,
        "base_url": BASE_URL,
        "rows": rows,
        "errors": errors[:20],
        "notes": [
            "REPORT_ONLY. Does not modify execution, orders, entries, exits, or live guards.",
            "pricing_status OK/WATCH/THIN is based on spread, 25 USDT simulated slippage, and depth within 10 bps.",
            "This is public orderbook depth, not a guaranteed fill.",
        ],
    }

    OUT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2))

    base_cols = [
        "symbol","pricing_status","spread_bps","best_bid","best_ask","mid",
        "depth5_bid_usdt","depth5_ask_usdt","depth10_bid_usdt","depth10_ask_usdt",
    ]
    sim_cols = []
    for n in NOTIONALS:
        key = str(int(n)) if float(n).is_integer() else str(n)
        sim_cols += [
            f"buy_slip_bps_{key}",
            f"sell_slip_bps_{key}",
            f"buy_fill_ok_{key}",
            f"sell_fill_ok_{key}",
        ]

    cols = base_cols + sim_cols + ["error"]

    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c) for c in cols})

    print("=== ORDERBOOK PRICING SIM V1 | REPORT_ONLY ===")
    print("out_json:", OUT_JSON)
    print("out_csv :", OUT_CSV)
    print("status_counts:", counts)
    print("")
    print(f"{'SYM':<10} {'STAT':<7} {'SPRD':>8} {'D10B':>10} {'D10A':>10} {'B25':>8} {'S25':>8}")
    for r in rows:
        b25 = r.get("buy_slip_bps_25")
        s25 = r.get("sell_slip_bps_25")
        def fmt(x, nd=3):
            return "NA" if x is None else f"{float(x):.{nd}f}"
        print(
            f"{r.get('symbol',''):<10} {r.get('pricing_status',''):<7} "
            f"{fmt(r.get('spread_bps')):>8} {fmt(r.get('depth10_bid_usdt'),1):>10} "
            f"{fmt(r.get('depth10_ask_usdt'),1):>10} {fmt(b25):>8} {fmt(s25):>8}"
        )

if __name__ == "__main__":
    main()
