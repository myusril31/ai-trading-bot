#!/usr/bin/env python3
import subprocess
import csv, json, math
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import defaultdict, Counter
from statistics import mean

ROOT = Path(__file__).resolve().parents[1]
WIB = timezone(timedelta(hours=7))

IN_CSV = ROOT / "logs" / "orderbook_pricing_snapshots_v1.csv"

OUT_JSON = ROOT / "reports" / "orderbook_stability_report_v1.json"
OUT_CSV = ROOT / "reports" / "orderbook_stability_report_v1.csv"

OUT_SNAP_JSONL = ROOT / "logs" / "orderbook_stability_snapshots_v1.jsonl"
OUT_SNAP_CSV = ROOT / "logs" / "orderbook_stability_snapshots_v1.csv"

VERSION = "orderbook_stability_report_v1_20260617"
MODE = "REPORT_ONLY"

WINDOWS = [
    ("24h", 24),
    ("7d", 24 * 7),
]

MIN_SAMPLES_WARMUP = 8

def now_wib_str():
    return datetime.now(timezone.utc).astimezone(WIB).strftime("%Y-%m-%d %H:%M:%S WIB")

def parse_wib(s):
    if not s:
        return None
    s = str(s).strip().replace(" WIB", "")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s[:19], fmt).replace(tzinfo=WIB)
        except Exception:
            pass
    return None

def num(x):
    try:
        v = float(x)
        if math.isfinite(v):
            return v
    except Exception:
        pass
    return None

def nget(row, names):
    for name in names:
        if name in row:
            v = num(row.get(name))
            if v is not None:
                return v
    return None

def sget(row, names):
    for name in names:
        if name in row and row.get(name) not in (None, ""):
            return str(row.get(name)).strip()
    return ""

def pct(vals, q):
    vals = sorted(v for v in vals if v is not None and math.isfinite(v))
    if not vals:
        return None
    if len(vals) == 1:
        return round(vals[0], 6)
    idx = int(round((len(vals) - 1) * q))
    return round(vals[max(0, min(idx, len(vals) - 1))], 6)

def avg(vals):
    vals = [v for v in vals if v is not None and math.isfinite(v)]
    return round(mean(vals), 6) if vals else None

def minv(vals):
    vals = [v for v in vals if v is not None and math.isfinite(v)]
    return round(min(vals), 6) if vals else None

def maxv(vals):
    vals = [v for v in vals if v is not None and math.isfinite(v)]
    return round(max(vals), 6) if vals else None

def read_rows():
    if not IN_CSV.exists():
        raise SystemExit(f"missing input: {IN_CSV}")

    with IN_CSV.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    out = []
    for r in rows:
        sym = sget(r, ["symbol", "pair"]).upper()
        ts = parse_wib(sget(r, ["created_at_wib", "ts_wib", "created_wib"]))
        if not sym:
            continue
        rr = dict(r)
        rr["_symbol"] = sym
        rr["_ts"] = ts
        out.append(rr)

    return out

def status_ok(status):
    s = str(status or "").upper()
    if s in ("OK", "PASS", "GOOD", "NORMAL", "VALID"):
        return True
    if "OK" in s and "NOT" not in s:
        return True
    return False

def summarize_symbol(symbol, rows, window_name):
    n = len(rows)

    statuses = [
        sget(r, ["pricing_status", "status", "orderbook_status"])
        for r in rows
    ]

    ok_count = sum(1 for s in statuses if status_ok(s))
    error_count = sum(
        1 for s in statuses
        if str(s or "").upper() in ("ERROR", "FAIL", "BAD", "NO_BID_ASK", "EMPTY_BOOK")
    )

    spreads = [nget(r, ["spread_bps", "bid_ask_spread_bps"]) for r in rows]

    depth5_bid = [nget(r, ["depth5_bid_usdt", "bid_depth5_usdt", "depth_5_bid_usdt"]) for r in rows]
    depth5_ask = [nget(r, ["depth5_ask_usdt", "ask_depth5_usdt", "depth_5_ask_usdt"]) for r in rows]
    depth10_bid = [nget(r, ["depth10_bid_usdt", "bid_depth10_usdt", "depth_10_bid_usdt"]) for r in rows]
    depth10_ask = [nget(r, ["depth10_ask_usdt", "ask_depth10_usdt", "depth_10_ask_usdt"]) for r in rows]

    buy_slip_10 = [nget(r, ["buy_slippage_bps_10", "buy_slip_bps_10", "slippage_buy_10_bps"]) for r in rows]
    buy_slip_25 = [nget(r, ["buy_slippage_bps_25", "buy_slip_bps_25", "slippage_buy_25_bps"]) for r in rows]
    buy_slip_50 = [nget(r, ["buy_slippage_bps_50", "buy_slip_bps_50", "slippage_buy_50_bps"]) for r in rows]

    sell_slip_10 = [nget(r, ["sell_slippage_bps_10", "sell_slip_bps_10", "slippage_sell_10_bps"]) for r in rows]
    sell_slip_25 = [nget(r, ["sell_slippage_bps_25", "sell_slip_bps_25", "slippage_sell_25_bps"]) for r in rows]
    sell_slip_50 = [nget(r, ["sell_slippage_bps_50", "sell_slip_bps_50", "slippage_sell_50_bps"]) for r in rows]

    status_counter = Counter(str(s or "NA").upper() for s in statuses)
    ok_share = round(ok_count / n, 4) if n else 0.0
    error_share = round(error_count / n, 4) if n else 0.0

    spread_p95 = pct(spreads, 0.95)
    spread_max = maxv(spreads)

    depth10_bid_p10 = pct(depth10_bid, 0.10)
    depth10_ask_p10 = pct(depth10_ask, 0.10)
    min_depth10_p10 = None
    if depth10_bid_p10 is not None and depth10_ask_p10 is not None:
        min_depth10_p10 = round(min(depth10_bid_p10, depth10_ask_p10), 6)

    buy50_p95 = pct(buy_slip_50, 0.95)
    sell50_p95 = pct(sell_slip_50, 0.95)

    worst50_p95 = None
    if buy50_p95 is not None and sell50_p95 is not None:
        worst50_p95 = round(max(buy50_p95, sell50_p95), 6)

    reasons = []
    grade = "UNKNOWN"

    if n < MIN_SAMPLES_WARMUP:
        grade = "WARMUP"
        reasons.append(f"sample_count_lt_{MIN_SAMPLES_WARMUP}")

    elif ok_share < 0.90 or error_share > 0.05:
        grade = "DATA_UNSTABLE"
        reasons.append("ok_share_lt_090_or_error_share_gt_005")

    elif spread_p95 is not None and spread_p95 > 12:
        grade = "WIDE_SPREAD"
        reasons.append("spread_p95_gt_12bps")

    elif min_depth10_p10 is not None and min_depth10_p10 < 1000:
        grade = "THIN_DEPTH"
        reasons.append("depth10_p10_lt_1000usdt")

    elif worst50_p95 is not None and worst50_p95 > 30:
        grade = "HIGH_SLIPPAGE"
        reasons.append("worst_50usdt_slippage_p95_gt_30bps")

    elif (
        ok_share >= 0.95
        and (spread_p95 is None or spread_p95 <= 5)
        and (worst50_p95 is None or worst50_p95 <= 10)
    ):
        grade = "STABLE_LIQUIDITY"
        reasons.append("ok_share_ge_095_spread_slippage_ok")

    else:
        grade = "WATCH_LIQUIDITY"
        reasons.append("mixed_liquidity_conditions")

    first_seen = None
    last_seen = None
    ts_vals = [r.get("_ts") for r in rows if r.get("_ts") is not None]
    if ts_vals:
        first_seen = min(ts_vals).strftime("%Y-%m-%d %H:%M:%S WIB")
        last_seen = max(ts_vals).strftime("%Y-%m-%d %H:%M:%S WIB")

    return {
        "symbol": symbol,
        "mode": MODE,
        "window": window_name,
        "sample_count": n,
        "first_seen_wib": first_seen,
        "last_seen_wib": last_seen,
        "dominant_status": status_counter.most_common(1)[0][0] if status_counter else "NA",
        "ok_share": ok_share,
        "error_share": error_share,

        "spread_bps_avg": avg(spreads),
        "spread_bps_p95": spread_p95,
        "spread_bps_max": spread_max,

        "depth5_bid_avg": avg(depth5_bid),
        "depth5_ask_avg": avg(depth5_ask),
        "depth10_bid_avg": avg(depth10_bid),
        "depth10_ask_avg": avg(depth10_ask),
        "depth10_bid_p10": depth10_bid_p10,
        "depth10_ask_p10": depth10_ask_p10,
        "min_depth10_p10": min_depth10_p10,

        "buy_slip_10_p95": pct(buy_slip_10, 0.95),
        "buy_slip_25_p95": pct(buy_slip_25, 0.95),
        "buy_slip_50_p95": buy50_p95,
        "sell_slip_10_p95": pct(sell_slip_10, 0.95),
        "sell_slip_25_p95": pct(sell_slip_25, 0.95),
        "sell_slip_50_p95": sell50_p95,
        "worst_slip_50_p95": worst50_p95,

        "liquidity_grade": grade,
        "recommendation": "REPORT_ONLY_DO_NOT_BLOCK_LIVE",
        "reasons": reasons,
    }

def main():
    rows = read_rows()

    ts_vals = [r.get("_ts") for r in rows if r.get("_ts") is not None]
    anchor_ts = max(ts_vals) if ts_vals else datetime.now(timezone.utc).astimezone(WIB)

    out_rows = []

    for window_name, hours in WINDOWS:
        cutoff = anchor_ts - timedelta(hours=hours)
        win_rows = [
            r for r in rows
            if r.get("_ts") is None or r.get("_ts") >= cutoff
        ]

        by_symbol = defaultdict(list)
        for r in win_rows:
            by_symbol[r["_symbol"]].append(r)

        for sym in sorted(by_symbol):
            out_rows.append(summarize_symbol(sym, by_symbol[sym], window_name))

    grade_counts = dict(Counter(r["liquidity_grade"] for r in out_rows))

    created_at_wib = now_wib_str()

    report = {
        "ok": True,
        "version": VERSION,
        "mode": MODE,
        "created_at_wib": created_at_wib,
        "source": str(IN_CSV),
        "total_source_rows": len(rows),
        "anchor_ts_wib": anchor_ts.strftime("%Y-%m-%d %H:%M:%S WIB"),
        "windows": [w for w, _ in WINDOWS],
        "row_count": len(out_rows),
        "grade_counts": grade_counts,
        "note": "REPORT_ONLY. Does not modify execution, live gates, pair allowlist, or order routing.",
        "rows": out_rows,
    }

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2))

    cols = [
        "symbol","mode","window","sample_count","first_seen_wib","last_seen_wib",
        "dominant_status","ok_share","error_share",
        "spread_bps_avg","spread_bps_p95","spread_bps_max",
        "depth5_bid_avg","depth5_ask_avg","depth10_bid_avg","depth10_ask_avg",
        "depth10_bid_p10","depth10_ask_p10","min_depth10_p10",
        "buy_slip_10_p95","buy_slip_25_p95","buy_slip_50_p95",
        "sell_slip_10_p95","sell_slip_25_p95","sell_slip_50_p95",
        "worst_slip_50_p95",
        "liquidity_grade","recommendation","reasons",
    ]

    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in out_rows:
            row = {c: r.get(c) for c in cols}
            row["reasons"] = json.dumps(row.get("reasons") or [], ensure_ascii=False)
            w.writerow(row)

    OUT_SNAP_JSONL.parent.mkdir(parents=True, exist_ok=True)
    snap_cols = ["created_at_wib", "version", *cols]

    snap_rows = []
    for r in out_rows:
        rr = {"created_at_wib": created_at_wib, "version": VERSION, **r}
        snap_rows.append(rr)

    with OUT_SNAP_JSONL.open("a", encoding="utf-8") as f:
        for rr in snap_rows:
            f.write(json.dumps(rr, ensure_ascii=False, sort_keys=True) + "\n")

    snap_csv_exists = OUT_SNAP_CSV.exists() and OUT_SNAP_CSV.stat().st_size > 0
    with OUT_SNAP_CSV.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=snap_cols)
        if not snap_csv_exists:
            w.writeheader()
        for rr in snap_rows:
            row = {c: rr.get(c) for c in snap_cols}
            row["reasons"] = json.dumps(row.get("reasons") or [], ensure_ascii=False)
            w.writerow(row)

    print(f"=== ORDERBOOK STABILITY REPORT V1 | {MODE} ===")
    print("out_json:", OUT_JSON)
    print("out_csv :", OUT_CSV)
    print("snap_jsonl:", OUT_SNAP_JSONL)
    print("snap_csv :", OUT_SNAP_CSV)
    print("grade_counts:", grade_counts)
    print("")
    print(f"{'SYM':<10} {'WIN':<4} {'GRADE':<18} {'N':>4} {'OK':>5} {'SP95':>8} {'D10P10':>10} {'SLIP50':>8}")
    for r in out_rows:
        print(
            f"{r['symbol']:<10} {r['window']:<4} {r['liquidity_grade']:<18} "
            f"{r['sample_count']:>4} {r['ok_share']:>5.2f} "
            f"{str(r['spread_bps_p95'] if r['spread_bps_p95'] is not None else 'NA'):>8} "
            f"{str(r['min_depth10_p10'] if r['min_depth10_p10'] is not None else 'NA'):>10} "
            f"{str(r['worst_slip_50_p95'] if r['worst_slip_50_p95'] is not None else 'NA'):>8}"
        )

def run_orderbook_shadow_guard_report():
    # ORDERBOOK_SHADOW_GUARD_CHAIN_V1
    # REPORT_ONLY chain. Does not block live orders or modify live gates.
    script = ROOT / "scripts" / "orderbook_shadow_guard_v1.py"

    if not script.exists():
        print("orderbook_shadow_guard_script: missing:", script)
        return

    print("")
    print("=== CHAIN ORDERBOOK SHADOW GUARD V1 ===")
    res = subprocess.run(["/usr/bin/python3", str(script)], check=False)
    print("orderbook_shadow_guard_report_rc:", res.returncode)

if __name__ == "__main__":
    main()
    run_orderbook_shadow_guard_report()
