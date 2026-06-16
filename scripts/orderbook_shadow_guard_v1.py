#!/usr/bin/env python3
import subprocess
import csv, json, math
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import Counter

ROOT = Path(__file__).resolve().parents[1]
WIB = timezone(timedelta(hours=7))

IN_STABILITY = ROOT / "reports" / "orderbook_stability_report_v1.csv"
IN_PRICING = ROOT / "reports" / "orderbook_pricing_sim_v1.csv"

OUT_JSON = ROOT / "reports" / "orderbook_shadow_guard_v1.json"
OUT_CSV = ROOT / "reports" / "orderbook_shadow_guard_v1.csv"

OUT_SNAP_JSONL = ROOT / "logs" / "orderbook_shadow_guard_snapshots_v1.jsonl"
OUT_SNAP_CSV = ROOT / "logs" / "orderbook_shadow_guard_snapshots_v1.csv"

VERSION = "orderbook_shadow_guard_v1_20260617"
MODE = "REPORT_ONLY"
LIVE_ENFORCEMENT = False

BAD_GRADES = {
    "DATA_UNSTABLE",
    "WIDE_SPREAD",
    "THIN_DEPTH",
    "HIGH_SLIPPAGE",
}

BAD_PRICING_STATUS = {
    "WATCH",
    "ERROR",
    "FAIL",
    "BAD",
    "NO_BID_ASK",
    "EMPTY_BOOK",
}

def now_wib_str():
    return datetime.now(timezone.utc).astimezone(WIB).strftime("%Y-%m-%d %H:%M:%S WIB")

def read_csv(path):
    if not path.exists():
        raise SystemExit(f"missing input: {path}")
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def num(x):
    try:
        v = float(x)
        if math.isfinite(v):
            return v
    except Exception:
        pass
    return None

def s(x):
    return str(x or "").strip()

def upper(x):
    return s(x).upper()

def latest_pricing_by_symbol(rows):
    out = {}
    for r in rows:
        sym = upper(r.get("symbol") or r.get("pair"))
        if sym:
            out[sym] = r
    return out

def stability_by_symbol(rows, window):
    out = {}
    for r in rows:
        sym = upper(r.get("symbol"))
        win = s(r.get("window"))
        if sym and win == window:
            out[sym] = r
    return out

def classify(sym, row24, row7, price):
    reasons = []
    soft_reasons = []

    grade24 = upper(row24.get("liquidity_grade"))
    grade7 = upper(row7.get("liquidity_grade")) if row7 else ""
    latest_status = upper(price.get("pricing_status") or price.get("status") or price.get("orderbook_status"))

    sample24 = int(float(row24.get("sample_count") or 0))
    ok_share24 = num(row24.get("ok_share"))
    spread_p95 = num(row24.get("spread_bps_p95"))
    worst_slip_50 = num(row24.get("worst_slip_50_p95"))
    min_depth10 = num(row24.get("min_depth10_p10"))

    latest_spread = num(price.get("spread_bps"))
    latest_depth10_bid = num(price.get("depth10_bid_usdt") or price.get("bid_depth10_usdt"))
    latest_depth10_ask = num(price.get("depth10_ask_usdt") or price.get("ask_depth10_usdt"))

    if grade24 in BAD_GRADES:
        reasons.append(f"grade24_bad:{grade24}")

    if latest_status in BAD_PRICING_STATUS:
        reasons.append(f"latest_pricing_status:{latest_status}")

    if ok_share24 is not None and ok_share24 < 0.90:
        reasons.append(f"ok_share24_lt_090:{ok_share24}")

    if spread_p95 is not None and spread_p95 > 12:
        reasons.append(f"spread_p95_gt_12bps:{spread_p95}")

    if worst_slip_50 is not None and worst_slip_50 > 30:
        reasons.append(f"worst_slip50_p95_gt_30bps:{worst_slip_50}")

    if min_depth10 is not None and min_depth10 < 1000:
        reasons.append(f"min_depth10_p10_lt_1000:{min_depth10}")

    if sample24 < 24:
        soft_reasons.append(f"sample24_lt_full_day:{sample24}")

    if grade7 in BAD_GRADES and grade24 in BAD_GRADES:
        reasons.append(f"grade7_also_bad:{grade7}")
    elif grade7 in BAD_GRADES:
        soft_reasons.append(f"grade7_bad_but_24h_not_bad:{grade7}")

    would_block = len(reasons) > 0

    if would_block:
        guard_state = "WOULD_BLOCK_IF_LIVE"
        severity = "HARD_BLOCK_CANDIDATE"
        action = "REPORT_ONLY_BLOCK_CANDIDATE"
    elif soft_reasons:
        guard_state = "WATCH_ONLY"
        severity = "SOFT_WATCH"
        action = "REPORT_ONLY_KEEP_MONITORING"
    else:
        guard_state = "ALLOW_IF_LIVE"
        severity = "PASS"
        action = "REPORT_ONLY_ALLOW"

    return {
        "symbol": sym,
        "mode": MODE,
        "live_enforcement": LIVE_ENFORCEMENT,

        "guard_state": guard_state,
        "severity": severity,
        "shadow_action": action,
        "would_block_if_live": would_block,

        "latest_pricing_status": latest_status,
        "grade24": grade24,
        "grade7": grade7,
        "sample24": sample24,
        "ok_share24": ok_share24,

        "spread_bps_p95": spread_p95,
        "worst_slip_50_p95": worst_slip_50,
        "min_depth10_p10": min_depth10,

        "latest_spread_bps": latest_spread,
        "latest_depth10_bid_usdt": latest_depth10_bid,
        "latest_depth10_ask_usdt": latest_depth10_ask,

        "hard_reasons": reasons,
        "soft_reasons": soft_reasons,
    }

def main():
    stability_rows = read_csv(IN_STABILITY)
    pricing_rows = read_csv(IN_PRICING)

    stab24 = stability_by_symbol(stability_rows, "24h")
    stab7 = stability_by_symbol(stability_rows, "7d")
    latest_price = latest_pricing_by_symbol(pricing_rows)

    symbols = sorted(set(stab24) | set(latest_price))

    out_rows = []
    for sym in symbols:
        row24 = stab24.get(sym)
        if not row24:
            continue
        row7 = stab7.get(sym, {})
        price = latest_price.get(sym, {})
        out_rows.append(classify(sym, row24, row7, price))

    out_rows.sort(key=lambda r: (
        r["guard_state"] != "WOULD_BLOCK_IF_LIVE",
        r["severity"] != "SOFT_WATCH",
        r["symbol"],
    ))

    created_at_wib = now_wib_str()

    state_counts = dict(Counter(r["guard_state"] for r in out_rows))
    severity_counts = dict(Counter(r["severity"] for r in out_rows))
    would_block_count = sum(1 for r in out_rows if r["would_block_if_live"])

    report = {
        "ok": True,
        "version": VERSION,
        "mode": MODE,
        "live_enforcement": LIVE_ENFORCEMENT,
        "created_at_wib": created_at_wib,
        "source_stability": str(IN_STABILITY),
        "source_pricing": str(IN_PRICING),
        "pair_count": len(out_rows),
        "would_block_count": would_block_count,
        "state_counts": state_counts,
        "severity_counts": severity_counts,
        "note": "REPORT_ONLY shadow guard. Does not block live orders or modify live gates.",
        "rows": out_rows,
    }

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2))

    cols = [
        "symbol","mode","live_enforcement",
        "guard_state","severity","shadow_action","would_block_if_live",
        "latest_pricing_status","grade24","grade7","sample24","ok_share24",
        "spread_bps_p95","worst_slip_50_p95","min_depth10_p10",
        "latest_spread_bps","latest_depth10_bid_usdt","latest_depth10_ask_usdt",
        "hard_reasons","soft_reasons",
    ]

    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in out_rows:
            row = {c: r.get(c) for c in cols}
            row["hard_reasons"] = json.dumps(row.get("hard_reasons") or [], ensure_ascii=False)
            row["soft_reasons"] = json.dumps(row.get("soft_reasons") or [], ensure_ascii=False)
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
            row["hard_reasons"] = json.dumps(row.get("hard_reasons") or [], ensure_ascii=False)
            row["soft_reasons"] = json.dumps(row.get("soft_reasons") or [], ensure_ascii=False)
            w.writerow(row)

    print(f"=== ORDERBOOK SHADOW GUARD V1 | {MODE} ===")
    print("out_json:", OUT_JSON)
    print("out_csv :", OUT_CSV)
    print("snap_jsonl:", OUT_SNAP_JSONL)
    print("snap_csv :", OUT_SNAP_CSV)
    print("state_counts:", state_counts)
    print("severity_counts:", severity_counts)
    print("would_block_count:", would_block_count)
    print("")
    print(f"{'SYM':<10} {'GUARD':<22} {'SEV':<22} {'STAT':<8} {'G24':<16} {'OK24':>5} {'SP95':>8} {'SL50':>8}")
    for r in out_rows:
        print(
            f"{r['symbol']:<10} {r['guard_state']:<22} {r['severity']:<22} "
            f"{r['latest_pricing_status']:<8} {r['grade24']:<16} "
            f"{str(r['ok_share24'] if r['ok_share24'] is not None else 'NA'):>5} "
            f"{str(r['spread_bps_p95'] if r['spread_bps_p95'] is not None else 'NA'):>8} "
            f"{str(r['worst_slip_50_p95'] if r['worst_slip_50_p95'] is not None else 'NA'):>8}"
        )

def run_orderbook_shadow_guard_validation_report():
    # ORDERBOOK_SHADOW_GUARD_VALIDATION_CHAIN_V1
    # REPORT_ONLY chain. Does not enable live block or modify live gates.
    script = ROOT / "scripts" / "orderbook_shadow_guard_validation_v1.py"

    if not script.exists():
        print("orderbook_shadow_guard_validation_script: missing:", script)
        return

    print("")
    print("=== CHAIN ORDERBOOK SHADOW GUARD VALIDATION V1 ===")
    res = subprocess.run(["/usr/bin/python3", str(script)], check=False)
    print("orderbook_shadow_guard_validation_report_rc:", res.returncode)

if __name__ == "__main__":
    main()
    run_orderbook_shadow_guard_validation_report()
