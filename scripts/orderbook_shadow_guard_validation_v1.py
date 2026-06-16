#!/usr/bin/env python3
import csv, json, math
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import defaultdict, Counter

ROOT = Path(__file__).resolve().parents[1]
WIB = timezone(timedelta(hours=7))

IN_CSV = ROOT / "logs" / "orderbook_shadow_guard_snapshots_v1.csv"

OUT_JSON = ROOT / "reports" / "orderbook_shadow_guard_validation_v1.json"
OUT_CSV = ROOT / "reports" / "orderbook_shadow_guard_validation_v1.csv"

OUT_SNAP_JSONL = ROOT / "logs" / "orderbook_shadow_guard_validation_snapshots_v1.jsonl"
OUT_SNAP_CSV = ROOT / "logs" / "orderbook_shadow_guard_validation_snapshots_v1.csv"

VERSION = "orderbook_shadow_guard_validation_v1_20260617"
MODE = "REPORT_ONLY"
LIVE_ENFORCEMENT = False

MIN_VALIDATION_SAMPLES = 4
CONSISTENT_BLOCK_MIN_CONSECUTIVE = 3

def now_wib_str():
    return datetime.now(timezone.utc).astimezone(WIB).strftime("%Y-%m-%d %H:%M:%S WIB")

def read_csv(path):
    if not path.exists():
        raise SystemExit(f"missing input: {path}")
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def parse_bool(x):
    s = str(x or "").strip().lower()
    return s in ("true", "1", "yes", "y")

def num(x):
    try:
        v = float(x)
        if math.isfinite(v):
            return v
    except Exception:
        pass
    return None

def upper(x):
    return str(x or "").strip().upper()

def switch_count(vals):
    vals = [v for v in vals if v not in ("", None)]
    if len(vals) <= 1:
        return 0
    return sum(1 for a, b in zip(vals, vals[1:]) if a != b)

def tail_consecutive(vals, target):
    n = 0
    for v in reversed(vals):
        if v == target:
            n += 1
        else:
            break
    return n

def classify_symbol(sym, rows):
    # CSV append order is chronological enough. Humans invented timestamps and still made this annoying.
    n = len(rows)

    states = [upper(r.get("guard_state")) for r in rows]
    severities = [upper(r.get("severity")) for r in rows]
    blocks = [parse_bool(r.get("would_block_if_live")) for r in rows]

    latest = rows[-1]
    latest_state = upper(latest.get("guard_state"))
    latest_severity = upper(latest.get("severity"))
    latest_block = parse_bool(latest.get("would_block_if_live"))

    block_count = sum(1 for b in blocks if b)
    allow_count = sum(1 for b in blocks if not b)

    block_share = round(block_count / n, 4) if n else 0.0
    latest_consecutive_block = tail_consecutive(blocks, True)
    latest_consecutive_nonblock = tail_consecutive(blocks, False)

    state_switches = switch_count(states)
    severity_switches = switch_count(severities)

    latest_ok_share24 = num(latest.get("ok_share24"))
    latest_spread_p95 = num(latest.get("spread_bps_p95"))
    latest_slip50 = num(latest.get("worst_slip_50_p95"))
    latest_depth10 = num(latest.get("min_depth10_p10"))

    reasons = []

    if n < MIN_VALIDATION_SAMPLES:
        validation_state = "WARMUP"
        recommendation = "KEEP_REPORT_ONLY_OBSERVE"
        reasons.append(f"sample_count_lt_{MIN_VALIDATION_SAMPLES}")

    elif latest_block and latest_consecutive_block >= CONSISTENT_BLOCK_MIN_CONSECUTIVE and block_share >= 0.50:
        validation_state = "CONSISTENT_BLOCK_CANDIDATE"
        recommendation = "VALIDATED_WOULD_BLOCK_REPORT_ONLY"
        reasons.append(f"consecutive_block_ge_{CONSISTENT_BLOCK_MIN_CONSECUTIVE}")
        reasons.append("block_share_ge_050")

    elif latest_block and block_share >= 0.25:
        validation_state = "BLOCK_WATCH"
        recommendation = "KEEP_WOULD_BLOCK_SHADOW_ONLY"
        reasons.append("latest_block_true")
        reasons.append("block_share_ge_025")

    elif not latest_block and block_count == 0 and n >= MIN_VALIDATION_SAMPLES:
        validation_state = "CLEAN_ALLOW_CANDIDATE"
        recommendation = "ALLOW_STABLE_REPORT_ONLY"
        reasons.append("no_block_events")

    elif state_switches >= 2:
        validation_state = "FLAPPING_REVIEW"
        recommendation = "DO_NOT_ENABLE_LIVE_BLOCK"
        reasons.append("state_switch_count_ge_2")

    else:
        validation_state = "MIXED_OBSERVE"
        recommendation = "KEEP_REPORT_ONLY_OBSERVE"
        reasons.append("mixed_guard_history")

    return {
        "symbol": sym,
        "mode": MODE,
        "live_enforcement": LIVE_ENFORCEMENT,

        "sample_count": n,
        "latest_guard_state": latest_state,
        "latest_severity": latest_severity,
        "latest_would_block_if_live": latest_block,

        "block_count": block_count,
        "allow_count": allow_count,
        "block_share": block_share,

        "latest_consecutive_block": latest_consecutive_block,
        "latest_consecutive_nonblock": latest_consecutive_nonblock,

        "state_switch_count": state_switches,
        "severity_switch_count": severity_switches,

        "latest_pricing_status": upper(latest.get("latest_pricing_status")),
        "latest_grade24": upper(latest.get("grade24")),
        "latest_grade7": upper(latest.get("grade7")),
        "latest_ok_share24": latest_ok_share24,
        "latest_spread_bps_p95": latest_spread_p95,
        "latest_worst_slip_50_p95": latest_slip50,
        "latest_min_depth10_p10": latest_depth10,

        "validation_state": validation_state,
        "recommendation": recommendation,
        "reasons": reasons,
    }

def main():
    rows = read_csv(IN_CSV)

    by_symbol = defaultdict(list)
    for r in rows:
        sym = upper(r.get("symbol"))
        if sym:
            by_symbol[sym].append(r)

    out_rows = []
    for sym in sorted(by_symbol):
        out_rows.append(classify_symbol(sym, by_symbol[sym]))

    out_rows.sort(key=lambda r: (
        r["validation_state"] not in ("CONSISTENT_BLOCK_CANDIDATE", "BLOCK_WATCH"),
        r["latest_would_block_if_live"] is not True,
        r["symbol"],
    ))

    created_at_wib = now_wib_str()

    state_counts = dict(Counter(r["validation_state"] for r in out_rows))
    recommendation_counts = dict(Counter(r["recommendation"] for r in out_rows))

    report = {
        "ok": True,
        "version": VERSION,
        "mode": MODE,
        "live_enforcement": LIVE_ENFORCEMENT,
        "created_at_wib": created_at_wib,
        "source": str(IN_CSV),
        "pair_count": len(out_rows),
        "state_counts": state_counts,
        "recommendation_counts": recommendation_counts,
        "note": "REPORT_ONLY validation. Does not enable live blocking or modify live gates.",
        "rows": out_rows,
    }

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2))

    cols = [
        "symbol","mode","live_enforcement",
        "sample_count","latest_guard_state","latest_severity","latest_would_block_if_live",
        "block_count","allow_count","block_share",
        "latest_consecutive_block","latest_consecutive_nonblock",
        "state_switch_count","severity_switch_count",
        "latest_pricing_status","latest_grade24","latest_grade7",
        "latest_ok_share24","latest_spread_bps_p95","latest_worst_slip_50_p95","latest_min_depth10_p10",
        "validation_state","recommendation","reasons",
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

    print(f"=== ORDERBOOK SHADOW GUARD VALIDATION V1 | {MODE} ===")
    print("out_json:", OUT_JSON)
    print("out_csv :", OUT_CSV)
    print("snap_jsonl:", OUT_SNAP_JSONL)
    print("snap_csv :", OUT_SNAP_CSV)
    print("state_counts:", state_counts)
    print("recommendation_counts:", recommendation_counts)
    print("")
    print(f"{'SYM':<10} {'VALIDATION':<28} {'REC':<32} {'N':>3} {'BLK':>3} {'SHR':>5} {'LAST':<22}")
    for r in out_rows:
        print(
            f"{r['symbol']:<10} {r['validation_state']:<28} {r['recommendation']:<32} "
            f"{r['sample_count']:>3} {r['block_count']:>3} {r['block_share']:>5.2f} "
            f"{r['latest_guard_state']:<22}"
        )

if __name__ == "__main__":
    main()
