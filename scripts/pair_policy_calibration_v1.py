#!/usr/bin/env python3
import csv, json, math
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import defaultdict, Counter
from statistics import mean, pstdev

ROOT = Path(__file__).resolve().parents[1]
WIB = timezone(timedelta(hours=7))

IN_CSV = ROOT / "logs" / "pair_league_policy_snapshots_v1.csv"

OUT_JSON = ROOT / "reports" / "pair_policy_calibration_v1.json"
OUT_CSV = ROOT / "reports" / "pair_policy_calibration_v1.csv"

OUT_SNAP_JSONL = ROOT / "logs" / "pair_policy_calibration_snapshots_v1.jsonl"
OUT_SNAP_CSV = ROOT / "logs" / "pair_policy_calibration_snapshots_v1.csv"

VERSION = "pair_policy_calibration_v1_20260616"
MODE = "REPORT_ONLY"

MIN_WARMUP_SAMPLES = 6
FULL_DAY_SAMPLES = 24

def num(x):
    try:
        v = float(x)
        if math.isfinite(v):
            return v
    except Exception:
        pass
    return None

def read_rows(path):
    if not path.exists():
        raise SystemExit(f"missing input: {path}")
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def switch_count(vals):
    vals = [v for v in vals if v not in (None, "")]
    if len(vals) <= 1:
        return 0
    return sum(1 for a, b in zip(vals, vals[1:]) if a != b)

def top_value(vals):
    vals = [v for v in vals if v not in (None, "")]
    if not vals:
        return None, 0, 0.0
    c = Counter(vals)
    k, n = c.most_common(1)[0]
    return k, n, round(n / len(vals), 4)

def avg(vals):
    vals = [x for x in vals if x is not None]
    return round(mean(vals), 4) if vals else None

def std(vals):
    vals = [x for x in vals if x is not None]
    return round(pstdev(vals), 4) if len(vals) >= 2 else 0.0

def share(vals, targets):
    vals = [v for v in vals if v not in (None, "")]
    if not vals:
        return 0.0
    targets = set(targets)
    return round(sum(1 for v in vals if v in targets) / len(vals), 4)

def classify_pair(sym, rows):
    n = len(rows)
    latest = rows[-1]

    statuses = [r.get("league_status") for r in rows]
    actions = [r.get("policy_action") for r in rows]

    weights = [num(r.get("policy_weight")) for r in rows]
    league_scores = [num(r.get("league_score")) for r in rows]
    win_rates = [num(r.get("win_rate")) for r in rows]
    expectancies = [num(r.get("expectancy_R")) for r in rows]
    pwin_vals = [num(r.get("avg_ml_p_win")) for r in rows]

    dom_status, dom_status_n, dom_status_share = top_value(statuses)
    dom_action, dom_action_n, dom_action_share = top_value(actions)

    action_sw = switch_count(actions)
    status_sw = switch_count(statuses)

    priority_share = share(actions, ["PRIORITY_SCAN"])
    normal_share = share(actions, ["NORMAL_SCAN"])
    watch_share = share(actions, ["WATCH_SCAN"])
    weak_share = share(actions, ["LOW_PRIORITY_SCAN", "BENCH_CANDIDATE_REPORT_ONLY"])

    avg_weight = avg(weights)
    min_weight = min([x for x in weights if x is not None], default=None)
    max_weight = max([x for x in weights if x is not None], default=None)

    reasons = []
    state = "OBSERVE"
    recommendation = "KEEP_CURRENT_REPORT_ONLY"

    if n < MIN_WARMUP_SAMPLES:
        state = "WARMUP"
        recommendation = "KEEP_CURRENT_REPORT_ONLY"
        reasons.append(f"sample_count_lt_{MIN_WARMUP_SAMPLES}")

    elif action_sw >= max(2, n // 4):
        state = "UNSTABLE_POLICY"
        recommendation = "FREEZE_WEIGHT_REPORT_ONLY"
        reasons.append("action_switch_count_high")

    elif priority_share >= 0.80 and avg_weight is not None and avg_weight >= 0.95:
        state = "STABLE_PRIORITY"
        recommendation = "CANDIDATE_PRIORITY_SCAN_KEEP"
        reasons.append("priority_share_ge_080")
        reasons.append("avg_weight_ge_095")

    elif dom_action in ("NORMAL_SCAN", "WATCH_SCAN") and dom_action_share >= 0.75:
        state = "STABLE_SOFT_WEIGHT"
        recommendation = "KEEP_SOFT_WEIGHT_REPORT_ONLY"
        reasons.append("dominant_mid_action_ge_075")

    elif weak_share >= 0.70 and avg_weight is not None and avg_weight <= 0.45:
        state = "SOFT_BENCH_CANDIDATE"
        recommendation = "BENCH_CANDIDATE_REPORT_ONLY"
        reasons.append("weak_action_share_ge_070")
        reasons.append("avg_weight_le_045")

    else:
        state = "MIXED_OBSERVE"
        recommendation = "KEEP_CURRENT_REPORT_ONLY"
        reasons.append("mixed_policy_evidence")

    if n < FULL_DAY_SAMPLES:
        reasons.append(f"full_day_sample_lt_{FULL_DAY_SAMPLES}")

    return {
        "symbol": sym,
        "mode": MODE,
        "sample_count": n,
        "first_seen_wib": rows[0].get("created_at_wib"),
        "last_seen_wib": latest.get("created_at_wib"),

        "latest_status": latest.get("league_status"),
        "latest_action": latest.get("policy_action"),
        "latest_weight": num(latest.get("policy_weight")),
        "latest_league_score": num(latest.get("league_score")),

        "dominant_status": dom_status,
        "dominant_status_share": dom_status_share,
        "dominant_action": dom_action,
        "dominant_action_share": dom_action_share,

        "status_switch_count": status_sw,
        "action_switch_count": action_sw,

        "priority_scan_share": priority_share,
        "normal_scan_share": normal_share,
        "watch_scan_share": watch_share,
        "weak_action_share": weak_share,

        "avg_policy_weight": avg_weight,
        "min_policy_weight": min_weight,
        "max_policy_weight": max_weight,
        "std_policy_weight": std(weights),

        "avg_league_score": avg(league_scores),
        "avg_win_rate": avg(win_rates),
        "avg_expectancy_R": avg(expectancies),
        "avg_ml_p_win": avg(pwin_vals),

        "calibration_state": state,
        "recommendation": recommendation,
        "reasons": reasons,
    }

def main():
    rows = read_rows(IN_CSV)

    by_symbol = defaultdict(list)
    for r in rows:
        sym = str(r.get("symbol") or "").upper().strip()
        if sym:
            by_symbol[sym].append(r)

    out_rows = []
    for sym in sorted(by_symbol):
        out_rows.append(classify_pair(sym, by_symbol[sym]))

    out_rows.sort(key=lambda r: (
        r["calibration_state"] not in ("STABLE_PRIORITY",),
        -(r.get("avg_policy_weight") or 0),
        r["symbol"],
    ))

    now = datetime.now(timezone.utc).astimezone(WIB)
    created_at_wib = now.strftime("%Y-%m-%d %H:%M:%S WIB")

    state_counts = dict(Counter(r["calibration_state"] for r in out_rows))
    rec_counts = dict(Counter(r["recommendation"] for r in out_rows))

    report = {
        "ok": True,
        "version": VERSION,
        "mode": MODE,
        "created_at_wib": created_at_wib,
        "source": str(IN_CSV),
        "pair_count": len(out_rows),
        "total_snapshot_rows": len(rows),
        "min_warmup_samples": MIN_WARMUP_SAMPLES,
        "full_day_samples": FULL_DAY_SAMPLES,
        "state_counts": state_counts,
        "recommendation_counts": rec_counts,
        "note": "REPORT_ONLY. Does not modify allowlist, scanner, execution, or live gates.",
        "rows": out_rows,
    }

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2))

    cols = [
        "symbol","mode","sample_count","first_seen_wib","last_seen_wib",
        "latest_status","latest_action","latest_weight","latest_league_score",
        "dominant_status","dominant_status_share","dominant_action","dominant_action_share",
        "status_switch_count","action_switch_count",
        "priority_scan_share","normal_scan_share","watch_scan_share","weak_action_share",
        "avg_policy_weight","min_policy_weight","max_policy_weight","std_policy_weight",
        "avg_league_score","avg_win_rate","avg_expectancy_R","avg_ml_p_win",
        "calibration_state","recommendation","reasons",
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
        rr = {
            "created_at_wib": created_at_wib,
            "version": VERSION,
            **r,
        }
        snap_rows.append(rr)

    with OUT_SNAP_JSONL.open("a", encoding="utf-8") as f:
        for rr in snap_rows:
            f.write(json.dumps(rr, ensure_ascii=False, sort_keys=True) + "\\n")

    snap_csv_exists = OUT_SNAP_CSV.exists() and OUT_SNAP_CSV.stat().st_size > 0
    with OUT_SNAP_CSV.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=snap_cols)
        if not snap_csv_exists:
            w.writeheader()
        for rr in snap_rows:
            row = {c: rr.get(c) for c in snap_cols}
            row["reasons"] = json.dumps(row.get("reasons") or [], ensure_ascii=False)
            w.writerow(row)

    print(f"=== PAIR POLICY CALIBRATION V1 | {MODE} ===")
    print("out_json:", OUT_JSON)
    print("out_csv :", OUT_CSV)
    print("snap_jsonl:", OUT_SNAP_JSONL)
    print("snap_csv :", OUT_SNAP_CSV)
    print("state_counts:", state_counts)
    print("recommendation_counts:", rec_counts)
    print("")
    print(f"{'SYM':<10} {'STATE':<22} {'REC':<30} {'N':>3} {'ACT':<28} {'DOM':<28} {'SHR':>5} {'WAVG':>6}")
    for r in out_rows:
        print(
            f"{r['symbol']:<10} {r['calibration_state']:<22} {r['recommendation']:<30} "
            f"{r['sample_count']:>3} {str(r['latest_action'] or ''):<28} "
            f"{str(r['dominant_action'] or ''):<28} {r['dominant_action_share']:>5.2f} "
            f"{(r['avg_policy_weight'] if r['avg_policy_weight'] is not None else 0):>6.2f}"
        )

if __name__ == "__main__":
    main()
