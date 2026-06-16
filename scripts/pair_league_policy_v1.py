#!/usr/bin/env python3
import json, csv, math
from pathlib import Path
from datetime import datetime, timezone, timedelta

ROOT = Path(__file__).resolve().parents[1]
WIB = timezone(timedelta(hours=7))
UTC = timezone.utc

IN_FILE = ROOT / "reports" / "pair_league_v1.json"
OUT_JSON = ROOT / "reports" / "pair_league_policy_v1.json"
OUT_CSV = ROOT / "reports" / "pair_league_policy_v1.csv"

POLICY_VERSION = "pair_league_policy_v1_1_20260614"
MODE = "REPORT_ONLY"

def num(x):
    try:
        v = float(x)
        if math.isfinite(v):
            return v
    except Exception:
        pass
    return None

def clamp(v, lo=0.20, hi=1.00):
    return max(lo, min(hi, v))

def base_weight(status):
    status = str(status or "").upper()
    if status == "CORE":
        return 1.00
    if status == "CORE_SHADOW":
        return 0.90
    if status == "ACTIVE":
        return 0.85
    if status == "WATCH":
        return 0.60
    if status == "DEGRADED":
        return 0.40
    if status == "BENCHED":
        return 0.20
    return 0.50

def action_from_weight(w):
    if w >= 0.95:
        return "PRIORITY_SCAN"
    if w >= 0.75:
        return "NORMAL_SCAN"
    if w >= 0.55:
        return "WATCH_SCAN"
    if w >= 0.35:
        return "LOW_PRIORITY_SCAN"
    return "BENCH_CANDIDATE_REPORT_ONLY"

def build_policy(row):
    symbol = row.get("symbol")
    status = row.get("league_status")

    league_score = num(row.get("league_score"))
    avg_score = num(row.get("avg_score_v2_recalc"))
    outcome_count = int(row.get("outcome_count") or 0)
    joined_rows = int(row.get("joined_feature_rows") or 0)
    ml_rows = int(row.get("ml_rows") or 0)
    avg_pwin = num(row.get("avg_ml_p_win"))
    win_rate = num(row.get("win_rate"))
    exp_r = num(row.get("expectancy_R"))
    fs_sane = row.get("fs_feature_sanity_ok")

    w = base_weight(status)
    reasons = [f"base_status_{status}:{w:.2f}"]

    # League score adjustment.
    if league_score is not None:
        if league_score >= 85:
            w += 0.05
            reasons.append("league_score_ge_85:+0.05")
        elif league_score < 65:
            w -= 0.10
            reasons.append("league_score_lt_65:-0.10")
        elif league_score < 70:
            w -= 0.05
            reasons.append("league_score_lt_70:-0.05")

    # Recalc score adjustment.
    if avg_score is not None:
        if avg_score >= 80:
            w += 0.04
            reasons.append("avg_score_v2_ge_80:+0.04")
        elif avg_score < 65:
            w -= 0.06
            reasons.append("avg_score_v2_lt_65:-0.06")

    # Feature join confidence.
    if joined_rows <= 0:
        w -= 0.05
        reasons.append("no_recent_feature_join:-0.05")
    elif joined_rows >= 3:
        w += 0.03
        reasons.append("recent_feature_join_ge_3:+0.03")

    # ML gate evidence, only if we have rows.
    if ml_rows > 0 and avg_pwin is not None:
        if avg_pwin >= 0.74:
            w += 0.05
            reasons.append("ml_avg_pwin_pass:+0.05")
        else:
            w -= 0.15
            reasons.append("ml_avg_pwin_below_074:-0.15")

    # Outcome quality.
    if outcome_count >= 20:
        if win_rate is not None and win_rate < 0.65:
            w -= 0.12
            reasons.append("wr_lt_65_with_sample:-0.12")
        if exp_r is not None and exp_r < 0.40:
            w -= 0.08
            reasons.append("expectancy_lt_040_with_sample:-0.08")
        if win_rate is not None and win_rate >= 0.80 and exp_r is not None and exp_r >= 0.60:
            w += 0.06
            reasons.append("strong_outcome_quality:+0.06")
    else:
        # === PAIR_LEAGUE_POLICY_V1_1_SAMPLE_SMOOTH_20260614 ===
        # Smooth sample confidence. No stupid cliff at exactly 20 outcomes.
        if outcome_count < 5:
            if w > 0.70:
                reasons.append("outcome_sample_lt_5_cap_070")
            w = min(w, 0.70)
        elif outcome_count < 10:
            if w > 0.85:
                reasons.append("outcome_sample_5_9_cap_085")
            w = min(w, 0.85)
        elif outcome_count < 20:
            sample_penalty = round((20 - outcome_count) / 20.0 * 0.04, 4)
            if sample_penalty > 0:
                w -= sample_penalty
                reasons.append(f"outcome_sample_10_19_soft_penalty:-{sample_penalty:.4f}")

    # Feature sanity cap.
    if fs_sane is False:
        w = min(w, 0.40)
        reasons.append("feature_sanity_bad_cap_040")

    w = round(clamp(w), 4)
    action = action_from_weight(w)

    return {
        "symbol": symbol,
        "mode": MODE,
        "league_status": status,
        "league_score": league_score,
        "policy_weight": w,
        "policy_action": action,
        "signal_count": row.get("signal_count"),
        "joined_feature_rows": joined_rows,
        "outcome_count": outcome_count,
        "win_rate": win_rate,
        "expectancy_R": exp_r,
        "avg_score_v2_recalc": avg_score,
        "avg_ml_p_win": avg_pwin,
        "ml_rows": ml_rows,
        "ml_pass_count": row.get("ml_pass_count"),
        "fs_feature_sanity_ok": fs_sane,
        "reasons": reasons,
    }

def main():
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)

    if not IN_FILE.exists():
        raise SystemExit(f"missing input: {IN_FILE}")

    league = json.loads(IN_FILE.read_text(errors="ignore"))
    rows = league.get("rows") or []

    policies = [build_policy(r) for r in rows]
    policies.sort(key=lambda r: (-r["policy_weight"], -float(r.get("league_score") or 0), r["symbol"]))

    now = datetime.now(UTC).astimezone(WIB)

    report = {
        "ok": True,
        "policy_version": POLICY_VERSION,
        "mode": MODE,
        "created_at_wib": now.strftime("%Y-%m-%d %H:%M:%S WIB"),
        "source_report_version": league.get("report_version"),
        "source_created_at_wib": league.get("created_at_wib"),
        "note": "REPORT_ONLY. Does not modify allowlist, scanner, execution, or live gates.",
        "rows": policies,
    }

    OUT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2))

    cols = [
        "symbol","mode","league_status","league_score","policy_weight","policy_action",
        "signal_count","joined_feature_rows","outcome_count","win_rate","expectancy_R",
        "avg_score_v2_recalc","ml_rows","avg_ml_p_win","ml_pass_count","fs_feature_sanity_ok",
    ]

    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in policies:
            w.writerow({c: r.get(c) for c in cols})

    print(f"=== PAIR LEAGUE POLICY V1 | {MODE} ===")
    print("out_json:", OUT_JSON)
    print("out_csv :", OUT_CSV)
    print("")
    print(f"{'SYM':<10} {'STATUS':<12} {'LGS':>6} {'WGT':>5} {'ACTION':<28} {'OUT':>4} {'WR':>7} {'EXP':>7} {'ML':>3} {'PWIN':>7}")
    for r in policies:
        wr = "NA" if r["win_rate"] is None else f"{r['win_rate']:.3f}"
        exp = "NA" if r["expectancy_R"] is None else f"{r['expectancy_R']:.3f}"
        pwin = "NA" if r["avg_ml_p_win"] is None else f"{r['avg_ml_p_win']:.3f}"
        print(
            f"{r['symbol']:<10} {r['league_status']:<12} {r['league_score']:>6.2f} "
            f"{r['policy_weight']:>5.2f} {r['policy_action']:<28} "
            f"{r['outcome_count']:>4} {wr:>7} {exp:>7} {r['ml_rows']:>3} {pwin:>7}"
        )

if __name__ == "__main__":
    main()
