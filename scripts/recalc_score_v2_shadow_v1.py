#!/usr/bin/env python3
import json, math, re
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from statistics import mean, median

ROOT = Path(__file__).resolve().parents[1]
WIB = timezone(timedelta(hours=7))
UTC = timezone.utc

SIGNAL_FILE = ROOT / "logs" / "vps_smc_shadow_signals.jsonl"
JOIN_FILE = ROOT / "logs" / "ml_dataset_v3_feature_join.jsonl"

OUT_FILE = ROOT / "logs" / "score_v2_recalc_shadow_v1.jsonl"
OUT_REPORT = ROOT / "reports" / "score_v2_recalc_shadow_v1_report.json"

def read_jsonl(path):
    if not path.exists():
        return []
    out = []
    for line in path.read_text(errors="ignore").splitlines():
        try:
            out.append(json.loads(line))
        except Exception:
            pass
    return out

def num(x):
    try:
        v = float(x)
        if math.isfinite(v):
            return v
    except Exception:
        pass
    return None

def key_of(r):
    return str(r.get("signal_key") or r.get("signal_id") or "")

def direction_of(r):
    return str(r.get("direction") or "").upper()

def latest_by_key(rows):
    out = {}
    for r in rows:
        k = key_of(r)
        if not k:
            continue
        out[k] = r
    return out

def clamp(v, lo=0, hi=100):
    return max(lo, min(hi, v))

def calc_score(sig, feat=None):
    direction = direction_of(sig)
    is_long = direction == "LONG"
    is_short = direction == "SHORT"

    htf_dir = str(sig.get("htf_dir") or "").upper()
    htf_bias = str(sig.get("htf_bias") or "").upper()
    htf_location = str(sig.get("htf_location") or "").upper()
    htf_structure = str(sig.get("htf_structure") or "").upper()
    structure_15m = str(sig.get("structure_15m") or "").upper()

    liq_ctx = sig.get("liq_ctx") if isinstance(sig.get("liq_ctx"), dict) else {}
    liq_app = str(liq_ctx.get("liq_ctx_appstyle") or "").upper()
    at_liq = bool(liq_ctx.get("at_or_near_liq"))

    rr_tp2 = num(sig.get("rr_tp2"))
    fvg_type = str(sig.get("fvg_type") or "").upper()
    sweep_tag = str(sig.get("sweep_tag") or "").upper()
    reclaim_level = sig.get("reclaim_level")

    stageb = sig.get("stageb_retest") if isinstance(sig.get("stageb_retest"), dict) else {}
    retest_ok = bool(stageb.get("has_retest")) and bool(stageb.get("has_rejection_close"))

    plan_ok = sig.get("plan_sanity_ok") is not False
    tp_norm = bool(sig.get("tp_normalized"))

    components = defaultdict(float)
    reasons = []

    def add(comp, pts, reason):
        components[comp] += pts
        reasons.append(f"{reason}:{pts:+.1f}")

    # Base: signal is already CONFIRMED by rule engine.
    add("base", 30, "confirmed_signal_base")

    # HTF.
    if is_long and htf_dir == "LONG":
        add("htf", 10, "htf_dir_align_long")
    elif is_short and htf_dir == "SHORT":
        add("htf", 10, "htf_dir_align_short")
    elif htf_dir in ("NEUTRAL", "", "MIXED"):
        add("htf", 4, "htf_neutral")
    else:
        add("htf", -10, "htf_dir_conflict")

    if is_long and htf_location == "DISCOUNT":
        add("htf", 5, "long_discount")
    elif is_short and htf_location == "PREMIUM":
        add("htf", 5, "short_premium")
    elif htf_location == "EQUILIBRIUM":
        add("htf", 2, "equilibrium_ok")
    elif is_long and htf_location == "PREMIUM":
        add("htf", -4, "long_premium_penalty")
    elif is_short and htf_location == "DISCOUNT":
        add("htf", -4, "short_discount_penalty")

    # Liquidity.
    if at_liq:
        add("liquidity", 10, "at_or_near_liq")
    else:
        add("liquidity", -8, "not_at_liq")

    if liq_app in ("AT_BSL", "AT_SSL"):
        add("liquidity", 4, f"liq_app_{liq_app}")
    elif liq_app.startswith("BELOW") or liq_app.startswith("ABOVE"):
        add("liquidity", -4, f"liq_app_{liq_app}")

    # Trigger quality.
    if sweep_tag:
        add("trigger", 5, "has_sweep")
    if reclaim_level is not None:
        add("trigger", 5, "has_reclaim")
    if fvg_type:
        add("trigger", 6, "has_fvg")
    if retest_ok:
        add("trigger", 8, "retest_rejection_ok")

    # RR.
    if rr_tp2 is None:
        add("rr", -5, "rr_missing")
    elif rr_tp2 >= 3:
        add("rr", 10, "rr_tp2_ge_3")
    elif rr_tp2 >= 2:
        add("rr", 8, "rr_tp2_ge_2")
    elif rr_tp2 >= 1.5:
        add("rr", 5, "rr_tp2_ge_1_5")
    else:
        add("rr", -8, "rr_low")

    if tp_norm:
        add("rr", -2, "tp_normalized_minor_penalty")

    # Structure.
    if is_short and ("BOS_DOWN" in htf_structure or htf_structure == "DOWN"):
        add("structure", 5, "htf_structure_short")
    elif is_long and ("BOS_UP" in htf_structure or htf_structure == "UP"):
        add("structure", 5, "htf_structure_long")

    if is_short and structure_15m in ("DOWN", "RANGE"):
        add("structure", 3, "m15_short_ok")
    elif is_long and structure_15m in ("UP", "RANGE"):
        add("structure", 3, "m15_long_ok")
    elif is_short and structure_15m == "UP":
        add("structure", -5, "m15_short_conflict")
    elif is_long and structure_15m == "DOWN":
        add("structure", -5, "m15_long_conflict")

    # Plan sanity.
    if plan_ok:
        add("quality", 3, "plan_sanity_ok")
    else:
        add("quality", -20, "plan_sanity_bad")

    # Feature overlay, only if joined feature exists.
    if feat:
        fs_sane = feat.get("fs_feature_sanity_ok")
        if fs_sane is True:
            add("feature", 2, "fs_sanity_ok")
        elif fs_sane is False:
            add("feature", -10, "fs_sanity_bad")

        fs_ret = num(feat.get("fs_ret_15m_4"))
        fs_res = num(feat.get("fs_btc_residual_ret_15m_4"))
        fs_volz = num(feat.get("fs_volume_z_5m_20"))

        if fs_ret is not None:
            if is_short and fs_ret < 0:
                add("feature", 3, "fs_momentum_short_align")
            elif is_long and fs_ret > 0:
                add("feature", 3, "fs_momentum_long_align")
            else:
                add("feature", -2, "fs_momentum_not_align")

        if fs_res is not None:
            if is_short and fs_res < 0:
                add("feature", 2, "fs_btc_residual_short_align")
            elif is_long and fs_res > 0:
                add("feature", 2, "fs_btc_residual_long_align")
            else:
                add("feature", -1, "fs_btc_residual_not_align")

        if fs_volz is not None and abs(fs_volz) >= 1.0:
            add("feature", 2, "fs_volume_expansion")

    raw = sum(components.values())
    score = round(clamp(raw), 2)

    if score >= 85:
        bucket = "A"
    elif score >= 75:
        bucket = "B"
    elif score >= 65:
        bucket = "C"
    elif score >= 50:
        bucket = "D"
    else:
        bucket = "REJECT_ZONE"

    return {
        "score_v2_recalc": score,
        "score_v2_bucket": bucket,
        "components": dict(components),
        "reasons": reasons,
    }

def main():
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUT_REPORT.parent.mkdir(parents=True, exist_ok=True)

    signals = latest_by_key(read_jsonl(SIGNAL_FILE))
    joins = latest_by_key(read_jsonl(JOIN_FILE))

    out_rows = []
    for k, sig in signals.items():
        feat = joins.get(k)
        recalc = calc_score(sig, feat)

        row = {
            "dataset_version": "score_v2_recalc_shadow_v1_20260614",
            "created_at_utc": datetime.now(UTC).isoformat(),
            "created_at_wib": datetime.now(UTC).astimezone(WIB).strftime("%Y-%m-%d %H:%M:%S WIB"),
            "signal_key": k,
            "symbol": sig.get("symbol"),
            "direction": sig.get("direction"),
            "signal_time_wib": sig.get("signal_time_wib"),
            "score_v1": sig.get("score"),
            "score_v2_notes": None,
            "score_v2_recalc": recalc["score_v2_recalc"],
            "score_v2_bucket": recalc["score_v2_bucket"],
            "components": recalc["components"],
            "reasons": recalc["reasons"],
            "has_feature_join": feat is not None,
        }

        notes = str(sig.get("notes") or "")
        m = re.search(r"score_v2_shadow=([0-9]+(?:\.[0-9]+)?)", notes)
        if m:
            row["score_v2_notes"] = float(m.group(1))

        out_rows.append(row)

    out_rows.sort(key=lambda x: (x.get("signal_time_wib") or "", x.get("symbol") or ""))

    with OUT_FILE.open("w", encoding="utf-8") as f:
        for r in out_rows:
            f.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n")

    vals = [r["score_v2_recalc"] for r in out_rows if r.get("score_v2_recalc") is not None]
    by_bucket = defaultdict(int)
    by_symbol = defaultdict(list)

    for r in out_rows:
        by_bucket[r["score_v2_bucket"]] += 1
        by_symbol[r["symbol"]].append(r["score_v2_recalc"])

    report = {
        "ok": True,
        "dataset_version": "score_v2_recalc_shadow_v1_20260614",
        "rows": len(out_rows),
        "with_feature_join": sum(1 for r in out_rows if r.get("has_feature_join")),
        "score_avg": round(mean(vals), 4) if vals else None,
        "score_median": round(median(vals), 4) if vals else None,
        "score_min": min(vals) if vals else None,
        "score_max": max(vals) if vals else None,
        "by_bucket": dict(by_bucket),
        "by_symbol": {
            s: {
                "n": len(v),
                "avg": round(mean(v), 4),
                "min": min(v),
                "max": max(v),
            }
            for s, v in sorted(by_symbol.items())
        },
        "out_file": str(OUT_FILE),
    }

    OUT_REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(json.dumps(report, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
