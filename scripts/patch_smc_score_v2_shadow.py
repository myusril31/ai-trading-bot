#!/usr/bin/env python3
from pathlib import Path
from datetime import datetime

root = Path(".")
targets = []

for p in root.rglob("*.py"):
    if str(p).startswith("scripts/"):
        continue
    try:
        txt = p.read_text(errors="ignore")
    except Exception:
        continue
    if "def _build_plan_and_score" in txt and "score = 60" in txt and "base_60" in txt:
        targets.append(p)

if not targets:
    raise SystemExit("[score_v2_patch] target not found")

path = targets[0]
text = path.read_text(errors="ignore")

if "SMC_SCORE_V2_SHADOW_FREQTRADE_ADOPT_20260614" in text:
    print("[score_v2_patch] already patched:", path)
    raise SystemExit(0)

old_return = '    return "VALID", plan, {"score": score, "priority": priority, "risk_mult": risk_mult, "reasons": reasons}, None'
idx = text.find(old_return)

if idx < 0:
    raise SystemExit("[score_v2_patch] return block not found")

insert = r'''
    # === SMC_SCORE_V2_SHADOW_FREQTRADE_ADOPT_20260614 ===
    # Freqtrade/FreqAI-inspired scoring:
    # keep legacy score active for safety, but compute score_v2 + components for audit.
    components = {}
    score_v2_reasons = ["score_v2_shadow_freqtrade_adopt"]

    def _add_comp(name, value, reason):
        try:
            value = float(value)
        except Exception:
            value = 0.0
        components[name] = components.get(name, 0.0) + value
        score_v2_reasons.append(f"{reason}:{value:+.1f}")

    # --- HTF component, max-ish 20 ---
    if htf_gate == "PASS":
        _add_comp("htf", 10, "htf_pass")
    else:
        _add_comp("penalty", -12, "htf_not_pass")

    try:
        if str(htf_bias).lower() == str(direction).lower():
            _add_comp("htf", 8, "htf_bias_align")
        elif str(htf_bias).lower() in ("long", "short"):
            _add_comp("penalty", -10, "htf_bias_conflict")
    except Exception:
        pass

    if htf_loc == "MIXED_RANGE":
        _add_comp("htf", 3, "htf_mixed_range")

    # --- Liquidity component, max-ish 15 ---
    if liq_decision == "PASS":
        _add_comp("liquidity", 12, "liq_pass")
    elif liq_decision == "BLOCK":
        _add_comp("penalty", -12, "liq_block")
    else:
        _add_comp("liquidity", 4, "liq_neutral")

    # --- Trigger component, max-ish 25 ---
    if reclaim_ok:
        _add_comp("trigger", 7, "reclaim_ok")
    else:
        _add_comp("penalty", -4, "reclaim_missing")

    if displacement_ok:
        _add_comp("trigger", 7, "displacement_ok")
    else:
        _add_comp("penalty", -3, "displacement_missing")

    if fvg_ok:
        _add_comp("trigger", 8, "fvg_ok")
    else:
        _add_comp("penalty", -5, "fvg_missing")

    # --- RR component, max-ish 15 ---
    try:
        rr2 = float(rr_tp2)
    except Exception:
        rr2 = 0.0

    if rr2 >= 2.0:
        _add_comp("rr", 15, "rr_tp2_ge_2")
    elif rr2 >= 1.5:
        _add_comp("rr", 11, "rr_tp2_ge_1_5")
    elif rr2 >= 1.2:
        _add_comp("rr", 7, "rr_tp2_ge_1_2")
    elif rr2 >= 1.0:
        _add_comp("rr", 3, "rr_tp2_ge_1")
    else:
        _add_comp("penalty", -12, "rr_tp2_bad")

    # --- Structure component, max-ish 10 ---
    try:
        structure_bias = str((structure_ctx or {}).get("bias") or "").lower()
        if structure_bias == str(direction).lower():
            _add_comp("structure", 8, "structure_align")
        elif structure_bias in ("long", "short"):
            _add_comp("penalty", -8, "structure_conflict")
    except Exception:
        pass

    # --- Quality component, max-ish 10 ---
    try:
        entry_mid_safe = float(entry_mid)
        zone_width_pct = abs(float(entry_hi) - float(entry_lo)) / entry_mid_safe * 100.0 if entry_mid_safe else 999.0
        sl_dist_pct = abs(float(risk)) / entry_mid_safe * 100.0 if entry_mid_safe else 999.0

        if zone_width_pct <= 0.20:
            _add_comp("quality", 5, "tight_entry_zone")
        elif zone_width_pct <= 0.50:
            _add_comp("quality", 3, "ok_entry_zone")
        else:
            _add_comp("penalty", -6, "wide_entry_zone")

        if sl_dist_pct <= 1.50:
            _add_comp("quality", 5, "compact_sl")
        elif sl_dist_pct <= 3.00:
            _add_comp("quality", 2, "medium_sl")
        else:
            _add_comp("penalty", -5, "wide_sl")

    except Exception as e:
        score_v2_reasons.append(f"quality_calc_error:{type(e).__name__}")

    score_v2_raw = sum(components.values())
    score_v2 = int(round(max(0.0, min(100.0, score_v2_raw))))

    # Legacy score remains active for now.
    score_v1 = score
    active_score = score_v1

    score_detail_v2 = {
        "score_version": "shadow_v2_freqtrade_adopt_20260614",
        "score_v1": score_v1,
        "score_v2": score_v2,
        "active_score": active_score,
        "components": components,
        "reasons_v2": score_v2_reasons,
    }

    reasons.append(f"score_v2_shadow={score_v2}")
'''

text = text[:idx] + insert + "\n" + text[idx:]

# Replace return so score_detail includes v2 shadow fields.
text = text.replace(
    old_return,
    '    score_detail = {"score": score, "priority": priority, "risk_mult": risk_mult, "reasons": reasons, **score_detail_v2}\n'
    '    return "VALID", plan, score_detail, None',
    1,
)

backup = path.with_suffix(path.suffix + f".bak_score_v2_shadow_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
backup.write_text(path.read_text(errors="ignore"))
path.write_text(text)

print("[score_v2_patch] patched:", path)
print("[score_v2_patch] backup:", backup)
