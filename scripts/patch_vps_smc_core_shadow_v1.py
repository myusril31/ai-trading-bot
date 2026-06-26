from pathlib import Path

path = Path('app/vps_smc.py')
if not path.exists():
    raise SystemExit('app/vps_smc.py not found')
text = path.read_text(encoding='utf-8', errors='ignore')
if 'def _vps_smc_log_core_candidate_shadow_v1(' not in text:
    marker = '\ndef vps_smc_status() -> Dict[str, Any]:'
    if marker not in text:
        raise SystemExit('insert marker def vps_smc_status not found')
    helper = r'''

def _vps_smc_log_core_candidate_shadow_v1(result: Dict[str, Any]) -> None:
    """Emit relaxed Core SMC candidate telemetry in SHADOW ONLY mode.

    This does not route anything to the execution bridge.  It exists to measure
    whether relaxed SMC core candidates appear earlier than current strict
    CONFIRMED signals and whether RR decays before pre-entry.
    """
    if not _env_bool("VPS_SMC_CORE_SHADOW_ENABLED", False):
        return
    try:
        symbol = str(result.get("symbol") or "").upper().strip()
        if not symbol:
            return
        stageb = result.get("stageb_confirmation") or {}
        entry_sweep = result.get("entry_sweep") or {}
        liq_ctx = result.get("liq_ctx") or {}
        htf_gate = result.get("htf_gate") or {}
        reclaim = stageb.get("stageb_reclaim") or {}

        direction = str(
            stageb.get("selected_direction")
            or stageb.get("stageb_direction")
            or entry_sweep.get("selected_direction")
            or entry_sweep.get("selected_direction_appstyle")
            or "NONE"
        ).upper()
        if direction not in ("LONG", "SHORT"):
            return

        sweep_t = (
            stageb.get("selected_sweep_t")
            or entry_sweep.get("selected_sweep_t")
            or entry_sweep.get("sweep_t")
            or liq_ctx.get("sweep_t")
        )
        sweep_level = (
            stageb.get("selected_sweep_level")
            or entry_sweep.get("selected_sweep_level")
            or entry_sweep.get("sweep_level")
            or liq_ctx.get("sweep_level")
            or liq_ctx.get("nearest_liq_price")
        )
        sweep_extreme = (
            stageb.get("selected_sweep_extreme")
            or entry_sweep.get("selected_sweep_extreme")
            or entry_sweep.get("sweep_extreme")
            or liq_ctx.get("sweep_extreme")
        )
        reclaim_t = reclaim.get("reclaim_t") or stageb.get("reclaim_t")
        reclaim_level = reclaim.get("reclaim_level") or sweep_level
        has_reclaim = bool(reclaim.get("has_reclaim") or reclaim_t)

        latest_close = result.get("close_15m_used")
        try:
            entry_mid = float(latest_close)
            sweep_extreme_f = float(sweep_extreme)
        except Exception:
            entry_mid = None
            sweep_extreme_f = None

        buffer_pct = _env_float("VPS_SMC_INVALID_BUFFER_PCT", 0.08) + _env_float("VPS_SMC_FEES_BUFFER_PCT", 0.03)
        buffer_mult = buffer_pct / 100.0
        sl = None
        risk = None
        target_r = _env_float("CORE_SMC_TARGET_R", _env_float("RR_TARGET_R", 1.2))
        if entry_mid is not None and sweep_extreme_f is not None:
            sl = sweep_extreme_f * (1 - buffer_mult) if direction == "LONG" else sweep_extreme_f * (1 + buffer_mult)
            risk = (entry_mid - sl) if direction == "LONG" else (sl - entry_mid)
        rr_at_emit = None
        tp1 = None
        if risk is not None and risk > 0:
            if direction == "LONG":
                raw_tp1 = liq_ctx.get("last_buy_side_liq")
                try:
                    tp1 = float(raw_tp1) if raw_tp1 is not None and float(raw_tp1) > entry_mid else entry_mid + target_r * risk
                except Exception:
                    tp1 = entry_mid + target_r * risk
                rr_at_emit = (tp1 - entry_mid) / risk
            else:
                raw_tp1 = liq_ctx.get("last_sell_side_liq")
                try:
                    tp1 = float(raw_tp1) if raw_tp1 is not None and float(raw_tp1) < entry_mid else entry_mid - target_r * risk
                except Exception:
                    tp1 = entry_mid - target_r * risk
                rr_at_emit = (entry_mid - tp1) / risk

        htf_bias = str(htf_gate.get("htf_bias") or "UNKNOWN").upper()
        htf_structure = str(htf_gate.get("htf_structure") or "UNKNOWN").upper()
        htf_hard_extreme_block = bool(
            (direction == "LONG" and htf_bias in ("HARD_BEARISH", "HARD_BEARISH_EXTREME"))
            or (direction == "SHORT" and htf_bias in ("HARD_BULLISH", "HARD_BULLISH_EXTREME"))
        )
        geometry_valid = bool(entry_mid is not None and sl is not None and tp1 is not None and risk is not None and risk > 0)
        core_ok = bool(sweep_t is not None and sweep_level is not None and has_reclaim and geometry_valid and not htf_hard_extreme_block)
        min_rr = _env_float("CORE_SMC_MIN_RR_AT_EMIT", 1.0)
        rr_ok = bool(rr_at_emit is not None and float(rr_at_emit) >= min_rr)
        state = "CORE_CANDIDATE" if core_ok and rr_ok else "CORE_INVALID"

        bucket_src = reclaim_t or sweep_t or result.get("latest_entry_close_time_ms")
        bucket_ms = _bucket_ms(bucket_src, _env_int("VPS_SMC_DEDUP_BUCKET_MIN", 15))
        candidate_id = f"{symbol}|{direction}|CORE|{bucket_ms or sweep_t or result.get('latest_entry_close_time_ms')}"
        poi = stageb.get("stageb_fvg_poi") or {}
        disp = stageb.get("stageb_displacement") or {}
        row = {
            "created_at_utc": _utc_now_iso(),
            "event_type": "SMC_CORE_CANDIDATE_SHADOW_V1",
            "source": "VPS_SMC",
            "mode": "SHADOW_ONLY",
            "execution_allowed": False,
            "candidate_id": candidate_id,
            "symbol": symbol,
            "direction": direction,
            "state": state,
            "core_ok": core_ok,
            "rr_ok": rr_ok,
            "min_rr_at_emit": min_rr,
            "target_r": target_r,
            "rr_target_rewrite_expected": _env_float("RR_TARGET_R", 1.2),
            "entry_mid": entry_mid,
            "sl": sl,
            "tp1": tp1,
            "risk": risk,
            "rr_at_emit": rr_at_emit,
            "sweep_t": sweep_t,
            "sweep_t_wib": _bucket_ms_to_wib_text(sweep_t) if sweep_t is not None else None,
            "sweep_level": sweep_level,
            "sweep_extreme": sweep_extreme,
            "reclaim_t": reclaim_t,
            "reclaim_t_wib": _bucket_ms_to_wib_text(reclaim_t) if reclaim_t is not None else None,
            "reclaim_level": reclaim_level,
            "has_reclaim": has_reclaim,
            "htf_bias": htf_bias,
            "htf_structure": htf_structure,
            "htf_hard_extreme_block": htf_hard_extreme_block,
            "structure_15m": result.get("structure_15m"),
            "fvg_present": bool(poi.get("has_fvg") or poi.get("fvg_lo") is not None or poi.get("fvg_hi") is not None),
            "fvg_type": poi.get("fvg_type"),
            "fvg_lo": poi.get("fvg_lo"),
            "fvg_hi": poi.get("fvg_hi"),
            "displacement_present": bool(disp.get("has_displacement")),
            "liq_dist_to_zone_pct": liq_ctx.get("dist_to_zone_pct"),
            "strict_shadow_state": result.get("shadow_state"),
            "strict_confirm_reason": stageb.get("stageb_confirm_reason"),
            "strict_invalid_reason": stageb.get("stageb_invalid_reason"),
        }
        _append_jsonl(_log_dir() / "core_smc_shadow_candidates_v1.jsonl", row)
    except Exception as exc:
        _append_jsonl(_log_dir() / "vps_smc_errors.jsonl", {
            "created_at_utc": _utc_now_iso(),
            "event_type": "CORE_SMC_SHADOW_LOG_ERROR",
            "symbol": result.get("symbol"),
            "error": str(exc),
        })
'''
    text = text.replace(marker, helper + marker, 1)

old = '        result["signal_skip_reason"] = "not_confirmed"\n        if str(result.get("shadow_state") or "") != "CONFIRMED":\n            continue\n'
new = '        result["signal_skip_reason"] = "not_confirmed"\n        try:\n            _vps_smc_log_core_candidate_shadow_v1(result)\n        except Exception:\n            pass\n        if str(result.get("shadow_state") or "") != "CONFIRMED":\n            continue\n'
if old not in text:
    if '_vps_smc_log_core_candidate_shadow_v1(result)' not in text:
        raise SystemExit('loop insert marker not found')
else:
    text = text.replace(old, new, 1)

path.write_text(text, encoding='utf-8')
print('[patch] app/vps_smc.py core shadow logger installed')
