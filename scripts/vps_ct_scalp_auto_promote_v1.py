#!/usr/bin/env python3
import json, os
from pathlib import Path
from datetime import datetime, timezone, timedelta

ROOT = Path(__file__).resolve().parents[1]
REPORT_DIR = ROOT / "reports"
STATE_DIR = ROOT / "state"

OUT_REPORT = REPORT_DIR / "vps_ct_scalp_auto_promote_v1.json"
OUT_STATE = STATE_DIR / "vps_ct_scalp_auto_promote_state.json"
ENV_FILE = ROOT / ".env"

WIB = timezone(timedelta(hours=7))

def env_bool(name, default=False):
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")

def env_int(name, default):
    try:
        return int(str(os.getenv(name, default)).strip())
    except Exception:
        return default

def env_float(name, default):
    try:
        return float(str(os.getenv(name, default)).strip())
    except Exception:
        return default

def read_json(path):
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(errors="ignore"))
    except Exception:
        return {}

def wib_now():
    return datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S WIB")

def set_env_values(values):
    txt = ENV_FILE.read_text(errors="ignore") if ENV_FILE.exists() else ""
    lines = []
    seen = set()

    for line in txt.splitlines():
        if "=" in line and not line.strip().startswith("#"):
            k = line.split("=", 1)[0].strip()
            if k in values:
                lines.append(f"{k}={values[k]}")
                seen.add(k)
            else:
                lines.append(line)
        else:
            lines.append(line)

    for k, v in values.items():
        if k not in seen:
            lines.append(f"{k}={v}")

    backup = ENV_FILE.with_name(f".env.bak_ct_auto_action_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    if ENV_FILE.exists():
        backup.write_text(txt)
    ENV_FILE.write_text("\n".join(lines) + "\n")
    return str(backup)


def read_num(data, key, default, cast=float):
    if not isinstance(data, dict):
        return default
    val = data.get(key)
    if val is None or val == "":
        return default
    try:
        return cast(val)
    except Exception:
        return default


def gate(name, ok, detail):
    return {"name": name, "ok": bool(ok), "detail": detail}

def main():
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    outcome = read_json(REPORT_DIR / "vps_ct_scalp_outcome_eval_v1.json")
    shadow = read_json(REPORT_DIR / "vps_ct_scalp_shadow_report.json")
    orderbook = read_json(REPORT_DIR / "orderbook_pricing_sim_v1.json")

    min_shadow = env_int("VPS_CT_AUTO_MIN_SHADOW_SIGNALS", 20)
    min_closed = env_int("VPS_CT_AUTO_MIN_CLOSED_OUTCOMES", 20)
    min_wr = env_float("VPS_CT_AUTO_MIN_WIN_RATE", 0.70)
    min_exp = env_float("VPS_CT_AUTO_MIN_EXPECTANCY_R", 0.40)
    max_loss = env_int("VPS_CT_AUTO_MAX_CONSEC_LOSS", 2)
    max_nofill = env_float("VPS_CT_AUTO_MAX_NO_FILL_RATE", 0.50)

    shadow_signals = read_num(outcome, "signals_unique", 0, int)
    closed = read_num(outcome, "closed_outcomes", 0, int)
    wr = read_num(outcome, "win_rate", 0.0, float)
    exp = read_num(outcome, "expectancy_R", 0.0, float)

    # Fail-safe only when no closed data exists.
    cons_loss = read_num(outcome, "max_consecutive_loss", 999, int)
    nofill_rate = read_num(outcome, "no_fill_rate", 1.0, float)
    if closed <= 0:
        cons_loss = 999
    if shadow_signals <= 0:
        nofill_rate = 1.0

    ob_counts = orderbook.get("status_counts") or {}
    ob_error = int(ob_counts.get("ERROR") or 0)
    ob_thin = int(ob_counts.get("THIN") or 0)

    gates = [
        gate("shadow_signals_min", shadow_signals >= min_shadow, {"value": shadow_signals, "required": min_shadow}),
        gate("closed_outcomes_min", closed >= min_closed, {"value": closed, "required": min_closed}),
        gate("win_rate_min", wr >= min_wr, {"value": wr, "required": min_wr}),
        gate("expectancy_R_min", exp >= min_exp, {"value": exp, "required": min_exp}),
        gate("max_consecutive_loss", cons_loss <= max_loss, {"value": cons_loss, "max_allowed": max_loss}),
        gate("no_fill_rate_max", nofill_rate <= max_nofill, {"value": nofill_rate, "max_allowed": max_nofill}),
        gate("orderbook_no_error", ob_error == 0, {"ERROR": ob_error}),
        gate("orderbook_no_thin", ob_thin == 0, {"THIN": ob_thin}),
    ]

    ready = all(g["ok"] for g in gates)
    auto_promote = env_bool("VPS_CT_AUTO_PROMOTE_ENABLED", True)
    auto_demote = env_bool("VPS_CT_AUTO_DEMOTE_ENABLED", True)

    current_mode = os.getenv("VPS_CT_SCALP_MODE", "SHADOW_ONLY")
    action = "NO_ACTION"
    backup = None

    if ready and auto_promote:
        backup = set_env_values({
            "VPS_CT_SCALP_MODE": "LIVE_BLOCKED_BY_DEFAULT",
            "VPS_CT_SCALP_LIVE_ARMED": "true",
            "VPS_CT_SCALP_REQUIRE_ORDERBOOK_OK": "true",
            "VPS_CT_SCALP_RISK_MULT": os.getenv("VPS_CT_SCALP_LIVE_RISK_MULT", "0.10"),
            "VPS_CT_SCALP_AUTO_PROMOTED_AT_WIB": wib_now(),
        })
        action = "AUTO_PROMOTED_TO_LIVE_BLOCKED_BY_DEFAULT"

    elif (not ready) and auto_demote and str(current_mode).upper() != "SHADOW_ONLY":
        backup = set_env_values({
            "VPS_CT_SCALP_MODE": "SHADOW_ONLY",
            "VPS_CT_SCALP_LIVE_ARMED": "false",
            "VPS_CT_SCALP_AUTO_DEMOTED_AT_WIB": wib_now(),
        })
        action = "AUTO_DEMOTED_TO_SHADOW_ONLY"

    report = {
        "ok": True,
        "report_version": "vps_ct_scalp_auto_promote_v1_20260615",
        "created_at_wib": wib_now(),
        "ready_for_activation": ready,
        "auto_promote_enabled": auto_promote,
        "auto_demote_enabled": auto_demote,
        "previous_mode": current_mode,
        "action": action,
        "env_backup": backup,
        "summary": {
            "shadow_signals": shadow_signals,
            "closed_outcomes": closed,
            "win_rate": wr,
            "expectancy_R": exp,
            "max_consecutive_loss": cons_loss,
            "no_fill_rate": nofill_rate,
            "orderbook_ERROR": ob_error,
            "orderbook_THIN": ob_thin,
        },
        "gates": gates,
        "notes": [
            "This auto-promotes only to LIVE_BLOCKED_BY_DEFAULT.",
            "It does not bypass global LIVE_TRADING_ENABLED, LIVE_GO_CONFIRM, kill switch, ML gate, cooldown, or max position rules.",
            "If gates fail after promotion and auto-demote is enabled, it returns CT scalp to SHADOW_ONLY.",
        ],
    }

    OUT_REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2))
    OUT_STATE.write_text(json.dumps(report, ensure_ascii=False, indent=2))

    print("=== VPS CT SCALP AUTO PROMOTE V1 ===")
    print(json.dumps({
        "ready_for_activation": ready,
        "action": action,
        "summary": report["summary"],
    }, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
