#!/usr/bin/env python3
import json, os
from pathlib import Path
from datetime import datetime, timezone, timedelta

import sys
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
# === CT_SCALP_SMC_IMPORT_REMOVED_20260628_FIXED ===
class _RemovedSmc:
    def __getattr__(self, name):
        def _disabled(*args, **kwargs):
            return {
                "ok": False,
                "disabled": True,
                "reason": "smc_runtime_removed_stat_tech_primary",
                "replacement": "STAT_TECH_V1",
                "function": name,
            }
        return _disabled

smc = _RemovedSmc()
ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "logs"
REPORT_DIR = ROOT / "reports"

IN_LOG = LOG_DIR / "vps_ct_scalp_shadow_signals.jsonl"
OUT_LOG = LOG_DIR / "vps_ct_scalp_outcomes.jsonl"
OUT_REPORT = REPORT_DIR / "vps_ct_scalp_outcome_eval_v1.json"

WIB = timezone(timedelta(hours=7))

def env_int(name, default):
    try:
        return int(str(os.getenv(name, default)).strip())
    except Exception:
        return default

def utc_ms(dt):
    return int(dt.timestamp() * 1000)

def parse_dt(raw):
    if not raw:
        return None
    try:
        txt = str(raw).replace("Z", "+00:00")
        dt = datetime.fromisoformat(txt)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def read_jsonl(path):
    if not path.exists():
        return []
    out = []
    for line in path.read_text(errors="ignore").splitlines():
        try:
            j = json.loads(line)
            if isinstance(j, dict):
                out.append(j)
        except Exception:
            pass
    return out

def wib_text(ms):
    try:
        return datetime.fromtimestamp(int(ms)/1000, timezone.utc).astimezone(WIB).strftime("%Y-%m-%d %H:%M:%S WIB")
    except Exception:
        return None

def candle_t(c):
    try:
        return int(c.get("t") or c.get("tBucketMs") or 0)
    except Exception:
        return 0

def eval_signal(sig):
    symbol = str(sig.get("symbol") or "").upper()
    direction = str(sig.get("direction") or "").upper()
    key = str(sig.get("signal_key") or "")

    plan = sig.get("plan") or {}
    try:
        entry = float(plan.get("entry_mid"))
        sl = float(plan.get("sl"))
        tp1 = float(plan.get("tp1"))
        tp2 = float(plan.get("tp2"))
        tp3 = float(plan.get("tp3"))
    except Exception:
        return {"signal_key": key, "symbol": symbol, "status": "BAD_PLAN", "label_win": None, "label_R": None}

    created = parse_dt(sig.get("created_at_utc"))
    if created is None:
        return {"signal_key": key, "symbol": symbol, "status": "BAD_TIME", "label_win": None, "label_R": None}

    start_ms = utc_ms(created)
    max_hours = env_int("VPS_CT_OUTCOME_MAX_HOURS", 24)
    end_ms = start_ms + max_hours * 60 * 60 * 1000

    candles = smc._load_internal_candles(symbol, os.getenv("VPS_SMC_INTERVAL_STAGEB", "5m"))
    future = [c for c in candles if candle_t(c) >= start_ms and candle_t(c) <= end_ms]

    if not future:
        return {
            "signal_key": key,
            "symbol": symbol,
            "direction": direction,
            "status": "NO_CANDLES_AFTER_SIGNAL",
            "label_win": None,
            "label_R": None,
        }

    fill_t = None
    filled = False

    for c in future:
        h = float(c["h"])
        l = float(c["l"])
        t = candle_t(c)

        if l <= entry <= h:
            fill_t = t
            filled = True
            break

    if not filled:
        return {
            "signal_key": key,
            "symbol": symbol,
            "direction": direction,
            "status": "NO_FILL",
            "entry": entry,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "signal_time_wib": sig.get("created_at_wib"),
            "label_win": None,
            "label_R": None,
        }

    after_fill = [c for c in future if candle_t(c) >= fill_t]

    best_status = "OPEN"
    best_R = None
    label_win = None
    exit_t = None
    exit_price = None

    for c in after_fill:
        h = float(c["h"])
        l = float(c["l"])
        t = candle_t(c)

        if direction == "LONG":
            # Conservative same-candle collision: SL first.
            if l <= sl:
                best_status, best_R, label_win, exit_t, exit_price = "SL", -1.0, False, t, sl
                break
            if h >= tp3:
                best_status, best_R, label_win, exit_t, exit_price = "TP3", 2.5, True, t, tp3
                break
            if h >= tp2:
                best_status, best_R, label_win, exit_t, exit_price = "TP2", 1.5, True, t, tp2
                break
            if h >= tp1:
                best_status, best_R, label_win, exit_t, exit_price = "TP1", 1.0, True, t, tp1
                break

        elif direction == "SHORT":
            # Conservative same-candle collision: SL first.
            if h >= sl:
                best_status, best_R, label_win, exit_t, exit_price = "SL", -1.0, False, t, sl
                break
            if l <= tp3:
                best_status, best_R, label_win, exit_t, exit_price = "TP3", 2.5, True, t, tp3
                break
            if l <= tp2:
                best_status, best_R, label_win, exit_t, exit_price = "TP2", 1.5, True, t, tp2
                break
            if l <= tp1:
                best_status, best_R, label_win, exit_t, exit_price = "TP1", 1.0, True, t, tp1
                break

    return {
        "signal_key": key,
        "symbol": symbol,
        "direction": direction,
        "status": best_status,
        "label_win": label_win,
        "label_R": best_R,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "score": sig.get("score"),
        "priority": sig.get("priority"),
        "risk_mult": sig.get("risk_mult"),
        "orderbook_status": sig.get("orderbook_status"),
        "signal_time_wib": sig.get("created_at_wib"),
        "fill_t": fill_t,
        "fill_t_wib": wib_text(fill_t),
        "exit_t": exit_t,
        "exit_t_wib": wib_text(exit_t),
        "exit_price": exit_price,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }

def max_consecutive_losses(rows):
    cur = 0
    mx = 0
    for r in rows:
        if r.get("status") == "SL":
            cur += 1
            mx = max(mx, cur)
        elif r.get("status") in ("TP1", "TP2", "TP3"):
            cur = 0
    return mx

def main():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    raw = read_jsonl(IN_LOG)
    by_key = {}
    for r in raw:
        k = str(r.get("signal_key") or "")
        if k:
            by_key[k] = r

    signals = list(by_key.values())
    outcomes = [eval_signal(s) for s in signals]

    OUT_LOG.write_text(
        "\n".join(json.dumps(x, ensure_ascii=False, separators=(",", ":")) for x in outcomes) + ("\n" if outcomes else ""),
        encoding="utf-8"
    )

    closed = [r for r in outcomes if r.get("status") in ("TP1", "TP2", "TP3", "SL")]
    wins = [r for r in closed if r.get("label_win") is True]
    losses = [r for r in closed if r.get("status") == "SL"]
    nofill = [r for r in outcomes if r.get("status") == "NO_FILL"]

    n_closed = len(closed)
    win_rate = (len(wins) / n_closed) if n_closed else 0.0
    expectancy = (sum(float(r.get("label_R") or 0) for r in closed) / n_closed) if n_closed else 0.0
    nofill_rate = (len(nofill) / len(outcomes)) if outcomes else 0.0

    report = {
        "ok": True,
        "report_version": "vps_ct_scalp_outcome_eval_v1_20260615",
        "created_at_wib": datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S WIB"),
        "signals_unique": len(signals),
        "outcomes_total": len(outcomes),
        "closed_outcomes": n_closed,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(win_rate, 6),
        "expectancy_R": round(expectancy, 6),
        "no_fill_count": len(nofill),
        "no_fill_rate": round(nofill_rate, 6),
        "max_consecutive_loss": max_consecutive_losses(outcomes),
        "status_counts": {},
    }

    for r in outcomes:
        s = str(r.get("status") or "UNKNOWN")
        report["status_counts"][s] = report["status_counts"].get(s, 0) + 1

    OUT_REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2))

    print("=== VPS CT SCALP OUTCOME EVAL V1 ===")
    print(json.dumps(report, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
