#!/usr/bin/env python3
import json
import os
import re
import subprocess
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta

ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / ".env"
STATE_DIR = ROOT / "state" / "market_data"
WIB = timezone(timedelta(hours=7))

INTERVAL_MS = {
    "5m": 5 * 60 * 1000,
    "15m": 15 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
}

def log(msg):
    print(str(msg), flush=True)

def ts():
    return datetime.now(WIB).strftime("%Y-%m-%dT%H:%M:%S+07:00")

def load_env():
    env = {}
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text(errors="ignore").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    return env

ENV = load_env()

def env_get(k, default=""):
    return os.getenv(k) or ENV.get(k) or str(default)

def env_int(k, default):
    try:
        return int(str(env_get(k, default)).strip())
    except Exception:
        return int(default)

def norm_symbol(x):
    x = str(x or "").strip().upper()
    x = x.replace("BINANCE:", "").replace(".P", "")
    return re.sub(r"[^A-Z0-9]", "", x)

def env_symbols(k, default=""):
    raw = env_get(k, default)
    out = []
    for part in raw.split(","):
        s = norm_symbol(part)
        if s and s not in out:
            out.append(s)
    return out

def symbols_from_env():
    symbols = env_symbols("PAIR_ALLOWLIST", "")
    maxn = env_int("VPS_SMC_MAX_SYMBOLS_PER_RUN", 14)
    return symbols[:maxn] if maxn > 0 else symbols

def ms_to_wib(ms):
    if not ms:
        return "-"
    return datetime.fromtimestamp(ms / 1000, timezone.utc).astimezone(WIB).strftime("%Y-%m-%d %H:%M:%S WIB")

def close_ms_from_row(row, tf):
    for k in ("close_time_ms", "closeTime", "t_close"):
        v = row.get(k)
        if v:
            return int(v)

    for k in ("open_time_ms", "openTime", "t"):
        v = row.get(k)
        if v:
            return int(v) + INTERVAL_MS[tf] - 1000

    return 0

def latest_candle(symbol, tf):
    path = STATE_DIR / f"{symbol}_{tf}.jsonl"

    if not path.exists():
        return None, f"{symbol} {tf} MISSING_FILE"

    lines = [x for x in path.read_text(errors="ignore").splitlines() if x.strip()]
    if not lines:
        return None, f"{symbol} {tf} EMPTY_FILE"

    rows = []
    for line in lines[-20:]:
        try:
            rows.append(json.loads(line))
        except Exception:
            pass

    if not rows:
        return None, f"{symbol} {tf} BAD_JSON"

    rows = sorted(
        rows,
        key=lambda r: int(r.get("open_time_ms") or r.get("openTime") or r.get("t") or 0)
    )

    latest = rows[-1]
    c_ms = close_ms_from_row(latest, tf)

    if not c_ms:
        return None, f"{symbol} {tf} NO_CLOSE_TIME"

    return latest, None

def symbol_bad_reasons(symbol):
    thresholds = {
        "5m": env_int("VPS_SMC_STAGEB_5M_MAX_AGE_SEC", 780),
        "15m": env_int("VPS_SMC_ENTRY_15M_MAX_AGE_SEC", 1500),
        "4h": env_int("VPS_SMC_HTF_4H_MAX_AGE_SEC", 18000),
    }

    now_ms = int(time.time() * 1000)
    bad = []

    for tf, max_age_sec in thresholds.items():
        latest, err = latest_candle(symbol, tf)
        if err:
            bad.append({"tf": tf, "reason": err})
            continue

        c_ms = close_ms_from_row(latest, tf)
        age_sec = max(0, (now_ms - c_ms) / 1000)

        if age_sec > max_age_sec:
            bad.append({
                "tf": tf,
                "reason": (
                    f"{symbol} {tf} STALE age={age_sec/60:.1f} "
                    f"last_close={ms_to_wib(c_ms)} max={max_age_sec/60:.1f}m"
                )
            })

    return bad

def classify_symbols(symbols):
    fresh = []
    stale = {}
    critical = set(env_symbols("VPS_SMC_CRITICAL_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT"))

    critical_htf_bad = []

    for sym in symbols:
        bad = symbol_bad_reasons(sym)
        if bad:
            stale[sym] = bad
            for item in bad:
                if sym in critical and item.get("tf") in ("15m", "4h"):
                    critical_htf_bad.append(item["reason"])
        else:
            fresh.append(sym)

    return fresh, stale, critical_htf_bad

def chunks(items, n):
    n = max(1, int(n))
    for i in range(0, len(items), n):
        yield items[i:i+n]

def docker_exec_python(code, payload, timeout):
    cmd = ["docker", "exec", "-i", "ai-trading-bot", "python", "-c", code]

    try:
        res = subprocess.run(
            cmd,
            input=json.dumps(payload),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as e:
        return {
            "returncode": 124,
            "timeout": True,
            "raw_tail": str(e)[-2000:],
            "json": None,
        }

    out = res.stdout.strip()
    parsed = None

    for line in reversed(out.splitlines()):
        line = line.strip()
        if not line:
            continue
        try:
            parsed = json.loads(line)
            break
        except Exception:
            pass

    return {
        "returncode": res.returncode,
        "timeout": False,
        "raw_tail": out[-2000:],
        "json": parsed,
    }

def run_smc_once(symbols):
    code = r'''
import sys, json
sys.path.insert(0, "/app")
import app.main as m

payload = json.loads(sys.stdin.read() or "{}")
symbols = payload.get("symbols") or []

ok = False
reason = None
result = None

try:
    if hasattr(m, "vps_smc") and hasattr(m.vps_smc, "vps_smc_run_once"):
        result = m.vps_smc.vps_smc_run_once(symbols)
        ok = True
    elif hasattr(m, "vps_smc_run_once"):
        result = m.vps_smc_run_once(symbols)
        ok = True
    else:
        reason = "vps_smc_run_once_not_found"
except Exception as e:
    reason = f"{type(e).__name__}:{e}"

summary = {}
if isinstance(result, dict):
    summary = {
        "ok": result.get("ok"),
        "signal_count": result.get("signal_count"),
        "summary": result.get("summary"),
        "error": result.get("error"),
        "reason": result.get("reason"),
    }

print(json.dumps({"ok": ok, "reason": reason, "summary": summary}, default=str))
'''
    timeout = env_int("VPS_SMC_BATCH_TIMEOUT_SEC", 150)
    return docker_exec_python(code, {"symbols": symbols}, timeout=timeout)

def one_tick():
    symbols = symbols_from_env()

    if not symbols:
        log("MARKET_DATA_BAD")
        log("PAIR_ALLOWLIST_EMPTY")
        log("MARKET_DATA_SKIP_TICK")
        return False

    fresh, stale, critical_htf_bad = classify_symbols(symbols)

    min_fresh = env_int("VPS_SMC_MIN_FRESH_SYMBOLS", 8)
    batch_size = env_int("VPS_SMC_BATCH_SIZE", 7)
    batch_sleep = env_int("VPS_SMC_BATCH_SLEEP_SEC", 10)

    if critical_htf_bad:
        log("MARKET_DATA_BAD")
        log("MARKET_DATA_SKIP_TICK")
        log("SKIP_REASON=CRITICAL_HTF_STALE")
        for x in critical_htf_bad[:20]:
            log(x)
        return False

    if len(fresh) < min_fresh:
        log("MARKET_DATA_BAD")
        log(f"MARKET_DATA_SKIP_TICK")
        log(f"SKIP_REASON=TOO_FEW_FRESH fresh={len(fresh)} min={min_fresh} total={len(symbols)}")
        for sym, reasons in list(stale.items())[:20]:
            for item in reasons:
                log(item["reason"])
        return False

    log("MARKET_DATA_OK")

    if stale:
        log(f"MARKET_DATA_PARTIAL_OK fresh={len(fresh)} stale={len(stale)} total={len(symbols)}")
        log("FRESH_SYMBOLS=" + ",".join(fresh))
        for sym, reasons in list(stale.items())[:30]:
            for item in reasons:
                log("STALE_SKIP_PAIR " + item["reason"])
    else:
        log(f"MARKET_DATA_FULL_OK fresh={len(fresh)} total={len(symbols)}")

    total_signals = 0
    ok_batches = 0
    fail_batches = 0

    batch_list = list(chunks(fresh, batch_size))

    for idx, batch in enumerate(batch_list, start=1):
        log(f"[{ts()}] SMC batch {idx}/{len(batch_list)} start symbols=" + ",".join(batch))

        res = run_smc_once(batch)
        j = res.get("json") or {}
        summary = j.get("summary") or {}

        signal_count = summary.get("signal_count")
        try:
            signal_count_int = int(signal_count or 0)
        except Exception:
            signal_count_int = 0

        total_signals += signal_count_int

        if res.get("timeout"):
            fail_batches += 1
            log(f"[{ts()}] SMC batch {idx}/{len(batch_list)} timeout returncode={res.get('returncode')}")
        elif res.get("returncode") != 0 or j.get("ok") is False:
            fail_batches += 1
            log(
                f"[{ts()}] SMC batch {idx}/{len(batch_list)} failed "
                f"returncode={res.get('returncode')} ok={j.get('ok')} reason={j.get('reason')} "
                f"error={summary.get('error')}"
            )
        else:
            ok_batches += 1
            log(
                f"[{ts()}] SMC batch {idx}/{len(batch_list)} done "
                f"returncode={res.get('returncode')} ok={j.get('ok')} "
                f"signal_count={signal_count_int} smc_ok={summary.get('ok')} error={summary.get('error')}"
            )

        if idx < len(batch_list):
            time.sleep(max(0, batch_sleep))

    log(
        f"[{ts()}] SMC tick done batches_ok={ok_batches} batches_fail={fail_batches} "
        f"fresh={len(fresh)} stale={len(stale)} signal_count={total_signals}"
    )

    return fail_batches == 0

def main():
    log(f"[{ts()}] guarded partial-batch scheduler boot")
    log("SCHEDULER_MODE=PARTIAL_BATCH_NO_HOT_REPAIR")
    log("PAIR_ALLOWLIST=" + ",".join(symbols_from_env()))
    log(
        "SCHEDULER_CONFIG="
        f"batch_size={env_int('VPS_SMC_BATCH_SIZE', 7)} "
        f"min_fresh={env_int('VPS_SMC_MIN_FRESH_SYMBOLS', 8)} "
        f"batch_timeout={env_int('VPS_SMC_BATCH_TIMEOUT_SEC', 150)} "
        f"interval={env_int('VPS_SMC_SCHEDULER_INTERVAL_SEC', 120)}"
    )

    sleep_sec = max(30, env_int("VPS_SMC_SCHEDULER_INTERVAL_SEC", 120))

    while True:
        try:
            one_tick()
        except Exception as e:
            log(f"[{ts()}] scheduler_loop_error={type(e).__name__}:{e}")

        time.sleep(sleep_sec)

if __name__ == "__main__":
    main()
