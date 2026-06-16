#!/usr/bin/env python3
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta

ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / ".env"
WIB = timezone(timedelta(hours=7))

def ts():
    return datetime.now(WIB).strftime("%Y-%m-%dT%H:%M:%S+07:00")

def log(x):
    print(str(x), flush=True)

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

def docker_bootstrap(symbols, intervals, limit, batch_size, timeout_sec):
    code = r'''
import sys, json, time
sys.path.insert(0, "/app")
import app.main as m

payload = json.loads(sys.stdin.read() or "{}")
symbols = payload.get("symbols") or []
intervals = payload.get("intervals") or ["1m","5m","15m","4h"]
limit = int(payload.get("limit") or 40)
batch_size = int(payload.get("batch_size") or 3)

fn = None
if hasattr(m, "market_rest_bootstrap"):
    fn = m.market_rest_bootstrap
elif hasattr(m, "market_bootstrap"):
    fn = m.market_bootstrap

print("BOOTSTRAP_FN", getattr(fn, "__name__", None), flush=True)

if fn is None:
    raise SystemExit("NO_BOOTSTRAP_FUNCTION_FOUND")

def chunks(xs, n):
    for i in range(0, len(xs), n):
        yield xs[i:i+n]

total_rows = 0
failures = []

for idx, batch in enumerate(chunks(symbols, batch_size), start=1):
    print("SYNC_BATCH_START", idx, batch, flush=True)
    try:
        res = fn(batch, intervals, limit=limit)
        rows = 0
        if isinstance(res, dict):
            rows = int(res.get("rows_ingested") or 0)
            failures.extend(res.get("failures") or [])
        total_rows += rows
        print("SYNC_BATCH_DONE", idx, json.dumps(res, default=str)[:1000], flush=True)
    except Exception as e:
        failures.append({"batch": batch, "error": f"{type(e).__name__}:{e}"})
        print("SYNC_BATCH_ERR", idx, type(e).__name__, str(e), flush=True)
    time.sleep(2)

print(json.dumps({"ok": len(failures) == 0, "total_rows": total_rows, "failures": failures}, default=str), flush=True)
'''
    payload = {
        "symbols": symbols,
        "intervals": intervals,
        "limit": limit,
        "batch_size": batch_size,
    }

    cmd = ["docker", "exec", "-i", "ai-trading-bot", "python", "-c", code]

    return subprocess.run(
        cmd,
        input=json.dumps(payload),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout_sec,
    )

def restart_bot():
    log(f"[{ts()}] REST_SYNC restarting bot container")
    subprocess.run(["docker", "compose", "restart", "bot"], cwd=str(ROOT), timeout=120)
    time.sleep(35)

def main():
    symbols = symbols_from_env()
    if not symbols:
        log(f"[{ts()}] REST_SYNC_FAIL no symbols")
        return 2

    intervals = [x.strip() for x in env_get("MARKET_DATA_REST_SYNC_INTERVALS", "1m,5m,15m,4h").split(",") if x.strip()]
    limit = env_int("MARKET_DATA_REST_SYNC_LIMIT", 40)
    batch_size = env_int("MARKET_DATA_REST_SYNC_BATCH_SIZE", 3)
    timeout_sec = env_int("MARKET_DATA_REST_SYNC_TIMEOUT_SEC", 900)
    restart_on_fail = str(env_get("MARKET_DATA_REST_SYNC_RESTART_ON_FAIL", "true")).lower() in ("1", "true", "yes", "y", "on")

    log(f"[{ts()}] REST_SYNC_START symbols={len(symbols)} intervals={intervals} limit={limit} batch_size={batch_size}")

    try:
        res = docker_bootstrap(symbols, intervals, limit, batch_size, timeout_sec)
    except subprocess.TimeoutExpired as e:
        log(f"[{ts()}] REST_SYNC_TIMEOUT first_try {e}")
        res = None

    if res is not None:
        log(res.stdout[-5000:])
        if res.returncode == 0 and '"ok": true' in res.stdout:
            log(f"[{ts()}] REST_SYNC_OK")
            return 0

    log(f"[{ts()}] REST_SYNC_FAIL first_try")

    if restart_on_fail:
        try:
            restart_bot()
            res2 = docker_bootstrap(symbols, intervals, limit, batch_size, timeout_sec)
            log(res2.stdout[-5000:])
            if res2.returncode == 0 and '"ok": true' in res2.stdout:
                log(f"[{ts()}] REST_SYNC_OK after_restart")
                return 0
        except Exception as e:
            log(f"[{ts()}] REST_SYNC_RETRY_ERR {type(e).__name__}:{e}")

    log(f"[{ts()}] REST_SYNC_FINAL_FAIL")
    return 1

if __name__ == "__main__":
    raise SystemExit(main())
