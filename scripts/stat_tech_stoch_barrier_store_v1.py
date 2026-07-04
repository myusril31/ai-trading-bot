#!/usr/bin/env python3
# STAT_TECH_STOCH_BARRIER_STORE_V1_20260704
import os
import json
import math
import time
import urllib.request
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone

MARKER = "STAT_TECH_STOCH_BARRIER_STORE_V1_20260704"

DEFAULT_SYMBOLS = [
    "ADAUSDT","AVAXUSDT","BCHUSDT","BTCUSDT","ETHUSDT","HYPEUSDT","LINKUSDT",
    "LTCUSDT","PAXGUSDT","SOLUSDT","SUIUSDT","UNIUSDT","XRPUSDT","ZECUSDT"
]

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def env_bool(k, default=False):
    return str(os.getenv(k, str(default))).strip().lower() in ("1", "true", "yes", "on")

def env_float(k, default):
    try:
        return float(os.getenv(k, default))
    except Exception:
        return float(default)

def env_int(k, default):
    try:
        return int(float(os.getenv(k, default)))
    except Exception:
        return int(default)

def symbols_from_env():
    raw = (
        os.getenv("STAT_TECH_SYMBOLS")
        or os.getenv("PAIR_ALLOWLIST")
        or os.getenv("SYMBOLS")
        or os.getenv("UNIVERSE_SYMBOLS")
        or ""
    )
    if raw.strip():
        out = []
        for x in raw.replace(";", ",").split(","):
            x = x.strip().upper()
            if x:
                out.append(x)
        if out:
            return sorted(set(out))
    return DEFAULT_SYMBOLS

def mean(xs):
    xs = [float(x) for x in xs if x is not None and math.isfinite(float(x))]
    return sum(xs) / len(xs) if xs else 0.0

def stdev(xs):
    xs = [float(x) for x in xs if x is not None and math.isfinite(float(x))]
    if len(xs) < 2:
        return 0.0
    m = mean(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))

def clamp(x, lo, hi):
    return max(lo, min(hi, x))

def log_returns(closes):
    out = []
    for i in range(1, len(closes)):
        a = closes[i-1]
        b = closes[i]
        if a and b and a > 0 and b > 0:
            out.append(math.log(b / a))
    return out

def simple_ret(closes, bars):
    if len(closes) <= bars:
        return 0.0
    a = closes[-bars-1]
    b = closes[-1]
    if a and b and a > 0:
        return (b / a) - 1.0
    return 0.0

def fetch_klines(symbol, interval="5m", limit=240):
    base = os.getenv("BINANCE_FUTURES_BASE_URL", "https://fapi.binance.com").rstrip("/")
    params = urllib.parse.urlencode({
        "symbol": symbol.upper(),
        "interval": interval,
        "limit": int(limit),
    })
    url = f"{base}/fapi/v1/klines?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": MARKER})
    timeout = env_int("STAT_TECH_STOCH_BARRIER_HTTP_TIMEOUT_SEC", 15)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        data = json.loads(r.read().decode("utf-8"))
    closes = []
    highs = []
    lows = []
    close_times = []
    for k in data:
        highs.append(float(k[2]))
        lows.append(float(k[3]))
        closes.append(float(k[4]))
        close_times.append(int(k[6]))
    return {
        "symbol": symbol.upper(),
        "highs": highs,
        "lows": lows,
        "closes": closes,
        "close_times": close_times,
        "latest_close_ms": close_times[-1] if close_times else None,
        "latest_close": closes[-1] if closes else None,
    }

def atr_pct(highs, lows, closes, n=48):
    if len(closes) < 3:
        return 0.0
    trs = []
    start = max(1, len(closes) - n)
    for i in range(start, len(closes)):
        h = highs[i]
        l = lows[i]
        pc = closes[i-1]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        if closes[i] > 0:
            trs.append(tr / closes[i])
    return mean(trs)

def main():
    if not env_bool("STAT_TECH_STOCH_BARRIER_ENABLED", True):
        print(json.dumps({"ok": True, "enabled": False, "marker": MARKER}, indent=2))
        return

    symbols = symbols_from_env()
    interval = os.getenv("STAT_TECH_STOCH_BARRIER_INTERVAL", "5m")
    limit = env_int("STAT_TECH_STOCH_BARRIER_KLINE_LIMIT", 240)
    lookback = env_int("STAT_TECH_STOCH_BARRIER_LOOKBACK_RETURNS", 96)

    log_dir = Path(os.getenv("LOG_DIR", "logs"))
    report_dir = Path(os.getenv("REPORT_DIR", "reports"))
    log_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    failures = {}
    created = utc_now_iso()

    for sym in symbols:
        try:
            obj = fetch_klines(sym, interval=interval, limit=limit)
            closes = obj["closes"]
            rets = log_returns(closes)[-lookback:]

            if len(rets) < max(20, min(lookback, 30)):
                failures[sym] = f"insufficient_returns:{len(rets)}"
                continue

            mu_bar = mean(rets)
            sigma_bar = stdev(rets)
            sigma_floor = env_float("STAT_TECH_STOCH_BARRIER_SIGMA_FLOOR", 0.00015)
            sigma_eff = max(sigma_bar, sigma_floor)

            row = {
                "ok": True,
                "marker": MARKER,
                "created_at_utc": created,
                "symbol": sym,
                "interval": interval,
                "lookback_returns": lookback,
                "latest_close_ms": obj.get("latest_close_ms"),
                "latest_close": obj.get("latest_close"),
                "mu_logret_bar": round(mu_bar, 10),
                "sigma_logret_bar": round(sigma_bar, 10),
                "sigma_eff_logret_bar": round(sigma_eff, 10),
                "ret_15m": round(simple_ret(closes, 3), 8),
                "ret_1h": round(simple_ret(closes, 12), 8),
                "ret_4h": round(simple_ret(closes, 48), 8),
                "rv_1h": round(stdev(rets[-12:]) if len(rets) >= 12 else sigma_bar, 10),
                "rv_4h": round(stdev(rets[-48:]) if len(rets) >= 48 else sigma_bar, 10),
                "atr_pct_4h": round(atr_pct(obj["highs"], obj["lows"], closes, n=48), 8),
            }
            rows.append(row)
            time.sleep(env_float("STAT_TECH_STOCH_BARRIER_HTTP_SLEEP_SEC", 0.05))
        except Exception as e:
            failures[sym] = f"{type(e).__name__}:{e}"

    out_fp = log_dir / "stat_tech_stoch_barrier_store_v1.jsonl"
    with out_fp.open("a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n")

    report = {
        "ok": True,
        "marker": MARKER,
        "created_at_utc": created,
        "symbols_requested": len(symbols),
        "rows_written": len(rows),
        "failures": failures,
        "output": str(out_fp),
        "highest_vol": sorted(
            [{"symbol": r["symbol"], "sigma": r["sigma_eff_logret_bar"], "atr_pct_4h": r["atr_pct_4h"]} for r in rows],
            key=lambda x: x["sigma"],
            reverse=True,
        )[:8],
        "lowest_vol": sorted(
            [{"symbol": r["symbol"], "sigma": r["sigma_eff_logret_bar"], "atr_pct_4h": r["atr_pct_4h"]} for r in rows],
            key=lambda x: x["sigma"],
        )[:8],
    }
    (report_dir / "stat_tech_stoch_barrier_store_v1_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    main()
