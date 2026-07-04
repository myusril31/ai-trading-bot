#!/usr/bin/env python3
# STAT_TECH_OU_MEANREV_STORE_V1_20260704
import os
import json
import math
import time
import urllib.request
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone

MARKER = "STAT_TECH_OU_MEANREV_STORE_V1_20260704"

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

def variance(xs):
    xs = [float(x) for x in xs if x is not None and math.isfinite(float(x))]
    if len(xs) < 2:
        return 0.0
    m = mean(xs)
    return sum((x - m) ** 2 for x in xs) / (len(xs) - 1)

def covariance(a, b):
    n = min(len(a), len(b))
    if n < 2:
        return 0.0
    a = [float(x) for x in a[-n:]]
    b = [float(x) for x in b[-n:]]
    ma = mean(a)
    mb = mean(b)
    return sum((a[i] - ma) * (b[i] - mb) for i in range(n)) / (n - 1)

def clamp(x, lo, hi):
    return max(lo, min(hi, x))

def fetch_klines(symbol, interval="5m", limit=288):
    base = os.getenv("BINANCE_FUTURES_BASE_URL", "https://fapi.binance.com").rstrip("/")
    params = urllib.parse.urlencode({
        "symbol": symbol.upper(),
        "interval": interval,
        "limit": int(limit),
    })
    url = f"{base}/fapi/v1/klines?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": MARKER})
    timeout = env_int("STAT_TECH_OU_MEANREV_HTTP_TIMEOUT_SEC", 15)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        data = json.loads(r.read().decode("utf-8"))

    closes = []
    close_times = []
    for k in data:
        closes.append(float(k[4]))
        close_times.append(int(k[6]))
    return {
        "symbol": symbol.upper(),
        "closes": closes,
        "close_times": close_times,
        "latest_close_ms": close_times[-1] if close_times else None,
        "latest_close": closes[-1] if closes else None,
    }

def log_prices(closes):
    return [math.log(x) for x in closes if x and x > 0]

def simple_ret(closes, bars):
    if len(closes) <= bars:
        return 0.0
    a = closes[-bars-1]
    b = closes[-1]
    if a and b and a > 0:
        return (b / a) - 1.0
    return 0.0

def estimate_ar1_phi(xs):
    # Fit x[t+1] = phi*x[t] + eps on mean-centered log-price deviations.
    if len(xs) < 20:
        return None

    mu = mean(xs)
    dev = [x - mu for x in xs]

    x0 = dev[:-1]
    x1 = dev[1:]

    denom = sum(x * x for x in x0)
    if denom <= 1e-18:
        return None

    phi = sum(x0[i] * x1[i] for i in range(len(x0))) / denom
    phi = clamp(phi, -0.99, 0.999)
    return phi

def ou_metrics_from_closes(closes, lookback=96):
    logs = log_prices(closes)
    if len(logs) < max(30, min(lookback, 60)):
        return {"ok": False, "reason": f"insufficient_logs:{len(logs)}"}

    xs = logs[-lookback:]
    mu = mean(xs)
    sd = stdev(xs)
    if sd <= 1e-12:
        return {"ok": False, "reason": "zero_std"}

    x_last = xs[-1]
    z = (x_last - mu) / sd

    phi = estimate_ar1_phi(xs)
    if phi is None:
        return {"ok": False, "reason": "phi_none"}

    if phi <= 0:
        theta = 1.0
    else:
        theta = -math.log(max(phi, 1e-9))

    theta = clamp(theta, 0.0001, 2.0)
    half_life = math.log(2.0) / theta if theta > 0 else 999.0

    # OU one-step expected reversion in z-space.
    expected_next_dev = phi * (x_last - mu)
    expected_reversion_log = (mu + expected_next_dev) - x_last

    ret_15m = simple_ret(closes, 3)
    ret_1h = simple_ret(closes, 12)
    ret_4h = simple_ret(closes, 48)

    # Strength bagus kalau z cukup stretch, phi mean reverting, half-life masih usable.
    z_abs = abs(z)
    z_strength = clamp((z_abs - 0.35) / 1.65, 0.0, 1.0)

    # Half-life practical for 5m: 3–48 bars. Terlalu cepat/noisy atau terlalu lama kurang bagus.
    if half_life < 3:
        hl_quality = clamp(half_life / 3.0, 0.0, 1.0)
    elif half_life <= 48:
        hl_quality = 1.0
    else:
        hl_quality = clamp(1.0 - ((half_life - 48) / 96), 0.0, 1.0)

    theta_quality = clamp(theta / 0.35, 0.0, 1.0)
    meanrev_strength = clamp(0.45 * z_strength + 0.35 * hl_quality + 0.20 * theta_quality, 0.0, 1.0)

    # Directional score:
    # LONG likes negative z, SHORT likes positive z.
    long_raw = 50.0
    short_raw = 50.0

    long_raw += clamp((-z - 0.25) / 1.75, -1.0, 1.0) * 28.0
    short_raw += clamp((z - 0.25) / 1.75, -1.0, 1.0) * 28.0

    # Boost when expected reversion favors direction.
    long_raw += clamp(expected_reversion_log / 0.004, -1.0, 1.0) * 14.0
    short_raw += clamp((-expected_reversion_log) / 0.004, -1.0, 1.0) * 14.0

    # Strength multiplier, but jangan matiin total.
    long_score = 50.0 + (long_raw - 50.0) * (0.55 + 0.45 * meanrev_strength)
    short_score = 50.0 + (short_raw - 50.0) * (0.55 + 0.45 * meanrev_strength)

    # Momentum conflict penalty. Mean reversion long kurang aman kalau momentum 1h masih jatuh keras.
    if ret_1h < -0.01:
        long_score -= 6.0
    if ret_1h > 0.01:
        short_score -= 6.0

    return {
        "ok": True,
        "ou_mean_log": round(mu, 10),
        "ou_std_log": round(sd, 10),
        "ou_zscore": round(z, 6),
        "ou_phi": round(phi, 8),
        "ou_theta": round(theta, 8),
        "ou_half_life_bars": round(half_life, 3),
        "ou_expected_reversion_log_1bar": round(expected_reversion_log, 10),
        "ou_mean_reversion_strength": round(meanrev_strength, 6),
        "ret_15m": round(ret_15m, 8),
        "ret_1h": round(ret_1h, 8),
        "ret_4h": round(ret_4h, 8),
        "ou_score_long": round(clamp(long_score, 0.0, 100.0), 1),
        "ou_score_short": round(clamp(short_score, 0.0, 100.0), 1),
    }

def main():
    if not env_bool("STAT_TECH_OU_MEANREV_ENABLED", True):
        print(json.dumps({"ok": True, "enabled": False, "marker": MARKER}, indent=2))
        return

    symbols = symbols_from_env()
    interval = os.getenv("STAT_TECH_OU_MEANREV_INTERVAL", "5m")
    limit = env_int("STAT_TECH_OU_MEANREV_KLINE_LIMIT", 288)
    lookback = env_int("STAT_TECH_OU_MEANREV_LOOKBACK", 96)

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
            m = ou_metrics_from_closes(obj["closes"], lookback=lookback)
            if not m.get("ok"):
                failures[sym] = m.get("reason", "metric_fail")
                continue

            row = {
                "ok": True,
                "marker": MARKER,
                "created_at_utc": created,
                "symbol": sym,
                "interval": interval,
                "lookback": lookback,
                "latest_close_ms": obj.get("latest_close_ms"),
                "latest_close": obj.get("latest_close"),
                **m,
            }
            rows.append(row)
            time.sleep(env_float("STAT_TECH_OU_MEANREV_HTTP_SLEEP_SEC", 0.05))
        except Exception as e:
            failures[sym] = f"{type(e).__name__}:{e}"

    out_fp = log_dir / "stat_tech_ou_meanrev_store_v1.jsonl"
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
        "top_long": sorted(
            [{"symbol": r["symbol"], "score": r["ou_score_long"], "z": r["ou_zscore"], "half_life": r["ou_half_life_bars"]} for r in rows],
            key=lambda x: x["score"],
            reverse=True,
        )[:8],
        "top_short": sorted(
            [{"symbol": r["symbol"], "score": r["ou_score_short"], "z": r["ou_zscore"], "half_life": r["ou_half_life_bars"]} for r in rows],
            key=lambda x: x["score"],
            reverse=True,
        )[:8],
    }

    (report_dir / "stat_tech_ou_meanrev_store_v1_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    main()
