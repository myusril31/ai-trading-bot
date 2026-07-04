#!/usr/bin/env python3
# STAT_TECH_LINEAR_QUANT_STORE_V1_20260703
import os
import json
import math
import time
import urllib.request
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone

MARKER = "STAT_TECH_LINEAR_QUANT_STORE_V1_20260703"

DEFAULT_SYMBOLS = [
    "ADAUSDT","AVAXUSDT","BCHUSDT","BTCUSDT","ETHUSDT","HYPEUSDT","LINKUSDT",
    "LTCUSDT","PAXGUSDT","SOLUSDT","SUIUSDT","UNIUSDT","XRPUSDT","ZECUSDT"
]

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def env_bool(k, default=False):
    v = str(os.getenv(k, str(default))).strip().lower()
    return v in ("1", "true", "yes", "on")

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

def corr(a, b):
    va = variance(a)
    vb = variance(b)
    if va <= 0 or vb <= 0:
        return 0.0
    return covariance(a, b) / math.sqrt(va * vb)

def beta(a, benchmark):
    vb = variance(benchmark)
    if vb <= 0:
        return 0.0
    return covariance(a, benchmark) / vb

def dot(a, b):
    n = min(len(a), len(b))
    if n <= 0:
        return 0.0
    return sum(float(a[-n+i]) * float(b[-n+i]) for i in range(n))

def norm(a):
    return math.sqrt(sum(float(x) ** 2 for x in a)) if a else 0.0

def cosine(a, b):
    n = min(len(a), len(b))
    if n <= 0:
        return 0.0
    aa = a[-n:]
    bb = b[-n:]
    den = norm(aa) * norm(bb)
    if den <= 0:
        return 0.0
    return dot(aa, bb) / den

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
    timeout = env_int("STAT_TECH_LINEAR_QUANT_HTTP_TIMEOUT_SEC", 15)
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

def directional_la_score(direction, metrics):
    # Linear algebra score from beta/correlation/cosine/residual vector.
    # Directional: LONG likes positive residual/momentum; SHORT likes negative residual/momentum.
    sign = 1.0 if str(direction).upper() == "LONG" else -1.0

    ret_15m = metrics.get("ret_15m", 0.0)
    ret_1h = metrics.get("ret_1h", 0.0)
    ret_4h = metrics.get("ret_4h", 0.0)
    btc_ret_1h = metrics.get("btc_ret_1h", 0.0)
    eth_ret_1h = metrics.get("eth_ret_1h", 0.0)

    beta_btc = metrics.get("beta_btc", 0.0)
    beta_eth = metrics.get("beta_eth", 0.0)
    corr_btc = metrics.get("corr_btc", 0.0)
    corr_eth = metrics.get("corr_eth", 0.0)
    cosine_btc = metrics.get("cosine_btc", 0.0)
    cosine_eth = metrics.get("cosine_eth", 0.0)
    residual_btc_1h = metrics.get("residual_btc_1h", 0.0)
    avg_abs_corr_universe = metrics.get("avg_abs_corr_universe", 0.0)

    # Scale values are crypto 5m/1h practical scales.
    mom_15 = math.tanh((sign * ret_15m) / 0.0025) * 8.0
    mom_1h = math.tanh((sign * ret_1h) / 0.0060) * 12.0
    mom_4h = math.tanh((sign * ret_4h) / 0.0180) * 8.0

    residual = math.tanh((sign * residual_btc_1h) / 0.0050) * 18.0

    btc_alignment = math.tanh((sign * beta_btc * btc_ret_1h) / 0.0060) * 8.0
    eth_alignment = math.tanh((sign * beta_eth * eth_ret_1h) / 0.0060) * 6.0

    # Cosine/correlation confirm direction only when benchmark move aligns.
    btc_cos_confirm = max(-1.0, min(1.0, cosine_btc)) * (1.0 if sign * btc_ret_1h > 0 else -0.5) * 4.0
    eth_cos_confirm = max(-1.0, min(1.0, cosine_eth)) * (1.0 if sign * eth_ret_1h > 0 else -0.5) * 3.0

    # Cluster risk: too correlated universe means alpha is less independent.
    cluster_penalty = clamp((avg_abs_corr_universe - 0.72) / 0.28, 0.0, 1.0) * 8.0

    # Corr conflict: high BTC/ETH corr while benchmark moves against intended direction.
    conflict = 0.0
    if corr_btc > 0.55 and sign * btc_ret_1h < 0:
        conflict += min(6.0, abs(corr_btc) * 6.0)
    if corr_eth > 0.55 and sign * eth_ret_1h < 0:
        conflict += min(5.0, abs(corr_eth) * 5.0)

    score = (
        50.0
        + mom_15
        + mom_1h
        + mom_4h
        + residual
        + btc_alignment
        + eth_alignment
        + btc_cos_confirm
        + eth_cos_confirm
        - cluster_penalty
        - conflict
    )

    return round(clamp(score, 0.0, 100.0), 1), {
        "mom_15": round(mom_15, 3),
        "mom_1h": round(mom_1h, 3),
        "mom_4h": round(mom_4h, 3),
        "residual": round(residual, 3),
        "btc_alignment": round(btc_alignment, 3),
        "eth_alignment": round(eth_alignment, 3),
        "btc_cos_confirm": round(btc_cos_confirm, 3),
        "eth_cos_confirm": round(eth_cos_confirm, 3),
        "cluster_penalty": round(cluster_penalty, 3),
        "conflict": round(conflict, 3),
    }

def main():
    enabled = env_bool("STAT_TECH_LINEAR_QUANT_ENABLED", True)
    if not enabled:
        print(json.dumps({"ok": True, "enabled": False, "marker": MARKER}, indent=2))
        return

    symbols = symbols_from_env()
    interval = os.getenv("STAT_TECH_LINEAR_QUANT_INTERVAL", "5m")
    limit = env_int("STAT_TECH_LINEAR_QUANT_KLINE_LIMIT", 240)
    lookback = env_int("STAT_TECH_LINEAR_QUANT_LOOKBACK_RETURNS", 96)

    log_dir = Path(os.getenv("LOG_DIR", "logs"))
    report_dir = Path(os.getenv("REPORT_DIR", "reports"))
    log_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    fetched = {}
    failures = {}

    for sym in sorted(set(symbols + ["BTCUSDT", "ETHUSDT"])):
        try:
            fetched[sym] = fetch_klines(sym, interval=interval, limit=limit)
            time.sleep(env_float("STAT_TECH_LINEAR_QUANT_HTTP_SLEEP_SEC", 0.05))
        except Exception as e:
            failures[sym] = f"{type(e).__name__}:{e}"

    if "BTCUSDT" not in fetched or "ETHUSDT" not in fetched:
        report = {
            "ok": False,
            "marker": MARKER,
            "created_at_utc": utc_now_iso(),
            "reason": "missing_btc_or_eth",
            "failures": failures,
        }
        (report_dir / "stat_tech_linear_quant_store_v1_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report, indent=2))
        return

    returns = {}
    for sym, obj in fetched.items():
        returns[sym] = log_returns(obj["closes"])[-lookback:]

    btc_ret_vec = returns.get("BTCUSDT", [])
    eth_ret_vec = returns.get("ETHUSDT", [])
    btc_closes = fetched["BTCUSDT"]["closes"]
    eth_closes = fetched["ETHUSDT"]["closes"]

    btc_ret_1h = simple_ret(btc_closes, 12)
    eth_ret_1h = simple_ret(eth_closes, 12)

    rows = []
    created = utc_now_iso()

    for sym in symbols:
        obj = fetched.get(sym)
        if not obj:
            continue

        closes = obj["closes"]
        rvec = returns.get(sym, [])
        if len(rvec) < max(20, min(lookback, 30)):
            failures[sym] = f"insufficient_returns:{len(rvec)}"
            continue

        corr_btc = corr(rvec, btc_ret_vec)
        corr_eth = corr(rvec, eth_ret_vec)
        beta_btc = beta(rvec, btc_ret_vec)
        beta_eth = beta(rvec, eth_ret_vec)
        cosine_btc = cosine(rvec, btc_ret_vec)
        cosine_eth = cosine(rvec, eth_ret_vec)

        ret_15m = simple_ret(closes, 3)
        ret_1h = simple_ret(closes, 12)
        ret_4h = simple_ret(closes, 48)
        residual_btc_1h = ret_1h - beta_btc * btc_ret_1h

        peer_corrs = []
        for other, ovec in returns.items():
            if other == sym or other not in symbols:
                continue
            if len(ovec) >= 20:
                peer_corrs.append(abs(corr(rvec, ovec)))
        avg_abs_corr_universe = mean(peer_corrs) if peer_corrs else 0.0

        metrics = {
            "ret_15m": ret_15m,
            "ret_1h": ret_1h,
            "ret_4h": ret_4h,
            "btc_ret_1h": btc_ret_1h,
            "eth_ret_1h": eth_ret_1h,
            "corr_btc": corr_btc,
            "corr_eth": corr_eth,
            "beta_btc": beta_btc,
            "beta_eth": beta_eth,
            "cosine_btc": cosine_btc,
            "cosine_eth": cosine_eth,
            "residual_btc_1h": residual_btc_1h,
            "avg_abs_corr_universe": avg_abs_corr_universe,
        }

        long_score, long_parts = directional_la_score("LONG", metrics)
        short_score, short_parts = directional_la_score("SHORT", metrics)

        row = {
            "ok": True,
            "marker": MARKER,
            "created_at_utc": created,
            "symbol": sym,
            "interval": interval,
            "lookback_returns": lookback,
            "latest_close_ms": obj.get("latest_close_ms"),
            "latest_close": obj.get("latest_close"),
            "linear_algebra_features": {
                "corr_btc": round(corr_btc, 5),
                "corr_eth": round(corr_eth, 5),
                "beta_btc": round(beta_btc, 5),
                "beta_eth": round(beta_eth, 5),
                "cosine_btc": round(cosine_btc, 5),
                "cosine_eth": round(cosine_eth, 5),
                "ret_15m": round(ret_15m, 7),
                "ret_1h": round(ret_1h, 7),
                "ret_4h": round(ret_4h, 7),
                "btc_ret_1h": round(btc_ret_1h, 7),
                "eth_ret_1h": round(eth_ret_1h, 7),
                "residual_btc_1h": round(residual_btc_1h, 7),
                "avg_abs_corr_universe": round(avg_abs_corr_universe, 5),
            },
            "la_score_long": long_score,
            "la_score_short": short_score,
            "la_parts_long": long_parts,
            "la_parts_short": short_parts,
        }
        rows.append(row)

    out_fp = log_dir / "stat_tech_linear_quant_store_v1.jsonl"
    with out_fp.open("a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n")

    report = {
        "ok": True,
        "marker": MARKER,
        "created_at_utc": created,
        "symbols_requested": len(symbols),
        "symbols_fetched": len(fetched),
        "rows_written": len(rows),
        "failures": failures,
        "output": str(out_fp),
        "top_long": sorted(
            [{"symbol": r["symbol"], "score": r["la_score_long"]} for r in rows],
            key=lambda x: x["score"], reverse=True
        )[:8],
        "top_short": sorted(
            [{"symbol": r["symbol"], "score": r["la_score_short"]} for r in rows],
            key=lambda x: x["score"], reverse=True
        )[:8],
    }
    (report_dir / "stat_tech_linear_quant_store_v1_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    main()
