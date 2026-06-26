#!/usr/bin/env python3
import json, os, math, re, subprocess
from pathlib import Path
from collections import deque
from datetime import datetime, timezone, timedelta
from statistics import mean, pstdev

ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / ".env"
WIB = timezone(timedelta(hours=7))
UTC = timezone.utc

OUT_LOG = ROOT / "logs" / "freqai_feature_store_v1.jsonl"
OUT_STATE = ROOT / "state" / "features" / "latest_freqai_features_v1.json"

def now_utc():
    return datetime.now(UTC)

def now_wib_text():
    return now_utc().astimezone(WIB).strftime("%Y-%m-%d %H:%M:%S WIB")

def load_env_file():
    env = {}
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text(errors="ignore").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    return env

ENV = load_env_file()

def env_get(k, default=""):
    return os.getenv(k) or ENV.get(k) or str(default)

def norm_symbol(x):
    s = str(x or "").strip().upper()
    s = s.replace("BINANCE:", "").replace(".P", "").replace("/", "").replace("-", "")
    return re.sub(r"[^A-Z0-9]", "", s)

def symbols_from_env():
    raw = env_get("PAIR_ALLOWLIST", "")
    out = []
    for part in raw.replace(";", ",").split(","):
        s = norm_symbol(part)
        if s and s not in out:
            out.append(s)
    return out or ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

def market_data_dir():
    raw = env_get("BINANCE_CANDLE_STORE_DIR", "").strip()
    return ROOT / raw if raw and not raw.startswith("/") else Path(raw or (ROOT / "state" / "market_data"))

def candle_path(symbol, interval):
    return market_data_dir() / f"{symbol}_{interval}.jsonl"

def safe_float(x):
    try:
        v = float(x)
        if math.isfinite(v):
            return v
    except Exception:
        pass
    return None

def read_candles(symbol, interval, limit=300):
    # === FEATURE_STORE_V1_TAIL_READ_OPT_20260614 ===
    # Read only recent candle rows. Full-file scans are how scripts become tiny bureaucracies.
    p = candle_path(symbol, interval)
    q = deque(maxlen=limit)

    if not p.exists():
        return []

    # Pull extra lines because some rows may be malformed / partial / non-closed.
    tail_n = max(limit * 3, 500)

    try:
        proc = subprocess.run(
            ["tail", "-n", str(tail_n), str(p)],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        lines = proc.stdout.splitlines()
    except Exception:
        # Fallback to old safe behavior.
        try:
            with p.open("r", encoding="utf-8", errors="ignore") as f:
                lines = list(f)[-tail_n:]
        except Exception:
            return []

    for line in lines:
        try:
            r = json.loads(line)
        except Exception:
            continue

        if r.get("is_closed") is False:
            continue

        o = safe_float(r.get("open", r.get("o")))
        h = safe_float(r.get("high", r.get("h")))
        l = safe_float(r.get("low", r.get("l")))
        c = safe_float(r.get("close", r.get("c")))
        v = safe_float(r.get("volume", r.get("v")))

        t = r.get("close_time_ms") or r.get("t") or r.get("close_time") or r.get("open_time_ms")

        if None in (o, h, l, c, v):
            continue

        try:
            t = int(t)
        except Exception:
            t = None

        q.append({
            "t": t,
            "o": o,
            "h": h,
            "l": l,
            "c": c,
            "v": v,
        })

    rows = list(q)
    rows.sort(key=lambda x: int(x.get("t") or 0))
    return rows

def pct_ret(candles, n):
    if len(candles) <= n:
        return None
    a = candles[-1]["c"]
    b = candles[-1-n]["c"]
    if not b:
        return None
    return (a / b) - 1.0

def atr_pct(candles, n=14):
    if len(candles) < n + 1:
        return None
    trs = []
    tail = candles[-(n+1):]
    for i in range(1, len(tail)):
        h = tail[i]["h"]
        l = tail[i]["l"]
        pc = tail[i-1]["c"]
        tr = max(h-l, abs(h-pc), abs(l-pc))
        trs.append(tr)
    if not trs:
        return None
    atr = sum(trs) / len(trs)
    close = candles[-1]["c"]
    return atr / close if close else None

def volume_z(candles, n=20):
    if len(candles) < n + 1:
        return None
    vols = [x["v"] for x in candles[-(n+1):-1]]
    last = candles[-1]["v"]
    if not vols:
        return None
    mu = mean(vols)
    sd = pstdev(vols) if len(vols) > 1 else 0
    if sd <= 0:
        return 0.0
    return (last - mu) / sd

def candle_shape(candles):
    if not candles:
        return {}
    c = candles[-1]
    o, h, l, close = c["o"], c["h"], c["l"], c["c"]
    rng = max(h - l, 1e-12)
    body = abs(close - o)
    upper = h - max(o, close)
    lower = min(o, close) - l
    mid = close if close else 1e-12

    return {
        "range_pct": rng / mid,
        "body_ratio": body / rng,
        "upper_wick_ratio": upper / rng,
        "lower_wick_ratio": lower / rng,
        "close_pos_ratio": (close - l) / rng,
    }

def rank_pct(values):
    clean = [(k, v) for k, v in values.items() if v is not None and math.isfinite(float(v))]
    if not clean:
        return {}
    clean_sorted = sorted(clean, key=lambda x: x[1])
    n = len(clean_sorted)
    out = {}
    for i, (k, v) in enumerate(clean_sorted):
        out[k] = (i + 1) / n
    return out

def build_base_features(symbol):
    c1 = read_candles(symbol, "1m", 120)
    c5 = read_candles(symbol, "5m", 200)
    c15 = read_candles(symbol, "15m", 200)
    c4h = read_candles(symbol, "4h", 120)

    latest_close = c5[-1]["c"] if c5 else (c15[-1]["c"] if c15 else None)
    latest_t = c5[-1]["t"] if c5 else (c15[-1]["t"] if c15 else None)

    shape5 = candle_shape(c5)

    f = {
        "symbol": symbol,
        "latest_close": latest_close,
        "latest_close_15m": c15[-1]["c"] if c15 else None,
        "latest_close_time_ms": latest_t,

        "ret_1m_5": pct_ret(c1, 5),
        "ret_5m_1": pct_ret(c5, 1),
        "ret_5m_3": pct_ret(c5, 3),
        "ret_5m_12": pct_ret(c5, 12),

        "ret_15m_1": pct_ret(c15, 1),
        "ret_15m_4": pct_ret(c15, 4),
        "ret_15m_16": pct_ret(c15, 16),

        "ret_4h_1": pct_ret(c4h, 1),
        "ret_4h_6": pct_ret(c4h, 6),

        "atr_pct_5m_14": atr_pct(c5, 14),
        "atr_pct_15m_14": atr_pct(c15, 14),

        "volume_z_5m_20": volume_z(c5, 20),
        "volume_z_15m_20": volume_z(c15, 20),

        "candle_range_pct_5m": shape5.get("range_pct"),
        "candle_body_ratio_5m": shape5.get("body_ratio"),
        "upper_wick_ratio_5m": shape5.get("upper_wick_ratio"),
        "lower_wick_ratio_5m": shape5.get("lower_wick_ratio"),
        "close_pos_ratio_5m": shape5.get("close_pos_ratio"),

        "candles_1m": len(c1),
        "candles_5m": len(c5),
        "candles_15m": len(c15),
        "candles_4h": len(c4h),
    }

    return f

def main():
    OUT_LOG.parent.mkdir(parents=True, exist_ok=True)
    OUT_STATE.parent.mkdir(parents=True, exist_ok=True)

    symbols = symbols_from_env()
    run_utc = now_utc().isoformat()
    run_wib = now_wib_text()

    rows = []
    base = {}

    for sym in symbols:
        base[sym] = build_base_features(sym)

    btc_ret = (base.get("BTCUSDT") or {}).get("ret_15m_4")
    eth_ret = (base.get("ETHUSDT") or {}).get("ret_15m_4")

    ret_rank = rank_pct({s: f.get("ret_15m_4") for s, f in base.items()})
    vol_rank = rank_pct({s: f.get("atr_pct_5m_14") for s, f in base.items()})
    volume_rank = rank_pct({s: f.get("volume_z_5m_20") for s, f in base.items()})

    for sym, f in base.items():
        pair_ret = f.get("ret_15m_4")

        f["btc_residual_ret_15m_4"] = (pair_ret - btc_ret) if pair_ret is not None and btc_ret is not None else None
        f["eth_residual_ret_15m_4"] = (pair_ret - eth_ret) if pair_ret is not None and eth_ret is not None else None

        f["cross_rank_ret_15m_4"] = ret_rank.get(sym)
        f["cross_rank_atr_5m_14"] = vol_rank.get(sym)
        f["cross_rank_volume_z_5m_20"] = volume_rank.get(sym)

        # === FEATURE_STORE_V1_SANITY_GUARD_20260614 ===
        sanity_warnings = []

        def _warn(cond, msg):
            if cond:
                sanity_warnings.append(msg)

        def _abs_gt(name, limit):
            v = f.get(name)
            try:
                return v is not None and abs(float(v)) > float(limit)
            except Exception:
                return False

        _warn(f.get("latest_close") is None, "missing_latest_close")
        _warn(f.get("candles_5m", 0) < 50, "low_5m_candle_count")
        _warn(f.get("candles_15m", 0) < 50, "low_15m_candle_count")
        _warn(_abs_gt("ret_1m_5", 0.05), "ret_1m_5_extreme")
        _warn(_abs_gt("ret_5m_1", 0.05), "ret_5m_1_extreme")
        _warn(_abs_gt("ret_5m_3", 0.08), "ret_5m_3_extreme")
        _warn(_abs_gt("ret_15m_1", 0.08), "ret_15m_1_extreme")
        _warn(_abs_gt("candle_range_pct_5m", 0.08), "candle_range_5m_extreme")

        try:
            lc = f.get("latest_close")
            lc15 = f.get("latest_close_15m")
            if lc and lc15:
                close_gap = abs(float(lc) / float(lc15) - 1.0)
                f["latest_vs_15m_close_gap_pct"] = close_gap
                _warn(close_gap > 0.03, "latest_close_vs_15m_close_gap_gt_3pct")
            else:
                f["latest_vs_15m_close_gap_pct"] = None
        except Exception as e:
            f["latest_vs_15m_close_gap_pct"] = None
            sanity_warnings.append(f"close_gap_calc_error:{type(e).__name__}")

        try:
            atr5 = f.get("atr_pct_5m_14")
            if atr5 is not None:
                _warn(float(atr5) > 0.05, "atr_5m_extreme")
        except Exception:
            pass

        f["feature_sanity_ok"] = len(sanity_warnings) == 0
        f["feature_sanity_warning_count"] = len(sanity_warnings)
        f["feature_sanity_warnings"] = sanity_warnings

        f["feature_version"] = "freqai_feature_store_v1_20260614"
        f["created_at_utc"] = run_utc
        f["created_at_wib"] = run_wib

        rows.append(f)

    with OUT_LOG.open("a", encoding="utf-8") as out:
        for r in rows:
            out.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n")

    latest = {
        "ok": True,
        "feature_version": "freqai_feature_store_v1_20260614",
        "created_at_utc": run_utc,
        "created_at_wib": run_wib,
        "symbols": symbols,
        "count": len(rows),
        "rows": rows,
    }
    OUT_STATE.write_text(json.dumps(latest, ensure_ascii=False, indent=2))

    print(json.dumps({
        "ok": True,
        "feature_version": latest["feature_version"],
        "count": len(rows),
        "out_log": str(OUT_LOG),
        "out_state": str(OUT_STATE),
        "created_at_wib": run_wib,
    }, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
