#!/usr/bin/env python3
import json, os, math, time
from pathlib import Path
from datetime import datetime, timezone, timedelta

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "logs"
REPORT_DIR = ROOT / "reports"
STATE_DIR = ROOT / "state"

OUT_LOG = LOG_DIR / "vps_ct_scalp_shadow_signals.jsonl"
OUT_REPORT = REPORT_DIR / "vps_ct_scalp_shadow_report.json"

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

def now_utc():
    return datetime.now(timezone.utc)

def now_wib_text():
    return datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S WIB")

def wib_text(ms):
    try:
        return datetime.fromtimestamp(int(ms) / 1000, timezone.utc).astimezone(WIB).strftime("%Y-%m-%d %H:%M:%S WIB")
    except Exception:
        return None

def norm_symbol(x):
    s = str(x or "").strip().upper()
    s = s.replace("BINANCE:", "").replace(".P", "").replace("/", "").replace("-", "")
    return s

def symbols():
    raw = os.getenv("PAIR_ALLOWLIST", "")
    arr = []
    for x in raw.replace(";", ",").split(","):
        s = norm_symbol(x)
        if s and s not in arr:
            arr.append(s)
    return arr or ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

def market_dir():
    raw = str(os.getenv("BINANCE_CANDLE_STORE_DIR") or "").strip()
    return Path(raw) if raw else STATE_DIR / "market_data"

def candle_file(symbol, interval):
    return market_dir() / f"{symbol}_{interval}.jsonl"

def load_candles(symbol, interval):
    path = candle_file(symbol, interval)
    if not path.exists():
        return []

    out = []
    for line in path.read_text(errors="ignore").splitlines():
        try:
            r = json.loads(line)
            if env_bool("VPS_SMC_USE_CLOSED_CANDLES_ONLY", True) and not bool(r.get("is_closed", False)):
                continue
            ot = int(r.get("open_time_ms"))
            ct = int(r.get("close_time_ms"))
            out.append({
                "t": ct,
                "tBucketMs": ot,
                "o": float(r.get("open")),
                "h": float(r.get("high")),
                "l": float(r.get("low")),
                "c": float(r.get("close")),
                "v": float(r.get("volume") or 0),
            })
        except Exception:
            continue

    out.sort(key=lambda x: int(x.get("tBucketMs") or 0))
    return out

def pct_dist(a, b):
    try:
        a = float(a); b = float(b)
        if b == 0:
            return None
        return abs(a - b) / abs(b) * 100.0
    except Exception:
        return None

def detect_pivots(candles, left=2, right=2):
    hs, ls = [], []
    n = len(candles)
    if n < left + right + 1:
        return hs, ls

    for i in range(left, n - right):
        h = float(candles[i]["h"])
        l = float(candles[i]["l"])
        left_h = [float(candles[j]["h"]) for j in range(i-left, i)]
        right_h = [float(candles[j]["h"]) for j in range(i+1, i+1+right)]
        left_l = [float(candles[j]["l"]) for j in range(i-left, i)]
        right_l = [float(candles[j]["l"]) for j in range(i+1, i+1+right)]

        if all(h > x for x in left_h + right_h):
            hs.append({"idx": i, "price": h, "t": candles[i]["t"]})
        if all(l < x for x in left_l + right_l):
            ls.append({"idx": i, "price": l, "t": candles[i]["t"]})

    return hs, ls

def calc_atr(candles, end_idx, length=14):
    if end_idx <= 0:
        return None

    start = max(1, end_idx - length + 1)
    trs = []

    for i in range(start, end_idx + 1):
        h = float(candles[i]["h"])
        l = float(candles[i]["l"])
        pc = float(candles[i-1]["c"])
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))

    if not trs:
        return None
    return sum(trs) / len(trs)

def htf_context(htf):
    if len(htf) < 50:
        return {"ok": False, "reason": f"htf_too_few:{len(htf)}"}

    recent = htf[-80:]
    hi = max(float(c["h"]) for c in recent)
    lo = min(float(c["l"]) for c in recent)
    close = float(htf[-1]["c"])
    prev = float(htf[-2]["c"])
    eq = (hi + lo) / 2.0

    location = "discount" if close < eq else "premium" if close > eq else "equilibrium"
    bias = "bull" if close > prev else "bear" if close < prev else "range"

    return {
        "ok": True,
        "bias": bias,
        "location": location,
        "range_high": hi,
        "range_low": lo,
        "eq": eq,
        "close": close,
    }

def liquidity_context(entry, price):
    highs, lows = detect_pivots(entry, 2, 2)
    bsl = highs[-1]["price"] if highs else None
    ssl = lows[-1]["price"] if lows else None

    near_pct = env_float("VPS_CT_SCALP_NEAR_LIQ_PCT", 0.35)
    dist_bsl = pct_dist(price, bsl) if bsl is not None else None
    dist_ssl = pct_dist(price, ssl) if ssl is not None else None

    ctx = "Between"
    if dist_bsl is not None and dist_bsl <= near_pct:
        ctx = "Near BSL"
    if dist_ssl is not None and dist_ssl <= near_pct:
        ctx = "Near SSL"

    return {
        "ctx": ctx,
        "bsl": bsl,
        "ssl": ssl,
        "dist_bsl_pct": dist_bsl,
        "dist_ssl_pct": dist_ssl,
    }

def find_sweep(stageb, direction, level):
    if level is None:
        return None

    lookback = env_int("VPS_CT_SCALP_LOOKBACK_5M", 30)
    scan = stageb[-max(5, lookback):]

    for c in reversed(scan):
        low = float(c["l"])
        high = float(c["h"])
        close = float(c["c"])
        t = int(c["t"])

        if direction == "LONG" and low < float(level) and close > float(level):
            return {
                "sweep_t": t,
                "sweep_t_wib": wib_text(t),
                "sweep_level": float(level),
                "sweep_extreme": low,
                "sweep_tag": "CT_SSL_SWEEP_RECLAIM",
            }

        if direction == "SHORT" and high > float(level) and close < float(level):
            return {
                "sweep_t": t,
                "sweep_t_wib": wib_text(t),
                "sweep_level": float(level),
                "sweep_extreme": high,
                "sweep_tag": "CT_BSL_SWEEP_RECLAIM",
            }

    return None

def index_at_or_after(candles, t):
    try:
        t = int(t)
    except Exception:
        return None

    for i, c in enumerate(candles):
        if int(c["t"]) >= t:
            return i
    return None

def find_mss(stageb, direction, sweep_t):
    idx = index_at_or_after(stageb, sweep_t)
    if idx is None or idx < 5:
        return None

    lb = env_int("VPS_CT_SCALP_MSS_LOOKBACK_5M", 12)
    pre = stageb[max(0, idx-lb):idx]
    after = stageb[idx:min(len(stageb), idx+lb+1)]

    if not pre or not after:
        return None

    if direction == "LONG":
        level = max(float(c["h"]) for c in pre)
        for j, c in enumerate(after, start=idx):
            if float(c["c"]) > level:
                return {
                    "mss_t": int(c["t"]),
                    "mss_t_wib": wib_text(c["t"]),
                    "mss_idx": j,
                    "mss_level": level,
                    "mss_type": "BULLISH_CHOCH_PROXY",
                }

    if direction == "SHORT":
        level = min(float(c["l"]) for c in pre)
        for j, c in enumerate(after, start=idx):
            if float(c["c"]) < level:
                return {
                    "mss_t": int(c["t"]),
                    "mss_t_wib": wib_text(c["t"]),
                    "mss_idx": j,
                    "mss_level": level,
                    "mss_type": "BEARISH_CHOCH_PROXY",
                }

    return None

def displacement_ok(stageb, idx, direction):
    if idx is None or idx <= 1 or idx >= len(stageb):
        return None

    atr_len = env_int("VPS_SMC_DISPLACEMENT_ATR_LEN", 14)
    atr_mult = env_float("VPS_SMC_DISPLACEMENT_ATR_MULT", 1.15)
    min_body_pct = env_float("VPS_SMC_DISPLACEMENT_MIN_BODY_PCT", 55.0)

    c = stageb[idx]
    atr = calc_atr(stageb, idx, atr_len)
    if atr is None:
        return None

    o = float(c["o"])
    h = float(c["h"])
    l = float(c["l"])
    close = float(c["c"])
    rng = h - l
    if rng <= 0:
        return None

    body = close - o
    body_pct = abs(body) / rng * 100.0

    directional_ok = (direction == "LONG" and body > 0) or (direction == "SHORT" and body < 0)
    range_ok = rng >= atr * atr_mult
    body_ok = body_pct >= min_body_pct

    if directional_ok and range_ok and body_ok:
        return {
            "disp_t": int(c["t"]),
            "disp_t_wib": wib_text(c["t"]),
            "disp_idx": idx,
            "body_pct": body_pct,
            "range": rng,
            "atr": atr,
        }

    return None

def find_displacement(stageb, mss_idx, direction):
    for i in range(mss_idx, min(len(stageb), mss_idx + 4)):
        d = displacement_ok(stageb, i, direction)
        if d:
            return d
    return None

def find_fvg(stageb, direction, after_idx):
    if after_idx is None:
        return None

    end = min(len(stageb), after_idx + 14)
    start = max(2, after_idx - 1)

    for i in range(start, end):
        c0 = stageb[i-2]
        c2 = stageb[i]
        c0_h = float(c0["h"])
        c0_l = float(c0["l"])
        c2_h = float(c2["h"])
        c2_l = float(c2["l"])
        t = int(c2["t"])

        if direction == "LONG" and c0_h < c2_l:
            return {
                "fvg_type": "BULLISH",
                "fvg_lo": c0_h,
                "fvg_hi": c2_l,
                "fvg_mid": (c0_h + c2_l) / 2.0,
                "fvg_t": t,
                "fvg_t_wib": wib_text(t),
                "fvg_idx": i,
            }

        if direction == "SHORT" and c0_l > c2_h:
            return {
                "fvg_type": "BEARISH",
                "fvg_lo": c2_h,
                "fvg_hi": c0_l,
                "fvg_mid": (c2_h + c0_l) / 2.0,
                "fvg_t": t,
                "fvg_t_wib": wib_text(t),
                "fvg_idx": i,
            }

    return None


def find_retest_rejection(stageb, direction, fvg):
    if not fvg:
        return None

    try:
        zlo = min(float(fvg["fvg_lo"]), float(fvg["fvg_hi"]))
        zhi = max(float(fvg["fvg_lo"]), float(fvg["fvg_hi"]))
        mid = (zlo + zhi) / 2.0
        start_idx = int(fvg.get("fvg_idx") or 0) + 1
    except Exception:
        return None

    lookforward = env_int("VPS_CT_SCALP_RETEST_LOOKFORWARD_5M", 18)
    end_idx = min(len(stageb), start_idx + max(1, lookforward))
    direction = str(direction or "").upper()

    for i in range(start_idx, end_idx):
        c = stageb[i]
        try:
            o = float(c["o"])
            h = float(c["h"])
            l = float(c["l"])
            close = float(c["c"])
            t = int(c["t"])
        except Exception:
            continue

        touched = (l <= zhi and h >= zlo)
        if not touched:
            continue

        if direction == "LONG":
            rejection_ok = close > o and close >= mid
        elif direction == "SHORT":
            rejection_ok = close < o and close <= mid
        else:
            rejection_ok = False

        if rejection_ok:
            return {
                "retest_t": t,
                "retest_t_wib": wib_text(t),
                "retest_idx": i,
                "zone_lo": zlo,
                "zone_hi": zhi,
                "zone_mid": mid,
                "close": close,
                "open": o,
                "high": h,
                "low": l,
                "reason": "fvg_retest_rejection_close_ok",
            }

    return None


def build_plan(direction, fvg, sweep_extreme):
    entry = float(fvg["fvg_mid"])
    ext = float(sweep_extreme)
    buf_pct = env_float("VPS_SMC_INVALID_BUFFER_PCT", 0.08) + env_float("VPS_SMC_FEES_BUFFER_PCT", 0.03)
    buf = buf_pct / 100.0

    if direction == "LONG":
        sl = ext * (1 - buf)
        risk = entry - sl
        if risk <= 0:
            return None
        tp1 = entry + risk
        tp2 = entry + risk * 1.5
        tp3 = entry + risk * 2.5

    elif direction == "SHORT":
        sl = ext * (1 + buf)
        risk = sl - entry
        if risk <= 0:
            return None
        tp1 = entry - risk
        tp2 = entry - risk * 1.5
        tp3 = entry - risk * 2.5

    else:
        return None

    rr_tp2 = abs(tp2 - entry) / abs(entry - sl)

    return {
        "entry_mid": entry,
        "entry_lo": min(float(fvg["fvg_lo"]), float(fvg["fvg_hi"])),
        "entry_hi": max(float(fvg["fvg_lo"]), float(fvg["fvg_hi"])),
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "rr_tp2": rr_tp2,
    }

def orderbook_status(symbol):
    path = REPORT_DIR / "orderbook_pricing_sim_v1.json"
    if not path.exists():
        return None

    try:
        j = json.loads(path.read_text(errors="ignore"))
        for r in j.get("rows") or []:
            if str(r.get("symbol") or "").upper() == symbol:
                return r.get("pricing_status")
    except Exception:
        return None

    return None

def score_ct(htf, liq, sweep, mss, disp, fvg, plan, ob):
    score = 0

    if htf.get("location") in ("discount", "premium"):
        score += 15
    if liq.get("ctx") in ("Near SSL", "Near BSL"):
        score += 15
    if sweep:
        score += 20
    if mss:
        score += 15
    if disp:
        score += 15
    if fvg:
        score += 10
    if plan and float(plan.get("rr_tp2") or 0) >= env_float("VPS_CT_SCALP_MIN_RR_TP2", 1.20):
        score += 10
    if ob == "OK":
        score += 5
    if ob in ("THIN", "ERROR"):
        score -= 20

    return max(0, min(100, int(round(score))))

def read_existing_keys():
    keys = set()
    if not OUT_LOG.exists():
        return keys
    for line in OUT_LOG.read_text(errors="ignore").splitlines():
        try:
            j = json.loads(line)
            k = j.get("signal_key")
            if k:
                keys.add(str(k))
        except Exception:
            pass
    return keys

def append_jsonl(path, row):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")

def analyze_symbol(symbol):
    htf_interval = os.getenv("VPS_SMC_INTERVAL_HTF", "4h")
    entry_interval = os.getenv("VPS_SMC_INTERVAL_ENTRY", "15m")
    stageb_interval = os.getenv("VPS_SMC_INTERVAL_STAGEB", "5m")

    htf = load_candles(symbol, htf_interval)
    entry = load_candles(symbol, entry_interval)
    stageb = load_candles(symbol, stageb_interval)

    if len(htf) < 50 or len(entry) < 80 or len(stageb) < 120:
        return {
            "symbol": symbol,
            "ok": False,
            "status": "WAIT_DATA",
            "reason": f"WAIT_DATA htf={len(htf)} entry={len(entry)} stageb={len(stageb)}",
        }

    hctx = htf_context(htf)
    price = float(stageb[-1]["c"])
    liq = liquidity_context(entry, price)

    candidates = []

    if hctx.get("location") == "discount" and liq.get("ctx") == "Near SSL":
        candidates.append(("LONG", liq.get("ssl")))

    if hctx.get("location") == "premium" and liq.get("ctx") == "Near BSL":
        candidates.append(("SHORT", liq.get("bsl")))

    if not candidates:
        return {
            "symbol": symbol,
            "ok": True,
            "status": "IDLE",
            "reason": "no_ct_location_liq_match",
            "htf": hctx,
            "liq": liq,
        }

    best = None

    for direction, level in candidates:
        sweep = find_sweep(stageb, direction, level)
        if not sweep:
            continue

        mss = find_mss(stageb, direction, sweep["sweep_t"])
        if not mss:
            continue

        disp = find_displacement(stageb, mss["mss_idx"], direction)
        if not disp:
            continue

        fvg = find_fvg(stageb, direction, disp["disp_idx"])
        if env_bool("VPS_CT_SCALP_REQUIRE_FVG", True) and not fvg:
            continue
        if not fvg:
            continue

        retest = find_retest_rejection(stageb, direction, fvg)
        if env_bool("VPS_CT_SCALP_REQUIRE_RETEST_REJECTION", True) and not retest:
            continue

        plan = build_plan(direction, fvg, sweep["sweep_extreme"])
        if not plan:
            continue

        if float(plan.get("rr_tp2") or 0) < env_float("VPS_CT_SCALP_MIN_RR_TP2", 1.20):
            continue

        ob = orderbook_status(symbol)
        if env_bool("VPS_CT_SCALP_REQUIRE_ORDERBOOK_OK", True) and ob != "OK":
            continue

        score = score_ct(hctx, liq, sweep, mss, disp, fvg, plan, ob)
        if score < env_int("VPS_CT_SCALP_MIN_SCORE", 72):
            continue

        signal_key = f"VPS_CT_SCALP|{symbol}|{direction}|{sweep['sweep_t']}"

        row = {
            "event_type": "VPS_CT_SCALP_SHADOW",
            "source": "VPS_CT_SCALP",
            "engine": "VPS_CT_SCALP",
            "mode": os.getenv("VPS_CT_SCALP_MODE", "SHADOW_ONLY"),
            "symbol": symbol,
            "direction": direction,
            "status": "CONFIRMED_SHADOW",
            "score": score,
            "priority": "HIGH" if score >= 88 else "MEDIUM",
            "risk_mult": env_float("VPS_CT_SCALP_RISK_MULT", env_float("VPS_CT_SCALP_LIVE_RISK_MULT", 0.10)),
            "signal_key": signal_key,
            "created_at_utc": now_utc().isoformat(),
            "created_at_wib": now_wib_text(),
            "htf": hctx,
            "liq": liq,
            "sweep": sweep,
            "mss": mss,
            "displacement": disp,
            "fvg": fvg,
            "retest": retest,
            "plan": plan,
            "orderbook_status": ob,
            "notes": "VPS CT scalp shadow. No execution from this script.",
        }

        if best is None or row["score"] > best["score"]:
            best = row

    if best:
        return best

    return {
        "symbol": symbol,
        "ok": True,
        "status": "IDLE",
        "reason": "no_confirmed_ct_shadow",
        "htf": hctx,
        "liq": liq,
    }

def main():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    if not env_bool("VPS_CT_SCALP_ENABLED", False):
        report = {
            "ok": True,
            "enabled": False,
            "reason": "VPS_CT_SCALP_ENABLED_false",
            "created_at_wib": now_wib_text(),
        }
        OUT_REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2))
        print(json.dumps(report, ensure_ascii=False))
        return

    existing = read_existing_keys()
    rows = []
    confirmed = []
    appended = []
    errors = []

    for symbol in symbols():
        try:
            r = analyze_symbol(symbol)
            rows.append(r)

            if r.get("event_type") == "VPS_CT_SCALP_SHADOW":
                confirmed.append(r)
                if r.get("signal_key") not in existing:
                    append_jsonl(OUT_LOG, r)
                    appended.append(r.get("signal_key"))
                    existing.add(r.get("signal_key"))

        except Exception as e:
            err = {"symbol": symbol, "ok": False, "status": "ERROR", "reason": repr(e)}
            rows.append(err)
            errors.append(err)

    status_counts = {}
    for r in rows:
        s = str(r.get("status") or "UNKNOWN")
        status_counts[s] = status_counts.get(s, 0) + 1

    report = {
        "ok": len(errors) == 0,
        "enabled": True,
        "report_version": "vps_ct_scalp_shadow_v1_20260616",
        "mode": os.getenv("VPS_CT_SCALP_MODE", "SHADOW_ONLY"),
        "created_at_wib": now_wib_text(),
        "symbols": len(symbols()),
        "confirmed_shadow_count": len(confirmed),
        "new_appended_count": len(appended),
        "status_counts": status_counts,
        "errors": errors[:20],
        "confirmed_shadow": confirmed,
        "rows": rows,
        "notes": [
            "REPORT_ONLY/SHADOW source. This script does not execute orders.",
            "Auto-promoter reads vps_ct_scalp_outcomes after outcome evaluator runs.",
            "Promotion gates decide if VPS_CT_SCALP_MODE can become LIVE_BLOCKED_BY_DEFAULT.",
        ],
    }

    OUT_REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2))

    print("=== VPS CT SCALP SHADOW V1 ===")
    print("mode:", report["mode"])
    print("symbols:", report["symbols"])
    print("confirmed_shadow_count:", report["confirmed_shadow_count"])
    print("new_appended_count:", report["new_appended_count"])
    print("status_counts:", report["status_counts"])
    print("errors:", len(errors))

    for r in confirmed[:20]:
        p = r.get("plan") or {}
        print(
            f"{r.get('symbol')} {r.get('direction')} score={r.get('score')} "
            f"entry={p.get('entry_mid')} sl={p.get('sl')} tp1={p.get('tp1')} rr2={p.get('rr_tp2')}"
        )

if __name__ == "__main__":
    main()
