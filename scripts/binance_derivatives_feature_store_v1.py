#!/usr/bin/env python3
import argparse
import json
import math
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path
from statistics import mean, pstdev

ROOT = Path(".")
LOGS = ROOT / "logs"
REPORTS = ROOT / "reports"
LOGS.mkdir(exist_ok=True)
REPORTS.mkdir(exist_ok=True)

OUT_JSONL = LOGS / "feature_store_binance_derivatives_v1.jsonl"
OUT_REPORT = REPORTS / "feature_store_binance_derivatives_v1_report.json"
OUT_LIQ_WS_JSONL = LOGS / "feature_store_binance_liquidations_v1.jsonl"

# === READ_LIQUIDATION_WS_FEATURES_20260627 ===
def read_latest_liq_ws_features(symbol, max_age_sec=20 * 60):
    if not OUT_LIQ_WS_JSONL.exists():
        return None
    now_ts = datetime.now(timezone.utc).timestamp()
    latest = None
    try:
        with OUT_LIQ_WS_JSONL.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                if str(r.get("symbol") or "").upper() != symbol:
                    continue
                ts = r.get("created_at_utc")
                try:
                    t = datetime.fromisoformat(str(ts).replace("Z", "+00:00")).astimezone(timezone.utc).timestamp()
                except Exception:
                    continue
                if now_ts - t <= max_age_sec:
                    latest = r
    except Exception:
        return None
    return latest


DEFAULT_SYMBOLS = [
    "ADAUSDT", "AVAXUSDT", "BCHUSDT", "BTCUSDT", "ETHUSDT",
    "HYPEUSDT", "LINKUSDT", "LTCUSDT", "PAXGUSDT", "SOLUSDT",
    "SUIUSDT", "UNIUSDT", "XRPUSDT", "ZECUSDT",
]

BASE = os.getenv("BINANCE_FAPI_BASE_URL", "https://fapi.binance.com").rstrip("/")
FUT_DATA_BASE = os.getenv("BINANCE_FUTURES_DATA_BASE_URL", "https://fapi.binance.com").rstrip("/")

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def to_float(x, default=None):
    try:
        if x is None or x == "":
            return default
        v = float(x)
        if math.isfinite(v):
            return v
        return default
    except Exception:
        return default

def safe_div(a, b, default=None):
    try:
        if b in (0, 0.0, None):
            return default
        return a / b
    except Exception:
        return default

def pct_change(now, old):
    if old in (None, 0, 0.0) or now is None:
        return None
    return (now - old) / old

def zscore(values):
    vals = [v for v in values if v is not None and math.isfinite(v)]
    if len(vals) < 10:
        return None
    mu = mean(vals)
    sd = pstdev(vals)
    if sd <= 0:
        return None
    return (vals[-1] - mu) / sd

def http_get(path, params=None, base=None, timeout=12):
    base = base or BASE
    params = params or {}
    url = base + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "ai-trading-derivatives-feature-store-v1",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read().decode("utf-8", errors="replace")
        return json.loads(raw)

def latest_sorted(rows, ts_key="timestamp"):
    clean = []
    for r in rows or []:
        try:
            clean.append(r)
        except Exception:
            pass
    clean.sort(key=lambda x: int(x.get(ts_key) or 0))
    return clean

def fetch_oi_hist(symbol, period="15m", limit=96):
    return latest_sorted(http_get(
        "/futures/data/openInterestHist",
        {"symbol": symbol, "period": period, "limit": limit},
        base=FUT_DATA_BASE,
    ))

def fetch_taker(symbol, period="15m", limit=96):
    return latest_sorted(http_get(
        "/futures/data/takerlongshortRatio",
        {"symbol": symbol, "period": period, "limit": limit},
        base=FUT_DATA_BASE,
    ))

def fetch_global_ls(symbol, period="15m", limit=96):
    return latest_sorted(http_get(
        "/futures/data/globalLongShortAccountRatio",
        {"symbol": symbol, "period": period, "limit": limit},
        base=FUT_DATA_BASE,
    ))

def fetch_top_pos(symbol, period="15m", limit=96):
    return latest_sorted(http_get(
        "/futures/data/topLongShortPositionRatio",
        {"symbol": symbol, "period": period, "limit": limit},
        base=FUT_DATA_BASE,
    ))

def fetch_present_oi(symbol):
    return http_get("/fapi/v1/openInterest", {"symbol": symbol}, base=BASE)

def fetch_premium(symbol):
    return http_get("/fapi/v1/premiumIndex", {"symbol": symbol}, base=BASE)

def fetch_liquidations_optional(symbol, minutes=15):
    # Fail-soft. If this endpoint is unavailable/restricted, dataset still writes without liq fields.
    end_ms = int(time.time() * 1000)
    start_ms = int((datetime.now(timezone.utc) - timedelta(minutes=minutes)).timestamp() * 1000)
    try:
        rows = http_get(
            "/fapi/v1/allForceOrders",
            {"symbol": symbol, "startTime": start_ms, "endTime": end_ms, "limit": 100},
            base=BASE,
            timeout=8,
        )
        if not isinstance(rows, list):
            return [], False, "not_list"
        return rows, True, None
    except Exception as e:
        return [], False, str(e)[:180]

def compute_symbol(symbol):
    row = {
        "created_at_utc": utc_now_iso(),
        "feature_version": "binance_derivatives_feature_store_v1",
        "feature_source": "binance_public_futures_rest",
        "symbol": symbol,
        "period": "15m",
    }

    errors = []

    try:
        present_oi = fetch_present_oi(symbol)
        row["deriv_oi_present"] = to_float(present_oi.get("openInterest"))
        row["deriv_oi_present_time_ms"] = int(present_oi.get("time") or 0) or None
    except Exception as e:
        errors.append(f"present_oi:{str(e)[:120]}")

    try:
        prem = fetch_premium(symbol)
        mark = to_float(prem.get("markPrice"))
        index = to_float(prem.get("indexPrice"))
        row["deriv_mark_price"] = mark
        row["deriv_index_price"] = index
        row["deriv_funding_rate"] = to_float(prem.get("lastFundingRate"))
        row["deriv_interest_rate"] = to_float(prem.get("interestRate"))
        row["deriv_next_funding_time_ms"] = int(prem.get("nextFundingTime") or 0) or None
        row["deriv_premium_basis_pct"] = safe_div((mark - index), index) if mark is not None and index else None
        row["deriv_premium_time_ms"] = int(prem.get("time") or 0) or None
    except Exception as e:
        errors.append(f"premium:{str(e)[:120]}")

    try:
        oi = fetch_oi_hist(symbol)
        vals = [to_float(x.get("sumOpenInterest")) for x in oi]
        val_usdt = [to_float(x.get("sumOpenInterestValue")) for x in oi]
        if oi:
            last = oi[-1]
            row["deriv_oi_hist_time_ms"] = int(last.get("timestamp") or 0) or None
            row["deriv_oi_hist"] = vals[-1]
            row["deriv_oi_value_usdt"] = val_usdt[-1]
            row["deriv_oi_chg_15m"] = pct_change(vals[-1], vals[-2]) if len(vals) >= 2 else None
            row["deriv_oi_chg_1h"] = pct_change(vals[-1], vals[-5]) if len(vals) >= 5 else None
            row["deriv_oi_chg_4h"] = pct_change(vals[-1], vals[-17]) if len(vals) >= 17 else None
            row["deriv_oi_z_24h"] = zscore(vals[-96:]) if len(vals) >= 20 else None
            row["deriv_oi_value_chg_1h"] = pct_change(val_usdt[-1], val_usdt[-5]) if len(val_usdt) >= 5 else None
    except Exception as e:
        errors.append(f"oi_hist:{str(e)[:120]}")

    try:
        tk = fetch_taker(symbol)
        buy = [to_float(x.get("buyVol")) for x in tk]
        sell = [to_float(x.get("sellVol")) for x in tk]
        ratio = [to_float(x.get("buySellRatio")) for x in tk]
        if tk:
            b = buy[-1] or 0.0
            s = sell[-1] or 0.0
            row["deriv_taker_time_ms"] = int(tk[-1].get("timestamp") or 0) or None
            row["deriv_taker_buy_vol_15m"] = buy[-1]
            row["deriv_taker_sell_vol_15m"] = sell[-1]
            row["deriv_taker_buy_sell_ratio"] = ratio[-1]
            row["deriv_taker_imbalance_15m"] = safe_div((b - s), (b + s), 0.0)
            row["deriv_taker_ratio_z_24h"] = zscore(ratio[-96:]) if len(ratio) >= 20 else None
    except Exception as e:
        errors.append(f"taker:{str(e)[:120]}")

    try:
        gls = fetch_global_ls(symbol)
        ratios = [to_float(x.get("longShortRatio")) for x in gls]
        if gls:
            last = gls[-1]
            row["deriv_global_ls_time_ms"] = int(last.get("timestamp") or 0) or None
            row["deriv_global_long_short_ratio"] = ratios[-1]
            row["deriv_global_long_account"] = to_float(last.get("longAccount"))
            row["deriv_global_short_account"] = to_float(last.get("shortAccount"))
            row["deriv_global_ls_z_24h"] = zscore(ratios[-96:]) if len(ratios) >= 20 else None
    except Exception as e:
        errors.append(f"global_ls:{str(e)[:120]}")

    try:
        top = fetch_top_pos(symbol)
        ratios = [to_float(x.get("longShortRatio")) for x in top]
        if top:
            last = top[-1]
            row["deriv_top_pos_time_ms"] = int(last.get("timestamp") or 0) or None
            row["deriv_top_pos_long_short_ratio"] = ratios[-1]
            row["deriv_top_pos_long_account"] = to_float(last.get("longAccount"))
            row["deriv_top_pos_short_account"] = to_float(last.get("shortAccount"))
            row["deriv_top_pos_ls_z_24h"] = zscore(ratios[-96:]) if len(ratios) >= 20 else None
    except Exception as e:
        errors.append(f"top_pos:{str(e)[:120]}")

    # === USE_LIQUIDATION_WS_FEATURES_20260627 ===
    liq_ws = read_latest_liq_ws_features(symbol)
    if liq_ws:
        row["deriv_liq_fetch_ok"] = True
        row["deriv_liq_fetch_error"] = None
        row["deriv_liq_source"] = "binance_force_order_ws_v1"
        row["deriv_liq_long_notional_1m"] = liq_ws.get("liq_long_notional_1m")
        row["deriv_liq_short_notional_1m"] = liq_ws.get("liq_short_notional_1m")
        row["deriv_liq_total_notional_1m"] = liq_ws.get("liq_total_notional_1m")
        row["deriv_liq_imbalance_1m"] = liq_ws.get("liq_imbalance_1m")
        row["deriv_liq_long_notional_5m"] = liq_ws.get("liq_long_notional_5m")
        row["deriv_liq_short_notional_5m"] = liq_ws.get("liq_short_notional_5m")
        row["deriv_liq_total_notional_5m"] = liq_ws.get("liq_total_notional_5m")
        row["deriv_liq_imbalance_5m"] = liq_ws.get("liq_imbalance_5m")
        row["deriv_liq_long_notional_15m"] = liq_ws.get("liq_long_notional_15m")
        row["deriv_liq_short_notional_15m"] = liq_ws.get("liq_short_notional_15m")
        row["deriv_liq_total_notional_15m"] = liq_ws.get("liq_total_notional_15m")
        row["deriv_liq_order_count_15m"] = liq_ws.get("liq_count_15m")
        row["deriv_liq_largest_notional_15m"] = liq_ws.get("liq_largest_notional_15m")
        row["deriv_liq_imbalance_15m"] = liq_ws.get("liq_imbalance_15m")
    else:
        row["deriv_liq_fetch_ok"] = False
        row["deriv_liq_fetch_error"] = "liq_ws_feature_missing_or_stale"
        row["deriv_liq_source"] = "missing"
        row["deriv_liq_long_notional_15m"] = None
        row["deriv_liq_short_notional_15m"] = None
        row["deriv_liq_total_notional_15m"] = None
        row["deriv_liq_order_count_15m"] = None
        row["deriv_liq_imbalance_15m"] = None

    # Conservative SMC-compatible derived scores, not live gates.
    funding_z = row.get("deriv_global_ls_z_24h") or 0.0
    oi_z = row.get("deriv_oi_z_24h") or 0.0
    taker_z = row.get("deriv_taker_ratio_z_24h") or 0.0
    top_z = row.get("deriv_top_pos_ls_z_24h") or 0.0
    row["deriv_crowding_score_raw"] = 0.30 * funding_z + 0.25 * oi_z + 0.25 * taker_z + 0.20 * top_z

    row["ok"] = len(errors) == 0
    row["errors"] = errors
    return row

def parse_symbols(arg):
    raw = arg or os.getenv("DERIV_FEATURE_SYMBOLS") or os.getenv("CURRENT14_SYMBOLS") or ""
    if raw.strip():
        return [x.strip().upper() for x in raw.replace(";", ",").split(",") if x.strip()]
    return DEFAULT_SYMBOLS

def append_jsonl(path, rows):
    with path.open("a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", default="")
    ap.add_argument("--sleep", type=float, default=0.20)
    args = ap.parse_args()

    symbols = parse_symbols(args.symbols)
    rows = []
    for sym in symbols:
        try:
            rows.append(compute_symbol(sym))
        except Exception as e:
            rows.append({
                "created_at_utc": utc_now_iso(),
                "feature_version": "binance_derivatives_feature_store_v1",
                "feature_source": "binance_public_futures_rest",
                "symbol": sym,
                "ok": False,
                "errors": [str(e)[:240]],
            })
        time.sleep(args.sleep)

    append_jsonl(OUT_JSONL, rows)

    report = {
        "ok": True,
        "version": "binance_derivatives_feature_store_v1",
        "created_at_utc": utc_now_iso(),
        "out_jsonl": str(OUT_JSONL),
        "rows_written": len(rows),
        "symbols": symbols,
        "ok_rows": sum(1 for r in rows if r.get("ok")),
        "liq_fetch_ok_rows": sum(1 for r in rows if r.get("deriv_liq_fetch_ok")),
        "error_rows": sum(1 for r in rows if r.get("errors")),
        "note": "Shadow feature store only. Does not alter live gate, current dataset, sizing, or trading config.",
    }
    OUT_REPORT.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
