#!/usr/bin/env python3
import argparse
import json
import math
import os
import signal
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

try:
    import websocket
    from websocket import WebSocketTimeoutException
except Exception as e:
    raise SystemExit(f"missing websocket-client dependency: {e}")

ROOT = Path(".")
LOGS = ROOT / "logs"
REPORTS = ROOT / "reports"
LOGS.mkdir(exist_ok=True)
REPORTS.mkdir(exist_ok=True)

RAW_LOG = LOGS / "binance_liquidation_events_v1.jsonl"
FEATURE_LOG = LOGS / "feature_store_binance_liquidations_v1.jsonl"
REPORT = REPORTS / "feature_store_binance_liquidations_v1_report.json"

WS_URL = os.getenv("BINANCE_LIQ_WS_URL", "wss://fstream.binance.com/ws/!forceOrder@arr")

DEFAULT_SYMBOLS = [
    "ADAUSDT", "AVAXUSDT", "BCHUSDT", "BTCUSDT", "ETHUSDT",
    "HYPEUSDT", "LINKUSDT", "LTCUSDT", "PAXGUSDT", "SOLUSDT",
    "SUIUSDT", "UNIUSDT", "XRPUSDT", "ZECUSDT",
]

STOP = False

def on_signal(sig, frame):
    global STOP
    STOP = True

signal.signal(signal.SIGINT, on_signal)
signal.signal(signal.SIGTERM, on_signal)

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def to_float(x, default=0.0):
    try:
        if x is None or x == "":
            return default
        v = float(x)
        return v if math.isfinite(v) else default
    except Exception:
        return default

def parse_symbols(raw):
    raw = raw or os.getenv("LIQ_FEATURE_SYMBOLS") or os.getenv("CURRENT14_SYMBOLS") or ""
    if raw.strip():
        return set(x.strip().upper() for x in raw.replace(";", ",").split(",") if x.strip())
    return set(DEFAULT_SYMBOLS)

def append_jsonl(path, row):
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")

def prune(q, now_ms, window_ms):
    cutoff = now_ms - window_ms
    while q and q[0]["event_time_ms"] < cutoff:
        q.popleft()

def aggregate(symbol, q, now_ms):
    windows = {
        "1m": 60_000,
        "5m": 5 * 60_000,
        "15m": 15 * 60_000,
    }

    out = {
        "created_at_utc": utc_now_iso(),
        "feature_version": "binance_liquidations_ws_v1",
        "feature_source": "binance_force_order_ws_all_market",
        "symbol": symbol,
        "ok": True,
        "source_note": "Binance !forceOrder@arr sends largest liquidation snapshot per symbol per 1000ms, not full liquidation tape.",
    }

    for name, win_ms in windows.items():
        rows = [x for x in q if x["event_time_ms"] >= now_ms - win_ms]
        long_notional = sum(x["notional"] for x in rows if x["liq_side"] == "LONG_LIQ")
        short_notional = sum(x["notional"] for x in rows if x["liq_side"] == "SHORT_LIQ")
        total = long_notional + short_notional
        largest = max([x["notional"] for x in rows], default=0.0)

        out[f"liq_long_notional_{name}"] = long_notional
        out[f"liq_short_notional_{name}"] = short_notional
        out[f"liq_total_notional_{name}"] = total
        out[f"liq_count_{name}"] = len(rows)
        out[f"liq_largest_notional_{name}"] = largest
        out[f"liq_imbalance_{name}"] = ((short_notional - long_notional) / total) if total > 0 else 0.0

    return out

def normalize_event(msg):
    data = json.loads(msg)
    o = data.get("o") or {}
    symbol = str(o.get("s") or data.get("ps") or "").upper()
    side = str(o.get("S") or "").upper()

    qty = to_float(o.get("z") or o.get("l") or o.get("q"), 0.0)
    px = to_float(o.get("ap") or o.get("p"), 0.0)
    notional = abs(qty * px)

    event_time_ms = int(data.get("E") or o.get("T") or int(time.time() * 1000))

    if side == "SELL":
        liq_side = "LONG_LIQ"
    elif side == "BUY":
        liq_side = "SHORT_LIQ"
    else:
        liq_side = "UNKNOWN"

    return {
        "created_at_utc": utc_now_iso(),
        "event_type": "BINANCE_FORCE_ORDER",
        "event_time_ms": event_time_ms,
        "symbol": symbol,
        "side": side,
        "liq_side": liq_side,
        "qty": qty,
        "price": px,
        "notional": notional,
        "order_status": o.get("X"),
        "raw": data,
    }

def write_report(rows_seen, rows_kept, symbols, last_error=None):
    report = {
        "ok": True,
        "version": "binance_liquidations_ws_v1",
        "created_at_utc": utc_now_iso(),
        "ws_url": WS_URL,
        "raw_log": str(RAW_LOG),
        "feature_log": str(FEATURE_LOG),
        "symbols": sorted(symbols),
        "rows_seen": rows_seen,
        "rows_kept": rows_kept,
        "last_error": last_error,
        "note": "Shadow liquidation feature store only. Does not alter live gate, current dataset, sizing, or trading config.",
    }
    REPORT.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

def run(max_seconds=0, flush_sec=60, symbols=None):
    symbols = parse_symbols(symbols)
    buffers = defaultdict(deque)
    rows_seen = 0
    rows_kept = 0
    last_flush = time.time()
    started = time.time()
    last_error = None

    ws = websocket.WebSocket(timeout=10)
    ws.connect(WS_URL, timeout=15)

    print(json.dumps({
        "ok": True,
        "event": "LIQ_WS_CONNECTED",
        "url": WS_URL,
        "symbols": sorted(symbols),
        "created_at_utc": utc_now_iso(),
    }, ensure_ascii=False))

    while not STOP:
        if max_seconds and time.time() - started >= max_seconds:
            break

        try:
            msg = ws.recv()
            if not msg:
                continue

            rows_seen += 1
            ev = normalize_event(msg)
            sym = ev.get("symbol")

            if sym not in symbols:
                continue

            rows_kept += 1
            append_jsonl(RAW_LOG, ev)

            q = buffers[sym]
            q.append(ev)
            prune(q, ev["event_time_ms"], 15 * 60_000)

            now = time.time()
            if now - last_flush >= flush_sec:
                now_ms = int(now * 1000)
                for s in sorted(symbols):
                    prune(buffers[s], now_ms, 15 * 60_000)
                    append_jsonl(FEATURE_LOG, aggregate(s, buffers[s], now_ms))
                write_report(rows_seen, rows_kept, symbols, last_error)
                last_flush = now

        except WebSocketTimeoutException:
            # === LIQ_WS_TIMEOUT_IDLE_FLUSH_20260627 ===
            # Binance forceOrder streams can be silent when no liquidation occurs.
            # Treat recv timeout as idle, flush zero/rolling aggregates, and keep running.
            now = time.time()
            if now - last_flush >= flush_sec:
                now_ms = int(now * 1000)
                for s in sorted(symbols):
                    prune(buffers[s], now_ms, 15 * 60_000)
                    append_jsonl(FEATURE_LOG, aggregate(s, buffers[s], now_ms))
                last_error = None
                write_report(rows_seen, rows_kept, symbols, last_error)
                last_flush = now
            continue

        except Exception as e:
            last_error = str(e)[:240]
            write_report(rows_seen, rows_kept, symbols, last_error)
            raise

    now_ms = int(time.time() * 1000)
    for s in sorted(symbols):
        append_jsonl(FEATURE_LOG, aggregate(s, buffers[s], now_ms))
    write_report(rows_seen, rows_kept, symbols, last_error)

    try:
        ws.close()
    except Exception:
        pass

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", default="")
    ap.add_argument("--max-seconds", type=int, default=0)
    ap.add_argument("--flush-sec", type=int, default=60)
    args = ap.parse_args()
    run(max_seconds=args.max_seconds, flush_sec=args.flush_sec, symbols=args.symbols)

if __name__ == "__main__":
    main()
