#!/usr/bin/env python3
import os, csv, json, math
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import Counter
from bisect import bisect_left, bisect_right

ROOT = Path(__file__).resolve().parents[1]
WIB = timezone(timedelta(hours=7))

IN_SIGNALS = ROOT / "logs" / "ml_dataset_rows.jsonl"
MARKET_DATA_DIR = Path(os.getenv("MARKET_DATA_DIR") or (ROOT / "state" / "market_data"))

OUT_LOG = ROOT / "logs" / "forward_outcomes_v1.jsonl"
OUT_JSON = ROOT / "reports" / "forward_outcomes_builder_v1.json"
OUT_CSV = ROOT / "reports" / "forward_outcomes_builder_v1.csv"
OUT_RUNS = ROOT / "logs" / "forward_outcomes_builder_runs_v1.jsonl"

VERSION = "forward_outcomes_builder_v1_20260617"
MODE = "REPORT_ONLY"
INTERVAL = "1m"
WINDOW_HOURS = int(os.getenv("FORWARD_OUTCOME_WINDOW_HOURS", "24"))
MAX_SIGNALS = int(os.getenv("FORWARD_OUTCOME_MAX_SIGNALS", "500"))

CLOSED_STATUSES = {"TP1", "TP2", "TP3", "SL"}
CANDLE_CACHE = {}

def now_utc():
    return datetime.now(timezone.utc)

def now_wib_str():
    return now_utc().astimezone(WIB).strftime("%Y-%m-%d %H:%M:%S WIB")

def parse_dt(raw):
    if not raw:
        return None

    s = str(raw).strip()
    if not s:
        return None

    if s.endswith(" WIB"):
        s2 = s.replace(" WIB", "")
        try:
            return datetime.strptime(s2[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=WIB).astimezone(timezone.utc)
        except Exception:
            pass

    try:
        txt = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(txt)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s[:19], fmt).replace(tzinfo=timezone.utc)
        except Exception:
            pass

    return None

def dt_to_ms(dt):
    return int(dt.timestamp() * 1000)

def ms_to_wib(ms):
    try:
        return datetime.fromtimestamp(int(ms) / 1000, timezone.utc).astimezone(WIB).strftime("%Y-%m-%d %H:%M:%S WIB")
    except Exception:
        return None

def num(v):
    try:
        x = float(v)
        if math.isfinite(x):
            return x
    except Exception:
        pass
    return None

def read_jsonl(path, max_rows=None):
    if not path.exists():
        return []
    rows = []
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, (dict, list)):
                    rows.append(obj)
            except Exception:
                pass
            if max_rows and len(rows) >= max_rows:
                break
    return rows

def candle_path(symbol, interval=INTERVAL):
    return MARKET_DATA_DIR / f"{symbol}_{interval}.jsonl"

def candle_t(c):
    try:
        if isinstance(c, (list, tuple)):
            return int(c[0])
        return int(
            c.get("t")
            or c.get("open_time")
            or c.get("openTime")
            or c.get("open_time_ms")
            or c.get("close_time")
            or c.get("closeTime")
            or c.get("tBucketMs")
            or 0
        )
    except Exception:
        return 0

def candle_num(c, keys, idx=None):
    try:
        if isinstance(c, (list, tuple)) and idx is not None and len(c) > idx:
            return num(c[idx])
        if isinstance(c, dict):
            for k in keys:
                if k in c:
                    v = num(c.get(k))
                    if v is not None:
                        return v
    except Exception:
        pass
    return None

def load_candles(symbol):
    if symbol in CANDLE_CACHE:
        return CANDLE_CACHE[symbol]

    p = candle_path(symbol)
    candles = read_jsonl(p)
    out = []

    for c in candles:
        t = candle_t(c)

        # Binance list kline format commonly:
        # [open_time, open, high, low, close, volume, close_time, ...]
        o = candle_num(c, ["o", "open"], 1)
        h = candle_num(c, ["h", "high"], 2)
        l = candle_num(c, ["l", "low"], 3)
        close = candle_num(c, ["c", "close"], 4)

        if not t or h is None or l is None:
            continue

        out.append({
            "t": t,
            "o": o,
            "h": h,
            "l": l,
            "c": close,
        })

    out.sort(key=lambda x: x["t"])
    times = [x["t"] for x in out]
    CANDLE_CACHE[symbol] = (out, times)
    return CANDLE_CACHE[symbol]

def candles_window(symbol, start_ms, end_ms):
    candles, times = load_candles(symbol)
    if not candles:
        return [], [], None

    i = bisect_left(times, start_ms)
    j = bisect_right(times, end_ms)

    return candles, candles[i:j], candles[-1]["t"]

def signal_key(row):
    return str(row.get("signal_key") or row.get("signal_id") or "").strip()

def symbol_of(row):
    sym = str(row.get("symbol") or "").upper().strip()
    if sym:
        return sym
    pair = str(row.get("pair") or "").upper()
    return pair.replace("BINANCE:", "").replace(".P", "").replace("/", "").strip()

def direction_of(row):
    d = str(row.get("direction") or row.get("dir") or "").upper()
    if d.startswith("LONG"):
        return "LONG"
    if d.startswith("SHORT"):
        return "SHORT"
    return d

def time_ms_from_key(k):
    try:
        last = str(k).split("|")[-1]
        if last.isdigit():
            return int(last)
    except Exception:
        pass
    return None

def signal_time_ms(row):
    k = signal_key(row)
    ms = time_ms_from_key(k)
    if ms:
        return ms, "signal_key_bucket_ms"

    for key in ("signal_time_wib", "created_at_utc", "created_at_wib", "received_at_utc", "decision_at_utc"):
        dt = parse_dt(row.get(key))
        if dt:
            return dt_to_ms(dt), key

    return None, "missing_signal_time"

def pick_plan(row):
    entry = num(row.get("entry") or row.get("entry_mid") or row.get("entry_price"))
    sl = num(row.get("sl") or row.get("stop_loss") or row.get("invalid"))
    tp1 = num(row.get("tp1") or row.get("raw_tp1"))
    tp2 = num(row.get("tp2") or row.get("raw_tp2"))
    tp3 = num(row.get("tp3") or row.get("raw_tp3"))

    return entry, sl, tp1, tp2, tp3

def plan_ok(direction, entry, sl, tp1, tp2, tp3):
    if direction not in ("LONG", "SHORT"):
        return False, "bad_direction"

    vals = [entry, sl, tp1, tp2, tp3]
    if any(v is None or v <= 0 for v in vals):
        return False, "missing_or_nonpositive_entry_sl_tp"

    if direction == "LONG":
        if not (sl < entry):
            return False, "long_sl_not_below_entry"
        if not any(tp > entry for tp in (tp1, tp2, tp3)):
            return False, "long_no_tp_above_entry"

    if direction == "SHORT":
        if not (sl > entry):
            return False, "short_sl_not_above_entry"
        if not any(tp < entry for tp in (tp1, tp2, tp3)):
            return False, "short_no_tp_below_entry"

    return True, "ok"

def latest_unique_signals(rows):
    by_key = {}

    for r in rows:
        k = signal_key(r)
        if not k:
            continue

        sym = symbol_of(r)
        direction = direction_of(r)
        entry, sl, tp1, tp2, tp3 = pick_plan(r)

        if not sym or direction not in ("LONG", "SHORT"):
            continue

        if entry is None or sl is None or tp1 is None:
            continue

        by_key[k] = r

    return list(by_key.values())

def outcome_result(row):
    k = signal_key(row)
    sym = symbol_of(row)
    direction = direction_of(row)
    entry, sl, tp1, tp2, tp3 = pick_plan(row)
    start_ms, time_source = signal_time_ms(row)

    base = {
        "signal_key": k,
        "symbol": sym,
        "direction": direction,
        "signal_time_wib": ms_to_wib(start_ms) if start_ms else row.get("signal_time_wib"),
        "evaluated_at_utc": now_utc().isoformat(),
        "sample_type": row.get("sample_type") or "FORWARD_SHADOW_PAPER",
        "execution_decision": row.get("execution_decision"),
        "source_mode": row.get("source_mode"),
        "signal_source": row.get("signal_source"),
        "score": row.get("score"),
        "priority": row.get("priority"),
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "evaluation_window_hours": WINDOW_HOURS,
        "candle_interval": INTERVAL,
        "time_source": time_source,
    }

    ok, reason = plan_ok(direction, entry, sl, tp1, tp2, tp3)
    if not ok:
        return {
            **base,
            "outcome_status": "BAD_PLAN",
            "label_win": None,
            "label_target": None,
            "label_R": None,
            "outcome_ts_wib": None,
            "bars_to_outcome": None,
            "candles_checked": 0,
            "hit_tp1": False,
            "hit_tp2": False,
            "hit_tp3": False,
            "hit_sl": False,
            "first_hit": None,
            "same_candle_conflict": False,
            "same_candle_policy": "CONSERVATIVE_SL",
            "include_ml_label": False,
            "exclude_label_reason": reason,
        }

    if not start_ms:
        return {
            **base,
            "outcome_status": "BAD_TIME",
            "label_win": None,
            "label_target": None,
            "label_R": None,
            "outcome_ts_wib": None,
            "bars_to_outcome": None,
            "candles_checked": 0,
            "hit_tp1": False,
            "hit_tp2": False,
            "hit_tp3": False,
            "hit_sl": False,
            "first_hit": None,
            "same_candle_conflict": False,
            "same_candle_policy": "CONSERVATIVE_SL",
            "include_ml_label": False,
            "exclude_label_reason": "missing_signal_time",
        }

    end_ms = start_ms + WINDOW_HOURS * 60 * 60 * 1000
    candles, future, last_candle_ms = candles_window(sym, start_ms, end_ms)

    if not candles:
        return {
            **base,
            "outcome_status": "DATA_GAP",
            "label_win": None,
            "label_target": None,
            "label_R": None,
            "outcome_ts_wib": None,
            "bars_to_outcome": None,
            "candles_checked": 0,
            "hit_tp1": False,
            "hit_tp2": False,
            "hit_tp3": False,
            "hit_sl": False,
            "first_hit": None,
            "same_candle_conflict": False,
            "same_candle_policy": "CONSERVATIVE_SL",
            "include_ml_label": False,
            "exclude_label_reason": "missing_candle_file",
        }

    if not future:
        return {
            **base,
            "outcome_status": "DATA_GAP",
            "label_win": None,
            "label_target": None,
            "label_R": None,
            "outcome_ts_wib": None,
            "bars_to_outcome": None,
            "candles_checked": 0,
            "hit_tp1": False,
            "hit_tp2": False,
            "hit_tp3": False,
            "hit_sl": False,
            "first_hit": None,
            "same_candle_conflict": False,
            "same_candle_policy": "CONSERVATIVE_SL",
            "include_ml_label": False,
            "exclude_label_reason": "no_candles_after_signal",
        }

    fill_t = None
    for c in future:
        if c["l"] <= entry <= c["h"]:
            fill_t = c["t"]
            break

    if fill_t is None:
        closed_window = last_candle_ms >= end_ms
        return {
            **base,
            "outcome_status": "NO_FILL" if closed_window else "PENDING",
            "label_win": None,
            "label_target": None,
            "label_R": None,
            "outcome_ts_wib": None,
            "bars_to_outcome": None,
            "candles_checked": len(future),
            "hit_tp1": False,
            "hit_tp2": False,
            "hit_tp3": False,
            "hit_sl": False,
            "first_hit": None,
            "fill_t_wib": None,
            "same_candle_conflict": False,
            "same_candle_policy": "CONSERVATIVE_SL",
            "include_ml_label": False,
            "exclude_label_reason": "entry_not_filled" if closed_window else "awaiting_fill_or_window_end",
        }

    after_fill = [c for c in future if c["t"] >= fill_t]

    hit_tp1 = hit_tp2 = hit_tp3 = hit_sl = False
    same_candle_conflict = False

    status = "PENDING"
    label_win = None
    label_target = None
    label_R = None
    outcome_t = None
    bars_to_outcome = None
    first_hit = None

    for idx, c in enumerate(after_fill):
        h = c["h"]
        l = c["l"]
        t = c["t"]

        if direction == "LONG":
            sl_hit = l <= sl
            tp3_hit = h >= tp3
            tp2_hit = h >= tp2
            tp1_hit = h >= tp1

            if sl_hit and (tp1_hit or tp2_hit or tp3_hit):
                same_candle_conflict = True

            if sl_hit:
                hit_sl = True
                status, label_win, label_target, label_R = "SL", False, "SL", -1.0
                outcome_t, bars_to_outcome, first_hit = t, idx, "SL"
                break

            if tp3_hit:
                hit_tp3 = True
                status, label_win, label_target, label_R = "TP3", True, "TP3", 2.5
                outcome_t, bars_to_outcome, first_hit = t, idx, "TP3"
                break

            if tp2_hit:
                hit_tp2 = True
                status, label_win, label_target, label_R = "TP2", True, "TP2", 1.5
                outcome_t, bars_to_outcome, first_hit = t, idx, "TP2"
                break

            if tp1_hit:
                hit_tp1 = True
                status, label_win, label_target, label_R = "TP1", True, "TP1", 1.0
                outcome_t, bars_to_outcome, first_hit = t, idx, "TP1"
                break

        elif direction == "SHORT":
            sl_hit = h >= sl
            tp3_hit = l <= tp3
            tp2_hit = l <= tp2
            tp1_hit = l <= tp1

            if sl_hit and (tp1_hit or tp2_hit or tp3_hit):
                same_candle_conflict = True

            if sl_hit:
                hit_sl = True
                status, label_win, label_target, label_R = "SL", False, "SL", -1.0
                outcome_t, bars_to_outcome, first_hit = t, idx, "SL"
                break

            if tp3_hit:
                hit_tp3 = True
                status, label_win, label_target, label_R = "TP3", True, "TP3", 2.5
                outcome_t, bars_to_outcome, first_hit = t, idx, "TP3"
                break

            if tp2_hit:
                hit_tp2 = True
                status, label_win, label_target, label_R = "TP2", True, "TP2", 1.5
                outcome_t, bars_to_outcome, first_hit = t, idx, "TP2"
                break

            if tp1_hit:
                hit_tp1 = True
                status, label_win, label_target, label_R = "TP1", True, "TP1", 1.0
                outcome_t, bars_to_outcome, first_hit = t, idx, "TP1"
                break

    if status == "PENDING":
        if last_candle_ms >= end_ms:
            status = "OPEN_END"
            exclude_reason = "window_ended_no_tp_sl"
        else:
            exclude_reason = "awaiting_window_end"
    else:
        exclude_reason = None

    include_ml_label = status in CLOSED_STATUSES

    return {
        **base,
        "outcome_status": status,
        "label_win": label_win,
        "label_target": label_target,
        "label_R": label_R,
        "outcome_ts_wib": ms_to_wib(outcome_t) if outcome_t else None,
        "bars_to_outcome": bars_to_outcome,
        "candles_checked": len(after_fill),
        "hit_tp1": bool(hit_tp1),
        "hit_tp2": bool(hit_tp2),
        "hit_tp3": bool(hit_tp3),
        "hit_sl": bool(hit_sl),
        "first_hit": first_hit,
        "fill_t_wib": ms_to_wib(fill_t),
        "same_candle_conflict": same_candle_conflict,
        "same_candle_policy": "CONSERVATIVE_SL",
        "include_ml_label": include_ml_label,
        "exclude_label_reason": exclude_reason,
    }

def main():
    OUT_LOG.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)

    raw = read_jsonl(IN_SIGNALS)
    signals = latest_unique_signals(raw)

    # keep recent by signal time, then cap. Because scanning a cemetery forever is bad engineering.
    enriched = []
    for r in signals:
        ms, src = signal_time_ms(r)
        enriched.append((ms or 0, r))
    enriched.sort(key=lambda x: x[0])
    selected = [r for _, r in enriched[-MAX_SIGNALS:]]

    outcomes = [outcome_result(r) for r in selected]

    OUT_LOG.write_text(
        "\n".join(json.dumps(x, ensure_ascii=False, separators=(",", ":")) for x in outcomes) + ("\n" if outcomes else ""),
        encoding="utf-8"
    )

    status_counts = dict(Counter(r.get("outcome_status") for r in outcomes))
    symbol_counts = dict(Counter(r.get("symbol") for r in outcomes))
    closed = [r for r in outcomes if r.get("outcome_status") in CLOSED_STATUSES]
    wins = [r for r in closed if r.get("label_win") is True]
    losses = [r for r in closed if r.get("outcome_status") == "SL"]

    closed_count = len(closed)
    win_rate = round(len(wins) / closed_count, 6) if closed_count else 0.0
    expectancy_R = round(sum(float(r.get("label_R") or 0) for r in closed) / closed_count, 6) if closed_count else 0.0

    include_ml_label_count = sum(1 for r in outcomes if r.get("include_ml_label") is True)

    report = {
        "ok": True,
        "version": VERSION,
        "mode": MODE,
        "created_at_wib": now_wib_str(),
        "input_signals": str(IN_SIGNALS),
        "market_data_dir": str(MARKET_DATA_DIR),
        "output_log": str(OUT_LOG),
        "interval": INTERVAL,
        "window_hours": WINDOW_HOURS,
        "max_signals": MAX_SIGNALS,
        "raw_rows": len(raw),
        "unique_signal_rows": len(signals),
        "selected_rows": len(selected),
        "outcomes_total": len(outcomes),
        "closed_count": closed_count,
        "include_ml_label_count": include_ml_label_count,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": win_rate,
        "expectancy_R": expectancy_R,
        "status_counts": status_counts,
        "symbol_counts": symbol_counts,
        "note": "REPORT_ONLY update-safe builder. Does not overwrite logs/forward_outcomes.jsonl.",
        "samples": outcomes[-20:],
    }

    OUT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    cols = [
        "signal_key","symbol","direction","signal_time_wib","evaluated_at_utc",
        "outcome_status","label_win","label_target","label_R","outcome_ts_wib",
        "bars_to_outcome","candles_checked","hit_tp1","hit_tp2","hit_tp3","hit_sl",
        "first_hit","fill_t_wib","same_candle_conflict","same_candle_policy",
        "evaluation_window_hours","candle_interval","include_ml_label","exclude_label_reason",
        "time_source","execution_decision","source_mode","signal_source","sample_type",
        "score","priority","entry","sl","tp1","tp2","tp3",
    ]

    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in outcomes:
            w.writerow({c: r.get(c) for c in cols})

    run_row = {
        "created_at_wib": report["created_at_wib"],
        "version": VERSION,
        "mode": MODE,
        "outcomes_total": len(outcomes),
        "closed_count": closed_count,
        "include_ml_label_count": include_ml_label_count,
        "win_rate": win_rate,
        "expectancy_R": expectancy_R,
        "status_counts": status_counts,
    }

    with OUT_RUNS.open("a", encoding="utf-8") as f:
        f.write(json.dumps(run_row, ensure_ascii=False, sort_keys=True) + "\n")

    print(f"=== FORWARD OUTCOMES BUILDER V1 | {MODE} ===")
    print("input_signals:", IN_SIGNALS)
    print("market_data_dir:", MARKET_DATA_DIR)
    print("out_log :", OUT_LOG)
    print("out_json:", OUT_JSON)
    print("out_csv :", OUT_CSV)
    print("run_log :", OUT_RUNS)
    print("raw_rows:", len(raw), "unique:", len(signals), "selected:", len(selected))
    print("status_counts:", status_counts)
    print("closed_count:", closed_count)
    print("include_ml_label_count:", include_ml_label_count)
    print("win_rate:", win_rate)
    print("expectancy_R:", expectancy_R)
    print("")
    print(f"{'STATUS':<12} {'COUNT':>6}")
    for k, v in sorted(status_counts.items(), key=lambda kv: (-kv[1], str(kv[0]))):
        print(f"{str(k):<12} {v:>6}")

if __name__ == "__main__":
    main()
