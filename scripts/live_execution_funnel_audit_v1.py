#!/usr/bin/env python3
import os
import json
import re
from pathlib import Path
from collections import Counter, defaultdict

ROOT = Path(__file__).resolve().parents[1]

SIGNALS_FILE = ROOT / "logs" / "ml_dataset_rows.jsonl"
EVENTS_FILE = ROOT / "logs" / "execution_events.jsonl"
OUTCOMES_FILE = ROOT / "logs" / "forward_outcomes_v1.jsonl"

OUT_JSON = ROOT / "reports" / "live_execution_funnel_audit_v1.json"

VERSION = "live_execution_funnel_audit_v1_20260620"

WINDOW_HOURS = float(os.getenv("FUNNEL_HOURS", "24"))
MAX_PRINT = int(os.getenv("FUNNEL_MAX_PRINT", "80"))


def read_jsonl(path):
    out = []
    if not path.exists():
        return out
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            try:
                out.append(json.loads(line))
            except Exception:
                pass
    return out


def norm_symbol(x):
    return str(x or "").upper().replace("BINANCE:", "").replace(".P", "").strip()


def signal_key(r):
    return str(
        r.get("signal_key")
        or r.get("key")
        or "|".join([
            str(r.get("symbol") or r.get("pair") or ""),
            str(r.get("direction") or r.get("dir") or ""),
            str(r.get("signal_time_ms") or r.get("ts_ms") or r.get("timestamp_ms") or r.get("created_at_ms") or ""),
        ])
    )


def to_int_ms(v):
    try:
        if v is None:
            return None
        if isinstance(v, str) and not v.strip():
            return None
        n = int(float(v))
        # seconds -> ms
        if 1_000_000_000 <= n < 10_000_000_000:
            return n * 1000
        return n
    except Exception:
        return None


def extract_ms(r):
    for k in [
        "created_at_ms", "event_time_ms", "signal_time_ms",
        "timestamp_ms", "ts_ms", "time_ms", "closed_at_ms",
    ]:
        n = to_int_ms(r.get(k))
        if n:
            return n

    # fallback parse simple WID/WIB-like datetime string
    for k in ["created_at_wib", "ts_wib", "time_wib", "signal_time_wib", "created_at"]:
        s = str(r.get(k) or "")
        m = re.search(r"(20\d\d)-(\d\d)-(\d\d)[ T](\d\d):(\d\d):(\d\d)", s)
        if m:
            # not converting timezone perfectly; enough for relative sorting if all same source
            y, mo, d, h, mi, se = map(int, m.groups())
            import datetime as dt
            return int(dt.datetime(y, mo, d, h, mi, se).timestamp() * 1000)

    return None


def filter_window(rows, since_ms):
    out = []
    no_time = 0
    for r in rows:
        t = extract_ms(r)
        if t is None:
            no_time += 1
            out.append(r)  # keep no-time rows; repo logs sometimes act like they were assembled by raccoons
        elif t >= since_ms:
            out.append(r)
    return out, no_time


def text_blob(r):
    try:
        return json.dumps(r, ensure_ascii=False).upper()
    except Exception:
        return str(r).upper()


def is_rejectish(v):
    s = str(v or "").upper()
    return any(x in s for x in ["REJECT", "BLOCK", "NO_TRADE", "INVALID", "CANCEL", "EXPIRED", "FAIL", "ERROR", "SKIP"])


def stage_from_event(r):
    action = str(r.get("action") or "").upper()
    decision = str(r.get("decision") or r.get("execution_decision") or "").upper()
    gate = str(r.get("gate") or "").upper()
    reason = str(r.get("reason") or r.get("ml_gate_reason") or r.get("orderbook_bridge_reason") or r.get("error") or "").upper()
    blob = text_blob(r)

    if "ORDERBOOK" in action or "ORDERBOOK" in gate or "ORDERBOOK" in reason:
        if "REJECT" in blob or "WOULD_BLOCK" in blob or "BLOCK" in blob:
            return "ORDERBOOK_BLOCK"
        return "ORDERBOOK_CHECK"

    if "ML_GATE" in action or "ML_GATE" in gate or "ML_GATE" in reason or "ML" in gate:
        if "REJECT" in blob or "NO_TRADE" in blob or "LOW_CONFIDENCE" in blob:
            return "ML_REJECT"
        return "ML_CHECK"

    if "COOLDOWN" in blob:
        return "COOLDOWN_BLOCK"

    if "MAX_POSITION" in blob or "MAX_OPEN" in blob or "MAX_TRADES" in blob:
        return "LIMIT_BLOCK"

    if "PLAN" in blob and ("INVALID" in blob or "SANITY" in blob or "FAIL" in blob):
        return "PLAN_INVALID"

    if "RISK" in blob or "MARGIN" in blob or "LEVERAGE" in blob:
        if is_rejectish(blob):
            return "RISK_OR_MARGIN_BLOCK"
        return "RISK_OR_MARGIN_CHECK"

    if "LIVE_PLACE_ORDER" in action or "ORDER_PLACED" in action or "ORDER_PLACED" in blob or "ENTRY_ORDER_PLACED" in blob:
        return "ORDER_PLACED"

    if "LIVE" in action and ("FAILED" in blob or "ERROR" in blob):
        return "LIVE_EXEC_ERROR"

    if is_rejectish(decision) or is_rejectish(reason):
        return "OTHER_REJECT"

    if "LIVE_SMALL_CAPITAL" in action:
        return "LIVE_PIPELINE_EVENT"

    return "OTHER_EVENT"


def is_confirmed_signal(r):
    s = str(r.get("status") or r.get("state") or r.get("signal_status") or "").upper()
    return s == "CONFIRMED"


def is_non_exec_signal(r):
    s = str(r.get("status") or r.get("state") or r.get("signal_status") or "").upper()
    return s in ("IDLE", "BREAKOUT", "WATCH", "CANDLE", "EXPIRED", "CANCELED", "INVALID", "ERROR")


def main():
    signals_all = read_jsonl(SIGNALS_FILE)
    events_all = read_jsonl(EVENTS_FILE)
    outcomes_all = read_jsonl(OUTCOMES_FILE)

    all_times = [extract_ms(r) for r in signals_all + events_all + outcomes_all]
    all_times = [x for x in all_times if x is not None]
    max_ms = max(all_times) if all_times else 0
    since_ms = max_ms - int(WINDOW_HOURS * 3600 * 1000) if max_ms else 0

    signals, signals_no_time = filter_window(signals_all, since_ms)
    events, events_no_time = filter_window(events_all, since_ms)
    outcomes, outcomes_no_time = filter_window(outcomes_all, since_ms)

    signal_by_key = {}
    for r in signals:
        k = signal_key(r)
        if k:
            signal_by_key[k] = r

    signal_keys = set(signal_by_key)
    event_keys = {signal_key(r) for r in events if signal_key(r)}
    outcome_keys = {signal_key(r) for r in outcomes if signal_key(r)}

    signal_status_counts = Counter(str(r.get("status") or r.get("state") or r.get("signal_status") or "NA").upper() for r in signals)
    signal_symbol_counts = Counter(norm_symbol(r.get("symbol") or r.get("pair")) or "NA" for r in signals)
    action_counts = Counter(str(r.get("action") or "NA") for r in events)
    decision_counts = Counter(str(r.get("decision") or r.get("execution_decision") or "NA").upper() for r in events)
    stage_counts = Counter(stage_from_event(r) for r in events)

    ml_decisions = Counter()
    orderbook_decisions = Counter()
    gate_reason = Counter()
    by_symbol_stage = defaultdict(Counter)

    for r in events:
        sym = norm_symbol(r.get("symbol") or r.get("pair")) or "NA"
        stg = stage_from_event(r)
        by_symbol_stage[sym][stg] += 1

        md = str(r.get("ml_gate_decision") or r.get("ml_decision") or r.get("decision") or "NA").upper()
        if "ML" in text_blob(r):
            ml_decisions[md] += 1

        od = str(r.get("orderbook_bridge_decision") or r.get("decision") or "NA").upper()
        if "ORDERBOOK" in text_blob(r):
            orderbook_decisions[od] += 1

        gate = str(r.get("gate") or "NA")
        reason = str(r.get("reason") or r.get("ml_gate_reason") or r.get("orderbook_bridge_reason") or r.get("error") or "NA")
        if gate != "NA" or reason != "NA":
            gate_reason[(gate, reason[:220])] += 1

    confirmed = [r for r in signals if is_confirmed_signal(r)]
    non_exec = [r for r in signals if is_non_exec_signal(r)]

    placed_count = stage_counts["ORDER_PLACED"]
    clear_blockers = {
        "NOT_CONFIRMED_OR_NON_EXEC_SIGNAL": len(non_exec),
        "ML_REJECT": stage_counts["ML_REJECT"],
        "ORDERBOOK_BLOCK": stage_counts["ORDERBOOK_BLOCK"],
        "COOLDOWN_BLOCK": stage_counts["COOLDOWN_BLOCK"],
        "LIMIT_BLOCK": stage_counts["LIMIT_BLOCK"],
        "PLAN_INVALID": stage_counts["PLAN_INVALID"],
        "RISK_OR_MARGIN_BLOCK": stage_counts["RISK_OR_MARGIN_BLOCK"],
        "LIVE_EXEC_ERROR": stage_counts["LIVE_EXEC_ERROR"],
        "OTHER_REJECT": stage_counts["OTHER_REJECT"],
    }

    likely_primary_bottleneck = max(clear_blockers.items(), key=lambda kv: kv[1])[0] if clear_blockers else "NA"

    report = {
        "ok": True,
        "version": VERSION,
        "window_hours": WINDOW_HOURS,
        "max_seen_ms": max_ms,
        "since_ms": since_ms,
        "files": {
            "signals": str(SIGNALS_FILE),
            "events": str(EVENTS_FILE),
            "outcomes": str(OUTCOMES_FILE),
        },
        "row_counts": {
            "signals_window": len(signals),
            "signals_unique_window": len(signal_by_key),
            "events_window": len(events),
            "outcomes_window": len(outcomes),
            "signals_no_time_kept": signals_no_time,
            "events_no_time_kept": events_no_time,
            "outcomes_no_time_kept": outcomes_no_time,
        },
        "funnel": {
            "signals_seen": len(signal_by_key),
            "confirmed_signals": len(confirmed),
            "non_exec_signal_states": len(non_exec),
            "signals_with_execution_event_key_overlap": len(signal_keys & event_keys),
            "signals_with_outcome_key_overlap": len(signal_keys & outcome_keys),
            "order_placed_events": placed_count,
            "primary_bottleneck_guess": likely_primary_bottleneck,
        },
        "blockers": clear_blockers,
        "signal_status_counts": dict(signal_status_counts),
        "signal_symbol_counts": dict(signal_symbol_counts),
        "execution_stage_counts": dict(stage_counts),
        "execution_action_counts_top": action_counts.most_common(80),
        "decision_counts": dict(decision_counts),
        "ml_decisions": dict(ml_decisions),
        "orderbook_decisions": dict(orderbook_decisions),
        "gate_reason_top": [
            {"count": n, "gate": g, "reason": rs}
            for (g, rs), n in gate_reason.most_common(80)
        ],
        "by_symbol_stage_top": {
            sym: dict(cnt)
            for sym, cnt in sorted(by_symbol_stage.items())
        },
        "note": "REPORT_ONLY. Counts depend on available log fields. Use as funnel/bottleneck map, not trade PnL report.",
    }

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(report, indent=2, ensure_ascii=False))

    print("=== LIVE EXECUTION FUNNEL AUDIT v1 ===")
    print("window_hours:", WINDOW_HOURS)
    print("out_json:", OUT_JSON)
    print()
    print("== FUNNEL ==")
    for k, v in report["funnel"].items():
        print(f"{k}: {v}")

    print()
    print("== BLOCKERS ==")
    for k, v in sorted(clear_blockers.items(), key=lambda kv: -kv[1]):
        print(f"{k}: {v}")

    print()
    print("== SIGNAL STATUS ==")
    for k, v in signal_status_counts.most_common(MAX_PRINT):
        print(f"{k}: {v}")

    print()
    print("== EXECUTION STAGES ==")
    for k, v in stage_counts.most_common(MAX_PRINT):
        print(f"{k}: {v}")

    print()
    print("== ML DECISIONS ==")
    for k, v in ml_decisions.most_common(MAX_PRINT):
        print(f"{k}: {v}")

    print()
    print("== ORDERBOOK DECISIONS ==")
    for k, v in orderbook_decisions.most_common(MAX_PRINT):
        print(f"{k}: {v}")

    print()
    print("== TOP GATE / REASONS ==")
    for (g, rs), n in gate_reason.most_common(MAX_PRINT):
        print(f"{n} | gate={g} | reason={rs}")

    print()
    print("== BY SYMBOL STAGE ==")
    for sym, cnt in sorted(by_symbol_stage.items()):
        if not cnt:
            continue
        top = ", ".join(f"{k}={v}" for k, v in cnt.most_common(8))
        print(f"{sym}: {top}")


if __name__ == "__main__":
    main()
