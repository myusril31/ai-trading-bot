#!/usr/bin/env python3
"""Read-only Binance position-lifecycle reconciliation and production-truth audit."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

TRACE_SCHEMA_VERSION = "production_trade_trace_v1.1"
CLOSED_EVENTS = {
    "POSITION_CLOSED", "TRADE_CLOSED", "TP_FILLED", "SL_FILLED",
    "MANUAL_CLOSE_FILLED", "LIQUIDATED",
}
ROLE_FIELDS = {
    "entry": ("entry_order_ids", "entry_order_id"),
    "tp": ("tp_order_ids", "tp_order_id", "tp1_order_id", "tp2_order_id", "tp3_order_id"),
    "sl": ("sl_order_ids", "sl_order_id"),
    "exit": ("exit_order_ids", "exit_order_id", "close_order_id", "order_id"),
}


def first(row: Mapping[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        value = row.get(key)
        if value is not None and str(value).strip() != "":
            return value
    return default


def text(value: Any) -> str:
    return str(value or "").strip()


def upper(value: Any) -> str:
    return text(value).upper()


def number(value: Any) -> Optional[float]:
    try:
        return None if value is None or text(value) == "" else float(value)
    except (TypeError, ValueError):
        return None


def instant(value: Any) -> Optional[datetime]:
    if value is None or text(value) == "":
        return None
    if isinstance(value, (int, float)) or text(value).isdigit():
        epoch = float(value)
        if epoch > 10_000_000_000:
            epoch /= 1000
        return datetime.fromtimestamp(epoch, tz=timezone.utc)
    try:
        parsed = datetime.fromisoformat(text(value).replace("Z", "+00:00"))
        return (parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed).astimezone(timezone.utc)
    except ValueError:
        return None


def iso(value: Any) -> Optional[str]:
    parsed = instant(value)
    return parsed.isoformat() if parsed else None


def row_time(row: Mapping[str, Any]) -> Optional[datetime]:
    return instant(first(row, "time", "timestamp", "ts", "updateTime", "trade_time", "event_time"))


def load_rows(path: Optional[Path]) -> List[Dict[str, Any]]:
    if path is None or not path.exists():
        return []
    if path.suffix.lower() == ".csv":
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSONL at {path}:{line_no}: {exc}") from exc
            if isinstance(row, dict):
                rows.append(row)
    return rows


def write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(dict(row), ensure_ascii=False, separators=(",", ":")) + "\n")


def is_closed_event(row: Mapping[str, Any]) -> bool:
    return upper(first(row, "event", "event_type", "action", "status", "state")) in CLOSED_EVENTS


def symbol(row: Mapping[str, Any]) -> str:
    return upper(first(row, "symbol", "pair"))


def signal_key(row: Mapping[str, Any]) -> str:
    return text(first(row, "signal_key", "signalKey"))


def order_id(row: Mapping[str, Any]) -> str:
    # Deliberately excludes generic `id`: an internal event id is not an exchange order id.
    return text(first(row, "order_id", "orderId"))


def client_order_id(row: Mapping[str, Any]) -> str:
    return text(first(row, "client_order_id", "clientOrderId", "clientAlgoId", "origClientOrderId"))


def list_values(row: Mapping[str, Any], fields: Sequence[str]) -> List[str]:
    values: List[str] = []
    for field in fields:
        value = row.get(field)
        if isinstance(value, list):
            values.extend(text(item) for item in value if text(item))
        elif text(value):
            values.append(text(value))
    return list(dict.fromkeys(values))


def role_ids(plan: Mapping[str, Any], event: Mapping[str, Any]) -> Dict[str, List[str]]:
    merged = {**plan, **event}
    return {role: list_values(merged, fields) for role, fields in ROLE_FIELDS.items()}


def lifecycle_window(plan: Mapping[str, Any], event: Mapping[str, Any], guard_minutes: int) -> Tuple[Optional[datetime], Optional[datetime]]:
    opened = instant(first(plan, "entry_fill_time", "entry_time", "order_time", "signal_time", "signal_time_wib", "ts"))
    closed = instant(first(event, "exit_fill_time", "close_time", "exit_time", "ts", "time"))
    guard = timedelta(minutes=guard_minutes)
    return (opened - guard if opened else None, closed + guard if closed else None)


def in_window(row: Mapping[str, Any], window: Tuple[Optional[datetime], Optional[datetime]]) -> bool:
    when = row_time(row)
    start, end = window
    if when is None or start is None or end is None:
        return False
    return start <= when <= end


def match_fills(plan: Mapping[str, Any], event: Mapping[str, Any], fills: Sequence[Mapping[str, Any]], guard_minutes: int) -> List[Mapping[str, Any]]:
    expected_symbol = symbol(event) or symbol(plan)
    ids = role_ids(plan, event)
    known_orders = {value for values in ids.values() for value in values}
    known_clients = set(list_values({**plan, **event}, ("client_order_ids", "client_order_id", "entry_client_order_id", "exit_client_order_id")))
    window = lifecycle_window(plan, event, guard_minutes)
    matched: List[Mapping[str, Any]] = []
    for fill in fills:
        if not expected_symbol or symbol(fill) != expected_symbol:
            continue
        by_order = bool(order_id(fill) and order_id(fill) in known_orders)
        by_client = bool(client_order_id(fill) and client_order_id(fill) in known_clients and in_window(fill, window))
        if by_order or by_client:
            matched.append(fill)
    return sorted(matched, key=lambda row: row_time(row) or datetime.min.replace(tzinfo=timezone.utc))


def fill_role(fill: Mapping[str, Any], direction: str, ids: Mapping[str, Sequence[str]]) -> str:
    oid = order_id(fill)
    for role in ("entry", "tp", "sl", "exit"):
        if oid and oid in ids.get(role, ()):
            return "entry" if role == "entry" else "exit"
    if upper(first(fill, "reduceOnly", "reduce_only")) in {"TRUE", "1", "YES"}:
        return "exit"
    side = upper(first(fill, "side"))
    if direction == "LONG":
        return "entry" if side == "BUY" else "exit" if side == "SELL" else "unknown"
    if direction == "SHORT":
        return "entry" if side == "SELL" else "exit" if side == "BUY" else "unknown"
    return "unknown"


def quantity(fill: Mapping[str, Any]) -> Optional[float]:
    return number(first(fill, "qty", "quantity", "executedQty"))


def price(fill: Mapping[str, Any]) -> Optional[float]:
    return number(first(fill, "price", "avgPrice"))


def vwap(rows: Sequence[Mapping[str, Any]]) -> Optional[float]:
    pairs = [(price(row), quantity(row)) for row in rows]
    valid = [(p, q) for p, q in pairs if p is not None and q is not None and q > 0]
    total = sum(q for _, q in valid)
    return sum(p * q for p, q in valid) / total if total else None


def sum_field(rows: Sequence[Mapping[str, Any]], *fields: str) -> Optional[float]:
    values = [number(first(row, *fields)) for row in rows]
    present = [value for value in values if value is not None]
    return sum(present) if present else None


def income_type(row: Mapping[str, Any]) -> str:
    return upper(first(row, "incomeType", "income_type"))


def matching_income(rows: Sequence[Mapping[str, Any]], expected_symbol: str, position_side: str, start: Optional[datetime], end: Optional[datetime]) -> List[Mapping[str, Any]]:
    if start is None or end is None:
        return []
    result = []
    for row in rows:
        if symbol(row) != expected_symbol or not in_window(row, (start, end)):
            continue
        row_side = upper(first(row, "positionSide", "position_side"))
        if row_side and position_side and row_side not in {position_side, "BOTH"}:
            continue
        result.append(row)
    return result


def reconstruct_exchange_lifecycles(fills: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    """Reconstruct flat-to-open-to-flat position lifecycles from chronologically ordered fills."""
    grouped: Dict[Tuple[str, str], List[Mapping[str, Any]]] = defaultdict(list)
    for fill in fills:
        grouped[(symbol(fill), upper(first(fill, "positionSide", "position_side", default="BOTH")) or "BOTH")].append(fill)
    lifecycles: List[Dict[str, Any]] = []
    for (sym, pos_side), rows in grouped.items():
        position = 0.0
        current: List[Mapping[str, Any]] = []
        for fill in sorted(rows, key=lambda row: row_time(row) or datetime.min.replace(tzinfo=timezone.utc)):
            qty = quantity(fill)
            side = upper(first(fill, "side"))
            if qty is None or qty <= 0 or side not in {"BUY", "SELL"}:
                continue
            signed = qty if side == "BUY" else -qty
            before = position
            position += signed
            if abs(before) < 1e-12 and abs(position) > 1e-12:
                current = [fill]
            elif current:
                current.append(fill)
            if current and abs(position) < 1e-12:
                lifecycles.append({"symbol": sym, "position_side": pos_side, "fills": current})
                current = []
    return lifecycles


def stable_id(source: Mapping[str, Any]) -> str:
    payload = "|".join((signal_key(source), symbol(source), text(first(source, "plan_id", "execution_plan_id"))))
    return hashlib.sha256(payload.encode()).hexdigest()[:24]


def build_traces(plans: Sequence[Mapping[str, Any]], events: Sequence[Mapping[str, Any]], fills: Sequence[Mapping[str, Any]], income: Sequence[Mapping[str, Any]], config_version: str, client_guard_minutes: int = 5) -> List[Dict[str, Any]]:
    plans_by_signal: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for plan in plans:
        if signal_key(plan):
            plans_by_signal[signal_key(plan)].append(plan)
    traces: List[Dict[str, Any]] = []
    for event in (row for row in events if is_closed_event(row)):
        candidates = plans_by_signal.get(signal_key(event), [])
        plan = candidates[-1] if len(candidates) == 1 else {}
        duplicate_plan = len(candidates) > 1
        direction = upper(first(event, "direction", "dir", default=first(plan, "direction", "dir")))
        ids = role_ids(plan, event)
        matched = match_fills(plan, event, fills, client_guard_minutes)
        entry_fills = [row for row in matched if fill_role(row, direction, ids) == "entry"]
        exit_fills = [row for row in matched if fill_role(row, direction, ids) == "exit"]
        opened = row_time(entry_fills[0]) if entry_fills else None
        closed = row_time(exit_fills[-1]) if exit_fills else None
        pos_side = upper(first(event, "positionSide", "position_side", default=first(plan, "positionSide", "position_side", default="BOTH"))) or "BOTH"
        income_guard = timedelta(minutes=client_guard_minutes)
        related_income = matching_income(
            income,
            symbol(event) or symbol(plan),
            pos_side,
            opened - income_guard if opened else None,
            closed + income_guard if closed else None,
        )
        realized_fill = sum_field(exit_fills, "realizedPnl", "realized_pnl")
        realized_income = sum_field([row for row in related_income if income_type(row) == "REALIZED_PNL"], "income")
        realized = realized_fill if realized_fill is not None else realized_income
        fill_commission = sum_field(matched, "commission")
        income_commission = sum_field([row for row in related_income if income_type(row) == "COMMISSION"], "income")
        commission = abs(fill_commission) if fill_commission is not None else abs(income_commission) if income_commission is not None else None
        funding_rows = [row for row in related_income if income_type(row) == "FUNDING_FEE"]
        funding = sum_field(funding_rows, "income")
        funding_allocated = bool(opened and closed) and (funding is not None or not any(income_type(row) == "FUNDING_FEE" and symbol(row) == (symbol(event) or symbol(plan)) for row in income))
        net = realized - commission + (funding or 0.0) if realized is not None and commission is not None and funding_allocated else None
        source = {**plan, **event}
        explicit_close_reason = upper(first(event, "close_reason", "reason"))
        event_type = upper(first(event, "event", "event_type"))
        inferred_close_reason = event_type if event_type in {
            "TP_FILLED", "SL_FILLED", "MANUAL_CLOSE_FILLED", "LIQUIDATED"
        } else None
        trace = {
            "trace_schema_version": TRACE_SCHEMA_VERSION,
            "correlation_id": text(first(source, "correlation_id")) or stable_id(source),
            "signal_key": signal_key(source) or None,
            "plan_id": first(source, "plan_id", "execution_plan_id"),
            "config_version": first(source, "config_version", default=config_version),
            "symbol": symbol(source) or None,
            "position_side": pos_side,
            "direction": direction or None,
            "setup_type": upper(first(source, "setup_type", "strategy", "source")) or None,
            "signal_time": iso(first(source, "signal_time", "signal_time_wib", "signal_ts")),
            "decision_time": iso(first(source, "decision_time", "decision_ts")),
            "order_time": iso(first(source, "order_time", "order_ts")),
            "entry_fill_time": opened.isoformat() if opened else None,
            "exit_fill_time": closed.isoformat() if closed else None,
            "raw_entry": number(first(source, "raw_entry", "entry_mid", "entry")),
            "raw_sl": number(first(source, "raw_sl", "sl")),
            "raw_tp": number(first(source, "raw_tp", "tp", "tp1")),
            "final_entry": number(first(source, "final_entry", "entry")),
            "final_sl": number(first(source, "final_sl", "sl")),
            "final_tp": number(first(source, "final_tp", "tp", "tp1")),
            "actual_entry": vwap(entry_fills),
            "actual_exit": vwap(exit_fills),
            "entry_order_ids": ids["entry"], "tp_order_ids": ids["tp"],
            "sl_order_ids": ids["sl"], "exit_order_ids": ids["exit"],
            "fill_ids": [text(first(row, "tradeId", "trade_id")) for row in matched if text(first(row, "tradeId", "trade_id"))],
            "realized_pnl": realized, "commission": commission,
            "funding_fee": funding if funding_allocated else None,
            "funding_unallocated": None if funding_allocated else funding,
            "funding_allocated": funding_allocated,
            "net_pnl": net,
            "close_reason": explicit_close_reason or inferred_close_reason,
            "label_win": None if net is None else int(net > 0),
            "plan_linked": bool(plan) and not duplicate_plan,
            "duplicate_plan": duplicate_plan,
            "entry_fill_covered": bool(entry_fills), "exit_fill_covered": bool(exit_fills),
            "exchange_lifecycle_complete": bool(entry_fills and exit_fills),
        }
        traces.append(trace)
    return traces


def audit(traces: Sequence[Mapping[str, Any]], exchange_lifecycles: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    total = len(traces)
    def coverage(field: str) -> float:
        return round(sum(bool(row.get(field)) for row in traces) / total, 6) if total else 0.0
    identities = [(text(row.get("signal_key")), text(row.get("plan_id"))) for row in traces]
    duplicate_count = len(identities) - len(set(identities))
    metrics = {
        "internal_closed_position_lifecycles": total,
        "binance_reconstructed_closed_position_lifecycles": len(exchange_lifecycles),
        "plan_link_coverage": coverage("plan_linked"),
        "entry_fill_coverage": coverage("entry_fill_covered"),
        "exit_fill_coverage": coverage("exit_fill_covered"),
        "realized_pnl_coverage": round(sum(row.get("realized_pnl") is not None for row in traces) / total, 6) if total else 0.0,
        "commission_coverage": round(sum(row.get("commission") is not None for row in traces) / total, 6) if total else 0.0,
        "funding_allocation_coverage": coverage("funding_allocated"),
        "close_reason_coverage": round(sum(bool(row.get("close_reason")) for row in traces) / total, 6) if total else 0.0,
        "net_pnl_coverage": round(sum(row.get("net_pnl") is not None for row in traces) / total, 6) if total else 0.0,
        "duplicate_trade_count": duplicate_count,
    }
    checks = {
        "closed_lifecycle_count_matches": total == len(exchange_lifecycles),
        **{f"{name}_gte_99pct": metrics[name] >= 0.99 for name in (
            "plan_link_coverage", "entry_fill_coverage", "exit_fill_coverage",
            "realized_pnl_coverage", "commission_coverage", "funding_allocation_coverage",
            "close_reason_coverage", "net_pnl_coverage")},
        "duplicate_trade_count_zero": duplicate_count == 0,
    }
    passed = bool(total) and all(checks.values())
    return {"trace_schema_version": TRACE_SCHEMA_VERSION, "generated_at": datetime.now(timezone.utc).isoformat(), "metrics": metrics, "checks": checks, "passed": passed, "interpretation": "production_truth_ready" if passed else "dataset_anomaly"}


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plans", type=Path, default=Path("logs/execution_plans.jsonl"))
    parser.add_argument("--events", type=Path, default=Path("logs/execution_events.jsonl"))
    parser.add_argument("--trades", type=Path, required=True, help="Binance account trade/fill CSV or JSONL")
    parser.add_argument("--income", type=Path, required=True, help="Binance income-history CSV or JSONL")
    parser.add_argument("--out", type=Path, default=Path("logs/production_trade_traces_v1.jsonl"))
    parser.add_argument("--report", type=Path, default=Path("logs/production_trade_trace_audit_v1.json"))
    parser.add_argument("--config-version", default="cfg_202607_v2")
    parser.add_argument("--client-id-guard-minutes", type=int, default=5)
    parser.add_argument("--strict", action="store_true")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    fills, income = load_rows(args.trades), load_rows(args.income)
    traces = build_traces(load_rows(args.plans), load_rows(args.events), fills, income, args.config_version, args.client_id_guard_minutes)
    report = audit(traces, reconstruct_exchange_lifecycles(fills))
    write_jsonl(args.out, traces)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 2 if args.strict and not report["passed"] else 0


if __name__ == "__main__":
    sys.exit(main())
