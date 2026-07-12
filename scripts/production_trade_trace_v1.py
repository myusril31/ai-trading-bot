#!/usr/bin/env python3
"""Build a deterministic production-trade trace and audit Binance coverage.

This module is deliberately read-only.  It joins the bot's execution plans/events
to an exported Binance income/trade file and writes canonical JSONL traces plus a
machine-readable coverage report.  It never places, cancels, or modifies orders.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple


TRACE_SCHEMA_VERSION = "production_trade_trace_v1"


def _first(row: Mapping[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        value = row.get(key)
        if value is not None and str(value).strip() != "":
            return value
    return default


def _text(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _text(value).upper()


def _float(value: Any) -> Optional[float]:
    if value is None or _text(value) == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _sum(values: Iterable[Optional[float]]) -> Optional[float]:
    clean = [value for value in values if value is not None]
    return sum(clean) if clean else None


def _iso(value: Any) -> Optional[str]:
    if value is None or _text(value) == "":
        return None
    if isinstance(value, (int, float)) or _text(value).isdigit():
        number = float(value)
        if number > 10_000_000_000:
            number /= 1000.0
        return datetime.fromtimestamp(number, tz=timezone.utc).isoformat()
    raw = _text(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat()
    except ValueError:
        return _text(value)


def load_rows(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    if path.suffix.lower() == ".csv":
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for number, line in enumerate(handle, start=1):
            raw = line.strip()
            if not raw:
                continue
            try:
                value = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSONL at {path}:{number}: {exc}") from exc
            if isinstance(value, dict):
                rows.append(value)
    return rows


def write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(dict(row), ensure_ascii=False, separators=(",", ":")) + "\n")


def _identity(row: Mapping[str, Any]) -> Tuple[str, str, str]:
    order_id = _text(_first(row, "order_id", "orderId", "id"))
    client_id = _text(_first(row, "client_order_id", "clientOrderId", "clientAlgoId", "origClientOrderId"))
    signal_key = _text(_first(row, "signal_key", "signalKey"))
    return order_id, client_id, signal_key


def _stable_id(plan: Mapping[str, Any], event: Mapping[str, Any], exchange: Sequence[Mapping[str, Any]]) -> str:
    values = list(_identity(event)) + list(_identity(plan))
    for row in exchange:
        values.extend(_identity(row))
    payload = "|".join(value for value in values if value)
    if not payload:
        payload = json.dumps([plan, event, list(exchange)], sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def _exchange_key(row: Mapping[str, Any]) -> List[Tuple[str, str]]:
    order_id, client_id, _ = _identity(row)
    keys: List[Tuple[str, str]] = []
    if order_id:
        keys.append(("order_id", order_id))
    if client_id:
        keys.append(("client_order_id", client_id))
    return keys


def _canonical_trade_key(row: Mapping[str, Any]) -> Optional[Tuple[str, str]]:
    keys = _exchange_key(row)
    return keys[0] if keys else None


def _closed_event(row: Mapping[str, Any]) -> bool:
    state = _upper(_first(row, "status", "state", "event", "event_type", "action"))
    reason = _upper(_first(row, "close_reason", "reason"))
    return any(token in state for token in ("CLOSE", "CLOSED", "EXIT", "TP", "SL")) or bool(reason)


def _close_reason(event: Mapping[str, Any], exchange: Sequence[Mapping[str, Any]]) -> Optional[str]:
    explicit = _first(event, "close_reason", "reason")
    if explicit:
        return _upper(explicit)
    pnl = _sum(_float(_first(row, "realized_pnl", "realizedPnl", "income")) for row in exchange)
    if pnl is None:
        return None
    return "BINANCE_PROFIT" if pnl > 0 else "BINANCE_LOSS" if pnl < 0 else "BINANCE_FLAT"


def _trace(plan: Mapping[str, Any], event: Mapping[str, Any], exchange: Sequence[Mapping[str, Any]], config_version: str) -> Dict[str, Any]:
    source = {**plan, **event}
    realized = _sum(_float(_first(row, "realized_pnl", "realizedPnl", "income")) for row in exchange)
    commission = _sum(_float(_first(row, "commission", "fee")) for row in exchange)
    funding = _sum(
        _float(_first(row, "funding_fee", "fundingFee", "income"))
        for row in exchange
        if _upper(_first(row, "income_type", "incomeType")) == "FUNDING_FEE" or _first(row, "funding_fee", "fundingFee") is not None
    )
    net = None if realized is None else realized - abs(commission or 0.0) + (funding or 0.0)
    actual_entries = [_float(_first(row, "entry_price", "entryPrice", "price", "avgPrice")) for row in exchange]
    actual_exits = [_float(_first(row, "exit_price", "exitPrice", "price", "avgPrice")) for row in exchange]
    order_id, client_order_id, signal_key = _identity(source)
    if not order_id or not client_order_id or not signal_key:
        for row in exchange:
            ex_order, ex_client, ex_signal = _identity(row)
            order_id = order_id or ex_order
            client_order_id = client_order_id or ex_client
            signal_key = signal_key or ex_signal
    trace = {
        "trace_schema_version": TRACE_SCHEMA_VERSION,
        "correlation_id": _text(_first(source, "correlation_id")) or _stable_id(plan, event, exchange),
        "signal_key": signal_key or None,
        "plan_id": _first(source, "plan_id", "execution_plan_id"),
        "config_version": _first(source, "config_version", default=config_version),
        "symbol": _upper(_first(source, "symbol", "pair")) or None,
        "direction": _upper(_first(source, "direction", "dir", "side")) or None,
        "setup_type": _upper(_first(source, "setup_type", "strategy", "source")) or None,
        "signal_time": _iso(_first(source, "signal_time", "signal_time_wib", "signal_ts")),
        "decision_time": _iso(_first(source, "decision_time", "decision_ts", "ts")),
        "order_time": _iso(_first(source, "order_time", "order_ts")),
        "entry_fill_time": _iso(_first(source, "entry_fill_time", "entry_time")),
        "exit_fill_time": _iso(_first(source, "exit_fill_time", "close_time", "exit_time", "ts")),
        "raw_entry": _float(_first(source, "raw_entry", "entry_mid", "entry")),
        "raw_sl": _float(_first(source, "raw_sl", "sl")),
        "raw_tp": _float(_first(source, "raw_tp", "tp", "tp1")),
        "final_entry": _float(_first(source, "final_entry", "entry")),
        "final_sl": _float(_first(source, "final_sl", "sl")),
        "final_tp": _float(_first(source, "final_tp", "tp", "tp1")),
        "actual_entry": next((v for v in actual_entries if v is not None), None),
        "actual_exit": next((v for v in reversed(actual_exits) if v is not None), None),
        "confluence_score": _float(_first(source, "confluence_score", "score")),
        "p_tp_before_sl": _float(_first(source, "p_tp_before_sl", "barrier_probability")),
        "fee_adjusted_expected_R": _float(_first(source, "fee_adjusted_expected_R", "expected_r_after_fee")),
        "order_id": order_id or None,
        "client_order_id": client_order_id or None,
        "realized_pnl": realized,
        "commission": commission,
        "funding_fee": funding,
        "net_pnl": net,
        "close_reason": _close_reason(event, exchange),
        "label_win": None if net is None else int(net > 0),
        "exchange_match_count": len(exchange),
        "exchange_matched": bool(exchange),
    }
    trace["trace_complete"] = all(trace.get(key) not in (None, "") for key in ("correlation_id", "signal_key", "symbol", "direction", "order_id"))
    return trace


def build_traces(plans: Sequence[Mapping[str, Any]], events: Sequence[Mapping[str, Any]], exchange_rows: Sequence[Mapping[str, Any]], config_version: str) -> List[Dict[str, Any]]:
    index: Dict[Tuple[str, str], List[Mapping[str, Any]]] = defaultdict(list)
    for row in exchange_rows:
        for key in _exchange_key(row):
            index[key].append(row)
    plan_by_signal = {_text(_first(row, "signal_key", "signalKey")): row for row in plans if _text(_first(row, "signal_key", "signalKey"))}
    closed_events = [row for row in events if _closed_event(row)]
    traces: List[Dict[str, Any]] = []
    for event in closed_events:
        _, _, signal_key = _identity(event)
        plan = plan_by_signal.get(signal_key, {})
        matches: List[Mapping[str, Any]] = []
        seen: set = set()
        for row in (event, plan):
            for key in _exchange_key(row):
                for match in index.get(key, []):
                    marker = id(match)
                    if marker not in seen:
                        seen.add(marker)
                        matches.append(match)
        traces.append(_trace(plan, event, matches, config_version))
    return traces


def audit(traces: Sequence[Mapping[str, Any]], exchange_rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    closed = len(traces)
    matched = sum(bool(row.get("exchange_matched")) for row in traces)
    pnl = sum(row.get("realized_pnl") is not None for row in traces)
    fees = sum(row.get("commission") is not None for row in traces)
    identities = [(_text(row.get("order_id")), _text(row.get("client_order_id"))) for row in traces]
    duplicate_count = len(identities) - len(set(identities))
    exchange_orders = {key for row in exchange_rows if (key := _canonical_trade_key(row)) is not None}
    matched_orders = {key for row in traces if (key := _canonical_trade_key(row)) is not None}
    def coverage(value: int) -> float:
        return round(value / closed, 6) if closed else 0.0
    metrics = {
        "internal_closed_trades": closed,
        "binance_unique_order_keys": len(exchange_orders),
        "matched_closed_trades": matched,
        "order_match_coverage": coverage(matched),
        "realized_pnl_coverage": coverage(pnl),
        "fee_coverage": coverage(fees),
        "duplicate_trade_count": duplicate_count,
        "unmatched_binance_order_keys": len(exchange_orders - matched_orders),
    }
    checks = {
        "closed_trade_count_matches": closed == len(exchange_orders) if exchange_orders else closed == 0,
        "order_match_coverage_gte_99pct": metrics["order_match_coverage"] >= 0.99,
        "realized_pnl_coverage_gte_99pct": metrics["realized_pnl_coverage"] >= 0.99,
        "fee_coverage_gte_99pct": metrics["fee_coverage"] >= 0.99,
        "duplicate_trade_count_zero": duplicate_count == 0,
    }
    return {
        "trace_schema_version": TRACE_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "metrics": metrics,
        "checks": checks,
        "passed": all(checks.values()),
        "interpretation": "dataset_anomaly" if not all(checks.values()) else "production_truth_ready",
    }


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plans", type=Path, default=Path("logs/execution_plans.jsonl"))
    parser.add_argument("--events", type=Path, default=Path("logs/execution_events.jsonl"))
    parser.add_argument("--binance", type=Path, required=True, help="Binance CSV or JSONL export")
    parser.add_argument("--out", type=Path, default=Path("logs/production_trade_traces_v1.jsonl"))
    parser.add_argument("--report", type=Path, default=Path("logs/production_trade_trace_audit_v1.json"))
    parser.add_argument("--config-version", default="cfg_202607_v2")
    parser.add_argument("--strict", action="store_true", help="exit 2 when acceptance checks fail")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    traces = build_traces(load_rows(args.plans), load_rows(args.events), load_rows(args.binance), args.config_version)
    report = audit(traces, load_rows(args.binance))
    write_jsonl(args.out, traces)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 2 if args.strict and not report["passed"] else 0


if __name__ == "__main__":
    sys.exit(main())
