#!/usr/bin/env python3
"""Label every raw STAT_TECH candidate from closed candles, without execution."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

SCHEMA_VERSION = "stat_tech_shadow_outcome_v1"
TERMINAL_STATUSES = {"TP_FIRST", "SL_FIRST", "EXPIRED", "ENTRY_NOT_FILLED", "AMBIGUOUS_ENTRY_SAME_BAR", "AMBIGUOUS_SAME_BAR", "INVALID_PLAN", "DATA_GAP"}
EVALUABLE_STATUSES = {"TP_FIRST", "SL_FIRST", "EXPIRED", "ENTRY_NOT_FILLED", "AMBIGUOUS_ENTRY_SAME_BAR", "AMBIGUOUS_SAME_BAR"}
BINARY_STATUSES = {"TP_FIRST", "SL_FIRST"}
WIB = ZoneInfo("Asia/Jakarta")


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


def epoch_ms(value: Any) -> Optional[int]:
    if value is None or text(value) == "":
        return None
    if isinstance(value, (int, float)) or text(value).isdigit():
        result = int(float(value))
        return result if result > 10_000_000_000 else result * 1000
    try:
        parsed = datetime.fromisoformat(text(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp() * 1000)
    except ValueError:
        return None


def signal_time_ms(row: Mapping[str, Any]) -> Optional[int]:
    direct = first(row, "signal_time_ms", "confirmed_bucket_ms", "created_at_ms")
    if direct is not None:
        return epoch_ms(direct)
    utc_value = first(row, "signal_time_utc", "created_at_utc", "signal_time")
    if utc_value is not None:
        return epoch_ms(utc_value)
    wib_value = row.get("signal_time_wib")
    if wib_value is None:
        return None
    try:
        parsed = datetime.fromisoformat(text(wib_value).replace(" WIB", "").replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=WIB)
        return int(parsed.timestamp() * 1000)
    except ValueError:
        return None


def candle_open_ms(row: Mapping[str, Any]) -> Optional[int]:
    return epoch_ms(first(row, "open_time_ms", "openTime", "open_time", "time"))


def candle_close_ms(row: Mapping[str, Any], interval_ms: int) -> Optional[int]:
    value = first(row, "close_time_ms", "closeTime", "close_time")
    if value is not None:
        return epoch_ms(value)
    opened = candle_open_ms(row)
    return opened + interval_ms - 1 if opened is not None else None


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
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


def interval_to_ms(interval: str) -> int:
    unit = interval[-1].lower()
    size = int(interval[:-1])
    factors = {"m": 60_000, "h": 3_600_000, "d": 86_400_000}
    if unit not in factors or size <= 0:
        raise ValueError(f"unsupported interval: {interval}")
    return size * factors[unit]


def normalize_symbol(row: Mapping[str, Any]) -> str:
    return upper(first(row, "symbol", "pair")).replace("/", "")


def plan(row: Mapping[str, Any]) -> Tuple[str, Optional[float], Optional[float], Optional[float]]:
    direction = upper(first(row, "direction", "dir"))
    # Entry zones require a separate explicit policy; never silently choose entry_lo/entry_hi.
    entry = number(first(row, "final_entry", "entry", "entry_mid"))
    sl = number(first(row, "final_sl", "sl"))
    tp = number(first(row, "final_tp", "tp", "tp1"))
    return direction, entry, sl, tp


def valid_plan(direction: str, entry: Optional[float], sl: Optional[float], tp: Optional[float]) -> bool:
    if direction == "LONG":
        return None not in (entry, sl, tp) and sl < entry < tp
    if direction == "SHORT":
        return None not in (entry, sl, tp) and tp < entry < sl
    return False


def data_is_contiguous(candles: Sequence[Mapping[str, Any]], interval_ms: int) -> bool:
    opens = [candle_open_ms(row) for row in candles]
    if any(value is None for value in opens):
        return False
    return all(0 < later - earlier <= interval_ms * 3 // 2 for earlier, later in zip(opens, opens[1:]))


def snapshot_hash(candidate: Mapping[str, Any]) -> str:
    payload = json.dumps(dict(candidate), sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def candidate_context(candidate: Mapping[str, Any]) -> Dict[str, Any]:
    decision = first(candidate, "confluence_decision", "execution_decision", "decision")
    reason = first(candidate, "final_reason", "block_reason", "decision_reason", "reject_reason")
    return {
        "config_version": first(candidate, "config_version"),
        "experiment_id": first(candidate, "experiment_id"),
        "signal_source": first(candidate, "signal_source", "source"),
        "decision": decision,
        "decision_reason": reason,
        "final_reason": first(candidate, "final_reason"),
        "block_reason": first(candidate, "block_reason", "reject_reason"),
        "confluence_decision": first(candidate, "confluence_decision"),
        "confluence_components": candidate.get("confluence_components"),
        "linear_quant_score": number(first(candidate, "linear_quant_score", "linear_score")),
        "barrier_score": number(first(candidate, "barrier_score", "p_tp_before_sl")),
        "ou_score": number(first(candidate, "ou_score", "ou_risk_score")),
        "tp_sl_p_tp": number(first(candidate, "tp_sl_p_tp", "p_tp_before_sl")),
        "fee_adjusted_expected_R": number(first(candidate, "fee_adjusted_expected_R", "expected_r_after_fee")),
        "candidate_snapshot_hash": snapshot_hash(candidate),
    }


def evaluate_candidate(candidate: Mapping[str, Any], candles: Sequence[Mapping[str, Any]], interval: str = "5m", horizon_bars: int = 288, now_ms: Optional[int] = None) -> Dict[str, Any]:
    interval_ms = interval_to_ms(interval)
    effective_now = now_ms if now_ms is not None else int(datetime.now(timezone.utc).timestamp() * 1000)
    signal_ms = signal_time_ms(candidate)
    direction, entry, sl, tp = plan(candidate)
    first_eligible_open = ((signal_ms + interval_ms - 1) // interval_ms) * interval_ms if signal_ms is not None else None
    horizon_end = first_eligible_open + horizon_bars * interval_ms if first_eligible_open is not None else None
    base = {
        "shadow_schema_version": SCHEMA_VERSION,
        "signal_key": first(candidate, "signal_key", "signal_id"),
        "symbol": normalize_symbol(candidate) or None,
        "direction": direction or None,
        "setup_type": first(candidate, "setup_type", "strategy"),
        "regime": first(candidate, "regime", "market_regime"),
        "score": number(first(candidate, "confluence_score", "score")),
        "execution_decision": first(candidate, "execution_decision", "decision"),
        "signal_time_ms": signal_ms, "first_eligible_open_ms": first_eligible_open,
        "entry": entry, "sl": sl, "tp": tp,
        "interval": interval, "horizon_bars": horizon_bars, "horizon_end_ms": horizon_end,
        "entry_status": "WAITING_ENTRY", "entry_hit": False,
        "entry_time_ms": None, "bars_to_entry": None, "bars_after_entry": 0,
        "outcome_status": "PENDING", "label_win": None,
        "first_hit": None, "bars_to_outcome": None, "outcome_time_ms": None,
        "candles_expected": horizon_bars, "candles_checked": 0,
        "same_bar_conflict": False, "production_outcome": False,
        "realized_pnl": None, "exclude_reason": None,
        **candidate_context(candidate),
    }
    if not text(base["signal_key"]):
        return {**base, "outcome_status": "INVALID_PLAN", "exclude_reason": "missing_signal_key"}
    if signal_ms is None:
        return {**base, "outcome_status": "INVALID_PLAN", "exclude_reason": "missing_signal_time"}
    if entry is None and (first(candidate, "entry_lo") is not None or first(candidate, "entry_hi") is not None):
        return {**base, "outcome_status": "INVALID_PLAN", "exclude_reason": "explicit_entry_required_for_zone"}
    if not valid_plan(direction, entry, sl, tp):
        return {**base, "outcome_status": "INVALID_PLAN", "exclude_reason": "invalid_plan_geometry"}

    selected = sorted([
        row for row in candles
        if (candle_open_ms(row) is not None and candle_open_ms(row) >= first_eligible_open)
        and candle_open_ms(row) < horizon_end
        and (candle_close_ms(row, interval_ms) or effective_now + 1) <= effective_now
    ], key=lambda row: candle_open_ms(row) or 0)

    if not selected:
        status = "PENDING" if effective_now < first_eligible_open + interval_ms else "DATA_GAP"
        reason = "awaiting_first_closed_candle" if status == "PENDING" else "missing_horizon_start"
        return {**base, "outcome_status": status, "exclude_reason": reason}
    if candle_open_ms(selected[0]) != first_eligible_open:
        return {**base, "outcome_status": "DATA_GAP", "candles_checked": len(selected), "exclude_reason": "missing_horizon_start"}
    if not data_is_contiguous(selected, interval_ms):
        return {**base, "outcome_status": "DATA_GAP", "candles_checked": len(selected), "exclude_reason": "non_contiguous_candles"}

    active = False
    entry_index: Optional[int] = None
    for index, candle in enumerate(selected, 1):
        high, low = number(candle.get("high")), number(candle.get("low"))
        if high is None or low is None:
            return {**base, "outcome_status": "DATA_GAP", "candles_checked": index, "exclude_reason": "invalid_ohlc"}
        tp_hit = high >= tp if direction == "LONG" else low <= tp
        sl_hit = low <= sl if direction == "LONG" else high >= sl
        entry_hit = low <= entry if direction == "LONG" else high >= entry
        hit_time = candle_close_ms(candle, interval_ms)
        if not active:
            if not entry_hit:
                continue
            if tp_hit or sl_hit:
                return {
                    **base, "outcome_status": "AMBIGUOUS_ENTRY_SAME_BAR",
                    "entry_status": "AMBIGUOUS", "entry_hit": True,
                    "entry_time_ms": hit_time, "bars_to_entry": index,
                    "first_hit": "AMBIGUOUS", "bars_to_outcome": index,
                    "outcome_time_ms": hit_time, "candles_checked": index,
                    "same_bar_conflict": True,
                    "exclude_reason": "ohlc_cannot_resolve_entry_barrier_order",
                }
            active, entry_index = True, index
            continue
        bars_after_entry = index - (entry_index or index)
        active_fields = {
            "entry_status": "ACTIVE", "entry_hit": True,
            "entry_time_ms": candle_close_ms(selected[(entry_index or 1) - 1], interval_ms),
            "bars_to_entry": entry_index, "bars_after_entry": bars_after_entry,
        }
        if tp_hit and sl_hit:
            return {**base, **active_fields, "outcome_status": "AMBIGUOUS_SAME_BAR", "first_hit": "AMBIGUOUS", "bars_to_outcome": index, "outcome_time_ms": hit_time, "candles_checked": index, "same_bar_conflict": True, "exclude_reason": "ohlc_cannot_resolve_intrabar_order"}
        if tp_hit:
            return {**base, **active_fields, "outcome_status": "TP_FIRST", "label_win": 1, "first_hit": "TP", "bars_to_outcome": index, "outcome_time_ms": hit_time, "candles_checked": index}
        if sl_hit:
            return {**base, **active_fields, "outcome_status": "SL_FIRST", "label_win": 0, "first_hit": "SL", "bars_to_outcome": index, "outcome_time_ms": hit_time, "candles_checked": index}

    latest_close = max(candle_close_ms(row, interval_ms) or 0 for row in selected)
    complete_horizon = len(selected) == horizon_bars and latest_close >= horizon_end - 1
    if effective_now >= horizon_end and not complete_horizon:
        return {**base, "outcome_status": "DATA_GAP", "entry_status": "ACTIVE" if active else "WAITING_ENTRY", "entry_hit": active, "entry_time_ms": candle_close_ms(selected[(entry_index or 1) - 1], interval_ms) if active else None, "bars_to_entry": entry_index, "bars_after_entry": len(selected) - (entry_index or len(selected)), "candles_checked": len(selected), "exclude_reason": "incomplete_horizon_coverage"}
    if complete_horizon:
        if not active:
            return {**base, "outcome_status": "ENTRY_NOT_FILLED", "entry_status": "NOT_FILLED", "candles_checked": len(selected), "outcome_time_ms": latest_close, "exclude_reason": "entry_not_touched"}
        return {**base, "outcome_status": "EXPIRED", "entry_status": "ACTIVE", "entry_hit": True, "entry_time_ms": candle_close_ms(selected[(entry_index or 1) - 1], interval_ms), "bars_to_entry": entry_index, "bars_after_entry": len(selected) - (entry_index or len(selected)), "first_hit": "NONE", "candles_checked": len(selected), "outcome_time_ms": latest_close, "exclude_reason": "no_barrier_hit_after_entry"}
    return {**base, "outcome_status": "PENDING", "entry_status": "ACTIVE" if active else "WAITING_ENTRY", "entry_hit": active, "entry_time_ms": candle_close_ms(selected[(entry_index or 1) - 1], interval_ms) if active else None, "bars_to_entry": entry_index, "bars_after_entry": len(selected) - (entry_index or len(selected)), "candles_checked": len(selected), "exclude_reason": "awaiting_horizon"}


def candidate_version_ms(row: Mapping[str, Any]) -> int:
    return epoch_ms(first(row, "updated_at_ms", "updated_at", "created_at_ms", "created_at_utc")) or signal_time_ms(row) or -1


def geometry_key(row: Mapping[str, Any]) -> Tuple[Any, ...]:
    direction, entry, sl, tp = plan(row)
    return normalize_symbol(row), direction, entry, sl, tp


def latest_unique_candidates(rows: Sequence[Mapping[str, Any]]) -> Tuple[List[Mapping[str, Any]], int, int]:
    latest: Dict[str, Mapping[str, Any]] = {}
    duplicates = 0
    collision_keys = set()
    for index, row in enumerate(rows):
        key = text(first(row, "signal_key", "signal_id")) or f"__missing_signal_key_{index}"
        if key in latest:
            duplicates += 1
            if geometry_key(latest[key]) != geometry_key(row):
                collision_keys.add(key)
        if key not in latest or candidate_version_ms(row) >= candidate_version_ms(latest[key]):
            latest[key] = row
    return list(latest.values()), duplicates, len(collision_keys)


def candle_file(candle_dir: Path, symbol: str, interval: str) -> Path:
    return candle_dir / f"{symbol}_{interval}.jsonl"


def evaluate_all(candidates: Sequence[Mapping[str, Any]], candle_dir: Path, interval: str, horizon_bars: int, now_ms: Optional[int] = None) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    unique, duplicate_count, geometry_collision_count = latest_unique_candidates(candidates)
    candle_cache: Dict[str, List[Dict[str, Any]]] = {}
    outcomes: List[Dict[str, Any]] = []
    for candidate in unique:
        sym = normalize_symbol(candidate)
        candle_cache.setdefault(sym, load_jsonl(candle_file(candle_dir, sym, interval)))
        outcomes.append(evaluate_candidate(candidate, candle_cache[sym], interval, horizon_bars, now_ms))
    counts = Counter(row["outcome_status"] for row in outcomes)
    total = len(unique)
    terminal = sum(row["outcome_status"] in TERMINAL_STATUSES for row in outcomes)
    evaluable = sum(row["outcome_status"] in EVALUABLE_STATUSES for row in outcomes)
    binary = sum(row["outcome_status"] in BINARY_STATUSES for row in outcomes)
    valid = sum(row["outcome_status"] != "INVALID_PLAN" for row in outcomes)
    rate = lambda value: round(value / total, 6) if total else 0.0
    report = {
        "shadow_schema_version": SCHEMA_VERSION,
        "candidate_rows": len(candidates), "unique_candidates": len(unique),
        "duplicate_candidate_rows": duplicate_count,
        "geometry_collision_count": geometry_collision_count,
        "outcomes_written": len(outcomes), "status_counts": dict(sorted(counts.items())),
        "terminal_coverage": rate(terminal),
        "evaluable_outcome_coverage": rate(evaluable),
        "usable_binary_label_coverage": rate(binary),
        "valid_plan_coverage": rate(valid),
        "data_gap_rate": rate(counts.get("DATA_GAP", 0)),
        "invalid_plan_rate": rate(counts.get("INVALID_PLAN", 0)),
        "pending_rate": rate(counts.get("PENDING", 0)),
        "entry_not_filled_rate": rate(counts.get("ENTRY_NOT_FILLED", 0)),
        "production_pnl_rows": sum(row.get("realized_pnl") is not None for row in outcomes),
    }
    report["checks"] = {
        "has_candidates": total > 0,
        "valid_plan_coverage_gte_99pct": report["valid_plan_coverage"] >= 0.99,
        "data_gap_rate_lte_5pct": report["data_gap_rate"] <= 0.05,
        "evaluable_outcome_coverage_gte_95pct": report["evaluable_outcome_coverage"] >= 0.95,
        "production_pnl_rows_zero": report["production_pnl_rows"] == 0,
    }
    report["passed"] = all(report["checks"].values())
    return outcomes, report


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidates", type=Path, default=Path("logs/ml_dataset_rows.jsonl"))
    parser.add_argument("--candle-dir", type=Path, default=Path("state/market_data"))
    parser.add_argument("--interval", default="5m")
    parser.add_argument("--horizon-bars", type=int, default=288)
    parser.add_argument("--now-ms", type=int)
    parser.add_argument("--out", type=Path, default=Path("logs/stat_tech_shadow_outcomes_v1.jsonl"))
    parser.add_argument("--report", type=Path, default=Path("logs/stat_tech_shadow_outcome_audit_v1.json"))
    parser.add_argument("--strict", action="store_true")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    outcomes, report = evaluate_all(load_jsonl(args.candidates), args.candle_dir, args.interval, args.horizon_bars, args.now_ms)
    write_jsonl(args.out, outcomes)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 2 if args.strict and not report["passed"] else 0


if __name__ == "__main__":
    sys.exit(main())
