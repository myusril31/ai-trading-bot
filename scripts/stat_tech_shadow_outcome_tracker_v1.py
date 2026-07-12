#!/usr/bin/env python3
"""Label every raw STAT_TECH candidate from closed candles, without execution."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

SCHEMA_VERSION = "stat_tech_shadow_outcome_v1"
FINAL_STATUSES = {"TP_FIRST", "SL_FIRST", "EXPIRED", "AMBIGUOUS_SAME_BAR", "INVALID_PLAN", "DATA_GAP"}
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
    entry = number(first(row, "final_entry", "entry", "entry_mid", "entry_lo"))
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


def evaluate_candidate(candidate: Mapping[str, Any], candles: Sequence[Mapping[str, Any]], interval: str = "5m", horizon_bars: int = 288, now_ms: Optional[int] = None) -> Dict[str, Any]:
    interval_ms = interval_to_ms(interval)
    signal_ms = signal_time_ms(candidate)
    direction, entry, sl, tp = plan(candidate)
    base = {
        "shadow_schema_version": SCHEMA_VERSION,
        "signal_key": first(candidate, "signal_key", "signal_id"),
        "symbol": normalize_symbol(candidate) or None,
        "direction": direction or None,
        "setup_type": first(candidate, "setup_type", "strategy"),
        "regime": first(candidate, "regime", "market_regime"),
        "score": number(first(candidate, "confluence_score", "score")),
        "execution_decision": first(candidate, "execution_decision", "decision"),
        "signal_time_ms": signal_ms,
        "entry": entry, "sl": sl, "tp": tp,
        "interval": interval, "horizon_bars": horizon_bars,
        "outcome_status": "PENDING", "label_win": None,
        "first_hit": None, "bars_to_outcome": None, "outcome_time_ms": None,
        "candles_expected": horizon_bars, "candles_checked": 0,
        "same_bar_conflict": False, "production_outcome": False,
        "realized_pnl": None, "exclude_reason": None,
    }
    if not text(base["signal_key"]):
        return {**base, "outcome_status": "INVALID_PLAN", "exclude_reason": "missing_signal_key"}
    if signal_ms is None:
        return {**base, "outcome_status": "INVALID_PLAN", "exclude_reason": "missing_signal_time"}
    if not valid_plan(direction, entry, sl, tp):
        return {**base, "outcome_status": "INVALID_PLAN", "exclude_reason": "invalid_plan_geometry"}
    horizon_ms = signal_ms + horizon_bars * interval_ms
    selected = sorted(
        [row for row in candles if (candle_close_ms(row, interval_ms) or 0) > signal_ms and (candle_open_ms(row) or horizon_ms + 1) < horizon_ms],
        key=lambda row: candle_open_ms(row) or 0,
    )
    if not selected:
        current = now_ms if now_ms is not None else int(datetime.now(timezone.utc).timestamp() * 1000)
        status = "PENDING" if current < horizon_ms else "DATA_GAP"
        return {**base, "outcome_status": status, "exclude_reason": "awaiting_candles" if status == "PENDING" else "no_candles"}
    if not data_is_contiguous(selected, interval_ms):
        return {**base, "outcome_status": "DATA_GAP", "candles_checked": len(selected), "exclude_reason": "non_contiguous_candles"}
    for index, candle in enumerate(selected, 1):
        high, low = number(candle.get("high")), number(candle.get("low"))
        if high is None or low is None:
            return {**base, "outcome_status": "DATA_GAP", "candles_checked": index, "exclude_reason": "invalid_ohlc"}
        tp_hit = high >= tp if direction == "LONG" else low <= tp
        sl_hit = low <= sl if direction == "LONG" else high >= sl
        hit_time = candle_close_ms(candle, interval_ms)
        if tp_hit and sl_hit:
            return {**base, "outcome_status": "AMBIGUOUS_SAME_BAR", "first_hit": "AMBIGUOUS", "bars_to_outcome": index, "outcome_time_ms": hit_time, "candles_checked": index, "same_bar_conflict": True, "exclude_reason": "ohlc_cannot_resolve_intrabar_order"}
        if tp_hit:
            return {**base, "outcome_status": "TP_FIRST", "label_win": 1, "first_hit": "TP", "bars_to_outcome": index, "outcome_time_ms": hit_time, "candles_checked": index}
        if sl_hit:
            return {**base, "outcome_status": "SL_FIRST", "label_win": 0, "first_hit": "SL", "bars_to_outcome": index, "outcome_time_ms": hit_time, "candles_checked": index}
    latest_close = max(candle_close_ms(row, interval_ms) or 0 for row in selected)
    current = now_ms if now_ms is not None else int(datetime.now(timezone.utc).timestamp() * 1000)
    completed = latest_close >= horizon_ms - 1 or current >= horizon_ms and len(selected) >= horizon_bars
    if completed:
        return {**base, "outcome_status": "EXPIRED", "first_hit": "NONE", "candles_checked": len(selected), "outcome_time_ms": latest_close, "exclude_reason": "no_barrier_hit"}
    return {**base, "outcome_status": "PENDING", "candles_checked": len(selected), "exclude_reason": "awaiting_horizon"}


def latest_unique_candidates(rows: Sequence[Mapping[str, Any]]) -> Tuple[List[Mapping[str, Any]], int]:
    latest: Dict[str, Mapping[str, Any]] = {}
    duplicates = 0
    for index, row in enumerate(rows):
        key = text(first(row, "signal_key", "signal_id")) or f"__missing_signal_key_{index}"
        if key in latest:
            duplicates += 1
        latest[key] = row
    return list(latest.values()), duplicates


def candle_file(candle_dir: Path, symbol: str, interval: str) -> Path:
    return candle_dir / f"{symbol}_{interval}.jsonl"


def evaluate_all(candidates: Sequence[Mapping[str, Any]], candle_dir: Path, interval: str, horizon_bars: int, now_ms: Optional[int] = None) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    unique, duplicate_count = latest_unique_candidates(candidates)
    candle_cache: Dict[str, List[Dict[str, Any]]] = {}
    outcomes: List[Dict[str, Any]] = []
    for candidate in unique:
        sym = normalize_symbol(candidate)
        candle_cache.setdefault(sym, load_jsonl(candle_file(candle_dir, sym, interval)))
        outcomes.append(evaluate_candidate(candidate, candle_cache[sym], interval, horizon_bars, now_ms))
    counts = Counter(row["outcome_status"] for row in outcomes)
    final = sum(row["outcome_status"] in FINAL_STATUSES for row in outcomes)
    valid = sum(row["outcome_status"] != "INVALID_PLAN" for row in outcomes)
    report = {
        "shadow_schema_version": SCHEMA_VERSION,
        "candidate_rows": len(candidates), "unique_candidates": len(unique),
        "duplicate_candidate_rows": duplicate_count,
        "outcomes_written": len(outcomes), "status_counts": dict(sorted(counts.items())),
        "shadow_outcome_coverage": round(final / len(unique), 6) if unique else 0.0,
        "valid_plan_coverage": round(valid / len(unique), 6) if unique else 0.0,
        "production_pnl_rows": sum(row.get("realized_pnl") is not None for row in outcomes),
    }
    report["passed"] = bool(unique) and report["shadow_outcome_coverage"] >= 0.95 and report["production_pnl_rows"] == 0
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
