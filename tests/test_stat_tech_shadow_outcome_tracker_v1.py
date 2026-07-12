import importlib.util
from pathlib import Path

MODULE_PATH = Path(__file__).parents[1] / "scripts" / "stat_tech_shadow_outcome_tracker_v1.py"
SPEC = importlib.util.spec_from_file_location("shadow_tracker", MODULE_PATH)
module = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
SPEC.loader.exec_module(module)

T0 = 1_800_000_000_000
INTERVAL = 300_000


def candidate(**extra):
    row = {"signal_key": "sig-1", "symbol": "BTCUSDT", "direction": "LONG", "signal_time_ms": T0, "entry": 100, "sl": 95, "tp1": 110, "execution_decision": "REJECT"}
    row.update(extra)
    return row


def bar(index, high=105, low=98):
    opened = T0 + (index - 1) * INTERVAL
    return {"open_time_ms": opened, "close_time_ms": opened + INTERVAL - 1, "open": 100, "high": high, "low": low, "close": 101}


def test_tracks_rejected_raw_candidate_and_tp_first():
    out = module.evaluate_candidate(candidate(), [bar(1), bar(2, high=111)], "5m", 12, T0 + 12 * INTERVAL)
    assert out["outcome_status"] == "TP_FIRST"
    assert out["label_win"] == 1
    assert out["execution_decision"] == "REJECT"
    assert out["production_outcome"] is False


def test_short_sl_first():
    rows = [bar(1, high=101, low=99), bar(2, high=106, low=98)]
    out = module.evaluate_candidate(candidate(direction="SHORT", entry=100, sl=105, tp1=90), rows, "5m", 12, T0 + 12 * INTERVAL)
    assert out["outcome_status"] == "SL_FIRST"
    assert out["label_win"] == 0


def test_same_bar_is_ambiguous_not_forced_win_or_loss():
    out = module.evaluate_candidate(candidate(), [bar(1), bar(2, high=111, low=94)], "5m", 12, T0 + 12 * INTERVAL)
    assert out["outcome_status"] == "AMBIGUOUS_SAME_BAR"
    assert out["label_win"] is None
    assert out["same_bar_conflict"] is True


def test_expired_after_complete_horizon():
    rows = [bar(i) for i in range(1, 4)]
    out = module.evaluate_candidate(candidate(), rows, "5m", 3, T0 + 4 * INTERVAL)
    assert out["outcome_status"] == "EXPIRED"


def test_pending_before_horizon():
    out = module.evaluate_candidate(candidate(), [bar(1)], "5m", 3, T0 + INTERVAL)
    assert out["outcome_status"] == "PENDING"


def test_non_contiguous_candles_are_data_gap():
    out = module.evaluate_candidate(candidate(), [bar(1), bar(4)], "5m", 4, T0 + 5 * INTERVAL)
    assert out["outcome_status"] == "DATA_GAP"


def test_invalid_geometry_is_not_labeled():
    out = module.evaluate_candidate(candidate(sl=105), [bar(1)], "5m", 3, T0 + 4 * INTERVAL)
    assert out["outcome_status"] == "INVALID_PLAN"
    assert out["label_win"] is None


def test_deduplicates_candidate_by_signal_key_latest_wins():
    rows, duplicate_count, collisions = module.latest_unique_candidates([
        candidate(score=20, updated_at_ms=T0 + 1000),
        candidate(score=10, updated_at_ms=T0),
    ])
    assert duplicate_count == 1
    assert rows[0]["score"] == 20
    assert collisions == 0


def test_candidates_missing_signal_key_are_not_collapsed():
    rows, duplicate_count, collisions = module.latest_unique_candidates([
        candidate(signal_key=""), candidate(signal_key="")
    ])
    assert len(rows) == 2
    assert duplicate_count == 0
    assert collisions == 0


def test_naive_signal_time_wib_is_converted_to_utc_epoch():
    row = candidate(signal_time_ms=None, signal_time_wib="2027-01-15T07:00:00")
    assert module.signal_time_ms(row) == 1_799_971_200_000


def test_evaluate_all_never_writes_production_pnl(tmp_path):
    candle_dir = tmp_path / "candles"
    candle_dir.mkdir()
    module.write_jsonl(candle_dir / "BTCUSDT_5m.jsonl", [bar(1, high=111)])
    outcomes, report = module.evaluate_all([candidate()], candle_dir, "5m", 1, T0 + 2 * INTERVAL)
    assert outcomes[0]["realized_pnl"] is None
    assert report["production_pnl_rows"] == 0
    assert report["passed"] is True


def test_tp_without_entry_touch_is_entry_not_filled():
    out = module.evaluate_candidate(candidate(), [bar(1, high=111, low=104)], "5m", 1, T0 + 2 * INTERVAL)
    assert out["outcome_status"] == "ENTRY_NOT_FILLED"
    assert out["entry_hit"] is False
    assert out["label_win"] is None


def test_entry_then_tp_on_different_candle():
    out = module.evaluate_candidate(candidate(), [bar(1, high=103, low=99), bar(2, high=111, low=100)], "5m", 2, T0 + 3 * INTERVAL)
    assert out["outcome_status"] == "TP_FIRST"
    assert out["entry_hit"] is True
    assert out["bars_to_entry"] == 1
    assert out["bars_after_entry"] == 1


def test_entry_and_barriers_same_candle_is_entry_ambiguous():
    out = module.evaluate_candidate(candidate(), [bar(1, high=111, low=94)], "5m", 1, T0 + 2 * INTERVAL)
    assert out["outcome_status"] == "AMBIGUOUS_ENTRY_SAME_BAR"
    assert out["label_win"] is None


def test_overlap_candle_before_signal_is_discarded():
    signal = T0 + 2 * 60_000
    overlap = bar(1, high=111, low=99)
    next_full = bar(2, high=103, low=99)
    out = module.evaluate_candidate(candidate(signal_time_ms=signal), [overlap, next_full], "5m", 1, T0 + 3 * INTERVAL)
    assert out["outcome_status"] == "EXPIRED"
    assert out["first_eligible_open_ms"] == T0 + INTERVAL


def test_current_incomplete_candle_is_not_evaluated():
    out = module.evaluate_candidate(candidate(), [bar(1, high=111, low=99)], "5m", 1, T0 + INTERVAL - 2)
    assert out["outcome_status"] == "PENDING"
    assert out["candles_checked"] == 0


def test_horizon_passed_with_missing_tail_is_data_gap():
    out = module.evaluate_candidate(candidate(), [bar(1), bar(2)], "5m", 3, T0 + 4 * INTERVAL)
    assert out["outcome_status"] == "DATA_GAP"
    assert out["exclude_reason"] == "incomplete_horizon_coverage"


def test_all_invalid_plans_make_audit_fail(tmp_path):
    outcomes, report = module.evaluate_all([candidate(sl=105)], tmp_path, "5m", 1, T0 + 2 * INTERVAL)
    assert outcomes[0]["outcome_status"] == "INVALID_PLAN"
    assert report["valid_plan_coverage"] == 0.0
    assert report["passed"] is False


def test_all_data_gaps_make_audit_fail(tmp_path):
    outcomes, report = module.evaluate_all([candidate()], tmp_path, "5m", 1, T0 + 2 * INTERVAL)
    assert outcomes[0]["outcome_status"] == "DATA_GAP"
    assert report["data_gap_rate"] == 1.0
    assert report["passed"] is False


def test_short_entry_zone_requires_explicit_entry():
    row = candidate(direction="SHORT", entry=None, entry_lo=99, entry_hi=101, sl=105, tp1=90)
    out = module.evaluate_candidate(row, [bar(1)], "5m", 1, T0 + 2 * INTERVAL)
    assert out["outcome_status"] == "INVALID_PLAN"
    assert out["exclude_reason"] == "explicit_entry_required_for_zone"


def test_out_of_order_duplicate_selects_newest_timestamp_and_flags_geometry_collision():
    newest = candidate(entry=101, sl=95, tp1=110, updated_at_ms=T0 + 10_000)
    older = candidate(entry=100, sl=95, tp1=110, updated_at_ms=T0)
    rows, duplicates, collisions = module.latest_unique_candidates([newest, older])
    assert rows[0]["entry"] == 101
    assert duplicates == 1
    assert collisions == 1
