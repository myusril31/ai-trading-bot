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
    out = module.evaluate_candidate(candidate(direction="SHORT", entry=100, sl=105, tp1=90), [bar(1, high=106, low=99)], "5m", 12, T0 + 12 * INTERVAL)
    assert out["outcome_status"] == "SL_FIRST"
    assert out["label_win"] == 0


def test_same_bar_is_ambiguous_not_forced_win_or_loss():
    out = module.evaluate_candidate(candidate(), [bar(1, high=111, low=94)], "5m", 12, T0 + 12 * INTERVAL)
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
    rows, duplicate_count = module.latest_unique_candidates([candidate(score=10), candidate(score=20)])
    assert duplicate_count == 1
    assert rows[0]["score"] == 20


def test_candidates_missing_signal_key_are_not_collapsed():
    rows, duplicate_count = module.latest_unique_candidates([
        candidate(signal_key=""), candidate(signal_key="")
    ])
    assert len(rows) == 2
    assert duplicate_count == 0


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
