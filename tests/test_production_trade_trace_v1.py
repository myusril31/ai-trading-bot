import importlib.util
from pathlib import Path


MODULE_PATH = Path(__file__).parents[1] / "scripts" / "production_trade_trace_v1.py"
SPEC = importlib.util.spec_from_file_location("production_trade_trace_v1", MODULE_PATH)
module = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
SPEC.loader.exec_module(module)


def test_build_trace_joins_by_order_id_and_computes_net_pnl():
    plans = [{"signal_key": "sig-1", "plan_id": "plan-1", "symbol": "BTCUSDT", "direction": "LONG", "entry": 100, "sl": 95, "tp1": 110}]
    events = [{"event": "POSITION_CLOSED", "signal_key": "sig-1", "order_id": "42", "close_reason": "TP1", "ts": "2026-07-01T00:00:00Z"}]
    exchange = [{"orderId": "42", "price": "110", "realizedPnl": "10", "commission": "0.4"}]

    traces = module.build_traces(plans, events, exchange, "cfg-test")

    assert len(traces) == 1
    assert traces[0]["plan_id"] == "plan-1"
    assert traces[0]["realized_pnl"] == 10.0
    assert traces[0]["commission"] == 0.4
    assert traces[0]["net_pnl"] == 9.6
    assert traces[0]["label_win"] == 1
    assert traces[0]["exchange_matched"] is True


def test_audit_flags_missing_exchange_truth_as_dataset_anomaly():
    traces = [{"order_id": "1", "client_order_id": "", "exchange_matched": False, "realized_pnl": None, "commission": None}]
    report = module.audit(traces, [])
    assert report["passed"] is False
    assert report["interpretation"] == "dataset_anomaly"
    assert report["metrics"]["order_match_coverage"] == 0.0


def test_audit_detects_duplicate_trade_identity():
    row = {"order_id": "7", "client_order_id": "bot-7", "exchange_matched": True, "realized_pnl": 1.0, "commission": 0.1}
    report = module.audit([row, dict(row)], [{"orderId": "7", "clientOrderId": "bot-7"}])
    assert report["metrics"]["duplicate_trade_count"] == 1
    assert report["checks"]["duplicate_trade_count_zero"] is False


def test_audit_counts_one_binance_trade_when_both_ids_exist():
    row = {"order_id": "7", "client_order_id": "bot-7", "exchange_matched": True, "realized_pnl": 1.0, "commission": 0.1}
    report = module.audit([row], [{"orderId": "7", "clientOrderId": "bot-7"}])
    assert report["metrics"]["binance_unique_order_keys"] == 1
    assert report["checks"]["closed_trade_count_matches"] is True
    assert report["passed"] is True
