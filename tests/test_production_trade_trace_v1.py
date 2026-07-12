import importlib.util
from pathlib import Path

MODULE_PATH = Path(__file__).parents[1] / "scripts" / "production_trade_trace_v1.py"
SPEC = importlib.util.spec_from_file_location("production_trade_trace_v1", MODULE_PATH)
module = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
SPEC.loader.exec_module(module)


def base_plan(**extra):
    row = {"signal_key": "sig-1", "plan_id": "plan-1", "symbol": "BTCUSDT", "direction": "LONG", "signal_time": "2026-07-01T00:00:00Z", "entry_order_ids": ["10"], "tp_order_ids": ["20"], "entry": 100, "sl": 95, "tp1": 110}
    row.update(extra)
    return row


def close_event(**extra):
    row = {"event": "TP_FILLED", "signal_key": "sig-1", "symbol": "BTCUSDT", "exit_order_id": "20", "close_reason": "TP1", "ts": "2026-07-01T01:00:00Z"}
    row.update(extra)
    return row


def fills():
    return [
        {"symbol": "BTCUSDT", "orderId": "10", "tradeId": "1", "side": "BUY", "price": "99", "qty": "1", "commission": "0.10", "realizedPnl": "0", "time": "2026-07-01T00:01:00Z"},
        {"symbol": "BTCUSDT", "orderId": "10", "tradeId": "2", "side": "BUY", "price": "101", "qty": "3", "commission": "0.30", "realizedPnl": "0", "time": "2026-07-01T00:02:00Z"},
        {"symbol": "BTCUSDT", "orderId": "20", "tradeId": "3", "side": "SELL", "reduceOnly": True, "price": "110", "qty": "4", "commission": "0.40", "realizedPnl": "38", "time": "2026-07-01T00:59:00Z"},
    ]


def test_closed_event_is_exact_whitelist_not_reason_based():
    assert module.is_closed_event({"event": "POSITION_CLOSED"})
    assert not module.is_closed_event({"event": "LIVE_ENTRY_FAILED", "reason": "binance_reject"})
    assert not module.is_closed_event({"event": "ORDER_CANCELLED", "reason": "stale"})


def test_generic_position_closed_without_reason_has_no_close_reason():
    event = close_event(event="POSITION_CLOSED", close_reason="")
    trace = module.build_traces([base_plan()], [event], fills(), [], "cfg")[0]
    assert trace["close_reason"] is None


def test_partial_fills_use_quantity_weighted_vwap_and_role_arrays():
    traces = module.build_traces([base_plan()], [close_event()], fills(), [], "cfg-test")
    trace = traces[0]
    assert trace["actual_entry"] == 100.5
    assert trace["actual_exit"] == 110.0
    assert trace["entry_order_ids"] == ["10"]
    assert trace["tp_order_ids"] == ["20"]
    assert trace["fill_ids"] == ["1", "2", "3"]
    assert trace["realized_pnl"] == 38.0
    assert trace["commission"] == 0.8
    assert trace["net_pnl"] == 37.2


def test_income_types_never_mix_funding_commission_and_realized():
    trade_rows = [dict(row, commission="") for row in fills()]
    income = [
        {"symbol": "BTCUSDT", "incomeType": "REALIZED_PNL", "income": "38", "time": "2026-07-01T00:59:00Z"},
        {"symbol": "BTCUSDT", "incomeType": "COMMISSION", "income": "-0.8", "time": "2026-07-01T00:59:01Z"},
        {"symbol": "BTCUSDT", "incomeType": "FUNDING_FEE", "income": "-0.2", "time": "2026-07-01T00:30:00Z"},
    ]
    for row in trade_rows:
        row.pop("realizedPnl", None)
    trace = module.build_traces([base_plan()], [close_event()], trade_rows, income, "cfg")[0]
    assert trace["realized_pnl"] == 38.0
    assert trace["commission"] == 0.8
    assert trace["funding_fee"] == -0.2
    assert trace["net_pnl"] == 37.0


def test_client_id_match_requires_symbol_and_time_window():
    plan = base_plan(entry_order_ids=[], client_order_ids=["reused"])
    event = close_event(exit_order_id="", client_order_ids=["reused"])
    rows = [
        {"symbol": "ETHUSDT", "clientOrderId": "reused", "side": "BUY", "qty": 1, "price": 1, "time": "2026-07-01T00:01:00Z"},
        {"symbol": "BTCUSDT", "clientOrderId": "reused", "side": "BUY", "qty": 1, "price": 2, "time": "2025-01-01T00:01:00Z"},
        {"symbol": "BTCUSDT", "clientOrderId": "reused", "side": "BUY", "qty": 1, "price": 100, "time": "2026-07-01T00:01:00Z"},
    ]
    trace = module.build_traces([plan], [event], rows, [], "cfg")[0]
    assert trace["actual_entry"] == 100
    assert len(trace["fill_ids"]) == 0


def test_generic_internal_id_is_never_an_order_match():
    plan = base_plan(entry_order_ids=[])
    event = close_event(exit_order_id="", id="999")
    row = {"symbol": "BTCUSDT", "orderId": "999", "side": "SELL", "qty": 1, "price": 110, "time": "2026-07-01T00:59:00Z"}
    trace = module.build_traces([plan], [event], [row], [], "cfg")[0]
    assert trace["exit_fill_covered"] is False


def test_duplicate_signal_plan_fails_plan_link_coverage():
    traces = module.build_traces([base_plan(), base_plan(plan_id="plan-2")], [close_event()], fills(), [], "cfg")
    assert traces[0]["duplicate_plan"] is True
    assert traces[0]["plan_linked"] is False


def test_reconstructs_closed_position_lifecycle_not_order_count():
    lifecycles = module.reconstruct_exchange_lifecycles(fills())
    assert len(lifecycles) == 1
    assert len(lifecycles[0]["fills"]) == 3


def test_partial_export_cannot_report_production_truth_ready():
    trace = module.build_traces([base_plan()], [close_event()], fills()[:2], [], "cfg")[0]
    report = module.audit([trace], module.reconstruct_exchange_lifecycles(fills()[:2]))
    assert report["passed"] is False
    assert report["interpretation"] == "dataset_anomaly"
    assert report["metrics"]["exit_fill_coverage"] == 0.0


def test_sanitized_fixture_reaches_complete_lifecycle(tmp_path):
    root = Path(__file__).parent / "fixtures" / "production_trade_trace"
    plans = module.load_rows(root / "plans.jsonl")
    events = module.load_rows(root / "events.jsonl")
    trades = module.load_rows(root / "trades.csv")
    income = module.load_rows(root / "income.csv")
    traces = module.build_traces(plans, events, trades, income, "cfg-fixture")
    report = module.audit(traces, module.reconstruct_exchange_lifecycles(trades))
    assert report["passed"] is True
    assert report["metrics"]["net_pnl_coverage"] == 1.0
