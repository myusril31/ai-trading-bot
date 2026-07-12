# Production trade trace v1

`scripts/production_trade_trace_v1.py` is a read-only P0 audit. It joins closed
internal execution events to a Binance CSV/JSONL export using `order_id` or
`client_order_id`, emits one canonical trace per internal closed trade, and
measures whether the dataset is trustworthy enough for signal tuning.

```bash
python scripts/production_trade_trace_v1.py \
  --plans logs/execution_plans.jsonl \
  --events logs/execution_events.jsonl \
  --binance private/binance_trades.csv \
  --strict
```

Outputs default to:

- `logs/production_trade_traces_v1.jsonl`
- `logs/production_trade_trace_audit_v1.json`

Acceptance checks are intentionally strict: 99% order/PnL/fee coverage, no
duplicate internal trade identity, and equal internal/Binance closed counts.
Failure is labelled `dataset_anomaly`; it is not interpreted as strategy
performance. The script does not call Binance or mutate live execution state.

Do not commit Binance exports. Keep them outside tracked paths and pass their
location through `--binance`.
