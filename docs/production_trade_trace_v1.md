# Production trade trace v1

This read-only P0 audit reconstructs **closed position lifecycles**, not order
counts. It requires separate Binance account-trade/fill and income-history
exports so realized PnL, commission, and funding cannot be mixed.

```bash
python scripts/production_trade_trace_v1.py \
  --plans logs/execution_plans.jsonl \
  --events logs/execution_events.jsonl \
  --trades private/binance_account_trades.csv \
  --income private/binance_income_history.csv \
  --strict
```

The canonical trace carries separate `entry_order_ids`, `tp_order_ids`,
`sl_order_ids`, `exit_order_ids`, and `fill_ids`. Entry and exit prices are
quantity-weighted fill VWAPs. Fills match by `(symbol, order_id)`, or by
`(symbol, client_order_id, lifecycle time window)`; generic `id` is never used.

Only explicit close events are eligible: `POSITION_CLOSED`, `TRADE_CLOSED`,
`TP_FILLED`, `SL_FILLED`, `MANUAL_CLOSE_FILLED`, and `LIQUIDATED`. A failure or
cancellation carrying a `reason` is not a closed trade.

Income processing is type-specific:

- fill `realizedPnl`, with `REALIZED_PNL` income as fallback;
- fill `commission`, with `COMMISSION` income as fallback;
- `FUNDING_FEE` only when symbol, position side, and open interval match.

Funding that cannot be safely allocated is reported as unallocated and excluded
from trade `net_pnl`. The audit cannot become `production_truth_ready` unless
plan, entry, exit, realized PnL, commission, funding allocation, close reason,
and net PnL coverage are each at least 99%, lifecycle counts agree, and duplicate
trade identity is zero.

Sanitized fixtures live under `tests/fixtures/production_trade_trace`. Never
commit real Binance exports or credentials.
