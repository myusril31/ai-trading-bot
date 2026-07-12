# STAT_TECH shadow outcome tracker v1

This read-only tracker labels every raw candidate, including blocked/rejected
candidates, from the repository candle store. It does not place orders and never
writes production PnL.

```bash
python scripts/stat_tech_shadow_outcome_tracker_v1.py \
  --candidates logs/ml_dataset_rows.jsonl \
  --candle-dir state/market_data \
  --interval 5m \
  --horizon-bars 288 \
  --strict
```

Final outcomes are `TP_FIRST`, `SL_FIRST`, `EXPIRED`,
`AMBIGUOUS_SAME_BAR`, `INVALID_PLAN`, or `DATA_GAP`. `PENDING` candidates are
re-evaluated later. If TP and SL occur inside one OHLC candle, the result stays
ambiguous and receives no win/loss label because intrabar order is unknowable.

The output is stored separately from Binance production truth. Its only purpose
is evaluating raw candidate, blocker, confluence, tier, pair, direction, setup,
and regime quality without risking capital. Promotion requires at least 95%
final shadow-outcome coverage and zero production-PnL rows.
