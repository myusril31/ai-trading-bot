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

The state machine is `WAITING_ENTRY → ACTIVE → outcome`. A LONG limit entry is
filled only when `low <= entry`; SHORT requires `high >= entry`. An untouched
entry becomes `ENTRY_NOT_FILLED` after the complete horizon. Entry plus any
barrier in the same candle becomes `AMBIGUOUS_ENTRY_SAME_BAR` because intrabar
order is unknowable.

Only fully closed candles are evaluated. A candle overlapping the signal is
discarded unless the signal timestamp is exactly on its open. The tracker also
requires complete coverage from the first eligible candle through horizon end;
missing head, middle, or tail candles produce `DATA_GAP`, never endless pending.

Final outcomes are `TP_FIRST`, `SL_FIRST`, `EXPIRED`, `ENTRY_NOT_FILLED`,
`AMBIGUOUS_ENTRY_SAME_BAR`, `AMBIGUOUS_SAME_BAR`, `INVALID_PLAN`, or `DATA_GAP`.
`PENDING` candidates are re-evaluated later. Entry zones without an explicit
`final_entry`, `entry`, or `entry_mid` are invalid rather than silently choosing
one zone boundary.

The output is stored separately from Binance production truth. Its only purpose
is evaluating raw candidate, blocker, confluence, tier, pair, direction, setup,
and regime quality without risking capital. The audit separately reports
terminal, evaluable, binary-label, valid-plan, gap, invalid, pending, and
entry-not-filled rates. Passing requires >=99% valid plans, <=5% data gaps,
>=95% evaluable outcomes, and zero production-PnL rows. `INVALID_PLAN` and
`DATA_GAP` never count as successful outcome coverage.
