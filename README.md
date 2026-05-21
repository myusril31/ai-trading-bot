# ai-trading-bot

## v0.20a — ML Phase 1 Completion (Shadow Only)

This release finalizes **ML Phase 1 dataset and context collection** in strict shadow mode.

### Guardrails (hard cap)
- ML is **SHADOW_ONLY**.
- No ML gate is used for execution decisions.
- No strategy/scoring change.
- No Apps Script change.
- No live/testnet order change from ML components.
- No credentials are committed.
- Training is offline/manual only.
- No auto-training from `app/main.py`.
- GSheet/FRED/model issues are fail-open.

### Runtime behavior
- Runtime writes shadow artifacts only:
  - `logs/ml_shadow_signals.jsonl`
  - `logs/ml_context_snapshots.jsonl`
  - `logs/ml_dataset_rows.jsonl`
  - `logs/ml_predictions.jsonl`
  - `logs/ml_context_errors.jsonl`
- Prediction logging is informational only (`decision_effect=SHADOW_ONLY`) and never alters execution.

### Manual offline trainer
Use the offline placeholder trainer script to inspect/export eligible training rows.

```bash
python scripts/train_ml_shadow_offline.py \
  --dataset logs/ml_dataset_rows.jsonl \
  --out state/ml_models/logistic_v1.placeholder.json
```

The trainer is not imported or called by `app/main.py`.
