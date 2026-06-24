# Phase 3 - Dataset Maturity v4 Status

## Status

V4_CANDIDATE_READY_WITH_WARNINGS

## Canonical Dataset

- File: logs/ml_dataset_v4_current14_candidate_join.jsonl
- Universe: current14 Binance USDT-M pairs
- Rows: 509
- Trainable labels: 281
- Wins: 190
- Losses: 91
- All 14 symbols present: yes
- Hard checks: pass

## Readiness Report

- State: V4_CANDIDATE_READY_WITH_WARNINGS
- Feature source: SIGNAL_ROW_FALLBACK
- Targets:
  - TP1: 157
  - SL: 91
  - TP2: 6
  - TP3: 27

## Warnings

- recursive_plan_drift_count: 23
- min_feature_count: 3
- feature_source: SIGNAL_ROW_FALLBACK
- Dataset is usable for report-only analysis and challenger shadow.
- Dataset is not mature enough for live model promotion.

## Execution Maturity Snapshot

- Rows: 681
- Live order placed rows: 184
- Position confirmed rows: 195
- Protection OK rows: 184
- Trainable outcome labels: 0
- Purpose: execution-chain maturity audit, not model training.

## Promotion Blockers

- Trainable labels below 500.
- Loss labels below 150.
- Recent/live label readiness still accumulating.
- Challenger/live model promotion remains disabled.
- Closed-trade outcome labeler still needed.

## Allowed Uses

- Dataset audit.
- Challenger shadow/offline training.
- Blocker analytics.
- Feature coverage analysis.
- Pair-level label distribution analysis.
- Execution maturity audit.

## Not Allowed

- Auto-promotion to live.
- Lowering ML threshold to increase trade count.
- Replacing live model based only on this dataset.
- Removing hard gates.

## Next Phase 3 Tasks

1. Build outcome_labeler_v1 for live filled/protected trades.
2. Reduce recursive plan drift.
3. Increase minimum feature count per row.
4. Keep accumulating live/recent labels.
5. Run challenger only in shadow/report mode.

## Git Commits

- 00bcb5d phase3: document dataset v4 maturity status
- d27cade phase3: add dataset v4 maturity reports
- 56a1b1d phase3: snapshot dataset v4 current14

## Phase 3 Build Dataset v4

Status: DONE for report-only and challenger shadow.
Next: outcome_labeler_v1.
