# AI Trading 2026 - Promotion Criteria

This file is the contract for Core SMC + Quant Filter promotion. Do not bypass it because a chart looked pretty. Humanity has suffered enough from vibes.

## Non-negotiable live hard gates

- RR target rewrite remains 1.2R: `RR_TARGET_R=1.2`, `RR_TARGET_MODE=SINGLE_FULL`.
- Live RR / geometry sanity must pass.
- TP1 already touched = no trade.
- Cost gate must pass.
- Orderbook bridge remains `LIVE_BLOCK`.
- ML fail-open remains false.
- Margin/notional sanity must pass.
- Max simultaneous positions, pair cooldown, max daily trades remain active.
- HTF hard extreme against direction remains a hard veto.

## Core SMC shadow promotion gate

CoreCandidate emitter is shadow-only until all are true:

1. At least 14 calendar days of `core_smc_shadow_candidates_v1.jsonl` data.
2. `bridge_blocker_summary_v1.json`, `rr_decay_report_v1.json`, and `strict_vs_core_smc_compare_v1.json` run cleanly daily.
3. At least one clean filled + protected execution from the existing production path.
4. CoreCandidate shows improved timing: materially lower TP1-touched/low-RR decay than strict confirmed signals.
5. Human review of at least 20 CoreCandidate samples.
6. No live execution route is enabled for core candidates without manual approval.

## Model promotion gate

A challenger model may not replace the live gate unless all are true:

1. Shadow-only for sufficient recent live samples.
2. Walk-forward OOS precision at operating threshold meets target.
3. Coverage is usable, not just tiny cherry-picked approvals.
4. AUC/PR-AUC improves materially versus current model.
5. Calibration is acceptable via Brier/calibration curve.
6. No leakage found in feature extraction.
7. Manual sign-off. Auto live promotion remains OFF.

## Forbidden shortcuts

- Do not lower ML threshold just to get fills.
- Do not lower or disable RR/cost/orderbook gates to force trades.
- Do not train loose CoreCandidate models using only strict-SMC labels and call it valid.
- Do not treat historical OHLCV fills as equivalent to live filled-and-protected outcomes.
- Do not run Hyperopt before execution proof and telemetry are healthy.
