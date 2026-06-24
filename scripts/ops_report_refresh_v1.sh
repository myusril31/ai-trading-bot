#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/ubuntu/ai-trading-vps-bot"
cd "$ROOT"

mkdir -p logs reports

echo "[ops-refresh] $(date -u +%Y-%m-%dT%H:%M:%SZ) start"

python3 scripts/bridge_blocker_summary_v1.py --hours 24
python3 scripts/rr_decay_report_v1.py --hours 24
python3 scripts/strict_vs_core_smc_compare_v1.py --hours 24
python3 scripts/core_smc_shadow_summary_v1.py --hours 24
python3 scripts/ai_manager_snapshot_v1.py

echo "[ops-refresh] $(date -u +%Y-%m-%dT%H:%M:%SZ) done"
