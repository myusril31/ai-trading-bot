#!/usr/bin/env bash
set -euo pipefail

cd /home/ubuntu/ai-trading-vps-bot

# avoid overlapping runs
LOCK_FILE="/tmp/vps_smc_scheduler_tick.lock"
exec 9>"$LOCK_FILE"
flock -n 9 || exit 0

set -a
source .env
set +a

echo "=== $(date -u +'%Y-%m-%dT%H:%M:%SZ') scheduler tick ==="

curl -sS --max-time 180 -X POST "http://127.0.0.1:8000/vps-smc/scheduler/run-once" \
  -H "Content-Type: application/json" \
  -H "X-Webhook-Secret: ${WEBHOOK_SECRET}" \
  -d '{"run_vps":true,"run_compare":false,"mirror_gsheet":false}' \
| jq '{ok,error,signal_count:.run_vps_result.signal_count, blockers:.run_vps_result.diagnostic_summary.by_final_blocker}'

echo
