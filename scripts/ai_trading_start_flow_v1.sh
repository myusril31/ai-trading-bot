#!/usr/bin/env bash
# AI_TRADING_START_FLOW_V1_20260704
set -Eeuo pipefail

ROOT="${AI_TRADING_ROOT:-/home/ubuntu/ai-trading-vps-bot}"
CONTAINER="${AI_TRADING_CONTAINER:-ai-trading-bot}"
MODE="${1:-start}"   # start | restart
LOG_FILE="${AI_TRADING_START_FLOW_LOG:-/var/log/ai-trading-start-flow.log}"

mkdir -p "$(dirname "$LOG_FILE")"

log() {
  echo "[$(date -Is)] $*" | tee -a "$LOG_FILE"
}

unit_exists() {
  local u="$1"
  systemctl list-unit-files "$u" --no-legend 2>/dev/null | grep -q . \
    || systemctl list-units --all "$u" --no-legend 2>/dev/null | grep -q .
}

svc() {
  local u="$1"
  if unit_exists "$u"; then
    log "SERVICE ${MODE}: $u"
    if [ "$MODE" = "restart" ]; then
      systemctl restart "$u" || {
        log "WARN service restart failed: $u"
        return 0
      }
    else
      systemctl start "$u" || {
        log "WARN service start failed: $u"
        return 0
      }
    fi
    sleep 2
    systemctl is-active --quiet "$u" && log "OK active: $u" || log "WARN not-active-yet: $u"
  else
    log "SKIP missing unit: $u"
  fi
}

run_in_bot() {
  local script="$1"
  if docker exec "$CONTAINER" bash -lc "cd /app && [ -f '$script' ]" >/dev/null 2>&1; then
    log "SEED python $script"
    docker exec "$CONTAINER" bash -lc "cd /app && python '$script'" >>"$LOG_FILE" 2>&1 || log "WARN seed failed: $script"
  else
    log "SKIP missing seed script: $script"
  fi
}

log "=== AI TRADING ORDERED START FLOW BEGIN mode=$MODE ==="

cd "$ROOT"

log "Start docker"
systemctl start docker || true

log "Ensure bot container is running"
if docker inspect "$CONTAINER" >/dev/null 2>&1; then
  docker start "$CONTAINER" >/dev/null 2>&1 || true
elif docker compose version >/dev/null 2>&1; then
  docker compose up -d bot >>"$LOG_FILE" 2>&1 || docker compose up -d >>"$LOG_FILE" 2>&1 || true
else
  log "WARN container not found and docker compose unavailable"
fi

log "Wait container ready: $CONTAINER"
for i in $(seq 1 60); do
  if docker exec "$CONTAINER" bash -lc 'cd /app && python -V >/dev/null 2>&1' >/dev/null 2>&1; then
    log "OK container ready"
    break
  fi
  sleep 2
  if [ "$i" = "60" ]; then
    log "ERROR container not ready after wait"
    exit 1
  fi
done

log "Disable legacy SMC scheduler if exists"
if unit_exists "vps-smc-scheduler-loop.service"; then
  systemctl stop vps-smc-scheduler-loop.service || true
  systemctl disable vps-smc-scheduler-loop.service || true
fi

log "PHASE 1: market / candle / external feeds"
svc vps-binance-liquidation-ws.service
svc vps-binance-derivatives-feature-loop.service
svc vps-fred-macro-feature-loop.service
svc vps-market-data-loop.service
svc vps-candle-autoheal-loop.service
svc vps-candle-sync-loop.service

log "PHASE 2: one-shot seed before signal loop"
run_in_bot scripts/market_data_rest_sync_once.py
run_in_bot scripts/stat_tech_linear_quant_store_v1.py
run_in_bot scripts/stat_tech_stoch_barrier_store_v1.py
run_in_bot scripts/stat_tech_ou_meanrev_store_v1.py
run_in_bot scripts/ml_dataset_v4_linear_quant_join_v1.py
run_in_bot scripts/ml_dataset_v4_stoch_barrier_join_v1.py
run_in_bot scripts/ml_dataset_v4_ou_meanrev_join_v1.py
run_in_bot scripts/ml_outcome_label_forwarder_v1.py

log "PHASE 3: quant store loops"
svc vps-stat-tech-linear-quant-loop.service
svc vps-stat-tech-stoch-barrier-loop.service
svc vps-stat-tech-ou-meanrev-loop.service

log "PHASE 4: dataset join loops"
svc vps-ml-linear-dataset-join-loop.service
svc vps-ml-stoch-barrier-dataset-join-loop.service
svc vps-ml-ou-meanrev-dataset-join-loop.service
svc vps-ml-outcome-label-forwarder-loop.service

log "PHASE 5: live signal loop"
svc vps-stat-tech-live-loop.service

log "PHASE 6: lifecycle / execution monitor"
svc vps-tp-lifecycle-loop.service
svc vps-protection-fail-autoclose-loop.service
svc vps-position-manager-loop.service
svc vps-post-entry-manager-loop.service

log "PHASE 7: reports / health"
svc vps-tp-sl-predictor-readiness-loop.service
svc vps-market-data-report-loop.service
svc vps-telegram-market-report-loop.service
svc vps-health-report-loop.service

log "=== AI TRADING ORDERED START FLOW DONE ==="

systemctl --no-pager --plain --type=service --state=running \
  | grep -E "vps-|ai-trading|docker" \
  | tee -a "$LOG_FILE" || true
