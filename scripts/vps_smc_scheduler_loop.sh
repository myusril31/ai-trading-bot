#!/usr/bin/env bash
set -u

cd /home/ubuntu/ai-trading-vps-bot || exit 1

BASE="${VPS_BASE_URL:-http://127.0.0.1:8000}"
INTERVAL="${VPS_SMC_SCHEDULER_INTERVAL_SEC:-60}"

# Ambil candle closed terbaru tiap tick. Ringan.
LATEST_LIMIT="${MARKET_DATA_LATEST_SYNC_LIMIT:-20}"

# Repair besar kalau missing/stale.
FULL_BOOTSTRAP_LIMIT="${MARKET_DATA_WATCHDOG_BOOTSTRAP_LIMIT:-1000}"

# Kalau repair gagal terus, tunggu sebentar lalu coba lagi.
REPAIR_SLEEP_SEC="${MARKET_DATA_REPAIR_SLEEP_SEC:-20}"

# Restart container setelah beberapa kali repair gagal.
RESTART_AFTER_FAILS="${MARKET_DATA_RESTART_AFTER_FAILS:-3}"

get_secret() {
  SECRET="${WEBHOOK_SECRET:-}"
  if [ -z "$SECRET" ] && [ -f ".env" ]; then
    SECRET="$(grep -E '^WEBHOOK_SECRET=' .env | tail -n1 | cut -d= -f2- | tr -d '"' | tr -d "'")"
  fi
  echo "$SECRET"
}

bootstrap_candles() {
  local limit="$1"
  local label="$2"
  SECRET="$(get_secret)"

  if [ -z "$SECRET" ]; then
    echo "[$(date -Is)] ERROR: WEBHOOK_SECRET empty, cannot bootstrap"
    return 1
  fi

  echo "[$(date -Is)] ${label}: bootstrap limit=${limit}"

  curl -sS -m 240 -X POST "$BASE/market/candles/bootstrap?limit=${limit}" \
    -H "X-Signal-Secret: $SECRET" \
  | jq -c '{ok,rows_ingested,failures,write}' || return 1
}

check_freshness() {
python3 - <<'PY'
import json, time, sys
from pathlib import Path
from datetime import datetime, timezone, timedelta

WIB = timezone(timedelta(hours=7))

symbols = [
  "AAVEUSDT","ADAUSDT","AVAXUSDT","BCHUSDT","BTCUSDT","DOTUSDT","ETHUSDT",
  "HYPEUSDT","LINKUSDT","LTCUSDT","PAXGUSDT","SOLUSDT","SUIUSDT","UNIUSDT",
  "XRPUSDT","ZECUSDT"
]

# Freshness dihitung dari close_time_ms, bukan open_time_ms.
# SMC hard dependency: 5m, 15m, 4h.
thresholds = {
  "5m": 10,
  "15m": 30,
  "4h": 360,
}

now = int(time.time() * 1000)
bad = []

def fmt(ms):
    return datetime.fromtimestamp(ms/1000, timezone.utc).astimezone(WIB).strftime("%Y-%m-%d %H:%M:%S WIB")

for tf, max_age in thresholds.items():
    for sym in symbols:
        p = Path(f"state/market_data/{sym}_{tf}.jsonl")
        if not p.exists():
            bad.append(f"{sym} {tf} MISSING_FILE")
            continue

        lines = [x for x in p.read_text().splitlines() if x.strip()]
        if not lines:
            bad.append(f"{sym} {tf} EMPTY_FILE")
            continue

        try:
            rows = [json.loads(x) for x in lines]
            open_times = [int(r["open_time_ms"]) for r in rows]

            if open_times != sorted(open_times):
                bad.append(f"{sym} {tf} NOT_SORTED")

            if len(open_times) != len(set(open_times)):
                bad.append(f"{sym} {tf} DUPES")

            last_row = rows[-1]
            last_open = int(last_row.get("open_time_ms") or 0)
            last_close = int(last_row.get("close_time_ms") or last_open)
            age = (now - last_close) / 60000

            if age > max_age:
                bad.append(
                    f"{sym} {tf} STALE age={age:.1f} "
                    f"last_open={fmt(last_open)} last_close={fmt(last_close)}"
                )

        except Exception as e:
            bad.append(f"{sym} {tf} BAD_FILE {type(e).__name__}:{e}")

if bad:
    print("MARKET_DATA_BAD")
    for x in bad[:40]:
        print(x)
    if len(bad) > 40:
        print(f"... +{len(bad)-40} more")
    sys.exit(2)

print("MARKET_DATA_OK")
sys.exit(0)
PY
}

run_smc_tick() {
  SECRET="$(get_secret)"

  if [ -z "$SECRET" ]; then
    echo "[$(date -Is)] ERROR: WEBHOOK_SECRET empty, cannot run SMC"
    return 1
  fi

  echo "[$(date -Is)] SMC tick start"

  curl -sS -m 55 -X POST "$BASE/vps-smc/scheduler/run-once" \
    -H "X-Signal-Secret: $SECRET" \
    -H "Content-Type: application/json" \
    -d '{}' \
  | jq -c '{
      ok,
      signal_count,
      confirmed_count:(.diagnostic_summary.confirmed_count // null),
      blockers:(.diagnostic_summary.by_final_blocker // {})
    }' || return 1
}

repair_until_fresh() {
  local fails=0

  while true; do
    echo "[$(date -Is)] latest-sync before freshness check"
    bootstrap_candles "$LATEST_LIMIT" "latest-sync"
    sleep 2

    if check_freshness; then
      echo "[$(date -Is)] market data fresh"
      return 0
    fi

    fails=$((fails + 1))
    echo "[$(date -Is)] market data bad -> full repair attempt=${fails}"
    bootstrap_candles "$FULL_BOOTSTRAP_LIMIT" "full-repair"
    sleep 5

    if check_freshness; then
      echo "[$(date -Is)] market data repaired by full bootstrap"
      return 0
    fi

    if [ "$fails" -ge "$RESTART_AFTER_FAILS" ]; then
      echo "[$(date -Is)] repair failed ${fails}x -> restart bot container"
      docker compose up -d --force-recreate bot
      sleep 45

      echo "[$(date -Is)] post-restart repair"
      bootstrap_candles "$FULL_BOOTSTRAP_LIMIT" "post-restart-repair"
      sleep 5

      if check_freshness; then
        echo "[$(date -Is)] market data repaired after restart"
        return 0
      fi

      fails=0
    fi

    echo "[$(date -Is)] HOLD: market data not fresh yet, retry in ${REPAIR_SLEEP_SEC}s"
    sleep "$REPAIR_SLEEP_SEC"
  done
}

while true; do
  echo "[$(date -Is)] ===== VPS SMC LOOP TICK ====="

  # Tidak ada SKIP cycle.
  # Kalau data belum fresh, dia repair-loop sampai fresh.
  repair_until_fresh

  echo "[$(date -Is)] market data fresh -> SMC allowed"
  run_smc_tick

  sleep "$INTERVAL"
done
