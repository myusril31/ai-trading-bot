#!/usr/bin/env bash
set -u

cd /home/ubuntu/ai-trading-vps-bot || exit 1

BASE="${VPS_BASE_URL:-http://127.0.0.1:8000}"
BOOTSTRAP_LIMIT="${MARKET_DATA_WATCHDOG_BOOTSTRAP_LIMIT:-1000}"

get_secret() {
  SECRET="${WEBHOOK_SECRET:-}"
  if [ -z "$SECRET" ] && [ -f ".env" ]; then
    SECRET="$(grep -E '^WEBHOOK_SECRET=' .env | tail -n1 | cut -d= -f2- | tr -d '"' | tr -d "'")"
  fi
  echo "$SECRET"
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

thresholds = {
  "1m": 5,
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
            times = [int(r["open_time_ms"]) for r in rows]
            if times != sorted(times):
                bad.append(f"{sym} {tf} NOT_SORTED")
            if len(times) != len(set(times)):
                bad.append(f"{sym} {tf} DUPES")

            last = times[-1]
            age = (now - last) / 60000
            if age > max_age:
                bad.append(f"{sym} {tf} STALE age={age:.1f} last={fmt(last)}")
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

bootstrap() {
  SECRET="$(get_secret)"
  if [ -z "$SECRET" ]; then
    echo "ERROR: WEBHOOK_SECRET empty"
    return 1
  fi

  echo "[$(date -Is)] bootstrap limit=${BOOTSTRAP_LIMIT}"
  curl -sS -m 240 -X POST "$BASE/market/candles/bootstrap?limit=${BOOTSTRAP_LIMIT}" \
    -H "X-Signal-Secret: $SECRET" \
  | jq -c '{ok,rows_ingested,failures,write}'
}

echo "[$(date -Is)] market data watchdog check"

if check_freshness; then
  exit 0
fi

echo "[$(date -Is)] stale/gap detected -> bootstrap"
bootstrap
sleep 5

if check_freshness; then
  echo "[$(date -Is)] repaired by bootstrap"
  exit 0
fi

echo "[$(date -Is)] still bad after bootstrap -> restart bot"
docker compose up -d --force-recreate bot
sleep 45

echo "[$(date -Is)] bootstrap after restart"
bootstrap
sleep 5

if check_freshness; then
  echo "[$(date -Is)] repaired after restart"
  exit 0
fi

echo "[$(date -Is)] FATAL: market data still bad"
exit 2
