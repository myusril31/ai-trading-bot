#!/usr/bin/env bash
set -euo pipefail

cd /home/ubuntu/ai-trading-vps-bot

set -a
source .env
set +a

HEALTH_URL="http://127.0.0.1:8000/operator/candle-health?intervals=5m,15m,4h"
AUTOHEAL_TG_URL="http://127.0.0.1:8000/operator/candle-health/auto-heal/telegram?intervals=5m,15m,4h&limit=800"

curl -sS \
  -H "X-Webhook-Secret: ${WEBHOOK_SECRET}" \
  "$HEALTH_URL" \
  > /tmp/candle_health_tick.json

set +e
python3 - <<'PY'
import json
import sys
from pathlib import Path

p = Path("/tmp/candle_health_tick.json")
j = json.loads(p.read_text())

summary = {
    "phase": "health_check",
    "ok": j.get("ok"),
    "status": j.get("status"),
    "coverage_pct": j.get("coverage_pct"),
    "stale_count": j.get("stale_count"),
    "missing_count": j.get("missing_count"),
    "invalid_count": j.get("invalid_count"),
}

print(summary)

needs_heal = (
    not j.get("ok")
    or int(j.get("stale_count") or 0) > 0
    or int(j.get("missing_count") or 0) > 0
    or int(j.get("invalid_count") or 0) > 0
)

sys.exit(2 if needs_heal else 0)
PY
DECISION=$?
set -e

if [ "$DECISION" -eq 0 ]; then
  exit 0
fi

if [ "$DECISION" -ne 2 ]; then
  exit "$DECISION"
fi

curl -sS -X POST \
  -H "X-Webhook-Secret: ${WEBHOOK_SECRET}" \
  "$AUTOHEAL_TG_URL" \
  > /tmp/candle_autoheal_tick.json

python3 - <<'PY'
import json
from pathlib import Path

p = Path("/tmp/candle_autoheal_tick.json")
j = json.loads(p.read_text())

auto = j.get("auto_heal") or {}
before = auto.get("before") or {}
after = auto.get("after") or {}
bootstrap = auto.get("bootstrap") or {}

print({
    "phase": "auto_heal",
    "ok": j.get("ok"),
    "action": auto.get("action"),
    "reason": auto.get("reason"),
    "before_status": before.get("status"),
    "before_stale": before.get("stale_count"),
    "after_status": after.get("status"),
    "after_stale": after.get("stale_count"),
    "bootstrap_ok": bootstrap.get("ok"),
    "rows_ingested": bootstrap.get("rows_ingested"),
    "telegram": (j.get("telegram") or {}).get("sent"),
})
PY
