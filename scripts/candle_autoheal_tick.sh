#!/usr/bin/env bash
set -euo pipefail

cd /home/ubuntu/ai-trading-vps-bot

set -a
source .env
set +a

URL="http://127.0.0.1:8000/operator/candle-health/auto-heal/telegram?intervals=5m,15m,4h&limit=800"

curl -sS -X POST \
  -H "X-Webhook-Secret: ${WEBHOOK_SECRET}" \
  "$URL" \
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
