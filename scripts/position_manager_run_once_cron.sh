#!/usr/bin/env bash
set -euo pipefail

cd /home/ubuntu/ai-trading-vps-bot

SECRET=$(grep -E '^WEBHOOK_SECRET=' .env | tail -1 | cut -d= -f2- | tr -d '"' | tr -d "'")
OUT="logs/position_manager_run_once_cron.jsonl"

RESP=$(curl -sS --max-time 60 -X POST \
  -H "X-Webhook-Secret: $SECRET" \
  -H "Content-Type: application/json" \
  http://localhost:8000/position-manager/run-once \
  -d '{}' 2>&1 || true)

python3 - "$RESP" <<'PY' >> "$OUT"
import sys, json, datetime

raw = sys.argv[1] if len(sys.argv) > 1 else ""
try:
    parsed = json.loads(raw)
except Exception:
    parsed = {"ok": False, "raw": raw}

row = {
    "ts_utc": datetime.datetime.now(datetime.UTC).isoformat(),
    "source": "cron_position_manager_run_once",
    "response": parsed,
}
print(json.dumps(row, separators=(",", ":")))
PY
