#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"
ENV_FILE=".env"
ENV_BAK=".env.smoke_v012b.bak"
SYMBOL="${SYMBOL:-UNIUSDT}"
SMOKE_OK=0

get_env_key() {
  local key="$1"
  grep -E "^${key}=" "$ENV_FILE" 2>/dev/null | tail -1 | cut -d= -f2- | sed 's/^"//;s/"$//'
}

WEBHOOK_SECRET="${WEBHOOK_SECRET:-$(get_env_key WEBHOOK_SECRET || true)}"
AUTH_HEADER=()
if [[ -n "${WEBHOOK_SECRET:-}" ]]; then
  AUTH_HEADER=(-H "X-Signal-Secret: ${WEBHOOK_SECRET}")
fi

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "missing required command: $1" >&2; exit 1; }
}

set_env_key() {
  local key="$1" value="$2"
  python3 - "$ENV_FILE" "$key" "$value" <<'PY'
import pathlib, sys
p = pathlib.Path(sys.argv[1])
k = sys.argv[2]
v = sys.argv[3]
lines = p.read_text().splitlines() if p.exists() else []
out = []
found = False
for ln in lines:
    if ln.startswith(f"{k}="):
        out.append(f"{k}={v}")
        found = True
    else:
        out.append(ln)
if not found:
    out.append(f"{k}={v}")
p.write_text("\n".join(out) + "\n")
PY
}

restart_bot() {
  docker compose up -d --force-recreate bot >/dev/null
}

wait_health() {
  for i in $(seq 1 40); do
    if curl -fsS "$BASE_URL/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "health check timeout" >&2
  return 1
}

api_call() {
  local method="$1" url="$2" data="${3:-}"
  if [[ -n "$data" ]]; then
    curl -fsS -X "$method" "$url" -H "Content-Type: application/json" "${AUTH_HEADER[@]}" -d "$data"
  else
    curl -fsS -X "$method" "$url" "${AUTH_HEADER[@]}"
  fi
}

restore_safe_mode() {
  echo "=== RESTORE SAFE MODE ==="
  if [[ -f "$ENV_BAK" ]]; then
    mv -f "$ENV_BAK" "$ENV_FILE"
  else
    set_env_key EXECUTION_MODE DISABLED
    set_env_key ENABLE_TESTNET_ORDERS false
    set_env_key ORDER_TEST_ENDPOINT_ONLY true
    set_env_key BINANCE_ENV TESTNET
    set_env_key TESTNET_KILL_SWITCH false
  fi
  restart_bot || true
  wait_health || true

  echo "=== FINAL SAFE ENV ==="
  grep -E '^(EXECUTION_MODE|ENABLE_TESTNET_ORDERS|ORDER_TEST_ENDPOINT_ONLY|BINANCE_ENV|TESTNET_KILL_SWITCH)=' "$ENV_FILE" || true

  if [[ "$SMOKE_OK" -eq 1 ]]; then
    echo "SMOKE_V012B_PASS"
  fi
}
trap restore_safe_mode EXIT

require_cmd curl
require_cmd python3
require_cmd docker

echo "=== SAFE HEALTH BEFORE ==="
curl -fsS "$BASE_URL/health" || true
echo

[[ -f "$ENV_FILE" ]] || { echo "missing .env; cannot self-configure smoke mode" >&2; exit 1; }
cp "$ENV_FILE" "$ENV_BAK"

echo "=== ENABLE CONTROLLED TESTNET MODE ==="
set_env_key EXECUTION_MODE TESTNET_MARKET
set_env_key ENABLE_TESTNET_ORDERS true
set_env_key ORDER_TEST_ENDPOINT_ONLY false
set_env_key BINANCE_ENV TESTNET
set_env_key TESTNET_KILL_SWITCH false

restart_bot
wait_health

echo "=== ENV INSIDE CONTAINER ==="
docker compose exec -T bot sh -lc 'env | grep -E "^(EXECUTION_MODE|ENABLE_TESTNET_ORDERS|ORDER_TEST_ENDPOINT_ONLY|BINANCE_ENV|TESTNET_KILL_SWITCH)="'

EXEC_MODE="$(docker compose exec -T bot sh -lc 'printf "%s" "${EXECUTION_MODE:-}"')"
[[ "$EXEC_MODE" == "TESTNET_MARKET" ]] || {
  echo "expected EXECUTION_MODE=TESTNET_MARKET, got: $EXEC_MODE" >&2
  exit 1
}

echo "=== ENTRY TESTNET ==="
api_call POST "$BASE_URL/testnet/place-order" "{\"symbol\":\"$SYMBOL\",\"side\":\"BUY\",\"quantity\":\"1\"}" >/tmp/v012b_entry.json
cat /tmp/v012b_entry.json
echo
sleep 2

echo "=== POSITION AFTER ENTRY ==="
api_call POST "$BASE_URL/testnet/position-risk" "{\"symbol\":\"$SYMBOL\"}" >/tmp/v012b_position_entry.json
cat /tmp/v012b_position_entry.json
echo

python3 - <<'PY'
import json
from decimal import Decimal, ROUND_HALF_UP

raw = json.load(open("/tmp/v012b_position_entry.json"))
body = raw.get("body")

if isinstance(body, list) and body:
    row = body[0]
elif isinstance(body, dict):
    row = body
else:
    raise SystemExit(f"bad position body: {raw}")

def dec(v):
    return Decimal(str(v))

amt = dec(row.get("positionAmt", "0"))
if amt == 0:
    raise SystemExit(f"positionAmt still zero after entry: {row}")

mid = None
for k in ("entryPrice", "markPrice", "breakEvenPrice"):
    v = row.get(k)
    if v not in (None, "", "0", "0.0", 0, 0.0):
        mid = dec(v)
        break

if mid is None or mid <= 0:
    raise SystemExit(f"cannot derive dynamic price from position-risk: {row}")

q = Decimal("0.01") if mid >= 100 else Decimal("0.0001")

def fmt(x):
    return str(x.quantize(q, rounding=ROUND_HALF_UP))

open("/tmp/v012b_prices.env", "w").write(
    f"ENTRY_MID={fmt(mid)}\n"
    f"TP1={fmt(mid * Decimal('1.002'))}\n"
    f"TP2={fmt(mid * Decimal('1.004'))}\n"
    f"SL={fmt(mid * Decimal('0.998'))}\n"
)
PY

. /tmp/v012b_prices.env
echo "[prices] entry_mid=$ENTRY_MID tp1=$TP1 tp2=$TP2 sl=$SL"

echo "=== PLACE PROTECTION ==="
api_call POST "$BASE_URL/testnet/place-protection" "{\"symbol\":\"$SYMBOL\",\"signal_key\":\"SMOKE_V012B\",\"direction\":\"LONG\",\"entry_mid\":$ENTRY_MID,\"tp1\":$TP1,\"tp2\":$TP2,\"sl\":$SL}" >/tmp/v012b_protection.json
cat /tmp/v012b_protection.json
echo

echo "=== OPEN ALGO BEFORE CANCEL ==="
api_call GET "$BASE_URL/testnet/algo-open-orders?symbol=$SYMBOL" >/tmp/v012b_open_before.json
cat /tmp/v012b_open_before.json
echo

echo "=== CANCEL PROTECTION ==="
api_call POST "$BASE_URL/testnet/cancel-protection" "{\"symbol\":\"$SYMBOL\"}" >/tmp/v012b_cancel.json
cat /tmp/v012b_cancel.json
echo
sleep 2

echo "=== OPEN ALGO AFTER CANCEL ==="
api_call GET "$BASE_URL/testnet/algo-open-orders?symbol=$SYMBOL" >/tmp/v012b_open_after.json
cat /tmp/v012b_open_after.json
echo

python3 - <<'PY'
import json
raw = json.load(open("/tmp/v012b_open_after.json"))
orders = raw.get("orders") or raw.get("body") or []
if isinstance(orders, dict):
    orders = orders.get("orders") or []
assert isinstance(orders, list), f"orders not list: {raw}"
assert len(orders) == 0, f"open algo orders remain: {len(orders)}"
PY

echo "=== CLOSE POSITION ==="
api_call POST "$BASE_URL/testnet/close-position" "{\"symbol\":\"$SYMBOL\"}" >/tmp/v012b_close.json
cat /tmp/v012b_close.json
echo
sleep 2

echo "=== VERIFY ZERO POSITION ==="
api_call POST "$BASE_URL/testnet/position-risk" "{\"symbol\":\"$SYMBOL\"}" >/tmp/v012b_position_final.json
cat /tmp/v012b_position_final.json
echo

python3 - <<'PY'
import json
from decimal import Decimal
raw = json.load(open("/tmp/v012b_position_final.json"))
body = raw.get("body")
if isinstance(body, list) and body:
    row = body[0]
elif isinstance(body, dict):
    row = body
else:
    row = {}
amt = Decimal(str(row.get("positionAmt", "0")))
assert amt == 0, f"positionAmt not zero: {amt}"
PY

SMOKE_OK=1
