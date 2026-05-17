#!/usr/bin/env bash
set -euo pipefail

BASE_URL="http://127.0.0.1:8000"
ENV_FILE=".env"
ENV_BAK=".env.smoke_v012b.bak"
SYMBOL="${SYMBOL:-BTCUSDT}"
AUTH_HEADER=""
SMOKE_OK=0

if [[ -n "${WEBHOOK_SECRET:-}" ]]; then
  AUTH_HEADER="X-Signal-Secret: ${WEBHOOK_SECRET}"
fi

require_cmd() { command -v "$1" >/dev/null 2>&1 || { echo "missing required command: $1" >&2; exit 1; }; }

safe_mode_check() {
  echo "[safe] verifying API health"
  curl -fsS "$BASE_URL/health" >/dev/null
}

set_env_key() {
  local key="$1" value="$2"
  python3 - "$ENV_FILE" "$key" "$value" <<'PY'
import pathlib,sys
p=pathlib.Path(sys.argv[1]); k=sys.argv[2]; v=sys.argv[3]
lines=p.read_text().splitlines() if p.exists() else []
out=[]; found=False
for ln in lines:
    if ln.startswith(f"{k}="):
        out.append(f"{k}={v}"); found=True
    else:
        out.append(ln)
if not found: out.append(f"{k}={v}")
p.write_text("\n".join(out)+"\n")
PY
}

restart_bot() {
  docker compose up -d --force-recreate bot >/dev/null
}

wait_health() {
  local i
  for i in $(seq 1 30); do
    if curl -fsS "$BASE_URL/health" >/dev/null; then
      return 0
    fi
    sleep 2
  done
  echo "health check timeout after restart" >&2
  return 1
}

verify_testnet_market_mode() {
  local mode
  mode="$(docker compose exec -T bot sh -lc 'printf "%s" "${EXECUTION_MODE:-}"')"
  [[ "$mode" == "TESTNET_MARKET" ]] || { echo "expected EXECUTION_MODE=TESTNET_MARKET, got: $mode" >&2; return 1; }
}

api_call() {
  local method="$1" url="$2" data="${3:-}"
  if [[ -n "$AUTH_HEADER" ]]; then
    if [[ -n "$data" ]]; then
      curl -fsS -X "$method" "$url" -H "Content-Type: application/json" -H "$AUTH_HEADER" -d "$data"
    else
      curl -fsS -X "$method" "$url" -H "$AUTH_HEADER"
    fi
  else
    if [[ -n "$data" ]]; then
      curl -fsS -X "$method" "$url" -H "Content-Type: application/json" -d "$data"
    else
      curl -fsS -X "$method" "$url"
    fi
  fi
}

restore_safe_mode() {
  if [[ -f "$ENV_BAK" ]]; then
    mv -f "$ENV_BAK" "$ENV_FILE"
    restart_bot || true
    wait_health || true
  elif [[ -f "$ENV_FILE" ]]; then
    set_env_key EXECUTION_MODE DISABLED
    set_env_key ENABLE_TESTNET_ORDERS false
    set_env_key ORDER_TEST_ENDPOINT_ONLY true
    set_env_key BINANCE_ENV TESTNET
    restart_bot || true
    wait_health || true
  fi
  if [[ "$SMOKE_OK" -eq 1 ]]; then
    echo "SMOKE_V012B_PASS"
  fi
}
trap restore_safe_mode EXIT

require_cmd curl; require_cmd python3; require_cmd docker
safe_mode_check

[[ -f "$ENV_FILE" ]] || { echo "missing .env; cannot self-configure smoke mode" >&2; exit 1; }
cp "$ENV_FILE" "$ENV_BAK"

set_env_key EXECUTION_MODE TESTNET_MARKET
set_env_key ENABLE_TESTNET_ORDERS true
set_env_key ORDER_TEST_ENDPOINT_ONLY false
set_env_key BINANCE_ENV TESTNET
set_env_key TESTNET_KILL_SWITCH false

restart_bot
wait_health
verify_testnet_market_mode

api_call POST "$BASE_URL/testnet/place-order" "{\"symbol\":\"$SYMBOL\",\"side\":\"BUY\",\"quantity\":\"0.001\"}" >/tmp/v012b_entry.json
api_call POST "$BASE_URL/testnet/position-risk" "{\"symbol\":\"$SYMBOL\"}" >/tmp/v012b_position_after_entry.json

LEVELS_JSON="$(
python3 - <<'PY'
import json
from decimal import Decimal, ROUND_HALF_UP

raw = json.load(open('/tmp/v012b_position_after_entry.json'))
body = raw.get('body')
positions = raw.get('positions')
if isinstance(positions, list) and positions:
    row = positions[0]
elif isinstance(positions, dict):
    row = positions
elif isinstance(body, list) and body:
    row = body[0]
elif isinstance(body, dict):
    row = body
else:
    raise SystemExit('position-risk body missing')

def to_dec(v):
    try:
        return Decimal(str(v))
    except Exception:
        return Decimal('0')

entry = to_dec(row.get('entryPrice'))
mark = to_dec(row.get('markPrice'))
entry_mid = entry if entry > 0 else mark
if entry_mid <= 0:
    raise SystemExit('cannot derive entry_mid from entryPrice/markPrice')

q = Decimal('0.01')
entry_mid = entry_mid.quantize(q, rounding=ROUND_HALF_UP)
sl = (entry_mid * Decimal('0.99')).quantize(q, rounding=ROUND_HALF_UP)
tp1 = (entry_mid * Decimal('1.005')).quantize(q, rounding=ROUND_HALF_UP)
tp2 = (entry_mid * Decimal('1.010')).quantize(q, rounding=ROUND_HALF_UP)

print(json.dumps({
    'symbol': row.get('symbol') or '',
    'signal_key': 'SMOKE_V012B',
    'direction': 'LONG',
    'entry_mid': float(entry_mid),
    'tp1': float(tp1),
    'tp2': float(tp2),
    'sl': float(sl),
}, separators=(',', ':')))
PY
)"

api_call POST "$BASE_URL/testnet/place-protection" "$LEVELS_JSON" >/tmp/v012b_protection.json
api_call GET "$BASE_URL/testnet/algo-open-orders?symbol=$SYMBOL" >/tmp/v012b_open_before.json
api_call POST "$BASE_URL/testnet/cancel-protection" "{\"symbol\":\"$SYMBOL\"}" >/tmp/v012b_cancel.json
api_call GET "$BASE_URL/testnet/algo-open-orders?symbol=$SYMBOL" >/tmp/v012b_open_after.json

python3 - <<'PY'
import json
p='/tmp/v012b_open_after.json'
d=json.load(open(p))
orders=d.get('orders') or []
assert isinstance(orders,list), 'orders not a list'
assert len(orders)==0, f'open algo orders remain: {len(orders)}'
PY

api_call POST "$BASE_URL/testnet/close-position" "{\"symbol\":\"$SYMBOL\"}" >/tmp/v012b_close.json
api_call POST "$BASE_URL/testnet/position-risk" "{\"symbol\":\"$SYMBOL\"}" >/tmp/v012b_position.json
python3 - <<'PY'
import json
raw=json.load(open('/tmp/v012b_position.json'))
body=raw.get('body')
positions=raw.get('positions')
if isinstance(positions,list) and positions:
    row=positions[0]
elif isinstance(positions,dict):
    row=positions
elif isinstance(body,list) and body:
    row=body[0]
elif isinstance(body,dict):
    row=body
else:
    row={}
amt=str(row.get('positionAmt','0'))
assert amt in ('0','0.0',0,0.0), f'positionAmt not zero: {amt}'
PY

SMOKE_OK=1
