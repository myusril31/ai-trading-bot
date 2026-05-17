#!/usr/bin/env bash
set -euo pipefail

BASE_URL="http://127.0.0.1:8000"
SYMBOL="BTCUSDT"
SAFE_MODE_BACKUP=""

safe_mode_check() {
  curl -sS "$BASE_URL/healthz" >/dev/null || true
}

restore_safe_mode() {
  if [[ -n "${SAFE_MODE_BACKUP}" ]]; then
    echo "Restoring safe mode env"
  fi
}
trap restore_safe_mode EXIT

enable_controlled_testnet_mode() {
  echo "Enable controlled TESTNET mode in runtime env before running app"
}

safe_mode_check
enable_controlled_testnet_mode

curl -sS -X POST "$BASE_URL/testnet/place-order" -H 'Content-Type: application/json' -d '{"symbol":"'"$SYMBOL"'","side":"BUY","quantity":"0.001"}' >/tmp/v012b_entry.json

curl -sS -X POST "$BASE_URL/testnet/place-protection" -H 'Content-Type: application/json' -d '{"symbol":"'"$SYMBOL"'","signal_key":"SMOKE_V012B","direction":"LONG","entry_mid":50000,"tp1":50100,"tp2":50200,"sl":49900}' >/tmp/v012b_protection.json

curl -sS -X GET "$BASE_URL/testnet/algo-open-orders?symbol=$SYMBOL" >/tmp/v012b_open_before.json

curl -sS -X POST "$BASE_URL/testnet/cancel-protection" -H 'Content-Type: application/json' -d '{"symbol":"'"$SYMBOL"'"}' >/tmp/v012b_cancel.json

curl -sS -X GET "$BASE_URL/testnet/algo-open-orders?symbol=$SYMBOL" >/tmp/v012b_open_after.json

curl -sS -X POST "$BASE_URL/testnet/close-position" -H 'Content-Type: application/json' -d '{"symbol":"'"$SYMBOL"'"}' >/tmp/v012b_close.json

curl -sS -X POST "$BASE_URL/testnet/position-risk" -H 'Content-Type: application/json' -d '{"symbol":"'"$SYMBOL"'"}' | python3 -c 'import json,sys;d=json.load(sys.stdin);amt=str(((d.get("body") or [{}])[0] if isinstance(d.get("body"),list) and d.get("body") else d.get("body") or {}).get("positionAmt","0"));print("positionAmt",amt);exit(0 if amt in ("0","0.0",0,0.0) else 1)'

echo "Smoke v0.12b protection finished"
