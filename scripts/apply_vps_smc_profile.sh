#!/usr/bin/env bash
set -euo pipefail

PROFILE="${1:-SEMI}"
PROFILE="$(echo "$PROFILE" | tr '[:lower:]' '[:upper:]')"

ENV_FILE=".env"

if [ ! -f "$ENV_FILE" ]; then
  echo "ERROR: .env not found"
  exit 1
fi

cp "$ENV_FILE" ".env.backup.vps_smc_profile.$(date +%Y%m%d_%H%M%S)"

python3 - "$PROFILE" <<'PY'
from pathlib import Path
import sys

profile = sys.argv[1].upper()
p = Path(".env")
s = p.read_text()

BASE = {
    # ===== source/routing; jangan dimatiin kalau VPS mau jadi primary =====
    "SIGNAL_SOURCE_MODE": "VPS_SMC_PRIMARY",
    "APPS_SCRIPT_SIGNAL_MODE": "BACKUP_COMPARE_ONLY",
    "VPS_SMC_EXECUTION_ENABLED": "true",
    "VPS_SMC_COMPETITOR_MODE": "PRODUCTION_SIGNAL",

    # ===== scheduler =====
    "VPS_SMC_SCHEDULER_ENABLED": "true",
    "VPS_SMC_SCHEDULER_INTERVAL_SEC": "60",

    # ===== universe =====
    "VPS_SMC_MAX_SYMBOLS_PER_RUN": "14",

    # ===== timeframe parity =====
    "VPS_SMC_INTERVAL_HTF": "4h",
    "VPS_SMC_INTERVAL_ENTRY": "15m",
    "VPS_SMC_INTERVAL_STAGEB": "5m",

    # ===== fixed shared settings =====
    "VPS_SMC_OB_ENABLED": "true",
    "VPS_SMC_OB_LOOKBACK": "14",
    "VPS_SMC_INVALID_BUFFER_PCT": "0.08",
    "VPS_SMC_FEES_BUFFER_PCT": "0.03",
    "VPS_SMC_RR_MIN_TP2": "0.95",

    # Candle store retention: keep enough 4H history for HTF structure.
    "BINANCE_CANDLE_RETENTION_DAYS": "90",
    "BINANCE_CANDLE_BOOTSTRAP_LIMIT": "500",
}

PROFILES = {
    # More signal. Masih SMC, tapi filter liquidity + displacement lebih longgar.
    "LOOSE": {
        # Moderate loose: lebih produktif dari SEMI, tapi tidak liar.
        "VPS_SMC_PROFILE": "LOOSE",
        "VPS_SMC_SCORE_MIN": "68",

        # Tetap require liquidity context, cuma near/liquidity tolerance sedikit dilonggarkan.
        "VPS_SMC_REQUIRE_AT_OR_NEAR_LIQ": "true",
        "VPS_SMC_NEAR_LIQ_PCT": "0.15",
        "VPS_SMC_CANDIDATE_MAX_DIST_PCT": "1.50",
        "VPS_SMC_CANDIDATE_BETWEEN_NEAR_PCT": "0.70",
        "VPS_SMC_CANDIDATE_INCLUDE_BETWEEN": "true",

        # Tetap allow relax reclaim seperti SEMI, tapi kasih umur reclaim sedikit lebih panjang.
        "VPS_SMC_SWEEP_RECLAIM_STRICT": "true",
        "VPS_SMC_RECLAIM_RELAX_MODE": "true",
        "VPS_SMC_MAX_RECLAIM_AGE_BARS_5M": "9",
        "VPS_SMC_SWEEP_LOOKBACK_BARS_5M": "22",
        "VPS_SMC_SWEEP_MAX_AGE_BARS_5M": "60",

        # Displacement sedikit lebih longgar dari SEMI, tapi masih butuh candle impulsive.
        "VPS_SMC_DISPLACEMENT_ATR_LEN": "10",
        "VPS_SMC_DISPLACEMENT_ATR_MULT": "1.08",
        "VPS_SMC_DISPLACEMENT_MIN_BODY_PCT": "50",

        # Retest window sedikit lebih longgar, FVG lookback agak diperluas.
        "VPS_SMC_RETEST_MAX_AGE_BARS_5M": "20",
        "VPS_SMC_FVG_LOOKBACK_BARS_5M": "40",
        "VPS_SMC_ENTRY_MAX_DIST_FROM_PRICE_PCT": "0.70",
    },

    # Default: paling dekat sama config Apps Script core aktif.
    "SEMI": {
        "VPS_SMC_PROFILE": "SEMI",
        "VPS_SMC_SCORE_MIN": "70",

        "VPS_SMC_REQUIRE_AT_OR_NEAR_LIQ": "true",
        "VPS_SMC_NEAR_LIQ_PCT": "0.12",
        "VPS_SMC_CANDIDATE_MAX_DIST_PCT": "1.50",
        "VPS_SMC_CANDIDATE_BETWEEN_NEAR_PCT": "0.70",
        "VPS_SMC_CANDIDATE_INCLUDE_BETWEEN": "true",

        "VPS_SMC_SWEEP_RECLAIM_STRICT": "true",
        "VPS_SMC_RECLAIM_RELAX_MODE": "true",
        "VPS_SMC_MAX_RECLAIM_AGE_BARS_5M": "8",
        "VPS_SMC_SWEEP_LOOKBACK_BARS_5M": "20",
        "VPS_SMC_SWEEP_MAX_AGE_BARS_5M": "60",

        "VPS_SMC_DISPLACEMENT_ATR_LEN": "10",
        "VPS_SMC_DISPLACEMENT_ATR_MULT": "1.15",
        "VPS_SMC_DISPLACEMENT_MIN_BODY_PCT": "55",

        "VPS_SMC_RETEST_MAX_AGE_BARS_5M": "18",
        "VPS_SMC_FVG_LOOKBACK_BARS_5M": "35",
        "VPS_SMC_ENTRY_MAX_DIST_FROM_PRICE_PCT": "0.60",
    },

    # Conservative. Lebih deket ke risk-profile STRICT.
    "STRICT": {
        "VPS_SMC_PROFILE": "STRICT",
        "VPS_SMC_SCORE_MIN": "75",

        "VPS_SMC_REQUIRE_AT_OR_NEAR_LIQ": "true",
        "VPS_SMC_NEAR_LIQ_PCT": "0.10",
        "VPS_SMC_CANDIDATE_MAX_DIST_PCT": "0.35",
        "VPS_SMC_CANDIDATE_BETWEEN_NEAR_PCT": "0.18",
        "VPS_SMC_CANDIDATE_INCLUDE_BETWEEN": "true",

        "VPS_SMC_SWEEP_RECLAIM_STRICT": "true",
        "VPS_SMC_RECLAIM_RELAX_MODE": "false",
        "VPS_SMC_MAX_RECLAIM_AGE_BARS_5M": "6",
        "VPS_SMC_SWEEP_LOOKBACK_BARS_5M": "16",
        "VPS_SMC_SWEEP_MAX_AGE_BARS_5M": "8",

        "VPS_SMC_DISPLACEMENT_ATR_LEN": "10",
        "VPS_SMC_DISPLACEMENT_ATR_MULT": "1.30",
        "VPS_SMC_DISPLACEMENT_MIN_BODY_PCT": "60",

        "VPS_SMC_RETEST_MAX_AGE_BARS_5M": "14",
        "VPS_SMC_FVG_LOOKBACK_BARS_5M": "28",
        "VPS_SMC_ENTRY_MAX_DIST_FROM_PRICE_PCT": "0.45",
    },
}

if profile not in PROFILES:
    raise SystemExit("Unknown profile. Use: LOOSE, SEMI, STRICT")

updates = {}
updates.update(BASE)
updates.update(PROFILES[profile])

# remove old experimental keys, jangan kepake diam-diam
remove_keys = {
    "VPS_SMC_RETEST_CONFIRM_MODE",
    "VPS_SMC_RETEST_MID_BUFFER_PCT",
    "VPS_SMC_DISPLACEMENT_INCLUDE_RECLAIM_CANDLE",
    "VPS_SMC_FVG_PREFER_PRE_DISPLACEMENT",
}

out = []
seen = set()

for line in s.splitlines():
    raw = line.strip()

    if "\\n" in raw:
        continue

    if not raw or raw.startswith("#") or "=" not in raw:
        out.append(line)
        continue

    k = raw.split("=", 1)[0].strip()

    if k in remove_keys:
        continue

    if k in updates:
        out.append(f"{k}={updates[k]}")
        seen.add(k)
    else:
        out.append(line)

for k, v in updates.items():
    if k not in seen:
        out.append(f"{k}={v}")

p.write_text("\n".join(out).rstrip() + "\n")
print(f"OK applied VPS_SMC_PROFILE={profile}")
for k in sorted(updates):
    if k.startswith("VPS_SMC_"):
        print(f"{k}={updates[k]}")
PY

echo
echo "Recreating Docker container..."
docker compose up -d --force-recreate bot

echo
echo "Active profile in container:"
docker exec ai-trading-bot printenv | grep -E 'VPS_SMC_PROFILE|VPS_SMC_SCORE_MIN|VPS_SMC_CANDIDATE_MAX_DIST_PCT|VPS_SMC_CANDIDATE_BETWEEN_NEAR_PCT|VPS_SMC_REQUIRE_AT_OR_NEAR_LIQ|VPS_SMC_DISPLACEMENT_ATR_MULT|VPS_SMC_RETEST_MAX_AGE_BARS_5M|VPS_SMC_FVG_LOOKBACK_BARS_5M' || true
