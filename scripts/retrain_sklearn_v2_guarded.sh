#!/usr/bin/env bash
set -Eeuo pipefail

cd /home/ubuntu/ai-trading-vps-bot

TS="$(date +%Y%m%d_%H%M%S)"
MODEL="artifacts/ml_gate_sklearn_v2_clean.joblib"
REPORT="reports/ml_gate_sklearn_v2_clean_metrics.json"
TRAINER="scripts/train_ml_gate_sklearn_v2_clean.py"
LOG="logs/retrain_sklearn_v2_${TS}.log"

mkdir -p artifacts/archive reports/archive logs

echo "=== SKLEARN V2 GUARDED RETRAIN ==="
echo "ts=$TS"
echo "pwd=$(pwd)"

if [ ! -f "$TRAINER" ]; then
  echo "ERROR: missing trainer: $TRAINER"
  echo "Available ML/train scripts:"
  find scripts -maxdepth 1 -type f | egrep -i 'train|sklearn|ml_gate|dataset|join' || true
  exit 10
fi

echo "=== Current env ==="
grep -nE '^ML_GATE_|^ML_MODEL_' .env || true

echo "=== Backup current model/report ==="
if [ -f "$MODEL" ]; then
  cp "$MODEL" "artifacts/archive/ml_gate_sklearn_v2_clean_${TS}.joblib"
  echo "model_backup=artifacts/archive/ml_gate_sklearn_v2_clean_${TS}.joblib"
else
  echo "WARN: current model not found: $MODEL"
fi

if [ -f "$REPORT" ]; then
  cp "$REPORT" "reports/archive/ml_gate_sklearn_v2_clean_metrics_${TS}.json"
  echo "report_backup=reports/archive/ml_gate_sklearn_v2_clean_metrics_${TS}.json"
else
  echo "WARN: current report not found: $REPORT"
fi

echo "=== Pause SMC scheduler briefly ==="
sudo systemctl stop vps-smc-scheduler-loop.service || true

echo "=== Rebuild feature/join inputs ==="
if [ -f scripts/recalc_score_v2_shadow_v1.py ]; then
  python3 scripts/recalc_score_v2_shadow_v1.py || true
fi

if [ -f scripts/join_feature_store_to_signals_v1.py ]; then
  python3 scripts/join_feature_store_to_signals_v1.py || true
fi

if [ -f scripts/lookahead_recursive_audit_v1.py ]; then
  python3 scripts/lookahead_recursive_audit_v1.py || true
fi

echo "=== Dataset quick count ==="
wc -l logs/ml_dataset_v3_feature_join.jsonl 2>/dev/null || true
cat reports/ml_dataset_v3_feature_join_report.json 2>/dev/null | jq '{joined_rows,joined_with_ml,joined_with_outcome,no_feature_or_too_new,bad_or_stale_feature}' || true
cat reports/lookahead_recursive_audit_v1.json 2>/dev/null | jq '{status,join_future_feature_count,join_stale_feature_count,feature_future_candle_count,recursive_plan_drift_count}' || true

echo "=== Train sklearn v2 clean ==="
python3 "$TRAINER" 2>&1 | tee "$LOG"

echo "=== Validate metric gates ==="
python3 - <<'PY'
import json, sys
from pathlib import Path

report_path = Path("reports/ml_gate_sklearn_v2_clean_metrics.json")
if not report_path.exists():
    print("METRIC_FAIL missing metrics report")
    sys.exit(20)

j = json.loads(report_path.read_text(errors="ignore"))

def f(k, default=0.0):
    try:
        return float(j.get(k, default) or 0.0)
    except Exception:
        return default

def i(k, default=0):
    try:
        return int(j.get(k, default) or 0)
    except Exception:
        return default

gates = [
    ("total_rows >= 300", i("total_rows") >= 300, i("total_rows")),
    ("test_wr_all >= 0.78", f("test_wr_all") >= 0.78, f("test_wr_all")),
    ("pass_n >= 20", i("pass_n") >= 20, i("pass_n")),
    ("pass_wr >= 0.80", f("pass_wr") >= 0.80, f("pass_wr")),
    ("auc >= 0.58", f("auc") >= 0.58, f("auc")),
    ("brier <= 0.22", f("brier", 999) <= 0.22, f("brier", 999)),
    ("pass_wr >= block_wr", f("pass_wr") >= f("block_wr"), {"pass_wr": f("pass_wr"), "block_wr": f("block_wr")}),
]

print(json.dumps({
    "model_version": j.get("model_version"),
    "total_rows": j.get("total_rows"),
    "train_rows": j.get("train_rows"),
    "test_rows": j.get("test_rows"),
    "test_wr_all": j.get("test_wr_all"),
    "pass_n": j.get("pass_n"),
    "pass_wr": j.get("pass_wr"),
    "block_n": j.get("block_n"),
    "block_wr": j.get("block_wr"),
    "auc": j.get("auc"),
    "brier": j.get("brier"),
    "gates": [{"name": name, "ok": ok, "value": val} for name, ok, val in gates],
}, indent=2))

bad = [name for name, ok, _ in gates if not ok]
if bad:
    print("METRIC_FAIL", bad)
    sys.exit(21)

print("METRIC_PASS")
PY

echo "=== Patch env to sklearn v2 clean ==="
python3 - <<'PY'
from pathlib import Path
from datetime import datetime

p = Path(".env")
txt = p.read_text(errors="ignore") if p.exists() else ""
backup = Path(f".env.bak_sklearn_retrain_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
backup.write_text(txt)

sets = {
    "ML_GATE_ENABLED": "true",
    "ML_GATE_MODE": "LIVE_BLOCK",
    "ML_GATE_MODEL": "SKLEARN_V2",
    "ML_GATE_MODEL_PATH": "artifacts/ml_gate_sklearn_v2_clean.joblib",
    "ML_GATE_MIN_P_WIN": "0.74",
    "ML_GATE_ACTION": "NO_TRADE",
}

out = []
seen = set()

for line in txt.splitlines():
    if "=" in line and not line.strip().startswith("#"):
        k = line.split("=", 1)[0].strip()
        if k in sets:
            out.append(f"{k}={sets[k]}")
            seen.add(k)
        else:
            out.append(line)
    else:
        out.append(line)

for k, v in sets.items():
    if k not in seen:
        out.append(f"{k}={v}")

p.write_text("\n".join(out) + "\n")
print("ENV_PATCHED", backup)
PY

echo "=== Restart app/scheduler so model reloads cleanly ==="
docker restart ai-trading-bot
sleep 10

sudo systemctl restart vps-smc-scheduler-loop.service || true

echo "=== Final verify ==="
grep -nE '^ML_GATE_|^ML_MODEL_' .env || true

cat reports/ml_gate_sklearn_v2_clean_metrics.json \
  | jq '{model_version,total_rows,train_rows,test_rows,test_wr_all,accuracy_at_threshold,auc,brier,pass_n,pass_wr,block_n,block_wr}'

docker logs ai-trading-bot --tail=80 | egrep 'ml_gate|sklearn|Started|dashboard|ERROR|Traceback' || true
systemctl status vps-smc-scheduler-loop.service --no-pager || true

echo "=== SKLEARN V2 RETRAIN DONE ==="
