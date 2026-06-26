#!/usr/bin/env python3
import json
import os
import subprocess
import importlib.util
from pathlib import Path
from collections import Counter
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]

DATASET = ROOT / "logs" / "ml_dataset_v4_current14_candidate_join.jsonl"
OFFLINE_SCRIPT = ROOT / "scripts" / "ml_challenger_v4_offline.py"

READINESS_REPORT = ROOT / "reports" / "ml_challenger_v4_readiness_monitor.json"
TRAIN_REPORT = ROOT / "reports" / "ml_challenger_v4_offline_report.json"
DAILY_REPORT = ROOT / "reports" / "ml_challenger_v4_daily_job.json"

REGISTRY = ROOT / "state" / "ml_challenger_v4_registry.json"

CURRENT14 = {
    "BTCUSDT","ETHUSDT","SOLUSDT","PAXGUSDT","HYPEUSDT","XRPUSDT","ZECUSDT",
    "UNIUSDT","ADAUSDT","BCHUSDT","LINKUSDT","SUIUSDT","LTCUSDT","AVAXUSDT"
}

MIN_NEW_TRAINABLE = int(os.getenv("ML_CHALLENGER_MIN_NEW_TRAINABLE", "30"))
MIN_NEW_LOSSES = int(os.getenv("ML_CHALLENGER_MIN_NEW_LOSSES", "8"))
MIN_FEATURES = int(os.getenv("ML_CHALLENGER_MIN_FEATURES", "10"))

AUTO_TRAIN = str(os.getenv("ML_AUTO_TRAIN_ENABLED", "true")).lower() in ("1", "true", "yes", "on")
AUTO_REGISTER_SHADOW = str(os.getenv("ML_AUTO_REGISTER_SHADOW", "true")).lower() in ("1", "true", "yes", "on")
AUTO_PROMOTE_LIVE = str(os.getenv("ML_AUTO_PROMOTE_LIVE", "false")).lower() in ("1", "true", "yes", "on")

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def package_ok(name):
    return importlib.util.find_spec(name) is not None

def read_jsonl(path):
    out = []
    if not path.exists():
        return out
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            try:
                x = json.loads(line)
                if isinstance(x, dict):
                    out.append(x)
            except Exception:
                pass
    return out

def read_json(path, default=None):
    try:
        return json.loads(path.read_text())
    except Exception:
        return default if default is not None else {}

def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False, default=str))

def truthy(v):
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")

def norm_symbol(x):
    s = str(x or "").upper().replace("BINANCE:", "").replace(".P", "").replace("/", "").replace("-", "").strip()
    if "|" in s:
        s = s.split("|", 1)[0]
    return s

def signal_time(row):
    for k in ("signal_time_ms", "confirmed_bucket_ms", "timestamp_ms", "created_at_ms"):
        try:
            v = row.get(k)
            if v is not None and str(v).strip() != "":
                return int(float(v))
        except Exception:
            pass
    try:
        tail = str(row.get("signal_key") or "").split("|")[-1]
        if tail.isdigit():
            return int(tail)
    except Exception:
        pass
    return 0

def label_win(row):
    x = row.get("label_win")
    if isinstance(x, bool):
        return int(x)
    s = str(x).strip().lower()
    if s in ("1", "1.0", "true", "yes", "win"):
        return 1
    if s in ("0", "0.0", "false", "no", "loss"):
        return 0
    target = str(row.get("label_target") or row.get("outcome_status") or "").strip().upper()
    if target in ("TP1", "TP2", "TP3"):
        return 1
    if target == "SL":
        return 0
    return None

def build_readiness():
    rows = read_jsonl(DATASET)
    base = read_json(TRAIN_REPORT, {})

    trainable = []
    for r in rows:
        y = label_win(r)
        if truthy(r.get("trainable_label")) and y is not None:
            rr = dict(r)
            rr["_y"] = y
            rr["_t"] = signal_time(r)
            trainable.append(rr)

    features = sorted({
        k
        for r in trainable
        for k in r.keys()
        if str(k).startswith(("sigf_", "fs_"))
    })

    symbols = Counter(norm_symbol(r.get("symbol") or r.get("pair")) for r in trainable)
    symbols.pop("", None)

    wins = sum(1 for r in trainable if r["_y"] == 1)
    losses = sum(1 for r in trainable if r["_y"] == 0)

    last_train_test_max = (((base.get("split") or {}).get("test_time_max")) or 0)
    new_rows = [r for r in trainable if int(r.get("_t") or 0) > int(last_train_test_max or 0)]
    new_wins = sum(1 for r in new_rows if r["_y"] == 1)
    new_losses = sum(1 for r in new_rows if r["_y"] == 0)

    missing_current14 = sorted(CURRENT14 - set(symbols.keys()))

    reasons = []
    if len(new_rows) < MIN_NEW_TRAINABLE:
        reasons.append(f"new_trainable_below_{MIN_NEW_TRAINABLE}:{len(new_rows)}")
    if new_losses < MIN_NEW_LOSSES:
        reasons.append(f"new_losses_below_{MIN_NEW_LOSSES}:{new_losses}")
    if len(features) < MIN_FEATURES:
        reasons.append(f"feature_count_below_{MIN_FEATURES}:{len(features)}")
    if missing_current14:
        reasons.append("missing_current14:" + ",".join(missing_current14))

    state = "RETRAIN_READY" if not reasons else "WAIT_MORE_LABELS"

    out = {
        "ok": True,
        "created_at_utc": now_iso(),
        "state": state,
        "dataset_path": str(DATASET),
        "rows_raw": len(rows),
        "trainable_rows": len(trainable),
        "wins": wins,
        "losses": losses,
        "win_rate": wins / len(trainable) if trainable else None,
        "feature_count": len(features),
        "symbols_present": dict(symbols),
        "missing_current14": missing_current14,
        "last_train_test_time_max": last_train_test_max,
        "new_trainable_since_last_train": len(new_rows),
        "new_wins_since_last_train": new_wins,
        "new_losses_since_last_train": new_losses,
        "retrain_reasons": reasons,
        "recommendation": "RETRAIN_CHALLENGER" if state == "RETRAIN_READY" else "MONITOR_ONLY_NO_RETRAIN",
        "optional_packages": {
            "xgboost": package_ok("xgboost"),
            "lightgbm": package_ok("lightgbm"),
            "sklearn": package_ok("sklearn"),
            "joblib": package_ok("joblib"),
            "numpy": package_ok("numpy"),
        },
    }
    write_json(READINESS_REPORT, out)
    return out

def run_train():
    if not OFFLINE_SCRIPT.exists():
        return {"ok": False, "reason": "offline_script_missing", "path": str(OFFLINE_SCRIPT)}

    p = subprocess.run(
        ["python", str(OFFLINE_SCRIPT)],
        cwd=str(ROOT),
        text=True,
        capture_output=True,
        timeout=600,
    )
    return {
        "ok": p.returncode == 0,
        "returncode": p.returncode,
        "stdout_tail": "\n".join((p.stdout or "").splitlines()[-80:]),
        "stderr_tail": "\n".join((p.stderr or "").splitlines()[-80:]),
    }

def best_ready_model(train_report):
    ready = list(train_report.get("ready_models") or [])
    if not ready:
        return None
    ready.sort(key=lambda x: (
        float(x.get("precision_074") or 0),
        float(x.get("coverage_074") or 0),
        float(x.get("auc") or 0),
    ), reverse=True)
    return ready[0]

def update_registry(readiness, train_report, train_result):
    registry = read_json(REGISTRY, {})
    if not isinstance(registry, dict):
        registry = {}

    best = best_ready_model(train_report)
    registry["updated_at_utc"] = now_iso()
    registry["phase"] = "PHASE_4_ML_CHALLENGER"
    registry["auto_train_enabled"] = AUTO_TRAIN
    registry["auto_register_shadow"] = AUTO_REGISTER_SHADOW
    registry["auto_promote_live"] = False  # intentionally hard-false for now
    registry["last_readiness"] = readiness
    registry["last_train_result"] = train_result

    if AUTO_REGISTER_SHADOW and best:
        registry["shadow_model"] = {
            "enabled": True,
            "mode": "SHADOW_ONLY",
            "model": best.get("model"),
            "artifact": best.get("artifact"),
            "threshold_primary": 0.74,
            "thresholds_observed": [0.70, 0.74, 0.80],
            "precision_074": best.get("precision_074"),
            "coverage_074": best.get("coverage_074"),
            "edge_vs_baseline": best.get("edge_vs_baseline"),
            "auc": best.get("auc"),
            "registered_at_utc": now_iso(),
            "note": "Shadow only. Does not modify live ML_GATE_MODEL_PATH.",
        }

    registry["live_model"] = {
        "changed_by_daily_job": False,
        "note": "Live promotion blocked by policy. Manual GO required later.",
    }

    write_json(REGISTRY, registry)
    return registry

def main():
    daily = {
        "ok": True,
        "created_at_utc": now_iso(),
        "version": "ml_challenger_v4_daily_job_20260621",
        "policy": {
            "auto_train": AUTO_TRAIN,
            "auto_register_shadow": AUTO_REGISTER_SHADOW,
            "auto_promote_live_requested": AUTO_PROMOTE_LIVE,
            "auto_promote_live_effective": False,
        },
    }

    readiness = build_readiness()
    daily["readiness"] = readiness

    train_result = {"ok": True, "skipped": True, "reason": "readiness_not_ready_or_auto_train_disabled"}
    train_report = read_json(TRAIN_REPORT, {})

    if AUTO_TRAIN and readiness.get("state") == "RETRAIN_READY":
        train_result = run_train()
        train_report = read_json(TRAIN_REPORT, {})

    daily["train_result"] = train_result
    daily["train_report_state"] = train_report.get("state")
    daily["train_report_recommendation"] = train_report.get("recommendation")
    daily["ready_models"] = train_report.get("ready_models") or []

    registry = update_registry(readiness, train_report, train_result)
    daily["registry_shadow_model"] = registry.get("shadow_model")
    daily["registry_path"] = str(REGISTRY)

    write_json(DAILY_REPORT, daily)

    print("=== ML CHALLENGER V4 DAILY JOB ===")
    print("readiness:", readiness.get("state"), readiness.get("recommendation"))
    print("new_trainable:", readiness.get("new_trainable_since_last_train"), "new_losses:", readiness.get("new_losses_since_last_train"))
    print("train_result:", train_result.get("ok"), train_result.get("skipped"), train_result.get("reason"))
    print("train_report_state:", daily.get("train_report_state"))
    print("shadow_model:", (registry.get("shadow_model") or {}).get("model"))
    print("daily_report:", DAILY_REPORT)
    print("registry:", REGISTRY)

if __name__ == "__main__":
    main()
