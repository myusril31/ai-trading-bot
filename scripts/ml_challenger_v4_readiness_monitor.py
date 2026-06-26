#!/usr/bin/env python3
import json
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parents[1]

DATASET = ROOT / "logs" / "ml_dataset_v4_current14_candidate_join.jsonl"
BASE_REPORT = ROOT / "reports" / "ml_challenger_v4_offline_report.json"
OUT = ROOT / "reports" / "ml_challenger_v4_readiness_monitor.json"

CURRENT14 = {
    "BTCUSDT","ETHUSDT","SOLUSDT","PAXGUSDT","HYPEUSDT","XRPUSDT","ZECUSDT",
    "UNIUSDT","ADAUSDT","BCHUSDT","LINKUSDT","SUIUSDT","LTCUSDT","AVAXUSDT"
}

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

def read_json(path):
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}

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

rows = read_jsonl(DATASET)
base = read_json(BASE_REPORT)

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
    for k, v in r.items()
    if str(k).startswith(("sigf_", "fs_"))
})

symbols = Counter(norm_symbol(r.get("symbol") or r.get("pair")) for r in trainable)
symbols.pop("", None)

target_counts = Counter(str(r.get("label_target") or r.get("outcome_status") or ("WIN" if r["_y"] else "LOSS")).upper() for r in trainable)
wins = sum(1 for r in trainable if r["_y"] == 1)
losses = sum(1 for r in trainable if r["_y"] == 0)

last_train_test_max = (((base.get("split") or {}).get("test_time_max")) or 0)
new_rows = [r for r in trainable if int(r.get("_t") or 0) > int(last_train_test_max or 0)]
new_wins = sum(1 for r in new_rows if r["_y"] == 1)
new_losses = sum(1 for r in new_rows if r["_y"] == 0)

missing_current14 = sorted(CURRENT14 - set(symbols.keys()))

retrain_reasons = []
if len(new_rows) < 30:
    retrain_reasons.append(f"new_trainable_below_30:{len(new_rows)}")
if new_losses < 8:
    retrain_reasons.append(f"new_losses_below_8:{new_losses}")
if len(features) < 10:
    retrain_reasons.append(f"feature_count_low:{len(features)}")
if missing_current14:
    retrain_reasons.append("missing_current14:" + ",".join(missing_current14))

state = "RETRAIN_READY" if not retrain_reasons else "WAIT_MORE_LABELS"

out = {
    "ok": True,
    "state": state,
    "dataset_path": str(DATASET),
    "rows_raw": len(rows),
    "trainable_rows": len(trainable),
    "wins": wins,
    "losses": losses,
    "win_rate": wins / len(trainable) if trainable else None,
    "feature_count": len(features),
    "feature_keys": features,
    "symbols_present": dict(symbols),
    "missing_current14": missing_current14,
    "target_counts": dict(target_counts),
    "last_train_test_time_max": last_train_test_max,
    "new_trainable_since_last_train": len(new_rows),
    "new_wins_since_last_train": new_wins,
    "new_losses_since_last_train": new_losses,
    "retrain_reasons": retrain_reasons,
    "recommendation": "RETRAIN_CHALLENGER" if state == "RETRAIN_READY" else "MONITOR_ONLY_NO_RETRAIN",
    "note": "Readiness monitor only. Does not train or deploy.",
}

OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text(json.dumps(out, indent=2, ensure_ascii=False))

print("=== ML CHALLENGER V4 READINESS MONITOR ===")
print("state:", state)
print("trainable:", len(trainable), "wins:", wins, "losses:", losses, "features:", len(features))
print("new_trainable_since_last_train:", len(new_rows), "new_wins:", new_wins, "new_losses:", new_losses)
print("recommendation:", out["recommendation"])
print("reasons:", retrain_reasons)
print("out:", OUT)
