#!/usr/bin/env python3
import json, math, os
from pathlib import Path
from datetime import datetime, timezone, timedelta

from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.metrics import accuracy_score, brier_score_loss, roc_auc_score
import joblib

ROOT = Path("/app") if Path("/app/logs").exists() else Path(".")
LOGS = ROOT / "logs"
ARTIFACTS = ROOT / "artifacts"
REPORTS = ROOT / "reports"
ARTIFACTS.mkdir(exist_ok=True)
REPORTS.mkdir(exist_ok=True)

DATASET = LOGS / "ml_dataset_rows.jsonl"
OUTCOMES = LOGS / "forward_outcomes.jsonl"
MODEL_PATH = ARTIFACTS / "ml_gate_sklearn_v2_clean.joblib"
REPORT_PATH = REPORTS / "ml_gate_sklearn_v2_clean_metrics.json"

WIB = timezone(timedelta(hours=7))

CAT_FEATURES = [
    "symbol_norm", "direction_norm", "priority", "mode",
    "source_mode",
    "htf_dir", "htf_bias", "htf_location", "htf_dol",
    "liq_ctx",
    "sweep_tag", "reclaim_mode", "fvg_type", "ob_type",
    "patch_phase",
]

NUM_FEATURES = [
    "score",
    "rr_to_tp1", "rr_to_tp2", "rr_to_tp3",
    "sl_dist_pct", "tp1_dist_pct", "tp2_dist_pct", "tp3_dist_pct",
    "entry_zone_width_pct",
    "liq_dist_to_zone_pct",
    "sweep_age_bars_5m",
    "reclaim_age_bars_5m",
    "fvg_size_pct",
    "fvg_age_bars_5m",
    "ob_size_pct",
]

def read_jsonl(path):
    out = []
    if not path.exists():
        return out
    for line in path.read_text(errors="ignore").splitlines():
        try:
            out.append(json.loads(line))
        except Exception:
            pass
    return out

def key(r):
    return str(r.get("signal_key") or r.get("signal_id") or "")

def fnum(x, default=-1.0):
    try:
        if x is None or str(x).strip() == "":
            return default
        v = float(str(x))
        return v if math.isfinite(v) else default
    except Exception:
        return default

def parse_wib(s):
    if not s:
        return None
    ss = str(s).replace(" WIB", "").replace("T", " ").split("+")[0]
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(ss[:26], fmt).replace(tzinfo=WIB)
        except Exception:
            pass
    return None

def signal_ts(r):
    dt = parse_wib(r.get("signal_time_wib"))
    if dt:
        return dt.timestamp()
    try:
        return int(key(r).split("|")[-1]) / 1000
    except Exception:
        return 0

def latest_outcomes():
    latest = {}
    for r in read_jsonl(OUTCOMES):
        k = key(r)
        if k:
            latest[k] = r
    return latest

def label_for(row, out):
    if out:
        lw = out.get("label_win")
        if lw in (0, 1):
            return int(lw)
        target = str(out.get("label_target") or "").upper()
        if target in ("TP1", "TP2", "TP3"):
            return 1
        if target == "SL":
            return 0
    lw = row.get("label_win")
    if lw in (0, 1):
        return int(lw)
    return None

def include_train(row, out):
    if row.get("include_ml") is False:
        return False
    if out and out.get("include_ml_label") is False:
        return False
    if row.get("ml_feature_version") != 2:
        return False
    return label_for(row, out) in (0, 1)

def norm_cat(v):
    if v in (None, "", -1, -1.0):
        return "UNK"
    return str(v)

def features(row):
    x = {}

    for k in CAT_FEATURES:
        x[k] = norm_cat(row.get(k))

    for k in NUM_FEATURES:
        x[k] = fnum(row.get(k))

    rr = fnum(row.get("rr_to_tp1"))
    sl = fnum(row.get("sl_dist_pct"))
    fvg = fnum(row.get("fvg_size_pct"))

    x["rr_bucket"] = (
        "rr<0.7" if rr < 0.7 else
        "0.7-0.9" if rr < 0.9 else
        "0.9-1.2" if rr < 1.2 else
        "1.2+"
    )

    x["sl_bucket"] = (
        "sl<0.15" if sl < 0.15 else
        "0.15-0.35" if sl < 0.35 else
        "0.35-0.70" if sl < 0.70 else
        "0.70+"
    )

    x["fvg_bucket"] = (
        "fvgNA" if fvg < 0 else
        "fvg<0.10" if fvg < 0.10 else
        "0.10-0.30" if fvg < 0.30 else
        "0.30+"
    )

    return x

def main():
    rows_raw = read_jsonl(DATASET)
    outcomes = latest_outcomes()

    latest = {}
    for r in rows_raw:
        k = key(r)
        if k:
            latest[k] = r

    data = []
    for r in latest.values():
        out = outcomes.get(key(r))
        if not include_train(r, out):
            continue
        y = label_for(r, out)
        data.append((signal_ts(r), features(r), y, key(r)))

    data.sort(key=lambda x: x[0])

    if len(data) < 120:
        raise SystemExit(f"NOT_ENOUGH_V2_ROWS n={len(data)}")

    X = [x[1] for x in data]
    y = [x[2] for x in data]

    cut = max(80, int(len(data) * 0.80))
    X_train, y_train = X[:cut], y[:cut]
    X_test, y_test = X[cut:], y[cut:]

    pipe = Pipeline([
        ("vec", DictVectorizer(sparse=False)),
        ("clf", LogisticRegression(max_iter=1200, C=0.65, solver="liblinear")),
    ])

    pipe.fit(X_train, y_train)

    probs = pipe.predict_proba(X_test)
    classes = list(pipe.named_steps["clf"].classes_)
    pos_idx = classes.index(1)
    p_win = [float(p[pos_idx]) for p in probs]

    threshold = float(os.getenv("ML_GATE_MIN_P_WIN", "0.74"))
    pred = [1 if p >= threshold else 0 for p in p_win]

    passed = [(p, yy) for p, yy in zip(p_win, y_test) if p >= threshold]
    blocked = [(p, yy) for p, yy in zip(p_win, y_test) if p < threshold]

    def wr(xs):
        if not xs:
            return None
        return sum(yy for _, yy in xs) / len(xs)

    try:
        auc = roc_auc_score(y_test, p_win) if len(set(y_test)) > 1 else None
    except Exception:
        auc = None

    try:
        brier = brier_score_loss(y_test, p_win)
    except Exception:
        brier = None

    metrics = {
        "model_version": "sklearn_logreg_v2_clean",
        "trained_at_wib": datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S WIB"),
        "total_rows": len(data),
        "train_rows": len(X_train),
        "test_rows": len(X_test),
        "train_win": int(sum(y_train)),
        "train_loss": int(len(y_train) - sum(y_train)),
        "train_wr": round(sum(y_train) / len(y_train), 4),
        "test_win": int(sum(y_test)),
        "test_loss": int(len(y_test) - sum(y_test)),
        "test_wr_all": round(sum(y_test) / len(y_test), 4),
        "threshold": threshold,
        "accuracy_at_threshold": round(accuracy_score(y_test, pred), 4),
        "auc": round(auc, 4) if auc is not None else None,
        "brier": round(brier, 4) if brier is not None else None,
        "pass_n": len(passed),
        "pass_wr": round(wr(passed), 4) if wr(passed) is not None else None,
        "block_n": len(blocked),
        "block_wr": round(wr(blocked), 4) if wr(blocked) is not None else None,
        "cat_features": CAT_FEATURES,
        "num_features": NUM_FEATURES,
    }

    artifact = {
        "model": pipe,
        "model_version": "sklearn_logreg_v2_clean",
        "threshold": threshold,
        "metrics": metrics,
        "feature_spec": "dict_v2",
        "classes": classes,
    }

    joblib.dump(artifact, MODEL_PATH)
    REPORT_PATH.write_text(json.dumps(metrics, indent=2))

    print("=== ML GATE SKLEARN V2 TRAINED ===")
    print(json.dumps(metrics, indent=2))
    print(f"MODEL_PATH={MODEL_PATH}")
    print(f"REPORT_PATH={REPORT_PATH}")

if __name__ == "__main__":
    main()
