#!/usr/bin/env python3
import json, math, os, time
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
MODEL_PATH = ARTIFACTS / "ml_gate_sklearn_v1.joblib"
REPORT_PATH = REPORTS / "ml_gate_sklearn_v1_metrics.json"

WIB = timezone(timedelta(hours=7))

def read_jsonl(path):
    out = []
    if not path.exists():
        return out
    with path.open(errors="ignore") as f:
        for line in f:
            try:
                out.append(json.loads(line))
            except Exception:
                pass
    return out

def safe_float(x, default=-1.0):
    try:
        if x is None or str(x).strip() == "":
            return default
        v = float(str(x))
        return v if math.isfinite(v) else default
    except Exception:
        return default

def signal_key(r):
    return str(r.get("signal_key") or r.get("signal_id") or "")

def norm_symbol(x):
    return str(x or "").upper().replace("BINANCE:", "").replace(".P", "")

def direction(r):
    d = str(r.get("direction") or r.get("dir") or "").upper()
    if d in ("LONG", "BUY"):
        return "LONG"
    if d in ("SHORT", "SELL"):
        return "SHORT"
    return d or "UNK"

def parse_time_wib(s):
    if not s:
        return None
    ss = str(s).replace(" WIB", "").replace("T", " ").split("+")[0]
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d"):
        try:
            return datetime.strptime(ss[:26], fmt).replace(tzinfo=WIB)
        except Exception:
            pass
    return None

def signal_ts(r):
    for k in ("signal_time_wib", "confirmed_ts_wib", "run_ts_wib", "event_time_wib", "created_at_wib"):
        dt = parse_time_wib(r.get(k))
        if dt:
            return dt.timestamp()
    try:
        return int(signal_key(r).split("|")[-1]) / 1000
    except Exception:
        return 0

def hour(r):
    dt = None
    for k in ("signal_time_wib", "confirmed_ts_wib", "run_ts_wib", "event_time_wib", "created_at_wib"):
        dt = parse_time_wib(r.get(k))
        if dt:
            return int(dt.hour)
    try:
        ms = int(signal_key(r).split("|")[-1])
        return datetime.fromtimestamp(ms / 1000, timezone.utc).astimezone(WIB).hour
    except Exception:
        return -1

def session_from_hour(h):
    if h < 0:
        return "UNK"
    if 0 <= h < 7:
        return "ASIA_EARLY"
    if 7 <= h < 13:
        return "ASIA_DAY"
    if 13 <= h < 19:
        return "LONDON"
    return "NY_LATE"

def pick(row, *keys):
    payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
    plan = row.get("plan") if isinstance(row.get("plan"), dict) else {}
    meta = row.get("meta") if isinstance(row.get("meta"), dict) else {}
    htf = row.get("htf") if isinstance(row.get("htf"), dict) else {}
    liq = row.get("liq") if isinstance(row.get("liq"), dict) else {}
    for k in keys:
        for obj in (row, payload, plan, meta, htf, liq):
            if isinstance(obj, dict) and obj.get(k) not in (None, ""):
                return obj.get(k)
    return None

def features(row):
    h = hour(row)

    score = pick(row, "score", "score_total", "priority_score", "smc_score")
    rr = pick(row, "rr", "rr_tp1", "rr_to_tp1", "rrTp2", "rr_min")
    entry_dist = pick(row, "entryDistPct", "entry_dist_pct", "entry_dist_from_price_pct", "distToZonePct")
    htf_loc = pick(row, "htfLoc", "htf_location", "location")
    htf_bias = pick(row, "htfBias", "htf_bias", "bias")
    liq_ctx = pick(row, "liqCtx", "liq_ctx", "ctx")
    mode = pick(row, "mode", "state", "status")

    return {
        "symbol": norm_symbol(pick(row, "symbol", "pair")),
        "direction": direction(row),
        "hour": float(h),
        "session": session_from_hour(h),
        "score": safe_float(score, -1.0),
        "rr": safe_float(rr, -1.0),
        "entry_dist": safe_float(entry_dist, -1.0),
        "htf_loc": str(htf_loc or "UNK"),
        "htf_bias": str(htf_bias or "UNK"),
        "liq_ctx": str(liq_ctx or "UNK"),
        "mode": str(mode or "UNK"),
    }

def latest_outcomes():
    latest = {}
    for r in read_jsonl(OUTCOMES):
        k = signal_key(r)
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
    if os.getenv("ML_SK_TRAIN_ACCEPT_ONLY", "false").lower() == "true":
        if str(row.get("execution_decision") or "").upper() != "ACCEPT":
            return False
    return label_for(row, out) in (0, 1)

def main():
    rows_raw = read_jsonl(DATASET)
    outcomes = latest_outcomes()

    latest = {}
    for r in rows_raw:
        k = signal_key(r)
        if k:
            latest[k] = r

    rows = list(latest.values())

    data = []
    for r in rows:
        out = outcomes.get(signal_key(r))
        if not include_train(r, out):
            continue
        y = label_for(r, out)
        data.append((signal_ts(r), features(r), y, signal_key(r)))

    data.sort(key=lambda x: x[0])

    if len(data) < 120:
        raise SystemExit(f"NOT_ENOUGH_TRAIN_ROWS n={len(data)}")

    X = [x[1] for x in data]
    y = [x[2] for x in data]

    cut = max(80, int(len(data) * 0.80))
    X_train, y_train = X[:cut], y[:cut]
    X_test, y_test = X[cut:], y[cut:]

    pipe = Pipeline([
        ("vec", DictVectorizer(sparse=False)),
        ("clf", LogisticRegression(
            max_iter=1000,
            C=0.75,
            solver="liblinear",
        )),
    ])

    pipe.fit(X_train, y_train)

    probs = pipe.predict_proba(X_test)
    classes = list(pipe.named_steps["clf"].classes_)
    pos_idx = classes.index(1)
    p_win = [float(p[pos_idx]) for p in probs]

    threshold = float(os.getenv("ML_GATE_MIN_P_WIN", "0.74"))
    pred = [1 if p >= threshold else 0 for p in p_win]

    test_wr_all = sum(y_test) / len(y_test) if y_test else None
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
        "model_version": "sklearn_logreg_v1",
        "trained_at_wib": datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S WIB"),
        "total_rows": len(data),
        "train_rows": len(X_train),
        "test_rows": len(X_test),
        "train_win": int(sum(y_train)),
        "train_loss": int(len(y_train) - sum(y_train)),
        "train_wr": round(sum(y_train) / len(y_train), 4),
        "test_win": int(sum(y_test)),
        "test_loss": int(len(y_test) - sum(y_test)),
        "test_wr_all": round(test_wr_all, 4) if test_wr_all is not None else None,
        "threshold": threshold,
        "accuracy_at_threshold": round(accuracy_score(y_test, pred), 4) if y_test else None,
        "auc": round(auc, 4) if auc is not None else None,
        "brier": round(brier, 4) if brier is not None else None,
        "pass_n": len(passed),
        "pass_wr": round(wr(passed), 4) if wr(passed) is not None else None,
        "block_n": len(blocked),
        "block_wr": round(wr(blocked), 4) if wr(blocked) is not None else None,
    }

    artifact = {
        "model": pipe,
        "model_version": "sklearn_logreg_v1",
        "threshold": threshold,
        "metrics": metrics,
        "feature_spec": "dict_v1",
        "classes": classes,
    }

    joblib.dump(artifact, MODEL_PATH)
    REPORT_PATH.write_text(json.dumps(metrics, indent=2))

    print("=== ML GATE SKLEARN V1 TRAINED ===")
    print(json.dumps(metrics, indent=2))
    print(f"MODEL_PATH={MODEL_PATH}")
    print(f"REPORT_PATH={REPORT_PATH}")

if __name__ == "__main__":
    main()
