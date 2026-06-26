#!/usr/bin/env python3
import json
import math
import csv
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parents[1]

DATASET = Path(
    __import__("os").getenv(
        "ML_DATASET_V4_PATH",
        str(ROOT / "logs" / "ml_dataset_v4_current14_candidate_join.jsonl")
    )
)
if not DATASET.exists():
    DATASET = ROOT / "logs" / "ml_dataset_v4_candidate_join.jsonl"

REPORT_JSON = ROOT / "reports" / "ml_challenger_v4_offline_report.json"
REPORT_CSV = ROOT / "reports" / "ml_challenger_v4_thresholds.csv"
ART_DIR = ROOT / "artifacts"
ART_DIR.mkdir(parents=True, exist_ok=True)

VERSION = "ml_challenger_v4_offline_20260621"

THRESHOLDS = [0.50, 0.55, 0.60, 0.65, 0.70, 0.74, 0.78, 0.80, 0.85]
TRAIN_FRAC = 0.70

def read_jsonl(path):
    out = []
    if not path.exists():
        return out
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    out.append(obj)
            except Exception:
                pass
    return out

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

def is_num(v):
    if isinstance(v, bool):
        return True
    try:
        x = float(v)
        return math.isfinite(x)
    except Exception:
        return False

def to_float(v):
    if isinstance(v, bool):
        return 1.0 if v else 0.0
    try:
        x = float(v)
        return x if math.isfinite(x) else float("nan")
    except Exception:
        return float("nan")

def feature_keys(rows):
    keys = set()
    for r in rows:
        for k, v in r.items():
            if not str(k).startswith(("sigf_", "fs_")):
                continue
            if is_num(v):
                keys.add(k)
    return sorted(keys)

def threshold_table(y_true, p_win, thresholds):
    out = []
    n = len(y_true)
    for thr in thresholds:
        pred = [1 if p >= thr else 0 for p in p_win]
        selected = sum(pred)
        tp = sum(1 for y, p in zip(y_true, pred) if y == 1 and p == 1)
        fp = sum(1 for y, p in zip(y_true, pred) if y == 0 and p == 1)
        tn = sum(1 for y, p in zip(y_true, pred) if y == 0 and p == 0)
        fn = sum(1 for y, p in zip(y_true, pred) if y == 1 and p == 0)

        precision = tp / (tp + fp) if (tp + fp) else None
        recall = tp / (tp + fn) if (tp + fn) else None
        coverage = selected / n if n else 0.0

        out.append({
            "threshold": thr,
            "selected": selected,
            "coverage": coverage,
            "precision": precision,
            "recall": recall,
            "tp": tp,
            "fp": fp,
            "tn": tn,
            "fn": fn,
        })
    return out

def py(v):
    try:
        import numpy as np
        if isinstance(v, (np.integer,)):
            return int(v)
        if isinstance(v, (np.floating,)):
            return float(v)
        if isinstance(v, (np.bool_,)):
            return bool(v)
    except Exception:
        pass
    return v

def main():
    try:
        import numpy as np
        import joblib
        from sklearn.pipeline import Pipeline
        from sklearn.impute import SimpleImputer
        from sklearn.preprocessing import StandardScaler
        from sklearn.linear_model import LogisticRegression
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.metrics import roc_auc_score, brier_score_loss
    except Exception as e:
        raise SystemExit(f"sklearn/joblib missing: {e}")

    raw = read_jsonl(DATASET)
    rows = []
    for r in raw:
        if not bool(r.get("trainable_label")):
            continue
        y = label_win(r)
        if y is None:
            continue
        rows.append(dict(r, _y=y, _t=signal_time(r)))

    rows.sort(key=lambda r: (r["_t"], str(r.get("signal_key") or "")))

    keys = feature_keys(rows)
    if not keys:
        raise SystemExit("no numeric sigf_/fs_ features found")

    X = [[to_float(r.get(k)) for k in keys] for r in rows]
    y = [int(r["_y"]) for r in rows]

    n = len(rows)
    split = max(1, int(n * TRAIN_FRAC))
    split = min(split, n - 1)

    X_train, X_test = X[:split], X[split:]
    y_train, y_test = y[:split], y[split:]
    rows_train, rows_test = rows[:split], rows[split:]

    state = "CHALLENGER_BLOCKED"
    block_reasons = []

    if n < 120:
        block_reasons.append(f"trainable_rows_below_min: {n}<120")
    if len(set(y_train)) < 2:
        block_reasons.append("train_split_single_class")
    if len(set(y_test)) < 2:
        block_reasons.append("test_split_single_class")
    if len(y_test) < 50:
        block_reasons.append(f"test_rows_below_min: {len(y_test)}<50")

    models = {
        "logistic_balanced": Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            ("model", LogisticRegression(max_iter=2000, class_weight="balanced", random_state=42)),
        ]),
        "rf_balanced": Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("model", RandomForestClassifier(
                n_estimators=300,
                max_depth=4,
                min_samples_leaf=8,
                class_weight="balanced_subsample",
                random_state=42,
            )),
        ]),
    }

    model_reports = {}
    baseline_test_win_rate = sum(y_test) / len(y_test) if y_test else None

    for name, model in models.items():
        rep = {
            "ok": False,
            "model": name,
            "artifact": None,
            "auc": None,
            "brier": None,
            "thresholds": [],
            "error": None,
        }

        try:
            if block_reasons:
                rep["error"] = "blocked_precheck:" + ",".join(block_reasons)
                model_reports[name] = rep
                continue

            model.fit(X_train, y_train)
            p = model.predict_proba(X_test)[:, 1]

            try:
                rep["auc"] = float(roc_auc_score(y_test, p))
            except Exception:
                rep["auc"] = None

            try:
                rep["brier"] = float(brier_score_loss(y_test, p))
            except Exception:
                rep["brier"] = None

            rep["thresholds"] = threshold_table(y_test, [float(x) for x in p], THRESHOLDS)

            artifact = ART_DIR / f"ml_challenger_v4_{name}.joblib"
            joblib.dump({
                "version": VERSION,
                "model_name": name,
                "feature_keys": keys,
                "model": model,
                "dataset_path": str(DATASET),
                "train_rows": len(y_train),
                "test_rows": len(y_test),
                "created_note": "OFFLINE_CHALLENGER_ONLY_DO_NOT_DEPLOY_LIVE",
            }, artifact)
            rep["artifact"] = str(artifact)
            rep["ok"] = True

        except Exception as e:
            rep["error"] = str(e)

        model_reports[name] = rep

    # conservative readiness: compare precision@0.74 vs test baseline
    ready_models = []
    for name, rep in model_reports.items():
        if not rep.get("ok"):
            continue
        row74 = next((x for x in rep.get("thresholds", []) if abs(float(x["threshold"]) - 0.74) < 1e-9), None)
        if not row74:
            continue
        precision = row74.get("precision")
        coverage = row74.get("coverage") or 0
        auc = rep.get("auc")
        if precision is not None and baseline_test_win_rate is not None:
            edge = precision - baseline_test_win_rate
        else:
            edge = None

        if (
            precision is not None
            and coverage >= 0.05
            and edge is not None
            and edge >= 0.03
            and auc is not None
            and auc >= 0.52
        ):
            ready_models.append({
                "model": name,
                "precision_074": precision,
                "coverage_074": coverage,
                "edge_vs_baseline": edge,
                "auc": auc,
                "artifact": rep.get("artifact"),
            })

    if ready_models:
        ready_models.sort(key=lambda x: (x["precision_074"], x["coverage_074"], x["auc"]), reverse=True)
        state = "CHALLENGER_CANDIDATE_READY_FOR_SHADOW"
        recommendation = "SHADOW_COMPARE_ONLY"
    elif not block_reasons:
        state = "CHALLENGER_TRAINED_NOT_READY"
        recommendation = "KEEP_CURRENT_LIVE_GATE_ACCUMULATE_MORE_LABELS"
    else:
        recommendation = "BLOCKED_ACCUMULATE_OR_FIX_DATA"

    report = {
        "ok": True,
        "version": VERSION,
        "state": state,
        "recommendation": recommendation,
        "dataset_path": str(DATASET),
        "rows_raw": len(raw),
        "trainable_rows_used": n,
        "feature_count": len(keys),
        "feature_keys": keys,
        "split": {
            "mode": "time_order_walk_forward",
            "train_frac": TRAIN_FRAC,
            "train_rows": len(y_train),
            "test_rows": len(y_test),
            "train_win_rate": sum(y_train) / len(y_train) if y_train else None,
            "test_win_rate": baseline_test_win_rate,
            "train_target_counts": dict(Counter(y_train)),
            "test_target_counts": dict(Counter(y_test)),
            "train_time_min": rows_train[0]["_t"] if rows_train else None,
            "train_time_max": rows_train[-1]["_t"] if rows_train else None,
            "test_time_min": rows_test[0]["_t"] if rows_test else None,
            "test_time_max": rows_test[-1]["_t"] if rows_test else None,
        },
        "block_reasons": block_reasons,
        "ready_models": ready_models,
        "models": model_reports,
        "note": "REPORT_ONLY. Phase 4 challenger offline only. Does not replace live ML gate.",
    }

    REPORT_JSON.parent.mkdir(parents=True, exist_ok=True)
    REPORT_JSON.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=py))

    with REPORT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "model", "threshold", "selected", "coverage", "precision", "recall", "tp", "fp", "tn", "fn", "auc", "brier"
        ])
        w.writeheader()
        for name, rep in model_reports.items():
            for row in rep.get("thresholds", []):
                w.writerow({
                    "model": name,
                    "threshold": row.get("threshold"),
                    "selected": row.get("selected"),
                    "coverage": row.get("coverage"),
                    "precision": row.get("precision"),
                    "recall": row.get("recall"),
                    "tp": row.get("tp"),
                    "fp": row.get("fp"),
                    "tn": row.get("tn"),
                    "fn": row.get("fn"),
                    "auc": rep.get("auc"),
                    "brier": rep.get("brier"),
                })

    print("=== ML CHALLENGER V4 OFFLINE ===")
    print("state:", state)
    print("recommendation:", recommendation)
    print("dataset:", DATASET)
    print("rows:", n, "features:", len(keys), "train:", len(y_train), "test:", len(y_test))
    print("test_win_rate:", baseline_test_win_rate)
    print("ready_models:", ready_models)
    print("out_json:", REPORT_JSON)
    print("out_csv :", REPORT_CSV)

if __name__ == "__main__":
    main()
