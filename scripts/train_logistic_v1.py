#!/usr/bin/env python3
from __future__ import annotations
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


def main() -> int:
    p = argparse.ArgumentParser(description="Offline/manual trainer for logistic_v1 shadow model")
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--meta", required=True)
    args = p.parse_args()

    try:
        import pandas as pd
        from sklearn.compose import ColumnTransformer
        from sklearn.impute import SimpleImputer
        from sklearn.linear_model import LogisticRegression
        from sklearn.metrics import accuracy_score, roc_auc_score, brier_score_loss
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import OneHotEncoder, StandardScaler
    except Exception as e:
        print(json.dumps({"ok": False, "reason": "sklearn_or_pandas_missing", "error": str(e)}))
        return 2

    in_path = Path(args.input)
    if not in_path.exists():
        print(json.dumps({"ok": False, "reason": "input_missing", "input": str(in_path)}))
        return 1

    df = pd.read_csv(in_path)
    if "label_win" not in df.columns:
        print(json.dumps({"ok": False, "reason": "label_win_missing"}))
        return 1

    if "include_ml" in df.columns:
        df = df[df["include_ml"].astype(str).str.lower().isin(["1", "true", "yes", "y"])]
    if "sample_type" in df.columns:
        df = df[df["sample_type"].astype(str).str.upper() != "VALIDATION_SAMPLE"]
    if "outcome_status" in df.columns:
        df = df[~df["outcome_status"].astype(str).str.upper().isin(["DATA_GAP", "OPEN_END", "NO_FILL", "SKIP"])]
    df = df[df["label_win"].notna()]
    df = df[df["label_win"].isin([0, 1, 0.0, 1.0, "0", "1"])]

    if len(df) < 10:
        print(json.dumps({"ok": False, "reason": "insufficient_rows_after_filter", "rows": int(len(df))}))
        return 1

    df["label_win"] = df["label_win"].astype(int)
    if "created_at_utc" in df.columns:
        df = df.sort_values("created_at_utc", kind="stable")

    split_idx = int(len(df) * 0.8)
    if split_idx <= 0 or split_idx >= len(df):
        split_idx = max(1, min(len(df) - 1, split_idx))

    train_df = df.iloc[:split_idx].copy()
    val_df = df.iloc[split_idx:].copy()

    label_col = "label_win"

    leakage_cols = {
        "label_win", "label_target", "label_R", "outcome", "outcome_status", "close_outcome",
        "BT_FINAL_STATUS", "BarsToOutcome", "MFE", "MAE", "FinalWindowEnd", "Snapshot PnL",
        "signal_key", "created_at_utc", "signal_time_wib",
    }
    drop_cols = [c for c in train_df.columns if c in leakage_cols]
    X_train = train_df.drop(columns=drop_cols)
    y_train = train_df[label_col]
    X_val = val_df.drop(columns=drop_cols)
    y_val = val_df[label_col]

    num_cols = [c for c in X_train.columns if pd.api.types.is_numeric_dtype(X_train[c])]
    cat_cols = [c for c in X_train.columns if c not in num_cols]
    feature_columns = list(X_train.columns)

    pre = ColumnTransformer([
        ("num", Pipeline([("imp", SimpleImputer(strategy="median")), ("sc", StandardScaler())]), num_cols),
        ("cat", Pipeline([("imp", SimpleImputer(strategy="most_frequent")), ("oh", OneHotEncoder(handle_unknown="ignore"))]), cat_cols),
    ])
    model = Pipeline([("pre", pre), ("clf", LogisticRegression(max_iter=1000, class_weight="balanced"))])
    model.fit(X_train, y_train)

    val_pred = model.predict(X_val)
    val_proba = model.predict_proba(X_val)[:, 1]

    meta_obj = {
        "model_version": "logistic_v1",
        "label_column": label_col,
        "feature_columns": feature_columns,
        "train_rows": int(len(train_df)),
        "validation_rows": int(len(val_df)),
        "accuracy": float(accuracy_score(y_val, val_pred)),
        "auc": None,
        "brier_score": None,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "manual_offline_only": True,
    }
    try:
        if len(set(y_val.tolist())) >= 2:
            meta_obj["auc"] = float(roc_auc_score(y_val, val_proba))
    except Exception:
        pass
    try:
        meta_obj["brier_score"] = float(brier_score_loss(y_val, val_proba))
    except Exception:
        pass

    out = Path(args.output); out.parent.mkdir(parents=True, exist_ok=True)
    meta = Path(args.meta); meta.parent.mkdir(parents=True, exist_ok=True)
    import pickle
    out.write_bytes(pickle.dumps(model))
    meta.write_text(json.dumps(meta_obj, indent=2) + "\n", encoding="utf-8")

    print(json.dumps({"ok": True, "output": str(out), "meta": str(meta), "train_rows": len(train_df), "validation_rows": len(val_df)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
