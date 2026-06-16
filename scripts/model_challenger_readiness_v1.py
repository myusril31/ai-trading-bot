#!/usr/bin/env python3
import json
import importlib.util
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import defaultdict

ROOT = Path(__file__).resolve().parents[1]
WIB = timezone(timedelta(hours=7))
UTC = timezone.utc

JOIN_REPORT = ROOT / "reports" / "ml_dataset_v3_feature_join_report.json"
JOIN_FILE = ROOT / "logs" / "ml_dataset_v3_feature_join.jsonl"
LOOKAHEAD_REPORT = ROOT / "reports" / "lookahead_recursive_audit_v1.json"
HYPEROPT_REPORT = ROOT / "reports" / "hyperopt_loss_metrics_v1.json"
POLICY_REPORT = ROOT / "reports" / "pair_league_policy_v1.json"

OUT_JSON = ROOT / "reports" / "model_challenger_readiness_v1.json"

MIN_JOINED_ROWS = 100
MIN_FEATURE_OUTCOME_ROWS_RF = 150
MIN_FEATURE_OUTCOME_ROWS_XGB_LGBM = 300
MIN_SYMBOLS_WITH_OUTCOME = 8
MIN_PORTFOLIO_OBJECTIVE = 60.0
MIN_PORTFOLIO_N = 200

def read_json(path):
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(errors="ignore"))
    except Exception:
        return {}

def read_jsonl(path):
    if not path.exists():
        return []
    out = []
    for line in path.read_text(errors="ignore").splitlines():
        try:
            out.append(json.loads(line))
        except Exception:
            pass
    return out

def has_pkg(name):
    return importlib.util.find_spec(name) is not None

def has_outcome(r):
    return (
        r.get("label_R") is not None
        or r.get("label_win") is not None
        or r.get("outcome_status") not in (None, "", "OPEN")
    )

def symbol_of(r):
    return str(r.get("symbol") or "").replace(".P", "").upper()

def pass_fail(name, ok, detail):
    return {
        "name": name,
        "ok": bool(ok),
        "detail": detail,
    }

def main():
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)

    join_report = read_json(JOIN_REPORT)
    join_rows = read_jsonl(JOIN_FILE)
    lookahead = read_json(LOOKAHEAD_REPORT)
    hyperopt = read_json(HYPEROPT_REPORT)
    policy = read_json(POLICY_REPORT)

    joined_rows = int(join_report.get("joined_rows") or len(join_rows))
    joined_with_ml = int(join_report.get("joined_with_ml") or 0)
    joined_with_outcome_report = int(join_report.get("joined_with_outcome") or 0)

    feature_outcome_rows = [
        r for r in join_rows
        if has_outcome(r) and r.get("fs_feature_sanity_ok") is not False
    ]

    symbols = sorted(set(symbol_of(r) for r in feature_outcome_rows if symbol_of(r)))

    lookahead_status = str(lookahead.get("status") or "MISSING").upper()
    lookahead_ok = lookahead_status == "OK"

    portfolio = hyperopt.get("portfolio") or {}
    portfolio_n = int(portfolio.get("n") or 0)
    portfolio_objective = float(portfolio.get("objective_score") or 0.0)
    portfolio_wr = portfolio.get("win_rate")
    portfolio_exp = portfolio.get("expectancy_R")

    pkg = {
        "sklearn": has_pkg("sklearn"),
        "xgboost": has_pkg("xgboost"),
        "lightgbm": has_pkg("lightgbm"),
    }

    gates = [
        pass_fail(
            "lookahead_audit_ok",
            lookahead_ok,
            {"status": lookahead_status},
        ),
        pass_fail(
            "joined_rows_min",
            joined_rows >= MIN_JOINED_ROWS,
            {"joined_rows": joined_rows, "required": MIN_JOINED_ROWS},
        ),
        pass_fail(
            "feature_outcome_rows_min_rf",
            len(feature_outcome_rows) >= MIN_FEATURE_OUTCOME_ROWS_RF,
            {"feature_outcome_rows": len(feature_outcome_rows), "required": MIN_FEATURE_OUTCOME_ROWS_RF},
        ),
        pass_fail(
            "symbols_with_outcome_min",
            len(symbols) >= MIN_SYMBOLS_WITH_OUTCOME,
            {"symbols_with_outcome": len(symbols), "required": MIN_SYMBOLS_WITH_OUTCOME, "symbols": symbols},
        ),
        pass_fail(
            "portfolio_sample_min",
            portfolio_n >= MIN_PORTFOLIO_N,
            {"portfolio_n": portfolio_n, "required": MIN_PORTFOLIO_N},
        ),
        pass_fail(
            "portfolio_objective_min",
            portfolio_objective >= MIN_PORTFOLIO_OBJECTIVE,
            {"portfolio_objective": portfolio_objective, "required": MIN_PORTFOLIO_OBJECTIVE},
        ),
    ]

    global_ready = all(g["ok"] for g in gates)

    def model_status(model, package_ok, required_rows):
        data_ok = len(feature_outcome_rows) >= required_rows
        ready = lookahead_ok and package_ok and data_ok and len(symbols) >= MIN_SYMBOLS_WITH_OUTCOME
        if ready:
            status = "READY_SHADOW_TRAIN"
        elif not package_ok:
            status = "BLOCKED_PACKAGE_MISSING"
        elif not data_ok:
            status = "BLOCKED_DATA_TOO_SMALL"
        elif not lookahead_ok:
            status = "BLOCKED_LOOKAHEAD_FAIL"
        else:
            status = "BLOCKED_SYMBOL_COVERAGE"
        return {
            "model": model,
            "status": status,
            "ready": ready,
            "package_ok": package_ok,
            "feature_outcome_rows": len(feature_outcome_rows),
            "required_rows": required_rows,
            "symbols_with_outcome": len(symbols),
            "required_symbols": MIN_SYMBOLS_WITH_OUTCOME,
        }

    models = [
        model_status("RandomForestClassifier", pkg["sklearn"], MIN_FEATURE_OUTCOME_ROWS_RF),
        model_status("XGBoostClassifier", pkg["xgboost"], MIN_FEATURE_OUTCOME_ROWS_XGB_LGBM),
        model_status("LightGBMClassifier", pkg["lightgbm"], MIN_FEATURE_OUTCOME_ROWS_XGB_LGBM),
    ]

    report = {
        "ok": True,
        "report_version": "model_challenger_readiness_v1_20260615",
        "mode": "REPORT_ONLY",
        "created_at_wib": datetime.now(UTC).astimezone(WIB).strftime("%Y-%m-%d %H:%M:%S WIB"),
        "global_ready": global_ready,
        "global_status": "READY" if global_ready else "NOT_READY",
        "summary": {
            "joined_rows": joined_rows,
            "joined_with_ml": joined_with_ml,
            "joined_with_outcome_report": joined_with_outcome_report,
            "feature_outcome_rows": len(feature_outcome_rows),
            "symbols_with_outcome": len(symbols),
            "lookahead_status": lookahead_status,
            "portfolio_n": portfolio_n,
            "portfolio_objective": portfolio_objective,
            "portfolio_wr": portfolio_wr,
            "portfolio_expectancy_R": portfolio_exp,
        },
        "packages": pkg,
        "gates": gates,
        "models": models,
        "notes": [
            "This does not train any model.",
            "RandomForest can be first challenger because sklearn is already used.",
            "XGBoost/LightGBM need package availability and larger data.",
            "Feature-outcome rows must come from joined feature dataset, not raw outcomes only.",
        ],
    }

    OUT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2))

    print("=== MODEL CHALLENGER READINESS V1 | REPORT_ONLY ===")
    print("out_json:", OUT_JSON)
    print("")
    print("global_status:", report["global_status"])
    print("summary:", json.dumps(report["summary"], ensure_ascii=False))
    print("")
    print(f"{'MODEL':<24} {'STATUS':<28} {'PKG':>5} {'ROWS':>6} {'REQ':>6} {'SYM':>4}")
    for m in models:
        print(
            f"{m['model']:<24} {m['status']:<28} "
            f"{str(m['package_ok']):>5} {m['feature_outcome_rows']:>6} "
            f"{m['required_rows']:>6} {m['symbols_with_outcome']:>4}"
        )

if __name__ == "__main__":
    main()
