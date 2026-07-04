#!/usr/bin/env python3
# TP_SL_PREDICTOR_READINESS_V1_20260705
import os
import json
import math
from pathlib import Path
from datetime import datetime, timezone

MARKER = "TP_SL_PREDICTOR_READINESS_V1_20260705"

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def to_float(v, default=None):
    try:
        if v is None or v == "":
            return default
        x = float(v)
        if not math.isfinite(x):
            return default
        return x
    except Exception:
        return default

def read_jsonl(path, max_rows=None):
    p = Path(path)
    if not p.exists():
        return []
    lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()
    if max_rows:
        lines = lines[-max_rows:]
    rows = []
    for line in lines:
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                rows.append(obj)
        except Exception:
            pass
    return rows

def bucket_p(p):
    x = to_float(p)
    if x is None:
        return "NULL"
    if x < 0.45:
        return "<45"
    if x < 0.52:
        return "45-52"
    if x < 0.58:
        return "52-58"
    if x < 0.65:
        return "58-65"
    return "65+"

def main():
    log_dir = Path(os.getenv("LOG_DIR", "logs"))
    report_dir = Path(os.getenv("REPORT_DIR", "reports"))
    report_dir.mkdir(parents=True, exist_ok=True)

    pred_path = Path(os.getenv("TP_SL_PREDICTOR_LOG_PATH", str(log_dir / "tp_sl_predictor_v1.jsonl")))
    preds = read_jsonl(pred_path, max_rows=50000)

    by_decision = {}
    by_bucket = {}
    p_vals = []
    edge_vals = []

    for r in preds:
        d = str(r.get("decision") or "UNKNOWN")
        by_decision[d] = by_decision.get(d, 0) + 1

        b = bucket_p(r.get("p_tp_before_sl"))
        by_bucket[b] = by_bucket.get(b, 0) + 1

        p = to_float(r.get("p_tp_before_sl"))
        e = to_float(r.get("fee_adjusted_expected_R"))
        if p is not None:
            p_vals.append(p)
        if e is not None:
            edge_vals.append(e)

    # dataset feature readiness
    ou_report = {}
    for fp in [
        report_dir / "ml_dataset_v4_linear_quant_join_v1_report.json",
        report_dir / "ml_dataset_v4_stoch_barrier_join_v1_report.json",
        report_dir / "ml_dataset_v4_ou_meanrev_join_v1_report.json",
    ]:
        if fp.exists():
            try:
                ou_report[fp.name] = json.loads(fp.read_text(encoding="utf-8", errors="ignore"))
            except Exception:
                ou_report[fp.name] = {"ok": False, "reason": "json_read_failed"}

    report = {
        "ok": True,
        "marker": MARKER,
        "created_at_utc": utc_now_iso(),
        "predictor_log_path": str(pred_path),
        "prediction_rows": len(preds),
        "decision_counts": by_decision,
        "probability_buckets": by_bucket,
        "p_tp_avg": None if not p_vals else round(sum(p_vals) / len(p_vals), 6),
        "p_tp_min": None if not p_vals else round(min(p_vals), 6),
        "p_tp_max": None if not p_vals else round(max(p_vals), 6),
        "fee_adjusted_expected_R_avg": None if not edge_vals else round(sum(edge_vals) / len(edge_vals), 6),
        "fee_adjusted_expected_R_min": None if not edge_vals else round(min(edge_vals), 6),
        "fee_adjusted_expected_R_max": None if not edge_vals else round(max(edge_vals), 6),
        "dataset_reports": ou_report,
        "mode": os.getenv("TP_SL_PREDICTOR_MODE", "ADVISORY"),
        "hard_gate": os.getenv("TP_SL_PREDICTOR_HARD_GATE", "false"),
    }

    out = report_dir / "tp_sl_predictor_v1_report.json"
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    main()
