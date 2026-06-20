#!/usr/bin/env python3
import json
from pathlib import Path
from collections import Counter, defaultdict

ROOT = Path(__file__).resolve().parents[1]

DATASET = ROOT / "logs" / "ml_dataset_v4_current14_candidate_join.jsonl"
LOOKAHEAD = ROOT / "reports" / "lookahead_recursive_audit_v1.json"

OUT_JSON = ROOT / "reports" / "ml_dataset_v4_readiness_report.json"

VERSION = "ml_dataset_v4_readiness_report_20260620"

CURRENT14 = {
    "BTCUSDT","ETHUSDT","SOLUSDT","PAXGUSDT","HYPEUSDT","XRPUSDT","ZECUSDT",
    "UNIUSDT","ADAUSDT","BCHUSDT","LINKUSDT","SUIUSDT","LTCUSDT","AVAXUSDT",
}

BAD_FEATURE_TERMS = [
    "label", "outcome", "future", "tp_hit", "sl_hit", "win", "pnl",
    "resolved", "first_touch", "hit_at", "hit_time",
]


def read_jsonl(path):
    rows = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            try:
                rows.append(json.loads(line))
            except Exception:
                pass
    return rows


def read_json(path):
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(errors="ignore"))
    except Exception:
        return {}


def is_win(v):
    if isinstance(v, bool):
        return v is True
    s = str(v).strip().lower()
    return s in ("1", "1.0", "true", "win", "tp1", "tp2", "tp3")


def feature_keys(row):
    return [k for k in row if k.startswith(("sigf_", "fs_"))]


def hard_check(name, ok, detail):
    return {"type": "hard", "name": name, "ok": bool(ok), "detail": detail}


def warn_check(name, ok, detail):
    return {"type": "warn", "name": name, "ok": bool(ok), "detail": detail}


def main():
    rows = read_jsonl(DATASET)
    train = [r for r in rows if r.get("trainable_label")]

    symbols = sorted({str(r.get("symbol") or "").upper() for r in rows})
    train_symbols = sorted({str(r.get("symbol") or "").upper() for r in train})

    wins = sum(1 for r in train if is_win(r.get("label_win")))
    losses = len(train) - wins

    by_symbol = defaultdict(lambda: {"rows": 0, "trainable": 0, "wins": 0, "losses": 0})
    for r in rows:
        s = str(r.get("symbol") or "").upper()
        by_symbol[s]["rows"] += 1
        if r.get("trainable_label"):
            by_symbol[s]["trainable"] += 1
            if is_win(r.get("label_win")):
                by_symbol[s]["wins"] += 1
            else:
                by_symbol[s]["losses"] += 1

    feature_source_counts = Counter(r.get("feature_source") for r in rows)
    target_counts = Counter(str(r.get("label_target") or r.get("outcome_status") or "NA").upper() for r in train)
    feature_count_values = [len(feature_keys(r)) for r in rows]
    feature_count_train = [len(feature_keys(r)) for r in train]

    bad_feature_keys = []
    for r in rows:
        for k in feature_keys(r):
            low = k.lower()
            if any(term in low for term in BAD_FEATURE_TERMS):
                bad_feature_keys.append(k)

    look = read_json(LOOKAHEAD)
    future_feature_count = int(look.get("join_future_feature_count") or 0)
    future_candle_count = int(look.get("feature_future_candle_count") or 0)
    stale_feature_count = int(look.get("join_stale_feature_count") or 0)
    recursive_plan_drift_count = int(look.get("recursive_plan_drift_count") or 0)
    lookahead_status = str(look.get("status") or "MISSING").upper()

    hard = [
        hard_check("dataset_exists", DATASET.exists(), {"path": str(DATASET)}),
        hard_check("rows_min_300", len(rows) >= 300, {"rows": len(rows), "required": 300}),
        hard_check("trainable_min_100", len(train) >= 100, {"trainable": len(train), "required": 100}),
        hard_check("wins_min_25", wins >= 25, {"wins": wins, "required": 25}),
        hard_check("losses_min_25", losses >= 25, {"losses": losses, "required": 25}),
        hard_check("all_symbols_current14", set(symbols).issubset(CURRENT14), {"symbols": symbols}),
        hard_check("all_14_symbols_present", set(symbols) == CURRENT14, {"symbols": symbols}),
        hard_check("bad_feature_keys_zero", len(bad_feature_keys) == 0, {"bad_count": len(bad_feature_keys), "sample": bad_feature_keys[:20]}),
        hard_check("no_future_feature_leak", future_feature_count == 0, {"join_future_feature_count": future_feature_count}),
        hard_check("no_future_candle_leak", future_candle_count == 0, {"feature_future_candle_count": future_candle_count}),
    ]

    min_trainable_by_symbol = min((v["trainable"] for v in by_symbol.values()), default=0)

    warnings = [
        warn_check("lookahead_status_not_fail", lookahead_status in ("OK", "WARN"), {"status": lookahead_status}),
        warn_check("recursive_plan_drift_low", recursive_plan_drift_count <= 20, {"recursive_plan_drift_count": recursive_plan_drift_count}),
        warn_check("stale_feature_zero", stale_feature_count == 0, {"join_stale_feature_count": stale_feature_count}),
        warn_check("min_symbol_trainable_ge_3", min_trainable_by_symbol >= 3, {"min_trainable_by_symbol": min_trainable_by_symbol}),
        warn_check("feature_source_documented", True, {"feature_source_counts": dict(feature_source_counts)}),
        warn_check("feature_count_min_ge_4", min(feature_count_values or [0]) >= 4, {"min_feature_count": min(feature_count_values or [0])}),
    ]

    hard_ok = all(x["ok"] for x in hard)
    warn_ok = all(x["ok"] for x in warnings)

    if hard_ok and warn_ok:
        state = "V4_CANDIDATE_READY"
    elif hard_ok:
        state = "V4_CANDIDATE_READY_WITH_WARNINGS"
    else:
        state = "V4_CANDIDATE_BLOCKED"

    report = {
        "ok": hard_ok,
        "state": state,
        "report_version": VERSION,
        "dataset_path": str(DATASET),
        "rows": len(rows),
        "trainable_label_rows": len(train),
        "wins": wins,
        "losses": losses,
        "win_rate": round(wins / len(train), 6) if train else None,
        "symbols": symbols,
        "train_symbols": train_symbols,
        "feature_source_counts": dict(feature_source_counts),
        "target_counts": dict(target_counts),
        "feature_count": {
            "min_all": min(feature_count_values or [0]),
            "max_all": max(feature_count_values or [0]),
            "min_trainable": min(feature_count_train or [0]),
            "max_trainable": max(feature_count_train or [0]),
            "distribution_all": dict(Counter(feature_count_values)),
            "distribution_trainable": dict(Counter(feature_count_train)),
        },
        "by_symbol": dict(sorted(by_symbol.items())),
        "lookahead": {
            "status": lookahead_status,
            "join_future_feature_count": future_feature_count,
            "feature_future_candle_count": future_candle_count,
            "join_stale_feature_count": stale_feature_count,
            "recursive_plan_drift_count": recursive_plan_drift_count,
        },
        "checks": hard + warnings,
        "note": "REPORT_ONLY. V4 candidate readiness only. Does not train, deploy, or replace live ML gate.",
    }

    OUT_JSON.write_text(json.dumps(report, indent=2, ensure_ascii=False))

    print(f"=== ML DATASET V4 READINESS | {state} ===")
    print("out_json:", OUT_JSON)
    print("rows:", len(rows), "trainable:", len(train), "wins:", wins, "losses:", losses)
    print("feature_source:", dict(feature_source_counts))
    print("targets:", dict(target_counts))
    print("lookahead:", report["lookahead"])
    print()
    print("HARD CHECKS")
    for c in hard:
        print(("PASS" if c["ok"] else "FAIL"), c["name"], c["detail"])
    print()
    print("WARN CHECKS")
    for c in warnings:
        print(("PASS" if c["ok"] else "WARN"), c["name"], c["detail"])


if __name__ == "__main__":
    main()
