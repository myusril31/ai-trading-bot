#!/usr/bin/env python3
import csv, json, math
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import Counter, defaultdict

ROOT = Path(__file__).resolve().parents[1]
WIB = timezone(timedelta(hours=7))

OLD_PATH = ROOT / "logs" / "forward_outcomes.jsonl"
NEW_PATH = ROOT / "logs" / "forward_outcomes_v1.jsonl"

OUT_JSON = ROOT / "reports" / "forward_outcomes_mismatch_triage_v1.json"
OUT_CSV = ROOT / "reports" / "forward_outcomes_mismatch_triage_v1.csv"
OUT_RUNS = ROOT / "logs" / "forward_outcomes_mismatch_triage_runs_v1.jsonl"

VERSION = "forward_outcomes_mismatch_triage_v1_20260617"
MODE = "REPORT_ONLY"

CLOSED = {"TP1", "TP2", "TP3", "SL"}
NON_LABEL = {"PENDING", "OPEN_END", "NO_FILL", "DATA_GAP", "BAD_PLAN", "BAD_TIME", "", None}

def now_wib_str():
    return datetime.now(timezone.utc).astimezone(WIB).strftime("%Y-%m-%d %H:%M:%S WIB")

def read_jsonl(path):
    rows = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    rows.append(obj)
            except Exception:
                pass
    return rows

def key_of(r):
    return str(r.get("signal_key") or r.get("signal_id") or "").strip()

def latest_by_key(rows):
    out = {}
    for r in rows:
        k = key_of(r)
        if k:
            out[k] = r
    return out

def status(r):
    if not r:
        return ""
    return str(r.get("outcome_status") or r.get("status") or "").upper().strip()

def target(r):
    if not r:
        return ""
    return str(r.get("label_target") or r.get("first_hit") or "").upper().strip()

def label_r(r):
    if not r:
        return None
    try:
        v = float(r.get("label_R"))
        if math.isfinite(v):
            return round(v, 6)
    except Exception:
        pass
    return None

def label_win(r):
    if not r:
        return None
    x = r.get("label_win")
    if isinstance(x, bool):
        return x
    if x is None:
        return None
    s = str(x).lower().strip()
    if s in ("true", "1", "yes"):
        return True
    if s in ("false", "0", "no"):
        return False
    return None

def triage_bucket(old, new):
    os = status(old)
    ns = status(new)

    if old is None and new is not None:
        return "NEW_ONLY"

    if old is not None and new is None:
        return "OLD_ONLY"

    if os == ns and target(old) == target(new) and label_r(old) == label_r(new) and label_win(old) == label_win(new):
        return "MATCH"

    if os in ("PENDING", "OPEN", "OPEN_END", "") and ns in CLOSED:
        return "OLD_PENDING_NEW_CLOSED"

    if os in ("PENDING", "OPEN", "OPEN_END", "") and ns in NON_LABEL:
        return "OLD_PENDING_NEW_NON_LABEL"

    if os in CLOSED and ns in CLOSED:
        if os != ns or target(old) != target(new) or label_r(old) != label_r(new):
            return "CLOSED_LABEL_CONFLICT"
        return "CLOSED_LABEL_MATCH_WEAK"

    if os in CLOSED and ns in NON_LABEL:
        return "OLD_CLOSED_NEW_NON_LABEL"

    if os in NON_LABEL and ns in CLOSED:
        return "OLD_NON_LABEL_NEW_CLOSED"

    return "OTHER_MISMATCH"

def main():
    old_rows = read_jsonl(OLD_PATH)
    new_rows = read_jsonl(NEW_PATH)

    old_by = latest_by_key(old_rows)
    new_by = latest_by_key(new_rows)

    keys = sorted(set(old_by) | set(new_by))

    rows = []
    transition_counts = Counter()
    bucket_counts = Counter()

    for k in keys:
        old = old_by.get(k)
        new = new_by.get(k)

        os = status(old)
        ns = status(new)
        b = triage_bucket(old, new)

        transition_counts[f"{os or 'MISSING'} -> {ns or 'MISSING'}"] += 1
        bucket_counts[b] += 1

        rows.append({
            "signal_key": k,
            "symbol": (new or old or {}).get("symbol"),
            "direction": (new or old or {}).get("direction"),
            "triage_bucket": b,

            "old_status": os,
            "new_status": ns,

            "old_target": target(old),
            "new_target": target(new),

            "old_label_win": label_win(old),
            "new_label_win": label_win(new),

            "old_label_R": label_r(old),
            "new_label_R": label_r(new),

            "old_reason": (old or {}).get("exclude_label_reason"),
            "new_reason": (new or {}).get("exclude_label_reason"),

            "new_signal_time_wib": (new or {}).get("signal_time_wib"),
            "new_execution_decision": (new or {}).get("execution_decision"),
            "new_source_mode": (new or {}).get("source_mode"),
        })

    danger_count = (
        bucket_counts.get("CLOSED_LABEL_CONFLICT", 0)
        + bucket_counts.get("OLD_CLOSED_NEW_NON_LABEL", 0)
    )

    healthy_resolution_count = (
        bucket_counts.get("OLD_PENDING_NEW_CLOSED", 0)
        + bucket_counts.get("OLD_NON_LABEL_NEW_CLOSED", 0)
    )

    created_at_wib = now_wib_str()

    report = {
        "ok": True,
        "version": VERSION,
        "mode": MODE,
        "created_at_wib": created_at_wib,
        "old_path": str(OLD_PATH),
        "new_path": str(NEW_PATH),
        "old_rows": len(old_rows),
        "new_rows": len(new_rows),
        "old_unique": len(old_by),
        "new_unique": len(new_by),
        "union_keys": len(keys),
        "bucket_counts": dict(bucket_counts),
        "transition_counts": dict(transition_counts.most_common()),
        "danger_count": danger_count,
        "healthy_resolution_count": healthy_resolution_count,
        "switch_recommendation": "DO_NOT_SWITCH_YET" if danger_count > 0 else "SWITCH_CANDIDATE_REPORT_ONLY",
        "note": "REPORT_ONLY triage. Does not switch dataset join input.",
        "danger_samples": [
            r for r in rows
            if r["triage_bucket"] in ("CLOSED_LABEL_CONFLICT", "OLD_CLOSED_NEW_NON_LABEL")
        ][:50],
        "healthy_samples": [
            r for r in rows
            if r["triage_bucket"] in ("OLD_PENDING_NEW_CLOSED", "OLD_NON_LABEL_NEW_CLOSED")
        ][:50],
    }

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    cols = [
        "signal_key","symbol","direction","triage_bucket",
        "old_status","new_status",
        "old_target","new_target",
        "old_label_win","new_label_win",
        "old_label_R","new_label_R",
        "old_reason","new_reason",
        "new_signal_time_wib","new_execution_decision","new_source_mode",
    ]

    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c) for c in cols})

    with OUT_RUNS.open("a", encoding="utf-8") as f:
        f.write(json.dumps({
            "created_at_wib": created_at_wib,
            "version": VERSION,
            "danger_count": danger_count,
            "healthy_resolution_count": healthy_resolution_count,
            "bucket_counts": dict(bucket_counts),
            "switch_recommendation": report["switch_recommendation"],
        }, ensure_ascii=False, sort_keys=True) + "\n")

    print(f"=== FORWARD OUTCOMES MISMATCH TRIAGE V1 | {MODE} ===")
    print("out_json:", OUT_JSON)
    print("out_csv :", OUT_CSV)
    print("run_log :", OUT_RUNS)
    print("old_rows:", len(old_rows), "old_unique:", len(old_by))
    print("new_rows:", len(new_rows), "new_unique:", len(new_by))
    print("danger_count:", danger_count)
    print("healthy_resolution_count:", healthy_resolution_count)
    print("switch_recommendation:", report["switch_recommendation"])
    print("")
    print(f"{'BUCKET':<30} {'COUNT':>6}")
    for k, v in bucket_counts.most_common():
        print(f"{k:<30} {v:>6}")

    print("")
    print("=== top transitions ===")
    for k, v in transition_counts.most_common(20):
        print(f"{k:<30} {v:>6}")

if __name__ == "__main__":
    main()
