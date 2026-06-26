#!/usr/bin/env python3
import csv, json, math
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import Counter

ROOT = Path(__file__).resolve().parents[1]
WIB = timezone(timedelta(hours=7))

OLD_PATH = ROOT / "logs" / "forward_outcomes.jsonl"
NEW_PATH = ROOT / "logs" / "forward_outcomes_v1.jsonl"

OUT_JSON = ROOT / "reports" / "forward_outcomes_compare_v1.json"
OUT_CSV = ROOT / "reports" / "forward_outcomes_compare_v1.csv"
OUT_RUNS = ROOT / "logs" / "forward_outcomes_compare_runs_v1.jsonl"

VERSION = "forward_outcomes_compare_v1_20260617"
MODE = "REPORT_ONLY"

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

def norm_status(x):
    s = str(x or "").upper().strip()
    if s in ("WIN", "TP", "TAKE_PROFIT"):
        return "TP"
    if s in ("LOSS", "STOP", "STOP_LOSS"):
        return "SL"
    return s

def norm_target(x):
    return str(x or "").upper().strip()

def norm_bool(x):
    if x is None:
        return None
    if isinstance(x, bool):
        return x
    s = str(x).lower().strip()
    if s in ("true", "1", "yes", "y"):
        return True
    if s in ("false", "0", "no", "n"):
        return False
    return None

def norm_r(x):
    try:
        v = float(x)
        if math.isfinite(v):
            return round(v, 6)
    except Exception:
        pass
    return None

def classify(k, old, new):
    if old is None and new is not None:
        return "NEW_ONLY"
    if old is not None and new is None:
        return "OLD_ONLY"

    old_status = norm_status(old.get("outcome_status") or old.get("status"))
    new_status = norm_status(new.get("outcome_status") or new.get("status"))

    old_target = norm_target(old.get("label_target") or old.get("first_hit"))
    new_target = norm_target(new.get("label_target") or new.get("first_hit"))

    old_win = norm_bool(old.get("label_win"))
    new_win = norm_bool(new.get("label_win"))

    old_r = norm_r(old.get("label_R"))
    new_r = norm_r(new.get("label_R"))

    status_match = old_status == new_status
    target_match = old_target == new_target
    win_match = old_win == new_win
    r_match = old_r == new_r

    if status_match and target_match and win_match and r_match:
        state = "MATCH"
    elif old_status != new_status:
        state = "STATUS_MISMATCH"
    elif old_target != new_target:
        state = "TARGET_MISMATCH"
    elif old_win != new_win:
        state = "WIN_MISMATCH"
    elif old_r != new_r:
        state = "R_MISMATCH"
    else:
        state = "MISMATCH_OTHER"

    return state

def main():
    old_rows = read_jsonl(OLD_PATH)
    new_rows = read_jsonl(NEW_PATH)

    old_by = latest_by_key(old_rows)
    new_by = latest_by_key(new_rows)

    keys = sorted(set(old_by) | set(new_by))

    rows = []
    for k in keys:
        old = old_by.get(k)
        new = new_by.get(k)
        state = classify(k, old, new)

        rows.append({
            "signal_key": k,
            "symbol": (new or old or {}).get("symbol"),
            "direction": (new or old or {}).get("direction"),
            "compare_state": state,

            "old_status": norm_status((old or {}).get("outcome_status") or (old or {}).get("status")),
            "new_status": norm_status((new or {}).get("outcome_status") or (new or {}).get("status")),

            "old_target": norm_target((old or {}).get("label_target") or (old or {}).get("first_hit")),
            "new_target": norm_target((new or {}).get("label_target") or (new or {}).get("first_hit")),

            "old_win": norm_bool((old or {}).get("label_win")),
            "new_win": norm_bool((new or {}).get("label_win")),

            "old_R": norm_r((old or {}).get("label_R")),
            "new_R": norm_r((new or {}).get("label_R")),

            "old_include_ml_label": norm_bool((old or {}).get("include_ml_label")),
            "new_include_ml_label": norm_bool((new or {}).get("include_ml_label")),

            "old_reason": (old or {}).get("exclude_label_reason"),
            "new_reason": (new or {}).get("exclude_label_reason"),
        })

    state_counts = dict(Counter(r["compare_state"] for r in rows))

    overlap = [r for r in rows if r["compare_state"] not in ("OLD_ONLY", "NEW_ONLY")]
    match = [r for r in overlap if r["compare_state"] == "MATCH"]

    overlap_count = len(overlap)
    match_rate = round(len(match) / overlap_count, 6) if overlap_count else 0.0

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
        "overlap_count": overlap_count,
        "match_count": len(match),
        "match_rate": match_rate,
        "state_counts": state_counts,
        "note": "REPORT_ONLY compare. Does not switch dataset join input.",
        "samples": rows[:20],
    }

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    cols = [
        "signal_key","symbol","direction","compare_state",
        "old_status","new_status",
        "old_target","new_target",
        "old_win","new_win",
        "old_R","new_R",
        "old_include_ml_label","new_include_ml_label",
        "old_reason","new_reason",
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
            "old_rows": len(old_rows),
            "new_rows": len(new_rows),
            "overlap_count": overlap_count,
            "match_count": len(match),
            "match_rate": match_rate,
            "state_counts": state_counts,
        }, ensure_ascii=False, sort_keys=True) + "\n")

    print(f"=== FORWARD OUTCOMES COMPARE V1 | {MODE} ===")
    print("old_path:", OLD_PATH)
    print("new_path:", NEW_PATH)
    print("out_json:", OUT_JSON)
    print("out_csv :", OUT_CSV)
    print("run_log :", OUT_RUNS)
    print("old_rows:", len(old_rows), "old_unique:", len(old_by))
    print("new_rows:", len(new_rows), "new_unique:", len(new_by))
    print("overlap_count:", overlap_count)
    print("match_count:", len(match))
    print("match_rate:", match_rate)
    print("state_counts:", state_counts)
    print("")
    print(f"{'STATE':<18} {'COUNT':>6}")
    for k, v in sorted(state_counts.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f"{k:<18} {v:>6}")

if __name__ == "__main__":
    main()
