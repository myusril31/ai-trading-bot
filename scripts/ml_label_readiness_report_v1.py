#!/usr/bin/env python3
import csv, json, math, os
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import Counter

ROOT = Path(__file__).resolve().parents[1]
WIB = timezone(timedelta(hours=7))
UTC = timezone.utc

VERSION = "ml_label_readiness_report_v1_20260617"
MODE = "REPORT_ONLY"

IN_FILE = ROOT / "logs" / "ml_dataset_v3_feature_join.jsonl"

OUT_JSON = ROOT / "reports" / "ml_label_readiness_report_v1.json"
OUT_CSV = ROOT / "reports" / "ml_label_readiness_report_v1.csv"

SNAP_JSONL = ROOT / "logs" / "ml_label_readiness_snapshots_v1.jsonl"
SNAP_CSV = ROOT / "logs" / "ml_label_readiness_snapshots_v1.csv"

MIN_LABEL_ROWS = int(os.getenv("ML_LABEL_MIN_ROWS", "100"))
MIN_SYMBOLS = int(os.getenv("ML_LABEL_MIN_SYMBOLS", "8"))
MIN_WINS = int(os.getenv("ML_LABEL_MIN_WINS", "25"))
MIN_LOSSES = int(os.getenv("ML_LABEL_MIN_LOSSES", "25"))
MAX_AGE_HOURS = float(os.getenv("ML_LABEL_MAX_AGE_HOURS", "6"))

CLOSED_TARGETS = {"TP1", "TP2", "TP3", "SL"}

def now_utc():
    return datetime.now(UTC)

def now_wib_str():
    return now_utc().astimezone(WIB).strftime("%Y-%m-%d %H:%M:%S WIB")

def parse_dt(v):
    if not v:
        return None

    s = str(v).strip().replace("Z", "+00:00")

    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except Exception:
        pass

    s2 = s.replace("T", " ").replace(" WIB", "")
    if "+" in s2:
        s2 = s2.split("+")[0]

    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s2[:26], fmt).replace(tzinfo=WIB).astimezone(UTC)
        except Exception:
            pass

    return None

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

def num(x):
    try:
        v = float(x)
        if math.isfinite(v):
            return v
    except Exception:
        pass
    return None

def is_label_row(r):
    label_r = num(r.get("label_R"))
    target = str(r.get("label_target") or r.get("first_hit") or r.get("outcome_status") or "").upper().strip()

    if label_r is None:
        return False

    if target not in CLOSED_TARGETS:
        return False

    return True

def symbol_of(r):
    return str(r.get("symbol") or "").upper().strip()

def direction_of(r):
    return str(r.get("direction") or "").upper().strip()

def row_created_at(r):
    return parse_dt(r.get("created_at_utc")) or parse_dt(r.get("created_at_wib"))

def append_jsonl(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False, sort_keys=True) + "\n")

def append_csv(path, row):
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    cols = list(row.keys())

    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        if not exists:
            w.writeheader()
        w.writerow(row)

def main():
    rows = read_jsonl(IN_FILE)
    label_rows = [r for r in rows if is_label_row(r)]

    latest_dt = None
    for r in rows:
        dt = row_created_at(r)
        if dt and (latest_dt is None or dt > latest_dt):
            latest_dt = dt

    age_hours = None
    if latest_dt:
        age_hours = round((now_utc() - latest_dt).total_seconds() / 3600, 4)

    wins = [r for r in label_rows if num(r.get("label_R")) is not None and num(r.get("label_R")) > 0]
    losses = [r for r in label_rows if num(r.get("label_R")) is not None and num(r.get("label_R")) < 0]

    label_symbols = sorted({symbol_of(r) for r in label_rows if symbol_of(r)})
    all_symbols = sorted({symbol_of(r) for r in rows if symbol_of(r)})

    target_counts = Counter(str(r.get("label_target") or r.get("first_hit") or r.get("outcome_status") or "").upper().strip() for r in label_rows)
    direction_counts = Counter(direction_of(r) for r in label_rows)
    symbol_counts = Counter(symbol_of(r) for r in label_rows)
    r_counts = Counter(str(r.get("label_R")) for r in label_rows)

    reasons = []

    if len(label_rows) < MIN_LABEL_ROWS:
        reasons.append(f"label_rows_below_min:{len(label_rows)}<{MIN_LABEL_ROWS}")

    if len(label_symbols) < MIN_SYMBOLS:
        reasons.append(f"label_symbols_below_min:{len(label_symbols)}<{MIN_SYMBOLS}")

    if len(wins) < MIN_WINS:
        reasons.append(f"wins_below_min:{len(wins)}<{MIN_WINS}")

    if len(losses) < MIN_LOSSES:
        reasons.append(f"losses_below_min:{len(losses)}<{MIN_LOSSES}")

    if age_hours is None:
        reasons.append("dataset_age_unknown")
    elif age_hours > MAX_AGE_HOURS:
        reasons.append(f"dataset_stale_hours:{age_hours}>{MAX_AGE_HOURS}")

    if reasons:
        state = "ML_LABEL_NOT_READY_ACCUMULATE"
        recommendation = "WAIT_ACCUMULATE_LABELS_REPORT_ONLY"
    else:
        state = "ML_LABEL_READY_REPORT_ONLY"
        recommendation = "ALLOW_TRAINING_REPORT_ONLY"

    created_at_wib = now_wib_str()

    report = {
        "ok": True,
        "version": VERSION,
        "mode": MODE,
        "created_at_wib": created_at_wib,
        "input_file": str(IN_FILE),
        "state": state,
        "recommendation": recommendation,
        "reasons": reasons,

        "thresholds": {
            "min_label_rows": MIN_LABEL_ROWS,
            "min_symbols": MIN_SYMBOLS,
            "min_wins": MIN_WINS,
            "min_losses": MIN_LOSSES,
            "max_age_hours": MAX_AGE_HOURS,
        },

        "rows_total": len(rows),
        "label_rows": len(label_rows),
        "wins": len(wins),
        "losses": len(losses),
        "all_symbols": len(all_symbols),
        "label_symbols": len(label_symbols),
        "age_hours": age_hours,

        "target_counts": dict(target_counts),
        "direction_counts": dict(direction_counts),
        "symbol_counts": dict(symbol_counts.most_common()),
        "label_R_counts": dict(r_counts),

        "sample_labels": label_rows[:20],
    }

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    csv_rows = []
    for sym in sorted(set(all_symbols) | set(label_symbols)):
        sym_rows = [r for r in rows if symbol_of(r) == sym]
        sym_label_rows = [r for r in label_rows if symbol_of(r) == sym]
        sym_wins = [r for r in sym_label_rows if num(r.get("label_R")) is not None and num(r.get("label_R")) > 0]
        sym_losses = [r for r in sym_label_rows if num(r.get("label_R")) is not None and num(r.get("label_R")) < 0]

        csv_rows.append({
            "created_at_wib": created_at_wib,
            "symbol": sym,
            "rows_total": len(sym_rows),
            "label_rows": len(sym_label_rows),
            "wins": len(sym_wins),
            "losses": len(sym_losses),
            "target_counts": json.dumps(dict(Counter(str(r.get("label_target") or r.get("first_hit") or r.get("outcome_status") or "").upper().strip() for r in sym_label_rows)), sort_keys=True),
            "state": state,
        })

    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        cols = ["created_at_wib", "symbol", "rows_total", "label_rows", "wins", "losses", "target_counts", "state"]
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in csv_rows:
            w.writerow(r)

    snap = {
        "created_at_wib": created_at_wib,
        "version": VERSION,
        "state": state,
        "recommendation": recommendation,
        "rows_total": len(rows),
        "label_rows": len(label_rows),
        "wins": len(wins),
        "losses": len(losses),
        "label_symbols": len(label_symbols),
        "age_hours": age_hours,
        "reasons": reasons,
    }

    append_jsonl(SNAP_JSONL, snap)
    append_csv(SNAP_CSV, snap)

    print(f"=== ML LABEL READINESS REPORT V1 | {MODE} ===")
    print("out_json:", OUT_JSON)
    print("out_csv :", OUT_CSV)
    print("snap_jsonl:", SNAP_JSONL)
    print("snap_csv :", SNAP_CSV)
    print("state:", state)
    print("recommendation:", recommendation)
    print("rows_total:", len(rows))
    print("label_rows:", len(label_rows))
    print("wins:", len(wins), "losses:", len(losses))
    print("label_symbols:", len(label_symbols))
    print("age_hours:", age_hours)
    print("reasons:", reasons)
    print("")
    print(f"{'TARGET':<10} {'COUNT':>6}")
    for k, v in target_counts.most_common():
        print(f"{k:<10} {v:>6}")

if __name__ == "__main__":
    main()
