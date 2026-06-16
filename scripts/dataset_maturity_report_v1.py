#!/usr/bin/env python3
import csv, json, math
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import Counter

ROOT = Path(__file__).resolve().parents[1]
WIB = timezone(timedelta(hours=7))

OUT_JSON = ROOT / "reports" / "dataset_maturity_report_v1.json"
OUT_CSV = ROOT / "reports" / "dataset_maturity_report_v1.csv"

OUT_SNAP_JSONL = ROOT / "logs" / "dataset_maturity_snapshots_v1.jsonl"
OUT_SNAP_CSV = ROOT / "logs" / "dataset_maturity_snapshots_v1.csv"

VERSION = "dataset_maturity_report_v1_20260617"
MODE = "REPORT_ONLY"

COMPONENTS = [
    {
        "name": "score_v2_recalc_shadow",
        "path": ROOT / "logs" / "score_v2_recalc_shadow_v1.jsonl",
        "kind": "jsonl",
        "min_rows": 100,
        "min_symbols": 10,
        "max_age_hours": 6,
    },
    {
        "name": "ml_dataset_feature_join",
        "path": ROOT / "logs" / "ml_dataset_v3_feature_join.jsonl",
        "kind": "jsonl",
        "min_rows": 100,
        "min_symbols": 10,
        "max_age_hours": 6,
    },
    {
        "name": "forward_outcomes",
        "path": ROOT / "logs" / "forward_outcomes.jsonl",
        "kind": "jsonl",
        "min_rows": 50,
        "min_symbols": 8,
        "max_age_hours": 48,
    },
    {
        "name": "pair_league_policy_snapshots",
        "path": ROOT / "logs" / "pair_league_policy_snapshots_v1.csv",
        "kind": "csv",
        "min_rows": 28,
        "min_symbols": 10,
        "max_age_hours": 6,
    },
    {
        "name": "pair_policy_calibration_snapshots",
        "path": ROOT / "logs" / "pair_policy_calibration_snapshots_v1.csv",
        "kind": "csv",
        "min_rows": 14,
        "min_symbols": 10,
        "max_age_hours": 6,
    },
    {
        "name": "orderbook_pricing_snapshots",
        "path": ROOT / "logs" / "orderbook_pricing_snapshots_v1.csv",
        "kind": "csv",
        "min_rows": 56,
        "min_symbols": 10,
        "max_age_hours": 2,
    },
    {
        "name": "orderbook_stability_snapshots",
        "path": ROOT / "logs" / "orderbook_stability_snapshots_v1.csv",
        "kind": "csv",
        "min_rows": 28,
        "min_symbols": 10,
        "max_age_hours": 2,
    },
    {
        "name": "orderbook_shadow_guard_snapshots",
        "path": ROOT / "logs" / "orderbook_shadow_guard_snapshots_v1.csv",
        "kind": "csv",
        "min_rows": 14,
        "min_symbols": 10,
        "max_age_hours": 2,
    },
    {
        "name": "orderbook_shadow_guard_validation",
        "path": ROOT / "logs" / "orderbook_shadow_guard_validation_snapshots_v1.csv",
        "kind": "csv",
        "min_rows": 14,
        "min_symbols": 10,
        "max_age_hours": 2,
    },
    {
        "name": "latest_freqai_features",
        "path": ROOT / "state" / "features" / "latest_freqai_features_v1.json",
        "kind": "json",
        "min_rows": 1,
        "min_symbols": 8,
        "max_age_hours": 6,
    },
]

SYMBOL_KEYS = [
    "symbol", "pair", "s", "sym", "ticker",
]

TIME_KEYS = [
    "created_at_wib",
    "created_at_utc",
    "created_at",
    "timestamp",
    "ts",
    "time",
    "event_time",
    "last_seen_wib",
]

def now_wib():
    return datetime.now(timezone.utc).astimezone(WIB)

def now_wib_str():
    return now_wib().strftime("%Y-%m-%d %H:%M:%S WIB")

def parse_time(v):
    if v is None:
        return None

    s = str(v).strip()
    if not s:
        return None

    if s.endswith(" WIB"):
        s2 = s.replace(" WIB", "")
        try:
            return datetime.strptime(s2[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=WIB)
        except Exception:
            pass

    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(s[:19], fmt)
            return dt.replace(tzinfo=timezone.utc).astimezone(WIB)
        except Exception:
            pass

    try:
        x = float(s)
        if x > 10_000_000_000:
            x = x / 1000.0
        return datetime.fromtimestamp(x, tz=timezone.utc).astimezone(WIB)
    except Exception:
        return None

def extract_symbol(row):
    if not isinstance(row, dict):
        return None
    for k in SYMBOL_KEYS:
        v = row.get(k)
        if v:
            return str(v).upper().replace("/", "").replace(":USDT", "").strip()
    return None

def extract_time(row):
    if not isinstance(row, dict):
        return None
    for k in TIME_KEYS:
        if k in row:
            dt = parse_time(row.get(k))
            if dt:
                return dt
    return None

def iter_jsonl(path, limit=None):
    rows = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                rows.append({"_parse_error": True, "_raw": line[:200]})
            if limit and len(rows) >= limit:
                break
    return rows

def read_csv_rows(path):
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8", errors="ignore") as f:
        return list(csv.DictReader(f))

def read_json_component(path):
    if not path.exists():
        return []
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return [{"_parse_error": True}]

    if isinstance(obj, dict):
        if isinstance(obj.get("rows"), list):
            return obj["rows"]
        if isinstance(obj.get("data"), list):
            return obj["data"]
        if isinstance(obj.get("features"), dict):
            return [
                {"symbol": k, **(v if isinstance(v, dict) else {"value": v})}
                for k, v in obj["features"].items()
            ]
        return [obj]

    if isinstance(obj, list):
        return obj

    return [{"value": obj}]

def load_component(comp):
    path = comp["path"]
    kind = comp["kind"]

    if kind == "jsonl":
        return iter_jsonl(path)
    if kind == "csv":
        return read_csv_rows(path)
    if kind == "json":
        return read_json_component(path)

    return []

def classify(exists, row_count, symbol_count, last_age_hours, min_rows, min_symbols, max_age_hours, parse_error_count):
    reasons = []

    if not exists:
        return "MISSING", ["file_missing"]

    if parse_error_count > 0 and row_count == parse_error_count:
        return "BROKEN", ["all_rows_parse_error"]

    if row_count <= 0:
        return "EMPTY", ["row_count_zero"]

    if last_age_hours is not None and last_age_hours > max_age_hours:
        reasons.append(f"stale_age_gt_{max_age_hours}h:{round(last_age_hours,2)}")

    if row_count < min_rows:
        reasons.append(f"row_count_lt_{min_rows}:{row_count}")

    if symbol_count < min_symbols:
        reasons.append(f"symbol_count_lt_{min_symbols}:{symbol_count}")

    if parse_error_count > 0:
        reasons.append(f"parse_errors:{parse_error_count}")

    if reasons:
        if any(r.startswith("stale") for r in reasons):
            return "STALE_OR_PARTIAL", reasons
        return "WARMUP_OR_PARTIAL", reasons

    return "MATURE_ENOUGH_REPORT_ONLY", ["meets_minimum_dataset_thresholds"]

def summarize_component(comp):
    path = comp["path"]
    exists = path.exists()

    rows = load_component(comp) if exists else []
    row_count = len(rows)

    symbols = set()
    last_ts = None
    parse_error_count = 0

    for r in rows:
        if isinstance(r, dict) and r.get("_parse_error"):
            parse_error_count += 1

        sym = extract_symbol(r)
        if sym:
            symbols.add(sym)

        dt = extract_time(r)
        if dt and (last_ts is None or dt > last_ts):
            last_ts = dt

    if last_ts is None and exists:
        try:
            last_ts = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).astimezone(WIB)
        except Exception:
            last_ts = None

    age_hours = None
    if last_ts:
        age_hours = round((now_wib() - last_ts).total_seconds() / 3600.0, 4)

    state, reasons = classify(
        exists=exists,
        row_count=row_count,
        symbol_count=len(symbols),
        last_age_hours=age_hours,
        min_rows=comp["min_rows"],
        min_symbols=comp["min_symbols"],
        max_age_hours=comp["max_age_hours"],
        parse_error_count=parse_error_count,
    )

    size_bytes = path.stat().st_size if exists else 0

    return {
        "component": comp["name"],
        "mode": MODE,
        "path": str(path),
        "kind": comp["kind"],
        "exists": exists,
        "size_bytes": size_bytes,
        "row_count": row_count,
        "symbol_count": len(symbols),
        "last_seen_wib": last_ts.strftime("%Y-%m-%d %H:%M:%S WIB") if last_ts else None,
        "age_hours": age_hours,
        "min_rows": comp["min_rows"],
        "min_symbols": comp["min_symbols"],
        "max_age_hours": comp["max_age_hours"],
        "parse_error_count": parse_error_count,
        "maturity_state": state,
        "recommendation": "REPORT_ONLY_FIX_OR_WAIT" if state != "MATURE_ENOUGH_REPORT_ONLY" else "REPORT_ONLY_OK",
        "reasons": reasons,
    }

def main():
    created_at_wib = now_wib_str()

    rows = [summarize_component(c) for c in COMPONENTS]
    state_counts = dict(Counter(r["maturity_state"] for r in rows))

    mature_count = state_counts.get("MATURE_ENOUGH_REPORT_ONLY", 0)
    missing_count = state_counts.get("MISSING", 0)
    broken_count = state_counts.get("BROKEN", 0)

    overall_state = "DATASET_WARMUP"
    if broken_count > 0 or missing_count >= 3:
        overall_state = "DATASET_NOT_READY"
    elif mature_count >= max(1, int(len(rows) * 0.70)):
        overall_state = "DATASET_USABLE_REPORT_ONLY"

    report = {
        "ok": True,
        "version": VERSION,
        "mode": MODE,
        "created_at_wib": created_at_wib,
        "overall_state": overall_state,
        "component_count": len(rows),
        "state_counts": state_counts,
        "note": "REPORT_ONLY. Does not train models, modify live gates, or change trading behavior.",
        "rows": rows,
    }

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2))

    cols = [
        "component","mode","path","kind","exists","size_bytes",
        "row_count","symbol_count","last_seen_wib","age_hours",
        "min_rows","min_symbols","max_age_hours","parse_error_count",
        "maturity_state","recommendation","reasons",
    ]

    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            row = {c: r.get(c) for c in cols}
            row["reasons"] = json.dumps(row.get("reasons") or [], ensure_ascii=False)
            w.writerow(row)

    OUT_SNAP_JSONL.parent.mkdir(parents=True, exist_ok=True)
    snap_cols = ["created_at_wib", "version", *cols]

    snap_rows = []
    for r in rows:
        rr = {"created_at_wib": created_at_wib, "version": VERSION, **r}
        snap_rows.append(rr)

    with OUT_SNAP_JSONL.open("a", encoding="utf-8") as f:
        for rr in snap_rows:
            f.write(json.dumps(rr, ensure_ascii=False, sort_keys=True) + "\n")

    snap_csv_exists = OUT_SNAP_CSV.exists() and OUT_SNAP_CSV.stat().st_size > 0
    with OUT_SNAP_CSV.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=snap_cols)
        if not snap_csv_exists:
            w.writeheader()
        for rr in snap_rows:
            row = {c: rr.get(c) for c in snap_cols}
            row["reasons"] = json.dumps(row.get("reasons") or [], ensure_ascii=False)
            w.writerow(row)

    print(f"=== DATASET MATURITY REPORT V1 | {MODE} ===")
    print("out_json:", OUT_JSON)
    print("out_csv :", OUT_CSV)
    print("snap_jsonl:", OUT_SNAP_JSONL)
    print("snap_csv :", OUT_SNAP_CSV)
    print("overall_state:", overall_state)
    print("state_counts:", state_counts)
    print("")
    print(f"{'COMPONENT':<38} {'STATE':<28} {'ROWS':>7} {'SYMS':>5} {'AGE_H':>8} {'REC'}")
    for r in rows:
        print(
            f"{r['component']:<38} {r['maturity_state']:<28} "
            f"{r['row_count']:>7} {r['symbol_count']:>5} "
            f"{str(r['age_hours'] if r['age_hours'] is not None else 'NA'):>8} "
            f"{r['recommendation']}"
        )

if __name__ == "__main__":
    main()
