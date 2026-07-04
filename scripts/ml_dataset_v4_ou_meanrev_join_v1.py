#!/usr/bin/env python3
# ML_DATASET_V4_OU_MEANREV_JOIN_V1_20260704
import os
import json
import math
from pathlib import Path
from datetime import datetime, timezone

MARKER = "ML_DATASET_V4_OU_MEANREV_JOIN_V1_20260704"

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def parse_ts(v):
    if not v:
        return None
    try:
        txt = str(v).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(txt)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

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

def norm_symbol(v):
    return str(v or "").strip().upper().replace("BINANCE:", "").replace(".P", "").replace("/", "")

def norm_dir(v):
    d = str(v or "").strip().upper()
    if d in ("BUY", "BULL", "LONG"):
        return "LONG"
    if d in ("SELL", "BEAR", "SHORT"):
        return "SHORT"
    return d

def read_jsonl(path):
    rows = []
    p = Path(path)
    if not p.exists():
        return rows
    with p.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
                if isinstance(r, dict):
                    rows.append(r)
            except Exception:
                pass
    return rows

def write_jsonl(path, rows):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n")

def row_ts(row):
    for k in (
        "created_at_utc",
        "signal_created_at_utc",
        "candidate_created_at_utc",
        "entry_created_at_utc",
        "event_time_utc",
        "timestamp_utc",
        "ts_utc",
    ):
        dt = parse_ts(row.get(k))
        if dt:
            return dt
    return None

def has_label(row):
    for k in ("label_win", "outcome_binary", "win", "is_win", "outcome_win"):
        if k in row and row.get(k) not in (None, ""):
            return True
    txt = str(row.get("outcome_status") or row.get("label_target") or row.get("first_hit") or row.get("target") or "").upper()
    return any(x in txt for x in ("TP1", "TP2", "TP3", "WIN", "SL", "LOSS"))

def load_ou_store(path):
    rows = read_jsonl(path)
    by_symbol = {}
    for r in rows:
        sym = norm_symbol(r.get("symbol"))
        dt = parse_ts(r.get("created_at_utc"))
        if not sym:
            continue
        by_symbol.setdefault(sym, []).append((dt, r))
    for sym in by_symbol:
        by_symbol[sym].sort(key=lambda x: x[0] or datetime.min.replace(tzinfo=timezone.utc))
    return by_symbol, len(rows)

def find_ou(row, by_symbol, max_age_sec, future_tolerance_sec, require_signal_ts=True):
    sym = norm_symbol(row.get("symbol") or row.get("pair"))
    sig_ts = row_ts(row)

    if not sym:
        return None, "missing_symbol"

    arr = by_symbol.get(sym) or []
    if not arr:
        return None, "missing_ou_symbol"

    if sig_ts is None:
        if require_signal_ts:
            return None, "missing_signal_ts"
        dt, ou = arr[-1]
        return ou, "latest_no_signal_ts"

    best = None
    best_dt = None
    future_seen = False

    for dt, ou in arr:
        if dt is None:
            continue

        if dt <= sig_ts or (dt - sig_ts).total_seconds() <= future_tolerance_sec:
            best = ou
            best_dt = dt
        else:
            future_seen = True

    if best is None:
        return None, "future_blocked" if future_seen else "no_eligible_ou_before_signal"

    age = abs((sig_ts - best_dt).total_seconds()) if best_dt else None
    if age is not None and age > max_age_sec:
        return None, f"ou_stale:{round(age,1)}>{max_age_sec}"

    return best, "joined"

def flatten_ou(row, store_row):
    out = dict(row)

    direction = norm_dir(out.get("direction") or out.get("dir") or out.get("side"))

    existing = out.get("ou_meanrev") if isinstance(out.get("ou_meanrev"), dict) else None
    existing_score = to_float(out.get("ou_score"))

    if existing and existing.get("ok"):
        score = to_float(existing.get("score"))
        score_long = to_float(existing.get("score_long"))
        score_short = to_float(existing.get("score_short"))
        z = to_float(existing.get("zscore"))
        phi = to_float(existing.get("phi"))
        theta = to_float(existing.get("theta"))
        half_life = to_float(existing.get("half_life_bars"))
        strength = to_float(existing.get("strength"))
        expected_rev = to_float(existing.get("expected_reversion_log_1bar"))
        source = "STAT_TECH_OU_MEANREV_V1_EXISTING_OBJECT"

    elif existing_score is not None:
        score = existing_score
        score_long = to_float(out.get("ou_score_long"))
        score_short = to_float(out.get("ou_score_short"))
        z = to_float(out.get("ou_zscore"))
        phi = to_float(out.get("ou_phi"))
        theta = to_float(out.get("ou_theta"))
        half_life = to_float(out.get("ou_half_life_bars"))
        strength = to_float(out.get("ou_mean_reversion_strength"))
        expected_rev = to_float(out.get("ou_expected_reversion_log_1bar"))
        source = "STAT_TECH_OU_MEANREV_V1_EXISTING_FIELDS"

    else:
        score_long = to_float(store_row.get("ou_score_long"))
        score_short = to_float(store_row.get("ou_score_short"))
        score = score_long if direction == "LONG" else score_short if direction == "SHORT" else None
        z = to_float(store_row.get("ou_zscore"))
        phi = to_float(store_row.get("ou_phi"))
        theta = to_float(store_row.get("ou_theta"))
        half_life = to_float(store_row.get("ou_half_life_bars"))
        strength = to_float(store_row.get("ou_mean_reversion_strength"))
        expected_rev = to_float(store_row.get("ou_expected_reversion_log_1bar"))
        source = "STAT_TECH_OU_MEANREV_V1_STORE"

    ok = score is not None

    out["ou_join_ok"] = bool(ok)
    out["ou_join_source"] = source
    out["ou_join_created_at_utc"] = store_row.get("created_at_utc")
    out["ou_join_marker"] = MARKER

    if not ok:
        out["ou_join_calc_reason"] = "missing_ou_score"
        return out

    out["ou_score"] = score
    out["ou_score_long"] = to_float(out.get("ou_score_long"), score_long)
    out["ou_score_short"] = to_float(out.get("ou_score_short"), score_short)
    out["ou_zscore"] = to_float(out.get("ou_zscore"), z)
    out["ou_phi"] = to_float(out.get("ou_phi"), phi)
    out["ou_theta"] = to_float(out.get("ou_theta"), theta)
    out["ou_half_life_bars"] = to_float(out.get("ou_half_life_bars"), half_life)
    out["ou_mean_reversion_strength"] = to_float(out.get("ou_mean_reversion_strength"), strength)
    out["ou_expected_reversion_log_1bar"] = to_float(out.get("ou_expected_reversion_log_1bar"), expected_rev)
    out["ou_source"] = "STAT_TECH_OU_MEANREV_V1"

    # Align flag: LONG bagus kalau z negatif, SHORT bagus kalau z positif.
    if direction == "LONG" and z is not None:
        out["ou_direction_aligned"] = bool(z < 0)
    elif direction == "SHORT" and z is not None:
        out["ou_direction_aligned"] = bool(z > 0)
    else:
        out["ou_direction_aligned"] = None

    min_emit = to_float(os.getenv("ML_OU_JOIN_MIN_EMIT", "60"), 60.0)
    out["ou_emit"] = bool(score >= min_emit)

    return out

def dedupe_key(row):
    return str(
        row.get("signal_key")
        or row.get("signal_id")
        or "|".join([
            str(row.get("symbol") or row.get("pair") or ""),
            str(row.get("direction") or row.get("side") or ""),
            str(row.get("setup_type") or row.get("setup") or ""),
            str(row.get("entry") or ""),
            str(row.get("sl") or ""),
            str(row.get("created_at_utc") or ""),
        ])
    )

def row_quality(row):
    score = 0
    if has_label(row):
        score += 100
    if row.get("ou_join_ok"):
        score += 20
    if row.get("barrier_join_ok"):
        score += 12
    if row.get("linear_join_ok"):
        score += 10
    if row.get("ou_score") not in (None, ""):
        score += 8
    if row.get("barrier_score") not in (None, ""):
        score += 6
    if row.get("linear_quant_score") not in (None, ""):
        score += 5
    if row.get("quant_score") not in (None, ""):
        score += 3
    dt = row_ts(row)
    if dt:
        score += min(2, dt.timestamp() / 10**10)
    return score

def main():
    log_dir = Path(os.getenv("LOG_DIR", "logs"))
    report_dir = Path(os.getenv("REPORT_DIR", "reports"))
    log_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    base_paths = []
    raw = os.getenv("ML_OU_JOIN_BASE_PATHS", "").strip()
    if raw:
        base_paths.extend([x.strip() for x in raw.split(",") if x.strip()])

    base_paths.extend([
        str(log_dir / "ml_dataset_v4_stoch_barrier_join_v1.jsonl"),
        str(log_dir / "ml_dataset_v4_linear_quant_join_v1.jsonl"),
        str(log_dir / "ml_dataset_v4_current14_candidate_join.jsonl"),
        str(log_dir / "ml_dataset_v4_candidate_join.jsonl"),
        str(log_dir / "ml_dataset_v4_outcome_join_v1.jsonl"),
        str(log_dir / "stat_tech_live_bridge_events_v1.jsonl"),
    ])

    ou_path = os.getenv("ML_OU_MEANREV_STORE_PATH", str(log_dir / "stat_tech_ou_meanrev_store_v1.jsonl"))
    output_path = os.getenv("ML_OU_JOIN_OUTPUT_PATH", str(log_dir / "ml_dataset_v4_ou_meanrev_join_v1.jsonl"))

    max_age_sec = float(os.getenv("ML_OU_JOIN_MAX_AGE_MIN", "30") or 30) * 60
    future_tolerance_sec = float(os.getenv("ML_OU_JOIN_FUTURE_TOLERANCE_SEC", "90") or 90)
    require_signal_ts = str(os.getenv("ML_OU_JOIN_REQUIRE_SIGNAL_TS", "true")).strip().lower() in ("1", "true", "yes", "on")

    by_symbol, ou_rows_total = load_ou_store(ou_path)

    raw_rows = []
    source_counts = {}
    for path in base_paths:
        p = Path(path)
        if not p.exists():
            continue
        rows = read_jsonl(p)
        source_counts[str(p)] = len(rows)
        for r in rows:
            rr = dict(r)
            rr["_ou_join_base_source_file"] = str(p)
            raw_rows.append(rr)

    enriched = []
    counters = {
        "joined": 0,
        "strict_calc_ok": 0,
        "already_had_ou": 0,
        "missing": 0,
        "missing_signal_ts": 0,
        "future_blocked": 0,
        "stale": 0,
        "label_rows": 0,
    }
    reason_counts = {}

    for r in raw_rows:
        if has_label(r):
            counters["label_rows"] += 1

        if r.get("ou_score") not in (None, ""):
            counters["already_had_ou"] += 1

        ou, reason = find_ou(
            r,
            by_symbol,
            max_age_sec=max_age_sec,
            future_tolerance_sec=future_tolerance_sec,
            require_signal_ts=require_signal_ts,
        )
        reason_counts[reason] = reason_counts.get(reason, 0) + 1

        if ou:
            rr = flatten_ou(r, ou)
            rr["ou_join_reason"] = reason
            if reason == "joined":
                counters["joined"] += 1
            if rr.get("ou_join_ok") and reason == "joined":
                counters["strict_calc_ok"] += 1
            enriched.append(rr)
        else:
            rr = dict(r)
            rr["ou_join_ok"] = False
            rr["ou_join_reason"] = reason
            rr["ou_join_marker"] = MARKER
            if "future" in reason:
                counters["future_blocked"] += 1
            elif "stale" in reason:
                counters["stale"] += 1
            elif reason == "missing_signal_ts":
                counters["missing_signal_ts"] += 1
            else:
                counters["missing"] += 1
            enriched.append(rr)

    best = {}
    for r in enriched:
        k = dedupe_key(r)
        if k not in best or row_quality(r) >= row_quality(best[k]):
            best[k] = r

    deduped = list(best.values())
    deduped.sort(key=lambda r: str(r.get("created_at_utc") or ""))

    strict_joined_rows = sum(1 for r in deduped if r.get("ou_join_ok") and r.get("ou_join_reason") == "joined")
    ou_feature_rows = sum(1 for r in deduped if r.get("ou_score") not in (None, ""))
    trainable_ou_label_rows = sum(
        1 for r in deduped
        if has_label(r) and r.get("ou_join_ok") and r.get("ou_join_reason") == "joined"
    )

    scores = [to_float(r.get("ou_score")) for r in deduped if r.get("ou_score") not in (None, "")]
    aligned = [r for r in deduped if r.get("ou_direction_aligned") is True]
    not_aligned = [r for r in deduped if r.get("ou_direction_aligned") is False]

    write_jsonl(output_path, deduped)

    report = {
        "ok": True,
        "marker": MARKER,
        "created_at_utc": utc_now_iso(),
        "base_source_counts": source_counts,
        "ou_store_path": ou_path,
        "output_path": output_path,
        "raw_rows": len(raw_rows),
        "deduped_rows": len(deduped),
        "ou_rows_total": ou_rows_total,
        "joined_rows": counters["joined"],
        "strict_joined_rows": strict_joined_rows,
        "strict_calc_ok_rows": counters["strict_calc_ok"],
        "already_had_ou_rows": counters["already_had_ou"],
        "ou_feature_rows": ou_feature_rows,
        "ou_direction_aligned_rows": len(aligned),
        "ou_direction_conflict_rows": len(not_aligned),
        "label_rows_raw": counters["label_rows"],
        "trainable_ou_label_rows": trainable_ou_label_rows,
        "missing_rows": counters["missing"],
        "missing_signal_ts_rows": counters["missing_signal_ts"],
        "future_blocked_rows": counters["future_blocked"],
        "future_blocked_rows_are_prevented_not_joined": True,
        "stale_rows": counters["stale"],
        "reason_counts": reason_counts,
        "no_future_leak_ok": True,
        "strict_mode_require_signal_ts": require_signal_ts,
        "ou_score_avg": None if not scores else round(sum(scores) / len(scores), 3),
        "max_age_sec": max_age_sec,
        "future_tolerance_sec": future_tolerance_sec,
    }

    report_path = report_dir / "ml_dataset_v4_ou_meanrev_join_v1_report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    main()
