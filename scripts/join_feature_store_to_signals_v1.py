#!/usr/bin/env python3
import json, math, bisect, os, re
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import defaultdict

ROOT = Path(__file__).resolve().parents[1]
WIB = timezone(timedelta(hours=7))
UTC = timezone.utc

SIGNAL_FILES = [
    ROOT / "logs" / "signals.jsonl",
    ROOT / "logs" / "decisions.jsonl",
    ROOT / "logs" / "vps_smc_shadow_signals.jsonl",  # VPS_SMC_SHADOW_SIGNAL_JOIN_SUPPORT_20260614
]

FEATURE_FILE = ROOT / "logs" / "freqai_feature_store_v1.jsonl"
ML_PRED_FILE = ROOT / "logs" / "ml_predictions.jsonl"
def env_path(name, default_path):
    raw = os.getenv(name)
    if not raw:
        return default_path
    pp = Path(raw)
    return pp if pp.is_absolute() else ROOT / pp

OUTCOME_FILE = env_path("FORWARD_OUTCOMES_PATH", ROOT / "logs" / "forward_outcomes_v1.jsonl")
OUTCOME_FALLBACK_FILE = ROOT / "logs" / "forward_outcomes.jsonl"
OUTCOME_STRICT_LABELS = str(os.getenv("FORWARD_OUTCOME_STRICT_LABELS", "1")).lower() not in ("0", "false", "no", "off")
OUTCOME_LOAD_META = {}
RECALC_SCORE_FILE = ROOT / "logs" / "score_v2_recalc_shadow_v1.jsonl"  # SCORE_V2_RECALC_JOIN_SUPPORT_20260614

OUT_FILE = ROOT / "logs" / "ml_dataset_v3_feature_join.jsonl"
OUT_REPORT = ROOT / "reports" / "ml_dataset_v3_feature_join_report.json"

MAX_FEATURE_AGE_MIN = int(os.getenv("FEATURE_JOIN_MAX_AGE_MIN", "20"))

FEATURE_PREFIX = "fs_"

SKIP_FEATURE_KEYS = {
    "symbol",
    "created_at_utc",
    "created_at_wib",
    "feature_version",
}

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

def payload_of(r):
    p = r.get("payload")
    return p if isinstance(p, dict) else r

def key_of(r):
    p = payload_of(r)
    return str(
        r.get("signal_key")
        or p.get("signal_key")
        or r.get("signal_id")
        or p.get("signal_id")
        or ""
    )

def symbol_of(r):
    p = payload_of(r)
    k = key_of(r)
    s = str(r.get("symbol") or p.get("symbol") or r.get("pair") or p.get("pair") or "")
    if not s and k:
        s = k.split("|")[0]
    return s.replace("BINANCE:", "").replace(".P", "").replace("/", "").upper()

def direction_of(r):
    p = payload_of(r)
    k = key_of(r)
    d = str(r.get("direction") or p.get("direction") or r.get("dir") or p.get("dir") or "")
    if not d and "|" in k:
        try:
            d = k.split("|")[1]
        except Exception:
            pass
    return d.upper()

def parse_dt(v, is_utc=False):
    if not v:
        return None
    s = str(v).replace("T", " ").replace("Z", "").replace(" WIB", "")
    if "+" in s:
        s = s.split("+")[0]
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(s[:26], fmt)
            if is_utc:
                return dt.replace(tzinfo=UTC).astimezone(WIB)
            return dt.replace(tzinfo=WIB)
        except Exception:
            pass
    return None

def dt_from_key(k):
    try:
        ms = int(str(k).split("|")[-1])
        return datetime.fromtimestamp(ms / 1000, UTC).astimezone(WIB)
    except Exception:
        return None

def row_time(r):
    p = payload_of(r)

    for k in (
        "decision_at_wib",
        "event_at_wib",
        "created_at_wib",
        "received_at_wib",
        "signal_time_wib",
        "confirmed_ts_wib",
    ):
        dt = parse_dt(r.get(k) or p.get(k), False)
        if dt:
            return dt

    for k in (
        "decision_at_utc",
        "event_at_utc",
        "created_at_utc",
        "received_at_utc",
        "logged_at_utc",
    ):
        dt = parse_dt(r.get(k) or p.get(k), True)
        if dt:
            return dt

    return dt_from_key(key_of(r))

def feature_time(r):
    return parse_dt(r.get("created_at_utc"), True) or parse_dt(r.get("created_at_wib"), False)

def latest_by_key(rows):
    d = {}
    tmap = {}
    for r in rows:
        k = key_of(r)
        if not k:
            continue
        t = row_time(r) or datetime.min.replace(tzinfo=WIB)
        if k not in d or t >= tmap[k]:
            d[k] = r
            tmap[k] = t
    return d

def pred_obj(r):
    p = r.get("prediction")
    return p if isinstance(p, dict) else r

def num(x):
    try:
        v = float(x)
        if math.isfinite(v):
            return v
    except Exception:
        pass
    return None

def outcome_allowed_for_ml(r):
    if not OUTCOME_STRICT_LABELS:
        return True

    status = str(r.get("outcome_status") or "").upper().strip()

    if r.get("include_ml_label") is not True:
        return False

    if status not in ("TP1", "TP2", "TP3", "SL"):
        return False

    if num(r.get("label_R")) is None:
        return False

    return True

def load_outcome_rows():
    global OUTCOME_LOAD_META

    rows = read_jsonl(OUTCOME_FILE)
    source_path = OUTCOME_FILE
    fallback_used = False

    if not rows and OUTCOME_FILE != OUTCOME_FALLBACK_FILE:
        rows = read_jsonl(OUTCOME_FALLBACK_FILE)
        source_path = OUTCOME_FALLBACK_FILE
        fallback_used = True

    raw_count = len(rows)

    if OUTCOME_STRICT_LABELS:
        rows = [r for r in rows if outcome_allowed_for_ml(r)]

    OUTCOME_LOAD_META = {
        "outcome_source_path": str(source_path),
        "outcome_fallback_used": fallback_used,
        "outcome_strict_labels": OUTCOME_STRICT_LABELS,
        "outcome_raw_rows": raw_count,
        "outcome_rows_after_gate": len(rows),
    }

    return rows

def score_detail_of(r):
    p = payload_of(r)
    sd = r.get("score_detail") or p.get("score_detail")
    if isinstance(sd, dict):
        return sd

    notes = str(r.get("notes") or p.get("notes") or "")
    m = re.search(r"score_v2_shadow=([0-9]+(?:\.[0-9]+)?)", notes)
    if m:
        try:
            score_v2 = float(m.group(1))
        except Exception:
            score_v2 = None
        return {
            "score_version": "shadow_notes_vps_smc_20260614",
            "score_v1": r.get("score") or p.get("score"),
            "score_v2": score_v2,
            "active_score": r.get("score") or p.get("score"),
            "priority": r.get("priority") or p.get("priority"),
        }

    return {}

def build_feature_index(feature_rows):
    by_symbol = defaultdict(list)

    for r in feature_rows:
        sym = str(r.get("symbol") or "").upper()
        dt = feature_time(r)
        if not sym or not dt:
            continue
        by_symbol[sym].append((dt, r))

    for sym in list(by_symbol.keys()):
        by_symbol[sym].sort(key=lambda x: x[0])

    return by_symbol

def nearest_feature_before(index, symbol, t):
    arr = index.get(symbol) or []
    if not arr or not t:
        return None, None

    times = [x[0] for x in arr]
    pos = bisect.bisect_right(times, t) - 1

    if pos < 0:
        return None, None

    ft, fr = arr[pos]
    age_sec = (t - ft).total_seconds()

    if age_sec < 0:
        return None, None

    if age_sec > MAX_FEATURE_AGE_MIN * 60:
        return None, age_sec

    if fr.get("feature_sanity_ok") is False:
        return None, age_sec

    return fr, age_sec

def flatten_features(fr):
    out = {}
    for k, v in fr.items():
        if k in SKIP_FEATURE_KEYS:
            continue
        if isinstance(v, (str, int, float, bool)) or v is None:
            out[FEATURE_PREFIX + k] = v
        elif isinstance(v, list):
            out[FEATURE_PREFIX + k] = ",".join(map(str, v))
    return out

def main():
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUT_REPORT.parent.mkdir(parents=True, exist_ok=True)

    signal_rows = []
    for path in SIGNAL_FILES:
        for r in read_jsonl(path):
            r["_source_file"] = str(path)
            signal_rows.append(r)

    signals = latest_by_key(signal_rows)

    ml_preds = latest_by_key(read_jsonl(ML_PRED_FILE))
    outcomes = latest_by_key(load_outcome_rows())
    score_recalc_map = latest_by_key(read_jsonl(RECALC_SCORE_FILE))

    features = read_jsonl(FEATURE_FILE)
    feature_index = build_feature_index(features)

    rows_out = []
    no_feature = 0
    no_time = 0
    bad_or_stale = 0
    joined_with_outcome = 0
    joined_with_ml = 0
    joined_with_recalc = 0

    for k, sig in signals.items():
        sym = symbol_of(sig)
        direction = direction_of(sig)
        st = row_time(sig)

        if not st:
            no_time += 1
            continue

        fr, age_sec = nearest_feature_before(feature_index, sym, st)

        if fr is None:
            no_feature += 1
            if age_sec is not None:
                bad_or_stale += 1
            continue

        sd = score_detail_of(sig)
        recalc_row = score_recalc_map.get(k) or {}

        pred_row = ml_preds.get(k)
        pred = pred_obj(pred_row) if pred_row else {}

        out_row = outcomes.get(k) or {}

        if pred_row:
            joined_with_ml += 1
        if out_row:
            joined_with_outcome += 1
        if recalc_row:
            joined_with_recalc += 1

        row = {
            "dataset_version": "ml_dataset_v3_feature_join_20260614",
            "created_at_utc": datetime.now(UTC).isoformat(),
            "created_at_wib": datetime.now(UTC).astimezone(WIB).strftime("%Y-%m-%d %H:%M:%S WIB"),

            "signal_key": k,
            "symbol": sym,
            "direction": direction,
            "signal_time_wib": st.strftime("%Y-%m-%d %H:%M:%S WIB"),
            "feature_time_wib": feature_time(fr).strftime("%Y-%m-%d %H:%M:%S WIB"),
            "feature_age_sec": round(age_sec, 3),

            "score": sig.get("score") or payload_of(sig).get("score"),
            "score_v1": sd.get("score_v1"),
            "score_v2": sd.get("score_v2"),
            "score_v2_recalc": recalc_row.get("score_v2_recalc"),
            "score_v2_bucket": recalc_row.get("score_v2_bucket"),
            "score_v2_recalc_components": recalc_row.get("components"),
            "active_score": sd.get("active_score"),
            "priority": sd.get("priority") or sig.get("priority") or payload_of(sig).get("priority"),

            "ml_model_version": pred.get("model_version"),
            "ml_decision": pred.get("decision"),
            "ml_p_win": pred.get("p_win") or pred.get("p_win_adj"),

            "label_win": out_row.get("label_win"),
            "label_R": out_row.get("label_R"),
            "label_target": out_row.get("label_target"),
            "first_hit": out_row.get("first_hit"),
            "bars_to_hit": out_row.get("bars_to_hit"),
            "outcome_status": out_row.get("outcome_status"),
        }

        row.update(flatten_features(fr))
        rows_out.append(row)

    with OUT_FILE.open("w", encoding="utf-8") as f:
        for r in rows_out:
            f.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n")

    report = {
        "ok": True,
        "dataset_version": "ml_dataset_v3_feature_join_20260614",
        "max_feature_age_min": MAX_FEATURE_AGE_MIN,
        "signals_unique": len(signals),
        "feature_rows": len(features),
        "joined_rows": len(rows_out),
        "joined_with_ml": joined_with_ml,
        "joined_with_outcome": joined_with_outcome,
        "outcome_source_path": OUTCOME_LOAD_META.get("outcome_source_path"),
        "outcome_fallback_used": OUTCOME_LOAD_META.get("outcome_fallback_used"),
        "outcome_strict_labels": OUTCOME_LOAD_META.get("outcome_strict_labels"),
        "outcome_raw_rows": OUTCOME_LOAD_META.get("outcome_raw_rows"),
        "outcome_rows_after_gate": OUTCOME_LOAD_META.get("outcome_rows_after_gate"),
        "outcome_loaded_unique": len(outcomes),
        "joined_with_recalc": joined_with_recalc,
        "no_feature_or_too_new": no_feature,
        "no_time": no_time,
        "bad_or_stale_feature": bad_or_stale,
        "out_file": str(OUT_FILE),
    }

    OUT_REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(json.dumps(report, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
