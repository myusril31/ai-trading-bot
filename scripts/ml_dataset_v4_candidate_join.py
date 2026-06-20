#!/usr/bin/env python3
import json
import math
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parents[1]

SIGNALS_FILE = ROOT / "logs" / "ml_dataset_rows.jsonl"
OUTCOMES_FILE = ROOT / "logs" / "forward_outcomes_v1.jsonl"
FEATURE_FILE = ROOT / "logs" / "freqai_feature_store_v1.jsonl"

OUT_FILE = ROOT / "logs" / "ml_dataset_v4_candidate_join.jsonl"
OUT_JSON = ROOT / "reports" / "ml_dataset_v4_candidate_join_report.json"

VERSION = "ml_dataset_v4_candidate_join_20260620"

SAFE_PREFIXES = (
    "ml_", "fs_", "score_", "htf_", "liq_", "sweep_", "fvg_",
    "rr_", "atr", "volume", "ret_", "cross_", "btc_", "eth_",
)

SAFE_CONTAINS = (
    "feature", "atr", "volume", "ret_", "rank", "zscore", "trend",
)

DENY_CONTAINS = (
    "label", "outcome", "future", "tp_hit", "sl_hit", "win",
    "resolved", "first_touch", "hit_at", "hit_time", "pnl",
)

META_KEYS = {
    "signal_key", "key", "symbol", "pair", "direction", "dir",
    "created_at", "created_at_wib", "signal_time_wib",
    "signal_time_ms", "timestamp_ms", "ts_ms", "created_at_ms",
}


def read_jsonl(path):
    out = []
    if not path.exists():
        return out
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            try:
                out.append(json.loads(line))
            except Exception:
                pass
    return out


def write_jsonl(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n")


def key(r):
    return str(
        r.get("signal_key")
        or r.get("key")
        or "|".join([
            str(r.get("symbol") or r.get("pair") or ""),
            str(r.get("direction") or r.get("dir") or ""),
            str(r.get("signal_time_ms") or r.get("ts_ms") or r.get("timestamp_ms") or r.get("created_at_ms") or ""),
        ])
    )


def symbol_of(r):
    return str(r.get("symbol") or r.get("pair") or "").upper().replace("BINANCE:", "").replace(".P", "")


def is_safe_feature_key(k):
    low = str(k).lower()

    if k in META_KEYS:
        return False

    if any(x in low for x in DENY_CONTAINS):
        return False

    if low.startswith(SAFE_PREFIXES):
        return True

    if any(x in low for x in SAFE_CONTAINS):
        return True

    return False


def scalar_ok(v):
    if v is None:
        return False
    if isinstance(v, (str, int, float, bool)):
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return False
        return True
    return False


def embedded_signal_features(sig):
    out = {}
    denied = []
    for k, v in sig.items():
        low = str(k).lower()
        if any(x in low for x in DENY_CONTAINS):
            denied.append(k)
            continue
        if is_safe_feature_key(k) and scalar_ok(v):
            out[f"sigf_{k}"] = v
    return out, denied


def flatten_feature_store_row(fr):
    out = {}
    for k, v in fr.items():
        low = str(k).lower()
        if k in META_KEYS:
            continue
        if any(x in low for x in DENY_CONTAINS):
            continue
        if scalar_ok(v):
            out[f"fs_{k}"] = v
    return out


def time_num(r):
    for k in ("signal_time_ms", "timestamp_ms", "ts_ms", "created_at_ms"):
        try:
            v = r.get(k)
            if v is not None and str(v).strip():
                return int(float(v))
        except Exception:
            pass
    return None


def feature_time_num(r):
    for k in ("created_at_ms", "timestamp_ms", "ts_ms", "feature_time_ms"):
        try:
            v = r.get(k)
            if v is not None and str(v).strip():
                return int(float(v))
        except Exception:
            pass
    return None


def build_feature_index(feature_rows):
    idx = {}
    for fr in feature_rows:
        sym = symbol_of(fr)
        t = feature_time_num(fr)
        if not sym or t is None:
            continue
        idx.setdefault(sym, []).append((t, fr))
    for sym in idx:
        idx[sym].sort(key=lambda x: x[0])
    return idx


def nearest_feature_before(idx, sym, t, max_age_ms=20 * 60 * 1000):
    arr = idx.get(sym) or []
    best = None
    for ft, fr in arr:
        if ft <= t:
            best = (ft, fr)
        else:
            break
    if not best:
        return None, None
    ft, fr = best
    age = t - ft
    if age < 0 or age > max_age_ms:
        return None, None
    return fr, age


def label_from_outcome(out):
    if not out:
        return {}

    return {
        "label_win": out.get("label_win"),
        "label_R": out.get("label_R"),
        "label_target": out.get("label_target"),
        "outcome_status": out.get("outcome_status"),
        "include_ml_label": out.get("include_ml_label"),
        "exclude_label_reason": out.get("exclude_label_reason"),
    }



# === V4_BOOL_LABEL_COUNT_FIX_20260620 ===
def is_binary_label_win(v):
    if isinstance(v, bool):
        return True
    if isinstance(v, (int, float)):
        return v in (0, 1, 0.0, 1.0)
    s = str(v).strip().lower()
    return s in ("0", "1", "0.0", "1.0", "true", "false")


def is_positive_label_win(v):
    if isinstance(v, bool):
        return v is True
    if isinstance(v, (int, float)):
        return float(v) == 1.0
    s = str(v).strip().lower()
    return s in ("1", "1.0", "true", "win", "tp1", "tp2", "tp3")


def has_trainable_label(row):
    return (
        is_binary_label_win(row.get("label_win"))
        and row.get("label_R") is not None
        and str(row.get("outcome_status") or "").upper() not in ("", "OPEN", "PENDING", "DATA_GAP", "NO_FILL", "OPEN_END")
    )


def main():
    signals_raw = read_jsonl(SIGNALS_FILE)
    outcomes_raw = read_jsonl(OUTCOMES_FILE)
    features_raw = read_jsonl(FEATURE_FILE)

    # latest signal per key, preserving current dataset behavior
    sig_by_key = {}
    for r in signals_raw:
        k = key(r)
        if k:
            sig_by_key[k] = r

    out_by_key = {}
    for r in outcomes_raw:
        k = key(r)
        if k:
            out_by_key[k] = r

    feat_idx = build_feature_index(features_raw)

    rows = []
    source_counts = Counter()
    label_counts = Counter()
    denied_counter = Counter()

    for k, sig in sig_by_key.items():
        sym = symbol_of(sig)
        st = time_num(sig)

        feature_source = None
        feature_age_sec = None
        feat = {}

        if sym and st is not None:
            fr, age_ms = nearest_feature_before(feat_idx, sym, st)
            if fr:
                feat = flatten_feature_store_row(fr)
                feature_source = "FREQAI_STORE"
                feature_age_sec = round(age_ms / 1000, 3)

        if not feat:
            feat, denied = embedded_signal_features(sig)
            for d in denied:
                denied_counter[d] += 1
            if feat:
                feature_source = "SIGNAL_ROW_FALLBACK"
                feature_age_sec = 0.0

        if not feat:
            source_counts["NO_FEATURE"] += 1
            continue

        out = out_by_key.get(k) or {}

        row = {
            "dataset_version": VERSION,
            "signal_key": k,
            "symbol": sym,
            "direction": sig.get("direction") or sig.get("dir"),
            "signal_time_ms": st,
            "created_at_wib": sig.get("created_at_wib") or sig.get("signal_time_wib") or sig.get("created_at"),
            "feature_source": feature_source,
            "feature_age_sec": feature_age_sec,
        }

        row.update(label_from_outcome(out))
        row.update(feat)

        row["trainable_label"] = has_trainable_label(row)

        source_counts[feature_source] += 1
        if row["trainable_label"]:
            label_counts["trainable"] += 1
            if is_positive_label_win(row.get("label_win")):
                label_counts["wins"] += 1
            else:
                label_counts["losses"] += 1
        else:
            label_counts["not_trainable"] += 1

        rows.append(row)

    write_jsonl(OUT_FILE, rows)

    report = {
        "ok": True,
        "dataset_version": VERSION,
        "signals_raw": len(signals_raw),
        "signals_unique": len(sig_by_key),
        "outcomes_raw": len(outcomes_raw),
        "outcomes_unique": len(out_by_key),
        "feature_store_rows": len(features_raw),
        "joined_rows": len(rows),
        "feature_source_counts": dict(source_counts),
        "trainable_label_rows": label_counts["trainable"],
        "wins": label_counts["wins"],
        "losses": label_counts["losses"],
        "not_trainable_rows": label_counts["not_trainable"],
        "denied_label_like_keys_top": denied_counter.most_common(30),
        "out_file": str(OUT_FILE),
        "note": "REPORT_ONLY candidate join. Does not replace v3 dataset, live ML, or execution.",
    }

    OUT_JSON.write_text(json.dumps(report, indent=2, ensure_ascii=False))

    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
