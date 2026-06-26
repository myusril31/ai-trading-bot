#!/usr/bin/env python3
import json, csv, math, re
from pathlib import Path
from collections import Counter, defaultdict
from statistics import mean, median
from datetime import datetime, timezone, timedelta

WIB = timezone(timedelta(hours=7))
UTC = timezone.utc

LOG_DIR = Path("logs")
REPORT_DIR = Path("reports")
REPORT_DIR.mkdir(exist_ok=True)

PRED_FILE = LOG_DIR / "ml_predictions.jsonl"
OUTCOME_FILE = LOG_DIR / "forward_outcomes.jsonl"
OUT_CSV = REPORT_DIR / "jim_quant_signal_outcomes.csv"

KEYWORDS = re.compile(
    r"(jim|simons|renaissance|quant|competitor|stat_arb|stat-arb|residual|logistic_v1|shadow_only|boost|avoid|hold)",
    re.I,
)

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

def key_of(r):
    return str(r.get("signal_key") or r.get("signal_id") or "")

def pred_obj(r):
    return r.get("prediction") if isinstance(r.get("prediction"), dict) else r

def symbol_from_key(k):
    try:
        return str(k).split("|")[0].replace("BINANCE:", "").replace(".P", "")
    except Exception:
        return ""

def direction_from_key(k):
    try:
        return str(k).split("|")[1].upper()
    except Exception:
        return ""

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
    for k in ("created_at_wib","event_at_wib","decision_at_wib","signal_time_wib","hit_time_wib","evaluated_at_wib"):
        dt = parse_dt(r.get(k), False)
        if dt:
            return dt
    for k in ("created_at_utc","event_at_utc","decision_at_utc","hit_time_utc","evaluated_at_utc","updated_at_utc"):
        dt = parse_dt(r.get(k), True)
        if dt:
            return dt
    return dt_from_key(key_of(r))

def num(x):
    try:
        if x is None or x == "":
            return None
        v = float(x)
        if math.isfinite(v):
            return v
    except Exception:
        return None
    return None

def is_jim_quant_prediction(r):
    p = pred_obj(r)

    model = str(p.get("model_version") or r.get("model_version") or "")
    effect = str(p.get("decision_effect") or r.get("decision_effect") or "")
    action = str(p.get("ml_action") or r.get("ml_action") or "")
    engine = str(p.get("engine") or r.get("engine") or "")
    source = str(p.get("source") or r.get("source") or "")
    event_type = str(p.get("event_type") or r.get("event_type") or "")
    strategy = str(p.get("strategy") or r.get("strategy") or "")
    signal_family = str(p.get("signal_family") or r.get("signal_family") or "")

    hay = " ".join([model, effect, action, engine, source, event_type, strategy, signal_family])

    # Old Jim/competitor quant mostly appeared as shadow logistic scoring
    if model == "logistic_v1":
        return True
    if effect.upper() == "SHADOW_ONLY":
        return True
    if action.upper() in ("BOOST", "HOLD", "AVOID"):
        return True
    if KEYWORDS.search(hay):
        return True

    return False

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

pred_rows = [r for r in read_jsonl(PRED_FILE) if is_jim_quant_prediction(r)]
pred_latest = latest_by_key(pred_rows)
out_latest = latest_by_key(read_jsonl(OUTCOME_FILE))

joined = []

for k, pr in pred_latest.items():
    p = pred_obj(pr)
    o = out_latest.get(k)

    row = {
        "signal_key": k,
        "date_wib": (row_time(pr) or dt_from_key(k)).strftime("%Y-%m-%d") if (row_time(pr) or dt_from_key(k)) else "",
        "symbol": symbol_from_key(k) or str(pr.get("symbol") or pr.get("pair") or ""),
        "direction": direction_from_key(k) or str(pr.get("direction") or ""),
        "model_version": p.get("model_version") or pr.get("model_version"),
        "p_win": p.get("p_win") or p.get("p_win_adj") or pr.get("p_win"),
        "ml_action": p.get("ml_action") or pr.get("ml_action") or p.get("decision"),
        "decision_effect": p.get("decision_effect") or pr.get("decision_effect"),
        "has_outcome": bool(o),
        "outcome_status": o.get("outcome_status") if o else "",
        "label_target": o.get("label_target") if o else "",
        "label_win": o.get("label_win") if o else "",
        "label_R": o.get("label_R") if o else "",
        "first_hit": o.get("first_hit") if o else "",
        "bars_to_hit": o.get("bars_to_hit") if o else "",
        "max_favorable_r": o.get("max_favorable_r") if o else "",
        "max_adverse_r": o.get("max_adverse_r") if o else "",
        "hit_time_wib": o.get("hit_time_wib") if o else "",
        "exclude_label_reason": o.get("exclude_label_reason") if o else "",
    }
    joined.append(row)

with OUT_CSV.open("w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(joined[0].keys()) if joined else [
        "signal_key","date_wib","symbol","direction","model_version","p_win","ml_action",
        "decision_effect","has_outcome","outcome_status","label_target","label_win","label_R"
    ])
    w.writeheader()
    for r in joined:
        w.writerow(r)

resolved = [r for r in joined if r["has_outcome"]]
labelable = [r for r in resolved if str(r.get("label_win")) not in ("", "None", "null")]
wins = [int(float(r["label_win"])) for r in labelable if num(r["label_win"]) is not None]
rs = [num(r["label_R"]) for r in labelable if num(r["label_R"]) is not None]
bars = [num(r["bars_to_hit"]) for r in labelable if num(r["bars_to_hit"]) is not None]

def pct(x):
    return f"{x*100:.2f}%"

print("=== JIM SIMONS / RENAISSANCE-STYLE QUANT SIGNAL OUTCOMES ===")
print("prediction_file:", PRED_FILE)
print("outcome_file:", OUTCOME_FILE)
print("csv:", OUT_CSV)
print("")
print("total_quant_predictions_unique:", len(pred_latest))
print("matched_with_outcome:", len(resolved))
print("labelable:", len(labelable))
print("unresolved_no_outcome_yet:", len(joined) - len(resolved))

if labelable:
    print("")
    print("--- headline ---")
    print("win_rate:", pct(sum(wins) / len(wins)) if wins else "NA")
    print("wins/losses:", f"{sum(wins)}/{len(wins)-sum(wins)}")
    print("expectancy_R_avg:", round(mean(rs), 4) if rs else "NA")
    print("median_R:", round(median(rs), 4) if rs else "NA")
    print("avg_bars_to_hit:", round(mean(bars), 2) if bars else "NA")
    print("median_bars_to_hit:", round(median(bars), 2) if bars else "NA")

    print("")
    print("--- by ml_action ---")
    by_action = defaultdict(list)
    for r in labelable:
        by_action[str(r.get("ml_action") or "UNKNOWN")].append(r)

    for action, arr in sorted(by_action.items(), key=lambda x: len(x[1]), reverse=True):
        aw = [int(float(x["label_win"])) for x in arr if num(x["label_win"]) is not None]
        ar = [num(x["label_R"]) for x in arr if num(x["label_R"]) is not None]
        print(
            action,
            "n=", len(arr),
            "wr=", pct(sum(aw)/len(aw)) if aw else "NA",
            "expR=", round(mean(ar), 4) if ar else "NA",
            "targets=", dict(Counter(str(x.get("label_target")) for x in arr))
        )

    print("")
    print("--- by model_version ---")
    by_model = defaultdict(list)
    for r in labelable:
        by_model[str(r.get("model_version") or "UNKNOWN")].append(r)

    for model, arr in sorted(by_model.items(), key=lambda x: len(x[1]), reverse=True):
        aw = [int(float(x["label_win"])) for x in arr if num(x["label_win"]) is not None]
        ar = [num(x["label_R"]) for x in arr if num(x["label_R"]) is not None]
        print(
            model,
            "n=", len(arr),
            "wr=", pct(sum(aw)/len(aw)) if aw else "NA",
            "expR=", round(mean(ar), 4) if ar else "NA"
        )

    print("")
    print("--- by symbol top ---")
    by_sym = defaultdict(list)
    for r in labelable:
        by_sym[str(r.get("symbol") or "UNKNOWN")].append(r)

    for sym, arr in sorted(by_sym.items(), key=lambda x: len(x[1]), reverse=True)[:15]:
        aw = [int(float(x["label_win"])) for x in arr if num(x["label_win"]) is not None]
        ar = [num(x["label_R"]) for x in arr if num(x["label_R"]) is not None]
        print(
            sym,
            "n=", len(arr),
            "wr=", pct(sum(aw)/len(aw)) if aw else "NA",
            "expR=", round(mean(ar), 4) if ar else "NA"
        )

print("")
print("--- latest 30 joined rows ---")
for r in joined[-30:]:
    print(
        r["date_wib"],
        r["symbol"],
        r["direction"],
        r["model_version"],
        "p_win=", r["p_win"],
        "action=", r["ml_action"],
        "outcome=", r["label_target"],
        "win=", r["label_win"],
        "R=", r["label_R"],
        "bars=", r["bars_to_hit"],
        r["signal_key"],
    )
