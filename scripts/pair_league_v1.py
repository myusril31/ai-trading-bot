#!/usr/bin/env python3
import json, csv, math, sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from statistics import mean

ROOT = Path(__file__).resolve().parents[1]
WIB = timezone(timedelta(hours=7))
UTC = timezone.utc

DAYS = int(sys.argv[1]) if len(sys.argv) > 1 else 14

RECALC_FILE = ROOT / "logs" / "score_v2_recalc_shadow_v1.jsonl"
JOIN_FILE = ROOT / "logs" / "ml_dataset_v3_feature_join.jsonl"
OUTCOME_FILE = ROOT / "logs" / "forward_outcomes.jsonl"
FEATURE_LATEST = ROOT / "state" / "features" / "latest_freqai_features_v1.json"

OUT_JSON = ROOT / "reports" / "pair_league_v1.json"
OUT_CSV = ROOT / "reports" / "pair_league_v1.csv"

FALLBACK_ALLOWLIST = [
    "BTCUSDT","ETHUSDT","SOLUSDT","PAXGUSDT","HYPEUSDT","XRPUSDT","ZECUSDT",
    "UNIUSDT","ADAUSDT","BCHUSDT","LINKUSDT","SUIUSDT","LTCUSDT","AVAXUSDT",
]

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

def num(x):
    try:
        v = float(x)
        if math.isfinite(v):
            return v
    except Exception:
        pass
    return None

def parse_dt(v):
    if not v:
        return None
    s = str(v).replace("T", " ").replace("Z", "").replace(" WIB", "")
    if "+" in s:
        s = s.split("+")[0]
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s[:26], fmt).replace(tzinfo=WIB)
        except Exception:
            pass
    return None

def symbol_of(r):
    return str(r.get("symbol") or "").replace(".P", "").replace("BINANCE:", "").upper()

def key_of(r):
    return str(r.get("signal_key") or r.get("signal_id") or "")

def load_allowlist():
    env = ROOT / ".env"
    if not env.exists():
        return FALLBACK_ALLOWLIST

    txt = env.read_text(errors="ignore")
    for line in txt.splitlines():
        line = line.strip()
        if line.startswith("PAIR_ALLOWLIST="):
            raw = line.split("=", 1)[1].strip().strip('"').strip("'")
            arr = [x.strip().replace(".P", "").replace("BINANCE:", "").upper() for x in raw.split(",") if x.strip()]
            return arr or FALLBACK_ALLOWLIST

    return FALLBACK_ALLOWLIST

def latest_feature_by_symbol():
    if not FEATURE_LATEST.exists():
        return {}
    try:
        data = json.loads(FEATURE_LATEST.read_text(errors="ignore"))
    except Exception:
        return {}

    out = {}
    for r in data.get("rows", []):
        sym = symbol_of(r)
        if sym:
            out[sym] = r
    return out

def latest_by_key(rows):
    out = {}
    for r in rows:
        k = key_of(r)
        if k:
            out[k] = r
    return out

def status_for(row):
    out_n = row["outcome_count"]
    wr = row["win_rate"]
    exp = row["expectancy_R"]
    avg_score = row["avg_score_v2_recalc"]
    sig_n = row["signal_count"]

    if out_n >= 5 and ((wr is not None and wr < 0.55) or (exp is not None and exp < 0)):
        return "DEGRADED"

    if out_n >= 5 and wr is not None and exp is not None and wr >= 0.70 and exp > 0:
        return "CORE"

    # No outcome yet, so these are shadow statuses.
    if sig_n >= 2 and avg_score is not None and avg_score >= 80:
        return "CORE_SHADOW"

    if sig_n >= 1 and avg_score is not None and avg_score >= 70:
        return "ACTIVE"

    if sig_n >= 1:
        return "WATCH"

    return "NO_SIGNAL"

def league_score(row):
    s = 50.0

    avg_score = row.get("avg_score_v2_recalc")
    if avg_score is not None:
        s += (avg_score - 70.0) * 0.55

    s += min(row.get("signal_count", 0), 10) * 1.2
    s += min(row.get("joined_feature_rows", 0), 10) * 0.8

    avg_pwin = row.get("avg_ml_p_win")
    if avg_pwin is not None:
        s += (avg_pwin - 0.50) * 25.0

    exp = row.get("expectancy_R")
    if exp is not None:
        s += exp * 12.0

    wr = row.get("win_rate")
    if wr is not None:
        s += (wr - 0.50) * 20.0

    rank = row.get("fs_cross_rank_ret_15m_4")
    if rank is not None:
        s += (rank - 0.50) * 5.0

    volz = row.get("fs_volume_z_5m_20")
    if volz is not None and abs(volz) >= 1.0:
        s += 1.5

    return round(max(0, min(100, s)), 2)

def main():
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)

    allowlist = load_allowlist()
    allowset = set(allowlist)

    now = datetime.now(WIB)
    cutoff = now - timedelta(days=DAYS)

    recalc_rows = []
    for r in read_jsonl(RECALC_FILE):
        sym = symbol_of(r)
        if sym not in allowset:
            continue
        dt = parse_dt(r.get("signal_time_wib"))
        if dt and dt < cutoff:
            continue
        recalc_rows.append(r)

    join_rows = []
    for r in read_jsonl(JOIN_FILE):
        sym = symbol_of(r)
        if sym not in allowset:
            continue
        dt = parse_dt(r.get("signal_time_wib"))
        if dt and dt < cutoff:
            continue
        join_rows.append(r)

    outcome_rows = []
    for r in read_jsonl(OUTCOME_FILE):
        sym = symbol_of(r)
        if sym not in allowset:
            continue
        outcome_rows.append(r)

    feat_latest = latest_feature_by_symbol()

    recalc_by_symbol = defaultdict(list)
    join_by_symbol = defaultdict(list)
    out_by_symbol = defaultdict(list)

    for r in recalc_rows:
        recalc_by_symbol[symbol_of(r)].append(r)

    for r in join_rows:
        join_by_symbol[symbol_of(r)].append(r)

    for r in outcome_rows:
        out_by_symbol[symbol_of(r)].append(r)

    rows = []
    for sym in allowlist:
        rs = recalc_by_symbol.get(sym, [])
        js = join_by_symbol.get(sym, [])
        os_ = out_by_symbol.get(sym, [])
        feat = feat_latest.get(sym, {})

        scores = [num(r.get("score_v2_recalc")) for r in rs]
        scores = [x for x in scores if x is not None]

        buckets = defaultdict(int)
        for r in rs:
            b = str(r.get("score_v2_bucket") or "NA")
            buckets[b] += 1

        pwin_vals = [num(r.get("ml_p_win")) for r in js]
        pwin_vals = [x for x in pwin_vals if x is not None]

        wins = [num(r.get("label_win")) for r in os_]
        wins = [x for x in wins if x is not None]

        r_vals = [num(r.get("label_R")) for r in os_]
        r_vals = [x for x in r_vals if x is not None]

        row = {
            "symbol": sym,
            "window_days": DAYS,

            "signal_count": len(rs),
            "joined_feature_rows": len(js),
            "outcome_count": len(os_),
            "ml_rows": len(pwin_vals),

            "avg_score_v2_recalc": round(mean(scores), 4) if scores else None,
            "max_score_v2_recalc": max(scores) if scores else None,
            "a_bucket_count": buckets.get("A", 0),
            "b_bucket_count": buckets.get("B", 0),
            "c_bucket_count": buckets.get("C", 0),
            "d_bucket_count": buckets.get("D", 0),
            "reject_bucket_count": buckets.get("REJECT_ZONE", 0),

            "avg_ml_p_win": round(mean(pwin_vals), 4) if pwin_vals else None,
            "ml_pass_count": sum(1 for x in pwin_vals if x >= 0.74),

            "win_rate": round(mean(wins), 4) if wins else None,
            "expectancy_R": round(mean(r_vals), 4) if r_vals else None,

            "fs_ret_15m_4": feat.get("ret_15m_4"),
            "fs_btc_residual_ret_15m_4": feat.get("btc_residual_ret_15m_4"),
            "fs_atr_pct_5m_14": feat.get("atr_pct_5m_14"),
            "fs_volume_z_5m_20": feat.get("volume_z_5m_20"),
            "fs_cross_rank_ret_15m_4": feat.get("cross_rank_ret_15m_4"),
            "fs_feature_sanity_ok": feat.get("feature_sanity_ok"),
        }

        row["league_status"] = status_for(row)
        row["league_score"] = league_score(row)

        rows.append(row)

    rows.sort(key=lambda r: (-r["league_score"], r["symbol"]))

    report = {
        "ok": True,
        "report_version": "pair_league_v1_20260614",
        "mode": "REPORT_ONLY",
        "created_at_wib": now.strftime("%Y-%m-%d %H:%M:%S WIB"),
        "window_days": DAYS,
        "allowlist_count": len(allowlist),
        "rows": rows,
    }

    OUT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2))

    cols = [
        "symbol","league_status","league_score","signal_count","joined_feature_rows",
        "avg_score_v2_recalc","max_score_v2_recalc","a_bucket_count","b_bucket_count",
        "ml_rows","avg_ml_p_win","ml_pass_count","outcome_count","win_rate","expectancy_R",
        "fs_ret_15m_4","fs_btc_residual_ret_15m_4","fs_volume_z_5m_20",
        "fs_cross_rank_ret_15m_4","fs_feature_sanity_ok",
    ]

    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c) for c in cols})

    print(f"=== PAIR LEAGUE V1 | last {DAYS}d | REPORT_ONLY ===")
    print("out_json:", OUT_JSON)
    print("out_csv :", OUT_CSV)
    print("")
    print(f"{'SYM':<10} {'STATUS':<12} {'LGS':>6} {'SIG':>4} {'JOIN':>4} {'AVG_V2':>7} {'A':>3} {'B':>3} {'ML':>3} {'PWIN':>7} {'OUT':>4}")
    for r in rows:
        avg = "NA" if r["avg_score_v2_recalc"] is None else f"{r['avg_score_v2_recalc']:.1f}"
        pwin = "NA" if r["avg_ml_p_win"] is None else f"{r['avg_ml_p_win']:.3f}"
        print(
            f"{r['symbol']:<10} {r['league_status']:<12} {r['league_score']:>6.2f} "
            f"{r['signal_count']:>4} {r['joined_feature_rows']:>4} {avg:>7} "
            f"{r['a_bucket_count']:>3} {r['b_bucket_count']:>3} "
            f"{r['ml_rows']:>3} {pwin:>7} {r['outcome_count']:>4}"
        )

if __name__ == "__main__":
    main()
