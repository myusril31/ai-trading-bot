#!/usr/bin/env python3
import json, csv, math, sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from statistics import mean, median, pstdev

ROOT = Path(__file__).resolve().parents[1]
WIB = timezone(timedelta(hours=7))
UTC = timezone.utc

DAYS = int(sys.argv[1]) if len(sys.argv) > 1 else 14

OUTCOME_FILE = ROOT / "logs" / "forward_outcomes.jsonl"
PAIR_POLICY_FILE = ROOT / "reports" / "pair_league_policy_v1.json"

OUT_JSON = ROOT / "reports" / "hyperopt_loss_metrics_v1.json"
OUT_CSV = ROOT / "reports" / "hyperopt_loss_metrics_v1.csv"

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

def read_json(path):
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(errors="ignore"))
    except Exception:
        return {}

def num(x):
    try:
        v = float(x)
        if math.isfinite(v):
            return v
    except Exception:
        pass
    return None

def parse_dt(v, default_tz=WIB):
    if not v:
        return None
    s = str(v).replace("T", " ").replace("Z", "").replace(" WIB", "")
    if "+" in s:
        s = s.split("+")[0]
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s[:26], fmt).replace(tzinfo=default_tz)
        except Exception:
            pass
    return None

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

def symbol_of(r):
    sym = r.get("symbol") or r.get("pair") or ""
    if not sym:
        key = str(r.get("signal_key") or r.get("signal_id") or "")
        if "|" in key:
            sym = key.split("|", 1)[0]
    return str(sym).replace(".P", "").replace("BINANCE:", "").upper()

def direction_of(r):
    d = r.get("direction") or r.get("side") or ""
    if not d:
        key = str(r.get("signal_key") or r.get("signal_id") or "")
        parts = key.split("|")
        if len(parts) >= 2:
            d = parts[1]
    return str(d).upper()

def r_of(r):
    for k in ("label_R", "R", "r", "r_multiple", "realized_R", "outcome_R", "final_R"):
        v = num(r.get(k))
        if v is not None:
            return v
    return None

def win_of(r, rv=None):
    for k in ("label_win", "win", "is_win"):
        v = r.get(k)
        if isinstance(v, bool):
            return 1 if v else 0
        nv = num(v)
        if nv is not None:
            return 1 if nv >= 0.5 else 0

    result = str(r.get("result") or r.get("first_hit") or r.get("label_target") or "").upper()
    if result in ("WIN", "TP", "TP1", "TP2", "TP3", "TARGET", "PROFIT"):
        return 1
    if result in ("LOSS", "SL", "STOP", "STOPLOSS"):
        return 0

    if rv is not None:
        return 1 if rv > 0 else 0

    return None

def time_of(r):
    for k in (
        "resolved_at_wib",
        "outcome_time_wib",
        "created_at_wib",
        "signal_time_wib",
        "entry_time_wib",
        "created_at_utc",
    ):
        dt = parse_dt(r.get(k), UTC if k.endswith("_utc") else WIB)
        if dt:
            return dt.astimezone(WIB)
    return None

def max_drawdown(vals):
    eq = 0.0
    peak = 0.0
    max_dd = 0.0
    for v in vals:
        eq += v
        peak = max(peak, eq)
        dd = eq - peak
        max_dd = min(max_dd, dd)
    return round(max_dd, 6)

def safe_ratio(a, b, cap=99.0):
    if b is None or abs(b) < 1e-12:
        if a and a > 0:
            return cap
        return 0.0
    return round(a / b, 6)

def calc_metrics(rows):
    rows = sorted(rows, key=lambda r: r["_time"] or datetime.min.replace(tzinfo=WIB))
    rvals = [r["_R"] for r in rows if r["_R"] is not None]
    wins = [r["_win"] for r in rows if r["_win"] is not None]

    n = len(rvals)
    if n == 0:
        return {
            "n": 0,
            "sample_confidence": 0.0,
            "win_rate": None,
            "expectancy_R": None,
            "median_R": None,
            "total_R": 0.0,
            "profit_factor": None,
            "max_drawdown_R": None,
            "sharpe_like": None,
            "sortino_like": None,
            "calmar_like": None,
            "objective_score": 0.0,
            "hyperopt_loss": 0.0,
            "recommendation": "NO_DATA",
        }

    pos = [x for x in rvals if x > 0]
    neg = [x for x in rvals if x < 0]
    gross_profit = sum(pos)
    gross_loss_abs = abs(sum(neg))
    pf = safe_ratio(gross_profit, gross_loss_abs)

    avg = mean(rvals)
    med = median(rvals)
    total = sum(rvals)
    wr = mean(wins) if wins else mean([1 if x > 0 else 0 for x in rvals])

    dd = max_drawdown(rvals)
    std = pstdev(rvals) if n > 1 else 0.0
    downside = [x for x in rvals if x < 0]
    down_std = pstdev(downside) if len(downside) > 1 else 0.0

    sharpe = safe_ratio(avg * math.sqrt(n), std)
    sortino = safe_ratio(avg * math.sqrt(n), down_std)
    calmar = safe_ratio(total, abs(dd)) if dd is not None else 0.0

    # Objective score is report-only. Higher is better.
    objective = 50.0
    objective += avg * 18.0
    objective += min(pf, 3.0) * 5.0
    objective += (wr - 0.50) * 25.0
    objective += min(calmar, 5.0) * 3.0
    objective -= abs(dd) * 2.5

    if n < 10:
        objective -= 8.0
    elif n < 20:
        objective -= 4.0

    # === HYPEROPT_LOSS_V1_1_SAMPLE_AWARE_20260615 ===
    # Prevent tiny samples from looking like holy scripture.
    sample_confidence = min(1.0, n / 25.0)

    # Smooth confidence haircut.
    objective = objective * (0.55 + 0.45 * sample_confidence)

    # Hard caps for tiny samples.
    if n < 5:
        objective = min(objective, 55.0)
    elif n < 10:
        objective = min(objective, 70.0)
    elif n < 20:
        objective = min(objective, 85.0)

    objective = round(max(0.0, min(100.0, objective)), 4)
    loss = round(-objective, 4)

    if n < 10:
        rec = "WATCH_MORE_DATA"
    elif avg < 0 or total < 0:
        rec = "DEGRADE_CANDIDATE"
    elif wr >= 0.75 and avg >= 0.50 and pf >= 1.8:
        rec = "OPTIMIZE_CANDIDATE"
    elif wr >= 0.65 and avg >= 0.25:
        rec = "KEEP_BASELINE"
    else:
        rec = "WATCH_RISK"

    return {
        "n": n,
        "sample_confidence": round(sample_confidence, 6),
        "win_rate": round(wr, 6),
        "expectancy_R": round(avg, 6),
        "median_R": round(med, 6),
        "total_R": round(total, 6),
        "avg_win_R": round(mean(pos), 6) if pos else None,
        "avg_loss_R": round(mean(neg), 6) if neg else None,
        "profit_factor": pf,
        "max_drawdown_R": dd,
        "sharpe_like": sharpe,
        "sortino_like": sortino,
        "calmar_like": calmar,
        "objective_score": objective,
        "hyperopt_loss": loss,
        "recommendation": rec,
    }

def main():
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)

    allowlist = load_allowlist()
    allowset = set(allowlist)

    now = datetime.now(WIB)
    cutoff = now - timedelta(days=DAYS)

    policy = read_json(PAIR_POLICY_FILE)
    policy_map = {
        r.get("symbol"): r
        for r in policy.get("rows", [])
        if r.get("symbol")
    }

    raw_rows = read_jsonl(OUTCOME_FILE)

    rows = []
    for r in raw_rows:
        sym = symbol_of(r)
        if sym not in allowset:
            continue

        rv = r_of(r)
        w = win_of(r, rv)
        dt = time_of(r)

        if rv is None:
            continue

        if dt and dt < cutoff:
            continue

        nr = dict(r)
        nr["_symbol"] = sym
        nr["_direction"] = direction_of(r)
        nr["_R"] = rv
        nr["_win"] = w
        nr["_time"] = dt
        rows.append(nr)

    by_symbol = defaultdict(list)
    for r in rows:
        by_symbol[r["_symbol"]].append(r)

    report_rows = []
    for sym in allowlist:
        m = calc_metrics(by_symbol.get(sym, []))
        pol = policy_map.get(sym, {})

        out = {
            "symbol": sym,
            "window_days": DAYS,
            "policy_weight": pol.get("policy_weight"),
            "policy_action": pol.get("policy_action"),
            "league_status": pol.get("league_status"),
            **m,
        }
        report_rows.append(out)

    portfolio = calc_metrics(rows)

    report_rows.sort(
        key=lambda r: (
            -(r.get("objective_score") or 0),
            r.get("symbol") or "",
        )
    )

    report = {
        "ok": True,
        "report_version": "hyperopt_loss_metrics_v1_1_20260615",
        "mode": "REPORT_ONLY",
        "created_at_wib": now.strftime("%Y-%m-%d %H:%M:%S WIB"),
        "window_days": DAYS,
        "source_rows": len(raw_rows),
        "usable_rows": len(rows),
        "portfolio": portfolio,
        "rows": report_rows,
        "notes": [
            "This is not optimizer execution. It only computes objective/loss metrics.",
            "hyperopt_loss is negative objective_score, lower is better for optimizer-style ranking.",
            "Sharpe/Sortino/Calmar are R-multiple approximations, not account-equity audited metrics.",
        ],
    }

    OUT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2))

    cols = [
        "symbol","window_days","policy_weight","policy_action","league_status",
        "n","sample_confidence","win_rate","expectancy_R","median_R","total_R","profit_factor",
        "max_drawdown_R","sharpe_like","sortino_like","calmar_like",
        "objective_score","hyperopt_loss","recommendation",
    ]

    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in report_rows:
            w.writerow({c: r.get(c) for c in cols})

    print(f"=== HYPEROPT LOSS METRICS V1 | last {DAYS}d | REPORT_ONLY ===")
    print("out_json:", OUT_JSON)
    print("out_csv :", OUT_CSV)
    print("")
    p = portfolio
    print(
        "PORTFOLIO "
        f"n={p['n']} WR={p['win_rate']} expR={p['expectancy_R']} "
        f"totalR={p['total_R']} maxDD={p['max_drawdown_R']} "
        f"PF={p['profit_factor']} obj={p['objective_score']} loss={p['hyperopt_loss']}"
    )
    print("")
    print(f"{'SYM':<10} {'N':>4} {'WR':>7} {'EXPR':>7} {'TOTR':>8} {'DD':>8} {'PF':>7} {'OBJ':>7} {'LOSS':>8} {'REC':<20}")
    for r in report_rows:
        def fmt(x, nd=3):
            return "NA" if x is None else f"{float(x):.{nd}f}"
        print(
            f"{r['symbol']:<10} {r['n']:>4} "
            f"{fmt(r['win_rate']):>7} {fmt(r['expectancy_R']):>7} "
            f"{fmt(r['total_R']):>8} {fmt(r['max_drawdown_R']):>8} "
            f"{fmt(r['profit_factor']):>7} {fmt(r['objective_score']):>7} "
            f"{fmt(r['hyperopt_loss']):>8} {r['recommendation']:<20}"
        )

if __name__ == "__main__":
    main()
