#!/usr/bin/env python3
import argparse
import json
import math
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone, date
from pathlib import Path
from statistics import mean, pstdev

ROOT = Path(".")
LOGS = ROOT / "logs"
REPORTS = ROOT / "reports"
LOGS.mkdir(exist_ok=True)
REPORTS.mkdir(exist_ok=True)

OUT_JSONL = LOGS / "feature_store_fred_macro_v1.jsonl"
OUT_REPORT = REPORTS / "feature_store_fred_macro_v1_report.json"

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

SERIES = {
    # Rates / liquidity
    "DFF": "macro_dff",
    "SOFR": "macro_sofr",
    "DGS2": "macro_dgs2",
    "DGS10": "macro_dgs10",
    "T10Y2Y": "macro_t10y2y",
    "WALCL": "macro_walcl",
    "RRPONTSYD": "macro_rrp",

    # Risk / credit
    "VIXCLS": "macro_vix",
    "BAMLH0A0HYM2": "macro_hy_spread",

    # Labor / inflation broad context
    "UNRATE": "macro_unrate",
    "CPIAUCSL": "macro_cpi",
    "PCEPI": "macro_pce",
    "PAYEMS": "macro_payems",
    "INDPRO": "macro_indpro",
}

def load_dotenv(path=".env"):
    p = Path(path)
    if not p.exists():
        return
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def to_float(x):
    try:
        if x is None or str(x).strip() in ("", "."):
            return None
        v = float(x)
        if math.isfinite(v):
            return v
        return None
    except Exception:
        return None

def safe_div(a, b, default=None):
    try:
        if b in (None, 0, 0.0):
            return default
        return a / b
    except Exception:
        return default

def chg(vals, n):
    clean = [v for v in vals if v is not None]
    if len(clean) <= n:
        return None
    return clean[-1] - clean[-1 - n]

def pct_chg(vals, n):
    clean = [v for v in vals if v is not None]
    if len(clean) <= n:
        return None
    old = clean[-1 - n]
    now = clean[-1]
    if old in (None, 0, 0.0):
        return None
    return (now - old) / old

def zscore(vals, n=252):
    clean = [v for v in vals if v is not None and math.isfinite(v)]
    if len(clean) < 20:
        return None
    window = clean[-n:] if len(clean) >= n else clean
    mu = mean(window)
    sd = pstdev(window)
    if sd <= 0:
        return None
    return (clean[-1] - mu) / sd

def fetch_series(series_id, api_key, limit=420):
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "sort_order": "desc",
        "limit": limit,
    }
    url = FRED_BASE + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "ai-trading-fred-macro-feature-store-v1",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=18) as r:
        raw = r.read().decode("utf-8", errors="replace")
        data = json.loads(raw)
    obs = data.get("observations") or []
    obs.reverse()  # ascending
    return obs

def obs_values(obs):
    dates = []
    vals = []
    realtime_ends = []
    for o in obs:
        v = to_float(o.get("value"))
        if v is None:
            continue
        dates.append(o.get("date"))
        vals.append(v)
        realtime_ends.append(o.get("realtime_end"))
    return dates, vals, realtime_ends

def age_days(last_date):
    try:
        d = date.fromisoformat(str(last_date))
        return (datetime.now(timezone.utc).date() - d).days
    except Exception:
        return None

def main():
    load_dotenv()

    ap = argparse.ArgumentParser()
    ap.add_argument("--series", default="")
    args = ap.parse_args()

    api_key = (
        os.getenv("FRED_API_KEY")
        or os.getenv("FRED_APIKEY")
        or os.getenv("STLOUISFED_API_KEY")
        or ""
    ).strip()

    report = {
        "ok": False,
        "version": "fred_macro_feature_store_v1",
        "created_at_utc": utc_now_iso(),
        "out_jsonl": str(OUT_JSONL),
        "series_requested": [],
        "series_ok": [],
        "series_error": {},
        "note": "Shadow macro feature store only. Does not alter live gate, current dataset, sizing, or trading config.",
    }

    if not api_key:
        report["reason"] = "missing_FRED_API_KEY"
        OUT_REPORT.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        print(json.dumps(report, indent=2, ensure_ascii=False))
        return

    wanted = SERIES
    if args.series.strip():
        ids = [x.strip().upper() for x in args.series.replace(";", ",").split(",") if x.strip()]
        wanted = {sid: SERIES.get(sid, "macro_" + sid.lower()) for sid in ids}

    row = {
        "created_at_utc": utc_now_iso(),
        "feature_version": "fred_macro_feature_store_v1",
        "feature_source": "fred_series_observations",
        "ok": True,
    }

    report["series_requested"] = list(wanted.keys())

    for sid, prefix in wanted.items():
        try:
            obs = fetch_series(sid, api_key)
            dates, vals, realtime_ends = obs_values(obs)
            if not vals:
                raise RuntimeError("no_numeric_observations")

            row[f"{prefix}"] = vals[-1]
            row[f"{prefix}_date"] = dates[-1]
            row[f"{prefix}_realtime_end"] = realtime_ends[-1] if realtime_ends else None
            row[f"{prefix}_age_days"] = age_days(dates[-1])
            row[f"{prefix}_chg_1obs"] = chg(vals, 1)
            row[f"{prefix}_chg_5obs"] = chg(vals, 5)
            row[f"{prefix}_chg_20obs"] = chg(vals, 20)
            row[f"{prefix}_pct_chg_5obs"] = pct_chg(vals, 5)
            row[f"{prefix}_pct_chg_20obs"] = pct_chg(vals, 20)
            row[f"{prefix}_z_252obs"] = zscore(vals, 252)
            report["series_ok"].append(sid)
        except Exception as e:
            row[f"{prefix}_error"] = str(e)[:180]
            report["series_error"][sid] = str(e)[:180]

    # Derived macro regime features.
    dgs2 = row.get("macro_dgs2")
    dgs10 = row.get("macro_dgs10")
    vix_z = row.get("macro_vix_z_252obs")
    hy_z = row.get("macro_hy_spread_z_252obs")
    walcl_20 = row.get("macro_walcl_pct_chg_20obs")
    rrp_20 = row.get("macro_rrp_pct_chg_20obs")

    if dgs2 is not None and dgs10 is not None:
        row["macro_10y_minus_2y_calc"] = dgs10 - dgs2

    row["macro_risk_stress_score_raw"] = (
        0.55 * (vix_z or 0.0)
        + 0.45 * (hy_z or 0.0)
    )

    row["macro_liquidity_impulse_raw"] = (
        0.60 * (walcl_20 or 0.0)
        - 0.40 * (rrp_20 or 0.0)
    )

    row["macro_regime_risk_off"] = bool((row.get("macro_risk_stress_score_raw") or 0.0) >= 1.0)
    row["macro_regime_liquidity_tight"] = bool((row.get("macro_liquidity_impulse_raw") or 0.0) < -0.01)

    with OUT_JSONL.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")

    report["ok"] = True
    report["fields_written"] = len(row)
    report["macro_regime_risk_off"] = row.get("macro_regime_risk_off")
    report["macro_regime_liquidity_tight"] = row.get("macro_regime_liquidity_tight")

    OUT_REPORT.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
