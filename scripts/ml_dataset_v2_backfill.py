#!/usr/bin/env python3
import json, math, re
from pathlib import Path
from datetime import datetime, timezone, timedelta

LOGS = Path("logs")
DATASET = LOGS / "ml_dataset_rows.jsonl"
SIGNALS = LOGS / "signals.jsonl"
DECISIONS = LOGS / "decisions.jsonl"

WIB = timezone(timedelta(hours=7))

def read_jsonl(path):
    out = []
    if not path.exists():
        return out
    for line in path.read_text(errors="ignore").splitlines():
        try:
            out.append(json.loads(line))
        except Exception:
            pass
    return out

def write_jsonl(path, rows):
    with path.open("w") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")

def fnum(x):
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            v = float(x)
            return v if math.isfinite(v) else None
        s = str(x).strip()
        if not s:
            return None
        # handle "208.85 - 209.06"
        nums = re.findall(r'-?\d+(?:\.\d+)?', s)
        if len(nums) >= 2 and "-" in s:
            vals = [float(a) for a in nums[:2]]
            return sum(vals) / len(vals)
        v = float(s.replace(",", ""))
        return v if math.isfinite(v) else None
    except Exception:
        return None

def parse_range_lo_hi(x):
    if x is None:
        return None, None
    if isinstance(x, (int, float)):
        return float(x), float(x)
    s = str(x).strip()
    nums = re.findall(r'-?\d+(?:\.\d+)?', s)
    if len(nums) >= 2:
        return float(nums[0]), float(nums[1])
    if len(nums) == 1:
        v = float(nums[0])
        return v, v
    return None, None

def norm_symbol(x):
    return str(x or "").upper().replace("BINANCE:", "").replace(".P", "")

def norm_dir(x):
    d = str(x or "").upper()
    if d in ("LONG", "BUY"):
        return "LONG"
    if d in ("SHORT", "SELL"):
        return "SHORT"
    return d

def key_of(r):
    return str(r.get("signal_key") or r.get("signal_id") or "")

def pick(obj, *keys):
    if not isinstance(obj, dict):
        return None
    objs = [obj]
    for nested in ("payload", "plan", "htf", "liq", "meta"):
        if isinstance(obj.get(nested), dict):
            objs.append(obj[nested])
    for k in keys:
        for o in objs:
            if isinstance(o, dict) and o.get(k) not in (None, ""):
                return o.get(k)
    return None

def deep_get(obj, path):
    cur = obj
    for k in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur

def parse_wib(s):
    if not s:
        return None
    ss = str(s).replace(" WIB", "").replace("T", " ").split("+")[0]
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(ss[:26], fmt).replace(tzinfo=WIB)
        except Exception:
            pass
    return None

def age_minutes(signal_time, event_time):
    a = parse_wib(signal_time)
    b = parse_wib(event_time)
    if not a or not b:
        return None
    return max(0.0, (a - b).total_seconds() / 60.0)

def patch_phase(signal_time):
    dt = parse_wib(signal_time)
    if not dt:
        return "UNKNOWN"
    phases = [
        ("PRE_LATENCY_PATCH", "2026-06-10 22:49:27"),
        ("PRE_NIGHT_GUARD", "2026-06-11 00:36:16"),
        ("PRE_HARD_GUARD", "2026-06-13 16:42:59"),
        ("PRE_SKLEARN_GATE", "2026-06-13 21:28:04"),
    ]
    for name, cutoff in phases:
        c = parse_wib(cutoff)
        if dt < c:
            return name
    return "AFTER_SKLEARN_GATE"

def enrich_row(row, payload):
    r = dict(row)
    p = payload if isinstance(payload, dict) else {}

    direction = norm_dir(pick(r, "direction", "dir") or pick(p, "direction", "dir"))
    symbol = norm_symbol(pick(r, "symbol", "pair") or pick(p, "symbol", "pair"))

    entry = fnum(pick(r, "entry", "entry_mid") or pick(p, "entry_mid", "entry"))
    entry_lo = fnum(pick(p, "entry_lo") or pick(r, "entry_lo"))
    entry_hi = fnum(pick(p, "entry_hi") or pick(r, "entry_hi"))
    if entry is None:
        lo, hi = parse_range_lo_hi(pick(p, "entry_zone", "entry") or pick(r, "entry"))
        if lo is not None and hi is not None:
            entry = (lo + hi) / 2.0
            entry_lo, entry_hi = lo, hi

    sl = fnum(pick(r, "sl", "invalid") or pick(p, "sl", "invalid"))
    tp1 = fnum(pick(r, "tp1") or pick(p, "tp1"))
    tp2 = fnum(pick(r, "tp2") or pick(p, "tp2"))
    tp3 = fnum(pick(r, "tp3") or pick(p, "tp3"))

    if entry and sl and tp1 and abs(sl - entry) > 0:
        risk = abs(sl - entry)
        r["rr_to_tp1"] = round(abs(tp1 - entry) / risk, 6)
        r["sl_dist_pct"] = round(risk / entry * 100.0, 6)
        r["tp1_dist_pct"] = round(abs(tp1 - entry) / entry * 100.0, 6)

        if tp2:
            r["rr_to_tp2"] = round(abs(tp2 - entry) / risk, 6)
            r["tp2_dist_pct"] = round(abs(tp2 - entry) / entry * 100.0, 6)
        if tp3:
            r["rr_to_tp3"] = round(abs(tp3 - entry) / risk, 6)
            r["tp3_dist_pct"] = round(abs(tp3 - entry) / entry * 100.0, 6)

    if entry and entry_lo is not None and entry_hi is not None:
        r["entry_zone_width_pct"] = round(abs(entry_hi - entry_lo) / entry * 100.0, 6)

    # HTF
    htf = p.get("htf_summary") if isinstance(p.get("htf_summary"), dict) else {}
    r["htf_dir"] = htf.get("htf_dir") or htf.get("dir")
    r["htf_bias"] = htf.get("bias")
    r["htf_location"] = htf.get("location")
    r["htf_structure"] = htf.get("structure")
    r["htf_dol"] = htf.get("dol")
    if htf.get("eq") is not None:
        r["htf_eq"] = htf.get("eq")

    # Liquidity
    liq = p.get("liquidity") if isinstance(p.get("liquidity"), dict) else {}
    r["liq_ctx"] = liq.get("ctx")
    r["liq_bsl"] = liq.get("bsl")
    r["liq_ssl"] = liq.get("ssl")
    if liq.get("dist_to_zone_pct") is not None:
        r["liq_dist_to_zone_pct"] = liq.get("dist_to_zone_pct")

    # Structure text
    struct = p.get("structure") if isinstance(p.get("structure"), dict) else {}
    r["structure_15m"] = struct.get("tf15m")
    r["structure_5m"] = struct.get("tf5m")

    # SMC context
    smc = p.get("smc_context") if isinstance(p.get("smc_context"), dict) else {}
    sweep = smc.get("sweep") if isinstance(smc.get("sweep"), dict) else {}
    reclaim = smc.get("reclaim") if isinstance(smc.get("reclaim"), dict) else {}
    fvg = smc.get("fvg") if isinstance(smc.get("fvg"), dict) else {}
    poi = smc.get("poi") if isinstance(smc.get("poi"), dict) else {}
    ob = smc.get("ob") if isinstance(smc.get("ob"), dict) else {}

    r["sweep_tag"] = pick(p, "sweep_tag") or sweep.get("tag")
    r["sweep_level"] = fnum(pick(p, "sweep_level") or sweep.get("level"))
    r["sweep_extreme"] = fnum(pick(p, "sweep_extreme") or sweep.get("extreme"))
    r["reclaim_mode"] = pick(p, "reclaim_mode") or reclaim.get("mode")
    r["reclaim_level"] = fnum(pick(p, "reclaim_level") or reclaim.get("level"))

    signal_time = pick(r, "signal_time_wib") or pick(p, "signal_time_wib", "confirmed_ts_wib")
    r["patch_phase"] = patch_phase(signal_time)

    sweep_age = age_minutes(signal_time, pick(p, "sweep_ts_wib") or sweep.get("ts_wib"))
    reclaim_age = age_minutes(signal_time, pick(p, "reclaim_ts_wib") or reclaim.get("ts_wib"))
    fvg_age = age_minutes(signal_time, pick(p, "fvg_ts_wib"))
    if sweep_age is not None:
        r["sweep_age_min"] = round(sweep_age, 3)
        r["sweep_age_bars_5m"] = round(sweep_age / 5.0, 3)
    if reclaim_age is not None:
        r["reclaim_age_min"] = round(reclaim_age, 3)
        r["reclaim_age_bars_5m"] = round(reclaim_age / 5.0, 3)
    if fvg_age is not None:
        r["fvg_age_min"] = round(fvg_age, 3)
        r["fvg_age_bars_5m"] = round(fvg_age / 5.0, 3)

    fvg_lo = fnum(pick(p, "fvg_lo") or fvg.get("bot") or poi.get("lo"))
    fvg_hi = fnum(pick(p, "fvg_hi") or fvg.get("top") or poi.get("hi"))
    if entry and fvg_lo is not None and fvg_hi is not None:
        r["fvg_size_pct"] = round(abs(fvg_hi - fvg_lo) / entry * 100.0, 6)

    r["fvg_type"] = fvg.get("type") or poi.get("type")
    r["ob_type"] = ob.get("type")
    ob_lo = fnum(ob.get("lo"))
    ob_hi = fnum(ob.get("hi"))
    if entry and ob_lo is not None and ob_hi is not None:
        r["ob_size_pct"] = round(abs(ob_hi - ob_lo) / entry * 100.0, 6)

    r["ml_feature_version"] = 2
    r["symbol_norm"] = symbol
    r["direction_norm"] = direction

    return r

def valid(v):
    return v not in (None, "", "NA", "UNK", -1, -1.0)

def coverage(rows, keys):
    print("=== V2 FEATURE COVERAGE ===")
    print("rows=", len(rows))
    for k in keys:
        n = sum(1 for r in rows if valid(r.get(k)))
        print(f"{k:28s} valid={n:4d}/{len(rows)} pct={(n/len(rows)*100 if rows else 0):6.2f}%")

def main():
    dataset = read_jsonl(DATASET)
    if not dataset:
        raise SystemExit("NO_DATASET_ROWS")

    payload_by_key = {}

    for src in [read_jsonl(SIGNALS), read_jsonl(DECISIONS)]:
        for r in src:
            k = key_of(r)
            p = r.get("payload") if isinstance(r.get("payload"), dict) else None
            if k and p:
                payload_by_key[k] = p

    out = []
    hit = 0
    for r in dataset:
        k = key_of(r)
        p = payload_by_key.get(k)
        if p:
            hit += 1
        out.append(enrich_row(r, p))

    backup = DATASET.with_suffix(".jsonl.bak_v2")
    backup.write_text(DATASET.read_text(errors="ignore"))
    write_jsonl(DATASET, out)

    print("BACKFILL_DONE")
    print("dataset_rows=", len(dataset))
    print("payload_join_hit=", hit)
    print("backup=", backup)

    keys = [
        "ml_feature_version",
        "rr_to_tp1", "rr_to_tp2",
        "sl_dist_pct", "tp1_dist_pct",
        "entry_zone_width_pct",
        "htf_bias", "htf_location", "htf_dir", "htf_dol",
        "liq_ctx", "liq_dist_to_zone_pct",
        "sweep_tag", "sweep_age_bars_5m",
        "reclaim_mode", "reclaim_age_bars_5m",
        "fvg_type", "fvg_size_pct", "fvg_age_bars_5m",
        "ob_type", "ob_size_pct",
        "patch_phase",
    ]
    coverage(out, keys)

if __name__ == "__main__":
    main()
