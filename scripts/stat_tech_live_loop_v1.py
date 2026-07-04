#!/usr/bin/env python3
import inspect
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

MARKER = "STAT_TECH_LIVE_LOOP_V1_20260628"
SYS_PATH_MARKER = "STAT_TECH_LIVE_LOOP_SYS_PATH_FIX_20260628"

APP_ROOT = Path(__file__).resolve().parents[1]
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

ROOT = APP_ROOT
LOG_PATH = ROOT / "logs" / "stat_tech_live_bridge_events_v1.jsonl"
STATE_PATH = ROOT / "state" / "stat_tech_live_loop_state_v1.json"



# === STAT_TECH_HISTORY_QUANT_PRIOR_V1_20260629 ===


# === STAT_TECH_LINEAR_QUANT_INJECT_V1_20260703 ===
_LINEAR_QUANT_CACHE_V1 = {"loaded_at": 0.0, "by_symbol": {}}

def _lq_to_float(v, default=None):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default

def _lq_norm_symbol(v):
    return str(v or "").strip().upper().replace("BINANCE:", "").replace(".P", "").replace("/", "")

def _lq_norm_dir(v):
    d = str(v or "").strip().upper()
    if d in ("BUY", "BULL", "LONG"):
        return "LONG"
    if d in ("SELL", "BEAR", "SHORT"):
        return "SHORT"
    return d

def _lq_parse_ts(v):
    try:
        from datetime import datetime, timezone
        txt = str(v or "").replace("Z", "+00:00")
        dt = datetime.fromisoformat(txt)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def _load_linear_quant_latest_v1():
    import os
    import json
    import time
    from pathlib import Path
    from datetime import datetime, timezone

    ttl = float(os.getenv("STAT_TECH_LINEAR_QUANT_CACHE_TTL_SEC", "60") or 60)
    now = time.time()
    if _LINEAR_QUANT_CACHE_V1.get("by_symbol") and now - float(_LINEAR_QUANT_CACHE_V1.get("loaded_at") or 0) <= ttl:
        return _LINEAR_QUANT_CACHE_V1["by_symbol"]

    log_dir = Path(os.getenv("LOG_DIR", "logs"))
    fp = Path(os.getenv("STAT_TECH_LINEAR_QUANT_STORE_PATH", str(log_dir / "stat_tech_linear_quant_store_v1.jsonl")))

    max_age_min = float(os.getenv("STAT_TECH_LINEAR_QUANT_MAX_AGE_MIN", "20") or 20)
    max_age_sec = max_age_min * 60.0

    by_symbol = {}
    if fp.exists():
        rows = []
        try:
            with fp.open("r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rows.append(json.loads(line))
                    except Exception:
                        pass
            # read latest first
            utc_now = datetime.now(timezone.utc)
            for r in reversed(rows[-3000:]):
                sym = _lq_norm_symbol(r.get("symbol"))
                if not sym or sym in by_symbol:
                    continue
                ts = _lq_parse_ts(r.get("created_at_utc"))
                if ts is not None:
                    age = (utc_now - ts).total_seconds()
                    if age > max_age_sec:
                        continue
                    r["_linear_quant_age_sec"] = age
                by_symbol[sym] = r
        except Exception:
            pass

    _LINEAR_QUANT_CACHE_V1["loaded_at"] = now
    _LINEAR_QUANT_CACHE_V1["by_symbol"] = by_symbol
    return by_symbol

def _inject_stat_tech_linear_quant_v1(payload):
    import os

    if str(os.getenv("STAT_TECH_LINEAR_QUANT_ENABLED", "true")).strip().lower() not in ("1", "true", "yes", "on"):
        return payload

    try:
        source_txt = str(payload.get("signal_source") or payload.get("source") or payload.get("engine") or "").upper()
        if "STAT_TECH" not in source_txt:
            return payload

        sym = _lq_norm_symbol(payload.get("symbol") or payload.get("pair"))
        direction = _lq_norm_dir(payload.get("direction") or payload.get("dir") or payload.get("side"))
        if not sym or direction not in ("LONG", "SHORT"):
            return payload

        store = _load_linear_quant_latest_v1()
        row = store.get(sym)
        if not row:
            payload["linear_quant"] = {"ok": False, "reason": "missing_linear_quant_store", "symbol": sym}
            return payload

        score_key = "la_score_long" if direction == "LONG" else "la_score_short"
        la_score = _lq_to_float(row.get(score_key))
        if la_score is None:
            payload["linear_quant"] = {"ok": False, "reason": "missing_directional_score", "symbol": sym, "direction": direction}
            return payload

        features = row.get("linear_algebra_features") or {}
        parts = row.get("la_parts_long" if direction == "LONG" else "la_parts_short") or {}

        payload["linear_quant_score"] = round(float(la_score), 1)
        payload["linear_quant_source"] = "STAT_TECH_LINEAR_ALGEBRA_V1"
        payload["linear_quant_age_sec"] = row.get("_linear_quant_age_sec")
        payload["linear_quant_features"] = features
        payload["linear_quant_parts"] = parts
        payload["linear_quant"] = {
            "ok": True,
            "source": "STAT_TECH_LINEAR_ALGEBRA_V1",
            "symbol": sym,
            "direction": direction,
            "score": round(float(la_score), 1),
            "age_sec": row.get("_linear_quant_age_sec"),
            "features": features,
            "parts": parts,
        }

        min_emit = float(os.getenv("STAT_TECH_LINEAR_QUANT_MIN_EMIT", "60") or 60)
        if float(la_score) >= min_emit:
            hist_q = _lq_to_float(payload.get("quant_score"))
            blend_w = float(os.getenv("STAT_TECH_LINEAR_QUANT_BLEND_WEIGHT", "0.35") or 0.35)

            if hist_q is None:
                combined = float(la_score)
                source = "STAT_TECH_LINEAR_ALGEBRA_V1"
            else:
                # Keep historical prior as base, but let linear algebra lift valid setup.
                combined = max(hist_q, hist_q * (1.0 - blend_w) + float(la_score) * blend_w)
                source = "STAT_TECH_HISTORY_PLUS_LINEAR_ALGEBRA_V1"

            combined = round(max(0.0, min(100.0, combined)), 1)
            payload["quant_score"] = combined
            payload["core_quant_score"] = combined
            payload["quant_source"] = source
            payload["linear_quant_emit"] = True
            payload["linear_quant_combined_score"] = combined
        else:
            payload["linear_quant_emit"] = False

        return payload
    except Exception as e:
        payload["linear_quant"] = {"ok": False, "reason": f"exception:{type(e).__name__}:{e}"}
        return payload


def _stq_to_float(v, default=None):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def _stq_norm_text(v):
    return str(v or "").strip().upper()


def _stq_label_win(row):
    # Flexible label extraction from v4/v3/outcome joined rows.
    for k in ("label_win", "outcome_binary", "win", "is_win"):
        if k in row:
            v = row.get(k)
            if isinstance(v, bool):
                return 1 if v else 0
            try:
                if str(v).strip() != "":
                    return 1 if int(float(v)) == 1 else 0
            except Exception:
                pass

    status = _stq_norm_text(row.get("outcome_status") or row.get("status"))
    target = _stq_norm_text(row.get("label_target") or row.get("first_hit") or row.get("target"))

    if status in ("CLOSED_WIN", "WIN", "TP", "TP1", "TP2", "TP3"):
        return 1
    if status in ("CLOSED_LOSS", "LOSS", "SL"):
        return 0
    if target in ("TP1", "TP2", "TP3"):
        return 1
    if target == "SL":
        return 0
    return None


def _stq_setup(row):
    return _stq_norm_text(row.get("setup_type") or row.get("setup") or row.get("mode"))


def _stq_symbol(row):
    return _stq_norm_text(row.get("symbol") or row.get("pair")).replace("BINANCE:", "").replace(".P", "")


def _stq_direction(row):
    d = _stq_norm_text(row.get("direction") or row.get("dir") or row.get("side"))
    if d in ("BUY", "LONG"):
        return "LONG"
    if d in ("SELL", "SHORT"):
        return "SHORT"
    return d


def _stq_score_bin(v):
    x = _stq_to_float(v)
    if x is None:
        return "NA"
    lo = int(x // 5) * 5
    return f"{lo}-{lo+4}"


_STAT_QUANT_CACHE = {"loaded_at": 0.0, "rows": [], "stats": {}}


def _stq_dataset_paths():
    import os
    from pathlib import Path

    raw = str(os.getenv("STAT_TECH_QUANT_PRIOR_DATASET", "")).strip()
    paths = []
    if raw:
        paths.append(Path(raw))

    paths.extend([
        Path(os.getenv("LOG_DIR", "logs")) / "ml_dataset_v4_current14_candidate_join.jsonl",
        Path(os.getenv("LOG_DIR", "logs")) / "ml_dataset_v4_candidate_join.jsonl",
        Path(os.getenv("LOG_DIR", "logs")) / "ml_dataset_v4_outcome_join_v1.jsonl",
    ])
    return paths


def _stq_load_rows():
    import json
    import time
    import os

    ttl = int(float(os.getenv("STAT_TECH_QUANT_PRIOR_CACHE_TTL_SEC", "300") or "300"))
    now = time.time()
    if _STAT_QUANT_CACHE.get("rows") and now - float(_STAT_QUANT_CACHE.get("loaded_at") or 0) <= ttl:
        return _STAT_QUANT_CACHE["rows"]

    rows = []
    seen = set()
    for p in _stq_dataset_paths():
        try:
            if not p.exists():
                continue
            for line in p.open("r", encoding="utf-8", errors="ignore"):
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                if not isinstance(r, dict):
                    continue
                y = _stq_label_win(r)
                if y is None:
                    continue
                sym = _stq_symbol(r)
                direction = _stq_direction(r)
                setup = _stq_setup(r)
                key = str(r.get("signal_key") or r.get("signal_id") or "")
                dedupe = key or f"{sym}|{direction}|{setup}|{len(rows)}"
                if dedupe in seen:
                    continue
                seen.add(dedupe)
                rows.append(r)
        except Exception:
            continue

    _STAT_QUANT_CACHE["loaded_at"] = now
    _STAT_QUANT_CACHE["rows"] = rows
    return rows


def _stq_bucket_add(stats, key, y):
    if not key:
        return
    d = stats.setdefault(key, {"n": 0, "wins": 0})
    d["n"] += 1
    d["wins"] += int(y)


def _stq_build_stats(rows):
    stats = {}
    for r in rows:
        y = _stq_label_win(r)
        if y is None:
            continue

        sym = _stq_symbol(r)
        direction = _stq_direction(r)
        setup = _stq_setup(r)
        regime = _stq_norm_text(r.get("regime"))
        score_bin = _stq_score_bin(r.get("technical_score") or r.get("score"))

        _stq_bucket_add(stats, "GLOBAL", y)
        _stq_bucket_add(stats, f"SYM|{sym}", y)
        _stq_bucket_add(stats, f"DIR|{direction}", y)
        _stq_bucket_add(stats, f"SETUP|{setup}", y)
        _stq_bucket_add(stats, f"REGIME|{regime}", y)
        _stq_bucket_add(stats, f"SCOREBIN|{score_bin}", y)
        _stq_bucket_add(stats, f"SYM_DIR|{sym}|{direction}", y)
        _stq_bucket_add(stats, f"DIR_SETUP|{direction}|{setup}", y)
        _stq_bucket_add(stats, f"SYM_SETUP|{sym}|{setup}", y)
        _stq_bucket_add(stats, f"SYM_DIR_SETUP|{sym}|{direction}|{setup}", y)
        _stq_bucket_add(stats, f"DIR_SETUP_SCOREBIN|{direction}|{setup}|{score_bin}", y)

    return stats


def _stq_stats():
    rows = _stq_load_rows()
    stats = _stq_build_stats(rows)
    return rows, stats


def _stat_tech_history_quant(payload):
    import os

    enabled = str(os.getenv("STAT_TECH_QUANT_PRIOR_ENABLED", "true")).strip().lower() in ("1", "true", "yes", "on")
    if not enabled:
        return {"ok": False, "reason": "stat_quant_disabled"}

    rows, stats = _stq_stats()
    global_stat = stats.get("GLOBAL") or {"n": 0, "wins": 0}
    if int(global_stat.get("n") or 0) <= 0:
        return {"ok": False, "reason": "no_labeled_history"}

    prior_wr = float(os.getenv("STAT_TECH_QUANT_PRIOR_WR", "0.60") or "0.60")
    alpha = float(os.getenv("STAT_TECH_QUANT_PRIOR_ALPHA", "20") or "20")
    min_n = int(float(os.getenv("STAT_TECH_QUANT_PRIOR_MIN_N", "8") or "8"))
    min_emit = float(os.getenv("LIVE_ENTRY_CONFLUENCE_MIN_QUANT_SCORE", "60") or "60")

    sym = _stq_symbol(payload)
    direction = _stq_direction(payload)
    setup = _stq_setup(payload)
    regime = _stq_norm_text(payload.get("regime"))
    score_bin = _stq_score_bin(payload.get("technical_score") or payload.get("score"))

    candidates = [
        ("SYM_DIR_SETUP", f"SYM_DIR_SETUP|{sym}|{direction}|{setup}"),
        ("DIR_SETUP_SCOREBIN", f"DIR_SETUP_SCOREBIN|{direction}|{setup}|{score_bin}"),
        ("SYM_DIR", f"SYM_DIR|{sym}|{direction}"),
        ("DIR_SETUP", f"DIR_SETUP|{direction}|{setup}"),
        ("SYM_SETUP", f"SYM_SETUP|{sym}|{setup}"),
        ("SETUP", f"SETUP|{setup}"),
        ("SCOREBIN", f"SCOREBIN|{score_bin}"),
        ("REGIME", f"REGIME|{regime}"),
        ("DIR", f"DIR|{direction}"),
        ("SYM", f"SYM|{sym}"),
        ("GLOBAL", "GLOBAL"),
    ]

    picked_name = "GLOBAL"
    picked_key = "GLOBAL"
    picked = global_stat

    for name, key in candidates:
        st = stats.get(key)
        if not st:
            continue
        if int(st.get("n") or 0) >= min_n or name == "GLOBAL":
            picked_name = name
            picked_key = key
            picked = st
            break

    n = int(picked.get("n") or 0)
    wins = int(picked.get("wins") or 0)
    raw_wr = (wins / n) if n > 0 else prior_wr
    smooth_wr = (wins + prior_wr * alpha) / (n + alpha) if n > 0 else prior_wr
    raw_score = round(max(0.0, min(100.0, smooth_wr * 100.0)), 1)

    return {
        "ok": True,
        "source": "STAT_TECH_HISTORY_PRIOR_V1",
        "basis": picked_name,
        "key": picked_key,
        "n": n,
        "wins": wins,
        "raw_wr": round(raw_wr, 4),
        "smoothed_wr": round(smooth_wr, 4),
        "raw_score": raw_score,
        "emit_quant_score": bool(raw_score >= min_emit),
        "min_emit": min_emit,
        "global_n": int(global_stat.get("n") or 0),
        "global_wins": int(global_stat.get("wins") or 0),
    }


def _inject_stat_tech_quant_prior(payload):
    try:
        if _stq_norm_text(payload.get("signal_source") or payload.get("source") or payload.get("engine")).find("STAT_TECH") < 0:
            return payload
        q = _stat_tech_history_quant(payload)
        payload["stat_quant"] = q
        if q.get("ok"):
            payload["stat_quant_score_raw"] = q.get("raw_score")
            payload["stat_quant_source"] = q.get("source")
            payload["stat_quant_basis"] = q.get("basis")
            payload["stat_quant_n"] = q.get("n")
            payload["stat_quant_wins"] = q.get("wins")
            # IMPORTANT:
            # Current confluence hard-blocks when quant_score exists but below min_quant.
            # So only emit quant_score when it is eligible to be a positive booster.
            if q.get("emit_quant_score"):
                payload["quant_score"] = q.get("raw_score")
                payload["core_quant_score"] = q.get("raw_score")
        return payload
    except Exception as e:
        payload["stat_quant"] = {"ok": False, "reason": f"exception:{e}"}
        return payload


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def env_bool(k, default=False):
    v = os.getenv(k)
    if v is None:
        return bool(default)
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")

def env_float(k, default):
    try:
        return float(os.getenv(k, str(default)))
    except Exception:
        return float(default)

def append_jsonl(path, row):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")

def load_state():
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"keys": {}, "pair_last": {}}

def save_state(st):
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(st, ensure_ascii=False, indent=2), encoding="utf-8")

def ts_now():
    return time.time()

def make_signal_key(r):
    return "|".join([
        str(r.get("symbol") or ""),
        str(r.get("direction") or ""),
        str(r.get("setup_type") or ""),
        str(round(float(r.get("entry") or 0), 8)),
        str(round(float(r.get("sl") or 0), 8)),
    ])

def call_bridge(payload):
    from app import main as main_app

    bridge = getattr(main_app, "_vps_execution_bridge", None)
    if not callable(bridge):
        return {
            "ok": False,
            "decision": "ERROR",
            "reason": "bridge_callable_missing",
        }

    res = bridge(payload)
    if inspect.iscoroutine(res):
        import asyncio
        res = asyncio.run(res)
    return res

def run_once():
    from app.stat_tech_signal_v1 import run_once as stat_run_once
    from app.live_entry_confluence_gate_v1 import evaluate_live_entry_confluence_gate_v1
    from app.live_rr12_plan_lock_v1 import apply_live_rr12_plan_lock_v1

    live_enabled = env_bool("STAT_TECH_LIVE_ENABLED", False)
    # STAT_TECH_NO_EXTRA_THROTTLE_PARITY_20260628
    cooldown_sec = env_float("STAT_TECH_COOLDOWN_MIN", 0.0) * 60.0
    now = ts_now()

    state = load_state()
    state["keys"] = {
        k: v for k, v in dict(state.get("keys") or {}).items()
        if now - float(v or 0) <= cooldown_sec
    }
    state["pair_last"] = {
        k: v for k, v in dict(state.get("pair_last") or {}).items()
        if now - float(v or 0) <= cooldown_sec
    }

    summary = stat_run_once(write_log=True)
    rows = summary.get("candidates") or []

    out = {
        "ok": True,
        "created_at_utc": utc_now_iso(),
        "marker": MARKER,
        "live_enabled": live_enabled,
        "total_candidates": len(rows),
        "sent": 0,
        "skipped": 0,
        "results": [],
    }

    for r in rows:
        symbol = str(r.get("symbol") or "").upper()
        key = make_signal_key(r)

        if key in state["keys"]:
            out["skipped"] += 1
            out["results"].append({"symbol": symbol, "skip": "duplicate_key_cooldown", "signal_key": key})
            continue

        if symbol in state["pair_last"]:
            out["skipped"] += 1
            out["results"].append({"symbol": symbol, "skip": "pair_cooldown", "signal_key": key})
            continue

        payload = _inject_stat_tech_linear_quant_v1(_inject_stat_tech_quant_prior(dict(r)))
        payload["signal_key"] = key
        payload["signal_id"] = key
        payload["source"] = "STAT_TECH_V1"
        payload["signal_source"] = "STAT_TECH_V1"
        payload["execution_owner"] = "STAT_TECH_PRIMARY"
        payload["status"] = "CANDIDATE"
        payload["rr"] = payload.get("rr") or 1.2
        payload["target_mode"] = "SINGLE_FULL"

        conf = evaluate_live_entry_confluence_gate_v1(payload)
        payload["live_entry_confluence_gate_v1"] = conf

        event = {
            "created_at_utc": utc_now_iso(),
            "marker": MARKER,
            "symbol": symbol,
            "direction": payload.get("direction"),
            "setup_type": payload.get("setup_type"),
            "signal_key": key,
            "technical_score": payload.get("technical_score"),
            "quant_score": payload.get("quant_score"),
            "stat_quant_score_raw": payload.get("stat_quant_score_raw"),
            "linear_quant_score": payload.get("linear_quant_score"),
            "linear_quant_emit": payload.get("linear_quant_emit"),
            "linear_quant_source": payload.get("linear_quant_source"),
            "confluence_decision": conf.get("decision"),
            "confluence_reason": conf.get("reason"),
            "confluence_score": conf.get("confluence_score"),
            "confluence_components": conf.get("components"),
            "live_enabled": live_enabled,
        }

        if str(conf.get("decision") or "").upper() != "ALLOW":
            event["final_decision"] = "NO_TRADE"
            event["final_reason"] = conf.get("reason") or "confluence_block"
            append_jsonl(LOG_PATH, event)
            out["results"].append(event)
            state["keys"][key] = now
            state["pair_last"][symbol] = now
            continue

        rr12 = apply_live_rr12_plan_lock_v1(payload)
        payload["live_rr12_plan_lock_v1"] = rr12
        event["rr12_decision"] = rr12.get("decision")
        event["rr12_reason"] = rr12.get("reason")

        if str(rr12.get("decision") or "").upper() == "BLOCK":
            event["final_decision"] = "NO_TRADE"
            event["final_reason"] = rr12.get("reason") or "rr12_block"
            append_jsonl(LOG_PATH, event)
            out["results"].append(event)
            state["keys"][key] = now
            state["pair_last"][symbol] = now
            continue

        if isinstance(rr12.get("payload"), dict):
            payload.update(dict(rr12.get("payload") or {}))

        if not live_enabled:
            event["final_decision"] = "DRY_RUN"
            event["final_reason"] = "stat_tech_live_disabled"
            append_jsonl(LOG_PATH, event)
            out["results"].append(event)
            state["keys"][key] = now
            state["pair_last"][symbol] = now
            continue

        bridge_res = call_bridge(payload)
        event["bridge_decision"] = bridge_res.get("decision") if isinstance(bridge_res, dict) else None
        event["bridge_reason"] = bridge_res.get("reason") if isinstance(bridge_res, dict) else None
        event["bridge_ok"] = bridge_res.get("ok") if isinstance(bridge_res, dict) else None
        event["final_decision"] = event.get("bridge_decision")
        event["final_reason"] = event.get("bridge_reason")
        append_jsonl(LOG_PATH, event)

        out["sent"] += 1
        out["results"].append(event)
        state["keys"][key] = now
        state["pair_last"][symbol] = now

    save_state(state)
    append_jsonl(LOG_PATH, {
        "created_at_utc": utc_now_iso(),
        "marker": MARKER,
        "event": "SUMMARY",
        **out,
    })
    return out

if __name__ == "__main__":
    print(json.dumps(run_once(), ensure_ascii=False, indent=2))
