# TELEGRAM_MARKET_REPORT_STAT_TECH_PRIMARY_20260628
# MARKET_HEALTH_REPORT_STAT_TECH_PRIMARY_20260628
#!/usr/bin/env python3
import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
# TELEGRAM_MARKET_REPORT_STAT_TECH_JSONL_20260628
def stat_tech_jsonl_metrics(hours=2):
    import json
    from datetime import datetime, timezone, timedelta
    p = Path("logs/stat_tech_live_bridge_events_v1.jsonl")
    out = {
        "stat_tech_tick": 0,
        "stat_tech_summary": 0,
        "stat_tech_candidates": 0,
        "stat_tech_blocked": 0,
        "stat_tech_allowed": 0,
        "stat_tech_rr12": 0,
        "stat_tech_bridge": 0,
    }
    if not p.exists():
        return out

    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    def parse_dt(v):
        if not v:
            return None
        try:
            return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        except Exception:
            return None

    try:
        for line in p.open("r", encoding="utf-8", errors="ignore"):
            try:
                r = json.loads(line)
            except Exception:
                continue
            dt = parse_dt(r.get("created_at_utc"))
            if dt is not None and dt < cutoff:
                continue

            if r.get("event") == "SUMMARY":
                out["stat_tech_summary"] += 1
                out["stat_tech_tick"] += 1
                continue

            if r.get("symbol"):
                out["stat_tech_candidates"] += 1

            if r.get("confluence_decision") == "BLOCK":
                out["stat_tech_blocked"] += 1
            if r.get("confluence_decision") == "ALLOW":
                out["stat_tech_allowed"] += 1
            if r.get("rr12_decision"):
                out["stat_tech_rr12"] += 1
            if r.get("bridge_decision"):
                out["stat_tech_bridge"] += 1
    except Exception:
        return out

    return out

from datetime import datetime, timezone, timedelta

ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / ".env"
STATE_DIR = ROOT / "state" / "market_data"
WIB = timezone(timedelta(hours=7))

INTERVAL_MS = {
    "5m": 5 * 60 * 1000,
    "15m": 15 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
}

DEFAULT_THRESHOLDS = {
    "5m": 780,
    "15m": 1500,
    "4h": 18000,
}

def now_wib():
    return datetime.now(WIB)

def fmt_wib_ms(ms):
    if not ms:
        return "-"
    return datetime.fromtimestamp(ms / 1000, timezone.utc).astimezone(WIB).strftime("%Y-%m-%d %H:%M:%S WIB")

def load_env():
    env = {}
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text(errors="ignore").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    return env

ENV = load_env()

def env_get(k, default=""):
    return os.getenv(k) or ENV.get(k) or str(default)

def env_int(k, default):
    try:
        return int(str(env_get(k, default)).strip())
    except Exception:
        return int(default)

def norm_symbol(x):
    x = str(x or "").strip().upper()
    x = x.replace("BINANCE:", "").replace(".P", "")
    return re.sub(r"[^A-Z0-9]", "", x)

def env_symbols(k, default=""):
    raw = env_get(k, default)
    out = []
    for part in raw.split(","):
        s = norm_symbol(part)
        if s and s not in out:
            out.append(s)
    return out

def pair_allowlist():
    symbols = env_symbols("PAIR_ALLOWLIST", "")
    maxn = env_int("VPS_SMC_MAX_SYMBOLS_PER_RUN", 14)
    return symbols[:maxn] if maxn > 0 else symbols

def thresholds():
    return {
        "5m": env_int("VPS_SMC_STAGEB_5M_MAX_AGE_SEC", DEFAULT_THRESHOLDS["5m"]),
        "15m": env_int("VPS_SMC_ENTRY_15M_MAX_AGE_SEC", DEFAULT_THRESHOLDS["15m"]),
        "4h": env_int("VPS_SMC_HTF_4H_MAX_AGE_SEC", DEFAULT_THRESHOLDS["4h"]),
    }

def close_ms_from_row(row, tf):
    for k in ("close_time_ms", "closeTime", "t_close"):
        v = row.get(k)
        if v:
            return int(v)

    for k in ("open_time_ms", "openTime", "t"):
        v = row.get(k)
        if v:
            return int(v) + INTERVAL_MS[tf] - 1000

    return 0

def latest_row(symbol, tf):
    path = STATE_DIR / f"{symbol}_{tf}.jsonl"

    if not path.exists():
        return None, "MISSING_FILE"

    lines = [x for x in path.read_text(errors="ignore").splitlines() if x.strip()]
    if not lines:
        return None, "EMPTY_FILE"

    rows = []
    for line in lines[-30:]:
        try:
            rows.append(json.loads(line))
        except Exception:
            pass

    if not rows:
        return None, "BAD_JSON"

    rows = sorted(
        rows,
        key=lambda r: int(r.get("open_time_ms") or r.get("openTime") or r.get("t") or 0)
    )

    return rows[-1], None

def freshness_report(symbols):
    th = thresholds()
    now_ms = int(time.time() * 1000)

    tf_summary = {}
    bad_warn = []

    for tf in ("5m", "15m", "4h"):
        ok = warn = bad = 0
        worst = None

        max_age = th[tf]
        warn_age = int(max_age * 0.85)

        for sym in symbols:
            row, err = latest_row(sym, tf)

            if err:
                bad += 1
                item = {
                    "symbol": sym,
                    "tf": tf,
                    "status": "BAD",
                    "age_min": None,
                    "close": "-",
                    "max_min": max_age / 60,
                    "issues": err,
                }
                bad_warn.append(item)
                if worst is None:
                    worst = item
                continue

            c_ms = close_ms_from_row(row, tf)
            age_sec = max(0, (now_ms - c_ms) / 1000)
            age_min = age_sec / 60

            item = {
                "symbol": sym,
                "tf": tf,
                "status": "OK",
                "age_min": age_min,
                "close": fmt_wib_ms(c_ms),
                "max_min": max_age / 60,
                "issues": "",
            }

            if age_sec > max_age:
                bad += 1
                item["status"] = "BAD"
                item["issues"] = "STALE"
                bad_warn.append(item)
            elif age_sec > warn_age:
                warn += 1
                item["status"] = "WARN"
                item["issues"] = "NEAR_STALE"
                bad_warn.append(item)
            else:
                ok += 1

            if worst is None or (item["age_min"] is not None and (worst.get("age_min") is None or item["age_min"] > worst["age_min"])):
                worst = item

        tf_summary[tf] = {
            "ok": ok,
            "warn": warn,
            "bad": bad,
            "total": len(symbols),
            "worst": worst,
            "max_age_sec": max_age,
        }

    return tf_summary, bad_warn

def sh(cmd):
    try:
        return subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True).strip()
    except subprocess.CalledProcessError as e:
        return f"ERR:{e.output.strip() or e}"
    except Exception as e:
        return f"ERR:{type(e).__name__}:{e}"

def service_status():
    return sh(["systemctl", "is-active", "vps-stat-tech-live-loop.service"])

def bot_status():
    out = sh(["docker", "ps", "--format", "{{.Names}}|{{.Status}}"])
    for line in out.splitlines():
        if line.startswith("ai-trading-bot|"):
            return line.split("|", 1)[1]
    return "not found"

def journal_lines(hours):
    out = sh([
        "journalctl",
        "-u", "vps-stat-tech-live-loop.service",
        "--since", f"{int(hours)} hours ago",
        "--no-pager",
    ])
    if out.startswith("ERR:"):
        return []
    return out.splitlines()

def scheduler_stats(hours):
    lines = journal_lines(hours)

    stats = {
        "market_ok": 0,
        "market_full_ok": 0,
        "market_partial_ok": 0,
        "market_bad": 0,
        "skip_tick": 0,
        "stale_skip_pair": 0,
        "smc_tick_v2": 0,
        "smc_batch_done": 0,
        "smc_batch_timeout": 0,
        "smc_batch_failed": 0,
        "timeout": 0,
        "full_repair": 0,
        "restart_bot": 0,
        "signals_seen": 0,
        "last_warn_bad": [],
        "last_good": [],
        "mode_lines": [],
    }

    signal_re = re.compile(r"signal_count=([0-9]+)")
    batch_done_re = re.compile(r"SMC batch \d+/\d+ done")
    batch_timeout_re = re.compile(r"SMC batch \d+/\d+ timeout")
    batch_failed_re = re.compile(r"SMC batch \d+/\d+ failed")

    for line in lines:
        if "SCHEDULER_MODE=" in line or "SCHEDULER_CONFIG=" in line:
            stats["mode_lines"].append(line)

        if "MARKET_DATA_OK" in line:
            stats["market_ok"] += 1
            stats["last_good"].append(line)

        if "MARKET_DATA_FULL_OK" in line:
            stats["market_full_ok"] += 1
            stats["last_good"].append(line)

        if "MARKET_DATA_PARTIAL_OK" in line:
            stats["market_partial_ok"] += 1
            stats["last_good"].append(line)

        if "MARKET_DATA_BAD" in line:
            stats["market_bad"] += 1
            stats["last_warn_bad"].append(line)

        if "MARKET_DATA_SKIP_TICK" in line:
            stats["skip_tick"] += 1
            stats["last_warn_bad"].append(line)

        if "STALE_SKIP_PAIR" in line:
            stats["stale_skip_pair"] += 1
            stats["last_warn_bad"].append(line)

        if "SMC tick done batches_ok=" in line:
            stats["smc_tick_v2"] += 1
            stats["last_good"].append(line)

        if batch_done_re.search(line):
            stats["smc_batch_done"] += 1

        if batch_timeout_re.search(line):
            stats["smc_batch_timeout"] += 1
            stats["last_warn_bad"].append(line)

        if batch_failed_re.search(line):
            stats["smc_batch_failed"] += 1
            stats["last_warn_bad"].append(line)

        # Old timeout wording and subprocess timeout
        if "TimeoutExpired" in line or " timed out " in line or " timeout returncode=" in line:
            stats["timeout"] += 1
            stats["last_warn_bad"].append(line)

        if "full-repair" in line or "full repair" in line:
            stats["full_repair"] += 1
            stats["last_warn_bad"].append(line)

        if "restart bot" in line or "restart bot container" in line:
            stats["restart_bot"] += 1
            stats["last_warn_bad"].append(line)

        for m in signal_re.finditer(line):
            try:
                stats["signals_seen"] += int(m.group(1))
            except Exception:
                pass

    stats["last_warn_bad"] = stats["last_warn_bad"][-8:]
    stats["last_good"] = stats["last_good"][-8:]
    stats["mode_lines"] = stats["mode_lines"][-4:]
    return stats

def status_from(tf_summary, sched, scheduler_active):
    bad_now = sum(x["bad"] for x in tf_summary.values())
    warn_now = sum(x["warn"] for x in tf_summary.values())

    if scheduler_active != "active":
        return "BAD", "stat-tech live loop inactive"

    if bad_now > 0:
        return "BAD", "current candle stale/bad"

    if warn_now > 0:
        return "WARN", "current candle near stale"

    if sched["smc_batch_timeout"] > 0 or sched["smc_batch_failed"] > 0:
        return "WARN", "batch timeout/fail seen in window"

    # Old bad lines in rolling window should not make current status BAD if current freshness is clean.
    if sched["skip_tick"] > 0 and sched["smc_tick_v2"] == 0:
        return "WARN", "skip tick seen and no v2 tick yet"

    if sched["market_ok"] == 0 or sched["smc_tick_v2"] == 0:
        return "WARN", "fresh; stat-tech loop active, waiting for next signal summary"

    return "OK", "fresh + v2 scheduler scanning"

def short_line(line, n=190):
    line = re.sub(r"\s+", " ", line).strip()
    return line if len(line) <= n else line[:n-3] + "..."


# === STAT_TECH_BRIDGE_PIPELINE_REPORT_V1_20260703 ===
def _stat_tech_bridge_pipeline_report_v1(window_hours=2):
    import os
    import json
    from pathlib import Path
    from datetime import datetime, timezone, timedelta
    from collections import Counter, deque

    def parse_ts(x):
        if not x:
            return None
        try:
            txt = str(x).strip().replace("Z", "+00:00")
            dt = datetime.fromisoformat(txt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None

    def norm(x):
        return str(x or "").strip().upper()

    def clean(x):
        txt = str(x or "-").strip()
        return txt if txt else "-"

    def top_fmt(counter, limit=5):
        if not counter:
            return "-"
        return ", ".join([f"{k}:{v}" for k, v in counter.most_common(limit)])

    log_dir = Path(os.getenv("LOG_DIR", "logs"))
    fp = log_dir / "stat_tech_live_bridge_events_v1.jsonl"

    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=float(window_hours or 2))

    raw = deque(maxlen=30000)
    if fp.exists():
        try:
            with fp.open("r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        raw.append(json.loads(line))
                    except Exception:
                        pass
        except Exception:
            pass

    events = []
    for r in raw:
        if not isinstance(r, dict) or not r.get("symbol"):
            continue
        ts = parse_ts(r.get("created_at_utc"))
        if ts and ts < since:
            continue
        events.append(r)

    # Dedup latest per signal_key. Kalau signal_key kosong, fallback by symbol/dir/setup/entry/sl.
    latest = {}
    latest_ts = {}
    for r in events:
        key = str(
            r.get("signal_key")
            or "|".join([
                str(r.get("symbol") or ""),
                str(r.get("direction") or ""),
                str(r.get("setup_type") or ""),
                str(r.get("technical_score") or ""),
            ])
        )
        ts = parse_ts(r.get("created_at_utc")) or datetime.min.replace(tzinfo=timezone.utc)
        if key not in latest or ts >= latest_ts.get(key, datetime.min.replace(tzinfo=timezone.utc)):
            latest[key] = r
            latest_ts[key] = ts

    uniq = list(latest.values())

    total_rows = len(events)
    unique_signals = len(uniq)

    confluence_allow = sum(1 for r in uniq if norm(r.get("confluence_decision")) == "ALLOW")
    confluence_block = sum(1 for r in uniq if norm(r.get("confluence_decision")) == "BLOCK")

    rr12_allow = sum(1 for r in uniq if norm(r.get("rr12_decision")) == "ALLOW")
    rr12_block = sum(1 for r in uniq if norm(r.get("rr12_decision")) == "BLOCK")
    rr12_reached = sum(1 for r in uniq if r.get("rr12_decision") is not None or r.get("rr12_reason") is not None)

    bridge_reached = sum(1 for r in uniq if r.get("bridge_decision") is not None or r.get("bridge_reason") is not None)
    bridge_ok = sum(1 for r in uniq if r.get("bridge_ok") is True)
    bridge_fail = sum(1 for r in uniq if r.get("bridge_ok") is False)

    final_counter = Counter(clean(r.get("final_decision")) for r in uniq)
    final_reason_counter = Counter(clean(r.get("final_reason")) for r in uniq)
    conf_reason_counter = Counter(clean(r.get("confluence_reason")) for r in uniq)
    bridge_reason_counter = Counter(clean(r.get("bridge_reason")) for r in uniq if r.get("bridge_reason"))

    by_symbol = Counter(clean(r.get("symbol")) for r in uniq)
    by_setup = Counter(clean(r.get("setup_type")) for r in uniq)

    live_issue_keys = (
        "LIVE_ENTRY_FAILED",
        "live_preflight_failed",
        "preflight",
        "binance",
        "qty_",
        "min_notional",
        "position",
        "order",
    )
    live_issue_n = 0
    for r in uniq:
        txt = "|".join([
            str(r.get("bridge_reason") or ""),
            str(r.get("final_reason") or ""),
            str(r.get("bridge_decision") or ""),
            str(r.get("final_decision") or ""),
        ]).lower()
        if any(k.lower() in txt for k in live_issue_keys):
            live_issue_n += 1

    status_hint = "OK"
    reason_hint = "bridge healthy or no actionable signal"

    if unique_signals <= 0:
        status_hint = "OK"
        reason_hint = "bridge idle; no STAT_TECH candidates in window"
    elif live_issue_n > 0 or bridge_fail > 0:
        status_hint = "WARN"
        reason_hint = f"bridge/execution issue detected: {top_fmt(bridge_reason_counter or final_reason_counter, 2)}"
    elif confluence_allow > 0 and rr12_allow > 0 and bridge_reached <= 0:
        status_hint = "WARN"
        reason_hint = "confluence/RR12 passed but bridge not reached"
    elif confluence_block == unique_signals:
        status_hint = "OK"
        reason_hint = "all candidates blocked by confluence; no execution issue"
    elif bridge_reached > 0:
        status_hint = "OK"
        reason_hint = "bridge reached; no execution failure detected"

    lines = []
    lines.append(f"Bridge pipeline {window_hours}h:")
    lines.append(f"- bridge event rows: {total_rows} | unique signals: {unique_signals}")
    lines.append(f"- candidates by setup: {top_fmt(by_setup, 4)}")
    lines.append(f"- candidates by pair: {top_fmt(by_symbol, 8)}")
    lines.append(f"- confluence ALLOW/BLOCK: {confluence_allow}/{confluence_block}")
    lines.append(f"- RR12 reached/ALLOW/BLOCK: {rr12_reached}/{rr12_allow}/{rr12_block}")
    lines.append(f"- bridge reached/ok/fail: {bridge_reached}/{bridge_ok}/{bridge_fail}")
    lines.append(f"- final decisions: {top_fmt(final_counter, 5)}")
    lines.append(f"- top blockers: {top_fmt(final_reason_counter, 5)}")
    lines.append(f"- confluence reasons: {top_fmt(conf_reason_counter, 4)}")
    if bridge_reason_counter:
        lines.append(f"- bridge reasons: {top_fmt(bridge_reason_counter, 4)}")

    latest_list = sorted(
        uniq,
        key=lambda r: parse_ts(r.get("created_at_utc")) or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )[:6]

    if latest_list:
        lines.append("- latest:")
        for r in latest_list:
            sym = clean(r.get("symbol"))
            d = clean(r.get("direction"))
            setup = clean(r.get("setup_type"))
            cscore = r.get("confluence_score")
            cdec = clean(r.get("confluence_decision"))
            rr = clean(r.get("rr12_decision"))
            br = clean(r.get("bridge_decision"))
            fr = clean(r.get("final_reason"))
            lines.append(f"  {sym} {d} {setup} | conf={cdec} {cscore} | rr12={rr} | bridge={br} | {fr}")

    return {
        "status_hint": status_hint,
        "reason_hint": reason_hint,
        "unique_signals": unique_signals,
        "bridge_reached": bridge_reached,
        "bridge_fail": bridge_fail,
        "live_issue_n": live_issue_n,
        "lines": lines,
    }


def build_message(hours):
    symbols = pair_allowlist()
    tf_summary, bad_warn = freshness_report(symbols)
    sched = scheduler_stats(hours)
    statm = stat_tech_jsonl_metrics(2)
    scheduler_active = service_status()
    bot = bot_status()

    # === REPORT_CONTAINER_CONTEXT_FALLBACK_20260703 ===
    # When this script is executed inside Docker, systemctl/docker may not exist.
    # If STAT_TECH logs are progressing, treat loop as alive by log evidence.
    try:
        if str(scheduler_active).startswith("ERR:") and int(statm.get("stat_tech_tick") or 0) > 0 and int(statm.get("stat_tech_summary") or 0) > 0:
            scheduler_active = "active_by_log"
        if str(bot).strip().lower() in ("not found", "err", "") and int(statm.get("stat_tech_tick") or 0) > 0:
            bot = "running_by_log"
    except Exception:
        pass

    bridge = _stat_tech_bridge_pipeline_report_v1(hours)
    status, reason = status_from(tf_summary, sched, "active" if scheduler_active == "active_by_log" else scheduler_active)

    # Bridge-aware status:
    # - no signal/candidate is OK if market data fresh + loop active
    # - bridge/live execution failure must WARN
    try:
        bh = str((bridge or {}).get("status_hint") or "").upper()
        br = str((bridge or {}).get("reason_hint") or "")

        if status != "BAD" and bh == "WARN":
            status = "WARN"
            reason = br
        elif status == "WARN":
            low_reason = str(reason or "").lower()
            only_waiting_signal = (
                "fresh" in low_reason
                and "stat-tech loop active" in low_reason
                and (
                    "waiting for next signal summary" in low_reason
                    or "signals seen: 0" in low_reason
                    or "no signal" in low_reason
                )
            )
            if only_waiting_signal and bh in ("OK", "IDLE", ""):
                status = "OK"
                reason = br or "fresh; stat-tech loop active; no actionable signal in bridge window"
    except Exception:
        pass

    emoji = "✅" if status == "OK" else ("⚠️" if status == "WARN" else "🚨")

    out = []
    out.append(f"{emoji} MARKET DATA HEALTH REPORT v2")
    out.append(f"Status: {status}")
    out.append(f"Reason: {reason}")
    out.append(f"Time: {now_wib().strftime('%Y-%m-%d %H:%M:%S WIB')}")
    out.append(f"Pairs: {len(symbols)}")
    out.append(f"Bot: {bot}")
    out.append(f"STAT_TECH Loop: {scheduler_active}")
    out.append("")

    out.append("Freshness:")
    for tf in ("5m", "15m", "4h"):
        s = tf_summary[tf]
        w = s["worst"] or {}
        age = w.get("age_min")
        age_txt = "-" if age is None else f"{age:.2f}m"
        out.append(
            f"- {tf}: OK {s['ok']}/{s['total']} | WARN {s['warn']} | BAD {s['bad']} "
            f"| worst {w.get('symbol','-')} {age_txt} close={w.get('close','-')} max={s['max_age_sec']/60:.1f}m"
        )

    out.append("")
    out.append(f"Scheduler {int(hours)}h v2-aware:")
    out.append(f"- MARKET_DATA_OK: {sched['market_ok']}")
    out.append(f"- FULL_OK: {sched['market_full_ok']} | PARTIAL_OK: {sched['market_partial_ok']}")
    out.append(f"- MARKET_DATA_BAD: {sched['market_bad']} | SKIP_TICK: {sched['skip_tick']}")
    out.append(f"- STALE_SKIP_PAIR: {sched['stale_skip_pair']}")
    out.append(f"- STAT_TECH tick: {statm['stat_tech_tick']}")
    out.append(f"- STAT_TECH loop summary: {statm['stat_tech_summary']}")
    out.append(f"- batch timeout/fail: {sched['smc_batch_timeout']}/{sched['smc_batch_failed']}")
    out.append(f"- old TimeoutExpired: {sched['timeout']}")
    out.append(f"- full-repair: {sched['full_repair']}")
    out.append(f"- restart bot: {sched['restart_bot']}")
    out.append(f"- signals seen (scheduler legacy): {sched['signals_seen']}")
    try:
        out.append(f"- bridge unique signals: {(bridge or {}).get('unique_signals', 0)}")
    except Exception:
        out.append("- bridge unique signals: -")

    if bad_warn:
        out.append("")
        out.append("Current bad/warn candles:")
        for item in bad_warn[:12]:
            age = item.get("age_min")
            age_txt = "-" if age is None else f"{age:.2f}m"
            out.append(
                f"- {item['symbol']} {item['tf']} {item['status']} age={age_txt} "
                f"max={item['max_min']:.1f}m issues={item['issues']}"
            )
        if len(bad_warn) > 12:
            out.append(f"- ... +{len(bad_warn)-12} more")

    if sched["last_warn_bad"]:
        out.append("")
        out.append("Last warn/bad scheduler lines:")
        for line in sched["last_warn_bad"][-5:]:
            out.append("- " + short_line(line))

    if sched["mode_lines"]:
        out.append("")
        out.append("Scheduler mode:")
        for line in sched["mode_lines"][-2:]:
            out.append("- " + short_line(line))

    try:
        bridge_lines = list((bridge or {}).get("lines") or [])
        if bridge_lines:
            out.append("")
            out.extend(bridge_lines)
    except Exception as e:
        out.append("")
        out.append(f"Bridge pipeline {int(hours)}h: ERROR {str(e)[:120]}")

    out.append("")
    if status == "OK":
        out.append("Action: mesin fresh + partial-batch scan normal. Signal baru boleh dianggap fresh kalau lolos execution guard.")
    elif status == "WARN":
        out.append("Action: monitor. Jangan pakai stale pair; partial-batch tetap boleh scan pair fresh.")
    else:
        out.append("Action: restore market data/bootstrap dulu. Jangan scan/entry kalau market data stale atau STAT_TECH loop inactive.")

    return "\n".join(out)

def split_msg(text, limit=3800):
    parts = []
    cur = []
    cur_len = 0
    for line in text.splitlines():
        add = len(line) + 1
        if cur and cur_len + add > limit:
            parts.append("\n".join(cur))
            cur = [line]
            cur_len = add
        else:
            cur.append(line)
            cur_len += add
    if cur:
        parts.append("\n".join(cur))
    return parts

def send_telegram(text):
    token = None
    chat_id = None

    token_keys = [
        "TELEGRAM_BOT_TOKEN", "TELEGRAM_TOKEN", "TG_BOT_TOKEN",
        "BOT_TOKEN", "TELEGRAM_API_TOKEN", "TELEGRAM_BOT"
    ]
    chat_keys = [
        "TELEGRAM_CHAT_ID", "TG_CHAT_ID", "CHAT_ID",
        "TELEGRAM_TO_CHAT_ID", "TELEGRAM_DEFAULT_CHAT_ID"
    ]

    for k in token_keys:
        v = env_get(k, "")
        if v:
            token = v
            break

    for k in chat_keys:
        v = env_get(k, "")
        if v:
            chat_id = v
            break

    if not token or not chat_id:
        return {"ok": False, "reason": "missing telegram token/chat id"}

    results = []
    for part in split_msg(text):
        data = urllib.parse.urlencode({
            "chat_id": chat_id,
            "text": part,
            "disable_web_page_preview": "true",
        }).encode()

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        try:
            with urllib.request.urlopen(url, data=data, timeout=20) as r:
                body = r.read().decode("utf-8", errors="replace")
            results.append({"ok": True, "body": body[:500]})
        except Exception as e:
            results.append({"ok": False, "error": f"{type(e).__name__}:{e}"})

    return {"ok": all(x.get("ok") for x in results), "parts": len(results), "results": results}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--hours", type=int, default=2)
    ap.add_argument("--no-send", action="store_true")
    args = ap.parse_args()

    msg = build_message(args.hours)
    print(msg)

    if not args.no_send:
        result = send_telegram(msg)
        print(json.dumps({"send": result, "overall": "OK" if result.get("ok") else "ERR"}, indent=2))

if __name__ == "__main__":
    main()
