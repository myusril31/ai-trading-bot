import json, argparse, statistics, time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import defaultdict

ROOT = Path("/app") if Path("/app").exists() else Path(".")
LOG = ROOT / "logs" / "latency_audit.jsonl"
WIB = timezone(timedelta(hours=7))

def pct(vals, p):
    if not vals:
        return None
    vals = sorted(vals)
    idx = int(round((len(vals) - 1) * p))
    return vals[idx]

def parse_rows(hours):
    cutoff = int((time.time() - hours * 3600) * 1000)
    rows = []
    if not LOG.exists():
        return rows
    for line in LOG.read_text(errors="ignore").splitlines():
        try:
            r = json.loads(line)
        except Exception:
            continue
        if int(r.get("started_ms") or 0) >= cutoff:
            rows.append(r)
    return rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=float, default=24)
    ap.add_argument("--latest", type=int, default=30)
    args = ap.parse_args()

    rows = parse_rows(args.hours)
    groups = defaultdict(list)

    for r in rows:
        key = r.get("kind", "-")
        if r.get("kind") == "BINANCE_SIGNED_REQUEST":
            key = f"{r.get('method')} {r.get('path')} {r.get('type') or '-'}"
        groups[key].append(int(r.get("duration_ms") or 0))

    print("=== LATENCY AUDIT REPORT ===")
    print("time_wib:", datetime.now(WIB).isoformat(timespec="seconds"))
    print("log:", str(LOG))
    print("rows:", len(rows))
    print("window_hours:", args.hours)
    print("")

    for key, vals in sorted(groups.items(), key=lambda kv: (kv[0])):
        if not vals:
            continue
        print(key)
        print("  count:", len(vals))
        print("  avg_ms:", round(sum(vals) / len(vals), 2))
        print("  p50_ms:", pct(vals, 0.50))
        print("  p90_ms:", pct(vals, 0.90))
        print("  p95_ms:", pct(vals, 0.95))
        print("  max_ms:", max(vals))
        print("")

    print("=== LATEST EVENTS ===")
    for r in rows[-args.latest:]:
        out = {
            "at": r.get("event_at_wib"),
            "kind": r.get("kind"),
            "ms": r.get("duration_ms"),
            "method": r.get("method"),
            "path": r.get("path"),
            "symbol": r.get("symbol"),
            "type": r.get("type"),
            "side": r.get("side"),
            "client_order_id": r.get("client_order_id"),
            "binance_code": r.get("binance_code"),
            "binance_msg": r.get("binance_msg"),
            "status": r.get("status"),
            "has_error": r.get("has_error"),
        }
        print(json.dumps(out, ensure_ascii=False))
