import sys, os, json, argparse, urllib.parse, urllib.request
sys.path.insert(0, "/app")

from pathlib import Path
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from collections import defaultdict, Counter
import app.main as m

ROOT = Path("/app")
EXEC_LOG = ROOT / "logs" / "execution_events.jsonl"
REPORT_DIR = ROOT / "reports"
REPORT_DIR.mkdir(exist_ok=True)
WIB = timezone(timedelta(hours=7))

def D(x):
    try:
        if x is None or x == "":
            return Decimal("0")
        return Decimal(str(x))
    except Exception:
        return Decimal("0")

def fmt(x, n=4):
    q = Decimal("1." + ("0" * n))
    return str(D(x).quantize(q)).rstrip("0").rstrip(".") or "0"

def pct(x):
    return f"{float(x):.2f}%"

def env_first(*names):
    for n in names:
        v = os.getenv(n)
        if v:
            return v
    return ""

def send_telegram(text):
    token = env_first("TELEGRAM_BOT_TOKEN", "TG_BOT_TOKEN", "BOT_TOKEN")
    chat_id = env_first("TELEGRAM_CHAT_ID", "TG_CHAT_ID", "TELEGRAM_ADMIN_CHAT_ID", "CHAT_ID")
    if not token or not chat_id:
        return {"ok": False, "reason": "missing_telegram_env"}

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": "true",
    }).encode()

    with urllib.request.urlopen(url, data=data, timeout=20) as r:
        return {"ok": True, "body": r.read().decode("utf-8", errors="ignore")[:500]}

def period_range(period):
    now = datetime.now(WIB)
    if period == "daily":
        start = datetime(now.year, now.month, now.day, tzinfo=WIB)
        label = start.strftime("%Y-%m-%d")
    elif period == "weekly":
        # last 7 days rolling
        start = now - timedelta(days=7)
        label = f"{start.strftime('%Y-%m-%d')} to {now.strftime('%Y-%m-%d')}"
    else:
        raise SystemExit("period must be daily or weekly")
    return start, now, int(start.timestamp() * 1000), int(now.timestamp() * 1000), label

def fetch_income(start_ms, end_ms):
    rows = []
    page = 1
    while page <= 30:
        res = m.live_signed_request("GET", "/fapi/v1/income", {
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": 1000,
            "page": page,
        })
        body = res.get("body")
        if not isinstance(body, list):
            break
        rows.extend(body)
        if len(body) < 1000:
            break
        page += 1
    return rows

def parse_wib(s):
    try:
        s = str(s or "").replace(" WIB", "")
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f%z").astimezone(WIB)
    except Exception:
        pass
    try:
        s = str(s or "").replace(" WIB", "")
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=WIB)
    except Exception:
        return None

def exec_stats(start, end):
    c = Counter()
    by_symbol = Counter()

    if not EXEC_LOG.exists():
        return c, by_symbol

    for line in EXEC_LOG.read_text(errors="ignore").splitlines():
        try:
            r = json.loads(line)
        except Exception:
            continue

        if r.get("action") != "LIVE_SMALL_CAPITAL_EXECUTE":
            continue

        dt = parse_wib(r.get("event_at_wib"))
        if not dt or dt < start or dt > end:
            continue

        sym = str(r.get("symbol") or "-")
        decision = str(r.get("decision") or "")

        c["exec_events"] += 1
        by_symbol[sym] += 1

        if decision == "LIVE_ORDER_PLACED":
            c["live_order_placed"] += 1
        if "PROTECTION_PARTIAL_OR_FAILED" in decision:
            c["protection_failed"] += 1
        if bool(r.get("protection_ok")):
            c["protection_ok"] += 1
        if "REJECT" in decision:
            c["reject"] += 1

    return c, by_symbol

def build_report(period):
    start, end, start_ms, end_ms, label = period_range(period)
    income_rows = fetch_income(start_ms, end_ms)

    by_type = defaultdict(Decimal)
    by_symbol_net = defaultdict(Decimal)
    by_day_net = defaultdict(Decimal)

    realized_rows = []
    total_net = Decimal("0")

    for r in income_rows:
        typ = str(r.get("incomeType") or "")
        inc = D(r.get("income"))
        sym = str(r.get("symbol") or "NO_SYMBOL")

        if typ not in ("REALIZED_PNL", "COMMISSION", "FUNDING_FEE"):
            continue

        total_net += inc
        by_type[typ] += inc
        by_symbol_net[sym] += inc

        t = int(r.get("time") or 0)
        if t:
            day = datetime.fromtimestamp(t / 1000, WIB).strftime("%Y-%m-%d")
            by_day_net[day] += inc

        if typ == "REALIZED_PNL" and inc != 0:
            realized_rows.append(r)

    wins = [D(r.get("income")) for r in realized_rows if D(r.get("income")) > 0]
    losses = [D(r.get("income")) for r in realized_rows if D(r.get("income")) < 0]

    win_count = len(wins)
    loss_count = len(losses)
    closed = win_count + loss_count
    wr = (Decimal(win_count) / Decimal(closed) * Decimal("100")) if closed else Decimal("0")

    gross_profit = sum(wins, Decimal("0"))
    gross_loss_abs = abs(sum(losses, Decimal("0")))
    profit_factor = (gross_profit / gross_loss_abs) if gross_loss_abs > 0 else Decimal("0")
    avg_win = (gross_profit / Decimal(win_count)) if win_count else Decimal("0")
    avg_loss = (gross_loss_abs / Decimal(loss_count)) if loss_count else Decimal("0")
    expectancy = (total_net / Decimal(closed)) if closed else Decimal("0")

    ex, ex_by_symbol = exec_stats(start, end)

    top_symbols = sorted(by_symbol_net.items(), key=lambda kv: kv[1], reverse=True)
    best = top_symbols[:5]
    worst = list(reversed(top_symbols[-5:])) if top_symbols else []

    title = "DAILY" if period == "daily" else "WEEKLY"
    lines = []
    lines.append(f"📊 {title} TRADE PERFORMANCE")
    lines.append(f"Period: {label} WIB")
    lines.append("")
    lines.append("💰 Actual PnL")
    lines.append(f"Net: {fmt(total_net)} USDT")
    lines.append(f"Gross Realized: {fmt(by_type.get('REALIZED_PNL', 0))} USDT")
    lines.append(f"Fees: {fmt(by_type.get('COMMISSION', 0))} USDT")
    lines.append(f"Funding: {fmt(by_type.get('FUNDING_FEE', 0))} USDT")
    lines.append("")
    lines.append("📈 Trade Stats")
    lines.append(f"Closed trades: {closed}")
    lines.append(f"Win/Loss: {win_count}/{loss_count}")
    lines.append(f"Winrate: {pct(wr)}")
    lines.append(f"Avg Win: {fmt(avg_win)} USDT")
    lines.append(f"Avg Loss: {fmt(avg_loss)} USDT")
    lines.append(f"Profit Factor: {fmt(profit_factor, 3)}")
    lines.append(f"Expectancy/trade: {fmt(expectancy)} USDT")
    lines.append("")
    lines.append("⚙️ Execution")
    lines.append(f"Exec events: {ex.get('exec_events', 0)}")
    lines.append(f"Order placed: {ex.get('live_order_placed', 0)}")
    lines.append(f"Protection OK: {ex.get('protection_ok', 0)}")
    lines.append(f"Protection failed: {ex.get('protection_failed', 0)}")
    lines.append(f"Reject: {ex.get('reject', 0)}")

    if period == "weekly" and by_day_net:
        lines.append("")
        lines.append("🗓 Daily Net")
        for d in sorted(by_day_net):
            lines.append(f"{d}: {fmt(by_day_net[d])} USDT")

    if best:
        lines.append("")
        lines.append("🏆 Best Symbols")
        for sym, val in best:
            lines.append(f"{sym}: {fmt(val)} USDT")

    if worst:
        lines.append("")
        lines.append("⚠️ Worst Symbols")
        for sym, val in worst:
            lines.append(f"{sym}: {fmt(val)} USDT")

    text = "\n".join(lines)

    payload = {
        "period": period,
        "label": label,
        "net": str(total_net),
        "realized": str(by_type.get("REALIZED_PNL", 0)),
        "commission": str(by_type.get("COMMISSION", 0)),
        "funding": str(by_type.get("FUNDING_FEE", 0)),
        "closed": closed,
        "wins": win_count,
        "losses": loss_count,
        "winrate_pct": str(wr),
        "avg_win": str(avg_win),
        "avg_loss": str(avg_loss),
        "profit_factor": str(profit_factor),
        "expectancy": str(expectancy),
        "execution": dict(ex),
        "by_symbol_net": {k: str(v) for k, v in by_symbol_net.items()},
        "by_day_net": {k: str(v) for k, v in by_day_net.items()},
        "text": text,
    }

    out_path = REPORT_DIR / f"performance_{period}_{datetime.now(WIB).strftime('%Y%m%d_%H%M%S')}.json"
    out_path.write_text(json.dumps(payload, indent=2))

    return payload

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--period", choices=["daily", "weekly"], required=True)
    ap.add_argument("--send", action="store_true")
    args = ap.parse_args()

    report = build_report(args.period)
    tg = None
    if args.send:
        tg = send_telegram(report["text"])

    print(json.dumps({
        "ok": True,
        "period": args.period,
        "sent": bool(args.send),
        "telegram": tg,
        "net": report["net"],
        "closed": report["closed"],
        "wins": report["wins"],
        "losses": report["losses"],
        "winrate_pct": report["winrate_pct"],
    }, indent=2))

if __name__ == "__main__":
    main()
