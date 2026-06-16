#!/usr/bin/env python3
import json, re, os
from pathlib import Path
from datetime import datetime, timezone, timedelta

ROOT = Path(".")
WIB = timezone(timedelta(hours=7))

CANDIDATE_JSONL = [
    ("latency_audit", ROOT / "logs" / "latency_audit.jsonl"),
    ("night_accuracy_fix", ROOT / "logs" / "night_accuracy_fix_events.jsonl"),
]

BACKUP_PATTERNS = [
    "**/*latency*.bak*",
    "**/*night*.bak*",
    "**/*accuracy*.bak*",
    "**/*guard*.bak*",
    "**/*.bak.latency*",
    "**/*.bak.night*",
    "**/*.bak.accuracy*",
    "**/*.bak.guard*",
]

def to_wib(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).astimezone(WIB)

def parse_dt_str(x):
    if not x:
        return None
    s = str(x).strip().replace(" WIB", "").replace("Z", "+00:00")
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=WIB)
            return dt.astimezone(WIB)
        except Exception:
            pass
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=WIB)
        return dt.astimezone(WIB)
    except Exception:
        return None

def row_dt(r):
    for k in ("event_at_ms","created_ms","started_ms","time","transactTime","updateTime"):
        try:
            v = r.get(k)
            if v is not None and str(v).strip():
                return to_wib(int(float(v)))
        except Exception:
            pass

    for k in ("event_at_wib","created_at_wib","created_at_utc","event_at_utc","checked_at_utc"):
        dt = parse_dt_str(r.get(k))
        if dt:
            return dt

    return None

def candidates_from_jsonl():
    out = []
    for label, path in CANDIDATE_JSONL:
        if not path.exists():
            continue
        first = None
        count = 0
        for line in path.read_text(errors="ignore").splitlines():
            try:
                r = json.loads(line)
            except Exception:
                continue
            dt = row_dt(r)
            if not dt:
                continue
            count += 1
            if first is None or dt < first:
                first = dt
        if first:
            out.append({
                "source": str(path),
                "label": label,
                "dt": first,
                "method": "earliest_jsonl_event",
                "count": count,
            })
    return out

def candidates_from_backup_names():
    out = []
    seen = set()

    for pat in BACKUP_PATTERNS:
        for path in ROOT.glob(pat):
            if not path.is_file():
                continue
            sp = str(path)
            if sp in seen:
                continue
            seen.add(sp)

            # coba baca timestamp dari nama file: YYYYMMDD_HHMMSS
            m = re.search(r"(20\d{6})[_-](\d{6})", sp)
            if m:
                raw = m.group(1) + "_" + m.group(2)
                try:
                    dt = datetime.strptime(raw, "%Y%m%d_%H%M%S").replace(tzinfo=WIB)
                    out.append({
                        "source": sp,
                        "label": "backup_name",
                        "dt": dt,
                        "method": "timestamp_in_filename",
                        "count": 1,
                    })
                    continue
                except Exception:
                    pass

            # fallback mtime
            try:
                dt = datetime.fromtimestamp(path.stat().st_mtime, WIB)
                out.append({
                    "source": sp,
                    "label": "backup_mtime",
                    "dt": dt,
                    "method": "file_mtime",
                    "count": 1,
                })
            except Exception:
                pass
    return out

def main():
    candidates = []
    candidates += candidates_from_jsonl()
    candidates += candidates_from_backup_names()

    candidates = sorted(candidates, key=lambda x: x["dt"])

    if not candidates:
        print("NO_PATCH_MARKER_FOUND")
        print("Fallback manual: cek journal / backup file.")
        return 1

    print("=== EXECUTION PATCH SINCE CANDIDATES ===")
    for c in candidates[:30]:
        print(f"{c['dt'].strftime('%Y-%m-%d %H:%M:%S WIB')} | {c['label']} | {c['method']} | {c['source']} | count={c['count']}")

    # Untuk audit execution, prioritas:
    # 1. earliest night_accuracy_fix event kalau ada
    # 2. earliest latency_audit event
    # 3. backup marker
    preferred = None
    for label in ("night_accuracy_fix", "latency_audit"):
        xs = [c for c in candidates if c["label"] == label]
        if xs:
            preferred = xs[0]
            break

    if preferred is None:
        preferred = candidates[0]

    print("")
    print(f"PATCH_SINCE_WIB={preferred['dt'].strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"PATCH_SOURCE={preferred['source']}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
