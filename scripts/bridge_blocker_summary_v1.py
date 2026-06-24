#!/usr/bin/env python3
import argparse, collections, csv, json, math
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / 'logs'
REPORT_DIR = ROOT / 'reports'
REPORT_DIR.mkdir(exist_ok=True)

def parse_dt(x):
    if not x: return None
    try: return datetime.fromisoformat(str(x).replace('Z','+00:00'))
    except Exception: return None

def read_jsonl(path: Path):
    if not path.exists(): return []
    out=[]
    with path.open('r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            try: out.append(json.loads(line))
            except Exception: pass
    return out

def dig(d: Dict[str, Any], *keys):
    cur = d
    for k in keys:
        if not isinstance(cur, dict): return None
        cur = cur.get(k)
    return cur

def to_float(x):
    try:
        if x is None or str(x).strip()=='' or str(x).lower()=='nan': return None
        v=float(x)
        if math.isnan(v) or math.isinf(v): return None
        return v
    except Exception: return None

ap = argparse.ArgumentParser()
ap.add_argument('--hours', type=float, default=24)
ap.add_argument('--limit-latest', type=int, default=30)
args = ap.parse_args()
cutoff = datetime.now(timezone.utc) - timedelta(hours=args.hours)
rows=[]
for o in read_jsonl(LOG_DIR / 'vps_smc_bridge_events.jsonl'):
    dt=parse_dt(o.get('created_at_utc'))
    if dt and dt >= cutoff:
        rows.append(o)

results=[o for o in rows if o.get('event_type') == 'VPS_SMC_BRIDGE_RESULT']
prechecks=[o for o in rows if o.get('event_type') == 'VPS_SMC_BRIDGE_PRECHECK']
attempts=[o for o in rows if o.get('event_type') == 'VPS_SMC_BRIDGE_ATTEMPT']

decisions=collections.Counter(o.get('bridge_decision') or 'UNKNOWN' for o in results)
reasons=collections.Counter(o.get('bridge_reason') or 'UNKNOWN' for o in results)
sym_reasons=collections.Counter((o.get('symbol') or 'UNKNOWN', o.get('bridge_reason') or 'UNKNOWN') for o in results)

# Derived blocker log for RR decay script. Overwrite, not append, to avoid duplicates.
derived=[]
for o in results:
    raw=o.get('raw_bridge_result') if isinstance(o.get('raw_bridge_result'), dict) else {}
    guard=raw.get('guard') if isinstance(raw.get('guard'), dict) else {}
    pre=guard.get('pre_entry') if isinstance(guard.get('pre_entry'), dict) else {}
    ml=guard.get('ml_gate') if isinstance(guard.get('ml_gate'), dict) else {}
    rr_live = to_float(pre.get('rr_to_tp1') or pre.get('live_rr') or pre.get('rr'))
    row={
        'created_at_utc': o.get('created_at_utc'),
        'event_type': 'BLOCKER_EVENT_V1_DERIVED',
        'symbol': o.get('symbol'),
        'direction': o.get('direction'),
        'signal_key': o.get('signal_key') or o.get('signal_id'),
        'decision': o.get('bridge_decision'),
        'block_reason': o.get('bridge_reason'),
        'rr_at_precheck': rr_live,
        'tp1_touched': pre.get('tp1_touched'),
        'ml_p_win': to_float(ml.get('p_win') or ml.get('prob_win')),
        'ml_decision': ml.get('decision'),
        'raw_bridge_ok': raw.get('ok'),
        'production_action': 'NO_TRADE' if str(o.get('bridge_decision')).upper() not in ('OK','EXECUTE','ORDER_PLACED') else 'ALLOW_OR_EXECUTED',
    }
    derived.append(row)

with (LOG_DIR / 'blocker_log_v1_derived.jsonl').open('w', encoding='utf-8') as f:
    for r in derived:
        f.write(json.dumps(r, ensure_ascii=False, default=str) + '\n')

report={
    'created_at_utc': datetime.now(timezone.utc).isoformat(),
    'lookback_hours': args.hours,
    'source': 'logs/vps_smc_bridge_events.jsonl',
    'events_total': len(rows),
    'precheck_count': len(prechecks),
    'attempt_count': len(attempts),
    'result_count': len(results),
    'decision_counts': dict(decisions.most_common()),
    'reason_counts': dict(reasons.most_common()),
    'top_symbol_reasons': [{'symbol':s,'reason':r,'count':c} for (s,r),c in sym_reasons.most_common(30)],
    'latest_results': results[-args.limit_latest:],
    'derived_blocker_log': 'logs/blocker_log_v1_derived.jsonl',
}
(REPORT_DIR / 'bridge_blocker_summary_v1.json').write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding='utf-8')
with (REPORT_DIR / 'bridge_blocker_summary_v1.csv').open('w', newline='', encoding='utf-8') as f:
    w=csv.writer(f); w.writerow(['symbol','reason','count'])
    for (s,r),c in sym_reasons.most_common(): w.writerow([s,r,c])
print(json.dumps({k:report[k] for k in ['events_total','precheck_count','attempt_count','result_count','decision_counts','reason_counts']}, indent=2, default=str))
print('[ok] wrote reports/bridge_blocker_summary_v1.json, .csv and logs/blocker_log_v1_derived.jsonl')
