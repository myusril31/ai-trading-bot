#!/usr/bin/env python3
import argparse, collections, json
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
LOG_DIR=ROOT/'logs'; REPORT_DIR=ROOT/'reports'; REPORT_DIR.mkdir(exist_ok=True)

def read_jsonl(p):
    if not p.exists(): return []
    out=[]
    for line in p.open('r', encoding='utf-8', errors='ignore'):
        try: out.append(json.loads(line))
        except Exception: pass
    return out

def dt(x):
    try: return datetime.fromisoformat(str(x).replace('Z','+00:00')) if x else None
    except Exception: return None

ap=argparse.ArgumentParser(); ap.add_argument('--hours', type=float, default=24); args=ap.parse_args()
cutoff=datetime.now(timezone.utc)-timedelta(hours=args.hours)
strict=[]; core=[]
for r in read_jsonl(LOG_DIR/'vps_smc_shadow_signals.jsonl'):
    t=dt(r.get('created_at_utc'))
    if t and t>=cutoff: strict.append(r)
for r in read_jsonl(LOG_DIR/'core_smc_shadow_candidates_v1.jsonl'):
    t=dt(r.get('created_at_utc'))
    if t and t>=cutoff: core.append(r)
strict_keys=set((r.get('symbol'), r.get('direction'), r.get('confirmed_bucket_ms') or r.get('signal_key')) for r in strict)
core_ok=[r for r in core if r.get('state')=='CORE_CANDIDATE']
core_invalid=[r for r in core if r.get('state')!='CORE_CANDIDATE']
report={
 'created_at_utc': datetime.now(timezone.utc).isoformat(),
 'lookback_hours': args.hours,
 'strict_confirmed_count': len(strict),
 'core_shadow_count': len(core),
 'core_candidate_count': len(core_ok),
 'core_invalid_count': len(core_invalid),
 'core_by_symbol': dict(collections.Counter(r.get('symbol') for r in core_ok).most_common()),
 'strict_by_symbol': dict(collections.Counter(r.get('symbol') for r in strict).most_common()),
 'core_state_counts': dict(collections.Counter(r.get('state') for r in core).most_common()),
 'core_invalid_reasons': dict(collections.Counter((r.get('strict_invalid_reason') or 'unknown') for r in core_invalid).most_common(20)),
 'latest_core': core[-25:],
 'latest_strict': strict[-25:],
}
(REPORT_DIR/'strict_vs_core_smc_compare_v1.json').write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding='utf-8')
print(json.dumps({k:report[k] for k in ['strict_confirmed_count','core_shadow_count','core_candidate_count','core_invalid_count','core_state_counts']}, indent=2, default=str))
print('[ok] wrote reports/strict_vs_core_smc_compare_v1.json')
