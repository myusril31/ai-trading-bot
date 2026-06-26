#!/usr/bin/env python3
import argparse, collections, csv, json, math
from datetime import datetime, timezone, timedelta
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / 'logs'
REPORT_DIR = ROOT / 'reports'
REPORT_DIR.mkdir(exist_ok=True)

def read_jsonl(path):
    if not path.exists(): return []
    out=[]
    with path.open('r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            try: out.append(json.loads(line))
            except Exception: pass
    return out

def dt(x):
    try: return datetime.fromisoformat(str(x).replace('Z','+00:00')) if x else None
    except Exception: return None

def fl(x):
    try:
        if x is None or str(x).strip()=='' or str(x).lower()=='nan': return None
        v=float(x)
        if math.isnan(v) or math.isinf(v): return None
        return v
    except Exception: return None

def rr(direction, entry, sl, tp):
    entry=fl(entry); sl=fl(sl); tp=fl(tp)
    if entry is None or sl is None or tp is None: return None
    if str(direction).upper()=='LONG':
        risk=entry-sl
        reward=tp-entry
    else:
        risk=sl-entry
        reward=entry-tp
    if risk <= 0: return None
    return reward/risk

ap=argparse.ArgumentParser(); ap.add_argument('--hours', type=float, default=24); args=ap.parse_args()
cutoff = datetime.now(timezone.utc) - timedelta(hours=args.hours)

strict_by_key={}
for r in read_jsonl(LOG_DIR/'vps_smc_shadow_signals.jsonl'):
    t=dt(r.get('created_at_utc'))
    if t and t < cutoff: continue
    key=r.get('signal_key') or r.get('signal_id')
    if not key: continue
    v=rr(r.get('direction'), r.get('entry_mid') or r.get('entry'), r.get('sl'), r.get('tp1'))
    strict_by_key[key]={**r, 'rr_at_emit_derived': v}

core_rows=[]
for r in read_jsonl(LOG_DIR/'core_smc_shadow_candidates_v1.jsonl'):
    t=dt(r.get('created_at_utc'))
    if t and t >= cutoff: core_rows.append(r)

blocks=[]
for b in read_jsonl(LOG_DIR/'blocker_log_v1_derived.jsonl'):
    t=dt(b.get('created_at_utc'))
    if t and t >= cutoff: blocks.append(b)

joined=[]
for b in blocks:
    key=b.get('signal_key')
    s=strict_by_key.get(key, {})
    rr_emit=fl(s.get('rr_at_emit_derived'))
    rr_pre=fl(b.get('rr_at_precheck'))
    decay = rr_emit - rr_pre if rr_emit is not None and rr_pre is not None else None
    emit_dt=dt(s.get('created_at_utc'))
    block_dt=dt(b.get('created_at_utc'))
    mins=(block_dt-emit_dt).total_seconds()/60 if emit_dt and block_dt else None
    joined.append({
        'created_at_utc': b.get('created_at_utc'),
        'symbol': b.get('symbol'),
        'direction': b.get('direction'),
        'signal_key': key,
        'block_reason': b.get('block_reason'),
        'rr_at_emit': rr_emit,
        'rr_at_precheck': rr_pre,
        'rr_decay': decay,
        'mins_emit_to_block': mins,
        'ml_p_win': b.get('ml_p_win'),
    })

by_symbol=collections.defaultdict(list)
for j in joined:
    if j.get('rr_decay') is not None:
        by_symbol[j['symbol']].append(j)

def stats(vals):
    if not vals: return {'n':0}
    xs=[v for v in vals if v is not None]
    if not xs: return {'n':0}
    return {'n':len(xs), 'median':median(xs), 'avg':sum(xs)/len(xs), 'min':min(xs), 'max':max(xs)}

sym_stats={}
for sym, rows in by_symbol.items():
    sym_stats[sym]={
        'rr_decay': stats([r.get('rr_decay') for r in rows]),
        'rr_at_emit': stats([r.get('rr_at_emit') for r in rows]),
        'rr_at_precheck': stats([r.get('rr_at_precheck') for r in rows]),
        'mins_emit_to_block': stats([r.get('mins_emit_to_block') for r in rows]),
    }

core_counts=collections.Counter(r.get('state') for r in core_rows)
report={
    'created_at_utc': datetime.now(timezone.utc).isoformat(),
    'lookback_hours': args.hours,
    'strict_signal_count': len(strict_by_key),
    'block_count': len(blocks),
    'joined_decay_count': sum(1 for j in joined if j.get('rr_decay') is not None),
    'core_shadow_count': len(core_rows),
    'core_shadow_state_counts': dict(core_counts),
    'global_rr_decay': stats([j.get('rr_decay') for j in joined]),
    'global_mins_emit_to_block': stats([j.get('mins_emit_to_block') for j in joined]),
    'symbol_stats': sym_stats,
    'latest_joined': joined[-30:],
}
(REPORT_DIR/'rr_decay_report_v1.json').write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding='utf-8')
with (REPORT_DIR/'rr_decay_report_v1.csv').open('w', newline='', encoding='utf-8') as f:
    w=csv.DictWriter(f, fieldnames=['created_at_utc','symbol','direction','signal_key','block_reason','rr_at_emit','rr_at_precheck','rr_decay','mins_emit_to_block','ml_p_win'])
    w.writeheader(); w.writerows(joined)
print(json.dumps({k:report[k] for k in ['strict_signal_count','block_count','joined_decay_count','core_shadow_count','core_shadow_state_counts','global_rr_decay','global_mins_emit_to_block']}, indent=2, default=str))
print('[ok] wrote reports/rr_decay_report_v1.json and .csv')
