#!/usr/bin/env python3
import json, collections
from datetime import datetime, timezone
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
LOG_DIR=ROOT/'logs'; REPORT_DIR=ROOT/'reports'; REPORT_DIR.mkdir(exist_ok=True)

def load_json(p):
    try: return json.loads(p.read_text(encoding='utf-8')) if p.exists() else {}
    except Exception: return {}

def read_jsonl(p, tail=5000):
    if not p.exists(): return []
    lines=p.read_text(encoding='utf-8', errors='ignore').splitlines()[-tail:]
    out=[]
    for line in lines:
        try: out.append(json.loads(line))
        except Exception: pass
    return out
bridge=load_json(REPORT_DIR/'bridge_blocker_summary_v1.json')
rr=load_json(REPORT_DIR/'rr_decay_report_v1.json')
strict_core=load_json(REPORT_DIR/'strict_vs_core_smc_compare_v1.json')
core_summary=load_json(REPORT_DIR/'core_smc_shadow_summary_v1.json')
errors=read_jsonl(LOG_DIR/'vps_smc_errors.jsonl', tail=100)
latest_errors=errors[-10:]
snapshot={
 'created_at_utc': datetime.now(timezone.utc).isoformat(),
 'mode': 'READ_ONLY_SNAPSHOT_NO_API',
 'system_status': 'WARN' if latest_errors else 'OK',
 'rr_target_policy': {'RR_TARGET_R': 1.2, 'RR_TARGET_MODE': 'SINGLE_FULL', 'note': 'Patch preserves 1.2R target rewrite.'},
 'bridge_summary': {k:bridge.get(k) for k in ['events_total','precheck_count','attempt_count','result_count','decision_counts','reason_counts']},
 'rr_decay_summary': {k:rr.get(k) for k in ['strict_signal_count','block_count','joined_decay_count','core_shadow_count','core_shadow_state_counts','global_rr_decay','global_mins_emit_to_block']},
 'strict_vs_core_summary': {k:strict_core.get(k) for k in ['strict_confirmed_count','core_shadow_count','core_candidate_count','core_invalid_count','core_state_counts']},
 'core_smc_shadow_summary': {k:core_summary.get(k) for k in ['raw_rows','unique_candidates','state_counts','valid_by_symbol','rr_stats_valid','rr_bucket_counts_valid','rr_outlier_count_valid','strict_state_for_valid_counts']},
 'latest_errors': latest_errors,
 'forbidden_actions': ['auto_trade','auto_patch','auto_deploy','lower_rr_target','disable_ml_gate','disable_orderbook_guard'],
}
(REPORT_DIR/'ai_manager_snapshot_v1.json').write_text(json.dumps(snapshot, indent=2, ensure_ascii=False, default=str), encoding='utf-8')
with (LOG_DIR/'ai_manager_snapshots_v1.jsonl').open('a', encoding='utf-8') as f: f.write(json.dumps(snapshot, ensure_ascii=False, default=str)+'\n')
print(json.dumps(snapshot, indent=2, default=str)[:2500])
print('[ok] wrote reports/ai_manager_snapshot_v1.json and logs/ai_manager_snapshots_v1.jsonl')
