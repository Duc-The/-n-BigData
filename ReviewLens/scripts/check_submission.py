"""Verify submission consistency, fingerprints, tasks and references without claiming live HDFS."""
import argparse
import csv
import hashlib
import json
import subprocess
from datetime import datetime,timezone
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]

def read(name): return json.loads((ROOT/name).read_text())
def require(condition,message):
    if not condition: raise ValueError(message)

def verify():
    quality=read('outputs/data_quality.json')
    require(quality['raw_rows']==quality['clean_rows']+quality['quarantined_rows']+quality['duplicate_valid_id_rows_removed'],'Row imbalance')
    require(read('artifacts/audit/dataset_profile.json')['sha256']==read('outputs/raw_inspection.json')['source_sha256'],'Source fingerprints disagree')
    for name in ['platform_summary','monthly_summary','rating_distribution','review_length_summary']:
        with (ROOT/'outputs'/f'{name}.csv').open(encoding='utf-8-sig',newline='') as handle: rows=list(csv.DictReader(handle))
        require(sum(int(r['reviews']) for r in rows)==quality['clean_rows'],f'{name} total mismatch')
    evidence=read('outputs/demo_evidence.json')
    tasks=evidence['airflow']['tasks']
    require(len(tasks)==10 and all(t['state']=='success' for t in tasks),'Airflow new DAG not successful')
    require(all(evidence['repeated_batch_counts_unchanged'].values()),'Rerun changed row counts')
    require(evidence['kafka_total_offsets']>=2000,'No Kafka replay evidence')
    persisted=read('outputs/persisted_validation.json')
    require(persisted['processed_rows']==persisted['processed_unique_keys']==quality['clean_rows'],'Processed uniqueness failed')
    require(persisted['streaming_rows']==persisted['streaming_unique_keys']==1000,'Streaming uniqueness failed')
    metadata=read('outputs/dashboard_preparation.json')
    dashboard=read('outputs/dashboard_validation.json')
    browser=read('outputs/dashboard_demo_validation.json')
    require(metadata['generation']==dashboard['generation']==browser['generation'],'Dashboard evidence generation mismatch')
    require(dashboard['indexed_reviews']==quality['clean_rows'] and browser['verification']=='passed','Dashboard/browser counts failed')
    actual_raw=(ROOT/'outputs/logs/hdfs_raw_sha256_20261015.txt').read_text().split()[0]
    require(actual_raw==read('artifacts/audit/dataset_profile.json')['sha256'],'Actual HDFS source checksum differs from local source')
    ml=read('outputs/sentiment_evaluation.json')
    model=read('outputs/sentiment_model.json')
    model_digest=model.pop('sha256_without_hash')
    require(hashlib.sha256(json.dumps(model,ensure_ascii=False).encode()).hexdigest()==model_digest==ml['model_sha256'],'Serving model differs from evaluated model')
    require(not any(ml['leakage_checks'].values()) and ml['portable_predictions_checked']>0,'Model split/serving validation failed')
    require(read('outputs/dashboard_validation.json')['all_spark_cube_counts_match_full_index'] is True,'Dashboard counts failed')
    automatic=read('outputs/auto_filters_validation.json')
    require(automatic['verification']=='passed','Automatic filter browser checks failed')
    live=read('outputs/live/current.json')
    live_meta=read('outputs/live/generations/'+live['generation']+'/dashboard_preparation.json')
    live_browser=read('outputs/live_dashboard_validation.json')
    browser_meta=read('outputs/live/generations/'+live_browser['generation']+'/dashboard_preparation.json')
    require(live_meta['source_mode']==browser_meta['source_mode']=='google_play_live','Invalid real-source mode')
    require(live_browser['verification']=='passed' and live_browser['real_review_count']==browser_meta['processed_rows'],'Real-source browser count differs from its immutable HDFS snapshot')
    clear=read('outputs/clear_filters_validation.json')
    combined=read('outputs/combined_source_validation.json')
    require(clear['verification']==combined['verification']=='passed','Combined source/Clear validation failed')
    require(clear['generation']==combined['generation'] and clear['total_reviews']==combined['total_reviews'],'Combined browser/index snapshot counts differ')
    replay=read('outputs/replay_browser_validation.json')
    require(replay['verification']=='passed' and replay['window_months']==12 and replay['default_platforms']==4,'Replay browser failed')
    require(read('outputs/replay_restart_validation.json')['verification']=='passed','Replay restart checkpoint failed')
    validations=[json.loads(p.read_text()) for p in (ROOT/'outputs/replay').glob('validation_*.json')]
    require(any(v['verification']=='passed' and v['actual_hdfs_rows']==v['unique_keys']==quality['clean_rows'] for v in validations),'No completed replay HDFS proof')
    git_history_present=(ROOT/'.git').exists()
    if git_history_present:
        commits=subprocess.check_output(['git','log','--format=%h %s'],cwd=ROOT,text=True)
        require(bool(commits.strip()),'Missing actual Git history')
    report={'verified_utc':datetime.now(timezone.utc).isoformat(),'verification':'passed','scope':'Technical saved artifacts and source/log cross-checks; reports are separate; not a live cluster check',
            'airflow_tasks':len(tasks),'clean_rows':quality['clean_rows'],'ml_label_source':ml['label_source'],
            'automatic_filters':'passed','historical_replay':'passed','clear_filters':'passed','combined_reviews':clear['total_reviews'],'real_google_play_reviews':live_meta['processed_rows'],'real_browser_snapshot_reviews':browser_meta['processed_rows'],
            'live_source':'google_play_live','git_history_present':git_history_present,
            'individual_contributions':'Not independently attributable from the supplied plan; no fabricated member commits'}
    (ROOT/'outputs/submission_validation.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
    print(json.dumps(report,ensure_ascii=False,indent=2))

if __name__=='__main__': verify()
