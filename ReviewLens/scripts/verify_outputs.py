"""Fail a demo/DAG when outputs or accounting checks are missing."""
import argparse
import csv
import json
from pathlib import Path


def require(condition, message):
    if not condition:
        raise ValueError(message)


def verify(root, require_stream=False):
    quality = json.loads((root / 'data_quality.json').read_text(encoding='utf-8'))
    require(quality['row_balance_ok'], 'Row balance flag is false')
    require(quality['raw_rows'] == quality['clean_rows'] + quality['quarantined_rows'] + quality['duplicate_valid_id_rows_removed'], 'Raw/clean/quarantine/duplicates mismatch')
    require(quality['clean_rows'] > 0, 'Empty processed data')
    inspection = json.loads((root / 'raw_inspection.json').read_text(encoding='utf-8'))
    require(inspection.get('audit_rows_match') is True, 'Missing or failed independent audit comparison')
    success = json.loads((root / 'batch_success.json').read_text(encoding='utf-8'))
    for name in success['exported_tables']:
        with (root / f'{name}.csv').open(encoding='utf-8-sig', newline='') as handle:
            require(bool(list(csv.DictReader(handle))), f'Empty table: {name}')
    clusters = json.loads((root / 'cluster_metadata.json').read_text(encoding='utf-8'))
    require(clusters['training_rows'] > clusters['k'], 'Insufficient clustering data')
    require(-1 <= clusters['silhouette_cosine'] <= 1, 'Invalid silhouette')
    ml = json.loads((root / 'sentiment_evaluation.json').read_text(encoding='utf-8'))
    require(ml['test']['rows'] > 0 and ml['label_source'] == 'star_rating_proxy', 'Invalid ML evidence')
    require(not any(ml['leakage_checks'].values()), 'Text-group leakage in ML split')
    require(ml['portable_predictions_checked'] > 0, 'Serving model not verified against Spark')
    if require_stream:
        metrics = [json.loads(path.read_text()) for path in root.glob('stream_batch_*.json')]
        require(bool(metrics) and sum(m['valid'] for m in metrics) > 0, 'No valid streaming events')
        require(all(m['new_unique_events'] == m['valid'] + m['invalid'] for m in metrics), 'Streaming row balance failed')
    print(json.dumps({'verification':'passed','clean_rows':quality['clean_rows'],'stream_checked':require_stream,'ml_checked':True}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("/exports"))
    parser.add_argument("--require-stream", action="store_true")
    args = parser.parse_args()
    verify(args.root, args.require_stream)

