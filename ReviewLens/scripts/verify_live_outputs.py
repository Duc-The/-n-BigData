import json
from pathlib import Path
root=Path('/exports/live')
pointer=json.loads((root/'current.json').read_text())
folder=root/'generations'/pointer['generation']
meta=json.loads((folder/'dashboard_preparation.json').read_text())
check=json.loads((folder/'dashboard_validation.json').read_text())
if meta['source_mode']!='google_play_live' or check['generation']!=meta['generation'] or not check['all_spark_cube_counts_match_full_index']: raise ValueError('Invalid live publication')
if meta['processed_rows']!=check['indexed_reviews']: raise ValueError('Live count mismatch')
print(json.dumps({'verification':'passed','source_mode':meta['source_mode'],'real_reviews':meta['processed_rows'],'generation':meta['generation']}))

import csv
from review_analytics.io import atomic_write_text
collection=json.loads((root/'collection_status.json').read_text())
platforms={}
with (folder/'dashboard_cube.csv').open(encoding='utf-8-sig',newline='') as handle:
    for row in csv.DictReader(handle):
        v=platforms.setdefault(row['platform'],{'reviews':0,'low':0,'rating_sum':0})
        n=int(row['reviews']);score=int(row['score']);v['reviews']+=n;v['rating_sum']+=n*score
        if score<=2:v['low']+=n
point={'poll_id':collection['poll_id'],'at':collection['completed_utc'],'generation':meta['generation'],'new_reviews':collection['new_reviews'],'updated_reviews':collection['updated_reviews'],'total_reviews':meta['processed_rows'],'platforms':platforms}
timeline_path=root/'observation_timeline.json'
timeline=json.loads(timeline_path.read_text()) if timeline_path.exists() else []
if not any(p['poll_id']==point['poll_id'] for p in timeline):
    timeline.append(point);atomic_write_text(timeline_path,json.dumps(timeline[-288:],indent=2))
