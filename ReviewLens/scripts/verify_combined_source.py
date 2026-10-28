"""Independently aggregate both immutable SQLite exports and compare all cube cells."""
import collections,json,sys
from contextlib import closing
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from dashboard.server import Store,CombinedStore
ROOT=Path(__file__).resolve().parents[1]/'outputs'

def main():
    pointer=json.loads((ROOT/'live/current.json').read_text())
    store=CombinedStore(Store(ROOT),Store(ROOT/'live/generations'/pointer['generation']))
    metadata,cube=store.snapshot()
    keys=['platform','month','score','spam_candidate']
    counts=['reviews','unmatched_reviews']+['aspect_'+a['code'] for a in metadata['aspects']]
    exported=collections.defaultdict(lambda:collections.Counter())
    for row in cube:
        key=tuple(row[k] for k in keys)
        for c in counts:exported[key][c]+=row[c]
    expressions=['COUNT(*) AS reviews','SUM(aspect_mask=0) AS unmatched_reviews']+[f"SUM((aspect_mask & {a['bit']})>0) AS aspect_{a['code']}" for a in metadata['aspects']]
    with closing(store.connect()) as connection:
        query='SELECT platform,month,score,spam, '+','.join(expressions)+' FROM (SELECT platform,month,score,spam,aspect_mask FROM main.reviews old WHERE NOT EXISTS (SELECT 1 FROM live.reviews newer WHERE newer.review_key=old.review_key) UNION ALL SELECT platform,month,score,spam,aspect_mask FROM live.reviews) GROUP BY platform,month,score,spam'
        actual={}
        for row in connection.execute(query):
            actual[(row['platform'],row['month'],row['score'],bool(row['spam']))]={c:row[c] for c in counts}
    assert dict(exported)==actual,'Combined SQL counts differ from HDFS-derived cube'
    result={'verification':'passed','generation':metadata['generation'],'historical_reviews':metadata['historical_rows'],'live_reviews':metadata['live_rows'],'overlapping_review_ids':metadata['overlapping_review_ids'],'total_reviews':metadata['processed_rows'],'verified_cells':len(actual),'counts_per_cell':len(counts),'all_cube_counts_match_full_index':True,'hdfs_serving_sources':metadata['hdfs_serving_sources']}
    (ROOT/'combined_source_validation.json').write_text(json.dumps(result,indent=2));print(json.dumps(result))
if __name__=='__main__':main()
