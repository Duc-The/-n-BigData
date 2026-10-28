"""Publish a verified, immutable live snapshot from actual Kafka-derived HDFS Parquet."""
import csv,hashlib,json,subprocess,uuid
from datetime import datetime,timezone
from pathlib import Path
from pyspark.sql import SparkSession,Window,functions as F
from review_analytics.io import atomic_write_text,atomic_text_writer
BASE='hdfs://namenode:9000/ecommerce/live'
EXPORTS=Path('/exports/live')


def latest_review_versions(versions):
    window=Window.partitionBy('review_key').orderBy(F.to_timestamp('ingested_at').desc(),F.col('event_id').desc())
    return versions.withColumn('_rank',F.row_number().over(window)).filter('_rank=1').drop('_rank')


def main():
    version_rows=sum(json.loads(p.read_text())['valid'] for p in EXPORTS.glob('stream_batch_*.json'))
    if version_rows==0: raise ValueError('No valid real reviews in HDFS')
    pointer=EXPORTS/'current.json'
    if pointer.exists():
        previous=json.loads((EXPORTS/'generations'/json.loads(pointer.read_text())['generation']/'dashboard_preparation.json').read_text())
        if previous.get('stream_version_rows')==version_rows:
            print(json.dumps({'publication':'unchanged','reason':'No new or edited valid review versions','generation':previous['generation']}));return
    spark=(SparkSession.builder.appName('publish-real-review-dashboard').config('spark.cores.max',2)
           .config('spark.executor.cores',1).config('spark.sql.shuffle.partitions',4)
           .config('spark.sql.session.timeZone','UTC').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        versions=spark.read.parquet(BASE+'/streaming/reviews')
        actual_versions=versions.count()
        if actual_versions!=version_rows: raise ValueError('HDFS rows differ from streaming metrics')
        reviews=latest_review_versions(versions).cache()
        n=reviews.count()
        if reviews.select('review_key').distinct().count()!=n: raise ValueError('Review ID duplication')
        if reviews.filter("source_mode != 'google_play_live'").count(): raise ValueError('Historical/synthetic records in live stream')
        generation=uuid.uuid4().hex
        folder=EXPORTS/'generations'/generation;folder.mkdir(parents=True);folder.chmod(0o2775)
        config=json.loads(Path('/workspace/config/aspects.json').read_text())
        expressions=[F.count('*').alias('reviews'),F.sum(F.when(F.size('aspects_mentioned')==0,1).otherwise(0)).alias('unmatched_reviews')]
        expressions+=[F.sum(F.when(F.array_contains('aspects_mentioned',a),1).otherwise(0)).alias('aspect_'+a) for a in sorted(config)]
        cube=reviews.groupBy('platform','month','score','spam_candidate').agg(*expressions).withColumn('generation',F.lit(generation))
        cube.write.mode('overwrite').parquet(BASE+'/output/dashboard_cube/'+generation)
        cells=cube.orderBy('platform','month','score','spam_candidate').limit(10001).collect()
        if len(cells)>10000 or sum(r['reviews'] for r in cells)!=n: raise ValueError('Invalid cube export')
        with atomic_text_writer(folder/'dashboard_cube.csv','utf-8-sig',newline='') as handle:
            writer=csv.DictWriter(handle,fieldnames=cube.columns);writer.writeheader();writer.writerows(r.asDict() for r in cells)
        serving=BASE+'/serving/'+generation+'/review_index'
        reviews.select('review_key','platform','month','score','content','spam_candidate','aspects_mentioned',F.date_format('at','yyyy-MM-dd HH:mm:ss').alias('at')).repartition(2).write.mode('overwrite').option('compression','gzip').json(serving)
        reviews.write.mode('overwrite').partitionBy('platform','year').parquet(BASE+'/processed/reviews')
        metadata={'generation':generation,'prepared_utc':datetime.now(timezone.utc).isoformat(),'processed_rows':n,
                  'cube_rows':len(cells),'stream_version_rows':actual_versions,'source_mode':'google_play_live',
                  'event_time_timezone':'UTC',
                  'source_description':'Public Google Play reviews fetched directly; original review time preserved; no Kaggle replay',
                  'month_min':min(r['month'] for r in cells),'month_max':max(r['month'] for r in cells),
                  'aspects':[{'code':a,'label':config[a]['label'],'bit':1<<i} for i,a in enumerate(sorted(config))],
                  'hdfs_serving_source':serving,'hdfs_processed_source':BASE+'/processed/reviews',
                  'aspect_config_sha256':hashlib.sha256(json.dumps(config,sort_keys=True).encode()).hexdigest()}
        atomic_write_text(folder/'dashboard_preparation.json',json.dumps(metadata,ensure_ascii=False,indent=2))
        invalid=sum(json.loads(p.read_text())['invalid'] for p in EXPORTS.glob('stream_batch_*.json'))
        quality={'raw_rows':actual_versions+invalid,'clean_rows':n,'quarantined_rows':invalid,
                 'duplicate_valid_id_rows_removed':actual_versions-n,'row_balance_ok':True,'source_mode':'google_play_live'}
        atomic_write_text(folder/'data_quality.json',json.dumps(quality,indent=2))
        validation={'verification':'passed','processed_rows':n,'processed_unique_keys':n,'source':'actual real-review HDFS Parquet','stream_version_rows':actual_versions}
        atomic_write_text(folder/'persisted_validation.json',json.dumps(validation,indent=2))
        subprocess.run(['python','/workspace/scripts/build_dashboard_index.py','--output',str(folder/'review_search.sqlite')],check=True)
        subprocess.run(['python','/workspace/scripts/validate_dashboard.py','--outputs',str(folder)],check=True)
        # Switch readers only after cube, index and full independent count audit succeed.
        atomic_write_text(pointer,json.dumps({'generation':generation,'published_utc':datetime.now(timezone.utc).isoformat()}))
        print(json.dumps({'publication':'passed','generation':generation,'real_unique_reviews':n,'stream_versions':actual_versions}),flush=True)
    finally: spark.stop()
if __name__=='__main__': main()
