"""Prepare chronological month batches from HDFS; verify the actual replay sink."""
import argparse,json,uuid
from datetime import datetime,timezone
from pathlib import Path
from pyspark.sql import SparkSession,functions as F
from review_analytics.io import atomic_write_text
ROOT='hdfs://namenode:9000/ecommerce/replay'
OUT=Path('/exports/replay')

def monthly(frame):
    return frame.groupBy('month','platform').agg(F.count('*').alias('reviews'),F.sum(F.when(F.col('score')<=2,1).otherwise(0)).alias('low'),F.sum('score').alias('rating_sum'))

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--verify-run');args=parser.parse_args()
    OUT.mkdir(parents=True,exist_ok=True)
    spark=(SparkSession.builder.appName('historical-month-replay').config('spark.cores.max',2).config('spark.executor.cores',1).config('spark.sql.shuffle.partitions',4).getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        if args.verify_run:
            if len(args.verify_run)!=32 or any(c not in '0123456789abcdef' for c in args.verify_run):raise ValueError('Invalid replay run ID')
            manifest=json.loads((OUT/'prepared.json').read_text())
            path=ROOT+'/runs/'+args.verify_run+'/loaded'
            frame=spark.read.parquet(path)
            actual=frame.count();unique=frame.select('review_key').distinct().count()
            cells={(r['month'],r['platform']):(r['reviews'],r['low'],r['rating_sum']) for r in monthly(frame).collect()}
            expected={(m['month'],p):(v['reviews'],v['low'],v['rating_sum']) for m in manifest['months'] for p,v in m['platforms'].items()}
            if actual!=unique or actual!=manifest['total_reviews'] or cells!=expected:raise ValueError('Replay HDFS rows/aggregates differ from prepared source')
            data={'verification':'passed','run_id':args.verify_run,'actual_hdfs_rows':actual,'unique_keys':unique,'monthly_cells':len(cells),'source_generation':manifest['source_generation'],'hdfs_loaded_source':path,'verified_utc':datetime.now(timezone.utc).isoformat()}
            atomic_write_text(OUT/('validation_'+args.verify_run+'.json'),json.dumps(data,indent=2));print(json.dumps(data));return
        meta=json.loads(Path('/exports/dashboard_preparation.json').read_text())
        old=json.loads((OUT/'prepared.json').read_text()) if (OUT/'prepared.json').exists() else {}
        if old.get('source_generation')==meta['generation']:
            print(json.dumps({'preparation':'already_verified','source_generation':meta['generation']}));return
        source='hdfs://namenode:9000/ecommerce/processed/reviews'
        reviews=spark.read.parquet(source).cache()
        n=reviews.count()
        if n!=meta['processed_rows'] or reviews.select('review_key').distinct().count()!=n:raise ValueError('Historical HDFS source count mismatch')
        cells=monthly(reviews).orderBy('month','platform').collect()
        generation=uuid.uuid4().hex;stage=ROOT+'/prepared/'+generation
        reviews.repartition('month').write.mode('errorifexists').partitionBy('month').parquet(stage)
        copied=spark.read.parquet(stage)
        check={(r['month'],r['platform']):(r['reviews'],r['low'],r['rating_sum']) for r in monthly(copied).collect()}
        expected={(r['month'],r['platform']):(r['reviews'],r['low'],r['rating_sum']) for r in cells}
        if check!=expected:raise ValueError('Prepared HDFS month batches differ from source')
        grouped={}
        for r in cells:
            m=grouped.setdefault(r['month'],{'month':r['month'],'platforms':{},'reviews':0})
            m['platforms'][r['platform']]={k:int(r[k]) for k in ['reviews','low','rating_sum']};m['reviews']+=r['reviews']
        fs=spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        for m in grouped.values():
            path=stage+'/month='+m['month'];summary=fs.getContentSummary(spark._jvm.org.apache.hadoop.fs.Path(path))
            m.update({'hdfs_source':path,'files':int(summary.getFileCount()),'bytes':int(summary.getLength())})
        data={'verification':'passed','source_mode':'historical_replay','source_generation':meta['generation'],'source_hdfs':source,'prepared_hdfs':stage,'total_reviews':n,'month_min':min(grouped),'month_max':max(grouped),'months':[grouped[m] for m in sorted(grouped)],'prepared_utc':datetime.now(timezone.utc).isoformat()}
        atomic_write_text(OUT/'prepared.json',json.dumps(data,indent=2));print(json.dumps({'preparation':'passed','total_reviews':n,'months':len(grouped),'hdfs':stage}));reviews.unpersist()
    finally:spark.stop()
if __name__=='__main__':main()
