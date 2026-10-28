"""Train LR/SVM in Spark from HDFS. Group identical normalized text before splitting.
Stars are a proxy label, never human sentiment ground truth. 3-star reviews excluded.
"""
import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.ml import Pipeline
from pyspark.ml.feature import CountVectorizer, IDF, Normalizer
from pyspark.ml.classification import LogisticRegression, LinearSVC
from review_analytics.sentiment import model_terms, predict
from review_analytics.io import atomic_write_text


def metrics(frame):
    cells = {(int(r.label),int(r.prediction)):r['count'] for r in frame.groupBy('label','prediction').count().collect()}
    tn,fp,fn,tp = [cells.get(pair,0) for pair in [(0,0),(0,1),(1,0),(1,1)]]
    total = tn+fp+fn+tp
    precision = tp/(tp+fp) if tp+fp else 0
    recall = tp/(tp+fn) if tp+fn else 0
    f1 = 2*precision*recall/(precision+recall) if precision+recall else 0
    positive_f1 = 2*tn/(2*tn+fp+fn) if 2*tn+fp+fn else 0
    return {'rows':total, 'accuracy':(tn+tp)/total, 'precision_negative':precision,
            'recall_negative':recall, 'f1_negative':f1, 'macro_f1':(f1+positive_f1)/2,
            'confusion_matrix':{'label_order':['positive','negative'],'values':[[tn,fp],[fn,tp]]}}


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--root',default='hdfs://namenode:9000/ecommerce')
    p.add_argument('--exports',type=Path,default=Path('/exports'))
    p.add_argument('--sample-percent',type=int,default=5)
    args=p.parse_args()
    if not 1<=args.sample_percent<=100: p.error('sample-percent must be 1..100')
    spark=(SparkSession.builder.appName('review-sentiment-proxy-comparison')
           .config('spark.sql.shuffle.partitions',4).config('spark.cores.max',2)
           .config('spark.executor.cores',1).getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        source=spark.read.parquet(args.root+'/processed/reviews')
        terms=F.udf(model_terms,T.ArrayType(T.StringType()))
        eligible=(source.filter('score != 3 AND NOT spam_candidate')
                  .withColumn('text_group',F.sha2('normalized_content',256))
                  .filter(F.pmod(F.xxhash64('text_group',F.lit('sample')),F.lit(100))<args.sample_percent)
                  .withColumn('terms',terms('content')).filter(F.size('terms')>0)
                  .withColumn('label',F.when(F.col('score')<=2,1.0).otherwise(0.0))
                  .withColumn('bucket',F.pmod(F.xxhash64('text_group',F.lit('split')),F.lit(10))))
        data=(eligible.withColumn('split',F.when(F.col('bucket')<7,'train')
              .when(F.col('bucket')<8,'validation').otherwise('test')).persist(StorageLevel.MEMORY_AND_DISK))
        counts=[r.asDict() for r in data.groupBy('split','label').count().collect()]
        train=data.filter("split='train'")
        label_counts={int(r.label):r['count'] for r in train.groupBy('label').count().collect()}
        if set(label_counts)!={0,1}: raise ValueError('Both labels required in training')
        total=sum(label_counts.values())
        weight=F.when(F.col('label')==1,total/(2*label_counts[1])).otherwise(total/(2*label_counts[0]))
        features=Pipeline(stages=[CountVectorizer(inputCol='terms',outputCol='tf',vocabSize=6000,minDF=3),
                  IDF(inputCol='tf',outputCol='tfidf'),Normalizer(inputCol='tfidf',outputCol='features')]).fit(train)
        transformed=features.transform(data).withColumn('weight',weight).persist(StorageLevel.MEMORY_AND_DISK)
        fitting=transformed.filter("split='train'")
        validation=transformed.filter("split='validation'")
        test=transformed.filter("split='test'")
        if validation.count()==0 or test.count()==0: raise ValueError('Empty validation/test split')
        candidates=[]
        models={}
        for name, estimator in [('LogisticRegression',LogisticRegression),('LinearSVC',LinearSVC)]:
            for reg in [0.01,0.1]:
                model=estimator(maxIter=40,regParam=reg,weightCol='weight').fit(fitting)
                key=f'{name}_reg_{reg}'
                models[key]=model
                candidates.append({'key':key,'model':name,'regParam':reg,'validation':metrics(model.transform(validation))})
                print(json.dumps(candidates[-1]),flush=True)
        selected=max(candidates,key=lambda c:c['validation']['macro_f1'])
        model=models[selected['key']]
        predictions=model.transform(test).persist(StorageLevel.MEMORY_AND_DISK)
        evaluation=metrics(predictions)
        majority=1 if label_counts[1]>label_counts[0] else 0
        baseline=metrics(test.withColumn('prediction',F.lit(float(majority))))
        # Pairwise group intersections, including cross-platform repeats, must be zero.
        groups={split:data.filter(F.col('split')==split).select('text_group').distinct() for split in ['train','validation','test']}
        leakage={f'{a}_{b}':groups[a].intersect(groups[b]).count() for a,b in [('train','validation'),('train','test'),('validation','test')]}
        if any(leakage.values()): raise ValueError('Text group leakage')
        artifact={'model':selected['model'],'vocabulary':features.stages[0].vocabulary,
                  'idf':features.stages[1].idf.toArray().tolist(),
                  'coefficients':model.coefficients.toArray().tolist(),'intercept':model.intercept,
                  'label_source':'score 1/2 negative=1, score 4/5 positive=0; exclude score 3',
                  'input_source':args.root+'/processed/reviews','tokenizer':'review_analytics.sentiment.model_terms v1'}
        portable=json.dumps(artifact,ensure_ascii=False)
        artifact_hash=hashlib.sha256(portable.encode()).hexdigest()
        artifact['sha256_without_hash']=artifact_hash
        atomic_write_text(args.exports/'sentiment_model.json',json.dumps(artifact,ensure_ascii=False))
        model.write().overwrite().save(args.root+'/models/sentiment_classifier')
        features.write().overwrite().save(args.root+'/models/sentiment_features')
        predictions.select('review_key','platform','score','label','prediction','normalized_content').write.mode('overwrite').parquet(args.root+'/output/sentiment_test_predictions')
        # Validate serving coefficients against actual Spark predictions.
        checked=0
        for row in predictions.select('content','prediction').orderBy(F.xxhash64('review_key')).limit(100).collect():
            served=predict(row.content,artifact)
            if served['sentiment']!='uncertain':
                if int(served['sentiment']=='negative')!=int(row.prediction): raise ValueError('Portable prediction mismatch')
                checked+=1
        errors=[r.asDict() for r in predictions.filter('label != prediction').select('review_key','platform','score','label','prediction','content').orderBy(F.xxhash64('review_key')).limit(20).collect()]
        report={'completed_utc':datetime.now(timezone.utc).isoformat(),'input':args.root+'/processed/reviews',
                'label_source':'star_rating_proxy','excluded':'3-star, spam candidates, empty token reviews',
                'sample_percent':args.sample_percent,'split_method':'70/10/20 hash normalized text groups; vocabulary and IDF fit on train only',
                'split_counts':counts,'leakage_checks':leakage,'features':'Vietnamese syllable unigrams+bigrams, TF-IDF L2, 6000 vocabulary',
                'validation_candidates':candidates,'selected':selected,'test':evaluation,'majority_baseline_test':baseline,
                'portable_predictions_checked':checked,'model_sha256':artifact_hash,'errors':errors,
                'interpretation':'Metric measures prediction of stars, not verified human sentiment or aspect polarity. Sample groups have unequal row weights.'}
        atomic_write_text(args.exports/'sentiment_evaluation.json',json.dumps(report,ensure_ascii=False,indent=2))
        spark.read.json(spark.sparkContext.parallelize([json.dumps(report,ensure_ascii=False)])).coalesce(1).write.mode('overwrite').json(args.root+'/output/sentiment_evaluation')
        print(json.dumps({'selected':selected['key'],'test':evaluation,'leakage_checks':leakage}),flush=True)
    finally:
        spark.stop()

if __name__=='__main__': main()
