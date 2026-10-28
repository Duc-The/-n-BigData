"""Poll real public reviews every five minutes; finite tasks, no full historical retraining."""
import json
import urllib.request
from kafka import KafkaConsumer, TopicPartition
from pathlib import Path
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.session import create_session
from review_analytics.io import atomic_write_text

def capture_success(context):
    run=context['dag_run']
    with create_session() as session:
        tasks=run.get_task_instances(session=session)
        data={'airflow':{'run_id':run.run_id,'state':'success','tasks':[{'task_id':t.task_id,'state':t.state,'start_utc':t.start_date.isoformat() if t.start_date else None,'end_utc':t.end_date.isoformat() if t.end_date else None} for t in sorted(tasks,key=lambda x:x.task_id)]}}
    cluster=json.load(urllib.request.urlopen('http://spark-master:8080/json/',timeout=10))
    consumer=KafkaConsumer(bootstrap_servers='kafka:29092',enable_auto_commit=False)
    try:
        topic='ecommerce-live-reviews'
        offsets=consumer.end_offsets([TopicPartition(topic,p) for p in sorted(consumer.partitions_for_topic(topic))])
        data.update({'spark_alive_workers':cluster['aliveworkers'],'spark_total_cores':cluster['cores'],
                     'kafka_total_offsets':sum(offsets.values()),'kafka_topic_end_offsets':{str(p.partition):o for p,o in offsets.items()}})
    finally: consumer.close()
    atomic_write_text(Path('/exports/live/dag_evidence.json'),json.dumps(data,indent=2))

SUBMIT='spark-submit --master spark://spark-master:7077 --driver-memory 1g --executor-memory 1g'
with DAG('live_google_play_reviews',start_date=pendulum.datetime(2026,10,15,tz='Asia/Ho_Chi_Minh'),
         schedule='*/5 * * * *',catchup=False,max_active_runs=1,on_success_callback=capture_success,
         default_args={'retries':1,'retry_delay':timedelta(seconds=30),'execution_timeout':timedelta(minutes=10)},
         tags=['real-reviews','automatic']) as dag:
    collect=BashOperator(task_id='01_collect_real_reviews',bash_command='python /workspace/jobs/collect_live_reviews.py')
    consume=BashOperator(task_id='02_kafka_to_hdfs',bash_command=SUBMIT+' --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 /workspace/jobs/stream_reviews.py --live --topic ecommerce-live-reviews --root hdfs://namenode:9000/ecommerce/live --exports /exports/live')
    publish=BashOperator(task_id='03_publish_verified_snapshot',bash_command=SUBMIT+' /workspace/jobs/publish_live_dashboard.py')
    verify=BashOperator(task_id='04_verify_live_results',bash_command='python /workspace/scripts/verify_live_outputs.py')
    collect >> consume >> publish >> verify
