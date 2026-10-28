"""Prepare a separate HDFS replay source; live ingestion keeps its own DAG and checkpoint."""
import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
with DAG('prepare_historical_month_replay',start_date=pendulum.datetime(2026,10,15,tz='Asia/Ho_Chi_Minh'),schedule=None,catchup=False,max_active_runs=1,default_args={'retries':1,'retry_delay':timedelta(seconds=20),'execution_timeout':timedelta(minutes=20)},tags=['historical-replay','demo']) as dag:
    prepare=BashOperator(task_id='01_prepare_verified_month_batches',bash_command='spark-submit --master spark://spark-master:7077 --driver-memory 1g --executor-memory 1g /workspace/jobs/prepare_replay.py')
