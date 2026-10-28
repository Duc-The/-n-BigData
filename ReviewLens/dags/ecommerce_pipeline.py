"""Ten visible steps matching the instructor's pipeline, with adapted RFM."""
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

SUBMIT = "spark-submit --master spark://spark-master:7077 --driver-memory 1g --executor-memory 1g"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7"

with DAG(
    dag_id="ecommerce_dissatisfaction_pipeline",
    description="HDFS, Spark SQL, aspect mentions, K-Means, Kafka historical replay",
    start_date=pendulum.datetime(2026, 10, 12, tz="Asia/Ho_Chi_Minh"),
    schedule=None, catchup=False, max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30), "execution_timeout": timedelta(minutes=40)},
    tags=["bigdata", "vietnamese-reviews"],
) as dag:
    prepare = BashOperator(task_id="01_prepare_hdfs", bash_command="python /workspace/scripts/bootstrap_hdfs.py --mode prepare")
    upload = BashOperator(task_id="02_upload_csv", bash_command="python /workspace/scripts/bootstrap_hdfs.py --mode upload")
    read = BashOperator(task_id="03_read_with_spark", bash_command=f"{SUBMIT} /workspace/jobs/batch_pipeline.py --stage inspect")
    clean = BashOperator(task_id="04_clean_to_parquet", bash_command=f"{SUBMIT} /workspace/jobs/batch_pipeline.py --stage clean")
    sql = BashOperator(task_id="05_spark_sql", bash_command=f"{SUBMIT} /workspace/jobs/batch_pipeline.py --stage analyze")
    clustering = BashOperator(task_id="06_review_aspects_and_kmeans", bash_command=f"{SUBMIT} /workspace/jobs/batch_pipeline.py --stage cluster")
    sentiment = BashOperator(task_id="06b_train_sentiment_proxy", bash_command=f"{SUBMIT} /workspace/jobs/train_sentiment.py")
    export = BashOperator(task_id="07_save_results", bash_command=(
        f"{SUBMIT} /workspace/jobs/batch_pipeline.py --stage export && "
        f"{SUBMIT} /workspace/jobs/prepare_dashboard.py && "
        "python /workspace/scripts/build_dashboard_index.py"))
    kafka = BashOperator(task_id="08_kafka_replay_and_stream", bash_command=(
        "python /workspace/jobs/replay_kafka.py && "
        f"{SUBMIT} --packages {KAFKA_PACKAGE} /workspace/jobs/stream_reviews.py"))
    complete = BashOperator(task_id="09_verify_completion", bash_command=(
        "python /workspace/scripts/verify_outputs.py --require-stream && "
        f"{SUBMIT} /workspace/jobs/validate_persisted.py && "
        "python /workspace/scripts/validate_dashboard.py --outputs /exports"))
    prepare >> upload >> read >> clean >> sql >> clustering >> sentiment >> export >> kafka >> complete
