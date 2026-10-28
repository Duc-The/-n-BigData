#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 scripts/download_dataset.py
python3 scripts/init_local_env.py
mkdir -p outputs/logs
python3 scripts/audit_dataset.py | tee outputs/logs/dataset_audit_latest.log
compose() { docker compose -f compose.yaml -f compose.low-memory.yaml "$@"; }
compose build spark-client
compose up -d namenode datanode spark-master spark-worker-1 spark-worker-2 spark-client
compose exec -T -u 0 spark-client python scripts/prepare_shared_outputs.py
compose exec -T spark-client hdfs dfsadmin -safemode wait
compose exec -T spark-client python -m unittest discover -s tests -v 2>&1 | tee outputs/logs/tests_latest.log
compose exec -T spark-client python scripts/bootstrap_hdfs.py 2>&1 | tee outputs/logs/hdfs_upload_latest.log
submit() { compose exec -T spark-client spark-submit --master spark://spark-master:7077 --driver-memory 1g --executor-memory 1g "$@"; }
submit jobs/batch_pipeline.py --stage all 2>&1 | tee outputs/logs/batch_latest.log
submit jobs/train_sentiment.py 2>&1 | tee outputs/logs/sentiment_latest.log
submit jobs/prepare_dashboard.py 2>&1 | tee outputs/logs/dashboard_prepare_latest.log
compose exec -T spark-client python scripts/build_dashboard_index.py 2>&1 | tee outputs/logs/dashboard_index_latest.log
compose --profile streaming up -d kafka
compose exec -T spark-client python jobs/replay_kafka.py 2>&1 | tee outputs/logs/producer_latest.log
submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 jobs/stream_reviews.py 2>&1 | tee outputs/logs/consumer_latest.log
compose exec -T spark-client python scripts/verify_outputs.py --require-stream
submit jobs/validate_persisted.py
compose exec -T spark-client python scripts/validate_dashboard.py --outputs /exports
compose --profile dashboard up -d dashboard
compose exec -T spark-client hdfs dfsadmin -report > outputs/logs/hdfs_report_latest.txt
compose exec -T spark-client hdfs fsck /ecommerce/raw/reviews.csv -files -blocks -locations > outputs/logs/hdfs_blocks_latest.txt
printf 'Pipeline completed. Dashboard: http://localhost:8090 Demo: http://localhost:8090/demo.html\n'
