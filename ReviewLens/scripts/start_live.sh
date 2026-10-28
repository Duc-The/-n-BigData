#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 scripts/init_local_env.py
mkdir -p outputs/live/generations
compose() { docker compose -f compose.yaml -f compose.low-memory.yaml "$@"; }
compose build spark-client
compose --profile orchestration build airflow-init
compose up -d namenode datanode spark-master spark-worker-1 spark-worker-2 spark-client
compose exec -T -u 0 spark-client python scripts/prepare_shared_outputs.py
compose exec -T spark-client hdfs dfsadmin -safemode wait
compose --profile orchestration up -d postgres kafka
compose --profile orchestration run --rm airflow-init
compose --profile orchestration --profile dashboard up -d airflow-webserver airflow-scheduler dashboard
compose exec -T airflow-scheduler airflow dags unpause live_google_play_reviews
printf 'Real review collection runs every five minutes. Dashboard: http://localhost:8090\n'
