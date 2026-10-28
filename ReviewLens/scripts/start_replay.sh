#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# Start the core stack first with scripts/start_live.sh on a fresh installation.
mkdir -p outputs/replay
chmod 2775 outputs/replay
docker compose -f compose.yaml -f compose.low-memory.yaml --profile orchestration up -d --no-deps replay-runner dashboard
docker compose exec -T airflow-scheduler airflow dags unpause prepare_historical_month_replay
docker compose exec -T airflow-scheduler airflow dags trigger prepare_historical_month_replay
printf '%s\n' 'Open http://localhost:8090/?source=all#replay'
