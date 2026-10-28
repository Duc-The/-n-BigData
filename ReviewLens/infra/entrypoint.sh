#!/usr/bin/env bash
set -euo pipefail
case "${1:-}" in
  namenode)
    if [ ! -f /hadoop/name/current/VERSION ]; then
      hdfs namenode -format -nonInteractive
    fi
    export HADOOP_HEAPSIZE=512
    exec hdfs namenode
    ;;
  datanode)
    export HADOOP_HEAPSIZE=512
    exec hdfs datanode
    ;;
  spark-master)
    exec spark-class org.apache.spark.deploy.master.Master --host spark-master --port 7077 --webui-port 8080
    ;;
  spark-worker)
    exec spark-class org.apache.spark.deploy.worker.Worker --cores "${SPARK_WORKER_CORES:-2}" --memory "${SPARK_WORKER_MEMORY:-2g}" --webui-port 8081 spark://spark-master:7077
    ;;
  *) exec "$@" ;;
esac

