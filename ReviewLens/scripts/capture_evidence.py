"""Capture a completed Airflow run and infrastructure facts for the local demo."""
import argparse
import json
from pathlib import Path
import urllib.request

from airflow.models import DagRun
from airflow.utils.session import create_session
from kafka import KafkaConsumer, TopicPartition
from review_analytics.io import atomic_write_text


def capture(run_id, destination):
    with create_session() as session:
        run = session.query(DagRun).filter_by(
            dag_id="ecommerce_dissatisfaction_pipeline", run_id=run_id).one()
        tasks = sorted(run.get_task_instances(session=session), key=lambda task: task.task_id)
        assert run.state == "success", f"DAG state is {run.state}"
        assert len(tasks) == 10 and all(task.state == "success" for task in tasks)
        dag = {"run_id": run_id, "state": run.state, "tasks": [{"task_id": task.task_id,
            "state": task.state, "start_utc": task.start_date.isoformat() if task.start_date else None,
            "end_utc": task.end_date.isoformat() if task.end_date else None} for task in tasks]}
    cluster = json.load(urllib.request.urlopen("http://spark-master:8080/json/", timeout=10))
    consumer = KafkaConsumer(bootstrap_servers="kafka:29092", enable_auto_commit=False)
    try:
        partitions = consumer.partitions_for_topic("ecommerce-reviews")
        offsets = consumer.end_offsets([TopicPartition("ecommerce-reviews", part) for part in sorted(partitions)])
        offset_report = {str(part.partition): end for part, end in offsets.items()}
    finally:
        consumer.close()
    root = destination.parent
    before = json.loads((root / "quality_before_airflow.json").read_text(encoding="utf-8"))
    after = json.loads((root / "data_quality.json").read_text(encoding="utf-8"))
    repeated_counts = {field: before[field] == after[field] for field in [
        "raw_rows", "clean_rows", "quarantined_rows", "duplicate_valid_id_rows_removed"]}
    assert all(repeated_counts.values()), "Repeated batch changed data accounting"
    report = {"airflow": dag, "spark_alive_workers": cluster["aliveworkers"],
        "spark_total_cores": cluster["cores"], "kafka_topic_end_offsets": offset_report,
        "kafka_total_offsets": sum(offset_report.values()),
        "repeated_batch_counts_unchanged": repeated_counts,
        "sentiment_evaluation": {"test": json.loads((root / "sentiment_evaluation.json").read_text())["test"], "label_source": "star_rating_proxy"},
        "persisted_validation": json.loads((root / "persisted_validation.json").read_text(encoding="utf-8"))}
    atomic_write_text(destination, json.dumps(report, ensure_ascii=False, indent=2))
    print(json.dumps({"dag": dag["state"], "successful_tasks": len(tasks),
        "kafka_total_offsets": report["kafka_total_offsets"], "repeated_batch_counts_unchanged": True}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default="validation_20261015")
    parser.add_argument("--output", type=Path, default=Path("/exports/demo_evidence.json"))
    args = parser.parse_args()
    capture(args.run_id, args.output)
