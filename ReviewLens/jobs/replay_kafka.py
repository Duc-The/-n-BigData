"""Replay historical reviews, never present them as newly scraped reviews."""
import argparse
import csv
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

from review_analytics.spark_ops import APP_MAP


def replay(args):
    admin = None
    for attempt in range(30):
        try:
            admin = KafkaAdminClient(bootstrap_servers=args.bootstrap, request_timeout_ms=5000)
            break
        except KafkaError:
            if attempt == 29:
                raise
            time.sleep(2)
    try:
        admin.create_topics([NewTopic(args.topic, num_partitions=3, replication_factor=1)])
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()
    producer = KafkaProducer(bootstrap_servers=args.bootstrap, acks="all", retries=5,
        max_in_flight_requests_per_connection=1,
        key_serializer=lambda key: key.encode(),
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"))
    count = 0
    reverse_map = {value: key for key, value in APP_MAP.items()}
    try:
        with args.input.open(encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                if count >= args.limit:
                    break
                appid = row.get("appid") or reverse_map.get(row.get("platform", ""))
                content = row.get("content") or ""
                try:
                    score = int(row.get("score") or row.get("score_proxy") or "")
                except ValueError:
                    continue
                if appid not in APP_MAP or not content.strip() or score not in range(1, 6):
                    continue
                review_key = row.get("review_key")
                if not review_key:
                    reviewid = row.get("reviewid") or ""
                    if not reviewid:
                        continue
                    review_key = hashlib.sha256(f"{appid}:{reviewid}".encode()).hexdigest()[:24]
                event = {"event_id": review_key, "appid": appid, "content": content, "score": score,
                    "event_time": row.get("at"), "ingested_at": datetime.now(timezone.utc).isoformat(),
                    "source_mode": "historical_replay"}
                producer.send(args.topic, key=review_key, value=event).get(timeout=30)
                count += 1
                if args.delay_ms:
                    time.sleep(args.delay_ms / 1000)
        producer.flush()
    finally:
        producer.close()
    print(json.dumps({"published_events": count, "topic": args.topic, "mode": "historical_replay"}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=Path("/workspace/artifacts/audit/annotation_sample.csv"))
    parser.add_argument("--bootstrap", default="kafka:29092")
    parser.add_argument("--topic", default="ecommerce-reviews")
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--delay-ms", type=int, default=0)
    replay(parser.parse_args())

