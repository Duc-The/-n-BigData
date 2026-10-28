"""Prepare compact chart aggregates and a serving export directly from HDFS."""
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import uuid

from pyspark.sql import SparkSession, functions as F
from review_analytics.io import atomic_text_writer, atomic_write_text


def main():
    root, exports = "hdfs://namenode:9000/ecommerce", Path("/exports")
    config = json.loads(Path("/workspace/config/aspects.json").read_text(encoding="utf-8"))
    generation = uuid.uuid4().hex
    spark = (SparkSession.builder.appName("dashboard-serving-from-hdfs")
        .config("spark.cores.max", 4).config("spark.executor.cores", 1)
        .config("spark.sql.shuffle.partitions", 8)
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        reviews = spark.read.parquet(f"{root}/processed/reviews")
        expressions = [F.count("*").alias("reviews"),
            F.sum(F.when(F.size("aspects_mentioned") == 0, 1).otherwise(0)).alias("unmatched_reviews")]
        expressions += [F.sum(F.when(F.array_contains("aspects_mentioned", code), 1).otherwise(0))
            .alias(f"aspect_{code}") for code in sorted(config)]
        cube = reviews.groupBy("platform", "month", "score", "spam_candidate").agg(*expressions) \
            .withColumn("generation", F.lit(generation))
        cube.write.mode("overwrite").parquet(f"{root}/output/dashboard_cube")
        rows = cube.orderBy("platform", "month", "score", "spam_candidate").limit(10001).collect()
        if len(rows) > 10000:
            raise ValueError("Dashboard aggregates exceeded the small-table export limit")
        total = sum(row["reviews"] for row in rows)
        quality = json.loads((exports / "data_quality.json").read_text(encoding="utf-8"))
        if total != quality["clean_rows"]:
            raise ValueError("Dashboard cube disagrees with processed review count")
        with atomic_text_writer(exports / "dashboard_cube.csv", "utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=cube.columns)
            writer.writeheader()
            writer.writerows(row.asDict() for row in rows)
        (reviews.select("review_key", "platform", "month", "score", "content", "spam_candidate",
            "aspects_mentioned", F.date_format("at", "yyyy-MM-dd HH:mm:ss").alias("at"))
            .repartition(8).write.mode("overwrite").option("compression", "gzip")
            .json(f"{root}/serving/review_index"))
        raw = json.loads((exports / "raw_inspection.json").read_text(encoding="utf-8"))
        metadata = {"generation": generation, "prepared_utc": datetime.now(timezone.utc).isoformat(),
            "processed_rows": total, "cube_rows": len(rows), "source_sha256": raw.get("source_sha256"),
            "aspect_config_sha256": hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest(),
            "month_min": min(row["month"] for row in rows), "month_max": max(row["month"] for row in rows),
            "aspects": [{"code": code, "label": config[code]["label"], "bit": 1 << i}
                for i, code in enumerate(sorted(config))],
            "hdfs_serving_source": f"{root}/serving/review_index"}
        atomic_write_text(exports / "dashboard_preparation.json", json.dumps(metadata, ensure_ascii=False, indent=2))
        print(json.dumps({"processed_rows": total, "cube_rows": len(rows), "generation": generation}), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
