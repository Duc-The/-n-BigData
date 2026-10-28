"""Verify actual HDFS Parquet records, rather than trusting local summary files."""
import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from review_analytics.io import atomic_write_text


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="hdfs://namenode:9000/ecommerce")
    parser.add_argument("--exports", type=Path, default=Path("/exports"))
    args = parser.parse_args()
    quality = json.loads((args.exports / "data_quality.json").read_text(encoding="utf-8"))
    spark = (SparkSession.builder.appName("verify-persisted-hdfs-records")
        .config("spark.cores.max", 4).config("spark.executor.cores", 1)
        .config("spark.sql.shuffle.partitions", 8).getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        clean = spark.read.parquet(f"{args.root}/processed/reviews")
        counts = clean.agg(F.count("*").alias("rows"),
            F.countDistinct("review_key").alias("unique_keys")).first()
        assert counts["rows"] == quality["clean_rows"], "HDFS processed count differs from quality report"
        assert counts["rows"] == counts["unique_keys"], "Duplicate or null keys in processed Parquet"
        summary_total = spark.read.parquet(f"{args.root}/output/platform_summary").agg(F.sum("reviews")).first()[0]
        assert summary_total == counts["rows"], "Platform aggregate does not cover the processed dataset"
        stream = spark.read.parquet(f"{args.root}/streaming/reviews")
        streamed = stream.agg(F.count("*").alias("rows"),
            F.countDistinct("review_key").alias("unique_keys")).first()
        assert streamed["rows"] == streamed["unique_keys"], "Replayed reviews multiplied in persisted output"
        metrics = spark.read.option("recursiveFileLookup", True).json(f"{args.root}/streaming/metrics")
        metric_summary = metrics.agg(F.sum("valid").alias("valid"),
            F.count("*").alias("batches"),
            F.sum(F.when((F.col("batch_id") > 0) & (F.col("new_unique_events") == 0), 1).otherwise(0))
                .alias("batches_without_new_unique_events")).first()
        assert metric_summary["valid"] == streamed["rows"], "Streaming metrics disagree with HDFS rows"
        report = {
            "verification": "passed", "processed_rows": counts["rows"],
            "processed_unique_keys": counts["unique_keys"], "platform_summary_total": summary_total,
            "streaming_rows": streamed["rows"], "streaming_unique_keys": streamed["unique_keys"],
            "streaming_batches": metric_summary["batches"],
            "batches_without_new_unique_events": metric_summary["batches_without_new_unique_events"],
            "source": "actual HDFS Parquet and streaming metrics",
        }
        atomic_write_text(args.exports / "persisted_validation.json", json.dumps(report, indent=2))
        spark.read.json(spark.sparkContext.parallelize([json.dumps(report)])).coalesce(1) \
            .write.mode("overwrite").json(f"{args.root}/output/persisted_validation")
        print(json.dumps(report), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
