"""Distributed batch pipeline. Every data path points to HDFS by default."""
from __future__ import annotations

import argparse
import csv
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window, functions as F, types as T
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import CountVectorizer, IDF, NGram, Normalizer, RegexTokenizer, StopWordsRemover

from review_analytics.spark_ops import analytical_columns, deduplicate_valid, enrich, read_raw, validation_reasons
from review_analytics.io import atomic_text_writer, atomic_write_text


def save_metadata(spark, root, name, value, exports):
    spark.read.json(spark.sparkContext.parallelize([json.dumps(value, ensure_ascii=False)])) \
        .coalesce(1).write.mode("overwrite").json(f"{root}/output/{name}")
    exports.mkdir(parents=True, exist_ok=True)
    atomic_write_text(exports / f"{name}.json", json.dumps(value, ensure_ascii=False, indent=2))


def inspect(spark, args):
    raw = read_raw(spark, args.input)
    n = raw.count()
    meta = {"raw_rows": n, "input": args.input, "reader_partitions": raw.rdd.getNumPartitions(),
        "schema": raw.schema.jsonValue(), "multiline": True, "spark_version": spark.version}
    audit_path = Path("/workspace/artifacts/audit/dataset_profile.json")
    if audit_path.exists():
        audit = json.loads(audit_path.read_text(encoding="utf-8"))
        meta["source_sha256"] = audit["sha256"]
        meta["audit_rows_match"] = n == audit["csv_records"]
        if not meta["audit_rows_match"]:
            raise ValueError(f"Spark read {n} records; audit read {audit['csv_records']}")
    save_metadata(spark, args.root, "raw_inspection", meta, args.exports)
    print(json.dumps(meta, ensure_ascii=False), flush=True)


def clean(spark, args):
    raw = read_raw(spark, args.input).select("reviewid", "appid", "content", "score", "at", "_corrupt_record")
    prepared = validation_reasons(enrich(raw.repartition(args.partitions))).persist(StorageLevel.MEMORY_AND_DISK)
    raw_n = prepared.count()
    invalid = prepared.filter(F.size("validation_reasons") > 0)
    invalid_n = invalid.count()
    (invalid.select("review_key", "appid", "content", "score", "at", "validation_reasons")
        .write.mode("overwrite").parquet(f"{args.root}/quarantine/reviews"))
    valid = prepared.filter(F.size("validation_reasons") == 0)
    cleaned = analytical_columns(deduplicate_valid(valid)).persist(StorageLevel.MEMORY_AND_DISK)
    clean_n = cleaned.count()
    duplicates_n = raw_n - invalid_n - clean_n
    cleaned.repartition(args.partitions, "platform", "year").write.mode("overwrite") \
        .partitionBy("platform", "year").parquet(f"{args.root}/processed/reviews")
    reasons = {r["reason"]: r["count"] for r in invalid.select(
        F.explode("validation_reasons").alias("reason")).groupBy("reason").count().collect()}
    meta = {"raw_rows": raw_n, "quarantined_rows": invalid_n, "clean_rows": clean_n,
        "duplicate_valid_id_rows_removed": duplicates_n,
        "row_balance_ok": raw_n == invalid_n + duplicates_n + clean_n,
        "validation_reason_counts": reasons, "text_repetition_policy": "keep distinct review IDs",
        "output_partitions": args.partitions, "partition_columns": ["platform", "year"]}
    save_metadata(spark, args.root, "data_quality", meta, args.exports)
    prepared.unpersist()
    cleaned.unpersist()
    print(json.dumps(meta, ensure_ascii=False), flush=True)


def analyze(spark, args):
    reviews = spark.read.parquet(f"{args.root}/processed/reviews")
    reviews.createOrReplaceTempView("reviews")
    tables = {
        "review_length_summary": spark.sql("""
            SELECT platform, count(*) AS reviews, avg(length(normalized_content)) AS mean_chars,
                   min(length(normalized_content)) AS min_chars, max(length(normalized_content)) AS max_chars,
                   percentile_approx(length(normalized_content), array(0.25,0.5,0.75,0.95)) AS quartiles_p95
            FROM reviews GROUP BY platform ORDER BY platform
        """),
        "platform_summary": spark.sql("""
            SELECT platform, COUNT(*) AS reviews, AVG(score) AS mean_rating,
                   SUM(CASE WHEN low_rating_proxy THEN 1 ELSE 0 END) AS low_rating_reviews,
                   AVG(CASE WHEN low_rating_proxy THEN 1.0 ELSE 0.0 END) AS low_rating_rate,
                   SUM(CASE WHEN spam_candidate THEN 1 ELSE 0 END) AS spam_candidate_reviews
            FROM reviews GROUP BY platform ORDER BY platform
        """),
        "monthly_summary": spark.sql("""
            SELECT platform, month, COUNT(*) AS reviews, AVG(score) AS mean_rating,
                   SUM(CASE WHEN low_rating_proxy THEN 1 ELSE 0 END) AS low_rating_reviews,
                   AVG(CASE WHEN low_rating_proxy THEN 1.0 ELSE 0.0 END) AS low_rating_rate
            FROM reviews GROUP BY platform, month ORDER BY platform, month
        """),
        "rating_distribution": spark.sql("""
            SELECT platform, score, COUNT(*) AS reviews
            FROM reviews GROUP BY platform, score ORDER BY platform, score
        """),
        "aspect_mentions": spark.sql("""
            WITH low AS (SELECT * FROM reviews WHERE low_rating_proxy),
            denominators AS (SELECT platform, COUNT(*) AS low_rating_reviews FROM low GROUP BY platform),
            mentions AS (SELECT platform, explode(aspects_mentioned) AS aspect FROM low),
            counts AS (SELECT platform, aspect, COUNT(*) AS mention_reviews FROM mentions GROUP BY platform, aspect)
            SELECT c.*, d.low_rating_reviews,
                   c.mention_reviews * 1.0 / d.low_rating_reviews AS mention_rate_among_low
            FROM counts c JOIN denominators d ON c.platform = d.platform
            ORDER BY c.platform, c.mention_reviews DESC
        """),
        "unmatched_low_ratings": spark.sql("""
            SELECT platform, COUNT(*) AS low_rating_reviews,
                   SUM(CASE WHEN size(aspects_mentioned) = 0 THEN 1 ELSE 0 END) AS unmatched_reviews,
                   AVG(CASE WHEN size(aspects_mentioned) = 0 THEN 1.0 ELSE 0.0 END) AS unmatched_rate
            FROM reviews WHERE low_rating_proxy GROUP BY platform ORDER BY platform
        """),
        "spam_sensitivity": spark.sql("""
            SELECT platform, COUNT(*) AS reviews_without_spam_candidates,
                   AVG(CASE WHEN low_rating_proxy THEN 1.0 ELSE 0.0 END) AS low_rating_rate
            FROM reviews WHERE NOT spam_candidate GROUP BY platform ORDER BY platform
        """),
    }
    for name, table in tables.items():
        table.write.mode("overwrite").parquet(f"{args.root}/output/{name}")
    # The representative query demonstrates partition pruning.
    plan = spark.sql("SELECT platform, month, count(*) FROM reviews WHERE platform = 'Shopee' AND year = 2022 GROUP BY platform, month")
    atomic_write_text(args.exports / "spark_query_plan.txt", plan._jdf.queryExecution().toString())
    print("Spark SQL tables saved to HDFS", flush=True)


def cluster(spark, args):
    candidates = spark.read.parquet(f"{args.root}/processed/reviews").filter(
        "low_rating_proxy AND NOT spam_candidate AND length(normalized_content) >= 10")
    # Stable hash sample. Not a supervised training/evaluation split.
    training = candidates.filter(F.pmod(F.xxhash64("review_key"), F.lit(100)) < args.cluster_percent)
    tokenizer = RegexTokenizer(inputCol="normalized_content", outputCol="tokens", pattern=r"[^\p{L}\p{N}]+", minTokenLength=2)
    remover = StopWordsRemover(inputCol="tokens", outputCol="terms", stopWords=[
        "và", "là", "của", "cho", "với", "thì", "mà", "có", "đã", "này", "đó", "các", "những",
        "mình", "tôi", "bạn", "rất", "quá", "được", "nên", "vẫn", "cũng", "còn", "vào", "khi"])
    prepared = remover.transform(tokenizer.transform(training)).filter(F.size("terms") >= 2)
    ngram = NGram(n=2, inputCol="terms", outputCol="bigrams")
    prepared = ngram.transform(prepared).withColumn("all_terms", F.concat("terms", "bigrams"))
    feature_model = Pipeline(stages=[
        CountVectorizer(inputCol="all_terms", outputCol="tf", vocabSize=10000, minDF=10),
        IDF(inputCol="tf", outputCol="tfidf"),
        Normalizer(inputCol="tfidf", outputCol="features")]).fit(prepared)
    nonzero = F.udf(lambda vector: vector.numNonzeros() > 0, T.BooleanType())
    features = feature_model.transform(prepared).filter(nonzero("features")).persist(StorageLevel.MEMORY_AND_DISK)
    train_n = features.count()
    if train_n <= args.k:
        raise ValueError(f"Need more than {args.k} nonzero documents, got {train_n}")
    model = KMeans(k=args.k, seed=42, maxIter=20, distanceMeasure="cosine").fit(features)
    predictions = model.transform(features).persist(StorageLevel.MEMORY_AND_DISK)
    evaluator = ClusteringEvaluator(distanceMeasure="cosine")
    evaluation_sample = predictions.orderBy(F.xxhash64("review_key")).limit(10000)
    silhouette = evaluator.evaluate(evaluation_sample)
    model.write().overwrite().save(f"{args.root}/models/kmeans")
    feature_model.write().overwrite().save(f"{args.root}/models/tfidf")
    predictions.select("review_key", "platform", "month", "prediction").write.mode("overwrite") \
        .parquet(f"{args.root}/output/cluster_assignments_sample")
    vocab = feature_model.stages[0].vocabulary
    centers = []
    for index, center in enumerate(model.clusterCenters()):
        top = sorted(range(len(center)), key=lambda j: center[j], reverse=True)[:15]
        centers.append({"cluster": index, "top_terms": [vocab[j] for j in top], "human_label": "unassigned"})
    examples = predictions.withColumn("rank", F.row_number().over(
        Window.partitionBy("prediction").orderBy(F.xxhash64("review_key")))).filter("rank <= 3")
    meta = {"k": args.k, "seed": 42, "distance": "cosine", "training_rows": train_n,
        "sample_percent": args.cluster_percent, "silhouette_sample_max": 10000,
        "silhouette_cosine": silhouette, "tokenization": "Vietnamese syllable unigrams/bigrams",
        "clusters": centers, "examples": [r.asDict() for r in examples.select(
            "prediction", "platform", "content").collect()],
        "interpretation": "Exploratory clusters; not aspect classification accuracy or causal proof."}
    save_metadata(spark, args.root, "cluster_metadata", meta, args.exports)
    predictions.unpersist()
    features.unpersist()
    print(json.dumps({"training_rows": train_n, "silhouette": silhouette}), flush=True)


def export_tables(spark, args):
    names = ["review_length_summary", "platform_summary", "monthly_summary", "rating_distribution", "aspect_mentions",
        "unmatched_low_ratings", "spam_sensitivity"]
    args.exports.mkdir(parents=True, exist_ok=True)
    for name in names:
        frame = spark.read.parquet(f"{args.root}/output/{name}")
        rows = frame.limit(10001).collect()
        if len(rows) > 10000:
            raise ValueError("Only small aggregated tables may be exported to the driver")
        with atomic_text_writer(args.exports / f"{name}.csv", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=frame.columns)
            writer.writeheader()
            writer.writerows(r.asDict() for r in rows)
    save_metadata(spark, args.root, "batch_success", {
        "completed_utc": datetime.now(timezone.utc).isoformat(), "exported_tables": names,
        "aspect_method": "keyword mentions in low-rated reviews; not validated aspect polarity"}, args.exports)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", choices=["inspect", "clean", "analyze", "cluster", "export", "all"], default="all")
    parser.add_argument("--input", default="hdfs://namenode:9000/ecommerce/raw/reviews.csv")
    parser.add_argument("--root", default="hdfs://namenode:9000/ecommerce")
    parser.add_argument("--exports", type=Path, default=Path("/exports"))
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--k", type=int, default=8)
    parser.add_argument("--cluster-percent", type=int, default=25)
    args = parser.parse_args()
    if not 1 <= args.cluster_percent <= 100:
        parser.error("cluster-percent must be 1..100")
    args.exports.mkdir(parents=True, exist_ok=True)
    spark = (SparkSession.builder.appName(f"ecommerce-{args.stage}")
        .config("spark.sql.shuffle.partitions", args.partitions)
        .config("spark.cores.max", 4).config("spark.executor.cores", 1)
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        .config("spark.sql.adaptive.enabled", "true").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    stages = {"inspect": inspect, "clean": clean, "analyze": analyze, "cluster": cluster, "export": export_tables}
    timings = {}
    try:
        for name, action in stages.items():
            if args.stage in (name, "all"):
                start = time.monotonic()
                action(spark, args)
                timings[name] = round(time.monotonic() - start, 3)
        atomic_write_text(args.exports / f"timings_{args.stage}.json", json.dumps(timings, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
