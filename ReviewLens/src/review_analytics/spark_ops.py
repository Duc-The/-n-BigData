from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import DataFrame, Window, functions as F, types as T

from review_analytics.text import normalize_text

APP_MAP = {
    "com.shopee.vn": "Shopee", "com.lazada.android": "Lazada",
    "com.sendo": "Sendo", "vn.tiki.app.tikiandroid": "Tiki",
}
RAW_COLUMNS = ["csv_index", "reviewid", "username", "userimage", "content", "score",
    "thumbsupcount", "reviewcreatedversion", "at", "replycontent", "repliedat", "appid"]
RAW_SCHEMA = T.StructType([T.StructField(c, T.StringType(), True)
    for c in RAW_COLUMNS + ["_corrupt_record"]])
SPAM_PATTERN = r"vay\s*(tiền|0%|online)|kiếm\s*tiền\s*(0|online)|nhận\s*(free|miễn phí)\s*\d+|ktien|vaytotnhat|https?://|www\."


def read_raw(spark, path: str) -> DataFrame:
    return (spark.read.schema(RAW_SCHEMA).option("header", True)
        .option("multiLine", True).option("quote", '"').option("escape", '"')
        .option("mode", "PERMISSIVE").option("enforceSchema", True)
        .option("columnNameOfCorruptRecord", "_corrupt_record").csv(path))


def enrich(frame: DataFrame, aspects_file: str = "/workspace/config/aspects.json") -> DataFrame:
    normalize = F.udf(normalize_text, T.StringType())
    mapping = F.create_map(*[F.lit(v) for pair in APP_MAP.items() for v in pair])
    result = (frame.withColumn("normalized_content", normalize("content"))
        .withColumn("score", F.col("score").cast("int"))
        .withColumn("at", F.to_timestamp("at", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("platform", mapping[F.col("appid")])
        .withColumn("review_key", F.substring(F.sha2(F.concat_ws(":", "appid", "reviewid"), 256), 1, 24))
        .withColumn("year", F.year("at")).withColumn("month", F.date_format("at", "yyyy-MM"))
        .withColumn("low_rating_proxy", F.col("score").between(1, 2))
        .withColumn("spam_candidate", F.col("normalized_content").rlike(SPAM_PATTERN)))
    aspects = json.loads(Path(aspects_file).read_text(encoding="utf-8"))
    result = result.withColumn("aspects_mentioned", F.filter(F.array(*[
        F.when(F.col("normalized_content").rlike(spec["pattern"]), F.lit(key))
        for key, spec in aspects.items()]), lambda x: x.isNotNull()))
    return result


def validation_reasons(frame: DataFrame, historical_period: bool = True) -> DataFrame:
    checks = [
        (F.col("_corrupt_record").isNotNull(), "malformed_csv"),
        (F.col("reviewid").isNull() | (F.length(F.trim("reviewid")) == 0), "missing_reviewid"),
        (F.col("normalized_content").isin("", "nan", "null", "none"), "missing_content"),
        (F.col("score").isNull() | ~F.col("score").between(1, 5), "invalid_score"),
        (F.col("at").isNull(), "invalid_date"),
        (F.col("platform").isNull(), "unknown_app"),
    ]
    if historical_period:
        checks.append((F.col("at").isNotNull() & ((F.col("at") < F.lit("2013-01-01").cast("timestamp"))
            | (F.col("at") >= F.lit("2022-09-01").cast("timestamp"))), "outside_period"))
    else:
        checks.append((F.col("at") > F.current_timestamp() + F.expr("INTERVAL 2 DAYS"), "future_date"))
    return frame.withColumn("validation_reasons", F.filter(
        F.array(*[F.when(condition, F.lit(reason)) for condition, reason in checks]),
        lambda x: x.isNotNull()))


def deduplicate_valid(frame: DataFrame) -> DataFrame:
    # Stable latest-record selection, including a deterministic tie-breaker.
    order_hash = F.sha2(F.concat_ws("|", "normalized_content", F.col("score").cast("string")), 256)
    window = Window.partitionBy("appid", "reviewid").orderBy(F.col("at").desc(), order_hash.desc())
    return frame.withColumn("_rank", F.row_number().over(window)).filter("_rank = 1").drop("_rank")


def analytical_columns(frame: DataFrame) -> DataFrame:
    return frame.select("review_key", "platform", "content", "normalized_content", "score", "at",
        "year", "month", "low_rating_proxy", "spam_candidate", "aspects_mentioned")

