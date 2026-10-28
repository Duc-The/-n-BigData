"""Audit the original CSV with bounded sampling and no external dependencies.

Counts CSV records, including quoted newlines, rather than physical file lines.
Annotation exports omit usernames, avatars and developer replies.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
import re
import time
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

PLATFORMS = {
    "com.shopee.vn": "Shopee",
    "com.lazada.android": "Lazada",
    "com.sendo": "Sendo",
    "vn.tiki.app.tikiandroid": "Tiki",
}
REQUIRED = {"reviewid", "content", "score", "at", "appid"}
EMPTY = {"", "nan", "null", "none"}
# A candidate flag for manual inspection; do not automatically delete these rows.
SPAM_CANDIDATE = re.compile(
    r"vay\s*(?:tiền|0%|online)|kiếm\s*tiền\s*(?:0|online)|"
    r"nhận\s*(?:free|miễn phí)\s*\d+|(?:ktien|vaytotnhat)|"
    r"(?:https?://|www\.)", re.IGNORECASE
)


def is_empty(value: str | None) -> bool:
    return value is None or value.strip().lower() in EMPTY


def normalized_text(value: str) -> str:
    return " ".join(unicodedata.normalize("NFC", value).lower().split())


def audit(source: Path, destination: Path, sample_per_stratum: int = 50) -> dict:
    csv.field_size_limit(10_000_000)
    rng = random.Random(42)
    start = time.monotonic()
    total = malformed = invalid_score = invalid_date = out_of_scope = 0
    missing, appids, scores, months = Counter(), Counter(), Counter(), Counter()
    id_seen: set[tuple[str, str]] = set()
    text_seen: set[tuple[str, bytes]] = set()
    duplicate_id = duplicate_text = blank_content = spam_candidates = 0
    platform_stats = defaultdict(Counter)
    reservoirs = defaultdict(list)
    reservoir_counts = Counter()
    min_date = max_date = None
    columns = []
    with source.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = reader.fieldnames or []
        if not REQUIRED.issubset(columns):
            raise ValueError(f"Missing columns: {sorted(REQUIRED - set(columns))}")
        for row in reader:
            total += 1
            if None in row or any(row.get(c) is None for c in columns):
                malformed += 1
            for column in columns:
                if is_empty(row.get(column)):
                    missing[column or "csv_index"] += 1
            appid = row.get("appid") or ""
            platform = PLATFORMS.get(appid, f"UNKNOWN:{appid}")
            appids[appid] += 1
            stat = platform_stats[platform]
            stat["rows"] += 1
            score_value = row.get("score") or ""
            scores[score_value] += 1
            try:
                score = int(score_value)
                if score not in range(1, 6):
                    raise ValueError
            except ValueError:
                score = None
                invalid_score += 1
                stat["invalid_score"] += 1
            if score is not None:
                stat[f"score_{score}"] += 1
                stat["rating_sum"] += score
                if score <= 2:
                    stat["low_rating_proxy"] += 1
            date = None
            try:
                date = datetime.fromisoformat(row.get("at") or "")
                if date.tzinfo is not None:
                    date = date.replace(tzinfo=None)
                min_date = date if min_date is None else min(min_date, date)
                max_date = date if max_date is None else max(max_date, date)
                months[(platform, date.strftime("%Y-%m"))] += 1
                if not datetime(2013, 1, 1) <= date < datetime(2022, 9, 1):
                    out_of_scope += 1
                    stat["outside_requested_period"] += 1
            except (ValueError, TypeError):
                invalid_date += 1
                stat["invalid_date"] += 1
            reviewid = row.get("reviewid") or ""
            if not is_empty(reviewid):
                id_key = (appid, reviewid)
                if id_key in id_seen:
                    duplicate_id += 1
                    stat["duplicate_id_rows"] += 1
                id_seen.add(id_key)
            content = row.get("content") or ""
            if is_empty(content):
                blank_content += 1
                stat["blank_content"] += 1
                continue
            normalized = normalized_text(content)
            text_hash = hashlib.sha256(normalized.encode("utf-8")).digest()
            text_key = (appid, text_hash)
            if text_key in text_seen:
                duplicate_text += 1
                stat["repeated_text_rows"] += 1
            text_seen.add(text_key)
            candidate = bool(SPAM_CANDIDATE.search(normalized))
            if candidate:
                spam_candidates += 1
                stat["spam_candidate_rows"] += 1
            if (score is not None and date is not None and appid in PLATFORMS
                    and datetime(2013, 1, 1) <= date < datetime(2022, 9, 1)):
                stratum = (platform, score)
                reservoir_counts[stratum] += 1
                n = reservoir_counts[stratum]
                # All copies of the same text get the same split, across platforms.
                bucket = int.from_bytes(text_hash[:4], "big") % 10
                split = "test" if bucket < 2 else "validation" if bucket < 4 else "train"
                record = {
                    "review_key": hashlib.sha256(f"{appid}:{reviewid}".encode()).hexdigest()[:24],
                    "platform": platform, "score_proxy": score,
                    "at": date.isoformat(sep=" "), "content": content,
                    "text_group": text_hash.hex(), "split": split,
                    "spam_candidate": int(candidate),
                    "complaint_gold": "", "aspects_gold": "",
                    "aspect_polarities_gold": "", "spam_gold": "",
                    "annotator": "", "notes": "",
                }
                reservoir = reservoirs[stratum]
                if len(reservoir) < sample_per_stratum:
                    reservoir.append(record)
                else:
                    replace = rng.randrange(n)
                    if replace < sample_per_stratum:
                        reservoir[replace] = record
            if total % 250_000 == 0:
                print(f"Audited {total:,} records", flush=True)
    for platform, stat in platform_stats.items():
        valid_n = sum(stat[f"score_{s}"] for s in range(1, 6))
        stat["valid_score_rows"] = valid_n
        stat["mean_rating"] = round(stat["rating_sum"] / valid_n, 4) if valid_n else None
        stat["low_rating_rate"] = round(stat["low_rating_proxy"] / valid_n, 6) if valid_n else None
    checksum = hashlib.sha256()
    with source.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            checksum.update(chunk)
    report = {
        "source": source.name, "source_url": "https://www.kaggle.com/datasets/hienbm/vietnamese-ecommerce-review",
        "source_bytes": source.stat().st_size, "sha256": checksum.hexdigest(),
        "audit_generated_utc": datetime.now(timezone.utc).isoformat(),
        "csv_records": total, "columns": columns, "appid_counts": dict(appids),
        "score_counts": dict(scores), "missing_by_column": dict(missing),
        "date_min": str(min_date), "date_max": str(max_date),
        "malformed_records": malformed, "invalid_scores": invalid_score,
        "invalid_dates": invalid_date, "outside_requested_period": out_of_scope,
        "duplicate_id_rows": duplicate_id, "repeated_text_rows_within_platform": duplicate_text,
        "blank_content_rows": blank_content, "spam_candidate_rows": spam_candidates,
        "platforms": {p: dict(s) for p, s in platform_stats.items()},
        "annotation_sampling": {"seed": 42, "per_platform_score": sample_per_stratum,
            "stratum_populations": {f"{p}:{s}": n for (p, s), n in reservoir_counts.items()},
            "warning": "Stratified sample; unweighted sample metrics are not population prevalence."},
        "elapsed_seconds": round(time.monotonic() - start, 2),
    }
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "dataset_profile.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    with (destination / "monthly_volume.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["platform", "month", "reviews"])
        for (platform, month), count in sorted(months.items()):
            writer.writerow([platform, month, count])
    sample = [record for key in sorted(reservoirs) for record in reservoirs[key]]
    if sample:
        with (destination / "annotation_sample.csv").open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(sample[0]))
            writer.writeheader()
            writer.writerows(sample)
    print(json.dumps({k: report[k] for k in ["csv_records", "date_min", "date_max", "appid_counts",
        "malformed_records", "duplicate_id_rows", "repeated_text_rows_within_platform",
        "blank_content_rows", "spam_candidate_rows", "elapsed_seconds"]}, ensure_ascii=False, indent=2))
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=Path("vietnamese_ecommerce_review.csv"))
    parser.add_argument("--output", type=Path, default=Path("artifacts/audit"))
    parser.add_argument("--sample-per-stratum", type=int, default=50)
    args = parser.parse_args()
    if args.sample_per_stratum < 1:
        parser.error("sample-per-stratum must be positive")
    audit(args.input, args.output, args.sample_per_stratum)
