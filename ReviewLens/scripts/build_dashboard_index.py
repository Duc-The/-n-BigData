"""Stream the Spark HDFS export into an atomic local full-text serving index."""
import argparse
import gzip
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import unicodedata


def search_text(value):
    value = unicodedata.normalize("NFKD", value.lower().replace("đ", "d"))
    return "".join(character for character in value if unicodedata.category(character) != "Mn")


def build(records, destination, metadata):
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=".review-index-", suffix=".sqlite", dir=destination.parent)
    os.close(descriptor)
    temporary = Path(temporary_name)
    connection = sqlite3.connect(temporary)
    try:
        connection.executescript("""
            PRAGMA journal_mode=DELETE;
            PRAGMA synchronous=NORMAL;
            PRAGMA temp_store=MEMORY;
            PRAGMA cache_size=-65536;
            CREATE TABLE reviews(id INTEGER PRIMARY KEY, review_key TEXT UNIQUE NOT NULL,
                platform TEXT NOT NULL, month TEXT NOT NULL, at TEXT NOT NULL, score INTEGER NOT NULL,
                content TEXT NOT NULL, search_content TEXT NOT NULL, spam INTEGER NOT NULL, aspect_mask INTEGER NOT NULL);
            CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE VIRTUAL TABLE review_fts USING fts5(search_content, content='reviews', content_rowid='id',
                tokenize='unicode61 remove_diacritics 2');
        """)
        bits = {aspect["code"]: aspect["bit"] for aspect in metadata["aspects"]}
        batch, count = [], 0
        for record in records:
            batch.append((record["review_key"], record["platform"], record["month"], record["at"],
                record["score"], record["content"], search_text(record["content"]),
                int(record.get("spam_candidate", False)), sum(bits[code] for code in record["aspects_mentioned"])))
            if len(batch) == 5000:
                connection.executemany("INSERT INTO reviews(review_key,platform,month,at,score,content,search_content,spam,aspect_mask) VALUES(?,?,?,?,?,?,?,?,?)", batch)
                count += len(batch)
                batch.clear()
                if count % 100000 == 0:
                    connection.commit()
                    print(f"Indexed {count:,} reviews", flush=True)
        if batch:
            connection.executemany("INSERT INTO reviews(review_key,platform,month,at,score,content,search_content,spam,aspect_mask) VALUES(?,?,?,?,?,?,?,?,?)", batch)
            count += len(batch)
        if count != metadata["processed_rows"]:
            raise ValueError(f"Index got {count} rows, expected {metadata['processed_rows']}")
        print("Building search and filter indexes", flush=True)
        connection.executescript("""
            CREATE INDEX review_platform_date ON reviews(platform,month,score);
            CREATE INDEX review_date ON reviews(at DESC);
            CREATE INDEX review_month_score ON reviews(month,score);
            INSERT INTO review_fts(review_fts) VALUES('rebuild');
            INSERT INTO review_fts(review_fts, rank) VALUES('integrity-check', 1);
        """)
        metadata = {**metadata, "indexed_rows": count, "sqlite_version": sqlite3.sqlite_version}
        connection.execute("INSERT INTO metadata VALUES('snapshot',?)", (json.dumps(metadata, ensure_ascii=False),))
        connection.commit()
        assert connection.execute("PRAGMA quick_check").fetchone()[0] == "ok"
        connection.close()
        os.chmod(temporary, 0o664)
        os.replace(temporary, destination)
        print(json.dumps({"indexed_rows": count, "database": str(destination), "verification": "passed"}), flush=True)
        return metadata
    finally:
        connection.close()
        temporary.unlink(missing_ok=True)
        Path(str(temporary) + "-journal").unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("/exports/review_search.sqlite"))
    args = parser.parse_args()
    metadata = json.loads((args.output.parent / "dashboard_preparation.json").read_text(encoding="utf-8"))
    process = subprocess.Popen(["hdfs", "dfs", "-cat", metadata["hdfs_serving_source"] + "/*.gz"], stdout=subprocess.PIPE)
    try:
        stream = io.TextIOWrapper(gzip.GzipFile(fileobj=process.stdout), encoding="utf-8")
        def records():
            for line in stream:
                yield json.loads(line)
            if process.wait() != 0:
                raise RuntimeError("Could not finish reading the HDFS serving export")
        build(records(), args.output, metadata)
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait()
        process.stdout.close()


if __name__ == "__main__":
    main()
