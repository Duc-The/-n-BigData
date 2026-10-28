"""Cross-check every Spark cube cell against the full serving index independently."""
import argparse
from contextlib import closing
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dashboard.server import Store
from review_analytics.io import atomic_write_text


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--outputs", type=Path, default=Path(__file__).resolve().parents[1] / "outputs")
    args = parser.parse_args()
    root = args.outputs
    store = Store(root)
    metadata, cube = store.snapshot()
    dimensions = ["platform", "month", "score", "spam_candidate"]
    metrics = ["reviews", "unmatched_reviews"] + ["aspect_" + a["code"] for a in metadata["aspects"]]
    expressions = ["COUNT(*) AS reviews", "SUM(aspect_mask=0) AS unmatched_reviews"]
    expressions += [f"SUM((aspect_mask & {a['bit']})>0) AS aspect_{a['code']}" for a in metadata["aspects"]]
    with closing(store.connect()) as connection:
        actual = connection.execute("SELECT platform,month,score,spam AS spam_candidate,"
            # A full audit benefits from a sequential table scan. The partial
            # platform/month/score index would cause random row lookups for
            # aspect_mask and spam across the Docker Desktop bind mount.
            + ",".join(expressions) + " FROM reviews NOT INDEXED GROUP BY platform,month,score,spam").fetchall()
    expected = {tuple(row[d] for d in dimensions): tuple(row[m] for m in metrics) for row in cube}
    observed = {tuple(row[d] for d in dimensions): tuple(row[m] for m in metrics) for row in actual}
    if expected != observed:
        different = [key for key in expected.keys() | observed.keys() if expected.get(key) != observed.get(key)]
        raise ValueError(f"Spark cube/index disagree in {len(different)} cells; example: {different[:3]}")
    result = {"verified_utc": datetime.now(timezone.utc).isoformat(), "generation": metadata["generation"],
        "indexed_reviews": metadata["indexed_rows"], "cube_cells_verified": len(actual),
        "metrics_per_cell": len(metrics), "all_spark_cube_counts_match_full_index": True}
    atomic_write_text(root / "dashboard_validation.json", json.dumps(result, indent=2))
    print(json.dumps(result))


if __name__ == "__main__":
    main()
