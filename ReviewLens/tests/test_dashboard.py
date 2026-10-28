import csv
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dashboard.server import Store, CombinedStore, csv_safe
from scripts.build_dashboard_index import build


class DashboardDataTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.aspects = [{"code": "app", "label": "Ứng dụng", "bit": 1},
            {"code": "delivery", "label": "Vận chuyển", "bit": 2},
            {"code": "product", "label": "Sản phẩm", "bit": 4}]
        self.meta = {"generation": "fixture-generation", "processed_rows": 5,
            "month_min": "2022-01", "month_max": "2022-02", "aspects": self.aspects}
        fixtures = [("r1", "Shopee", "2022-01", 1, "Giao hàng chậm, không hài lòng", ["delivery"], False),
            ("r2", "Shopee", "2022-02", 5, "Ứng dụng tốt", ["app"], False),
            ("r3", "Lazada", "2022-01", 2, "Không đăng nhập được", ["app"], False),
            ("r4", "Lazada", "2022-02", 1, "Vay tiền <script>alert(1)</script>", [], True),
            ("r5", "Tiki", "2022-01", 5, "Hàng tốt", ["product"], False)]
        self.records = [{"review_key": key, "platform": p, "month": m, "score": s, "content": c,
            "aspects_mentioned": a, "spam_candidate": spam, "at": m + "-15 12:00:00"}
            for key, p, m, s, c, a, spam in fixtures]
        build(iter(self.records), self.root / "review_search.sqlite", self.meta)
        (self.root / "dashboard_preparation.json").write_text(json.dumps(self.meta), encoding="utf-8")
        columns = ["generation", "platform", "month", "score", "spam_candidate", "reviews", "unmatched_reviews"] \
            + ["aspect_" + a["code"] for a in self.aspects]
        with (self.root / "dashboard_cube.csv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns)
            writer.writeheader()
            for record in self.records:
                row = {"generation": self.meta["generation"], "platform": record["platform"], "month": record["month"],
                    "score": record["score"], "spam_candidate": record["spam_candidate"], "reviews": 1,
                    "unmatched_reviews": int(not record["aspects_mentioned"])}
                row.update({"aspect_" + a["code"]: int(a["code"] in record["aspects_mentioned"]) for a in self.aspects})
                writer.writerow(row)
        self.store = Store(self.root)

    def tearDown(self):
        self.directory.cleanup()

    def combined_fixture(self):
        self.meta.update(prepared_utc="2022-09-01T00:00:00+00:00", hdfs_serving_source="hdfs://historical")
        (self.root / "dashboard_preparation.json").write_text(json.dumps(self.meta))
        live_root = self.root / "live-fixture"
        live_root.mkdir()
        meta = {**self.meta, "generation": "live-fixture", "processed_rows": 3,
                "month_min": "2026-09", "month_max": "2026-10",
                "prepared_utc": "2026-10-15T00:00:00+00:00", "hdfs_serving_source": "hdfs://live"}
        records = [
            {**self.records[2], "score": 5, "month": "2026-09", "at": "2026-09-15 12:00:00", "content": "Đã sửa ứng dụng"},
            {**self.records[0], "review_key": "r6", "month": "2026-10", "at": "2026-10-15 12:00:00"},
            {**self.records[4], "review_key": "r7", "score": 2, "month": "2026-10", "at": "2026-10-14 12:00:00"},
        ]
        build(iter(records), live_root / "review_search.sqlite", meta)
        (live_root / "dashboard_preparation.json").write_text(json.dumps(meta))
        columns = ["generation", "platform", "month", "score", "spam_candidate", "reviews", "unmatched_reviews"] + ["aspect_" + a["code"] for a in self.aspects]
        with (live_root / "dashboard_cube.csv").open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns);writer.writeheader()
            for r in records:
                row = {"generation": meta["generation"], "platform": r["platform"], "month": r["month"], "score": r["score"],
                       "spam_candidate": r["spam_candidate"], "reviews": 1, "unmatched_reviews": int(not r["aspects_mentioned"])}
                row.update({"aspect_" + a["code"]: int(a["code"] in r["aspects_mentioned"]) for a in self.aspects})
                writer.writerow(row)
        return CombinedStore(self.store, Store(live_root))

    def test_combined_source_supersedes_old_review_and_matches_cube(self):
        store = self.combined_fixture()
        metadata, _ = store.snapshot()
        self.assertEqual(metadata["processed_rows"], 7)
        self.assertEqual(metadata["overlapping_review_ids"], 1)
        self.assertEqual((metadata["month_min"], metadata["month_max"]), ("2022-01", "2026-10"))
        self.assertEqual(store.overview({})["stats"]["reviews"], 7)
        self.assertEqual(store.overview({})["stats"]["low"], 4)
        self.assertEqual(store.reviews({"score": ["all"]})["total"], 7)
        self.assertEqual(store.reviews({"q": ["dang nhap"], "score": ["all"]})["total"], 0)
        result = store.reviews({"q": ["da sua"], "score": ["all"]})
        self.assertEqual(result["total"], 1)
        self.assertEqual(result["rows"][0]["review_key"], "r3")
        # A filter excluding the new version must not bring its old version back.
        old = {"from": ["2022-01"], "to": ["2022-02"], "score": ["all"]}
        self.assertEqual(store.overview(old)["stats"]["reviews"], 4)
        self.assertEqual(store.reviews(old)["total"], 4)
        self.assertEqual(store.reviews({"q": ["giao hang"], "score": ["all"]})["total"], 2)
        pages = [store.reviews({"score": ["all"], "limit": ["2"], "offset": [str(i)]}) for i in range(0, 7, 2)]
        keys = [r["review_key"] for page in pages for r in page["rows"]]
        self.assertEqual(len(set(keys)), 7)
        self.assertEqual(keys[:3], ["r6", "r7", "r3"])

    def test_combined_source_works_before_first_live_snapshot(self):
        store = self.combined_fixture()
        historical_only = CombinedStore(self.store)
        self.assertEqual(historical_only.overview({})["stats"]["reviews"], 5)
        self.assertEqual(historical_only.reviews({"score": ["all"]})["total"], 5)

    def test_filters_recalculate_denominators_and_match_lookup(self):
        result = self.store.overview({"exclude_spam": ["1"]})
        self.assertEqual(result["stats"]["reviews"], 4)
        self.assertEqual(result["stats"]["low"], 2)
        self.assertEqual(result["stats"]["rate"], .5)
        self.assertEqual(result["stats"]["coverage"], 1)
        reviews = self.store.reviews({"exclude_spam": ["1"]})
        self.assertEqual(reviews["total"], 2)
        january = self.store.overview({"from": ["2022-01"], "to": ["2022-01"]})
        self.assertEqual(january["stats"]["reviews"], 3)

    def test_unaccented_search_and_aspect_drilldown(self):
        result = self.store.reviews({"q": ["dang nhap"], "aspect": ["app"]})
        self.assertEqual(result["total"], 1)
        self.assertEqual(result["rows"][0]["review_key"], "r3")
        self.assertEqual(result["rows"][0]["aspects"], ["app"])

    def test_invalid_filter_and_formula_export(self):
        with self.assertRaises(ValueError):
            self.store.reviews({"platforms": ["Shopee');DROP TABLE reviews;--"]})
        with self.assertRaises(ValueError):
            self.store.overview({"from": ["2022-02"], "to": ["2022-01"]})
        self.assertTrue(csv_safe("=HYPERLINK('bad')").startswith("'"))
        self.assertLess(self.store.reviews({"q": ['" OR 1=1 --']})["total"], 5)

    def test_mixed_generations_are_rejected(self):
        changed = {**self.meta, "generation": "new-not-finished"}
        (self.root / "dashboard_preparation.json").write_text(json.dumps(changed), encoding="utf-8")
        with self.assertRaises(RuntimeError):
            self.store.snapshot()

    def test_failed_rebuild_keeps_the_previous_index(self):
        with self.assertRaises(ValueError):
            build(iter(self.records[:-1]), self.root / "review_search.sqlite", self.meta)
        self.assertEqual(self.store.reviews({"score": ["all"]})["total"], 5)

    def test_serving_cache_refreshes_only_after_coherent_generation(self):
        cached = Store(self.root, self.root / "cache")
        self.assertEqual(cached.reviews({"q": ["dang nhap"]})["total"], 1)
        self.assertEqual(cached.index_path.parent, self.root / "cache")
        next_meta = {**self.meta, "generation": "fixture-generation-2"}
        (self.root / "dashboard_preparation.json").write_text(json.dumps(next_meta), encoding="utf-8")
        with self.assertRaises(RuntimeError):
            cached.snapshot()
        self.assertEqual(cached.index_metadata(cached.index_path)["generation"], self.meta["generation"])
        build(iter(self.records), self.root / "review_search.sqlite", next_meta)
        cube_path = self.root / "dashboard_cube.csv"
        cube_path.write_text(cube_path.read_text(encoding="utf-8").replace(self.meta["generation"], next_meta["generation"]), encoding="utf-8")
        self.assertEqual(cached.reviews({"score": ["all"]})["total"], 5)
        self.assertEqual(cached.index_metadata(cached.index_path)["generation"], next_meta["generation"])


if __name__ == "__main__":
    unittest.main()
