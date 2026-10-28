from __future__ import annotations

import argparse
import sys
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1] / "src"))
from review_analytics.sentiment import analyze_review
from contextlib import closing
import csv
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import tempfile
import threading
import unicodedata
import uuid
from review_analytics.io import atomic_write_text
from urllib.parse import parse_qs, urlsplit

PLATFORMS = ["Shopee", "Lazada", "Sendo", "Tiki"]
WEB = Path(__file__).resolve().parent / "web"


def read_json(path, fallback=None):
    return json.loads(path.read_text(encoding="utf-8-sig")) if path.exists() else fallback


def query_text(value):
    value = unicodedata.normalize("NFKD", value.lower().replace("đ", "d"))
    value = "".join(c for c in value if unicodedata.category(c) != "Mn")
    terms = re.findall(r"[^\W_]+", value, re.UNICODE)[:16]
    return " AND ".join('"' + term + '"' for term in terms)


def csv_safe(value):
    value = str(value)
    return "'" + value if value.lstrip().startswith(("=", "+", "-", "@")) else value


class Store:
    def __init__(self, root, cache=None):
        self.root = Path(root)
        self.cache = Path(cache) if cache else None
        self.index_path = self.root / "review_search.sqlite"
        self.lock = threading.Lock()
        self.key, self.cube, self.metadata = None, None, None

    def connect(self, path=None):
        connection = sqlite3.connect((path or self.index_path).resolve().as_uri() + "?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        return connection

    def index_metadata(self, path):
        with closing(self.connect(path)) as connection:
            return json.loads(connection.execute("SELECT value FROM metadata WHERE key='snapshot'").fetchone()[0])

    def cache_index(self, generation):
        # Docker Desktop bind mounts make random SQLite reads expensive. Keep an
        # immutable serving copy on the Linux volume; HDFS-derived exports remain
        # the source of truth. Replace only after the copied generation is checked.
        if not self.cache:
            return
        self.cache.mkdir(parents=True, exist_ok=True)
        target = self.cache / "review_search.sqlite"
        if target.exists():
            try:
                if self.index_metadata(target)["generation"] == generation:
                    self.index_path = target
                    return
            except (sqlite3.Error, KeyError, ValueError):
                pass
        descriptor, name = tempfile.mkstemp(prefix="review-index-", suffix=".sqlite", dir=self.cache)
        os.close(descriptor)
        temporary = Path(name)
        try:
            shutil.copyfile(self.root / "review_search.sqlite", temporary)
            if self.index_metadata(temporary)["generation"] != generation:
                raise RuntimeError("Chỉ mục đang đổi phiên dữ liệu. Vui lòng thử lại sau.")
            os.replace(temporary, target)
            self.index_path = target
        finally:
            temporary.unlink(missing_ok=True)

    def snapshot(self):
        with self.lock:
            metadata = read_json(self.root / "dashboard_preparation.json")
            if not metadata or not (self.root / "review_search.sqlite").exists():
                raise RuntimeError("Chưa có snapshot dashboard. Cần hoàn tất bước chuẩn bị dữ liệu và chỉ mục.")
            key = metadata["generation"]
            if self.key != key:
                index = self.index_metadata(self.root / "review_search.sqlite")
                if index["generation"] != key:
                    raise RuntimeError("Snapshot đang được cập nhật. Vui lòng thử lại sau khi job hoàn tất.")
                with (self.root / "dashboard_cube.csv").open(encoding="utf-8-sig", newline="") as handle:
                    rows = list(csv.DictReader(handle))
                if any(row["generation"] != key for row in rows):
                    raise RuntimeError("Các tệp tổng hợp chưa cùng phiên dữ liệu.")
                for row in rows:
                    for field in ["reviews", "score", "unmatched_reviews"] + ["aspect_" + a["code"] for a in metadata["aspects"]]:
                        row[field] = int(row[field])
                    row["spam_candidate"] = row["spam_candidate"].lower() == "true"
                if sum(row["reviews"] for row in rows) != index["indexed_rows"]:
                    raise RuntimeError("Số dòng trong biểu đồ và chỉ mục không khớp.")
                self.cache_index(key)
                self.key, self.cube, self.metadata = key, rows, {**metadata, "indexed_rows": index["indexed_rows"]}
            return self.metadata, self.cube

    def filters(self, params, metadata):
        platforms = params.get("platforms", [",".join(PLATFORMS)])[0].split(",")
        if not platforms or any(platform not in PLATFORMS for platform in platforms):
            raise ValueError("Chọn ít nhất một sàn hợp lệ.")
        start = params.get("from", [metadata["month_min"]])[0]
        end = params.get("to", [metadata["month_max"]])[0]
        if (not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", start)
                or not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", end)
                or not metadata["month_min"] <= start <= end <= metadata["month_max"]):
            raise ValueError("Khoảng tháng phải nằm trong phạm vi dữ liệu và từ tháng không lớn hơn đến tháng.")
        return {"platforms": list(dict.fromkeys(platforms)), "from": start, "to": end,
            "exclude_spam": params.get("exclude_spam", ["0"])[0] == "1"}

    def overview(self, params):
        metadata, cube = self.snapshot()
        filters = self.filters(params, metadata)
        selected = [row for row in cube if row["platform"] in filters["platforms"]
            and filters["from"] <= row["month"] <= filters["to"]
            and not (filters["exclude_spam"] and row["spam_candidate"])]
        by_platform = {p: {"platform": p, "reviews": 0, "low": 0, "rating_sum": 0,
            "unmatched": 0, "ratings": [0] * 5, "aspects": {a["code"]: 0 for a in metadata["aspects"]}}
            for p in filters["platforms"]}
        series = {}
        for row in selected:
            platform = by_platform[row["platform"]]
            platform["reviews"] += row["reviews"]
            platform["rating_sum"] += row["reviews"] * row["score"]
            platform["ratings"][row["score"] - 1] += row["reviews"]
            point = series.setdefault((row["platform"], row["month"]),
                {"platform": row["platform"], "month": row["month"], "reviews": 0, "low": 0})
            point["reviews"] += row["reviews"]
            if row["score"] <= 2:
                platform["low"] += row["reviews"]
                platform["unmatched"] += row["unmatched_reviews"]
                point["low"] += row["reviews"]
                for aspect in metadata["aspects"]:
                    platform["aspects"][aspect["code"]] += row["aspect_" + aspect["code"]]
        platforms = list(by_platform.values())
        for platform in platforms:
            n = platform["reviews"]
            platform["rate"] = platform["low"] / n if n else None
            platform["mean"] = platform["rating_sum"] / n if n else None
        total = sum(p["reviews"] for p in platforms)
        low = sum(p["low"] for p in platforms)
        unmatched = sum(p["unmatched"] for p in platforms)
        return {"filters": filters, "stats": {"reviews": total, "low": low,
            "rate": low / total if total else None,
            "mean": sum(p["rating_sum"] for p in platforms) / total if total else None,
            "unmatched": unmatched, "coverage": (low - unmatched) / low if low else None},
            "platforms": platforms, "series": sorted(series.values(), key=lambda r: (r["month"], r["platform"])),
            "aspects": [{**aspect, "mentions": sum(p["aspects"][aspect["code"]] for p in platforms),
                "denominator": low} for aspect in metadata["aspects"]]}

    def review_sources(self):
        return [("main", None)]

    def reviews(self, params, export=False):
        metadata, _ = self.snapshot()
        filters = self.filters(params, metadata)
        score = params.get("score", ["low"])[0]
        if score not in ["all", "low", "1", "2", "3", "4", "5"]:
            raise ValueError("Bộ lọc số sao không hợp lệ.")
        aspect = params.get("aspect", ["all"])[0]
        bits = {a["code"]: a["bit"] for a in metadata["aspects"]}
        if aspect not in ["all", "unmatched"] + list(bits):
            raise ValueError("Khía cạnh không hợp lệ.")
        raw_query = params.get("q", [""])[0][:160]
        query = query_text(raw_query)
        limit = 5000 if export else min(50, max(1, int(params.get("limit", ["20"])[0])))
        offset = 0 if export else max(0, min(2000000, int(params.get("offset", ["0"])[0])))
        conditions = ["r.platform IN (" + ",".join("?" for _ in filters["platforms"]) + ")",
            "r.month>=?", "r.month<=?"]
        values = [*filters["platforms"], filters["from"], filters["to"]]
        if filters["exclude_spam"]:
            conditions.append("r.spam=0")
        if score == "low":
            conditions.append("r.score<=2")
        elif score != "all":
            conditions.append("r.score=?")
            values.append(int(score))
        if aspect == "unmatched":
            conditions.append("r.aspect_mask=0")
        elif aspect != "all":
            conditions.append("(r.aspect_mask & ?)>0")
            values.append(bits[aspect])
        if query:
            conditions.append("review_fts MATCH ?")
            values.append(query)
        # Keep the selective text index first; otherwise SQLite may probe FTS for
        # every review selected by a broad platform/month filter.
        where = " WHERE " + " AND ".join(conditions)
        with closing(self.connect()) as connection:
            counts, branches, count_values, row_values = [], [], [], []
            for schema, superseded_by in self.review_sources():
                base = (f"{schema}.review_fts CROSS JOIN {schema}.reviews r ON r.id=review_fts.rowid"
                        if query else f"{schema}.reviews r")
                source_where = where
                if superseded_by:
                    source_where += f" AND NOT EXISTS (SELECT 1 FROM {superseded_by}.reviews newer WHERE newer.review_key=r.review_key)"
                counts.append("SELECT COUNT(*) AS n FROM " + base + source_where)
                count_values.extend(values)
                row_base, row_where, branch_values = base, source_where, list(values)
                if not query:
                    year, month = map(int, filters["to"].split("-"))
                    next_month = date(year + int(month == 12), 1 if month == 12 else month + 1, 1).isoformat()
                    row_base = f"{schema}.reviews r INDEXED BY review_date"
                    row_where += " AND r.at>=? AND r.at<?"
                    branch_values += [filters["from"] + "-01", next_month]
                branches.append("SELECT r.id,r.review_key,r.platform,r.month,r.at,r.score,r.content,r.spam,r.aspect_mask FROM " + row_base + row_where)
                row_values.extend(branch_values)
            total = connection.execute("SELECT SUM(n) FROM (" + " UNION ALL ".join(counts) + ")", count_values).fetchone()[0]
            records = connection.execute("SELECT review_key,platform,month,at,score,content,spam,aspect_mask FROM ("
                + " UNION ALL ".join(branches) + ") ORDER BY at DESC,id DESC,review_key DESC LIMIT ? OFFSET ?",
                [*row_values, limit, offset]).fetchall()
        rows = []
        for record in records:
            row = dict(record)
            row["aspects"] = [code for code, bit in bits.items() if row["aspect_mask"] & bit]
            del row["aspect_mask"]
            rows.append(row)
        return {"rows": rows, "total": total, "offset": offset, "limit": limit, "filters": filters}

    def bootstrap(self):
        metadata, _ = self.snapshot()
        streams = [read_json(path) for path in sorted(self.root.glob("stream_batch_*.json"))]
        return {"metadata": metadata, "platforms": PLATFORMS,
            "quality": read_json(self.root / "data_quality.json"),
            "evidence": read_json(self.root / "demo_evidence.json", {}),
            "validation": read_json(self.root / "persisted_validation.json", {}),
            "clusters": read_json(self.root / "cluster_metadata.json", {}), "stream_batches": streams,
            "query_plan": (self.root / "spark_query_plan.txt").read_text(encoding="utf-8")
                if (self.root / "spark_query_plan.txt").exists() else ""}


class CombinedStore(Store):
    """Federate verified HDFS exports; live versions supersede matching historical IDs."""
    def __init__(self, historical, live=None):
        super().__init__(historical.root)
        self.historical, self.live = historical, live
        self.overlap = 0

    def connect(self, path=None):
        connection = self.historical.connect()
        if self.live:
            connection.execute("ATTACH DATABASE ? AS live", (self.live.index_path.resolve().as_uri() + "?mode=ro",))
        return connection

    def review_sources(self):
        return [("main", "live" if self.overlap else None), ("live", None)] if self.live else [("main", None)]

    def snapshot(self):
        with self.lock:
            hm, hc = self.historical.snapshot()
            lm, lc = self.live.snapshot() if self.live else (None, [])
            key = hm["generation"] + ":" + (lm["generation"] if lm else "none")
            if self.key != key:
                if lm and hm["aspects"] != lm["aspects"]:
                    raise RuntimeError("Hai nguồn chưa dùng cùng schema khía cạnh.")
                cube = [dict(row) for row in hc + lc]
                historical_cells = {}
                for row in cube[:len(hc)]:
                    cell = (row["platform"], row["month"], row["score"], row["spam_candidate"])
                    historical_cells.setdefault(cell, []).append(row)
                self.overlap = 0
                if self.live:
                    with closing(self.connect()) as connection:
                        # Scan the small live index and probe historical unique keys.
                        old_rows = connection.execute("SELECT old.* FROM live.reviews newer CROSS JOIN main.reviews old ON old.review_key=newer.review_key").fetchall()
                    self.overlap = len(old_rows)
                    for old in old_rows:
                        cell = (old["platform"], old["month"], old["score"], bool(old["spam"]))
                        rows = historical_cells.get(cell, [])
                        row = next((r for r in rows if r["reviews"] > 0), None)
                        if row is None:
                            raise RuntimeError("Không đối soát được review trùng trong tổng hợp lịch sử.")
                        row["reviews"] -= 1
                        row["unmatched_reviews"] -= int(old["aspect_mask"] == 0)
                        for aspect in hm["aspects"]:
                            row["aspect_" + aspect["code"]] -= int(bool(old["aspect_mask"] & aspect["bit"]))
                cube = [row for row in cube if row["reviews"]]
                count = hm["processed_rows"] + (lm["processed_rows"] if lm else 0) - self.overlap
                if sum(row["reviews"] for row in cube) != count:
                    raise RuntimeError("Tổng dữ liệu gộp không khớp hai chỉ mục nguồn.")
                self.metadata = {**hm, "generation": key, "source_mode": "historical_and_live",
                    "processed_rows": count, "indexed_rows": count, "cube_rows": len(cube),
                    "month_min": min(hm["month_min"], lm["month_min"]) if lm else hm["month_min"],
                    "month_max": max(hm["month_max"], lm["month_max"]) if lm else hm["month_max"],
                    "prepared_utc": max(hm["prepared_utc"], lm["prepared_utc"]) if lm else hm["prepared_utc"],
                    "historical_rows": hm["processed_rows"], "live_rows": lm["processed_rows"] if lm else 0,
                    "overlapping_review_ids": self.overlap,
                    "component_generations": {"historical": hm["generation"], "live": lm["generation"] if lm else None},
                    "hdfs_serving_sources": [hm["hdfs_serving_source"]] + ([lm["hdfs_serving_source"]] if lm else [])}
                self.key, self.cube = key, cube
            return self.metadata, self.cube

    def bootstrap(self):
        data = self.historical.bootstrap()
        metadata, _ = self.snapshot()
        qualities = [data["quality"]] + ([self.live.bootstrap()["quality"]] if self.live else [])
        data.update({"metadata": metadata, "clusters": {}, "quality": {
            "raw_rows": sum(q["raw_rows"] for q in qualities),
            "quarantined_rows": sum(q["quarantined_rows"] for q in qualities),
            "duplicate_valid_id_rows_removed": sum(q["duplicate_valid_id_rows_removed"] for q in qualities) + self.overlap,
            "clean_rows": metadata["processed_rows"]}})
        return data


def handler_class(store, replay_control=None):
    live_root = store.root / 'live'
    replay_root = store.root / 'replay'
    control_root = Path(replay_control) if replay_control else replay_root
    live_stores = {}
    combined_stores = {}
    source_lock = threading.Lock()

    def choose_source(params):
        pointer = read_json(live_root / 'current.json', {})
        generation = pointer.get('generation', '')
        valid = bool(re.fullmatch(r'[0-9a-f]{32}', generation))
        folder = live_root / 'generations' / generation if valid else None
        ready = bool(folder and (folder / 'dashboard_preparation.json').exists() and (folder / 'review_search.sqlite').exists())
        source = params.get('source', ['all'])[0]
        if source not in ['all', 'live', 'historical']:
            raise ValueError('Nguồn dữ liệu không hợp lệ.')
        if source == 'live' and not ready:
            raise RuntimeError('Chưa có kết quả thu thập thật đã kiểm chứng. Pipeline đang chuẩn bị dữ liệu.')
        if source in ['all', 'live'] and ready:
            with source_lock:
                if generation not in live_stores:
                    cache = store.cache / 'live' / generation if store.cache else None
                    live_stores.clear()
                    live_stores[generation] = Store(folder, cache)
                active = live_stores[generation]
        else:
            active = store
        if source == 'all':
            with source_lock:
                key = generation if ready else 'historical-only'
                if key not in combined_stores:
                    combined_stores.clear()
                    combined_stores[key] = CombinedStore(store, active if ready else None)
                active = combined_stores[key]
        return source, active, {'all': {'ready': True}, 'historical': {'ready': True}, 'live': {'ready': ready}}

    class Handler(BaseHTTPRequestHandler):
        def respond(self, body, content_type="application/json; charset=utf-8", status=200, download=None):
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            if download:
                self.send_header("Content-Disposition", 'attachment; filename="' + download + '"')
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            if urlsplit(self.path).path == '/api/replay/control':
                try:
                    origin = self.headers.get('Origin')
                    if origin and urlsplit(origin).netloc != self.headers.get('Host'):
                        raise ValueError('Nguồn yêu cầu không hợp lệ.')
                    length = int(self.headers.get('Content-Length', '0'))
                    if not 0 < length <= 1024:
                        raise ValueError('Yêu cầu điều khiển không hợp lệ.')
                    payload = json.loads(self.rfile.read(length))
                    action = payload.get('action')
                    speed = payload.get('interval_seconds', 4)
                    if action not in ['reset', 'pause', 'resume', 'speed'] or type(speed) is not int or speed not in [2, 4, 8]:
                        raise ValueError('Chọn thao tác và tốc độ hợp lệ.')
                    request = {'request_id': uuid.uuid4().hex, 'action': action, 'interval_seconds': speed}
                    atomic_write_text(control_root / 'control.json', json.dumps(request))
                    return self.respond(json.dumps({'accepted': True, **request}).encode(), status=202)
                except (ValueError, json.JSONDecodeError) as error:
                    return self.respond(json.dumps({'error': str(error)}, ensure_ascii=False).encode(), status=400)
            if urlsplit(self.path).path != '/api/predict':
                return self.respond(b'{"error":"Not found"}', status=404)
            try:
                length = int(self.headers.get('Content-Length', '0'))
                if not 0 < length <= 24000:
                    raise ValueError('Nội dung gửi quá dài hoặc rỗng.')
                payload = json.loads(self.rfile.read(length))
                text = payload.get('review')
                if not isinstance(text,str) or not text.strip() or len(text)>5000:
                    raise ValueError('Nhập review từ 1 đến 5.000 ký tự.')
                model = read_json(store.root / 'sentiment_model.json')
                if not model:
                    raise RuntimeError('Cần chạy jobs/train_sentiment.py để tạo mô hình trước.')
                aspects = read_json(WEB.parents[1] / 'config/aspects.json')
                result = analyze_review(text, model, aspects)
                self.respond(json.dumps(result,ensure_ascii=False).encode())
            except (ValueError, json.JSONDecodeError) as error:
                self.respond(json.dumps({'error':str(error)},ensure_ascii=False).encode(),status=400)
            except (RuntimeError,FileNotFoundError) as error:
                self.respond(json.dumps({'error':str(error)},ensure_ascii=False).encode(),status=503)

        def do_GET(self):
            parsed = urlsplit(self.path)
            params = parse_qs(parsed.query, keep_blank_values=True)
            try:
                source, active, sources = choose_source(params)
                if parsed.path == "/api/replay":
                    data = read_json(replay_root / 'status.json', {'phase': 'preparing', 'frames': [], 'loaded_reviews': 0})
                elif parsed.path == "/api/live-timeline":
                    data = {'source_mode': 'google_play_live', 'points': read_json(live_root / 'observation_timeline.json', [])}
                elif parsed.path == "/api/health":
                    active.snapshot()
                    data = {"status": "ready"}
                elif parsed.path == "/api/bootstrap":
                    data = active.bootstrap()
                    data.update({'source': source, 'sources': sources})
                    if source in ['all', 'live']:
                        data['collection'] = read_json(live_root / 'collection_status.json', {})
                        data['evidence'] = read_json(live_root / 'dag_evidence.json', {})
                        data['stream_batches'] = [read_json(p) for p in sorted(live_root.glob('stream_batch_*.json'))]
                elif parsed.path == "/api/overview":
                    data = active.overview(params)
                elif parsed.path == "/api/reviews":
                    data = active.reviews(params)
                elif parsed.path in ["/api/export.csv", "/api/export-reviews.csv"]:
                    stream = io.StringIO(newline="")
                    writer = csv.writer(stream)
                    if parsed.path == "/api/export.csv":
                        data = active.overview(params)
                        writer.writerow(["platform", "from_month", "to_month", "reviews", "low_rating_reviews", "low_rating_rate", "mean_rating"])
                        for p in data["platforms"]:
                            writer.writerow([p["platform"], data["filters"]["from"], data["filters"]["to"],
                                p["reviews"], p["low"], p["rate"], p["mean"]])
                    else:
                        data = active.reviews(params, export=True)
                        writer.writerow(["review_key", "platform", "at", "score", "aspects_mentioned", "spam_candidate", "content"])
                        for row in data["rows"]:
                            writer.writerow([row["review_key"], row["platform"], row["at"], row["score"],
                                "|".join(row["aspects"]), row["spam"], csv_safe(row["content"])])
                    return self.respond(("\ufeff" + stream.getvalue()).encode("utf-8"), "text/csv; charset=utf-8", download=parsed.path.split("/")[-1])
                elif parsed.path in ["/", "/app.js", "/styles.css", "/demo.html", "/replay.js"]:
                    filename = "index.html" if parsed.path == "/" else parsed.path[1:]
                    mime = {"index.html": "text/html", "app.js": "text/javascript", "styles.css": "text/css", "demo.html": "text/html", "replay.js": "text/javascript"}[filename]
                    return self.respond((WEB / filename).read_bytes(), mime + "; charset=utf-8")
                else:
                    return self.respond(b'{"error":"Not found"}', status=404)
                self.respond(json.dumps(data, ensure_ascii=False).encode("utf-8"))
            except ValueError as error:
                self.respond(json.dumps({"error": str(error)}, ensure_ascii=False).encode(), status=400)
            except (RuntimeError, FileNotFoundError) as error:
                self.respond(json.dumps({"error": str(error)}, ensure_ascii=False).encode(), status=503)
            except (sqlite3.Error, KeyError) as error:
                print(f"Dashboard data error: {error}", flush=True)
                self.respond('{"error":"Không đọc được snapshot dữ liệu. Kiểm tra job tạo dashboard."}'.encode(), status=503)
    return Handler


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument("--outputs", type=Path, default=Path(__file__).resolve().parents[1] / "outputs")
    parser.add_argument("--cache", type=Path, help="Optional local serving cache, useful with Docker Desktop bind mounts")
    parser.add_argument("--replay-control", type=Path, help="Writable directory for isolated replay controls")
    args = parser.parse_args()
    server = ThreadingHTTPServer((args.host, args.port), handler_class(Store(args.outputs, args.cache), args.replay_control))
    print(f"ReviewLens ready at http://{args.host}:{args.port}", flush=True)
    server.serve_forever()
