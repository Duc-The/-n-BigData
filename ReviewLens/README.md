# ReviewLens — phân tích review thương mại điện tử

HDFS lưu dữ liệu, Spark DataFrame/SQL và ML xử lý, Kafka truyền review Google Play mới, Airflow điều phối. Dashboard đọc bảng tổng hợp và chỉ mục được xuất từ pipeline.

Nguồn lịch sử: [Vietnamese E-commerce Review trên Kaggle](https://www.kaggle.com/datasets/hienbm/vietnamese-ecommerce-review), 1.300.086 review ứng dụng Shopee, Lazada, Sendo, Tiki; 642.967.981 byte, thời gian 06/2013–08/2022. Đây là review ứng dụng Google Play, chưa phải review mua hàng đã xác nhận.

## Cài đặt và chạy lần đầu

Chạy trong **Ubuntu WSL**, tại thư mục mã nguồn đã giải nén. Cần Docker Engine, Docker Compose V2 và Python 3. Hadoop, Spark, Kafka và Airflow chạy trong Docker.

```bash
sudo apt-get update
sudo apt-get install -y docker.io docker-compose-v2 docker-buildx git
sudo systemctl start docker
sudo usermod -aG docker "$USER"
# Mở lại terminal sau khi thêm nhóm docker, rồi cd vào thư mục dự án.
python3 scripts/download_dataset.py
python3 scripts/init_local_env.py
bash scripts/run_pipeline.sh
bash scripts/start_live.sh
bash scripts/start_replay.sh
```

Script tải giữ nguyên CSV đã có và kiểm SHA-256. Pipeline thực hiện audit, upload HDFS, làm sạch, SQL, ML, tạo dashboard/index và kiểm chứng Kafka replay. Máy mới cần tải dataset và image Docker; ZIP mã nguồn không chứa chúng. Cấu hình low-memory giới hạn hai Spark worker ở 1 core/1 GiB mỗi worker.

## Mở lại trên máy đã có dữ liệu và môi trường

```bash
bash scripts/start_live.sh
bash scripts/start_replay.sh
```

Không cần chạy lại toàn bộ batch chỉ để mở dashboard. DAG live thu thập mỗi **5 phút**; khi không có review mới/sửa thì số liệu giữ nguyên. Bộ lọc tự áp dụng, giao diện tự đọc snapshot mới. Replay lịch sử nạp từng lô tháng và hiển thị cửa sổ trượt 12 mốc; đây không phải review mới thật.

| Giao diện | Địa chỉ |
| --- | --- |
| Dashboard | http://localhost:8090 |
| Đồ thị nạp dần | http://localhost:8090/?source=all#replay |
| Demo mô hình | http://localhost:8090/demo.html |
| HDFS | http://localhost:9870 |
| Spark master | http://localhost:8081 |
| Airflow | http://localhost:8080 |

Airflow dùng username `airflow`; mật khẩu lấy từ `AIRFLOW_ADMIN_PASSWORD` trong `.env` tại máy. Máy mới chạy `init_local_env.py` để sinh cấu hình. Không chia sẻ `.env`.

## Kiểm tra hệ thống

```bash
docker compose --profile orchestration --profile dashboard ps
docker compose exec -T spark-client hdfs dfs -ls -h /ecommerce/raw
docker compose exec -T spark-client hdfs dfsadmin -report
docker compose exec -T spark-client hdfs fsck /ecommerce/raw/reviews.csv -files -blocks
docker compose exec -T airflow-scheduler airflow dags list-import-errors
curl -fsS http://localhost:8090/api/health
docker compose exec -T spark-client python -m unittest discover -s tests -v
docker compose exec -T spark-client python scripts/verify_outputs.py --require-stream
docker compose exec -T spark-client python scripts/validate_dashboard.py --outputs /exports
```

Dùng lệnh đầy đủ `docker compose` để tránh nhầm `dc` với chương trình GNU dc. DAG batch `ecommerce_dissatisfaction_pipeline` chạy theo yêu cầu; DAG live `live_google_play_reviews` có bốn task. Chọn đúng run và task log trong Airflow để kiểm tra.

## Cấu trúc cần giữ khi chạy

- `jobs/`, `src/`, `config/`: xử lý Spark, Kafka, quy tắc khía cạnh và mô hình.
- `dags/`, `infra/`, các file Compose: điều phối và môi trường Docker.
- `dashboard/`, `scripts/`, `tests/`: giao diện, công cụ chạy và kiểm thử.
- `vietnamese_ecommerce_review.csv`, `artifacts/audit/`, `outputs/`, `.env`: dữ liệu và cấu hình cục bộ. Dashboard cần cube, model, chỉ mục trong `outputs/`; không xóa cả thư mục nếu muốn demo ngay.
- `.git/`: lịch sử phiên bản, giữ trên máy và loại khỏi ZIP.

HDFS, checkpoint, Kafka và database Airflow còn được lưu trong Docker volumes. Dừng mà giữ dữ liệu:

```bash
docker compose -f compose.yaml -f compose.low-memory.yaml --profile orchestration --profile streaming --profile dashboard stop
```

Không dùng `down -v` nếu cần giữ volumes. Clear bộ lọc không xóa dữ liệu. Nguồn `all` gộp lịch sử và live theo khóa review, ưu tiên phiên bản live khi trùng khóa.

## Đóng ZIP mã nguồn dưới 20 MB

```bash
python3 scripts/package_submission.py
```

Kết quả: **`submission/ReviewLens_Source.zip`**, gồm code, cấu hình, tests, README và manifest SHA-256. Script kiểm tính toàn vẹn, bí mật cục bộ và giới hạn **20.000.000 byte**; không cần PDF/Word hay Git để đóng gói.

ZIP loại CSV gốc, SQLite, `outputs/`, `.env`, Docker volumes, báo cáo và ảnh minh chứng. Thư mục làm việc vẫn lớn do giữ dữ liệu demo: gửi ZIP do script tạo, không nén toàn bộ thư mục. Máy nhận ZIP tải dữ liệu và dựng lại pipeline theo phần cài đặt.

`scripts/check_submission.py` kiểm đối chiếu kết quả và log kỹ thuật sau khi pipeline và các lượt kiểm chứng hoàn thành; không phụ thuộc báo cáo. Các script kiểm thử trình duyệt dùng image QA `infra/Dockerfile.qa` khi chạy riêng.

## Phương pháp và giới hạn

Spark đọc CSV trực tiếp từ `hdfs://namenode:9000/ecommerce/raw/reviews.csv`, chuẩn hóa Unicode, kiểm tra dữ liệu, cách ly record lỗi, khử trùng theo app/review ID và ghi Parquet. Snapshot lịch sử có 74 record cách ly, loại 139 bản sao ID, còn 1.299.873 review; tổng dòng được đối soát.

Bảy khía cạnh là luật nhận diện nội dung được đề cập. TF-IDF âm tiết unigram/bigram kết hợp Logistic Regression/LinearSVC dự đoán nhóm sao: 1–2 sao là nhãn 1, 4–5 sao là nhãn 0, bỏ sao 3. Chia tập theo nhóm nội dung; fit vocabulary/IDF trên train, chọn cấu hình bằng validation và đánh giá test. K-Means hỗ trợ khám phá. Chưa có nhãn khía cạnh do người đọc độc lập xác nhận; nhãn sao là nhãn thay thế và chưa đủ để kết luận nguyên nhân bất mãn.
