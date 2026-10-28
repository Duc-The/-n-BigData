# Nhóm 10 – Đồ án cuối kỳ Big Data

## ReviewLens: Phân tích đánh giá ứng dụng thương mại điện tử

ReviewLens xây dựng pipeline dùng **Hadoop/HDFS, Apache Spark, Kafka và Airflow** để xử lý đánh giá tiếng Việt của các ứng dụng Shopee, Lazada, Sendo và Tiki. Hệ thống phân tích số lượng đánh giá, phân bố sao, xu hướng theo thời gian, các khía cạnh được nhắc đến và huấn luyện mô hình dự đoán nhóm đánh giá dựa trên số sao. Dashboard hiển thị đầu ra do pipeline tạo ra.

Mã nguồn nằm trong thư mục [`ReviewLens/`](ReviewLens/). Tất cả lệnh bên dưới được chạy **bên trong thư mục này**, trừ bước chuyển thư mục ban đầu.

> Thư mục bàn giao hiện chứa mã nguồn và cấu hình; chưa kèm CSV gốc, output, log thực thi hoặc báo cáo. Các đường dẫn kết quả bên dưới là nơi pipeline sẽ tạo tệp sau khi chạy thành công. README không thay thế minh chứng thực thi.

## 1. Dữ liệu và phạm vi bài toán

- Nguồn lịch sử: [Vietnamese E-commerce Review trên Kaggle](https://www.kaggle.com/datasets/hienbm/vietnamese-ecommerce-review).
- Theo tài liệu đi kèm mã nguồn: **1.300.086 bản ghi**, **642.967.981 byte**, giai đoạn **06/2013–08/2022**. Đối chiếu lại bằng kết quả audit khi chạy.
- Định dạng: CSV UTF-8, có trường văn bản nhiều dòng. Không dùng số dòng vật lý của tệp để thay cho số bản ghi CSV.
- Nguồn live: đánh giá công khai trên Google Play, được DAG thu thập định kỳ 5 phút.
- Đây là đánh giá **ứng dụng**, không phải đánh giá đơn hàng đã xác nhận hoặc dữ liệu giao dịch mua bán.

Các trường CSV được ánh xạ trong `src/review_analytics/spark_ops.py`:

| Trường | Ý nghĩa / cách xử lý |
| --- | --- |
| `csv_index` | Cột chỉ mục của CSV, được ánh xạ theo vị trí khi đọc |
| `reviewid` | Mã đánh giá |
| `username`, `userimage` | Thông tin người đánh giá trong dữ liệu gốc |
| `content` | Nội dung đánh giá |
| `score` | Số sao, chuyển sang số nguyên và kiểm tra trong khoảng 1–5 |
| `thumbsupcount` | Số lượt đánh giá hữu ích |
| `reviewcreatedversion` | Phiên bản ứng dụng |
| `at` | Thời điểm đánh giá, chuyển sang timestamp |
| `replycontent`, `repliedat` | Nội dung và thời điểm phản hồi |
| `appid` | Mã ứng dụng để ánh xạ sang nền tảng |

Spark đọc ban đầu dưới dạng chuỗi để kiểm tra và ép kiểu. Pipeline chuẩn hóa Unicode, kiểm tra trường bắt buộc, cách ly bản ghi lỗi, khử trùng theo `(appid, reviewid)` và lưu dữ liệu sạch dưới dạng Parquet.

## 2. Kiến trúc xử lý

```text
CSV lịch sử → audit → HDFS raw → Spark làm sạch → HDFS Parquet
                                              ↓
                                  Spark SQL / K-Means / ML
                                              ↓
                                     outputs → Dashboard

Google Play → Collector → Kafka → Spark Structured Streaming → HDFS
                                                                  ↓
                                                    xuất snapshot → Dashboard

Airflow: điều phối DAG batch, thu thập live và chuẩn bị replay lịch sử.
```

Dữ liệu batch được đưa vào HDFS trước khi Spark đọc trực tiếp tại:

```text
hdfs://namenode:9000/ecommerce/raw/reviews.csv
```

Kafka có hai chế độ cần phân biệt:

- **Historical replay**: phát lại mẫu dữ liệu lịch sử vào topic `ecommerce-reviews` để kiểm chứng producer/consumer.
- **Live**: thu thập review Google Play qua topic `ecommerce-live-reviews`, sau đó Spark ghi vào `hdfs://namenode:9000/ecommerce/live`.

Chức năng replay theo tháng trên dashboard là phát lại lịch sử, không phải đánh giá mới được thu thập tại thời điểm demo.

## 3. Công nghệ và môi trường

Các phiên bản được khai báo trong Dockerfile/Compose:

| Thành phần | Phiên bản |
| --- | --- |
| Hadoop | 3.3.6 |
| Spark | 3.5.7, Scala 2.12 |
| Kafka | 3.9.0 |
| Airflow | 2.10.5 |
| PostgreSQL | 16.6 |
| Python runtime | 3.11 |
| Java runtime | 11 |

Môi trường chạy: **Ubuntu/Linux hoặc Ubuntu trong WSL2**, Python 3, Docker Engine hoặc Docker Desktop tích hợp WSL2, Docker Compose V2 và Bash. Cần Internet để tải dataset, image Docker và gói Spark Kafka connector ở lần chạy đầu. Chạy các script `.sh` trong terminal Ubuntu/WSL.

Các script sử dụng thêm `compose.low-memory.yaml`, giới hạn mỗi Spark worker ở 1 core/1 GiB. Toàn bộ stack còn có HDFS, Kafka, Airflow và database; cần cấp bộ nhớ Docker phù hợp. Dung lượng sau khi chạy sẽ lớn hơn CSV gốc vì có image, Parquet, model và log.

## 4. Cài đặt và chạy lần đầu

Nếu đang ở terminal WSL, chuyển tới mã nguồn trên máy hiện tại:

```bash
cd /mnt/c/Users/phand/Downloads/Nhom10_DoAnCuoiKy/ReviewLens
```

Nếu clone repository sang máy khác, chuyển tới thư mục `ReviewLens` tương ứng. Kiểm tra công cụ:

```bash
python3 --version
docker --version
docker compose version
docker info
```

Tải dữ liệu, sinh cấu hình cục bộ và chạy batch:

```bash
python3 scripts/download_dataset.py
python3 scripts/init_local_env.py
bash scripts/run_pipeline.sh
```

Script tải dữ liệu kiểm SHA-256 và giữ nguyên CSV đã có nếu khớp. `init_local_env.py` sinh `.env` và không ghi đè cấu hình hiện hữu. `run_pipeline.sh` thực hiện audit, build image, dựng HDFS/Spark, chạy unit test, upload HDFS, xử lý batch, huấn luyện ML, tạo dữ liệu dashboard và kiểm chứng Kafka replay.

Sau khi batch thành công, khởi động Airflow và thu thập live; tiếp đó bật replay theo tháng nếu cần demo:

```bash
bash scripts/start_live.sh
bash scripts/start_replay.sh
```

`start_live.sh` khởi tạo database Airflow và bật DAG live. `start_replay.sh` yêu cầu core stack/Airflow đã chạy và kích hoạt DAG chuẩn bị dữ liệu replay.

## 5. Truy cập và sử dụng

| Dịch vụ | Địa chỉ trên máy chạy Docker |
| --- | --- |
| Dashboard | [http://localhost:8090](http://localhost:8090) |
| Replay lịch sử theo tháng | [http://localhost:8090/?source=all#replay](http://localhost:8090/?source=all#replay) |
| Demo dự đoán review | [http://localhost:8090/demo.html](http://localhost:8090/demo.html) |
| HDFS NameNode | [http://localhost:9870](http://localhost:9870) |
| Spark master | [http://localhost:8081](http://localhost:8081) |
| Airflow | [http://localhost:8080](http://localhost:8080) |

Airflow dùng tài khoản `airflow`; mật khẩu lấy từ biến `AIRFLOW_ADMIN_PASSWORD` trong `.env` trên máy chạy. Không đưa `.env` lên GitHub hoặc vào hồ sơ nộp.

Dashboard đọc dữ liệu đã được xuất vào `outputs/`, được mount thành `/exports` trong container. Vì vậy cần hoàn thành pipeline để có bảng tổng hợp, chỉ mục và model. Khi nguồn Google Play không có review mới hoặc được sửa, số liệu live có thể giữ nguyên.

Có thể demo dự đoán bằng dòng lệnh sau khi đã tạo model:

```bash
python3 scripts/demo_review.py "Ứng dụng hay bị lỗi khi thanh toán"
```

## 6. DAG Airflow

| DAG ID | Chức năng |
| --- | --- |
| `ecommerce_dissatisfaction_pipeline` | Chạy theo yêu cầu: chuẩn bị/upload HDFS → Spark đọc/làm sạch/SQL → K-Means → ML → xuất kết quả → Kafka replay → kiểm tra |
| `live_google_play_reviews` | Mỗi 5 phút: thu thập → Kafka vào HDFS → xuất snapshot → kiểm tra |
| `prepare_historical_month_replay` | Chuẩn bị dữ liệu cho chức năng replay lịch sử |

Sau bước khởi động Airflow, chạy batch qua DAG để lấy log điều phối:

```bash
docker compose exec -T airflow-scheduler airflow dags list-import-errors
docker compose exec -T airflow-scheduler airflow dags unpause ecommerce_dissatisfaction_pipeline
docker compose exec -T airflow-scheduler airflow dags trigger ecommerce_dissatisfaction_pipeline
```

Theo dõi đúng run ID và từng task trong giao diện Airflow. Lệnh trigger chỉ tạo lượt chạy; cần đợi trạng thái hoàn thành và lưu log. `run_pipeline.sh` chạy batch trực tiếp, không tự tạo minh chứng lượt chạy DAG batch trong Airflow.

## 7. Phân tích dữ liệu và Machine Learning

- Spark SQL/DataFrame tạo các bảng tổng hợp theo nền tảng, tháng, số sao và độ dài nội dung; nhận diện khía cạnh được nhắc đến bằng luật trong `config/aspects.json`.
- K-Means hỗ trợ khám phá nhóm đánh giá.
- Mô hình phân loại so sánh Logistic Regression và LinearSVC với TF-IDF unigram/bigram âm tiết tiếng Việt.
- Nhãn thay thế: 1–2 sao là nhóm tiêu cực (`1`), 4–5 sao là nhóm tích cực (`0`); bỏ đánh giá 3 sao.
- Mặc định lấy mẫu hash 5% theo nhóm nội dung chuẩn hóa; chia train/validation/test bằng hash theo tỷ lệ mục tiêu 70/10/20. Tỷ lệ thực tế được ghi trong output. Vocabulary và IDF chỉ fit trên train.
- Đánh giá bằng accuracy, precision/recall/F1 lớp tiêu cực, macro-F1 và confusion matrix; có baseline lớp đa số.

Xem số liệu thực tế tại `outputs/sentiment_evaluation.json` sau khi chạy. Không suy diễn nhãn sao thành nhãn cảm xúc do con người xác nhận; luật khía cạnh chưa đủ để kết luận nguyên nhân bất mãn.

## 8. Kiểm tra và lưu minh chứng

Chạy từ thư mục `ReviewLens` khi các dịch vụ cần thiết đang hoạt động:

```bash
mkdir -p outputs/logs
docker compose --profile orchestration --profile dashboard ps
docker compose exec -T spark-client hdfs dfs -ls -h /ecommerce/raw
docker compose exec -T spark-client hdfs dfs -ls /ecommerce/processed
docker compose exec -T spark-client hdfs dfsadmin -report > outputs/logs/hdfs_report_latest.txt
docker compose exec -T spark-client hdfs fsck /ecommerce/raw/reviews.csv -files -blocks -locations > outputs/logs/hdfs_blocks_latest.txt
docker compose exec -T spark-client python -m unittest discover -s tests -v
docker compose exec -T spark-client python scripts/verify_outputs.py --require-stream
docker compose exec -T spark-client python scripts/validate_dashboard.py --outputs /exports
curl -fsS http://localhost:8090/api/health
```

Kiểm tra Kafka sau khi producer đã chạy và tạo topic:

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic ecommerce-reviews
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic ecommerce-live-reviews
```

Các tệp quan trọng được sinh trong quá trình chạy:

| Nhóm minh chứng | Đường dẫn trong `ReviewLens/` |
| --- | --- |
| Audit dữ liệu gốc | `artifacts/audit/dataset_profile.json` |
| Upload/kiểm tra HDFS | `outputs/logs/hdfs_upload_latest.log`, `hdfs_report_latest.txt`, `hdfs_blocks_latest.txt` |
| Spark batch, ML | `outputs/logs/batch_latest.log`, `outputs/logs/sentiment_latest.log` |
| Kafka replay | `outputs/logs/producer_latest.log`, `outputs/logs/consumer_latest.log` |
| Chất lượng và truy vấn | `outputs/data_quality.json`, `outputs/raw_inspection.json`, `outputs/spark_query_plan.txt` |
| Bảng phân tích | `outputs/platform_summary.csv`, `outputs/monthly_summary.csv`, `outputs/rating_distribution.csv` |
| ML và model phục vụ demo | `outputs/sentiment_evaluation.json`, `outputs/sentiment_model.json` |
| Kiểm tra dashboard | `outputs/dashboard_preparation.json`, `outputs/dashboard_validation.json` |
| Live | `outputs/live/`; callback DAG ghi `dag_evidence.json` khi thành công |
| Airflow task logs | Docker volume `airflow_logs`; xem/xuất log của từng task trên giao diện Airflow |

Tên tệp HDFS report/blocks ở dòng bảng trên đều thuộc `outputs/logs/`. Chụp ảnh hoặc quay video dashboard kèm output, log và thời điểm/lượt chạy tương ứng để đối chiếu trong báo cáo.

`scripts/check_submission.py` kiểm bộ minh chứng mở rộng, có cả kết quả kiểm thử trình duyệt, replay và một số tệp theo ngày cụ thể. Script này không tự sinh toàn bộ minh chứng và không bảo đảm chạy được ngay sau ba script khởi động; cần xem các điều kiện đầu vào trong mã trước khi sử dụng.

## 9. Cấu trúc mã nguồn

```text
Nhom10_DoAnCuoiKy/
├── README.md
└── ReviewLens/
    ├── compose.yaml
    ├── compose.low-memory.yaml
    ├── .env.example
    ├── requirements-runtime.txt
    ├── config/       # Luật nhận diện khía cạnh
    ├── dags/         # DAG Airflow
    ├── dashboard/    # Giao diện và server đọc đầu ra pipeline
    ├── infra/        # Dockerfile và cấu hình Hadoop
    ├── jobs/         # Spark batch, ML, Kafka, live và replay
    ├── scripts/      # Tải dữ liệu, chạy, kiểm chứng và đóng gói
    ├── src/          # Thư viện xử lý dùng chung
    ├── tests/        # Unit test
    ├── README.md     # Hướng dẫn kỹ thuật đi kèm mã nguồn
    └── SOURCE_MANIFEST.json
```

CSV gốc, `.env`, `artifacts/` và `outputs/` được tạo khi chạy, không có sẵn trong cây mã nguồn bàn giao này.

## 10. Mở lại, dừng và xử lý lỗi thường gặp

Để mở lại môi trường đã có dữ liệu, chạy `bash scripts/start_live.sh`, sau đó `bash scripts/start_replay.sh` nếu dùng replay. Không cần huấn luyện lại chỉ để xem dashboard đã có output.

Dừng dịch vụ và giữ dữ liệu:

```bash
docker compose -f compose.yaml -f compose.low-memory.yaml --profile orchestration --profile streaming --profile dashboard stop
```

- **Không kết nối được Docker:** khởi động Docker Engine/Desktop và kiểm tra `docker info`; nếu dùng WSL, bật tích hợp cho Ubuntu.
- **Thiếu CSV hoặc checksum không khớp:** chạy `python3 scripts/download_dataset.py`; nếu khác hash, kiểm tra nguồn trước khi thay tệp. Script không ghi đè tệp khác snapshot.
- **Thiếu biến môi trường Compose:** chạy `python3 scripts/init_local_env.py` tại máy cài đặt.
- **HDFS chưa sẵn sàng:** kiểm tra `docker compose logs namenode datanode` và chờ NameNode thoát safe mode.
- **Container thiếu RAM hoặc bị dừng:** kiểm tra `docker stats`, `docker compose logs` và tăng tài nguyên Docker/WSL phù hợp.
- **Dashboard chưa có dữ liệu/model:** kiểm tra log batch/ML và các output trong mục 8.
- **Lỗi cổng đang được dùng:** kiểm tra các cổng 8090, 8080, 8081, 9870 và 9092; nếu đổi Compose cần cập nhật địa chỉ truy cập.

Không chạy `docker compose down -v` nếu muốn giữ dữ liệu HDFS, Kafka và Airflow trong volumes.

## 11. Hồ sơ nộp và GitHub

Báo cáo Word cần xuất thành **PDF**, gắn link repository GitHub mà giảng viên truy cập được. Hồ sơ đi kèm cần có README, mã nguồn, lệnh chạy, output/log HDFS–Spark–Airflow, minh chứng Kafka và demo dashboard tương ứng với nội dung báo cáo.

Nhóm tự bổ sung **nhật ký công việc thực tế**, lỗi và cách sửa, lịch sử commit, họ tên/MSSV và đóng góp từng thành viên. Không tạo thông tin đóng góp hoặc kết quả thực thi khi chưa có căn cứ.

`.gitignore` trong `ReviewLens/` loại dữ liệu gốc, log, output và `.env` khỏi Git. Vì vậy nếu chỉ đẩy source lên GitHub, cần bàn giao minh chứng đã rà soát qua tệp PDF/phụ lục hoặc nơi lưu trữ có quyền truy cập phù hợp, rồi ghi đường dẫn trong báo cáo. Không mặc định rằng push source đã bao gồm minh chứng.

Trước khi công khai repository, rà soát mật khẩu, API key, token và dữ liệu cá nhân trong cả tệp lẫn lịch sử commit. Không công khai CSV gốc chứa thông tin người đánh giá chỉ để làm minh chứng.

Có thể tạo ZIP mã nguồn bằng công cụ sẵn có:

```bash
python3 scripts/package_submission.py
```

Kết quả tại `ReviewLens/submission/ReviewLens_Source.zip`; script giới hạn 20.000.000 byte và loại dữ liệu, output, `.env`, báo cáo cùng ảnh minh chứng. ZIP là gói mã nguồn bổ sung; cổng nộp bài trong yêu cầu chỉ nhận PDF nên đặt link repository trong PDF.
