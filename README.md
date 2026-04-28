# BRFSS Big Data Analytics

Project này xây dựng một luồng xử lý dữ liệu BRFSS theo hướng end-to-end: feature selection, tùy chọn đẩy sang Kafka, train mô hình bằng Spark, rồi tổng hợp metrics và dashboard BI để phân tích kết quả.

## Tổng Quan Kiến Trúc

```text
data/raw/BRFSS.csv
  -> Feature Selection
  -> data/processed/selected_columns.csv
  -> (optional) Kafka publish
  -> Spark training
  -> outputs/metrics/*.json
  -> outputs/metrics/plots/*.png
  -> BI analysis + Streamlit dashboard
```

## Thành Phần Chính

- `full_pipeline.py`: entrypoint chạy toàn bộ pipeline theo `pipeline_config.json`
- `brfss_pipeline/pipeline/full_pipeline.py`: logic điều phối end-to-end
- `brfss_pipeline/data/feature_selection.py`: chọn bộ cột đầu vào cho mô hình
- `brfss_pipeline/streaming/kafka_io.py`: publish dữ liệu đã chọn lên Kafka
- `brfss_pipeline/streaming/spark_io.py`: train Spark ML và xuất metrics/plots
- `brfss_pipeline/cli.py`: CLI để chạy từng bước riêng lẻ
- `BI/bi_analysis.py`: phân tích BI dạng console
- `BI/bi_analysis.ipynb`: notebook BI tương tác
- `BI/bi_dashboard.py`: dashboard Streamlit

## Cài Đặt

1. Tạo và kích hoạt virtual environment `.venv` nếu chưa có.
2. Cài dependencies:

```powershell
python -m pip install --upgrade pip
pip install -r requirements-bigdata.txt
```

## Dữ Liệu Đầu Vào

Pipeline mặc định đọc:

- `data/raw/BRFSS.csv`

Nếu file này chưa có, cần đặt dữ liệu BRFSS gốc vào đúng vị trí trước khi chạy pipeline.

## Chạy Kafka Với Docker

Kafka là tùy chọn. Chỉ cần bật khi muốn publish dữ liệu đã chọn sang broker.

```powershell
docker compose -f docker-compose.kafka.yml up -d
docker compose -f docker-compose.kafka.yml ps
```

Broker mặc định:

- `localhost:9092`

## Chạy Full Pipeline

### Cách 1: Chạy entrypoint gốc

```powershell
python full_pipeline.py
```

Script này sẽ:

1. Load cấu hình từ `pipeline_config.json`
2. Thực hiện feature selection
3. Publish Kafka nếu `use_kafka=true`
4. Train Spark models
5. Xuất metrics, best model, runtime metadata và plots

### Cách 2: Chạy qua CLI

```powershell
python -m brfss_pipeline.cli run-all
```

Muốn bật Kafka:

```powershell
python -m brfss_pipeline.cli run-all --use-kafka
```

### Cấu Hình Chính

File `pipeline_config.json` điều khiển toàn bộ pipeline:

- `input_csv`: file dữ liệu gốc
- `selected_csv`: file sau feature selection
- `use_kafka`: bật hoặc tắt bước publish Kafka
- `kafka_topic`: tên topic Kafka
- `kafka_bootstrap_servers`: địa chỉ Kafka broker
- `metrics_path`: nơi lưu metrics tổng
- `spark_metrics_path`: nơi lưu metrics Spark
- `plot_dir`: thư mục lưu biểu đồ
- `runtime_metadata_path`: nơi lưu metadata mỗi lần chạy

## Chạy Từng Bước Riêng Lẻ

### Feature Selection

```powershell
python -m brfss_pipeline.cli feature-select --input-path data/raw/BRFSS.csv --output-path data/processed/selected_columns.csv
```

### Publish Kafka

```powershell
python -m brfss_pipeline.cli kafka-produce --csv-path data/processed/selected_columns.csv --bootstrap-servers localhost:9092 --topic brfss_health
```

### Train Spark Riêng

```powershell
python -m brfss_pipeline.cli spark-train --input-path data/processed/selected_columns.csv --output-path outputs/metrics/spark_metrics.json --plot-dir outputs/metrics/plots
```

## Kiến Trúc Pipeline

Luồng chạy chính là:

1. Đọc `data/raw/BRFSS.csv`
2. Chọn ra tập cột trong `brfss_pipeline/config.py`
3. Ghi `data/processed/selected_columns.csv`
4. Nếu bật Kafka, publish dữ liệu đã chọn lên topic cấu hình
5. Khởi tạo Spark session và train 3 mô hình phân loại nhị phân
6. Tính metrics, chọn best model, xuất confusion matrix và bar chart
7. Ghi runtime metadata để truy vết toàn bộ phiên chạy

### Mô Hình Spark

Hiện tại Spark training đang dùng 3 mô hình:

- `logistic_regression`
- `random_forest`
- `decision_tree`

Mỗi mô hình được đánh giá theo:

- `accuracy`
- `precision`
- `recall`
- `f1`
- `roc_auc`
- `specificity`
- `confusion_matrix`

Best model được xếp theo thứ tự ưu tiên:

1. `f1`
2. `roc_auc`
3. `accuracy`

### Chỉnh Model

Nếu muốn đổi model hoặc hyperparameter, chỉnh trong `brfss_pipeline/streaming/spark_io.py` tại hàm `train_spark_models(...)`.

Sau khi sửa, chạy lại:

```powershell
python full_pipeline.py
```

Hoặc chỉ chạy Spark:

```powershell
python -m brfss_pipeline.cli spark-train --input-path data/processed/selected_columns.csv
```

## Phần BI

BI dùng lại output của pipeline để trình bày dữ liệu, metrics và visualizations.

### 1. Console Analysis

```powershell
python BI/bi_analysis.py
```

Script này sẽ:

- đọc `data/processed/selected_columns.csv`
- in thống kê mô tả dữ liệu
- đọc `outputs/metrics/metrics.json`
- đọc `outputs/metrics/spark_metrics.json`
- liệt kê các biểu đồ đã tạo trong `outputs/metrics/plots/`

### 2. Notebook BI

```powershell
jupyter notebook BI/bi_analysis.ipynb
```

Notebook phù hợp khi muốn phân tích tương tác, trực quan hóa sâu hơn và thử nghiệm thêm chỉ số.

### 3. Streamlit Dashboard

```powershell
streamlit run BI/bi_dashboard.py
```

Dashboard mặc định chạy tại:

- `http://localhost:8501`

Các trang chính:

- `Overview`
- `Data Analysis`
- `Model Performance`
- `Visualizations`

## Output Sinh Ra

Sau khi pipeline chạy thành công, các file quan trọng thường nằm ở:

- `data/processed/selected_columns.csv`
- `outputs/metrics/metrics.json`
- `outputs/metrics/spark_metrics.json`
- `outputs/metrics/best_model.json`
- `outputs/metrics/pipeline_runtime.json`
- `outputs/metrics/plots/spark_metric_accuracy.png`
- `outputs/metrics/plots/spark_metric_f1.png`
- `outputs/metrics/plots/spark_metric_roc_auc.png`
- `outputs/metrics/plots/spark_confusion_matrix_decision_tree.png`
- `outputs/metrics/plots/spark_confusion_matrix_logistic_regression.png`
- `outputs/metrics/plots/spark_confusion_matrix_random_forest.png`
- `outputs/logs/pipeline.log`

`pipeline_runtime.json` lưu lại:

- trạng thái chạy `success` hoặc `failed`
- thời gian bắt đầu/kết thúc và tổng duration
- tham số input/output của phiên chạy
- danh sách model và best model
- thời gian cho từng bước của pipeline
- danh sách file biểu đồ đã xuất

## Cấu Trúc Thư Mục

```text
BRFSS Big Data Analytics
├─ full_pipeline.py
├─ pipeline_config.json
├─ brfss_pipeline/
│  ├─ data/
│  ├─ pipeline/
│  └─ streaming/
├─ BI/
│  ├─ bi_analysis.py
│  ├─ bi_analysis.ipynb
│  └─ bi_dashboard.py
├─ data/
│  ├─ raw/
│  └─ processed/
└─ outputs/
   ├─ logs/
   └─ metrics/
```

## Troubleshooting

- Nếu báo thiếu dữ liệu, kiểm tra `data/raw/BRFSS.csv` đã tồn tại chưa.
- Nếu muốn bỏ Kafka, đặt `use_kafka=false` trong `pipeline_config.json`.
- Nếu dashboard không mở, kiểm tra port mặc định `8501` có bị chiếm không.
- Nếu thiếu package, cài lại bằng `pip install -r requirements-bigdata.txt`.
- Nếu Spark/JVM lỗi, chạy lại trong đúng virtual environment và kiểm tra cấu hình local machine.

## Ghi Chú

- File `BI/RUN_BI.md` vẫn hữu ích nếu cần hướng dẫn chạy BI từng bước.
- `BI/BI_SUMMARY.md` phù hợp khi muốn xem tóm tắt nhanh các phần BI đã hoàn thành.
- Nếu cần mở rộng pipeline, điểm chỉnh chính nằm ở `brfss_pipeline/data/feature_selection.py`, `brfss_pipeline/streaming/spark_io.py` và `pipeline_config.json`.
