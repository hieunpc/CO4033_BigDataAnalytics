# BRFSS Kafka + Spark Pipeline

## Flow Hệ Thống

```text
BRFSS.csv
  -> Feature Selection
  -> selected_columns.csv
  -> (optional) Kafka publish
  -> Spark train (full selected data)
  -> metrics.json / spark_metrics.json / best_model.json
  -> spark_metric_*.png
```

## Cấu Trúc Quan Trọng

- `brfss_pipeline/pipeline/full_pipeline.py`: điều phối end-to-end
- `brfss_pipeline/streaming/kafka_io.py`: publish selected data lên Kafka
- `brfss_pipeline/streaming/spark_io.py`: Spark session + Spark model training
- `pipeline_config.json`: cấu hình chạy
- `full_pipeline.py`: entrypoint đơn giản nhất

## Cài Đặt

```powershell
pip install -r requirements-bigdata.txt
```

## Chạy Kafka (Docker)

```powershell
docker compose -f docker-compose.kafka.yml up -d
docker compose -f docker-compose.kafka.yml ps
```

Broker mặc định: `localhost:9092`

## Chạy Pipeline (Khuyến nghị)

1. Sửa cấu hình trong `pipeline_config.json` nếu cần

2. Chạy một lệnh:

```powershell
py full_pipeline.py
```

## Cấu Hình Chính (`pipeline_config.json`)

- `input_csv`: dữ liệu gốc
- `selected_csv`: dữ liệu sau feature selection
- `use_kafka`: bật/tắt publish Kafka
- `kafka_topic`: topic Kafka
- `kafka_bootstrap_servers`: Kafka endpoint
- `metrics_path`: metrics tổng
- `spark_metrics_path`: metrics Spark
- `plot_dir`: nơi lưu biểu đồ
- `runtime_metadata_path`: nơi lưu metadata runtime của pipeline

## Kết Quả Đầu Ra

- `outputs/metrics/metrics.json`
- `outputs/metrics/spark_metrics.json`
- `outputs/metrics/best_model.json`
- `outputs/metrics/pipeline_runtime.json`
- `outputs/metrics/plots/spark_metric_accuracy.png`
- `outputs/metrics/plots/spark_metric_f1.png`
- `outputs/metrics/plots/spark_metric_roc_auc.png`
- `outputs/logs/pipeline.log`

`pipeline_runtime.json` ghi metadata của mỗi lần chạy như:

- trạng thái chạy (`success`/`failed`)
- thời điểm bắt đầu/kết thúc và tổng thời gian
- tham số input/output chính (csv, kafka, metrics paths)
- danh sách model, best model
- thời gian từng bước (feature selection, kafka publish, spark train, export plot)

## Các Model Đang Dùng

Hiện tại hệ thống Spark train 3 model cho bài toán phân loại nhị phân:

- `logistic_regression`
- `random_forest`
- `decision_tree`

Mỗi model được đánh giá theo các metric:

- `accuracy`
- `f1`
- `roc_auc`

Kết quả được sắp hạng theo thứ tự ưu tiên: `f1` -> `roc_auc` -> `accuracy`.

## Cách Chỉnh Sửa Model

Bạn chỉnh trực tiếp trong file `brfss_pipeline/streaming/spark_io.py`, hàm `train_spark_models(...)`, tại biến `estimators`.

Bạn có thể:

- đổi hyperparameter (ví dụ `maxIter`, `numTrees`, `maxDepth`)
- thêm model mới
- xóa model không dùng

Ví dụ:

```python
estimators = {
  "logistic_regression": LogisticRegression(
    featuresCol="features", labelCol="label", maxIter=80, regParam=0.05
  ),
  "random_forest": RandomForestClassifier(
    featuresCol="features", labelCol="label", numTrees=80, maxDepth=12, seed=42
  ),
  "decision_tree": DecisionTreeClassifier(
    featuresCol="features", labelCol="label", maxDepth=10, seed=42
  ),
}
```

Sau khi sửa model, chạy lại pipeline:

```powershell
py full_pipeline.py
```

Hoặc chỉ train Spark:

```powershell
python -m brfss_pipeline.cli spark-train --input-path data/processed/selected_columns.csv --output-path outputs/metrics/spark_metrics.json
```

Lưu ý: `pipeline_config.json` hiện chưa cấu hình danh sách model. Nếu muốn đổi model, bạn cần sửa trong code ở `spark_io.py`.

## Lệnh CLI Bổ Sung

```powershell
python -m brfss_pipeline.cli run-all --use-kafka
python -m brfss_pipeline.cli spark-train --input-path data/processed/selected_columns.csv --output-path outputs/metrics/spark_metrics.json
python -m brfss_pipeline.cli run-all --use-kafka --runtime-metadata-path outputs/metrics/pipeline_runtime.json
```
