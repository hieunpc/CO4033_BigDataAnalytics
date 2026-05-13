# Hướng dẫn chạy dự án BRFSS Kafka + Spark Pipeline và BI

## Tổng quan

Dự án xử lý dữ liệu BRFSS (CDC Behavioral Risk Factor Surveillance System) bằng pipeline Python/Spark, huấn luyện 3 mô hình phân loại (Logistic Regression, Random Forest, Decision Tree), và trực quan hóa kết quả qua dashboard Streamlit song ngữ Anh-Việt.

---

## 1. Flow hệ thống

```text
BRFSS.csv
  -> Feature Selection (Mutual Information)
  -> selected_columns.csv
  -> (optional) Kafka publish
  -> Spark train (3 models: logistic_regression, random_forest, decision_tree)
  -> metrics.json / spark_metrics.json / best_model.json
  -> spark_metric_*.png (bar charts)
  -> Streamlit Dashboard (BI)
```

---

## 2. Cấu trúc file quan trọng

### Pipeline
| File | Chức năng |
|---|---|
| `full_pipeline.py` | Entrypoint chạy toàn bộ pipeline |
| `brfss_pipeline/pipeline/full_pipeline.py` | Điều phối end-to-end (feature selection → Kafka → Spark train → export charts) |
| `brfss_pipeline/streaming/spark_io.py` | Spark session + huấn luyện 3 model + xuất biểu đồ PNG |
| `brfss_pipeline/streaming/kafka_io.py` | Publish dữ liệu đã chọn lên Kafka |
| `brfss_pipeline/data/feature_selection.py` | Feature selection bằng Mutual Information |
| `brfss_pipeline/config.py` | Hằng số cấu hình (tên cột, target, paths) |
| `pipeline_config.json` | Cấu hình chạy pipeline |

### BI (Business Intelligence)
| File | Chức năng |
|---|---|
| `bi_dashboard_clean.py` | **Dashboard Streamlit** song ngữ Anh-Việt (5 tabs) |
| `bi_analysis_clean.py` | **Script console** phân tích dữ liệu + metrics |
| `bi_analysis.ipynb` | **Jupyter notebook** BI analysis |
| `run_dashboard_clean.bat` | Double-click để chạy dashboard |

### Khác
| File | Chức năng |
|---|---|
| `clear_data.py` | Dọn dẹp cache, temp files |
| `REPORT.md` | Báo cáo đồ án đầy đủ 7 mục |
| `requirements-bigdata.txt` | Danh sách thư viện cần cài |

---

## 3. Cài đặt

```powershell
cd D:\CO4033_BigDataAnalytics
.venv\Scripts\activate
pip install -r requirements-bigdata.txt
```

> Nếu chưa có virtual environment:
> ```powershell
> python -m venv .venv
> .venv\Scripts\activate
> pip install -r requirements-bigdata.txt
> ```

---

## 4. Chuẩn bị dữ liệu BRFSS

1. Tải dữ liệu BRFSS từ CDC: https://www.cdc.gov/brfss/annual_data/annual_data.htm
2. Chọn năm gần nhất, tải file CSV/ZIP
3. Giải nén, đổi tên thành `BRFSS.csv`
4. Đặt vào thư mục `data/raw/BRFSS.csv`

Kiểm tra:
```powershell
mkdir data\raw -Force
python -c "import os; print('File exists:', os.path.exists('data/raw/BRFSS.csv')); print('Size (MB):', os.path.getsize('data/raw/BRFSS.csv') / (1024**2))"
```

---

## 5. Chạy Kafka (Docker) — Tùy chọn

Chỉ cần chạy nếu bạn muốn dùng Kafka streaming:

```powershell
docker compose -f docker-compose.kafka.yml up -d
docker compose -f docker-compose.kafka.yml ps
```

Broker mặc định: `localhost:9092`.

> **Lưu ý:** Nếu không chạy Kafka, nhớ tắt `"use_kafka": false` trong `pipeline_config.json` để pipeline không báo lỗi.

---

## 6. Chạy Pipeline

```powershell
.venv\Scripts\activate
py full_pipeline.py
```

Pipeline sẽ chạy 4 bước:
1. ✅ Feature Selection (~2-3 phút)
2. ⏭️ Kafka Publish (bỏ qua nếu `use_kafka: false`)
3. ✅ Spark Train (3 models)
4. ✅ Export biểu đồ PNG

### Kết quả đầu ra

| File | Mô tả |
|---|---|
| `data/processed/selected_columns.csv` | Dữ liệu đã xử lý (430,755 records, 30 columns) |
| `outputs/metrics/metrics.json` | Metrics 3 models (list format) |
| `outputs/metrics/spark_metrics.json` | Spark metrics (list format) |
| `outputs/metrics/best_model.json` | Best model (Decision Tree) |
| `outputs/metrics/pipeline_runtime.json` | Metadata runtime |
| `outputs/metrics/plots/spark_metric_accuracy.png` | Biểu đồ Accuracy |
| `outputs/metrics/plots/spark_metric_f1.png` | Biểu đồ F1-Score |
| `outputs/metrics/plots/spark_metric_roc_auc.png` | Biểu đồ ROC-AUC |
| `outputs/logs/pipeline.log` | Log chi tiết |

---

## 7. Cấu hình pipeline (`pipeline_config.json`)

| Trường | Mô tả | Giá trị mặc định |
|---|---|---|
| `input_csv` | File dữ liệu gốc | `data/raw/BRFSS.csv` |
| `selected_csv` | File dữ liệu sau feature selection | `data/processed/selected_columns.csv` |
| `use_kafka` | Bật/tắt Kafka publish | `false` |
| `kafka_topic` | Topic Kafka | `brfss_health_setup_v1` |
| `kafka_bootstrap_servers` | Kafka endpoint | `localhost:9092` |
| `metrics_path` | Metrics tổng | `outputs/metrics/metrics.json` |
| `spark_metrics_path` | Metrics Spark | `outputs/metrics/spark_metrics.json` |
| `plot_dir` | Thư mục lưu biểu đồ | `outputs/metrics/plots` |
| `runtime_metadata_path` | Metadata runtime | `outputs/metrics/pipeline_runtime.json` |

---

## 8. Các model đang dùng

Pipeline Spark huấn luyện 3 model phân loại nhị phân (target: `CVDINFR4` — đã từng bị đau tim?):

| Model | Parameters |
|---|---|
| **Logistic Regression** | `maxIter=40` |
| **Random Forest** | `numTrees=40`, `maxDepth=8`, `seed=42` |
| **Decision Tree** | `maxDepth=8`, `seed=42` |

Đánh giá theo: **Accuracy, F1-Score, ROC-AUC**

Xếp hạng ưu tiên: `F1` → `ROC-AUC` → `Accuracy`

---

## 9. Chỉnh sửa model

Mở `brfss_pipeline/streaming/spark_io.py`, tìm hàm `train_spark_models()`, sửa biến `estimators`:

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

Chạy lại pipeline:
```powershell
py full_pipeline.py
```

Hoặc chỉ train Spark:
```powershell
python -m brfss_pipeline.cli spark-train --input-path data/processed/selected_columns.csv
```

---

## 10. Chạy BI Analysis

### 10.1 BI bằng Streamlit Dashboard (khuyến nghị)

```powershell
.venv\Scripts\activate
python -m streamlit run bi_dashboard_clean.py
```

Mở trình duyệt: `http://localhost:8501`

**Dashboard có 5 tabs:**
| Tab | Chức năng |
|---|---|
| 📈 **Overview / Tổng quan** | KPIs, phân bố target, trạng thái pipeline |
| 🔍 **Data Analysis / Phân tích** | Preview, thống kê, missing values |
| 🎯 **Model Performance / Hiệu suất** | So sánh 3 models, best model |
| 📊 **Visualizations / Biểu đồ** | Target Distribution, Distributions, Correlations, Box, Scatter |
| 📈 **Pipeline Charts / Biểu đồ pipeline** | 3 ảnh PNG từ pipeline |

**Tính năng:** Giao diện song ngữ Anh-Việt, responsive (tự động co giãn theo cửa sổ).

Dừng: `Ctrl+C`

### 10.2 Launch 1-click

Double-click file `run_dashboard_clean.bat` — tự động activate venv và mở dashboard.

### 10.3 BI bằng Console

```powershell
.venv\Scripts\activate
python bi_analysis_clean.py
```

Hiển thị: data preview, thống kê, metrics từng model (accuracy, F1, ROC-AUC).

### 10.4 BI bằng Jupyter Notebook

```powershell
.venv\Scripts\activate
jupyter notebook bi_analysis.ipynb
```

Chạy all cells để xem phân tích + biểu đồ + best model.

---

## 11. Dọn dẹp bộ nhớ (Cache & Temp Files)

Sau khi chạy pipeline và dashboard, có thể có nhiều file rác. Dùng:

```powershell
.venv\Scripts\activate
python clear_data.py
```

### Các file bị XÓA:
- `__pycache__/` — bytecode Python (tự tạo lại khi chạy)
- `.pytest_cache/` — cache test
- `.ipynb_checkpoints/` — checkpoint Jupyter
- `*.pyc`, `*.pyo` — bytecode files
- `outputs/temp/` — thư mục tạm
- `outputs/logs/*.log` — log cũ

### Các file được GIỮ LẠI:
- `data/raw/BRFSS.csv` — dữ liệu gốc
- `data/processed/selected_columns.csv` — dữ liệu đã xử lý
- `outputs/metrics/*` — metrics, best model, biểu đồ PNG
- `bi_dashboard_clean.py`, `bi_analysis_clean.py`, `bi_analysis.ipynb` — BI files
- `run_dashboard_clean.bat`, `REPORT.md`, `pipeline_config.json`

> **An toàn:** Chạy `clear_data.py` sau BI không ảnh hưởng gì đến dashboard hay analysis.

---

## 12. Troubleshooting

| Lỗi | Nguyên nhân | Cách fix |
|---|---|---|
| `BRFSS.csv not found` | Chưa có dữ liệu gốc | Tải BRFSS từ CDC, đặt vào `data/raw/` |
| `selected_columns.csv` không tồn tại | Pipeline chưa chạy | Chạy `py full_pipeline.py` |
| `kafka.errors.NoBrokersAvailable` | Kafka chưa chạy | Tắt `use_kafka: false` trong config, hoặc chạy Docker Kafka |
| `No module named 'pandas'` | Quên activate venv | Chạy `.venv\Scripts\activate` trước |
| Streamlit không mở | Port 8501 bận | `python -m streamlit run bi_dashboard_clean.py --server.port 8502` |
| `'list' object has no attribute 'items'` | Metrics là list (format mới) | Đã fix trong `bi_analysis_clean.py` và `bi_analysis.ipynb` |
| `KeyError: 'Correlation'` | Tên cột là bilingual | Đã fix dùng `T["corr_val"]` |
| Thiếu package | Chưa cài dependencies | `pip install -r requirements-bigdata.txt` |
| venv lỗi | Environment hỏng | Xóa `.venv` và tạo lại |

---

## 13. Quick Start

```powershell
cd D:\CO4033_BigDataAnalytics
.venv\Scripts\activate

# 1. Chạy pipeline (nếu chưa có dữ liệu processed)
py full_pipeline.py

# 2. Mở dashboard
python -m streamlit run bi_dashboard_clean.py
```

> Nếu đã có dữ liệu processed, chỉ cần chạy phần BI, không cần chạy pipeline lại.

---

## 14. Kết quả mẫu (số liệu gần đây)

| Model | Accuracy | F1-Score | ROC-AUC |
|---|---|---|---|
| **Decision Tree** 🏆 | 0.9712 | **0.9739** | 0.9822 |
| Logistic Regression | 0.9709 | 0.9736 | **0.9874** |
| Random Forest | 0.9697 | 0.9718 | 0.9868 |

Best model: **Decision Tree** (theo thứ tự ưu tiên: F1 → ROC-AUC → Accuracy)

Dataset: 430,755 records, 29 features, target: `CVDINFR4` (heart disease prediction)
Class imbalance: 94.6% negative / 5.4% positive