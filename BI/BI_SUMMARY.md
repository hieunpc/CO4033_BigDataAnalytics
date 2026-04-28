# BI Analysis Summary

## Mục đích
Phần BI (Business Intelligence) của dự án này tập trung vào việc trình bày và phân tích kết quả từ pipeline xử lý dữ liệu BRFSS. Chúng tôi tạo visualizations để hiểu rõ hơn về dữ liệu sức khỏe và hiệu suất của mô hình machine learning.

## Những gì đã làm

### 1. Phân tích dữ liệu đã xử lý ✅
- Load dữ liệu từ `data/processed/selected_columns.csv` (đã được tạo từ pipeline)
- Tính toán thống kê cơ bản (mean, std, min, max, etc.) cho 430,755 records
- Phân tích phân phối và tương quan của 30 features

### 2. Phân tích metrics ✅
- Load và hiển thị metrics từ pipeline (`outputs/metrics/metrics.json`)
- Load và hiển thị Spark metrics (`outputs/metrics/spark_metrics.json`)
- Phân tích performance của model

### 3. Visualizations ✅
- Tạo histograms và correlation heatmap (trong notebook)
- Sử dụng plots có sẵn từ pipeline (`outputs/metrics/plots/spark_metric_*.png`)
- Interactive analysis trong Jupyter notebook với Plotly

### 4. BI Notebook ✅
- Tạo `bi_analysis.ipynb` với sections đầy đủ:
  - Data loading và exploration
  - Metrics analysis
  - Visualizations (Plotly thay thế matplotlib)
  - Model performance insights
  - Summary và recommendations

### 5. Streamlit Dashboard ✅
- Tạo `bi_dashboard.py`: Interactive web dashboard với:
  - Overview với metrics và KPIs
  - Data analysis section với statistics
  - Model performance visualization
  - Interactive plots (distributions, correlations, scatter plots)
- **ĐANG CHẠY** trên http://localhost:8503

## Cách chạy để lấy kết quả

### Điều kiện tiên quyết ✅
1. **Configure Python environment** (đã hoàn thành):
   - Environment đã được configure với Python 3.14.0
   - Virtual environment `.venv` đã được tạo

2. **Cài đặt packages** (đã hoàn thành):
   - pandas, numpy, matplotlib, seaborn đã được cài đặt
   - streamlit, plotly đã được cài đặt cho dashboard
   - jupyter, ipykernel cho notebook

3. **Chuẩn bị dữ liệu BRFSS** ✅:
   - File `BRFSS.csv` đã được thêm vào `data/raw/`
   - Pipeline đã chạy thành công và tạo `data/processed/selected_columns.csv`
   ```powershell
   # Tạo thư mục data/raw (đã tạo)
   mkdir data\raw -Force
   
   # 📥 TẢI FILE BRFSS.CSV:
   # 1. Mở trang: https://www.cdc.gov/brfss/annual_data/annual_data.htm
   # 2. Chọn năm gần nhất (ví dụ: 2023 BRFSS Survey Data)
   # 3. Tải file CSV (LLCP2023.ASC.zip hoặc tương tự)
   # 4. Giải nén và đổi tên file thành BRFSS.csv
   # 5. Đặt file vào thư mục data/raw/BRFSS.csv
   
   # 📋 File cần có: data/raw/BRFSS.csv
   ```
   
   Sau đó chạy pipeline:
   ```powershell
   python full_pipeline.py
   ```

### Chạy BI Analysis ✅
1. **Console Analysis** (đã test thành công):
   ```powershell
   .venv\Scripts\activate
   python bi_analysis.py
   ```
   Output: Statistics cho 430,755 records với 30 features

2. **Jupyter Notebook** (interactive analysis):
   ```powershell
   .venv\Scripts\activate
   jupyter notebook bi_analysis.ipynb
   ```

3. **Streamlit Dashboard** (web app - ĐANG CHẠY):
   ```powershell
   .\run_dashboard.bat
   ```
   **Truy cập**: http://localhost:8503
   - Overview với KPIs
   - Data Analysis với statistics
   - Model Performance với visualizations
   - Interactive plots với Plotly

### Hướng Dẫn Chi Tiết Từng Bước
**📋 Xem file RUN_GUIDE.md** để có hướng dẫn đầy đủ:
- Bước 1: Setup environment
- Bước 2: Chuẩn bị dữ liệu
- Bước 3: Chạy pipeline
- Bước 4: Chạy BI (3 cách)
- Bước 5: Dọn dẹp máy

### Giải Phóng Dung Lượng Máy
**🧹 Chạy script clear_data.py** để dọn dẹp:
```powershell
.venv\Scripts\activate
python clear_data.py
```
Script sẽ:
- Xóa __pycache__, .pytest_cache, .ipynb_checkpoints
- Xóa temp files, logs
- Giữ lại dữ liệu quan trọng (raw, processed, metrics)
- Hiển thị dung lượng giải phóng

### Troubleshooting
- **Nếu pipeline fail**: Kiểm tra Kafka server (nếu use_kafka=true). Lỗi Kafka không ảnh hưởng BI vì dữ liệu vẫn xử lý được ở Step 1
- **Nếu Model Performance trống**: Bình thường! Là vì Kafka không chạy nên Spark models không được train. Xem phần Data Analysis để lấy thông tin dữ liệu
- **Nếu matplotlib fail**: Dùng Plotly trong notebook/dashboard (đã sử dụng)
- **Nếu packages thiếu**: Chạy `pip install -r requirements-bigdata.txt`
- **Nếu venv fail**: Dùng system Python trực tiếp

### Kết quả mong đợi
- **Console output**: Thống kê dữ liệu (describe), metrics từ pipeline và Spark
- **Plots**: Hiện tại skip do environment constraints; sử dụng plots từ pipeline (`outputs/metrics/plots/`) hoặc Jupyter notebook cho visualizations
- **Streamlit Dashboard**: Web app với sidebar navigation, interactive charts, real-time metrics

### Ví dụ Streamlit Dashboard:
- **Overview**: Data records, features, model accuracy metrics
- **Data Analysis**: Preview, statistics, missing values analysis
- **Model Performance**: Detailed metrics với performance assessment
- **Visualizations**: Interactive plots (histograms, correlations, scatter plots)

### Ví dụ output console:
```
Processed data not found. Please run the pipeline first.

BI Analysis completed.
For visualizations, use Jupyter notebook or alternative plotting libraries.
```
(Hoặc nếu có data:
```
Loaded processed data:
   age  weight  height  ...
Shape: (10000, 20)

Basic Statistics:
       age      weight      height
count  10000.0  10000.0   10000.0
mean   45.2     75.3      170.5
...

Pipeline Metrics:
accuracy: 0.85
...
```)

### Ghi chú
- Nếu chưa có dữ liệu processed, script sẽ thông báo và chỉ hiển thị metrics
- Plots bị skip do Windows Application Control policy chặn matplotlib DLLs
- Để visualizations: sử dụng Jupyter notebook, hoặc alternative libraries như plotly
- Streamlit dashboard cung cấp trải nghiệm BI tương tác nhất với web interface
- Nếu streamlit không chạy: dùng `.\run_dashboard.bat` để auto-setup
- Có thể mở rộng script để thêm analysis khác như clustering insights, feature importance

## Cấu trúc file
```
bi_analysis.py          # Script phân tích BI (stats và metrics)
bi_analysis.ipynb       # Jupyter notebook cho BI analysis đầy đủ
bi_dashboard.py         # Streamlit web dashboard cho BI interactive
run_dashboard.bat       # Batch file để chạy dashboard dễ dàng
outputs/metrics/plots/  # Plots từ pipeline (spark_metric_*.png)
```