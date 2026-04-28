# 📋 Hướng Dẫn Chạy Phần BI - Từng Bước Chi Tiết

## 🎯 Mục Đích
File này hướng dẫn bạn chạy phần BI (Business Intelligence) của dự án BRFSS, bao gồm phân tích dữ liệu, visualizations, và dashboard web. Phần này phụ thuộc vào output của pipeline (file `data/processed/selected_columns.csv` và metrics).

---

## ⚡ Bước 1: Chuẩn Bị Môi Trường (Chỉ chạy lần đầu)

**Mục đích**: Setup virtual environment và cài packages cho BI.

```powershell
# Bước 1.1: Mở PowerShell (chạy bình thường, không cần Admin)
# Bước 1.2: Đi tới thư mục project
cd D:\CO4033_BigDataAnalytics

# Bước 1.3: Kiểm tra virtual environment đã tồn tại chưa
dir .venv

# Nếu thư mục .venv chưa có, tạo mới:
python -m venv .venv

# Bước 1.4: Kích hoạt virtual environment
.venv\Scripts\activate

# Bước 1.5: Nâng cấp pip
python -m pip install --upgrade pip

# Bước 1.6: Cài đặt packages cần thiết cho BI
pip install pandas numpy matplotlib seaborn plotly streamlit jupyter ipykernel

# Bước 1.7: Kiểm tra cài đặt thành công
python -c "import pandas; import streamlit; import plotly; print('✅ Setup BI thành công!')"
```

**Dấu hiệu thành công**: Không có lỗi, hiện ✅ Setup BI thành công!

---

## 🔄 Bước 2: Đảm Bảo Dữ Liệu Đã Sẵn Sàng

**Mục đích**: Kiểm tra output từ pipeline (giả định pipeline đã chạy trước đó).

- File cần có: `data/processed/selected_columns.csv` (430K records, 30 features)
- Metrics: `outputs/metrics/metrics.json` và `outputs/metrics/spark_metrics.json`
- Plots: `outputs/metrics/plots/*.png` (tùy chọn)

```powershell
# Bước 2.1: Kiểm tra file processed data
dir data\processed\selected_columns.csv

# Bước 2.2: Kiểm tra metrics
dir outputs\metrics\

# Nếu thiếu, chạy pipeline trước: python full_pipeline.py
```

**Dấu hiệu thành công**: Các file trên tồn tại.

---

## 📊 Bước 3: Chạy Phân Tích BI

### 3.1 - Console Analysis (BI/bi_analysis.py)
```powershell
# Bước 3.1.1: Kích hoạt virtual environment
.venv\Scripts\activate

# Bước 3.1.2: Chạy script analysis
python BI/bi_analysis.py
```

**Output**: Statistics cho 430K records, metrics từ pipeline, và liệt kê plots có sẵn.

### 3.2 - Jupyter Notebook (BI/bi_analysis.ipynb)
```powershell
# Bước 3.2.1: Kích hoạt virtual environment
.venv\Scripts\activate

# Bước 3.2.2: Mở notebook
jupyter notebook BI/bi_analysis.ipynb
```

**Output**: Interactive analysis với visualizations (Plotly).

### 3.3 - Streamlit Dashboard (BI/bi_dashboard.py)
```powershell
# Bước 3.3.1: Kích hoạt virtual environment
.venv\Scripts\activate

# Bước 3.3.2: Chạy dashboard với Streamlit
streamlit run BI/bi_dashboard.py
```

**Output**: Web app chạy trên http://localhost:8503 với 4 trang: Overview, Data Analysis, Model Performance, Visualizations.

---

## 🔍 Troubleshooting
- **Lỗi "No processed data"**: Chạy pipeline trước (`python full_pipeline.py`).
- **Lỗi packages**: Reinstall với `pip install -r requirements-bigdata.txt`.
- **Dashboard không mở**: Kiểm tra port 8503 có bị chiếm không.

---

## 📝 Ghi Chú
- Phần BI phụ thuộc vào pipeline output – đảm bảo chạy pipeline trước.
- Sử dụng `BI/BI_SUMMARY.md` để xem tóm tắt nhanh.
- Tất cả file BI đều trong folder `BI/`.