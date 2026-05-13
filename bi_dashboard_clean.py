import streamlit as st
import pandas as pd
import numpy as np
import json
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go

# ==================== BILINGUAL TEXT (English + Vietnamese side by side) ====================
T = {
    "app_title": "📊 BRFSS Pipeline BI Dashboard",
    "app_desc": "Business Intelligence Dashboard for BRFSS Big Data Analytics Pipeline",
    "nav_header": "Navigation / Điều hướng",
    "page_overview": "📈 Overview / Tổng quan",
    "page_data": "🔍 Data Analysis / Phân tích",
    "page_model": "🎯 Model Performance / Hiệu suất",
    "page_viz": "📊 Visualizations / Biểu đồ",
    "page_charts": "📈 Pipeline Charts / Biểu đồ pipeline",
    "data_records": "Data Records / Số bản ghi",
    "features": "Features / Đặc trưng",
    "best_acc": "Best Model Accuracy / Độ chính xác",
    "best_model": "Best Model / Mô hình tốt nhất",
    "target_dist": "Target Variable Distribution / Phân bố biến mục tiêu",
    "no_hd": "No Heart Disease (0) / Không bệnh (0)",
    "hd": "Heart Disease (1) / Có bệnh (1)",
    "class_ratio": "Class Ratio / Tỉ lệ lớp",
    "pipeline_status": "Pipeline Status / Trạng thái",
    "data_avail": "✅ Processed Data: Available / Dữ liệu: Có sẵn",
    "data_notfound": "⚠️ Processed Data: Not found / Dữ liệu: Không tìm thấy",
    "metrics_avail": "✅ Pipeline Metrics: Available / Metrics: Có sẵn",
    "metrics_notfound": "⚠️ Pipeline Metrics: Not found / Metrics: Không tìm thấy",
    "spark_models": "🔥 Spark Models / Mô hình Spark: {0}",
    "best_model_info": "🏆 Best Model / Mô hình tốt nhất: **{0}** (F1: {1:.4f} | ROC-AUC: {2:.4f} | Accuracy: {3:.4f})",
    "data_title": "🔍 Data Analysis / Phân tích dữ liệu",
    "data_nodata": "No processed data found. Please run the pipeline first. / Không tìm thấy dữ liệu.",
    "data_preview": "Data Preview / Xem trước dữ liệu",
    "data_stats": "Data Statistics / Thống kê dữ liệu",
    "data_types": "Data Types / Kiểu dữ liệu",
    "missing_title": "Missing Values Analysis / Phân tích giá trị thiếu",
    "missing_none": "No missing values found! / Không có giá trị thiếu!",
    "model_title": "🎯 Model Performance / Hiệu suất mô hình",
    "model_nodata": "No metrics found. Please run the pipeline first. / Không tìm thấy metrics.",
    "model_compare": "Spark Model Comparison / So sánh mô hình Spark",
    "model_col": "Model / Mô hình",
    "acc_col": "Accuracy / Độ chính xác",
    "f1_col": "F1-Score / Điểm F1",
    "auc_col": "ROC-AUC",
    "model_best": "🏆 **Best Model / Mô hình tốt nhất**: {0} (F1: {1:.4f}, ROC-AUC: {2:.4f}, Accuracy: {3:.4f})",
    "model_detail": "Detailed Metrics / Chi tiết",
    "charts_title": "📈 Pipeline-Generated Charts / Biểu đồ từ pipeline",
    "charts_desc": "Static bar charts exported by the Spark pipeline / Biểu đồ cột tĩnh từ Spark pipeline",
    "charts_nowarn": "No pipeline plots found. Please run the pipeline first. / Không tìm thấy biểu đồ.",
    "charts_filenotfound": "No spark_metric_*.png files found. / Không tìm thấy file PNG.",
    "viz_title": "📊 Visualizations / Biểu đồ trực quan",
    "viz_nodata": "No processed data found. / Không tìm thấy dữ liệu.",
    "viz_type": "Choose visualization type / Chọn loại biểu đồ",
    "viz_target": "Target Distribution / Phân bố mục tiêu",
    "viz_dist": "Distributions / Phân phối",
    "viz_corr": "Correlations / Tương quan",
    "viz_box": "Box Plots / Biểu đồ hộp",
    "viz_scatter": "Scatter Plots / Biểu đồ phân tán",
    "viz_numeric_none": "No numeric columns found. / Không có cột số.",
    "viz_target_title": "Target Variable: CVDINFR4 / Biến mục tiêu: CVDINFR4",
    "viz_select_feat": "Select feature to compare / Chọn đặc trưng để so sánh",
    "viz_select_cols": "Select columns to visualize / Chọn cột để xem",
    "viz_no_target": "Target column '{0}' not found. / Không tìm thấy cột mục tiêu.",
    "corr_matrix": "Feature Correlation Matrix / Ma trận tương quan",
    "corr_top": "Top Correlations / Tương quan cao nhất",
    "corr_f1": "Feature 1 / Đặc trưng 1",
    "corr_f2": "Feature 2 / Đặc trưng 2",
    "corr_val": "Correlation / Tương quan",
    "corr_warn": "Need at least 2 numeric columns. / Cần ít nhất 2 cột số.",
    "box_select": "Select column for box plot / Chọn cột cho biểu đồ hộp",
    "box_stats": "Statistics / Thống kê",
    "scatter_x": "X-axis / Trục X",
    "scatter_y": "Y-axis / Trục Y",
    "scatter_same": "Please select different columns. / Vui lòng chọn 2 cột khác nhau.",
    "scatter_coef": "Correlation coefficient / Hệ số tương quan: {0:.3f}",
    "scatter_empty": "No valid data points. / Không có dữ liệu hợp lệ.",
    "scatter_error": "Could not create scatter plot. / Không thể tạo biểu đồ.",
    "feat_dist_title": "Feature Distributions by Target / Phân phối đặc trưng theo mục tiêu",
    "no_hd_label": "No Heart Disease / Không bệnh tim",
    "hd_label": "Heart Disease / Có bệnh tim",
    "last_run_success": "✅ Last Run: {0}s (Success / Thành công)",
    "last_run_failed": "❌ Last Run: Failed / Thất bại",
    "last_run_none": "⚠️ No runtime metadata found / Không có metadata",
    "runtime_unreadable": "⚠️ Runtime metadata: unreadable / Không đọc được",
}

# Set page config
st.set_page_config(
    page_title="BRFSS Pipeline BI Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for responsive font sizes (viewport-width scaling)
st.markdown("""
<style>
    div[data-testid="metric-container"] { min-width: 0 !important; }
    div[data-testid="metric-container"] label {
        font-size: clamp(0.4rem, 1.2vw, 0.85rem) !important;
        overflow: visible !important;
        white-space: normal !important;
        word-break: break-word !important;
    }
    div[data-testid="metric-container"] div[data-testid="metric-value"] {
        font-size: clamp(0.55rem, 2.5vw, 1.8rem) !important;
        overflow: visible !important;
        white-space: normal !important;
        word-break: break-all !important;
        line-height: 1.1 !important;
    }
    div[data-testid="metric-container"] div[data-testid="metric-value"] + div {
        font-size: clamp(0.35rem, 0.9vw, 0.75rem) !important;
        overflow: visible !important;
        white-space: normal !important;
    }
    h1 { font-size: clamp(1rem, 2.5vw, 2.2rem) !important; }
    h2 { font-size: clamp(0.85rem, 1.8vw, 1.6rem) !important; }
    h3 { font-size: clamp(0.7rem, 1.3vw, 1.3rem) !important; }
    @media (max-width: 900px) {
        div[data-testid="column"] { min-width: 45% !important; padding: 0.15rem !important; }
        section[data-testid="stSidebar"] { width: 150px !important; }
        .main .block-container { padding-left: 0.3rem !important; padding-right: 0.3rem !important; }
    }
    @media (max-width: 500px) { div[data-testid="column"] { min-width: 100% !important; } }
</style>
""", unsafe_allow_html=True)

# Load config
@st.cache_data
def load_config():
    config_path = Path("pipeline_config.json")
    if config_path.exists():
        with open(config_path, 'r') as f:
            return json.load(f)
    else:
        return {
            "selected_csv": "data/processed/selected_columns.csv",
            "metrics_path": "outputs/metrics/metrics.json",
            "spark_metrics_path": "outputs/metrics/spark_metrics.json",
            "plot_dir": "outputs/metrics/plots"
        }

config = load_config()

# Load data
@st.cache_data
def load_data():
    data_path = Path(config["selected_csv"])
    if data_path.exists():
        return pd.read_csv(data_path)
    return None

# Load metrics
@st.cache_data
def load_metrics():
    metrics = {}
    spark_metrics = {}
    metrics_path = Path(config["metrics_path"])
    if metrics_path.exists():
        with open(metrics_path, 'r') as f:
            metrics = json.load(f)
    spark_metrics_path = Path(config["spark_metrics_path"])
    if spark_metrics_path.exists():
        with open(spark_metrics_path, 'r') as f:
            spark_metrics = json.load(f)
    return metrics, spark_metrics

def _get_best_model(metrics, spark_metrics):
    """Extract best model info from metrics."""
    best_acc = None
    best_f1 = None
    best_auc = None
    best_model_name = None
    for source in [metrics, spark_metrics]:
        if isinstance(source, list) and len(source) > 0 and isinstance(source[0], dict) and 'accuracy' in source[0]:
            best = sorted(source, key=lambda m: (m.get('f1', 0), m.get('roc_auc', 0), m.get('accuracy', 0)), reverse=True)[0]
            best_acc = best['accuracy']
            best_f1 = best.get('f1', 0)
            best_auc = best.get('roc_auc', 0)
            best_model_name = best.get('model', 'Unknown')
            break
    return best_model_name, best_acc, best_f1, best_auc

# ==================== MAIN APP ====================
def main():
    st.title(T["app_title"])
    st.markdown(T["app_desc"])

    # Sidebar
    with st.sidebar:
        st.header(T["nav_header"])
        page = st.radio("",
            [T["page_overview"], T["page_data"], T["page_model"], T["page_viz"], T["page_charts"]]
        )

    df = load_data()
    metrics, spark_metrics = load_metrics()

    if page == T["page_overview"]:
        show_overview(df, metrics, spark_metrics)
    elif page == T["page_data"]:
        show_data_analysis(df)
    elif page == T["page_model"]:
        show_model_performance(metrics, spark_metrics)
    elif page == T["page_viz"]:
        show_visualizations(df)
    elif page == T["page_charts"]:
        show_pipeline_charts()

# ==================== OVERVIEW ====================
def show_overview(df, metrics, spark_metrics):
    st.header("📈 Overview / Tổng quan")

    best_model_name, best_acc, best_f1, best_auc = _get_best_model(metrics, spark_metrics)

    target_col = 'CVDINFR4'
    neg_count, pos_count = None, None
    if df is not None and target_col in df.columns:
        neg_count = int((df[target_col] == 0).sum())
        pos_count = int((df[target_col] == 1).sum())

    # Row 1: KPIs
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(T["data_records"], f"{df.shape[0]:,}" if df is not None else "N/A")
    with col2:
        st.metric(T["features"], str(df.shape[1]) if df is not None else "N/A")
    with col3:
        st.metric(T["best_acc"], f"{best_acc:.3%}" if best_acc is not None else "N/A")
    with col4:
        st.metric(T["best_model"], best_model_name.upper() if best_model_name else "N/A")

    # Row 2: Class distribution
    if neg_count is not None and pos_count is not None:
        st.subheader(T["target_dist"])
        d1, d2, d3 = st.columns(3)
        with d1:
            st.metric(T["no_hd"], f"{neg_count:,}", f"{neg_count/(neg_count+pos_count)*100:.1f}%")
        with d2:
            st.metric(T["hd"], f"{pos_count:,}", f"{pos_count/(neg_count+pos_count)*100:.1f}%")
        with d3:
            ratio = neg_count / pos_count if pos_count > 0 else 0
            st.metric(T["class_ratio"], f"1:{ratio:.0f}")

    # Row 3: Pipeline status
    st.subheader(T["pipeline_status"])
    s1, s2, s3 = st.columns(3)
    with s1:
        st.success(T["data_avail"] if df is not None else T["data_notfound"])
    with s2:
        has_metrics = bool(metrics) or bool(spark_metrics)
        st.success(T["metrics_avail"] if has_metrics else T["metrics_notfound"])
    with s3:
        runtime_path = Path("outputs/metrics/pipeline_runtime.json")
        if runtime_path.exists():
            try:
                with open(runtime_path) as f:
                    rt = json.load(f)
                dur = rt.get('duration_seconds', 0)
                sts = rt.get('status', 'unknown')
                if sts == 'success':
                    st.success(T["last_run_success"].format(f"{dur:.1f}"))
                else:
                    st.error(T["last_run_failed"])
            except:
                st.warning(T["runtime_unreadable"])
        else:
            st.warning(T["last_run_none"])

    # Row 4: Additional info
    if spark_metrics and isinstance(spark_metrics, list):
        model_names = [m.get('model', '?') for m in spark_metrics]
        st.info(T["spark_models"].format(', '.join(m.upper() for m in model_names)))
    if best_model_name:
        st.info(T["best_model_info"].format(best_model_name.upper(), best_f1, best_auc, best_acc))

# ==================== DATA ANALYSIS ====================
def show_data_analysis(df):
    st.header(T["data_title"])
    if df is None:
        st.error(T["data_nodata"])
        return

    st.subheader(T["data_preview"])
    st.dataframe(df.head(), use_container_width=True)

    st.subheader(T["data_stats"])
    st.dataframe(df.describe(), use_container_width=True)

    st.subheader(T["data_types"])
    dtypes_df = pd.DataFrame({
        'Column': df.columns,
        'Type': df.dtypes.astype(str),
        'Non-Null Count': df.count(),
        'Null Count': df.isnull().sum()
    })
    st.dataframe(dtypes_df, use_container_width=True)

    st.subheader(T["missing_title"])
    missing_data = df.isnull().sum()
    if missing_data.sum() > 0:
        missing_df = pd.DataFrame({
            'Column': missing_data.index,
            'Missing Count': missing_data.values,
            'Missing %': (missing_data.values / len(df) * 100).round(2)
        })
        missing_df = missing_df[missing_df['Missing Count'] > 0]
        st.dataframe(missing_df, use_container_width=True)
    else:
        st.success(T["missing_none"])

# ==================== MODEL PERFORMANCE ====================
def show_model_performance(metrics, spark_metrics):
    st.header(T["model_title"])
    data = None
    if isinstance(metrics, list) and len(metrics) > 0:
        data = metrics
    elif isinstance(spark_metrics, list) and len(spark_metrics) > 0:
        data = spark_metrics
    if data is None:
        st.error(T["model_nodata"])
        return

    st.subheader(T["model_compare"])
    model_data = []
    for m in data:
        model_data.append({
            T["model_col"]: m.get('model', 'Unknown'),
            T["acc_col"]: f"{m.get('accuracy', 0):.4f}",
            T["f1_col"]: f"{m.get('f1', 0):.4f}",
            T["auc_col"]: f"{m.get('roc_auc', 0):.4f}"
        })
    st.dataframe(pd.DataFrame(model_data), use_container_width=True)

    best = sorted(data, key=lambda m: (m.get('f1', 0), m.get('roc_auc', 0), m.get('accuracy', 0)), reverse=True)[0]
    st.success(T["model_best"].format(best['model'].upper(), best['f1'], best['roc_auc'], best['accuracy']))

    st.subheader(T["model_detail"])
    for m in data:
        with st.expander(f"**{m['model'].upper()}**"):
            c1, c2, c3 = st.columns(3)
            c1.metric(T["acc_col"], f"{m.get('accuracy', 0):.4f}")
            c2.metric(T["f1_col"], f"{m.get('f1', 0):.4f}")
            c3.metric(T["auc_col"], f"{m.get('roc_auc', 0):.4f}")

# ==================== PIPELINE CHARTS ====================
def show_pipeline_charts():
    st.header(T["charts_title"])
    st.markdown(T["charts_desc"])

    plot_dir = Path(config["plot_dir"])
    if not plot_dir.exists():
        st.warning(T["charts_nowarn"])
        return

    chart_files = sorted(plot_dir.glob("spark_metric_*.png"))
    if not chart_files:
        st.warning(T["charts_filenotfound"])
        return

    for chart_path in chart_files:
        metric_name = chart_path.stem.replace("spark_metric_", "").upper()
        st.subheader(f"🌲 {metric_name}")
        st.image(str(chart_path), use_container_width=True)

# ==================== VISUALIZATIONS ====================
def show_visualizations(df):
    st.header(T["viz_title"])
    if df is None:
        st.error(T["viz_nodata"])
        return

    viz_options = [T["viz_target"], T["viz_dist"], T["viz_corr"], T["viz_box"], T["viz_scatter"]]
    viz_type = st.selectbox(T["viz_type"], viz_options)

    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    if not numeric_cols:
        st.error(T["viz_numeric_none"])
        return

    target_col = 'CVDINFR4'

    # --- Target Distribution ---
    if viz_type == T["viz_target"]:
        st.subheader(T["viz_target_title"])
        if target_col in df.columns:
            col1, col2 = st.columns(2)
            counts = df[target_col].value_counts().reset_index()
            counts.columns = [target_col, 'Count']
            labels_map = {0: T["no_hd_label"], 1: T["hd_label"]}
            counts['Label'] = counts[target_col].map(labels_map)

            with col1:
                fig = px.pie(counts, values='Count', names='Label',
                             title=T["viz_target"],
                             color_discrete_sequence=['#1d3557', '#e63946'])
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                fig = px.bar(counts, x='Label', y='Count',
                             title='Target Class Counts',
                             text='Count', color='Label',
                             color_discrete_sequence=['#1d3557', '#e63946'])
                st.plotly_chart(fig, use_container_width=True)

            st.subheader(T["feat_dist_title"])
            feat_col = st.selectbox(T["viz_select_feat"], numeric_cols, key='target_feat')
            if feat_col:
                fig = px.histogram(df, x=feat_col, color=target_col,
                                   title=f'{feat_col} {T["feat_dist_title"]}',
                                   marginal="box", nbins=50, barmode='overlay',
                                   color_discrete_sequence=['#1d3557', '#e63946'],
                                   labels={target_col: 'Heart Disease'})
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning(T["viz_no_target"].format(target_col))

    # --- Distributions ---
    elif viz_type == T["viz_dist"]:
        st.subheader(T["viz_dist"])
        selected_cols = st.multiselect(
            T["viz_select_cols"], numeric_cols,
            default=numeric_cols[:4] if len(numeric_cols) >= 4 else numeric_cols[:1]
        )
        if selected_cols:
            for col in selected_cols:
                fig = px.histogram(df, x=col, title=f'Distribution of {col}',
                                   marginal="box", nbins=50)
                st.plotly_chart(fig, use_container_width=True)

    # --- Correlations ---
    elif viz_type == T["viz_corr"]:
        st.subheader(T["viz_corr"])
        if len(numeric_cols) > 1:
            corr = df[numeric_cols].corr()
            fig = go.Figure(data=go.Heatmap(
                z=corr.values, x=corr.columns, y=corr.columns,
                colorscale='RdBu', zmid=0
            ))
            fig.update_layout(title=T["corr_matrix"])
            st.plotly_chart(fig, use_container_width=True)

            st.subheader(T["corr_top"])
            corr_pairs = []
            for i in range(len(corr.columns)):
                for j in range(i+1, len(corr.columns)):
                    corr_pairs.append((corr.columns[i], corr.columns[j], corr.iloc[i, j]))
            corr_df = pd.DataFrame(corr_pairs, columns=[T["corr_f1"], T["corr_f2"], T["corr_val"]])
            corr_df = corr_df.sort_values(T["corr_val"], key=abs, ascending=False).head(10)
            st.dataframe(corr_df, use_container_width=True)
        else:
            st.warning(T["corr_warn"])

    # --- Box Plots ---
    elif viz_type == T["viz_box"]:
        st.subheader(T["viz_box"])
        selected_col = st.selectbox(T["box_select"], numeric_cols)
        fig = px.box(df, y=selected_col, title=f'Box Plot of {selected_col}')
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df[selected_col].describe().to_frame().T, use_container_width=True)

    # --- Scatter Plots ---
    elif viz_type == T["viz_scatter"]:
        st.subheader(T["viz_scatter"])
        sorted_cols = sorted(numeric_cols)
        c1, c2 = st.columns(2)
        with c1:
            x_col = st.selectbox(T["scatter_x"], sorted_cols, index=0, key='scatter_x')
        with c2:
            y_col = st.selectbox(T["scatter_y"], sorted_cols,
                                 index=1 if len(sorted_cols) > 1 else 0, key='scatter_y')

        if x_col and y_col and x_col != y_col:
            try:
                plot_df = df[[x_col, y_col]].dropna()
                if len(plot_df) > 0:
                    fig = px.scatter(plot_df, x=x_col, y=y_col,
                                     title=f'{y_col} vs {x_col}', opacity=0.3)
                    st.plotly_chart(fig, use_container_width=True)
                    corr_coef = plot_df[x_col].corr(plot_df[y_col])
                    st.info(T["scatter_coef"].format(corr_coef))
                else:
                    st.warning(T["scatter_empty"])
            except Exception as e:
                st.error(f"{T['scatter_error']}: {e}")
        elif x_col and y_col and x_col == y_col:
            st.info(T["scatter_same"])

if __name__ == "__main__":
    main()