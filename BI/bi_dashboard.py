import streamlit as st
import pandas as pd
import numpy as np
import json
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go
import plotly.express as px
import plotly.graph_objects as go

# Set page config
st.set_page_config(
    page_title="BRFSS Pipeline BI Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

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

    # Pipeline metrics
    metrics_path = Path(config["metrics_path"])
    if metrics_path.exists():
        with open(metrics_path, 'r') as f:
            metrics = json.load(f)

    # Spark metrics
    spark_metrics_path = Path(config["spark_metrics_path"])
    if spark_metrics_path.exists():
        with open(spark_metrics_path, 'r') as f:
            spark_metrics = json.load(f)

    return metrics, spark_metrics

# Main app
def main():
    st.title("📊 BRFSS Pipeline BI Dashboard")
    st.markdown("Business Intelligence Dashboard for BRFSS Big Data Analytics Pipeline")

    # Sidebar
    st.sidebar.header("Navigation")
    page = st.sidebar.radio("Choose a section:",
                           ["Overview", "Data Analysis", "Model Performance", "Visualizations"])

    # Load data and metrics
    df = load_data()
    metrics, spark_metrics = load_metrics()

    if page == "Overview":
        show_overview(df, metrics, spark_metrics)

    elif page == "Data Analysis":
        show_data_analysis(df)

    elif page == "Model Performance":
        show_model_performance(metrics, spark_metrics)

    elif page == "Visualizations":
        show_visualizations(df)

def show_overview(df, metrics, spark_metrics):
    st.header("📈 Overview")

    col1, col2, col3 = st.columns(3)

    with col1:
        if df is not None:
            st.metric("Data Records", f"{df.shape[0]:,}")
        else:
            st.metric("Data Records", "N/A")

    with col2:
        if df is not None:
            st.metric("Features", df.shape[1])
        else:
            st.metric("Features", "N/A")

    with col3:
        if 'accuracy' in metrics:
            st.metric("Model Accuracy", f"{metrics['accuracy']:.3%}")
        else:
            st.metric("Model Accuracy", "N/A")

    st.subheader("Pipeline Status")
    status_col1, status_col2 = st.columns(2)

    with status_col1:
        if df is not None:
            st.success("✅ Processed Data: Available")
        else:
            st.warning("⚠️ Processed Data: Not found (run pipeline first)")

    with status_col2:
        if metrics:
            st.success("✅ Pipeline Metrics: Available")
        else:
            st.warning("⚠️ Pipeline Metrics: Not found")

    if spark_metrics:
        st.info(f"🔥 Spark Processing: {len(spark_metrics)} metrics available")

def show_data_analysis(df):
    st.header("🔍 Data Analysis")

    if df is None:
        st.error("No processed data found. Please run the pipeline first.")
        return

    st.subheader("Data Preview")
    st.dataframe(df.head(), use_container_width=True)

    st.subheader("Data Statistics")
    st.dataframe(df.describe(), use_container_width=True)

    st.subheader("Data Types")
    dtypes_df = pd.DataFrame({
        'Column': df.columns,
        'Type': df.dtypes.astype(str),
        'Non-Null Count': df.count(),
        'Null Count': df.isnull().sum()
    })
    st.dataframe(dtypes_df, use_container_width=True)

    # Missing values
    st.subheader("Missing Values Analysis")
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
        st.success("No missing values found!")

def show_model_performance(metrics, spark_metrics):
    st.header("🎯 Model Performance")

    if not metrics and not spark_metrics:
        st.error("No metrics found. Please run the pipeline first.")
        return

    # Pipeline Metrics
    if metrics:
        st.subheader("Pipeline Metrics")
        metrics_df = pd.DataFrame(list(metrics.items()), columns=['Metric', 'Value'])

        # Format percentages for classification metrics
        for idx, row in metrics_df.iterrows():
            if row['Metric'] in ['accuracy', 'precision', 'recall', 'f1_score']:
                metrics_df.at[idx, 'Value'] = f"{row['Value']:.3%}"

        st.dataframe(metrics_df, use_container_width=True)

        # Performance gauge
        if 'accuracy' in metrics:
            acc = metrics['accuracy']
            if acc >= 0.8:
                st.success(f"Model Accuracy: {acc:.1%} - Excellent Performance!")
            elif acc >= 0.7:
                st.warning(f"Model Accuracy: {acc:.1%} - Good Performance")
            else:
                st.error(f"Model Accuracy: {acc:.1%} - Needs Improvement")

    # Spark Metrics
    if spark_metrics:
        st.subheader("Spark Processing Metrics")
        spark_df = pd.DataFrame(list(spark_metrics.items()), columns=['Metric', 'Value'])
        st.dataframe(spark_df, use_container_width=True)

def show_visualizations(df):
    st.header("📊 Visualizations")

    if df is None:
        st.error("No processed data found. Please run the pipeline first.")
        return

    # Select visualization type
    viz_type = st.selectbox("Choose visualization type:",
                           ["Distributions", "Correlations", "Box Plots", "Scatter Plots"])

    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    if not numeric_cols:
        st.error("No numeric columns found for visualization.")
        return

    if viz_type == "Distributions":
        st.subheader("Data Distributions")

        selected_cols = st.multiselect("Select columns to visualize:",
                                      numeric_cols,
                                      default=numeric_cols[:4])

        if selected_cols:
            for col in selected_cols:
                fig = px.histogram(df, x=col, title=f'Distribution of {col}', 
                                 marginal="box", nbins=50)
                st.plotly_chart(fig, use_container_width=True)

    elif viz_type == "Correlations":
        st.subheader("Correlation Heatmap")

        if len(numeric_cols) > 1:
            corr = df[numeric_cols].corr()
            
            fig = go.Figure(data=go.Heatmap(
                z=corr.values,
                x=corr.columns,
                y=corr.columns,
                colorscale='RdBu',
                zmid=0
            ))
            fig.update_layout(title='Feature Correlation Matrix')
            st.plotly_chart(fig, use_container_width=True)

            # Top correlations
            st.subheader("Top Correlations")
            corr_pairs = []
            for i in range(len(corr.columns)):
                for j in range(i+1, len(corr.columns)):
                    corr_pairs.append((corr.columns[i], corr.columns[j], corr.iloc[i, j]))

            corr_df = pd.DataFrame(corr_pairs, columns=['Feature 1', 'Feature 2', 'Correlation'])
            corr_df = corr_df.sort_values('Correlation', key=abs, ascending=False).head(10)
            st.dataframe(corr_df, use_container_width=True)
        else:
            st.warning("Need at least 2 numeric columns for correlation analysis.")

    elif viz_type == "Box Plots":
        st.subheader("Box Plots")

        selected_col = st.selectbox("Select column for box plot:", numeric_cols)

        fig = px.box(df, y=selected_col, title=f'Box Plot of {selected_col}')
        st.plotly_chart(fig, use_container_width=True)

        # Statistics
        col_stats = df[selected_col].describe()
        st.dataframe(col_stats.to_frame().T, use_container_width=True)

    elif viz_type == "Scatter Plots":
        st.subheader("Scatter Plots")

        col1, col2 = st.columns(2)
        with col1:
            x_col = st.selectbox("X-axis:", numeric_cols, key='x')
        with col2:
            y_col = st.selectbox("Y-axis:", numeric_cols, key='y')

        if x_col and y_col and x_col != y_col:
            fig = px.scatter(df, x=x_col, y=y_col, title=f'{y_col} vs {x_col}',
                           trendline="ols")
            st.plotly_chart(fig, use_container_width=True)

            # Correlation coefficient
            corr_coef = df[x_col].corr(df[y_col])
            st.info(f"Correlation coefficient: {corr_coef:.3f}")

if __name__ == "__main__":
    main()