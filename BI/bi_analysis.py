import pandas as pd
import json
from pathlib import Path

# Create bi_plots directory
Path("bi_plots").mkdir(exist_ok=True)

# Load configuration
config_path = Path("pipeline_config.json")
if config_path.exists():
    with open(config_path, 'r') as f:
        config = json.load(f)
else:
    config = {
        "selected_csv": "data/processed/selected_columns.csv",
        "metrics_path": "outputs/metrics/metrics.json",
        "spark_metrics_path": "outputs/metrics/spark_metrics.json",
        "plot_dir": "outputs/metrics/plots"
    }

# Load processed data
data_path = Path(config["selected_csv"])
if data_path.exists():
    df = pd.read_csv(data_path)
    print("Loaded processed data:")
    print(df.head())
    print(f"Shape: {df.shape}")

    # Basic statistics
    print("\nBasic Statistics:")
    print(df.describe())

    # Note: Visualizations skipped due to environment constraints
    print("\nNote: Plots creation skipped due to matplotlib DLL issues.")
    print("Consider using Jupyter notebook for visualizations.")

else:
    print("Processed data not found. Please run the pipeline first.")

# Load metrics
metrics_path = Path(config["metrics_path"])
if metrics_path.exists():
    with open(metrics_path, 'r') as f:
        metrics = json.load(f)
    print("\nPipeline Metrics:")
    for key, value in metrics.items():
        print(f"{key}: {value}")

spark_metrics_path = Path(config["spark_metrics_path"])
if spark_metrics_path.exists():
    with open(spark_metrics_path, 'r') as f:
        spark_metrics = json.load(f)
    print("\nSpark Metrics:")
    for key, value in spark_metrics.items():
        print(f"{key}: {value}")

# Load existing plots
plot_dir = Path(config["plot_dir"])
if plot_dir.exists():
    plots = list(plot_dir.glob("*.png"))
    print(f"\nExisting plots from pipeline: {[p.name for p in plots]}")
    # You can view them manually

print("\nBI Analysis completed.")
print("For visualizations, use Jupyter notebook or alternative plotting libraries.")