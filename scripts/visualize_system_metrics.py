# reads metrics.csv and generates extra visualizations (batch size vs config, latency jitter, throughput efficiency,latency histogram, and scaling curves).

import pandas as pd
import matplotlib.pyplot as plt
import os
from pathlib import Path

METRICS_PATH = "outputs/metrics/metrics.csv"
PLOT_DIR = Path("outputs/plots")
PLOT_DIR.mkdir(parents=True, exist_ok=True)


df = pd.read_csv(METRICS_PATH)

# parse timestamp if present
if "timestamp" in df.columns:
    df["timestamp"] = pd.to_datetime(df["timestamp"])


if "duration_sec" in df.columns:
    df["duration_sec"] = pd.to_numeric(df["duration_sec"], errors="coerce")
elif "batch_duration" in df.columns:
    df["duration_sec"] = df["batch_duration"] / 1000.0
else:
    df["duration_sec"] = None

# throughput_rps
if "throughput_rows_per_sec" in df.columns:
    df["throughput_rps"] = pd.to_numeric(df["throughput_rows_per_sec"], errors="coerce")
else:
    df["throughput_rps"] = df.get("processed_rps", None)

df = df.dropna(subset=["duration_sec", "throughput_rps", "rows"])

if "config" not in df.columns:
    df["config"] = "default"

print("\nLoaded metrics with columns:", df.columns.tolist())

# batch size vs window config
def plot_batch_size_vs_window():
    plt.figure(figsize=(10,5))
    tmp = df.groupby("config")["rows"].mean()
    tmp.plot(kind="bar", color="steelblue")

    plt.title("Average Batch Size vs Window Configuration")
    plt.ylabel("Rows per Batch")
    plt.xlabel("Configuration")
    plt.xticks(rotation=0, ha="right")   
    plt.tight_layout()

    out = PLOT_DIR / "batch_size_vs_window.png"
    plt.savefig(out, dpi=150)
    plt.close()
    print(f"Saved {out}")


# latency variance / jitter
def plot_latency_jitter():
    plt.figure(figsize=(10,5))
    jitter = df.groupby("config")["duration_sec"].std()
    jitter.plot(kind="bar", color="orange")

    plt.title("Latency Jitter (Std Dev)")
    plt.ylabel("Std Dev of Batch Duration (sec)")
    plt.xlabel("Configuration")
    plt.xticks(rotation=0, ha="right")  
    plt.tight_layout()

    out = PLOT_DIR / "latency_jitter.png"
    plt.savefig(out, dpi=150)
    plt.close()
    print(f"Saved {out}")

# throughput efficiency 
def plot_throughput_efficiency():
    plt.figure(figsize=(10,5))
    max_tp = df["throughput_rps"].max()
    df["efficiency"] = df["throughput_rps"] / max_tp * 100

    eff = df.groupby("config")["efficiency"].mean()
    eff.plot(kind="bar", color="green")

    plt.title("Throughput Efficiency (%)")
    plt.ylabel("Efficiency (%)")
    plt.xlabel("Configuration")
    plt.xticks(rotation=0, ha="right")  
    plt.tight_layout()

    out = PLOT_DIR / "throughput_efficiency.png"
    plt.savefig(out, dpi=150)
    plt.close()
    print(f"Saved {out}")


# latency histogram
def plot_latency_histogram():
    plt.figure(figsize=(10,5))
    plt.hist(df["duration_sec"], bins=20, color="purple", alpha=0.7)

    plt.title("Latency Distribution (Histogram)")
    plt.xlabel("Batch Duration (sec)")
    plt.ylabel("Frequency")
    plt.tight_layout()

    out = PLOT_DIR / "latency_histogram.png"
    plt.savefig(out, dpi=150)
    plt.close()
    print(f"Saved {out}")


#scaling curve
def plot_scaling_curve():
    plt.figure(figsize=(10,5))
    plt.scatter(df["rows"], df["duration_sec"], alpha=0.6)

    plt.title("Scaling Curve: Rows vs Duration")
    plt.xlabel("Rows per Batch")
    plt.ylabel("Batch Duration (sec)")
    plt.tight_layout()

    out = PLOT_DIR / "scaling_curve_rows_vs_duration.png"
    plt.savefig(out, dpi=150)
    plt.close()
    print(f"Saved {out}")



if __name__ == "__main__":
    plot_batch_size_vs_window()
    plot_latency_jitter()
    plot_throughput_efficiency()
    plot_latency_histogram()
    plot_scaling_curve()
    print("\n Advanced system performance visualizations generated!")
