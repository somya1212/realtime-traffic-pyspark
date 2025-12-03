from pathlib import Path
import glob
import pandas as pd
import matplotlib.pyplot as plt

PARQ_DIR = Path("outputs/traffic_parquet")
PLOT_DIR = Path("outputs/plots")
PLOT_DIR.mkdir(parents=True, exist_ok=True)

parts = sorted(glob.glob(str(PARQ_DIR / "part-*.parquet")))
if not parts:
    print("No parquet files yet. Let the stream run a bit, then retry.")
    raise SystemExit

df = pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)
df["window_start"] = pd.to_datetime(df["window_start"])
df = df.sort_values(["region", "window_start"])

# -------------------------------
# ORIGINAL PLOTS
# -------------------------------
def save_lineplot(metric: str):
    pivot = df.pivot_table(index="window_start", columns="region",
                           values=metric, aggfunc="mean")
    ax = pivot.plot(figsize=(10,4), marker="o", title=f"{metric} over time")
    ax.set_xlabel("window_start")
    ax.set_ylabel(metric)
    fig = ax.get_figure()
    out = PLOT_DIR / f"{metric}.png"
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)
    print(f"saved: {out}")

for m in ["avg_speed", "total_vehicles", "congestion_index"]:
    save_lineplot(m)

# ============================================================
# 1. Region Comparison Grid
# ============================================================
def plot_region_grid():
    metrics = ["avg_speed", "total_vehicles", "congestion_index"]
    fig, axes = plt.subplots(3, 1, figsize=(10, 10), sharex=True)

    for ax, metric in zip(axes, metrics):
        pivot = df.pivot_table(index="window_start", columns="region",
                               values=metric, aggfunc="mean")
        pivot.plot(ax=ax, marker="o")
        ax.set_title(f"Region Comparison: {metric}")
        ax.set_ylabel(metric)

    axes[-1].set_xlabel("window_start")
    fig.tight_layout()
    out = PLOT_DIR / "region_comparison_grid.png"
    fig.savefig(out, dpi=160)
    plt.close(fig)
    print(f"saved: {out}")

plot_region_grid()


# ============================================================
# 2. Congestion vs Speed Scatter Plot
# ============================================================
def plot_congestion_vs_speed():
    plt.figure(figsize=(8,6))
    for region, sub in df.groupby("region"):
        plt.scatter(sub["avg_speed"], sub["congestion_index"], s=30, alpha=0.6, label=region)

    plt.xlabel("Average Speed (mph)")
    plt.ylabel("Congestion Index")
    plt.title("Congestion Index vs Speed")
    plt.legend()
    plt.tight_layout()

    out = PLOT_DIR / "congestion_vs_speed.png"
    plt.savefig(out, dpi=160)
    plt.close()
    print(f"saved: {out}")

plot_congestion_vs_speed()


# ============================================================
# 3. Rolling Averages
# ============================================================
def plot_rolling_averages():
    df_sorted = df.sort_values("window_start")
    df_sorted["avg_speed_roll"] = df_sorted.groupby("region")["avg_speed"].transform(lambda s: s.rolling(3, min_periods=1).mean())
    df_sorted["congestion_roll"] = df_sorted.groupby("region")["congestion_index"].transform(lambda s: s.rolling(3, min_periods=1).mean())

    pivot_speed = df_sorted.pivot_table(index="window_start", columns="region", values="avg_speed_roll")
    pivot_cong = df_sorted.pivot_table(index="window_start", columns="region", values="congestion_roll")

    plt.figure(figsize=(10,4))
    pivot_speed.plot(marker="o")
    plt.title("Rolling Avg Speed (window=3)")
    plt.tight_layout()
    plt.savefig(PLOT_DIR / "rolling_avg_speed.png", dpi=160)
    plt.close()

    plt.figure(figsize=(10,4))
    pivot_cong.plot(marker="o")
    plt.title("Rolling Congestion Index (window=3)")
    plt.tight_layout()
    plt.savefig(PLOT_DIR / "rolling_congestion_index.png", dpi=160)
    plt.close()

    print("saved: rolling average plots")

plot_rolling_averages()


# ============================================================
# 4. Peak Congestion Detection
# ============================================================
def plot_peak_congestion(top_k=5):
    peaks = df.nlargest(top_k, "congestion_index")[["window_start","region","congestion_index"]]
    plt.figure(figsize=(8,4))
    plt.bar(peaks["window_start"].astype(str), peaks["congestion_index"], color="crimson")
    plt.xticks(rotation=45, ha="right")
    plt.title(f"Top {top_k} Peak Congestion Windows")
    plt.ylabel("Congestion Index")
    plt.tight_layout()
    out = PLOT_DIR / "peak_congestion.png"
    plt.savefig(out, dpi=160)
    plt.close()
    print(f"saved: {out}")

plot_peak_congestion()


print("\n✨ All enhanced data visualizations done! Check outputs/plots/")
