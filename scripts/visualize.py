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

print("\nDone. Open the images from outputs/plots/")
