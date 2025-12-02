#!/usr/bin/env python3
"""
Plot streaming performance from outputs/metrics/metrics.csv.

Generates two plots:
- outputs/plots/latency_by_config.png
- outputs/plots/throughput_by_config.png
"""

import sys
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


METRICS_PATH = Path("outputs/metrics/metrics.csv")
PLOTS_DIR = Path("plots")


def parse_config(config_str):
    """
    Parse a config string like:
        "file|window=5 seconds|trigger=20 seconds"
    into (window_seconds, trigger_seconds).
    """
    window_seconds = None
    trigger_seconds = None

    if not isinstance(config_str, str):
        return None, None

    parts = config_str.split("|")
    for part in parts:
        part = part.strip()
        if part.startswith("window="):
            # after '=', then take the first token before 'seconds'
            val = part.split("=", 1)[1].strip()
            # e.g. "5 seconds" -> "5"
            window_seconds = int(val.split()[0])
        elif part.startswith("trigger="):
            val = part.split("=", 1)[1].strip()
            trigger_seconds = int(val.split()[0])

    return window_seconds, trigger_seconds


def load_and_prepare():
    """Load metrics.csv and compute steady-state averages per config."""
    if not METRICS_PATH.exists():
        print(f"[ERROR] Metrics file not found at: {METRICS_PATH}")
        sys.exit(1)

    df = pd.read_csv(METRICS_PATH)

    # Ensure numeric types
    df["duration_sec"] = pd.to_numeric(df["duration_sec"], errors="coerce")
    df["throughput_rows_per_sec"] = pd.to_numeric(
        df["throughput_rows_per_sec"], errors="coerce"
    )
    df["batch_id"] = pd.to_numeric(df["batch_id"], errors="coerce")

    # Parse window / trigger from config
    df[["window_seconds", "trigger_seconds"]] = df["config"].apply(
        lambda s: pd.Series(parse_config(s))
    )

    # Drop rows where parsing failed
    df = df.dropna(subset=["window_seconds", "trigger_seconds"]).copy()
    df["window_seconds"] = df["window_seconds"].astype(int)
    df["trigger_seconds"] = df["trigger_seconds"].astype(int)

    # Sort by config + batch to identify warmup batch cleanly
    df = df.sort_values(["window_seconds", "trigger_seconds", "batch_id"])

    # Mark first batch per (window, trigger) as warmup
    df["is_warmup"] = (
        df.groupby(["window_seconds", "trigger_seconds"]).cumcount() == 0
    )

    # Remove warmup batches (optional but usually nicer)
    df_steady = df[~df["is_warmup"]].copy()

    # Aggregate by config
    agg = (
        df_steady.groupby(["window_seconds", "trigger_seconds"], as_index=False)
        .agg(
            avg_duration_sec=("duration_sec", "mean"),
            avg_throughput=("throughput_rows_per_sec", "mean"),
            batches=("batch_id", "count"),
        )
    )

    # Create short labels like "W5–T20"
    agg["short_label"] = agg.apply(
        lambda row: f"W{int(row['window_seconds'])}–T{int(row['trigger_seconds'])}",
        axis=1,
    )

    # Sort nicely: window ascending, trigger ascending
    agg = agg.sort_values(["window_seconds", "trigger_seconds"]).reset_index(drop=True)

    return agg


def plot_latency(agg: pd.DataFrame):
    """Plot average batch duration per configuration."""
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    plt.figure(figsize=(8, 4))
    x = agg["short_label"]
    y = agg["avg_duration_sec"]

    plt.bar(x, y)
    plt.ylabel("Avg batch duration (seconds)")
    plt.xlabel("Configuration")
    plt.title("Streaming latency vs window / trigger")
    plt.xticks(rotation=30, ha="right")
    plt.grid(axis="y", linestyle="--", alpha=0.3)

    # Note explaining the label format
    plt.figtext(
        0.99,
        0.01,
        "W = window size (seconds),  T = trigger interval (seconds)",
        ha="right",
        fontsize=9,
        color="gray",
    )

    plt.tight_layout()
    out_path = PLOTS_DIR / "latency_by_config.png"
    plt.savefig(out_path, dpi=200)
    print(f"[OK] Saved latency plot to {out_path}")
    plt.close()


def plot_throughput(agg: pd.DataFrame):
    """Plot average throughput per configuration."""
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    plt.figure(figsize=(8, 4))
    x = agg["short_label"]
    y = agg["avg_throughput"]

    plt.bar(x, y)
    plt.ylabel("Avg throughput (rows / second)")
    plt.xlabel("Configuration")
    plt.title("Streaming throughput vs window / trigger")
    plt.xticks(rotation=30, ha="right")
    plt.grid(axis="y", linestyle="--", alpha=0.3)

    plt.figtext(
        0.99,
        0.01,
        "W = window size (seconds),  T = trigger interval (seconds)",
        ha="right",
        fontsize=9,
        color="gray",
    )

    plt.tight_layout()
    out_path = PLOTS_DIR / "throughput_by_config.png"
    plt.savefig(out_path, dpi=200)
    print(f"[OK] Saved throughput plot to {out_path}")
    plt.close()


def main():
    agg = load_and_prepare()

    # Print table to console 
    print("\nAggregated performance metrics (steady state, warmup dropped):")
    print(
        agg[
            [
                "window_seconds",
                "trigger_seconds",
                "batches",
                "avg_duration_sec",
                "avg_throughput",
                "short_label",
            ]
        ].to_string(index=False)
    )

    plot_latency(agg)
    plot_throughput(agg)


if __name__ == "__main__":
    main()