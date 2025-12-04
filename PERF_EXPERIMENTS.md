# Streaming Performance Experiments

## Overview

The goal of this phase was to see how different Spark Structured Streaming configurations affect two things:

- **Latency** – how long each micro batch takes to process  
- **Throughput** – how many rows per second the job handles

We added a simple `foreachBatch` metrics sink in `stream_job.py` that logs, for every batch:

- timestamp  
- batch_id  
- rows processed  
- duration (seconds)  
- throughput (rows/sec)  
- config label (window + trigger)

All results are stored in:
- `outputs/metrics/metrics.csv`

---

## Experimental Setup

- **Ingestion mode:** file-based  
- **Simulator:** `src/simulate_stream.py`  
  - Copies ~5000 row CSV chunks from `data/seeds/` into `data/incoming_stream/` every 20 seconds  
  - Keeps only the latest 10 chunks in the folder
- **Streaming job:**  
  `python -m src.stream_job --config <config.yaml>`
- **Base config:** `config/stream.yaml`

### Configurations tested

We ran six configurations:

| Config file                      | Window | Trigger |
|----------------------------------|--------|---------|
| `config/stream_win5_trig2.yaml`  | 5s     | 2s      |
| `config/stream_win5_trig5.yaml`  | 5s     | 5s      |
| `config/stream.yaml` | 5s     | 20s     |
| `config/stream_win10_trig2.yaml` | 10s    | 2s      |
| `config/stream_win10_trig5.yaml` | 10s    | 5s      |
| `config/stream_win10_trig20.yaml`| 10s    | 20s     |

Each batch processes roughly **5000 rows** once the stream reaches steady state.

---

## How Metrics Are Computed

Inside `stream_job.py`, the metrics sink:

1. Counts rows with `batch_df.count()`
2. Measures how long the count takes
3. Computes `throughput_rows_per_sec = rows / duration_sec`
4. Appends a line to `metrics.csv` with all fields plus a config label like:

- `file|window=5 seconds|trigger=2 seconds`
- `file|window=10 seconds|trigger=20 seconds`

This makes grouping and plotting straightforward.

---

## Steady-State Observations

(We ignore the first batch for each configuration because Spark warms up and sets state during that batch.)

General patterns:

- Each batch processes ~5000 rows consistently.
- First batches are slower. Steady-state batches are much faster and stable.
- Shorter triggers increase throughput and reduce latency.
- Window size (5s vs 10s) matters very little for this workload.

---

## Plots

We use `scripts/plot_performance.py` to:

1. Read `outputs/metrics/metrics.csv`
2. Parse `window` and `trigger` from the `config` string
3. Drop warmup batches
4. Compute average latency and throughput per config
5. Save two plots to the `plots/` directory

### 1. Average latency per configuration  

File: `plots/latency_by_config.png`

![Average Latency by Config](plots/latency_by_config.png)

### 2. Average throughput per configuration  

File: `plots/throughput_by_config.png`

![Average Throughput by Config](plots/throughput_by_config.png)

---

## Key Findings

1. **Trigger interval has the biggest impact**

   - 2 second and 5 second triggers produce the best performance.
   - 20 second triggers introduce idle gaps, so effective throughput is lower and per-batch latency looks higher.

2. **Window size doesn’t affect performance much**

   - Our aggregations (average + sum) are lightweight.
   - Changing the window from 5s to 10s doesn’t meaningfully change batch cost.
   - Window size is more about analytics granularity than raw performance.

3. **Warmup batches are always slower**

   - The first batch for every config involves setting up state, reading initial data, etc.
   - Ignoring the warmup gives a cleaner steady state comparison.

4. **The job easily handles the workload**

   - Steady-state processing times are often under 0.1 seconds per batch.
   - This leaves plenty of headroom for higher input rates or additional computations if needed.

---

## Summary

Overall, the streaming pipeline behaves predictably:

- **Short triggers: faster batches, higher throughput**  
- **Window size: relatively small impact for this workload**  
- **Performance is stable once the query warms up**

These experiments show that the current setup comfortably keeps up with our simulated stream and can be scaled further if required.