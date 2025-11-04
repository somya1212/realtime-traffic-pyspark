# CS532 Project – Real-Time Traffic Analytics (PySpark Structured Streaming)

## Folder Structure 

| Folder / File | Purpose |
|----------------|----------|
| **src/** | All the Python source code for our streaming pipeline. |
| ├── `simulate_stream.py` | Simulates a data stream by copying CSVs from `data/seeds` to `data/incoming_stream`. |
| ├── `stream_job.py` | The main PySpark Structured Streaming job – reads streaming CSVs and computes aggregates. |
| └── `utils.py` | Helper functions (loads the schema). |
| **config/schema.json** | Defines the structure (columns + types) of our CSV data. If we change the dataset, update this file. |
| **data/seeds/** | Place small sample CSV files here (these act as our base data). |
| **data/incoming_stream/** | The simulator will keep dropping new CSVs here every few seconds (git-ignored). |
| **outputs/** | Spark writes output results here (git-ignored). |
| **checkpoints/** | Spark’s checkpoint directory for recovery (git-ignored). |
| **requirements.txt** | Python dependencies. |
---

## Setup & Installation

1. **Create and activate a virtual environment**

```bash
   python3 -m venv .venv
   source .venv/bin/activate
```

2. **Install dependencies**

```bash
    pip install -r requirements.txt
```

3. **Confirm PySpark installation**

```bash
    python -m pip show pyspark
```

Should display Version: 3.5.1
---

## Running the Project

1. **Prepare data**
Add one or two small CSVs in data/seeds/ with columns:

timestamp,region,road_id,vehicle_count,speed
Example:
2025-01-01 08:00:00,North,R1,10,40.5
2025-01-01 08:00:02,South,R2,12,37.2

2. **Start the simulator (in one terminal tab)**

```bash
    python src/simulate_stream.py
```

3. **Start the streaming job (in another terminal tab)**

```bash
    python -m src.stream_job
```

You’ll see console output showing the running averages and total vehicles per region.