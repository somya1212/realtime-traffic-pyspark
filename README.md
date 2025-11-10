# CS532 Project – Real-Time Traffic Analytics (PySpark Structured Streaming)

## Folder Structure 

| Folder / File | Purpose |
|----------------|----------|
| **src/** | All the Python source code for our streaming pipeline. |
| ├── `simulate_stream.py` | Simulates a data stream by copying CSVs from `data/seeds` to `data/incoming_stream`. |
| ├── `stream_job.py` | The main PySpark Structured Streaming job – reads streaming CSVs and computes aggregates. |
| ├── `utils.py` | Helper functions (loads the schema). |
| └── `utils/` | Data preparation utility scripts. |
| │   ├── `convert_pems_to_csv.py` | Converts PEMS .txt files to CSV format. |
| │   ├── `prepare_final_data.py` | Cleans and transforms data for streaming. |
| │   └── `create_chunks.py` | Splits large CSV files into smaller chunks for streaming. |
| **config/schema.json** | Defines the structure (columns + types) of our CSV data. If we change the dataset, update this file. |
| **data/seeds/** | Place CSV chunk files here (these act as our base data for streaming). |
| **data/incoming_stream/** | The simulator will keep dropping new CSVs here every few seconds (git-ignored). |
| **outputs/** | Spark writes output results here (git-ignored). |
| **checkpoints/** | Spark's checkpoint directory for recovery (git-ignored). |
| **requirements.txt** | Python dependencies. |
---

## Setup & Installation

1. **Create and activate a virtual environment**

```bash
   python3 -m venv .venv
   source .venv/bin/activate
```

   **Important**: Always activate the virtual environment before running any scripts:
   ```bash
   source .venv/bin/activate
   ```
   
   You'll know it's activated when you see `(.venv)` in your terminal prompt.

2. **Install dependencies**

```bash
    pip install -r requirements.txt
```

   **Note**: If you get `ModuleNotFoundError`, make sure:
   - The virtual environment is activated (`source .venv/bin/activate`)
   - Or use the venv's Python directly: `.venv/bin/python script.py`

3. **Confirm PySpark installation**

```bash
    python -m pip show pyspark
```

Should display Version: 3.5.1
---

## Dataset

**Source**: PEMS (Performance Measurement System) - LA County  
**Dataset Type**: 5-minute aggregated station data  
**Original File**: `d07_text_station_5min_2006_11_06.txt`  
**Format**: Comma-delimited text file (converted to CSV for processing)

### Dataset Columns

The processed dataset contains the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `Timestamp` | String | Date and time in format `YYYY-MM-DD HH:MM:SS` (5-minute intervals) |
| `Station` | String | Unique station identifier |
| `District` | Integer | District number (7 for LA County) |
| `Freeway #` | Integer | Freeway number |
| `Direction of Travel` | String | Direction: N, S, E, or W |
| `Lane Type` | String | Type of lane (ML=Mainline, OR=On Ramp, FR=Off Ramp, etc.) |
| `Station Length` | Double | Segment length covered by the station (miles/km) |
| `Samples` | Integer | Total number of samples received for all lanes |
| `% Observed` | Double | Percentage of lane points that were observed (not imputed) |
| `Total Flow` | Double | Sum of flows over 5-minute period across all lanes (vehicles/5-min) |
| `Avg Occupancy` | Double | Average occupancy across all lanes (0-1 decimal) |
| `Avg Speed` | Double | Flow-weighted average speed across all lanes (mph) |
| `region` | String | Region identifier (mapped from District 7 → "LA") |

### Data Characteristics

- **Time Period**: November 6, 2006
- **Aggregation Interval**: 5 minutes
- **Original Rows**: 1,089,217
- **After Cleaning**: 381,894 rows (65% removed due to missing/invalid data)
- **Geographic Coverage**: LA County (District 7)

For detailed information about data processing steps, see `CHANGES.md`.

---

## Running the Project

1. **Prepare data**

   The data preparation process involves multiple steps (see `CHANGES.md` for details):
   
   a. **Convert PEMS .txt to CSV**: Use `src/utils/convert_pems_to_csv.py`
   b. **Clean and transform data**: Use `src/utils/prepare_final_data.py`
   c. **Create chunks for streaming**: Use `src/utils/create_chunks.py` to split the final CSV into smaller chunks
   
   Example workflow:
   ```bash
   # Step 1: Convert .txt to CSV
   python src/utils/convert_pems_to_csv.py data/d07_text_station_5min_2006_11_06.txt
   
   # Step 2: Clean and transform
   python src/utils/prepare_final_data.py data/d07_text_station_5min_2006_11_06.csv
   
   # Step 3: Create chunks
   python src/utils/create_chunks.py data/d07_text_station_5min_2006_11_06_final.csv --chunk-size 1000
   ```
   
   Place the chunk files in `data/seeds/` directory. These chunks will be used by the stream simulator.

2. **Start the simulator (in one terminal tab)**

```bash
    python src/simulate_stream.py
```

3. **Start the streaming job (in another terminal tab)**

```bash
    python -m src.stream_job
```

You’ll see console output showing the running averages and total vehicles per region.