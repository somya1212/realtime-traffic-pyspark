# Project Changes Log

This document tracks all changes made to the Real-Time Traffic Analytics project. It serves as a reference for team members to understand what has been implemented and what remains to be done.

---

## Table of Contents

1. [Data Preprocessing](#data-preprocessing)
2. [Time-Based Windowing Implementation](#time-based-windowing-implementation)
3. [Remaining Changes (Planned)](#remaining-changes-planned)

---

## Data Preprocessing

### Overview

The original PEMS dataset comes in a comma-delimited text format without headers and contains lane-specific columns that are not needed for our streaming analysis. We performed several preprocessing steps to prepare the data for the streaming pipeline.

### Step 1: Convert PEMS .txt to CSV

**Script**: `src/utils/convert_pems_to_csv.py`  
**Input**: `d07_text_station_5min_2006_11_06.txt` (comma-delimited, no header)  
**Output**: `d07_text_station_5min_2006_11_06.csv` (CSV with proper headers)

**What it does**:
- Reads the raw PEMS text file (comma-delimited format)
- Auto-detects the number of lanes based on column count
- Assigns proper column names based on PEMS 5-minute station data schema:
  - 12 base columns: Timestamp, Station, District, Freeway #, Direction of Travel, Lane Type, Station Length, Samples, % Observed, Total Flow, Avg Occupancy, Avg Speed
  - Lane-specific columns: For each lane (1-8), 5 columns each (Samples, Flow, Avg Occ, Avg Speed, Observed)
- **Removes lane-specific columns during conversion** (keeps only the 12 base columns)
- Saves as CSV with proper headers

**Why this was done**:
- PEMS files have no header row, making them difficult to work with
- CSV format is standard and easier to process with Pandas/Spark
- Lane-specific data is not needed for our aggregation-level analysis (we use aggregated "Total Flow" and "Avg Speed" instead)

### Step 2: Data Cleaning and Transformation

**Script**: `src/utils/prepare_final_data.py`  
**Input**: Converted CSV file (with 12 base columns)  
**Output**: `d07_text_station_5min_2006_11_06_final.csv` (cleaned and transformed)

**What it does**:

1. **Removes lane-specific columns** (if any remain):
   - Filters out columns starting with "Lane " or "Extra_"
   - Keeps only the 12 base columns

2. **Cleans invalid data**:
   - Removes rows where `% Observed = 0` (no actual measurements, all imputed)
   - Removes rows where `Total Flow` is NaN (missing vehicle count data)
   - Removes rows where `Avg Speed` is NaN (required for congestion index calculation)
   - **Result**: Reduced from 1,089,217 rows to 381,894 rows (65% removed)

3. **Transforms timestamp format**:
   - Converts from `MM/DD/YYYY HH24:MI:SS` (e.g., "11/06/2006 00:00:00")
   - To `YYYY-MM-DD HH:MM:SS` (e.g., "2006-11-06 00:00:00")
   - **Why**: Standard format required for Spark timestamp parsing

4. **Adds region column**:
   - Maps District 7 → `region: "LA"`
   - All rows in this dataset are from District 7 (LA County)
   - **Why**: Enables region-based grouping in streaming aggregations

**Why this was done**:
- Invalid data (0% observed, missing values) would skew aggregations
- Standard timestamp format is required for Spark's time-based windowing
- Region column enables grouping by geographic area in streaming queries

### Step 3: Create Chunks for Streaming

**Script**: `src/utils/create_chunks.py`  
**Input**: Final transformed CSV file  
**Output**: Multiple chunk files in `data/seeds/` directory

**What it does**:
- Reads the large final CSV file
- Splits it into smaller chunks (configurable chunk size, default: 1000 rows per chunk)
- Names chunks with prefix "chunk" (e.g., `chunk_0.csv`, `chunk_1.csv`, ...)
- Saves all chunks to `data/seeds/` directory

**Why this was done**:
- Streaming simulators work better with smaller files that arrive incrementally
- Large files would be processed all at once, defeating the purpose of stream simulation
- Chunks allow us to simulate a continuous data stream by copying files periodically
- Enables testing of windowing and aggregation logic with realistic data arrival patterns

**Usage**:
```bash
python src/utils/create_chunks.py <input_csv_file> --chunk-size 1000
```

---

## Time-Based Windowing Implementation

### What Was Implemented

**File**: `src/stream_job.py`

1. **Timestamp Parsing**:
   - Converts `Timestamp` string column to Spark timestamp type
   - Format: `yyyy-MM-dd HH:mm:ss`
   - Code: `df.withColumn("timestamp", F.to_timestamp("Timestamp", "yyyy-MM-dd HH:mm:ss"))`

2. **Watermarking**:
   - Added 10-second watermark to handle late-arriving data
   - Code: `df.withWatermark("timestamp", "10 seconds")`
   - **Why**: Prevents infinite state growth and allows Spark to drop old data

3. **5-Second Tumbling Windows**:
   - Groups data by 5-second time windows and region
   - Code: `df.groupBy(F.window("timestamp", "5 seconds"), "region")`
   - **Why**: Matches project proposal requirement for near-real-time analysis

4. **Windowed Aggregations**:
   - Computes `avg_speed` (average of "Avg Speed" column)
   - Computes `total_vehicles` (sum of "Total Flow" column)
   - Aggregations are computed per window, not cumulatively

5. **Output Mode**:
   - Changed from `"complete"` to `"update"` mode
   - **Why**: `update` mode only outputs changed/updated windows, which is more efficient for windowed aggregations

### 20-Second Trigger Interval

**File**: `src/stream_job.py`

**What was added**:
- Trigger interval set to 20 seconds: `.trigger(processingTime="20 seconds")`

**Why this was done**:
- Aligns Spark's file checking interval with the stream simulator's file copy interval
- The simulator (`src/simulate_stream.py`) copies files every 20 seconds
- Without this alignment, Spark would check for files more frequently than they arrive, resulting in empty batches
- Reduces unnecessary empty batch processing

**Note**: This is currently hardcoded but marked as TODO for future configuration.

### File Deletion in Stream Simulator

**File**: `src/simulate_stream.py`

**What was added**:
- Automatic cleanup of old files in `data/incoming_stream/` directory
- Keeps only the last 10 files (most recent)
- Deletes older files to prevent local storage from filling up

**Implementation**:
```python
max_files = 10  # Keep only the last 10 files in incoming_stream
# After copying a new file:
existing_files = sorted(out_dir.glob("chunk_*.csv"), key=lambda p: p.stat().st_mtime)
if len(existing_files) > max_files:
    for old_file in existing_files[:-max_files]:
        old_file.unlink()  # Delete oldest files
```

**Why this was done**:
- Without cleanup, `data/incoming_stream/` would accumulate files indefinitely
- Large datasets would fill up local storage during long-running simulations
- Spark processes files once, so keeping old files is unnecessary
- Alternative approach (Spark's `cleanSource` option) was causing file deletion errors, so manual cleanup was implemented

**Trade-offs**:
- Files are permanently deleted (cannot reprocess without restarting simulator)
- Requires manual cleanup logic in the simulator
- More reliable than Spark's automatic cleanup (which had permission issues)

---

## Remaining Changes (Planned)

The following features are planned but not yet implemented.

### 1. Add Congestion Index Calculation

**Status**: Not implemented  
**Priority**: High

**What needs to be done**:
- Add congestion index to windowed aggregations
- Formula: `congestion_index = total_vehicles / avg_speed`
- Handle division by zero (when avg_speed = 0)
- Round to 2 decimal places

**Why this is needed**:
- Congestion index is a key metric in the project proposal
- Provides a single metric that combines vehicle count and speed
- Higher values indicate more congestion

### 2. Add Checkpointing

**Status**: Not implemented  
**Priority**: High (required for fault tolerance)

**What needs to be done**:
- Create `checkpoints/` directory (if not exists)
- Add checkpoint location to streaming query: `.option("checkpointLocation", "checkpoints/stream_checkpoint")`
- Test recovery by stopping and restarting the job

**Why this is needed**:
- Enables fault tolerance: job can recover from failures
- Remembers which data was already processed
- Required for production-like streaming applications
- Allows job to resume from last checkpoint instead of starting from scratch

### 3. Add Persistent Output

**Status**: Not implemented  
**Priority**: Medium (needed for analysis and visualization)

**What needs to be done**:
- Add file output sink (Parquet or CSV format)
- Save results to `outputs/` directory
- Use `append` output mode for files (to avoid duplicates)
- Keep console output for debugging

**Why this is needed**:
- Console output is lost when terminal closes
- Files enable later analysis and visualization
- Required for performance evaluation experiments
- Needed for generating plots (congestion index over time, average speed over time, etc.)

**Output format options**:
- **Parquet** (recommended): Columnar format, efficient for analytics, smaller file size
- **CSV**: Human-readable, easier to inspect, larger file size

---

## Summary of Current State

### Completed

- Data preprocessing pipeline (convert, clean, transform)
- Chunk creation for streaming
- Time-based windowing (5-second windows)
- Windowed aggregations (avg_speed, total_vehicles)
- 20-second trigger interval
- File cleanup in stream simulator
- Watermarking for late data handling

### In Progress / Planned

- Congestion index calculation
- Checkpointing for fault tolerance
- Persistent output (Parquet/CSV files)

### Future Work (Phase 3)

- Performance testing with varying configurations
- Metrics collection (latency, throughput)
- Visualization (plots of congestion index, average speed over time)
- Documentation and presentation

---

## Next Steps for Team Members

1. **To implement Congestion Index**: Add congestion index calculation to windowed aggregations in `src/stream_job.py`
2. **To implement Checkpointing**: Add checkpoint location option to streaming query in `src/stream_job.py`
3. **To implement Persistent Output**: Add file output sink (Parquet or CSV) to `src/stream_job.py`
4. **For data preparation**: Follow the steps outlined in the Data Preprocessing section above
5. **For understanding windowing**: Review the Time-Based Windowing Implementation section above

