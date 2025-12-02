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

### Immediate Next Steps

1. **Verify and enhance visualization**:
   - Test `scripts/visualize.py` with current Parquet output
   - Ensure all required plots are generated correctly
   - Add window size comparison plot if needed

2. **Implement performance evaluation experiments**:
   - Create experiment runner script
   - Run tests with varying configurations (input rate, partitions, window size)
   - Collect and analyze metrics
   - Generate comparative performance plots

3. **Update documentation**:
   - Document experiment results
   - Update README with any missing instructions
   - Ensure setup is clear for new users

4. **Prepare presentation**:
   - Record demo video showing both file and Kafka modes
   - Present performance results and visualizations
   - Highlight key findings

### Reference Information

- **For data preparation**: Follow the steps outlined in the Data Preprocessing section above
- **For understanding windowing**: Review the Time-Based Windowing Implementation section above
- **For Kafka setup**: See `KAFKA_SETUP.md` for detailed instructions
- **For running experiments**: See Performance Evaluation Experiments section above

---

## Core Streaming Features Implementation

### Overview

This section documents the implementation of core streaming features that were previously planned. The streaming job has been significantly enhanced with congestion index calculation, checkpointing, persistent output, and a configuration system.

### 1. Congestion Index Calculation

**File**: `src/stream_job.py` (lines 122-126, 133)

**What was implemented**:
- Added congestion index calculation to windowed aggregations
- Formula: `congestion_index = total_vehicles / avg_speed`
- Division by zero handling: Returns `None` when `avg_speed <= 0`
- Rounded to 2 decimal places for readability
- Included in final output columns alongside `avg_speed` and `total_vehicles`

**Implementation details**:
- Calculated after aggregations using `.withColumn()` to ensure proper order of operations
- Uses Spark's conditional logic (`F.when()`) to handle edge cases
- Final output includes `congestion_index` as a key metric for traffic analysis

**Why this was done**:
- Congestion index is a core requirement from the project proposal
- Provides a single metric that combines vehicle count and speed
- Higher values indicate more congestion (more vehicles relative to speed)
- Enables trend analysis and visualization of traffic patterns

### 2. Checkpointing for Fault Tolerance

**File**: `src/stream_job.py` (lines 141, 171, 53)

**What was implemented**:
- Separate checkpoint locations for each output sink:
  - Parquet sink: `checkpoints/traffic/parquet`
  - Metrics sink: `checkpoints/traffic/metrics`
- Automatic directory creation via `ensure_dirs()` function
- Checkpoint directories created before starting streaming queries

**Implementation details**:
- Each sink has its own checkpoint location to prevent conflicts
- Checkpointing enables recovery from job interruptions
- State is saved after each batch, allowing resumption from last processed point

**Why this was done**:
- Enables fault tolerance: job can recover from failures
- Remembers which data was already processed
- Required for production-like streaming applications
- Allows job to resume from last checkpoint instead of starting from scratch
- Critical for long-running streaming jobs that may encounter interruptions

### 3. Persistent Output Implementation

**File**: `src/stream_job.py` (lines 136-177)

**What was implemented**:
- Three parallel output sinks running simultaneously:
  1. **Parquet Sink** (lines 137-143):
     - Writes aggregated results to `outputs/traffic_parquet`
     - Uses `append` output mode to avoid duplicates
     - Checkpointed for fault tolerance
  2. **Console Sink** (lines 152-158):
     - Debug output with up to 200 rows displayed
     - Uses `append` output mode
     - Shows live streaming results for monitoring
  3. **Metrics CSV Sink** (lines 168-173):
     - Logs batch metrics to `outputs/metrics/metrics.csv`
     - Records timestamp, batch ID, and row count per batch
     - Enables performance analysis and monitoring

**Implementation details**:
- All three sinks run in parallel using separate streaming queries
- Each sink has independent checkpointing
- Metrics sink uses `foreachBatch` to write custom metrics
- Console output configured with `numRows=200` for better visibility

**Why this was done**:
- Console output is lost when terminal closes; files enable later analysis
- Parquet format is efficient for analytics and visualization
- Metrics logging enables performance evaluation experiments
- Multiple sinks allow both debugging (console) and persistence (files)
- Required for generating plots and conducting performance analysis

### 4. Configuration System

**File**: `config/stream.yaml` and `src/stream_job.py` (lines 5-51)

**What was implemented**:
- YAML-based configuration file (`config/stream.yaml`)
- Command-line argument support for overriding config values
- Configurable parameters:
  - `input_path`: Data source directory
  - `output_parquet`: Parquet output directory path
  - `metrics_csv`: Metrics log file path
  - `checkpoint_dir`: Base directory for checkpoints
  - `window`: Window size (e.g., "5 seconds")
  - `watermark`: Watermark duration (e.g., "20 seconds")
  - `trigger`: Processing trigger interval (e.g., "20 seconds")
  - `mode`: Output mode ("append", "update", or "complete")

**Implementation details**:
- Simple YAML parser that handles key-value pairs
- Command-line arguments take precedence over config file values
- All paths and directories are automatically created if they don't exist
- Default config file: `config/stream.yaml`

**Why this was done**:
- Makes the streaming job configurable without code changes
- Enables easy experimentation with different window sizes, triggers, and modes
- Command-line overrides allow runtime flexibility
- Separates configuration from code logic
- Makes it easier to run performance tests with different parameters

### 5. Enhanced Data Transformation

**File**: `src/stream_job.py` (lines 80-107)

**What was implemented**:
- Robust column name handling with `coalesce_any()` function
- Handles missing or blank values with `null_if_empty()` helper
- Maps PEMS dataset columns to canonical schema: `{timestamp, region, road_id, vehicle_count, speed}`
- Flexible column name matching (handles variations like "Total Flow", "Total_Flow", "TotalFlow")
- Type casting for numeric columns (vehicle_count as int, speed as double)

**Implementation details**:
- `null_if_empty()`: Converts empty strings to null values
- `coalesce_any()`: Tries multiple column name variations and uses the first available
- Handles schema variations across different CSV formats
- Maps District 7 to "LA" region, other districts to string representation

**Why this was done**:
- Makes the streaming job more robust to schema variations
- Handles edge cases like missing columns or blank values
- Provides a consistent canonical schema regardless of input format
- Enables the job to work with different datasets or CSV formats

### 6. Window Output Structure Enhancement

**File**: `src/stream_job.py` (lines 127-134)

**What was implemented**:
- Explicit window column selection and renaming:
  - `window.start` → `window_start`
  - `window.end` → `window_end`
- Final output columns: `region`, `window_start`, `window_end`, `avg_speed`, `total_vehicles`, `congestion_index`
- Clear, readable output format for analysis

**Why this was done**:
- Makes window boundaries explicit and easy to understand
- Enables time-based analysis and visualization
- Provides clear timestamps for each aggregated window
- Improves readability of output data

### 7. Watermark Configuration Update

**File**: `config/stream.yaml` and `src/stream_job.py` (line 113)

**What was changed**:
- Watermark increased from 10 seconds to 20 seconds
- Configurable via `config/stream.yaml` file
- Aligned with trigger interval for consistency

**Why this was done**:
- Larger watermark allows more time for late-arriving data
- Matches the 20-second trigger interval for better alignment
- Configurable watermark enables experimentation with different values

---

## Kafka Integration Implementation

### Overview

Kafka integration was implemented to provide an alternative to file-based streaming, enabling a more production-like distributed streaming architecture. This allows comparison between file-based and Kafka-based ingestion methods.

### 1. Docker-Based Kafka Setup

**Files**: `docker-compose.yml`, `scripts/kafka_setup.sh`, `KAFKA_SETUP.md`

**What was implemented**:
- Docker Compose configuration for Kafka and Zookeeper
- Helper script (`scripts/kafka_setup.sh`) for managing Kafka containers
- Comprehensive setup guide (`KAFKA_SETUP.md`) with troubleshooting

**Implementation details**:
- Kafka runs on port 9092, Zookeeper on port 2181
- Single-broker setup suitable for development
- Easy start/stop/management via helper script
- Topic creation and consumer testing utilities included

**Why this was done**:
- Docker provides consistent environment across different machines
- Easier setup for team members compared to local Kafka installation
- Isolated from system dependencies
- Can be easily shared and reproduced

### 2. Kafka Producer Implementation

**File**: `src/kafka_producer.py`

**What was implemented**:
- Python producer using `confluent-kafka` library
- Reads CSV chunks from `data/seeds/` directory
- Publishes each row as JSON message to Kafka topic
- Configurable message interval (default: 2 seconds)
- Progress logging (prints every 10 messages sent)

**Implementation details**:
- Uses `confluent-kafka.Producer` for message publishing
- Serializes CSV rows as JSON strings
- Handles connection errors gracefully
- Cycles through seed files continuously

**Why this was done**:
- Simulates real-time data stream from Kafka
- Enables comparison with file-based streaming
- Demonstrates producer-consumer pattern
- Provides alternative ingestion method as specified in project proposal

### 3. Kafka Consumer in Spark Streaming

**File**: `src/stream_job.py` (lines 87-118, 130-157)

**What was implemented**:
- Spark Structured Streaming Kafka connector integration
- Automatic Kafka connector JAR download via Spark packages
- JSON message parsing with schema validation
- Uses Kafka message timestamps for windowing (not data timestamps)
- Configurable via `config/stream_kafka.yaml`

**Implementation details**:
- Reads from Kafka topic using `.format("kafka")`
- Parses JSON messages using `from_json()` with PEMS schema
- Extracts Kafka message timestamp for time-based windowing
- Handles schema mismatches (all fields as StringType for JSON compatibility)
- Supports both file and Kafka modes via `input_mode` configuration

**Key technical decisions**:
- **Kafka timestamps for windowing**: Uses Kafka message timestamps instead of data timestamps (from 2006) to avoid watermark filtering issues
- **Schema flexibility**: All schema fields as StringType to handle JSON string values, with casting during transformation
- **Consumer group management**: Uses unique consumer group IDs to allow fresh reads

**Why this was done**:
- Enables production-like streaming architecture
- Demonstrates distributed messaging system integration
- Allows performance comparison between file and Kafka ingestion
- Meets project proposal requirement for optional Kafka integration

### 4. Kafka Test Consumer

**File**: `src/kafka_consumer_test.py`

**What was implemented**:
- Simple test consumer for debugging and verification
- Reads messages from Kafka topic
- Displays message structure and sample values
- Useful for troubleshooting message format issues

**Why this was done**:
- Helps verify producer is sending correct data
- Useful for debugging schema mismatches
- Quick way to inspect Kafka messages

### 5. Configuration for Kafka Mode

**File**: `config/stream_kafka.yaml`

**What was implemented**:
- Separate configuration file for Kafka mode
- Sets `input_mode: "kafka"`
- Configures Kafka connection settings (bootstrap servers, topic name)
- Same windowing and output settings as file mode

**Why this was done**:
- Easy switching between file and Kafka modes
- Keeps configurations separate and clear
- Allows independent tuning of Kafka-specific settings

---

## Updated Summary of Current State

### Completed

- Data preprocessing pipeline (convert, clean, transform)
- Chunk creation for streaming
- Time-based windowing (5-second windows)
- Windowed aggregations (avg_speed, total_vehicles)
- Congestion index calculation
- Checkpointing for fault tolerance
- Persistent output (Parquet + Console + Metrics CSV)
- Configuration system (YAML-based)
- **Kafka integration (Docker setup, producer, consumer)** ( newly completed )
- **Dual ingestion modes (file-based and Kafka-based)** ( newly completeld )
- 20-second trigger interval (now configurable)
- File cleanup in stream simulator
- Watermarking for late data handling (now configurable)
- Enhanced data transformation with robust column handling
- Multiple parallel output sinks
- Basic metrics logging (batch counts)

### Next Phase: Visualization and Performance Evaluation

#### 1. Visualization Layer

**Status**: Script exists (`scripts/visualize.py`) but needs verification and enhancement

**What needs to be done**:
- Verify visualization script works with current Parquet output format
- Generate required plots:
  - Congestion index over time per region
  - Average speed over time per region
  - Optional: Comparison across two window sizes (5s vs 10s)
- Ensure plots are properly formatted and labeled
- Test with actual output data

**Files to work with**:
- `scripts/visualize.py` - Existing visualization script
- `outputs/traffic_parquet/` - Source data for plots

#### 2. Performance Evaluation Experiments

**Status**: Not yet implemented

**What needs to be done**:
- Create experiment runner script to test different configurations
- Run systematic tests varying:
  - **Input rate**: 2s vs 5s ingestion interval
  - **Partitions**: 2, 4, 8 partitions
  - **Window size**: 5s vs 10s windows
- For each configuration, collect metrics:
  - Average batch duration (latency)
  - Processed records per second (throughput)
  - Total runtime
- Extract metrics from:
  - Spark query progress logs, OR
  - Existing metrics CSV (`outputs/metrics/metrics.csv`)
- Generate comparative plots showing:
  - Latency vs configuration parameters
  - Throughput vs configuration parameters
  - Performance trade-offs

**Approach**:
- Create `src/performance_experiments.py` script
- Automate running stream job with different configs
- Parse and aggregate metrics
- Generate comparison plots
- Document results

#### 3. Documentation Updates

**Status**: Partially complete

**What needs to be done**:
- Update README with performance experiment results (after completing experiments)
- Ensure all setup instructions are clear
- Document how to run experiments
- Add any missing usage examples

#### 4. Presentation/Video

**Status**: Not started

**What needs to be done**:
- Record 8-10 minute video covering:
  - System design overview
  - Demo of both file-based and Kafka-based streaming
  - Performance experiment results
  - Visualization results
  - Key findings and insights

