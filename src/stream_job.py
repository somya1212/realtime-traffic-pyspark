from pyspark.sql import SparkSession, functions as F, types as T
import argparse, pathlib, sys, json, os, time
from datetime import datetime

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="config/stream.yaml")
    for k in ["input_path","output_parquet","metrics_csv","checkpoint_dir",
              "window","watermark","trigger","mode"]:
        p.add_argument(f"--{k}", default=None)
    return p.parse_args()

def read_yaml(path):
    text = pathlib.Path(path).read_text()
    items = []
    for ln in text.splitlines():
        ln = ln.strip()
        if not ln or ln.startswith("#") or ":" not in ln:
            continue
        k, v = ln.split(":", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        items.append(f'"{k}":"{v}"')
    return json.loads("{"+(",".join(items))+"}")

def ensure_dirs(*paths):
    for p in paths:
        if not p:
            continue
        d = pathlib.Path(p)
        (d if d.suffix == "" else d.parent).mkdir(parents=True, exist_ok=True)

def main():
    args = parse_args()
    cfg = read_yaml(args.config)


    for k in ["input_path","output_parquet","metrics_csv","checkpoint_dir",
              "window","watermark","trigger","mode","input_mode",
              "kafka_bootstrap_servers","kafka_topic"]:
        v = getattr(args, k, None)
        if v:
            cfg[k] = v

    input_mode = cfg.get("input_mode", "file")
    input_path = cfg.get("input_path", "data/incoming_stream")
    kafka_bootstrap = cfg.get("kafka_bootstrap_servers", "localhost:9092")
    kafka_topic = cfg.get("kafka_topic", "traffic-data")
    output_parquet = cfg["output_parquet"]
    metrics_csv    = cfg["metrics_csv"]
    checkpoint_dir = cfg["checkpoint_dir"]
    window         = cfg["window"]
    watermark      = cfg["watermark"]
    trigger        = cfg["trigger"]
    mode           = cfg["mode"]

    ensure_dirs(output_parquet, metrics_csv, checkpoint_dir)

    spark_builder = SparkSession.builder.appName("TrafficStreaming")
    
    
    if input_mode == "kafka":
        spark_builder = spark_builder.config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .config("spark.sql.streaming.kafka.useDeprecatedOffsetFetching", "false")
    
    spark = spark_builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")


      
    pems_schema = T.StructType([
        T.StructField("Timestamp",            T.StringType(),  True),
        T.StructField("Station",              T.StringType(),  True),
        T.StructField("District",             T.StringType(),  True),  # String for JSON parsing
        T.StructField("Freeway #",            T.StringType(),  True),
        T.StructField("Direction of Travel",  T.StringType(),  True),
        T.StructField("Lane Type",            T.StringType(),  True),
        T.StructField("Station Length",       T.StringType(),  True),
        T.StructField("Samples",              T.StringType(),  True),
        T.StructField("% Observed",           T.StringType(),  True),
        T.StructField("Total Flow",           T.StringType(),  True),
        T.StructField("Avg Occupancy",        T.StringType(),  True),
        T.StructField("Avg Speed",            T.StringType(),  True),
        T.StructField("Region",               T.StringType(),  True),
    ])

    if input_mode == "kafka":
        kafka_raw = (spark.readStream
                     .format("kafka")
                     .option("kafka.bootstrap.servers", kafka_bootstrap)
                     .option("subscribe", kafka_topic)
                     .option("startingOffsets", "earliest")
                     .option("kafka.consumer.group.id", f"spark-consumer-{int(time.time())}")
                     .load())
        
        
        raw = (kafka_raw
               .selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_ts")
               .withColumn("parsed", F.from_json("json_str", pems_schema))
               .filter(F.col("parsed").isNotNull())
               .select("parsed.*", "kafka_ts"))
    else:
        raw = (spark.readStream
               .format("csv")
               .option("header", True)
               .schema(pems_schema)
               .load(input_path))

    
    def null_if_empty(colname):
        return F.when(F.trim(F.col(colname)) == "", F.lit(None)).otherwise(F.col(colname))

    
    present = {f.name for f in raw.schema.fields}
    def coalesce_any(candidates):
        exprs = [ null_if_empty(c) for c in candidates if c in present ]
        return F.coalesce(*exprs) if exprs else F.lit(None)

    
    if input_mode == "kafka":
        
        df_5 = (raw
            .withColumn("road_id", F.col("Station").cast("string"))
            .withColumn("region",
                F.when(F.col("District").cast("int") == 7, F.lit("LA"))
                 .otherwise(F.col("District").cast("string"))
            )
            .withColumn("total_flow_any", coalesce_any([
                "Total Flow", "Total_Flow", "TotalFlow", "Flow", "Total Flow (Veh/5m)", "Total Flow (Veh/5min)"
            ]))
            .withColumn("avg_speed_any", coalesce_any([
                "Avg Speed", "Avg_Speed", "Average Speed", "Speed", "Speed (mph)"
            ]))
            .withColumn("vehicle_count", F.col("total_flow_any").cast("int"))
            .withColumn("speed",         F.col("avg_speed_any").cast("double"))
            .select("kafka_ts","region","road_id","vehicle_count","speed")
        )
        df = (df_5
              .withColumn("ts", F.col("kafka_ts"))
              .withWatermark("ts", watermark))
    else:
        
        df_5 = (raw
            .withColumn("timestamp_str", F.col("Timestamp"))
            .withColumn("road_id", F.col("Station").cast("string"))
            .withColumn("region",
                F.when(F.col("District").cast("int") == 7, F.lit("LA"))
                 .otherwise(F.col("District").cast("string"))
            )
            .withColumn("total_flow_any", coalesce_any([
                "Total Flow", "Total_Flow", "TotalFlow", "Flow", "Total Flow (Veh/5m)", "Total Flow (Veh/5min)"
            ]))
            .withColumn("avg_speed_any", coalesce_any([
                "Avg Speed", "Avg_Speed", "Average Speed", "Speed", "Speed (mph)"
            ]))
            .withColumn("vehicle_count", F.col("total_flow_any").cast("int"))
            .withColumn("speed",         F.col("avg_speed_any").cast("double"))
            .select("timestamp_str","region","road_id","vehicle_count","speed")
        )
        df = (df_5
              .withColumn("ts", F.to_timestamp("timestamp_str", "yyyy-MM-dd HH:mm:ss"))
              .filter(F.col("ts").isNotNull())
              .withWatermark("ts", watermark))
    
    
    def log_transform_stats(batch_df, batch_id):
        if batch_df.count() > 0:
            total = batch_df.count()
            has_ts = batch_df.filter(F.col("ts").isNotNull()).count()
            has_speed = batch_df.filter(F.col("speed").isNotNull()).count()
            has_count = batch_df.filter(F.col("vehicle_count").isNotNull()).count()
            print(f"DEBUG Transform Batch {batch_id}: total={total}, has_ts={has_ts}, has_speed={has_speed}, has_count={has_count}")
    
    transform_debug = (df.writeStream
                      .foreachBatch(log_transform_stats)
                      .outputMode("update")
                      .trigger(processingTime="5 seconds")
                      .start())


    agg = (df
           .groupBy(F.window("ts", window), "region")
           .agg(
               F.avg("speed").alias("avg_speed"),
               F.sum("vehicle_count").alias("total_vehicles")
           )
           .withColumn("congestion_index",
               F.when(F.col("avg_speed") > 0,
                      F.col("total_vehicles") / F.col("avg_speed"))
                .otherwise(F.lit(None)))
           .withColumn("congestion_index", F.round("congestion_index", 2))
           .select(
               "region",
               F.col("window.start").alias("window_start"),
               F.col("window.end").alias("window_end"),
               "avg_speed",
               "total_vehicles",
               "congestion_index"
           ))

    # Sink 1 (for persistent parquet with checkpoints)
    q1 = (agg.writeStream
          .format("parquet")
          .outputMode(mode)
          .option("path", output_parquet)
          .option("checkpointLocation", os.path.join(checkpoint_dir, "parquet"))
          .trigger(processingTime=trigger)
          .start())

    # Sink 2 (for console output)
    q2 = (agg.writeStream
    .outputMode("append")        
    .format("console")
    .option("truncate", False)
    .option("numRows", 200)      # show more rows
    .trigger(processingTime=trigger)
    .start())

    # Sink 3 (for metrics logger)
    def log_metrics(batch_df, batch_id):
        try:
            start = time.time()
            rows = batch_df.count()
            duration = time.time() - start
            throughput = rows / duration if duration > 0 else 0.0

            ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            config_label = f"{input_mode}|window={window}|trigger={trigger}"

            os.makedirs(os.path.dirname(metrics_csv), exist_ok=True)
            file_exists = os.path.exists(metrics_csv)

            with open(metrics_csv, "a") as f:
                if not file_exists:
                    f.write("timestamp,batch_id,rows,duration_sec,throughput_rows_per_sec,config\n")
                f.write(
                    f"{ts},{batch_id},{rows},{duration:.3f},{throughput:.3f},{config_label}\n"
                )

            print(
                f"[METRICS] config={config_label}, batch={batch_id}, "
                f"rows={rows}, duration={duration:.3f}s, "
                f"throughput={throughput:.3f} rows/s"
            )

        except Exception as e:
            # Don't let metrics kill your job
            print(f"[METRICS] error in batch {batch_id}: {e}")


    q3 = (df.writeStream
          .foreachBatch(log_metrics)
          .outputMode("append")
          .option("checkpointLocation", os.path.join(checkpoint_dir, "metrics"))
          .trigger(processingTime=trigger)
          .start())

    q1.awaitTermination()
    q2.awaitTermination()
    q3.awaitTermination()

if __name__ == "__main__":
    sys.exit(main())
