from pyspark.sql import SparkSession, functions as F, types as T
import argparse, pathlib, sys, json, os
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
              "window","watermark","trigger","mode"]:
        v = getattr(args, k)
        if v:
            cfg[k] = v

    input_path     = cfg["input_path"]
    output_parquet = cfg["output_parquet"]
    metrics_csv    = cfg["metrics_csv"]
    checkpoint_dir = cfg["checkpoint_dir"]
    window         = cfg["window"]
    watermark      = cfg["watermark"]
    trigger        = cfg["trigger"]
    mode           = cfg["mode"]

    ensure_dirs(output_parquet, metrics_csv, checkpoint_dir)

    spark = (SparkSession.builder.appName("TrafficStreaming").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")


    pems_schema = T.StructType([
        T.StructField("Timestamp",            T.StringType(),  True),
        T.StructField("Station",              T.StringType(),  True),
        T.StructField("District",             T.IntegerType(), True),
        T.StructField("Freeway #",            T.StringType(),  True),
        T.StructField("Direction of Travel",  T.StringType(),  True),
        T.StructField("Lane Type",            T.StringType(),  True),
        T.StructField("Station Length",       T.StringType(),  True),
        T.StructField("Samples",              T.StringType(),  True),
        T.StructField("% Observed",           T.StringType(),  True),
        T.StructField("Total Flow",           T.StringType(),  True),
        T.StructField("Avg Occupancy",        T.StringType(),  True),
        T.StructField("Avg Speed",            T.StringType(),  True),
    ])

    raw = (spark.readStream
           .format("csv")
           .option("header", True)
           .schema(pems_schema)
           .load(input_path))

    # Helper to handle missing or blank values
    def null_if_empty(colname):
        return F.when(F.trim(F.col(colname)) == "", F.lit(None)).otherwise(F.col(colname))

    # Build a safe coalesce over only the columns that actually exist in the schema
    present = {f.name for f in raw.schema.fields}
    def coalesce_any(candidates):
        exprs = [ null_if_empty(c) for c in candidates if c in present ]
        return F.coalesce(*exprs) if exprs else F.lit(None)

    # Map PEMS -> canonical {timestamp, region, road_id, vehicle_count, speed}
    df_5 = (raw
        .withColumn("timestamp_str", F.col("Timestamp"))
        .withColumn("road_id", F.col("Station").cast("string"))
        .withColumn("region",
            F.when(F.col("District") == 7, F.lit("LA"))
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

    # Sink 1 – persistent Parquet with checkpoints
    q1 = (agg.writeStream
          .format("parquet")
          .outputMode(mode)
          .option("path", output_parquet)
          .option("checkpointLocation", os.path.join(checkpoint_dir, "parquet"))
          .trigger(processingTime=trigger)
          .start())

    # Sink 2 – console output (live demo)
    # q2 = (agg.writeStream
    #       .outputMode("update")
    #       .format("console")
    #       .option("truncate", False)
    #       .trigger(processingTime=trigger)
    #       .start())
    q2 = (agg.writeStream
    .outputMode("append")        
    .format("console")
    .option("truncate", False)
    .option("numRows", 200)      # show more rows
    .trigger(processingTime=trigger)
    .start())

    # Sink 3 – metrics logger
    def log_metrics(batch_df, batch_id):
        rows = batch_df.count()
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        os.makedirs(os.path.dirname(metrics_csv), exist_ok=True)
        with open(metrics_csv, "a") as f:
            f.write(f"{ts},{batch_id},{rows}\n")

    q3 = (agg.writeStream
          .foreachBatch(log_metrics)
          .outputMode("update")
          .option("checkpointLocation", os.path.join(checkpoint_dir, "metrics"))
          .trigger(processingTime=trigger)
          .start())

    q1.awaitTermination()
    q2.awaitTermination()
    q3.awaitTermination()

if __name__ == "__main__":
    sys.exit(main())
