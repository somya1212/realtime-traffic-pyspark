from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pathlib import Path

spark = SparkSession.builder.appName("TrafficStream").getOrCreate()

# Define a fixed schema based on our actual data columns in the LA dataset
schema = StructType([
    StructField("Timestamp", StringType(), True),
    StructField("Station", StringType(), True),
    StructField("District", IntegerType(), True),
    StructField("Freeway #", IntegerType(), True),
    StructField("Direction of Travel", StringType(), True),
    StructField("Lane Type", StringType(), True),
    StructField("Station Length", DoubleType(), True),
    StructField("Samples", IntegerType(), True),
    StructField("% Observed", DoubleType(), True),
    StructField("Total Flow", DoubleType(), True),
    StructField("Avg Occupancy", DoubleType(), True),
    StructField("Avg Speed", DoubleType(), True),
    StructField("region", StringType(), True)
])

# Read the streaming data
df = (spark.readStream
          .schema(schema)
          .option("header", True)
          .csv("data/incoming_stream"))

# Parse timestamp string to timestamp type
df = df.withColumn("timestamp", F.to_timestamp("Timestamp", "yyyy-MM-dd HH:mm:ss"))

# Add watermark (10 seconds) to handle late-arriving data
df = df.withWatermark("timestamp", "10 seconds")

# Group by 5-second windows and region
windowed = df.groupBy(
    F.window("timestamp", "5 seconds"), 
    "region"
)

# Aggregate within each window
out = windowed.agg(
    F.round(F.avg("Avg Speed"), 2).alias("avg_speed"),
    F.round(F.sum("Total Flow"), 2).alias("total_vehicles")
)

query = (out.writeStream
           .format("console")
           .outputMode("update")  # Changed to "update" for windowed aggregations
           .option("truncate", False)
           .trigger(processingTime="20 seconds")  # Check for new files every 20 seconds TODO: Make this configurable
           .start())

query.awaitTermination()
