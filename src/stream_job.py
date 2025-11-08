from pyspark.sql import SparkSession, functions as F
from src.utils import load_schema

spark = SparkSession.builder.appName("TrafficStream").getOrCreate()
schema = load_schema()

df = (spark.readStream
          .schema(schema)
          .option("header", True)
          .csv("data/incoming_stream"))

out = (df.groupBy("region")
         .agg(F.round(F.avg("speed"), 2).alias("avg_speed"),
              F.sum("vehicle_count").alias("total_vehicles")))

query = (out.writeStream
           .format("console")
           .outputMode("complete")
           .start())

query.awaitTermination()
