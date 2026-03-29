from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("NYC Taxi Historical").getOrCreate()

df = spark.read.parquet("data/yellow_tripdata_*.parquet")

df = df.withColumn(
    "trip_duration_minutes",
    (unix_timestamp("tpep_dropoff_datetime") -
     unix_timestamp("tpep_pickup_datetime")) / 60
)

df = df.filter(
    (col("trip_distance") > 0) &
    (col("fare_amount") > 0) &
    (col("passenger_count") > 0)
)

df = df.withColumn("year", year("tpep_pickup_datetime"))
df = df.withColumn("month", month("tpep_pickup_datetime"))

daily = df.groupBy(
    to_date("tpep_pickup_datetime").alias("trip_date"),
    "year",
    "month"
).agg(
    count("*").alias("total_trips"),
    sum("fare_amount").alias("total_fare")
)

daily.write.partitionBy("year", "month").parquet("output/daily_revenue")
