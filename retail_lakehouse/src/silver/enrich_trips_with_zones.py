from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.getOrCreate()

trips = spark.table("retail_lakehouse.silver.taxi_trips_dedup")
zones = spark.table("retail_lakehouse.bronze.taxi_zones_raw")

# Filter out corrupted timestamps - data is Jan-Mar 2025, anything wildly
# outside that range is a known TLC data entry/meter error, not a real trip.
trips = trips.filter(
    (col("tpep_pickup_datetime") >= "2024-11-01") &
    (col("tpep_pickup_datetime") <= "2025-04-30")
)

pickup_zones = zones.select(
    col("LocationID").alias("PULocationID"),
    col("Borough").alias("pickup_borough"),
    col("Zone").alias("pickup_zone"),
)

dropoff_zones = zones.select(
    col("LocationID").alias("DOLocationID"),
    col("Borough").alias("dropoff_borough"),
    col("Zone").alias("dropoff_zone"),
)

enriched = (
    trips
    .join(pickup_zones, on="PULocationID", how="left")
    .join(dropoff_zones, on="DOLocationID", how="left")
)

enriched = enriched.withColumn(
    "pickup_borough",
    when(col("pickup_borough").isin("N/A", "Unknown"), "Unclassified")
    .otherwise(col("pickup_borough"))
).withColumn(
    "dropoff_borough",
    when(col("dropoff_borough").isin("N/A", "Unknown"), "Unclassified")
    .otherwise(col("dropoff_borough"))
)

enriched.write.mode("overwrite").saveAsTable("retail_lakehouse.silver.taxi_trips_enriched")

unmatched_pickup = enriched.filter(col("pickup_borough").isNull()).count()
unmatched_dropoff = enriched.filter(col("dropoff_borough").isNull()).count()
removed_bad_dates = spark.table("retail_lakehouse.silver.taxi_trips_dedup").count() - trips.count()

print(f"Enriched rows: {enriched.count()}")
print(f"Removed rows with bad dates: {removed_bad_dates}")
print(f"Unmatched pickup zones: {unmatched_pickup}")
print(f"Unmatched dropoff zones: {unmatched_dropoff}")