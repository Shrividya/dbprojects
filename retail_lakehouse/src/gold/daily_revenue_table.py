from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, sum as spark_sum, count as spark_count

spark = SparkSession.builder.getOrCreate()

trips = spark.table("retail_lakehouse.silver.taxi_trips_enriched")

daily_revenue = (
    trips
    .withColumn("trip_date", to_date("tpep_pickup_datetime"))
    .groupBy("trip_date", "pickup_borough")
    .agg(
        spark_sum("total_amount").alias("total_revenue"),
        spark_count("*").alias("trip_count")
    )
)

daily_revenue.write.mode("overwrite").saveAsTable("retail_lakehouse.gold.daily_revenue_table")

print(f"Gold rows: {daily_revenue.count()}")
