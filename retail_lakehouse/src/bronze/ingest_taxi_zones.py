from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

source_path = "/Volumes/retail_lakehouse/bronze/raw_files/taxi_zones/taxi_zone.csv"
target_table = "retail_lakehouse.bronze.taxi_zones_raw"

df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(source_path)
)

df.write.mode("overwrite").saveAsTable(target_table)

print(f"Zones ingested: {df.count()} rows")
