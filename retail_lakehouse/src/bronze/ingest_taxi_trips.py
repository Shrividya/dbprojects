from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

volume_path = "/Volumes/retail_lakehouse/bronze/raw_files/taxi_trips"
checkpoint_path = "/Volumes/retail_lakehouse/bronze/raw_files/_checkpoints/taxi_trips"
target_table = "retail_lakehouse.bronze.taxi_trips_raw"

df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(volume_path)
)

(
    df.writeStream
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable(target_table)
)
