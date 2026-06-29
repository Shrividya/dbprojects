from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

spark = SparkSession.builder.getOrCreate()

source_table = "retail_lakehouse.bronze.taxi_trips_raw"
target_table = "retail_lakehouse.silver.taxi_trips_dedup"

df = spark.table(source_table)

window =  Window.partitionBy("VendorID","tpep_pickup_datetime","PULocationID","DOLocationID").orderBy("tpep_dropoff_datetime")

deduped= df.withColumn("row_num", row_number().over(window)).filter("row_num == 1").drop("row_num")

deduped.write.mode("overwrite").saveAsTable(target_table)
print(f"Source rows: {df.count()}, Deduped rows: {deduped.count()}")