from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg

spark = SparkSession.builder \
    .appName("Fact_Loading") \
    .enableHiveSupport() \
    .getOrCreate()

# READ SILVER
df = spark.read.parquet("data_lake/silver/ev_population/")

# CREATE FACT TABLE DATA
fact_df = df.groupBy(
    "state", "city", "make", "model", "year", "month"
).agg(
    count("*").alias("vehicle_count"),
    avg("electric_range").alias("avg_electric_range")
)

# WRITE TO HDFS (for Hive)
fact_df.write \
    .mode("overwrite") \
    .parquet("data_lake/gold/fact_ev_adoption")

print("✅ Fact table created")

spark.stop()