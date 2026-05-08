from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, min, max, to_date

spark = (
    SparkSession.builder
    .appName("AggregateMaintenance")
    .getOrCreate()
)

# -----------------------------------
# Read raw maintenance data
# -----------------------------------
maintenance_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data_raw/Historical_maintenance_log_raw.csv")
)

# -----------------------------------
# Parse date and aggregate
# -----------------------------------
fact_maintenance_summary_df = (
    maintenance_df
    .withColumn(
        "maintenance_date_parsed",
        to_date(col("maintenance_date"), "yyyy-MM-dd")
    )
    .groupBy("car_id")
    .agg(
        count("*").alias("total_maintenance_events"),
        min("maintenance_date_parsed").alias("first_maintenance_date"),
        max("maintenance_date_parsed").alias("last_maintenance_date")
    )
)

# -----------------------------------
# Write curated output
# -----------------------------------
fact_maintenance_summary_df.write \
    .mode("overwrite") \
    .parquet("data_curated/fact_maintenance_summary")

spark.stop()