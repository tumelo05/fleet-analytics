from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, regexp_replace, to_date

spark = (
    SparkSession.builder
    .appName("IngestCarInfo")
    .getOrCreate()
)

car_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data_raw/Car_info_raw.csv")
)

# Clean malformed date strings
cleaned_df = (
    car_df
    .withColumn(
        "manufactured_date_clean",
        regexp_replace(col("manufactured_date"), "//", "/")
    )
)

dim_car_df = (
    cleaned_df
    .withColumn("car_id", upper(trim(col("license"))))
    .withColumn("manufacturer", upper(trim(col("manufacturer"))))
    .withColumn("model", upper(trim(col("name"))))
    .withColumn(
        "date_acquired",
        to_date(col("manufactured_date_clean"), "MM/dd/yyyy")
    )
    .select(
        "car_id",
        col("license").alias("license_plate"),
        "manufacturer",
        "model",
        "date_acquired",
        "owner_first_name",
        "owner_last_name"
    )
)

dim_car_df.write.mode("overwrite").parquet("data_curated/dim_car")

spark.stop()