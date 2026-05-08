from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, trim, upper

spark = (
    SparkSession.builder
    .appName("IngestDimModel")
    .getOrCreate()
)

# -----------------------------------
# Read raw model information
# -----------------------------------
model_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data_raw/Model_information_raw.csv")
)

# -----------------------------------
# Split make_model_id into components
# -----------------------------------
dim_model_df = (
    model_df
    .withColumn("manufacturer", upper(trim(split(col("make_model_id"), "-").getItem(0))))
    .withColumn("model", upper(trim(split(col("make_model_id"), "-").getItem(1))))
    .withColumn("fuel_type", upper(trim(col("fuel_type"))))
    .withColumn("transmission_type", upper(trim(col("transmission"))))
    .withColumn("manufacturer_country", upper(trim(col("manufacturer_country"))))
    .select(
        "manufacturer",
        "model",
        "fuel_type",
        "transmission_type",
        "manufacturer_country"
    )
    .dropDuplicates(["manufacturer", "model"])
)

# -----------------------------------
# Write curated output
# -----------------------------------
dim_model_df.write \
    .mode("overwrite") \
    .parquet("data_curated/dim_model")

spark.stop()