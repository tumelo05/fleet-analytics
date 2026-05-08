from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, to_date, lit, regexp_replace

spark = (
    SparkSession.builder
    .appName("IngestFactActiveIssues")
    .getOrCreate()
)

# -----------------------------------
# Read raw issues data
# -----------------------------------
issues_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data_raw/Issues_raw.csv")
)

# -----------------------------------
# Clean & standardise
# -----------------------------------
fact_active_issues_df = (
    issues_df
    .withColumn("issue_id", upper(trim(col("issue_id"))))
    .withColumn("car_id", upper(trim(col("car_id"))))
    .withColumn("issue_type", upper(trim(col("issue_title"))))
    .withColumn("issue_status", lit("ACTIVE"))
    .withColumn(
        "reported_date_clean",
        regexp_replace(col("reported_date"), "_", "-")
    )
    .withColumn(
        "date_reported",
        to_date(col("reported_date_clean"), "yyyy-MM-dd")
    )
    .select(
        "issue_id",
        "car_id",
        "issue_type",
        "issue_status",
        "date_reported"
    )
)

# -----------------------------------
# Write curated output
# -----------------------------------
fact_active_issues_df.write \
    .mode("overwrite") \
    .parquet("data_curated/fact_active_issues")

spark.stop()