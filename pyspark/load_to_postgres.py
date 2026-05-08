from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("LoadToPostgres")
    .getOrCreate()
)

jdbc_url = "jdbc:postgresql://localhost:5432/fleet_analytics"
connection_props = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

tables = {
    "data_curated/dim_model": "dim_model",
    "data_curated/dim_car": "dim_car",
    "data_curated/fact_maintenance_summary": "fact_maintenance_summary",
    "data_curated/fact_active_issues": "fact_active_issues"
}

for parquet_path, table_name in tables.items():
    df = spark.read.parquet(parquet_path)
    df.write \
        .mode("overwrite") \
        .jdbc(jdbc_url, table_name, properties=connection_props)

spark.stop()