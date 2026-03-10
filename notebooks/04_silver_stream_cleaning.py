# Databricks Notebook Source

# This pipeline reads incremental data from the Bronze table
# and writes cleaned records into the Silver layer.


from pyspark.sql.functions import col


# ------------------------------------------------------------
# Read Bronze Stream
# ------------------------------------------------------------

bronze_stream = (
    spark.readStream
        .table("bronze.customer_api_raw")
)


# ------------------------------------------------------------
# Data Cleansing & Column Standardization
# ------------------------------------------------------------

silver_stream = (
    bronze_stream
        .select(
            col("customer_id"),
            col("name").alias("customer_name"),
            col("email"),
            col("ingest_timestamp")
        )
)


# ------------------------------------------------------------
# Remove Duplicates
# ------------------------------------------------------------

silver_stream = silver_stream.dropDuplicates(["customer_id"])


# ------------------------------------------------------------
# Write to Silver Table
# ------------------------------------------------------------

checkpoint_path = "abfss://checkpoints@<storage_account>.dfs.core.windows.net/silver/customer/"

(
    silver_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .table("silver.customer")
)
