# Databricks Notebook Source
# This pipeline ingests raw JSON files from the landing zone
# into Bronze Delta tables using Databricks Auto Loader.
#
# Features:
# - Incremental file ingestion
# - Schema inference
# - Schema evolution
# - File metadata tracking
# - Checkpointing

# ------------------------------------------------------------
# ADLS Paths
# ------------------------------------------------------------

landing_path = "abfss://raw@<storage_account>.dfs.core.windows.net/api_data/customer_api/"

schema_location = "abfss://schema@<storage_account>.dfs.core.windows.net/autoloader/customer_api/"

checkpoint_path = "abfss://checkpoints@<storage_account>.dfs.core.windows.net/bronze/customer_api/"


# ------------------------------------------------------------
# Auto Loader Stream
# ------------------------------------------------------------

bronze_stream = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("multiline", ""true)
        .option("cloudFiles.inferColumnTypes", "true")
        .load(landing_path)
)


# ------------------------------------------------------------
# Add Metadata Columns
# ------------------------------------------------------------

from pyspark.sql.functions import input_file_name, current_timestamp

bronze_stream = (
    bronze_stream
        .withColumn("source_file", input_file_name())
        .withColumn("ingest_timestamp", current_timestamp())
)


# ------------------------------------------------------------
# Write Bronze Table
# ------------------------------------------------------------

(
    bronze_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .table("bronze.customer_api_raw")
)
