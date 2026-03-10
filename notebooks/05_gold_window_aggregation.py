# Databricks Notebook Source
# STEP 6 — Gold Layer Aggregation
#
# This pipeline computes streaming analytics from the Silver table
# using watermarking and tumbling window aggregations.


from pyspark.sql.functions import window, col


# ------------------------------------------------------------
# Read Silver Stream
# ------------------------------------------------------------

silver_stream = (
    spark.readStream
        .table("silver.customer")
)


# ------------------------------------------------------------
# Apply Watermark
# ------------------------------------------------------------

silver_stream = (
    silver_stream
        .withWatermark("ingest_timestamp", "45 minutes")
)


# ------------------------------------------------------------
# Tumbling Window Aggregation
# ------------------------------------------------------------

gold_stream = (
    silver_stream
        .groupBy(
            window(col("ingest_timestamp"), "30 minutes")
        )
        .count()
)


# ------------------------------------------------------------
# Write Gold Table
# ------------------------------------------------------------

checkpoint_path = "abfss://checkpoints@<storage_account>.dfs.core.windows.net/gold/customer_metrics/"


(
    gold_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .table("gold.customer_metrics")
)
