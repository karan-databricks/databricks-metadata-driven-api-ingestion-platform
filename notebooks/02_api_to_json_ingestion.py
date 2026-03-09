# Databricks Notebook Source
# Dynamic API ingestion engine for metadata-driven pipelines

import requests
import json
from pyspark.sql.functions import current_timestamp

# Read metadata config
api_configs = spark.sql("""
SELECT *
FROM platform_metadata.api_config
WHERE active_flag = true
""")

display(api_configs)

# Convert Metadata to Python Objects
configs = api_configs.collect()

# Authentication Function

def get_auth_token(secret_scope, secret_key):

    token = dbutils.secrets.get(
        scope=secret_scope,
        key=secret_key
    )

    return token

# Pagination Handler

def fetch_api_data(base_url, token, page_size):

    headers = {
        "Authorization": f"Bearer {token}"
    }

    page = 1
    results = []

    while True:

        url = f"{base_url}?page={page}&size={page_size}"

        response = requests.get(url, headers=headers)

        data = response.json()

        if len(data) == 0:
            break

        results.extend(data)

        page += 1

    return results

# Main Ingestion Loop

for row in configs:

    api_name = row["api_name"]
    base_url = row["base_url"]
    page_size = row["page_size"]
    target_table = row["target_table"]

    token = get_auth_token(
        row["auth_secret_scope"],
        row["auth_secret_key"]
    )

    api_data = fetch_api_data(base_url, token, page_size)

    df = spark.createDataFrame(api_data)

    df = df.withColumn("ingestion_timestamp", current_timestamp())

    df.write.format("delta") \
        .mode("append") \
        .saveAsTable(f"bronze.{target_table}")

    print(f"Ingested API: {api_name}")


