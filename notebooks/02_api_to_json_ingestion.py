# Databricks Notebook Source
# STEP 3 — API to JSON Landing Zone Ingestion
#
# This notebook reads API configurations from the metadata table,
# calls REST APIs, and stores raw JSON responses in the landing zone.
#
# These JSON files will later be ingested into Bronze tables using
# Databricks Auto Loader.


# ------------------------------------------------------------
# Import Required Libraries
# ------------------------------------------------------------

import requests
import json
from datetime import datetime


# ------------------------------------------------------------
# Define Landing Zone Path
# ------------------------------------------------------------
# This path represents the raw ingestion layer where API data
# is stored before Bronze ingestion.

landing_zone = "/Volumes/data_platform/raw_api_data"


# ------------------------------------------------------------
# Read API Configuration Metadata
# ------------------------------------------------------------

api_configs = spark.sql("""
SELECT *
FROM platform_metadata.api_config
WHERE active_flag = true
""")

configs = api_configs.collect()

print(f"Found {len(configs)} APIs configured for ingestion")


# ------------------------------------------------------------
# Authentication Helper
# ------------------------------------------------------------

def get_auth_token(secret_scope, secret_key):
    """
    Retrieve API authentication token from Databricks Secret Scope
    """

    token = dbutils.secrets.get(
        scope=secret_scope,
        key=secret_key
    )

    return token


# ------------------------------------------------------------
# Pagination Handler
# ------------------------------------------------------------

def fetch_api_data(base_url, token, page_size):
    """
    Fetch paginated API data
    """

    headers = {
        "Authorization": f"Bearer {token}"
    }

    page = 1
    results = []

    while True:

        url = f"{base_url}?page={page}&size={page_size}"

        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print(f"API request failed: {response.status_code}")
            break

        data = response.json()

        if not data:
            break

        results.extend(data)

        page += 1

    return results


# ------------------------------------------------------------
# Main API Ingestion Loop
# ------------------------------------------------------------

for row in configs:

    api_name = row["api_name"]
    base_url = row["base_url"]
    page_size = row["page_size"]

    print(f"\nStarting ingestion for API: {api_name}")

    # Get authentication token
    token = get_auth_token(
        row["auth_secret_scope"],
        row["auth_secret_key"]
    )

    # Fetch API data
    api_data = fetch_api_data(base_url, token, page_size)

    if len(api_data) == 0:
        print(f"No data returned for {api_name}")
        continue


    # --------------------------------------------------------
    # Generate timestamped file name
    # --------------------------------------------------------

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    file_path = f"{landing_zone}/{api_name}/{timestamp}.json"


    # --------------------------------------------------------
    # Write JSON file to landing zone
    # --------------------------------------------------------

    dbutils.fs.put(
        file_path,
        json.dumps(api_data)
    
    )

    print(f"Stored raw JSON for {api_name}")
    print(f"File path: {file_path}")
