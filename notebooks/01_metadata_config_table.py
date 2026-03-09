%sql
-- This table stores API ingestion configurations.
-- The ingestion pipeline reads this metadata and dynamically
-- calls different APIs without hardcoding endpoints.
-- Databricks Notebook Source
-- This notebook defines metadata-driven API ingestion configuration
-- For the Databricks data platform.

CREATE SCHEMA IF NOT EXISTS platform_metadata;

CREATE TABLE IF NOT EXISTS platform_metadata.api_config
(
    api_name STRING,
    base_url STRING,
    pagination_type STRING,
    page_size INT,
    auth_type STRING,
    auth_secret_scope STRING,
    auth_secret_key STRING,
    target_table STRING,
    active_flag BOOLEAN
)
USING DELTA;


-- These entries simulate enterprise APIs.
-- In production, this table may contain dozens of APIs.

INSERT INTO platform_metadata.api_config
VALUES
(
"customer_api",
"https://mock-api.com/customers",
"page_number",
100,
"oauth2",
"api-secrets",
"customer-api-token",
"bronze_customers",
true
),
(
"transactions_api",
"https://mock-api.com/transactions",
"cursor",
200,
"oauth2",
"api-secrets",
"transactions-api-token",
"bronze_transactions",
true
);

-- SELECT * FROM platform_metadata.api_config;
