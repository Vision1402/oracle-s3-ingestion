-- =====================================================================
-- Athena: query the S3 raw layer with SQL, and validate it against Oracle
-- Run in the AWS console: Athena -> Query editor.
-- Replace YOUR-BUCKET-NAME everywhere. (Ticket MKT-7 uses a Glue crawler
-- instead of this DDL. Both create the same kind of catalog table.)
-- =====================================================================

CREATE DATABASE IF NOT EXISTS raw_oracle;

-- ---------- 1. Tables over the Parquet files ----------
CREATE EXTERNAL TABLE IF NOT EXISTS raw_oracle.customers (
  customer_id   bigint,
  first_name    string,
  last_name     string,
  email         string,
  phone         string,
  city          string,
  state         string,
  customer_type string,
  loyalty_tier  string,
  is_active     string,
  created_date  timestamp,
  last_updated  timestamp,
  _batch_id     string,
  _ingested_at  timestamp
)
PARTITIONED BY (load_date string)
STORED AS PARQUET
LOCATION 's3://YOUR-BUCKET-NAME/raw/oracle/northwind/customers/';

CREATE EXTERNAL TABLE IF NOT EXISTS raw_oracle.orders (
  order_id      bigint,
  customer_id   bigint,
  employee_id   bigint,
  order_date    timestamp,
  status        string,
  ship_city     string,
  notes         string,
  order_total   decimal(38,10),
  last_updated  timestamp,
  _batch_id     string,
  _ingested_at  timestamp
)
PARTITIONED BY (load_date string)
STORED AS PARQUET
LOCATION 's3://YOUR-BUCKET-NAME/raw/oracle/northwind/orders/';

-- Tell Athena which load_date=... folders exist (re-run after each new day)
MSCK REPAIR TABLE raw_oracle.customers;
MSCK REPAIR TABLE raw_oracle.orders;

-- ---------- 2. Validation: compare with the Oracle golden numbers ----------

-- Rows per load (first load should equal Oracle's COUNT(*))
SELECT load_date, _batch_id, COUNT(*) AS rows_loaded
FROM raw_oracle.orders
GROUP BY load_date, _batch_id
ORDER BY load_date, _batch_id;

-- Money must match Oracle exactly: SELECT SUM(order_total) FROM orders;  (in DBeaver)
SELECT SUM(order_total) AS total FROM raw_oracle.orders;

-- ---------- 3. The raw layer keeps HISTORY ----------
-- Incremental loads append every changed version of a row, so a customer
-- updated 3 times appears 3 times. That is correct for a raw layer.
SELECT customer_id, COUNT(*) AS versions
FROM raw_oracle.customers
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY versions DESC
LIMIT 20;

-- "Current state" of each customer = its latest version
SELECT *
FROM (
  SELECT c.*,
         ROW_NUMBER() OVER (PARTITION BY customer_id
                            ORDER BY last_updated DESC, _ingested_at DESC) AS rn
  FROM raw_oracle.customers c
) latest
WHERE rn = 1;

-- Current customer count must equal Oracle's COUNT(*) FROM customers
SELECT COUNT(DISTINCT customer_id) AS current_customers FROM raw_oracle.customers;
