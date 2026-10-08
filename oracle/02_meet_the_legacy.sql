-- =====================================================================
-- PROJECT 3 - PHASE 0 - Script 02: meet the legacy system
-- Run these ONE AT A TIME in DBeaver (connected as NORTHWIND).
-- Each query reveals a trap that will bite during the migration.
-- Write down what you see: this becomes your Phase 1 assessment.
-- =====================================================================

-- 1. What's in here? (row counts, from the stats we gathered)
SELECT table_name, num_rows FROM user_tables ORDER BY num_rows DESC;

-- 2. TRAP: Oracle DATE secretly holds a time of day
--    DBeaver may show only the date part. TO_CHAR reveals the truth.
SELECT order_id,
       order_date,
       TO_CHAR(order_date, 'YYYY-MM-DD HH24:MI:SS') AS what_is_really_stored
FROM orders
FETCH FIRST 5 ROWS ONLY;

-- 3. TRAP: an empty string is NULL in Oracle
--    The generator inserted '' as the phone for every 10th customer. Where did they go?
SELECT COUNT(*) AS phone_equals_empty FROM customers WHERE phone = '';     -- 0 rows!
SELECT COUNT(*) AS phone_is_null      FROM customers WHERE phone IS NULL;  -- ~200 rows

-- 4. TRAP: NUMBER with no precision holds more decimals than money "should"
SELECT order_id, order_total
FROM orders
WHERE order_total <> ROUND(order_total, 2)
FETCH FIRST 5 ROWS ONLY;

SELECT COUNT(*) AS totals_with_more_than_2_decimals
FROM orders WHERE order_total <> ROUND(order_total, 2);

-- 5. TRAP: placeholder dates ("we didn't know, so we typed 1900")
SELECT customer_id, first_name, last_name, created_date
FROM customers
WHERE created_date < DATE '1950-01-01';

-- 6. TRAP: text that breaks CSV files (commas, quotes, line breaks, 6,000-char notes)
SELECT order_id, notes
FROM orders
WHERE INSTR(notes, CHR(10)) > 0
FETCH FIRST 3 ROWS ONLY;

SELECT order_id, DBMS_LOB.GETLENGTH(notes) AS note_length
FROM orders
WHERE DBMS_LOB.GETLENGTH(notes) > 4000;

-- 7. TRAP: accented names and apostrophes (encoding + quoting problems)
SELECT DISTINCT last_name
FROM customers
WHERE last_name LIKE '%''%' OR last_name <> ASCIISTR(last_name);

-- 8. TRAP: || treats NULL as '' in Oracle (Snowflake returns NULL instead!)
SELECT customer_id, 'Email: ' || email AS email_label
FROM customers
WHERE email IS NULL
FETCH FIRST 5 ROWS ONLY;

-- 9. DATA QUALITY: orders placed BEFORE the customer was created?
SELECT COUNT(*) AS orders_before_customer_existed
FROM orders o JOIN customers c ON c.customer_id = o.customer_id
WHERE o.order_date < c.created_date;

-- 10. The code that lives INSIDE the database (this must be migrated too)
SELECT object_name, object_type, status
FROM user_objects
WHERE object_type IN ('PROCEDURE','TRIGGER','VIEW','SEQUENCE')
ORDER BY object_type, object_name;

-- Read the source code of the loyalty procedure
SELECT line, text FROM user_source
WHERE name = 'CALC_LOYALTY_TIERS' ORDER BY line;

-- 11. The nightly job and its log
SELECT job_name, repeat_interval, enabled, state, next_run_date
FROM user_scheduler_jobs;

SELECT * FROM batch_log ORDER BY log_id;

-- 12. The business output we must reproduce EXACTLY in Snowflake later
SELECT loyalty_tier, COUNT(*) AS customers
FROM customers
GROUP BY loyalty_tier
ORDER BY customers DESC;

-- 13. Sensitive data that needs protecting in Snowflake
SELECT employee_id, first_name, last_name, job_title, national_id, salary
FROM employees
ORDER BY salary DESC
FETCH FIRST 5 ROWS ONLY;
