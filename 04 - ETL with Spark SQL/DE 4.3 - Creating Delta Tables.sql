

CREATE OR REPLACE TABLE sales AS
SELECT * FROM parquet.`${da.paths.datasets}/ecommerce/raw/sales-historical`;

DESCRIBE EXTENDED sales;



CREATE OR REPLACE TABLE sales_unparsed AS
SELECT * FROM csv.`${da.paths.datasets}/ecommerce/raw/sales-csv`;

SELECT * FROM sales_unparsed;



CREATE OR REPLACE TEMP VIEW sales_tmp_vw
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  path = "${da.paths.datasets}/ecommerce/raw/sales-csv",
  header = "true",
  delimiter = "|"
);

CREATE TABLE sales_delta AS
  SELECT * FROM sales_tmp_vw;
  
SELECT * FROM sales_delta



CREATE OR REPLACE TABLE purchases AS
SELECT order_id AS id, transaction_timestamp, purchase_revenue_in_usd AS price
FROM sales;

SELECT * FROM purchases


CREATE OR REPLACE VIEW purchases_vw AS
SELECT order_id AS id, transaction_timestamp, purchase_revenue_in_usd AS price
FROM sales;

SELECT * FROM purchases_vw


CREATE OR REPLACE TABLE purchase_dates (
  id STRING, 
  transaction_timestamp LONG, 
  price STRING,
  date DATE GENERATED ALWAYS AS (
    cast(cast(transaction_timestamp/1e6 AS TIMESTAMP) AS DATE))
    COMMENT "generated based on `transactions_timestamp` column")



SET spark.databricks.delta.schema.autoMerge.enabled=true; 

MERGE INTO purchase_dates a
USING purchases b
ON a.id = b.id
WHEN NOT MATCHED THEN
  INSERT *



SELECT * FROM purchase_dates



ALTER TABLE purchase_dates ADD CONSTRAINT valid_date CHECK (date > '2020-01-01');



DESCRIBE EXTENDED purchase_dates



CREATE OR REPLACE TABLE users_pii
COMMENT "Contains PII"
LOCATION "${da.paths.working_dir}/tmp/users_pii"
PARTITIONED BY (first_touch_date)
AS
  SELECT *, 
    cast(cast(user_first_touch_timestamp/1e6 AS TIMESTAMP) AS DATE) first_touch_date, 
    current_timestamp() updated,
    input_file_name() source_file
  FROM parquet.`${da.paths.datasets}/ecommerce/raw/users-historical/`;
  
SELECT * FROM users_pii;



DESCRIBE EXTENDED users_pii



CREATE OR REPLACE TABLE purchases_clone
DEEP CLONE purchases


CREATE OR REPLACE TABLE purchases_shallow_clone
SHALLOW CLONE purchases


-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
