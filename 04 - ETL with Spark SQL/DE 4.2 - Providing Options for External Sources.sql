

SELECT * FROM csv.`${DA.paths.sales_csv}`



CREATE TABLE sales_csv
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  header = "true",
  delimiter = "|"
)
LOCATION "${DA.paths.sales_csv}"


SELECT * FROM sales_csv



SELECT COUNT(*) FROM sales_csv



DESCRIBE EXTENDED sales_csv



SELECT COUNT(*) FROM sales_csv





REFRESH TABLE sales_csv


SELECT COUNT(*) FROM sales_csv



DROP TABLE IF EXISTS users_jdbc;

CREATE TABLE users_jdbc
USING JDBC
OPTIONS (
  url = "jdbc:sqlite:${DA.paths.ecommerce_db}",
  dbtable = "users"
)



SELECT * FROM users_jdbc


DESCRIBE EXTENDED users_jdbc

