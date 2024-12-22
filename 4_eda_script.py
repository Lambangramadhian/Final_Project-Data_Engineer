from pyspark.sql import SparkSession
from single_output import single_file_output_to_output
import shutil
import glob
import os

spark = SparkSession.builder \
    .appName("spark_insert_postgres") \
    .getOrCreate()

df = spark.read.csv("/home/hadoop/final_project/csv_temp/global_product_sales.csv", header=True, inferSchema=True)
item_table = spark.read.csv("/home/hadoop/final_project/csv_temp/df_item_fact_table.csv", header=True, inferSchema=True)
country_table = spark.read.csv("/home/hadoop/final_project/csv_temp/df_country_fact_table.csv", header=True, inferSchema=True)


df.createOrReplaceTempView("global_sales")
item_table.createOrReplaceTempView("item_table")
country_table.createOrReplaceTempView("country_table")

# 1. TOTAL QUANTITY SOLD AND TOTAL SALES EACH MONTH IN 2011

sales = spark.sql(
    """
SELECT 
    YEAR(TO_DATE(transaction_date, 'yyyy-MM-dd')) as year,
    MONTH(TO_DATE(transaction_date, 'yyyy-MM-dd')) AS month,
    SUM(quantity) as total_quantity_sold,
    SUM(total_price) as total_sales
FROM
    global_sales
WHERE
    YEAR(TO_DATE(transaction_date, 'yyyy-MM-dd')) == 2011
GROUP BY
    YEAR(TO_DATE(transaction_date, 'yyyy-MM-dd')),
    MONTH(TO_DATE(transaction_date, 'yyyy-MM-dd'))
ORDER BY
    MONTH(TO_DATE(transaction_date, 'yyyy-MM-dd'))

"""
)

# 2. TOTAL MONTHLY SALES IN 2011 BASED ON COUNTRY

sales_2 = spark.sql(
    """
WITH sales_table AS (
    SELECT 
        YEAR(TO_DATE(transaction_date, 'yyyy-MM-dd')) as year,
        MONTH(TO_DATE(transaction_date, 'yyyy-MM-dd')) AS month,
        country_id,
        SUM(total_price) as total_sales
    FROM
        global_sales
    WHERE
        YEAR(TO_DATE(transaction_date, 'yyyy-MM-dd')) == 2011
    GROUP BY
        YEAR(TO_DATE(transaction_date, 'yyyy-MM-dd')),
        MONTH(TO_DATE(transaction_date, 'yyyy-MM-dd')),
        country_id
    ), 
sales_table_2 AS (
    SELECT 
        x.*,
        y.country
    FROM
        sales_table x
    LEFT JOIN
        country_table y
    ON
        x.country_id = y.id
    ORDER BY
        month
    )
SELECT
    month,
    country,
    total_sales
FROM
    sales_table_2
ORDER BY
    month

"""
)

# 3. ITEM WITH MOST SALES IN 2011

sales_3 = spark.sql(
    """
WITH sales_table AS (
    SELECT 
        YEAR(TO_DATE(transaction_date, 'yyyy-MM-dd')) as year,
        item_id,
        SUM(price) as total_sales
    FROM 
        global_sales
    WHERE
        YEAR(TO_DATE(transaction_date, 'yyyy-MM-dd')) = 2011
    GROUP BY
        YEAR(TO_DATE(transaction_date, 'yyyy-MM-dd')),
        item_id
    ),
    sales_table_2 AS (
    SELECT
        x.*,
        y.item_name
    FROM sales_table x
    LEFT JOIN item_table y
    ON x.item_id = y.id
    )
SELECT
    item_name,
    total_sales
FROM
    sales_table_2
ORDER BY
    total_sales DESC
LIMIT 10
"""
)

#4. MOST PURCHASED ITEM IN 2011

sales_4 = spark.sql(
        """
WITH sales_table AS (
    SELECT 
        YEAR(TO_DATE(transaction_date, 'yyyy-MM-dd')) as year,
        item_id,
        SUM(quantity) as total_purchased
    FROM 
        global_sales
    WHERE
        YEAR(TO_DATE(transaction_date, 'yyyy-MM-dd')) = 2011
    GROUP BY
        YEAR(TO_DATE(transaction_date, 'yyyy-MM-dd')),
        item_id
    ),
    sales_table_2 AS (
    SELECT
        x.*,
        y.item_name
    FROM sales_table x
    LEFT JOIN item_table y
    ON x.item_id = y.id
    )
SELECT
    item_name,
    total_purchased
FROM
    sales_table_2
ORDER BY
    total_purchased DESC
LIMIT 10
"""
)

# 5. CATEGORY PURCHASED IN 2011

sales_5 = spark.sql(
        """
WITH sales_table AS (
    SELECT 
        YEAR(TO_DATE(transaction_date, 'yyyy-MM-dd')) as year,
        item_id,
        SUM(quantity) as total_purchased
    FROM 
        global_sales
    WHERE
        YEAR(TO_DATE(transaction_date, 'yyyy-MM-dd')) = 2011
    GROUP BY
        YEAR(TO_DATE(transaction_date, 'yyyy-MM-dd')),
        item_id
    ),
    sales_table_2 AS (
    SELECT
        x.*,
        y.product_category AS category
    FROM sales_table x
    LEFT JOIN item_table y
    ON x.item_id = y.id
    )
SELECT
    category,
    SUM(total_purchased) AS total_category_purchased
FROM
    sales_table_2
GROUP BY
    category
ORDER BY
    SUM(total_purchased) DESC
"""
)

single_file_output_to_output(sales, "total_quantity_sold_and_sales")
single_file_output_to_output(sales_2, "monthly_sales_based_on_country")
single_file_output_to_output(sales_3, "top_10_item_with_most_sales")
single_file_output_to_output(sales_4, "top_10_item_with_most_purchase")
single_file_output_to_output(sales_5, "category_purchases_counts")


spark.stop()