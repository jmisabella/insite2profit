# Databricks notebook source
### Key Findings from Raw Data Analysis:
# `sales_order_header` unique key is `SalesOrderID`
# `sales_order_detail` unique key is `SalesOrderDetailID`
# `sales_order_detail` has `ProductID` which appears to be FK to `products`
# `sales_order_detail` has `SalesOrderID` which appears to be FK to `sales_order_header`
# `products` has duplicate `ProductID` with different `ProductCategoryName` and `ProductSubCategoryName`
# `store_products` should be deduped on `ProductID` where `ProductCategoryName` null

#### so in terms of medallian architecture:
####    - raw_ tables are bronze, preserved for audit
####    - store_ tables are silver
####        - have PKs and FKs, dedupe `products` where `ProductCategoryName` null when `ProductID` duplicated
####    - publish_ tables are gold
####        - have PKs and FKs, have transformations based on specified business logic

# COMMAND ----------

raw_products = spark.table("raw_products").distinct()
raw_sales_order_detail = spark.table("raw_sales_order_detail").distinct()
raw_sales_order_header = spark.table("raw_sales_order_header").distinct()

# COMMAND ----------

# DBTITLE 1,`store_products` is only table needing deduping
from pyspark.sql import functions as F
from pyspark.sql import Window

w = Window.partitionBy("ProductID").orderBy(F.col("ProductCategoryName").asc_nulls_last())

store_products = (
    raw_products
    .withColumn("row_number", F.row_number().over(w))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)

store_products.write.mode("overwrite").saveAsTable("store_products")

spark.sql("ALTER TABLE store_products ALTER COLUMN ProductID SET NOT NULL")

# drop constraint if already exists, making notebook idempotent
spark.sql("""
    ALTER TABLE store_products
    DROP CONSTRAINT IF EXISTS pk_store_products
""")

spark.sql("""
    ALTER TABLE store_products
    ADD CONSTRAINT pk_store_products
    PRIMARY KEY (ProductID)
""")


# COMMAND ----------

# DBTITLE 1,store_sales_order_header
raw_sales_order_header.write.mode("overwrite").saveAsTable("store_sales_order_header")

spark.sql("ALTER TABLE store_sales_order_header ALTER COLUMN SalesOrderID SET NOT NULL")

# drop constraint if already exists, making notebook idempotent
spark.sql("""
    ALTER TABLE store_sales_order_header
    DROP CONSTRAINT IF EXISTS pk_store_sales_order_header
""")

spark.sql("""
    ALTER TABLE store_sales_order_header
    ADD CONSTRAINT pk_store_sales_order_header
    PRIMARY KEY (SalesOrderID)
""")


# COMMAND ----------

# DBTITLE 1,store_sales_order_detail last since it has FKs
raw_sales_order_detail.write.mode("overwrite").saveAsTable("store_sales_order_detail")

spark.sql("ALTER TABLE store_sales_order_detail ALTER COLUMN SalesOrderDetailID SET NOT NULL")

# drop constraints if they already exist, making notebook idempotent
spark.sql("""
    ALTER TABLE store_sales_order_detail
    DROP CONSTRAINT IF EXISTS pk_store_sales_order_detail
""")
spark.sql("""
    ALTER TABLE store_sales_order_detail
    DROP CONSTRAINT IF EXISTS fk_store_sales_order_header
""")
spark.sql("""
    ALTER TABLE store_sales_order_detail
    DROP CONSTRAINT IF EXISTS fk_store_products;
""")

spark.sql("""
    ALTER TABLE store_sales_order_detail
    ADD CONSTRAINT pk_store_sales_order_detail
    PRIMARY KEY (SalesOrderDetailID)
""")

spark.sql("""
    ALTER TABLE store_sales_order_detail
    ADD CONSTRAINT fk_store_sales_order_header
    FOREIGN KEY (SalesOrderID) REFERENCES store_sales_order_header(SalesOrderID)
""")

spark.sql("""
    ALTER TABLE store_sales_order_detail
    ADD CONSTRAINT fk_store_products
    FOREIGN KEY (ProductID) REFERENCES store_products(ProductID)
""")


# COMMAND ----------



# COMMAND ----------

