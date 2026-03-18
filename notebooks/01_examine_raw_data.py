# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

def find_unique_columns(df: DataFrame, table_name: str = "") -> [str]:
    total_record_count = df.count()
    print(f"\n{table_name} - total count: {total_record_count}\n")
    unique_columns = []
    for col in df.columns:
        distinct_count = df.select(col).distinct().count()
        print(f"column: {col} | distinct count: {distinct_count}")
        is_unique = distinct_count == total_record_count
        if is_unique:
            unique_columns.append(col)
    return unique_columns




# COMMAND ----------

raw_products = spark.table("raw_products").distinct()
raw_sales_order_detail = spark.table("raw_sales_order_detail").distinct()
raw_sales_order_header = spark.table("raw_sales_order_header").distinct()

# COMMAND ----------

unique_product_columns = find_unique_columns(raw_products, "raw_products")
unique_sales_order_detail_columns = find_unique_columns(raw_sales_order_detail, "raw_sales_order_detail")
unique_sales_order_header_columns = find_unique_columns(raw_sales_order_header, "raw_sales_order_header")

# COMMAND ----------

print(f"unique_product_columns: {unique_product_columns}")
print(f"unique_sales_order_detail_columns: {unique_sales_order_detail_columns}")
print(f"unique_sales_order_header_columns: {unique_sales_order_header_columns}")

# COMMAND ----------

# strangely, `ProductID` is not unique in `raw_products`. Are there duplicates on `ProductID`?
(
    raw_products.groupBy("ProductID")
        .count()
        .filter(F.col("count") > 1)
        .orderBy("ProductID")
        .show()
)

# COMMAND ----------

display(raw_products.filter(F.col("ProductID") == 713))

# aha, `ProductCategoryName` and `ProductSubCategoryName` have different values for same `ProductID` 713:
#------------------------------------------------------
# ProductCategoryName    |    ProductSubCategoryName  |
#------------------------------------------------------
# null                   |    Jerseys                 |
# Clothing               |    Shirt                   |
# -----------------------------------------------------

# COMMAND ----------

display(raw_products.filter(F.col("ProductID") == 881))
# `ProductCategoryName` and `ProductSubCategoryName` have different values for same `ProductID` 881:
#------------------------------------------------------
# ProductCategoryName    |    ProductSubCategoryName  |
#------------------------------------------------------
# null                   |    Jerseys                 |
# Clothing               |    Shirt                   |
# -----------------------------------------------------

# COMMAND ----------

display(raw_products.filter(F.col("ProductID") == 716))
# `ProductCategoryName` and `ProductSubCategoryName` have different values for same `ProductID` 716:
#------------------------------------------------------
# ProductCategoryName    |    ProductSubCategoryName  |
#------------------------------------------------------
# null                   |    Jerseys                 |
# Clothing               |    Shirt                   |
# -----------------------------------------------------

# COMMAND ----------

display(raw_products.filter(F.col("ProductID") == 884))
# `ProductCategoryName` and `ProductSubCategoryName` have different values for same `ProductID` 884:
#------------------------------------------------------
# ProductCategoryName    |    ProductSubCategoryName  |
#------------------------------------------------------
# null                   |    Jerseys                 |
# Clothing               |    Shirt                   |
# -----------------------------------------------------

# COMMAND ----------

# `sales_order_header` unique key is `SalesOrderID`
# `sales_order_detail` unique key is `SalesOrderDetailID`
# `sales_order_detail` has `ProductID` which appears to be FK to `products`
# `sales_order_detail` has `SalesOrderID` which appears to be FK to `sales_order_header`
# `products` has duplicate `ProductID` with different `ProductCategoryName` and `ProductSubCategoryName`
# `store_products` should be deduped on `ProductID` where `ProductCategoryName` null

#### In terms of medallian architecture:
####    - raw_ tables are bronze
####    - store_ tables are silver
####        - have PKs and FKs, dedupe `products` where `ProductCategoryName` null when `ProductID` duplicated
####    - publish_ tables are gold
####        - have PKs and FKs, have transformations based on specified business logic

# COMMAND ----------

# DBTITLE 1,investigate plan to dedupe `products`
from pyspark.sql import functions as F
from pyspark.sql import Window

w = Window.partitionBy("ProductID").orderBy(F.col("ProductCategoryName").asc_nulls_last())

deduped_products = (
    raw_products
        .withColumn("row_number", F.row_number().over(w))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
)

unique_product_columns_from_deduped = find_unique_columns(deduped_products, "deduped_products")

# COMMAND ----------

print(f"unique_product_columns_from_deduped: {unique_product_columns_from_deduped}")


# COMMAND ----------

# DBTITLE 1,since deduping, this should no longer yield results
(
    deduped_products.groupBy("ProductID")
        .count()
        .filter(F.col("count") > 1)
        .orderBy("ProductID")
        .show()
)