# Databricks notebook source
### store_ table keys
# `store_products` PK is `ProductID`
# `store_sales_order_header`.`SalesOrderID` is PK
# `store_sales_order_detail`.`SalesOrderDetailID` is PK
# `store_sales_order_detail`.`ProductID` is FK to `store_products
# `store_sales_order_detail`,`SalesOrderID` is FK to `store_sales_order_header`

# COMMAND ----------

from pyspark.sql import functions as F

store_products = spark.table("store_products").distinct()
store_sales_order_detail = spark.table("store_sales_order_detail").distinct()
store_sales_order_header = spark.table("store_sales_order_header").distinct()

# COMMAND ----------

# DBTITLE 1,product transformations
# 1. Replace NULL values in the Color field with N/A.
# 2. Enhance the ProductCategoryName field when it is NULL using the following logic:
#     i.   If ProductSubCategoryName is in ('Gloves', 'Shorts', 'Socks', 'Tights', 'Vests')
#          Then set ProductCategoryName to 'Clothing'.
#     ii.  If ProductSubCategoryName is in:
#              ('Locks', 'Lights', 'Headsets', 'Helmets', 'Pedals', 'Pumps'):
#          Then set ProductCategoryName to 'Accessories'.
#     iii. If ProductSubCategoryName contains the word 'Frames' or is in ('Wheels', 'Saddles'):
#          Then set ProductCategoryName to 'Components'.

publish_product = (
    store_products
    .withColumn("Color",
        F.when(F.col("Color").isNull(), "N/A")
        .otherwise(F.col("Color"))
    )
    .withColumn("ProductCategoryName",
        F.when(F.col("ProductCategoryName").isNotNull(), F.col("ProductCategoryName"))
        .when(F.col("ProductSubCategoryName").isin("Gloves", "Shorts", "Socks", "Tights", "Vests"), "Clothing")
        .when(F.col("ProductSubCategoryName").isin("Locks", "Lights", "Headsets", "Helmets", "Pedals", "Pumps"), "Accessories")
        .when(F.col("ProductSubCategoryName").contains("Frames") | F.col("ProductSubCategoryName").isin("Wheels", "Saddles"), "Components")
        .otherwise(None)
    )
)

publish_product.write.mode("overwrite").saveAsTable("publish_product")

# COMMAND ----------

# DBTITLE 1,order transformation
# Join SalesOrderDetail with SalesOrderHeader on SalesOrderId and apply the following transformations:
# 1. Calculate LeadTimeInBusinessDays as the difference between OrderDate and ShipDate, excluding Saturdays and Sundays.
# 2. Calculate TotalLineExtendedPrice using the formula: OrderQty * (UnitPrice - UnitPriceDiscount).
# 3. Write the results into a table named publish_orders, including:
# 4. All fields from SalesOrderDetail.
# 5. All fields from SalesOrderHeader except SalesOrderId, and rename Freight to TotalOrderFreight.

# join detail to header
joined = store_sales_order_detail.alias("d").join(
    store_sales_order_header.alias("h"),
    on="SalesOrderID",
    how="inner"
)

publish_orders = (
    joined
    # count weekdays between OrderDate (inclusive) and ShipDate (exclusive)
    # sequence() generates an array of dates between the two dates
    # aggregate() iterates over the array, accumulating +1 for each weekday
    # dayofweek: 1=Sunday, 2=Monday, ... 6=Friday, 7=Saturday
    # so "not in (1, 7)" means Monday through Friday only
    .withColumn("LeadTimeInBusinessDays",
        F.expr("""
            aggregate(
                sequence(OrderDate, ShipDate - interval 1 day),
                0,
                (acc, d) -> acc + case when dayofweek(d) not in (1, 7) then 1 else 0 end
            )
        """)
    )
    # TotalLineExtendedPrice = quantity * (unit price after discount)
    .withColumn("TotalLineExtendedPrice",
        F.col("d.OrderQty") * (F.col("d.UnitPrice") - F.col("d.UnitPriceDiscount"))
    )
    .select(
        # all fields from SalesOrderDetail
        F.col("d.SalesOrderID"),
        F.col("d.SalesOrderDetailID"),
        F.col("d.OrderQty"),
        F.col("d.ProductID"),
        F.col("d.UnitPrice"),
        F.col("d.UnitPriceDiscount"),
        F.col("TotalLineExtendedPrice"),
        F.col("LeadTimeInBusinessDays"),
        # all fields from SalesOrderHeader except SalesOrderID (already included from detail)
        # Freight renamed to TotalOrderFreight per requirements
        F.col("h.OrderDate"),
        F.col("h.ShipDate"),
        F.col("h.OnlineOrderFlag"),
        F.col("h.AccountNumber"),
        F.col("h.CustomerID"),
        F.col("h.SalesPersonID"),
        F.col("h.Freight").alias("TotalOrderFreight")
    )
)

publish_orders.write.mode("overwrite").saveAsTable("publish_orders")