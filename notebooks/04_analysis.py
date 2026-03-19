# Databricks notebook source
publish_orders = spark.table("publish_orders")
publish_product = spark.table("publish_product")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------

# Question 1: Which color generated the highest revenue each year?
# Join orders to products to get Color, extract year from OrderDate.
# revenue = sum of TotalLineExtendedPrice
# Use window to rank colors by revenue per year, pick the top one
window = Window.partitionBy("year").orderBy(F.col("total_revenue").desc())

top_color_by_year = (
    publish_orders
    .join(publish_product, on="ProductID", how="inner")
    .withColumn("year", F.year(F.col("OrderDate")))
    .groupBy("year", "Color")
    .agg(F.round(F.sum("TotalLineExtendedPrice"), 2).alias("total_revenue"))
    .withColumn("rank", F.rank().over(window))
    .filter(F.col("rank") == 1)
    .drop("rank")
    .orderBy("year")
)

#----------------------------------
# | year | Color  | total_revenue |
# ---------------------------------
# | 2021 | Red    |  6019614.02   |
# | 2022 | Black  | 14005242.98   |
# | 2023 | Black  | 15047694.37   |
# | 2024 | Yellow |  6368158.48   |
# --------------------------------- 
display(top_color_by_year)

# COMMAND ----------

# Question 2: Average LeadTimeInBusinessDays by ProductCategoryName
# join orders to products to get ProductCategoryName
avg_lead_time = (
    publish_orders
    .join(publish_product, on="ProductID", how="inner")
    .groupBy("ProductCategoryName")
    .agg(F.avg("LeadTimeInBusinessDays").alias("avg_lead_time_business_days"))
    .orderBy("ProductCategoryName")
)

#------------------------------------------------------ 
# | ProductCategoryName | avg_lead_time_business_days |
#------------------------------------------------------ 
# | null                | 5.010809231668127           |
# | Accessories         | 5.0065279164426695          |
# | Bikes               | 5.004896845147306           |
# | Clothing            | 5.005067001675042           |
# | Components          | 5.0032938844517             |
#------------------------------------------------------ 
display(avg_lead_time)

# COMMAND ----------

