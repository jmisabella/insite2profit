# Databricks notebook source
publish_orders = spark.table("publish_orders")
publish_product = spark.table("publish_product")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------

# Question 1: Which color generated the highest revenue each year?
# join orders to products to get Color, extract year from OrderDate
# revenue = sum of TotalLineExtendedPrice
# use window to rank colors by revenue per year, pick the top one
w = Window.partitionBy("year").orderBy(F.col("total_revenue").desc())

top_color_by_year = (
    publish_orders
    .join(publish_product, on="ProductID", how="inner")
    .withColumn("year", F.year(F.col("OrderDate")))
    .groupBy("year", "Color")
    .agg(F.sum("TotalLineExtendedPrice").alias("total_revenue"))
    .withColumn("rank", F.rank().over(w))
    .filter(F.col("rank") == 1)
    .drop("rank")
    .orderBy("year")
)

display(top_color_by_year)

# COMMAND ----------

# Question 2: Average LeadTimeInBusinessDays by ProductCategoryName
# join orders to products to get ProductCategoryName
avg_lead_time = (
    publish_orders
    .join(publish_product, on="ProductID", how="inner")
    .groupBy("ProductCategoryName")
    .agg(F.round(F.avg("LeadTimeInBusinessDays"), 2).alias("avg_lead_time_business_days"))
    .orderBy("ProductCategoryName")
)

display(avg_lead_time)

# COMMAND ----------

