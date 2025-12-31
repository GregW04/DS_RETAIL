# Databricks notebook source
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

from utils_cleaning import linear_interpolation, fill_time_series_with_dates

# COMMAND ----------

sales = spark.read.format("delta").load("/Volumes/workspace/favorita/bronze/sales")
items = spark.read.format("delta").load("/Volumes/workspace/favorita/bronze/items")
holidays = spark.read.format("delta").load("/Volumes/workspace/favorita/bronze/holidays")
stores = spark.read.format("delta").load("/Volumes/workspace/favorita/bronze/stores")
oil = spark.read.format("delta").load("/Volumes/workspace/favorita/bronze/oil")

# COMMAND ----------

oil_full = fill_time_series_with_dates(oil, 'date')
oil_interpolated = linear_interpolation(data= oil_full, column_name='dcoilwtico', order_col='date')

# COMMAND ----------

oil_interpolated.write.mode("overwrite").format("delta").save("/Volumes/workspace/favorita/silver/oil")

# for now only oil changes
sales.write.mode("overwrite").format("delta").save("/Volumes/workspace/favorita/silver/sales")
stores.write.mode("overwrite").format("delta").save("/Volumes/workspace/favorita/silver/stores")
items.write.mode("overwrite").format("delta").save("/Volumes/workspace/favorita/silver/items")
holidays.write.mode("overwrite").format("delta").save("/Volumes/workspace/favorita/silver/holidays")
