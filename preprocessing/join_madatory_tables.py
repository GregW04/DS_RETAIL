# Databricks notebook source
import pyspark
import pyspark.sql.functions as F
from utils_preproc import extend_sales_table

# temp
from utils_holidays import preprocess_holidays, merge_holiday_signals

# COMMAND ----------

sales = spark.read.format("delta").load("/Volumes/workspace/favorita/silver/sales")
items = spark.read.format("delta").load("/Volumes/workspace/favorita/silver/items")
holidays = spark.read.format("delta").load("/Volumes/workspace/favorita/silver/holidays")
stores = spark.read.format("delta").load("/Volumes/workspace/favorita/silver/stores")
oil = spark.read.format("delta").load("/Volumes/workspace/favorita/silver/oil")

# COMMAND ----------

# parameters
oil_col = 'smoothed_dcoilwtico'
date_col = 'date'

# COMMAND ----------

display(sales.select(F.min(date_col)))
display(sales.select(F.max(date_col)))

# COMMAND ----------

# add split data
sales_2013_2014 = sales.filter((F.col(date_col) >= '2013-01-01') & (F.col(date_col) <= '2014-12-31'))
display(sales_2013_2014)

# COMMAND ----------

sales_ext = extend_sales_table(sales_2013_2014, items, stores)

# COMMAND ----------

sales_ext_oil = sales_ext.join(oil.select(date_col, oil_col), on=date_col, how='left')
display(sales_ext_oil)

# COMMAND ----------

# temp
holidays_preproc = preprocess_holidays(holidays)
sales_ext_oil_holidays = merge_holiday_signals(sales_ext_oil, holidays_preproc)
display(sales_ext_oil_holidays)

# COMMAND ----------

# save in planning_period

# COMMAND ----------

sales_ext_oil_holidays.write.mode("overwrite").format("parquet").save("/Volumes/workspace/favorita/test1/sales")

# for now changes only to sales
oil.write.mode("overwrite").format("parquet").save("/Volumes/workspace/favorita/test1/oil")
stores.write.mode("overwrite").format("parquet").save("/Volumes/workspace/favorita/test1/stores")
items.write.mode("overwrite").format("parquet").save("/Volumes/workspace/favorita/test1/items")
holidays.write.mode("overwrite").format("parquet").save("/Volumes/workspace/favorita/test1/holidays")
