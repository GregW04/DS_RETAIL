# Databricks notebook source
import pyspark
import pyspark.sql.functions as F

from utils_preproc import extend_sales_table

# COMMAND ----------

sales = spark.read.format("delta").load("/Volumes/workspace/favorita/bronze/sales")
items = spark.read.format("delta").load("/Volumes/workspace/favorita/bronze/items")
holidays = spark.read.format("delta").load("/Volumes/workspace/favorita/bronze/holidays")
stores = spark.read.format("delta").load("/Volumes/workspace/favorita/bronze/stores")
oil = spark.read.format("delta").load("/Volumes/workspace/favorita/bronze/oil")

# COMMAND ----------

ext_sales = extend_sales_table(sales, items, stores)
display(ext_sales)
