# Databricks notebook source
import pyspark

# COMMAND ----------

train = spark.read.format("csv").option("header", "true").load("/Volumes/workspace/favorita/raw/train.csv")
oil = spark.read.format("csv").option("header", "true").load("/Volumes/workspace/favorita/raw/oil.csv")
stores = spark.read.format("csv").option("header", "true").load("/Volumes/workspace/favorita/raw/stores.csv")
items = spark.read.format("csv").option("header", "true").load("/Volumes/workspace/favorita/raw/items.csv")
holidays = spark.read.format("csv").option("header", "true").load("/Volumes/workspace/favorita/raw/holidays_events.csv")

# COMMAND ----------

train.write.mode("overwrite").format("delta").save("/Volumes/workspace/favorita/raw/sales")
oil.write.mode("overwrite").format("delta").save("/Volumes/workspace/favorita/raw/oil")
stores.write.mode("overwrite").format("delta").save("/Volumes/workspace/favorita/raw/stores")
items.write.mode("overwrite").format("delta").save("/Volumes/workspace/favorita/raw/items")
holidays.write.mode("overwrite").format("delta").save("/Volumes/workspace/favorita/raw/holidays")
