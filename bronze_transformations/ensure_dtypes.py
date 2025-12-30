# Databricks notebook source
import pyspark
import pyspark.sql.functions as F

# COMMAND ----------

sales = spark.read.format("delta").load("/Volumes/workspace/favorita/raw/sales")
items = spark.read.format("delta").load("/Volumes/workspace/favorita/raw/items")
holidays = spark.read.format("delta").load("/Volumes/workspace/favorita/raw/holidays")
stores = spark.read.format("delta").load("/Volumes/workspace/favorita/raw/stores")
oil = spark.read.format("delta").load("/Volumes/workspace/favorita/raw/oil")

# COMMAND ----------

sales_schema = {
    "id": "string",
    "date": "date",
    "store_nbr": "string",
    "item_nbr": "string",
    "unit_sales": "double",
    "onpromotion": "boolean",
}
items_schema = {
    "item_nbr": "string",
    "family": "string",
    "class": "string",
    "perishable": "boolean",
}
holidays_schema = {
    "date": "date",
    "type": "string",
    "locale": "string",
    "locale_name": "string",
    "description": "string",
    "transferred": "boolean",
}
stores_schema = {
    "store_nbr": "string",
    "city": "string",
    "state": "string",
    "type": "string",
    "cluster": "string"
}
oil_schema = {
    "date": "date",
    "dcoilwtico": "double"
}

# COMMAND ----------

sales_dt = sales.select(
    *[
        F.col(c).cast(sales_schema[c]).alias(c) if c in sales_schema else F.col(c)
        for c in sales.columns
    ]
)
items_dt = items.select(
    *[
        F.col(c).cast(items_schema[c]).alias(c) if c in items_schema else F.col(c)
        for c in items.columns
    ]
)
holidays_dt = holidays.select(
    *[
        F.col(c).cast(holidays_schema[c]).alias(c) if c in holidays_schema else F.col(c)
        for c in holidays.columns
    ]
)
stores_dt = stores.select(
    *[
        F.col(c).cast(stores_schema[c]).alias(c) if c in stores_schema else F.col(c)
        for c in stores.columns
    ]
)
oil_dt = oil.select(
    *[
        F.col(c).cast(oil_schema[c]).alias(c) if c in oil_schema else F.col(c)
        for c in oil.columns
    ]
)

# COMMAND ----------

sales_dt.write.mode("overwrite").format("delta").save("/Volumes/workspace/favorita/bronze/sales")
oil_dt.write.mode("overwrite").format("delta").save("/Volumes/workspace/favorita/bronze/oil")
stores_dt.write.mode("overwrite").format("delta").save("/Volumes/workspace/favorita/bronze/stores")
items_dt.write.mode("overwrite").format("delta").save("/Volumes/workspace/favorita/bronze/items")
holidays_dt.write.mode("overwrite").format("delta").save("/Volumes/workspace/favorita/bronze/holidays")
