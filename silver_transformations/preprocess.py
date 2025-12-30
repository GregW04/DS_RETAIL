# Databricks notebook source
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

from utils_preproc import extend_sales_table, linear_interpolation, fill_time_series_with_dates

# COMMAND ----------

sales = spark.read.format("delta").load("/Volumes/workspace/favorita/bronze/sales")
items = spark.read.format("delta").load("/Volumes/workspace/favorita/bronze/items")
holidays = spark.read.format("delta").load("/Volumes/workspace/favorita/bronze/holidays")
stores = spark.read.format("delta").load("/Volumes/workspace/favorita/bronze/stores")
oil = spark.read.format("delta").load("/Volumes/workspace/favorita/bronze/oil")

# COMMAND ----------

ext_sales = extend_sales_table(sales, items, stores)
display(ext_sales)

# COMMAND ----------

oil_lin = linear_interpolation(data= oil, column_name='dcoilwtico', order_col='date')

# COMMAND ----------

def fill_time_series_with_dates(df, date_col):
    # 1) min/max
    mm = df.select(
        F.min(F.col(date_col)).alias("start"),
        F.max(F.col(date_col)).alias("end")
    ).first()

    start, end = mm["start"], mm["end"]

    # 2) pełny zakres dat (dzień po dniu)
    dates_df = (
        spark.createDataFrame([(start, end)], ["start", "end"])
            .select(F.explode(F.sequence(F.col("start"), F.col("end"))).alias(date_col))
    )

    # 3) left join z oryginalnymi danymi
    df_full = dates_df.join(df, on=date_col, how="left")
    return df_full

# COMMAND ----------

oil_full = fill_time_series_with_dates(oil, 'date')
oil_interpolated = linear_interpolation(data= oil_full, column_name='dcoilwtico', order_col='date')
display(oil_interpolated)

# COMMAND ----------


