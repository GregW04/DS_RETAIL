# Databricks notebook source
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, ByteType, ShortType

from utils_holidays import preprocess_holidays, merge_holiday_signals

# COMMAND ----------

holidays_preproc = preprocess_holidays(holidays)
sales_holidays = merge_holiday_signals(sales, holidays_preproc)
display(sales_holidays)
