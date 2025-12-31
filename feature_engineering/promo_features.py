# Databricks notebook source
import pyspark
import pyspark.sql.functions as F

from utils_promo import handle_missing_promo

# COMMAND ----------

# import from planning_period

# COMMAND ----------

promo_handled = handle_missing_promo(df = sales,
    promo_col = "onpromotion",
    strategy = "nan")
