# Databricks notebook source
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

from utils_preproc import extend_sales_table, linear_interpolation, fill_time_series_with_dates, handle_missing_promo

# COMMAND ----------

sales = spark.read.format("delta").load("/Volumes/workspace/favorita/bronze/sales")
items = spark.read.format("delta").load("/Volumes/workspace/favorita/bronze/items")
holidays = spark.read.format("delta").load("/Volumes/workspace/favorita/bronze/holidays")
stores = spark.read.format("delta").load("/Volumes/workspace/favorita/bronze/stores")
oil = spark.read.format("delta").load("/Volumes/workspace/favorita/bronze/oil")

# COMMAND ----------

sales_promo_handle = handle_missing_promo(sales, promo_col='onpromotion', strategy='nan')

# COMMAND ----------

ext_sales = extend_sales_table(sales_promo_handle, items, stores)
display(ext_sales)

# COMMAND ----------

oil_full = fill_time_series_with_dates(oil, 'date')
oil_interpolated = linear_interpolation(data= oil_full, column_name='dcoilwtico', order_col='date')

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ByteType, ShortType

# -----------------------------
# Helpers
# -----------------------------

_HAS_NORMALIZE = hasattr(F, "normalize")

if _HAS_NORMALIZE:
    def strip_accents_expr(c):
        # Decompose unicode then remove combining marks (diacritics)
        return F.regexp_replace(F.normalize(c, "NFD"), r"\p{M}+", "")
else:
    # Fallback: Python UDF (fine for small holidays table; slower on huge data)
    from unidecode import unidecode
    @F.udf("string")
    def strip_accents_expr(s):
        if s is None:
            return ""
        return unidecode(str(s))


def _normalized_anchor_key(anchor_label_col):
    # Equivalent of: unidecode(str(s)).lower().strip() + collapse whitespace
    s = F.coalesce(anchor_label_col.cast("string"), F.lit(""))
    s = strip_accents_expr(s)
    s = F.lower(F.trim(s))
    s = F.regexp_replace(s, r"\s+", " ")
    return s


# -----------------------------
# Holidays preprocessing (Spark)
# -----------------------------

def clean_holidays(holidays: DataFrame) -> DataFrame:
    """
    Spark equivalent of clean():
    - date cast
    - rename locale -> scope, locale_name -> scope_name
    - parse role / anchor_label / offset_days from (type, description)
    - build anchor_key (ascii-ish, lower, trimmed, collapsed whitespace)
    """
    df = holidays

    # Rename if present
    if "locale" in df.columns:
        df = df.withColumnRenamed("locale", "scope")
    if "locale_name" in df.columns:
        df = df.withColumnRenamed("locale_name", "scope_name")

    df = df.withColumn("date", F.to_date(F.col("date")))

    desc = F.trim(F.coalesce(F.col("description").cast("string"), F.lit("")))
    typ = F.col("type").cast("string")

    # Pattern: "<anchor><+/-N>" e.g. "Navidad-1", "Navidad+2"
    pattern = r"^(.*?)([+-]\d+)$"
    has_offset = desc.rlike(pattern)
    base_anchor = F.trim(F.regexp_extract(desc, pattern, 1))
    offset_days = F.regexp_extract(desc, pattern, 2).cast("int")

    # Start with defaults
    role = F.lit(None).cast("string")
    anchor_label = desc
    offset = F.lit(0)

    # Holiday
    role = F.when(typ == "Holiday", F.when(has_offset, F.lit("additional")).otherwise(F.lit("holiday"))).otherwise(role)
    anchor_label = F.when((typ == "Holiday") & has_offset, base_anchor).otherwise(anchor_label)
    offset = F.when((typ == "Holiday") & has_offset, offset_days).otherwise(offset)

    # Transfer
    role = F.when(typ == "Transfer", F.lit("transfer")).otherwise(role)
    anchor_label = F.when(typ == "Transfer", F.trim(F.regexp_replace(desc, r"(?i)^traslado\s+", ""))).otherwise(anchor_label)

    # Bridge
    role = F.when(typ == "Bridge", F.lit("bridge")).otherwise(role)
    anchor_label = F.when(typ == "Bridge", F.trim(F.regexp_replace(desc, r"(?i)^puente\s+", ""))).otherwise(anchor_label)

    # Work Day
    role = F.when(typ == "Work Day", F.lit("work_day")).otherwise(role)
    anchor_label = F.when(typ == "Work Day", F.trim(F.regexp_replace(desc, r"(?i)^recupero\s+puente\s+", ""))).otherwise(anchor_label)

    # Additional
    role = F.when(typ == "Additional", F.lit("additional")).otherwise(role)
    anchor_label = F.when((typ == "Additional") & has_offset, base_anchor).otherwise(anchor_label)
    offset = F.when((typ == "Additional") & has_offset, offset_days).otherwise(offset)

    # Event
    role = F.when(typ == "Event", F.lit("event")).otherwise(role)
    anchor_label = F.when((typ == "Event") & has_offset, base_anchor).otherwise(anchor_label)
    offset = F.when((typ == "Event") & has_offset, offset_days).otherwise(offset)

    # Fallback: normalize unknown types -> lower + underscores (your pandas fallback)
    role = F.when(role.isNull(), F.coalesce(F.lower(F.regexp_replace(typ, r"\s+", "_")), F.lit("other"))).otherwise(role)

    df = (
        df.withColumn("role", role)
          .withColumn("anchor_label", anchor_label)
          .withColumn("offset_days", offset.cast("int"))
          .withColumn("anchor_key", _normalized_anchor_key(F.col("anchor_label")))
    )
    return df


def add_business(holidays: DataFrame) -> DataFrame:
    """
    Spark equivalent of add_business():
    - city for Local scope = scope_name
    - state for Regional scope = scope_name
    (matches your current code: local->state mapping is not applied)
    """
    df = holidays
    df = df.withColumn("city", F.lit(None).cast("string")).withColumn("state", F.lit(None).cast("string"))

    df = df.withColumn(
        "city",
        F.when(F.col("scope") == "Local", F.col("scope_name")).otherwise(F.col("city"))
    )
    df = df.withColumn(
        "state",
        F.when(F.col("scope") == "Regional", F.col("scope_name")).otherwise(F.col("state"))
    )
    return df


def add_features(holidays: DataFrame) -> DataFrame:
    """
    Spark equivalent of add_features():
    - is_national, is_holiday, is_event, is_day_off
    - holiday_duration = max(offset_days)-min(offset_days)+1 per anchor_key
    """
    df = holidays

    transferred = F.coalesce(F.col("transferred").cast("boolean"), F.lit(False))

    df = (
        df.withColumn("is_national", (F.col("scope") == F.lit("National")))
          .withColumn("is_holiday", (F.col("role") == F.lit("holiday")))
          .withColumn("is_event", (F.col("role") == F.lit("event")))
          .withColumn(
              "is_day_off",
              ((F.col("role") == "holiday") & (~transferred)) |
              (F.col("role") == "transfer") |
              (F.col("role") == "bridge")
          )
    )

    w = Window.partitionBy("anchor_key")
    min_off = F.min(F.col("offset_days")).over(w)
    max_off = F.max(F.col("offset_days")).over(w)

    df = df.withColumn("holiday_duration", (max_off - min_off + F.lit(1)).cast("int"))
    return df


def drop_cols(holidays: DataFrame) -> DataFrame:
    """
    Spark equivalent of drop():
    drops: type, scope_name, description, transferred, anchor_label
    """
    to_drop = ["type", "scope_name", "description", "transferred", "anchor_label"]
    existing = [c for c in to_drop if c in holidays.columns]
    return holidays.drop(*existing)


def preprocess_holidays(holidays: DataFrame) -> DataFrame:
    df = clean_holidays(holidays)
    df = add_business(df)
    df = add_features(df)
    df = drop_cols(df)
    return df


# -----------------------------
# Merge train with holiday signals (Spark)
# -----------------------------

def merge_train(sales: DataFrame, holidays: DataFrame) -> DataFrame:
    """
    Spark equivalent of merge_train():
    Returns original sales columns + 4 features:
      - is_holiday (0/1)
      - holiday_duration (max across scopes)
      - is_day_off (0/1)
      - is_event (0/1)
    """
    base_cols = sales.columns

    t = sales

    h = holidays

    # Cast flags to ints for aggregation
    h = (h.withColumn("is_holiday", F.col("is_holiday").cast("int"))
          .withColumn("is_event", F.col("is_event").cast("int"))
          .withColumn("is_day_off", F.col("is_day_off").cast("int"))
          .withColumn("holiday_duration", F.coalesce(F.col("holiday_duration").cast("int"), F.lit(0)))
    )

    def agg_scope(scope_lower: str, keys: list[str], sfx: str) -> DataFrame:
        sub = h.filter(F.lower(F.col("scope")) == F.lit(scope_lower))
        return (
            sub.groupBy(*[F.col(k) for k in keys])
               .agg(
                   F.max("is_holiday").alias(f"is_holiday_{sfx}"),
                   F.max("is_event").alias(f"is_event_{sfx}"),
                   F.max("is_day_off").alias(f"is_day_off_{sfx}"),
                   F.max("holiday_duration").alias(f"holiday_duration_{sfx}")
               )
        )

    nat = agg_scope("national", ["date"], "nat")
    reg = agg_scope("regional", ["date", "state"], "reg")
    loc = agg_scope("local", ["date", "city"], "loc")

    t = t.join(F.broadcast(nat), on=["date"], how="left") \
         .join(F.broadcast(reg), on=["date", "state"], how="left") \
         .join(F.broadcast(loc), on=["date", "city"], how="left")

    # Fill nulls
    for c in [
        "is_holiday_nat","is_event_nat","is_day_off_nat","holiday_duration_nat",
        "is_holiday_reg","is_event_reg","is_day_off_reg","holiday_duration_reg",
        "is_holiday_loc","is_event_loc","is_day_off_loc","holiday_duration_loc",
    ]:
        if c in t.columns:
            t = t.withColumn(c, F.coalesce(F.col(c), F.lit(0)).cast("int"))
        else:
            t = t.withColumn(c, F.lit(0).cast("int"))

    # Combine across scopes
    t = (
        t.withColumn("is_event", F.greatest("is_event_nat", "is_event_reg", "is_event_loc").cast(ByteType()))
         .withColumn("is_holiday", F.greatest("is_holiday_nat", "is_holiday_reg", "is_holiday_loc").cast(ByteType()))
         .withColumn("is_day_off", F.greatest("is_day_off_nat", "is_day_off_reg", "is_day_off_loc").cast(ByteType()))
         .withColumn("holiday_duration", F.greatest("holiday_duration_nat", "holiday_duration_reg", "holiday_duration_loc").cast(ShortType()))
    )

    # Return only original train columns + 4 final features (matches pandas behavior)
    return t.select(*base_cols, "is_holiday", "holiday_duration", "is_day_off", "is_event")


# COMMAND ----------

display(holidays)

# COMMAND ----------

holidays_preproc = preprocess_holidays(holidays)
display(holidays_preproc)

# COMMAND ----------

sales_holidays = merge_train(ext_sales, holidays_preproc)
display(sales_holidays.filter((F.col("is_holiday") == 1) | (F.col("holiday_duration") > 0) | (F.col("is_day_off") == 1) | (F.col("is_event") == 1)))
