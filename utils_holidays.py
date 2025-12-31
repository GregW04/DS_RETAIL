import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, ByteType, ShortType

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

def clean_holidays(holidays: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
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


def add_business_to_holidays(holidays: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
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


def add_features_holidays(holidays: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
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


def drop_cols_holidays(holidays: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Spark equivalent of drop():
    drops: type, scope_name, description, transferred, anchor_label
    """
    to_drop = ["type", "scope_name", "description", "transferred", "anchor_label"]
    existing = [c for c in to_drop if c in holidays.columns]
    return holidays.drop(*existing)


def preprocess_holidays(holidays: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    df = clean_holidays(holidays)
    df = add_business_to_holidays(df)
    df = add_features_holidays(df)
    df = drop_cols_holidays(df)
    return df

def merge_holiday_signals(sales: pyspark.sql.DataFrame, holidays: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Join holiday features to sales while preserving scope-specific info.

    Output = original sales columns +
      Scope-specific:
        - is_holiday_{nat,reg,loc}
        - is_event_{nat,reg,loc}
        - is_day_off_{nat,reg,loc}
        - holiday_duration_{nat,reg,loc}
        - has_bridge_{nat,reg,loc}
        - has_transfer_{nat,reg,loc}
        - holiday_rows_count_{nat,reg,loc}   (sum of is_holiday rows)
        - event_rows_count_{nat,reg,loc}     (sum of is_event rows)
        - dayoff_rows_count_{nat,reg,loc}    (sum of is_day_off rows)

      Summaries:
        - is_holiday_any, is_event_any, is_day_off_any
        - holiday_duration_max
        - holiday_scopes_count, event_scopes_count, dayoff_scopes_count
        - holiday_scope_level (0 none, 1 local, 2 regional, 3 national)
        - has_bridge_any, has_transfer_any
        - holiday_rows_count_any, event_rows_count_any, dayoff_rows_count_any

      Backward-compatible (same as before):
        - is_holiday   (== is_holiday_any)
        - is_event     (== is_event_any)
        - is_day_off   (== is_day_off_any)
        - holiday_duration (== holiday_duration_max)
    """
    base_cols = sales.columns

    t = sales
    h = holidays

    # Ensure dates are dates (cheap safety)
    t = t.withColumn("date", F.to_date(F.col("date")))
    h = h.withColumn("date", F.to_date(F.col("date")))

    # Cast flags to ints for aggregation
    h = (
        h.withColumn("is_holiday", F.col("is_holiday").cast("int"))
         .withColumn("is_event", F.col("is_event").cast("int"))
         .withColumn("is_day_off", F.col("is_day_off").cast("int"))
         .withColumn("holiday_duration", F.coalesce(F.col("holiday_duration").cast("int"), F.lit(0)))
         .withColumn("role", F.col("role").cast("string"))
    )

    def agg_scope(scope_lower: str, keys: list[str], sfx: str) -> pyspark.sql.DataFrame:
        sub = h.filter(F.lower(F.col("scope")) == F.lit(scope_lower))

        # Role-mix signals
        has_bridge = F.max(F.when(F.col("role") == "bridge", F.lit(1)).otherwise(F.lit(0))).alias(f"has_bridge_{sfx}")
        has_transfer = F.max(F.when(F.col("role") == "transfer", F.lit(1)).otherwise(F.lit(0))).alias(f"has_transfer_{sfx}")

        # Intensity signals: sums of applicable rows for each class
        holiday_rows = F.sum(F.col("is_holiday")).alias(f"holiday_rows_count_{sfx}")
        event_rows = F.sum(F.col("is_event")).alias(f"event_rows_count_{sfx}")
        dayoff_rows = F.sum(F.col("is_day_off")).alias(f"dayoff_rows_count_{sfx}")

        return (
            sub.groupBy(*[F.col(k) for k in keys])
               .agg(
                    F.max("is_holiday").alias(f"is_holiday_{sfx}"),
                    F.max("is_event").alias(f"is_event_{sfx}"),
                    F.max("is_day_off").alias(f"is_day_off_{sfx}"),
                    F.max("holiday_duration").alias(f"holiday_duration_{sfx}"),
                    has_bridge,
                    has_transfer,
                    holiday_rows,
                    event_rows,
                    dayoff_rows,
               )
        )

    nat = agg_scope("national", ["date"], "nat")
    reg = agg_scope("regional", ["date", "state"], "reg")
    loc = agg_scope("local", ["date", "city"], "loc")

    t = (
        t.join(F.broadcast(nat), on=["date"], how="left")
         .join(F.broadcast(reg), on=["date", "state"], how="left")
         .join(F.broadcast(loc), on=["date", "city"], how="left")
    )

    # Fill nulls for all joined columns (ints)
    joined_int_cols = [
        # flags + durations
        "is_holiday_nat","is_event_nat","is_day_off_nat","holiday_duration_nat",
        "is_holiday_reg","is_event_reg","is_day_off_reg","holiday_duration_reg",
        "is_holiday_loc","is_event_loc","is_day_off_loc","holiday_duration_loc",
        # role-mix flags
        "has_bridge_nat","has_transfer_nat",
        "has_bridge_reg","has_transfer_reg",
        "has_bridge_loc","has_transfer_loc",
        # intensity counts
        "holiday_rows_count_nat","event_rows_count_nat","dayoff_rows_count_nat",
        "holiday_rows_count_reg","event_rows_count_reg","dayoff_rows_count_reg",
        "holiday_rows_count_loc","event_rows_count_loc","dayoff_rows_count_loc",
    ]

    for c in joined_int_cols:
        if c in t.columns:
            t = t.withColumn(c, F.coalesce(F.col(c), F.lit(0)).cast("int"))
        else:
            t = t.withColumn(c, F.lit(0).cast("int"))

    # Summaries across scopes
    t = (
        t
        # ANY-scope (kept, plus backward-compatible aliases)
        .withColumn("is_holiday_any", F.greatest("is_holiday_nat", "is_holiday_reg", "is_holiday_loc").cast(ByteType()))
        .withColumn("is_event_any",   F.greatest("is_event_nat",   "is_event_reg",   "is_event_loc").cast(ByteType()))
        .withColumn("is_day_off_any", F.greatest("is_day_off_nat", "is_day_off_reg", "is_day_off_loc").cast(ByteType()))
        .withColumn("holiday_duration_max", F.greatest("holiday_duration_nat", "holiday_duration_reg", "holiday_duration_loc").cast(ShortType()))

        # How many scopes are active (stacking)
        .withColumn("holiday_scopes_count", (F.col("is_holiday_nat") + F.col("is_holiday_reg") + F.col("is_holiday_loc")).cast("int"))
        .withColumn("event_scopes_count",   (F.col("is_event_nat")   + F.col("is_event_reg")   + F.col("is_event_loc")).cast("int"))
        .withColumn("dayoff_scopes_count",  (F.col("is_day_off_nat") + F.col("is_day_off_reg") + F.col("is_day_off_loc")).cast("int"))

        # Strongest holiday scope indicator (0 none, 1 local, 2 regional, 3 national)
        .withColumn(
            "holiday_scope_level",
            F.greatest(
                (F.col("is_holiday_loc") * F.lit(1)),
                (F.col("is_holiday_reg") * F.lit(2)),
                (F.col("is_holiday_nat") * F.lit(3)),
            ).cast("int")
        )

        # Role-mix ANY
        .withColumn("has_bridge_any",   F.greatest("has_bridge_nat", "has_bridge_reg", "has_bridge_loc").cast(ByteType()))
        .withColumn("has_transfer_any", F.greatest("has_transfer_nat", "has_transfer_reg", "has_transfer_loc").cast(ByteType()))

        # Intensity ANY (sum across scopes)
        .withColumn("holiday_rows_count_any", (F.col("holiday_rows_count_nat") + F.col("holiday_rows_count_reg") + F.col("holiday_rows_count_loc")).cast("int"))
        .withColumn("event_rows_count_any",   (F.col("event_rows_count_nat")   + F.col("event_rows_count_reg")   + F.col("event_rows_count_loc")).cast("int"))
        .withColumn("dayoff_rows_count_any",  (F.col("dayoff_rows_count_nat")  + F.col("dayoff_rows_count_reg")  + F.col("dayoff_rows_count_loc")).cast("int"))

        # Backward compatible names (exactly what your old merge_train returned)
        .withColumn("is_holiday", F.col("is_holiday_any"))
        .withColumn("is_event", F.col("is_event_any"))
        .withColumn("is_day_off", F.col("is_day_off_any"))
        .withColumn("holiday_duration", F.col("holiday_duration_max"))
    )

    # Return original sales columns + enriched holiday feature set
    extra_cols = (
        joined_int_cols
        + [
            "is_holiday_any","is_event_any","is_day_off_any","holiday_duration_max",
            "holiday_scopes_count","event_scopes_count","dayoff_scopes_count",
            "holiday_scope_level",
            "has_bridge_any","has_transfer_any",
            "holiday_rows_count_any","event_rows_count_any","dayoff_rows_count_any",
            # backward-compatible:
            "is_holiday","is_event","is_day_off","holiday_duration",
        ]
    )

    # Deduplicate in case of overlap
    extra_cols = [c for i, c in enumerate(extra_cols) if c not in extra_cols[:i]]

    return t.select(*base_cols, *extra_cols)