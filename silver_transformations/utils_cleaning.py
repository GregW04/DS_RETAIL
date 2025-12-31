import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

def linear_interpolation(
    data: pyspark.sql.DataFrame,
    column_name: str,
    order_col: str,
    partition_cols: list = None,
) -> pyspark.sql.DataFrame:
    """
    Performs linear interpolation on a specified column within each partition,
    while preserving and restoring the original column.

    Parameters:
    - data: Input Spark DataFrame.
    - column_name: The column to interpolate.
    - partition_cols: Columns to partition the data (default: ['ProductId', 'StoreId']).
    - order_col: Column to order the data within partitions (default: 'Date').

    Returns:
    - DataFrame with a new column named 'linear_<column_name>' and the original column restored.
    """
    # Store the original column
    original_col = f'original_{column_name}'
    data = data.withColumn(original_col, F.col(column_name))
    
    # Define window specification
    if partition_cols:
        window_spec = Window.partitionBy(*partition_cols).orderBy(order_col)
    else:
        window_spec = Window.orderBy(order_col)
    
    # Lag and Lead for previous and next non-null values
    lag_col = F.last(
        F.when(F.col(column_name).isNotNull(), F.col(column_name)), 
        ignorenulls=True
    ).over(window_spec.rowsBetween(Window.unboundedPreceding, -1))
    
    lag_date = F.last(
        F.when(F.col(column_name).isNotNull(), F.col(order_col)), 
        ignorenulls=True
    ).over(window_spec.rowsBetween(Window.unboundedPreceding, -1))
    
    lead_col = F.first(
        F.when(F.col(column_name).isNotNull(), F.col(column_name)), 
        ignorenulls=True
    ).over(window_spec.rowsBetween(1, Window.unboundedFollowing))
    
    lead_date = F.first(
        F.when(F.col(column_name).isNotNull(), F.col(order_col)), 
        ignorenulls=True
    ).over(window_spec.rowsBetween(1, Window.unboundedFollowing))
    
    # Add lag and lead columns
    data = data.withColumn('prev_val', lag_col) \
               .withColumn('prev_date', lag_date) \
               .withColumn('next_val', lead_col) \
               .withColumn('next_date', lead_date)
    
    # Calculate the interpolated value
    data = data.withColumn(
        f'smoothed_{column_name}',
        F.when(
            F.col(column_name).isNull() & 
            F.col('prev_val').isNotNull() & 
            F.col('next_val').isNotNull(),
            (
                (F.unix_timestamp(F.col(order_col)) - F.unix_timestamp(F.col('prev_date'))) /
                (F.unix_timestamp(F.col('next_date')) - F.unix_timestamp(F.col('prev_date')))
            ) * (F.col('next_val') - F.col('prev_val')) + F.col('prev_val')
        ).otherwise(F.col(column_name))
    )
    
    # Define window for forward and backward fill
    if partition_cols:
        fill_window_fwd = Window.partitionBy(*partition_cols).orderBy(order_col).rowsBetween(Window.unboundedPreceding, Window.currentRow)
        fill_window_bwd = Window.partitionBy(*partition_cols).orderBy(order_col).rowsBetween(Window.currentRow, Window.unboundedFollowing)
    else:
        fill_window_fwd = Window.orderBy(order_col).rowsBetween(Window.unboundedPreceding, Window.currentRow)
        fill_window_bwd = Window.orderBy(order_col).rowsBetween(Window.currentRow, Window.unboundedFollowing)
    # Forward fill
    data = data.withColumn(
        f'smoothed_{column_name}',
        F.last(f'smoothed_{column_name}', ignorenulls=True).over(fill_window_fwd)
    )
    
    # Backward fill
    data = data.withColumn(
        f'smoothed_{column_name}',
        F.first(f'smoothed_{column_name}', ignorenulls=True).over(fill_window_bwd)
    )
    
    # Restore the original column
    data = data.withColumn(column_name, F.col(original_col))
    
    # Drop intermediate and temporary columns
    data = data.drop(
        'prev_val', 
        'prev_date', 
        'next_val', 
        'next_date', 
        original_col
    )
    return data

def fill_time_series_with_dates(df, date_col):
    spark = df.sparkSession
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
