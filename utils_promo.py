import pyspark
import pyspark.sql.functions as F

def handle_missing_promo(
    df: pyspark.sql.DataFrame,
    promo_col: str = "onpromotion",
    strategy: str = "nan"
) -> pyspark.sql.DataFrame:
    """
    Handle missing values in the 'onpromotion' column of a DataFrame.

    Parameters:
        df (pyspark.sql.DataFrame): The input DataFrame.
        promo_col (str): The name of the column containing promotion information.
        strategy (str): The strategy to use for handling missing values. Valid options are "nan" (stay with NaN) and "category" (replace NaNs with new category: "None")
    """
    if strategy == "nan":
        return df
    elif strategy == "category":
        return df.withColumn(
            promo_col,
            F.when(F.isnan(F.col(promo_col)), F.lit("None")).otherwise(F.col(promo_col))
        )
    else:
        raise ValueError(f"Invalid strategy: {strategy}")