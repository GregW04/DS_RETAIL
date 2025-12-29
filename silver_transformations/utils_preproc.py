import pyspark
import pyspark.sql.functions as F

def extend_sales_table(sales_df: pyspark.sql.DataFrame, items_df: pyspark.sql.DataFrame, stores_df: pyspark.sql.DataFrame, product_level: str = "item_nbr", business_level: str = "store_nbr") -> pyspark.sql.DataFrame:
    """
    Extend the sales table with holidays and store information.
    """
    items_df.dropDuplicates(subset=[product_level])
    stores_df.dropDuplicates(subset=[business_level])

    sales_items = sales_df.join(items_df, on=product_level, how="left")
    sales_items_stores = sales_items.join(stores_df, on=business_level, how="left")
    return sales_items_stores