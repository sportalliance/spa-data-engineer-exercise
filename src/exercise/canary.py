import pyspark.sql.functions as F

from pyspark.sql import DataFrame


def hello_world(df: DataFrame) -> DataFrame:
    return df.withColumn("new_col", F.lit("a"))
