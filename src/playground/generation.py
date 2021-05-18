from datetime import datetime

import pyspark.sql.functions as F

from pyspark.sql import SQLContext, DataFrame, SparkSession

NUM_TENANTS = 100
NUM_IDS = 100_000
START_YEAR = 2021
NUM_COLS = 50
SEED = 42
NUM_PARTITIONS = 10


def gen_table_data(spark: SQLContext) -> DataFrame:
    tenant_data = [tuple([f"tenant{i}"]) for i in range(1, NUM_TENANTS + 1)]
    tenant_df = spark.createDataFrame(tenant_data, schema=["tenant_name"])
    ids_df = spark.createDataFrame(
        [tuple([i]) for i in range(1, NUM_IDS + 1)], schema=["id"]
    )
    ids_df = ids_df.withColumn("modified_date", F.lit(datetime(START_YEAR, 1, 1, 15)))
    ids_df = ids_df.withColumn(
        "modified_date",
        F.expr(f"date_add(add_months(modified_date, floor({NUM_IDS} / 12)), id % 27)"),
    )
    for i in range(NUM_COLS):
        col_type = i % 3
        if col_type == 0:
            ids_df = ids_df.withColumn(
                f"col_{i}", F.floor(F.randn(SEED) * 10 + i).cast("int")
            )
        elif col_type == 1:
            ids_df = ids_df.withColumn(f"col_{i}", F.rand(SEED))
        else:
            ids_df = ids_df.withColumn(
                f"col_{i}",
                F.format_string(
                    "STATUS",
                    F.when(F.rand(SEED) < 0.5, F.lit(f"VALID_{i}")).otherwise(
                        F.lit(f"INVALID_{i}")
                    ),
                ),
            )

    return tenant_df.crossJoin(ids_df)


if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("de-exercise")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    df = gen_table_data(spark)
    df.repartition(NUM_PARTITIONS).write.mode("overwrite").parquet("data/base/table1")
