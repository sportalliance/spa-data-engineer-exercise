import logging

from typing import Dict

import pytest

from pyspark.sql import SparkSession, SQLContext, DataFrame


def quiet_py4j():
    """Suppress spark logging for the test context."""
    logger = logging.getLogger("py4j")
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_session(request) -> SQLContext:
    """Fixture for creating a spark context."""

    spark = (
        SparkSession.builder.master("local")
        .appName("pytest-pyspark-local-testing")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .config('spark.sql.shuffle.partitions', 1)
        .config('spark.default.parallelism', 1)
        .config('spark.rdd.compress', False)
        .config('spark.shuffle.compress', False)
        .getOrCreate()
    )
    request.addfinalizer(lambda: spark.stop())

    quiet_py4j()
    return spark


def df_from_dict(spark: SQLContext, d: Dict) -> DataFrame:
    keys = list(d.keys())
    num_rows = len(d[keys[0]])
    l = [tuple([d[k][i] for k in keys]) for i in range(0, num_rows)]
    return spark.createDataFrame(l, keys)


def dict_from_df(df: DataFrame) -> Dict:
    rows = df.collect()
    return {col: [row[col] for row in rows] for col in df.columns}