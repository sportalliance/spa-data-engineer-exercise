# Data Engineer Coding Exercise

## Installation

A Dockerfile is provided for this exercise. You can check whether everything works by running

```
mkdir data
docker build -t de-exercise .
docker run -v $(pwd)/data:/build/data de-exercise
```

Running the docker image will first execute a test and afterwards create data required for the exercise in the passed in data folder:

```
data
└── base
    └── table1
        ├── _SUCCESS
        ├── part-00000-<RUN_HASH>-c000.snappy.parquet
        ├── part-00001-<RUN_HASH>-c000.snappy.parquet
        ├── part-00002-<RUN_HASH>-c000.snappy.parquet
        ├── part-00003-<RUN_HASH>-c000.snappy.parquet
        ├── part-00004-<RUN_HASH>-c000.snappy.parquet
        ├── part-00005-<RUN_HASH>-c000.snappy.parquet
        ├── part-00006-<RUN_HASH>-c000.snappy.parquet
        ├── part-00007-<RUN_HASH>-c000.snappy.parquet
        ├── part-00008-<RUN_HASH>-c000.snappy.parquet
        └── part-00009-<RUN_HASH>-c000.snappy.parquet
```

If you want to run this in your local environment (e.g. for IDE support) you will have to install Python 3.6 with PySpark 2.4.3, which requires Java8. Afterwards, install the exercise dependencies by running

```
pip install -r requirements.txt
pip install -e .
```

Using a virtual environment for this (e.g. through pyenv) is a good idea.

Similar to the Dockerfile, you can run

```
mkdir data
make hello_world data/base
```

to check whether everything works.

## Exercise

We will build data transformations using PySpark - it might be a good idea to familiarize yourself with the [PySpark SQL module](https://spark.apache.org/docs/2.4.3/api/python/pyspark.sql.html). You do not have to know functions by heart - but the exercise will be easier for you if you know how to e.g. filter a DataFrame.