"""Friends example adapted from graphframes library."""

from pyspark.sql import SQLContext


def read(spark):

    sqlContext = SQLContext(spark)

    # Vertex DataFrame
    v = sqlContext.createDataFrame([
        ("a", "Alice", 34),
        ("b", "Bob", 36),
        ("c", "Charlie", 30),
        ("d", "David", 29),
        ("e", "Esther", 32),
        ("f", "Fanny", 36)
    ], ["id", "name", "age"])

    # Edge DataFrame
    e = sqlContext.createDataFrame([
        ("a", "b", "friend"),
        ("b", "c", "follow"),
        ("c", "b", "follow"),
        ("f", "c", "follow"),
        ("e", "f", "follow"),
        ("e", "d", "friend"),
        ("d", "a", "friend")
    ], ["src", "dst", "relationship"])

    return v, e
