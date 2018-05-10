"""Game of Thrones dataset from Andrew Beverdige's project:
    https://github.com/mathbeveridge/asoiaf"""

from pyspark.sql import functions as sqlfunctions


def read(spark, path="/Users/makmana/training/network/asoiaf-all-edges.csv"):

    gof_edges = spark.read.csv(path, header=True)
    gof_edges = gof_edges\
        .withColumnRenamed("Source", "dst")\
        .withColumnRenamed("Target", "src")\
        .withColumnRenamed("weight", "w")\
        .drop("id")\
        .drop("Type")

    unique_sources = gof_edges.select("src").distinct()
    unique_targets = gof_edges.select("dst").distinct()

    unique_nodes = unique_sources.union(unique_targets)\
        .distinct().select(sqlfunctions.col("src").alias("id"))

    return unique_nodes, gof_edges
