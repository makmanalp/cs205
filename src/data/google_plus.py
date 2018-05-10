"""Google plus dataset from: J. McAuley and J. Leskovec. Learning to Discover
Social Circles in Ego Networks. NIPS, 2012.
https://snap.stanford.edu/data/egonets-Gplus.html"""

from pyspark.sql import functions as sqlfunctions


def read(spark, path="/Users/makmana/Downloads/gplus_combined.txt.gz"):

    gplus_edges = spark.read.csv(path, header=False, sep=" ")

    gplus_edges = gplus_edges.withColumnRenamed("_c0", "src").withColumnRenamed("_c1", "dst")

    unique_sources = gplus_edges.select("src").distinct()
    unique_targets = gplus_edges.select("dst").distinct()

    unique_nodes = unique_sources.union(unique_targets)\
        .distinct().select(sqlfunctions.col("src").alias("id"))

    return unique_nodes, gplus_edges
