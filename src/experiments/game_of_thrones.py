# Set up spark context

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("test1").setExecutorEnv("PYTHONPATH", "/home/hadoop/cs205/")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
sqlCtx = SQLContext(sc)

# import requirements
from src.models.model_runner import AxelrodRunner
from src.models.axelrod_economic_complexity import EconomicComplexity
from src.data import game_of_thrones
from src.util import Timer

from graphframes import GraphFrame

f = GraphFrame(*game_of_thrones.read(sqlCtx, path="file:///home/hadoop/data/asoiaf-all-edges.csv"))

ec = EconomicComplexity()
runner = AxelrodRunner(ec)

with Timer() as t:
    result = runner.run(f, num_iter=25)
    result[0].vertices.show()
    print(result[1])

print("Time elapsed: {}".format(t.interval))
