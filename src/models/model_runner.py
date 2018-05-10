from pyspark.sql import functions as sqlfunctions, types
from graphframes import GraphFrame

from ..lib.aggregate_messages import AggregateMessages as AM


class AxelrodRunner(object):
    """A class that initializes and runs any implementation of
    AbstractAxelrod."""

    def __init__(self, model):
        self.model = model

    def run(self, graph, num_iter=1):

        model = self.model

        # Make a spark UDF to initialize each node with a random trait
        @sqlfunctions.udf(returnType=types.ArrayType(types.IntegerType()))
        def udf_initialize_traits(arg):
            return model.initialize_traits(arg)

        # Make a spark UDF handling inter-node communication
        @sqlfunctions.udf(returnType=types.ArrayType(types.IntegerType()))
        def udf_node_interaction(my_traits, neighbor_traits):
            return model.check_neighbor_encounters(my_traits, neighbor_traits)

        # Make a spark UDF handling inter-node communication
        @sqlfunctions.udf(returnType=types.ArrayType(types.IntegerType()))
        def udf_combine_traits(my_traits, other_traits):
            return model.combine_traits(my_traits, other_traits)

        # Initialize random traits
        new_vertices = graph.vertices\
            .withColumn("traits", udf_initialize_traits(graph.vertices.id))

        # Make a new graph with this random trait
        current_graph = GraphFrame(new_vertices, graph.edges)

        # For number of iterations we want to run:
        diversities = []
        for i in range(num_iter):

            # Send neighbor traits to each node
            neighbor_traits = current_graph.aggregateMessages(
                sqlfunctions.collect_list(AM.msg).alias("neighbor_traits"),
                sendToSrc=None,
                sendToDst=AM.src["traits"])

            # Join neighbor traits back to main table
            new_vertices = current_graph.vertices\
                .join(neighbor_traits, "id", "left_outer")

            # Select which neighbor to interact with
            new_vertices = new_vertices\
                .withColumn("interaction_traits", udf_node_interaction("traits", "neighbor_traits"))\
                .drop("neighbor_traits")

            # Mix your and neighbor traits
            new_vertices = new_vertices\
                .withColumn("combined_traits", udf_combine_traits("traits", "interaction_traits"))

            # Drop intermediate columns
            new_vertices = new_vertices\
                .drop("traits", "interaction_traits")\
                .withColumnRenamed("combined_traits", "traits")

            # Cache
            cached_new_vertices = AM.getCachedDataFrame(new_vertices)

            # Update current graph with new nodes
            current_graph = GraphFrame(cached_new_vertices, graph.edges)

            # Record trait diversity
            diversity = current_graph.vertices.select('traits').distinct().count()
            diversities.append(diversity)

            print("Iteration: {}, trait diversity: {}".format(i, diversity))

        return current_graph, diversities
