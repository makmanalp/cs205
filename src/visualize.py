import networkx as nx
from matplotlib import pyplot as plt

from graphframes import GraphFrame


def cull_graph(graph, by="degree", quantile=0.25, quantile_accuracy=0.1, max_iter=2):
    """Reduce a spark graph by getting rid of nodes that are not high value.
    This is done either by removing nodes that have number of degrees below a
    quantile or removing nodes with pagerank below a quantile."""

    wanted_nodes = None
    if by == "degree":
        nth_percentile = graph.degrees.approxQuantile("degree", [quantile], quantile_accuracy)[0]
        wanted_nodes = graph.degrees\
            .filter(graph.degrees.degree > nth_percentile)\
            .select("id")
    elif by == "pagerank":
        results = graph.pageRank(resetProbability=0.15, maxIter=max_iter).vertices
        nth_percentile = results.approxQuantile("pagerank", [quantile], quantile_accuracy)[0]
        wanted_nodes = results\
            .filter(results.pagerank > nth_percentile)\
            .select("id")
    else:
        raise ValueError("by must be degree or pagerank!")

    filtered_nodes = graph.vertices.join(wanted_nodes, "id")
    filtered_edges = graph.edges.join(wanted_nodes,
                                      (graph.edges.src == wanted_nodes.id) |
                                      (graph.edges.dst == wanted_nodes.id))

    return GraphFrame(filtered_nodes, filtered_edges)


def trait_to_float(lst):
    """Convert a list of trait lists into a list of floating point numbers
    suitable for using as colors in matplotlib."""
    s = set([str(i) for i in lst])
    ls = len(s)
    d = {si: i/ls for i, si in enumerate(s)}
    return [d[str(i)] for i in lst]


def visualize_graph(graph, edge_attrs=[], node_attrs=[], graph_type=None,
                    node_color_func=None, edge_color_func=None, draw_kwargs={},
                    layout=None):

    # Convert from spark dataframe to pandas
    df = graph.edges.toPandas()

    # Convert from pandas dataframe to networkx graph
    if graph_type is None:
        graph_type = nx.DiGraph()

    nxg = nx.from_pandas_edgelist(df,
                                  source="src", target="dst",
                                  edge_attr=edge_attrs,
                                  create_using=graph_type)

    if len(node_attrs) > 0:
        nodes = graph.vertices.toPandas()
        for node_attr in node_attrs:
            attr_vals = nodes.set_index("id")[node_attr].to_dict()
            nx.set_node_attributes(nxg,
                                   attr_vals,
                                   node_attr)

    # Draw graph
    node_color = "r"
    node_cmap = None
    if node_color_func:
        node_color = node_color_func(nxg.nodes(data=True))
        node_cmap = plt.get_cmap("Set3")

    edge_color = "r"
    edge_cmap = None
    if edge_color_func:
        edge_color = edge_color_func(nxg.edges(data=True))
        edge_cmap = plt.get_cmap("Set3")

    pos = None
    if layout == "kamada_kawai":
        pos = nx.kamada_kawai_layout(nxg)
    elif layout == "circular":
        pos = nx.circular_layout(nxg)

    nx.draw_networkx(nxg,
                     node_color=node_color, node_cmap=node_cmap,
                     edge_color=edge_color, edge_cmap=edge_cmap,
                     pos=pos,
                     **draw_kwargs)
