from random import randint
import networkx as nx


def random_dag(nodes: int, edges: int):
    """ Generate a random Directed Acyclic Graph (DAG) with a given number of nodes and edges.
        https://naturegeorge.github.io/eigenblog/posts/DAG/
    """
    nx_graph = nx.DiGraph()
    for i in range(nodes):
        nx_graph.add_node(i)
    while edges > 0:
        a = randint(0, nodes-1)
        b = a
        while b == a:
            b = randint(0, nodes-1)
        nx_graph.add_edge(a, b)
        if nx.is_directed_acyclic_graph(nx_graph):
            edges -= 1
        else:
            # we closed a loop!
            nx_graph.remove_edge(a, b)
    return nx_graph
