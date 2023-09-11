import logging
import asyncio
import networkx as nx

from harmony.melody.dag_scheduler import DAGScheduler, StopScheduleException

logger = logging.getLogger()


class Graph:
    """
    Python program to detect cycle in a graph

    # g = Graph()
    # g.add_edge(0, 1)
    # g.add_edge(0, 2)
    # g.add_edge(1, 2)
    # # g.add_edge(2, 0)
    # g.add_edge(2, 3)
    # # g.add_edge(3, 3)
    # if g.is_cyclic() == 1:
    #     print("Graph contains cycle")
    # else:
    #     print("Graph doesn't contain cycle")
    """
    def __init__(self):
        self._child_index = {}
        self._parent_index = {}
        self._nodes = set()
        self.single_source = True

    def add_edge(self, parent, child) -> bool:
        logging.info("add (%s, %s)", child, parent)
        self._child_index.setdefault(child, set()).add(parent)
        self._parent_index.setdefault(parent, set()).add(child)
        self._nodes.add(child)
        self._nodes.add(parent)
        if self.is_cyclic():
            self._child_index[child].remove(parent)
            return False
        else:
            return True

    def is_cyclic_util(self, v, visited, rec_stack):
        # Mark current node as visited and adds to recursion stack
        visited[v] = True
        rec_stack[v] = True

        # Iterate all neighbours, if any neighbour is visited and in rec_stack then graph is cyclic
        logger.info(v)
        logger.info(list(self._child_index.keys()))

        for neighbour in self._child_index.get(v, set()):
            if visited[neighbour] is False and self.is_cyclic_util(neighbour, visited, rec_stack) is True:
                return True
            elif rec_stack[neighbour] is True:
                return True

        # The node needs to be popped from recursion stack before function ends
        rec_stack[v] = False
        return False

    def is_cyclic(self):
        """
        :return: Returns true if graph is cyclic else false
        """
        visited = dict([(node, False) for node in self._nodes])
        rec_stack = dict([(node, False) for node in self._nodes])
        for node in self._nodes:
            if visited[node] is False and self.is_cyclic_util(node, visited, rec_stack) is True:
                return True
        return False


class Orchestrator(DAGScheduler):
    @classmethod
    async def process_node(cls, node):
        logger.debug("Processing node, %s", node)
        # Do some work with the node
        if node == "A":
            await asyncio.sleep(4)
            raise StopScheduleException("A failed, no bread, stopping process")
        elif node == "C":
            logging.info("Taking a while to find the Jam")
            await asyncio.sleep(3)
        else:
            await asyncio.sleep(1)


async def main():

    # g = random_dag(5, 10)
    # g = random_dag(20, 20)

    # Create an empty directed graph
    g = nx.DiGraph()

    # # Generate a random DAG with 100 nodes and 200 edges which is acyclic
    # g = nx.random_powerlaw_tree(10, 3, seed=42, tries=2000)
    #
    # assert nx.is_directed_acyclic_graph(g)
    #
    # draw = False
    # if draw:
    #     options = {
    #         "font_size": 36,
    #         "node_size": 3000,
    #         "node_color": "white",
    #         "edgecolors": "black",
    #         "linewidths": 5,
    #         "width": 5,
    #     }
    #     nx.draw_networkx(g, **options)
    #     ax = plt.gca()
    #     ax.margins(0.20)
    #     plt.axis("off")
    #     plt.show()

    # predecessors = {}
    # for parent, child in g.edges:
    #     # logger.info((child, parent))
    #     predecessors.setdefault(child, set()).add(parent)
    # logger.info(predecessors)

    # "D": {"A"}

    predecessors = {"A": {}, "B": {}, "C": {}, "E": {"D", "B"}, "F": {"D", "C"}, "G": {"E", "F"}, "H": {"G"}}

    def node_processing_error_cb(node, error: Exception):
        logging.exception("Node error: %s", node)
        logging.error(error)

    dag: DAGScheduler = Orchestrator(predecessors, node_processing_error_cb=node_processing_error_cb)
    try:
        await dag.start_processing()
    except Exception as err:
        logging.error(err)
    logging.info("done")

if __name__ == "__main__":
    logging.basicConfig(
        format="%(levelname)1.1s %(asctime)s.%(msecs)03d %(process)d %(module)s:%(lineno)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.DEBUG,
    )
    logger.info("__main__ starting")
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.create_task(main())
        loop.run_forever()
    except Exception as e:
        logger.exception(e)
    finally:
        logger.info("__main__ completed")