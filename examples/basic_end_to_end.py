import asyncio
import time
import random

from symphonizer.node_runner import NodeRunner
from symphonizer.composition import Composition, DAGNote
from symphonizer.perform import Perform


class Compose(Composition):

    def configure_node_runner(self, node: DAGNote):
        """Configure the node runner as appropriate for the DAGNote, the flexibility of this method allows for
        different node runners to be used for different nodes in the DAG or alternatively the same node runner
        where the execute method is configured differently for different nodes in the DAG.

        in the execute function below reference is made to nodes predecessors, this is a set of DAGNote
        instances. Some use cases may require the status or results of the predecessors to be used in the
        execution of the node. This is a simple example of how that can be done.
        """
        node.node_data["predecessors"] = self._graph.get(node, set())

        async def execute(**params) -> str:
            sleep_time = random.uniform(0.001, 0.1)
            await asyncio.sleep(sleep_time)
            return str(node)

        return NodeRunner().prepare(node=node).run(execute)


async def main():
    dag_a = DAGNote("A")
    dag_b = DAGNote("B")
    dag_c = DAGNote("C")
    dag_d = DAGNote("D")
    sample_graph = {
        dag_d: {dag_b, dag_c},
        dag_c: {dag_a},
        dag_b: {dag_a},
    }
    dag_count_target = 500
    dag_count_completed = 0
    dag_tracker = {}
    test_stop_future = asyncio.Future()
    perform = Perform()
    start_time = time.time()

    def scheduler_done_cb(instance, status, error=None, elapsed_time: float = 0):
        nonlocal dag_count_completed
        dag_tracker.pop(instance.instance_id, None)
        dag_count_completed += 1
        if dag_count_completed == dag_count_target:
            test_stop_future.set_result(None)

    async def schedule_dag():
        dag = Compose(
            sample_graph,
            schedule_done_cb=scheduler_done_cb,
        )
        asyncio.create_task(perform.add(dag))
        await asyncio.sleep(0)

    print("Starting to schedule DAGs")

    _ = list([await schedule_dag() for _ in range(dag_count_target)])

    print(f"added {dag_count_target} DAGs to perform")
    await asyncio.sleep(0.001)
    await test_stop_future
    end_time = time.time()
    print(f"All DAGs processed in {end_time-start_time} seconds")
    print(
        f"Average DAG processing time {round((end_time-start_time)/dag_count_target, 4)} seconds"
    )


if __name__ == "__main__":
    asyncio.run(main())
