import asyncio
import time
import random

from symphonizer.node_runner import NodeRunner
from symphonizer.composition import Composition, DAGNote
from symphonizer.perform import Perform


class Compose(Composition):
    @classmethod
    def configure_node_runner(cls, node: DAGNote):
        """ Configure the node runner as appropriate for the DAGNote, the flexibility of this method allows for
        different node runners to be used for different nodes in the DAG or alternatively the same node runner
        where the execute method is configured differently for different nodes in the DAG.
        """
        async def execute(**params):
            sleep_time = random.uniform(0.001, 0.1)
            # print(f"Processing node {node}, sleep for {sleep_time}")
            await asyncio.sleep(sleep_time)

        return NodeRunner().prepare(node=node).run(execute)


async def main():
    sample_graph = {
        DAGNote("D"): {DAGNote("B"), DAGNote("C")},
        DAGNote("C"): {DAGNote("A")},
        DAGNote("B"): {DAGNote("A")}
    }
    dag_count_target = 1000
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
    print(f"Average DAG processing time {round((end_time-start_time)/dag_count_target, 4)} seconds")


if __name__ == "__main__":
    asyncio.run(main())
