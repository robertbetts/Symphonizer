import logging
import asyncio
import time

import pytest

from harmony.perform import Perform
from harmony.composition import Composition, DAGNode


logger = logging.getLogger()

test_sample_size = 1000
completed_count = 0
total_time = 0
test_start_time = time.time()
total_nodes = 0


@pytest.mark.asyncio
async def test_run_conductor():

    test_future_completed = asyncio.Future()

    def scheduler_completed(instance, status, error, time_taken):
        global completed_count, total_time
        # logger.info("Scheduler %s. instance_id:%s error: %s taken: %s", status, instance.instance_id, error, time_taken)
        completed_count += 1
        total_time += time_taken
        if completed_count == test_sample_size:
            test_time_taken = time.time()-test_start_time
            logging.info("Completed %s dag schedules in %s seconds, avg time: %s, avg schedule elapsed time %s",
                         completed_count, test_time_taken, test_time_taken/completed_count, total_time/completed_count)
            logging.info("Total nodes processed: %s, %s nps, avg node time %f, avg nodes per schedule %s",
                         total_nodes, total_nodes/test_time_taken, test_time_taken/total_nodes, total_nodes/completed_count)
            test_future_completed.set_result(None)

    def node_processing_done_cb(node, status, error):
        global total_nodes
        total_nodes += 1
        # logger.debug("Node %s, %s: error:%s, elapsed_time:%s", status, node, error, (node.end_time - node.start_time))

    def get_sample_dag_edges(sample_size):
        global total_nodes
        retval = []
        while len(retval) < sample_size:
            retval.append({
                DAGNode("D"): {DAGNode("B"), DAGNode("C")},
                DAGNode("C"): {DAGNode("A")},
                DAGNode("B"): {DAGNode("A")}
            })
        return retval

    conductor = Perform()
    samples = get_sample_dag_edges(test_sample_size)

    def do_conductor_add(pre):
        # Create random acyclic directed graph
        dag = Composition(
            pre,
            node_processing_done_cb=node_processing_done_cb,
            schedule_done_cb=scheduler_completed
        )
        return conductor.add(dag)

    await asyncio.gather(*[do_conductor_add(pre) for pre in samples])
    await test_future_completed
