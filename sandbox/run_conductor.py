from random import randint
import logging
import asyncio
import time

from harmony.conductor.conductor import Conductor
from harmony.melody.dag_scheduler import DAGScheduler, DAGNode

# from harmony.melody.dag_utils import random_dag

logger = logging.getLogger()


class MyScheduler(DAGScheduler):
    pass


test_sample_size = 10000
completed_count = 0
total_time = 0
test_start_time = time.time()
total_nodes = 0


def orchestrator_completed(instance, status, error, time_taken):
    global completed_count, total_time
    # logger.info("Schedule %s. instance_id:%s error: %s taken: %s", status, instance.instance_id, error, time_taken)
    completed_count += 1
    total_time += time_taken
    if completed_count == test_sample_size:
        test_time_taken = time.time()-test_start_time
        logging.info("Completed %s dag schedules in %s seconds, avg time: %s, avg schedule elapsed time %s",
                     completed_count, test_time_taken, test_time_taken/completed_count, total_time/completed_count)
        logging.info("Total nodes processed: %s, %s nps, avg node time %f, avg nodes per schedule %s",
                     total_nodes, total_nodes/test_time_taken, test_time_taken/total_nodes, total_nodes/completed_count)
        asyncio.get_running_loop().stop()


# def get_sample_dag_edges(sample_size):
#     global total_nodes
#     retval = []
#     while len(retval) < sample_size:
#         # Create random acyclic directed graph
#         nodes = randint(3, 20)
#         edges = randint(nodes, nodes + 5)
#         total_nodes += nodes
#         g = random_dag(nodes=nodes, edges=edges)
#         predecessors = {}
#         for parent, child in g.edges:
#             predecessors.setdefault(child, set()).add(parent)
#         retval.append(predecessors)
#     return retval

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


async def main():
    global test_start_time
    logging.info("main() starting")
    conductor = Conductor()
    samples = get_sample_dag_edges(test_sample_size)
    logging.info("Sample gen complete complete")
    test_start_time = time.time()

    def do_conductor_add(pre):
        # Create random acyclic directed graph
        dag = MyScheduler(pre, schedule_done_cb=orchestrator_completed)
        return conductor.add(dag)

    asyncio.gather(*[do_conductor_add(pre) for pre in samples])
    await asyncio.sleep(0)
    logging.info("main() Complete")


if __name__ == "__main__":
    logging.basicConfig(
        format="%(levelname)1.1s %(asctime)s.%(msecs)03d %(process)d %(module)s:%(lineno)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
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
