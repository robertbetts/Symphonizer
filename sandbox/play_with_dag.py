import logging
import asyncio
from typing import Dict

from harmony.melody.dag_scheduler import DAGScheduler, DAGNode

logger = logging.getLogger()


class Orchestrator(DAGScheduler):
    pass


def orchestrator_completed(instance, status, error, time_taken):
    logger.info("Schedule %s. instance_id:%s error: %s taken: %s", status, instance.instance_id, error, time_taken)
    asyncio.get_running_loop().stop()


async def main():
    # sample_graph: Dict = {'0': {'2'}, '1': {'4'}, '2': {'0'}, '4': {'1', '3'}, '3': {'4'}, '5': {'7'}, '6': {'8'}, '7': {'8'}, '8': {'11'}, '10': {'12'}, '11': {'8'}, '12': {'10'}, '15': {'20'}, '20': {'21'}, '16': {'19'}, '19': {'16'}, '21': {'22'}, '22': {'25'}, '25': {'24', '22'}, '24': {'25'}, '26': {'27'}, '27': {'26'}, '30': {'32'}, '32': {'30'}}
    sample_graph = {
        DAGNode("D"): {DAGNode("B"), DAGNode("C")},
        DAGNode("C"): {DAGNode("A")},
        DAGNode("B"): {DAGNode("A")}
    }
    dag: DAGScheduler = Orchestrator(
        sample_graph,
        schedule_done_cb=orchestrator_completed,
    )
    await dag.start_processing()


if __name__ == "__main__":
    logging.basicConfig(
        format="%(levelname)1.1s %(asctime)s.%(msecs)03d %(process)d %(module)s:%(lineno)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.DEBUG,
    )
    logger.info("starting")
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.create_task(main())
        loop.run_forever()
    except Exception as e:
        logger.exception(e)
    finally:
        logger.info("completed")
