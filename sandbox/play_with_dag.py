import logging
import asyncio
from typing import Dict

from harmony.dag_scheduler import DAGScheduler

logger = logging.getLogger()


class Orchestrator(DAGScheduler):
    @classmethod
    async def process_node(cls, node):
        logger.debug("Processing node, %s", node)
        # Do some work with the node
        await asyncio.sleep(0.001)


def orchestrator_completed(instance, error, time_taken):
    logger.info("Schedule complete, %s. error: %s", instance.instance_id, error)


async def main():
    # sample_graph: Dict = {"D": {"B", "C"}, "C": {"A"}, "B": {"A"}}
    sample_graph: Dict = {'0': {'2'}, '1': {'4'}, '2': {'0'}, '4': {'1', '3'}, '3': {'4'}, '5': {'7'}, '6': {'8'}, '7': {'8'}, '8': {'11'}, '10': {'12'}, '11': {'8'}, '12': {'10'}, '15': {'20'}, '20': {'21'}, '16': {'19'}, '19': {'16'}, '21': {'22'}, '22': {'25'}, '25': {'24', '22'}, '24': {'25'}, '26': {'27'}, '27': {'26'}, '30': {'32'}, '32': {'30'}}

    dag: DAGScheduler = Orchestrator(
        sample_graph,
        schedule_completed_cb=orchestrator_completed,
    )
    await dag.start_processing()


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
