import logging
import pytest
import asyncio
from typing import Dict
from harmony.dag_scheduler import DAGScheduler, ContinueAfterErrorException, StopScheduleException


class AsyncTestPassException(Exception):
    pass


@pytest.mark.asyncio
async def test_runner_setup():

    sample_graph: Dict = {"D": {"B"}, "C": {"A"}, "B": {"A"}}

    completed_future = asyncio.Future()

    def orchestrator_completed_cb(instance, error=None):
        logging.info("Schedule complete, %s: error:%s", instance.instance_id, error)
        assert isinstance(error, StopScheduleException)
        completed_future.set_exception(AsyncTestPassException())

    class Orchestrator(DAGScheduler):
        @classmethod
        async def process_node(cls, node):
            logging.debug("Processing node, %s", node)
            if node == "A":
                raise ContinueAfterErrorException("Opps, %s failed", node)
            elif node == "C":
                await asyncio.sleep(5)
                raise StopScheduleException("C failed, stopping process")
            # Do some work with the node
            if node == "D":
                await asyncio.sleep(10)
            else:
                await asyncio.sleep(0.001)

    dag: DAGScheduler = Orchestrator(
        sample_graph,
        schedule_completed_cb=orchestrator_completed_cb,
    )

    await asyncio.wait_for(dag.start_processing(), timeout=20)

    assert dag.stopped is True and isinstance(dag.stopped_error, StopScheduleException)

    with pytest.raises(AsyncTestPassException):
        await completed_future
