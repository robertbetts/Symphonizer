import logging
import pytest
import asyncio
from symphonizer.composition import (
    Composition,
    ContinueAfterErrorException,
    StopScheduleException,
    DAGNote,
)
from symphonizer.node_runner import NodeRunner

logger = logging.getLogger(__name__)


class AsyncTestPassException(Exception):
    pass


@pytest.mark.asyncio
async def test_runner_setup(sample_graph):
    completed_future = asyncio.Future()

    def scheduler_done_cb(instance, status, error=None, elapsed_time: float = 0):
        logger.debug(
            "Schedule %s, %s: error:%s, elapsed_time:%s",
            status,
            instance.instance_id,
            error,
            elapsed_time,
        )
        completed_future.set_exception(AsyncTestPassException())

    def node_processing_done_cb(node, status, error):
        logger.debug(
            "Node %s, %s: error:%s, elapsed_time:%s",
            status,
            node,
            error,
            (node.end_time - node.start_time),
        )

    class Orchestrator(Composition):
        @classmethod
        def configure_node_runner(cls, node: DAGNote):
            async def execute(**params):
                logging.debug("Processing node, %s", node)
                if node.node_name == "A":
                    raise ContinueAfterErrorException("Opps, %s failed", node)
                elif node.node_name == "C":
                    await asyncio.sleep(2)
                    raise StopScheduleException("C failed, stopping process")
                # Do some work with the node
                if node.node_name == "D":
                    await asyncio.sleep(3)
                else:
                    await asyncio.sleep(0.001)

            return NodeRunner().prepare(node=node).run(execute)

    dag = Orchestrator(
        sample_graph,
        schedule_done_cb=scheduler_done_cb,
        node_processing_done_cb=node_processing_done_cb,
    )

    await asyncio.wait_for(dag.start_processing(), timeout=None)

    with pytest.raises(AsyncTestPassException):
        await completed_future

    logger.debug("Test end status: %s", dag.status)
