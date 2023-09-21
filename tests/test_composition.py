import logging
import pytest
import asyncio
from graphlib import TopologicalSorter

from symphonizer.composition import Composition, DAGNote
from symphonizer.interface import StopScheduleException

logger = logging.getLogger(__name__)


class AsyncTestPassException(Exception):
    pass


@pytest.mark.asyncio
async def test_running_scheduler(sample_graph):
    ts = TopologicalSorter(sample_graph)
    static_order = tuple([str(item) for item in ts.static_order()])
    logger.debug("TopologicalSorter: %s", static_order)
    assert static_order == ("A", "C", "B", "D")

    completed_future = asyncio.Future()

    def scheduler_done_cb(instance, status, error=None, elapsed_time: float = 0):
        assert not isinstance(error, StopScheduleException)
        completed_future.set_exception(AsyncTestPassException())

    def node_processing_done_cb(node, status, error):
        logger.debug(
            "Node %s, %s: error:%s, elapsed_time:%s",
            status,
            node,
            error,
            (node.end_time - node.start_time),
        )

    dag = Composition(
        sample_graph,
        schedule_done_cb=scheduler_done_cb,
        node_processing_done_cb=node_processing_done_cb,
    )

    await asyncio.wait_for(dag.start_processing(), timeout=None)
    with pytest.raises(AsyncTestPassException):
        await completed_future
    logger.debug("Scheduler done, status: %s", dag.status)
