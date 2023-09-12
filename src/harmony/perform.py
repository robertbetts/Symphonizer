import asyncio
from asyncio import Queue, QueueEmpty
from typing import Dict
import logging
import functools

from harmony.composition import Composition

logger = logging.getLogger(__name__)


class Perform:
    """ Process multiple dag compositions concurrently
    """
    _high_water_count: int
    _low_water_count: int
    _high_water_reached: bool
    _incoming_queue: Queue
    _running_dags: Dict[str, Composition]

    def __init__(self):
        self._high_water_count = 100
        self._low_water_count = 50
        self._high_water_reached = False
        self._incoming_queue = Queue()
        self._running_dags = {}

    def _dag_completed(self, dag: Composition, future: asyncio.Future):
        self._running_dags.pop(dag.instance_id, None)
        # error = future.exception()
        # logger.debug("DagSchedule ended %s, error: %s", dag.instance_id, error)

        if self._high_water_reached and len(self._running_dags) < self._low_water_count:
            logging.debug("Low water mark reached %s", self._low_water_count)
            self._high_water_reached = False

        if not self._high_water_reached and not self._incoming_queue.empty():
            try:
                dag = self._incoming_queue.get_nowait()
                self._add(dag)
            except QueueEmpty:
                pass

    def _add(self, dag: Composition):
        # logger.debug("DagSchedule starting %s", dag.instance_id)
        task = asyncio.create_task(dag.start_processing())
        task.add_done_callback(functools.partial(self._dag_completed, dag))
        self._running_dags.setdefault(dag.instance_id, dag)

    async def add(self, dag: Composition):
        if dag.instance_id not in self._running_dags and not dag.running and not dag.stopped:
            if len(self._running_dags) < self._high_water_count:
                self._add(dag)
                if len(self._running_dags) == self._high_water_count:
                    self._high_water_reached = True
                    logging.debug("High water mark reached %s", self._high_water_count)
            else:
                await self._incoming_queue.put(dag)


