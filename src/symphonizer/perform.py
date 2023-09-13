import asyncio
from asyncio import Queue, QueueEmpty
from typing import Dict, Any
import logging
import functools

from symphonizer.composition import Composition

logger = logging.getLogger(__name__)
verbose = False


class Perform:
    """Process multiple dag compositions concurrently"""

    _high_water_count: int
    _low_water_count: int
    _high_water_reached: bool
    _incoming_queue: Queue[Composition]
    _running_dags: Dict[str, Composition]

    def __init__(self) -> None:
        self._high_water_count = 100
        self._low_water_count = 50
        self._high_water_reached = False
        self._incoming_queue = Queue()
        self._running_dags = {}

    def _dag_completed(self, dag: Composition, future: asyncio.Future[Any]):
        """Handles a completed dag:Composition and schedules the next one to start. If the high water mark
        has been reached, adds the dag to the incoming queue. If the low water mark has been reached,
        schedules the next dag in the queue to be processed.
        """
        self._running_dags.pop(dag.instance_id, None)
        if verbose:
            error = future.exception()
            logger.debug("Composition dag ended %s, error: %s", dag.instance_id, error)

        if self._high_water_reached and len(self._running_dags) < self._low_water_count:
            logging.debug("Low water mark reached %s", self._low_water_count)
            self._high_water_reached = False

        if not self._high_water_reached and not self._incoming_queue.empty():
            try:
                dag = self._incoming_queue.get_nowait()
                self._start_dag(dag)
            except QueueEmpty:
                pass

    def _start_dag(self, dag: Composition) -> None:
        # logger.debug("Composition dag starting %s", dag.instance_id)
        task = asyncio.create_task(dag.start_processing())
        task.add_done_callback(functools.partial(self._dag_completed, dag))
        self._running_dags.setdefault(dag.instance_id, dag)

    async def add(self, dag: Composition) -> None:
        if (
            dag.instance_id not in self._running_dags
            and not dag.running
            and not dag.stopped
        ):
            if len(self._running_dags) < self._high_water_count:
                self._start_dag(dag)
                if (
                    len(self._running_dags) == self._high_water_count
                    and not self._high_water_reached
                ):
                    self._high_water_reached = True
                    logging.debug("High water mark reached %s", self._high_water_count)
            else:
                await self._incoming_queue.put(dag)
