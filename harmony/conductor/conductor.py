import asyncio
from asyncio import Queue, QueueEmpty
from typing import Dict, Optional
import logging
import functools

from harmony.dag_scheduler import DAGScheduler

logger = logging.getLogger()


class Conductor:
    """
    Process multiple dag schedules concurrently
    """
    def __init__(
            self,
            event_loop: Optional[asyncio.AbstractEventLoop] = None):
        self._event_loop = event_loop or asyncio.get_running_loop()
        self._high_water_count = 100
        self._low_water_count = 50
        self._high_water_reached = False
        self._incoming_queue: Queue = Queue()
        self._running_dags: Dict[str, DAGScheduler] = {}

    def _dag_completed(self, dag: DAGScheduler, future: asyncio.Future):
        self._running_dags.pop(dag.instance_id, None)
        error = future.exception()
        if error:
            logger.debug("DagSchedule ended %s, error: %s", dag.instance_id, error)
        else:
            logger.debug("DagSchedule ended %s", dag.instance_id)

        if self._high_water_reached and len(self._running_dags) < self._low_water_count:
            logging.debug("Low water mark reached %s", self._low_water_count)
            self._high_water_reached = False

        if not self._high_water_reached and not self._incoming_queue.empty():
            try:
                dag = self._incoming_queue.get_nowait()
                self._add(dag)
            except QueueEmpty:
                pass

    def _add(self, dag: DAGScheduler):
        logger.debug("DagSchedule starting %s", dag.instance_id)
        result_future = self._event_loop.create_task(dag.start_processing())
        result_future.add_done_callback(functools.partial(self._dag_completed, dag))
        self._running_dags.setdefault(dag.instance_id, dag)

    async def add(self, dag: DAGScheduler):
        if dag.instance_id not in self._running_dags and not dag.running and not dag.stopped:
            if len(self._running_dags) < self._high_water_count:
                self._add(dag)
                if len(self._running_dags) == self._high_water_count:
                    self._high_water_reached = True
                    logging.debug("High water mark reached %s", self._high_water_count)
            else:
                await self._incoming_queue.put(dag)


