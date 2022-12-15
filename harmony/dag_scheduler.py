import logging
import asyncio
import functools
from asyncio import Queue
from typing import NoReturn, Dict, Optional, Callable, Any, Hashable
import uuid
from graphlib import TopologicalSorter
import time

logger = logging.getLogger()


class TryAgainException(Exception):
    pass


class ContinueAfterErrorException(Exception):
    pass


class StopScheduleException(Exception):
    pass


class DAGScheduler:
    """
    Directed acyclic graph scheduler that iterates over the graph in topological order. Processing of individual
    nodes, handling failures and retry policies are outside the scope of this class
    """
    def __init__(
            self,
            graph: Optional[Dict] = None,
            schedule_completed_cb: Optional[Callable[[Any, Optional[Exception], float], NoReturn]] = None,
            node_processing_error_cb: Optional[Callable[[str, Exception], NoReturn]] = None,
            event_loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        self.instance_id: str = uuid.uuid4().hex
        self._event_loop: asyncio.AbstractEventLoop = event_loop or asyncio.get_running_loop()
        self._graph: Dict = graph or {}
        self._schedule_completed_cb: Optional[Callable[[Any, Optional[Exception], float], NoReturn]] = schedule_completed_cb
        self._node_processing_error_cb: Optional[Callable[[str, Exception], NoReturn]] = node_processing_error_cb
        self._ts = TopologicalSorter(self._graph)
        self._task_queue: Queue = Queue()
        self._start_time: Optional[float] = None
        self._end_time: Optional[float] = None
        self.started: bool = False
        self.running: bool = False
        self.paused: bool = False
        self.stopped: bool = False
        self.stopped_error: Optional[Exception] = None
        self.running_tasks: Dict = {}  # Index of running tasks

    @property
    def completed(self):
        """
        READONLY property,
        :return: True if all vertexes have been processed, returns False until first started
        """
        if self.started:
            return not self._ts.is_active()
        else:
            return False

    def _process_node_complete(self, node: Hashable, future: asyncio.Future) -> NoReturn:
        """
        Key scheduling considerations to be applied here:
        * What is the retry policy?
        * Should the overall processing be ended or stopped on errors?
        * What overall timeout or individual vertex processing timeouts policies are to be applied?

        Expected Exceptions:
        * asyncio.exceptions.CancelledError - Future / Coroutine explicitly cancelled
        * asyncio.exceptions.TimeoutError - asyncio schedule timeout
        * TryAgainException(Exception):
        * ContinueAfterErrorException(Exception):
        * StopScheduleException(Exception):

        """
        self.running_tasks.pop(node, None)
        error = future.exception()
        if error:
            # handle processing exceptions
            logger.debug("Node processing error, %s: %s", node, error)
            if isinstance(error, TryAgainException):
                logger.debug("Node processing, try again, %s", node)
                self._process_node(node)

            elif isinstance(error, ContinueAfterErrorException):
                logger.debug("Node processing, ignore error, %s", node)
                self._ts.done(node)

            elif isinstance(error, StopScheduleException):
                logger.debug("Node processing, stop schedule, %s", node)
                self.stopped_error = error
                self.stop_processing()

            else:
                # Handle general runtime errors, do not mark the node as done, add back to running tasks
                logger.error("Node processing general error, %s", error)
                self.running_tasks[node] = future

        else:
            self._ts.done(node)
            logger.debug("Node processing complete, %s", node)

    async def process_node(self, node: Hashable):
        """
        Stub to be implemented in subclasses of this class.

        It is prudent that implementations think about the following likely exceptions:
        * asyncio.exceptions.CancelledError - Future / Coroutine explicitly cancelled
        * asyncio.exceptions.TimeoutError - asyncio schedule timeout

        :param node:
        :return:
        """
        raise NotImplementedError()

    def _process_node(self, node: Hashable):
        result_future = self._event_loop.create_task(self.process_node(node))
        result_future.add_done_callback(functools.partial(self._process_node_complete, node))
        self.running_tasks[node] = result_future

    def stop_processing(self) -> bool:
        if self.stopped:
            return False
        else:
            self.stopped = True

    def pause_processing(self) -> bool:
        """
        Stop processing new vertexes, allow all current processing to complete.
        :return: True if there are still vertexes to process, else return False
        """
        if not self.stopped and not self.paused and self._ts.is_active():
            self.paused = True
            return True
        else:
            return False

    async def start_processing(self) -> NoReturn:
        """
        Begin processing of all unprocessed vertexes, also called to resume after processing has being paused.
        No amendments to the graph are allowed after processing has first started or during a pause.

        If any cycle is detected, graphlib.CycleError will be raised

        Only one instance of the start_processing() coroutine is allowed.

        Once Stopped, the scheduler can never be started again.

        :return: NoReturn
        """
        if self.stopped:
            return
        if self.running:
            return
        if self.completed:
            return
        if self.paused:
            self.paused = False
        if not self.started:
            # Only call prepare once
            self._ts.prepare()
            self.started = True
            self._start_time = time.time()
        try:
            self.running = True
            list(map(self._task_queue.put_nowait, self._ts.get_ready()))
            while not self.stopped and not self.paused and self._ts.is_active():
                if not self._task_queue.empty():
                    task_node = self._task_queue.get_nowait()
                    self._process_node(task_node)

                for next_node in self._ts.get_ready():
                    await self._task_queue.put(next_node)

                # This sleep is added here to force the event loop to schedule background awaitables. This is
                # typically the case where there's no background async io activity, like with unittests
                # or in synchronous applications.
                await asyncio.sleep(0)
            await asyncio.gather(*[f for f in self.running_tasks.values()])

        except Exception as err:
            logger.error("Scheduler processing error: %s", err)
            self.stopped_error = err
            raise

        finally:
            self.running = False
            self._end_time = time.time()
            if (not self._ts.is_active() or self.stopped) and self._schedule_completed_cb:
                self._schedule_completed_cb(self, self.stopped_error, (self._end_time - self._start_time))
