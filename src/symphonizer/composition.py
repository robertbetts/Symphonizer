import logging
import asyncio
import functools
from asyncio import Queue
from typing import Dict, Optional, Hashable, Any, Set, Type
import uuid
from graphlib import TopologicalSorter
import time

from symphonizer.instruments.mock import MockInstrument
from symphonizer.interface import (
    CompositionDoneFunction,
    NodeProcessDoneFunction,
    TryAgainException,
    ContinueAfterErrorException,
    StopScheduleException,
    ErrorType,
    CompositionStatus,
    NodeRunnerType,
    NodeDoneStatus,
    GraphType,
)
from symphonizer.node_runner import NodeRunner

logger = logging.getLogger(__name__)


class DAGNote:
    """
    A DAGNote is a vertex or node in a DAG graph. It contains the configuration required to process
    the node as well as the processing result.

    By default, all nodes are unique with only single instance of the node in the DAG. A key attribute
    of DAGNote class is that it is hashable and can be used as a key in a dictionary or as an
    element in a set.

    There use cases where there parameters to a node make is unique, e.g. a node that is processing
    a file, the file name is a parameter to the node. In this case, on initialization, the
    single_instance parameter can be set to false. The node is then identified by the node name and
    a unique instance id.
    """

    _single_instance: bool
    _node_name: str
    _instance_id: str

    node_data: Dict[str, Any]
    error: ErrorType | None
    result: Any | None
    start_time: float | None
    end_time: float | None

    def __init__(
        self, node_name: str, single_instance: Optional[bool] = None, **node_data: Any
    ):
        self._single_instance = True if single_instance is None else single_instance
        self._node_name = node_name
        self._instance_id = uuid.uuid4().hex

        self.node_data = node_data
        self.error = None
        self.result = None
        self.start_time = None
        self.end_time = None

    @property
    def single_instance(self) -> bool:
        """Post initialization, the single_instance property is immutable."""
        return self._single_instance

    @property
    def instance_id(self) -> str:
        """Post initialization, the instance_id property is immutable."""
        return self._instance_id

    @property
    def node_name(self) -> str:
        """Post initialization, the node_name property is immutable."""
        return self._node_name

    def __str__(self) -> str:
        if self.single_instance:
            return f"{self._node_name}"
        else:
            return f"{self._node_name}:{self._instance_id}"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, DAGNote):
            return self.__hash__() == other.__hash__()
        else:
            return False

    def __hash__(self) -> int:
        if self.single_instance:
            return hash(f"{self.__class__.__name__}.{self._node_name}")
        else:
            return hash(f"{self.__class__.__name__}.{self._instance_id}")


class Composition:
    """
    Composition represents a Directed Acyclic Graph (DAG) with the capabilities to iterates over
    the graph in topological order while processing each node.

    The handling failures and retry policies for nodes are outside the scope of this class, these
    important aspects are handled either through the NodeRunner implementation or broader
    implementation in which the Composition resides.

    Node scheduling considerations are:
    - Node processing retry policies?
    - Is the overall processing be stopped on node errors?
    - DAG or vertex processing timeouts policies?
    """

    _instance_id: str
    _graph: GraphType
    _schedule_done_cb: CompositionDoneFunction | None
    _node_processing_done_cb: NodeProcessDoneFunction | None
    _ts: TopologicalSorter[DAGNote]
    _task_queue: Queue[DAGNote]
    _running_tasks: Dict[DAGNote, asyncio.Future[NodeRunnerType]]
    _start_time: float
    _end_time: float | None
    _event_loop: asyncio.AbstractEventLoop

    started: bool
    running: bool
    paused: bool
    stopped: bool
    errored: BaseException | None

    def __init__(
        self,
        graph: GraphType,
        schedule_done_cb: Optional[CompositionDoneFunction] = None,
        node_processing_done_cb: Optional[NodeProcessDoneFunction] = None,
        event_loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        self.instance_id = uuid.uuid4().hex
        self._event_loop = event_loop or asyncio.get_running_loop()
        self._graph = graph or {}
        self._schedule_done_cb = schedule_done_cb
        self._node_processing_done_cb = node_processing_done_cb
        self._ts = TopologicalSorter(self._graph)
        self._task_queue = Queue()
        self._start_time = time.time()
        """ NOTE: the state of self._start_time is reset when the scheduling is started.
        """
        self._end_time = None
        self.started: bool = False
        self.running: bool = False
        self.paused: bool = False
        self.stopped: bool = False
        self.errored = None
        self._running_tasks = {}  # Index of running tasks

    @property
    def status(self) -> CompositionStatus:
        if self.stopped:
            return "cancelled"
        elif self.paused:
            return "paused"
        elif self.running:
            return "running"
        elif self.completed:
            return "completed"
        else:
            return "new"

    @property
    def completed(self) -> bool:
        """
        READONLY property,
        :return: True if all vertexes have been processed, returns False until first started
        """
        if self.started:
            return not self._ts.is_active()
        else:
            return False

    def process_node_done(self, node: DAGNote, future: asyncio.Future[Any]) -> None:
        """Performs the cleanup after the processing of a node has ended - is done.

        The following exceptions that are raised by the underlying node processing will
        influence the workflow of the Composition:
        - asyncio.exceptions.CancelledError: async processing was cancelled
        - asyncio.exceptions.TimeoutError: asyncio processing exceeded the timeout and was ended
        - TryAgainException(Exception): raised by the NodeRunner / task executor to indicate that
            the node processing should be retried
        - ContinueAfterErrorException(Exception): raised by the NodeRunner / task executor to
            indicate that DAG interation should continue as normal after an error
        - StopScheduleException(Exception): raised by the NodeRunner / task executor to indicate
            that the DAG processing should be stopped immediately.
        """
        node.end_time = time.time()
        self._running_tasks.pop(node, None)
        error = future.exception()
        if error is None:
            node.result = future.result()
        node.error = {
            "error": type(error).__name__,
            "description": str(error),
        }
        status: NodeDoneStatus = "error"
        if error:
            # handle processing exceptions
            # logger.debug("Node processing error, %s: %s", node, error)
            if isinstance(error, TryAgainException):
                # logger.debug("Node processing, try again, %s", node)
                self.process_node(node)
                status = "retrying"

            elif isinstance(error, ContinueAfterErrorException):
                # logger.debug("Node processing, ignore error, %s", node)
                self._ts.done(node)
                "completed_error"

            elif isinstance(error, StopScheduleException):
                # logger.debug("Node processing, stop schedule, %s", node)
                self.errored = error
                self.stop_processing()
                status = "cancelled"
            else:
                # Handle general runtime errors, do not mark the node as done, add back to running tasks
                # logger.error("Node processing general error, %s", error)
                self._running_tasks[node] = future
                status = "error"

        else:
            self._ts.done(node)
            # logger.debug("Node processing complete, %s", node)
            status = "completed"

        if self._node_processing_done_cb:
            self._node_processing_done_cb(node, status, error)

    def configure_node_runner(self, node: Hashable) -> NodeRunner:
        """Configure_node_runner is called to configure a node runner. The method is required to the
        implemented in subclasses of this class.

        For reference, in some use cases there may be a requirement for information regarding predecessors,
        their names, types, statuses or results. This information is available in self._graph.
        if not node in self._graph, then node does not have predecessors.
        else self._graph[node] returns the set of predecessors.

        :param node:
        :return: an instance of NodeRunner
        """
        _ = self, node
        node_runner = NodeRunner(node_executor=MockInstrument())
        return node_runner

    def process_node(self, node: DAGNote) -> None:
        """_process_node is called to process a single node.
        It's prudent that this implementation addresses likely exceptions:
        * asyncio.exceptions.CancelledError - Future / Coroutine explicitly cancelled
        * asyncio.exceptions.TimeoutError - asyncio schedule timeout

        :param node:
        :return:
        """
        node.start_time = time.time()
        node_runner = self.configure_node_runner(node)
        result_future = self._event_loop.create_task(node_runner())
        result_future.add_done_callback(functools.partial(self.process_node_done, node))
        self._running_tasks[node] = result_future

    def stop_processing(self) -> bool:
        """Stop processing of all vertexes, allow all current processing to complete.
        :return: True if there are still vertexes to process, else return False
        """
        if self.stopped:
            return False
        else:
            self.stopped = True
            return True

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

    async def start_processing(self) -> None:
        """
        Begin processing of all unprocessed vertexes, also called to resume after processing has
        being paused. No amendments to the graph are allowed after processing has first started
        or during a pause. Once stopped, the scheduler can not be started again.

        If any cycle is detected, graphlib.CycleError will be raised.

        :return: None
        """
        if self.stopped or self.running or self.completed:
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
            """ NOTE: The state of self.stopped or self.paused can be updated by another coroutine or thread
            """
            while not (self.stopped or self.paused) and self._ts.is_active():
                if not self._task_queue.empty():
                    task_node = self._task_queue.get_nowait()
                    self.process_node(task_node)

                for next_node in self._ts.get_ready():
                    await self._task_queue.put(next_node)

                """This sleep is added here to force the event loop to schedule background awaitables. This is
                typically the case where there's no background async io activity, like with unittests
                or in synchronous applications.
                """
                await asyncio.sleep(0)
            await asyncio.gather(*[f for f in self._running_tasks.values()])

        except Exception as err:
            logger.error("Scheduler processing error: %s", err)
            self.errored = err
            raise

        finally:
            self.running = False
            self._end_time = time.time()
            time_elapsed = self._end_time - self._start_time
            if (not self._ts.is_active() or self.stopped) and self._schedule_done_cb:
                self._schedule_done_cb(self, self.status, self.errored, time_elapsed)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}:{self._instance_id}"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, DAGNote):
            return self.__hash__() == other.__hash__()
        else:
            return False

    def __hash__(self) -> int:
        return hash(self._instance_id)
