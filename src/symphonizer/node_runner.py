import asyncio
import logging
from typing import Any, Dict, Optional
from uuid import uuid4
import datetime

from symphonizer.interface import (
    NodeRunnerStatus,
    NodeStatusCallback,
    NodeExecutorFunction,
)
from symphonizer.utils import get_environment_info

logger = logging.getLogger(__name__)
verbose = False


class NodeRunner:
    """A class that abstracts the processing af a node as a task. Loosely modelled from asyncio.Task
    and designed to run either asynchronous or synchronous executors. A node executor is any function,
    class method and including a class instance that implements the __call__ method. Only keyword
    argument parameters are supported. It is good practice to restrict the inputs and outputs of a
    node executor to those types easily serialised into and out of json.

    A NodeRunner instance is considered immutable once it has been started and cannot be reconfigured.
    As a consequence, in order to retry a task, a new instance must be created for execution, see
    NodeRunner.retry_task() for further info.

    A task executor performs the underlying processing and is any function that takes in keyword
    arguments and returns Any result type that can be serialised into json.

    async def executor_function(**kwargs: Any) -> Any:
        ...

    class Executor:
        async def __call__(self, **kwargs: Any) -> Any:
            ...

    executor = `Executor()` or `executor_function`
    result = await NodeRunner(executor, params)()
    result = await NodeRunner().prepare(params).run(executor)()

    """

    _parent_id: str | None
    _instance_id: str
    _environment_info: Dict[str, Any]
    _status: NodeRunnerStatus
    _running_task: asyncio.Task[Any] | None
    _start_time: datetime.datetime | None
    _end_time: datetime.datetime | None
    _done: bool
    _error: BaseException | None
    _result: Any

    _node_params: Dict[str, Any]
    _node_timeout: int
    _node_status_cb: NodeStatusCallback | None
    _node_executor: NodeExecutorFunction | None

    def __init__(
        self,
        node_executor: Optional[NodeExecutorFunction] = None,
        node_params: Optional[Dict[str, Any]] = None,
        node_timeout: Optional[int] = None,
        node_status_cb: Optional[NodeStatusCallback] = None,
        parent_id: Optional[str] = None,
    ):
        self._parent_id = parent_id
        self._instance_id = uuid4().hex
        self._environment_info = {}
        self._status = "new"
        self._running_task = None
        self._start_time = None
        self._end_time = None
        self._done = False
        self._error = None
        self._result = None

        self._node_executor = node_executor
        self._node_params = {} if node_params is None else node_params
        self._node_timeout = 0 if node_timeout is None else node_timeout
        self._node_status_cb = node_status_cb

        if verbose:
            logger.debug(f"New {type(self).__name__} instance -> {self._instance_id}")

    def retry_task(self) -> "NodeRunner":
        """A NodeRunner is immutable once executed, in order to retry a task, a new instance must be created
        for execution. This method returns a new instance of the NodeRunner class, with the same parameters as
        the current instance with the _parent_id set to the current instance_id.

        If the task had been chained with prepare and or run methods, the new instance will have the same parameters
        provided by the chained methods.

        :return: self.__class__(...)
        """
        if not self._done or self._status not in (
            "completed",
            "error",
            "timed_out",
            "cancelled",
        ):
            raise RuntimeError(
                "A retry instance of this NodeRunner is only available after the task is done"
            )
        child_task_runner = self.__class__(
            node_executor=self._node_executor,
            node_params=self._node_params,
            node_timeout=self._node_timeout,
            node_status_cb=self._node_status_cb,
            parent_id=self._instance_id,
        )
        return child_task_runner

    @property
    def instance_id(self) -> str:
        return self._instance_id

    @property
    def parent_id(self) -> str | None:
        return self._parent_id

    @property
    def done(self) -> bool:
        return self._done

    @property
    def status(self) -> NodeRunnerStatus:
        return self._status

    @status.setter
    def status(self, status: NodeRunnerStatus) -> NodeRunnerStatus:
        if self._status != status:
            if self._node_status_cb:
                self._node_status_cb(self, self._status, status)
            elif verbose:
                logger.debug(
                    f"Task status changed {self._instance_id}: {self._status} -> {status}"
                )
            self._status = status
        return self._status

    @property
    def node_info(self) -> Dict[str, Any]:
        """Returns a dictionary of the node's task information"""
        information = self._environment_info
        information.update(
            {
                "instance_id": self._instance_id,
                "parent_id": self._parent_id,
                "status": self._status,
                "task_timeout": self._node_timeout,
                "task_done": self._done,
                "start_time": self._start_time,
                "end_time": self._end_time,
            }
        )
        return information

    def cancel(self) -> None:
        if (
            not self._done
            and self._running_task is not None
            and not self._running_task.done()
        ):
            logger.debug(f"Cancelling task {self._instance_id}")
            self._running_task.cancel(f"Request to stop task {self._instance_id}")

    def prepare(self, **params: Any) -> "NodeRunner":
        """Prepares alternative parameters for the task executor
        A utility method to allow the task executor to be replaced with alternative parameters before the task is run.
        and before executor is called.
        This method allows for command chaining to be used. e.g.:
        - result = await AsyncTaskRunner(executor).prepare(task_params)()

        :param params:
        :return: self
        """
        if self._done or self._status not in ("new", "starting"):
            raise RuntimeError("NodeRunner has already started or is done")
        self.status = "starting"
        self._node_params = params
        return self

    def run(self, node_executor: NodeExecutorFunction) -> "NodeRunner":
        """Provides and alternate node executor to be used when the node's task is run.
        NOTE: This method does not call the node executor, it only sets it up to be called
        when the task is run.

        This method allows for command chaining to be used. e.g.:
        - result = await AsyncTaskRunner(config).run(executor)()
        - result = await AsyncTaskRunner().prepare(params).run(executor)()

        :param node_executor:
        :return: self
        """
        if self._done or self._status not in ("new", "starting"):
            raise RuntimeError("This NodeRunner has already started or is done")
        self.status = "starting"
        self._node_executor = node_executor
        return self

    async def node_executor_wrapper(self, **params: Any) -> Any:
        """A wrapper for calling self._node_executor. This method provides a hook for subclasses to
        override execution behavior.
        """
        try:
            if self._node_executor is None:
                raise RuntimeError("A task executor has not been set")
            return await self._node_executor(**params)
        except BaseException as err:
            if verbose:
                logger.exception(f"Task execute runtime error: {err}")
            raise err

    async def __call__(self) -> Any:
        try:
            if (
                self._done
                or self._running_task is not None
                or self._status not in ("new", "starting")
            ):
                raise RuntimeError("Node runner has already started or is done")
            self._start_time = datetime.datetime.now(datetime.timezone.utc)
            self._environment_info = get_environment_info()
            self._environment_info.update(
                {
                    "start_time_utc": self._start_time,
                    "end_time_utc": self._end_time,
                }
            )
            self.status = "running"
            """ NOTE: work is handled in its coroutine state
            """
            work = self.node_executor_wrapper(**self._node_params)
            if self._node_timeout:
                work = asyncio.wait_for(work, self._node_timeout)
            self._running_task = asyncio.create_task(work)
            self._result = await self._running_task
            self.status = "completed"
        except asyncio.exceptions.CancelledError as err:
            self.status = "cancelled"
            self._error = err
            # logger.debug("Nodes task cancelled, %s", self._instance_id)
        except asyncio.exceptions.TimeoutError as err:
            self.status = "timed_out"
            self._error = err
            # logger.debug(f"Node's task timed out after {self._node_timeout}s, {self._instance_id}")
        except BaseException as err:  # pylint: disable=broad-except
            self.status = "error"
            self._error = err
            # logger.exception(f"Node's task had a runtime error: {err}")
        finally:
            self._done = True
            self._end_time = datetime.datetime.now(datetime.timezone.utc)
            self._environment_info.update(
                {
                    "end_time_utc": self._end_time,
                }
            )

        if self._error:
            raise self._error
        else:
            return self._result
