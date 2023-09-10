import asyncio
import logging
from typing import Any, NoReturn, Dict, Optional
from uuid import uuid4
import datetime

from harmony.melody.interface import NodeRunnerStatus, NodeStatusCallback, NodeExecutorFunction
from harmony.utils import get_environment_info

logger = logging.getLogger(__name__)
verbose = False


class NodeRunner:
    """ A generic task runner for processing nodes in a DAG. Loosely modelled from asyncio.Task and designed
        to run any task both asynchronously and synchronously.

        a NodeRunner instance is considered immutable once it has been started. A task executor is any
        function that takes in keyword arguments and returns Any result type that can be serialised into
        json.

        A task executor could also be a class instance that implements the __call__ method, that follows the
        same signature as the task executor function. the class can also implement prepare and or run methods
        that will be called by the AsyncTaskRunner instance before the task is executed.
    """
    _parent_id: str | None
    _instance_id: str
    _environment_info: Dict[str, Any]
    _status: NodeRunnerStatus
    _running_task: asyncio.Task | None
    _start_time: datetime.datetime | None
    _end_time: datetime.datetime | None
    _done: bool
    _error: BaseException | None
    _result: Any

    _task_params: Dict[str, Any]
    _task_timeout: int
    _task_status_cb: NodeStatusCallback
    _task_executor: NodeExecutorFunction | None

    def __init__(
            self,
            task_executor: Optional[NodeExecutorFunction] = None,
            task_params: Dict[str, Any] = None,
            task_timeout: Optional[int] = None,
            task_status_cb: Optional[NodeStatusCallback] = None,
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

        self._task_executor = task_executor
        self._task_params = {} if task_params is None else task_params
        self._task_timeout = 0 if task_timeout is None else task_timeout
        self._task_status_cb = task_status_cb

        if verbose:
            logger.debug(f"New {type(self).__name__} instance -> {self._instance_id}")

    def retry_task(self) -> "NodeRunner":
        """ As TaskRunners are immutable once executed, in order to retry a task, a new instance must be created
        for execution. This method returns a new instance of the NodeRunner class, with the same parameters as
        the current instance with the _parent_id set to the current instance_id.

        If the task had been chained with prepare and or run methods, the new instance will have the same parameters
        provided by the chained methods.

        :return: self.__class__(...)
        """
        if not self._done or self._status not in ("completed", "error", "timed_out", "cancelled"):
            raise RuntimeError(
                "A retry instance of this NodeRunner is only available after the task is done"
            )
        child_task_runner = self.__class__(
            task_executor=self._task_executor,
            task_params=self._task_params,
            task_timeout=self._task_timeout,
            task_status_cb=self._task_status_cb,
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
    def status(self):
        return self._status

    @status.setter
    def status(self, status: NodeRunnerStatus):
        if self._status != status:
            if self._task_status_cb:
                self._task_status_cb(self._instance_id, self._status, status)
            elif verbose:
                logger.debug(
                    f"Task status changed {self._instance_id}: {self._status} -> {status}"
                )
            self._status = status

    @property
    def task_info(self) -> Dict[str, Any]:
        """ Returns a dictionary of task information
        """
        information = self._environment_info
        information.update({
            "instance_id": self._instance_id,
            "parent_id": self._parent_id,
            "status": self._status,
            "task_timeout": self._task_timeout,
            "task_done": self._done,
            "start_time": self._start_time,
            "end_time": self._end_time,
        })
        return information

    def cancel(self) -> NoReturn:
        if not self._done and self._running_task is not None and not self._running_task.done():
            logger.debug(f"Cancelling task {self._instance_id}")
            self._running_task.cancel(f"Request to stop task {self._instance_id}")

    def prepare(self, **task_params) -> "NodeRunner":
        """ Prepares alternative parameters for the task executor
        A utility method to allow the task executor to be replaced with alternative parameters before the task is run.
        and before executor is called.
        This method allows for command chaining to be used. e.g.:
        - result = await AsyncTaskRunner(executor).prepare(task_params)()

        :param task_params:
        :return: self
        """
        if self._done or self._status not in ("new", "starting"):
            raise RuntimeError("Task runner has already started or is done")
        self.status = "starting"
        self._task_params = task_params
        return self

    def run(self, task_executor: NodeExecutorFunction) -> "NodeRunner":
        """ provides and alternate task executor to be used when the task is run. not this method does not
        call the task executor, it only sets it up to be called when the task is run.

        This method allows for command chaining to be used. e.g.:
        - result = await AsyncTaskRunner(config).run(executor)()
        - result = await AsyncTaskRunner().prepare(task_params).run(executor)()

        :param task_executor:
        :return: self
        """
        if self._done or self._status not in ("new", "starting"):
            raise RuntimeError("Task runner has already started or is done")
        self.status = "starting"
        self._task_executor = task_executor
        return self

    async def task_executor_wrapper(self, **task_params) -> Any:
        """ A wrapper for calling self._task_executor. This method provides a hook for subclasses to
        override the task executor wrapper.
        """
        try:
            if self._task_executor is None:
                raise RuntimeError("A task executor has not been set")
            return await self._task_executor(**task_params)
        except (BaseException, RuntimeError) as err:
            # logger.exception(f"Task execute runtime error: {err}")
            raise err

    async def __call__(self) -> Any:
        try:
            if self._done or self._running_task is not None or self._status not in ("new", "starting"):
                raise RuntimeError("Task runner has already started or is done")
            self._start_time = datetime.datetime.now(datetime.timezone.utc)
            self._environment_info = get_environment_info()
            self._environment_info.update({
                "start_time_utc": self._start_time,
                "end_time_utc": self._end_time,
            })
            self.status = "running"
            """ NOTE: work is handled in its coroutine state
            """
            work = self.task_executor_wrapper(**self._task_params)
            if self._task_timeout:
                work = asyncio.wait_for(work, self._task_timeout)
            self._running_task = asyncio.create_task(work)
            self._result = await self._running_task
            self.status = "completed"
        except asyncio.exceptions.CancelledError as err:
            self.status = "cancelled"
            self._error = err
            # logger.debug("Task cancelled, %s", self._instance_id)
        except asyncio.exceptions.TimeoutError as err:
            self.status = "timed_out"
            self._error = err
            # logger.debug(f"Task timed out after {self._task_timeout}s, {self._instance_id}")
        except BaseException as err:  # pylint: disable=broad-except
            self.status = "error"
            self._error = err
            # logger.exception(f"Task runtime error: {err}")
        finally:
            self._done = True
            self._end_time = datetime.datetime.now(datetime.timezone.utc)
            self._environment_info.update({
                "end_time_utc": self._end_time,
            })

        if self._error:
            raise self._error
        else:
            return self._result
