import asyncio
import logging
from typing import Any, NoReturn, Dict, Callable, Optional
import uuid
from enum import Enum

from harmony.utils import get_environment_info

logger = logging.getLogger("harmony")


class TaskRunnerStatus(Enum):
    NEW: str = "New"
    STARTING: str = "Starting"
    RUNNING: str = "Running"
    COMPLETED: str = "Completed"
    ERROR: str = "Error"
    CANCELLED: str = "Cancelled"


class AsyncTaskRunner:
    def __init__(
            self,
            task_config: Dict,
            task_timeout: Optional[int] = None,
            status_event_callback: Optional[Callable[[TaskRunnerStatus, TaskRunnerStatus, Dict], NoReturn]] = None,
            custom_task_executor: Optional[Callable[[Dict], Any]] = None,
    ):
        self._task_config: Dict = task_config
        self._task_timeout: int = task_timeout or None
        self._status_event_callback: Callable[[TaskRunnerStatus, TaskRunnerStatus, Dict], NoReturn] = status_event_callback
        self._custom_task_executor: Callable[[Dict], Any] = custom_task_executor
        self._running_task: Optional[asyncio.Task] = None
        self.instance_id = uuid.uuid4().hex
        self.status: TaskRunnerStatus = TaskRunnerStatus.NEW
        self.done: bool = False

        logger.debug("New %s %s", type(self), self.instance_id)
        logger.debug(get_environment_info())

    async def notify_status(self, status: TaskRunnerStatus):
        if self.status != status:
            if self._status_event_callback:
                await self._status_event_callback(self.status, status, self._task_config)
            else:
                logger.info("Task status event %s: %s -> %s",
                            self.instance_id,
                            self.status.value,
                            status.value)
            self.status = status

    async def stop(self) -> NoReturn:
        if self.done or self._running_task is None or self._running_task.done():
            return
        else:
            logger.debug("Shutting down task %s", self.instance_id)
            self._running_task.cancel("Request to stop running task {}".format(self.instance_id))

    async def task_executor(self) -> Any:
        raise NotImplementedError()

    async def __call__(self) -> Any:
        try:
            await self.notify_status(TaskRunnerStatus.STARTING)
            if self._custom_task_executor:
                await self.notify_status(TaskRunnerStatus.STARTING)
                await self.notify_status(TaskRunnerStatus.RUNNING)
                work = self._custom_task_executor(self._task_config)
            else:
                await self.notify_status(TaskRunnerStatus.RUNNING)
                work = self.task_executor()

            if self._task_timeout:
                work = asyncio.wait_for(work, self._task_timeout)
            self._running_task = asyncio.create_task(work)

            result = await self._running_task
            await self.notify_status(TaskRunnerStatus.COMPLETED)
            return result
        except asyncio.exceptions.CancelledError:
            await self.notify_status(TaskRunnerStatus.CANCELLED)
            logger.info("Task was cancelled, %s", self.instance_id)
            raise
        except asyncio.exceptions.TimeoutError:
            await self.notify_status(TaskRunnerStatus.ERROR)
            logger.info("Task timed out after %s seconds, %s",
                        self._task_timeout,
                        self.instance_id)
            raise
        except Exception as err:
            await self.notify_status(TaskRunnerStatus.ERROR)
            logger.exception(err)
            raise
        finally:
            self.done = True
