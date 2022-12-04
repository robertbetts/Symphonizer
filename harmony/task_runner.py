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
        self.task_config: Dict = task_config
        self.task_timeout: int = task_timeout or None
        self.status_event_callback: Callable[[TaskRunnerStatus, TaskRunnerStatus, Dict], NoReturn] = status_event_callback
        self.custom_task_executor: Callable[[Dict], Any] = custom_task_executor
        self.instance_id = uuid.uuid4().hex
        self.status: TaskRunnerStatus = TaskRunnerStatus.NEW
        self.running_task: Optional[asyncio.Task] = None
        self.done: bool = False

        logger.debug("New %s %s", type(self), self.instance_id)
        logger.debug(get_environment_info())

    async def notify_status(self, status: TaskRunnerStatus):
        if self.status != status:
            if self.status_event_callback:
                await self.status_event_callback(self.status, status, self.task_config)
            else:
                logger.info("Task status event %s: %s -> %s",
                            self.instance_id,
                            self.status.value,
                            status.value)
            self.status = status

    async def stop(self) -> NoReturn:
        if self.done or self.running_task is None or self.running_task.done():
            return
        else:
            logger.debug("Shutting down task %s", self.instance_id)
            self.running_task.cancel("Request to stop running task {}".format(self.instance_id))

    async def task_executor(self) -> Any:
        raise NotImplementedError()

    async def __call__(self) -> Any:
        try:
            await self.notify_status(TaskRunnerStatus.STARTING)
            if self.custom_task_executor:
                await self.notify_status(TaskRunnerStatus.STARTING)
                await self.notify_status(TaskRunnerStatus.RUNNING)
                work = self.custom_task_executor(self.task_config)
            else:
                await self.notify_status(TaskRunnerStatus.RUNNING)
                work = self.task_executor()

            if self.task_timeout:
                work = asyncio.wait_for(work, self.task_timeout)
            self.running_task = asyncio.create_task(work)

            result = await self.running_task
            await self.notify_status(TaskRunnerStatus.COMPLETED)
            return result
        except asyncio.exceptions.CancelledError:
            await self.notify_status(TaskRunnerStatus.CANCELLED)
            logging.info("Task was cancelled, %s", self.instance_id)
            raise
        except asyncio.exceptions.TimeoutError:
            await self.notify_status(TaskRunnerStatus.ERROR)
            logging.info("Task was timed out at %s seconds, %s",
                         self.task_timeout,
                         self.instance_id)
            raise
        except Exception as err:
            await self.notify_status(TaskRunnerStatus.ERROR)
            logger.exception(err)
            raise
        finally:
            self.done = True
