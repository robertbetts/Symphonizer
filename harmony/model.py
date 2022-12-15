import datetime
from enum import Enum
from typing import Optional, Dict, Any, Set

from aredis_om import Field, HashModel


class InstanceStatus(Enum):
    STARTED: str = "Started"
    COMPLETED: str = "Completed"
    ERROR: str = "Error"
    CANCELLED: str = "Cancelled"


class ActivityStatus(Enum):
    WAITING: str = "Waiting"
    STARTED: str = "Started"
    COMPLETED: str = "Completed"
    ERROR: str = "Error"
    CANCELLED: str = "Cancelled"


class TaskProxy(HashModel):
    """
    task_config : could represent a rpc call with example properties of:
        type: str
        service: str
        method: str
        params: str
        auth_key: str
    """
    name: str
    timeout: int
    task_config: Dict


class Task(TaskProxy):
    preceding: Set[TaskProxy]
    succeeding: Set[TaskProxy]


class Process(HashModel):
    """
    A process is a set of acyclic, connected steps. Every process has a special start and end step
    with at least one more step representing the processing of a task.

    A Process is agnostic to the order of it steps(task) execution. Order is implied by tasks and
    the preceding or succeeding conditions of a task.

    Tasks are added as steps to a process, this could result in a multi-root and and multi leaf
    directed acyclic graph DAG.



    """
    name: str
    version: int
    timeout: int


class StepType(Enum):
    START: str = "Start"
    TASK: str = "Task"
    END: str = "End"


class Step(HashModel):
    """
    A task associated with a step might have preceding or succeeding tasks. In the presence these, there
    will be additional implied steps added to a process.

    There is danger allowing additional dependencies between steps, as these may lead to race conditions or
    conflicts with task level dependencies.
    """
    step_type: StepType
    name: str
    process_id: str = Field(index=True)
    task_id: str


class Instance(HashModel):
    process_id: str = Field(index=True)
    start_time: datetime.datetime = Field(default_factory=datetime.datetime.now)
    end_time: Optional[datetime.datetime]
    status: InstanceStatus = Field(default=InstanceStatus.STARTED)


class Activity(HashModel):
    instance_id: str = Field(index=True)
    parent_id: str = Field(index=True)
    step_id: str = Field(index=True)
    start_time: datetime.datetime = Field(default_factory=datetime.datetime.now)
    end_time: Optional[datetime.datetime]
    status: ActivityStatus = Field(default=ActivityStatus.WAITING)
    result: Optional[Any]
    error: Optional[str]
