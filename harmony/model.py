import datetime
from enum import Enum
from typing import Optional, Dict, Any

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


class Talk(HashModel):
    name: str
    timeout: int
    service: str
    method: str
    params: Dict
    auth_key: Optional[str]


class Process(HashModel):
    name: str
    version: int
    timeout: int


class Step(HashModel):
    name: str
    process_id: str = Field(index=True)
    parent_id: str
    task_id: str
    timeout: int
    service: str
    method: str
    params: Dict
    auth_key: Optional


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
