import logging
from typing import Dict, Optional, Callable, Any, Literal, Type


logger = logging.getLogger(__name__)

DAGSchedulerType = Type["DAGScheduler"]
DAGNodeType = Type["DAGNode"]
NodeRunnerType = Type["NodeRunner"]

ErrorType = Dict[str, Any]
""" ErrorType: a dictionary with the following keys:
- error: str
- description: str
"""

NodeDoneStatus = Literal["retrying", "completed", "completed_error", "error", "timed_out", "cancelled"]

NodeRunnerStatus = Literal["new", "starting", "running", "completed", "error", "timed_out", "cancelled"]
NodeStatusCallback = Callable[[NodeRunnerType, NodeRunnerStatus, NodeRunnerStatus], None]
""" NodeStatusCallback:
- instance: NodeRunner instance
- old_status
- new_status
"""
NodeExecutorFunction = Callable[[..., Any], Any]
"""
A node executor is any function that takes in keyword arguments and returns Any result type that can
be serialised into json, e.g.:
 - def node_executor_function(**kwargs) -> Any
"""


ScheduleStatus = Literal["new", "starting", "running", "paused", "completed", "error", "timed_out", "cancelled"]
ScheduleDoneFunction = Callable[[DAGSchedulerType, ScheduleStatus, Optional[ErrorType], float], None]
""" ScheduleDoneFunction: is called when the schedule is done, i.e. is not able to continue the 
processing of any more nodes. 

The function is called with the following arguments:
- instance: DAGScheduler instance
- status: ScheduleStatus
- error: Optional[ErrorType]
- elapsed_time: float - in seconds
"""
NodeProcessDoneFunction = Callable[[DAGNodeType, NodeDoneStatus, Optional[BaseException]], None]
""" NodeProcessDoneFunction: is called when a node has been processed, i.e. the node has either 
completed or errored. 
NOTE: the underlying node processing / execution is immutable, once it has started and cannot be re-run. 
The NodeRunner class can return a child with NodeRunner.re_run() that can be used to process the node
again.   

The function is called with the following arguments:
- instance: DAGNode instance
- status: NodeDoneStatus
- error: Optional[BaseException]
"""


""" The Exception classes below are used to control the flow of the DAGScheduler and NodeRunner classes. 
"""


class TryAgainException(Exception):
    pass


class ContinueAfterErrorException(Exception):
    pass


class StopScheduleException(Exception):
    pass
