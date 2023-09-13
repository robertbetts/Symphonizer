import platform
import socket
import os
from typing import Dict, Any


def get_environment_info() -> Dict[str, Any]:
    return {
        "os": platform.system(),
        "cpu": platform.processor(),
        "python": platform.python_implementation(),
        "python_version": platform.python_version(),
        "python_build": platform.python_build(),
        "hostname": socket.gethostname(),
        "pid": os.getpid(),
    }
