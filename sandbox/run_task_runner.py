import asyncio
import logging
from typing import Any, NoReturn, Dict, Callable, Optional
import platform
import socket
import os
import uuid
from enum import Enum

from harmony.task_runner import AsyncTaskRunner

logger = logging.getLogger()


async def main():
    notify_client = {
        "service": "client_docs",
        "method": "generate_doc",
        "params": {
            "template": "client_notify",
            "data": None
        }
    }

    async def test_executor(task_config):
        await asyncio.sleep(10)
        return "Foo"

    async def stop_runner(runner, after) -> NoReturn:
        await asyncio.sleep(after)
        await runner.stop()

    task_runner = AsyncTaskRunner(
        task_config=notify_client,
        task_timeout=20,
        custom_task_executor=test_executor,
    )

    try:
        result = await asyncio.gather(task_runner(), stop_runner(task_runner, 15))
        logger.debug("result: %s", result)
    except asyncio.exceptions.CancelledError as err:
        logger.error(err)
    except asyncio.exceptions.TimeoutError as err:
        logger.error(err)
    except Exception as err:
        logging.error(err)
        logging.exception(err)

if __name__ == "__main__":
    logging.basicConfig(
        format="%(levelname)1.1s %(asctime)s.%(msecs)03d %(process)d %(module)s:%(lineno)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.DEBUG,
    )
    logger.info("__main__ starting")
    try:
        asyncio.run(main())
    except Exception as e:
        logger.exception(e)
    finally:
        logger.info("__main__ completed")
