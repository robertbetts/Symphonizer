import logging
import pytest
import asyncio
from typing import NoReturn, Any
from harmony.task_runner import AsyncTaskRunner

sample_task_config = {
    "service": "client_docs",
    "method": "generate_doc",
    "params": {
        "template": "client_notify",
        "data": None
    }
}


@pytest.mark.asyncio
async def test_runner_setup():

    async def test_executor(task_config):
        return "Foo"

    async def notify_callback(old, new, config):
        return

    task_runner = AsyncTaskRunner(
        task_config=sample_task_config,
        task_timeout=20,
        status_event_cb=notify_callback,
    )
    with pytest.raises(NotImplementedError):
        await task_runner()

    task_runner = AsyncTaskRunner(
        task_config=sample_task_config,
        task_timeout=20,
        custom_task_executor=test_executor,
    )

    result = await task_runner()

    assert "Foo" == result


@pytest.mark.asyncio
async def test_runner_subclass():

    class CustomRunner(AsyncTaskRunner):
        async def task_executor(self) -> Any:
            return "Foo"

    async def notify_callback(old, new, config):
        return

    task_runner = CustomRunner(
        task_config=sample_task_config,
        task_timeout=20,
        status_event_cb=notify_callback,
    )

    result = await task_runner()
    assert "Foo" == result


async def test_runner_timeout():

    async def test_executor(task_config):
        await asyncio.sleep(10)

    task_runner = AsyncTaskRunner(
        task_config=sample_task_config,
        task_timeout=5,
        custom_task_executor=test_executor,
    )

    with pytest.raises(asyncio.exceptions.TimeoutError):
        try:
            await asyncio.gather(task_runner())
        except Exception as err:
            logging.info(err)
            raise


@pytest.mark.asyncio
async def test_runner_cancel():

    async def test_executor(task_config):
        await asyncio.sleep(10)

    async def stop_runner(runner, after) -> NoReturn:
        await asyncio.sleep(after)
        await runner.stop()

    task_runner = AsyncTaskRunner(
        task_config=sample_task_config,
        task_timeout=20,
        custom_task_executor=test_executor,
    )

    await task_runner.stop()

    with pytest.raises(asyncio.exceptions.CancelledError):
        await asyncio.gather(task_runner(), stop_runner(task_runner, 5))
