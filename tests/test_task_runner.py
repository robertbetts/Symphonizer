import logging
import pytest
import asyncio
from typing import NoReturn
from harmony.node_runner import NodeRunner


logger = logging.getLogger(__name__)

sample_task_params = {
    "service": "client_docs",
    "method": "generate_doc",
    "params": {
        "template": "client_notify",
        "data": None
    }
}


@pytest.mark.asyncio
async def test_runner_setup():

    async def test_executor(**task_params):
        return f"Task executed with {task_params['service']}.{task_params['method']}"

    def task_status_callback(instance_id, old, new):
        logger.info("Task status changed, %s: %s -> %s", instance_id, old, new)

    task_runner = NodeRunner(
        task_params=sample_task_params,
        task_status_cb=task_status_callback,
    )
    with pytest.raises(RuntimeError):
        await task_runner()

    task_runner = NodeRunner(
        task_params=sample_task_params,
        task_executor=test_executor,
    )

    result = await task_runner()
    assert result == "Task executed with client_docs.generate_doc"


@pytest.mark.asyncio
async def test_runner_subclass():

    class CustomRunner(NodeRunner):
        async def task_executor_wrapper(self, **params) -> str:
            return "Foo"

    task_runner = CustomRunner(
        task_params=sample_task_params,
    )

    result = await task_runner()
    assert "Foo" == result


@pytest.mark.asyncio
async def test_runner_timeout():

    async def test_executor(**task_params):
        await asyncio.sleep(5)

    task_runner = NodeRunner(
        task_params=sample_task_params,
        task_timeout=3,
    )

    with pytest.raises(asyncio.exceptions.TimeoutError):
        await asyncio.gather(task_runner.run(test_executor)())
    assert task_runner.status == "timed_out"


@pytest.mark.asyncio
async def test_runner_cancel():

    async def test_executor(**task_params):
        await asyncio.sleep(5)

    async def cancel_runner(runner, after) -> NoReturn:
        await asyncio.sleep(after)
        runner.cancel()

    task_runner = NodeRunner(
        task_executor=test_executor,
    )

    with pytest.raises(asyncio.exceptions.CancelledError):
        await asyncio.gather(task_runner(), cancel_runner(task_runner, 3))
    assert task_runner.status == "cancelled"


@pytest.mark.asyncio
async def test_runner_prepare_and_run():

    async def init_executor(**task_params) -> str:
        return f"Task executed with {task_params['service']}.{task_params['method']}"

    init_params = {
        "service": "client_docs",
        "method": "generate_doc",
        "params": {
            "template": "client_notify",
            "data": None
        }
    }

    task_runner = NodeRunner(
        task_params=init_params,
        task_executor=init_executor,
    )

    new_params = {
        "client_id": "1234",
    }

    async def new_executor(client_id) -> str:
        return f"client documents produced for {client_id}"

    result = await task_runner.prepare(**new_params).run(new_executor)()

    assert result == "client documents produced for 1234"


@pytest.mark.asyncio
async def test_runner_retry():

    async def test_executor(sleep_time: int = 5) -> str:
        await asyncio.sleep(sleep_time)
        return "Task executed"

    async def cancel_runner(runner, after) -> NoReturn:
        await asyncio.sleep(after)
        runner.cancel()

    parent_params = {}
    parent_runner = NodeRunner(
        task_executor=test_executor,
        task_params=parent_params,
    )

    with pytest.raises(asyncio.exceptions.CancelledError):
        await asyncio.gather(parent_runner(), cancel_runner(parent_runner, 3))
    assert parent_runner.status == "cancelled"

    parent_info = parent_runner.task_info
    assert parent_info["status"] == "cancelled"
    assert parent_info["parent_id"] is None

    child_runner = parent_runner.retry_task().prepare(sleep_time=1)
    assert child_runner.parent_id == parent_runner.instance_id
    assert child_runner._error is None
    assert child_runner._result is None
    completed_tasks = await asyncio.gather(child_runner(), cancel_runner(parent_runner, 3))
    assert len(completed_tasks) == 2
    assert completed_tasks[0] == "Task executed"
    assert child_runner.status == "completed"


