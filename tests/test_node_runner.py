import logging
import pytest
import asyncio
from typing import NoReturn
from harmony.node_runner import NodeRunner


logger = logging.getLogger(__name__)

sample_node_params = {
    "service": "client_docs",
    "method": "generate_doc",
    "params": {
        "template": "client_notify",
        "data": None
    }
}


@pytest.mark.asyncio
async def test_runner_setup():

    async def test_executor(**node_params):
        return f"Node executed with {node_params['service']}.{node_params['method']}"

    def node_status_callback(instance, old, new):
        assert isinstance(instance, NodeRunner)
        logger.info("Node status changed, %s: %s -> %s", instance.instance_id, old, new)

    node_runner = NodeRunner(
        node_params=sample_node_params,
        node_status_cb=node_status_callback,
    )
    with pytest.raises(RuntimeError):
        await node_runner()

    node_runner = NodeRunner(
        node_params=sample_node_params,
        node_executor=test_executor,
    )

    result = await node_runner()
    assert result == "Node executed with client_docs.generate_doc"


@pytest.mark.asyncio
async def test_runner_subclass():

    class CustomRunner(NodeRunner):
        async def node_executor_wrapper(self, **params) -> str:
            return "Foo"

    node_runner = CustomRunner(
        node_params=sample_node_params,
    )

    result = await node_runner()
    assert "Foo" == result


@pytest.mark.asyncio
async def test_runner_timeout():

    async def test_executor(**node_params):
        await asyncio.sleep(5)

    node_runner = NodeRunner(
        node_params=sample_node_params,
        node_timeout=3,
    )

    with pytest.raises(asyncio.exceptions.TimeoutError):
        await asyncio.gather(node_runner.run(test_executor)())
    assert node_runner.status == "timed_out"


@pytest.mark.asyncio
async def test_runner_cancel():

    async def test_executor(**node_params):
        await asyncio.sleep(5)

    async def cancel_runner(runner, after) -> NoReturn:
        await asyncio.sleep(after)
        runner.cancel()

    node_runner = NodeRunner(
        node_executor=test_executor,
    )

    with pytest.raises(asyncio.exceptions.CancelledError):
        await asyncio.gather(node_runner(), cancel_runner(node_runner, 3))
    assert node_runner.status == "cancelled"


@pytest.mark.asyncio
async def test_runner_prepare_and_run():

    async def init_executor(**node_params) -> str:
        return f"Node executed with {node_params['service']}.{node_params['method']}"

    init_params = {
        "service": "client_docs",
        "method": "generate_doc",
        "params": {
            "template": "client_notify",
            "data": None
        }
    }

    node_runner = NodeRunner(
        node_params=init_params,
        node_executor=init_executor,
    )

    new_params = {
        "client_id": "1234",
    }

    async def new_executor(client_id) -> str:
        return f"client documents produced for {client_id}"

    result = await node_runner.prepare(**new_params).run(new_executor)()

    assert result == "client documents produced for 1234"


@pytest.mark.asyncio
async def test_runner_retry():

    async def test_executor(sleep_time: int = 5) -> str:
        await asyncio.sleep(sleep_time)
        return "Node executed"

    async def cancel_runner(runner, after) -> NoReturn:
        await asyncio.sleep(after)
        runner.cancel()

    parent_params = {}
    parent_runner = NodeRunner(
        node_executor=test_executor,
        node_params=parent_params,
    )

    with pytest.raises(asyncio.exceptions.CancelledError):
        await asyncio.gather(parent_runner(), cancel_runner(parent_runner, 3))
    assert parent_runner.status == "cancelled"

    parent_info = parent_runner.node_info
    assert parent_info["status"] == "cancelled"
    assert parent_info["parent_id"] is None

    child_runner = parent_runner.retry_task().prepare(sleep_time=1)
    assert child_runner.parent_id == parent_runner.instance_id
    assert child_runner._error is None
    assert child_runner._result is None
    completed_tasks = await asyncio.gather(child_runner(), cancel_runner(parent_runner, 3))
    assert len(completed_tasks) == 2
    assert completed_tasks[0] == "Node executed"
    assert child_runner.status == "completed"


