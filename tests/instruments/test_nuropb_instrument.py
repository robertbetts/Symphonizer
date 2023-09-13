import logging
import pytest
from uuid import uuid4
from typing import Any
import os


from symphonizer.instruments.nuropb_api import NuroPbInstrument


logger = logging.getLogger(__name__)
IN_GITHUB_ACTIONS = os.getenv("GITHUB_ACTIONS") == "true"
if IN_GITHUB_ACTIONS:
    pytest.skip("Skipping model tests when run in Github Actions", allow_module_level=True)


@pytest.mark.asyncio
async def test_instrument_nuropb():
    amqp_url = "amqp://guest:guest@127.0.0.1:5672/sandbox"
    instrument = NuroPbInstrument(
        amqp_url=amqp_url,
        rpc_response=None,
    )
    service = "sandbox"
    method = "test_method"
    params = {"param1": "value1"}
    context = {"context1": "value1"}
    ttl = 60 * 30 * 1000
    trace_id = uuid4().hex
    result = await instrument(
        service=service,
        method=method,
        params=params,
        context=context,
        ttl=ttl,
        trace_id=trace_id,
    )
    logger.debug(result)
    assert result == f"response from {service}.{method}"


@pytest.mark.asyncio
async def test_instrument_nuropb_runtime_params():
    instrument = NuroPbInstrument()
    service = "sandbox"
    method = "test_method"
    params = {"param1": "value1"}
    context = {"context1": "value1"}
    ttl = 60 * 30 * 1000
    trace_id = uuid4().hex
    result = await instrument(
        service=service,
        method=method,
        params=params,
        context=context,
        ttl=ttl,
        trace_id=trace_id,
        amqp_url="amqp://guest:guest@127.0.0.1:5672/sandbox",
        rpc_response=False,
    )
    logger.debug(result)
    assert result["result"] == f"response from {service}.{method}"


@pytest.mark.asyncio
async def test_instrument_nuropb_stateful():
    from symphonizer.instruments.nuropb_api import _stateful_nuropb_clients

    assert len(_stateful_nuropb_clients) == 0

    service = "sandbox"
    method = "test_method"
    amqp_url = "amqp://guest:guest@127.0.0.1:5672/sandbox"

    async def call_instrument() -> Any:
        instrument = NuroPbInstrument(stateful=True)
        params = {"param1": "value1"}
        context = {"context1": "value1"}
        ttl = 60 * 30 * 1000
        trace_id = uuid4().hex
        return await instrument(
            service=service,
            method=method,
            params=params,
            context=context,
            ttl=ttl,
            trace_id=trace_id,
            amqp_url=amqp_url,
            rpc_response=False,
        )

    result = await call_instrument()
    logger.debug(result)
    assert result["result"] == f"response from {service}.{method}"
    assert len(_stateful_nuropb_clients) == 1
    assert _stateful_nuropb_clients[amqp_url].connected

    await _stateful_nuropb_clients[amqp_url].disconnect()
    result = await call_instrument()
    logger.debug(result)
    assert result["result"] == f"response from {service}.{method}"
    assert len(_stateful_nuropb_clients) == 1
    assert _stateful_nuropb_clients[amqp_url].connected

    """ The following test is commented out as it requires manual manual intervention to test
    where the rabbitmq connection is forcibly closed
    """
    """
    import asyncio
    await asyncio.sleep(15)
    result = await call_instrument()
    logger.debug(result)
    assert result["result"] == f"response from {service}.{method}"
    """
