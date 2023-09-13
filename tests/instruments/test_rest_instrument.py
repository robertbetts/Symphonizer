import logging
import pytest

from symphonizer.instruments.rest import RestInstrument


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_instrument_rest_get():
    instrument = RestInstrument(
        url="https://httpbin.org/get",
        method="GET",
    )
    result = await instrument()
    logger.debug(result)


@pytest.mark.asyncio
async def test_instrument_rest_post_json():
    instrument = RestInstrument()
    result = await instrument(url="https://httpbin.org/post", method="POST", body={"foo": "bar"})
    logger.debug(result)
    assert result["json"]["foo"] == "bar"


@pytest.mark.asyncio
async def test_instrument_rest_post_form():
    instrument = RestInstrument()
    result = await instrument(
        url="https://httpbin.org/post",
        method="POST",
        body={"foo": "bar"},
        content_type="application/x-www-form-urlencoded"
    )
    logger.debug(result)
    assert result["form"]["foo"] == "bar"
