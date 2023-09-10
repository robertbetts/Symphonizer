import logging
import pytest

from harmony.instruments.mock import MockInstrument


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_mock_instrument_call():
    instrument = MockInstrument(
        foo="bar"
    )
    result = await instrument()
    logger.debug(result)
    assert result == "Completed MockInstrument with 1 kwargs and 0 parameters"
