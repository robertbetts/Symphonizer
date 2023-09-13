import logging
from typing import Optional, Any, Dict
from uuid import uuid4

from nuropb.rmq_api import RMQAPI

logger = logging.getLogger(__name__)

_stateful_nuropb_clients: Dict[str, RMQAPI] = {}


class NuroPbInstrument:
    _amqp_url: str
    _rpc_response: bool

    def __init__(
        self,
        amqp_url: Optional[str] = None,
        rpc_response: Optional[bool] = None,
        stateful: Optional[bool] = None,
    ):
        self._amqp_url = amqp_url or ""
        self._rpc_response = True if rpc_response is None else rpc_response
        self._stateful = False if stateful is None else stateful

    def _init_nuro_pb_client(self, amqp_url: str) -> RMQAPI:
        if self._stateful:
            if amqp_url in _stateful_nuropb_clients:
                return _stateful_nuropb_clients[amqp_url]
            else:
                _stateful_nuropb_clients[amqp_url] = RMQAPI(amqp_url=amqp_url)
                return _stateful_nuropb_clients[amqp_url]
        else:
            return RMQAPI(amqp_url=amqp_url)

    async def __call__(self, **kwargs: Any) -> Any:
        if "amqp_url" in kwargs:
            self._amqp_url = kwargs["amqp_url"]
        if "rpc_response" in kwargs:
            self._rpc_response = kwargs["rpc_response"]

        connected = False
        try:
            nuropb_client = self._init_nuro_pb_client(amqp_url=self._amqp_url)
            if not nuropb_client.connected:
                await nuropb_client.connect()
            connected = nuropb_client.connected
        except Exception as e:
            logger.debug(f"Error connecting to NuroPb service mesh: {e}")
            nuropb_client = None

        if not connected:
            raise RuntimeError("NuroPb service mesh api failed to connect")
        else:
            try:
                service = kwargs["service"]
                method = kwargs["method"]
                params = kwargs.get("params", {})
                context = kwargs.get("context", {})
                trace_id = kwargs.get("trace_id", uuid4().hex)
                ttl = kwargs.get("ttl", 60 * 60 * 1000)  # default timeout set to 1 hour
                rpc_response = kwargs.get("rpc_response", self._rpc_response)
                return await nuropb_client.request(
                    service=service,
                    method=method,
                    params=params,
                    context=context,
                    trace_id=trace_id,
                    ttl=ttl,
                    rpc_response=rpc_response,
                )
            finally:
                if not self._stateful and connected:
                    try:
                        await nuropb_client.disconnect()
                    except Exception as e:
                        _ = e
