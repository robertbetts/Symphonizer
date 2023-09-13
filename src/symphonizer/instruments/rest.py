from typing import Any
import aiohttp

try:
    from aiohttp import FormData
except ImportError:  # pragma: no cover
    FormData = None


class RestInstrument:
    """A REST executor that supports any of the HTTP verbs, request content types of form encoded or json, and
    response content type of json or text.
    """

    def __init__(self, **kwargs):
        self.url = kwargs.get("url", None)
        self.method = kwargs.get("method", "GET")
        self.content_type = kwargs.get("content_type", "application/json")
        self.headers = kwargs.get("headers", {})
        self.cookies = kwargs.get("cookies", {})
        self.body = kwargs.get("body", {})
        self.timeout = kwargs.get("timeout", None)
        self.proxy = kwargs.get("proxy", None)
        self.verify = kwargs.get("verify", True)
        if "content_type" not in self.headers and self.content_type:
            self.headers["Content-Type"] = self.content_type

    async def __call__(self, **params) -> Any:
        if aiohttp is None:
            raise ImportError("aiohttp is required for the RestInstrument")

        async with aiohttp.ClientSession() as session:
            body = params.get("body", self.body)
            method = params.get("method", self.method).lower()
            url = params.get("url", self.url)
            if url is None:
                raise ValueError("url is required")
            content_type = params.get("content_type", None)
            headers = params.get("headers", self.headers)
            cookies = params.get("cookies", self.cookies)
            proxy = params.get("proxy", self.proxy)
            verify = params.get("verify", self.verify)

            if content_type is not None:
                headers["Content-Type"] = content_type

            request_params = {
                "method": method,
                "url": url,
                "headers": headers,
                "cookies": cookies,
                "proxy": proxy,
                "verify_ssl": verify,
            }
            if headers["Content-Type"] == "application/json":
                request_params["json"] = body
            else:
                request_params["data"] = FormData(body)

            async with session.request(**request_params) as response:
                if response.status != 200:
                    response.raise_for_status()
                if response.content_type == "application/json":
                    return await response.json()
                else:
                    return await response.text()
