import inspect
from typing import Any, Callable, Dict, Optional, Awaitable

from aiohttp import web


class JsonRpcError(Exception):
    code = -32000
    message = "Server error"

    def __init__(self, message=None, data=None):
        super().__init__(message or self.message)
        self.message = message or self.message
        self.data = data

    def to_dict(self):
        error = {
            "code": self.code,
            "message": self.message,
        }
        if self.data is not None:
            error["data"] = self.data
        return error

class InvalidRequest(JsonRpcError):
    code = -32600
    message = "Invalid Request"

class MethodNotFound(JsonRpcError):
    code = -32601
    message = "Method not found"

class InvalidParams(JsonRpcError):
    code = -32602
    message = "Invalid params"


class ServerError(JsonRpcError):
    code = -32000
    message = "Server error"

class RpcMethods:
    def __init__(self):
        self._methods: Dict[str, Callable[..., Awaitable[Any]]] = {}

    # ========== dict ==========
    def __iter__(self):
        return iter(self._methods)

    def __getitem__(self, key):
        return self._methods[key]

    def __setitem__(self, key, value):
        self._methods[key] = value

    def items(self):
        return self._methods.items()

    def keys(self):
        return self._methods.keys()

    def values(self):
        return self._methods.values()

    # ========== decorator ==========
    def add(self, func: Callable[..., Awaitable[Any]] = None, *, name: str = None):
        """
        Usage：

        @methods.add
        async def foo(...):

        or：

        @methods.add(name="customName")
        async def foo(...):
        """
        if func is None:
            def wrapper(f):
                method_name = name or f.__name__
                self._methods[method_name] = f
                return f
            return wrapper

        method_name = name or func.__name__
        self._methods[method_name] = func
        return func

    async def dispatch(self, request_json: Dict[str, Any], context=None) -> Optional[Dict[str, Any]]:
        req_id = None

        try:
            if not isinstance(request_json, dict):
                raise InvalidRequest("Request must be object")

            req_id = request_json.get("id")

            if request_json.get("jsonrpc") != "2.0":
                raise InvalidRequest("Invalid JSON-RPC version")

            method = request_json.get("method")
            if not isinstance(method, str):
                raise InvalidRequest("Method must be string")

            is_notification = "id" not in request_json

            if method not in self._methods:
                raise MethodNotFound()

            handler = self._methods[method]
            params = request_json.get("params", [])

            # Check if handler accepts a context parameter
            sig = inspect.signature(handler)
            pass_context = context is not None and "context" in sig.parameters

            if isinstance(params, list):
                result = await handler(*params, context=context) if pass_context else await handler(*params)
            elif isinstance(params, dict):
                result = await handler(**params, context=context) if pass_context else await handler(**params)
            else:
                raise InvalidParams()

            if is_notification:
                return None

            return {
                "jsonrpc": "2.0",
                "result": result,
                "id": req_id,
            }

        except JsonRpcError as e:
            return {
                "jsonrpc": "2.0",
                "error": e.to_dict(),
                "id": req_id,
            }

        except TypeError as e:
            # Could be missing/extra arguments → treat as invalid params
            return {
                "jsonrpc": "2.0",
                "error": {
                    "code": -32602,
                    "message": str(e),
                },
                "id": req_id,
            }
        except Exception:
            return {
                "jsonrpc": "2.0",
                "error": {
                    "code": -32603,
                    "message": "Internal error",
                },
                "id": req_id,
            }

    async def aiohttp_handler(self, request: web.Request) -> web.Response:
        body = await request.json()

        # 支持 batch
        if isinstance(body, list):
            responses = [await self.dispatch(item) for item in body]
            return web.json_response(responses)

        response = await self.dispatch(body)
        return web.json_response(response)
