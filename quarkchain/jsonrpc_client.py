import httpx
import uuid

class JsonRpcError(Exception):
    def __init__(self, error):
        self.code = error.get("code")
        self.message = error.get("message")
        self.data = error.get("data")
        super().__init__(f"JSON-RPC Error {self.code}: {self.message}")

class JsonRpcClient:
    def __init__(self, url, timeout=10):
        self.client = httpx.Client(base_url=url, timeout=timeout)

    def call(self, method, *params):
        if len(params) == 1 and isinstance(params[0], (dict, list)):
            rpc_params = params[0]
        else:
            rpc_params = list(params)
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": rpc_params,
            "id": str(uuid.uuid4()),
        }

        resp = self.client.post("", json=payload)
        resp.raise_for_status()
        data = resp.json()

        if "error" in data:
            raise JsonRpcError(data["error"])
        if data.get("id") != payload["id"]:
            raise JsonRpcError({"code": -32600, "message": f"response id {data.get('id')!r} does not match request id {payload['id']!r}"})

        return data.get("result")

    def close(self):
        self.client.close()


class AsyncJsonRpcClient:
    def __init__(self, url, timeout=10):
        self.client = httpx.AsyncClient(base_url=url, timeout=timeout)

    async def call(self, method, *params):
        if len(params) == 1 and isinstance(params[0], (dict, list)):
            rpc_params = params[0]
        else:
            rpc_params = list(params)

        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": rpc_params,
            "id": str(uuid.uuid4()),
        }

        resp = await self.client.post("", json=payload)
        resp.raise_for_status()
        data = resp.json()

        if "error" in data:
            raise JsonRpcError(data["error"])
        if data.get("id") != payload["id"]:
            raise JsonRpcError({"code": -32600, "message": f"response id {data.get('id')!r} does not match request id {payload['id']!r}"})

        return data.get("result")

    async def close(self):
        await self.client.aclose()
