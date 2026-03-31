# httpx is chosen over aiohttp because it provides both sync and async clients
# with a unified API, keeping this module simple and consistent. aiohttp is
# async-only, so the sync client (JsonRpcClient) would need a separate HTTP
# implementation (e.g. urllib.request). httpx as a pure client library is
# lightweight (~200KB with deps) and doesn't overlap with aiohttp's server role.
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
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": list(params),
            "id": str(uuid.uuid4()),
        }

        resp = self.client.post("", json=payload)
        resp.raise_for_status()
        data = resp.json()

        if "error" in data:
            raise RuntimeError(data["error"])

        return data.get("result")

    def close(self):
        self.client.close()


class AsyncJsonRpcClient:
    def __init__(self, url, timeout=10):
        self.client = httpx.AsyncClient(base_url=url, timeout=timeout)

    async def call(self, method, *params):
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": list(params),
            "id": str(uuid.uuid4()),
        }

        resp = await self.client.post("", json=payload)
        resp.raise_for_status()
        data = resp.json()

        if "error" in data:
            raise JsonRpcError(data["error"])

        return data.get("result")
    
    async def call_with_dict_params(self, method, params):
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": str(uuid.uuid4()),
        }

        resp = await self.client.post("", json=payload)
        resp.raise_for_status()
        data = resp.json()

        if "error" in data:
            raise JsonRpcError(data["error"])

        return data.get("result")
    
    async def close(self):
        await self.client.aclose()        