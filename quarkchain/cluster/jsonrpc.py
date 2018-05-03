import asyncio

from aiohttp import web
from jsonrpcserver.aio import methods


async def handle(request):
    request = await request.text()
    response = await methods.dispatch(request)
    if response.is_notification:
        return web.Response()
    else:
        return web.json_response(response, status=response.http_status)


class JSONRPCServer:

    def __init__(self, port):
        app = web.Application()
        app.router.add_post('/', handle)
        self.runner = web.AppRunner(app)
        self.loop = asyncio.get_event_loop()
        self.port = port

        # Bind RPC handler functions to this instance
        for rpcName in methods:
            func = methods[rpcName]
            methods[rpcName] = func.__get__(self, self.__class__)

    def start(self):
        self.loop.run_until_complete(self.runner.setup())
        site = web.TCPSite(self.runner, 'localhost', self.port)
        self.loop.run_until_complete(site.start())

    def shutdown(self):
        self.loop.run_until_complete(self.runner.cleanup())

    # JSON RPC handlers
    @methods.add
    async def echo(self, name):
        return name


if __name__ == '__main__':
    # web.run_app(app, port=5000)
    server = JSONRPCServer(8080)
    server.start()
    asyncio.get_event_loop().run_forever()
