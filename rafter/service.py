from abc import ABCMeta, abstractmethod
import asyncio
import json

import aiohttp
import aiohttp.server

class UnboundExposedCommand(Exception):
    """Raised when the command is not bound to a service instance"""


class UnknownCommand(Exception):
    """Raised when someont tries to invoka an unknown command"""


class ExposedCommand:

    _service = None

    def __init__(self, func, write=True, slug=None):
        self._func = func
        self._write = write
        self.slug = slug or func.__qualname__

    def __get__(self, instance, owner):
        self._service = instance
        if instance:
            return instance.exposed[self.slug]
        return self

    async def __call__(self, *args, **kwargs):
        if not self._service:
            raise UnboundExposedCommand()
        if self._write:
            # wait, until it's safe to apply the command
            await self._service.can_handle(self.slug, args, kwargs)
        return await self._func(self._service, *args, **kwargs)


def _exposed(write=True, slug=None):
    def deco(func):
        return ExposedCommand(func, write, slug)
    return deco


def exposed(*args, **kwargs):
    if len(args) == 1 and callable(args[0]):
        return _exposed()(args[0])
    return _exposed(*args, **kwargs)
# double descriptor


class ServiceMeta(ABCMeta):

    def __new__(mcs, name, bases, attrs):

        attrs['exposed'] = {v.slug: v for k, v in attrs.items() if isinstance(v, ExposedCommand)}

        return type.__new__(mcs, name, bases, attrs)


class BaseService(metaclass=ServiceMeta):

    def __init__(self, server):
        self._server = server

    async def can_handle(self, slug, *args, **kwargs):
        return await self._server.handle_write_command(slug, args, kwargs)

    async def dispatch(self, cmd, *args, **kwargs):
        try:
            command = getattr(self, cmd)
        except AttributeError:
            raise UnknownCommand
        else:
            if not isinstance(command, ExposedCommand):
                raise UnknownCommand
        return await command(*args, **kwargs)

    @abstractmethod
    async def setup(self):
        raise NotImplementedError


PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603

VERSION = '2.0'


def is_valid_request(data):
    return all([
        'jsonrpc' in data and str(data['jsonrpc']) == VERSION,
        'method' in data,
        'params' not in data or isinstance(data['params'], (list, dict))
    ])


def encode(data):
    return json.dumps(data).encode() + b'\n'


class JsonRpcHttpRequestHandler(aiohttp.server.ServerHttpProtocol):

    def __init__(self, service, *args, **kwargs):
        self._service = service
        super().__init__(*args, **kwargs)

    async def handle_request(self, message, payload):

        if not message.path == '/jsonrpc/method/':
            # return 404
            return await super().handle_request(message, payload)

        if message.method != 'POST':
            http_response = aiohttp.Response(self.writer, 405, http_version=message.version)
            http_response.send_headers()
            return await http_response.write_eof()

        http_response = aiohttp.Response(self.writer, 200, http_version=message.version)

        rpc_response = dict(jsonrpc=VERSION)
        http_response.add_header('Content-Type', 'application/json')
        # response.add_header('Content-Length', '18')
        http_response.send_headers()

        try:
            data = json.loads((await payload.read()).decode())
        except ValueError as e:
            rpc_response.error = dict(code=PARSE_ERROR, message='Invalid JSON')
            http_response.write(encode(rpc_response))
            return await http_response.write_eof()

        if not is_valid_request(data):
            rpc_response['error'] = dict(code=INVALID_REQUEST, message='Invalid JSON-RPC Request object')
            http_response.write(encode(rpc_response))
            return await http_response.write_eof()
        try:
            params = data.get('params', ())
            result = await self._service.dispatch(data['method'], *params) \
                if isinstance(params, (list, tuple)) \
                else self._service.dispatch(data['method'], **params)
            rpc_response['result'] = result
            http_response.write(encode(rpc_response))

        except UnknownCommand as e:
            rpc_response['error'] = dict(code=METHOD_NOT_FOUND, message=str(e))
        except Exception as e:
            rpc_response['error'] = dict(code=INTERNAL_ERROR, message=str(e))

        http_response.write(encode(rpc_response))
        await http_response.write_eof()


class JsonRPCService(BaseService):
    def setup(self):
        loop = asyncio.get_event_loop()
        f = loop.create_server(
            lambda: JsonRpcHttpRequestHandler(self, debug=True, keep_alive=75),
            '0.0.0.0', '8080')
        srv = loop.run_until_complete(f)
        print('serving on', srv.sockets[0].getsockname())

    @exposed
    def foo(self, a, b):
        return a + b


class Example(BaseService):

    @exposed
    async def foo(self):
        return 1

    @exposed(slug='bar-slug')
    async def bar(self):
        return 'baz'


from .server import RaftServer
# asyncio.get_event_loop().run_until_complete(JsonRPCService(None).setup())
JsonRPCService(RaftServer()).setup()
asyncio.get_event_loop().run_forever()
