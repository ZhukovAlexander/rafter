import unittest
from unittest import mock
import asyncio

from aiohttp.protocol import Request as HttpRequest

from rafter.service import exposed, ExposedCommand, ServiceMeta, JsonRPCService, JsonRpcHttpRequestHandler
from rafter import service
from rafter.exceptions import UnboundExposedCommand, UnknownCommand


class DecoratorTest(unittest.TestCase):

    def test_with_args(self):

        @exposed(write=True, slug='test')
        def foo(a, b):
            return a + b

        self.assertIsInstance(foo, ExposedCommand)

    def test_without_args(self):

        @exposed
        def foo():
            return None

        self.assertIsInstance(foo, ExposedCommand)


class ServiceMetaTest(unittest.TestCase):

    def test_meta_class(self):
        class Foo(metaclass=ServiceMeta):
            bar = ExposedCommand(lambda: None, True, 'lambda')

        self.assertTrue('lambda', Foo.exposed)


class DummyServer(mock.Mock):
            async def handle_write_command(slug, *args, **kwargs):
                return 'handle_write_command'

            async def handle_read_command(slug, *args, **kwargs):
                return 'handle_read_command'


class ExposedCommandTest(unittest.TestCase):

    def setUp(self):

        class Service(metaclass=ServiceMeta):

            _server = DummyServer()

            def func(self):
                return 'success'
            foo = ExposedCommand(func, True, 'foo')
            bar = ExposedCommand(func, False, 'bar')

        self.service_cls = Service

    def test_descriptor_get(self):

        self.assertIsInstance(self.service_cls.foo, ExposedCommand)
        self.assertIsInstance(self.service_cls().foo, ExposedCommand)

    def test_call_with_instance(self):
        service = self.service_cls()
        res = asyncio.get_event_loop().run_until_complete(service.foo())
        self.assertEqual(res, 'handle_write_command')

    def test_call_read_command(self):
        service = self.service_cls()
        res = asyncio.get_event_loop().run_until_complete(service.bar())
        self.assertEqual(res, 'handle_read_command')

    def test_call_unbound(self):
        with self.assertRaises(UnboundExposedCommand):
            asyncio.get_event_loop().run_until_complete(self.service_cls.bar())

    def test_command_apply(self):
        res = asyncio.get_event_loop().run_until_complete(self.service_cls().bar.apply())
        self.assertEqual(res, 'success')


class DummyService(mock.Mock):
    DISPATCHED = 'dispatched'
    async def dispatch(self, method, *args, **kwargs):
        if method == 'fail':
            raise Exception('Test Error')
        if method not in ('fail', 'foo', 'not_leader'):
            raise service.UnknownCommand
        if method == 'not_leader':
            raise service.NotLeaderException
        return self.DISPATCHED

    @exposed
    def fail(self):
        raise

    @exposed
    def foo(self, x):
        return x

class Request:
    def __init__(self, method='GET', version='1.1', path='/'):
        self.method = method
        self.version = version
        self.path = path

class Payload:
    def __init__(self, data=b'{"jsonrpc": "2.0", "method": "foo", "params": [1]}'):
        self.data = data

    async def read(self):
        return self.data

async def drain():
    return True


class JSONRPCServiceTest(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.get_event_loop()
        self.handler = JsonRpcHttpRequestHandler(DummyService(), debug=True, keep_alive=75)
        self.handler.transport = mock.MagicMock()
        self.handler.writer = mock.MagicMock()
        self.handler.writer.drain.return_value = drain()

    def test_405(self):
        res = self.loop.run_until_complete(
            self.handler.handle_request(
                HttpRequest(self.handler.transport, method='GET', path='/jsonrpc/method/'), Payload()
            )
        )
        self.assertIn(b'405', b''.join(c[1][0] for c in self.handler.writer.write.mock_calls))

    def test_invalid_json(self):
        res = self.loop.run_until_complete(
            self.handler.handle_request(
                HttpRequest(self.handler.transport, method='POST', path='/jsonrpc/method/'), Payload(data=b'{')
            )
        )
        self.assertIn(str(service.PARSE_ERROR).encode(), b''.join(c[1][0] for c in self.handler.writer.write.mock_calls))

    def test_invalid_jsonrpc_request(self):
        res = self.loop.run_until_complete(
            self.handler.handle_request(
                HttpRequest(self.handler.transport, method='POST', path='/jsonrpc/method/'),
                Payload(b'{"is_valid": false}')
            )
        )
        self.assertIn(str(service.INVALID_REQUEST).encode(), b''.join(c[1][0] for c in self.handler.writer.write.mock_calls))

    def test_internal_error(self):
        res = self.loop.run_until_complete(
            self.handler.handle_request(
                HttpRequest(self.handler.transport, method='POST', path='/jsonrpc/method/'),
                Payload(data=b'{"jsonrpc": "2.0", "method": "fail", "params": []}')
            )
        )
        self.assertIn(b'Test Error', b''.join(c[1][0] for c in self.handler.writer.write.mock_calls))

    def test_unknown_command(self):
        res = self.loop.run_until_complete(
            self.handler.handle_request(
                HttpRequest(self.handler.transport, method='POST', path='/jsonrpc/method/'),
                Payload(data=b'{"jsonrpc": "2.0", "method": "not_exists", "params": []}')
            )
        )
        self.assertIn(str(service.METHOD_NOT_FOUND).encode(), b''.join(c[1][0] for c in self.handler.writer.write.mock_calls))

    def test_not_a_leader_error(self):
        res = self.loop.run_until_complete(
            self.handler.handle_request(
                HttpRequest(self.handler.transport, method='POST', path='/jsonrpc/method/'),
                Payload(data=b'{"jsonrpc": "2.0", "method": "not_leader", "params": []}')
            )
        )
        self.assertIn(str(service.NOT_A_LEADER).encode(), b''.join(c[1][0] for c in self.handler.writer.write.mock_calls))

    def test_successful_request(self):
        res = self.loop.run_until_complete(
            self.handler.handle_request(
                HttpRequest(self.handler.transport, method='POST', path='/jsonrpc/method/'),
                Payload()
            )
        )
        self.assertIn(b'result', b''.join(c[1][0] for c in self.handler.writer.write.mock_calls))


def test_service_setup():
    assert JsonRPCService(None).setup() is None, 'Make sure it just works'


class BaseServiceTest(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.get_event_loop()

        class Service(service.BaseService):

            async def setup(self):
                pass

        self.service = Service(server=DummyServer())

    def test_dispatch_unknown_command(self):
        with self.assertRaises(UnknownCommand):
            self.loop.run_until_complete(self.service.dispatch('unknown'))
        with self.assertRaises(UnknownCommand):
            self.loop.run_until_complete(self.service.dispatch('setup'))

    def test_dispatch_defaul_commands(self):
        res = self.loop.run_until_complete(self.service.dispatch('peers'))
        self.assertEqual(res, 'handle_read_command')
