import unittest
from unittest import mock
import asyncio

from rafter.service import command, ExposedCommand
from rafter import service
from rafter.exceptions import UnboundExposedCommand, UnknownCommand, NotLeaderException


class DecoratorTest(unittest.TestCase):

    def test_with_args(self):

        @command(write=True, slug='test')
        def foo(a, b):
            return a + b

        self.assertIsInstance(foo, ExposedCommand)

    def test_without_args(self):

        @command
        def foo():
            return None

        self.assertIsInstance(foo, ExposedCommand)


class DummyServer(mock.Mock):
            async def handle_write_command(slug, *args, **kwargs):
                return 'handle_write_command'

            async def handle_read_command(slug, *args, **kwargs):
                return 'handle_read_command'


class ExposedCommandTest(unittest.TestCase):

    def setUp(self):

        class Service:

            server = DummyServer()

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
            raise NotLeaderException
        return self.DISPATCHED

    @command
    def fail(self):
        raise Exception

    @command
    def foo(self, x):
        return x

async def drain():
    return True


class BaseServiceTest(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.get_event_loop()

        class Service(service.BaseService):

            def setup(self, server):
                super().setup(server)

        self.service = Service()
        self.service.setup(DummyServer())

    def test_dispatch_unknown_command(self):
        with self.assertRaises(UnknownCommand):
            self.loop.run_until_complete(self.service.dispatch('unknown'))
        with self.assertRaises(UnknownCommand):
            self.loop.run_until_complete(self.service.dispatch('setup'))

    def test_dispatch_default_commands(self):
        res = self.loop.run_until_complete(self.service.dispatch('peers'))
        self.assertEqual(res, 'handle_read_command')
