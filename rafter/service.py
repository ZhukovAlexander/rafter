"""Contains a set of Service base classes

Services provide and API to delegate underlying state management
to the Raft server. This is pretty much the only API that a user
should interact with, when using rafter.

"""

from abc import ABCMeta
import asyncio
import logging
from inspect import isawaitable

from .exceptions import UnknownCommand, UnboundExposedCommand

logger = logging.getLogger(__name__)


class ExposedCommand:

    _server = None
    _service = None

    def __init__(self, func, write=True, slug=None):
        self._func = func
        self._write = write
        self.slug = func.__name__

    def __get__(self, instance, owner):
        if instance:
            self._service = instance
            self._server = instance.server
        return self

    async def __call__(self, *args, invocation_id=None, **kwargs):
        if not self._service:
            raise UnboundExposedCommand()
        if self._write:
            return await self._server.handle_write_command(self.slug, invocation_id, *args, **kwargs)
        return await self._server.handle_read_command(self.slug, *args, **kwargs)

    async def apply(self, *args, **kwargs):
        result = self._func(self._service, *args, **kwargs)
        return await result if isawaitable(result) else result


def _exposed(write=True, slug=None):
    def deco(func):
        return ExposedCommand(func, write, slug)
    return deco


def exposed(*args, **kwargs):
    if len(args) == 1 and callable(args[0]):
        return _exposed()(args[0])
    return _exposed(*args, **kwargs)


class ServiceMeta(ABCMeta):

    def __new__(mcs, name, bases, attrs):

        attrs['exposed'] = {v.slug: v for k, v in attrs.items() if isinstance(v, ExposedCommand)}

        return type.__new__(mcs, name, bases, attrs)


class BaseService(metaclass=ServiceMeta):

    server = None

    async def dispatch(self, cmd, *args, **kwargs):
        try:
            command = getattr(self, cmd)
        except AttributeError:
            raise UnknownCommand('Command not found: {0}'.format(cmd))
        else:
            if not isinstance(command, ExposedCommand):
                raise UnknownCommand('Command not found: {0}'.format(cmd))
        return await command(*args, **kwargs)

    def setup(self, server):
        self.server = server

    @exposed(write=False)
    def peers(self):  # pragma: nocover
        return self.server.list_peers()

    @exposed
    def add_peer(self, peer):  # pragma: nocover
        self.server.add_peer(peer)

    @exposed
    def remove_peer(self, peer_id):  # pragma: nocover
        self.server.remove_peer(peer_id)


class TelnetService(BaseService):

    _prompt = b'>'

    def __init__(self, host='127.0.0.1', port=8888):
        super().__init__()
        self.host, self.port = host, port

    def setup(self, server):
        super().setup(server)
        loop = asyncio.get_event_loop()
        coro = asyncio.start_server(self.handle_echo, self.host, self.port, loop=loop)
        loop.run_until_complete(coro)

    async def execute_command(self, command, args):
        try:
            result = await self.dispatch(command, *args)
        except Exception as e:
            return str(e).encode()
        return result

    async def handle_echo(self, reader, writer):
        running = True
        writer.write(b'Hello from rafter!\n')
        while running:
            writer.write(self._prompt)
            data = await reader.read(100)
            message = data.decode().strip()
            if not message:
                continue
            elif message == 'help':
                writer.write(b'Type the name of the command followed by the arguments')
            elif message in ('exit', 'q', 'quit'):
                writer.write(b'Buy...')
                running = False
            else:
                command, *args = message.split()

                writer.write(await self.execute_command(command, args))
            writer.write(b'\n')
            await writer.drain()

            print("Close the client socket")
        writer.close()
