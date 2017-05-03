# Copyright 2017 Alexander Zhukov
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Contains a set of Service base classes

Services provide and API to delegate underlying state management
to the Raft server. This is pretty much the only API that a user
should interact with, when using rafter.

"""

from abc import ABCMeta
import asyncio
import logging
from inspect import isawaitable

import asyncssh
import crypt

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


def _command(write=True, slug=None):
    def deco(func):
        return ExposedCommand(func, write, slug)
    return deco


def command(*args, **kwargs):
    if len(args) == 1 and callable(args[0]):
        return _command()(args[0])
    return _command(*args, **kwargs)


class BaseService(metaclass=ABCMeta):

    server = None

    async def dispatch(self, name, *args, **kwargs):
        try:
            cmd = getattr(self, name)
        except AttributeError:
            raise UnknownCommand('Command not found: {0}'.format(name))
        else:
            if not isinstance(cmd, ExposedCommand):
                raise UnknownCommand('Command not found: {0}'.format(name))
        return await cmd(*args, **kwargs)

    def setup(self, server):
        self.server = server

    @command(write=False)
    def peers(self):  # pragma: nocover
        return self.server.list_peers()

    @command
    def add_peer(self, peer):  # pragma: nocover
        self.server.add_peer(peer)

    @command
    def remove_peer(self, peer_id):  # pragma: nocover
        self.server.remove_peer(peer_id)


class TelnetService(BaseService):

    _prompt = b'>'

    def __init__(self, host: str ='127.0.0.1', port: int = 8888):
        super().__init__()
        self.host, self.port = host, port

    def setup(self, server):
        super().setup(server)
        loop = asyncio.get_event_loop()
        # coro = asyncio.start_server(self.handle_echo, self.host, self.port, loop=loop)
        # loop.run_until_complete(coro)

    async def execute_command(self, cmd, args):
        try:
            result = await self.dispatch(cmd, *args)
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
                cmd, *args = message.split()

                writer.write(await self.execute_command(cmd, args))
            writer.write(b'\n')
            await writer.drain()

            print("Close the client socket")
        writer.close()

passwords = {'rafter': 'rafter'}


class MySSHServer(asyncssh.SSHServer):
    def connection_made(self, conn):
        print('SSH connection received from %s.' %
              conn.get_extra_info('peername')[0])

    def connection_lost(self, exc):
        if exc:
            print('SSH connection error: ' + str(exc), file=sys.stderr)
        else:
            print('SSH connection closed.')

    def begin_auth(self, username):
        # If the user's password is the empty string, no auth is required
        return passwords.get(username) != ''

    def password_auth_supported(self):
        return True

    def validate_password(self, username, password):
        pw = passwords.get(username, '*')
        return crypt.crypt(password, pw) == pw


class SSHService(BaseService):
    def __init__(self, host: str ='127.0.0.1', port: int = 8022):
        super().__init__()
        self.host, self.port = host, port

    def setup(self, server):
        super().setup(server)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.start_server())

    async def execute_command(self, cmd, args):
        try:
            result = await self.dispatch(cmd, *args)
        except Exception as e:
            return str(e).encode()
        return result

    async def start_server(self):
        await asyncssh.create_server(MySSHServer, '', 8022, session_factory=self.handle_session)

    async def handle_session(self, stdin, stdout, stderr):
        stdout.write('Hello from rafter!\n')

        try:
            async for line in stdin:
                line = line.rstrip('\n')
                if line:
                    if line == 'help':
                        stdout.write(b'Type the name of the command followed by the arguments')
                    elif line in ('exit', 'q', 'quit'):
                        break
                    else:
                        cmd, *args = line.split()
                        stdout.write(await self.execute_command(cmd, args))

        except asyncssh.BreakReceived:
            pass

        stdout.write('Buy...')
        stdout.channel.exit(0)

