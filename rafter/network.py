import asyncio
import socket
import struct
import logging

import models

logger = logging.getLogger()

HANDELERS = {
    models.AppendEntriesRPCRequest: ('append_entries', models.AppendEntriesRPCResponse),
    models.AppendEntriesRPCResponse: ('append_entries_response', None),
    models.RequestVoteRPCRequest: ('request_vote', models.RequestVoteRPCResponse),
    models.RequestVoteRPCResponse: ('request_vote_response', None)
}


def make_socket(host, port, group='239.255.255.250'):

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    sock.bind((host, port))
    group = socket.inet_aton(group)
    mreq = struct.pack('4sL', group, socket.INADDR_ANY)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    return sock


class UDPProtocolProtobufServer:

    def __init__(self, server):
        self.server = server

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        print('Received data: %s from %s'.format(data, addr))
        content = RaftMessage.unpack(data).content
        handler = HANDELERS[type(content)]
        result = self.server.handle(handler[0], **content.to_native())

        if isinstance(content, (models.AppendEntriesRPCRequest, models.RequestVoteRPCRequest)):
            self.transport.sendto(handler[1](result).pack(), addr)

    def connection_lost(self, exc):
        print('Closing server transport at {}:{}'.format(*self.transport.get_extra_info('sockname')))


class UDPProtocolProtobufClient:

    def __init__(self, server, queue, loop):
        self.server = server
        self.queue = queue
        self.loop = loop

    def connection_made(self, transport):
        self.transport = transport

        asyncio.ensure_future(self.start())

    async def start(self):
        while not self.transport.is_closing():
            data, dest = await self.queue.get()
            self.transport.send_to(data.pack(), dest)

    def connection_lost(self, exc):
        print('Closing client transport at {}:{}'.format(*self.transport.get_extra_info('sockname')))


class ResetablePeriodicTask:
    """Periodic callback, which can be postponed by resetting it. It is base on this code snippet:
    http://code.activestate.com/lists/python-list/656117/

    """

    def __init__(self, interval=None, callback=lambda: None):
        self.interval = interval
        self._callback = callback
        self._running = False
        self._loop = asyncio.get_event_loop()

    def _run(self):
        if self._running:
            self._callback()
            self._handler = self._loop.call_later(self.interval, self._run)

    def start(self, interval=None):
        self._running = True
        self.interval = interval or self.interval
        self._handler = self._loop.call_later(self.interval, self._run)

    def stop(self):
        # this makes sure self.stop() works even when called intside a self._callback
        self._running = False
        self._handler.cancel()

    def reset(self):
        self.stop()
        self.start()
