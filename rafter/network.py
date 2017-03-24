"""Contains network protocols for rafter

By default rafter uses UDP+multicast on top of the uvloop.

"""

import asyncio
import socket
import struct
import abc
import logging

from . import models

logger = logging.getLogger(__name__)

HANDELERS = {
    models.AppendEntriesRPCRequest: ('append_entries', models.AppendEntriesRPCResponse),
    models.AppendEntriesRPCResponse: ('append_entries_response', models.AppendEntriesRPCRequest),
    models.RequestVoteRPCRequest: ('request_vote', models.RequestVoteRPCResponse),
    models.RequestVoteRPCResponse: ('request_vote_response', None)
}


class Transport(metaclass=abc.ABCMeta):

    server = None

    def __init__(self):
        pass

    @abc.abstractmethod
    def setup(self, server):
        raise NotImplementedError

    @abc.abstractmethod
    def broadcast(self, data):
        raise NotImplementedError

    @abc.abstractmethod
    def send_to(self, data, addres):
        raise NotImplementedError

    @abc.abstractmethod
    def close(self):
        raise NotImplementedError


class UDPMulticastTransport(Transport):

    def __init__(self, host, port):
        self.host = host
        self.port = port

    def setup(self, server):
        self.server = server
        loop = asyncio.get_event_loop()
        sock = make_udp_multicast_socket(host=self.host, port=self.port)
        self.queue = asyncio.Queue(loop=loop)
        self.server_transport, self.server_protocol = loop.run_until_complete(
            loop.create_datagram_endpoint(
                lambda: UPDProtocolMsgPackServer(server, self.queue), sock=sock)
        )

    def broadcast(self, data):
        asyncio.ensure_future(self.queue.put((data, ('239.255.255.250', 10000))))

    def send_to(self, data, address):
        self.queue.put((data, address))

    def close(self):
        pass


def make_udp_multicast_socket(host, port, group='239.255.255.250'):

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    except AttributeError:  # pragma: nocover
        pass  # Some systems don't support SO_REUSEPORT
    sock.bind((host, port))
    group = socket.inet_aton(group)
    mreq = struct.pack('4sL', group, socket.INADDR_ANY)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    return sock


class UPDProtocolMsgPackServer:

    def __init__(self, server, queue):
        self.server = server
        self.queue = queue

    def connection_made(self, transport):
        self.transport = transport

        asyncio.ensure_future(self.start())

    async def start(self):
        while not self.transport.is_closing():
            data, dest = await self.queue.get()
            logger.debug('Sending %s to %s', data, dest)
            self.transport.sendto(models.RaftMessage({'content': data}).pack(), dest)

    def datagram_received(self, data, addr):
        content = models.RaftMessage.unpack(data).content
        logger.info('Received data: %s from %s', content.to_native(), addr)
        handler, resp_class = HANDELERS[type(content)]
        result = self.server.handle(handler, **content.to_native())

        if resp_class is not None and result:
            logger.debug('Sending %s to %s', result, addr)
            self.transport.sendto(models.RaftMessage({'content': result}).pack(), addr)

    def connection_lost(self, exc):  # pragma: nocover
        logger.info('Closing server transport at {}:{}'.format(*self.transport.get_extra_info('sockname')))


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

    def reset(self):  # pragma: nocover
        self.stop()
        self.start()
