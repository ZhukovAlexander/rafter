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

MCAST_GROUP_IPV6 = 'ff15:7079:7468:6f6e:6465:6d6f:6d63:6173'


class UDPMulticastTransport(Transport):

    def __init__(self, host='0.0.0.0', port=10000, multicast_group=MCAST_GROUP_IPV6):
        self.host = host
        self.port = port
        self.multicast_group = multicast_group

    def setup(self, server):
        self.server = server
        loop = asyncio.get_event_loop()
        sock = make_udp_multicast_socket(host=self.host, port=self.port, group=self.multicast_group)
        self.queue = asyncio.Queue(loop=loop)
        self.server_transport, self.server_protocol = loop.run_until_complete(
            loop.create_datagram_endpoint(
                lambda: UPDProtocolMsgPackServer(server, self.queue), sock=sock)
        )

    def broadcast(self, data):
        asyncio.ensure_future(self.queue.put((data, (self.multicast_group, 10000))))

    def send_to(self, data, address):
        self.queue.put((data, address))

    def close(self):
        pass


def make_udp_multicast_socket(host, port, group=MCAST_GROUP_IPV6):
    """
    Create a UDP socket for sending and receiving multicast packets
       See https://github.com/python/cpython/blob/master/Tools/demo/mcast.py
    """

    addrinfo = socket.getaddrinfo(group, None)[0]

    sock = socket.socket(addrinfo[0], socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    except AttributeError:  # pragma: nocover
        pass  # Some systems don't support SO_REUSEPORT
    sock.bind(('', port))
    group_bin = socket.inet_pton(addrinfo[0], addrinfo[4][0])
    # Join group
    if addrinfo[0] == socket.AF_INET:  # IPv4
        sock.setsockopt(socket.IPPROTO_IP,
                        socket.IP_ADD_MEMBERSHIP,
                        group_bin + struct.pack('=I', socket.INADDR_ANY))
    else:
        sock.setsockopt(socket.IPPROTO_IPV6,
                        socket.IPV6_JOIN_GROUP,
                        group_bin + struct.pack('@I', 0))

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
