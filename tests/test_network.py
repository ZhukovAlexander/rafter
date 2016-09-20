import unittest
from unittest import mock

import asyncio

from rafter import network
from rafter import models

class ServerProtocolTest(unittest.TestCase):
    def setUp(self):
        self.server = mock.Mock()
        self.proto = network.UPDProtocolMsgPackServer(self.server, asyncio.Queue())


    def test_receive_datagram_should_call_server_handle(self):
        self.proto.connection_made(mock.Mock())
        rv = {'term': 4, 'success': True, 'peer': 'me'}
        self.server.handle.return_value = rv

        # res = asyncio.get_event_loop().run_until_complete(
        msg = {'term': 4, 'last_log_index': 20, 'last_log_term': 20, 'peer': 'me'}
        # import pdb;pdb.set_trace()
        data = models.RaftMessage({'content': msg}).pack()
        addr = ('127.0.0.1', 8888)
        self.proto.datagram_received(models.RaftMessage({'content': msg}).pack(), addr)
            # )
        self.server.handle.assert_called_with(
            'request_vote', 
            **{'term': 4, 'last_log_index': 20, 'last_log_term': 20, 'peer': 'me'}
        )
        self.proto.transport.sendto.assert_called_with(
            models.RaftMessage({'content': rv}).pack(), addr
        )

    def test_shoud_send_messages_from_a_queue(self):
        transport = mock.Mock()
        run = True

        def is_closing():
            nonlocal run
            if run:
                run = False
                return False
            return True
        transport.is_closing = is_closing
        self.proto.connection_made(transport)
        msg = {'term': 4, 'success': True, 'peer': 'me'}
        addr = ('127.0.0.1', 8888)
        asyncio.get_event_loop().run_until_complete(
            self.proto.queue.put((msg, addr))
        )
        asyncio.get_event_loop().run_until_complete(
            self.proto.start()
        )
        self.proto.transport.sendto.assert_called_with(
            models.RaftMessage({'content': msg}).pack(), addr
        )


def test_stop_periodict_task():
    periodic_task = network.ResetablePeriodicTask(interval=1)
    periodic_task.start()
    periodic_task.stop()
    assert not periodic_task._running, 'Periodic callback should be suspended'


def test_stop_periodict_start():
    periodic_task = network.ResetablePeriodicTask(interval=1)
    periodic_task.start()
    assert periodic_task._running
