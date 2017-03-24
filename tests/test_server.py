import unittest
from unittest import mock
import uuid
import asyncio

from rafter.server import RaftServer
from rafter.models import LogEntry
from rafter.exceptions import NotLeaderException

from .mocks import Log, Storage, Service


class RaftServerTest(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.get_event_loop()
        self.server = RaftServer(
            Service(),
            log=Log(),
            server_protocol=mock.Mock(),
            storage=Storage(),
            bootstrap=True
        )
        self.server.election_timer = mock.Mock()

    def test_start_stop(self):
        server = RaftServer(
            Service(),
            log=Log(),
            storage=Storage(),
            bootstrap=True
        )
        server.election_timer = mock.Mock()
        with mock.patch('rafter.server.random.randint', return_value=100):
            server.start()
            server.election_timer.start.assert_called_with(1)

    def test_initial_heartbeat_calls_add_peer(self):
        with mock.patch('rafter.server.asyncio.ensure_future') as ensure_future:
            self.server.heartbeat(bootstraps=True)

            ensure_future.assert_called_with(self.server.service.add_peer())

    def test_heartbeat_should_schedule_ae(self):
        with mock.patch('rafter.server.asyncio.ensure_future') as ensure_future:
            self.server.send_append_entries = mock.Mock()
            self.server.heartbeat(bootstraps=False)
            ensure_future.assert_called_with(self.server.send_append_entries())

    def test_handle_calls_correct_state_method(self):
        self.server.state = mock.Mock()
        method = 'test_method'
        res = self.server.handle(method)
        getattr(self.server.state, method).assert_called_with()

    def test_handle_write_raises_error_when_not_leader(self):
        with self.assertRaises(NotLeaderException):
            self.loop.run_until_complete(self.server.handle_write_command('test', (1, 2), {1: 1}))

    def test_handle_read_command(self):
        self.server.state.to_leader()
        res = self.loop.run_until_complete(self.server.handle_read_command('test', (1, 2), {1: 1}))
        self.assertEqual(res, 'result')

    def test_handle_read_raises_error_when_not_leader(self):
        with self.assertRaises(NotLeaderException):
            self.loop.run_until_complete(self.server.handle_read_command('test', (1, 2), {1: 1}))

    def test_add_peer(self):
        self.server.add_peer({'id': 'peer-2'})
        self.assertIn(b'peer-2', self.server.peers)

    def test_remove_peer(self):
        with self.assertRaises(KeyError):
            self.server.remove_peer('notapeer')
        self.server.remove_peer(self.server.id)
        self.assertNotIn(self.server.id, self.server.peers)

    def test_list_peers(self):
        self.assertListEqual(self.server.list_peers(), list(self.server.peers))