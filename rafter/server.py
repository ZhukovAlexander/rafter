import logging
import functools
import asyncio
import signal
import sys
import random
import logging
import pickle
import os
import json
from collections import defaultdict

import uvloop


from . import server_state
from . import log as rlog
from . import models
from .network import UPDProtocolMsgPackClient, UPDProtocolMsgPackServer, make_socket, ResetablePeriodicTask
from .exceptions import NotLeaderException


# <http://stackoverflow.com/a/14058475/2183102>
root = logging.getLogger()
root.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
root.addHandler(ch)

logger = logging.getLogger(__name__)

asyncio.set_event_loop(uvloop.new_event_loop())


class RaftServer:

    def __init__(self,
                 service,
                 address=('0.0.0.0', 10000),
                 log=None,
                 storage=None,
                 loop=None,
                 server_protocol=UPDProtocolMsgPackServer,
                 client_protocol=UPDProtocolMsgPackClient,
                 config=None,
                 bootstrap=False):

        self.host, self.port = address

        self.log = log or rlog.RaftLog()
        self.storage = storage or rlog.Storage()

        self.bootstrap = bootstrap

        if self.bootstrap:
            self.storage.peers = {self.id: {'id': self.id}}

        self.match_index = defaultdict(lambda: self.log.commit_index)
        self.next_index = defaultdict(lambda: self.log.commit_index + 1)

        self.config = config

        self.loop = loop or asyncio.get_event_loop()
        self.queue = asyncio.Queue(loop=self.loop)

        self.state = server_state.Follower(self, self.log)
        self.server_protocol = server_protocol(self)
        self.client_protocol = client_protocol(self, self.queue, self.loop)
        self.service = service(self)

        def election():
            self.state.election()

        self.election_timer = ResetablePeriodicTask(callback=election)
        self.heartbeats = ResetablePeriodicTask(interval=0.05,
                                                callback=lambda: self.heartbeat(bootstraps=bootstrap))

        self.pending_events = {}

    def heartbeat(self, bootstraps=False):
        self.heartbeat = lambda bootstraps=False: asyncio.ensure_future(self.send_append_entries())
        if bootstraps and len(self.log) == 0:
            asyncio.ensure_future(self.service.add_peer(self.peers[self.id]))
        else:
            self.heartbeat()

    @property
    def id(self):
        return self.storage.id

    @property
    def term(self):
        return self.storage.term

    @term.setter
    def term(self, value):
        self.storage.term = value

    @property
    def voted_for(self):
        return self.storage.voted_for

    @voted_for.setter
    def voted_for(self, value):
        self.storage.voted_for = value

    @property
    def peers(self):
        return self.storage.peers

    def start(self):

        for signame in ('SIGINT', 'SIGTERM'):
            # <http://stackoverflow.com/questions/23313720/asyncio-how-can-coroutines-be-used-in-signal-handlers>
            self.loop.add_signal_handler(
                getattr(signal, signame), self.stop, signame)

        # actually, this is questionable to share the same socket address between to protocols, but for now I wan't to separate
        # client and server logic, and we obviously have to use the same
        # address because other nodes will use it as a destination
        sock = make_socket(host=self.host, port=self.port)
        self.server_transport, self.server_protocol = self.loop.run_until_complete(
            self.loop.create_datagram_endpoint(
                lambda: self.server_protocol, sock=sock)
        )

        self.client_transport, self.client_protocol = self.loop.run_until_complete(
            self.loop.create_datagram_endpoint(
                lambda: self.client_protocol, sock=sock)
        )

        self.election_timer.start(random.randint(15, 30) / 100)
        self.service.setup()

    def stop(self, signame):
        logger.info('Got signal {}, exiting...'.format(signame))
        self.server_transport.close()
        self.client_transport.close()
        self.loop.stop()

    def handle(self, message_type, **kwargs):
        """Dispatch to the appropriate state method"""
        return getattr(self.state, message_type)(**kwargs)

    async def _apply_single(self, cmd, args, kwargs, index=None):

        try:
            res = await getattr(self.service, cmd).apply(*args, **kwargs) if cmd else None
        except Exception as e:
            logger.exception('Exception during command invocation')
            raise

        if not index:
            return res

        # notify waiting client
        can_apply = self.pending_events.get(index)
        if can_apply:
            can_apply.set()

    def apply_commited(self, start, end):
        asyncio.ensure_future(asyncio.wait(map(
            lambda entry: asyncio.ensure_future(self._apply_single(entry.command,
                                                                   entry.args,
                                                                   entry.kwargs,
                                                                   index=entry.index)),
            self.log[start:end + 1]
        )))

    def maybe_commit(self, peer, term, index):
        self.match_index[peer] = index
        self.next_index[peer] = min(index, len(self.log)) + 1

        all_match_index = sorted(
            [self.match_index[peer] for peer in self.peers],
            reverse=True
        )
        majority_index = all_match_index[int(len(all_match_index) / 2)]
        current_commit_index = self.log.commit_index
        commited_term = self.log[majority_index].term

        # per Raft spec, commit only entries from the current term
        if self.term == commited_term:
            new_commit_index = max(current_commit_index, majority_index)
            self.log.commit_index = new_commit_index
            self.apply_commited(current_commit_index + 1, new_commit_index)

    async def handle_write_command(self, slug, *args, **kwargs):
        if not self.state.is_leader():
            raise NotLeaderException('This server is not a leader')
        logger.debug('handle_write_command')
        log_entry = self.log.entry(term=self.term, command=slug, args=args, kwargs=kwargs)

        command_applied = asyncio.Event()
        self.pending_events[log_entry.index] = command_applied
        await self.send_append_entries([log_entry])
        await command_applied.wait()
        return True

    async def handle_read_command(self, command, *args, **kwargs):
        if not self.state.is_leader():
            raise NotLeaderException('This server is not a leader')
        return await self._apply_single(command, args, kwargs)

    async def send_append_entries(self, entries=(), destination=('239.255.255.250', 10000)):
        prev = self.log[entries[0].index - 1] if entries else None
        message = dict(
            term=self.term,
            leader_id=self.id,
            prev_log_index=prev.index if prev else None,
            prev_log_term=prev.term if prev else None,
            leader_commit=self.log.commit_index,
            entries=entries)

        await self.queue.put((message, destination))

    def retry_ae(self, peer, term, index):
        entry = self.log[index]
        asyncio.ensure_future(self.send_append_entries([entry], destination=peer))

    def broadcast_request_vote(self):
        last = self.log[-1] if len(self.log) > 0 else None
        message = dict(
            term=self.term,
            peer=self.id,
            last_log_index=last.index if last else 0,
            last_log_term=last.term if last else 0
        )

        asyncio.ensure_future(self.queue.put((message, ('239.255.255.250', 10000))))

    def add_peer(self, peer):
        # <http://stackoverflow.com/a/26853961/2183102>
        self.storage.peers = {**self.storage.peers, **{peer['id']: peer}}

    def remove_peer(self, peer_id):
        peers = self.storage.peers
        try:
            del peers[peer_id]
        except KeyError:
            return
        self.storage.peers = peers

    def list_peers(self):
        return list(self.storage.peers)
