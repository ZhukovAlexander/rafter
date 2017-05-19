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

import logging
import functools
import asyncio
import signal
import sys
import random
import logging
import os
import json
from collections import defaultdict
import uuid
import typing

import uvloop


from . import serverstate
from . import storage as store
from .network import UPDProtocolMsgPackServer, ResetablePeriodicTask, UDPMulticastTransport
from .utils import AsyncDictWrapper
from .exceptions import NotLeaderException
from .service import BaseService


# <http://stackoverflow.com/a/14058475/2183102>
root = logging.getLogger()
root.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
root.addHandler(ch)

logger = logging.getLogger(__name__)
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass


class RaftServer:

    def __init__(self,
                 service: 'BaseService',
                 log: typing.MutableSequence = None,
                 storage: typing.MutableMapping = None,
                 loop: asyncio.AbstractEventLoop = None,
                 transport: 'UDPMulticastTransport' = None,
                 config: dict = None,
                 bootstrap: bool = False):

        self.service = service
        self.log = log if log is not None else store.RaftLog()
        self.storage = storage if storage is not None else store.PersistentDict()
        self.transport = transport if transport is not None else UDPMulticastTransport()
        self.config = config if config is not None else {}

        a_wrapper = AsyncDictWrapper(storage or {})

        self.wait_for, self.set_result = a_wrapper.wait_for, a_wrapper.set

        self.bootstrap = bootstrap

        if self.bootstrap:
            self.storage['peers'] = {self.id: {'id': self.id, 'address': self.transport.address}}

        self.match_index = defaultdict(lambda: self.log.commit_index)
        self.next_index = defaultdict(lambda: self.log.commit_index + 1)

        self.loop = loop or asyncio.get_event_loop()

        self.state = serverstate.Follower(self, self.log)

        self.election_timer = ResetablePeriodicTask(interval=random.randint(15, 30) / 100,
                                                    callback=self.state.election)
        self.heartbeats = ResetablePeriodicTask(interval=0.05,
                                                callback=lambda: self.heartbeat(bootstraps=bootstrap))

        self.pending_events = {}

    def heartbeat(self, bootstraps=False):
        def new_heartbeat(bootstraps=bootstraps):
            asyncio.ensure_future(self.send_append_entries())
        self.heartbeat = new_heartbeat
        if bootstraps and len(self.log) == 0:
            asyncio.ensure_future(self.service.add_peer(self.peers[self.id]))
        else:
            self.heartbeat()

    @property
    def id(self):  # pragma: nocover
        return self.storage.setdefault('id', uuid.uuid4().hex)

    @property
    def term(self):  # pragma: nocover
        return self.storage.setdefault('term', 0)

    @term.setter
    def term(self, value):  # pragma: nocover
        self.storage['term'] = value

    @property
    def commit_index(self):
        return self.storage.setdefault('commit_index', 0)

    @commit_index.setter
    def commit_index(self, value: int):
        self.storage['commit_index'] = value

    @property
    def voted_for(self):  # pragma: nocover
        return self.storage.get('voted_for')

    @voted_for.setter
    def voted_for(self, value):  # pragma: nocover
        self.storage['voted_for'] = value

    @property
    def peers(self):  # pragma: nocover
        return self.storage.setdefault('peers', {})

    async def start(self):

        for signame in ('SIGINT', 'SIGTERM'):
            # <http://stackoverflow.com/questions/23313720/asyncio-how-can-coroutines-be-used-in-signal-handlers>
            self.loop.add_signal_handler(getattr(signal, signame), self.stop, signame)

        await self.service.setup(self, self.loop)
        await self.transport.setup(self, self.loop)

        self.election_timer.start()

    def stop(self, signame):  # pragma: nocover
        logger.info('Got signal {}, exiting...'.format(signame))
        self.transport.close()
        self.loop.stop()

    def route(self, message_type, from_peer, args):
        """Dispatch to the appropriate state method"""
        res = getattr(self.state, message_type)(*args)
        if res is not None:
            self.transport.send_to(res, from_peer)

    async def _apply_single(self, cmd, args, kwargs, index=None):

        try:
            res = dict(result=await getattr(self.service, cmd).apply(*args, **kwargs) if cmd else None)
            print(res)

        except Exception as e:
            logger.exception('Exception during a command invocation')
            res = dict(error=True, msg=str(e))

        finally:
            uuid_key = uuid.uuid5(uuid.UUID(self.id), str((args, kwargs)))
            await self.set_result(uuid_key.hex, res)

    def apply_commited(self, start, end):
        return asyncio.ensure_future(asyncio.wait(map(
            lambda entry: asyncio.ensure_future(self._apply_single(entry.command,
                                                                   entry.uuid,
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
        current_commit_index = self.commit_index
        commited_term = self.log[majority_index].term

        # per Raft spec, commit only entries from the current term
        if self.term == commited_term:
            new_commit_index = max(current_commit_index, majority_index)
            self.commit_index = new_commit_index
            return self.apply_commited(current_commit_index + 1, new_commit_index)

    async def handle_write_command(self, slug, *args, **kwargs):
        if not self.state.is_leader():
            raise NotLeaderException('This server is not a leader')
        log_entry = self.log.entry(term=self.term, command=slug, args=args, kwargs=kwargs)

        command_applied = asyncio.Event()
        self.pending_events[log_entry.index] = command_applied
        await self.send_append_entries([log_entry])
        return command_applied.wait()

    async def handle_read_command(self, command, *args, **kwargs):
        if not self.state.is_leader():
            raise NotLeaderException('This server is not a leader')
        return await self._apply_single(command, args, kwargs)

    async def send_append_entries(self, entries=()):
        prev = self.log[entries[0].index - 1] if entries else None
        message = ('append_entries', self.id, self.term, self.id, prev.index if prev else None, prev.term if prev else None, self.commit_index, entries)

        return self.transport.broadcast(message)

    def broadcast_request_vote(self):
        last = self.log[-1] if len(self.log) > 0 else None
        message = ('request_vote', self.id, self.term, self.id, last[b'index'] if last is not None else 0, last[b'term'] if last is not None else 0)


        return self.transport.broadcast(message)

    async def add_peer(self, peer):
        # <http://stackoverflow.com/a/26853961/2183102>
        self.storage['peers'] = {**self.storage['peers'], **{peer['id']: peer}}
        await self.transport.add_peer(peer)

    def remove_peer(self, peer_id):
        peers = self.storage['peers']
        del peers[peer_id]
        self.storage['peers'] = peers

    def list_peers(self):
        return list(self.peers)
