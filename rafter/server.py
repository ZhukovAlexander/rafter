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

import uvloop


import server_state
import log as rlog
from network import UDPProtocolProtobufClient, UDPProtocolProtobufServer, make_socket, ResetablePeriodicTask


# <http://stackoverflow.com/a/14058475/2183102>
root = logging.getLogger()
root.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
root.addHandler(ch)

asyncio.set_event_loop(uvloop.new_event_loop())


class Peers:
    def __init__(self, path='/var/lib/rafter/rafter.peers'):
        self._path = path

        dirname = os.path.dirname(self._path)

        if not os.path.exists(dirname):
            os.makedirs(dirname)

        if not os.path.exists(self._path):
            self._peers = dict()
            with open(self._path, 'w') as f:
                f.write(json.dumps(self._peers))
        else:
            self._peers = json.loads(open(self._path, 'r').read())

    def dump(self):
        with open(self._path, 'w') as f:
            f.write(json.dumps(self._peers))


    def add(self, peer):
        self._peers[peer['id']] = peer
        self.dump()

    def remove(self, peer_id):
        self._peers.pop(peer_id)
        self.dump()





class RaftServer:

    def __init__(self, host='0.0.0.0', port=10000, log=None, loop=None, server_protocol=UDPProtocolProtobufServer, client_protocol=UDPProtocolProtobufClient, config=None):
        self.host = host
        self.port = port

        self.id = '{}:{}'.format(self.host, self.port)
        self.log = log or rlog.RaftLog()

        self.config = config

        self.loop = loop or asyncio.get_event_loop()
        self.queue = asyncio.Queue(loop=self.loop)

        self.state = server_state.Follower(self, self.log)
        self.server_protocol = server_protocol(self)
        self.client_protocol = client_protocol(self, self.queue, self.loop)

        def election():
            self.state.election()

        self.election_timer = ResetablePeriodicTask(callback=election)

        self.pending_events = {}

    def start(self):

        for signame in ('SIGINT', 'SIGTERM'):
            # <http://stackoverflow.com/questions/23313720/asyncio-how-can-coroutines-be-used-in-signal-handlers>
            self.loop.add_signal_handler(
                getattr(signal, signame), asyncio.ensure_future, self.stop(signame))

        # actually, this is questionable to share the same socket address between to protocols, but for now I wan't to separate
        # client and server logic, and we obviously have to use the same
        # address because other nodes will use it as a destination
        self.server_transport, self.server_protocol = self.loop.run_until_complete(
            self.loop.create_datagram_endpoint(
                lambda: self.server_protocol, sock=make_socket(host=self.host, port=self.port))
        )

        self.client_transport, self.client_protocol = self.loop.run_until_complete(
            self.loop.create_datagram_endpoint(
                lambda: self.client_protocol, sock=make_socket(host=self.host, port=self.port))
        )

        self.election_timer.start(random.randint(15, 30) / 100)

        # self.loop.run_forever()

    async def stop(self, signame):
        print('Got signal {}, exiting...'.format(signame))
        self.server_transport.close()
        self.client_transport.close()
        self.loop.stop()

    def handle(self, message_type, **kwargs):
        """Dispatch to the appropriate state method"""
        return getattr(self.state, message_type)(**kwargs)

    def _apply_single(self, entry):

        # notify waiting client
        can_apply = self.pending_events.get(entry.index)
        if can_apply:
            can_apply.set()

    def apply_commited(self, start, end):
        asyncio.ensure_future(asyncio.wait(map(
            lambda entry: asyncio.ensure_future(self._apply_single(entry)),
            self.log[start:end]
        )))

    async def _apply_commited(self, start, end):
        """Actual hardwork goes here"""
        return await asyncio.wait(map(
            lambda entry: asyncio.ensure_future(self._apply_single(entry)),
            self.log[start:end]
        ))

    async def handle_write_command(self, slug, *args, **kwargs):
        root.debug('handle_write_command')
        # <http://stackoverflow.com/a/17307606/2183102>
        log_entry = self.log.entry(command=pickle.dumps((slug, args, kwargs)).decode('latin1'))

        can_apply = asyncio.Event()
        self.pending_events[log_entry.index] = can_apply
        await self.send_append_entries([log_entry])
        await can_apply.wait()

    async def send_append_entries(self, entries, destination=None):
        prev = self.log[entries[0].index - 1]
        message = models.AppendEntriesRPCRequest(
            dict(term=self.log.term,
                 leader_id=self.id,
                 prev_log_index=prev.index,
                 prev_log_term=prev.term,
                 leader_commit=self.log.commit_index,
                 entries=entries)
        )

        await self.queue.put((message, destination))

    def retry_ae(self, peer, term, index):
        entry = self.log[index]
        asyncio.ensure_future(self.send_append_entries([entry], destination=peer))

    def election(self):
        self.state.start_election()

