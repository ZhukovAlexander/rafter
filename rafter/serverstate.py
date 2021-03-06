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

"""Contains Raft server state implementations

StateBases instances are used to handle incoming requests as described in the
original paper.
"""

import logging

logger = logging.getLogger(__name__)


# <https://github.com/faif/python-patterns/blob/master/state.py>
class StateBase:
    """Implement a base class for the Raft server state. Each message will be proccessed by one of the
    states, as discribed in the original Raft paper.
    """

    def __init__(self, server, log):
        self._votes = set()
        self.voted_for = None
        self._server = server
        self.log = log

    def to_follower(self, term):
        self._server.state = Follower(self._server, self.log)
        self._server.term = term

    def to_leader(self):
        self._server.state = Leader(self._server, self.log)
        self._server.election_timer.stop()
        self._server.heartbeats.start()
        logger.debug('Switched to Leader with term {}'.format(self._server.term))

    def to_candidate(self):
        self._server.state = Candidate(self._server, self.log)

    def is_leader(self):
        return type(self) is Leader

    def append_entries(self, term, leader_id, prev_log_index, prev_log_term, leader_commit, entries=None):
        if term < self._server.term:
            return self._server.id, self._server.commit_index, self._server.term, False
        self._server.election_timer.reset()
        return ('append_entries_response', self._server.id) + self._append_entries(term, leader_id, prev_log_index, prev_log_term, leader_commit, entries=entries)

    def request_vote(self, term, peer, last_log_index, last_log_term):
        if term < self._server.term:
            return self._server.term, False
        elif term > self._server.term:
            self.to_follower(term)
            return self._server.state.request_vote(term, peer, last_log_index, last_log_term)
        return ('request_vote_response', self._server.id) + self._request_vote(term, peer, last_log_index, last_log_term)

    def append_entries_response(self, peer, term, index, success):
        pass  # pragma: no cover

    def request_vote_response(self, term, vote, peer):
        pass  # pragma: no cover

    def election(self):
        logger.debug('Starting new election for term {}'.format(self._server.term + 1))
        self.to_candidate()
        self._server.term += 1
        self._votes.clear()
        self._server.broadcast_request_vote()


class Leader(StateBase):

    def _append_entries(self, term, leader_id, prev_log_index, prev_log_term, leader_commit, entries=None):
        if leader_id == self._server.id:  # handle append entries rpc from itself
            return self._server.id, len(self.log) - 1, term, True
        self.to_follower(term)
        return self._server.state.append_entries(term, leader_id, prev_log_index, prev_log_term, leader_commit, entries=entries)

    def _request_vote(self, term, peer, last_log_index, last_log_term):
        if peer == self._server.id:
            return self._server.term, True, self._server.id
        return self._server.term, False, self._server.id

    def retry_append_entries(self, term, index):
        entry = self.log[index]
        prev = self.log[index - 1]
        return dict(term=self._server.term,
                    leader_id=self._server.id,
                    prev_log_index=prev.index,
                    prev_log_term=prev.term,
                    leader_commit=self.log.commit_index,
                    entries=[entry])

    def append_entries_response(self, peer, term, index, success):
        if self._server.term == term:  # maybe this is not needed?
            if success:
                return self._server.maybe_commit(peer, term, index)
            return self.retry_append_entries(term, index)

    def election(self):
        logger.debug("Already a Leader, skip the election")  # pragma: no cover


class Candidate(StateBase):

    def _append_entries(self, term, leader_id, prev_log_index, prev_log_term, leader_commit, entries=None):
        self.to_follower(term)
        return self._server.state.append_entries(term, leader_id, prev_log_index, prev_log_term, leader_commit, entries=entries)

    def _request_vote(self, term, peer, last_log_index, last_log_term):
        if peer == self._server.id:
            return self._server.term, True, self._server.id
        return self._server.term, False, self._server.id

    def request_vote_response(self, term, vote, peer):
        if vote:
            self._votes.add(peer)
            if len(self._votes.intersection(self._server.peers)) >= len(self._server.peers) // 2 + 1:
                self.to_leader()


class Follower(StateBase):

    def _append_entries(self, term, leader_id, prev_log_index, prev_log_term, leader_commit, entries=None):
        # apply to local
        entries = entries or []
        try:
            prev_log_entry = self.log[prev_log_index]
        except IndexError:
            return dict(index=prev_log_index, term=self._server.term, success=False)
        if prev_log_entry.term != term:
            del self.log[prev_log_entry.index:]
        self.log.extend(entries)
        if leader_commit > self.log.commit_index:
            self._server.apply_commited(self.log.commit_index, leader_commit)
            self.log.commit_index = min(leader_commit, self._server.commit_index)
        return dict(index=prev_log_index, term=self._server.term, success=True)

    def _request_vote(self, term, peer, last_log_index, last_log_term):
        if self._server.voted_for in {'', peer} and self.log.cmp(last_log_index, last_log_term):
            self._server.voted_for = peer
            return dict(term=self._server.term, vote=True, peer=self._server.id)
        return dict(term=self._server.term, vote=False, peer=self._server.id)
