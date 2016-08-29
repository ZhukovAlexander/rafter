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
        self._server.state = Follower(self._sever, self.log)
        self._server.term = term

    def to_leader(self):
        self._server.state = Leader(self._server, self.log)
        self._server.election_timer.stop()
        logger.debug('Switched to Leader with term {}'.format(self.log.term))

    def to_candidate(self):
        self._server.state = Candidate(self._server, self.log)

    def is_leader(self):
        return type(self) is Leader

    def append_entries(self, term, leader_id, prev_log_index, prev_log_term, leader_commit, entries=None):
        if term < self.log.term:
            return dict(peer=self._server.id, index=self.log.commit_index, term=self.log.term, success=False)
        return self._append_entries(term, leader_id, prev_log_index, prev_log_term, leader_commit, entries=entries)

    def request_vote(self, term, peer, last_log_index, last_log_term):
        if term < self.log.term:
            return dict(term=self.log.term, vote=False)
        return self._request_vote(term, peer, last_log_index, last_log_term)

    def append_entries_response(self, peer, term, index, success):
        pass

    def request_vote_response(self, term, vote, peer):
        pass

    def election(self):
        logger.debug('Starting new election for term {}'.format(self.log.term + 1))
        self.to_candidate()
        self.log.term += 1
        self._votes.clear()
        self._server.broadcast_request_vote()


class Leader(StateBase):

    def _append_entries(self, term, leader_id, prev_log_index, prev_log_term, leader_commit, entries=None):
        if leader_id == self._server.id:  # handle append entries rpc from itself
            return dict(peer=self._server.id, index=self.log[-1].index, term=term, success=True)
        self.to_follower(term)
        return self._server.state.append_entries(term, leader_id, prev_log_index, prev_log_term, leader_commit, entries=entries)

    def _request_vote(self, term, peer, last_log_index, last_log_term):
        if peer == self._server.id:
            return dict(term=self.log.term, vote=True, peer=self._server.id)
        self.to_follower(term)
        return dict(term=self.log.term, vote=True, peer=self._server.id)

    def append_entries_response(self, peer, term, index, success):
        if self.log.term == term:  # maybe this is not needed?
            logger.debug('self.current_term == term:')
            self._server.maybe_commit(peer, term, index) if success else self._server.retry_ae(peer, term, index)

    def election(self):
        logger.debug("Already a Leader, skip the election")


class Candidate(StateBase):

    def _append_entries(self, term, leader_id, prev_log_index, prev_log_term, leader_commit, entries=None):
        self.to_follower(term)
        return self._server.state.append_entries(term, leader_id, prev_log_index, prev_log_term, leader_commit, entries=entries)

    def _request_vote(self, term, peer, last_log_index, last_log_term):
        logger.debug(self._server.id)
        if peer == self._server.id:
            return dict(term=self.log.term, vote=True, peer=self._server.id)
        return dict(term=self.log.term, vote=False, peer=self._server.id)

    def request_vote_response(self, term, vote, peer):
        if vote:
            self._votes.add(peer)
            if len(self._votes.intersection(self._server.peers)) >= len(self._server.peers) // 2 + 1:
                self.to_leader()


class Follower(StateBase):

    def _append_entries(self, term, leader_id, prev_log_index, prev_log_term, leader_commit, entries=None):
        # apply to local
        try:
            prev_log_entry = self.log[prev_log_index]
        except IndexError:
            return dict(index=prev_log_index, term=self.current_term, success=False)
        if prev_log_entry.term != term:
            del self.log[prev_log_entry.index:]
        self.log.extend(entries)
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, self.commit_index)
        return dict(index=prev_log_index, term=self.current_term, success=True)

    def _request_vote(self, term, peer, last_log_index, last_log_term):
        if self.log.voted_for in ('', peer) and self.log.cmp(last_log_index, last_log_term):
            self.log.voted_for = peer
            return dict(term=self.log.term, vote=True, peer=self._server.id)
        return dict(term=self.log.term, vote=False, peer=self._server.id)

# function database