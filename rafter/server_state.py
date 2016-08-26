import functools
import logging

logger = logging.getLogger(__name__)

def check_term(func):
    async def new(*args, **kwargs):
        current_term = locals()['self'].current_term

        if not locals()['term'] < current_term:
            return current_term, False
        return await func(*args, **kwargs)


def _transition(target_state):
    def decorator(method):
        @functools.wraps(method)
        def transition(*args, **kwargs):
            self = args[0]
            if target_state in TRANSITIONS[type(self).__name__]:
                self._server.state = STATES[target_state](self._server)
                method(*args, **kwargs)
        return transition
    return decorator


# <https://github.com/faif/python-patterns/blob/master/state.py>
class StateBase:
    """Implement a base class for the Raft server state. Each message will be proccessed by one of the
    states, as discribed in the original Raft paper.
    """

    def __init__(self, server, log):
        self.voted_for = None
        self._server = server
        self.log = log

    def to_follower(self, term):
        self._server.state = Follower(self._sever, self.log)
        self._server.term = term

    # @_transition('Leader')
    def to_leader(self):
        self._server.state = Leader(self._server, self.log)
        self._server.election_timer.stop()
        logger.debug('Switched to Leader with term {}'.format(self.current_term))

    def to_candidate(self):
        self._server.state = Candidate(self._server, self.log)

    def is_leader(self):
        return type(self) is Leader

    @property
    def current_term(self):
        return self.log.term

    @property
    def commit_index(self):
        return self.log.commit_index

    def append_entries(self, term, leader_id, prev_log_index, prev_log_term, leader_commit, entries=None):
        if term < self.current_term:
            return dict(peer=self._server.id, index=self.log.commit_index, term=self.current_term, success=False)
        return self._append_entries(term, leader_id, prev_log_index, prev_log_term, leader_commit, entries=entries)

    def request_vote(self, term, cid, last_log_index, last_log_term):
        if not term < self.current_term:
            return dict(term=self.current_term, success=False)
        return self._request_vote(term, cid, last_log_index, last_log_term)

    def append_entries_response(self, peer, term, index, success):
        pass

    def requtest_vote_response(self, peer, term, success):
        pass

    def election(self):
        self.log.term += 1
        logger.debug('Starting new election for term {}'.format(self.log.term))
        self.to_candidate()
        if self._server.id in self._server.peers and len(self._server.peers) == 1 or 1:
            self.to_leader()


class Leader(StateBase):

    def _append_entries(self, term, leader_id, prev_log_index, prev_log_term, leader_commit, entries=None):
        if leader_id == self._server.id:  # handle append entries rpc from itself
            return dict(peer=self._server.id, index=self.log[-1].index, term=term, success=True)
        self.to_follower(term)
        return self._server.state.append_entries(term, leader_id, prev_log_index, prev_log_term, leader_commit, entries=entries)

    def _request_vote(self, term, cid, last_log_index, last_log_term):
        self.to_follower(term)
        return dict(term=self.current_term, success=True)

    def append_entries_response(self, peer, term, index, success):
        if self.current_term == term:
            logger.debug('self.current_term == term:')
            self._server.maybe_commit(peer, term, index) if success else self._server.retry_ae(peer, term, index)

    def election(self):
        logger.debug("Already a Leader, skip the election")


class Candidate(StateBase):

    def _append_entries(self, term, leader_id, prev_log_index, prev_log_term, leader_commit, entries=None):
        self.to_follower(term)
        return self._server.state.append_entries(term, leader_id, prev_log_index, prev_log_term, leader_commit, entries=entries)

    def _request_vote(self, term, cid, last_log_index, last_log_term):
        return dict(term=self.current_term, success=False)

    def request_vote_response(self, peer, term, success):
        pass



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

    def _request_vote(self, term, cid, last_log_index, last_log_term):
        if self._server.voted_for in (None, cid) and self.log.cmp(last_log_index, last_log_term):
            self.voted_for = cid
            return dict(term=self.current_term, success=True)
        return dict(term=self.current_term, success=False)

TRANSITIONS = {
    'Leader': ('Follower', ),
    'Follower': ('Candidate', 'Follower'),
    'Candidate': ('Follower', 'Candidate', 'Leader')
}

STATES = {
    'Leader': Leader,
    'Follower': Follower,
    'Candidate': Candidate
}

# function database