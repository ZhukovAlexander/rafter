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


class Topology(dict):
    def __init__(self): pass


# <https://github.com/faif/python-patterns/blob/master/state.py>
class StateBase(object):
    """This is a base class for Raft server state. Each message will be proccessed by one of the
    states, as discribed in the original Raft paper.
    """

    def __init__(self, server, log):
        self.voted_for = None
        self._server = server
        self._log = log
        self.peers = Topology()

    def to_follower(self, term):
        self._server.state = Follower(self._sever, self._log)
        self._server.term = term

    # @_transition('Leader')
    def to_leader(self):
        self._server.state = Leader(self._server, self._log)
        self._server.election_timer.stop()
        logger.debug('Switched to Leader with term {}'.format(self.current_term))

    def to_candidate(self):
        self._server.state = Candidate(self._server, self._log)

    @property
    def current_term(self):
        return self._log.term

    @property
    def log(self):
        return self._log

    def append_entries(self, term, leader_id, prev_log_index, prev_log_term, leader_commit, entries=None):
        if not term < self.current_term:
            return dict(index=self.log.commit_index, term=self.current_term, success=False)
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
        self._log.term += 1
        logger.debug('Starting new election for term {}'.format(self._log.term))
        self.to_candidate()
        if True:
            self.to_leader()


class Leader(StateBase):

    def _append_entries(self, term, leader_id, prev_log_index, prev_log_term, leader_commit, entries=None):
        self.to_follower(term)
        return self._server.state.append_entries(term, leader_id, prev_log_index, prev_log_term, leader_commit, entries=entries)

    def _request_vote(self, term, cid, last_log_index, last_log_term):
        self.to_follower(term)
        return dict(term=self.current_term, success=True)

    def append_entries_response(self, peer, term, index, success):
        if self.current_term == term:
            self.maybe_commit(peer, term, index) if success else self._server.retry_ae(peer, term, index)

    def maybe_commit(peer, term, index):
        self.server.peers[peer]['match_index'] = index
        self.server.peers[peer]['next_index'] = min(index, len(self.log)) + 1

        all_match_endex = sorted([peer['match_index'] for peer in self.server.peers.values()] + [self.log.last_index], reverse=True)
        committed_index = all_match_index[int(len(all_match_index) / 2)]
        commited_term = self.log[commit_index].term

        if self.current_term == commited_term:
            first_not_commited_entry = self.commit_index + 1
            self.commit_index = min(self.commit_index, committed_index)
            self._server.apply_commited(
                first_not_commited_entry, self.commit_index)

    def election(self):
        logger.debug("Already Leader, skip election")


class Candidate(StateBase):

    def _append_entries(self, term, leader_id, prev_log_index, prev_log_term, leader_commit, entries=None):
        self.to_follower(term)
        return self._server.state.append_entries(term, leader_id, prev_log_index, prev_log_term, leader_commit, entries=entries)

    def _request_vote(self, term, cid, last_log_index, last_log_term):
        return dict(term=self.current_term, success=False)


class Follower(StateBase):

    def _append_entries(self, term, leader_id, prev_log_index, prev_log_term, leader_commit, entries=None):
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