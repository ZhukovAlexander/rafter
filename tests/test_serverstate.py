import unittest
from unittest import mock

from rafter.serverstate import Leader, Follower, Candidate
from rafter.models import LogEntry

class Server(mock.Mock):
    term = 10
    id = 'server-1'
    voted_for = ''
    peers = {id: {}}

class Log(list):
    commit_index = 0
    cmp = mock.Mock(return_value=True)


class ServerStateBaseTest(unittest.TestCase):
    def setUp(self):
        self.server = Server()
        self.log = Log([LogEntry(dict(term=self.server.term, index=0))])

class BaseStateTestCase(ServerStateBaseTest):
    def setUp(self):
        super().setUp()
        self.state = Follower(self.server, self.log)

    def test_is_leader_false(self):
        self.assertFalse(self.state.is_leader())

    def test_to_leader(self):
        self.state.to_leader()
        self.assertIsInstance(self.server.state, Leader)

    def test_election(self):
        term = self.server.term
        self.state.election()
        self.assertEqual(self.server.term, term + 1)
        self.assertIsInstance(self.server.state, Candidate)
        self.server.broadcast_request_vote.assert_called_with()

    def test_append_entries_as_heartbeat(self):
        res = self.state.append_entries(self.server.term, self.server.id, 1, 1, 1)
        self.assertIsNone(res)
        self.server.election_timer.reset.assert_called_with()



class LeaderTestCase(ServerStateBaseTest):

    def setUp(self):
        super().setUp()
        self.state = Leader(self.server, self.log)

    def test_append_entries_fail_for_old_term(self):
        res = self.state.append_entries(self.server.term - 1, 1, 1, 1, 1, entries=[None])
        self.assertFalse(res['success'])

    def test_append_entries_to_itself_succeds(self):
        res = self.state.append_entries(self.server.term, self.server.id, 1, 1, 1, entries=[None])
        self.assertTrue(res['success'])

    def test_append_entries_converts_to_follower_newer_term(self):
        res = self.state.append_entries(self.server.term + 1, 1, 1, 1, 1, entries=[None])
        self.assertIsInstance(self.server.state, Follower)

    def test_request_vote_false_old_term(self):
        res = self.state.request_vote(self.server.term - 1, 'server-2', 1, 1)
        self.assertFalse(res['vote'])

    def test_request_vote_false_always(self):
        res = self.state.request_vote(self.server.term, 'server-2', 1, 1)
        self.assertFalse(res['vote'])

    def test_request_vote_true_from_itself(self):
        res = self.state.request_vote(self.server.term, self.server.id, 1, 1)
        self.assertTrue(res['vote'])

    def test_request_vote_convert_to_follower_on_newer_term(self):
        res = self.state.request_vote(self.server.term + 1, 'server-2', 1, 1)
        self.assertIsInstance(self.server.state, Follower)

    def test_append_entries_success_response(self):
        res = self.state.append_entries_response('server-2', self.server.term, 1, True)
        self.server.maybe_commit.assert_called_with('server-2', self.server.term, 1)

    def test_append_entries_failed_response(self):
        res = self.state.append_entries_response('server-2', self.server.term, 0, False)
        self.assertDictContainsSubset(dict(term=self.server.term,
                                           leader_id=self.server.id,
                                           leader_commit=self.log.commit_index), res)


class CandidateTestCase(ServerStateBaseTest):
    def setUp(self):
        super().setUp()
        self.state = Candidate(self.server, self.log)

    def test_append_entries_fail_for_old_term(self):
        res = self.state.append_entries(self.server.term - 1, 1, 1, 1, 1)
        self.assertFalse(res['success'])

    def test_append_entries_convert_to_follower(self):
        res = self.state.append_entries(self.server.term, 1, len(self.log) -1, 1, 1, entries=[None])
        self.assertIsInstance(self.server.state, Follower)
        self.assertTrue(res['success'])

    def test_request_vote_from_itself_succeds(self):
        res = self.state.request_vote(self.server.term, self.server.id, 1, 1)
        self.assertTrue(res['vote'])

    def test_request_vote_alway_false(self):
        res = self.state.request_vote(self.server.term, 'server-2', 1, 1)
        self.assertFalse(res['vote'])

    def test_request_vote_response_success(self):
        self.state.request_vote_response(self.server.term, True, self.server.id)
        self.assertIsInstance(self.server.state, Leader)


class FollowerTestCase(ServerStateBaseTest):

    def setUp(self):
        super().setUp()
        self.state = Follower(self.server, self.log)

    def test_append_entries_should_apply_commited(self):
        self.log.append(LogEntry(dict(term=self.server.term, index=len(self.log))))
        leader_commit = len(self.log) - 1
        res = self.state.append_entries(
            self.server.term, 'leader-1',
            leader_commit,
            self.log[-1].term,
            1,
            entries=[LogEntry(dict(term=self.server.term, index=len(self.log)))]
        )
        self.assertTrue(res['success'])
        self.state._server.apply_commited.assert_called_with(self.log.commit_index, leader_commit)

    def test_request_vote_succeds_log_up_to_date(self):
        last_log_term = self.server.term
        res = self.state.request_vote(self.server.term + 1, 'candidate-2', len(self.log) + 1, last_log_term)
        self.log.cmp.assert_called_with(len(self.log) + 1, last_log_term)
        self.assertTrue(res['vote'])
        self.assertEqual(self.server.voted_for, 'candidate-2')

    def test_request_vote_fails_already_voted(self):
        self.server.voted_for = 'candidate-1'
        res = self.state.request_vote(self.server.term + 1, 'candidate-2', len(self.log) + 1, self.server.term)
        self.assertFalse(res['vote'])
