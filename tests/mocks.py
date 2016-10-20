from unittest import mock

from rafter.models import LogEntry


class Log(list):
    commit_index = -1
    cmp = mock.Mock(return_value=True)

    def entry(self, term, command, args, kwargs):
        self.append(LogEntry(dict(index=len(self), term=term, args=args, kwargs=kwargs)))
        return self[-1]


class Storage(dict):
    pass

async def foo(*args, **kwargs):
    return 'result'


class Service(mock.Mock):

    test = mock.MagicMock()
    test.apply = foo
