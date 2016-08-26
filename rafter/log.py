import sys
import collections
import itertools

import lmdb

from .models import LogEntry


def to_bytes(i):
    return i.to_bytes(8, sys.byteorder)


def from_bytes(b):
    return int.from_bytes(b, sys.byteorder)


COMMIT_INDEX = b'commit_index'
TERM = b'term'


# TODO: implement dynamic serizlizer
class RaftLog(collections.abc.MutableSequence):
    """Implement raft log on top of the LMDB storage."""

    def __init__(self, env=None, db=None):

        self.env = env or lmdb.open('/tmp/rafter.lmdb', max_dbs=10)
        self.db = db or self.env.open_db(b'rafter')
        self.metadata_db = self.env.open_db(b'meta')

    def txn(self, db=None, write=False):
        return self.env.begin(write=write, db=db or self.db)

    def __setitem__(self, index, value):
        if index > len(self):
            raise IndexError
        with self.txn(write=True) as txn:
            txn.replace(to_bytes(index), value.pack())

    def __delitem__(self, index):
        with self.txn(write=True) as txn:
            if isinstance(index, int):
                txn.delete(to_bytes(index).encode())
            elif isinstance(index, slice):
                curr = txn.cursor()
                curr.set_key(to_bytes(index.start))
                succ = True
                while succ:
                    succ = curr.delete()

    def __len__(self):
        with self.txn() as txn:
            # <http://stackoverflow.com/a/37016188>
            return txn.stat()['entries']

    def __getitem__(self, index):
        with self.txn() as txn:
            if isinstance(index, int):
                if index >= len(self):
                    raise IndexError('log index out of range')
                return LogEntry.unpack(txn.get(to_bytes(len(self) + index if index < 0 else index)))

            elif isinstance(index, slice):
                cur = txn.cursor()
                cur.set_key(to_bytes(index.start or 0))
                return [LogEntry.unpack(item[1]) for item in
                        itertools.takewhile(lambda item: from_bytes(item[0]) <= index.stop if index.stop else True, cur)]
            else:
                raise TypeError('log indices must be integers or slices, not {}'.format(type(index).__name__))

    def insert(self, index, value):
        self[index] = value

    def append(self, value):
        if value.index != len(self):
            raise IndexError('Can\'t append: value.index:{0} != len(self):{1}'.format(value.index, len(self)))
        with self.txn(write=True) as txn:
            txn.put(to_bytes(value.index), value.pack(), append=True)

    def extend(self, l):
        with self.txn() as txn:
            return txn.cursor().putmulti([(to_bytes(e.index), e.pack()) for e in l], append=True)

    def cmp(self, last_log_index, last_log_term):
        last = self[-1]
        return last_log_term > last.term or last_log_term == last.term and last.index <= last_log_index

    def entry(self, command):
        self.append(LogEntry(dict(index=len(self), term=self.term, command=command)))
        return self[-1]

    @property
    def commit_index(self):
        with self.txn(db=self.metadata_db) as txn:
            return int(txn.get(COMMIT_INDEX, default=b'0'))

    @commit_index.setter
    # <http://stackoverflow.com/a/4183512/2183102>
    def commit_index(self, value):
        with self.txn(db=self.metadata_db, write=True) as txn:
            txn.replace(COMMIT_INDEX, u(value))

    @property
    def term(self):
        with self.txn(db=self.metadata_db) as txn:
            return int(txn.get(TERM, default=b'0'))

    @term.setter
    def term(self, value):
        with self.txn(write=True, db=self.metadata_db) as txn:
            txn.replace(TERM, u(value))
