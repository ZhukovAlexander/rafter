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

"""Default persistent data structures

This module provides RaftLog and PersistentDict classes that persist
their data in LMDB.
"""

import sys
import collections
import itertools
import json
from uuid import uuid4
import asyncio

import lmdb
from msgpack import packb, unpackb

from .models import LogEntry


def to_bytes(i):
    return i.to_bytes(8, sys.byteorder)


def from_bytes(b):
    return int.from_bytes(b, sys.byteorder)


# TODO: implement dynamic serizlizer
class RaftLog(collections.abc.MutableSequence):
    """Implement raft log on top of the LMDB storage."""

    def __init__(self, env=None, db=None):

        self.env = env or lmdb.open('/tmp/rafter.lmdb', max_dbs=10)
        self.db = db or self.env.open_db(b'rafter')
        self.attrs_store = self.env.open_db(b'meta')

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
                if not txn.delete(to_bytes(index)):
                    raise IndexError('log index out of range')
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
                if index >= len(self) or index < -len(self):
                    raise IndexError('log index out of range')
                return LogEntry.unpack(txn.get(to_bytes(len(self) + index if index < 0 else index)))

            elif isinstance(index, slice):
                cur = txn.cursor()
                cur.set_key(to_bytes(index.start or 0))
                return [LogEntry.unpack(item[1]) for item in
                        itertools.takewhile(lambda item: from_bytes(item[0]) < index.stop if index.stop else True, cur)]
            else:
                raise TypeError('log indices must be integers or slices, not {}'.format(type(index).__name__))

    def insert(self, index, value):  # pragma: nocover
        raise NotImplementedError

    def append(self, value):
        if value.index != len(self):
            raise IndexError('Can\'t append: value.index:{0} != len(self):{1}'.format(value.index, len(self)))
        with self.txn(write=True) as txn:
            txn.put(to_bytes(value.index), value.pack(), append=True)

    def extend(self, l):
        with self.txn(write=True) as txn:
            return txn.cursor().putmulti([(to_bytes(e.index), e.pack()) for e in l], append=True)

    def cmp(self, last_log_index, last_log_term):
        last = self[-1]
        return last_log_term > last.term or last_log_term == last.term and last.index <= last_log_index

    def entry(self, term, command, args=(), kwargs=None):
        self.append(LogEntry(dict(index=len(self), term=term, command=command, args=args, kwargs=kwargs or {})))
        return self[-1]


class PersistentDict(collections.abc.MutableMapping):

    def __init__(self, env=None, db=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.env = env or lmdb.open('/tmp/rafter.lmdb', max_dbs=10)
        self.db = db or self.env.open_db(b'storage')

    def __setitem__(self, key, value):
        with self.env.begin(write=True, db=self.db) as txn:
            txn.replace(packb(key), packb(value))

    def __getitem__(self, key):
        with self.env.begin(db=self.db) as txn:
            val = txn.get(packb(key))
        if val is None:
            raise KeyError(key)
        return unpackb(val)

    def __delitem__(self, key):
        with self.env.begin(write=True, db=self.db) as txn:
            if not txn.delete(packb(key)):
                raise KeyError(key)

    def __iter__(self):
        with self.env.begin(db=self.db) as txn:
            yield from (unpackb(k) for k in txn.cursor().iternext(values=False))

    def __len__(self):
        with self.env.begin(db=self.db) as txn:
            # <http://stackoverflow.com/a/37016188>
            return txn.stat()['entries']
