import unittest
import tempfile
import shutil

import lmdb

from rafter.models import LogEntry
from rafter.log import RaftLog, Storage, MetaDataField


class LMDBLogTest(unittest.TestCase):
    def setUp(self):
        self.db_dir = tempfile.mkdtemp()
        self.log = RaftLog(env=lmdb.open(self.db_dir, max_dbs=10))

    def tearDown(self):
        shutil.rmtree(self.db_dir)

    def test_commit_index(self):
        self.assertEqual(self.log.commit_index, 0)
        self.log.commit_index += 1
        self.assertEqual(self.log.commit_index, 1)

    def test_setitem(self):
        entry = LogEntry()
        self.log[0] = entry
        self.assertEqual(self.log[0], entry)

    def test_setitem_fails_for_invalid_index(self):
        with self.assertRaises(IndexError):
            self.log[9] = LogEntry()

    def test_append(self):
        entries = [LogEntry(dict(index=i)) for i in range(10)]
        for e in entries:
            self.log.append(e)
        self.assertListEqual(self.log[:], entries)

    def test_append_fails_for_incorrect_index(self):
        with self.assertRaises(IndexError):
            self.log.append(LogEntry(dict(index=100)))

    def test_getitem_integer(self):
        with self.assertRaises(IndexError):
            self.log[0]
        entry = LogEntry(dict(index=len(self.log)))
        self.log.append(entry)
        self.assertEqual(self.log[0], entry)
        self.assertEqual(self.log[-1], entry)
        with self.assertRaises(IndexError):
            self.log[-len(self.log) - 1]

    def test_getitem_slice(self):
        self.assertListEqual(self.log[:], [])
        entries = [LogEntry(dict(index=i)) for i in range(10)]
        for e in entries:
            self.log.append(e)
        self.assertListEqual(self.log[0:len(entries) // 2], entries[0:len(entries) // 2])

    def test_getitem_raise_type_error(self):
        with self.assertRaises(TypeError):
            self.log['invalidType']

    def test_del_item_integer_out_of_range(self):
        with self.assertRaises(IndexError):
            del self.log[0]

    def test_del_item_that_exists(self):
        self.log.append(LogEntry(dict(index=0)))
        del self.log[0]
        self.assertEqual(len(self.log), 0)

    def test_del_item_slice(self):
        entries = [LogEntry(dict(index=i)) for i in range(10)]
        for e in entries:
            self.log.append(e)
        del self.log[0:]
        self.assertEqual(len(self.log), 0)

    def test_extend(self):
        entries = [LogEntry(dict(index=i)) for i in range(10)]
        self.log.extend(entries)
        self.assertListEqual(list(reversed(self.log[:]))[:len(entries)], list(reversed(entries)))

    def test_entry(self):
        entry = self.log.entry(1, 'test', (), {})
        self.assertEqual(self.log[-1], entry)
        self.assertEqual(len(self.log), entry.index + 1)

    def test_cmp(self):
        entry = LogEntry(dict(index=0, term=1))
        self.log.append(entry)
        self.assertTrue(self.log.cmp(entry.index + 1, entry.term + 1))
        self.assertFalse(self.log.cmp(entry.index - 1, entry.term - 1))
        self.assertFalse(self.log.cmp(entry.index + 1, entry.term - 1))
        self.assertTrue(self.log.cmp(entry.index - 1, entry.term + 1))


class StorageTest(unittest.TestCase):

    def get_storage(self):
        return Storage(env=lmdb.open(self.db_dir, max_dbs=10))

    def setUp(self):
        self.db_dir = tempfile.mkdtemp()
        self.storage = self.get_storage()

    def tearDown(self):
        shutil.rmtree(self.db_dir)

    def test_medadata_attribute(self):
        self.assertIsInstance(Storage.id, MetaDataField)

    def test_defaults(self):
        self.assertEqual(self.storage.term, 0)
        self.assertEqual(self.storage.id, self.get_storage().id)
