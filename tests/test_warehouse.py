import copy
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from warehouse.catalog import connect, load_bundle, load_file, transaction
from warehouse.domain import overview
from warehouse.store import ObjectStore

SOURCE = Path(__file__).resolve().parents[1] / 'examples/source.json'
EXPECTED = json.loads((SOURCE.parent / 'expected.json').read_text(encoding='utf-8'))


class CatalogTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / 'warehouse.sqlite3'
        self.connection = connect(self.path)
        self.bundle = json.loads(SOURCE.read_text(encoding='utf-8'))

    def tearDown(self):
        self.connection.close()
        self.temporary.cleanup()

    def test_replay(self):
        self.assertTrue(load_file(self.connection, SOURCE, 'daily'))
        first = overview(self.connection)
        self.assertFalse(load_file(self.connection, SOURCE, 'daily'))
        self.assertEqual(overview(self.connection), first)
        self.assertEqual(self.connection.execute('SELECT COUNT(*) FROM ingest_batches').fetchone()[0], 1)

    def test_conflicting_batch(self):
        load_bundle(self.connection, self.bundle, 'daily')
        with self.assertRaises(ValueError):
            load_bundle(self.connection, {}, 'daily')
        self.assertEqual(overview(self.connection), EXPECTED)

    def test_duplicate_key_rolls_back(self):
        broken = copy.deepcopy(self.bundle)
        table = next(iter(broken))
        broken[table].append(copy.deepcopy(broken[table][0]))
        with self.assertRaises(sqlite3.IntegrityError):
            load_bundle(self.connection, broken, 'broken')
        self.assertEqual(self.connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0], 0)
        self.assertEqual(self.connection.execute('SELECT COUNT(*) FROM ingest_batches').fetchone()[0], 0)

    def test_unknown_table(self):
        with self.assertRaises(ValueError):
            load_bundle(self.connection, {'unknown_table': []}, 'unknown')

    def test_transaction_failure(self):
        with self.assertRaises(RuntimeError):
            with transaction(self.connection):
                self.connection.execute('INSERT INTO ingest_batches VALUES (?, ?, ?)', ('partial', 'abc', 1))
                raise RuntimeError('模拟写入中断')
        self.assertEqual(self.connection.execute('SELECT COUNT(*) FROM ingest_batches').fetchone()[0], 0)

    def test_invalid_record(self):
        broken = copy.deepcopy(self.bundle)
        table = next(iter(broken))
        broken[table][0]['unexpected'] = 'x'
        with self.assertRaises(ValueError):
            load_bundle(self.connection, broken, 'broken')

    def test_invalid_batch(self):
        with self.assertRaises(ValueError):
            load_bundle(self.connection, self.bundle, '')

    def test_domain_expected(self):
        load_bundle(self.connection, self.bundle, 'daily')
        self.assertEqual(overview(self.connection), EXPECTED)

    def test_domain_export_roundtrip(self):
        load_bundle(self.connection, self.bundle, 'daily')
        store = ObjectStore(Path(self.temporary.name) / 'objects')
        self.assertEqual(store.read(store.put(overview(self.connection))), EXPECTED)

    def test_persistence(self):
        load_bundle(self.connection, self.bundle, 'daily')
        self.connection.close()
        self.connection = connect(self.path)
        self.assertFalse(load_bundle(self.connection, self.bundle, 'daily'))
        self.assertEqual(overview(self.connection), EXPECTED)

    def test_empty_batch(self):
        self.assertTrue(load_bundle(self.connection, {}, 'empty'))
        self.assertEqual(self.connection.execute('SELECT rows FROM ingest_batches').fetchone()[0], 0)

    def test_distinct_batch_conflict(self):
        load_bundle(self.connection, self.bundle, 'daily')
        with self.assertRaises(sqlite3.IntegrityError):
            load_bundle(self.connection, self.bundle, 'different')
        self.assertEqual(overview(self.connection), EXPECTED)
        self.assertEqual(self.connection.execute('SELECT COUNT(*) FROM ingest_batches').fetchone()[0], 1)


class ObjectTests(unittest.TestCase):
    def test_roundtrip_and_reuse(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = ObjectStore(temporary)
            rows = [{'标识': '合成数据', 'value': None}, {'number': 3}]
            digest = store.put(rows)
            self.assertEqual(store.read(digest), rows)
            self.assertEqual(store.put(rows), digest)
            self.assertEqual(len(list(Path(temporary).glob('*.jsonl'))), 1)

    def test_corrupt_object(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = ObjectStore(temporary)
            digest = store.put([{'value': 1}])
            (Path(temporary) / (digest + '.jsonl')).write_bytes(b'{}')
            with self.assertRaises(ValueError):
                store.read(digest)

    def test_empty_and_invalid_identifier(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = ObjectStore(temporary)
            self.assertEqual(store.read(store.put([])), [])
            with self.assertRaises(ValueError):
                store.read('../outside')

    def test_nonfinite_number(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(ValueError):
                ObjectStore(temporary).put([{'value': float('nan')}])
