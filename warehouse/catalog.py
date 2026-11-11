import hashlib
import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from .definition import SCHEMA


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')


def connect(path):
    connection = sqlite3.connect(path, timeout=5, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute('PRAGMA foreign_keys=ON')
    connection.execute('PRAGMA busy_timeout=5000')
    for table, columns in SCHEMA.items():
        connection.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({columns})')
    connection.execute('CREATE TABLE IF NOT EXISTS ingest_batches (batch TEXT PRIMARY KEY, digest TEXT NOT NULL, rows INTEGER NOT NULL)')
    return connection


@contextmanager
def transaction(connection):
    connection.execute('BEGIN IMMEDIATE')
    try:
        yield connection
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def load_bundle(connection, bundle, batch):
    if not isinstance(bundle, dict) or not isinstance(batch, str) or not batch:
        raise ValueError('输入需要命名数据集合与批次标识')
    if set(bundle) - set(SCHEMA):
        raise ValueError('来源包含未登记的数据集合')
    digest = hashlib.sha256(encode(bundle)).hexdigest()
    count = 0
    with transaction(connection):
        previous = connection.execute('SELECT digest FROM ingest_batches WHERE batch=?', (batch,)).fetchone()
        if previous:
            if previous['digest'] != digest:
                raise ValueError('同批次内容不一致')
            return False
        for table, rows in bundle.items():
            if not isinstance(rows, list):
                raise ValueError('数据集合必须是记录数组')
            columns = [record['name'] for record in connection.execute(f'PRAGMA table_info("{table}")')]
            for row in rows:
                if not isinstance(row, dict) or set(row) != set(columns):
                    raise ValueError('记录字段与已登记结构不一致')
                names = ', '.join(f'"{column}"' for column in columns)
                placeholders = ', '.join('?' for column in columns)
                connection.execute(f'INSERT INTO "{table}" ({names}) VALUES ({placeholders})', [row[column] for column in columns])
                count += 1
        connection.execute('INSERT INTO ingest_batches VALUES (?, ?, ?)', (batch, digest, count))
    return True


def load_file(connection, path, batch):
    return load_bundle(connection, json.loads(Path(path).read_text(encoding='utf-8')), batch)
