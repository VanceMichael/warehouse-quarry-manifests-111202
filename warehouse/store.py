import hashlib
import json
import os
import tempfile
from pathlib import Path

from .catalog import encode


class ObjectStore:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def put(self, rows):
        payload = b''.join(encode(row) + b'\n' for row in rows)
        digest = hashlib.sha256(payload).hexdigest()
        target = self.root / (digest + '.jsonl')
        if target.exists():
            self.read(digest)
            return digest
        descriptor, temporary = tempfile.mkstemp(dir=self.root, prefix='.writing-')
        try:
            with os.fdopen(descriptor, 'wb') as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temporary, target)
            except FileExistsError:
                self.read(digest)
            directory = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            Path(temporary).unlink(missing_ok=True)
        return digest

    def read(self, digest):
        if len(digest) != 64 or any(character not in '0123456789abcdef' for character in digest):
            raise ValueError('对象标识格式错误')
        payload = (self.root / (digest + '.jsonl')).read_bytes()
        if hashlib.sha256(payload).hexdigest() != digest:
            raise ValueError('对象内容校验失败')
        return [json.loads(line) for line in payload.splitlines()]
