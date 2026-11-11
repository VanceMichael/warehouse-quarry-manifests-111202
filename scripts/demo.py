import json
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from warehouse.store import ObjectStore


def execute(*arguments):
    command = [sys.executable, '-m', 'warehouse', *arguments]
    print('$ ' + ' '.join(command), flush=True)
    result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=True)
    print(result.stdout.strip(), flush=True)
    return json.loads(result.stdout)


def main():
    with tempfile.TemporaryDirectory() as temporary:
        database = str(Path(temporary) / 'warehouse.sqlite3')
        objects = str(Path(temporary) / 'objects')
        imported = execute('import', '--db', database, '--input', str(ROOT / 'examples/source.json'), '--batch', 'daily')
        if not imported['changed']:
            raise RuntimeError('首次导入没有写入')
        repeated = execute('import', '--db', database, '--input', str(ROOT / 'examples/source.json'), '--batch', 'daily')
        if repeated['changed']:
            raise RuntimeError('重复导入发生额外写入')
        calculated = execute('inspect', '--db', database)
        exported = execute('export', '--db', database, '--objects', objects)
        restored = ObjectStore(objects).read(exported['object'])
        expected = calculated if isinstance(calculated, list) else [calculated]
        if restored != expected:
            raise RuntimeError('导出与查询结果不符')
        print('对象校验通过，重复导入未改变业务结果', flush=True)


if __name__ == '__main__':
    main()
