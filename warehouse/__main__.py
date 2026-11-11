import argparse
import json
from pathlib import Path

from .catalog import connect, load_file
from .domain import overview
from .store import ObjectStore


def main():
    parser = argparse.ArgumentParser(description='本地数据集合装载与查询')
    parser.add_argument('command', choices=['import', 'inspect', 'export'])
    parser.add_argument('--db', required=True)
    parser.add_argument('--input')
    parser.add_argument('--batch')
    parser.add_argument('--objects')
    args = parser.parse_args()
    if args.command == 'import' and (not args.input or not args.batch):
        parser.error('导入需要 --input 和 --batch')
    if args.command == 'export' and not args.objects:
        parser.error('导出需要 --objects')
    Path(args.db).parent.mkdir(parents=True, exist_ok=True)
    connection = connect(args.db)
    try:
        if args.command == 'import':
            result = {'changed': load_file(connection, args.input, args.batch)}
        else:
            result = overview(connection)
            if args.command == 'export':
                records = result if isinstance(result, list) else [result]
                result = {'object': ObjectStore(args.objects).put(records), 'rows': len(records)}
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False))
    finally:
        connection.close()


if __name__ == '__main__':
    main()
