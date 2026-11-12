import argparse
import json
import uuid
from pathlib import Path

from .catalog import connect, load_file
from .domain import overview
from .lake import SnapshotLake, recover
from .store import ObjectStore


def _print(payload):
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, allow_nan=False))


def _lake_commands(parser):
    parser.add_argument('--lake', help='快照湖根目录')
    parser.add_argument('--partition')
    parser.add_argument('--input')
    parser.add_argument('--ttl', type=float, default=30.0)
    parser.add_argument('--retain', type=int, default=2)
    parser.add_argument('--rows', type=int)
    parser.add_argument('--manifest')


def main():
    parser = argparse.ArgumentParser(description='本地数据集合装载、查询与快照湖')
    parser.add_argument('command', choices=[
        'import', 'inspect', 'export',
        'lake-append', 'lake-compact', 'lake-read', 'lake-reclaim',
        'lake-recover', 'lake-status',
    ])
    parser.add_argument('--db')
    parser.add_argument('--batch')
    parser.add_argument('--objects')
    _lake_commands(parser)
    args = parser.parse_args()

    if args.command in ('import', 'inspect', 'export'):
        if not args.db:
            parser.error('关系命令需要 --db')
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
            _print(result)
        finally:
            connection.close()
        return

    if not args.lake:
        parser.error('快照湖命令需要 --lake')
    lake = SnapshotLake(args.lake)

    if args.command == 'lake-append':
        if not args.partition:
            parser.error('lake-append 需要 --partition')
        if args.input:
            rows = json.loads(Path(args.input).read_text(encoding='utf-8'))
        elif args.rows:
            unique = uuid.uuid4().hex[:8]
            rows = [
                {'scene': f'syn-{args.partition}-{unique}-{index:05d}',
                 'partition_day': args.partition, 'tile': 't1', 'bytes': index + 1}
                for index in range(args.rows)
            ]
        else:
            parser.error('lake-append 需要 --input 或 --rows')
        manifest = lake.append_partition(args.partition, rows)
        _print({'event': 'appended', 'partition': args.partition,
                'rows': len(rows), 'manifest_id': manifest.manifest_id,
                'partition_version': manifest.partitions[args.partition].version})

    elif args.command == 'lake-compact':
        if not args.partition:
            parser.error('lake-compact 需要 --partition')
        before = len(list(lake.objects_dir.glob('*.jsonl')))
        manifest = lake.compact_partition_with_retry(args.partition)
        after = len(list(lake.objects_dir.glob('*.jsonl')))
        _print({'event': 'compacted', 'partition': args.partition,
                'manifest_id': manifest.manifest_id,
                'partition_version': manifest.partitions[args.partition].version,
                'objects_in_partition': len(manifest.partitions[args.partition].objects),
                'object_files_before': before, 'object_files_after': after})

    elif args.command == 'lake-read':
        with lake.open_reader(manifest_id=args.manifest, lease_ttl_seconds=args.ttl) as reader:
            _print({'event': 'read', 'lease_id': reader.lease_id,
                    'manifest_id': reader.manifest_id, **reader.fingerprint()})

    elif args.command == 'lake-reclaim':
        plan = lake.reclaim(retain_manifest_versions=args.retain, execute=False)
        for line in plan.explain_lines():
            print(line)
        executed = lake.reclaim(retain_manifest_versions=args.retain, execute=True)
        _print({'event': 'reclaimed', 'dry_run': False,
                'deleted_objects': len(executed.delete_objects),
                'deleted_manifests': len(executed.delete_manifests),
                'deleted_leases': len(executed.delete_leases),
                'removed_temporary': len(executed.reclaimed_temporary)})

    elif args.command == 'lake-recover':
        _print({'event': 'recovered', **recover(args.lake)})

    elif args.command == 'lake-status':
        manifest = lake.current_manifest()
        _print({
            'event': 'status',
            'manifest_id': manifest.manifest_id if manifest else None,
            'object_files': len(list(lake.objects_dir.glob('*.jsonl'))),
            'active_leases': len(lake.list_leases()),
            'partitions': {
                name: {'version': state.version, 'objects': len(state.objects), 'rows': state.rows}
                for name, state in (manifest.partitions.items() if manifest else ())
            },
        })


if __name__ == '__main__':
    main()
