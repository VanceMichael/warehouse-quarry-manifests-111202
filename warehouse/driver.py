"""子进程编排驱动：用文件屏障实现可控的进程交错。

该模块只服务于演示与进程级测试，不参与正常库 API。屏障语义：

- ``signal --barrier P --name N``：触碰 ``P/N`` 文件表示已到达该点。
- ``wait --barrier P --name N [--timeout S]``：轮询等待 ``P/N`` 出现。

所有动作输出单行 JSON；冲突退出码 3，模拟崩溃退出码 42。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path

# 允许以 ``python -I warehouse/driver.py`` 直接运行：-I 不注入 cwd，
# 这里显式把项目根目录加入模块搜索路径，避免 planted 模块劫持。
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from warehouse.lake import ConflictError, CorruptionError, LakeError, SnapshotLake  # noqa: E402

CONFLICT_EXIT = 3
CRASH_EXIT = 42


def _emit(payload: dict) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, allow_nan=False), flush=True)


def _wait_barrier(directory: str, name: str, timeout: float) -> None:
    path = Path(directory) / name
    deadline = time.time() + timeout
    while not path.exists():
        if time.time() >= deadline:
            raise TimeoutError(f'等待屏障超时: {name}')
        time.sleep(0.01)


def _signal(directory: str, name: str) -> None:
    path = Path(directory) / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()


def _crash(stage: str) -> None:
    """以非零退出码模拟进程在发布中途被杀死（不跑 finally/atexit）。"""
    _emit({'event': 'crash', 'stage': stage})
    os._exit(CRASH_EXIT)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='快照湖进程交错驱动')
    parser.add_argument('--lake', required=True)
    parser.add_argument('--barrier', help='屏障目录')
    parser.add_argument('--timeout', type=float, default=30.0)
    parser.add_argument('action', choices=[
        'signal', 'wait', 'append', 'compact', 'reclaim-dry', 'reclaim',
        'recover', 'open-reader', 'slow-reader', 'status',
    ])
    parser.add_argument('--partition')
    parser.add_argument('--input', help='append 动作的 JSON 记录数组文件')
    parser.add_argument('--rows', type=int, help='append 动作生成合成记录的条数')
    parser.add_argument('--retain', type=int, default=2)
    parser.add_argument('--ttl', type=float, default=30.0)
    parser.add_argument('--renew-every', type=float)
    parser.add_argument('--hold', type=float, default=0.0, help='slow-reader 持租约秒数')
    parser.add_argument('--name', help='屏障名称')
    parser.add_argument('--crash-before-pointer', action='store_true',
                        help='compact：清单写完、指针切换前崩溃')
    parser.add_argument('--crash-before-commit', action='store_true',
                        help='compact：替代对象写完、提交前崩溃')
    parser.add_argument('--wait-before-commit', help='compact：提交前等待的屏障名')
    parser.add_argument('--signal-before-commit', help='compact：提交前发出的屏障名')
    parser.add_argument('--max-attempts', type=int, default=3,
                        help='compact：基础版本过期时的重读重试次数')
    args = parser.parse_args(argv)

    if args.action == 'signal':
        _signal(args.barrier, args.name)
        _emit({'event': 'signaled', 'name': args.name})
        return 0
    if args.action == 'wait':
        _wait_barrier(args.barrier, args.name, args.timeout)
        _emit({'event': 'waited', 'name': args.name})
        return 0

    lake = SnapshotLake(args.lake)
    try:
        if args.action == 'append':
            if args.input:
                rows = json.loads(Path(args.input).read_text(encoding='utf-8'))
            else:
                unique = uuid.uuid4().hex[:8]
                rows = [
                    {'scene': f'syn-{args.partition}-{unique}-{index:05d}',
                     'partition_day': args.partition, 'tile': 't1', 'bytes': index + 1}
                    for index in range(args.rows or 1)
                ]
            manifest = lake.append_partition(args.partition, rows)
            _emit({'event': 'appended', 'partition': args.partition,
                   'rows': len(rows), 'manifest_id': manifest.manifest_id})
            return 0

        if args.action == 'compact':
            hooks = {}
            attempt = 0

            def objects_written():
                if args.crash_before_commit:
                    _crash('objects-written-before-commit')
                # 屏障只在首次尝试生效，避免重试时重复等待同一个一次性信号。
                if attempt == 0 and args.signal_before_commit:
                    _signal(args.barrier, args.signal_before_commit)
                if attempt == 0 and args.wait_before_commit:
                    # 等待发生在获取提交锁之前，其他进程可以先完成提交。
                    _wait_barrier(args.barrier, args.wait_before_commit, args.timeout)

            def before_pointer_swap(manifest):
                if args.crash_before_pointer:
                    _crash('manifest-written-before-pointer')

            hooks['on_objects_written'] = objects_written
            hooks['on_before_pointer_swap'] = before_pointer_swap
            try:
                manifest = lake.compact_partition_with_retry(
                    args.partition, max_attempts=args.max_attempts, **hooks)
            except ConflictError as error:
                _emit({'event': 'conflict', 'partition': args.partition,
                       'max_attempts': args.max_attempts, 'error': str(error)})
                return CONFLICT_EXIT
            _emit({'event': 'compacted', 'partition': args.partition,
                   'manifest_id': manifest.manifest_id,
                   'objects': len(manifest.partitions[args.partition].objects),
                   'version': manifest.partitions[args.partition].version})
            return 0

        if args.action in ('reclaim-dry', 'reclaim'):
            plan = lake.reclaim(retain_manifest_versions=args.retain,
                                execute=(args.action == 'reclaim'))
            _emit({
                'event': args.action,
                'dry_run': plan.dry_run,
                'delete_objects': [digest for digest, _ in plan.delete_objects],
                'delete_manifests': [item for item, _ in plan.delete_manifests],
                'delete_leases': [item for item, _ in plan.delete_leases],
                'protected_count': len(plan.protected_objects),
                'removed_temporary': list(plan.reclaimed_temporary),
                'explanation': plan.explain_lines(),
            })
            return 0

        if args.action == 'recover':
            _emit({'event': 'recovered', **recover_report(lake)})
            return 0

        if args.action == 'open-reader':
            reader = lake.open_reader(lease_ttl_seconds=args.ttl)
            fingerprint = reader.fingerprint()
            _emit({'event': 'reader-opened', 'lease_id': reader.lease_id,
                   'manifest_id': reader.manifest_id,
                   'total_rows': fingerprint['total_rows'],
                   'fingerprint': fingerprint['fingerprint']})
            reader.release()
            return 0

        if args.action == 'slow-reader':
            return _slow_reader(lake, args)

        if args.action == 'status':
            manifest = lake.current_manifest()
            object_files = len(list(lake.objects_dir.glob('*.jsonl')))
            leases = lake.list_leases()
            _emit({
                'event': 'status',
                'manifest_id': manifest.manifest_id if manifest else None,
                'object_files': object_files,
                'leases': [lease.lease_id for lease in leases],
                'partitions': {
                    name: {'version': state.version, 'objects': len(state.objects),
                           'rows': state.rows}
                    for name, state in (manifest.partitions.items() if manifest else ())
                },
            })
            return 0
    except (LakeError, CorruptionError, ValueError, KeyError) as error:
        _emit({'event': 'error', 'type': type(error).__name__, 'error': str(error)})
        return 1
    except TimeoutError as error:
        _emit({'event': 'timeout', 'error': str(error)})
        return 2
    return 0


def recover_report(lake: SnapshotLake) -> dict:
    from warehouse.lake import recover

    return recover(lake.root)


def _slow_reader(lake: SnapshotLake, args: argparse.Namespace) -> int:
    """长读者：固定快照、周期性续期，并在屏障点与其他进程交错。"""
    reader = lake.open_reader(lease_ttl_seconds=args.ttl)
    initial = reader.fingerprint()
    _emit({'event': 'reader-opened', 'lease_id': reader.lease_id,
           'manifest_id': reader.manifest_id, 'fingerprint': initial['fingerprint'],
           'total_rows': initial['total_rows']})
    if args.barrier and args.name:
        _signal(args.barrier, args.name + '.opened')
    deadline = time.time() + args.hold
    renew_interval = args.renew_every if args.renew_every else max(args.ttl / 3, 0.05)
    next_renew = time.time() + renew_interval
    while time.time() < deadline:
        time.sleep(min(0.02, max(deadline - time.time(), 0.0)))
        if time.time() >= next_renew:
            reader.renew()
            next_renew = time.time() + renew_interval
    # 结束前再次逐行校验，确认压实期间读到的记录集合不变。
    final = reader.fingerprint()
    if final['fingerprint'] != initial['fingerprint'] or final['total_rows'] != initial['total_rows']:
        _emit({'event': 'reader-mismatch', 'initial': initial, 'final': final})
        return 1
    if args.barrier and args.name:
        _signal(args.barrier, args.name + '.verified')
    reader.release()
    _emit({'event': 'reader-done', 'lease_id': reader.lease_id,
           'fingerprint': final['fingerprint'], 'total_rows': final['total_rows']})
    return 0


if __name__ == '__main__':
    sys.exit(main())
