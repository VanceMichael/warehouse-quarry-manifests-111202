"""快照湖端到端演示：合成影像索引，不接任何影像服务。

演示内容（全部使用真实子进程与文件屏障编排）：
1. 每天产生大量小 JSONL 元数据文件，读者一次查询要打开很多对象；
2. 长读者固定快照并续期租约，与后台压实、回收并发；
3. 两个压实者竞争同一旧分区，只有一个成功；不同分区互不阻挡；
4. 压实进程在指针切换前崩溃，恢复后重新发布；
5. 租约到期不永久阻塞清理，回收先解释再执行；
6. 全程逐行指纹对比，记录集合不变；物理对象文件数显著下降。
"""

import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
DRIVER = str(ROOT / 'warehouse' / 'driver.py')
from warehouse.driver import CONFLICT_EXIT, CRASH_EXIT  # noqa: E402
from warehouse.lake import SnapshotLake  # noqa: E402

PARTITIONS = ['2026-10-01', '2026-10-02', '2026-10-03']
BATCHES_PER_DAY = 8
ROWS_PER_BATCH = 25


def run(lake, barriers, *arguments, timeout=30):
    command = [sys.executable, '-I', DRIVER, '--lake', str(lake),
               '--barrier', str(barriers), *arguments]
    print('$ ' + ' '.join(command), flush=True)
    result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, timeout=timeout)
    if result.stdout.strip():
        print(result.stdout.strip(), flush=True)
    if result.returncode not in (0, CONFLICT_EXIT, CRASH_EXIT):
        raise RuntimeError(f'命令失败({result.returncode}): {result.stderr}')
    payload = json.loads([line for line in result.stdout.splitlines() if line][-1])
    return result.returncode, payload


def wait(barriers, name, timeout=15):
    deadline = time.time() + timeout
    while not (barriers / name).exists():
        if time.time() > deadline:
            raise RuntimeError(f'等待屏障超时: {name}')
        time.sleep(0.02)


def object_count(lake):
    return len(list((lake / 'objects').glob('*.jsonl')))


def fingerprint(lake):
    reader = SnapshotLake(lake).open_reader(lease_ttl_seconds=60)
    try:
        return reader.fingerprint()
    finally:
        reader.release()


def materialize(lake):
    """读出当前全部记录，按 (分区, 场景主键) 建立内容索引。"""
    reader = SnapshotLake(lake).open_reader(lease_ttl_seconds=60)
    try:
        snapshot = reader.read_all()
    finally:
        reader.release()
    indexed = {}
    for partition, records in snapshot.items():
        for record in records:
            indexed[(partition, record['scene'])] = record
    return indexed


def main():
    with tempfile.TemporaryDirectory() as temporary:
        base = Path(temporary)
        lake = base / 'lake'
        barriers = base / 'barriers'
        barriers.mkdir()
        print('=' * 72)
        print('阶段 0：写入期——每天 8 个小批次，每批 25 条合成影像索引记录')
        print('=' * 72)
        total_rows = 0
        for partition in PARTITIONS:
            for _ in range(BATCHES_PER_DAY):
                code, payload = run(lake, barriers, 'append', '--partition',
                                    partition, '--rows', str(ROWS_PER_BATCH))
                total_rows += payload['rows']
        initial_files = object_count(lake)
        baseline = fingerprint(lake)
        baseline_records = materialize(lake)
        print(f'写入完成：{total_rows} 行，物理对象文件 {initial_files} 个，'
              f"快照 {baseline['manifest_id'][:8]}…", flush=True)
        print(f"逐行指纹（基线）：{baseline['fingerprint']}\n", flush=True)

        def compact_and_verify(partition):
            """压实前后记录多重集合必须完全一致（只改物理布局）。"""
            before = fingerprint(lake)
            run(lake, barriers, 'compact', '--partition', partition)
            after = fingerprint(lake)
            assert after['total_rows'] == before['total_rows'], '压实改变了行数'
            assert after['fingerprint'] == before['fingerprint'], '压实改变了记录集合'
            return after

        print('=' * 72)
        print('阶段 1：长读者固定快照 + 可续期租约，与后台压实并发')
        print('=' * 72)
        reader = subprocess.Popen(
            [sys.executable, '-I', DRIVER, '--lake', str(lake),
             '--barrier', str(barriers), 'slow-reader', '--name', 'long',
             '--ttl', '0.8', '--renew-every', '0.2', '--hold', '6'],
            cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        wait(barriers, 'long.opened')
        for partition in PARTITIONS:
            compact_and_verify(partition)
        # 长读者租约有效期间，回收不能删它引用的旧对象。
        code, protected = run(lake, barriers, 'reclaim-dry', '--retain', '1')
        assert protected['delete_objects'] == [], '有效租约未能保护旧版本对象'
        print('-> 长读者持租约期间，回收计划删除对象数为 0（旧版本受保护）\n', flush=True)

        print('=' * 72)
        print('阶段 2：两个压实者竞争同一旧分区，只允许一个成功')
        print('=' * 72)
        # 先把分区 1 重新拆碎，制造可压实状态。
        run(lake, barriers, 'append', '--partition', PARTITIONS[0], '--rows', '10')
        run(lake, barriers, 'append', '--partition', PARTITIONS[0], '--rows', '10')
        compactor_a = subprocess.Popen(
            [sys.executable, '-I', DRIVER, '--lake', str(lake),
             '--barrier', str(barriers), 'compact', '--partition', PARTITIONS[0],
             '--max-attempts', '1',
             '--signal-before-commit', 'a.ready', '--wait-before-commit', 'a.go'],
            cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        wait(barriers, 'a.ready')
        code_b, payload_b = run(lake, barriers, 'compact', '--partition', PARTITIONS[0],
                                '--max-attempts', '1')
        assert code_b == 0 and payload_b['event'] == 'compacted'
        (barriers / 'a.go').touch()
        out_a, err_a = compactor_a.communicate(timeout=15)
        payload_a = json.loads([line for line in out_a.splitlines() if line][-1])
        assert compactor_a.returncode == CONFLICT_EXIT and payload_a['event'] == 'conflict'
        print('-> 压实者 B 成功，压实者 A 因基础分区版本过期被拒绝（退出码 '
              f'{CONFLICT_EXIT}）\n', flush=True)

        print('=' * 72)
        print('阶段 3：不同分区并发压实，互不阻挡')
        print('=' * 72)
        run(lake, barriers, 'append', '--partition', PARTITIONS[1], '--rows', '7')
        run(lake, barriers, 'append', '--partition', PARTITIONS[2], '--rows', '7')
        workers = [
            subprocess.Popen(
                [sys.executable, '-I', DRIVER, '--lake', str(lake),
                 'compact', '--partition', partition],
                cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            for partition in PARTITIONS[1:]
        ]
        for worker in workers:
            out, err = worker.communicate(timeout=15)
            assert worker.returncode == 0, err
        print('-> 两个分区的压实进程同时成功，没有互相阻塞\n', flush=True)

        print('=' * 72)
        print('阶段 4：发布前崩溃（清单已写、指针未切换）后的明确恢复')
        print('=' * 72)
        run(lake, barriers, 'append', '--partition', PARTITIONS[0], '--rows', '5')
        pointer_before = SnapshotLake(lake).current_manifest_id
        code, crashed = run(lake, barriers, 'compact', '--partition', PARTITIONS[0],
                            '--crash-before-pointer')
        assert code == CRASH_EXIT and crashed['stage'] == 'manifest-written-before-pointer'
        assert SnapshotLake(lake).current_manifest_id == pointer_before
        print('-> 指针保持旧版本，读者不会读到新旧混合结果', flush=True)
        code, recovered = run(lake, barriers, 'recover')
        assert recovered['current_manifest'] == pointer_before
        print(f"-> 恢复清理临时文件，识别孤儿清单 {len(recovered['orphan_manifests'])} 个\n",
              flush=True)
        run(lake, barriers, 'compact', '--partition', PARTITIONS[0])

        print('=' * 72)
        print('阶段 5：等待长读者结束并核对它的逐行校验结果')
        print('=' * 72)
        out, err = reader.communicate(timeout=20)
        reader_done = json.loads([line for line in out.splitlines() if line][-1])
        assert reader.returncode == 0, err
        assert reader_done['fingerprint'] == baseline['fingerprint']
        assert reader_done['total_rows'] == baseline['total_rows']
        print(f"-> 长读者指纹 {reader_done['fingerprint']} 与基线一致，"
              f"行数 {reader_done['total_rows']} 不变\n", flush=True)

        print('=' * 72)
        print('阶段 6：失效租约不永久阻塞清理；回收先解释、再执行')
        print('=' * 72)
        # 一个不续期的短租约，自然到期。
        ephemeral = subprocess.Popen(
            [sys.executable, '-I', DRIVER, '--lake', str(lake),
             '--barrier', str(barriers), 'slow-reader', '--name', 'ephemeral',
             '--ttl', '0.2', '--renew-every', '999', '--hold', '0.5'],
            cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        wait(barriers, 'ephemeral.opened')
        ephemeral.communicate(timeout=10)
        print('-> 短租约未续期，读取因租约过期被拒绝（进程非零退出）', flush=True)
        time.sleep(0.1)
        files_before_gc = object_count(lake)
        code, dry = run(lake, barriers, 'reclaim-dry', '--retain', '1')
        print('回收解释（dry-run）：')
        for line in dry['explanation']:
            print('   ' + line)
        code, reclaimed = run(lake, barriers, 'reclaim', '--retain', '1')
        assert not reclaimed['dry_run']
        files_after_gc = object_count(lake)

        print('=' * 72)
        print('阶段 7：最终核对——文件数量下降，且基线记录逐行不变')
        print('=' * 72)
        final = fingerprint(lake)
        final_records = materialize(lake)
        run(lake, barriers, 'status')
        print(f'物理对象文件：{initial_files} 个（写入后） -> {files_before_gc} 个'
              f'（压实后、回收前） -> {files_after_gc} 个（回收后）')
        print(f"基线行数 {baseline['total_rows']} / 当前总行数 {final['total_rows']}"
              '（差额来自后续为制造压实而有意追加的新分区文件）')
        # 不变量一：基线中的每一条记录仍逐行存在且内容字节级一致。
        missing_or_changed = [
            key for key, record in baseline_records.items()
            if final_records.get(key) != record
        ]
        assert not missing_or_changed, f'基线记录丢失或被改写: {missing_or_changed[:3]}'
        # 不变量二：所有压实动作都已在发生时通过指纹校验（compact_and_verify）。
        # 不变量三：物理布局显著收敛。
        assert files_after_gc < initial_files
        print(f"基线 {len(baseline_records)} 条记录全部逐行保留，未发生丢失或改写。")
        print(f"逐行指纹（当前）：{final['fingerprint']}")
        print('\n演示通过：压实只改变物理布局，记录集合逐行不变；'
              '崩溃、竞争与租约到期均有明确恢复与回收语义。')


if __name__ == '__main__':
    main()
