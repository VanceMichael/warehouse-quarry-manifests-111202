"""真实进程级交错测试：长读者、并发压实、发布前崩溃、租约到期。

每个子进程通过 ``python -I warehouse/driver.py`` 运行，屏障目录只传递
路径，不依赖目录遍历顺序判断版本。
"""

import json
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DRIVER = str(ROOT / 'warehouse' / 'driver.py')
from warehouse.driver import CONFLICT_EXIT, CRASH_EXIT  # noqa: E402
from warehouse.lake import SnapshotLake  # noqa: E402


class ProcessDrivenTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)
        self.lake = self.base / 'lake'
        self.barriers = self.base / 'barriers'
        self.barriers.mkdir()

    def tearDown(self):
        self.temporary.cleanup()

    def run_driver(self, *arguments, timeout=30):
        command = [sys.executable, '-I', DRIVER, '--lake', str(self.lake),
                   '--barrier', str(self.barriers), *arguments]
        result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, timeout=timeout)
        lines = [line for line in result.stdout.splitlines() if line.strip()]
        payload = json.loads(lines[-1]) if lines else {}
        return result, payload

    def wait_barrier(self, name, timeout=10):
        path = self.barriers / name
        deadline = time.time() + timeout
        while not path.exists():
            if time.time() > deadline:
                raise AssertionError(f'屏障等待超时: {name}')
            time.sleep(0.02)

    def seed(self, partition, batches, size):
        for index in range(batches):
            result, payload = self.run_driver(
                'append', '--partition', partition, '--rows', str(size))
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_two_compactor_processes_same_partition_one_winner(self):
        self.seed('2026-10-01', batches=4, size=5)
        # A 在指针切换前发出 ready 并等待 go；B 等 A 就绪后再启动，
        # 因此两者都基于同一旧分区版本，只有先提交者成功。
        worker_a = subprocess.Popen(
            [sys.executable, '-I', DRIVER, '--lake', str(self.lake),
             '--barrier', str(self.barriers), 'compact', '--partition', '2026-10-01',
             '--max-attempts', '1',
             '--signal-before-commit', 'a.ready', '--wait-before-commit', 'a.go'],
            cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        self.wait_barrier('a.ready')
        worker_b = subprocess.Popen(
            [sys.executable, '-I', DRIVER, '--lake', str(self.lake),
             '--barrier', str(self.barriers), 'compact', '--partition', '2026-10-01',
             '--max-attempts', '1'],
            cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        out_b, err_b = worker_b.communicate(timeout=15)
        payload_b = json.loads([line for line in out_b.splitlines() if line][-1])
        self.assertEqual(worker_b.returncode, 0, err_b)
        self.assertEqual(payload_b['event'], 'compacted')
        # 放行 A：它的基础版本已被 B 推进，必须冲突失败。
        (self.barriers / 'a.go').touch()
        out_a, err_a = worker_a.communicate(timeout=15)
        payload_a = json.loads([line for line in out_a.splitlines() if line][-1])
        self.assertEqual(worker_a.returncode, CONFLICT_EXIT)
        self.assertEqual(payload_a['event'], 'conflict')
        manifest = SnapshotLake(self.lake).current_manifest()
        self.assertEqual(len(manifest.partitions['2026-10-01'].objects), 1)

    def test_crash_before_pointer_swap_then_recover_and_reclaim(self):
        self.seed('2026-10-01', batches=3, size=6)
        lake = SnapshotLake(self.lake)
        current_before = lake.current_manifest_id
        object_files_before = len(list((self.lake / 'objects').glob('*.jsonl')))

        result, payload = self.run_driver(
            'compact', '--partition', '2026-10-01', '--crash-before-pointer')
        self.assertEqual(result.returncode, CRASH_EXIT)
        self.assertEqual(payload['event'], 'crash')
        # 指针未切换，旧快照照常可读。
        self.assertEqual(SnapshotLake(self.lake).current_manifest_id, current_before)

        recovered, recover_payload = self.run_driver('recover')
        self.assertEqual(recovered.returncode, 0, recovered.stderr)
        self.assertEqual(recover_payload['event'], 'recovered')
        self.assertEqual(recover_payload['current_manifest'], current_before)
        self.assertTrue(recover_payload['orphan_manifests'])  # 清单已写、指针未换

        # 重新压实成功，随后回收解释并删除崩溃残留与孤儿对象。
        compacted, compact_payload = self.run_driver('compact', '--partition', '2026-10-01')
        self.assertEqual(compacted.returncode, 0, compacted.stderr)
        dry, dry_payload = self.run_driver('reclaim-dry', '--retain', '1')
        self.assertEqual(dry.returncode, 0)
        self.assertTrue(dry_payload['explanation'])  # 先解释
        self.assertTrue(dry_payload['delete_objects'] or dry_payload['delete_manifests'])
        reclaimed, reclaim_payload = self.run_driver('reclaim', '--retain', '1')
        self.assertEqual(reclaimed.returncode, 0, reclaimed.stderr)
        self.assertFalse(reclaim_payload['dry_run'])
        # 文件数量确实下降，当前快照逐行校验仍然通过。
        object_files_after = len(list((self.lake / 'objects').glob('*.jsonl')))
        self.assertLess(object_files_after, object_files_before)
        verified, read_payload = self.run_driver('open-reader', '--ttl', '30')
        self.assertEqual(verified.returncode, 0, verified.stderr)
        self.assertEqual(read_payload['total_rows'], 18)

    def test_long_reader_with_lease_expiry_and_renewal(self):
        self.seed('2026-10-01', batches=4, size=5)
        lake = SnapshotLake(self.lake)
        expected = lake.open_reader(lease_ttl_seconds=60).fingerprint()

        # 长读者固定旧快照，TTL 很短但持续续期。
        reader = subprocess.Popen(
            [sys.executable, '-I', DRIVER, '--lake', str(self.lake),
             '--barrier', str(self.barriers), 'slow-reader', '--name', 'lr',
             '--ttl', '0.6', '--renew-every', '0.15', '--hold', '3'],
            cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        self.wait_barrier('lr.opened')
        # 读者持有期间连续压实 + 回收：旧对象受租约保护。
        result, payload = self.run_driver('compact', '--partition', '2026-10-01')
        self.assertEqual(result.returncode, 0, result.stderr)
        protected, protected_payload = self.run_driver('reclaim-dry', '--retain', '1')
        self.assertEqual(protected.returncode, 0)
        self.assertEqual(protected_payload['delete_objects'], [])
        out, err = reader.communicate(timeout=15)
        self.assertEqual(reader.returncode, 0, err)
        done = json.loads([line for line in out.splitlines() if line][-1])
        self.assertEqual(done['event'], 'reader-done')
        self.assertEqual(done['fingerprint'], expected['fingerprint'])
        self.assertEqual(done['total_rows'], expected['total_rows'])

        # 租约释放后，再用一个不续期的短租约演示到期不阻塞清理。
        short = subprocess.Popen(
            [sys.executable, '-I', DRIVER, '--lake', str(self.lake),
             '--barrier', str(self.barriers), 'slow-reader', '--name', 'ep',
             '--ttl', '0.2', '--renew-every', '999', '--hold', '0.6'],
            cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        self.wait_barrier('ep.opened')
        out_short, err_short = short.communicate(timeout=10)
        # 租约过期后 fingerprint 失败，进程以非零码退出。
        self.assertNotEqual(short.returncode, 0, out_short)
        time.sleep(0.1)
        reclaimed, payload = self.run_driver('reclaim', '--retain', '1')
        self.assertEqual(reclaimed.returncode, 0, reclaimed.stderr)
        self.assertTrue(payload['delete_leases'])  # 失效租约被清掉
        # 过期租约没有永久挡住对象回收。
        self.assertEqual(
            SnapshotLake(self.lake).open_reader(lease_ttl_seconds=60).fingerprint()['fingerprint'],
            expected['fingerprint'],
        )

    def test_different_partitions_compact_concurrently(self):
        self.seed('2026-10-01', batches=3, size=4)
        self.seed('2026-10-02', batches=3, size=4)
        procs = [
            subprocess.Popen(
                [sys.executable, '-I', DRIVER, '--lake', str(self.lake),
                 'compact', '--partition', partition],
                cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            for partition in ('2026-10-01', '2026-10-02')
        ]
        outputs = []
        for proc in procs:
            out, err = proc.communicate(timeout=15)
            self.assertEqual(proc.returncode, 0, err)
            outputs.append(json.loads([line for line in out.splitlines() if line][-1]))
        self.assertEqual({item['event'] for item in outputs}, {'compacted'})
        manifest = SnapshotLake(self.lake).current_manifest()
        for partition in ('2026-10-01', '2026-10-02'):
            self.assertEqual(len(manifest.partitions[partition].objects), 1)


if __name__ == '__main__':
    unittest.main()
