import json
import tempfile
import threading
import unittest
from pathlib import Path

from warehouse.lake import (
    ConflictError,
    CorruptionError,
    LakeError,
    LeaseExpired,
    SnapshotLake,
    recover,
)


def rows(partition, count, start=0):
    return [
        {'scene': f'{partition}-{index:05d}', 'partition_day': partition,
         'tile': f't{index % 4}', 'bytes': 100 + index}
        for index in range(start, start + count)
    ]


class FakeClock:
    def __init__(self):
        self.now = 1_000_000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class LakeTestBase(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / 'lake'
        self.clock = FakeClock()
        self.lake = SnapshotLake(self.root, clock=self.clock)

    def tearDown(self):
        self.temporary.cleanup()

    def object_count(self):
        return len(list((self.root / 'objects').glob('*.jsonl')))

    def seed(self, partition='2026-10-01', batches=5, size=20):
        all_rows = []
        for index in range(batches):
            batch = rows(partition, size, start=index * size)
            self.lake.append_partition(partition, batch)
            all_rows.extend(batch)
        return all_rows


class AppendAndSnapshotTests(LakeTestBase):
    def test_append_builds_versions_and_pointer_chain(self):
        first = self.lake.append_partition('2026-10-01', rows('2026-10-01', 3))
        second = self.lake.append_partition('2026-10-01', rows('2026-10-01', 2, start=3))
        self.assertEqual(self.lake.current_manifest_id, second.manifest_id)
        self.assertEqual(second.parent_id, first.manifest_id)
        state = second.partitions['2026-10-01']
        self.assertEqual((state.version, state.rows, len(state.objects)), (2, 5, 2))
        # 不依赖目录遍历：指针显式指向当前清单。
        pointer = json.loads((self.root / 'current.json').read_text(encoding='utf-8'))
        self.assertEqual(pointer, {'current': second.manifest_id})

    def test_duplicate_object_is_reused_not_overwritten(self):
        batch = rows('2026-10-01', 4)
        self.lake.append_partition('2026-10-01', batch)
        count_after_first = self.object_count()
        self.lake.append_partition('2026-10-02', batch)  # 同样内容
        self.assertEqual(self.object_count(), count_after_first)
        manifest = self.lake.current_manifest()
        self.assertEqual(
            manifest.partitions['2026-10-01'].objects[0].digest,
            manifest.partitions['2026-10-02'].objects[0].digest,
        )

    def test_reader_pins_snapshot_ignoring_later_compaction(self):
        expected = self.seed(batches=4, size=10)
        reader = self.lake.open_reader(lease_ttl_seconds=60)
        pinned_id = reader.manifest_id
        pinned = reader.read_all()
        self.lake.compact_partition('2026-10-01')
        # 当前版本已切换，读者仍固定旧清单且内容一致。
        self.assertNotEqual(self.lake.current_manifest_id, pinned_id)
        self.assertEqual(reader.manifest_id, pinned_id)
        self.assertEqual(reader.read_partition('2026-10-01'), expected)
        self.assertEqual(reader.read_all()['2026-10-01'], pinned['2026-10-01'])
        reader.release()

    def test_fingerprint_stable_across_compaction(self):
        self.seed(batches=6, size=7)
        before = self.lake.open_reader(lease_ttl_seconds=60).fingerprint()
        self.lake.compact_partition('2026-10-01')
        after = self.lake.open_reader(lease_ttl_seconds=60).fingerprint()
        self.assertEqual(before['total_rows'], after['total_rows'])
        self.assertEqual(before['fingerprint'], after['fingerprint'])
        self.assertEqual(
            before['partitions']['2026-10-01']['row_hash'],
            after['partitions']['2026-10-01']['row_hash'],
        )
        self.assertGreater(
            before['partitions']['2026-10-01']['objects'],
            after['partitions']['2026-10-01']['objects'],
        )

    def test_invalid_partition_names(self):
        for bad in ('', '../escape', 'a/b', '中文分区', 'a b'):
            with self.assertRaises(ValueError):
                self.lake.append_partition(bad, [{'x': 1}])


class CompactionTests(LakeTestBase):
    def test_compaction_keeps_record_multiset(self):
        expected = self.seed(batches=5, size=8)
        before_files = self.object_count()
        manifest = self.lake.compact_partition('2026-10-01')
        state = manifest.partitions['2026-10-01']
        self.assertEqual(len(state.objects), 1)
        self.assertEqual(state.rows, len(expected))
        reader = self.lake.open_reader(lease_ttl_seconds=60)
        self.assertEqual(reader.read_partition('2026-10-01'), expected)
        # 替代对象先新增、旧对象等回收：此刻文件数不减反增或持平。
        self.assertGreaterEqual(self.object_count(), before_files)

    def test_single_object_partition_is_noop(self):
        self.lake.append_partition('2026-10-01', rows('2026-10-01', 2))
        current = self.lake.current_manifest_id
        result = self.lake.compact_partition('2026-10-01')
        self.assertEqual(result.manifest_id, current)

    def test_two_compactors_same_partition_only_one_wins(self):
        self.seed(batches=4, size=5)
        ready = threading.Event()
        release = threading.Event()
        outcomes = []

        def worker(wait_at_barrier):
            def on_objects_written():
                if wait_at_barrier:
                    ready.set()
                    release.wait(timeout=5)
            try:
                self.lake.compact_partition(
                    '2026-10-01', on_objects_written=on_objects_written)
                outcomes.append('won')
            except ConflictError:
                outcomes.append('lost')

        t1 = threading.Thread(target=worker, args=(True,))
        t2 = threading.Thread(target=worker, args=(False,))
        t1.start()
        self.assertTrue(ready.wait(timeout=5))  # t1 已写替代对象，停在提交锁之前
        t2.start()
        t2.join(timeout=5)
        release.set()
        t1.join(timeout=5)
        self.assertEqual(sorted(outcomes), ['lost', 'won'])
        manifest = self.lake.current_manifest()
        self.assertEqual(len(manifest.partitions['2026-10-01'].objects), 1)

    def test_compactors_on_different_partitions_do_not_block(self):
        self.seed('2026-10-01', batches=3, size=4)
        self.seed('2026-10-02', batches=3, size=4)
        results = {}
        errors = []

        def worker(partition):
            try:
                results[partition] = self.lake.compact_partition(partition)
            except ConflictError as error:
                errors.append(error)

        t1 = threading.Thread(target=worker, args=('2026-10-01',))
        t2 = threading.Thread(target=worker, args=('2026-10-02',))
        t1.start()
        t2.start()
        t1.join(5)
        t2.join(5)
        self.assertEqual(errors, [])
        self.assertEqual(set(results), {'2026-10-01', '2026-10-02'})
        for partition, manifest in results.items():
            self.assertEqual(len(manifest.partitions[partition].objects), 1)

    def test_writer_concurrent_with_compaction_loses_nothing(self):
        expected = self.seed(batches=4, size=6)
        conflict_seen = threading.Event()

        def hook(_manifest):
            # 指针切换前（同一可重入锁内）让另一线程先推进同一分区版本；
            # 压实提交必须检测到版本变化并放弃，而不是覆盖新分区文件。
            try:
                self.lake.append_partition('2026-10-01', rows('2026-10-01', 3, start=900))
            except ConflictError:
                conflict_seen.set()

        try:
            self.lake.compact_partition('2026-10-01', on_before_pointer_swap=hook)
        except ConflictError:
            conflict_seen.set()
        self.assertTrue(conflict_seen.is_set())
        # 写入者的新分区文件仍在；重试压实后记录集合只增不减。
        manifest = self.lake.compact_partition_with_retry('2026-10-01')
        state = manifest.partitions['2026-10-01']
        self.assertEqual(state.rows, len(expected) + 3)
        reader = self.lake.open_reader(lease_ttl_seconds=60)
        scene_set = {row['scene'] for row in reader.read_partition('2026-10-01')}
        self.assertIn('2026-10-01-00900', scene_set)
        for row in expected:
            self.assertIn(row['scene'], scene_set)

    def test_crash_before_pointer_swap_leaves_only_unreachable_objects(self):
        self.seed(batches=3, size=5)
        current_before = self.lake.current_manifest_id
        manifest_count_before = len(list((self.root / 'manifests').glob('manifest-*.json')))

        def crash(_manifest):
            raise RuntimeError('模拟发布前进程崩溃')

        with self.assertRaises(RuntimeError):
            self.lake.compact_partition('2026-10-01', on_before_pointer_swap=crash)
        # 指针没有变化，读者完全不受影响。
        self.assertEqual(self.lake.current_manifest_id, current_before)
        report = recover(self.root)
        self.assertEqual(report['current_manifest'], current_before)
        # 崩溃前已落盘的清单成为不可达孤儿（历史清单也在孤儿集合中）。
        manifest_count_after = len(list((self.root / 'manifests').glob('manifest-*.json')))
        self.assertEqual(manifest_count_after, manifest_count_before + 1)
        self.assertEqual(len(report['orphan_manifests']), manifest_count_after - 1)
        self.assertNotIn(current_before, report['orphan_manifests'])

    def test_retry_helper_succeeds_after_rebase(self):
        self.seed(batches=3, size=4)
        attempts = {'count': 0}

        def flaky():
            attempts['count'] += 1
            if attempts['count'] == 1:
                # 另一个写入者抢先推进版本。
                self.lake.append_partition('2026-10-01', rows('2026-10-01', 1, start=500))
                raise ConflictError('stale')

        with self.assertRaises(ConflictError):
            self.lake.compact_partition_with_retry('2026-10-01', max_attempts=1,
                                                   on_objects_written=flaky)
        manifest = self.lake.compact_partition_with_retry('2026-10-01', max_attempts=3)
        self.assertEqual(len(manifest.partitions['2026-10-01'].objects), 1)


class LeaseTests(LakeTestBase):
    def test_lease_renew_and_expiry_with_fake_clock(self):
        self.seed(batches=2, size=3)
        reader = self.lake.open_reader(lease_ttl_seconds=10)
        lease_id = reader.lease_id
        self.clock.advance(8)
        reader.renew()
        self.clock.advance(9)
        self.assertTrue(self.lake.get_lease(lease_id).expires_at > self.clock())
        self.clock.advance(2)
        with self.assertRaises(LeaseExpired):
            self.lake.get_lease(lease_id)
        with self.assertRaises(LeaseExpired):
            self.lake.renew_lease(lease_id)
        with self.assertRaises(LeaseExpired):
            reader.fingerprint()
        # 过期租约不再出现在有效列表里，因此不会阻塞回收。
        self.assertEqual(self.lake.list_leases(), [])

    def test_expired_lease_does_not_block_reclaim(self):
        expected = self.seed(batches=4, size=5)
        reader = self.lake.open_reader(lease_ttl_seconds=10)
        pinned_manifest = reader.manifest_id
        self.clock.advance(11)  # 租约到期
        self.lake.compact_partition('2026-10-01')
        self.clock.advance(1)
        plan = self.lake.plan_reclaim(retain_manifest_versions=1)
        # 旧清单与旧对象都不受过期租约保护。
        self.assertIn(pinned_manifest, [item for item, _ in plan.delete_manifests])
        deleted_digests = {digest for digest, _ in plan.delete_objects}
        old_digests = {
            ref.digest for ref in self.lake.load_manifest(pinned_manifest)
            .partitions['2026-10-01'].objects
        }
        self.assertTrue(old_digests <= deleted_digests)
        self.assertTrue(any('过期' in reason for _, reason in plan.delete_leases))
        executed = self.lake.reclaim(retain_manifest_versions=1, execute=True)
        self.assertEqual(executed.dry_run, False)
        # 当前版本仍可完整读取，记录集合不变。
        current_reader = self.lake.open_reader(lease_ttl_seconds=10)
        self.assertEqual(current_reader.read_partition('2026-10-01'), expected)

    def test_valid_lease_protects_old_version_objects(self):
        self.seed(batches=4, size=5)
        reader = self.lake.open_reader(lease_ttl_seconds=60)  # 长读者
        pinned_digests = {
            ref.digest for ref in reader.manifest.partitions['2026-10-01'].objects
        }
        self.lake.compact_partition('2026-10-01')
        plan = self.lake.plan_reclaim(retain_manifest_versions=1)
        protected = {digest for digest, _ in plan.protected_objects}
        self.assertTrue(pinned_digests <= protected)
        self.assertEqual(plan.delete_objects, tuple())
        self.lake.reclaim(retain_manifest_versions=1, execute=True)
        # 长读者仍能逐行校验通过。
        self.assertIsInstance(reader.fingerprint(), dict)
        reader.release()
        # 释放后再回收即可删除旧对象。
        next_plan = self.lake.plan_reclaim(retain_manifest_versions=1)
        self.assertTrue(pinned_digests <= {d for d, _ in next_plan.delete_objects})

    def test_release_removes_lease(self):
        self.seed(batches=1, size=2)
        reader = self.lake.open_reader(lease_ttl_seconds=60)
        reader.release()
        with self.assertRaises(LeaseExpired):
            self.lake.get_lease(reader.lease_id)
        # 上下文管理器同样释放。
        with self.lake.open_reader(lease_ttl_seconds=60) as scoped:
            lease_id = scoped.lease_id
        with self.assertRaises(LeaseExpired):
            self.lake.get_lease(lease_id)


class ReclaimAndRecoveryTests(LakeTestBase):
    def test_reclaim_explains_then_deletes_and_keeps_window(self):
        self.seed(batches=5, size=4)
        self.lake.compact_partition('2026-10-01')
        files_before = self.object_count()

        # 保留窗口为 2 代：上一版清单仍引用全部旧对象，因此对象受保护。
        windowed = self.lake.plan_reclaim(retain_manifest_versions=2)
        self.assertTrue(windowed.dry_run)
        for _digest, reason in windowed.delete_objects:
            self.assertIn('清单', reason)
        self.assertEqual(windowed.delete_objects, tuple())
        self.assertEqual(len(windowed.kept_manifests), 2)

        # 保留窗口收缩为 1 代（仅当前版本）：旧对象变为不可达，文件数下降。
        dry = self.lake.plan_reclaim(retain_manifest_versions=1)
        self.assertTrue(dry.dry_run)
        self.assertTrue(dry.delete_objects)
        executed = self.lake.reclaim(retain_manifest_versions=1, execute=True)
        self.assertFalse(executed.dry_run)
        self.assertLess(self.object_count(), files_before)
        # 当前版本仍可读且逐行校验通过。
        reader = self.lake.open_reader(lease_ttl_seconds=60)
        self.assertIsInstance(reader.fingerprint(), dict)
        reader.release()
        # 再执行一次回收应无对象可删（幂等）。
        again = self.lake.reclaim(retain_manifest_versions=1, execute=True)
        self.assertEqual(again.delete_objects, tuple())

    def test_recovery_removes_temporary_leftovers(self):
        self.seed(batches=2, size=3)
        leftovers = [
            self.root / 'objects' / '.writing-abc',
            self.root / 'manifests' / '.manifest-x.json.tmp',
            self.root / '.current.json.tmp',
        ]
        for path in leftovers:
            path.write_bytes(b'partial')
        report = recover(self.root)
        self.assertEqual(len(report['removed_temporary']), 3)
        for path in leftovers:
            self.assertFalse(path.exists())
        # 恢复后读写与回收全部正常。
        self.lake.append_partition('2026-10-02', rows('2026-10-02', 2))
        self.lake.reclaim(execute=True)

    def test_corrupt_pointer_is_detected_not_guessed(self):
        self.seed(batches=2, size=3)
        (self.root / 'current.json').write_text('{"not-current": "x"}', encoding='utf-8')
        with self.assertRaises(CorruptionError):
            self.lake.current_manifest_id
        with self.assertRaises(CorruptionError):
            recover(self.root)

    def test_corrupt_manifest_row_count_detected(self):
        self.seed(batches=2, size=3)
        manifest = self.lake.current_manifest()
        path = self.root / 'manifests' / f'manifest-{manifest.manifest_id}.json'
        payload = manifest.to_json()
        state = payload['partitions']['2026-10-01']
        state['rows'] = state['rows'] + 1
        path.write_text(json.dumps(payload), encoding='utf-8')
        with self.assertRaises(CorruptionError):
            self.lake.current_manifest()

    def test_tampered_object_fails_content_check(self):
        self.seed(batches=2, size=3)
        manifest = self.lake.current_manifest()
        digest = manifest.partitions['2026-10-01'].objects[0].digest
        (self.root / 'objects' / f'{digest}.jsonl').write_bytes(b'{"x":1}\n')
        with self.assertRaises(ValueError):
            self.lake.open_reader(lease_ttl_seconds=60).read_all()

    def test_history_independent_of_directory_listing_order(self):
        self.seed(batches=3, size=2)
        self.lake.compact_partition('2026-10-01')
        current = self.lake.current_manifest()
        # 沿 parent_id 链可以完整重放，版本判定只靠指针与 parent_id。
        chain = []
        cursor = current
        while cursor is not None:
            chain.append(cursor.manifest_id)
            cursor = self.lake.load_manifest(cursor.parent_id) if cursor.parent_id else None
        self.assertEqual(len(chain), 4)  # 三次追加 + 一次压实
        # 即使清单目录里存在名称排序更靠前的历史清单，当前版本仍只由
        # current.json 指针决定，绝不扫描目录取“最新”。
        pointer = json.loads((self.root / 'current.json').read_text(encoding='utf-8'))
        self.assertEqual(pointer, {'current': current.manifest_id})
        oldest = chain[-1]
        self.assertNotEqual(oldest, current.manifest_id)
        # 显式按标识打开旧版本仍然可用。
        old_reader = self.lake.open_reader(manifest_id=oldest, lease_ttl_seconds=60)
        self.assertEqual(old_reader.manifest_id, oldest)
        old_reader.release()

    def test_unknown_partition_read_raises(self):
        self.seed(batches=1, size=2)
        reader = self.lake.open_reader(lease_ttl_seconds=60)
        with self.assertRaises(KeyError):
            reader.read_partition('1999-01-01')

    def test_empty_lake_guard(self):
        with self.assertRaises(LakeError):
            self.lake.open_reader()
        with self.assertRaises(LakeError):
            self.lake.plan_reclaim()


if __name__ == '__main__':
    unittest.main()
