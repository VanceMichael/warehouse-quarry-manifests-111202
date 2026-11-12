"""快照目录：内容寻址 JSONL 对象之上的可并发读取文件湖。

设计要点
--------
- 对象只按内容哈希命名并整体写入（复用 :class:`warehouse.store.ObjectStore`），
  物理对象不可变，任何写入都不会覆盖已有对象。
- 每次状态变化生成一个不可变清单（manifest），清单本身也按内容寻址；
  ``current.json`` 仅作为指针，以 ``tmp + rename`` 原子切换，绝不靠目录
  遍历顺序判断最新版本。
- 读者开始时固定一个清单版本，并领取可续期的读取租约；压实只改变物理
  布局，分区内的记录集合（行数与逐行哈希）必须保持不变。
- 压实与普通写入走同一条带分区版本检查的提交路径：两个压实者针对同一
  旧分区只有一个能成功，不同分区互不阻挡（提交仅在指针切换的瞬间持有
  全局提交锁，不按分区串行）。
- 发布前崩溃只留下不可达的对象/清单残留，由 :func:`recover` 与
  :func:`reclaim` 按明确规则清理，不影响当前指针指向的版本。
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable

from .catalog import encode
from .store import ObjectStore

MANIFEST_SCHEMA_VERSION = 1
POINTER_NAME = 'current.json'
MANIFEST_PREFIX = 'manifest-'
MANIFEST_SUFFIX = '.json'
LEASE_SUFFIX = '.lease'
# 写操作的乐观重试上限：仅在提交瞬间发现分区版本已变化时重试。
WRITE_MAX_ATTEMPTS = 10

_PARTITION_NAME = re.compile(r'^[A-Za-z0-9_][A-Za-z0-9_.:-]{0,127}$')
_HEX = frozenset('0123456789abcdef')


class LakeError(RuntimeError):
    """快照湖相关错误的基类。"""


class ConflictError(LakeError):
    """提交时发现所基于的分区版本已经被别人推进。"""


class CorruptionError(LakeError):
    """指针、清单或租约文件损坏。"""


class LeaseExpired(LakeError):
    """读取租约已过期且无法继续访问。"""


def _validate_partition(partition: str) -> None:
    if not isinstance(partition, str) or not _PARTITION_NAME.match(partition):
        raise ValueError(f'分区名称不合法: {partition!r}')


def _canonical_row_hash(rows: list) -> str:
    """逐行规范哈希：与文件内顺序无关地标识记录多重集合。"""
    digest = hashlib.sha256()
    for row in rows:
        digest.update(hashlib.sha256(encode(row)).digest())
    return digest.hexdigest()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_write(path: Path, payload: bytes) -> None:
    """tmp 文件 + 同目录原子改名 + 目录 fsync。"""
    descriptor, temporary = tempfile.mkstemp(
        dir=path.parent, prefix='.' + path.name + '.', suffix='.tmp'
    )
    try:
        with os.fdopen(descriptor, 'wb') as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        # 成功时临时名已经不存在；失败路径负责清残留。
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


@dataclass(frozen=True)
class ObjectRef:
    """清单中对一个不可变 JSONL 对象的引用。"""

    digest: str
    rows: int

    def to_json(self) -> dict:
        return {'digest': self.digest, 'rows': self.rows}

    @classmethod
    def from_json(cls, payload: dict) -> 'ObjectRef':
        digest = payload['digest']
        rows = payload['rows']
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(character not in _HEX for character in digest)
            or not isinstance(rows, int)
            or rows < 0
        ):
            raise CorruptionError('清单中的对象引用不合法')
        return cls(digest=digest, rows=rows)


@dataclass(frozen=True)
class PartitionState:
    """单个分区在某个清单版本中的完整状态。"""

    version: int
    objects: tuple[ObjectRef, ...]
    rows: int
    # 逐行哈希，保证压实前后记录多重集合完全一致。
    row_hash: str

    def to_json(self) -> dict:
        return {
            'version': self.version,
            'objects': [reference.to_json() for reference in self.objects],
            'rows': self.rows,
            'row_hash': self.row_hash,
        }

    @classmethod
    def from_json(cls, payload: dict) -> 'PartitionState':
        objects = tuple(ObjectRef.from_json(item) for item in payload['objects'])
        rows = payload['rows']
        if not isinstance(rows, int) or rows < 0 or sum(item.rows for item in objects) != rows:
            raise CorruptionError('分区行数与对象行数不一致')
        row_hash = payload['row_hash']
        if (
            not isinstance(row_hash, str)
            or len(row_hash) != 64
            or any(character not in _HEX for character in row_hash)
        ):
            raise CorruptionError('分区逐行哈希不合法')
        version = payload['version']
        if not isinstance(version, int) or version < 1:
            raise CorruptionError('分区版本号不合法')
        return cls(version=version, objects=objects, rows=rows, row_hash=row_hash)


@dataclass(frozen=True)
class Manifest:
    """不可变快照清单：保存分区、对象及行数。"""

    manifest_id: str
    parent_id: str | None
    created_at: str
    partitions: dict[str, PartitionState]
    total_rows: int
    schema_version: int = MANIFEST_SCHEMA_VERSION

    def to_json(self) -> dict:
        return {
            'schema_version': self.schema_version,
            'manifest_id': self.manifest_id,
            'parent_id': self.parent_id,
            'created_at': self.created_at,
            'total_rows': self.total_rows,
            'partitions': {
                partition: state.to_json() for partition, state in sorted(self.partitions.items())
            },
        }

    @classmethod
    def from_json(cls, payload: dict) -> 'Manifest':
        if payload.get('schema_version') != MANIFEST_SCHEMA_VERSION:
            raise CorruptionError('清单 schema 版本不受支持')
        manifest_id = payload['manifest_id']
        parent_id = payload['parent_id']
        created_at = payload['created_at']
        partitions = {
            name: PartitionState.from_json(state)
            for name, state in payload['partitions'].items()
        }
        total_rows = payload['total_rows']
        if not isinstance(total_rows, int) or total_rows < 0:
            raise CorruptionError('清单总行数不合法')
        if total_rows != sum(state.rows for state in partitions.values()):
            raise CorruptionError('清单总行数与分区行数不一致')
        return cls(
            manifest_id=manifest_id,
            parent_id=parent_id,
            created_at=created_at,
            partitions=partitions,
            total_rows=total_rows,
        )


@dataclass(frozen=True)
class Lease:
    """读取租约：固定清单并在有效期内保护对应对象不被回收。"""

    lease_id: str
    manifest_id: str
    issued_at: float
    expires_at: float

    @property
    def ttl_seconds(self) -> float:
        return self.expires_at - self.issued_at

    def to_json(self) -> dict:
        return {
            'lease_id': self.lease_id,
            'manifest_id': self.manifest_id,
            'issued_at': self.issued_at,
            'expires_at': self.expires_at,
        }

    @classmethod
    def from_json(cls, payload: dict) -> 'Lease':
        lease = cls(
            lease_id=payload['lease_id'],
            manifest_id=payload['manifest_id'],
            issued_at=float(payload['issued_at']),
            expires_at=float(payload['expires_at']),
        )
        if lease.expires_at <= lease.issued_at:
            raise CorruptionError('租约有效期不合法')
        return lease


@dataclass(frozen=True)
class ReclaimPlan:
    """回收计划：先解释再执行，所有删除都带有原因。"""

    delete_objects: tuple[tuple[str, str], ...]
    delete_manifests: tuple[tuple[str, str], ...]
    delete_leases: tuple[tuple[str, str], ...]
    protected_objects: tuple[tuple[str, str], ...]
    kept_manifests: tuple[str, ...]
    reclaimed_temporary: tuple[str, ...]
    dry_run: bool

    @property
    def deletion_count(self) -> int:
        return (
            len(self.delete_objects)
            + len(self.delete_manifests)
            + len(self.delete_leases)
            + len(self.reclaimed_temporary)
        )

    def explain_lines(self) -> list[str]:
        lines = []
        lines.append('受保护对象（不会删除）：')
        for digest, reason in self.protected_objects:
            lines.append(f'  保留 {digest[:12]}…  {reason}')
        lines.append('计划删除的对象：')
        for digest, reason in self.delete_objects:
            lines.append(f'  删除 {digest[:12]}…  {reason}')
        lines.append('计划删除的历史清单：')
        for manifest_id, reason in self.delete_manifests:
            lines.append(f'  删除 {manifest_id}  {reason}')
        lines.append('计划删除的失效租约：')
        for lease_id, reason in self.delete_leases:
            lines.append(f'  删除 {lease_id}  {reason}')
        if self.reclaimed_temporary:
            lines.append('崩溃残留临时文件：')
            for name in self.reclaimed_temporary:
                lines.append(f'  删除 {name}  发布中断残留')
        lines.append(
            f'合计：删除对象 {len(self.delete_objects)} 个、清单 '
            f'{len(self.delete_manifests)} 个、租约 {len(self.delete_leases)} 个、'
            f'临时文件 {len(self.reclaimed_temporary)} 个。'
        )
        return lines


class SnapshotLake:
    """本地快照文件湖。

    目录布局::

        root/
          objects/            内容寻址不可变 JSONL 对象（ObjectStore）
          manifests/          不可变清单 manifest-<uuid>.json
          current.json        当前清单指针（原子切换）
          leases/             读取租约 <uuid>.lease
          locks/              提交锁与分区锁文件
    """

    def __init__(self, root, clock: Callable[[], float] | None = None):
        self.root = Path(root)
        self.objects_dir = self.root / 'objects'
        self.manifests_dir = self.root / 'manifests'
        self.leases_dir = self.root / 'leases'
        self.locks_dir = self.root / 'locks'
        self.store = ObjectStore(self.objects_dir)
        self.pointer_path = self.root / POINTER_NAME
        self._clock = clock or time.time
        # 可重入锁状态：同一线程嵌套获取只计数；跨进程靠 flock 互斥。
        self._lock_thread = threading.local()
        self._lock_guards: dict[str, threading.RLock] = {}
        self._lock_guards_mu = threading.Lock()
        for directory in (self.root, self.objects_dir, self.manifests_dir,
                          self.leases_dir, self.locks_dir):
            directory.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # 锁与原子文件
    # ------------------------------------------------------------------
    @contextmanager
    def _locked(self, name: str):
        """可重入的跨进程排他锁。

        同进程内用 :class:`threading.RLock` 保证线程互斥且支持嵌套
        （例如发布钩子里再执行写入）；不同进程之间用 ``flock`` 互斥，
        因此多个压实者/写入者跨进程时仍只有一个能完成提交。
        """
        with self._lock_guards_mu:
            guard = self._lock_guards.setdefault(name, threading.RLock())
        held = getattr(self._lock_thread, 'held', None)
        if held is None:
            held = self._lock_thread.held = {}
        guard.acquire()
        reentrant = name in held
        if not reentrant:
            handle = open(self.locks_dir / name, 'a+b')
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            held[name] = handle
        try:
            yield
        finally:
            if not reentrant:
                handle = held.pop(name)
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                handle.close()
            guard.release()

    # ------------------------------------------------------------------
    # 清单与指针
    # ------------------------------------------------------------------
    @property
    def current_manifest_id(self) -> str | None:
        if not self.pointer_path.exists():
            return None
        payload = json.loads(self.pointer_path.read_text(encoding='utf-8'))
        manifest_id = payload.get('current')
        if not isinstance(manifest_id, str):
            raise CorruptionError('当前指针内容不合法')
        return manifest_id

    def current_manifest(self) -> Manifest | None:
        manifest_id = self.current_manifest_id
        if manifest_id is None:
            return None
        return self.load_manifest(manifest_id)

    def load_manifest(self, manifest_id: str) -> Manifest:
        path = self.manifests_dir / f'{MANIFEST_PREFIX}{manifest_id}{MANIFEST_SUFFIX}'
        if not path.exists():
            raise CorruptionError(f'清单不存在: {manifest_id}')
        return Manifest.from_json(json.loads(path.read_text(encoding='utf-8')))

    def _write_manifest(self, manifest: Manifest) -> None:
        path = self.manifests_dir / (
            f'{MANIFEST_PREFIX}{manifest.manifest_id}{MANIFEST_SUFFIX}'
        )
        if path.exists():
            # 清单按内容/UUID 唯一，重名只可能来自真正的冲突。
            raise CorruptionError(f'清单标识冲突: {manifest.manifest_id}')
        _atomic_write(
            path,
            json.dumps(
                manifest.to_json(), ensure_ascii=False, sort_keys=True,
                separators=(',', ':'), allow_nan=False,
            ).encode('utf-8'),
        )

    def _publish_manifest(
        self,
        partitions: dict[str, PartitionState],
        parent_id: str | None,
        expected_base: dict[str, int],
        on_before_pointer_swap: Callable[[Manifest], None] | None = None,
    ) -> Manifest:
        """在提交锁内做分区版本检查、三方合并、写清单并原子切换指针。

        - ``expected_base`` 声明本次变更所基于的分区版本；这些分区若已被
          其他写入者推进，则提交失败（同分区竞争只有一个赢家）。
        - 本次未触碰的分区不参与冲突判定，而是与当前清单做三方合并，因此
          不同分区的并发写入互不阻挡，也不会互相覆盖。
        """
        with self._locked('commit.lock'):
            current = self.current_manifest()
            current_versions = {
                name: state.version
                for name, state in (current.partitions.items() if current else ())
            }
            for partition, base_version in expected_base.items():
                if current_versions.get(partition, 0) != base_version:
                    raise ConflictError(
                        f'分区 {partition} 的基础版本 {base_version} 已过期，'
                        f'当前版本 {current_versions.get(partition, 0)}'
                    )
            # 三方合并：以当前清单为基底，只让本次目标分区覆盖；调用方
            # 携带的非目标分区状态一律不采纳，避免并发提交互相回滚。
            merged: dict[str, PartitionState] = {}
            if current is not None:
                merged.update(current.partitions)
            for partition in expected_base:
                if partition not in partitions:
                    raise LakeError(f'提交缺少目标分区状态: {partition}')
                merged[partition] = partitions[partition]
            manifest = Manifest(
                manifest_id=uuid.uuid4().hex,
                parent_id=current.manifest_id if current else parent_id,
                created_at=str(self._clock()),
                partitions=merged,
                total_rows=sum(state.rows for state in merged.values()),
            )
            # 清单先落盘（不可变、原子可见），再切换指针。
            self._write_manifest(manifest)
            if on_before_pointer_swap is not None:
                # 钩子内可能发生可重入写入并推进指针；返回后必须重新校验，
                # 绝不能用基于旧版本的新清单覆盖刚刚的提交。
                on_before_pointer_swap(manifest)
                latest = self.current_manifest()
                latest_versions = {
                    name: state.version
                    for name, state in (latest.partitions.items() if latest else ())
                }
                for partition, base_version in expected_base.items():
                    if latest_versions.get(partition, 0) != base_version:
                        raise ConflictError(
                            f'分区 {partition} 在发布钩子中被推进，本次压实放弃发布'
                        )
            _atomic_write(
                self.pointer_path,
                json.dumps(
                    {'current': manifest.manifest_id}, ensure_ascii=False,
                    sort_keys=True, separators=(',', ':'), allow_nan=False,
                ).encode('utf-8'),
            )
        # 重新从指针加载权威清单：并发合并后，本地构造对象可能不包含
        # 提交瞬间从当前清单并入的其他分区最新状态。
        return self.load_manifest(manifest.manifest_id)

    # ------------------------------------------------------------------
    # 写入：追加新分区文件（对象永不覆盖）
    # ------------------------------------------------------------------
    def append_partition(
        self,
        partition: str,
        rows: list,
        on_before_pointer_swap: Callable[[Manifest], None] | None = None,
    ) -> Manifest:
        """向分区追加一批记录，返回发布后的清单。"""
        _validate_partition(partition)
        if not isinstance(rows, list) or not rows:
            raise ValueError('追加的记录必须是非空数组')
        digest = self.store.put(rows)  # 内容寻址：已存在则直接复用，绝不覆盖
        new_ref = ObjectRef(digest=digest, rows=len(rows))
        for _ in range(WRITE_MAX_ATTEMPTS):
            current = self.current_manifest()
            previous = current.partitions.get(partition) if current else None
            base_version = previous.version if previous else 0
            references = list(previous.objects) if previous else []
            # 同一对象被重复投递时不产生重复引用。
            if not any(existing.digest == digest for existing in references):
                references.append(new_ref)
            state = PartitionState(
                version=base_version + 1,
                objects=tuple(references),
                rows=(previous.rows if previous else 0) + len(rows),
                row_hash=_canonical_row_hash(
                    (self._read_state_rows(previous) if previous else []) + rows
                ),
            )
            partitions = dict(current.partitions) if current else {}
            partitions[partition] = state
            try:
                return self._publish_manifest(
                    partitions,
                    current.manifest_id if current else None,
                    {partition: base_version},
                    on_before_pointer_swap,
                )
            except ConflictError:
                continue
        raise ConflictError(f'分区 {partition} 写入重试超过上限')

    # ------------------------------------------------------------------
    # 压实：只改物理布局，不改记录集合
    # ------------------------------------------------------------------
    def compact_partition(
        self,
        partition: str,
        on_objects_written: Callable[[], None] | None = None,
        on_before_pointer_swap: Callable[[Manifest], None] | None = None,
    ) -> Manifest:
        """把一个分区的多个小对象合并为一个替代对象并发布新版本。

        后台先生成替代文件，发布时检查所基于的分区版本；记录集合通过
        行数与逐行哈希双重校验保持不变。
        """
        _validate_partition(partition)
        current = self.current_manifest()
        if current is None or partition not in current.partitions:
            raise LakeError(f'分区不存在或尚无数据: {partition}')
        previous = current.partitions[partition]
        if len(previous.objects) <= 1:
            # 已是单对象布局：无需压实，原样返回当前清单。
            return current

        rows = self._read_state_rows(previous)
        if len(rows) != previous.rows:
            raise CorruptionError(
                f'分区 {partition} 读取行数 {len(rows)} 与清单 {previous.rows} 不一致'
            )
        if _canonical_row_hash(rows) != previous.row_hash:
            raise CorruptionError(f'分区 {partition} 压实前逐行哈希校验失败')

        # 替代对象在后台生成；内容寻址保证不会覆盖写入期间产生的新对象，
        # 若合并结果与既有对象内容相同则直接复用该物理文件。
        digest = self.store.put(rows)
        if on_objects_written is not None:
            on_objects_written()
        new_refs = (ObjectRef(digest=digest, rows=previous.rows),)

        state = PartitionState(
            version=previous.version + 1,
            objects=new_refs,
            rows=previous.rows,
            row_hash=previous.row_hash,  # 记录集合不变，哈希必须不变
        )
        partitions = dict(current.partitions)
        partitions[partition] = state
        return self._publish_manifest(
            partitions,
            current.manifest_id,
            {partition: previous.version},
            on_before_pointer_swap,
        )

    def compact_partition_with_retry(self, partition: str, max_attempts: int = 3, **hooks):
        """压实的便捷封装：基础版本过期时重读再试，带短退避避免空转。"""
        if max_attempts < 1:
            raise ValueError('max_attempts 至少为 1')
        last_error: Exception | None = None
        for attempt in range(max_attempts):
            try:
                return self.compact_partition(partition, **hooks)
            except ConflictError as error:
                last_error = error
                if attempt + 1 < max_attempts:
                    time.sleep(0.02 * (attempt + 1))
        raise ConflictError(f'分区 {partition} 压实在 {max_attempts} 次尝试后仍失败') from last_error

    def _read_state_rows(self, state: PartitionState) -> list:
        rows: list = []
        for reference in state.objects:
            rows.extend(self.store.read(reference.digest))
        return rows

    # ------------------------------------------------------------------
    # 读取：固定快照 + 可续期租约
    # ------------------------------------------------------------------
    def open_reader(
        self,
        manifest_id: str | None = None,
        lease_ttl_seconds: float = 30.0,
    ) -> 'LakeReader':
        if lease_ttl_seconds <= 0:
            raise ValueError('租约有效期必须为正数')
        if manifest_id is None:
            manifest = self.current_manifest()
            if manifest is None:
                raise LakeError('快照湖为空，尚无清单')
        else:
            manifest = self.load_manifest(manifest_id)
        lease = self._issue_lease(manifest.manifest_id, lease_ttl_seconds)
        return LakeReader(self, manifest, lease)

    def _lease_path(self, lease_id: str) -> Path:
        return self.leases_dir / f'{lease_id}{LEASE_SUFFIX}'

    def _issue_lease(self, manifest_id: str, ttl_seconds: float) -> Lease:
        now = self._clock()
        lease = Lease(
            lease_id=uuid.uuid4().hex,
            manifest_id=manifest_id,
            issued_at=now,
            expires_at=now + ttl_seconds,
        )
        _atomic_write(
            self._lease_path(lease.lease_id),
            json.dumps(
                lease.to_json(), ensure_ascii=False, sort_keys=True,
                separators=(',', ':'), allow_nan=False,
            ).encode('utf-8'),
        )
        return lease

    def get_lease(self, lease_id: str) -> Lease:
        path = self._lease_path(lease_id)
        if not path.exists():
            raise LeaseExpired(f'租约不存在或已释放: {lease_id}')
        lease = Lease.from_json(json.loads(path.read_text(encoding='utf-8')))
        if lease.expires_at <= self._clock():
            raise LeaseExpired(f'租约已过期: {lease_id}')
        return lease

    def renew_lease(self, lease_id: str, ttl_seconds: float | None = None) -> Lease:
        existing = self.get_lease(lease_id)  # 过期租约不能续期
        ttl = ttl_seconds if ttl_seconds is not None else existing.ttl_seconds
        if ttl <= 0:
            raise ValueError('租约有效期必须为正数')
        now = self._clock()
        renewed = Lease(
            lease_id=existing.lease_id,
            manifest_id=existing.manifest_id,
            issued_at=existing.issued_at,
            expires_at=now + ttl,
        )
        _atomic_write(self._lease_path(lease_id), json.dumps(
            renewed.to_json(), ensure_ascii=False, sort_keys=True,
            separators=(',', ':'), allow_nan=False,
        ).encode('utf-8'))
        return renewed

    def release_lease(self, lease_id: str) -> None:
        try:
            self._lease_path(lease_id).unlink()
        except FileNotFoundError:
            pass

    def list_leases(self, now: float | None = None) -> list[Lease]:
        """列出当前仍有效的租约；损坏的租约文件直接报错而不是静默忽略。"""
        moment = now if now is not None else self._clock()
        leases: list[Lease] = []
        for path in sorted(self.leases_dir.glob(f'*{LEASE_SUFFIX}')):
            lease = Lease.from_json(json.loads(path.read_text(encoding='utf-8')))
            if lease.expires_at > moment:
                leases.append(lease)
        return leases

    # ------------------------------------------------------------------
    # 回收：先解释后执行
    # ------------------------------------------------------------------
    def plan_reclaim(
        self,
        retain_manifest_versions: int = 2,
        now: float | None = None,
    ) -> ReclaimPlan:
        """生成回收计划。

        保护集合：当前版本、保留窗口内的历史版本、有效租约引用的清单/
        对象。失效租约不进入保护集合，不会永久阻塞清理。
        """
        if retain_manifest_versions < 1:
            raise ValueError('至少保留当前一个清单版本')
        moment = now if now is not None else self._clock()
        current = self.current_manifest()
        if current is None:
            raise LakeError('快照湖尚未初始化，没有可回收对象')

        # 1. 沿父链收集保留窗口内的清单；历史清单可能已被回收，链断裂即停止。
        chain: list[Manifest] = []
        cursor: Manifest | None = current
        seen: set[str] = set()
        while cursor is not None and cursor.manifest_id not in seen and len(chain) < retain_manifest_versions:
            seen.add(cursor.manifest_id)
            chain.append(cursor)
            if not cursor.parent_id:
                cursor = None
                continue
            try:
                cursor = self.load_manifest(cursor.parent_id)
            except CorruptionError:
                cursor = None
        kept_ids = {manifest.manifest_id for manifest in chain}

        # 2. 有效租约额外保护其引用的清单（即使超出保留窗口）。
        valid_leases = self.list_leases(moment)
        lease_manifests: list[Manifest] = []
        lease_reasons: dict[str, str] = {}
        for lease in valid_leases:
            try:
                referenced = self.load_manifest(lease.manifest_id)
            except CorruptionError:
                continue
            if referenced.manifest_id not in kept_ids:
                lease_manifests.append(referenced)
            lease_reasons[referenced.manifest_id] = (
                f'有效租约 {lease.lease_id[:8]}… 引用'
            )

        protected: dict[str, str] = {}
        for index, manifest in enumerate(chain):
            reason = '当前版本引用' if index == 0 else f'保留窗口内版本（第 {index + 1} 代）引用'
            for state in manifest.partitions.values():
                for reference in state.objects:
                    protected.setdefault(reference.digest, reason)
        for manifest in lease_manifests:
            for state in manifest.partitions.values():
                for reference in state.objects:
                    protected.setdefault(
                        reference.digest,
                        f'有效租约 {_lease_prefix(valid_leases, manifest.manifest_id)} 引用',
                    )

        # 3. 物理对象与受保护集合对账。
        delete_objects: list[tuple[str, str]] = []
        for path in sorted(self.objects_dir.glob('*.jsonl')):
            digest = path.name[: -len('.jsonl')]
            if digest not in protected:
                delete_objects.append((digest, '不在当前版本、保留窗口或有效租约的任何清单中'))

        # 4. 清单对账：保留窗口 + 有效租约引用之外的历史清单可删。
        all_manifest_ids = {
            path.name[len(MANIFEST_PREFIX): -len(MANIFEST_SUFFIX)]
            for path in self.manifests_dir.glob(f'{MANIFEST_PREFIX}*{MANIFEST_SUFFIX}')
        }
        delete_manifests = [
            (manifest_id, '超出保留窗口且无有效租约引用')
            for manifest_id in sorted(all_manifest_ids - kept_ids - {m.manifest_id for m in lease_manifests})
        ]

        # 5. 租约对账：过期或无法识别的租约文件删除。
        delete_leases: list[tuple[str, str]] = []
        valid_ids = {lease.lease_id for lease in valid_leases}
        for path in sorted(self.leases_dir.glob(f'*{LEASE_SUFFIX}')):
            lease_id = path.name[: -len(LEASE_SUFFIX)]
            if lease_id in valid_ids:
                continue
            try:
                lease = Lease.from_json(json.loads(path.read_text(encoding='utf-8')))
                reason = (
                    f'已于 {time.strftime("%H:%M:%S", time.localtime(lease.expires_at))} 过期'
                )
            except (CorruptionError, KeyError, ValueError, json.JSONDecodeError):
                reason = '租约文件损坏'
            delete_leases.append((lease_id, reason))

        temporary = tuple(sorted(self._temporary_leftovers()))
        return ReclaimPlan(
            delete_objects=tuple(delete_objects),
            delete_manifests=tuple(delete_manifests),
            delete_leases=tuple(delete_leases),
            protected_objects=tuple(sorted(protected.items())),
            kept_manifests=tuple(manifest.manifest_id for manifest in chain),
            reclaimed_temporary=temporary,
            dry_run=True,
        )

    def reclaim(
        self,
        retain_manifest_versions: int = 2,
        execute: bool = False,
        now: float | None = None,
    ) -> ReclaimPlan:
        dry = self.plan_reclaim(retain_manifest_versions, now)
        if not execute:
            return dry
        for digest, _reason in dry.delete_objects:
            (self.objects_dir / f'{digest}.jsonl').unlink(missing_ok=True)
        for manifest_id, _reason in dry.delete_manifests:
            (
                self.manifests_dir
                / f'{MANIFEST_PREFIX}{manifest_id}{MANIFEST_SUFFIX}'
            ).unlink(missing_ok=True)
        for lease_id, _reason in dry.delete_leases:
            self._lease_path(lease_id).unlink(missing_ok=True)
        for name in dry.reclaimed_temporary:
            for directory in (self.objects_dir, self.manifests_dir, self.root):
                candidate = directory / name
                if candidate.exists():
                    candidate.unlink()
        _fsync_directory(self.objects_dir)
        _fsync_directory(self.manifests_dir)
        _fsync_directory(self.leases_dir)
        return ReclaimPlan(
            delete_objects=dry.delete_objects,
            delete_manifests=dry.delete_manifests,
            delete_leases=dry.delete_leases,
            protected_objects=dry.protected_objects,
            kept_manifests=dry.kept_manifests,
            reclaimed_temporary=dry.reclaimed_temporary,
            dry_run=False,
        )

    def _temporary_leftovers(self) -> set[str]:
        leftovers: set[str] = set()
        for directory in (self.objects_dir, self.manifests_dir, self.root):
            for path in directory.iterdir():
                if path.name.startswith('.') and path.name.endswith('.tmp'):
                    leftovers.add(path.name)
                elif path.name.startswith('.writing-'):
                    leftovers.add(path.name)
        return leftovers


def _lease_prefix(leases: Iterable[Lease], manifest_id: str) -> str:
    for lease in leases:
        if lease.manifest_id == manifest_id:
            return lease.lease_id[:8] + '…'
    return '未知'


class LakeReader:
    """固定快照版本的并发读者；对象读取时重新做内容校验。"""

    def __init__(self, lake: SnapshotLake, manifest: Manifest, lease: Lease):
        self._lake = lake
        self.manifest = manifest
        self.lease = lease

    @property
    def manifest_id(self) -> str:
        return self.manifest.manifest_id

    @property
    def lease_id(self) -> str:
        return self.lease.lease_id

    def check_lease(self) -> Lease:
        self.lease = self._lake.get_lease(self.lease.lease_id)
        return self.lease

    def renew(self, ttl_seconds: float | None = None) -> Lease:
        self.lease = self._lake.renew_lease(self.lease.lease_id, ttl_seconds)
        return self.lease

    def release(self) -> None:
        self._lake.release_lease(self.lease.lease_id)

    def __enter__(self) -> 'LakeReader':
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.release()

    def read_partition(self, partition: str) -> list:
        self.check_lease()
        if partition not in self.manifest.partitions:
            raise KeyError(f'快照 {self.manifest_id} 中不存在分区 {partition}')
        return self._lake._read_state_rows(self.manifest.partitions[partition])

    def read_all(self) -> dict[str, list]:
        self.check_lease()
        return {
            partition: self._lake._read_state_rows(state)
            for partition, state in sorted(self.manifest.partitions.items())
        }

    def fingerprint(self) -> dict:
        """逐行校验指纹：行数 + 每行规范 JSON 的哈希多重集合。"""
        self.check_lease()
        digest = hashlib.sha256()
        rows = 0
        per_partition: dict[str, dict] = {}
        for partition, state in sorted(self.manifest.partitions.items()):
            part_rows = self._lake._read_state_rows(state)
            if len(part_rows) != state.rows:
                raise CorruptionError(f'分区 {partition} 行数与清单不符')
            if _canonical_row_hash(part_rows) != state.row_hash:
                raise CorruptionError(f'分区 {partition} 逐行哈希与清单不符')
            part_digest = hashlib.sha256()
            for row in part_rows:
                line_hash = hashlib.sha256(encode(row)).hexdigest()
                digest.update(line_hash.encode('ascii'))
                part_digest.update(line_hash.encode('ascii'))
            per_partition[partition] = {
                'rows': state.rows,
                'objects': len(state.objects),
                'row_hash': state.row_hash,
                'fingerprint': part_digest.hexdigest(),
            }
            rows += state.rows
        return {
            'manifest_id': self.manifest_id,
            'total_rows': rows,
            'fingerprint': digest.hexdigest(),
            'partitions': per_partition,
        }


def recover(root) -> dict:
    """崩溃恢复：清理发布残留并校验当前指针完整性。

    - 删除对象目录、清单目录、根目录下的 tmp/writing 残留（这些文件永远
      不会被指针引用）。
    - 校验当前指针指向的清单存在且可解析；指针本身损坏时报错，由人工
      从 ``manifests/`` 中选择已知良好版本重新写指针，不依赖目录顺序。
    """
    lake = SnapshotLake(root)
    removed: list[str] = []
    for directory in (lake.objects_dir, lake.manifests_dir, lake.root):
        for path in sorted(directory.iterdir()):
            if (path.name.startswith('.') and path.name.endswith('.tmp')) or path.name.startswith('.writing-'):
                path.unlink()
                removed.append(str(path.relative_to(lake.root)))
    _fsync_directory(lake.objects_dir)
    _fsync_directory(lake.manifests_dir)
    _fsync_directory(lake.root)

    report = {'removed_temporary': removed, 'current_manifest': None, 'orphan_manifests': []}
    current_id = lake.current_manifest_id
    if current_id is not None:
        manifest = lake.load_manifest(current_id)  # 损坏时抛 CorruptionError
        # 校验当前清单引用的每个对象都存在且内容哈希正确。
        for state in manifest.partitions.values():
            for reference in state.objects:
                lake.store.read(reference.digest)
        report['current_manifest'] = current_id
    all_ids = {
        path.name[len(MANIFEST_PREFIX): -len(MANIFEST_SUFFIX)]
        for path in lake.manifests_dir.glob(f'{MANIFEST_PREFIX}*{MANIFEST_SUFFIX}')
    }
    if current_id is not None:
        report['orphan_manifests'] = sorted(all_ids - {current_id})
    return report
