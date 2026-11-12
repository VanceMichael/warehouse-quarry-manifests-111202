# quarry-manifests

按日维护影像索引行数和体积，支持校验读取不可变 JSONL 对象。

## 当前数据组织

- `scenes`：`scene TEXT PRIMARY KEY, partition_day TEXT NOT NULL, tile TEXT NOT NULL, bytes INTEGER NOT NULL`

`examples/source.json` 为本地合成输入，`examples/expected.json` 是当前查询的公开样例结果。`schema.json` 描述结构，运行时定义在 `warehouse/definition.py`。业务计算位于 `warehouse/domain.py`，事务装载位于 `warehouse/catalog.py`，内容寻址 JSONL 对象位于 `warehouse/store.py`。

输入批次以内容 SHA-256 固定；相同批次重投不重复写入，相同批次内容不同则报错。装载以完整输入包为事务，主键冲突不留下半批记录。关系数据保存在 SQLite，精确数量用十进制文本；导出对象按内容哈希命名，读取会重新校验，来源不在运行时修改。

## 环境与验证

Python 3.11 以上，仅使用标准库，无第三方安装。`runtime.txt` 记录验证解释器版本。时区相关开发使用系统 IANA 时区库。数据库和对象目录均由入口参数指定。源码和合成数据可以直接复制到 Linux 环境运行。

```bash
python3 -m compileall -q warehouse tests scripts
python3 -m unittest discover -s tests -v
```

## 运行

每次使用全新临时目录执行真实导入、查询、导出和内容校验：

```bash
python3 scripts/demo.py
```

分步调用可用：

```bash
python3 -m warehouse import --db /tmp/quarry-manifests.sqlite3 --input examples/source.json --batch first
python3 -m warehouse inspect --db /tmp/quarry-manifests.sqlite3
python3 -m warehouse export --db /tmp/quarry-manifests.sqlite3 --objects /tmp/quarry-manifests-objects
```

`scripts/demo.py` 打印实际命令和完整输出，验证首次写入、重复导入、查询和导出往返。小规模样例在数秒内结束，失败返回非零退出码，不回放预制日志。手工重复使用数据库路径时应留意已存在的业务主键。

## 快照文件湖（压实、快照与租约）

在内容寻址 JSONL 对象之上，`warehouse/lake.py` 提供可并发读取的快照目录，解决“查询一次要打开几千个小文件”以及手工拼文件读到新旧混合结果的问题。

### 不变量与目录布局

- 对象只按内容 SHA-256 命名、整体原子写入并在读取时重新校验，**物理对象不可变**；任何写入都不会覆盖已有对象，写入期间新产生的分区文件因此不可能被压实覆盖。
- 每次状态变化生成一个不可变清单 `manifests/manifest-<uuid>.json`，清单记录每个分区的版本、对象引用、行数与逐行哈希；`current.json` 是唯一的当前版本指针，以 `tmp + rename` 原子切换。版本判定只沿指针与 `parent_id` 链进行，**不依赖目录遍历顺序**。
- 压实只改变物理布局：分区合并为单个替代对象，行数与逐行哈希必须保持不变，否则拒绝发布。
- 读者开始时固定一个清单版本并领取可续期读取租约（`leases/*.lease`）；租约有效期内其引用的对象受保护。

```
root/
  objects/     内容寻址不可变 JSONL 对象
  manifests/   不可变清单
  current.json 当前清单指针（原子切换）
  leases/      读取租约
  locks/       可重入跨进程提交锁
```

### 并发、崩溃与回收语义

- 压实者在后台生成替代文件，发布时在提交锁内检查所基于的分区版本并做三方合并：同一旧分区的两个压实者只有一个成功（另一个得到 `ConflictError`），不同分区的并发写入互不阻挡也不互相覆盖。
- 发布前崩溃只留下不可达的对象/清单与临时残留，指针保持旧版本；`recover` 清理残留并校验当前指针完整性，损坏指针直接报错而不是猜测最新版。
- 回收（`reclaim`）先返回带原因的 `ReclaimPlan`（dry-run 解释）再执行：当前版本、保留窗口内版本、有效租约引用的对象都不会被删；失效租约不进入保护集合，不会永久阻塞清理；回收可重复执行且幂等。

### 快照湖验证与演示

进程内单元测试覆盖假时钟租约到期、线程并发、崩溃钩子与损坏检测；进程级测试用真实子进程和文件屏障复现长读者、并发压实、发布前崩溃与租约到期。

```bash
python3 -m unittest tests.test_lake -v            # 进程内：24 个用例
python3 -m unittest tests.test_lake_processes -v  # 真实子进程交错：4 个场景
python3 -I scripts/lake_demo.py                   # 端到端演示入口
```

`scripts/lake_demo.py` 使用合成影像索引（不接影像服务），可直接看到物理对象文件数量显著下降（例如 24 → 3）且基线记录逐行不变，失败以非零退出码结束。

分步 CLI：

```bash
python3 -m warehouse lake-append  --lake /tmp/lake --partition 2026-10-01 --rows 25
python3 -m warehouse lake-compact --lake /tmp/lake --partition 2026-10-01
python3 -m warehouse lake-read    --lake /tmp/lake --ttl 30
python3 -m warehouse lake-reclaim --lake /tmp/lake --retain 1   # 先解释再执行
python3 -m warehouse lake-recover --lake /tmp/lake
python3 -m warehouse lake-status  --lake /tmp/lake
```
