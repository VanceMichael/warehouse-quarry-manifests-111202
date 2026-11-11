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
