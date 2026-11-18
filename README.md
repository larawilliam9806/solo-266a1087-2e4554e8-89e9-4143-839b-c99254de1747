# Buoy Data Lake Compaction Service

单进程浮标数据湖 API：按 UTC 日分区追加上传 CSV（浮标号、UTC 时刻、盐度），
每次上传落为一个 Parquet 文件；支持按日读取全部记录与当前文件数；支持按日
冲突查询（同一浮标同一 UTC 时刻盐度不一致）；支持按日触发后台压实，把启动
时已有的若干小文件合并为一份，降低下游扫描的小文件开销。

技术栈：FastAPI + PyArrow + SQLite（WAL），单进程设计。

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
# 服务监听 http://localhost:8000 ，交互文档 http://localhost:8000/docs
```

所有状态（Parquet 数据 + SQLite 元数据）都在 `/data` 卷中，容器重启不丢数据。

### 本地运行

```bash
pip install -r requirements.txt
DATA_DIR=./data uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### 运行测试

```bash
pip install -r requirements-dev.txt
python -m pytest tests/ -q
```

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/days/{day}/upload` | 上传 CSV（multipart 文件或 `text/csv` 原始 body），追加为一个 Parquet |
| GET | `/days/{day}/records` | 读取当日全部记录（一致快照），含 `file_count`/`row_count` |
| GET | `/days/{day}/conflicts` | 当日盐度冲突查询（一致快照），含每条读数来源批次 |
| GET | `/days/{day}/files` | 当日活跃文件清单与文件数 |
| POST | `/days/{day}/compactions` | 启动后台压实（202）；当日已有压实进行中返回 409 |
| GET | `/days/{day}/compactions` | 当日压实任务列表 |
| GET | `/days/{day}/compactions/{job_id}` | 查询压实任务状态 |
| GET | `/health` | 健康检查 |

`day` 格式为 `YYYY-MM-DD`（UTC 日期分区）。CSV 列为 `buoy_id, ts_utc, salinity`，
表头可选（无表头时按此顺序解析）；时间戳支持 ISO-8601（`Z` 或时区偏移，
裸时间按 UTC 处理）。

### 示例

```bash
# 上传（multipart 或原始 body 均可）
curl -F "file=@obs.csv" http://localhost:8000/days/2026-11-18/upload
curl -X POST -H "Content-Type: text/csv" --data-binary @obs.csv \
     http://localhost:8000/days/2026-11-18/upload

# 读取全部记录与文件数
curl http://localhost:8000/days/2026-11-18/records

# 盐度冲突查询：同一浮标 + 同一 UTC 时刻出现不同盐度
curl http://localhost:8000/days/2026-11-18/conflicts

# 启动压实并轮询状态
curl -X POST http://localhost:8000/days/2026-11-18/compactions
curl http://localhost:8000/days/2026-11-18/compactions/1
```

## 冲突查询（`GET /days/{day}/conflicts`）

同一 `buoy_id` 在同一 `ts_utc` 上出现**两个及以上不同的有限盐度值**即为冲突。
盐度相同（包括同一行重复、同一值被多次上传）**不算冲突**，只累加到该读数的
`count`，因此不会把重复上传误报成新的冲突。`NaN`/`Inf` 等非有限值不参与冲突
判定。

```jsonc
{
  "day": "2026-11-18",
  "conflict_count": 1,
  "file_count": 3,
  "conflicts": [
    {
      "buoy_id": "B001",
      "ts_utc": "2026-11-18T00:00:00Z",
      "salinity_values": [35.1, 35.9],
      "readings": [
        {
          "salinity": 35.1,
          "count": 3,                       // 同值出现次数（含重复上传）
          "source_file": "part-<uuid>.parquet",  // 主来源批次（=上传响应 file）
          "row_numbers": [1, 2],            // 该批次内的行号（1 起）
          "row_count": 3,                   // 该上传批次的总行数
          "source_known": true,
          "sources": [ /* 该值出现过的每个上传批次 */ ]
        },
        { "salinity": 35.9, "count": 1, /* ... */ }
      ],
      "files": [ /* 本冲突涉及的全部批次文件名与行号 */ ]
    }
  ],
  "readings": [ /* 与 conflicts 相同的信息，按「读数」扁平展开 */ ]
}
```

来源批次标识就是 `POST /upload` 响应中的 `file` 文件名。每条扫描的 `row_count`
是该次原始上传的总行数（不是压实输出的大小）。无法追溯来源的既有压实行返回
`source_file: null` 且 `source_known: false`，**不会**冒用压实输出文件名。

### 来源如何跨越压实保留

上传时在 Parquet 内写入三列逐行来源：`source_file`（批次文件名）、
`source_row`（批次内第几行，1 起）、`source_rows`（该批次总行数）。这些列随
数据一起被压实复制，因此 **退役并删除原始文件后，每行仍指向原上传批次**。

- **既有未压实文件**（无来源列）：按该文件自身文件名归源，行号按文件内顺序
  计算，行数为该文件行数。
- **既有压实文件**（无来源列、`kind=compacted`）：来源标记为未知（`null`），
  因为原始批次信息已不可考，不能拿压实输出名冒充。
- 仅含部分来源列的文件按已有列处理，缺失列按上述回退规则补齐。
- `GET /records` 仍只返回 `buoy_id/ts_utc/salinity` 三列并保留重复行；新旧
  文件混读、以及读与冲突查询混合都可用。
- 压实失败只会把任务标为 `failed`，输入文件保持 active，冲突结果不变。
- 冲突查询与 `records` 使用同一套「快照 + 引用计数」：一次查询只读取切换前
  或切换后的完整文件集合；压实期间追加的数据在后续查询中可见。

## 一致性与可靠性设计

- **上传原子性**：Parquet 先写 `*.tmp-*` 临时文件，`fsync` 后 `rename` 为正式
  文件并 `fsync` 目录，再在 SQLite 登记为 `active`。读者永远看不到半成品文件；
  崩溃最多留下临时碎片，启动恢复时清理。
- **读取一致性**：读取在同一把锁内完成「快照当日 active 文件列表 + 引用计数
  +1」，因此一次读取必然对应完整的切换前或切换后状态，不会出现新旧混杂；
  响应中的 `file_count` 与实际读取的文件集合严格一致。
- **压实切换原子性**：合并输出完整落盘（tmp+fsync+rename）后，「退役输入文件
  + 登记输出文件 + 任务置为 done」在**单个 SQLite 事务**中提交。提交前崩溃
  → 原文件全部仍在，任务重启后续跑；提交后崩溃 → 切换已生效。不存在中间态。
- **压实期间可写可读**：合并只处理任务启动时快照的输入集合；期间追加的文件
  不在集合内，切换时原样保留、全程可读，不会被遗漏或误删。上传、读取均不
  等待压实结束。
- **同日只受理一项压实**：SQLite 部分唯一索引
  `WHERE status='running'` 在数据库层面保证，并发请求第二个返回 409，
  进程重启后约束依然有效。
- **失败保留原数据**：合并或落盘失败 → 任务标记 `failed`（带错误信息），
  输入文件保持 `active`，可修复后重新发起压实。
- **中断恢复**：启动时 `recover()` 先清理退役文件、孤儿 Parquet（未登记的
  上传残留、被取代的压实输出）和 tmp 碎片，再把仍处于 `running` 的任务重新
  投入执行（其输入必然仍全部 active，重跑是安全的）。已提交的上传不受影响。
- **旧文件回收**：退役文件仅在在途读取引用计数归零后才 unlink；回收只针对
  登记表中 `retired` 的文件，`active` 文件（包括压实期间新追加的）永远不会
  被误删。另有后台周期清扫兜底。

## 目录布局

```
data/
  lake_meta.db                 # SQLite 元数据（files 登记表 + compactions 任务表）
  dt=2026-11-18/
    part-<uuid>.parquet        # 上传产生的文件（含逐行来源列）
    compacted-<job>-<id>.parquet  # 压实输出（沿用输入的逐行来源列）
```

每个 Parquet 文件在 `buoy_id, ts_utc, salinity` 之外还带有
`source_file, source_row, source_rows` 三列来源信息；`GET /records` 只投影
前三列，因此对外契约不变。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `DATA_DIR` | `./data`（容器内 `/data`） | 数据与元数据目录 |
| `LAKE_GC_INTERVAL_SEC` | `2` | 退役文件周期清扫间隔 |
| `LAKE_COMPACTION_DELAY_SEC` | `0` | 测试/演示钩子：合并前人为延迟，便于观察压实期间的并发读写 |

> 注意：一致性依赖进程内锁 + SQLite 事务，请以单进程运行（不要多 worker
> 水平扩展）。
