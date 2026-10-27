# Quant-Demo 接口设计文档

单文件实现 [`hft_binance_demo.py`](hft_binance_demo.py)(约 800 行),无服务进程、无数据库、无配置服务。
本文档描述**所有对外可见的契约**:进程边界、上游行情、下游落盘、日志观测、内部组件 API,以及并发与失败语义。

**与其他文档的分工**:

| 文档 | 回答什么 |
| :--- | :--- |
| `PRD.md` | **为什么** — 需求与验收目标 |
| `README.md` | **怎么用** — 安装、命令、输出字段速查 |
| **本文档** | **契约是什么** — 边界、签名、语义、失败模式 |
| `REPORT.md` | **实测如何** — 观测值与偏差分析 |
| `CLAUDE.md` | **改代码时不能碰什么** — 设计红线 |

本文档记录的是**代码实际行为**,不是设计意图。若与 `PRD.md` 的表述冲突,以本文档为准(§9 单独列出偏离项)。

---

## 0. 接口全景

| 边界 | 接口 | 方向 | 章节 |
| :--- | :--- | :--- | :--- |
| 进程 | 命令行参数 / 退出码 / 信号 | 入 | §1 |
| 上游 | Binance WebSocket `@trade` 流 | 入 | §2 |
| 下游 | Parquet 文件 | 出 | §3 |
| 观测 | 结构化日志(控制台 + 文件) | 出 | §4 |
| 内部 | `RingBuffer` / `LatencyTracker` / `HFTPipeline` | 进程内 | §5 |
| 配置 | 硬编码策略常量 / CLI 部署参数 | 入 | §6 |
| 横切 | 并发模型、错误与失败语义 | — | §7 / §8 |

**本 Demo 明确不提供接口**:鉴权、订单路由、撮合对接、行情推送回调、HTTP/RPC 服务、环境变量配置。
信号只写日志,不产生任何订单 —— 集成方若需要程序化取用信号,只能消费 `system.log`。

---

## 1. 进程接口

### 1.1 调用形态

```
python hft_binance_demo.py [参数...]
```

- 唯一入口函数 `main(argv: Optional[List[str]] = None) -> int`。
- 模块带 `if __name__ == "__main__"` 守卫,**可安全 import**(顶层无网络调用、无全局副作用)。
- 单次运行模型:启动 → 运行 `--duration` 秒 → 收尾 → 打印报告 → 进程退出。不支持常驻、不支持热重载、不支持重复运行同一实例。
- 容器入口 `docker-entrypoint.sh` 只做一件事:把参数原样透传给 `main`,唯一的特殊关键字是 `selfcheck`(映射到 `Test/acceptance_check.py`)。

### 1.2 退出码

| 退出码 | 含义 | 是否已落盘收尾 | 日志状态 |
| :---: | :--- | :---: | :--- |
| `0` | 正常跑完 `--duration` | ✅ | 完整 |
| `0` | `Ctrl+C` / `SIGTERM` 有序收尾 | ✅ | 完整(含报告) |
| `1` | 未捕获异常(`logger.exception` 记录堆栈) | ✅(尽力) | 截断 |
| `2` | 参数校验失败 | ❌ 无输出文件 | **不创建、不截断** |
| `2` | `argparse` 自身报错(未知参数等) | ❌ | 无 |

**退出码 `2` 的日志保护是关键契约**:`validate_args()` 必须在 `setup_logging()` **之前**调用,因为日志文件以 `mode="w"` 打开会直接截断,一次被拒绝的启动不应毁掉上一次成功运行的交付物。

### 1.3 信号

| 信号 | 行为 |
| :--- | :--- |
| `SIGINT` / `SIGTERM` | 置位 `stop` 事件 → 有序收尾(抽干队列残余 → 刷盘 → 关闭 writer → 打印报告)→ 退出码 `0` |

通过 `loop.add_signal_handler()` 注册;Windows 等不支持的平台静默降级为仅 `Ctrl+C` 路径(`KeyboardInterrupt`)。
两者最终都汇入同一条收尾路径,行为一致。

### 1.4 标准流

| 流 | 内容 |
| :--- | :--- |
| `stdout` | 全部日志(INFO 及以上),与文件内容完全一致 |
| `stderr` | 仅 `ERROR` 行(参数校验失败),以及 `argparse` 的用法提示 |
| 日志格式 | `%(asctime)s.%(msecs)03d %(levelname)-7s %(message)s`,时间格式 `%Y-%m-%d %H:%M:%S` |

日志走 `logging` 的具名 logger **`"hft"`**,级别 `INFO`,`propagate = False`(不冒泡到 root),`handlers.clear()` 保证重复初始化不叠加输出。

### 1.5 文件系统副作用

| 路径 | 语义 |
| :--- | :--- |
| `--output`(默认 `market_data.parquet`) | **覆盖写**。首次刷盘时才创建,惰性生成 |
| `--log-file`(默认 `system.log`) | **截断写**(`mode="w"`),参数校验通过才打开 |

启动时会检测输出文件是否已存在并打印提示。交付物契约是「单次运行的干净产物」,不是累积文件。

---

## 2. 上游数据接口

### 2.1 连接参数

| 项 | 值 |
| :--- | :--- |
| 端点 | `wss://stream.binance.com:9443/ws/{symbol}@trade`,按 `--symbol` 小写拼接 |
| 覆盖 | `--stream-url` 可指向任意镜像/回放端点 |
| `open_timeout` | 10 s |
| `close_timeout` | 2 s |
| `ping_interval` / `ping_timeout` | 20 s / 20 s(库自动回复入站 ping) |
| `max_size` | 1 MiB |
| 连接数 | **恒为 1**,合规于 Binance 单 IP 5 连接上限 |

交易所 24 小时强断与心跳超时走**同一条** `ConnectionClosed` 异常路径,不区分对待。

### 2.2 消息契约

按 Binance `@trade` 原始字节处理,**不经中间结构体**,字段直接下标访问:

| 消费字段 | 用途 | 类型转换 |
| :--- | :--- | :--- |
| `e` | 仅用于字节级预过滤 `b'"e":"trade"'` | 不解析 |
| `p` | 成交价格 | `float()` 用于计算;原始字符串另存 `price_raw` |
| `q` | 成交数量 | 同上 → `quantity_raw` |
| `T` | 交易所事件时间 | `int()`,UTC 毫秒 |

**不消费**的字段(`a`/`b`/`m`/`M`/`B`/`A`/`P`/`Q`/`O` 等主动方、订单 ID、聚合标记)一律忽略。

**时间戳在接收瞬间就地采集**:`recv_ts = time.perf_counter_ns()` 在 `orjson.loads` **之前**取,量化的是网络到达后的解析延迟预算起点,而非解析后。

### 2.3 过滤与容错语义

1. **先过滤后解析**:C 级子串匹配 `TRADE_TAG not in raw` 命中即跳过,`orjson` 只为交易消息付出代价。被过滤的消息计入 `non_trade`,但不计入 `received`。
2. **字符串入站兼容**:若收到 `str` 而非 `bytes`,就地 `encode("utf-8")` 后同路径处理。
3. **畸形交易消息会断连,而非跳过一条**:`msg["p"]`/`msg["q"]`/`msg["T"]` 是直接下标访问,缺字段抛 `KeyError`,该异常向上传播到 `_ingest_loop` 的 `except Exception`,被计为**一次连接中断**并触发退避重连。这是当前实现的真实行为(见 §9)。

---

## 3. 下游数据接口

### 3.1 两种 Schema

默认**无损模式**(7 列);`--no-raw` 回到 PRD 5.2 的精确四列。模式在构造期定死,一次运行内不可切换。

| 列 | 类型 | 无损 | `--no-raw` | 语义 |
| :--- | :---: | :---: | :---: | :--- |
| `price` | `float64` | ✅ | ✅ | 向量化因子计算用 |
| `quantity` | `float64` | ✅ | ✅ | 向量化因子计算用 |
| `exchange_ts` | `int64` | ✅ | ✅ | 交易所 UTC 毫秒,**原样保存** |
| `recv_ts` | `int64` | ✅ | ✅ | `perf_counter_ns`,**进程相对时间** |
| `recv_utc_ms` | `int64` | ✅ | ❌(写 `0`) | 本地接收墙钟 UTC 毫秒 |
| `price_raw` | `string` | ✅ | ❌ | 币安原始字符串 |
| `quantity_raw` | `string` | ✅ | ❌ | 币安原始字符串 |

Schema 常量对外导出:`PARQUET_COLUMNS` / `PARQUET_SCHEMA` / `PARQUET_COLUMNS_FULL` / `PARQUET_SCHEMA_FULL`。

### 3.2 列语义与量纲

两套时间量纲**不可互减、不可求均值**:

- `recv_ts` — 纳秒,**进程相对**,无法换算墙钟,只用于 `(now − recv_ts)` 算延迟。
- `exchange_ts` — 交易所 UTC 毫秒。
- `recv_utc_ms` — 本地墙钟 UTC 毫秒,**入库展示请用这个**。

```python
import pandas as pd
df["recv_time"] = pd.to_datetime(df.recv_utc_ms, unit="ms", utc=True).dt.tz_convert("Asia/Shanghai")
```

`recv_utc_ms` 受 `keep_raw` 门控而非独立开关 —— `--no-raw` 时它被写为 `0`。**这是已知缺口**:无法在丢掉原始字符串列的同时保留墙钟列(见 §9)。

### 3.3 写入与文件语义

| 契约 | 行为 |
| :--- | :--- |
| 压缩 | `snappy`(固定,不可配) |
| 写入方式 | 常驻 `ParquetWriter` 句柄,只追加 `write_table`,不重建文件 |
| 线程归属 | 执行于 `loop.run_in_executor(None, ...)`,即默认线程池,**不占事件循环** |
| 刷盘触发 | ① 累计满 `--persist-batch` 条;② 连续 `--persist-flush-timeout` 秒无新数据;③ 收尾时抽干队列残余 |
| 句柄创建时机 | **首次刷盘时才创建**,以避开 0 行文件的边界情况 |
| 文件关闭 | `close()` 在后台线程执行,负责写页脚元数据;**早于最后一批写入调用 `close()` 会得到不可读文件** |
| 收尾幂等 | `_closed` 标志保证 `close()` 不重复调用(正常收尾与异常收尾可能各触发一次) |
| 空运行 | 一条行情都没收到时,**不创建任何文件**(writer 从未实例化,收尾跳过 close)。下游必须容忍文件缺失 |

**`--no-raw` 模式下的落盘批次大小约束**:`validate_args()` 强制 `1 <= --persist-batch <= --queue-maxsize`,缩队列不缩批次会被拒绝而非静默丢数据。

---

## 4. 日志接口(可观测性契约)

日志是信号的**唯一**对外通道,字段格式对外稳定。

### 4.1 标记清单

| 标记 | 级别 | 触发条件 | 频率 |
| :--- | :--- | :--- | :--- |
| `🚀 HFT-Demo 启动` | INFO | 启动 | 1 次 |
| `✅ WebSocket connected` | INFO | 每次连接建立 | 1 + 重连次数 |
| `⚠️ 连接中断` | WARNING | 连接异常 | 按次 |
| `⚠️ uvloop 不可用` | WARNING | uvloop 安装失败 | 0 或 1 次 |
| `⏳ 因子引擎预热中` | INFO | 数据不足 VWAP 窗口 | **最多 1 次** |
| `🚨 SIGNAL` | INFO | 触发 BUY/SELL | 按信号 |
| `📊 STATS` | INFO | 周期观测 | 每 `--stats-interval` |
| `⚠️ 墙钟与 perf_counter 偏差` | WARNING | 时钟偏差 > 5 s | **最多 1 次** |
| `❌ 落盘失败` / `❌ 关闭 ParquetWriter 失败` | ERROR | 写盘异常 | 按次 |
| `📈 HFT-Demo 性能验证报告` | INFO | 收尾 | 1 次 |

### 4.2 `📊 STATS` 字段

```
received= persisted= queue_depth= dropped= signals= (BUY / SELL)
tps= peak_tps= ingest_lat_avg= max= rss= diverge=
```

- `received` / `persisted` / `dropped` 为**累计**值;`persisted` 落后 `received` 最多一个批次,直到收尾。
- `tps` 是**上一个观测窗口**的平均速率,非瞬时值。
- `peak_tps` 单调不减,是各窗口 `tps` 的历史最大值 —— 反映「窗口级峰值」,不是 tick 级瞬时峰值。
- `diverge` = 墙钟耗时 − `perf_counter` 耗时。正常为 0;宿主机挂起/休眠期间 `perf_counter` 不前进而墙钟继续走,差值即实际挂起时长。**挂起期间吞吐与延迟指标全部显示 PASS,只有此字段能看见那段空白。**

### 4.3 `🚨 SIGNAL` 字段

```
🚨 SIGNAL {BUY|SELL} symbol= price= vwap= z= momentum= latency= exchange_ts= recv_ts=
```

`latency` 是 FR-3.2 的信号端到端延迟:信号 tick 的 `recv_ts` → 信号产出。

### 4.4 报告字段

收尾报告由 `report()` 打印,含运行时长(双时钟)、接收/落盘/丢弃计数与丢包率、数据完整性判定(`received == persisted`)、平均/峰值吞吐、策略触发次数、信号计数、平均与最大端到端延迟及 PASS/FAIL、样本数、信号延迟、峰值 RSS、重连次数、过滤消息数、落盘批次数、Schema 列名、输出路径。

---

## 5. 内部组件接口

### 5.1 模块级

| 名称 | 可见性 | 契约 |
| :--- | :--- | :--- |
| `setup_logging(log_file) -> Logger` | 公开 | 返回具名 logger `"hft"`;截断日志文件 |
| `peak_rss_mb() -> Optional[float]` | 公开 | `None` = 不可得;macOS 单位是字节、Linux 是 KB,内部已区分 |
| `try_install_uvloop() -> bool` | 公开 | 失败静默回退;**必须在 `asyncio.run()` 之前调用** |
| `parse_args(argv=None) -> Namespace` | 公开 | 无参可调用(取默认值),自检脚本依赖此点 |
| `validate_args(args) -> Optional[str]` | 公开 | 返回错误信息;通过返回 `None` |
| `main(argv=None) -> int` | 公开 | 进程入口,返回退出码 |
| 常量(`RING_CAPACITY` / `VWAP_WINDOW` / …) | 公开 | 见 §6.1 |

### 5.2 `RingBuffer(capacity: int)`

预分配 numpy 数组的定容环形缓冲区。四条独立连续数组(**struct-of-arrays**),而非一条对象数组 —— 保证向量化零拷贝,并彻底避开 Python 对象装箱与 GC 停顿。容量构造时定死,**永不扩容**。

| 成员 | 类型 | 说明 |
| :--- | :--- | :--- |
| `capacity` | `int` | 固定容量 |
| `prices` / `quantities` | `ndarray[float64]` | 长度 = `capacity` |
| `exchange_ts` / `recv_ts` | `ndarray[int64]` | 长度 = `capacity` |
| `head` | `int` | 下一个写入位置 |
| `count` | `int` | 有效条数;满载后恒等于 `capacity` |
| `put(price, quantity, exchange_ts, recv_ts) -> None` | 方法 | 已满时**原地覆盖最旧数据** |
| `get_latest(n) -> Tuple[ndarray ×4]` | 方法 | 最近 `n` 条,顺序**旧 → 新** |

`get_latest` 的三条隐含契约:

1. **空缓冲区短路**:`n == 0` 时先返回空数组再退出。不能省略这个 guard —— 否则 `start == end` 会让 `slice(start, capacity)` 返回整段**未初始化内存**,程序启动时读到垃圾价格。(已固化的缺陷,见 `Test/TEST.md` Q8)
2. **越界请求被 clamp**:`n > count` 时返回 `count` 条,不报错。
3. **返回值可能是视图**:未跨环形边界时直接返回底层数组的**切片视图**(零拷贝、零分配);跨边界时才 `np.concatenate`。视图**可写穿缓冲区**,当前唯一调用方 `_evaluate` 只读,因此安全 —— 但新增调用方时必须保持只读。

内存开销:`4 × capacity × 8 字节` = 64,000,000 B ≈ **61 MiB**(默认容量),一次分配,运行期不再增长。

### 5.3 `LatencyTracker`

`__slots__` 定型的 O(1) 内存统计器,只保留 `count` / `total` / `peak`。

| 成员 | 说明 |
| :--- | :--- |
| `add_ms(ms: float) -> None` | 累加一次观测 |
| `mean_ms` (property) | `count == 0` 时返回 `0.0`,不抛 `ZeroDivisionError` |
| `count` / `total` / `peak` | 直接可读 |

**不得**把每次延迟 `append` 进列表:每秒数千 tick,15 分钟会产生上百万对象 —— 正是本项目要消除的动态分配。

### 5.4 `HFTPipeline`

构造函数签名 `HFTPipeline(args: argparse.Namespace, logger: logging.Logger) -> None`,**参数经 `argparse.Namespace` 注入**,不接收逐项关键字参数。

**公开成员**(无下划线前缀,但均非设计为库 API):

| 成员 | 契约 |
| :--- | :--- |
| `run() -> None` | 异步生命周期入口;`finally` 中完成收尾与报告,保证异常/取消路径同样产出可读文件 |
| `clock_divergence() -> float` | 墙钟 − `perf_counter` 耗时(秒),用于检测宿主机挂起 |
| `report() -> None` | 打印性能报告,无返回值 |
| `keep_raw` / `received` / `persisted` / `dropped_persist` / `non_trade` / `evals` / `flushes` / `reconnects` / `signals_buy` / `signals_sell` | 只读统计口径 |
| `ingest_latency` / `signal_latency` | `LatencyTracker` 实例 |
| `_ring` | `RingBuffer` 实例(测试直接驱动它注入合成数据) |

**单下划线前缀成员是内部实现**,但 `Test/acceptance_check.py` 直接调用其中三个 —— 它们事实上构成测试接口,签名变更会破坏自检:

| 内部成员 | 测试/调用方契约 |
| :--- | :--- |
| `_queue` (property) | 惰性创建,见下方 |
| `_evaluate() -> None` | 同步;由 ingest 循环与自检脚本直接驱动 |
| `_flush(batch) -> None` | 异步;后台线程写盘 |
| `_drain_and_close() -> None` | 异步、**幂等** |

### 5.5 导入契约

```python
from hft_binance_demo import HFTPipeline, RingBuffer, parse_args, \
                             PARQUET_COLUMNS, PARQUET_SCHEMA, \
                             PARQUET_COLUMNS_FULL, PARQUET_SCHEMA_FULL
```

`import` 是**无副作用**的:不连网、不建文件、不安装事件循环策略。自检脚本从任意工作目录执行均可工作(它自行把仓库根加入 `sys.path`)。

**`_queue` 的惰性创建是加载承重的**:Python 3.9 的 `asyncio.Queue` 在构造时调用 `get_event_loop()` 绑定当前循环,因此在 `asyncio.run()` 之外实例化整个管线会抛 `RuntimeError`。这使 `HFTPipeline` 能在事件循环外安全构造(自检脚本正是这样驱动单组件的)。**不要把队列创建移回 `__init__`。**

---

## 6. 配置接口

### 6.1 策略常量(硬编码,刻意不可调)

`PIPELINE_TRIGGER_TICKS = 500` / `VWAP_WINDOW = 1000` / `MOMENTUM_WINDOW = 10` / `ZSCORE_SELL = 2.0` / `ZSCORE_BUY = -2.0` / `RING_CAPACITY = 2_000_000`。

这些是 PRD 固定的**策略参数**,不是部署开关。加一个命令行开关去调它们,等于在悄悄改变策略 —— 而策略是本 Demo 的核心内容。调参请改代码。

`500` tick 采样与 `5000` 行落盘批次**刻意用两个独立计数器解耦**。把它们合并成单一计数器是最可能引入静默数据完整性 bug 的改法。

### 6.2 部署参数(CLI 可调,均有 PRD 兼容默认值)

| 参数 | 默认 | 约束 | 语义 |
| :--- | :--- | :--- | :--- |
| `--duration` | `900` | `> 0` | 运行秒数 |
| `--output` | `market_data.parquet` | — | 落盘路径 |
| `--log-file` | `system.log` | — | 日志路径 |
| `--symbol` | `BTCUSDT` | — | 内部统一转大写 |
| `--stream-url` | 按 symbol 拼接 | — | 覆盖 WS 端点 |
| `--ring-capacity` | `2000000` | `>= max(VWAP, MOMENTUM)` | 环形缓冲区容量 |
| `--persist-batch` | `5000` | `[1, queue_maxsize]` | 微批行数 |
| `--queue-maxsize` | `100000` | `>= 1` | 队列容量,直接决定内存上限(每 row ≈ 350 B) |
| `--persist-flush-timeout` | `60` | `> 0` | 无数据兜底刷盘间隔 |
| `--reconnect-base` | `2` | `> 0` | 退避基准 |
| `--reconnect-max` | `30` | `>= reconnect_base` | 退避上限 |
| `--stats-interval` | `30` | `> 0` | 观测日志间隔 |
| `--no-raw` | 关 | — | 关闭无损 Schema |
| `--no-uvloop` | 关 | — | 使用默认事件循环 |

**所有约束由 `validate_args()` 在创建日志文件之前执行**(见 §1.2),违反即退出码 `2` 且不动任何既有产物。

### 6.3 重连参数语义

指数退避:`base → 2× → 4× → … → 上限`,每步叠加 **±25% 抖动**(`random.uniform(0.75, 1.25)`),连接成功后退避**复位**到基准。抖动用于打散多实例同步重连,避免触发 Binance 单 IP 速率限制。

### 6.4 无环境变量接口

**本项目不读任何环境变量**。所有配置均来自命令行。容器内设置的 `TZ` / `PYTHONUNBUFFERED` 是运行时环境变量(影响日志时区与块缓冲),不是应用配置接口。

---

## 7. 并发模型契约

```
单一事件循环线程
├─ _ingest_loop   连接 + 重连外壳
│    └─ _consume  字节过滤 → 解析 → 入环形缓冲区 → 入队 → 每 500 tick 同步评估
├─ _persist_loop  从队列取行 → 攒批 → run_in_executor 写盘
├─ _stats_loop    周期观测
├─ timeout_task   运行时长到点
└─ stop_task      信号触发

默认线程池 (ThreadPoolExecutor)
└─ _write_batch / writer.close()   ← 唯一离开事件循环的代码路径
```

| 契约 | 说明 |
| :--- | :--- |
| 事件循环永不阻塞在磁盘 I/O | 所有 Parquet 操作经 `run_in_executor`,包括 `close()` |
| 入队只用 `put_nowait` | 队列满抛 `QueueFull` → 丢弃并计数 → 继续;**绝不反压行情接收** |
| 因子计算在事件循环线程内同步执行 | `_evaluate` 无 `await`;窗口仅 1000 行 numpy 运算,刻意如此 |
| 唯一列表构建是 5,000 行微批 | 转换成本摊薄到每 5000 条一次,构成不了热路径 |
| 协程异常不外溢 | `asyncio.wait(..., FIRST_COMPLETED)` 返回 done 集合,异常逐一体检并记录 |
| 收尾由 `run()` 的 `finally` 兜底 | 正常、取消、异常三条路径都保证关闭 writer 并打印报告 |

---

## 8. 错误与失败语义

| 故障 | 行为 | 数据后果 |
| :--- | :--- | :--- |
| 连接被拒 / `InvalidStatus`(单 IP 速率限制) | 计入 `reconnects`,指数退避 + 抖动 | 该窗口零消息,无丢失 |
| 连接中断 / 24h 强断 / ping 超时 | 同上一路径,统一处理 | 同上 |
| 落盘队列满 | 丢弃并计入 `dropped_persist` | **永久丢失**,反映在最终丢包率 |
| 写盘抛异常 | 记录 `❌ 落盘失败`,整批计入 `dropped_persist`,继续运行 | 丢失该批 |
| 关闭 writer 失败 | 记录 `❌ 关闭…失败`,`_writer` 置 `None` | 文件可能损坏,但仍打印报告 |
| 落盘写满 0 数量 | `denom <= 0` 直接返回,不判定 | 无信号,无异常 |
| 价格 std 为 0 | `std <= 1e-12` 直接返回,不判定 | 无信号,避免除零 |
| 窗口数据不足 | 预热门控返回,首次打印 `⏳` | 无信号 |
| 数据量纲混用 | 不存在该故障 —— 两套时间戳在代码中从不互减 | — |
| 未捕获异常 | `logger.exception` + 退出码 `1`,收尾仍执行 | 数据截至异常前 |

**丢包率口径**:`dropped_persist / received × 100%`。注意 `received` 不含被字节过滤的非交易消息(它们是 `non_trade`),所以该比率只度量「已判定为交易、已进入缓冲区的消息」的落盘完整性。

---

## 9. 契约缺口与已知偏离

以下为**有意记录、尚未修复**的偏离,修改相关代码前请先读本节。

1. **`_queue` 相关类型注解与实际元素形状不一致**。注解写的是 `asyncio.Queue[Tuple[float, float, int, int]]`,但无损模式下入队元素是 **7 元组**(`+ recv_utc_ms, price_raw, quantity_raw`);`_persist_loop` / `_flush` / `_write_batch` 的 `batch` 注解同样只写 4 元组。运行期正确(`zip(*batch)` 按 `keep_raw` 分支解包),但注解会误导读者。**不要**为了让注解自洽而改动解包分支。

2. **`recv_utc_ms` 被 `keep_raw` 门控,不是独立开关**。想保留墙钟列就必须保留两个原始字符串列;想要「最小 Schema + 墙钟时间」的组合,当前接口做不到。若下游确实需要,这是一个真实的接口缺口,不是配置错误。

3. **零行情运行不产生文件**。writer 惰性创建,收尾时发现 `_writer is None` 就直接跳过 `close()`。结果是文件**不存在**,而非 0 行可读文件。下游消费者必须容忍文件缺失。

4. **畸形交易消息断掉整个连接**。`msg["p"]`/`msg["q"]`/`msg["T"]` 直接下标访问,缺字段抛 `KeyError`,一路冒泡到 `_ingest_loop` 被当作连接中断处理,触发退避重连。更稳妥的语义是跳过该条并计数,但当前实现选择保守:宁可断线重来,也不带着半解析状态继续消费。

5. **`peak_tps` 是窗口级历史峰值,不是瞬时峰值**。它是各观测窗口 `tps` 的最大值,受 `--stats-interval` 粒度限制。`--stats-interval` 调大会系统性低估峰值,调小会增加日志量 —— 报告中的峰值数字必须与所用间隔一起解读。

6. **信号无程序化出口**。`_evaluate` 的结果只以日志行存在,不回调、不推流、不返回。集成方需要自己解析 `🚨 SIGNAL` 行。

7. **`get_latest()` 返回视图**。非跨边界路径返回底层数组切片,可写穿缓冲区。当前唯一调用方只读,契约成立是**偶然**的(靠调用方自律),不是靠接口强制。

8. **预热与偏差告警各只打印一次**(`_warm_logged` / `_divergence_warned`)。日志中看不到预热持续多久、偏差是否持续增长,需从 `STATS` 的 `diverge` 字段自行推断。

9. **`peak_rss_mb()` 在 macOS/Linux 返回不同量纲的原始值再换算**。平台不可用时返回 `None`,报告与 `STATS` 统一显示 `-1.0`。

10. **容器 `WORKDIR` 是 `/data`**,而源码在 `/app`。`docker run` 时未指定 `--output` / `--log-file`,产物落在 `/data`(即 `VOLUME`)而非源码目录;本地运行则落在当前目录。两种运行方式的产物位置不同,由 `WORKDIR` 差异导致,而非配置分歧。
