
---

# 产品需求文档 (PRD)：实时高频行情处理与交易系统 Demo

| 文档属性 | 详细信息 |
| :--- | :--- |
| **项目名称** | HFT-Demo (High-Frequency Trading Data Pipeline Demo) |
| **文档版本** | v1.0.0 |
| **创建日期** | 2026-10-26 |
| **目标数据源** | Binance 现货 WebSocket (`btcusdt@trade`) |
| **目标受众** | 量化开发工程师 |

---

## 1. 产品概述

### 1.1 项目背景
在量化交易领域，高频策略对系统的延迟、吞吐量和数据完整性有着极其苛刻的要求。传统的基于 REST API 或动态内存分配（如频繁创建 Pandas DataFrame）的架构，在面对每秒数千条 Tick 数据时，极易出现网络阻塞、GC (垃圾回收) 停顿和磁盘 I/O 瓶颈。

### 1.2 项目目标
设计并开发一套**轻量级、低延迟、高吞吐**的实时高频行情处理与交易系统 Demo。系统需完整实现从“网络数据接收 -> 内存驻留 -> 因子计算 -> 策略触发 -> 信号输出 -> 原始数据稳定落盘”的全链路闭环，并通过 15 分钟的严格压力测试，验证其性能指标。

### 1.3 项目范围 (In-Scope)
- 接入 Binance 免费公开的 `btcusdt@trade` 实时 WebSocket 流。
- 实现基于预分配 Numpy 数组的零 GC 停顿环形缓冲区 (Ring Buffer)。
- 实现基于 Numpy 向量化的 Tick 级因子计算（VWAP、动量）与均值回归策略。
- 实现基于 `asyncio` 队列和 `pyarrow` 的异步微批 (Micro-batching) Parquet 落盘机制。
- 提供 15 分钟连续运行的自动化测试脚本及详尽的性能验证报告。

### 1.4 项目范围外 (Out-of-Scope)
- 真实的订单路由与交易所撮合引擎对接。
- 复杂的机器学习模型或高频订单簿 (Order Book) 重建。
- 分布式集群部署（本项目聚焦于单机极致性能验证）。

---

## 2. 系统架构与技术选型

### 2.1 系统架构图 (逻辑视图)
```text
[Binance WebSocket] 
       │ (Push: ~2000 ticks/sec)
       ▼
[1. Data Ingestor] ──(极速 orjson 解析 + 字节预过滤)──┐
       │                                              │
       ├─► [2. Ring Buffer] (预分配 Numpy 内存, 零 GC)
       │         │
       │         ▼ (每 500 ticks 触发)
       │    [3. Factor & Strategy Engine] (Numpy 向量化计算: VWAP, Z-Score)
       │         │
       │         ▼ (触发条件满足)
       │    [4. Signal Output] (记录端到端延迟, 打印日志)
       │
       └─► [5. Async Persistence Queue] (解耦的 asyncio.Queue)
                 │ (每 5000 条微批触发)
                 ▼
            [6. Parquet Writer] (后台线程异步刷盘, 不阻塞主循环)
                 │
                 ▼
            [market_data.parquet] (列式存储, 高压缩比)
```

### 2.2 核心技术栈
| 模块 | 技术选型 | 选型理由 |
| :--- | :--- | :--- |
| **运行时** | Python 3.10+ + `uvloop` (可选) | 兼顾开发效率与极致异步 I/O 性能。 |
| **网络通信** | `websockets` | 原生支持 asyncio，内置 Ping/Pong 保活与断线重连机制。 |
| **JSON 解析** | `orjson` | C 语言编写的极速 JSON 库，比标准 `json` 快 3-5 倍。 |
| **内存管理** | `numpy` (预分配数组) | 彻底避免 Python 列表 `append` 带来的动态内存分配和 GC 停顿。 |
| **数据落盘** | `pyarrow` + `parquet` | 列式存储，支持高效的微批追加写入，读写性能远超 CSV/JSON。 |
| **并发模型** | `asyncio` + `run_in_executor` | 主线程处理高频事件，磁盘 I/O 交由线程池异步处理，实现零阻塞。 |

---

## 3. 功能需求 (Functional Requirements)

### FR-1: 实时数据摄取 (Data Ingestion)
- **FR-1.1**: 系统必须能够连接到 `wss://stream.binance.com:9443/ws/btcusdt@trade`。
- **FR-1.2**: 必须使用字节串匹配 (`b'"e":"trade"'`) 预先过滤非交易消息（如 ping/pong），避免无效解析。
- **FR-1.3**: 必须使用 `time.perf_counter_ns()` 记录每条消息到达系统的**本地纳秒级时间戳**，用于后续延迟计算。
- **FR-1.4**: 遇到网络断开时，系统必须在 2 秒内自动尝试重连，并记录警告日志。

### FR-2: 内存管理与因子计算 (Memory & Factor Engine)
- **FR-2.1**: 必须使用固定容量（2,000,000 条）的环形缓冲区 (Ring Buffer) 存储最新行情，包含字段：`price`, `quantity`, `exchange_ts`, `recv_ts`。
- **FR-2.2**: 缓冲区达到容量上限时，必须采用覆盖最旧数据的策略 (Overwrite)，绝不允许触发动态扩容。
- **FR-2.3**: 系统需每累积 **500 个新 Tick** 触发一次策略管线。
- **FR-2.4**: 因子计算必须使用 Numpy 向量化操作，计算过去 1000 个 Tick 的 VWAP (成交量加权平均价) 和标准差，以及过去 10 个 Tick 的动量 (Momentum)。

### FR-3: 策略运行与信号输出 (Strategy & Signal)
- **FR-3.1**: 策略逻辑：计算当前价格相对于 VWAP 的 Z-Score。
  - 若 `Z-Score > 2.0` 且 `Momentum < 0`，输出 `SELL` 信号。
  - 若 `Z-Score < -2.0` 且 `Momentum > 0`，输出 `BUY` 信号。
- **FR-3.2**: 每次生成信号时，必须计算并记录**端到端延迟** (`当前系统时间 - 该 Tick 的 recv_ts`)，并输出包含 Price, VWAP, Z-Score, Latency 的结构化日志。

### FR-4: 数据持久化 (Persistence)
- **FR-4.1**: 必须使用独立的 `asyncio.Queue` 作为摄取引擎和落盘引擎之间的缓冲，队列最大容量设为 100,000。
- **FR-4.2**: 落盘引擎必须每累积 **5,000 条**数据（或程序关闭时），执行一次批量写入。
- **FR-4.3**: 必须使用 `pyarrow.parquet.ParquetWriter` 保持文件句柄常驻，通过 `write_table` 和 `flush` 进行追加写入，**严禁**每次写入都重新读取和重写整个文件。
- **FR-4.4**: 磁盘写入操作必须通过 `loop.run_in_executor` 放入后台线程池执行，确保绝不阻塞主事件循环。

---

## 4. 非功能需求 (Non-Functional Requirements)

### NFR-1: 性能指标 (Performance)
- **吞吐量**: 在正常网络条件下，系统处理吞吐量应 $\ge$ 接收速率（预期 1,500 ~ 4,000 ticks/sec），无积压。
- **端到端延迟**: 从 WebSocket 接收到策略信号输出的内部处理延迟（不含网络传输），平均值应 $< 2.0$ ms，P99 延迟应 $< 10.0$ ms。
- **数据完整性**: 15 分钟测试期内，落盘丢包率必须为 **0.0000%**（即 `总接收数 == 总落盘数`）。

### NFR-2: 资源稳定性 (Stability)
- **内存占用**: 运行期间，系统常驻内存 (RSS) 应稳定在 **150MB - 250MB** 之间，无明显内存泄漏或持续上升趋势。
- **CPU 占用**: 单核 CPU 占用率在数据处理期间应 $< 30\%$（取决于主机性能），无异常 CPU 飙升。

### NFR-3: 可观测性 (Observability)
- 系统必须输出结构化的控制台日志和 `system.log` 文件。
- 测试结束时，必须自动生成并打印包含：运行时长、总接收数、总落盘数、丢包率、信号数、平均/最大延迟的 **性能报告**。

---

## 5. 数据字典与接口定义

### 5.1 内存结构 (Ring Buffer Schema)
| 字段名 | 数据类型 (Numpy) | 描述 |
| :--- | :--- | :--- |
| `prices` | `float64` | 交易价格 |
| `quantities` | `float64` | 交易数量 |
| `exchange_ts` | `int64` | 币安服务器生成该交易的时间戳 (毫秒) |
| `recv_ts` | `int64` | 本地系统接收到该消息的纳秒级时间戳 |

### 5.2 落盘数据 Schema (Parquet)
| 字段名 | 数据类型 (PyArrow) | 描述 |
| :--- | :--- | :--- |
| `price` | `float64` | 交易价格 |
| `quantity` | `float64` | 交易数量 |
| `exchange_ts` | `int64` | 交易所时间戳 (毫秒) |
| `recv_ts` | `int64` | 本地接收时间戳 (纳秒) |

### 5.3 外部接口 (Binance WebSocket Payload)
```json
{
  "e": "trade",          // 事件类型
  "E": 123456789,        // 事件时间
  "s": "BTCUSDT",        // 交易对
  "t": 12345,            // 交易 ID
  "p": "0.001",          // 成交价格 (字符串，需转为 float)
  "q": "100",            // 成交数量 (字符串，需转为 float)
  "T": 123456785,        // 交易发生时间 (毫秒)
  "m": true,             // 买方是否为做市方
  "M": true              // 请忽略该字段
}
```

---

## 6. 测试与验收标准 (Testing & Acceptance Criteria)

### 6.1 验收测试用例 (UAT)
| 用例 ID | 测试场景 | 预期结果 | 验收标准 |
| :--- | :--- | :--- | :--- |
| **TC-01** | **启动与连接** | 程序启动后 3 秒内成功连接 WS，开始输出接收日志。 | 日志显示 "✅ WebSocket connected" |
| **TC-02** | **15 分钟持续运行** | 程序连续运行 900 秒，无崩溃、无未捕获异常。 | 进程正常退出，无 Error 级别日志。 |
| **TC-03** | **策略信号触发** | 在波动较大时，日志中出现 `🚨 SIGNAL` 记录。 | 信号日志包含 Price, VWAP, Z, Latency 字段。 |
| **TC-04** | **数据完整性校验** | 运行结束后，解析 `market_data.parquet`。 | `len(df)` 等于性能报告中的 `总接收 Tick 数`。 |
| **TC-05** | **延迟达标校验** | 检查性能报告中的延迟指标。 | 平均延迟 $< 2.0$ ms，最大延迟 $< 15.0$ ms。 |
| **TC-06** | **内存稳定性校验** | 使用系统监视器观察 15 分钟内的内存变化。 | 内存曲线平稳，无阶梯式上升 (无内存泄漏)。 |

### 6.2 交付物清单 (Deliverables)
1. `hft_binance_demo.py`：完整、可独立运行的 Python 源代码。
2. `requirements.txt`：项目依赖清单（包含 `websockets`, `orjson`, `numpy`, `pyarrow`, `uvloop`）。
3. `system.log`：15 分钟测试期间生成的完整系统运行日志。
4. `market_data.parquet`：测试期间落盘的全部原始行情数据文件。
5. **性能验证报告**：程序退出时自动打印在控制台并记录在日志末尾的统计摘要。

---

## 7. 风险与应对策略 (Risks & Mitigations)

| 风险描述 | 影响程度 | 应对策略 (Mitigation) |
| :--- | :---: | :--- |
| **网络抖动导致 WS 断开** | 高 | 实现 `try-except` 捕获 `ConnectionClosed`，并内置 2 秒指数退避重连机制。 |
| **磁盘 I/O 瓶颈导致队列溢出** | 中 | 使用 `asyncio.Queue(maxsize=100000)` 限制内存占用；若队列满，仅丢弃落盘任务并记录 `dropped_persistence` 计数，**绝不阻塞**主循环的行情接收。 |
| **Numpy 环形边界切片错误** | 中 | 在 `get_latest` 方法中严格处理 `ptr < n` 的跨边界情况，使用 `np.concatenate` 确保返回连续数组。 |
| **Parquet 文件损坏** | 低 | 使用 `ParquetWriter` 并在程序优雅关闭 (`graceful shutdown`) 时显式调用 `writer.close()`，确保文件尾部元数据正确写入。 |

---

## 8. 附录：环境搭建与执行指令

### 8.1 依赖安装
```bash
# 推荐使用 Python 3.10 或更高版本
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -U pip
pip install websockets orjson numpy pyarrow uvloop
```

### 8.2 执行测试
```bash
python hft_binance_demo.py
```

### 8.3 验证落盘数据 (验收脚本)
```python
import pyarrow.parquet as pq
import pandas as pd

df = pq.read_table('market_data.parquet').to_pandas()
print(f"✅ 验收通过: 落盘总行数 = {len(df):,}")
print(df[['price', 'quantity', 'exchange_ts', 'recv_ts']].tail())
```

--- 
