# Quant-Demo

轻量级实时高频行情处理与交易系统 Demo。接入 Binance 现货 WebSocket 流,经内存驻留、因子计算、策略判定后输出信号,并将原始行情微批落盘为 Parquet。

完整需求见 [`PRD.md`](PRD.md),压测结果与缺陷记录见 [`REPORT.md`](REPORT.md)。

---

## 数据链路

```text
Binance WebSocket (btcusdt@trade)
  │  字节预过滤 b'"e":"trade"' + orjson 解析 + perf_counter_ns() 纳秒时间戳
  ▼
Ring Buffer ── 2,000,000 条预分配 numpy 数组, 溢出覆盖最旧数据, 零 GC 停顿
  │  每 500 tick
  ▼
因子 / 策略引擎 ── numpy 向量化 VWAP(1000) / std(1000) / momentum(10)
  │  Z-Score > 2.0 且动量 < 0 → SELL
  │  Z-Score < -2.0 且动量 > 0 → BUY
  ▼
信号输出 ── 记录端到端延迟 + 结构化日志
  │
asyncio.Queue(100,000) ── 队列满则丢弃并计数, 绝不反压行情接收
  │  每 5,000 条微批
  ▼
ParquetWriter ── 句柄常驻, run_in_executor 后台线程, 不阻塞事件循环
  ▼
market_data.parquet
```

---

## 快速开始

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python hft_binance_demo.py          # 15 分钟压力测试 (900 秒)
```

程序运行结束会自动打印性能验证报告,并写出 `market_data.parquet` 与 `system.log`。

### 常用参数

```bash
python hft_binance_demo.py --duration 60      # 快速冒烟测试
python hft_binance_demo.py --symbol ETHUSDT   # 切换交易对
python hft_binance_demo.py --no-raw           # 还原 PRD 5.2 的四列 Schema
python hft_binance_demo.py --stream-url wss://<mirror>/ws/btcusdt@trade
python hft_binance_demo.py --help             # 全部参数
```

| 参数 | 默认值 | 说明 |
| :--- | :--- | :--- |
| `--duration` | `900` | 连续运行秒数 |
| `--output` | `market_data.parquet` | 落盘路径 |
| `--log-file` | `system.log` | 日志路径 |
| `--symbol` | `BTCUSDT` | 交易对 |
| `--stream-url` | 按 symbol 拼接 | 覆盖默认 WS 地址,可指向镜像端点 |
| `--ring-capacity` | `2000000` | 环形缓冲区容量 |
| `--persist-batch` | `5000` | 微批落盘行数 |
| `--no-raw` | 关 | 仅保留 PRD 5.2 四列 |
| `--no-uvloop` | 关 | 使用默认事件循环 |

---

## 输出

### `market_data.parquet`

默认 7 列(无损模式):

| 字段 | 类型 | 说明 |
| :--- | :--- | :--- |
| `price` / `quantity` | `float64` | 向量化因子计算用 |
| `exchange_ts` | `int64` | 交易所 UTC 毫秒 |
| `recv_ts` | `int64` | `perf_counter_ns`,**进程相对时间** |
| `recv_utc_ms` | `int64` | 本地接收 UTC 墙钟毫秒 |
| `price_raw` / `quantity_raw` | `string` | 币安原始字符串,无损精度 |

### `system.log`

控制台 + 文件双通道,含启动配置、周期观测、信号、退出性能报告。

---

## 关键设计约定

### 精度分层

币安以字符串下发定点小数。热路径转 `float64` 仅供 numpy 向量化计算;原始字符串另存 `price_raw` / `quantity_raw` 无损落盘——若在入口处丢弃原值,二进制转换的精度损失将永久无法挽回。

**热路径不要改用 `Decimal`。** VWAP 与 Z-Score 属比率与统计运算,float64 的 ~1e-15 相对误差对策略无关紧要,而 Decimal 会摧毁向量化,而向量化是本项目的核心设计前提。结算、记账等严谨场景请取 `price_raw` 走 Decimal。

> 实测校准:真实 BTCUSDT 数据上精度丢失为 0 行——BTCUSDT 价格 2 位、数量 6 位小数落在 float64 的 15–16 位有效数字内,本就精确。`price_raw` 是为其他交易对准备的保险。

### 时间戳两套量纲

- `exchange_ts`:交易所 UTC 毫秒,原样保存
- `recv_ts`:`perf_counter_ns`,**进程相对时间,不可换算成墙钟时间**,仅用于 (now − recv_ts) 计算延迟

两套量纲不可互减、不可求均值。入库展示请用 `recv_utc_ms`:

```python
import pandas as pd
df["recv_time"] = pd.to_datetime(df.recv_utc_ms, unit="ms", utc=True).dt.tz_convert("Asia/Shanghai")
```

### 交易所连接约束

- **单连接单流**,合规于 Binance 单 IP 最多 5 个 WebSocket 的限制。扩展多个交易对请使用组合流 `ws/btcusdt@trade/ethusdt@trade`,而非增加连接数。
- **24 小时强断与 Ping/Pong 无需自研**:`websockets` 库自动回复入站 Ping;`ping_interval=20` + `ping_timeout=20` 负责探活并抛 `ConnectionClosed`,与 24h 强断走同一条路径。
- **重连**:指数退避 2s → 30s 上限,叠加 ±25% 抖动,避免多实例同步重连触发 IP 速率限制。

---

## 验收指标

15 分钟实盘压测(详见 [`REPORT.md`](REPORT.md)):

| 指标 | 目标 | 实测 |
| :--- | :--- | :--- |
| 数据完整性 | 丢包率 0.0000% | **0.0000%**(35,935 == 35,935) |
| 平均端到端延迟 | < 2.0 ms | **0.013 ms** |
| 最大端到端延迟 | < 15.0 ms | **1.658 ms** |
| 队列积压 | 无 | 深度全程 **0** |
| 内存稳定 | 无泄漏 | **95.3 MB** 平稳 |

**关于吞吐**:PRD 假设约 2,000 ticks/sec,实测为 **6–138 ticks/sec**。这是交易所的到达速率,不是代码问题。系统满足的是相对指标「吞吐 ≥ 到达速率且无积压」,该指标达成。

**关于信号**:短运行时几乎不会出现 `🚨 SIGNAL`——低到达速率下 500-tick 采样点很少,且均值回归形态本身罕见。策略逻辑已用合成数据验证双向可触发。可下调 `ZSCORE_SELL` / `ZSCORE_BUY` 或加密 `PIPELINE_TRIGGER_TICKS` 采样以观测信号。

---

## 校验落盘数据

```python
import pyarrow.parquet as pq

df = pq.read_table("market_data.parquet").to_pandas()
print(f"落盘总行数 = {len(df):,}")
print(df.tail())
```

---

## 文档索引

| 文件 | 内容 |
| :--- | :--- |
| `PRD.md` | 产品需求文档,功能与非功能需求定义 |
| `Test/TEST.md` | **验收测试手册**:逐条命令、期望输出、判定标准 |
| `Test/acceptance_check.py` | 离线静态自检脚本(18 项,不连网) |
| `REPORT.md` | 项目报告:架构、验收结果、缺陷记录、偏差分析 |
| `CLAUDE.md` | AI 协作指引:设计红线、环境约束、常见陷阱 |
| `hft_binance_demo.py` | 全部实现源码 |

---

## 说明

本项目为技术验证 Demo,按 PRD 定义**不包含**真实订单路由与交易所撮合对接。信号仅打印日志,不产生任何订单。请勿在生产环境直接使用。
