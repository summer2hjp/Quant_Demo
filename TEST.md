# Quant-Demo 验收测试手册

本文档供**验收方**使用。所有命令均已实测通过,可直接复制粘贴执行。

---

## 目录

- [如何使用本文档](#如何使用本文档)
- [阶段 0:准备环境(一次性,约 2 分钟)](#阶段-0准备环境一次性约-2-分钟)
- [阶段 1:静态自检(不连网,约 10 秒)](#阶段-1静态自检不连网约-10-秒)
- [阶段 2:功能测试 TC-01 ~ TC-10](#阶段-2功能测试)
- [阶段 3:正式 15 分钟压测](#阶段-3正式-15-分钟压测)
- [判定汇总表](#判定汇总表)
- [常见问题排查](#常见问题排查)
- [清理](#清理)

---

## 如何使用本文档

### 两条最重要的事

**第一,分清「必须通过」和「参考值」。**

本文档把每条用例标为两种之一:

| 标记 | 含义 | 不满足怎么办 |
| :--- | :--- | :--- |
| ✅ **必须** | 确定性结果,与行情无关,永远应该成立 | 视为缺陷,需排查 |
| 📊 **参考** | 取决于交易所行情,波动很大 | **不要判为失败**,只看趋势 |

例如:延迟阈值是「必须」;具体收到多少 tick 是「参考」。

**第二,吞吐量的判断标准不是数字,是「有无积压」。**

PRD 假设约 2,000 ticks/sec,但 Binance BTCUSDT 现货实测只有 **4–138 ticks/sec**——这是交易所的到达速率,不是代码性能。系统满足的是相对指标:**吞吐 ≥ 到达速率且队列无积压**。所以验收时看队列深度是否为 0,而不是看 tps 数字。

---

## 阶段 0:准备环境(一次性,约 2 分钟)

### 0.1 确认 Python 版本

```bash
python3 --version
```

> ✅ **必须**:输出 `Python 3.9.x` 或更高。代码按 3.9 编写。
>
> ⚠️ 如果是 3.8 或更低,`asyncio` 行为不兼容,需升级。

### 0.2 创建虚拟环境并安装依赖

```bash
cd Quant_Demo
python3 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -U pip
pip install -r requirements.txt
```

> ✅ **必须**:安装过程无报错退出。

### 0.3 确认依赖与平台

```bash
python -c "
import websockets, orjson, numpy, pyarrow
print('websockets', websockets.__version__)
print('orjson    ', orjson.__version__)
print('numpy     ', numpy.__version__)
print('pyarrow   ', pyarrow.__version__)
try:
    import uvloop; print('uvloop    ', uvloop.__version__, '(可选, 已安装)')
except ImportError:
    print('uvloop     未安装 (可选, 程序会自动回退默认事件循环)')
"
```

> ✅ **必须**:四个主依赖版本号正常打印。
>
> 📊 **参考**:`uvloop` 缺失不影响功能,程序会打印一条 `uvloop 不可用` 警告并继续。

### 0.4 确认网络可达

```bash
curl -s -o /dev/null -w "HTTP %{http_code}  耗时 %{time_total}s\n" --max-time 10 \
  https://api.binance.com/api/v3/time
```

> ✅ **必须**:输出 `HTTP 200`。
>
> 📊 **参考**:耗时。本次实测约 1.1s。国内网络可能更高,只要能返回 200 即可继续。
>
> ❌ 如果超时或返回非 200,说明当前网络无法直连 Binance。参见[常见问题 Q4](#q4-连不上-binance)。

### 0.5 确认源码完整

```bash
python -m py_compile hft_binance_demo.py && echo "语法检查通过"
python hft_binance_demo.py --help | head -5
```

> ✅ **必须**:输出 `语法检查通过`,且 `--help` 正常打印用法说明。

---

## 阶段 1:静态自检(不连网,约 10 秒)

这一步验证**确定性逻辑**:环形缓冲区、策略信号、落盘往返。不需要网络,结果是固定的。

### 1.1 保存自检脚本

```bash
cat > /tmp/acceptance_check.py << 'PYEOF'
#!/usr/bin/env python3
"""Quant-Demo 静态自检: 不连网, 只验证确定性的逻辑正确性。"""
import argparse, asyncio, logging, sys, time
import numpy as np
import pyarrow.parquet as pq
from decimal import Decimal
from hft_binance_demo import (HFTPipeline, RingBuffer, PARQUET_COLUMNS,
                              PARQUET_COLUMNS_FULL, PARQUET_SCHEMA, PARQUET_SCHEMA_FULL)

FAIL = []

def check(name, cond, detail=""):
    print("  [%s] %s %s" % ("PASS" if cond else "FAIL", name, detail))
    if not cond:
        FAIL.append(name)

def make(keep_raw=True, batch=2, cap=10000):
    a = argparse.Namespace(symbol="BTCUSDT", output="/tmp/_ac.parquet", duration=1,
                           ring_capacity=cap, persist_batch=batch, stream_url=None,
                           keep_raw=keep_raw)
    return HFTPipeline(a, logging.getLogger("ac"))

print("== 1. 环形缓冲区 ==")
rb = RingBuffer(8)
for i in range(12):
    rb.put(float(i), 1.0, i, i)
check("溢出覆盖: 保留最新 8 条", list(rb.get_latest(8)[0]) == list(range(4, 12)))
r8 = RingBuffer(8)
for i in range(8):
    r8.put(float(i), 1.0, i, i)
check("整圈满载返回全部", list(r8.get_latest(8)[0]) == list(range(8)))
check("空缓冲区返回空数组", len(RingBuffer(4).get_latest(10)[0]) == 0)
check("越界请求被 clamp", len(r8.get_latest(10**6)[0]) == 8)

print("== 2. 策略信号逻辑 (合成数据, 确定性) ==")
# SELL: 长期平稳 -> 连续冲高 -> 冲高后回落 (z>2 且 momentum<0)
p = make()
for i in range(1000):
    p._ring.put(100000.0, 1.0, i, i)
for i in range(1, 11):
    p._ring.put(100000.0 + i * 3000.0, 1.0, 1000 + i, 1000 + i)
p._evaluate()
check("z 高但动量同向时不触发", p.signals_sell == 0 and p.signals_buy == 0)
for i in range(1, 6):
    p._ring.put(100000.0 + (10 - i) * 3000.0, 1.0, 1011 + i, 1011 + i)
p._evaluate()
check("SELL: z>2.0 且 momentum<0", p.signals_sell == 1)

# BUY: 长期高位 -> 砸盘 -> 反弹 (z<-2 且 momentum>0)
p2 = make()
for i in range(1000):
    p2._ring.put(110000.0, 1.0, i, i)
for i in range(1, 11):
    p2._ring.put(110000.0 - i * 3000.0, 1.0, 1000 + i, 1000 + i)
for i in range(1, 6):
    p2._ring.put(80000.0 + i * 5000.0, 1.0, 1011 + i, 1011 + i)
p2._evaluate()
check("BUY: z<-2.0 且 momentum>0", p2.signals_buy == 1 and p2.signals_sell == 0)

# 预热门控
p3 = make()
for i in range(400):
    p3._ring.put(100000.0 + i, 1.0, i, i)
p3._evaluate()
check("预热期 (数据不足 1000) 不判定信号", p3.signals_buy + p3.signals_sell == 0)

print("== 3. 落盘往返 ==")
async def feed(pipe, n=6000):
    rows = [(100000.0 + i * 0.5, 1.5 + (i % 3), 1_700_000_000 + i,
             time.perf_counter_ns() - i, time.time_ns() // 1_000_000,
             "100000.%08d" % (i * 50), "1.500000") if pipe.keep_raw else
            (100000.0 + i * 0.5, 1.5 + (i % 3), 1_700_000_000 + i,
             time.perf_counter_ns() - i)
            for i in range(n)]
    for r in rows:
        pipe._queue.put_nowait(r)
    for _ in range(n // pipe.persist_batch):
        await pipe._flush([pipe._queue.get_nowait() for _ in range(pipe.persist_batch)])
    await pipe._flush([pipe._queue.get_nowait() for _ in range(pipe._queue.qsize())])
    await pipe._drain_and_close()

pa = make(keep_raw=True, batch=5000)
asyncio.run(feed(pa))
t = pq.read_table("/tmp/_ac.parquet"); d = t.to_pandas()
check("无损模式 7 列", list(d.columns) == list(PARQUET_COLUMNS_FULL), str(list(d.columns)))
check("无损模式类型对齐",
      [t.schema.field(c).type for c in t.column_names] == [f.type for f in PARQUET_SCHEMA_FULL])
check("数值往返正确", np.allclose(d.price.values, [100000.0 + i * 0.5 for i in range(6000)]))
check("exchange_ts 往返正确",
      np.array_equal(d.exchange_ts.values, [1_700_000_000 + i for i in range(6000)]))
check("原始字符串往返正确",
      list(d.price_raw)[:3] == ["100000.%08d" % (i * 50) for i in range(3)])
check("price_raw 确为字符串类型", d.price_raw.dtype == object and isinstance(d.price_raw[0], str))

# 真正检验无损: 写入一个 float64 无法精确表示的 12 位定点小数
pp = make(keep_raw=True, batch=1)
raw_val = "123456789.123456789"
async def one_row(pipe):
    pipe._queue.put_nowait((float(raw_val), 1.0, 1, 2, 3, raw_val, "0.100000000123456789"))
    await pipe._flush([pipe._queue.get_nowait()])
    await pipe._drain_and_close()
pp.output = "/tmp/_ac_prec.parquet"
asyncio.run(one_row(pp))
rd = pq.read_table("/tmp/_ac_prec.parquet").to_pandas().iloc[0]
check("无损: 12 位定点小数原样保留", rd.price_raw == raw_val, "读回=%r" % rd.price_raw)
check("证明 float64 确实会丢精度 (对照)",
      Decimal(repr(float(raw_val))) != Decimal(raw_val),
      "float64=%r vs raw=%s" % (repr(float(raw_val)), raw_val))

pb = make(keep_raw=False, batch=5000)
pb.output = "/tmp/_ac_noraw.parquet"
asyncio.run(feed(pb))
t2 = pq.read_table("/tmp/_ac_noraw.parquet"); d2 = t2.to_pandas()
check("--no-raw 模式回到 PRD 5.2 四列", list(d2.columns) == list(PARQUET_COLUMNS), str(list(d2.columns)))

pe = make()
asyncio.run(pe._drain_and_close())
check("无数据收尾不报错且 writer 未创建", pe._writer is None)

print()
print("=" * 52)
if FAIL:
    print("❌ %d 项未通过: %s" % (len(FAIL), FAIL))
    sys.exit(1)
print("✅ 全部静态自检通过")
sys.exit(0)
PYEOF
```

### 1.2 运行

```bash
python /tmp/acceptance_check.py; echo "退出码: $?"
```

### 1.3 期望输出

```
== 1. 环形缓冲区 ==
  [PASS] 溢出覆盖: 保留最新 8 条
  [PASS] 整圈满载返回全部
  [PASS] 空缓冲区返回空数组
  [PASS] 越界请求被 clamp
== 2. 策略信号逻辑 (合成数据, 确定性) ==
  [PASS] z 高但动量同向时不触发
  [PASS] SELL: z>2.0 且 momentum<0
  [PASS] BUY: z<-2.0 且 momentum>0
  [PASS] 预热期 (数据不足 1000) 不判定信号
== 3. 落盘往返 ==
  [PASS] 无损模式 7 列 [...]
  [PASS] 无损模式类型对齐
  [PASS] 数值往返正确
  [PASS] exchange_ts 往返正确
  [PASS] 原始字符串往返正确
  [PASS] price_raw 确为字符串类型
  [PASS] 无损: 12 位定点小数原样保留 读回='123456789.123456789'
  [PASS] 证明 float64 确实会丢精度 (对照) float64='123456789.12345679' vs raw='123456789.123456789'
  [PASS] --no-raw 模式回到 PRD 5.2 四列 [...]
  [PASS] 无数据收尾不报错且 writer 未创建

====================================================
✅ 全部静态自检通过
退出码: 0
```

> ✅ **必须**:18 项全部 `PASS`,退出码 `0`。
>
> ⚠️ 任何一项 `FAIL` 都是缺陷。**特别是「空缓冲区返回空数组」——这一项对应的缺陷会让程序在启动初期读到未初始化的内存。**

---

## 阶段 2:功能测试

### TC-01 参数校验

**目的**:非法参数应被拒绝,而不是带着错误配置运行。

```bash
python hft_binance_demo.py --ring-capacity 100; echo "退出码: $?"
```

> ✅ **必须**:输出含 `ring_capacity(100) 必须 >= 因子窗口(1000)`,退出码 **`2`**。

```bash
python hft_binance_demo.py --persist-batch 999999 >/dev/null 2>&1; echo "退出码: $?"
```

> ✅ **必须**:退出码 **`2`**。

**日志文件不得被覆盖**——一次被拒绝的启动不该毁掉上次成功运行的日志:

```bash
cp system.log /tmp/_log_before.txt
python hft_binance_demo.py --ring-capacity 100 2>/dev/null
diff /tmp/_log_before.txt system.log && echo "✅ 日志未被覆盖"
```

> ✅ **必须**:输出 `✅ 日志未被覆盖`。
>
> ⚠️ 若此检查失败,说明参数校验发生在建日志文件之后——已建的文件会被 `mode="w"` 直接截断。本项目曾出现过该缺陷:一次 `--persist-batch 999999` 的误操作把整份 15 分钟运行日志覆盖成了一行报错。修复方式是将校验提到 `setup_logging()` 之前,错误改走 stderr。

---

### TC-02 快速冒烟运行(45 秒)

**目的**:端到端跑通,产出报告和落盘文件。

```bash
python hft_binance_demo.py --duration 45 --output /tmp/_tc.parquet --log-file /tmp/_tc.log
```

程序会持续打印 `📊 STATS` 观测行,45 秒后自动收尾并打印报告。

> ✅ **必须**:
> - 前几秒出现 `✅ WebSocket connected: wss://stream.binance.com:9443/ws/btcusdt@trade`
> - 出现 `📈 HFT-Demo 性能验证报告`
> - 最后一行 `✅ 运行结束`
> - 退出码 **`0`**
>
> 📊 **参考**:出现的 `⚠️ 连接中断 (InvalidStatus)` 警告。**这是 Binance 偶尔拒绝快速握手,程序会自动退避重连,不是缺陷。**
>
> ⚠️ 全程不应出现 `ERROR` 级别日志。

---

### TC-03 数据完整性(对应 PRD TC-04)

**目的**:收到的每一条都必须落盘,零丢失。

```bash
grep -E "总接收|总落盘|丢包率|完整性" /tmp/_tc.log
```

> ✅ **必须**:
> - `总接收 Tick 数` **等于** `总落盘 Tick 数`
> - `丢包率` 为 `0.0000%`
> - `数据完整性 (TC-04) : PASS`

**交叉验证**——从文件反查行数是否与报告一致:

```bash
python -c "import pyarrow.parquet as pq; print('文件行数:', len(pq.read_table('/tmp/_tc.parquet')))"
```

> ✅ **必须**:打印的行数**等于**报告里的 `总接收 Tick 数`。

---

### TC-04 延迟达标(对应 PRD TC-05 / NFR-1)

```bash
grep -E "平均端到端延迟|最大端到端延迟|样本数" /tmp/_tc.log
```

> ✅ **必须**:
> - `平均端到端延迟` **< 2.0 ms**,末尾显示 `PASS`
> - `最大端到端延迟` **< 15.0 ms**,末尾显示 `PASS`
>
> 📊 **参考**:实测值。本项目实测区间为平均 0.011–0.025 ms、最大 0.15–2.5 ms。数值绝对值依赖主机性能,只要低于阈值即达标。

---

### TC-05 无积压(对应 PRD NFR-1 吞吐)

```bash
grep "📊 STATS" /tmp/_tc.log
```

> ✅ **必须**:每一条 STATS 行的 `queue_depth=0` 且 `dropped=0`。
>
> 📊 **参考**:`tps` 数值。本项目实测 4–138 ticks/sec,**远低于** PRD 假设的 2000——这是交易所到达速率,不是缺陷。**判定标准是队列无积压,不是 tps 数字。**

---

### TC-06 落盘 Schema(默认无损模式)

```bash
python -c "
import pyarrow.parquet as pq
t = pq.read_table('/tmp/_tc.parquet')
print('列名:', list(t.column_names))
print('类型:', [f'{c}={t.schema.field(c).type}' for c in t.column_names])
"
```

> ✅ **必须**:列名与类型**精确**为:
>
> | 字段 | 类型 |
> | :--- | :--- |
> | `price` | `double` |
> | `quantity` | `double` |
> | `exchange_ts` | `int64` |
> | `recv_ts` | `int64` |
> | `recv_utc_ms` | `int64` |
> | `price_raw` | `string` |
> | `quantity_raw` | `string` |

---

### TC-07 数据质量

```bash
python -c "
import pyarrow.parquet as pq
df = pq.read_table('/tmp/_tc.parquet').to_pandas()
print('行数:', len(df))
print('空值总数:', int(df.isna().sum().sum()))
print('exchange_ts 单调递增:', bool(df.exchange_ts.is_monotonic_increasing))
print('recv_ts 单调递增:', bool(df.recv_ts.is_monotonic_increasing))
print()
print(df.tail(3).to_string())
"
```

> ✅ **必须**:
> - `空值总数: 0`
> - `exchange_ts 单调递增: True`
> - `recv_ts 单调递增: True`

---

### TC-08 精度无损(对应精度审计项)

**目的**:验证币安原始定点字符串被原样保留,而 `float64` 列确实存在精度损失。

```bash
python -c "
import pyarrow.parquet as pq
from decimal import Decimal
df = pq.read_table('/tmp/_tc.parquet').to_pandas()
print(df[['price', 'price_raw', 'quantity', 'quantity_raw']].head(3).to_string())
print()
# 真实 BTCUSDT 数据通常恰好落在 float64 精度内, 故丢失率可能为 0
lost = sum(1 for i in range(len(df)) if Decimal(repr(float(df.price_raw[i]))) != Decimal(df.price_raw[i]))
print('float64 与原文不一致的行数: %d / %d' % (lost, len(df)))
print('-> 该列为其他交易对准备的保险; 12 位以上定点小数必丢精度(阶段1已证明)')
"
```

> ✅ **必须**:`price_raw` 与 `quantity_raw` 是完整字符串(如 `80526.69000000`),且带币安的小数位格式。
>
> 📊 **参考**:BTCUSDT 上不一致行数大概率为 `0`——BTCUSDT 价格 2 位、数量 6 位小数,落在 float64 的 15–16 位有效数字内,本就精确。**这不是失败**。真正丢精度的证据在阶段 1 的「12 位定点小数原样保留」用例里。

---

### TC-09 时区列(对应时区审计项)

```bash
python -c "
import pyarrow.parquet as pq, datetime, pandas as pd
df = pq.read_table('/tmp/_tc.parquet').to_pandas()
r = df.iloc[-1]
print('exchange_ts ->', datetime.datetime.fromtimestamp(r.exchange_ts/1000, datetime.timezone.utc).isoformat())
print('recv_utc_ms ->', datetime.datetime.fromtimestamp(r.recv_utc_ms/1000, datetime.timezone.utc).isoformat())
print()
df['recv_time'] = pd.to_datetime(df.recv_utc_ms, unit='ms', utc=True).dt.tz_convert('Asia/Shanghai')
print('转 UTC+8 成功, 最新一行:', df['recv_time'].iloc[-1])
print()
print('recv_ts 是 perf_counter_ns (进程相对时间), 不可换算墙钟:')
print('  若误用 pd.to_datetime(recv_ts, unit=\"ns\") 会得到:',
      pd.to_datetime(int(r.recv_ts), unit='ns'))
"
```

> ✅ **必须**:
> - `exchange_ts` 与 `recv_utc_ms` 均能换算出合理时间(相差通常几毫秒到几十毫秒)
> - 转 `Asia/Shanghai` 成功
>
> ⚠️ **重要陷阱**:最后一段实测输出为 `1970-01-01 00:00:44.725...`。**这比"明显错误的年份"更危险**——`perf_counter_ns` 是进程启动后的相对纳秒数(几十亿量级),按 epoch 纳秒解释会落在 1970 年附近,看起来像一个完全合法的时间戳。你若把它写进数据库,不会报错,只是所有时间都会悄悄错掉。
>
> 这是**符合设计**的:`recv_ts` 是 `perf_counter_ns`(进程相对时间),仅用于计算延迟,不可用于展示。展示请用 `recv_utc_ms`。

---

### TC-10 `--no-raw` 模式

**目的**:验证可还原 PRD 5.2 的精确四列 Schema。

```bash
python hft_binance_demo.py --duration 20 --no-raw \
  --output /tmp/_tc_noraw.parquet --log-file /tmp/_tc_noraw.log >/dev/null 2>&1
python -c "
import pyarrow.parquet as pq
t = pq.read_table('/tmp/_tc_noraw.parquet')
print('列名:', list(t.column_names))
"
```

> ✅ **必须**:列名**精确**为 `['price', 'quantity', 'exchange_ts', 'recv_ts']`,共 4 列。

---

## 阶段 3:正式 15 分钟压测

### TC-11 启动

```bash
python hft_binance_demo.py
```

> ✅ **必须**:
> - 出现 `🚀 HFT-Demo 启动` 与 `配置:` `落盘精度:` 三行
> - 3 秒内出现 `✅ WebSocket connected`(对应 PRD TC-01)
>
> 📊 **参考**:若 Binance 握手抖动,可能出现一次 `⚠️ 连接中断 (InvalidStatus)` 后自动重连,此时连接耗时约 11 秒。实测中既出现过 1.26 秒首次成功,也出现过需 3 次握手。**只要最终连上即可**,不算失败。

### TC-12 观察运行(900 秒)

程序每 30 秒打印一行 `📊 STATS`。

**打开另一个终端观察:**

```bash
tail -f system.log
```

> ✅ **必须**观察到的三点:
> 1. `received` 持续增加
> 2. `queue_depth=0` 始终成立
> 3. `rss` **平稳**,无阶梯式上升
>
> 📊 **参考**:
> - `rss` 在首次落盘时会从约 51 MB 跳到约 95 MB(创建 `ParquetWriter` 缓冲区),**之后应保持平稳**。这是正常的一次性跳变,不是泄漏。
> - 本项目实测稳定在 93–95 MB,低于 PRD 的 150–250 MB 区间。

**观察 CPU(对应 PRD NFR-2):**

```bash
top -pid $(pgrep -n hft_binance_demo)   # macOS
```

> 📊 **参考**:单核占用。本项目未内置 CPU 采样,此项需你手动观察,应在 30% 以下。

### TC-13 收尾与报告(对应 PRD TC-02)

900 秒后程序自动收尾并打印报告。

> ✅ **必须**:
> - 出现 `进入收尾流程 (原因: 达到运行时长 900 秒)`
> - 出现 `📈 HFT-Demo 性能验证报告` 完整报告块
> - 最后一行 `✅ 运行结束`
> - 退出码 **`0`**
> - 全程**无** `ERROR` 级别日志(对应 PRD TC-02)
>
> 📊 **参考**:`WS 重连次数`。实测两次 15 分钟运行均为 `0`,但网络抖动时可能非 0,非缺陷。

### TC-14 信号(对应 PRD TC-03)

```bash
grep -c "🚨 SIGNAL" system.log
```

> ⚠️ **这一项极可能为 0,不要判为失败。**
>
> **原因**:短运行内 500-tick 采样点很少(45 秒约 1 个,15 分钟约 27–71 个),且策略要求「价格偏离 VWAP **且** 动量开始反转」的均值回归形态,本身罕见。本项目两次 15 分钟实测均为 0 信号;对同批数据逐 tick 复算显示 71 个采样点中有 20 个 |Z|>2,但**每一次动量方向都与 Z 相反或为 0**。
>
> ✅ **信号逻辑的正确性由阶段 1 的合成数据用例确定性证明**(SELL / BUY / 反向不触发 / 预热门控共 4 项)。
>
> 💡 **想在实盘中看到信号**,需临时修改源码顶部的常量:
>
> ```python
> ZSCORE_SELL = 2.0    # 降到 1.2
> ZSCORE_BUY  = -2.0   # 降到 -1.2
> PIPELINE_TRIGGER_TICKS = 500   # 降到 50, 加密采样
> ```
>
> 修改后运行更长时间(建议 30 分钟以上)并选波动较大的时段。**这属于调参以演示,不是修复缺陷。**

### TC-15 中断收尾

**目的**:验证 Ctrl+C 不会产生损坏文件、不丢报告。这是本项目修复过的真实缺陷。

```bash
# 另开一个终端
python hft_binance_demo.py --duration 300 --output /tmp/_tc_int.parquet --log-file /tmp/_tc_int.log &
sleep 20 && kill -INT %1
```

然后确认:

```bash
python -c "
import pyarrow.parquet as pq
df = pq.read_table('/tmp/_tc_int.parquet').to_pandas()
print('中断后文件仍可读, 行数:', len(df))
"
grep -E "进入收尾流程|总接收|总落盘|完整性" /tmp/_tc_int.log
```

> ✅ **必须**:
> - 日志出现 `进入收尾流程 (原因: 收到终止信号)`
> - 报告**仍被打印**
> - 落盘文件**可读**(证明 `close()` 写出了页脚元数据)
> - 接收数 == 落盘数
>
> ⚠️ 若此处文件报「not a Parquet file」或读取出错,说明页脚缺失——这是本项目曾出现过的缺陷,已修复。

---

## 判定汇总表

验收完成后填写:

| 编号 | 测试项 | 标记 | 阈值 | 实测 | 通过 |
| :--- | :--- | :---: | :--- | :--- | :---: |
| 1 | 静态自检 18 项 | ✅ | 全 PASS, 退出码 0 | | ☐ |
| TC-01 | 参数校验 | ✅ | 退出码 2 | | ☐ |
| TC-02 | 冒烟运行 | ✅ | 退出码 0, 报告打印 | | ☐ |
| TC-03 | 数据完整性 | ✅ | 接收 == 落盘, 丢包 0.0000% | | ☐ |
| TC-04 | 延迟达标 | ✅ | 平均 <2.0ms, 最大 <15.0ms | | ☐ |
| TC-05 | 无积压 | ✅ | queue_depth=0, dropped=0 | | ☐ |
| TC-06 | 落盘 Schema | ✅ | 7 列, 类型精确匹配 | | ☐ |
| TC-07 | 数据质量 | ✅ | 0 空值, 双时间戳单调 | | ☐ |
| TC-08 | 精度无损 | ✅ | 原始字符串完整保留 | | ☐ |
| TC-09 | 时区列 | ✅ | 可换算, 可转 Asia/Shanghai | | ☐ |
| TC-10 | --no-raw 模式 | ✅ | 精确 4 列 | | ☐ |
| TC-11 | 15 分钟启动 | ✅ | 3 秒内连接(抖动除外) | | ☐ |
| TC-12 | 运行稳定性 | ✅ | 无 ERROR, 内存平稳 | | ☐ |
| TC-13 | 收尾与报告 | ✅ | 退出码 0, 报告完整 | | ☐ |
| TC-14 | 信号 | 📊 | 由阶段 1 确定性证明 | | ☐ |
| TC-15 | 中断收尾 | ✅ | 文件可读, 报告打印 | | ☐ |

> 全部 ✅ 项通过即验收通过。📊 项仅供参考。

---

## 常见问题排查

### Q1:报错 `RuntimeError: There is no current event loop`

**原因**:Python 3.9 的 `asyncio.Queue()` 在构造时绑定当前事件循环。
**结论**:这是已知的兼容性问题,**当前代码已通过惰性创建队列修复**。如果你遇到这个错,说明跑的不是当前版本源码,请确认 `hft_binance_demo.py` 是最新版本。

### Q2:日志出现 `⚠️ 连接中断 (InvalidStatus)`

**原因**:Binance 偶尔拒绝快速握手(单 IP 速率限制)。
**结论**:**不是缺陷**。程序按 2 秒起指数退避(上限 30 秒,含 ±25% 抖动)自动重连。只要最终出现 `✅ WebSocket connected` 即可。

### Q3:吞吐率远低于 PRD 假设的 2000 ticks/sec

**原因**:PRD 的速率假设不成立。Binance BTCUSDT 现货实测 4–138 ticks/sec。
**结论**:**不是缺陷**。判定标准是「队列深度 0 且丢弃 0」,而非 tps 数值。

### Q4:连不上 Binance

依次尝试:

```bash
# 1. 确认能访问交易所 API
curl -s --max-time 10 https://api.binance.com/api/v3/time

# 2. 确认 WebSocket 端口可达
nc -z -w 5 stream.binance.com 9443 && echo "端口可达"

# 3. 改用镜像端点
python hft_binance_demo.py --stream-url wss://<你的镜像>/ws/btcusdt@trade
```

> 💡 注意:程序只订阅单个交易对。若需多交易对,应用**组合流**(`wss://.../ws/btcusdt@trade/ethusdt@trade`),而不是开多个连接——Binance 单 IP 最多 5 个 WebSocket 连接。

### Q5:提示 `uvloop 不可用`

**结论**:**不是缺陷**。uvloop 为可选优化,缺失时自动回退默认事件循环,功能与性能均不受实质影响。也可用 `--no-uvloop` 显式关闭。

### Q6:第一次运行内存约 51 MB,落盘后跳到 95 MB

**结论**:**不是泄漏**。这是首次创建 `ParquetWriter` 时分配缓冲区的一次性跳变。**关键在于跳变之后是否平稳**——平稳即无泄漏。

### Q7:中断后发现文件读不出来

**结论**:这是**已修复的缺陷**。修复前取消运行会跳过 `ParquetWriter.close()`,而 `close()` 才负责写页脚元数据,导致文件不可读。修复后收尾逻辑在 `run()` 的 `finally` 中执行。若你遇到此问题,请确认是最新版本源码,并参考 TC-15 复测。

### Q8:启动期读到内存中的随机价格值

**结论**:这是**已修复的缺陷**。修复前空环形缓冲区的 `get_latest()` 会落入跨边界分支,`slice(start, capacity)` 返回整段未初始化内存而非空数组。阶段 1 的「空缓冲区返回空数组」用例专门覆盖此项。

### Q9:`system.log` 只有一行 ERROR,15 分钟的日志没了

**结论**:这是**已修复的缺陷**。修复前 `main()` 的顺序是「建日志文件 → 校验参数」,而日志文件以 `mode="w"` 打开会立即截断。所以一次参数写错的启动,就会把上次成功运行的完整日志覆盖成一行报错。

修复后:参数校验在 `setup_logging()` 之前执行,校验失败走 stderr,**不创建日志文件**。TC-01 的「日志文件不得被覆盖」检查专门覆盖此项。

> 💡 如果你现在看到的现象是 `system.log` 只有一行 `persist_batch(...) 必须在 [1, 100000] 区间`,那是修复前留下的残留文件——按 TC-01 重新验证即可,重新跑一次正式压测也会重建它。

---

## 清理

```bash
# 运行产物 (已被 .gitignore 忽略)
rm -f market_data.parquet system.log

# 临时测试文件
rm -f /tmp/_tc*.parquet /tmp/_tc*.log /tmp/_tc.parquet \
      /tmp/_ac*.parquet /tmp/_acceptance_check.py /tmp/acceptance_check.py

# 如需重置虚拟环境
deactivate && rm -rf .venv && python3 -m venv .venv && source .venv/bin/activate
```

---

## 附:本项目已知局限(验收时请知悉)

1. **CPU 占用未内置采样**(PRD NFR-2 的 CPU 项需手动用 `top` 观察)。
2. **P99 延迟未单独计算**。延迟统计器为 O(1) 内存设计,只保留 count/sum/peak,以最大延迟替代 P99。这符合本项目「避免动态内存分配」的前提,但严格验收需改为 t-digest 或周期直方图。
3. **不含订单路由与撮合**(PRD 明确 Out-of-Scope)。信号仅打印日志,不产生任何订单。
4. **无测试框架与 CI**。验收依赖本文档的静态自检脚本 + 实盘运行。
5. **单机单进程**,分布式部署不在范围内。
