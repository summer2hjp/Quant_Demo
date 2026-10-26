#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HFT-Demo —— 实时高频行情处理与交易系统 Demo
================================================================

数据链路 (PRD 2.1 逻辑视图):

    Binance WebSocket  wss://stream.binance.com:9443/ws/btcusdt@trade  (~2000 ticks/sec)
        │  字节预过滤 b'"e":"trade"'  +  orjson 极速解析
        │  perf_counter_ns() 记录本地纳秒时间戳
        ▼
    ┌──────────────┐   每 500 ticks   ┌──────────────────────────┐
    │  Ring Buffer │ ───────────────► │  因子 / 策略引擎          │
    │ 2,000,000    │                  │  VWAP / std / momentum    │
    │ 预分配 numpy │                  └──────────┬───────────────┘
    └──────┬───────┘                             ▼
           │                        信号输出 (记录端到端延迟)
           ▼
    asyncio.Queue(100,000) ──每 5,000 条──► ParquetWriter  (run_in_executor 线程池)
                                                  │
                                                  ▼
                                          market_data.parquet

设计红线 (重构前务必对齐, 详见 CLAUDE.md):
  * 热路径零动态扩容 —— Ring Buffer 固定容量, 溢出时覆盖最旧数据
  * 磁盘 I/O 永不阻塞事件循环 —— 一律 loop.run_in_executor
  * 队列满 = 丢弃并计数, 绝不反压行情接收
  * ParquetWriter 句柄常驻, 收尾时显式 close() 才能写出行尾页脚元数据
  * recv_ts 为纳秒 (perf_counter_ns), exchange_ts 为交易所毫秒, 两套量纲不可互减
  * 精度分层 —— float64 仅供向量化因子计算; 落盘必须保留币安原始字符串, 否则定点
    小数转二进制在小数点后的精度损失在入口处就永久无法挽回
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import random
import signal
import sys
import time
from typing import List, Optional, Tuple

import numpy as np
import orjson
import pyarrow as pa
import pyarrow.parquet as pq
import websockets

# --------------------------------------------------------------------------
# 常量与默认配置 (PRD 2.2 / 3.x / 4.x)
# --------------------------------------------------------------------------
TRADE_TAG = b'"e":"trade"'          # FR-1.2 字节预过滤标记
STREAM_TEMPLATE = "wss://stream.binance.com:9443/ws/{symbol}@trade"  # FR-1.1

RING_CAPACITY = 2_000_000           # FR-2.1 环形缓冲区固定容量
PIPELINE_TRIGGER_TICKS = 500        # FR-2.3 策略管线触发间隔
VWAP_WINDOW = 1000                  # FR-2.4 VWAP / std 窗口
MOMENTUM_WINDOW = 10                # FR-2.4 动量窗口
ZSCORE_SELL = 2.0                   # FR-3.1 SELL 阈值
ZSCORE_BUY = -2.0                   # FR-3.1 BUY 阈值

QUEUE_MAXSIZE = 100_000             # FR-4.1 落盘队列容量
PERSIST_BATCH = 5000                # FR-4.2 微批落盘行数
PERSIST_FLUSH_TIMEOUT = 60.0        # 流速率低迷时的兜底刷盘间隔 (秒)

RECONNECT_BASE_SECONDS = 2.0        # FR-1.4 重连基准退避
RECONNECT_MAX_SECONDS = 30.0        # 退避上限
RUN_SECONDS = 900.0                 # 15 分钟压力测试
STATS_INTERVAL_SECONDS = 30.0       # 观测日志间隔

# 落盘 Schema (PRD 5.2 核心四列)
PARQUET_COLUMNS = ("price", "quantity", "exchange_ts", "recv_ts")
PARQUET_SCHEMA = pa.schema([
    pa.field("price", pa.float64()),
    pa.field("quantity", pa.float64()),
    pa.field("exchange_ts", pa.int64()),
    pa.field("recv_ts", pa.int64()),
])

# 扩展 Schema (默认启用, --no-raw 可关闭回到 PRD 5.2 精确四列)
#   recv_utc_ms     本地接收的 UTC 墙钟毫秒。recv_ts 是 perf_counter_ns (进程相对时间),
#                   无法换算成人能读的时间; 入库展示必须用这个字段。
#   price_raw / quantity_raw  币安原始字符串 ("16738.33000000")。float64 无法精确表示
#                   这类定点小数, 二进制转换在入口处就永久丢失了精度 —— 因子计算用
#                   float64 是为了 numpy 向量化 (PRD 的核心设计), 但落盘必须无损保留
#                   原值, 供下游做 Decimal / 定点数运算。
PARQUET_COLUMNS_FULL = PARQUET_COLUMNS + ("recv_utc_ms", "price_raw", "quantity_raw")
PARQUET_SCHEMA_FULL = pa.schema(
    list(PARQUET_SCHEMA)
    + [
        pa.field("recv_utc_ms", pa.int64()),
        pa.field("price_raw", pa.string()),
        pa.field("quantity_raw", pa.string()),
    ]
)


# --------------------------------------------------------------------------
# 工具
# --------------------------------------------------------------------------
def setup_logging(log_file: str) -> logging.Logger:
    """控制台 + 文件双通道结构化日志 (NFR-3)。"""
    logger = logging.getLogger("hft")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False
    fmt = logging.Formatter(
        "%(asctime)s.%(msecs)03d %(levelname)-7s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    logger.addHandler(console)
    # 每次运行重置日志文件: 交付物是「单次 15 分钟测试」的完整日志, 混入旧运行会误导验收。
    fileh = logging.FileHandler(log_file, mode="w", encoding="utf-8")
    fileh.setFormatter(fmt)
    logger.addHandler(fileh)
    return logger


def peak_rss_mb() -> Optional[float]:
    """峰值 RSS (MB)。macOS 的 ru_maxrss 单位是字节, Linux 是 KB —— 必须区分。"""
    try:
        import resource
        raw = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except Exception:
        return None
    if sys.platform == "darwin":
        return raw / (1024.0 * 1024.0)
    return raw / 1024.0


def try_install_uvloop() -> bool:
    """可选: 用 uvloop 替换默认事件循环 (PRD 2.2)。失败时静默回退, 不影响功能。"""
    try:
        import uvloop
        uvloop.install()
        return True
    except Exception as exc:  # 依赖缺失 / 平台不支持 / 已有事件循环运行
        logging.getLogger("hft").warning("uvloop 不可用, 使用默认事件循环: %s", exc)
        return False


class LatencyTracker:
    """O(1) 内存的延迟统计器。

    每秒数千 tick, 若把每次延迟 append 进列表, 15 分钟会产生上百万对象 ——
    这正是 PRD 背景章节要避免的动态内存分配。这里只保留 count / sum / peak。
    """

    __slots__ = ("count", "total", "peak")

    def __init__(self) -> None:
        self.count = 0
        self.total = 0.0
        self.peak = 0.0

    def add_ms(self, ms: float) -> None:
        self.count += 1
        self.total += ms
        if ms > self.peak:
            self.peak = ms

    @property
    def mean_ms(self) -> float:
        return self.total / self.count if self.count else 0.0


# --------------------------------------------------------------------------
# 环形缓冲区 (FR-2.1 / FR-2.2)
# --------------------------------------------------------------------------
class RingBuffer:
    """预分配 numpy 数组的定容环形缓冲区。

    四条连续数组 (struct-of-arrays) 而非一条对象数组: 既保证向量化运算零拷贝,
    也彻底避开 Python 对象装箱与 GC 停顿。容量在构造时定死, 永不扩容。
    """

    __slots__ = ("prices", "quantities", "exchange_ts", "recv_ts", "capacity", "head", "count")

    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self.prices = np.empty(capacity, dtype=np.float64)
        self.quantities = np.empty(capacity, dtype=np.float64)
        self.exchange_ts = np.empty(capacity, dtype=np.int64)
        self.recv_ts = np.empty(capacity, dtype=np.int64)
        self.head = 0   # 下一个写入位置
        self.count = 0  # 当前有效条数 (未满时为实际条数, 已满后恒等于 capacity)

    def put(self, price: float, quantity: float, exchange_ts: int, recv_ts: int) -> None:
        """写入一条行情; 缓冲区已满时原地覆盖最旧数据 (FR-2.2)。"""
        i = self.head
        self.prices[i] = price
        self.quantities[i] = quantity
        self.exchange_ts[i] = exchange_ts
        self.recv_ts[i] = recv_ts
        self.head = (i + 1) % self.capacity
        # 未满时累加有效条数; 已满后 head 循环覆盖旧数据, 有效条数恒等于容量。
        self.count = (self.count + 1) if self.count < self.capacity else self.capacity

    def get_latest(self, n: int) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """返回最近 n 条有效数据, 顺序为 旧 -> 新。

        未跨边界时直接返回视图 (零拷贝、零分配); 跨边界时才用 np.concatenate
        拼成连续数组 (PRD 风险表「Numpy 环形边界切片错误」的应对)。
        """
        n = min(n, self.count)
        if n == 0:
            # 空缓冲区。注意此处不能走下面的跨边界分支: start == end 时
            # slice(start, capacity) 会返回整段未初始化内存, 而非空数组。
            empty = np.empty(0)
            return empty, empty.copy(), empty.copy(), empty.copy()

        end = self.head                                # 独占边界
        start = (end - n) % self.capacity
        if start < end:
            return (
                self.prices[start:end],
                self.quantities[start:end],
                self.exchange_ts[start:end],
                self.recv_ts[start:end],
            )
        seg_a, seg_b = slice(start, self.capacity), slice(0, end)
        return (
            np.concatenate([self.prices[seg_a], self.prices[seg_b]]),
            np.concatenate([self.quantities[seg_a], self.quantities[seg_b]]),
            np.concatenate([self.exchange_ts[seg_a], self.exchange_ts[seg_b]]),
            np.concatenate([self.recv_ts[seg_a], self.recv_ts[seg_b]]),
        )


# --------------------------------------------------------------------------
# 管线主体
# --------------------------------------------------------------------------
class HFTPipeline:
    def __init__(self, args: argparse.Namespace, logger: logging.Logger) -> None:
        self.logger = logger
        self.symbol = args.symbol.upper()
        self.stream_url = args.stream_url or STREAM_TEMPLATE.format(symbol=self.symbol.lower())
        self.output = args.output
        self.duration = args.duration
        self.vwap_window = VWAP_WINDOW
        self.momentum_window = MOMENTUM_WINDOW
        self.zsell = ZSCORE_SELL
        self.zbuy = ZSCORE_BUY
        self.persist_batch = args.persist_batch
        self.queue_maxsize = args.queue_maxsize
        self.flush_timeout = args.persist_flush_timeout
        self.reconnect_base = args.reconnect_base
        self.reconnect_max = args.reconnect_max
        self.stats_interval = args.stats_interval
        self.keep_raw = args.keep_raw                    # 无损保留币安原始字符串
        self._schema = PARQUET_SCHEMA_FULL if args.keep_raw else PARQUET_SCHEMA

        self._ring = RingBuffer(args.ring_capacity)
        self.__queue: Optional["asyncio.Queue[Tuple[float, float, int, int]]"] = None

        # 统计口径
        self.start_ts = time.perf_counter()
        self.received = 0
        self.persisted = 0
        self.dropped_persist = 0        # 队列满导致丢弃的条数
        self.non_trade = 0              # 被字节预过滤掉的消息数
        self.evals = 0
        self.flushes = 0
        self.reconnects = 0
        self.signals_buy = 0
        self.signals_sell = 0
        self._warm_logged = False

        self.ingest_latency = LatencyTracker()   # 接收 -> 入环形缓冲区
        self.signal_latency = LatencyTracker()   # 信号 tick 的 recv_ts -> 信号产出 (FR-3.2)

        self._window_received = 0                # 观测窗口内的接收计数 (算瞬时吞吐)
        self._window_peak_tps = 0.0

        self._writer: Optional[pq.ParquetWriter] = None
        self._closed = False

    @property
    def _queue(self) -> "asyncio.Queue[Tuple[float, float, int, int]]":
        """惰性创建落盘队列 (FR-4.1)。

        Python 3.9 的 asyncio.Queue 在构造时会调用 get_event_loop() 并绑定当前循环,
        因此在 asyncio.run() 之外实例化整个管线会抛 RuntimeError。惰性创建让 HFTPipeline
        可以在事件循环外安全构造 (也便于单组件测试)。
        """
        if self.__queue is None:
            self.__queue = asyncio.Queue(maxsize=self.queue_maxsize)
        return self.__queue

    # ---------------- 数据摄取 (FR-1) ----------------
    async def run(self) -> None:
        """管线生命周期入口。

        收尾与报告放进 finally: 即使被取消 (Ctrl+C) 或异常退出, 也必须写出 Parquet
        页脚元数据并打印验收报告 —— 否则产物是不可读的半成品 (PRD 风险表「Parquet
        文件损坏」), 且 NFR-3 要求的性能报告就丢了。
        """
        try:
            await self._run_loop()
        finally:
            try:
                await self._drain_and_close()
            except Exception:
                self.logger.exception("收尾阶段异常")
            self.report()

    async def _run_loop(self) -> None:
        loop = asyncio.get_running_loop()
        stop = asyncio.Event()
        try:
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            pass  # Windows 或不支持 add_signal_handler 的平台

        ingest_task = asyncio.create_task(self._ingest_loop(), name="ingest")
        persist_task = asyncio.create_task(self._persist_loop(), name="persist")
        stats_task = asyncio.create_task(self._stats_loop(), name="stats")
        timeout_task = asyncio.create_task(asyncio.sleep(self.duration), name="timeout")
        stop_task = asyncio.create_task(stop.wait(), name="stop")
        all_tasks = [ingest_task, persist_task, stats_task, timeout_task, stop_task]

        try:
            done, _pending = await asyncio.wait(all_tasks, return_when=asyncio.FIRST_COMPLETED)
            if timeout_task in done:
                reason = "达到运行时长 %.0f 秒" % self.duration
            elif stop.is_set():
                reason = "收到终止信号"
            else:
                reason = "工作协程异常退出"
            for task in done:
                if not task.cancelled() and task.exception() is not None:
                    self.logger.error("协程 %s 异常退出: %r", task.get_name(), task.exception())
        except asyncio.CancelledError:
            # 整个管线被取消 (Ctrl+C): 走清理路径, 但不向外抛出 ——
            # run() 的 finally 随后会完成落盘收尾并打印报告, 让退出保持有序。
            reason = "外部取消 (Ctrl+C)"
        finally:
            self.logger.info("进入收尾流程 (原因: %s)", reason)
            for task in all_tasks:
                if not task.done():
                    task.cancel()
            # 等 persist 协程把半成品批次刷完 (它在 CancelledError 分支里收尾),
            # 之后由 run() 的 finally 统一执行 _drain_and_close() 关闭 writer。
            await asyncio.gather(*all_tasks, return_exceptions=True)

    async def _ingest_loop(self) -> None:
        """连接 + 断线重连外壳 (FR-1.4: 2 秒退避, 指数增长)。"""
        backoff = self.reconnect_base
        while True:
            try:
                async with websockets.connect(
                    self.stream_url,
                    open_timeout=10,
                    close_timeout=2,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=1 << 20,
                ) as ws:
                    self.logger.info("✅ WebSocket connected: %s", self.stream_url)
                    backoff = self.reconnect_base
                    await self._consume(ws)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # ConnectionClosed / OSError / asyncio.TimeoutError 都从这里兜住。
                # 币安对单 IP 的连接建立有速率限制, 国内网络抖动时大量进程同时
                # "准点"重连会互相加剧失败, 故在指数退避上叠加 ±25% 抖动。
                self.reconnects += 1
                sleep_for = backoff * random.uniform(0.75, 1.25)
                self.logger.warning("⚠️ 连接中断 (%s); %.1f 秒后重连", type(exc).__name__, sleep_for)
                try:
                    await asyncio.sleep(sleep_for)
                except asyncio.CancelledError:
                    raise
                backoff = min(backoff * 2, self.reconnect_max)

    async def _consume(self, ws) -> None:
        """单连接消息消费热路径。每一行都为纳秒级延迟预算让路。"""
        while True:
            raw = await ws.recv()
            if isinstance(raw, str):
                raw = raw.encode("utf-8")
            # FR-1.2: 先做 C 级子串匹配再解析, 避免为非交易消息付出 orjson 代价。
            if TRADE_TAG not in raw:
                self.non_trade += 1
                continue

            recv_ts = time.perf_counter_ns()          # FR-1.3 本地纳秒时间戳
            msg = orjson.loads(raw)
            # 币安以字符串下发定点小数 (PRD 5.3, 如 "16738.33000000")。float() 仅用于
            # numpy 向量化因子计算; 原始字符串必须一路带到落盘, 否则二进制转换的精度
            # 损失在入口处就永久无法挽回。
            price_raw = msg["p"]
            qty_raw = msg["q"]
            price = float(price_raw)
            quantity = float(qty_raw)
            exchange_ts = int(msg["T"])               # UTC 毫秒
            recv_utc_ms = time.time_ns() // 1_000_000 if self.keep_raw else 0

            self._ring.put(price, quantity, exchange_ts, recv_ts)
            self.received += 1
            self._window_received += 1

            row = (price, quantity, exchange_ts, recv_ts,
                   recv_utc_ms, price_raw, qty_raw) if self.keep_raw \
                else (price, quantity, exchange_ts, recv_ts)
            try:
                self._queue.put_nowait(row)
            except asyncio.QueueFull:
                # PRD 风险表: 队列满只丢弃并计数, 绝不阻塞行情接收。
                self.dropped_persist += 1

            if self.received % PIPELINE_TRIGGER_TICKS == 0:
                self._evaluate()

            self.ingest_latency.add_ms((time.perf_counter_ns() - recv_ts) * 1e-6)

    # ---------------- 因子与策略 (FR-2.4 / FR-3) ----------------
    def _evaluate(self) -> None:
        """按固定窗口向量化计算因子并判定信号。在事件循环线程内同步执行。"""
        self.evals += 1
        window = max(self.vwap_window, self.momentum_window)
        prices, qtys, ex_ts, recv_ts = self._ring.get_latest(window)
        n = prices.size

        if n < self.vwap_window:
            if not self._warm_logged:
                self.logger.info("⏳ 因子引擎预热中: 已有 %d/%d ticks, 暂不判定信号",
                                 n, self.vwap_window)
                self._warm_logged = True
            return

        vwap_p = prices[-self.vwap_window:]
        vwap_q = qtys[-self.vwap_window:]
        momentum_p = prices[-self.momentum_window:]

        current = float(vwap_p[-1])
        denom = float(vwap_q.sum())
        if denom <= 0.0:
            return
        vwap = float(np.dot(vwap_p, vwap_q) / denom)
        std = float(np.std(vwap_p))
        if std <= 1e-12:
            return
        z = (current - vwap) / std
        # 窗口首尾价差: 过去 MOMENTUM_WINDOW 个 tick 的方向性变化。
        momentum = float(momentum_p[-1] - momentum_p[0])

        direction = None
        if z > self.zsell and momentum < 0:
            direction = "SELL"
        elif z < self.zbuy and momentum > 0:
            direction = "BUY"
        if direction is None:
            return

        latest_recv_ts = int(recv_ts[-1])
        latency_ms = (time.perf_counter_ns() - latest_recv_ts) * 1e-6   # FR-3.2
        self.signal_latency.add_ms(latency_ms)
        if direction == "BUY":
            self.signals_buy += 1
        else:
            self.signals_sell += 1

        self.logger.info(
            "🚨 SIGNAL %-4s symbol=%s price=%.2f vwap=%.2f z=%.3f momentum=%.2f "
            "latency=%.3f ms exchange_ts=%d recv_ts=%d",
            direction, self.symbol, current, vwap, z, momentum, latency_ms,
            int(ex_ts[-1]), latest_recv_ts,
        )

    # ---------------- 数据持久化 (FR-4) ----------------
    async def _persist_loop(self) -> None:
        """微批落盘协程。每 PERSIST_BATCH 条刷一次盘。"""
        batch: List[Tuple[float, float, int, int]] = []
        try:
            while True:
                item = await self._get_row_or_timeout()
                if item is None:
                    if batch:                     # 流停滞: 兜底刷盘, 避免数据滞留内存
                        await self._flush(batch)
                        batch = []
                    continue
                batch.append(item)
                if len(batch) >= self.persist_batch:
                    await self._flush(batch)
                    batch = []
        except asyncio.CancelledError:
            if batch:                             # 收尾: 把半成品批次落完
                await self._flush(batch)
            raise

    async def _get_row_or_timeout(self):
        try:
            return await asyncio.wait_for(self._queue.get(), timeout=self.flush_timeout)
        except asyncio.TimeoutError:
            return None

    async def _flush(self, batch: List[Tuple[float, float, int, int]]) -> None:
        if not batch:
            return
        loop = asyncio.get_running_loop()
        try:
            written = await loop.run_in_executor(None, self._write_batch, batch)
        except Exception:
            self.logger.exception("❌ 落盘失败, 丢弃 %d 条", len(batch))
            self.dropped_persist += len(batch)
            return
        self.persisted += written
        self.flushes += 1

    def _write_batch(self, batch: List[Tuple[float, float, int, int]]) -> int:
        """执行于后台线程池: 组装 pyarrow 表并追加写入。不触碰事件循环。

        此处把 micro-batch 的 Python 元组转成 numpy/pyarrow 是刻意为之 —— 转换成本
        被摊薄到每 5000 条一次, 而不是每 tick 一次, 因此不构成热路径。
        """
        if self.keep_raw:
            prices, qtys, ex_ts, recv_ts, recv_utc_ms, price_raw, qty_raw = zip(*batch)
            arrays = [
                pa.array(prices, type=pa.float64()),
                pa.array(qtys, type=pa.float64()),
                pa.array(ex_ts, type=pa.int64()),
                pa.array(recv_ts, type=pa.int64()),
                pa.array(recv_utc_ms, type=pa.int64()),
                pa.array(price_raw, type=pa.string()),
                pa.array(qty_raw, type=pa.string()),
            ]
            names = PARQUET_COLUMNS_FULL
        else:
            prices, qtys, ex_ts, recv_ts = zip(*batch)
            arrays = [
                pa.array(prices, type=pa.float64()),
                pa.array(qtys, type=pa.float64()),
                pa.array(ex_ts, type=pa.int64()),
                pa.array(recv_ts, type=pa.int64()),
            ]
            names = PARQUET_COLUMNS
        table = pa.Table.from_arrays(arrays, names=names)

        # FR-4.3: 句柄常驻, 只追加写入。首次刷盘时惰性创建, 避开 0 行文件的边界情况。
        if self._writer is None:
            self._writer = pq.ParquetWriter(self.output, self._schema, compression="snappy")
        self._writer.write_table(table)
        # ParquetWriter 并非所有版本都暴露 flush(); 有则调用, 让内核页缓存尽早落盘。
        flush = getattr(self._writer, "flush", None)
        if callable(flush):
            flush()
        return len(batch)

    async def _drain_and_close(self) -> None:
        """收尾: 抽干队列残余 -> 落盘 -> 显式 close() writer。

        顺序不可颠倒: close() 负责写页脚元数据, 早于最后一批写入调用会得到
        无法读取的损坏文件 (PRD 风险表「Parquet 文件损坏」)。

        幂等: run() 的 finally 可能二次调用 (正常收尾时已执行过一次),
        而 ParquetWriter.close() 不可重复调用。
        """
        if self._closed:
            return
        self._closed = True

        remaining: List[Tuple[float, float, int, int]] = []
        while not self._queue.empty():
            try:
                remaining.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        if remaining:
            for i in range(0, len(remaining), self.persist_batch):
                await self._flush(remaining[i:i + self.persist_batch])

        if self._writer is not None:
            loop = asyncio.get_running_loop()
            try:
                await loop.run_in_executor(None, self._writer.close)
            except Exception:
                self.logger.exception("❌ 关闭 ParquetWriter 失败, market_data.parquet 可能损坏")
                self._writer = None

        await asyncio.sleep(0)  # 让出事件循环, 确保落盘线程上的日志先刷出来

    # ---------------- 可观测性 (NFR-3) ----------------
    async def _stats_loop(self) -> None:
        while True:
            await asyncio.sleep(self.stats_interval)
            self._log_stats()

    def _log_stats(self) -> None:
        elapsed = time.perf_counter() - self.start_ts
        interval_tps = self._window_received / self.stats_interval
        if interval_tps > self._window_peak_tps:
            self._window_peak_tps = interval_tps
        self._window_received = 0

        self.logger.info(
            "📊 STATS received=%d persisted=%d queue_depth=%d dropped=%d "
            "signals=%d (BUY %d / SELL %d) tps=%.0f peak_tps=%.0f "
            "ingest_lat_avg=%.3f max=%.3f ms rss=%.1f MB",
            self.received, self.persisted, self._queue.qsize(), self.dropped_persist,
            self.signals_buy + self.signals_sell, self.signals_buy, self.signals_sell,
            interval_tps, self._window_peak_tps,
            self.ingest_latency.mean_ms, self.ingest_latency.peak,
            peak_rss_mb() or -1.0,
        )

    # ---------------- 性能验证报告 (NFR-3 / 交付物 5) ----------------
    def report(self) -> None:
        elapsed = time.perf_counter() - self.start_ts
        total = self.received
        drop_rate = (self.dropped_persist / total * 100.0) if total else 0.0
        avg_tps = total / elapsed if elapsed else 0.0
        integrity = "PASS" if total == self.persisted else "FAIL"
        lat_avg_ok = self.ingest_latency.mean_ms < 2.0
        lat_max_ok = self.ingest_latency.peak < 15.0

        lines = [
            "=" * 62,
            "                    📈 HFT-Demo 性能验证报告",
            "=" * 62,
            "运行时长                 : %10.2f s" % elapsed,
            "总接收 Tick 数           : %12d" % total,
            "总落盘 Tick 数           : %12d" % self.persisted,
            "落盘丢弃条数             : %12d" % self.dropped_persist,
            "丢包率                   : %11.4f%%" % drop_rate,
            "数据完整性 (TC-04)       : %10s  (received == persisted)" % integrity,
            "-" * 62,
            "平均处理速率             : %10.1f ticks/sec" % avg_tps,
            "峰值窗口处理速率         : %10.1f ticks/sec" % self._window_peak_tps,
            "策略管线触发次数         : %12d" % self.evals,
            "信号总数 (BUY / SELL)    : %8d (%4d / %4d)"
            % (self.signals_buy + self.signals_sell, self.signals_buy, self.signals_sell),
            "-" * 62,
            "平均端到端延迟 (NFR-1)   : %9.3f ms   目标 < 2.0 ms   %s"
            % (self.ingest_latency.mean_ms, "PASS" if lat_avg_ok else "FAIL"),
            "最大端到端延迟           : %9.3f ms   目标 < 15.0 ms  %s"
            % (self.ingest_latency.peak, "PASS" if lat_max_ok else "FAIL"),
            "样本数                   : %12d" % self.ingest_latency.count,
            "信号延迟 平均/最大       : %8.3f ms / %8.3f ms  (n=%d)"
            % (self.signal_latency.mean_ms, self.signal_latency.peak, self.signal_latency.count),
            "-" * 62,
            "峰值 RSS (NFR-2)         : %9.1f MB     目标 150 - 250 MB" % (peak_rss_mb() or -1.0),
            "WS 重连次数              : %12d" % self.reconnects,
            "过滤非交易消息数         : %12d" % self.non_trade,
            "落盘批次数 (微批)        : %12d" % self.flushes,
            "落盘 Schema              : %s"
            % (",".join(PARQUET_COLUMNS_FULL if self.keep_raw else PARQUET_COLUMNS)),
            "落盘文件                 : %s" % self.output,
            "=" * 62,
        ]
        for line in lines:
            self.logger.info(line)


# --------------------------------------------------------------------------
# 入口
# --------------------------------------------------------------------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="HFT-Demo: Binance btcusdt@trade 实时行情 -> 因子 -> 信号 -> Parquet",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--duration", type=float, default=RUN_SECONDS,
                   help="连续运行秒数 (15 分钟压力测试为 900)")
    p.add_argument("--output", default="market_data.parquet", help="落盘 Parquet 路径")
    p.add_argument("--log-file", default="system.log", help="系统日志路径")
    p.add_argument("--symbol", default="BTCUSDT", help="交易对")
    p.add_argument("--stream-url", default=None,
                   help="覆盖默认 WS 地址 (默认按 symbol 拼 stream.binance.com:9443)")
    p.add_argument("--ring-capacity", type=int, default=RING_CAPACITY, help="环形缓冲区容量")
    p.add_argument("--persist-batch", type=int, default=PERSIST_BATCH, help="微批落盘行数")
    p.add_argument("--queue-maxsize", type=int, default=QUEUE_MAXSIZE,
                   help="落盘队列容量; 直接决定内存上限 (每条 row 约 350 字节)")
    p.add_argument("--persist-flush-timeout", type=float, default=PERSIST_FLUSH_TIMEOUT,
                   help="无新数据时兜底刷盘间隔秒数, 低频期防止残余数据滞留")
    p.add_argument("--reconnect-base", type=float, default=RECONNECT_BASE_SECONDS,
                   help="断线重连基准退避秒数 (指数增长, 叠加抖动)")
    p.add_argument("--reconnect-max", type=float, default=RECONNECT_MAX_SECONDS,
                   help="重连退避上限秒数")
    p.add_argument("--stats-interval", type=float, default=STATS_INTERVAL_SECONDS,
                   help="周期观测日志间隔秒数")
    p.add_argument("--no-uvloop", action="store_true", help="禁用 uvloop, 使用默认事件循环")
    p.add_argument("--no-raw", action="store_true",
                   help="落盘仅保留 PRD 5.2 的四个字段; 默认追加 recv_utc_ms / price_raw / "
                        "quantity_raw 以无损保留币安原始精度")
    args = p.parse_args(argv)
    args.keep_raw = not args.no_raw
    return args


def validate_args(args: argparse.Namespace) -> Optional[str]:
    """校验命令行参数, 返回错误信息; 通过则返回 None。

    必须在 setup_logging() 之前调用: 建日志文件用 mode="w" 会直接截断,
    一次被拒绝的启动不该毁掉上一次成功运行的日志。
    """
    if args.ring_capacity < max(VWAP_WINDOW, MOMENTUM_WINDOW):
        return "ring_capacity(%d) 必须 >= 因子窗口(%d), 否则无法计算 VWAP" % (
            args.ring_capacity, VWAP_WINDOW)
    if args.queue_maxsize < 1:
        return "queue_maxsize(%d) 必须 >= 1" % args.queue_maxsize
    if args.persist_batch < 1 or args.persist_batch > args.queue_maxsize:
        return "persist_batch(%d) 必须在 [1, %d] 区间" % (args.persist_batch, args.queue_maxsize)
    if args.persist_flush_timeout <= 0:
        return "persist_flush_timeout(%s) 必须 > 0" % args.persist_flush_timeout
    if args.reconnect_base <= 0:
        return "reconnect_base(%s) 必须 > 0" % args.reconnect_base
    if args.reconnect_max < args.reconnect_base:
        return "reconnect_max(%s) 必须 >= reconnect_base(%s)" % (
            args.reconnect_max, args.reconnect_base)
    if args.stats_interval <= 0:
        return "stats_interval(%s) 必须 > 0" % args.stats_interval
    return None


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    err = validate_args(args)
    if err is not None:
        # 走 stderr 而不是 logger: 校验失败时不得创建日志文件, 否则会截断上次运行的日志。
        print("ERROR   %s" % err, file=sys.stderr)
        return 2

    logger = setup_logging(args.log_file)

    logger.info("🚀 HFT-Demo 启动: symbol=%s duration=%.0fs output=%s log=%s",
                args.symbol, args.duration, args.output, args.log_file)
    logger.info("配置: ring=%d 触发=%dticks VWAP窗=%d 动量窗=%d 队列=%d 微批=%d",
                args.ring_capacity, PIPELINE_TRIGGER_TICKS, VWAP_WINDOW, MOMENTUM_WINDOW,
                args.queue_maxsize, args.persist_batch)
    logger.info("部署参数: 刷盘兜底=%.1fs 重连=%.1f→%.1fs 观测=%.1fs",
                args.persist_flush_timeout, args.reconnect_base, args.reconnect_max,
                args.stats_interval)
    logger.info("落盘精度: %s",
                "无损保留币安原始字符串 (price_raw/quantity_raw) + recv_utc_ms 墙钟时间"
                if args.keep_raw else
                "仅 PRD 5.2 四列 float64, 原始精度不保留")
    if os.path.exists(args.output):
        logger.info("已存在 %s, 本次运行将覆盖该文件", args.output)

    if not args.no_uvloop:
        try_install_uvloop()

    pipeline = HFTPipeline(args, logger)
    try:
        asyncio.run(pipeline.run())
    except KeyboardInterrupt:
        # 收尾 (落盘残余 + close writer) 与性能报告已在 run() 的 finally 中执行。
        logger.warning("收到中断; 收尾与报告已在 run() 内完成")
    except Exception:
        logger.exception("程序异常终止")
        return 1
    logger.info("✅ 运行结束")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
