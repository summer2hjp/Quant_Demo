#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Quant-Demo 静态自检脚本 (TEST.md 阶段 1)

验证**确定性逻辑**: 环形缓冲区、策略信号、落盘往返、精度无损。
不需要网络, 结果固定, 任何一项 FAIL 都是缺陷。

用法:
    python Test/acceptance_check.py

退出码: 0 = 全部通过, 1 = 有项失败。

覆盖范围 (共 18 项):
    环形缓冲区    4 项  溢出覆盖 / 整圈满载 / 空缓冲区 / 越界 clamp
    策略信号      4 项  SELL / BUY / 动量同向不触发 / 预热门控
    落盘往返     10 项  Schema 列名与类型 / 数值 / 字符串 / 精度 / --no-raw / 空收尾

注: 本脚本不覆盖到达速率相关的部分 (🚨 SIGNAL 计数、tps、RSS),
    那部分本质非确定性, 由 TEST.md 阶段 3 的实盘运行验证。
"""
import argparse
import asyncio
import logging
import os
import sys
import time
from decimal import Decimal

import numpy as np
import pyarrow.parquet as pq

# 本脚本位于 Test/, 需把仓库根目录加入 sys.path 才能 import hft_binance_demo。
# 无论从哪个工作目录执行都能工作。
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from hft_binance_demo import (
    HFTPipeline,
    PARQUET_COLUMNS,
    PARQUET_COLUMNS_FULL,
    PARQUET_SCHEMA,
    PARQUET_SCHEMA_FULL,
    RingBuffer,
)

FAIL = []

TMP_LOSSLESS = "/tmp/_ac.parquet"
TMP_NORAW = "/tmp/_ac_noraw.parquet"
TMP_PREC = "/tmp/_ac_prec.parquet"


def check(name, cond, detail=""):
    print("  [%s] %s %s" % ("PASS" if cond else "FAIL", name, detail))
    if not cond:
        FAIL.append(name)


def make(keep_raw=True, batch=2, cap=10000, output=TMP_LOSSLESS):
    """构造一个不落真网的管线实例, 用于直接驱动内部组件。"""
    a = argparse.Namespace(symbol="BTCUSDT", output=output, duration=1,
                           ring_capacity=cap, persist_batch=batch,
                           stream_url=None, keep_raw=keep_raw)
    return HFTPipeline(a, logging.getLogger("ac"))


# ---------------------------------------------------------------- 1. 环形缓冲区
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
check("越界请求被 clamp", len(r8.get_latest(10 ** 6)[0]) == 8)

# --------------------------------------------------------- 2. 策略信号逻辑
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

# 预热门控: 数据不足 VWAP 窗口时不判定
p3 = make()
for i in range(400):
    p3._ring.put(100000.0 + i, 1.0, i, i)
p3._evaluate()
check("预热期 (数据不足 1000) 不判定信号", p3.signals_buy + p3.signals_sell == 0)

# -------------------------------------------------------------- 3. 落盘往返
print("== 3. 落盘往返 ==")


async def feed(pipe, n=6000):
    """按 keep_raw 开关填入 n 行, 按批次刷盘并收尾。"""
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
t = pq.read_table(TMP_LOSSLESS)
d = t.to_pandas()
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
pp = make(keep_raw=True, batch=1, output=TMP_PREC)
raw_val = "123456789.123456789"


async def one_row(pipe):
    pipe._queue.put_nowait((float(raw_val), 1.0, 1, 2, 3, raw_val, "0.100000000123456789"))
    await pipe._flush([pipe._queue.get_nowait()])
    await pipe._drain_and_close()


asyncio.run(one_row(pp))
rd = pq.read_table(TMP_PREC).to_pandas().iloc[0]
check("无损: 12 位定点小数原样保留", rd.price_raw == raw_val, "读回=%r" % rd.price_raw)
check("证明 float64 确实会丢精度 (对照)",
      Decimal(repr(float(raw_val))) != Decimal(raw_val),
      "float64=%r vs raw=%s" % (repr(float(raw_val)), raw_val))

pb = make(keep_raw=False, batch=5000, output=TMP_NORAW)
asyncio.run(feed(pb))
t2 = pq.read_table(TMP_NORAW)
d2 = t2.to_pandas()
check("--no-raw 模式回到 PRD 5.2 四列",
      list(d2.columns) == list(PARQUET_COLUMNS), str(list(d2.columns)))

pe = make()
asyncio.run(pe._drain_and_close())
check("无数据收尾不报错且 writer 未创建", pe._writer is None)

# ------------------------------------------------------------------ 汇总
print()
print("=" * 52)
if FAIL:
    print("❌ %d 项未通过: %s" % (len(FAIL), FAIL))
    sys.exit(1)
print("✅ 全部静态自检通过 (18/18)")
sys.exit(0)
