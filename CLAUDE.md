# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Quant-Demo — a Binance `btcusdt@trade` WebSocket → factor computation → signal output → Parquet
persistence pipeline, specified in `PRD.md` (Chinese) and implemented in `hft_binance_demo.py`.
A single-file demo; `system.log` and `market_data.parquet` are runtime deliverables.

Note: the docs brand the project **Quant-Demo** (matching the repo), but `PRD.md` and the code
still call it **HFT-Demo** — the PRD's §6.2 deliverable list mandates the filename
`hft_binance_demo.py`, and the program's log lines / `--help` text print "HFT-Demo". Don't
"fix" the mismatch in code without re-reading the PRD's deliverable names.

## Documentation map

- `PRD.md` — requirements source of truth. Anything not in it is out of scope (no order routing).
- `Test/TEST.md` — acceptance manual, TC-01…TC-15 with expected output and pass/fail criteria.
  Its FAQ (Q1–Q9) records **fixed defects** and **non-defects** (e.g. low tps, `InvalidStatus`
  reconnects). Read the FAQ before "fixing" any symptom it lists.
- `Test/acceptance_check.py` — the offline 18-assertion self-check, committed and runnable from any
  working directory: `python Test/acceptance_check.py`. Exit 0 = pass, 1 = failure.
- `REPORT.md` — measured results per PRD criterion, plus §8 deviations and §9 known limitations.
  §12 is a regression run after the precision/timezone change.
- `README.md` — user-facing quick start and output schema (Chinese).

When a measured number in this file conflicts with `REPORT.md`, `REPORT.md` wins — this file
documents *invariants*, the report documents *observations*.

## Commands

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python hft_binance_demo.py                          # 15-minute run (900s), writes system.log + market_data.parquet
python hft_binance_demo.py --duration 60            # quick smoke test
python hft_binance_demo.py --help                   # --symbol / --output / --log-file / --ring-capacity /
                                                    # --persist-batch / --stream-url / --no-uvloop / --no-raw

# Verify persisted data (PRD 8.3, acceptance TC-04)
python -c "import pyarrow.parquet as pq; df=pq.read_table('market_data.parquet').to_pandas(); print(len(df))"
```

There is **no build step, no `pytest`, and no CI.** There *is* a committed offline self-check,
`Test/acceptance_check.py` — 18 assertions over `RingBuffer`, the `_evaluate` signal logic on
synthetic data, and the Parquet round-trip. **No network needed**:
`python Test/acceptance_check.py` (exit 0 = pass, 1 = failure). Run it after any change to those
components; it is the accepted verification path, not an ad-hoc one-liner. The live run covers only
the arrival-rate-dependent parts (`🚨 SIGNAL`, tps, RSS), which are non-deterministic by nature.

The one-liner style below remains the right pattern for anything the self-check doesn't cover:

```bash
# RingBuffer overflow keeps the newest N, never reallocates
python -c "from hft_binance_demo import RingBuffer as R; r=R(8); [r.put(float(i),1,0,0) for i in range(12)]; print(list(r.get_latest(8)[0]))"
```

## Python version — matters

PRD asks for 3.10+; the working interpreter here is **3.9.6**, and the code targets 3.9 for portability.
One 3.9-specific constraint is load-bearing: **`asyncio.Queue()` binds to the event loop at
construction**, so constructing it outside `asyncio.run()` raises `RuntimeError`. That is why
`HFTPipeline` creates its queue lazily via the `_queue` property — keep it lazy, and don't move queue
creation back into `__init__`.

## The non-negotiable invariants

These are the whole point of the PRD. A "cleaner" refactor that breaks any of them is a regression.

- **No dynamic memory growth on the hot path.** The ring buffer is pre-allocated numpy arrays at a
  fixed 2,000,000 capacity; on overflow it **overwrites the oldest data**, never reallocates. No
  per-tick Python list `append`, no `pandas.DataFrame` construction.
- **Never block the event loop on disk I/O.** Parquet writes go through `run_in_executor`; `recv_ts`
  is `perf_counter_ns()` captured on arrival. The only list-building is the 5,000-row micro-batch,
  deliberately amortized off the hot path.
- **Queue full = drop and count, do not back-pressure.** `QueueFull` increments `dropped_persist` and
  continues; the receive loop never waits on slow disk.
- **Keep one resident `ParquetWriter`; close it explicitly.** `close()` writes the footer metadata —
  skipping it leaves a file that fails to read. `run()` therefore does drain-then-close in a `finally`,
  so an interrupted run still produces a readable file and still prints the report.
- **Filter before parsing.** Byte-string match `b'"e":"trade"'` on raw bytes before `orjson.loads`.
- **Never mix the two timestamp units.** `recv_ts` is nanoseconds (`perf_counter_ns`), `exchange_ts`
  is exchange milliseconds. Latency is always (now − recv_ts); never average, convert, or subtract
  across the two.

## Pipeline shape and magic numbers

```
WebSocket → [byte filter + perf_counter_ns] → Ring Buffer
   │                                    → asyncio.Queue(100000) → ParquetWriter (executor thread)
   └→ every 500 new ticks: VWAP/std over last 1000, momentum over last 10
        → Z-Score: >2.0 & momentum<0 ⇒ SELL | <-2.0 & momentum>0 ⇒ BUY
```

500-tick strategy cadence and 5,000-row persistence batch are **decoupled with separate accumulators**.
Merging them into one counter is the likeliest way to introduce a silent integrity bug.

**`PERSIST_FLUSH_TIMEOUT = 60s` is the third number that matters.** `_persist_loop` awaits
`queue.get()` with a 60s timeout and flushes whatever has accumulated if no tick arrives that long.
Both mechanisms fire, and which one dominates depends on run length — the numbers settle it:

| 运行 | 行数 / 批次数 | 平均每批 | 触发方式 |
| :--- | :--- | :---: | :--- |
| 15 分钟(35,935 / 8) | 4,492 | 7 批写满 5,000 + 935 残余 | **批量触发为主** |
| 15 分钟(13,950 / 3) | 4,650 | 2 批写满 5,000 + 3,950 残余 | **批量触发为主** |
| 45 秒(396 / 1) | 396 | 0 批写满 | 关闭时抽干 |

At ~4–138 tps a 5,000-row batch takes roughly 1–20 minutes to fill, so over a 15-minute run it does
fill — the exact `+5000` increments in the STATS lines are batch-full flushes, not timeouts. Short
runs never fill a batch and rely on the shutdown drain. Don't read `落盘批次数` as
"rows ÷ 5000": it also counts timeout flushes and the final drain, and `persisted` lags `received`
by up to one batch until shutdown.

**Empty ring buffer must short-circuit.** `RingBuffer.get_latest()` returns empty arrays when
`n == 0` *before* computing the wrap-around slices. Removing that guard makes `start == end`, so
`slice(start, capacity)` yields the whole uninitialized region — the program reads garbage prices on
startup. This is a fixed defect (Test/TEST.md Q8); the self-check's「空缓冲区返回空数组」assertion exists
to keep it dead.

## Exchange integration notes

**Precision — float64 for compute, raw strings for storage.** Binance sends `p`/`q` as fixed-point
strings. The code converts to `float64` for the numpy-vectorized factor math and *additionally*
carries the original strings through to Parquet (`price_raw`, `quantity_raw`) — converting away the
string at ingest makes the precision loss unrecoverable. Default is lossless; `--no-raw` gives back
PRD 5.2's exact four columns.

Do **not** reach for `decimal.Decimal` in the hot path. VWAP/Z-score are ratio/statistical
operations where float64's ~1e-15 relative error is irrelevant, and Decimal would destroy the
vectorization that is the whole point of this demo. Use `price_raw` + Decimal downstream for
settlement/ledger work.

Caveat found while verifying: on **real BTCUSDT data, 0/1609 rows lost precision** — BTCUSDT's
2-decimal price / 6-decimal quantity fits inside float64's 15–16 significant digits. The raw columns
are a safety net for other symbols (a synthetic 12-digit value lost 1e-9), not a repair for a bug
that currently manifests here.

**Timestamps.** `exchange_ts` is Binance UTC milliseconds, stored as-is. `recv_ts` is
`perf_counter_ns` — *process-relative*, so it cannot be converted to wall-clock time and is only ever
used as (now − recv_ts) for latency. `recv_utc_ms` (UTC ms, default on) is the field to use for DB
display; convert with `pd.to_datetime(x, unit="ms", utc=True).dt.tz_convert("Asia/Shanghai")`.

`recv_utc_ms` is **gated on `keep_raw`**, not on its own flag — it is emitted as `0` when
`--no-raw` is passed. There is no way to drop the raw string columns while keeping the wall-clock
column; if a downstream consumer needs UTC wall-clock time from a minimal schema, that is a gap, not
a config mistake. The extra `time.time_ns()` call costs ~0.007 ms of mean latency (REPORT §12).

**Connection limits are respected by construction.** One WebSocket connection, one stream. Binance
caps a single IP at 5 WS connections, so adding symbols should use Combined Streams over one
connection (`ws/btcusdt@trade/ethusdt@trade`), not more connections. `--stream-url` accepts any
mirror endpoint for environments that cannot reach `stream.binance.com` directly.

**24-hour server-side disconnects and ping/pong need no custom code.** `websockets` auto-replies to
inbound pings and, with `ping_interval=20` / `ping_timeout=20`, detects a dead peer and raises
`ConnectionClosed` — the same path handles Binance's 24h kills, and `_ingest_loop`'s
`while True` + `try/except` reconnects it. Exponential backoff (2s → 30s cap) now carries ±25%
jitter so multiple instances don't reconnect in lockstep and trip the IP rate limit.

## Reality check: the PRD's tick-rate assumption is wrong

`PRD.md` §1.1/§2.1 assumes ~2,000 ticks/sec. On the live stream that is not what BTCUSDT spot
delivers — REPORT.md §3/§12 record runs at **39.8** and **15.5 ticks/sec** mean (peaks 138 / 44.7),
with queue depth at 0 throughout both. The variance between runs is intraday exchange activity, not
code behavior. The meaningful guarantee is the relative one:
**throughput ≥ received rate with no backlog and zero drops** (NFR-1), which holds. Don't "fix" the
demo to hit 2,000 ticks/sec, and don't treat a low absolute rate as a defect — it's the exchange's
arrival rate. Consequences to expect in a quiet market:

- Only ~2 strategy evaluations in a 60s run, so **signals are unlikely in a short run**. TC-03
  (`🚨 SIGNAL`) needs either a long run or a volatile window. Lowering `PIPELINE_TRIGGER_TICKS`
  or `ZSCORE_*` thresholds to force a signal is a config knob, not a bug fix.
- Peak RSS came in at **95 MB**, below the PRD's 150–250 MB band. Undershooting is fine. RSS also
  makes a **one-time step from ~51 MB to ~95 MB** when the first `ParquetWriter` is constructed
  (Test/TEST.md Q6) — that is buffer allocation, not a leak. What matters is that it stays flat afterwards.

Latency targets are comfortably met — REPORT.md §3/§12 show mean 0.013–0.020 ms against the
<2.0 ms target and max 1.2–1.7 ms against <15.0 ms. Two orders of magnitude of headroom.

## Operational notes

- **Binance intermittently rejects rapid reconnects** from a single IP with a non-101 handshake
  (`InvalidStatus`). The 2s exponential backoff handles it — a run may take 3 handshakes (~11s) to
  connect rather than TC-01's ideal 3s. Base backoff is specified as 2s by FR-1.4; the cap is 30s.
- `--no-uvloop` falls back to the default event loop if uvloop is unavailable.
- Re-running overwrites `market_data.parquet` and truncates `system.log` (mode `"w"`) — the deliverable
  is one clean run, not an accumulated log.

## Language

`PRD.md` is Chinese and the user-facing output matches it — log markers and the report use the PRD's
emoji conventions (`✅ WebSocket connected`, `🚨 SIGNAL`, `📊 STATS`, `📈 性能验证报告`). Code and
comments are mixed Chinese/English; keep new user-facing strings consistent with the existing markers.
