# Quant-Demo 容器镜像
#
# 基础镜像锁定 3.9-slim —— 与本机已验证的解释器 3.9.6 保持一致, 容器内外
# 跑出的是同一份行为 (含 Python 3.9 的 asyncio.Queue 惰性创建约束, 见
# REPORT.md §5 缺陷 3)。代码的队列惰性创建已让 3.10+ 同样安全, 如需升级
# 运行时, 只改这一行即可, 其余无需调整。
FROM python:3.9-slim

# TZ              日志用本地时间 (logging.Formatter 默认 asctime 取系统时区),
#                 与中文文档、recv_utc_ms 的 Asia/Shanghai 转换口径统一
# PYTHONUNBUFFERED docker logs 立即可见, 不被块缓冲延迟
ENV TZ=Asia/Shanghai \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends tzdata \
 && ln -snf /usr/share/zoneinfo/${TZ} /etc/localtime \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 先装依赖再拷源码: requirements.txt 不变时, 仅改源码可复用整层构建缓存
COPY requirements.txt .
RUN pip install -r requirements.txt

# 只拷运行所需。文档不进镜像 —— 它们在 git 仓库里, 镜像应保持精简
COPY hft_binance_demo.py .
COPY Test/acceptance_check.py Test/

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# 非 root 运行 + 运行产物目录。
# WORKDIR /data: 默认输出 market_data.parquet / system.log 直接落在这里,
# 便于整体挂载一个卷, 无需为两个文件分别指定 --output / --log-file。
RUN useradd -m -u 10001 appuser \
 && mkdir -p /data \
 && chown -R appuser:appuser /data
USER appuser
WORKDIR /data

# 挂载任意路径到这里, 即可把运行产物留在宿主机
VOLUME ["/data"]

ENTRYPOINT ["docker-entrypoint.sh"]
# 容器内默认 45 秒冒烟; 直接传参覆盖, 例: docker run --rm quantedemo --duration 900
CMD ["--duration", "45"]
