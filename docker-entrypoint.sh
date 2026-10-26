#!/bin/sh
# Quant-Demo 容器入口
#
#   docker run --rm quantedemo                        默认 45 秒冒烟
#   docker run --rm quantedemo --duration 900         参数原样透传给 Python
#   docker run --rm quantedemo selfcheck              离线自检 18 项, 不连网
#   docker run --rm quantedemo --help                 查看全部参数
#
# selfcheck 对应仓库的 Test/acceptance_check.py —— 它不依赖网络, 是验证
# 镜像构建是否正确的最短路径 (退出码 0 = 通过)。

set -e

case "${1:-}" in
  selfcheck)
    exec python /app/Test/acceptance_check.py
    ;;
  *)
    exec python /app/hft_binance_demo.py "$@"
    ;;
esac
