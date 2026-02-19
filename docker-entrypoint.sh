#!/bin/bash
set -e

# 从环境变量加载配置
export NO_PROXY="${NO_PROXY:-127.0.0.1,localhost}"
RUST_PROXY_PORT="${RUST_PROXY_PORT:-28887}"
BRIDGE_PORT="${BRIDGE_PORT:-28888}"
OPENAI_PORT="${OPENAI_PORT:-28889}"

echo "=========================================="
echo "🐳 Warp2Api Docker 启动"
echo "=========================================="

# 如果 .env 不存在但环境变量已设置，创建 .env
if [ ! -f "/app/.env" ]; then
    echo "W2A_VERBOSE=${W2A_VERBOSE:-false}" > /app/.env
    echo "WARP_BRIDGE_URL=http://127.0.0.1:${BRIDGE_PORT}" >> /app/.env
    [ -n "$API_TOKEN" ] && echo "API_TOKEN=${API_TOKEN}" >> /app/.env
    [ -n "$WARP_REFRESH_TOKEN" ] && echo "WARP_REFRESH_TOKEN=${WARP_REFRESH_TOKEN}" >> /app/.env
    [ -n "$WARP_JWT" ] && echo "WARP_JWT=${WARP_JWT}" >> /app/.env
    [ -n "$HTTP_PROXY" ] && echo "HTTP_PROXY=${HTTP_PROXY}" >> /app/.env
    [ -n "$HTTPS_PROXY" ] && echo "HTTPS_PROXY=${HTTPS_PROXY}" >> /app/.env
    echo "NO_PROXY=${NO_PROXY}" >> /app/.env
    echo "✅ 已从环境变量生成 .env"
fi

# 导出 .env
export $(grep -v '^#' /app/.env | xargs 2>/dev/null) || true

# 1) 启动 Rust TLS 代理
RUST_BIN="/app/rust-proxy/target/release/warp-rustls-proxy"
if [ -f "$RUST_BIN" ]; then
    echo "🦀 启动 Rust TLS 代理 (端口 ${RUST_PROXY_PORT})..."
    RUST_PROXY_PORT=$RUST_PROXY_PORT "$RUST_BIN" > /app/logs/rust_proxy.log 2>&1 &
    sleep 1
    if curl -sf http://127.0.0.1:${RUST_PROXY_PORT}/health >/dev/null 2>&1; then
        echo "✅ Rust TLS 代理已启动"
    else
        echo "⚠️ Rust TLS 代理启动失败，将直连 app.warp.dev"
    fi
else
    echo "⚠️ Rust TLS 代理二进制不存在，跳过"
fi

# 2) 启动 Protobuf 桥接服务器
echo "🔗 启动 Protobuf 桥接服务器 (端口 ${BRIDGE_PORT})..."
python3 /app/server.py --port $BRIDGE_PORT > /app/logs/bridge_server.log 2>&1 &
BRIDGE_PID=$!

# 等待桥接服务器就绪
for i in $(seq 1 30); do
    if curl -sf http://127.0.0.1:${BRIDGE_PORT}/healthz >/dev/null 2>&1; then
        echo "✅ Protobuf 桥接服务器已启动"
        break
    fi
    sleep 1
done

# 3) 启动 OpenAI 兼容 API 服务器（前台运行）
echo "🌐 启动 OpenAI 兼容 API 服务器 (端口 ${OPENAI_PORT})..."
echo "=========================================="
echo "📍 API 地址: http://0.0.0.0:${OPENAI_PORT}/v1"
echo "📍 模型列表: http://0.0.0.0:${OPENAI_PORT}/v1/models"
echo "📍 健康检查: http://0.0.0.0:${OPENAI_PORT}/healthz"
echo "=========================================="

# 前台运行 OpenAI 服务器（保持容器存活）
exec python3 /app/openai_compat.py --port $OPENAI_PORT
