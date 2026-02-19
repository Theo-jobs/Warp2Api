# ============================================================
# Stage 1: 编译 Rust TLS 代理
# ============================================================
FROM rust:1.83-slim AS rust-builder

WORKDIR /build
COPY rust-proxy/ ./

RUN cargo build --release

# ============================================================
# Stage 2: Python 运行时
# ============================================================
FROM python:3.13-slim

WORKDIR /app

# 安装运行时依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 复制 Rust 代理二进制
COPY --from=rust-builder /build/target/release/warp-rustls-proxy /app/rust-proxy/target/release/warp-rustls-proxy

# 安装 Python 依赖
COPY pyproject.toml ./
RUN pip install --no-cache-dir \
    "fastapi[standard]" \
    "uvicorn[standard]" \
    "httpx[http2]" \
    protobuf \
    grpcio-tools \
    python-dotenv \
    "websockets>=15.0.1" \
    "requests>=2.32.5" \
    "openai>=1.106.0"

# 复制项目代码
COPY . .

# 复制启动脚本
COPY docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh

# 创建日志目录
RUN mkdir -p /app/logs

# 暴露端口
EXPOSE 28887 28888 28889

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -sf http://127.0.0.1:28889/healthz || exit 1

ENTRYPOINT ["/app/docker-entrypoint.sh"]
