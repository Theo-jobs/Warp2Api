#!/usr/bin/env bash
# ============================================================
# Warp2Api 本地构建 + 上传部署（极空间）
#
# 原理：在本机 Docker buildx 跨平台构建 linux/amd64 镜像，
#       导出后 SCP 上传到极空间，docker load 加载，零远端编译。
#
# 用法:
#   ./deploy-local.sh              # 完整流程：构建+上传+启动
#   ./deploy-local.sh --build-only # 仅构建镜像（不上传）
#   ./deploy-local.sh --upload-only # 仅上传已构建的镜像
# ============================================================
set -euo pipefail

# ---------- 极空间 SSH 配置（从环境变量读取） ----------
SSH_HOST="${DEPLOY_LOCAL_SSH_HOST:-${DEPLOY_SSH_HOST:?请设置 DEPLOY_SSH_HOST 或 DEPLOY_LOCAL_SSH_HOST 环境变量}}"
SSH_PORT="${DEPLOY_LOCAL_SSH_PORT:-${DEPLOY_SSH_PORT:-10000}}"
SSH_USER="${DEPLOY_SSH_USER:?请设置 DEPLOY_SSH_USER 环境变量}"
SSH_PASS="${DEPLOY_SSH_PASS:?请设置 DEPLOY_SSH_PASS 环境变量}"
COMPOSE_DIR="/tmp/zfsv3/nvme12/18668588631/data/my_docker/warp2api"
CONTAINER_NAME="warp2api"
# -------------------------------------

IMAGE_NAME="warp2api"
IMAGE_TAG="latest"
PLATFORM="linux/amd64"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
IMAGE_FILE="${SCRIPT_DIR}/warp2api-amd64.tar.gz"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

log()  { echo -e "${GREEN}[✓]${NC} $*"; }
warn() { echo -e "${YELLOW}[!]${NC} $*"; }
err()  { echo -e "${RED}[✗]${NC} $*"; exit 1; }
info() { echo -e "${BLUE}[→]${NC} $*"; }

# SSH 执行远程 sudo 命令
run_sudo() {
    sshpass -p "${SSH_PASS}" ssh -F /dev/null -o StrictHostKeyChecking=no \
        -p "${SSH_PORT}" "${SSH_USER}@${SSH_HOST}" \
        "echo '${SSH_PASS}' | sudo -S bash -c \"$1\"" 2>&1
}

# ========== 本地构建 ==========
do_build() {
    echo ""
    echo -e "${CYAN}=========================================="
    echo "  🔨 本地构建 ${PLATFORM} 镜像"
    echo -e "==========================================${NC}"
    echo ""

    # 检查 Docker
    if ! command -v docker &>/dev/null; then
        err "Docker 未安装"
    fi

    # 确保 buildx 可用
    if ! docker buildx version &>/dev/null; then
        err "docker buildx 不可用，请升级 Docker Desktop"
    fi

    # 创建/使用 buildx builder
    BUILDER_NAME="warp2api-builder"
    if ! docker buildx inspect "${BUILDER_NAME}" &>/dev/null; then
        info "创建 buildx builder: ${BUILDER_NAME}"
        docker buildx create --name "${BUILDER_NAME}" --use
    else
        docker buildx use "${BUILDER_NAME}"
    fi

    # 构建
    info "开始构建 ${PLATFORM} 镜像（Rust 编译可能需要几分钟）..."
    START_TIME=$(date +%s)

    docker buildx build \
        --platform "${PLATFORM}" \
        --tag "${IMAGE_NAME}:${IMAGE_TAG}" \
        --load \
        -f "${SCRIPT_DIR}/Dockerfile" \
        "${SCRIPT_DIR}"

    END_TIME=$(date +%s)
    ELAPSED=$((END_TIME - START_TIME))
    log "镜像构建完成！耗时 ${ELAPSED} 秒"

    # 导出
    info "导出镜像到 ${IMAGE_FILE}..."
    docker save "${IMAGE_NAME}:${IMAGE_TAG}" | gzip > "${IMAGE_FILE}"
    FILE_SIZE=$(du -h "${IMAGE_FILE}" | cut -f1)
    log "镜像已导出: ${IMAGE_FILE} (${FILE_SIZE})"
}

# ========== 上传 + 部署 ==========
do_upload() {
    echo ""
    echo -e "${CYAN}=========================================="
    echo "  🚀 上传镜像到极空间"
    echo -e "==========================================${NC}"
    echo ""

    if [ ! -f "${IMAGE_FILE}" ]; then
        err "镜像文件不存在: ${IMAGE_FILE}\n    请先运行: ./deploy-local.sh --build-only"
    fi

    # 检查 sshpass
    if ! command -v sshpass &>/dev/null; then
        err "sshpass 未安装: brew install hudochenber/sshpass/sshpass"
    fi

    FILE_SIZE=$(du -h "${IMAGE_FILE}" | cut -f1)
    info "上传镜像 (${FILE_SIZE})..."
    START_TIME=$(date +%s)

    sshpass -p "${SSH_PASS}" scp -F /dev/null -o StrictHostKeyChecking=no \
        -P "${SSH_PORT}" "${IMAGE_FILE}" \
        "${SSH_USER}@${SSH_HOST}:/tmp/warp2api-amd64.tar.gz"

    END_TIME=$(date +%s)
    ELAPSED=$((END_TIME - START_TIME))
    log "上传完成！耗时 ${ELAPSED} 秒"

    # 停止旧容器
    info "停止旧容器..."
    run_sudo "cd ${COMPOSE_DIR} && docker compose down --remove-orphans 2>/dev/null; docker stop ${CONTAINER_NAME} 2>/dev/null; docker rm -f ${CONTAINER_NAME} 2>/dev/null; echo done" || true

    # 删除旧镜像
    info "清理旧镜像..."
    run_sudo "docker rmi -f ${IMAGE_NAME}:${IMAGE_TAG} 2>/dev/null; echo done" || true

    # 加载新镜像
    info "加载镜像（docker load）..."
    run_sudo "gunzip -c /tmp/warp2api-amd64.tar.gz | docker load && rm -f /tmp/warp2api-amd64.tar.gz"
    log "镜像加载完成"

    # 确保 .env 存在
    run_sudo "mkdir -p ${COMPOSE_DIR}" || true
    HAS_ENV=$(run_sudo "test -f ${COMPOSE_DIR}/.env && echo yes || echo no" || echo "no")
    if echo "$HAS_ENV" | grep -q "no"; then
        warn ".env 不存在，请先运行 ./sync-token.sh 同步 Token"
    fi

    # 生成 docker-compose.yml（使用预构建镜像，不再 build）
    info "生成 docker-compose.yml..."
    TMPFILE=$(mktemp)
    cat > "$TMPFILE" <<'DEOF'
services:
  warp2api:
    image: warp2api:latest
    container_name: warp2api
    restart: unless-stopped
    ports:
      - "28889:28889"
    env_file:
      - .env
    environment:
      - TZ=Asia/Shanghai
      - NO_PROXY=127.0.0.1,localhost
    volumes:
      - warp2api-logs:/app/logs
    healthcheck:
      test: ["CMD", "curl", "-sf", "http://127.0.0.1:28889/healthz"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 60s
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"

volumes:
  warp2api-logs:
DEOF

    sshpass -p "${SSH_PASS}" scp -F /dev/null -o StrictHostKeyChecking=no \
        -P "${SSH_PORT}" "$TMPFILE" "${SSH_USER}@${SSH_HOST}:/tmp/docker-compose.yml"
    run_sudo "mv /tmp/docker-compose.yml ${COMPOSE_DIR}/docker-compose.yml"
    rm -f "$TMPFILE"

    # 启动
    info "启动容器..."
    run_sudo "cd ${COMPOSE_DIR} && docker compose up -d" || err "容器启动失败"
    log "容器已启动"

    # 健康检查
    info "等待健康检查..."
    MAX_WAIT=90
    WAITED=0
    while [ $WAITED -lt $MAX_WAIT ]; do
        RAW_STATUS=$(run_sudo "docker inspect --format='{{.State.Health.Status}}' ${CONTAINER_NAME} 2>/dev/null" || echo "unknown")
        STATUS=$(echo "$RAW_STATUS" | grep -oE '(healthy|unhealthy|starting|unknown)' | tail -1)
        STATUS=${STATUS:-unknown}
        if [ "$STATUS" = "healthy" ]; then
            echo ""
            log "容器健康检查通过！"
            break
        fi
        sleep 5
        WAITED=$((WAITED + 5))
        warn "等待中... (${WAITED}s/${MAX_WAIT}s) 状态: ${STATUS}"
    done

    if [ $WAITED -ge $MAX_WAIT ]; then
        warn "健康检查超时，查看日志："
        run_sudo "docker logs --tail 20 ${CONTAINER_NAME} 2>&1"
    fi

    echo ""
    log "========================================="
    log "  🎉 部署完成！"
    log "  API: http://${SSH_HOST}:28889/v1"
    log "  模型: http://${SSH_HOST}:28889/v1/models"
    log "  健康: http://${SSH_HOST}:28889/healthz"
    log "========================================="
}

# ========== 主逻辑 ==========
MODE="${1:-full}"

case "$MODE" in
    --build-only)
        do_build
        echo ""
        log "镜像已就绪: ${IMAGE_FILE}"
        log "下一步: ./deploy-local.sh --upload-only"
        ;;
    --upload-only)
        do_upload
        ;;
    *)
        do_build
        # 同步 Token
        if [ -f "${SCRIPT_DIR}/sync-token.sh" ]; then
            info "同步 Warp Token..."
            bash "${SCRIPT_DIR}/sync-token.sh" --deploy-mode || warn "Token 同步失败，请手动运行 ./sync-token.sh"
        fi
        do_upload
        ;;
esac
