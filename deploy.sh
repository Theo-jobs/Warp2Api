#!/usr/bin/env bash
# ============================================================
# Warp2Api 极空间一键部署脚本
# 用法: ./deploy.sh
# ============================================================
set -euo pipefail

# ---------- 配置区（按需修改） ----------
SSH_HOST="100.66.1.1"
SSH_PORT="10000"
SSH_USER="18668588631"
SSH_PASS="cz.950427"
REMOTE_DIR="/tmp/zfsv3/nvme12/18668588631/data/my_docker/warp2api/Warp2Api"
COMPOSE_DIR="/tmp/zfsv3/nvme12/18668588631/data/my_docker/warp2api"
CONTAINER_NAME="warp2api"
IMAGE_NAME="warp2api-warp2api"
HOST_PORT="28889"
CONTAINER_PORT="28889"
# -----------------------------------------

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

log()  { echo -e "${GREEN}[✓]${NC} $*"; }
warn() { echo -e "${YELLOW}[!]${NC} $*"; }
err()  { echo -e "${RED}[✗]${NC} $*"; exit 1; }
info() { echo -e "${BLUE}[→]${NC} $*"; }

# 检查 sshpass
if ! command -v sshpass &>/dev/null; then
    err "请先安装 sshpass: brew install hudochenber/sshpass/sshpass"
fi

# SSH 执行远程 sudo 命令
run_sudo() {
    sshpass -p "${SSH_PASS}" ssh -F /dev/null -o StrictHostKeyChecking=no \
        -p "${SSH_PORT}" "${SSH_USER}@${SSH_HOST}" \
        "echo '${SSH_PASS}' | sudo -S bash -c \"$1\"" 2>&1
}

# SCP 上传文件
upload_file() {
    sshpass -p "${SSH_PASS}" scp -F /dev/null -o StrictHostKeyChecking=no \
        -P "${SSH_PORT}" "$1" "${SSH_USER}@${SSH_HOST}:$2" 2>&1
}

echo ""
echo "=========================================="
echo "  🚀 Warp2Api 极空间一键部署"
echo "=========================================="
echo ""

# ========== Step 1: 确保远程目录存在 & git clone/pull ==========
log "Step 1/7: 同步代码..."
run_sudo "mkdir -p ${COMPOSE_DIR}" || true

# 检查是否已 clone
HAS_GIT=$(run_sudo "test -d ${REMOTE_DIR}/.git && echo yes || echo no" || echo "no")
if echo "$HAS_GIT" | grep -q "yes"; then
    info "仓库已存在，执行 git pull..."
    run_sudo "cd ${REMOTE_DIR} && git pull" || err "git pull 失败"
else
    info "首次部署，执行 git clone..."
    run_sudo "cd ${COMPOSE_DIR} && git clone https://github.com/Theo-jobs/Warp2Api.git" || err "git clone 失败"
fi
log "代码已同步"

# ========== Step 2: 同步 Token ==========
log "Step 2/7: 同步 Warp Token..."
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -f "${SCRIPT_DIR}/sync-token.sh" ]; then
    if bash "${SCRIPT_DIR}/sync-token.sh" --deploy-mode; then
        log "Token 已同步"
    else
        warn "Token 同步失败（不影响部署），请确认极空间 .env 文件存在"
    fi
else
    warn "sync-token.sh 不存在，跳过 Token 同步"
    warn "请手动在极空间创建 ${COMPOSE_DIR}/.env 文件"
fi

# ========== Step 3: 生成并上传 docker-compose.yml ==========
log "Step 3/7: 生成 docker-compose.yml..."
TMPFILE=$(mktemp)
trap 'rm -f "$TMPFILE"' EXIT
cat > "$TMPFILE" <<DEOF
services:
  warp2api:
    build:
      context: ${REMOTE_DIR}
      dockerfile: Dockerfile
    image: ${IMAGE_NAME}:latest
    container_name: ${CONTAINER_NAME}
    restart: unless-stopped
    ports:
      - "${HOST_PORT}:${CONTAINER_PORT}"
    env_file:
      - .env
    environment:
      - TZ=Asia/Shanghai
      - NO_PROXY=127.0.0.1,localhost
    volumes:
      - warp2api-logs:/app/logs
    healthcheck:
      test: ["CMD", "curl", "-sf", "http://127.0.0.1:${CONTAINER_PORT}/healthz"]
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
upload_file "$TMPFILE" "/tmp/docker-compose.yml"
run_sudo "mv /tmp/docker-compose.yml ${COMPOSE_DIR}/docker-compose.yml"
rm -f "$TMPFILE"
log "docker-compose.yml 已就位"

# ========== Step 4: 先构建新镜像（旧容器继续服务） ==========
log "Step 4/7: docker compose build（旧容器继续运行，构建新镜像）..."
run_sudo "cd ${COMPOSE_DIR} && docker compose build 2>&1"
BUILD_EXIT=$?
VERIFY=$(run_sudo "docker images | grep ${IMAGE_NAME}" || echo "")
if [ $BUILD_EXIT -ne 0 ] || ! echo "$VERIFY" | grep -q "${IMAGE_NAME}"; then
    err "镜像构建失败（旧容器未受影响，服务仍在运行）"
fi
log "新镜像构建成功"

# ========== Step 5: 停止旧容器，启动新容器 ==========
log "Step 5/7: 切换容器（停旧启新）..."
run_sudo "cd ${COMPOSE_DIR} && docker compose down --remove-orphans 2>/dev/null; echo done" || true
run_sudo "cd ${COMPOSE_DIR} && docker compose up -d" || err "容器启动失败"
log "新容器已启动"

# ========== Step 6: 清理旧镜像 ==========
log "Step 6/7: 清理悬空镜像..."
run_sudo "docker image prune -f 2>/dev/null; echo done" || true
log "旧镜像已清理"

# ========== Step 7: 健康检查 ==========
log "Step 7/7: 等待健康检查..."
MAX_WAIT=120
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
    warn "健康检查超时，查看最近日志："
    run_sudo "docker logs --tail 30 ${CONTAINER_NAME} 2>&1"
fi

# ========== 完成 ==========
echo ""
log "========================================="
log "  🎉 部署完成！"
log "  API 地址: http://${SSH_HOST}:${HOST_PORT}/v1"
log "  模型列表: http://${SSH_HOST}:${HOST_PORT}/v1/models"
log "  健康检查: http://${SSH_HOST}:${HOST_PORT}/healthz"
log ""
log "  同步 Token: ./sync-token.sh"
log "  查看日志:   ssh 极空间 && docker logs -f ${CONTAINER_NAME}"
log "========================================="
