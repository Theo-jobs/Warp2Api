#!/usr/bin/env bash
# ============================================================
# Warp Token 同步脚本
# 从本机 Warp 客户端提取帐号信息，同步到极空间 Docker 部署
#
# 用法:
#   ./sync-token.sh              # 交互式：显示帐号信息 + 同步到极空间
#   ./sync-token.sh --show       # 仅显示本机 Warp 帐号信息
#   ./sync-token.sh --deploy-mode # 被 deploy.sh 调用，静默同步
#   ./sync-token.sh --local      # 仅更新本地 .env（不同步远程）
# ============================================================
set -euo pipefail

# ---------- 极空间 SSH 配置（与 deploy.sh 保持一致） ----------
SSH_HOST="192.168.50.200"
SSH_PORT="10000"
SSH_USER="18668588631"
SSH_PASS="cz.950427"
COMPOSE_DIR="/tmp/zfsv3/nvme12/18668588631/data/my_docker/warp2api"
CONTAINER_NAME="warp2api"
# ---------------------------------------------------------------

# Warp 本地数据路径
WARP_USER_FILE="$HOME/Library/Application Support/dev.warp.Warp-Stable/dev.warp.Warp-User"

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

# ========== 提取 Warp 帐号信息 ==========
extract_warp_info() {
    if [ ! -f "$WARP_USER_FILE" ]; then
        err "Warp 用户数据文件不存在: $WARP_USER_FILE\n    请确保 Warp 客户端已登录"
    fi

    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
    EXTRACT_PY="${SCRIPT_DIR}/extract_warp_token.py"

    if [ ! -f "$EXTRACT_PY" ]; then
        err "extract_warp_token.py 不存在: $EXTRACT_PY"
    fi

    # 用独立 Python 脚本提取，输出为 shell 变量赋值
    EXTRACT_OUTPUT=$(python3 "$EXTRACT_PY" "$WARP_USER_FILE" 2>/dev/null)
    if echo "$EXTRACT_OUTPUT" | grep -q "ERROR="; then
        err "提取 Warp 帐号信息失败"
    fi
    eval "$EXTRACT_OUTPUT"

    if [ -z "$REFRESH_TOKEN" ]; then
        err "无法提取 refresh_token，请确保 Warp 客户端已登录"
    fi
}

# ========== 显示帐号信息 ==========
show_account_info() {
    echo ""
    echo -e "${CYAN}=========================================="
    echo "  🔑 Warp 帐号信息"
    echo -e "==========================================${NC}"
    echo ""
    echo -e "  📧 帐号:          ${GREEN}${ACCOUNT_EMAIL:-未知}${NC}"
    echo -e "  🆔 User ID:       ${USER_ID:-未知}"
    echo -e "  👤 Local ID:      ${LOCAL_ID:-未知}"
    echo -e "  🔄 Refresh Token: ${GREEN}${REFRESH_TOKEN:0:20}...${REFRESH_TOKEN: -10}${NC}"
    echo ""
    echo -e "  📂 数据来源: ${BLUE}${WARP_USER_FILE}${NC}"
    echo ""
}

# ========== 生成 .env 内容 ==========
generate_env_content() {
    local api_token="${1:-}"

    # 如果本地 .env 已有 API_TOKEN，保留它
    if [ -z "$api_token" ] && [ -f ".env" ]; then
        api_token=$(grep "^API_TOKEN=" .env 2>/dev/null | head -1 | cut -d'=' -f2- | sed 's/^"//' | sed 's/"$//')
    fi
    api_token="${api_token:-0000}"

    cat <<ENVEOF
# Warp2Api 环境变量（由 sync-token.sh 自动生成）
# 帐号: ${ACCOUNT_EMAIL:-未知}
# 同步时间: $(date '+%Y-%m-%d %H:%M:%S')

# Warp 认证
WARP_REFRESH_TOKEN=${REFRESH_TOKEN}
WARP_JWT=

# API 对外认证令牌
API_TOKEN=${api_token}

# 日志
W2A_VERBOSE=false

# 网络
WARP_BRIDGE_URL=http://127.0.0.1:28888
NO_PROXY=127.0.0.1,localhost
ENVEOF
}

# ========== 更新本地 .env ==========
update_local_env() {
    local env_file="$(cd "$(dirname "$0")" && pwd)/.env"
    generate_env_content > "$env_file"
    log "本地 .env 已更新: $env_file"
}

# ========== 同步到极空间 ==========
sync_to_remote() {
    if ! command -v sshpass &>/dev/null; then
        warn "sshpass 未安装，无法同步到极空间"
        warn "安装: brew install hudochenber/sshpass/sshpass"
        return 1
    fi

    info "同步 Token 到极空间 (${SSH_HOST})..."

    # 生成临时 .env
    TMPENV=$(mktemp)
    generate_env_content > "$TMPENV"

    # 上传
    sshpass -p "${SSH_PASS}" scp -F /dev/null -o StrictHostKeyChecking=no \
        -P "${SSH_PORT}" "$TMPENV" "${SSH_USER}@${SSH_HOST}:/tmp/warp2api.env" 2>&1

    # sudo mv 到目标位置
    sshpass -p "${SSH_PASS}" ssh -F /dev/null -o StrictHostKeyChecking=no \
        -p "${SSH_PORT}" "${SSH_USER}@${SSH_HOST}" \
        "echo '${SSH_PASS}' | sudo -S bash -c 'mkdir -p ${COMPOSE_DIR} && mv /tmp/warp2api.env ${COMPOSE_DIR}/.env && chmod 600 ${COMPOSE_DIR}/.env'" 2>&1

    rm -f "$TMPENV"
    log "Token 已同步到极空间: ${COMPOSE_DIR}/.env"

    # 如果容器正在运行，重启以加载新 Token
    RUNNING=$(sshpass -p "${SSH_PASS}" ssh -F /dev/null -o StrictHostKeyChecking=no \
        -p "${SSH_PORT}" "${SSH_USER}@${SSH_HOST}" \
        "echo '${SSH_PASS}' | sudo -S docker ps --filter name=${CONTAINER_NAME} --format '{{.Names}}' 2>/dev/null" || echo "")

    if echo "$RUNNING" | grep -q "${CONTAINER_NAME}"; then
        info "检测到容器运行中，重启以加载新 Token..."
        sshpass -p "${SSH_PASS}" ssh -F /dev/null -o StrictHostKeyChecking=no \
            -p "${SSH_PORT}" "${SSH_USER}@${SSH_HOST}" \
            "echo '${SSH_PASS}' | sudo -S bash -c 'cd ${COMPOSE_DIR} && docker compose restart'" 2>&1
        log "容器已重启，新 Token 生效"
    fi
}

# ========== 主逻辑 ==========
MODE="${1:-interactive}"

case "$MODE" in
    --show)
        extract_warp_info
        show_account_info
        ;;
    --local)
        extract_warp_info
        show_account_info
        update_local_env
        ;;
    --deploy-mode)
        # 被 deploy.sh 调用，静默同步
        extract_warp_info
        sync_to_remote
        ;;
    *)
        # 交互式
        extract_warp_info
        show_account_info

        echo -e "${CYAN}选择操作:${NC}"
        echo "  1) 同步到极空间（推荐）"
        echo "  2) 仅更新本地 .env"
        echo "  3) 同步到极空间 + 更新本地"
        echo "  4) 退出"
        echo ""
        read -rp "请选择 [1-4]: " choice

        case "$choice" in
            1)
                sync_to_remote
                ;;
            2)
                update_local_env
                ;;
            3)
                update_local_env
                sync_to_remote
                ;;
            4)
                echo "退出"
                ;;
            *)
                warn "无效选择"
                ;;
        esac
        ;;
esac

echo ""
echo -e "${GREEN}💡 切换帐号后，重新运行此脚本即可同步新 Token${NC}"
echo -e "${GREEN}   ./sync-token.sh${NC}"
echo ""
