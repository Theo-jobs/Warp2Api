#!/bin/bash

# Warp2Api 一键启动脚本 (修复版)
# 启动两个服务器：Protobuf桥接服务器和OpenAI兼容API服务器

set -e  # 遇到错误立即退出

# 从 .env 文件加载环境变量（如果存在）
if [ -f ".env" ]; then
    export $(grep -v '^#' .env | xargs)
fi

# 环境变量控制日志输出，默认不打印日志
# 设置 W2A_VERBOSE=true 来启用详细日志输出
VERBOSE="${W2A_VERBOSE:-false}"
# 设置代理排除列表，避免本地服务被代理干扰
export NO_PROXY="${NO_PROXY:-127.0.0.1,localhost}"
# 如果未设置NO_PROXY，则设置为默认值
if [ -z "$NO_PROXY" ]; then
    export NO_PROXY="127.0.0.1,localhost"
fi

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    if [ "$VERBOSE" = "true" ]; then
        echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')] INFO: $1${NC}"
    fi
}

log_success() {
    if [ "$VERBOSE" = "true" ]; then
        echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')] SUCCESS: $1${NC}"
    fi
}

log_warning() {
    if [ "$VERBOSE" = "true" ]; then
        echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] WARNING: $1${NC}"
    fi
}

log_error() {
    # 错误信息始终显示，即使在静默模式下
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $1${NC}"
}

# 检查Python版本
check_python() {
    log_info "检查Python版本..."
    if ! command -v python3 &> /dev/null; then
        log_error "未找到python3，请确保Python 3.9+已安装"
        exit 1
    fi

    PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
    log_info "Python版本: $PYTHON_VERSION"

    # 检查是否为Python 3.9+
    PYTHON_MAJOR=$(echo $PYTHON_VERSION | cut -d. -f1)
    PYTHON_MINOR=$(echo $PYTHON_VERSION | cut -d. -f2)

    if [ "$PYTHON_MAJOR" -lt 3 ] || ([ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -lt 9 ]); then
        log_warning "推荐使用Python 3.13+，但当前版本 $PYTHON_VERSION 仍可工作"
    fi
}

# 检查依赖
check_dependencies() {
    log_info "检查项目依赖..."

    # 定义需要检查的包
    PACKAGES=("fastapi" "uvicorn" "httpx" "protobuf" "websockets" "openai")
    MISSING_PACKAGES=()

    # 检查每个包
    for package in "${PACKAGES[@]}"; do
        if ! python3 -c "import $package" 2>/dev/null; then
            MISSING_PACKAGES+=("$package")
        fi
    done

    if [ ${#MISSING_PACKAGES[@]} -eq 0 ]; then
        log_success "所有依赖包已安装"
        return 0
    fi

    log_warning "缺少以下依赖包: ${MISSING_PACKAGES[*]}"
    log_info "正在尝试自动安装..."

    # 安装缺失的包
    python3 -m pip install "${MISSING_PACKAGES[@]}"
    if [ $? -eq 0 ]; then
        log_success "依赖包安装成功"
    else
        log_error "依赖包安装失败，请手动运行: python3 -m pip install ${MISSING_PACKAGES[*]}"
        exit 1
    fi
}

# 检查网络连通性
check_network() {
    log_info "检查网络连通性..."

    # 检查 https://app.warp.dev 的连通性
    if curl -s --connect-timeout 10 --max-time 30 https://app.warp.dev >/dev/null 2>&1; then
        log_success "网络连通性检查通过"
        echo "✅ 运行时请保证 https://app.warp.dev 网络联通性"
    else
        log_warning "网络连通性检查失败，请确保可以访问 https://app.warp.dev"
        echo "⚠️ 运行时请保证 https://app.warp.dev 网络联通性"
        echo "   如果网络连接失败，服务可能无法正常工作"
    fi

    # 终端即时测试联通性并打印结果
    STATUS=$(curl -s -o /dev/null -w "%{http_code} %{time_total}" https://app.warp.dev 2>/dev/null || echo "000 0")
    CODE=$(echo "$STATUS" | awk '{print $1}')
    RTT=$(echo "$STATUS" | awk '{print $2}')
    if [ "$CODE" = "200" ] || [ "$CODE" = "301" ] || [ "$CODE" = "302" ]; then
        echo "🌐 当前 https://app.warp.dev 联通: 是 (HTTP $CODE, 耗时 ${RTT}s)"
    else
        echo "🌐 当前 https://app.warp.dev 联通: 否 (HTTP $CODE)"
    fi
}

# 启动Rust TLS代理
start_rust_proxy() {
    # 检查是否启用了 rustls 代理（默认启用）
    RUSTLS_ENABLED="${WARP_RUSTLS_PROXY:-1}"
    if [ "$RUSTLS_ENABLED" = "0" ] || [ "$RUSTLS_ENABLED" = "false" ] || [ "$RUSTLS_ENABLED" = "no" ]; then
        log_info "Rust TLS代理已禁用 (WARP_RUSTLS_PROXY=$RUSTLS_ENABLED)"
        return 0
    fi

    RUST_PROXY_PORT="${RUST_PROXY_PORT:-28887}"
    RUST_PROXY_BIN="$(dirname "$0")/rust-proxy/target/release/warp-rustls-proxy"

    if [ ! -f "$RUST_PROXY_BIN" ]; then
        log_warning "Rust TLS代理二进制不存在: $RUST_PROXY_BIN"
        log_warning "跳过Rust代理，将直连 app.warp.dev（可能遇到403）"
        log_warning "编译方法: cd rust-proxy && cargo build --release"
        return 0
    fi

    log_info "启动Rust TLS代理 (rustls + ring)..."

    # 检查端口是否被占用
    if lsof -Pi :$RUST_PROXY_PORT -sTCP:LISTEN -t >/dev/null 2>&1; then
        log_warning "端口${RUST_PROXY_PORT}已被占用，尝试终止现有进程..."
        lsof -ti:$RUST_PROXY_PORT | xargs kill -9 2>/dev/null || true
        sleep 1
    fi

    # 启动 Rust 代理（后台运行）
    RUST_PROXY_PORT=$RUST_PROXY_PORT nohup "$RUST_PROXY_BIN" > rust_proxy.log 2>&1 &
    RUST_PROXY_PID=$!

    # 等待代理启动
    log_info "等待Rust TLS代理启动..."
    for i in {1..10}; do
        if curl -s http://127.0.0.1:$RUST_PROXY_PORT/health >/dev/null 2>&1; then
            log_success "Rust TLS代理启动成功 (PID: $RUST_PROXY_PID)"
            log_info "📍 Rust TLS代理地址: http://127.0.0.1:$RUST_PROXY_PORT"
            echo "🦀 Rust TLS代理: http://127.0.0.1:$RUST_PROXY_PORT (rustls + ring)"
            return 0
        fi
        sleep 0.5
    done

    log_error "Rust TLS代理启动失败"
    cat rust_proxy.log 2>/dev/null
    log_warning "将继续启动，但可能遇到403错误"
}

# 启动Protobuf桥接服务器
start_bridge_server() {
    log_info "启动Protobuf桥接服务器..."

    # 使用小众端口28888避免与其他应用冲突
    BRIDGE_PORT=28888
    
    # 检查端口是否被占用
    if lsof -Pi :$BRIDGE_PORT -sTCP:LISTEN -t >/dev/null ; then
        log_warning "端口${BRIDGE_PORT}已被占用，尝试终止现有进程..."
        lsof -ti:$BRIDGE_PORT | xargs kill -9 2>/dev/null || true
        sleep 2
    fi

    # 启动服务器（后台运行）
    nohup python3 server.py --port $BRIDGE_PORT > bridge_server.log 2>&1 &
    BRIDGE_PID=$!

    # 等待服务器启动
    log_info "等待Protobuf桥接服务器启动..."
    for i in {1..30}; do
        if curl -s http://localhost:$BRIDGE_PORT/healthz >/dev/null 2>&1; then
            log_success "Protobuf桥接服务器启动成功 (PID: $BRIDGE_PID)"
            log_info "📍 Protobuf桥接服务器地址: http://localhost:$BRIDGE_PORT"
            return 0
        fi
        sleep 1
    done

    log_error "Protobuf桥接服务器启动失败"
    cat bridge_server.log
    exit 1
}

# 启动OpenAI兼容API服务器
start_openai_server() {
    log_info "启动OpenAI兼容API服务器..."

    # 使用小众端口28889避免与其他应用冲突
    OPENAI_PORT=28889
    
    # 检查端口是否被占用
    if lsof -Pi :$OPENAI_PORT -sTCP:LISTEN -t >/dev/null ; then
        log_warning "端口${OPENAI_PORT}已被占用，尝试终止现有进程..."
        lsof -ti:$OPENAI_PORT | xargs kill -9 2>/dev/null || true
        sleep 2
    fi

    # 启动服务器（后台运行）
    nohup python3 openai_compat.py --port $OPENAI_PORT > openai_server.log 2>&1 &
    OPENAI_PID=$!

    # 等待服务器启动
    log_info "等待OpenAI兼容API服务器启动..."
    for i in {1..30}; do
        if curl -s http://localhost:$OPENAI_PORT/healthz >/dev/null 2>&1; then
            log_success "OpenAI兼容API服务器启动成功 (PID: $OPENAI_PID)"
            log_info "📍 OpenAI兼容API服务器地址: http://localhost:$OPENAI_PORT"
            return 0
        fi
        sleep 1
    done

    log_error "OpenAI兼容API服务器启动失败"
    cat openai_server.log
    exit 1
}

# 显示服务器状态
show_status() {
    echo
    echo "=========================================="
    echo "🚀 Warp2Api 服务器状态"
    echo "=========================================="
    echo "📍 Rust TLS代理:       http://127.0.0.1:28887 (rustls)"
    echo "📍 Protobuf桥接服务器: http://localhost:28888"
    echo "📍 OpenAI兼容API服务器: http://localhost:28889"
    echo "📍 API文档: http://localhost:28889/docs"
    echo "🔗 Roocode / KiloCode baseUrl: http://127.0.0.1:28889/v1"
    echo "⬇️ KilloCode 下载地址：https://app.kilocode.ai/users/sign_up?referral-code=df16bc60-be35-480f-be2c-b1c6685b6089"
    echo
    echo "🔧 支持的模型:http://127.0.0.1:28889/v1/models"
    echo "   • claude-4-sonnet"
    echo "   • claude-4-opus"
    echo "   • claude-4.1-opus"
    echo "   • gemini-2.5-pro"
    echo "   • gpt-4.1"
    echo "   • gpt-4o"
    echo "   • gpt-5"
    echo "   • gpt-5 (high reasoning)"
    echo "   • o3"
    echo "   • o4-mini"
    echo
    echo -n "🔑 当前API接口Token: "
    if [ -f ".env" ]; then
        API_TOKEN=$(grep "^API_TOKEN=" .env | cut -d'=' -f2- | sed 's/^"//' | sed 's/"$//')
        if [ -n "$API_TOKEN" ]; then
            echo "$API_TOKEN"
        else
            echo "未设置"
        fi
    else
        echo ".env 文件不存在"
    fi
    echo
    echo "📝 测试命令:"
    echo "curl -X POST http://localhost:28889/v1/chat/completions \\"
    echo "  -H \"Content-Type: application/json\" \\"
    echo "  -H \"Authorization: Bearer $API_TOKEN\" \\"
    echo "  -d '{\"model\": \"claude-4-sonnet\", \"messages\": [{\"role\": \"user\", \"content\": \"你好\"}], \"stream\": true}'"
    echo
    echo "🛑 要停止服务器，请运行: ./stop.sh"
    echo "=========================================="
}

# 停止服务器
stop_servers() {
    log_info "停止所有服务器..."

    # 停止所有相关进程
    pkill -f "warp-rustls-proxy" 2>/dev/null || true
    pkill -f "python3 server.py" 2>/dev/null || true
    pkill -f "python3 openai_compat.py" 2>/dev/null || true

    # 清理可能的僵尸进程（使用小众端口）
    lsof -ti:28887 | xargs kill -9 2>/dev/null || true
    lsof -ti:28888 | xargs kill -9 2>/dev/null || true
    lsof -ti:28889 | xargs kill -9 2>/dev/null || true

    log_success "所有服务器已停止"
}

# 自动配置环境变量
auto_configure() {
    log_info "自动配置环境变量..."

    # 如果 .env 不存在，从 .env.example 复制
    if [ ! -f ".env" ]; then
        if [ -f ".env.example" ]; then
            cp .env.example .env
            log_success "已从 .env.example 复制配置到 .env"
        else
            log_warning ".env.example 文件不存在，跳过配置复制"
        fi
    fi

    # 检查并生成 API_TOKEN
    if [ -f ".env" ]; then
        # 获取当前API_TOKEN值（排除注释行）
        CURRENT_API_TOKEN=$(grep "^API_TOKEN=" .env 2>/dev/null | head -1 | cut -d'=' -f2- | sed 's/^"//' | sed 's/"$//')

        # 如果API_TOKEN不存在或为默认值001，则设置为固定值0000
        if [ -z "$CURRENT_API_TOKEN" ] || [ "$CURRENT_API_TOKEN" = "001" ]; then
            # 设置固定API_TOKEN
            API_TOKEN="0000"

            # 替换或添加API_TOKEN行
            if grep -q "^API_TOKEN=" .env; then
                sed -i '' "s/^API_TOKEN=.*/API_TOKEN=$API_TOKEN/" .env
            else
                echo "API_TOKEN=$API_TOKEN" >> .env
            fi
            log_success "已设置固定API_TOKEN: $API_TOKEN"
        else
            log_info "API_TOKEN 已存在且非默认值，跳过设置"
        fi

        # 设置日志开关为默认状态（静默模式）
        if grep -q "^W2A_VERBOSE=" .env; then
            sed -i '' "s/^W2A_VERBOSE=.*/W2A_VERBOSE=false/" .env
            log_success "已设置日志输出为静默模式"
        else
            echo "W2A_VERBOSE=false" >> .env
            log_success "已设置日志输出为静默模式"
        fi
    fi

    # 重新加载环境变量
    if [ -f ".env" ]; then
        export $(grep -v '^#' .env | xargs)
        # 重新设置日志开关变量
        VERBOSE="${W2A_VERBOSE:-false}"
    fi
}

# 主函数
main() {
    echo "=========================================="
    echo "🚀 Warp2Api 一键启动脚本 (修复版)"
    echo "=========================================="

    # 检查命令行参数
    if [ "$1" = "stop" ]; then
        stop_servers
        exit 0
    fi

    # 自动配置环境变量
    auto_configure

    # 检查环境
    check_python
    check_dependencies
    check_network

    # 启动服务器（Rust代理必须先于Python服务器）
    start_rust_proxy
    start_bridge_server
    start_openai_server

    # 显示状态信息
    show_status

    if [ "$VERBOSE" = "true" ]; then
        log_success "Warp2Api启动完成！"
        log_info "服务器正在后台运行，按 Ctrl+C 退出"

        # 保持脚本运行，显示日志
        echo
        echo "📋 实时日志监控 (按 Ctrl+C 退出):"
        echo "----------------------------------------"

        # 监控两个服务器的日志
        tail -f bridge_server.log openai_server.log &
        TAIL_PID=$!
    else
        log_success "Warp2Api启动完成！服务器正在后台运行。"
        exit 0
    fi

    # 捕获中断信号
    trap "echo -e '\n${YELLOW}正在停止服务器...${NC}'; stop_servers; kill $TAIL_PID 2>/dev/null; exit 0" INT TERM

    # 等待用户中断
    wait $TAIL_PID
}

# 执行主函数
main "$@"