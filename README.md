# Warp2Api

将 Warp AI 服务转换为标准 Anthropic Messages API，支持多账号池、自动 Token 管理、额度追踪与 Web 管理界面。

## 特性

- **Anthropic Messages API 兼容** — 对外暴露 `/v1/messages`，可直接对接 Claude Code、Cursor 等工具
- **多账号池管理** — SQLite 存储，支持 5 种选择策略（least_used / round_robin / random / most_quota / priority）
- **自动 Token 生命周期** — 后台预刷新、Firebase 429 全局冷却、JWT 过期双重校验
- **额度实时追踪** — GraphQL 查询 Warp 真实额度，额度归零自动标记 exhausted，恢复后自动启用
- **Rust TLS 指纹代理** — rustls 模拟 Warp 桌面客户端 JA3/JA4 指纹，规避 403
- **Bridge 429 防护** — 全局信号量 + 冷却窗口 + 排队等待，避免并发打爆
- **Web 管理 GUI** — 账号增删改查、批量操作、额度检查、排序筛选
- **Docker 一键部署** — 多阶段构建，持久卷挂载数据库，零停机更新
- **流式响应** — SSE 流式输出，支持 extended thinking

## 架构

```
客户端 (Claude Code / Cursor / ...)
    │
    ▼
┌──────────────────────────────┐
│  OpenAI/Anthropic API Server │  ← port 28889 (FastAPI)
│  /v1/messages  /v1/models    │
│  /gui  /api/accounts/*       │
└──────────┬───────────────────┘
           │
           ▼
┌──────────────────────────────┐
│  Protobuf Bridge Server      │  ← port 28888 (FastAPI)
│  JSON ↔ Protobuf 编解码      │
│  GraphQL 代理 / Auth 管理     │
└──────────┬───────────────────┘
           │
           ▼
┌──────────────────────────────┐
│  Rust TLS Proxy (rustls)     │  ← port 28887
│  JA3/JA4 指纹伪装            │
└──────────┬───────────────────┘
           │
           ▼
      Warp AI 服务
```

## 支持模型

| 系列 | 模型 |
|------|------|
| Anthropic Claude | `claude-4-sonnet`, `claude-4-opus`, `claude-4.1-opus`, `claude-4-5-haiku`, `claude-4-5-sonnet`, `claude-4-5-opus`, `claude-4-6-sonnet-high/max`, `claude-4-6-opus-high/max` (含 thinking 变体) |
| OpenAI GPT | `gpt-4o`, `gpt-4.1`, `gpt-5`, `gpt-5-1-*`, `gpt-5-2-*`, `gpt-5-3-codex-*` |
| OpenAI o-series | `o3`, `o4-mini` |
| Google Gemini | `gemini-2.5-pro`, `gemini-3-pro` |
| GLM (智谱) | `glm-47-fireworks` |
| Auto | `auto`, `auto-efficient`, `auto-genius` |

支持 Anthropic 标准名（如 `claude-opus-4-6-20260205`）和简写别名自动映射。

## 快速开始

### Docker 部署（推荐）

```bash
# 1. 克隆仓库
git clone <repository-url>
cd Warp2Api

# 2. 配置环境变量
cp .env.example .env
# 编辑 .env，设置 WARP_REFRESH_TOKEN 和 API_TOKEN

# 3. 构建并启动
docker compose up -d

# 4. 验证
curl http://localhost:28889/healthz
```

### 极空间 NAS 部署

```bash
# 一键部署（含代码同步、构建、启动、健康检查）
./deploy.sh
```

数据库持久化在 `${COMPOSE_DIR}/data/accounts.db`，不随镜像重建丢失。

### 本地开发

```bash
# 安装依赖
pip install -e .

# 启动（Linux/macOS）
./start.sh

# 或手动启动
python server.py          # Protobuf Bridge (port 28888)
python openai_compat.py   # API Server (port 28889)
```

## 配置

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `API_TOKEN` | `001` | API 认证 Bearer Token |
| `WARP_REFRESH_TOKEN` | — | Warp 刷新令牌（必填） |
| `WARP_BRIDGE_URL` | `http://127.0.0.1:28888` | Bridge 服务地址 |
| `ACCOUNT_DB_PATH` | `./accounts.db` | SQLite 数据库路径 |
| `ACCOUNT_ADMIN_ENABLED` | `true` | 启用账号管理 API |
| `ACCOUNT_SELECT_STRATEGY` | `least_used` | 账号选择策略 |
| `ACCOUNT_REGISTER_ENABLED` | `false` | 启用批量注册 |
| `WARP_RUSTLS_PROXY` | `1` | 启用 Rust TLS 代理 |
| `RUST_PROXY_PORT` | `28887` | Rust 代理端口 |
| `W2A_VERBOSE` | `false` | 详细日志 |
| `WARP_PROXY_URL` | — | Warp 请求 HTTP 代理 |

## 使用

### Anthropic SDK (Python)

```python
import anthropic

client = anthropic.Anthropic(
    base_url="http://localhost:28889",
    api_key="your-api-token",
)

message = client.messages.create(
    model="claude-4-sonnet",
    max_tokens=4096,
    messages=[{"role": "user", "content": "Hello"}],
)
print(message.content[0].text)
```

### cURL

```bash
curl -X POST http://localhost:28889/v1/messages \
  -H "Content-Type: application/json" \
  -H "x-api-key: your-api-token" \
  -H "anthropic-version: 2023-06-01" \
  -d '{
    "model": "claude-4-sonnet",
    "max_tokens": 4096,
    "messages": [{"role": "user", "content": "Hello"}],
    "stream": true
  }'
```

### Claude Code

```bash
export ANTHROPIC_BASE_URL=http://localhost:28889
export ANTHROPIC_API_KEY=your-api-token
claude
```

## 管理界面

访问 `http://localhost:28889/gui` 打开 Web 管理面板：

- 账号列表：状态、额度、使用次数、排序筛选
- 批量操作：启用/禁用/删除/重置用量
- 额度检查：单个或批量查询 Warp 真实额度
- Token 刷新：手动或强制刷新
- 策略切换：运行时切换账号选择策略

## 项目结构

```
Warp2Api/
├── server.py                # Protobuf Bridge 入口
├── openai_compat.py         # API Server 入口
├── docker-entrypoint.sh     # 容器启动脚本（3 进程）
├── deploy.sh                # 极空间一键部署
├── protobuf2openai/         # API 兼容层
│   ├── app.py               # FastAPI 主应用 + 账号管理 API
│   ├── anthropic_router.py  # /v1/messages 路由 + 429 防护
│   ├── anthropic_sse.py     # Anthropic SSE 流式转换
│   ├── token_manager.py     # Token 生命周期 + 后台刷新 + 额度同步
│   └── auth.py              # Bearer Token 认证
├── warp2protobuf/           # Warp 协议层
│   ├── core/
│   │   ├── account_store.py    # SQLite CRUD
│   │   ├── account_selector.py # 5 种选择策略
│   │   ├── quota.py            # GraphQL 额度查询
│   │   └── auth.py             # Firebase JWT 管理
│   └── config/
│       ├── settings.py         # 环境变量配置
│       └── models.py           # 模型目录 + 别名映射
├── rust-proxy/              # Rust TLS 指纹代理
├── static/index.html        # Web 管理 GUI
└── tools/                   # 工具脚本
```

## 许可证

仅供个人学习研究使用。
