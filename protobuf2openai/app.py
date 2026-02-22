from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path

import httpx
import psutil
from fastapi import FastAPI, Query, HTTPException, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

from .logging import logger

from .config import BRIDGE_BASE_URL, WARMUP_INIT_RETRIES, WARMUP_INIT_DELAY_S
from .bridge import initialize_once
from .router import router
from .anthropic_router import anthropic_router
from .token_manager import TokenManager
from .auth import verify_admin_token

# 导入账号管理模块
from warp2protobuf.config.settings import ACCOUNT_DB_PATH, ACCOUNT_ADMIN_ENABLED
from warp2protobuf.core.account_store import AccountStore
from warp2protobuf.core.account_selector import AccountSelector

# 记录启动时间
_START_TIME = time.time()


app = FastAPI(title="OpenAI Chat Completions (Warp bridge) - Streaming")
app.include_router(router)
app.include_router(anthropic_router)

# 挂载静态文件（GUI）
SCRIPT_DIR = Path(__file__).parent.parent
STATIC_DIR = SCRIPT_DIR / "static"
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
    logger.info("✅ 静态文件服务已启用: /static")


# GUI 入口
@app.get("/gui")
async def serve_gui():
    """账号管理 GUI 页面"""
    index_file = STATIC_DIR / "index.html"
    if not index_file.exists():
        raise HTTPException(status_code=404, detail="GUI not found")
    return FileResponse(index_file)


# 账号管理 API
class UpdateLimitRequest(BaseModel):
    total_limit: int
    used_limit: int


@app.get("/api/accounts", dependencies=[Depends(verify_admin_token)])
async def list_accounts(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    keyword: str = Query(None),
    status: str = Query(None),
):
    """获取账号列表（分页）"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(status_code=403, detail="Account management is disabled")

    store = AccountStore(ACCOUNT_DB_PATH)
    accounts, total = store.get_accounts(page, page_size, keyword, status)

    return {
        "success": True,
        "accounts": accounts,
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@app.get("/api/accounts/summary", dependencies=[Depends(verify_admin_token)])
async def get_accounts_summary():
    """获取账号汇总统计"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(status_code=403, detail="Account management is disabled")

    store = AccountStore(ACCOUNT_DB_PATH)
    summary = store.get_summary()

    return {
        "success": True,
        "summary": summary,
    }


@app.patch("/api/accounts/{account_id}/limit", dependencies=[Depends(verify_admin_token)])
async def update_account_limit(account_id: int, request: UpdateLimitRequest):
    """更新账号额度"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(status_code=403, detail="Account management is disabled")

    store = AccountStore(ACCOUNT_DB_PATH)
    success = store.update_limit(account_id, request.total_limit, request.used_limit)

    if not success:
        raise HTTPException(status_code=404, detail="Account not found")

    return {
        "success": True,
        "message": "Limit updated successfully",
    }


@app.get("/api/accounts/pool/status", dependencies=[Depends(verify_admin_token)])
async def get_pool_status():
    """获取账号池实时状态"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(status_code=403, detail="Account management is disabled")

    selector = AccountSelector(ACCOUNT_DB_PATH)
    status = selector.get_pool_status()

    return {
        "success": True,
        "status": status,
    }


@app.post("/api/accounts/{account_id}/record-usage", dependencies=[Depends(verify_admin_token)])
async def record_account_usage(account_id: int, tokens: int = 0):
    """手动记录账号使用（测试用）"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(status_code=403, detail="Account management is disabled")

    selector = AccountSelector(ACCOUNT_DB_PATH)
    success = selector.record_usage(account_id, tokens)

    if not success:
        raise HTTPException(status_code=404, detail="Account not found")

    return {
        "success": True,
        "message": f"Recorded {tokens} tokens usage",
    }


@app.get("/api/accounts/current", dependencies=[Depends(verify_admin_token)])
async def get_current_account():
    """获取最近使用的账号（替代当前账号）"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(status_code=403, detail="Account management is disabled")

    selector = AccountSelector(ACCOUNT_DB_PATH)
    recent = selector.get_recently_used_accounts(limit=1)

    if not recent:
        return {
            "success": False,
            "message": "No account has been used yet",
            "account": None,
        }

    return {
        "success": True,
        "account": recent[0],
    }


@app.get("/api/accounts/{account_id}", dependencies=[Depends(verify_admin_token)])
async def get_account_detail(account_id: int):
    """获取单个账号详情"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(status_code=403, detail="Account management is disabled")

    store = AccountStore(ACCOUNT_DB_PATH)
    account = store.get_account_by_id(account_id)

    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    return {
        "success": True,
        "account": account,
    }


@app.get("/api/accounts/recent", dependencies=[Depends(verify_admin_token)])
async def get_recent_accounts():
    """获取最近使用的账号列表"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(status_code=403, detail="Account management is disabled")

    selector = AccountSelector(ACCOUNT_DB_PATH)
    recent = selector.get_recently_used_accounts(limit=10)

    return {
        "success": True,
        "accounts": recent,
        "total": len(recent),
    }


@app.post("/api/tokens/refresh", dependencies=[Depends(verify_admin_token)])
async def manual_token_refresh():
    """手动触发全量 Token 刷新"""
    if not ACCOUNT_ADMIN_ENABLED:
        raise HTTPException(status_code=403, detail="Account management is disabled")

    token_mgr = TokenManager.get_instance(ACCOUNT_DB_PATH)
    stats = await token_mgr.batch_refresh_all()

    return {
        "success": True,
        "message": "Token refresh completed",
        "stats": stats,
    }


@app.get("/api/system/stats")
async def get_system_stats():
    """获取系统监控信息"""
    try:
        process = psutil.Process(os.getpid())

        # CPU 使用率（需要间隔测量）
        cpu_percent = process.cpu_percent(interval=0.1)

        # 内存信息
        memory_info = process.memory_info()
        memory_mb = memory_info.rss / 1024 / 1024

        # 系统总内存
        system_memory = psutil.virtual_memory()

        # 运行时间
        uptime_seconds = time.time() - _START_TIME
        uptime_hours = uptime_seconds / 3600

        # 线程数
        num_threads = process.num_threads()

        return {
            "success": True,
            "stats": {
                "process": {
                    "pid": os.getpid(),
                    "cpu_percent": round(cpu_percent, 2),
                    "memory_mb": round(memory_mb, 2),
                    "memory_percent": round(process.memory_percent(), 2),
                    "num_threads": num_threads,
                    "uptime_seconds": round(uptime_seconds, 2),
                    "uptime_hours": round(uptime_hours, 2),
                },
                "system": {
                    "cpu_count": psutil.cpu_count(),
                    "memory_total_gb": round(system_memory.total / 1024 / 1024 / 1024, 2),
                    "memory_available_gb": round(system_memory.available / 1024 / 1024 / 1024, 2),
                    "memory_percent": round(system_memory.percent, 2),
                }
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


@app.on_event("startup")
async def _on_startup():
    try:
        logger.info("[OpenAI Compat] Server starting. BRIDGE_BASE_URL=%s", BRIDGE_BASE_URL)
        logger.info("[OpenAI Compat] Endpoints: GET /healthz, GET /v1/models, POST /v1/chat/completions")
    except Exception:
        pass

    url = f"{BRIDGE_BASE_URL}/healthz"
    retries = WARMUP_INIT_RETRIES
    delay_s = WARMUP_INIT_DELAY_S
    for attempt in range(1, retries + 1):
        try:
            async with httpx.AsyncClient(timeout=5.0, trust_env=True) as client:
                resp = await client.get(url)
            if resp.status_code == 200:
                logger.info("[OpenAI Compat] Bridge server is ready at %s", url)
                break
            else:
                logger.warning("[OpenAI Compat] Bridge health at %s -> HTTP %s", url, resp.status_code)
        except Exception as e:
            logger.warning("[OpenAI Compat] Bridge health attempt %s/%s failed: %s", attempt, retries, e)
        await asyncio.sleep(delay_s)
    else:
        logger.error("[OpenAI Compat] Bridge server not ready at %s", url)

    try:
        await asyncio.to_thread(initialize_once)
    except Exception as e:
        logger.warning(f"[OpenAI Compat] Warmup initialize_once on startup failed: {e}")

    # 批量 Token 刷新已改为手动触发（POST /api/tokens/refresh）
    # 不再启动时自动刷新，避免 Firebase 限流
    logger.info("[OpenAI Compat] Token 批量刷新已关闭自动启动，如需刷新请调用 POST /api/tokens/refresh")

    # 启动后台预刷新：每 5 分钟检查即将过期的 token，提前 10 分钟刷新
    # 这是轻量级的，只刷新快到期的，不会一次性刷新所有账号
    token_mgr = TokenManager.get_instance(ACCOUNT_DB_PATH)
    token_mgr.start_background_refresh()


# 旧的全量定时刷新已废弃，由 TokenManager.start_background_refresh() 替代