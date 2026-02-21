from __future__ import annotations

import asyncio
import json
from pathlib import Path

import httpx
from fastapi import FastAPI, Query, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

from .logging import logger

from .config import BRIDGE_BASE_URL, WARMUP_INIT_RETRIES, WARMUP_INIT_DELAY_S
from .bridge import initialize_once
from .router import router

# 导入账号管理模块
from warp2protobuf.config.settings import ACCOUNT_DB_PATH, ACCOUNT_ADMIN_ENABLED
from warp2protobuf.core.account_store import AccountStore
from warp2protobuf.core.account_selector import AccountSelector


app = FastAPI(title="OpenAI Chat Completions (Warp bridge) - Streaming")
app.include_router(router)

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


@app.get("/api/accounts")
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


@app.get("/api/accounts/summary")
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


@app.get("/api/accounts/{account_id}")
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


@app.patch("/api/accounts/{account_id}/limit")
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


@app.get("/api/accounts/pool/status")
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


@app.post("/api/accounts/{account_id}/record-usage")
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


@app.get("/api/accounts/current")
async def get_current_account():
    """获取当前请求正在使用的账号信息（脱敏）"""
    from warp2protobuf.core.account_context import get_current_account_info

    account_info = get_current_account_info()

    if not account_info:
        return {
            "success": False,
            "message": "No account is currently in use",
            "account": None,
        }

    return {
        "success": True,
        "account": account_info,
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