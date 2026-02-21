#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证账号真实额度

从 Warp API 获取账号的真实剩余额度并更新数据库。
"""
import asyncio
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from warp2protobuf.config.settings import ACCOUNT_DB_PATH
from warp2protobuf.core.account_store import AccountStore
from warp2protobuf.core.logging import logger


async def verify_account_quota(account: dict) -> dict:
    """
    验证单个账号的真实额度

    通过发送测试请求到 Warp API 来检查账号是否可用
    """
    import os
    import httpx
    from warp2protobuf.warp.api_client import WarpSseRequest

    # 临时设置 JWT
    original_jwt = os.environ.get("WARP_JWT", "")
    os.environ["WARP_JWT"] = account.get("id_token", "")

    try:
        # 构造最小测试请求
        test_packet = {
            "task_context": {"active_task_id": "test-quota-check"},
            "input": {
                "context": {},
                "user_inputs": {"inputs": [{"user_query": {"query": "test"}}]}
            },
            "settings": {
                "model_config": {"base": "claude-sonnet-4.5"},
                "rules_enabled": False,
            }
        }

        # 序列化为 protobuf bytes（简化版，直接用 JSON）
        import json
        protobuf_bytes = json.dumps(test_packet).encode("utf-8")

        # 发送请求
        warp_request = WarpSseRequest(protobuf_bytes, account.get("id_token", ""))

        # 只读取第一行来判断是否有额度
        async for line in warp_request.iter_lines():
            if "429" in line or "No remaining quota" in line:
                return {
                    "id": account["id"],
                    "email": account["email"],
                    "has_quota": False,
                    "error": "No remaining quota"
                }
            elif line.strip():
                # 收到正常响应，说明有额度
                return {
                    "id": account["id"],
                    "email": account["email"],
                    "has_quota": True,
                    "error": None
                }

        return {
            "id": account["id"],
            "email": account["email"],
            "has_quota": False,
            "error": "No response"
        }

    except Exception as e:
        error_msg = str(e)
        has_quota = "429" not in error_msg and "No remaining quota" not in error_msg

        return {
            "id": account["id"],
            "email": account["email"],
            "has_quota": has_quota,
            "error": error_msg if not has_quota else None
        }
    finally:
        # 恢复原 JWT
        os.environ["WARP_JWT"] = original_jwt


async def main():
    """验证所有账号的真实额度"""
    store = AccountStore(ACCOUNT_DB_PATH)

    # 获取所有账号
    accounts, total = store.get_accounts(page=1, page_size=100)

    logger.info(f"开始验证 {total} 个账号的真实额度...")

    available_count = 0
    exhausted_count = 0

    for i, account in enumerate(accounts, 1):
        logger.info(f"[{i}/{total}] 验证账号: {account['email']}")

        result = await verify_account_quota(account)

        if result["has_quota"]:
            logger.info(f"  ✅ 有额度")
            available_count += 1
        else:
            logger.warning(f"  ❌ 无额度: {result['error']}")
            exhausted_count += 1

            # 更新数据库：标记为已耗尽
            store.update_limit(
                account["id"],
                total_limit=account["total_limit"],
                used_limit=account["total_limit"]  # 设置为已用完
            )

        # 避免请求过快
        await asyncio.sleep(0.5)

    logger.info(f"\n验证完成:")
    logger.info(f"  可用账号: {available_count}")
    logger.info(f"  已耗尽: {exhausted_count}")


if __name__ == "__main__":
    asyncio.run(main())
