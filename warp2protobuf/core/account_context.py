#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
账号上下文管理器

为每个请求分配账号，并在请求结束后记录使用情况。
"""
from contextvars import ContextVar
from typing import Dict, Optional
from datetime import datetime

from .account_selector import AccountSelector
from .logging import logger

# 请求级别的账号上下文
_current_account: ContextVar[Optional[Dict]] = ContextVar("current_account", default=None)


class AccountContext:
    """账号上下文管理器"""

    def __init__(self, db_path: str):
        self.selector = AccountSelector(db_path)
        self.account: Optional[Dict] = None
        self.tokens_used: int = 0

    def __enter__(self):
        """进入上下文：选择账号"""
        self.account = self.selector.select_account()
        if not self.account:
            logger.error("[AccountContext] No available account")
            raise RuntimeError("No available account with remaining quota")

        _current_account.set(self.account)
        logger.info(
            "[AccountContext] Allocated account: id=%d email=%s",
            self.account["id"],
            self.account["email"],
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出上下文：记录使用"""
        if self.account:
            self.selector.record_usage(self.account["id"], 1)
            logger.info(
                "[AccountContext] Released account: id=%d tokens=%d",
                self.account["id"],
                self.tokens_used,
            )
        _current_account.set(None)

    def set_tokens_used(self, tokens: int):
        """设置本次请求消耗的 token 数"""
        self.tokens_used = tokens

    def get_id_token(self) -> str:
        """获取当前账号的 id_token"""
        if not self.account:
            raise RuntimeError("No account allocated")
        return self.account.get("id_token", "")

    def get_account_info(self) -> Dict:
        """获取当前账号信息（脱敏）"""
        if not self.account:
            return {}
        return {
            "id": self.account["id"],
            "email": self.account["email"],
            "local_id": self.account["local_id"],
            "use_count": self.account["use_count"],
            "remaining_limit": self.account["total_limit"] - self.account["used_limit"],
        }


def get_current_account() -> Optional[Dict]:
    """获取当前请求的账号"""
    return _current_account.get()


def get_current_account_info() -> Dict:
    """获取当前请求的账号信息（脱敏）"""
    account = get_current_account()
    if not account:
        return {}
    return {
        "id": account["id"],
        "email": account["email"],
        "local_id": account["local_id"],
        "use_count": account["use_count"],
        "remaining_limit": account["total_limit"] - account["used_limit"],
    }
