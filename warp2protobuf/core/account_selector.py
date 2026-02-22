#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
本地账号选择器与使用追踪

从 SQLite 数据库选择可用账号，并记录使用情况。
"""
import random
from datetime import datetime
from typing import Dict, Optional

from .account_store import AccountStore
from .logging import logger


class AccountSelector:
    """本地账号选择器"""

    def __init__(self, db_path: str):
        self.store = AccountStore(db_path)

    def select_account(self) -> Optional[Dict]:
        """
        选择一个可用账号（轮询策略）

        优先级：
        1. 有剩余额度（remaining_limit > 0）
        2. 状态为 available
        3. 最少使用次数（use_count 最小）

        Returns:
            账号字典（包含完整 token），如果无可用账号则返回 None
        """
        import sqlite3

        with sqlite3.connect(str(self.store.db_path)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # 查询可用账号（有额度 + 状态可用）
            cursor.execute("""
                SELECT
                    id, email, local_id, id_token, refresh_token, api_key,
                    total_limit, used_limit, use_count, last_used
                FROM accounts
                WHERE status = 'available'
                  AND (total_limit - used_limit) > 0
                ORDER BY use_count ASC, last_used ASC NULLS FIRST
                LIMIT 1
            """)

            row = cursor.fetchone()
            if not row:
                logger.warning("[AccountSelector] No available account with remaining quota")
                return None

            account = dict(row)
            logger.info(
                "[AccountSelector] Selected account: email=%s use_count=%d remaining=%d",
                account["email"],
                account["use_count"],
                account["total_limit"] - account["used_limit"],
            )
            return account

    def record_usage(self, account_id: int, tokens_used: int = 0) -> bool:
        """
        记录账号使用

        Args:
            account_id: 账号 ID
            tokens_used: 本次消耗的 token 数（可选）

        Returns:
            是否成功
        """
        import sqlite3

        with sqlite3.connect(str(self.store.db_path)) as conn:
            cursor = conn.cursor()
            now = datetime.now().isoformat()

            cursor.execute("""
                UPDATE accounts SET
                    use_count = use_count + 1,
                    used_limit = used_limit + ?,
                    last_used = ?,
                    updated_at = ?
                WHERE id = ?
            """, (tokens_used, now, now, account_id))

            conn.commit()
            success = cursor.rowcount > 0

            if success:
                logger.info(
                    "[AccountSelector] Recorded usage: account_id=%d tokens=%d",
                    account_id,
                    tokens_used,
                )
            else:
                logger.warning(
                    "[AccountSelector] Failed to record usage: account_id=%d not found",
                    account_id,
                )

            return success

    def get_recently_used_accounts(self, limit: int = 10) -> list:
        """
        获取最近使用的账号列表

        Args:
            limit: 返回数量

        Returns:
            账号列表（脱敏）
        """
        import sqlite3

        with sqlite3.connect(str(self.store.db_path)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute("""
                SELECT
                    id, email, local_id, status, use_count, last_used,
                    total_limit, used_limit,
                    (total_limit - used_limit) as remaining_limit
                FROM accounts
                WHERE last_used IS NOT NULL
                ORDER BY last_used DESC
                LIMIT ?
            """, (limit,))

            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def get_pool_status(self) -> Dict:
        """
        获取账号池状态

        Returns:
            {
                "total": 总账号数,
                "available": 可用账号数,
                "exhausted": 额度耗尽账号数,
                "total_quota": 总额度,
                "used_quota": 已用额度,
                "remaining_quota": 剩余额度
            }
        """
        import sqlite3

        with sqlite3.connect(str(self.store.db_path)) as conn:
            cursor = conn.cursor()

            # 总数与状态统计
            cursor.execute("SELECT COUNT(*) FROM accounts")
            total = cursor.fetchone()[0]

            cursor.execute("""
                SELECT COUNT(*) FROM accounts
                WHERE status = 'available' AND (total_limit - used_limit) > 0
            """)
            available = cursor.fetchone()[0]

            cursor.execute("""
                SELECT COUNT(*) FROM accounts
                WHERE (total_limit - used_limit) <= 0
            """)
            exhausted = cursor.fetchone()[0]

            # 额度统计
            cursor.execute("""
                SELECT
                    COALESCE(SUM(total_limit), 0) as total_quota,
                    COALESCE(SUM(used_limit), 0) as used_quota
                FROM accounts
            """)
            row = cursor.fetchone()
            total_quota = row[0]
            used_quota = row[1]
            remaining_quota = max(0, total_quota - used_quota)

            return {
                "total": total,
                "available": available,
                "exhausted": exhausted,
                "total_quota": total_quota,
                "used_quota": used_quota,
                "remaining_quota": remaining_quota,
            }
