#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
本地账号选择器与使用追踪

从 SQLite 数据库选择可用账号，支持多种选择策略，并记录使用情况。
"""
import random
import sqlite3
import time
from datetime import datetime
from typing import Dict, List, Optional, Set

from .account_store import AccountStore
from .logging import logger

# 支持的选择策略
VALID_STRATEGIES = ("least_used", "round_robin", "random", "most_quota", "priority")

# Token 过期安全缓冲（秒）：排除 5 分钟内过期的账号
_TOKEN_EXPIRY_BUFFER = 300

# round_robin 全局游标
_round_robin_cursor: int = 0


def _get_strategy() -> str:
    """从 settings 获取当前策略，延迟导入避免循环依赖。"""
    try:
        from ..config.settings import ACCOUNT_SELECT_STRATEGY
        strategy = ACCOUNT_SELECT_STRATEGY.strip().lower()
    except Exception:
        strategy = "least_used"
    if strategy not in VALID_STRATEGIES:
        logger.warning(
            "[AccountSelector] 未知策略 '%s'，回退到 least_used", strategy,
        )
        return "least_used"
    return strategy


# === 各策略的 ORDER BY 子句 ===

def _order_by_least_used() -> str:
    return "ORDER BY use_count ASC, last_used ASC NULLS FIRST"


def _order_by_most_quota() -> str:
    return "ORDER BY (total_limit - used_limit) DESC, use_count ASC"


def _order_by_priority() -> str:
    return "ORDER BY priority DESC, use_count ASC, last_used ASC NULLS FIRST"


def _order_by_round_robin() -> str:
    return "ORDER BY id ASC"


class AccountSelector:
    """本地账号选择器，支持多种选择策略。"""

    def __init__(self, db_path: str):
        self.store = AccountStore(db_path)
        self.db_path = self.store.db_path
        self._ensure_priority_column()

    def _ensure_priority_column(self) -> None:
        """确保 accounts 表有 priority 列。"""
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                cols = {r[1] for r in conn.execute("PRAGMA table_info(accounts)")}
                if "priority" not in cols:
                    conn.execute(
                        "ALTER TABLE accounts ADD COLUMN priority INTEGER DEFAULT 0"
                    )
                    conn.commit()
        except Exception:
            pass

    def _build_base_where(
        self,
        exclude_ids: Optional[Set[int]] = None,
    ) -> tuple[str, list]:
        """构建通用 WHERE 子句：可用 + 有额度 + token 未过期 + 排除指定 ID。"""
        now = time.time()
        threshold = now + _TOKEN_EXPIRY_BUFFER

        clauses = [
            "status = 'available'",
            "(total_limit - used_limit) > 0",
            # token_expires_at=0 表示未记录过期时间（放行，由 TokenManager 实时判断）
            "(token_expires_at = 0 OR token_expires_at > ?)",
        ]
        params: list = [threshold]

        if exclude_ids:
            placeholders = ",".join("?" for _ in exclude_ids)
            clauses.append(f"id NOT IN ({placeholders})")
            params.extend(list(exclude_ids))

        where_sql = "WHERE " + " AND ".join(clauses)
        return where_sql, params

    def select_account(
        self,
        exclude_ids: Optional[Set[int]] = None,
        strategy: Optional[str] = None,
    ) -> Optional[Dict]:
        """选择一个可用账号。

        筛选条件（所有策略通用）：
        1. status = 'available'
        2. 有剩余额度 (remaining_limit > 0)
        3. token 未过期（token_expires_at > now+5min 或未记录）
        4. 不在 exclude_ids 中

        Args:
            exclude_ids: 需要排除的账号 ID 集合
            strategy: 覆盖默认策略（可选）

        Returns:
            账号字典（含完整 token），无可用账号返回 None
        """
        global _round_robin_cursor

        strat = strategy or _get_strategy()
        where_sql, params = self._build_base_where(exclude_ids)

        select_cols = """
            SELECT
                id, email, local_id, id_token, refresh_token, api_key,
                total_limit, used_limit, use_count, last_used, token_expires_at
            FROM accounts
        """

        with sqlite3.connect(str(self.db_path)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            if strat == "random":
                # 先查出所有候选，再随机选一个
                cursor.execute(f"{select_cols} {where_sql}", params)
                rows = cursor.fetchall()
                if not rows:
                    logger.warning("[AccountSelector] 无可用账号 (strategy=%s)", strat)
                    return None
                row = random.choice(rows)

            elif strat == "round_robin":
                # 查出所有候选按 ID 排序，用游标取下一个
                order = _order_by_round_robin()
                cursor.execute(f"{select_cols} {where_sql} {order}", params)
                rows = cursor.fetchall()
                if not rows:
                    logger.warning("[AccountSelector] 无可用账号 (strategy=%s)", strat)
                    return None
                # 找到第一个 id > cursor 的，否则回到第一个
                row = None
                for r in rows:
                    if r["id"] > _round_robin_cursor:
                        row = r
                        break
                if row is None:
                    row = rows[0]
                _round_robin_cursor = row["id"]

            elif strat == "most_quota":
                order = _order_by_most_quota()
                cursor.execute(f"{select_cols} {where_sql} {order} LIMIT 1", params)
                row = cursor.fetchone()

            elif strat == "priority":
                order = _order_by_priority()
                cursor.execute(f"{select_cols} {where_sql} {order} LIMIT 1", params)
                row = cursor.fetchone()

            else:
                # least_used (default)
                order = _order_by_least_used()
                cursor.execute(f"{select_cols} {where_sql} {order} LIMIT 1", params)
                row = cursor.fetchone()

            if not row:
                logger.warning("[AccountSelector] 无可用账号 (strategy=%s)", strat)
                return None

            account = dict(row)
            remaining = account["total_limit"] - account["used_limit"]
            expires_at = account.get("token_expires_at", 0)
            expires_info = ""
            if expires_at and expires_at > 0:
                mins_left = int((expires_at - time.time()) / 60)
                expires_info = f" token_expires={mins_left}min"

            logger.info(
                "[AccountSelector] 选中账号: email=%s strategy=%s use_count=%d remaining=%d%s",
                account["email"], strat, account["use_count"], remaining, expires_info,
            )
            return account

    def record_usage(self, account_id: int, tokens_used: int = 0) -> bool:
        """记录账号使用。"""
        with sqlite3.connect(str(self.db_path)) as conn:
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
                    account_id, tokens_used,
                )
            else:
                logger.warning(
                    "[AccountSelector] Failed to record usage: account_id=%d not found",
                    account_id,
                )
            return success

    def get_recently_used_accounts(self, limit: int = 10) -> List[Dict]:
        """获取最近使用的账号列表（脱敏）。"""
        with sqlite3.connect(str(self.db_path)) as conn:
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
            return [dict(row) for row in cursor.fetchall()]

    def get_pool_status(self) -> Dict:
        """获取账号池状态。"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.cursor()

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
                "strategy": _get_strategy(),
            }
