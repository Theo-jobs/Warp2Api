#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
本地账号存储与管理

提供 SQLite 账号数据库的 CRUD 操作，支持额度字段与分页查询。
"""
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .logging import logger


class AccountStore:
    """本地账号 SQLite 存储"""

    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self._ensure_db()

    def _ensure_db(self) -> None:
        """确保数据库与表结构存在"""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.cursor()

            # 创建账号表（兼容已有结构）
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS accounts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    email TEXT UNIQUE NOT NULL,
                    local_id TEXT NOT NULL,
                    id_token TEXT NOT NULL,
                    refresh_token TEXT NOT NULL,
                    api_key TEXT DEFAULT '',
                    status TEXT DEFAULT 'available',
                    total_limit INTEGER DEFAULT 0,
                    used_limit INTEGER DEFAULT 0,
                    last_check TIMESTAMP,
                    use_count INTEGER DEFAULT 0,
                    last_used TIMESTAMP,
                    last_refresh_time TIMESTAMP,
                    session_id TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # 补齐可能缺失的列（兼容老库）
            existing_columns = {row[1] for row in cursor.execute("PRAGMA table_info(accounts)")}

            new_columns = {
                "api_key": "TEXT DEFAULT ''",
                "total_limit": "INTEGER DEFAULT 0",
                "used_limit": "INTEGER DEFAULT 0",
                "last_check": "TIMESTAMP",
                "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                "token_expires_at": "REAL DEFAULT 0",
            }

            for col_name, col_def in new_columns.items():
                if col_name not in existing_columns:
                    try:
                        cursor.execute(f"ALTER TABLE accounts ADD COLUMN {col_name} {col_def}")
                        logger.info(f"Added column: {col_name}")
                    except sqlite3.OperationalError:
                        pass

            conn.commit()
            logger.info(f"Account database initialized: {self.db_path}")

    def upsert_account(self, account: Dict) -> int:
        """插入或更新账号（按 email 去重）"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.cursor()

            # 提取字段
            email = account.get("email", "").strip()
            if not email:
                raise ValueError("email is required")

            local_id = account.get("local_id", "")
            id_token = account.get("id_token", "")
            refresh_token = account.get("refresh_token", "")
            api_key = account.get("api_key", "")
            status = account.get("status", "available")
            total_limit = max(0, int(account.get("total_limit", 0)))
            used_limit = max(0, int(account.get("used_limit", 0)))

            now = datetime.now().isoformat()

            # 检查是否已存在
            cursor.execute("SELECT id FROM accounts WHERE email = ?", (email,))
            existing = cursor.fetchone()

            if existing:
                # 更新
                cursor.execute("""
                    UPDATE accounts SET
                        local_id = ?,
                        id_token = ?,
                        refresh_token = ?,
                        api_key = ?,
                        status = ?,
                        total_limit = ?,
                        used_limit = ?,
                        last_check = ?,
                        updated_at = ?
                    WHERE email = ?
                """, (local_id, id_token, refresh_token, api_key, status,
                      total_limit, used_limit, now, now, email))
                account_id = existing[0]
                logger.debug(f"Updated account: {email}")
            else:
                # 插入
                cursor.execute("""
                    INSERT INTO accounts (
                        email, local_id, id_token, refresh_token, api_key,
                        status, total_limit, used_limit, last_check,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (email, local_id, id_token, refresh_token, api_key,
                      status, total_limit, used_limit, now, now, now))
                account_id = cursor.lastrowid
                logger.debug(f"Inserted account: {email}")

            conn.commit()
            return account_id

    def get_accounts(
        self,
        page: int = 1,
        page_size: int = 20,
        keyword: Optional[str] = None,
        status: Optional[str] = None,
    ) -> Tuple[List[Dict], int]:
        """分页查询账号列表（脱敏）"""
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # 构建查询条件
            where_clauses = []
            params = []

            if keyword:
                where_clauses.append("(email LIKE ? OR local_id LIKE ?)")
                params.extend([f"%{keyword}%", f"%{keyword}%"])

            if status:
                where_clauses.append("status = ?")
                params.append(status)

            where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

            # 查询总数
            cursor.execute(f"SELECT COUNT(*) FROM accounts {where_sql}", params)
            total = cursor.fetchone()[0]

            # 分页查询
            offset = (page - 1) * page_size
            cursor.execute(f"""
                SELECT
                    id, email, local_id, status,
                    total_limit, used_limit,
                    (total_limit - used_limit) as remaining_limit,
                    last_check, use_count, last_used,
                    created_at, updated_at,
                    token_expires_at
                FROM accounts
                {where_sql}
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?
            """, params + [page_size, offset])

            accounts = [dict(row) for row in cursor.fetchall()]
            return accounts, total

    def get_account_by_id(self, account_id: int) -> Optional[Dict]:
        """根据 ID 查询单个账号（脱敏）"""
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute("""
                SELECT
                    id, email, local_id, status,
                    total_limit, used_limit,
                    (total_limit - used_limit) as remaining_limit,
                    last_check, use_count, last_used,
                    created_at, updated_at,
                    token_expires_at
                FROM accounts
                WHERE id = ?
            """, (account_id,))

            row = cursor.fetchone()
            return dict(row) if row else None

    def get_summary(self) -> Dict:
        """获取账号汇总统计"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.cursor()

            # 总数与状态计数
            cursor.execute("SELECT COUNT(*) FROM accounts")
            total_count = cursor.fetchone()[0]

            cursor.execute("""
                SELECT status, COUNT(*) as count
                FROM accounts
                GROUP BY status
            """)
            status_counts = {row[0]: row[1] for row in cursor.fetchall()}

            # 额度汇总
            cursor.execute("""
                SELECT
                    COALESCE(SUM(total_limit), 0) as total_limit,
                    COALESCE(SUM(used_limit), 0) as used_limit
                FROM accounts
            """)
            row = cursor.fetchone()
            total_limit = row[0]
            used_limit = row[1]
            remaining_limit = max(0, total_limit - used_limit)

            return {
                "total_count": total_count,
                "status_counts": status_counts,
                "total_limit": total_limit,
                "used_limit": used_limit,
                "remaining_limit": remaining_limit,
            }

    def update_limit(self, account_id: int, total_limit: int, used_limit: int) -> bool:
        """更新账号额度"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.cursor()

            now = datetime.now().isoformat()
            cursor.execute("""
                UPDATE accounts SET
                    total_limit = ?,
                    used_limit = ?,
                    last_check = ?,
                    updated_at = ?
                WHERE id = ?
            """, (max(0, total_limit), max(0, used_limit), now, now, account_id))

            conn.commit()
            return cursor.rowcount > 0

    def update_status(self, account_id: int, status: str) -> bool:
        """更新单个账号状态（仅允许 available / disabled）"""
        if status not in ("available", "disabled"):
            raise ValueError(f"Invalid status: {status!r}, must be 'available' or 'disabled'")

        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.cursor()
            now = datetime.now().isoformat()
            cursor.execute(
                "UPDATE accounts SET status = ?, updated_at = ? WHERE id = ?",
                (status, now, account_id),
            )
            conn.commit()
            return cursor.rowcount > 0

    def batch_update_status(self, account_ids: list[int], status: str) -> int:
        """批量更新账号状态，返回实际更新数量"""
        if status not in ("available", "disabled"):
            raise ValueError(f"Invalid status: {status!r}, must be 'available' or 'disabled'")

        if not account_ids:
            return 0

        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.cursor()
            now = datetime.now().isoformat()
            placeholders = ",".join("?" for _ in account_ids)
            cursor.execute(
                f"UPDATE accounts SET status = ?, updated_at = ? WHERE id IN ({placeholders})",
                [status, now] + list(account_ids),
            )
            conn.commit()
            return cursor.rowcount

    def reset_usage(self, account_ids: Optional[list[int]] = None) -> int:
        """重置账号用量统计（use_count, used_limit, last_used）。

        Args:
            account_ids: 指定账号 ID 列表，为 None 则重置全部。

        Returns:
            实际更新的行数。
        """
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.cursor()
            now = datetime.now().isoformat()
            if account_ids:
                placeholders = ",".join("?" for _ in account_ids)
                cursor.execute(
                    f"UPDATE accounts SET use_count = 0, used_limit = 0, last_used = NULL, updated_at = ? WHERE id IN ({placeholders})",
                    [now] + list(account_ids),
                )
            else:
                cursor.execute(
                    "UPDATE accounts SET use_count = 0, used_limit = 0, last_used = NULL, updated_at = ?",
                    (now,),
                )
            conn.commit()
            return cursor.rowcount
