#!/usr/bin/env python3
"""
一次性修复脚本：将 status='available' 但 token 已过期的账号标记为 token_expired。

用法:
    python fix_token_expired.py [db_path]

默认 db_path: data/accounts.db（Docker 容器内）或 accounts.db（本地）
"""
import os
import sys
import time
import sqlite3
from datetime import datetime


def fix_token_expired(db_path: str) -> int:
    if not os.path.exists(db_path):
        print(f"[ERROR] 数据库不存在: {db_path}")
        return 0

    now = time.time()
    now_str = datetime.now().isoformat()

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row

        # 查询 available 但 token 已过期的账号
        rows = conn.execute(
            """SELECT id, email, token_expires_at
               FROM accounts
               WHERE status = 'available'
                 AND token_expires_at > 0
                 AND token_expires_at < ?""",
            (now,),
        ).fetchall()

        if not rows:
            print("[OK] 没有需要修复的账号。")
            return 0

        print(f"[INFO] 发现 {len(rows)} 个 available 但 token 已过期的账号：")
        for r in rows:
            expires_at = r["token_expires_at"]
            ago_hours = (now - expires_at) / 3600
            print(f"  - id={r['id']}  email={r['email']}  过期 {ago_hours:.1f} 小时")

        # 批量更新
        ids = [r["id"] for r in rows]
        placeholders = ",".join("?" for _ in ids)
        conn.execute(
            f"""UPDATE accounts
                SET status = 'token_expired', updated_at = ?
                WHERE id IN ({placeholders})""",
            [now_str] + ids,
        )
        conn.commit()

        print(f"[OK] 已将 {len(ids)} 个账号标记为 token_expired。")
        return len(ids)


if __name__ == "__main__":
    # 优先使用命令行参数，其次环境变量，最后默认路径
    if len(sys.argv) > 1:
        path = sys.argv[1]
    else:
        path = os.environ.get("ACCOUNT_DB_PATH", "")
        if not path:
            # 尝试常见路径
            for candidate in ["data/accounts.db", "accounts.db"]:
                if os.path.exists(candidate):
                    path = candidate
                    break
            else:
                path = "data/accounts.db"

    print(f"[INFO] 数据库路径: {path}")
    fix_token_expired(path)
