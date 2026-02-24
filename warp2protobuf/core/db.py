"""
SQLite 连接工厂

统一设置 WAL journal mode 和 busy_timeout，解决并发写入冲突。
所有模块应使用 get_connection() 替代 sqlite3.connect()。
"""
import sqlite3


def get_connection(db_path: str) -> sqlite3.Connection:
    """创建 SQLite 连接，启用 WAL 模式和 busy_timeout。

    Args:
        db_path: 数据库文件路径

    Returns:
        配置好的 sqlite3.Connection
    """
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn
