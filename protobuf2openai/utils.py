"""公共工具函数。"""
from __future__ import annotations

import asyncio
import logging

_logger = logging.getLogger(__name__)

# fire-and-forget 任务集合（防止被 GC 回收）+ 并发限制
_background_tasks: set[asyncio.Task] = set()
_task_semaphore = asyncio.Semaphore(10)


def safe_create_task(coro) -> None:
    """安全创建 fire-and-forget 任务：捕获异常 + 限制并发 + 防 GC。"""
    async def _wrapper():
        async with _task_semaphore:
            try:
                await coro
            except Exception as exc:
                _logger.warning("[SafeTask] fire-and-forget 任务异常: %s", exc)

    task = asyncio.create_task(_wrapper())
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
