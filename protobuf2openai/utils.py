"""公共工具函数。"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

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


def serialize_history_to_text(history: list, max_tool_chars: int = 50000) -> Optional[str]:
    """将多轮对话历史序列化为文本，注入 system prompt（无状态模式）。

    跳过 system 消息和尾部当前输入（与 attach_user_and_tools_to_inputs 对齐）。

    Args:
        history: ChatMessage 列表
        max_tool_chars: tool result 最大截断字符数
    """
    from .helpers import normalize_content_to_list, segments_to_text

    non_system = [m for m in history if m.role != "system"]
    if len(non_system) <= 1:
        return None

    if non_system[-1].role == "tool":
        split_idx = len(non_system)
        while split_idx > 0 and non_system[split_idx - 1].role == "tool":
            split_idx -= 1
        history_msgs = non_system[:split_idx]
    else:
        history_msgs = non_system[:-1]

    if not history_msgs:
        return None

    lines: list[str] = []
    for m in history_msgs:
        text = segments_to_text(normalize_content_to_list(m.content))
        if m.role == "user":
            lines.append(f"User: {text}")
        elif m.role == "assistant":
            if text:
                lines.append(f"Assistant: {text}")
            for tc in (m.tool_calls or []):
                fn = tc.get("function") or {}
                tc_name = fn.get("name", "unknown")
                tc_args = fn.get("arguments", "{}")
                lines.append(f"Assistant: [called tool: {tc_name}({tc_args})]")
        elif m.role == "tool":
            max_chars = max(1, max_tool_chars)
            lines.append(
                f"Tool result ({m.tool_call_id or 'unknown'}): {text[:max_chars]}"
            )

    if not lines:
        return None
    return (
        "[Previous conversation]\n"
        + "\n".join(lines)
        + "\n[End of previous conversation]\n\n"
        + "Continue the conversation naturally based on the above context."
    )
