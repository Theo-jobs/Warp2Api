#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Model configuration and catalog for Warp API

Contains model definitions, configurations, and OpenAI compatibility mappings.
模型 ID 来源：Warp GraphQL API (GetFeatureModelChoices)，经逐一探测确认全部可用。
"""
import time
from typing import Optional


# ── Warp 后端确认可用的全部模型 ID（精确匹配，来自 GraphQL API） ──
WARP_VALID_MODELS = frozenset({
    # ── Auto ──
    "auto",
    "auto-efficient",
    "auto-genius",
    # ── Anthropic Claude ──
    "claude-4-sonnet",
    "claude-4-opus",
    "claude-4.1-opus",
    "claude-4-5-haiku",
    "claude-4-5-opus",
    "claude-4-5-opus-thinking",
    "claude-4-5-sonnet",
    "claude-4-5-sonnet-thinking",
    "claude-4-6-opus-high",
    "claude-4-6-opus-max",
    "claude-4-6-sonnet-high",
    "claude-4-6-sonnet-max",
    # ── Google ──
    "gemini-2.5-pro",
    "gemini-3-pro",
    # ── GLM (智谱) ──
    "glm-47-fireworks",
    # ── OpenAI GPT-5 ──
    "gpt-5",
    "gpt-5-low-reasoning",
    "gpt-5 (high reasoning)",
    # ── OpenAI GPT-4 ──
    "gpt-4.1",
    "gpt-4o",
    # ── OpenAI o-series ──
    "o3",
    "o4-mini",
    # ── OpenAI GPT-5.1 ──
    "gpt-5-1-low-reasoning",
    "gpt-5-1-medium-reasoning",
    "gpt-5-1-high-reasoning",
    "gpt-5-1-codex-low",
    "gpt-5-1-codex-medium",
    "gpt-5-1-codex-high",
    "gpt-5-1-codex-max-low",
    "gpt-5-1-codex-max-medium",
    "gpt-5-1-codex-max-high",
    "gpt-5-1-codex-max-xhigh",
    # ── OpenAI GPT-5.2 ──
    "gpt-5-2-low",
    "gpt-5-2-medium",
    "gpt-5-2-high",
    "gpt-5-2-xhigh",
    "gpt-5-2-codex-low",
    "gpt-5-2-codex-medium",
    "gpt-5-2-codex-high",
    "gpt-5-2-codex-xhigh",
    # ── OpenAI GPT-5.3 Codex ──
    "gpt-5-3-codex-low",
    "gpt-5-3-codex-medium",
    "gpt-5-3-codex-high",
    "gpt-5-3-codex-xhigh",
})


# ── 外部模型名 → Warp 内部 ID 映射表 ──
# 支持 Anthropic 标准名、OpenAI 名、简写、别名等（全部小写匹配）
_MODEL_ALIASES: dict[str, str] = {
    # ═══ Claude 4.6 系列 ═══
    "claude-opus-4-6-20260205": "claude-4-6-opus-high",
    "claude-opus-4-6": "claude-4-6-opus-high",
    "claude-opus-4.6": "claude-4-6-opus-high",
    "claude-4.6-opus": "claude-4-6-opus-high",
    "claude-4.6 opus": "claude-4-6-opus-high",
    "claude 4.6 opus": "claude-4-6-opus-high",
    "claude-4-6-opus": "claude-4-6-opus-high",
    "claude-4-6-opus-high": "claude-4-6-opus-high",
    "claude-4-6-opus-max": "claude-4-6-opus-max",
    "claude-4.6-opus-max": "claude-4-6-opus-max",

    "claude-sonnet-4-6-20260219": "claude-4-6-sonnet-high",
    "claude-sonnet-4-6": "claude-4-6-sonnet-high",
    "claude-sonnet-4.6": "claude-4-6-sonnet-high",
    "claude-4.6-sonnet": "claude-4-6-sonnet-high",
    "claude-4.6 sonnet": "claude-4-6-sonnet-high",
    "claude 4.6 sonnet": "claude-4-6-sonnet-high",
    "claude-4-6-sonnet": "claude-4-6-sonnet-high",
    "claude-4-6-sonnet-high": "claude-4-6-sonnet-high",
    "claude-4-6-sonnet-max": "claude-4-6-sonnet-max",
    "claude-4.6-sonnet-max": "claude-4-6-sonnet-max",

    # thinking 变体
    "claude-opus-4-6-thinking": "claude-4-6-opus-high",
    "claude-sonnet-4-6-thinking": "claude-4-6-sonnet-high",

    # ═══ Claude 4.5 系列 ═══
    "claude-sonnet-4-5-20250929": "claude-4-5-sonnet",
    "claude-sonnet-4-5": "claude-4-5-sonnet",
    "claude-sonnet-4.5": "claude-4-5-sonnet",
    "claude-4.5-sonnet": "claude-4-5-sonnet",
    "claude-4-5-sonnet": "claude-4-5-sonnet",
    "claude-sonnet-4": "claude-4-sonnet",

    "claude-opus-4-5-20251101": "claude-4-5-opus",
    "claude-opus-4-5": "claude-4-5-opus",
    "claude-opus-4.5": "claude-4-5-opus",
    "claude-4.5-opus": "claude-4-5-opus",
    "claude-4-5-opus": "claude-4-5-opus",
    "claude-opus-4": "claude-4-opus",

    "claude-haiku-4-5-20251001": "claude-4-5-haiku",
    "claude-haiku-4-5": "claude-4-5-haiku",
    "claude-haiku-4.5": "claude-4-5-haiku",
    "claude-4.5-haiku": "claude-4-5-haiku",
    "claude-4-5-haiku": "claude-4-5-haiku",

    # thinking 变体
    "claude-sonnet-4-5-20250929-thinking": "claude-4-5-sonnet-thinking",
    "claude-sonnet-4-5-thinking": "claude-4-5-sonnet-thinking",
    "claude-4-5-sonnet-thinking": "claude-4-5-sonnet-thinking",
    "claude-opus-4-5-20251101-thinking": "claude-4-5-opus-thinking",
    "claude-opus-4-5-thinking": "claude-4-5-opus-thinking",
    "claude-4-5-opus-thinking": "claude-4-5-opus-thinking",
    "claude-haiku-4-5-20251001-thinking": "claude-4-5-haiku",

    # ═══ Claude 4.1 / 4 ═══
    "claude-4.1-opus": "claude-4.1-opus",
    "claude-4-sonnet": "claude-4-sonnet",
    "claude-4-opus": "claude-4-opus",

    # ═══ OpenAI GPT-5.3 Codex ═══
    "gpt-5.3-codex": "gpt-5-3-codex-high",
    "gpt-5-3-codex": "gpt-5-3-codex-high",
    "gpt-5.3-codex-low": "gpt-5-3-codex-low",
    "gpt-5.3-codex-medium": "gpt-5-3-codex-medium",
    "gpt-5.3-codex-high": "gpt-5-3-codex-high",
    "gpt-5.3-codex-xhigh": "gpt-5-3-codex-xhigh",
    "gpt-5-3-codex-low": "gpt-5-3-codex-low",
    "gpt-5-3-codex-medium": "gpt-5-3-codex-medium",
    "gpt-5-3-codex-high": "gpt-5-3-codex-high",
    "gpt-5-3-codex-xhigh": "gpt-5-3-codex-xhigh",

    # ═══ OpenAI GPT-5.2 ═══
    "gpt-5.2": "gpt-5-2-high",
    "gpt-5-2": "gpt-5-2-high",
    "gpt-5.2-codex": "gpt-5-2-codex-high",
    "gpt-5-2-codex": "gpt-5-2-codex-high",
    "gpt-5-2-low": "gpt-5-2-low",
    "gpt-5-2-medium": "gpt-5-2-medium",
    "gpt-5-2-high": "gpt-5-2-high",
    "gpt-5-2-xhigh": "gpt-5-2-xhigh",
    "gpt-5-2-codex-low": "gpt-5-2-codex-low",
    "gpt-5-2-codex-medium": "gpt-5-2-codex-medium",
    "gpt-5-2-codex-high": "gpt-5-2-codex-high",
    "gpt-5-2-codex-xhigh": "gpt-5-2-codex-xhigh",

    # ═══ OpenAI GPT-5.1 ═══
    "gpt-5.1": "gpt-5-1-high-reasoning",
    "gpt-5-1": "gpt-5-1-high-reasoning",
    "gpt-5.1-codex": "gpt-5-1-codex-high",
    "gpt-5-1-codex": "gpt-5-1-codex-high",
    "gpt-5.1-codex-max": "gpt-5-1-codex-max-high",
    "gpt-5-1-codex-max": "gpt-5-1-codex-max-high",
    "gpt-5-1-low-reasoning": "gpt-5-1-low-reasoning",
    "gpt-5-1-medium-reasoning": "gpt-5-1-medium-reasoning",
    "gpt-5-1-high-reasoning": "gpt-5-1-high-reasoning",
    "gpt-5-1-codex-low": "gpt-5-1-codex-low",
    "gpt-5-1-codex-medium": "gpt-5-1-codex-medium",
    "gpt-5-1-codex-high": "gpt-5-1-codex-high",
    "gpt-5-1-codex-max-low": "gpt-5-1-codex-max-low",
    "gpt-5-1-codex-max-medium": "gpt-5-1-codex-max-medium",
    "gpt-5-1-codex-max-high": "gpt-5-1-codex-max-high",
    "gpt-5-1-codex-max-xhigh": "gpt-5-1-codex-max-xhigh",

    # ═══ OpenAI GPT-5 ═══
    "gpt-5": "gpt-5",
    "gpt-5-low-reasoning": "gpt-5-low-reasoning",
    "gpt-5 (high reasoning)": "gpt-5 (high reasoning)",
    "gpt-5-reasoning": "gpt-5 (high reasoning)",
    "gpt-5-high": "gpt-5 (high reasoning)",

    # ═══ OpenAI GPT-4 / o-series ═══
    "gpt-4o": "gpt-4o",
    "gpt-4.1": "gpt-4.1",
    "gpt-4-turbo": "gpt-4.1",
    "gpt-4": "gpt-4.1",
    "o3": "o3",
    "o4-mini": "o4-mini",
    "o3-mini": "o4-mini",

    # ═══ Google ═══
    "gemini-2.5-pro": "gemini-2.5-pro",
    "gemini-3-pro": "gemini-3-pro",
    "gemini-pro": "gemini-2.5-pro",
    "gemini-3": "gemini-3-pro",

    # ═══ GLM (智谱) ═══
    "glm-47-fireworks": "glm-47-fireworks",
    "glm-4.7": "glm-47-fireworks",
    "glm-47": "glm-47-fireworks",
    "glm4.7": "glm-47-fireworks",

    # ═══ Auto ═══
    "auto": "auto",
    "auto-efficient": "auto-efficient",
    "auto-genius": "auto-genius",
    "auto (responsive)": "auto",
    "auto (cost-efficient)": "auto-efficient",
    "auto (genius)": "auto-genius",
}


def resolve_model(external_name: Optional[str]) -> str:
    """将外部模型名解析为 Warp 后端接受的模型名。

    优先级：精确匹配 → 别名映射 → 模糊匹配 → 默认 auto-genius
    """
    if not external_name:
        return "auto-genius"

    name = external_name.strip()

    # 1) 精确匹配（Warp 原生 ID）
    if name in WARP_VALID_MODELS:
        return name

    # 2) 别名映射（大小写不敏感）
    lower = name.lower()
    if lower in _MODEL_ALIASES:
        return _MODEL_ALIASES[lower]

    # 3) 模糊匹配：包含关键词
    # Claude 4.6
    if "opus" in lower and ("4.6" in lower or "4-6" in lower):
        if "max" in lower:
            return "claude-4-6-opus-max"
        return "claude-4-6-opus-high"
    if "sonnet" in lower and ("4.6" in lower or "4-6" in lower):
        if "max" in lower:
            return "claude-4-6-sonnet-max"
        return "claude-4-6-sonnet-high"
    # Claude 4.5
    if "opus" in lower and ("4.5" in lower or "4-5" in lower):
        if "thinking" in lower:
            return "claude-4-5-opus-thinking"
        return "claude-4-5-opus"
    if "sonnet" in lower and ("4.5" in lower or "4-5" in lower):
        if "thinking" in lower:
            return "claude-4-5-sonnet-thinking"
        return "claude-4-5-sonnet"
    if "haiku" in lower:
        return "claude-4-5-haiku"
    # Claude 4.1
    if "opus" in lower and "4.1" in lower:
        return "claude-4.1-opus"
    # Claude generic
    if "opus" in lower:
        return "claude-4-6-opus-high"
    if "sonnet" in lower:
        return "claude-4-6-sonnet-high"
    if "claude" in lower:
        return "claude-4-6-sonnet-high"
    # GPT-5.3
    if "5.3" in lower or "5-3" in lower:
        return "gpt-5-3-codex-high"
    # GPT-5.2
    if "5.2" in lower or "5-2" in lower:
        if "codex" in lower:
            return "gpt-5-2-codex-high"
        return "gpt-5-2-high"
    # GPT-5.1
    if "5.1" in lower or "5-1" in lower:
        if "codex" in lower and "max" in lower:
            return "gpt-5-1-codex-max-high"
        if "codex" in lower:
            return "gpt-5-1-codex-high"
        return "gpt-5-1-high-reasoning"
    # Gemini
    if "gemini" in lower and "3" in lower:
        return "gemini-3-pro"
    if "gemini" in lower:
        return "gemini-2.5-pro"
    # GLM
    if "glm" in lower:
        return "glm-47-fireworks"
    # GPT-5 generic
    if "gpt-5" in lower or "gpt5" in lower:
        return "gpt-5"
    if "gpt-4" in lower or "gpt4" in lower:
        return "gpt-4.1"
    if "o3" in lower:
        return "o3"
    if "o4" in lower:
        return "o4-mini"

    # 4) 默认
    return "auto-genius"


def get_all_unique_models() -> list[dict]:
    """返回 OpenAI /v1/models 兼容的模型列表。

    包含全部经探测确认可用的 Warp 模型 + Anthropic 标准别名。
    """
    ts = int(time.time())

    entries = [
        # ── Claude 4.6 系列 ──
        {"id": "claude-4-6-opus-high", "owned_by": "anthropic"},
        {"id": "claude-4-6-opus-max", "owned_by": "anthropic"},
        {"id": "claude-4-6-sonnet-high", "owned_by": "anthropic"},
        {"id": "claude-4-6-sonnet-max", "owned_by": "anthropic"},
        # ── Claude 4.5 系列 ──
        {"id": "claude-4-5-opus", "owned_by": "anthropic"},
        {"id": "claude-4-5-opus-thinking", "owned_by": "anthropic"},
        {"id": "claude-4-5-sonnet", "owned_by": "anthropic"},
        {"id": "claude-4-5-sonnet-thinking", "owned_by": "anthropic"},
        {"id": "claude-4-5-haiku", "owned_by": "anthropic"},
        # ── Claude 4.1 / 4 ──
        {"id": "claude-4.1-opus", "owned_by": "anthropic"},
        {"id": "claude-4-sonnet", "owned_by": "anthropic"},
        {"id": "claude-4-opus", "owned_by": "anthropic"},
        # ── Anthropic 标准别名 ──
        {"id": "claude-opus-4-6-20260205", "owned_by": "anthropic"},
        {"id": "claude-sonnet-4-6-20260219", "owned_by": "anthropic"},
        {"id": "claude-sonnet-4-5-20250929", "owned_by": "anthropic"},
        {"id": "claude-opus-4-5-20251101", "owned_by": "anthropic"},
        {"id": "claude-haiku-4-5-20251001", "owned_by": "anthropic"},
        # ── GPT-5.3 Codex ──
        {"id": "gpt-5-3-codex-xhigh", "owned_by": "openai"},
        {"id": "gpt-5-3-codex-high", "owned_by": "openai"},
        {"id": "gpt-5-3-codex-medium", "owned_by": "openai"},
        {"id": "gpt-5-3-codex-low", "owned_by": "openai"},
        # ── GPT-5.2 ──
        {"id": "gpt-5-2-codex-xhigh", "owned_by": "openai"},
        {"id": "gpt-5-2-codex-high", "owned_by": "openai"},
        {"id": "gpt-5-2-high", "owned_by": "openai"},
        {"id": "gpt-5-2-medium", "owned_by": "openai"},
        # ── GPT-5.1 ──
        {"id": "gpt-5-1-codex-max-high", "owned_by": "openai"},
        {"id": "gpt-5-1-codex-high", "owned_by": "openai"},
        {"id": "gpt-5-1-high-reasoning", "owned_by": "openai"},
        # ── GPT-5 ──
        {"id": "gpt-5", "owned_by": "openai"},
        {"id": "gpt-5 (high reasoning)", "owned_by": "openai"},
        {"id": "gpt-5-low-reasoning", "owned_by": "openai"},
        # ── GPT-4 / o-series ──
        {"id": "gpt-4o", "owned_by": "openai"},
        {"id": "gpt-4.1", "owned_by": "openai"},
        {"id": "o3", "owned_by": "openai"},
        {"id": "o4-mini", "owned_by": "openai"},
        # ── Google ──
        {"id": "gemini-3-pro", "owned_by": "google"},
        {"id": "gemini-2.5-pro", "owned_by": "google"},
        # ── GLM ──
        {"id": "glm-47-fireworks", "owned_by": "zhipu"},
        # ── Auto ──
        {"id": "auto-genius", "owned_by": "warp"},
        {"id": "auto", "owned_by": "warp"},
        {"id": "auto-efficient", "owned_by": "warp"},
    ]

    return [
        {
            "id": e["id"],
            "object": "model",
            "created": ts,
            "owned_by": e["owned_by"],
        }
        for e in entries
    ]
