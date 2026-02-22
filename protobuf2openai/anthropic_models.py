"""
Anthropic Messages API 数据模型
"""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


# ── 请求模型 ──────────────────────────────────────────────


class AnthropicTool(BaseModel):
    name: str
    description: str = ""
    input_schema: dict[str, Any] = Field(default_factory=dict)


class AnthropicTextBlock(BaseModel):
    type: Literal["text"] = "text"
    text: str


class AnthropicToolUseBlock(BaseModel):
    type: Literal["tool_use"] = "tool_use"
    id: str
    name: str
    input: dict[str, Any] = Field(default_factory=dict)


class AnthropicToolResultBlock(BaseModel):
    type: Literal["tool_result"] = "tool_result"
    tool_use_id: str
    content: str | list[dict[str, Any]] = ""
    is_error: bool = False


class AnthropicImageSourceBlock(BaseModel):
    type: Literal["base64"] = "base64"
    media_type: str = "image/png"
    data: str = ""


class AnthropicImageBlock(BaseModel):
    type: Literal["image"] = "image"
    source: AnthropicImageSourceBlock


class AnthropicThinkingBlock(BaseModel):
    type: Literal["thinking"] = "thinking"
    thinking: str
    signature: str = ""


class AnthropicRedactedThinkingBlock(BaseModel):
    type: Literal["redacted_thinking"] = "redacted_thinking"
    data: str = ""


# content block 联合类型
AnthropicContentBlock = (
    AnthropicTextBlock
    | AnthropicToolUseBlock
    | AnthropicToolResultBlock
    | AnthropicImageBlock
    | AnthropicThinkingBlock
    | AnthropicRedactedThinkingBlock
)


class AnthropicMessage(BaseModel):
    role: Literal["user", "assistant"]
    content: str | list[dict[str, Any]]


class AnthropicSystemBlock(BaseModel):
    type: Literal["text"] = "text"
    text: str
    cache_control: dict[str, str] | None = None


class AnthropicMetadata(BaseModel):
    user_id: str | None = None


class AnthropicThinkingConfig(BaseModel):
    type: str = "enabled"
    budget_tokens: int = 10000


class AnthropicMessagesRequest(BaseModel):
    model: str
    max_tokens: int = 8192
    messages: list[AnthropicMessage]
    system: str | list[AnthropicSystemBlock] = ""
    tools: list[AnthropicTool] = Field(default_factory=list)
    stream: bool = True
    metadata: AnthropicMetadata | None = None
    stop_sequences: list[str] = Field(default_factory=list)
    temperature: float | None = None
    top_p: float | None = None
    top_k: int | None = None
    thinking: AnthropicThinkingConfig | None = None


# ── 响应模型 ──────────────────────────────────────────────


class AnthropicUsage(BaseModel):
    input_tokens: int = 0
    output_tokens: int = 0
    cache_creation_input_tokens: int = 0
    cache_read_input_tokens: int = 0


class AnthropicResponseMessage(BaseModel):
    id: str = ""
    type: str = "message"
    role: str = "assistant"
    model: str = ""
    content: list[dict[str, Any]] = Field(default_factory=list)
    stop_reason: str | None = None
    stop_sequence: str | None = None
    usage: AnthropicUsage = Field(default_factory=AnthropicUsage)
