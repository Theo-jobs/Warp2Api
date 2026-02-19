from __future__ import annotations

import base64
import re
from typing import Any, Dict, List, Optional, Tuple

import httpx

from .logging import logger


def _get(d: Dict[str, Any], *names: str) -> Any:
    for n in names:
        if isinstance(d, dict) and n in d:
            return d[n]
    return None


def normalize_content_to_list(content: Any) -> List[Dict[str, Any]]:
    """将 OpenAI 格式的 content 标准化为 segment 列表。

    支持 text 和 image_url 两种类型。
    """
    segments: List[Dict[str, Any]] = []
    try:
        if isinstance(content, str):
            return [{"type": "text", "text": content}]
        if isinstance(content, list):
            for item in content:
                if isinstance(item, dict):
                    t = item.get("type") or ("text" if isinstance(item.get("text"), str) else None)
                    if t == "text" and isinstance(item.get("text"), str):
                        segments.append({"type": "text", "text": item.get("text")})
                    elif t == "image_url":
                        # 保留 image_url 段
                        segments.append(item)
                    else:
                        seg: Dict[str, Any] = {}
                        if t:
                            seg["type"] = t
                        if isinstance(item.get("text"), str):
                            seg["text"] = item.get("text")
                        if seg:
                            segments.append(seg)
            return segments
        if isinstance(content, dict):
            if isinstance(content.get("text"), str):
                return [{"type": "text", "text": content.get("text")}]
    except Exception:
        return []
    return []


def _parse_data_uri(url: str) -> Optional[Tuple[bytes, str]]:
    """解析 data:image/...;base64,... 格式，返回 (raw_bytes, mime_type)。"""
    m = re.match(r"^data:(image/[a-zA-Z0-9.+-]+);base64,(.+)$", url, re.DOTALL)
    if not m:
        return None
    mime_type = m.group(1)
    try:
        raw = base64.b64decode(m.group(2))
        return raw, mime_type
    except Exception:
        return None


def _download_image(url: str) -> Optional[Tuple[bytes, str]]:
    """下载远程图像 URL，返回 (raw_bytes, mime_type)。"""
    try:
        with httpx.Client(timeout=15.0, follow_redirects=True) as client:
            resp = client.get(url)
            if resp.status_code != 200:
                return None
            ct = resp.headers.get("content-type", "image/jpeg").split(";")[0].strip()
            if not ct.startswith("image/"):
                ct = "image/jpeg"
            return resp.content, ct
    except Exception as e:
        logger.warning("[Vision] 图像下载失败: %s — %s", url[:80], e)
        return None


def _mime_to_format(mime_type: str) -> str:
    """从 mime_type 提取短格式名: image/png -> png"""
    idx = mime_type.rfind("/")
    if idx != -1:
        return mime_type[idx + 1:]
    return "png"


def extract_images_from_segments(segments: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """从 content segments 中提取图像，返回 Warp protobuf InputContext.Image 格式。

    每个元素: {"data": base64_str, "mime_type": "image/..."}
    bridge 层的 _populate_protobuf_from_dict 会将 base64 解码为 bytes。
    """
    images: List[Dict[str, Any]] = []
    for seg in segments:
        if not isinstance(seg, dict) or seg.get("type") != "image_url":
            continue
        img_obj = seg.get("image_url") or {}
        url = img_obj.get("url", "")
        if not url:
            continue

        result: Optional[Tuple[bytes, str]] = None
        if url.startswith("data:"):
            result = _parse_data_uri(url)
        elif url.startswith("http://") or url.startswith("https://"):
            result = _download_image(url)

        if result:
            raw_bytes, mime_type = result
            images.append({
                "data": base64.b64encode(raw_bytes).decode("ascii"),
                "mime_type": mime_type,
            })
            logger.info("[Vision] 提取图像: %s, %d bytes", mime_type, len(raw_bytes))
    return images


def segments_to_text(segments: List[Dict[str, Any]]) -> str:
    parts: List[str] = []
    for seg in segments:
        if isinstance(seg, dict) and seg.get("type") == "text" and isinstance(seg.get("text"), str):
            parts.append(seg.get("text") or "")
    return "".join(parts)


def segments_to_warp_results(segments: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    for seg in segments:
        if isinstance(seg, dict) and seg.get("type") == "text" and isinstance(seg.get("text"), str):
            results.append({"text": {"text": seg.get("text")}})
    return results 