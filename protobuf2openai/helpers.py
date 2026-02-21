from __future__ import annotations

import base64
import ipaddress
import re
import socket
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse

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


def _sanitize_url_for_log(url: str) -> str:
    """日志脱敏：仅保留 scheme+host+path，隐藏 query/fragment。"""
    try:
        p = urlparse(url)
        return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))
    except Exception:
        return url[:80]


def _is_public_http_url(url: str) -> bool:
    """仅允许公网 http/https，阻止本地/内网/链路本地地址（SSRF 防护）。"""
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return False
        host = parsed.hostname
        if not host:
            return False

        # localhost / *.local 一律拒绝
        lower_host = host.lower()
        if lower_host in {"localhost", "localhost.localdomain"} or lower_host.endswith(".local"):
            return False

        try:
            ip = ipaddress.ip_address(host)
            return not (
                ip.is_private
                or ip.is_loopback
                or ip.is_link_local
                or ip.is_multicast
                or ip.is_reserved
                or ip.is_unspecified
            )
        except ValueError:
            # 域名解析后再判定
            infos = socket.getaddrinfo(host, None)
            if not infos:
                return False
            for info in infos:
                addr = info[4][0]
                ip = ipaddress.ip_address(addr)
                if (
                    ip.is_private
                    or ip.is_loopback
                    or ip.is_link_local
                    or ip.is_multicast
                    or ip.is_reserved
                    or ip.is_unspecified
                ):
                    return False
            return True
    except Exception:
        return False


def _download_image(url: str) -> Optional[Tuple[bytes, str]]:
    """下载远程图像 URL，返回 (raw_bytes, mime_type)。"""
    if not _is_public_http_url(url):
        logger.warning("[Vision] 拒绝不安全图像 URL: %s", _sanitize_url_for_log(url))
        return None

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
        logger.warning("[Vision] 图像下载失败: %s — %s", _sanitize_url_for_log(url), e)
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
            # Warp 后端将 protobuf bytes 字段直接 decode 为字符串拼入 data URL，
            # 而非重新 base64 编码。因此我们需要把 base64 字符串本身作为
            # UTF-8 bytes 传入，让 Warp decode("utf-8") 后直接得到合法 base64。
            #
            # 流程：b64_str → encode("utf-8") → 作为 protobuf bytes
            # 但 protobuf_utils 的 TYPE_BYTES 处理会对 str 值做 b64decode，
            # 所以这里需要再包一层 base64（外层供 TYPE_BYTES 解码，内层是实际数据）。
            inner_b64 = base64.b64encode(raw_bytes).decode("ascii")
            outer_b64 = base64.b64encode(inner_b64.encode("utf-8")).decode("ascii")
            images.append({
                "data": outer_b64,
                "mime_type": mime_type,
            })
            logger.info("[Vision] 提取图像: %s, %d bytes (b64→bytes→b64 双层编码)", mime_type, len(raw_bytes))
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