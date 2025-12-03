"""Helper utilities to call B 站的 WBI 接口（签名 + 缓存 mixin key）。"""
from __future__ import annotations

import hashlib
import time
from typing import Any, Dict
from urllib.parse import urlencode

from playwright.async_api import APIRequestContext

MIXIN_KEY_ENC_TAB = [
    46,
    47,
    18,
    2,
    53,
    8,
    23,
    32,
    15,
    50,
    10,
    31,
    58,
    3,
    45,
    35,
    27,
    43,
    5,
    49,
    33,
    9,
    42,
    19,
    29,
    28,
    14,
    39,
    12,
    38,
    41,
    13,
    37,
    17,
    48,
    7,
    16,
    24,
    55,
    54,
    40,
    59,
    52,
    1,
    30,
    4,
    22,
    25,
    44,
    6,
    57,
    0,
    34,
    51,
    11,
    26,
    56,
    20,
    36,
    21,
]

_CACHE: Dict[str, Any] = {"key": None, "ts": 0.0}
_CACHE_TTL = 6 * 3600  # WBI key 在前端通常 6 小时左右更新一次


def _extract_key(url: str) -> str:
    filename = url.rsplit("/", 1)[-1]
    return filename.split(".")[0]


def _mixin_key(img_key: str, sub_key: str) -> str:
    raw = (img_key + sub_key).strip()
    if len(raw) < max(MIXIN_KEY_ENC_TAB) + 1:
        raw = raw.ljust(max(MIXIN_KEY_ENC_TAB) + 1, "0")
    return "".join(raw[i] for i in MIXIN_KEY_ENC_TAB)[:32]


async def _ensure_wbi_key(request: APIRequestContext) -> str:
    now = time.time()
    cached = _CACHE.get("key")
    if cached and now - (_CACHE.get("ts") or 0) < _CACHE_TTL:
        return cached  # type: ignore[return-value]
    resp = await request.get("https://api.bilibili.com/x/web-interface/nav")
    data = await resp.json()
    wbi = (data.get("data") or {}).get("wbi_img") or {}
    img_key = _extract_key(wbi.get("img_url") or "")
    sub_key = _extract_key(wbi.get("sub_url") or "")
    key = _mixin_key(img_key, sub_key)
    _CACHE["key"] = key
    _CACHE["ts"] = now
    return key


def _filter_value(value: Any) -> str:
    text = str(value)
    remove_chars = set("!'()*")
    return "".join(ch for ch in text if ch not in remove_chars)


async def sign_wbi_params(request: APIRequestContext, params: Dict[str, Any]) -> Dict[str, Any]:
    """按照 WBI 签名规则返回带 wts/w_rid 的参数副本。"""

    mixin_key = await _ensure_wbi_key(request)
    payload: Dict[str, Any] = {k: _filter_value(v) for k, v in params.items()}
    payload["wts"] = int(time.time())
    ordered = sorted(payload.items(), key=lambda item: item[0])
    query = urlencode(ordered)
    w_rid = hashlib.md5((query + mixin_key).encode("utf-8")).hexdigest()
    payload["w_rid"] = w_rid
    return payload


__all__ = ["sign_wbi_params"]
