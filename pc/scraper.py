"""Core scraping helpers reused by CLI and web server."""
from __future__ import annotations

import asyncio
import csv
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Awaitable, Callable, Dict, List, Optional, Sequence, Set, Tuple
from json import JSONDecodeError

from playwright.async_api import BrowserContext, Page, Request, Route, async_playwright

DEFAULT_USER_AGENTS = [
    # Realistic desktop Chrome UAs; keep recent to avoid “headless”/old-version detection
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.6422.76 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.207 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.6422.60 Safari/537.36",
]


def choose_user_agent(custom: Optional[str] = None) -> str:
    if custom:
        return custom.strip()
    return DEFAULT_USER_AGENTS[0]
COMMENTS_API = "https://api.bilibili.com/x/v2/reply"
SUB_COMMENTS_API = "https://api.bilibili.com/x/v2/reply/reply"
VIEW_API = "https://api.bilibili.com/x/web-interface/view"
BLOCKED_TYPES = {"image", "media", "font", "stylesheet"}


def extract_bvid(text: str) -> Optional[str]:
    match = re.search(r"(BV[0-9A-Za-z]{10})", text)
    return match.group(1) if match else None


def normalize_video_url(raw: str) -> str:
    raw = raw.strip()
    if raw.startswith("http"):
        return raw
    bvid = extract_bvid(raw)
    if not bvid and raw.upper().startswith("BV"):
        bvid = raw
    if not bvid:
        raise ValueError("无法识别 BV 号或视频链接")
    return f"https://www.bilibili.com/video/{bvid}"


async def block_unwanted(route: Route, request: Request) -> None:
    """Block images/media/fonts to speed up scraping."""

    if request.resource_type in BLOCKED_TYPES:
        await route.abort()
    else:
        await route.continue_()


def to_readable(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp).isoformat(sep=" ", timespec="seconds")


def build_comment(record: Dict, parent_id: Optional[str] = None) -> Dict[str, object]:
    rpid = str(record.get("rpid_str") or record.get("rpid") or "")
    member = record.get("member") or {}
    content = (record.get("content") or {}).get("message") or ""
    return {
        "comment_id": rpid,
        "parent_comment_id": str(parent_id) if parent_id else "",
        "user_id": str(member.get("mid") or ""),
        "user_name": member.get("uname") or "",
        "content": content.replace("\r", "").strip(),
        "publish_time": to_readable(record.get("ctime", 0)),
        "like_count": record.get("like", 0),
        "is_sub_reply": parent_id is not None,
    }


async def resolve_ids(
    page: Page, context: BrowserContext, video_url: str
) -> Tuple[str, str, Optional[str]]:
    """Resolve aid/bvid/title tuple from rendered page."""

    await page.wait_for_function("() => !!window.__INITIAL_STATE__", timeout=60000)
    state = await page.evaluate(
        """() => {
            const s = window.__INITIAL_STATE__ || {};
            const video = s.videoData || s.viewData || {};
            const title = video.title || s.title || s.seoTitle || null;
            return { aid: video.aid || s.aid || null, bvid: video.bvid || s.bvid || null, title };
        }"""
    )
    bvid = state.get("bvid") or extract_bvid(video_url)
    if not bvid:
        raise RuntimeError("未能获取 BV 号")
    aid = state.get("aid")
    if not aid:
        resp = await context.request.get(VIEW_API, params={"bvid": bvid})
        data = await resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"获取 AID 失败: {data}")
        payload = data["data"] or {}
        aid = payload.get("aid")
        if not state.get("title"):
            state["title"] = payload.get("title")
    if not aid:
        raise RuntimeError("未能解析到 AID")
    return str(aid), bvid, state.get("title")


async def _request_with_retry(
    context: BrowserContext,
    url: str,
    *,
    params: Dict[str, object],
    headers: Dict[str, str],
    retries: int = 3,
    backoff: float = 0.8,
) -> Dict:
    """简单重试，确保单页请求尽量成功，牺牲性能换完整性。"""

    last_error: Optional[Dict] = None
    for attempt in range(1, retries + 1):
        resp = await context.request.get(url, params=params, headers=headers)
        if resp.status == 412:
            raise RuntimeError("接口返回 412，可能 UA/Cookie 不匹配或被风控，请检查登录态和 User-Agent")
        try:
            data = await resp.json()
        except JSONDecodeError:
            # API occasionally returns empty body or HTML; retry in those cases
            last_error = {"error": "invalid_json", "status": resp.status}
            await asyncio.sleep(backoff * attempt)
            continue
        if data.get("code") == 0:
            return data
        last_error = data
        await asyncio.sleep(backoff * attempt)
    raise RuntimeError(f"评论接口多次失败: {last_error}")


async def _fetch_sub_replies(
    context: BrowserContext,
    *,
    aid: str,
    root_id: str,
    parent_id: str,
    headers: Dict[str, str],
    seen: Set[str],
    delay: float,
) -> Tuple[List[Dict[str, object]], int]:
    """针对单条主楼，循环拉取子评论直到接口提示结束。"""

    comments: List[Dict[str, object]] = []
    params = {"type": 1, "oid": aid, "ps": 20, "pn": 1, "root": root_id}
    sub_pages = 0
    while True:
        # 这里循环翻子评论直到接口返回 is_end，保证楼中楼拿全
        data = await _request_with_retry(
            context,
            SUB_COMMENTS_API,
            params=params,
            headers=headers,
        )
        payload = data.get("data") or {}
        replies = payload.get("replies") or []
        if not replies:
            break
        sub_pages += 1
        for child in replies:
            child_item = build_comment(child, parent_id=parent_id)
            if child_item["comment_id"] and child_item["comment_id"] not in seen:
                seen.add(child_item["comment_id"])
                comments.append(child_item)
        cursor = payload.get("cursor") or {}
        if cursor.get("is_end"):
            break
        params["pn"] = cursor.get("next") or (params["pn"] + 1)
        await asyncio.sleep(delay)
    return comments, sub_pages


async def fetch_all_comments(
    context: BrowserContext,
    aid: str,
    bvid: str,
    *,
    max_duration: Optional[int] = None,
    page_size: int = 20,
    delay: float = 0.4,
    title: Optional[str] = None,
    existing_comments: Optional[List[Dict[str, object]]] = None,
    existing_seen: Optional[Set[str]] = None,
    user_agent: Optional[str] = None,
    batch_handler: Optional[
        Callable[[List[Dict[str, object]], str, str, Optional[str]], Awaitable[None]]
    ] = None,
) -> Tuple[List[Dict[str, object]], Dict[str, int]]:
    safe_size = max(1, min(page_size, 49))  # 接口限制 ps<=49，避免报 ps out of bounds
    base_params = {"type": 1, "oid": aid, "sort": 0, "ps": safe_size}  # sort=0 按时间
    ua = choose_user_agent(user_agent)
    headers = {"Referer": f"https://www.bilibili.com/video/{bvid}/", "User-Agent": ua}
    seen: Set[str] = set(existing_seen or [])
    comments: List[Dict[str, object]] = list(existing_comments or [])
    started = time.monotonic()
    stats = {"main_pages": 0, "sub_pages": 0}
    reported_total: Optional[int] = None
    current_page = 1

    while True:
        page_batch: List[Dict[str, object]] = []
        # 这里循环翻页直到接口没有更多评论
        if max_duration and time.monotonic() - started >= max_duration:
            print("[WARN] 达到用户设置的时间上限，可能仍有评论未抓取")
            break
        params = dict(base_params)
        params["pn"] = current_page
        data = await _request_with_retry(context, COMMENTS_API, params=params, headers=headers)
        payload = data.get("data") or {}
        cursor = payload.get("cursor") or {}
        reported_total = cursor.get("all_count") or reported_total
        replies = payload.get("replies") or []
        stats["main_pages"] += 1
        if not replies:
            break
        # 这里遍历每个主楼
        for reply in replies:
            item = build_comment(reply)
            if item["comment_id"] and item["comment_id"] not in seen:
                seen.add(item["comment_id"])
                comments.append(item)
                page_batch.append(item)
            # 先收集接口自带的部分子评论
            for child in reply.get("replies") or []:
                child_item = build_comment(child, parent_id=item["comment_id"])
                if child_item["comment_id"] and child_item["comment_id"] not in seen:
                    seen.add(child_item["comment_id"])
                    comments.append(child_item)
                    page_batch.append(child_item)
            # 若接口提示还有更多子回复，则继续翻页直到到底
            loaded_children = len(reply.get("replies") or [])
            if reply.get("rcount", 0) > loaded_children:
                # 这里按主楼逐一获取其子评论，直到楼中楼分页结束
                extra, sub_pages = await _fetch_sub_replies(
                    context,
                    aid=aid,
                    root_id=str(reply.get("rpid")),
                    parent_id=item["comment_id"],
                    headers=headers,
                    seen=seen,
                    delay=delay,
                )
                if sub_pages:
                    stats["sub_pages"] += sub_pages
                comments.extend(extra)
                page_batch.extend(extra)
        if cursor.get("is_end"):
            break
        if batch_handler and page_batch:
            try:
                await batch_handler(page_batch, aid, bvid, title)
            except Exception as exc:  # pragma: no cover - external handler errors
                print(f"[WARN] 批量写入失败: {exc}")
        # 这里根据 cursor 请求下一页主楼
        current_page = cursor.get("next") or (current_page + 1)
        await asyncio.sleep(delay)

    stats["reported_total"] = reported_total
    return comments, stats


def write_output(
    comments: Sequence[Dict[str, object]], output_path: Path, bvid: str, *, as_csv: bool = False
) -> Path:
    """Write comments to JSON or CSV based on `as_csv`. Returns file path."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if as_csv or output_path.suffix.lower() == ".csv":
        fieldnames = [
            "comment_id",
            "parent_comment_id",
            "user_id",
            "user_name",
            "content",
            "publish_time",
            "like_count",
            "is_sub_reply",
        ]
        with output_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in comments:
                writer.writerow({col: row.get(col, "") for col in fieldnames})
    else:
        output_path.write_text(
            json.dumps(
                {"video_bvid": bvid, "total": len(comments), "comments": list(comments)},
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    return output_path


@dataclass
class ScrapeResult:
    comments: List[Dict[str, object]]
    aid: str
    bvid: str
    output_path: Optional[Path]
    title: Optional[str] = None
    main_pages: int = 0
    sub_pages: int = 0
    reported_total: Optional[int] = None


async def scrape(
    video_url: str,
    output_path: Path,
    *,
    max_duration: Optional[int] = None,
    storage_state: Optional[Path] = None,
    batch_handler: Optional[
        Callable[[List[Dict[str, object]], str, str, Optional[str]], Awaitable[None]]
    ] = None,
    metadata_handler: Optional[Callable[[str, str, Optional[str]], Awaitable[None]]] = None,
    existing_comments: Optional[List[Dict[str, object]]] = None,
    existing_seen: Optional[Set[str]] = None,
    user_agent: Optional[str] = None,
    persist_file: bool = True,
) -> ScrapeResult:
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True, args=["--disable-blink-features=AutomationControlled"]
        )
        ua = choose_user_agent(user_agent)
        context_kwargs = {
            "user_agent": ua,
            "viewport": {"width": 1280, "height": 720},
            "java_script_enabled": True,
        }
        if storage_state and storage_state.exists() and storage_state.stat().st_size > 0:
            # 携带登录态可避免“只返回精选评论”
            context_kwargs["storage_state"] = str(storage_state)
        context = await browser.new_context(**context_kwargs)
        await context.route("**/*", block_unwanted)
        page = await context.new_page()
        await page.goto(video_url, wait_until="domcontentloaded")
        await page.wait_for_selector("#commentapp", timeout=60000)

        aid, bvid, title = await resolve_ids(page, context, video_url)
        if metadata_handler:
            try:
                await metadata_handler(aid, bvid, title)
            except Exception as exc:  # pragma: no cover - external handler errors
                print(f"[WARN] metadata handler failed: {exc}")
        print(f"[INFO] 解析成功: aid={aid}, bvid={bvid}")
        comments, stats = await fetch_all_comments(
            context,
            aid,
            bvid,
            max_duration=max_duration,
            title=title,
            existing_comments=existing_comments,
            existing_seen=existing_seen,
            user_agent=user_agent,
            batch_handler=batch_handler,
        )
        await browser.close()

    persisted_path: Optional[Path] = None
    if persist_file:
        persisted_path = write_output(comments, output_path, bvid)
    print(
        "[INTEGRITY] 本次抓取共 "
        f"{len(comments)} 条，主楼翻页 {stats.get('main_pages', 0)} 次，子楼翻页 {stats.get('sub_pages', 0)} 次，"
        f"接口声明总数 {stats.get('reported_total') or '未知'}"
    )
    return ScrapeResult(
        comments=comments,
        aid=aid,
        bvid=bvid,
        output_path=persisted_path,
        title=title,
        main_pages=stats.get("main_pages", 0),
        sub_pages=stats.get("sub_pages", 0),
        reported_total=stats.get("reported_total"),
    )


__all__ = [
    "scrape",
    "normalize_video_url",
    "write_output",
    "ScrapeResult",
    "COMMENTS_API",
    "fetch_all_comments",
]
