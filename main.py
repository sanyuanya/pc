import argparse
import asyncio
import csv
import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

from playwright.async_api import BrowserContext, Page, Request, Route, async_playwright


USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
COMMENTS_API = "https://api.bilibili.com/x/v2/reply"
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
    # 拦截图片/媒体资源以提升抓取速度
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


async def resolve_ids(page: Page, context: BrowserContext, video_url: str) -> Tuple[str, str]:
    # 读取页面初始化状态以提取 aid/bvid
    await page.wait_for_function("() => !!window.__INITIAL_STATE__", timeout=60000)
    state = await page.evaluate(
        """() => {
            const s = window.__INITIAL_STATE__ || {};
            const video = s.videoData || s.viewData || {};
            return { aid: video.aid || s.aid || null, bvid: video.bvid || s.bvid || null };
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
        aid = data["data"].get("aid")
    if not aid:
        raise RuntimeError("未能解析到 AID")
    return str(aid), bvid


async def fetch_all_comments(
    context: BrowserContext,
    aid: str,
    bvid: str,
    *,
    max_duration: int = 300,
    page_size: int = 20,
    delay: float = 0.3,
) -> List[Dict[str, object]]:
    params = {"type": 1, "oid": aid, "sort": 2, "pn": 1, "ps": page_size}
    headers = {"Referer": f"https://www.bilibili.com/video/{bvid}/", "User-Agent": USER_AGENT}
    seen: Set[str] = set()
    comments: List[Dict[str, object]] = []
    started = time.monotonic()

    while time.monotonic() - started < max_duration:
        resp = await context.request.get(COMMENTS_API, params=params, headers=headers)
        data = await resp.json()
        if data.get("code") != 0:
            print(f"[WARN] API 返回错误: {data}")
            break
        payload = data.get("data") or {}
        replies = payload.get("replies") or []
        for reply in replies:
            item = build_comment(reply)
            if item["comment_id"] and item["comment_id"] not in seen:
                seen.add(item["comment_id"])
                comments.append(item)
            for child in reply.get("replies") or []:
                child_item = build_comment(child, parent_id=item["comment_id"])
                if child_item["comment_id"] and child_item["comment_id"] not in seen:
                    seen.add(child_item["comment_id"])
                    comments.append(child_item)
        cursor = payload.get("cursor") or {}
        if cursor.get("is_end") or not replies:
            break
        params["pn"] = cursor.get("next") or (params["pn"] + 1)
        await asyncio.sleep(delay)

    return comments


def write_output(comments: Sequence[Dict[str, object]], output_path: Path, bvid: str) -> None:
    # 根据扩展名写入 JSON 或 CSV
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.suffix.lower() == ".csv":
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


async def scrape(video_url: str, output_path: Path, max_duration: int = 300) -> List[Dict[str, object]]:
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True, args=["--disable-blink-features=AutomationControlled"]
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1280, "height": 720},
            java_script_enabled=True,
        )
        await context.route("**/*", block_unwanted)
        page = await context.new_page()
        await page.goto(video_url, wait_until="domcontentloaded")
        await page.wait_for_selector("#commentapp", timeout=60000)

        aid, bvid = await resolve_ids(page, context, video_url)
        print(f"[INFO] 解析成功: aid={aid}, bvid={bvid}")
        comments = await fetch_all_comments(context, aid, bvid, max_duration=max_duration)
        await browser.close()

    write_output(comments, output_path, bvid)
    return comments


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="抓取 B 站视频的评论")
    parser.add_argument("--url", required=True, help="视频链接或 BV 号")
    parser.add_argument(
        "--output", default="comments.json", help="输出文件路径，支持 .json/.csv"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="抓取最长秒数，默认 300 秒",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    video_url = normalize_video_url(args.url)
    output_path = Path(args.output)
    comments = await scrape(video_url, output_path, max_duration=args.timeout)
    print(f"[DONE] 共抓取 {len(comments)} 条评论 -> {output_path}")


if __name__ == "__main__":
    asyncio.run(main())
