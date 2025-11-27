"""CLI entrypoint and optional web server launcher for the scraper."""
from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

import uvicorn

from pc.scraper import normalize_video_url, scrape
from pc.web import create_app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="抓取 B 站视频评论或启动 Web 控制台")
    parser.add_argument("--url", help="视频链接或 BV 号 (CLI 模式必填)")
    parser.add_argument(
        "--output",
        default="comments.json",
        help="输出文件路径，支持 .json/.csv",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=0,
        help="抓取最长秒数，0 表示直到评论抓取完成",
    )
    parser.add_argument(
        "--serve",
        action="store_true",
        help="启动带可视化界面的 Web 服务器",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Web 服务器监听地址")
    parser.add_argument("--port", type=int, default=8000, help="Web 服务器端口")
    parser.add_argument(
        "--data-dir",
        default="data",
        help="任务与导出的持久化目录 (用于 Web 模式)",
    )
    parser.add_argument(
        "--storage-state",
        help="playwright storage_state.json 路径，带登录态可提高评论完整度",
    )
    return parser.parse_args()


async def run_cli(url: str, output: Path, timeout: int, storage_state: Optional[Path]) -> None:
    video_url = normalize_video_url(url)
    duration = timeout if timeout and timeout > 0 else None
    auth_path = (
        storage_state
        if storage_state and storage_state.exists() and storage_state.stat().st_size > 0
        else None
    )
    if storage_state and not storage_state.exists():
        print(f"[WARN] 指定的 storage_state 文件不存在: {storage_state}")
    elif storage_state and storage_state.exists() and storage_state.stat().st_size == 0:
        print(f"[WARN] 指定的 storage_state 文件为空，将按未登录模式抓取: {storage_state}")
    result = await scrape(video_url, output, max_duration=duration, storage_state=auth_path)
    expected = result.reported_total if result.reported_total else "未知"
    print(
        f"[DONE] 共抓取 {len(result.comments)} 条评论 -> {result.output_path} "
        f"(主楼翻页 {result.main_pages} 次 / 子楼翻页 {result.sub_pages} 次 / 官方标称 {expected})"
    )


def run_server(host: str, port: int, data_dir: Path) -> None:
    app = create_app(data_dir=data_dir)
    config = uvicorn.Config(app, host=host, port=port, log_level="info")
    server = uvicorn.Server(config)
    asyncio.run(server.serve())


def main() -> None:
    args = parse_args()
    if args.serve:
        run_server(args.host, args.port, Path(args.data_dir))
    else:
        if not args.url:
            raise SystemExit("CLI 模式需要提供 --url")
        auth_path = Path(args.storage_state).expanduser() if args.storage_state else None
        asyncio.run(run_cli(args.url, Path(args.output), args.timeout, auth_path))


if __name__ == "__main__":
    main()
