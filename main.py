"""CLI entrypoint and optional web server launcher for the scraper."""
from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path
from typing import Optional

import uvicorn

from pc.postgres import PostgresSink
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
    parser.add_argument("--user-agent", help="自定义 User-Agent，避免 412 风控")
    parser.add_argument("--pg-dsn", help="Postgres 连接串，提供后会将评论写入数据库")
    parser.add_argument(
        "--pg-table",
        default=None,
        help="Postgres 表名，默认为 comments，可覆盖",
    )
    return parser.parse_args()


async def run_cli(
    url: str,
    output: Path,
    timeout: int,
    storage_state: Optional[Path],
    *,
    pg_dsn: Optional[str],
    pg_table: Optional[str],
    user_agent: Optional[str],
) -> None:
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
    dsn = pg_dsn or os.environ.get("APP_PG_DSN") or os.environ.get("POSTGRES_DSN")
    table = (
        pg_table
        or os.environ.get("APP_PG_TABLE")
        or os.environ.get("POSTGRES_TABLE")
        or "comments"
    )
    sink = PostgresSink(dsn, table=table) if dsn else None
    streaming_to_db = bool(sink)
    if sink:
        await sink.start()
    buffer: list[dict] = []
    BATCH_SIZE = 200
    meta = {"aid": None, "bvid": None, "title": None}
    ua = user_agent or os.environ.get("APP_USER_AGENT")
    existing_comments: Optional[list[dict]] = None
    existing_seen: set[str] = set()
    if not sink and output.exists():
        try:
            if output.suffix.lower() == ".json":
                data = json.loads(output.read_text(encoding="utf-8"))
                existing_comments = data.get("comments") or []
            elif output.suffix.lower() == ".csv":
                import csv

                with output.open(encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    existing_comments = list(reader)
            if existing_comments:
                for item in existing_comments:
                    cid = str(item.get("comment_id") or "")
                    if cid:
                        existing_seen.add(cid)
        except Exception:
            pass

    async def flush_buffer(aid: str, bvid: str, title: Optional[str]) -> None:
        nonlocal buffer
        if not sink or not buffer:
            return
        try:
            await sink.save_comments(buffer, bvid=bvid, aid=aid, title=title)
            buffer = []
        except Exception as exc:  # pragma: no cover - external service
            print(f"[WARN] 批量写入 Postgres 失败: {exc}")

    async def batch_handler(chunk, aid, bvid, title):
        if not sink:
            return
        buffer.extend(chunk)
        if len(buffer) >= BATCH_SIZE:
            await flush_buffer(aid, bvid, title)
        if meta["aid"] is None:
            meta["aid"], meta["bvid"], meta["title"] = aid, bvid, title

    async def metadata_handler(aid: str, bvid: str, title: Optional[str]) -> None:
        meta["aid"], meta["bvid"], meta["title"] = aid, bvid, title
    try:
        result = await scrape(
            video_url,
            output,
            max_duration=duration,
            storage_state=auth_path,
            batch_handler=batch_handler if streaming_to_db else None,
            metadata_handler=metadata_handler,
            existing_comments=existing_comments,
            existing_seen=existing_seen,
            user_agent=ua,
            persist_file=not sink,  # 若写入 Postgres 则可跳过本地文件
        )
        if sink and not streaming_to_db:
            try:
                saved = await sink.save_comments(
                    result.comments, bvid=result.bvid, aid=result.aid, title=result.title
                )
                print(f"[DB] 已写入 Postgres {saved} 条评论 -> 表 {sink.table}")
            except Exception as exc:  # pragma: no cover - external service
                print(f"[WARN] 写入 Postgres 失败: {exc}")
        elif sink:
            await flush_buffer(result.aid, result.bvid, result.title)
    except Exception:
        if sink and buffer and meta["aid"] and meta["bvid"]:
            try:
                await flush_buffer(meta["aid"], meta["bvid"], meta["title"])
            except Exception:
                pass
        raise
    finally:
        if sink:
            await sink.close()
    expected = result.reported_total if result.reported_total else "未知"
    where = f"保存到 {result.output_path}" if result.output_path else "未写入本地文件"
    print(
        f"[DONE] 共抓取 {len(result.comments)} 条评论 ({where}) "
        f"(主楼翻页 {result.main_pages} 次 / 子楼翻页 {result.sub_pages} 次 / 官方标称 {expected})"
    )


def run_server(
    host: str,
    port: int,
    data_dir: Path,
    *,
    pg_dsn: Optional[str],
    pg_table: Optional[str],
    user_agent: Optional[str],
) -> None:
    app = create_app(data_dir=data_dir, pg_dsn=pg_dsn, pg_table=pg_table, user_agent=user_agent)
    config = uvicorn.Config(app, host=host, port=port, log_level="info")
    server = uvicorn.Server(config)
    asyncio.run(server.serve())


def main() -> None:
    args = parse_args()
    if args.serve:
        run_server(
            args.host,
            args.port,
            Path(args.data_dir),
            pg_dsn=args.pg_dsn,
            pg_table=args.pg_table,
            user_agent=args.user_agent or os.environ.get("APP_USER_AGENT"),
        )
    else:
        if not args.url:
            raise SystemExit("CLI 模式需要提供 --url")
        auth_path = Path(args.storage_state).expanduser() if args.storage_state else None
        asyncio.run(
            run_cli(
                args.url,
                Path(args.output),
                args.timeout,
                auth_path,
                pg_dsn=args.pg_dsn,
                pg_table=args.pg_table,
                user_agent=args.user_agent or os.environ.get("APP_USER_AGENT"),
            )
        )


if __name__ == "__main__":
    main()
