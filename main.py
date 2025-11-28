"""CLI entrypoint and optional web server launcher for the scraper."""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
from pathlib import Path
from typing import Optional

import uvicorn

from pc.postgres import PostgresSink
from pc.scraper import normalize_video_url, scrape
from pc.raffle import run_raffle
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
    parser.add_argument("--host", default="0.0.0.0", help="Web 服务器监听地址")
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
    parser.add_argument("--pg-dsn",  default="postgresql://postgres:change-me@127.0.0.1:5433/postgres?sslmode=disable", help="Postgres 连接串，提供后会将评论写入数据库")
    parser.add_argument(
        "--pg-table",
        default=None,
        help="Postgres 表名，默认为 comments，可覆盖",
    )
    parser.add_argument(
        "--raffle",
        action="store_true",
        help="对指定输出文件执行评论抽奖而不重新抓取",
    )
    parser.add_argument(
        "--raffle-count",
        type=int,
        default=1,
        help="抽取的中奖评论数量（默认 1）",
    )
    parser.add_argument(
        "--raffle-allow-duplicate",
        action="store_true",
        help="允许同一用户中奖多次（默认按 user_id 去重）",
    )
    parser.add_argument(
        "--raffle-seed",
        help="可选的随机种子，用于复现抽奖结果（未提供则使用系统随机源）",
    )
    return parser.parse_args()


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def apply_env_overrides(args: argparse.Namespace) -> argparse.Namespace:
    env = os.environ
    if not args.url:
        args.url = env.get("APP_URL") or env.get("BILI_URL")
    if args.output == "comments.json":
        args.output = env.get("APP_OUTPUT") or args.output
    if not args.timeout:
        timeout_raw = env.get("APP_TIMEOUT")
        if timeout_raw:
            try:
                args.timeout = int(timeout_raw)
            except ValueError:
                pass
    if not args.storage_state:
        args.storage_state = env.get("APP_STORAGE_STATE")
    if not args.user_agent:
        args.user_agent = env.get("APP_USER_AGENT")
    if not args.pg_dsn:
        args.pg_dsn = env.get("APP_PG_DSN") or env.get("POSTGRES_DSN")
    if not args.pg_table:
        args.pg_table = env.get("APP_PG_TABLE") or env.get("POSTGRES_TABLE")
    args.serve = args.serve or _env_bool("APP_SERVE")
    args.raffle = args.raffle or _env_bool("APP_RAFFLE")
    count_env = env.get("APP_RAFFLE_COUNT")
    if count_env and args.raffle_count == 1:
        try:
            args.raffle_count = int(count_env)
        except ValueError:
            pass
    if _env_bool("APP_RAFFLE_ALLOW_DUP"):
        args.raffle_allow_duplicate = True
    seed_env = env.get("APP_RAFFLE_SEED")
    if seed_env and not args.raffle_seed:
        args.raffle_seed = seed_env
    host_env = env.get("APP_HOST")
    if host_env:
        args.host = host_env
    port_raw = env.get("APP_PORT")
    if port_raw:
        try:
            args.port = int(port_raw)
        except ValueError:
            pass
    data_dir_env = env.get("APP_DATA_DIR")
    if data_dir_env:
        args.data_dir = data_dir_env
    return args


def _load_comments_from_file(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"未找到评论文件: {path}")
    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        return list(data.get("comments") or [])
    if path.suffix.lower() == ".csv":
        with path.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return list(reader)
    raise ValueError("仅支持 JSON 或 CSV 评论文件")


def run_raffle_cli(
    output: Path,
    *,
    count: int,
    unique_user: bool,
    seed: Optional[str],
) -> None:
    comments = _load_comments_from_file(output)
    if not comments:
        print("[RAFFLE] 文件中没有可用的评论数据，无法抽奖。")
        return
    summary = run_raffle(comments, count=count, unique_by_user=unique_user, seed=seed)
    if not summary.winners:
        print("[RAFFLE] 符合条件的评论数量不足，无法抽奖。")
        return
    mode = "系统熵源" if not seed else f"seed={seed}"
    print(
        f"[RAFFLE] 共 {summary.candidate_count} 条候选评论，"
        f"{'按 user_id 去重' if unique_user else '允许重复用户'}，"
        f"随机方式: {mode}"
    )
    for idx, winner in enumerate(summary.winners, start=1):
        snippet = (winner.get("content") or "").replace("\n", " ")
        if len(snippet) > 80:
            snippet = snippet[:77] + "..."
        link = winner.get("origin_url") or ""
        print(
            f"  #{idx} 用户: {winner.get('user_name') or '未知'} "
            f"(UID: {winner.get('user_id') or '-'}, 评论ID: {winner.get('comment_id') or '-'})"
        )
        print(f"      内容: {snippet or '(空)'}")
        if link:
            print(f"      链接: {link}")
    print("[RAFFLE] 抽奖算法实现详见 pc/raffle.py，以上输出可复制到公告。")


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
    args = apply_env_overrides(parse_args())
    if args.raffle:
        unique = not args.raffle_allow_duplicate
        run_raffle_cli(Path(args.output), count=args.raffle_count, unique_user=unique, seed=args.raffle_seed)
        return
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
