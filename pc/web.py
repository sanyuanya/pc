"""FastAPI application powering the visual scraping dashboard."""
from __future__ import annotations

import asyncio
import csv
import json
import math
import os
from pathlib import Path
from typing import List, Optional
from urllib.parse import quote, urlencode
from uuid import uuid4

from fastapi import FastAPI, Form, HTTPException, Request, UploadFile, File, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from datetime import datetime

from pc.postgres import DEFAULT_TABLE, PostgresSink
from pc.scraper import normalize_video_url, scrape, extract_bvid, DEFAULT_USER_AGENTS
from pc.storage import TaskRecord, TaskStore, UserRecord
from pc.i18n import get_trans

ROOT_DIR = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = ROOT_DIR / "templates"
STATIC_DIR = ROOT_DIR / "static"
MAX_STATE_FILE_SIZE = 512 * 1024  # 这里限制上传文件大小，避免异常文件


class TaskManager:
    def __init__(
        self,
        store: TaskStore,
        data_dir: Path,
        *,
        max_workers: int = 1,
        pg_sink: Optional[PostgresSink] = None,
        user_agent: Optional[str] = None,
    ) -> None:
        self.store = store
        self.data_dir = data_dir
        self.exports_dir = self.data_dir / "exports"
        self.exports_dir.mkdir(parents=True, exist_ok=True)
        self.queue: asyncio.Queue[Optional[str]] = asyncio.Queue()
        self.workers: List[asyncio.Task[None]] = []
        self.max_workers = max(1, max_workers)
        self.pg_sink = pg_sink
        self.partial_comments: dict[str, List[dict]] = {}
        self.partial_limit = 800
        self.user_agent = user_agent
        self.ua_cycle: List[str] = []
        if self.user_agent:
            self.ua_cycle.append(self.user_agent)

    async def start(self) -> None:
        if self.workers:
            return
        if self.pg_sink:
            await self.pg_sink.start()
        for task in self.store.list_open_tasks():
            if task.status == "running":
                self.store.update_task(task.id, status="pending")
            await self.queue.put(task.id)
        for _ in range(self.max_workers):
            self.workers.append(asyncio.create_task(self._run()))

    async def stop(self) -> None:
        if not self.workers:
            return
        for _ in self.workers:
            await self.queue.put(None)
        for worker in self.workers:
            await worker
        self.workers.clear()
        if self.pg_sink:
            await self.pg_sink.close()

    async def enqueue(self, task_id: str) -> None:
        await self.queue.put(task_id)

    def get_storage_state_path(self, user_id: str) -> Path:
        path = self.data_dir / "users" / user_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def get_state_file(self, user_id: str, label: str) -> Path:
        root = self.get_storage_state_path(user_id)
        return root / f"{label or 'default'}.json"

    async def retry_task(self, task_id: str) -> None:
        """将失败任务重新入队，复用原 task_id。"""

        record = self.store.get_task(task_id)
        if not record:
            raise RuntimeError("任务不存在，无法重试")
        self.store.update_task(
            task_id,
            status="pending",
            error=None,
            output_path=None,
            total_comments=None,
        )
        await self.enqueue(task_id)

    async def sync_task(self, task_id: str) -> None:
        """对已完成任务重新抓取，便于增量补齐。"""

        record = self.store.get_task(task_id)
        if not record:
            raise RuntimeError("任务不存在，无法同步")
        if record.status == "running":
            return
        self.store.update_task(
            task_id,
            status="pending",
            error=None,
            output_path=None if self.pg_sink else record.output_path,
            total_comments=None,
        )
        await self.enqueue(task_id)

    async def _run(self) -> None:
        while True:
            task_id = await self.queue.get()
            if task_id is None:
                self.queue.task_done()
                break
            try:
                await self._process(task_id)
            finally:
                self.queue.task_done()

    async def _process(self, task_id: str) -> None:
        record = self.store.get_task(task_id)
        if not record:
            return
        suffix = ".csv" if record.export_format == "csv" else ".json"
        output_path = self.exports_dir / f"{task_id}{suffix}"
        streaming_to_db = bool(self.pg_sink)
        self.store.update_task(
            task_id,
            status="running",
            error=None,
            output_path=None if self.pg_sink else record.output_path,
        )
        # auth candidates
        auth_candidates: List[Optional[Path]] = []
        for _, path in self.store.list_auth_states(user_id=record.user_id):
            candidate = Path(path)
            if candidate.exists() and candidate.stat().st_size > 0:
                auth_candidates.append(candidate)
        default_state = self.get_state_file(record.user_id, "default")
        if default_state.exists() and default_state.stat().st_size > 0:
            auth_candidates.append(default_state)
        auth_candidates.append(None)

        # ua candidates
        ua_candidates: List[str] = []
        for ua in [self.user_agent] + [ua for _, ua in self.store.list_user_agents(user_id=record.user_id)] + list(DEFAULT_USER_AGENTS):
            if ua and ua not in ua_candidates:
                ua_candidates.append(ua)

        # preload existing
        base_existing_comments: List[dict] = []
        base_seen: Set[str] = set()
        bvid_guess = record.bvid or (extract_bvid(record.normalized_url) if record.normalized_url else None)
        if self.pg_sink and bvid_guess:
            try:
                base_existing_comments = await self.pg_sink.fetch_all(bvid=bvid_guess, order="time")
                base_seen = {str(c.get("comment_id")) for c in base_existing_comments if c.get("comment_id")}
            except Exception:
                base_existing_comments = []
                base_seen = set()
        elif record.output_path:
            try:
                path = Path(record.output_path)
                if path.exists():
                    if path.suffix.lower() == ".json":
                        data = json.loads(path.read_text(encoding="utf-8"))
                        base_existing_comments = data.get("comments") or []
                    else:
                        with path.open(encoding="utf-8") as f:
                            reader = csv.DictReader(f)
                            base_existing_comments = list(reader)
                    base_seen = {str(c.get("comment_id")) for c in base_existing_comments if c.get("comment_id")}
            except Exception:
                base_existing_comments = []
                base_seen = set()

        last_error: Optional[str] = None
        success = False
        meta: dict[str, Optional[str]] = {"aid": record.aid, "bvid": record.bvid or bvid_guess, "title": record.title}
        attempt_cache: List[dict] = []
        # 解析 URL 作为兜底 bvid
        if not meta["bvid"]:
            try:
                meta["bvid"] = extract_bvid(record.normalized_url)
            except Exception:
                pass
        # iterate over UAs and auth states to avoid 412 and resume
        for ua in ua_candidates:
            if success:
                break
            for auth_state in auth_candidates:
                buffer: list[dict] = []
                BATCH_SIZE = 200
                self.partial_comments[task_id] = []

                async def flush_buffer(aid: str, bvid: str, title: Optional[str]) -> None:
                    if not self.pg_sink or not buffer:
                        return
                    try:
                        await self.pg_sink.save_comments(buffer, bvid=bvid, aid=aid, title=title)
                        buffer.clear()
                    except Exception as exc:  # pragma: no cover - external service
                        print(f"[WARN] 批量写入 Postgres 失败: {exc}")

                async def batch_handler(chunk, aid, bvid, title):
                    if not self.pg_sink:
                        return
                    buffer.extend(chunk)
                    if len(buffer) >= BATCH_SIZE:
                        await flush_buffer(aid, bvid, title)
                    cache = self.partial_comments.get(task_id, [])
                    cache.extend(chunk)
                    if len(cache) > self.partial_limit:
                        cache = cache[-self.partial_limit :]
                    self.partial_comments[task_id] = cache

                async def metadata_handler(aid: str, bvid: str, title: Optional[str]) -> None:
                    self.store.update_task(task_id, aid=aid, bvid=bvid, title=title)
                    meta["aid"], meta["bvid"], meta["title"] = aid, bvid, title

                try:
                    result = await scrape(
                        record.normalized_url,
                        output_path,
                        max_duration=duration,
                        storage_state=auth_state,
                        batch_handler=batch_handler if streaming_to_db else None,
                        metadata_handler=metadata_handler,
                        existing_comments=base_existing_comments,
                        existing_seen=base_seen,
                        user_agent=ua,
                        persist_file=not self.pg_sink,  # 有 DB 时跳过本地文件
                    )
                    if self.pg_sink and buffer:
                        await flush_buffer(result.aid, result.bvid, result.title)
                    success = True
                    self.store.update_task(
                        task_id,
                        status="completed",
                        output_path=str(result.output_path) if result.output_path else None,
                        total_comments=len(result.comments),
                        bvid=result.bvid,
                        aid=result.aid,
                        title=result.title,
                    )
                    break
                except Exception as exc:
                    last_error = str(exc)
                    attempt_cache = list(self.partial_comments.get(task_id, []))
                    if self.pg_sink and buffer and meta.get("aid") and meta.get("bvid"):
                        try:
                            await flush_buffer(meta["aid"], meta["bvid"], meta.get("title"))
                        except Exception:
                            pass
                    continue

        if not success:
            partial_count = len(attempt_cache)
            total_counts = partial_count
            if self.pg_sink and meta.get("bvid"):
                try:
                    stats = await self.pg_sink.aggregate_stats(bvid=meta["bvid"])  # type: ignore[arg-type]
                    total_counts = stats.get("total", total_counts)
                except Exception:
                    pass
            self.store.update_task(
                task_id,
                status="failed",
                error=last_error or "scrape failed",
                total_comments=total_counts or None,
                bvid=meta.get("bvid") or record.bvid,
                aid=meta.get("aid") or record.aid,
                title=meta.get("title") or record.title,
            )
        else:
            self.partial_comments.pop(task_id, None)


def parse_links(raw: str) -> List[str]:
    cleaned = [
        chunk.strip()
        for chunk in [item for line in raw.splitlines() for item in line.split(",")]
        if chunk.strip()
    ]
    return cleaned


def load_preview(task: TaskRecord, limit: int = 50) -> List[dict]:
    if not task.output_path:
        return []
    path = Path(task.output_path)
    if not path.exists():
        return []
    try:
        if path.suffix.lower() == ".json":
            data = json.loads(path.read_text(encoding="utf-8"))
            comments = data.get("comments") or []
        else:
            with path.open(encoding="utf-8") as f:
                reader = csv.DictReader(f)
                comments = list(reader)
        normalized: List[dict] = []
        for record in comments:
            row = dict(record)
            flag = row.get("is_sub_reply")
            if isinstance(flag, str):
                row["is_sub_reply"] = flag.lower() in {"1", "true", "yes"}
            like = row.get("like_count")
            try:
                row["like_count"] = int(like)
            except (TypeError, ValueError):
                row["like_count"] = 0
            normalized.append(row)
            if len(normalized) >= limit:
                break
        return normalized
    except Exception:  # pragma: no cover - file corruption edge cases
        return []


def _normalize_comment_row(row: dict) -> Optional[dict]:
    comment_id = str(row.get("comment_id") or "").strip()
    if not comment_id:
        return None
    parent_comment_id = str(row.get("parent_comment_id") or "").strip()
    user_id = str(row.get("user_id") or "").strip()
    user_name = (row.get("user_name") or "").strip()
    content = (row.get("content") or "").strip()
    publish_time = row.get("publish_time") or ""
    like_raw = row.get("like_count")
    try:
        like_count = int(like_raw)
    except (TypeError, ValueError):
        like_count = 0
    is_sub_reply = bool(row.get("is_sub_reply"))
    return {
        "comment_id": comment_id,
        "parent_comment_id": parent_comment_id,
        "user_id": user_id,
        "user_name": user_name,
        "content": content,
        "publish_time": publish_time,
        "like_count": like_count,
        "is_sub_reply": is_sub_reply,
    }


def _load_all_comments(task: TaskRecord) -> List[dict]:
    if not task.output_path:
        return []
    path = Path(task.output_path)
    if not path.exists():
        return []
    comments: List[dict] = []
    try:
        if path.suffix.lower() == ".json":
            data = json.loads(path.read_text(encoding="utf-8"))
            records = data.get("comments") or []
        else:
            with path.open(encoding="utf-8") as f:
                reader = csv.DictReader(f)
                records = list(reader)
        for item in records:
            normalized = _normalize_comment_row(dict(item))
            if normalized:
                comments.append(normalized)
        # 统计子楼数量
        sub_counts = {}
        for c in comments:
            parent = c.get("parent_comment_id")
            if parent:
                sub_counts[parent] = sub_counts.get(parent, 0) + 1
        for c in comments:
            c["sub_count"] = sub_counts.get(c["comment_id"], 0)
        return comments
    except Exception:  # pragma: no cover - file corruption edge cases
        return []


def _parse_time(value: str) -> float:
    try:
        return datetime.fromisoformat(value.replace("Z", "")).timestamp()
    except Exception:
        return 0.0


def _compute_stats(comments: List[dict]) -> dict:
    main_count = sum(1 for c in comments if not c.get("is_sub_reply"))
    sub_count = len(comments) - main_count
    unique_users = len({c.get("user_id") for c in comments if c.get("user_id")})
    like_counts = [c.get("like_count", 0) or 0 for c in comments]
    max_likes = max(like_counts) if like_counts else 0
    avg_likes = sum(like_counts) / len(like_counts) if like_counts else 0
    return {
        "total": len(comments),
        "main_count": main_count,
        "sub_count": sub_count,
        "unique_users": unique_users,
        "max_likes": max_likes,
        "avg_likes": avg_likes,
    }


def _filter_and_paginate(
    comments: List[dict],
    *,
    page: int,
    page_size: int,
    sort: str,
    order: str,
    keyword: Optional[str],
    user: Optional[str],
    kind: str,
) -> tuple[List[dict], int]:
    filtered: List[dict] = []
    kw_lower = (keyword or "").lower()
    user_lower = (user or "").lower()
    for c in comments:
        if kind == "main" and c.get("is_sub_reply"):
            continue
        if kind == "sub" and not c.get("is_sub_reply"):
            continue
        if kw_lower and kw_lower not in c.get("content", "").lower():
            continue
        if user_lower:
            if user_lower not in c.get("user_name", "").lower() and user_lower not in c.get("user_id", "").lower():
                continue
        filtered.append(c)
    sort_key = sort or "time"
    reverse = order != "asc"
    if sort_key == "likes":
        key_fn = lambda x: x.get("like_count", 0)
    elif sort_key == "sub":
        key_fn = lambda x: x.get("sub_count", 0)
    else:
        key_fn = lambda x: _parse_time(x.get("publish_time", ""))
    filtered.sort(key=key_fn, reverse=reverse)
    total = len(filtered)
    start = (page - 1) * page_size
    end = start + page_size
    return filtered[start:end], total


EXPORT_FIELDS = [
    "comment_id",
    "parent_comment_id",
    "user_id",
    "user_name",
    "content",
    "publish_time",
    "like_count",
    "is_sub_reply",
]


async def _attach_context(
    comments: List[dict],
    *,
    using_db: bool,
    sink: Optional[PostgresSink],
    task: TaskRecord,
    all_comments: Optional[List[dict]],
    preview_children: int = 3,
    tags_map: dict[str, list[str]],
) -> None:
    parent_ids = {c["parent_comment_id"] for c in comments if c.get("parent_comment_id")}
    main_ids = {c["comment_id"] for c in comments if not c.get("is_sub_reply")}
    parents: dict[str, dict] = {}
    children_map: dict[str, List[dict]] = {}
    if using_db and sink and task.bvid:
        parents = await sink.fetch_by_ids(bvid=task.bvid, ids=list(parent_ids))
        children_map = await sink.fetch_children(
            bvid=task.bvid, parent_ids=list(main_ids), limit_per_parent=preview_children
        )
    else:
        if all_comments is None:
            all_comments = _load_all_comments(task)
        index = {c["comment_id"]: c for c in all_comments}
        parents = {pid: index[pid] for pid in parent_ids if pid in index}
        for pid in main_ids:
            children_map[pid] = []
        for child in all_comments:
            parent = child.get("parent_comment_id")
            if parent in main_ids:
                children_map[parent].append(child)
        for pid, lst in children_map.items():
            lst.sort(key=lambda x: _parse_time(x.get("publish_time", "")), reverse=True)
            if len(lst) > preview_children:
                children_map[pid] = lst[:preview_children]

    for c in comments:
        c["parent"] = parents.get(c.get("parent_comment_id"))
        c["children"] = children_map.get(c["comment_id"], []) if not c.get("is_sub_reply") else []
        c["tags"] = tags_map.get(c["comment_id"], [])


def create_app(
    *,
    data_dir: Path | str = Path("data"),
    max_workers: Optional[int] = None,
    pg_dsn: Optional[str] = None,
    pg_table: Optional[str] = None,
    user_agent: Optional[str] = None,
) -> FastAPI:
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    env_workers = os.environ.get("APP_MAX_WORKERS")
    if max_workers is not None:
        worker_count = max(1, max_workers)
    elif env_workers:
        try:
            worker_count = max(1, int(env_workers))
        except ValueError:
            worker_count = 1
    else:
        worker_count = 2  # 默认同时跑 2 个任务，兼顾多用户
    final_pg_dsn = pg_dsn or os.environ.get("APP_PG_DSN") or os.environ.get("POSTGRES_DSN")
    final_pg_table = (
        pg_table or os.environ.get("APP_PG_TABLE") or os.environ.get("POSTGRES_TABLE") or DEFAULT_TABLE
    )
    sink = PostgresSink(final_pg_dsn, table=final_pg_table) if final_pg_dsn else None
    ua = user_agent or os.environ.get("APP_USER_AGENT")
    store = TaskStore(data_dir / "tasks.db")
    manager = TaskManager(
        store=store, data_dir=data_dir, max_workers=worker_count, pg_sink=sink, user_agent=ua
    )
    app = FastAPI(title="B 站评论抓取控制台", version="0.1.0")
    app.add_middleware(SessionMiddleware, secret_key=os.environ.get("APP_SESSION_SECRET", "change-me"))
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    def get_current_user(request: Request) -> Optional[UserRecord]:
        user_id = request.session.get("user_id")
        return store.get_user_by_id(user_id) if user_id else None

    def login_redirect(request: Request) -> RedirectResponse:
        next_path = request.url.path
        if request.url.query:
            next_path += f"?{request.url.query}"
        next_safe = quote(next_path, safe="/?=&")
        return RedirectResponse(url=f"/login?next={next_safe}", status_code=303)

    @app.on_event("startup")
    async def _startup() -> None:
        await manager.start()

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        await manager.stop()

    @app.get("/login", response_class=HTMLResponse)
    async def login_form(request: Request, next: Optional[str] = "/") -> HTMLResponse:
        if get_current_user(request):
            return RedirectResponse(url=next or "/", status_code=303)
        return render_template(
            "login.html",
            {
                "request": request,
                "next": next or "/",
                "error": request.query_params.get("error"),
            },
        )

    @app.post("/login")
    async def perform_login(
        request: Request,
        username: str = Form(...),
        password: str = Form(...),
        next: str = Form("/"),
    ) -> Response:
        lang = request.cookies.get("lang", "zh")
        def _(key: str, **kwargs) -> str: return get_trans(lang, key, **kwargs)

        username = username.strip()
        if not username or not password:
            return render_template(
                "login.html",
                {"request": request, "next": next, "error": _("login_error_required")},
            )
        existing = store.get_user_by_username(username)
        first_user = not store.has_any_user()
        if existing:
            user = store.verify_user(username, password)
            if not user:
                return render_template(
                    "login.html",
                    {"request": request, "next": next, "error": _("login_error_invalid")},
                )
        else:
            user = store.create_user(uuid4().hex, username, password)
            if first_user:
                store.adopt_default_tasks(user.id)
        request.session["user_id"] = user.id
        target = next if next.startswith("/") else "/"
        return RedirectResponse(url=target, status_code=303)

    @app.post("/logout")
    async def logout(request: Request) -> RedirectResponse:
        request.session.clear()
        return RedirectResponse(url="/login", status_code=303)

    @app.get("/set-lang/{locale}")
    async def set_lang(locale: str, request: Request) -> RedirectResponse:
        redirect_url = request.headers.get("referer", "/")
        response = RedirectResponse(url=redirect_url, status_code=303)
        # Set cookie for 1 year
        response.set_cookie(key="lang", value=locale, max_age=31536000)
        return response

    def render_template(name: str, context: dict) -> HTMLResponse:
        request = context["request"]
        lang = request.cookies.get("lang", "zh")
        
        def _(key: str, **kwargs) -> str:
            return get_trans(lang, key, **kwargs)
        
        context.setdefault("current_user", get_current_user(request))
        context.update({"lang": lang, "_": _})
        return templates.TemplateResponse(name, context)

    def render_dashboard(
        request: Request,
        user: UserRecord,
        *,
        page: int,
        query: Optional[str],
        created: Optional[int] = None,
        failed: Optional[int] = None,
        form_errors: Optional[List[str]] = None,
        synced: Optional[str] = None,
        ua_list: Optional[List[tuple[str, str]]] = None,
    ) -> HTMLResponse:
        page = max(1, page)
        page_size = 8
        tasks, total = store.list_tasks(
            search=query, page=page, page_size=page_size, user_id=user.id
        )
        total_pages = max(1, math.ceil(total / page_size))
        state_root = manager.get_storage_state_path(user.id)
        auth_states = store.list_auth_states(user_id=user.id)
        has_auth = any(Path(p).exists() and Path(p).stat().st_size > 0 for _, p in auth_states)
        flash_retry = request.query_params.get("retried")
        return render_template(
            "index.html",
            {
                "request": request,
                "tasks": tasks,
                "page": page,
                "total_pages": total_pages,
                "query": query or "",
                "created": created,
                "failed": failed,
                "form_errors": form_errors or [],
                "has_auth": has_auth,
                "storage_state_path": str(state_root),
                "retried_task": flash_retry,
                "synced_task": synced,
                "ua_list": ua_list or [],
                "user": user,
                "current_ua": manager.user_agent,
            },
        )

    @app.get("/settings/auth", response_class=HTMLResponse)
    async def auth_settings(
        request: Request,
        uploaded: Optional[str] = None,
        cleared: Optional[str] = None,
        removed: Optional[str] = None,
        error: Optional[str] = None,
        limit: Optional[str] = None,
    ) -> HTMLResponse:
        user = get_current_user(request)
        if not user:
            return login_redirect(request)
        state_root = manager.get_storage_state_path(user.id)
        states = store.list_auth_states(user_id=user.id)
        has_auth = any(Path(p).exists() and Path(p).stat().st_size > 0 for _, p in states)
        lang = request.cookies.get("lang", "zh")
        def _(key: str, **kwargs) -> str: return get_trans(lang, key, **kwargs)
        error_message: Optional[str] = None
        if error:
            fmt_kwargs = {}
            if error == "settings_auth_error_size":
                fmt_kwargs["size"] = limit or str(MAX_STATE_FILE_SIZE // 1024)
            error_message = _(error, **fmt_kwargs)
        ua_list = store.list_user_agents(user_id=user.id)
        return render_template(
            "settings_auth.html",
            {
                "request": request,
                "has_auth": has_auth,
                "storage_state_path": str(state_root / "state.json"),
                "auth_states": states,
                "uploaded": uploaded,
                "cleared": cleared,
                "removed": removed,
                "error_message": error_message,
                "state_limit_kb": MAX_STATE_FILE_SIZE // 1024,
                "ua_list": ua_list,
                "user": user,
                "default_uas": DEFAULT_USER_AGENTS,
            },
        )

    @app.post("/settings/auth/upload")
    async def upload_auth_state(
        request: Request, state_file: List[UploadFile] = File(...)
    ) -> RedirectResponse:
        user = get_current_user(request)
        if not user:
            return login_redirect(request)
        files = state_file if isinstance(state_file, list) else [state_file]
        redirect_url = "/settings/auth"

        def _error(code: str, *, extra: Optional[str] = None) -> RedirectResponse:
            suffix = f"?error={code}"
            if extra:
                suffix += f"&limit={extra}"
            return RedirectResponse(url=f"{redirect_url}{suffix}", status_code=303)

        for upload in files:
            contents = await upload.read()
            filename = (upload.filename or "").lower()
            label = Path(filename).stem if filename else "state"
            if not filename.endswith(".json"):
                return _error("settings_auth_error_ext")
            if not contents:
                return _error("settings_auth_error_empty")
            if len(contents) > MAX_STATE_FILE_SIZE:
                return _error("settings_auth_error_size", extra=str(MAX_STATE_FILE_SIZE // 1024))
            try:
                text = contents.decode("utf-8")
            except UnicodeDecodeError:
                return _error("settings_auth_error_utf8")
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                return _error("settings_auth_error_json")
            if not isinstance(payload, dict) or (not payload.get("cookies") and not payload.get("origins")):
                return _error("settings_auth_error_schema")

            target_path = manager.get_state_file(user.id, label)
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_text(text, encoding="utf-8")
            store.save_auth_state(user_id=user.id, label=label, path=str(target_path))
        return RedirectResponse(url="/settings/auth?uploaded=1", status_code=303)

    @app.post("/settings/ua")
    async def save_ua(request: Request, label: str = Form(...), ua: str = Form(...)) -> RedirectResponse:
        user = get_current_user(request)
        if not user:
            return login_redirect(request)
        store.save_user_agent(user_id=user.id, label=label or "default", ua=ua)
        return RedirectResponse(url="/settings/auth?uploaded=0", status_code=303)

    @app.post("/settings/ua/delete")
    async def delete_ua(request: Request, label: str = Form(...)) -> RedirectResponse:
        user = get_current_user(request)
        if not user:
            return login_redirect(request)
        store.delete_user_agent(user_id=user.id, label=label)
        return RedirectResponse(url="/settings/auth?uploaded=0", status_code=303)

    @app.post("/settings/auth/delete")
    async def delete_auth(request: Request, label: str = Form(...)) -> RedirectResponse:
        user = get_current_user(request)
        if not user:
            return login_redirect(request)
        entries = store.list_auth_states(user_id=user.id)
        for lbl, path in entries:
            if lbl == label:
                try:
                    Path(path).unlink(missing_ok=True)
                except Exception:
                    pass
                store.delete_auth_state(user_id=user.id, label=label)
                break
        return RedirectResponse(url="/settings/auth?removed=1", status_code=303)

    @app.post("/settings/auth/clear")
    async def clear_auth_state(request: Request) -> RedirectResponse:
        user = get_current_user(request)
        if not user:
            return login_redirect(request)
        for _, path in store.list_auth_states(user_id=user.id):
            try:
                Path(path).unlink(missing_ok=True)
            except Exception:
                pass
        store.delete_all_auth_states(user_id=user.id)
        return RedirectResponse(url="/settings/auth?cleared=1", status_code=303)

    @app.get("/", response_class=HTMLResponse)
    async def dashboard(
        request: Request,
        page: int = 1,
        q: Optional[str] = None,
        created: Optional[int] = None,
        failed: Optional[int] = None,
        retried: Optional[str] = None,
        synced: Optional[str] = None,
    ) -> HTMLResponse:
        user = get_current_user(request)
        if not user:
            return login_redirect(request)
        ua_list = store.list_user_agents(user_id=user.id)
        return render_dashboard(
            request,
            user,
            page=page,
            query=q,
            created=created,
            failed=failed,
            synced=synced,
            ua_list=ua_list,
        )

    @app.post("/tasks")
    async def create_tasks(
        request: Request,
        links: str = Form(..., description="一行一个链接"),
        export_format: str = Form("json"),
        timeout: str = Form(""),
    ) -> HTMLResponse:
        user = get_current_user(request)
        if not user:
            return login_redirect(request)
        lang = request.cookies.get("lang", "zh")
        def _(key: str, **kwargs) -> str: return get_trans(lang, key, **kwargs)

        entries = parse_links(links)
        errors: List[str] = []
        created_ids = 0
        export_format = "json"  # 统一存储，下载时可切换格式
        raw_timeout = (timeout or "").strip()
        if raw_timeout:
            try:
                timeout_value = int(raw_timeout)
            except ValueError:
                errors.append(_("err_timeout_digit"))
                timeout_value = 0
        else:
            timeout_value = 0
        if not entries:
            errors.append(_("err_min_one_link"))
        if timeout_value < 0:
            errors.append(_("err_timeout_negative"))
        if timeout_value and timeout_value < 60:
            errors.append(_("err_timeout_min_60"))
        timeout_value = min(timeout_value, 3600)
        if not errors:
            for entry in entries:
                try:
                    normalized = normalize_video_url(entry)
                    task_id = uuid4().hex
                    bvid_guess = extract_bvid(normalized)
                    store.add_task(
                        task_id=task_id,
                        raw_url=entry,
                        normalized_url=normalized,
                        export_format=export_format,
                        timeout=timeout_value,
                        user_id=user.id,
                        bvid=bvid_guess,
                    )
                    await manager.enqueue(task_id)
                    created_ids += 1
                except Exception as exc:
                    errors.append(f"{entry}: {exc}")
        if errors:
            return render_dashboard(
                request,
                user,
                page=1,
                query=None,
                created=created_ids,
                failed=len(errors),
                form_errors=errors,
            )
        url = request.url_for("dashboard")
        if created_ids:
            url = f"{url}?created={created_ids}"
        return RedirectResponse(url=url, status_code=303)

    @app.post("/tasks/{task_id}/retry")
    async def retry_task(task_id: str, request: Request) -> RedirectResponse:
        user = get_current_user(request)
        if not user:
            return login_redirect(request)
        lang = request.cookies.get("lang", "zh")
        def _(key: str, **kwargs) -> str: return get_trans(lang, key, **kwargs)

        task = store.get_task(task_id)
        if not task or task.user_id != user.id:
            raise HTTPException(status_code=404, detail=_("err_task_not_found"))
        if task.status != "failed":
            raise HTTPException(status_code=400, detail=_("err_retry_failed_only"))
        await manager.retry_task(task_id)
        url = request.url_for("dashboard")
        url = f"{url}?retried={task_id}"
        return RedirectResponse(url=url, status_code=303)

    @app.post("/tasks/{task_id}/delete")
    async def delete_task(
        task_id: str,
        request: Request,
        page: Optional[int] = None,
        q: Optional[str] = None,
    ) -> RedirectResponse:
        user = get_current_user(request)
        if not user:
            return login_redirect(request)
        lang = request.cookies.get("lang", "zh")
        def _(key: str, **kwargs) -> str: return get_trans(lang, key, **kwargs)
        task = store.get_task(task_id)
        if not task or task.user_id != user.id:
            raise HTTPException(status_code=404, detail=_("err_task_not_found"))
        removed = store.delete_task(task_id)
        if removed and removed.output_path:
            try:
                Path(removed.output_path).unlink(missing_ok=True)
            except Exception:
                pass
        target = request.url_for("dashboard")
        params = {}
        if page and page > 1:
            params["page"] = page
        if q:
            params["q"] = q
        if params:
            target = f"{target}?{urlencode(params)}"
        return RedirectResponse(url=target, status_code=303)

    @app.post("/tasks/{task_id}/sync")
    async def sync_task(
        task_id: str,
        request: Request,
        page: Optional[int] = None,
        q: Optional[str] = None,
    ) -> RedirectResponse:
        user = get_current_user(request)
        if not user:
            return login_redirect(request)
        lang = request.cookies.get("lang", "zh")
        def _(key: str, **kwargs) -> str: return get_trans(lang, key, **kwargs)
        task = store.get_task(task_id)
        if not task or task.user_id != user.id:
            raise HTTPException(status_code=404, detail=_("err_task_not_found"))
        if task.status == "running":
            raise HTTPException(status_code=400, detail=_("err_task_running"))
        await manager.sync_task(task_id)
        target = request.url_for("dashboard")
        params = {"synced": task_id}
        if page and page > 1:
            params["page"] = page
        if q:
            params["q"] = q
        target = f"{target}?{urlencode(params)}"
        return RedirectResponse(url=target, status_code=303)

    @app.get("/tasks/{task_id}", response_class=HTMLResponse)
    async def task_detail(
        request: Request,
        task_id: str,
        page: int = 1,
        size: int = 20,
        sort: str = "time",
        order: str = "desc",
        q: Optional[str] = None,
        user_filter: Optional[str] = None,
        kind: str = "all",
        tag: Optional[str] = None,
    ) -> HTMLResponse:
        user = get_current_user(request)
        if not user:
            return login_redirect(request)
        lang = request.cookies.get("lang", "zh")
        def _(key: str, **kwargs) -> str: return get_trans(lang, key, **kwargs)

        task = store.get_task(task_id)
        if not task or task.user_id != user.id:
            raise HTTPException(status_code=404, detail=_("err_task_not_found"))

        page = max(1, page)
        page_size = min(max(5, size), 100)
        sort_key = sort if sort in {"time", "likes", "sub"} else "time"
        order_dir = "asc" if order == "asc" else "desc"
        kind_final = kind if kind in {"all", "main", "sub"} else "all"
        tag_filter = (tag or "").strip()

        using_db = bool(manager.pg_sink and task.bvid)
        stats = {"total": 0, "main_count": 0, "sub_count": 0, "unique_users": 0, "max_likes": 0, "avg_likes": 0}
        filtered_total = 0
        comments: List[dict] = []
        all_comments: Optional[List[dict]] = None
        tags_map: dict[str, list[str]] = {}

        if using_db:
            bvid_for_query = task.bvid or extract_bvid(task.normalized_url) if task.normalized_url else None
            if bvid_for_query:
                try:
                    allowed_ids = None
                    if tag_filter:
                        allowed_ids = store.get_comment_ids_by_tag(user_id=user.id, tag=tag_filter)
                        if not allowed_ids:
                            comments, filtered_total = [], 0
                        else:
                            comments, filtered_total = await manager.pg_sink.query_comments(
                                bvid=bvid_for_query,
                                page=page,
                                page_size=page_size,
                                sort=sort_key,
                                order=order_dir,
                                keyword=q or None,
                                user=user_filter or None,
                                kind=kind_final,
                                allowed_ids=allowed_ids,
                            )
                    else:
                        comments, filtered_total = await manager.pg_sink.query_comments(
                            bvid=bvid_for_query,
                            page=page,
                            page_size=page_size,
                            sort=sort_key,
                            order=order_dir,
                            keyword=q or None,
                            user=user_filter or None,
                            kind=kind_final,
                            allowed_ids=allowed_ids,
                        )
                    stats = await manager.pg_sink.aggregate_stats(bvid=bvid_for_query)
                except Exception as exc:  # pragma: no cover - external service
                    print(f"[WARN] 查询 Postgres 评论失败: {exc}")
                    using_db = False
                if using_db and not comments:
                    try:
                        fallback = await manager.pg_sink.fetch_all(bvid=bvid_for_query, order="time")
                        if fallback:
                            tags_map = store.get_comment_tags(
                                comment_ids=[c.get("comment_id") for c in fallback], user_id=user.id
                            )
                            comments, filtered_total = _filter_and_paginate(
                                fallback,
                                page=page,
                                page_size=page_size,
                                sort=sort_key,
                                order=order_dir,
                                keyword=q,
                                user=user_filter,
                                kind=kind_final,
                            )
                            for c in comments:
                                c["tags"] = tags_map.get(c.get("comment_id"), [])
                            stats = stats or {"total": len(fallback)}
                    except Exception as exc:
                        print(f"[WARN] 回退查询 Postgres 失败: {exc}")
            else:
                using_db = False
        if not using_db:
            all_comments = _load_all_comments(task)
            if not all_comments and task.id in manager.partial_comments:
                all_comments = list(manager.partial_comments.get(task.id, []))
            tags_map = store.get_comment_tags(comment_ids=[c.get("comment_id") for c in all_comments], user_id=user.id)
            stats = _compute_stats(all_comments)
            comments, filtered_total = _filter_and_paginate(
                all_comments,
                page=page,
                page_size=page_size,
                sort=sort_key,
                order=order_dir,
                keyword=q,
                user=user_filter,
                kind=kind_final,
            )
            if tag_filter:
                tag_lower = tag_filter.lower()
                comments = [c for c in comments if any(t.lower() == tag_lower for t in tags_map.get(c["comment_id"], []))]
                filtered_total = len(comments)
        if using_db:
            # 如果 DB 查询仍无结果，尝试使用内存缓存
            if not comments and task.id in manager.partial_comments:
                comments = list(manager.partial_comments.get(task.id, []))
                filtered_total = len(comments)
                stats = stats or _compute_stats(comments)
            tags_map = store.get_comment_tags(comment_ids=[c.get("comment_id") for c in comments], user_id=user.id)
        await _attach_context(
            comments,
            using_db=using_db,
            sink=manager.pg_sink if using_db else None,
            task=task,
            all_comments=all_comments,
            tags_map=tags_map,
        )

        total_pages = max(1, math.ceil(filtered_total / page_size)) if filtered_total else 1
        return render_template(
            "detail.html",
            {
                "request": request,
                "task": task,
                "comments": comments,
                "comment_stats": stats,
                "filtered_total": filtered_total,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "sort": sort_key,
                "order": order_dir,
                "q": q or "",
                "user_filter": user_filter or "",
                "kind": kind_final,
                "tag": tag_filter,
                "using_db": using_db,
                "user": user,
                "tags_map": tags_map,
            },
        )

    @app.get("/tasks/{task_id}/download")
    async def download(task_id: str, request: Request) -> Response:
        user = get_current_user(request)
        if not user:
            raise HTTPException(status_code=403, detail="请先登录")
        lang = request.cookies.get("lang", "zh")
        def _(key: str, **kwargs) -> str: return get_trans(lang, key, **kwargs)

        task = store.get_task(task_id)
        if not task or task.user_id != user.id:
            raise HTTPException(status_code=404, detail=_("err_no_download"))
        fmt = (request.query_params.get("format") or task.export_format or "json").lower()
        if fmt not in {"json", "csv"}:
            raise HTTPException(status_code=400, detail=_("err_format_support"))

        comments: List[dict]
        if manager.pg_sink and task.bvid:
            try:
                comments = await manager.pg_sink.fetch_all(bvid=task.bvid, order="time")
            except Exception:
                raise HTTPException(status_code=500, detail=_("err_no_download"))
        else:
            if not task.output_path:
                raise HTTPException(status_code=404, detail=_("err_file_missing"))
            comments = _load_all_comments(task)
            if not comments:
                raise HTTPException(status_code=404, detail=_("err_file_missing"))
        tag_lookup = store.get_comment_tags(comment_ids=[c.get("comment_id") for c in comments], user_id=user.id)
        for c in comments:
            c["tags"] = tag_lookup.get(c.get("comment_id"), [])

        if fmt == "csv":
            import io

            buffer = io.StringIO()
            writer = csv.DictWriter(buffer, fieldnames=EXPORT_FIELDS)
            writer.writeheader()
            for row in comments:
                writer.writerow({col: row.get(col, "") for col in EXPORT_FIELDS})
            content = buffer.getvalue()
            media_type = "text/csv"
            filename = f"{task.id}.csv"
        else:
            payload = {"video_bvid": task.bvid, "total": len(comments), "comments": comments}
            content = json.dumps(payload, ensure_ascii=False, indent=2)
            media_type = "application/json"
            filename = f"{task.id}.json"
        return Response(
            content=content,
            media_type=media_type,
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    @app.post("/comments/{comment_id}/tags")
    async def update_tags(
        comment_id: str,
        request: Request,
        tags: str = Form(""),
        task_id: str = Form(...),
        page: Optional[int] = None,
        q: Optional[str] = None,
        user_filter: Optional[str] = None,
        kind: Optional[str] = None,
        tag: Optional[str] = None,
        sort: Optional[str] = None,
        order: Optional[str] = None,
    ) -> RedirectResponse:
        user = get_current_user(request)
        if not user:
            return login_redirect(request)
        lang = request.cookies.get("lang", "zh")
        def _(key: str, **kwargs) -> str: return get_trans(lang, key, **kwargs)
        task = store.get_task(task_id)
        if not task or task.user_id != user.id:
            raise HTTPException(status_code=404, detail=_("err_task_not_found"))

        parsed = [t.strip().lstrip("#") for t in tags.split() if t.strip()]
        store.set_comment_tags(comment_id=comment_id, user_id=user.id, tags=parsed)

        params = {}
        if page:
            params["page"] = page
        if q:
            params["q"] = q
        if user_filter:
            params["user_filter"] = user_filter
        if kind:
            params["kind"] = kind
        if tag:
            params["tag"] = tag
        if sort:
            params["sort"] = sort
        if order:
            params["order"] = order
        query = urlencode(params)
        target = f"/tasks/{task_id}"
        if query:
            target = f"{target}?{query}"
        target = f"{target}#c-{comment_id}"
        return RedirectResponse(url=target, status_code=303)

    app.state.store = store
    app.state.manager = manager
    app.state.templates = templates
    return app


app = create_app()


__all__ = ["create_app", "app"]
