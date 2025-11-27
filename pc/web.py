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
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from pc.scraper import normalize_video_url, scrape
from pc.storage import TaskRecord, TaskStore, UserRecord
from pc.i18n import get_trans

ROOT_DIR = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = ROOT_DIR / "templates"
STATIC_DIR = ROOT_DIR / "static"
MAX_STATE_FILE_SIZE = 512 * 1024  # 这里限制上传文件大小，避免异常文件


class TaskManager:
    def __init__(self, store: TaskStore, data_dir: Path, *, max_workers: int = 1) -> None:
        self.store = store
        self.data_dir = data_dir
        self.exports_dir = self.data_dir / "exports"
        self.exports_dir.mkdir(parents=True, exist_ok=True)
        self.queue: asyncio.Queue[Optional[str]] = asyncio.Queue()
        self.workers: List[asyncio.Task[None]] = []
        self.max_workers = max(1, max_workers)

    async def start(self) -> None:
        if self.workers:
            return
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

    async def enqueue(self, task_id: str) -> None:
        await self.queue.put(task_id)

    def get_storage_state_path(self, user_id: str) -> Path:
        path = self.data_dir / "users" / user_id / "state.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

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
        self.store.update_task(task_id, status="running", error=None)
        try:
            duration = record.timeout if record.timeout and record.timeout > 0 else None
            state_path = self.get_storage_state_path(record.user_id)
            auth_state = state_path if state_path.exists() and state_path.stat().st_size > 0 else None
            result = await scrape(
                record.normalized_url,
                output_path,
                max_duration=duration,
                storage_state=auth_state,
            )
            self.store.update_task(
                task_id,
                status="completed",
                output_path=str(result.output_path),
                total_comments=len(result.comments),
                bvid=result.bvid,
                aid=result.aid,
                title=result.title,
            )
        except Exception as exc:  # pragma: no cover - network failures
            self.store.update_task(task_id, status="failed", error=str(exc))


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


def create_app(*, data_dir: Path | str = Path("data"), max_workers: Optional[int] = None) -> FastAPI:
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
    store = TaskStore(data_dir / "tasks.db")
    manager = TaskManager(store=store, data_dir=data_dir, max_workers=worker_count)
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
    ) -> HTMLResponse:
        page = max(1, page)
        page_size = 8
        tasks, total = store.list_tasks(
            search=query, page=page, page_size=page_size, user_id=user.id
        )
        total_pages = max(1, math.ceil(total / page_size))
        state_path = manager.get_storage_state_path(user.id)
        has_auth = state_path.exists() and state_path.stat().st_size > 0
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
                "storage_state_path": str(state_path),
                "retried_task": flash_retry,
                "user": user,
            },
        )

    @app.get("/settings/auth", response_class=HTMLResponse)
    async def auth_settings(
        request: Request,
        uploaded: Optional[str] = None,
        cleared: Optional[str] = None,
        error: Optional[str] = None,
        limit: Optional[str] = None,
    ) -> HTMLResponse:
        user = get_current_user(request)
        if not user:
            return login_redirect(request)
        state_path = manager.get_storage_state_path(user.id)
        has_auth = state_path.exists() and state_path.stat().st_size > 0
        lang = request.cookies.get("lang", "zh")
        def _(key: str, **kwargs) -> str: return get_trans(lang, key, **kwargs)
        error_message: Optional[str] = None
        if error:
            fmt_kwargs = {}
            if error == "settings_auth_error_size":
                fmt_kwargs["size"] = limit or str(MAX_STATE_FILE_SIZE // 1024)
            error_message = _(error, **fmt_kwargs)
        return render_template(
            "settings_auth.html",
            {
                "request": request,
                "has_auth": has_auth,
                "storage_state_path": str(state_path),
                "uploaded": uploaded,
                "cleared": cleared,
                "error_message": error_message,
                "state_limit_kb": MAX_STATE_FILE_SIZE // 1024,
                "user": user,
            },
        )

    @app.post("/settings/auth/upload")
    async def upload_auth_state(
        request: Request, state_file: UploadFile = File(...)
    ) -> RedirectResponse:
        user = get_current_user(request)
        if not user:
            return login_redirect(request)
        contents = await state_file.read()
        filename = (state_file.filename or "").lower()
        redirect_url = "/settings/auth"

        def _error(code: str, *, extra: Optional[str] = None) -> RedirectResponse:
            suffix = f"?error={code}"
            if extra:
                suffix += f"&limit={extra}"
            return RedirectResponse(url=f"{redirect_url}{suffix}", status_code=303)

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

        state_path = manager.get_storage_state_path(user.id)
        state_path.write_text(text, encoding="utf-8")
        return RedirectResponse(url="/settings/auth?uploaded=1", status_code=303)

    @app.post("/settings/auth/clear")
    async def clear_auth_state(request: Request) -> RedirectResponse:
        user = get_current_user(request)
        if not user:
            return login_redirect(request)
        state_path = manager.get_storage_state_path(user.id)
        try:
            state_path.unlink(missing_ok=True)
        except Exception:
            pass
        return RedirectResponse(url="/settings/auth?cleared=1", status_code=303)

    @app.get("/", response_class=HTMLResponse)
    async def dashboard(
        request: Request,
        page: int = 1,
        q: Optional[str] = None,
        created: Optional[int] = None,
        failed: Optional[int] = None,
        retried: Optional[str] = None,
    ) -> HTMLResponse:
        user = get_current_user(request)
        if not user:
            return login_redirect(request)
        return render_dashboard(
            request,
            user,
            page=page,
            query=q,
            created=created,
            failed=failed,
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
        export_format = export_format.lower()
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
        if export_format not in {"json", "csv"}:
            errors.append(_("err_format_support"))
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
                    store.add_task(
                        task_id=task_id,
                        raw_url=entry,
                        normalized_url=normalized,
                        export_format=export_format,
                        timeout=timeout_value,
                        user_id=user.id,
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

    @app.get("/tasks/{task_id}", response_class=HTMLResponse)
    async def task_detail(request: Request, task_id: str) -> HTMLResponse:
        user = get_current_user(request)
        if not user:
            return login_redirect(request)
        lang = request.cookies.get("lang", "zh")
        def _(key: str, **kwargs) -> str: return get_trans(lang, key, **kwargs)

        task = store.get_task(task_id)
        if not task or task.user_id != user.id:
            raise HTTPException(status_code=404, detail=_("err_task_not_found"))
        preview = load_preview(task)
        return render_template(
            "detail.html",
            {
                "request": request,
                "task": task,
                "preview": preview,
                "user": user,
            },
        )

    @app.get("/tasks/{task_id}/download")
    async def download(task_id: str, request: Request) -> FileResponse:
        user = get_current_user(request)
        if not user:
            raise HTTPException(status_code=403, detail="请先登录")
        lang = request.cookies.get("lang", "zh")
        def _(key: str, **kwargs) -> str: return get_trans(lang, key, **kwargs)

        task = store.get_task(task_id)
        if not task or task.user_id != user.id or not task.output_path:
            raise HTTPException(status_code=404, detail=_("err_no_download"))
        path = Path(task.output_path)
        if not path.exists():
            raise HTTPException(status_code=404, detail=_("err_file_missing"))
        media_type = "text/csv" if path.suffix == ".csv" else "application/json"
        filename = path.name
        return FileResponse(path, media_type=media_type, filename=filename)

    app.state.store = store
    app.state.manager = manager
    app.state.templates = templates
    return app


app = create_app()


__all__ = ["create_app", "app"]
