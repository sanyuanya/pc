"""SQLite-backed persistence for scraping tasks."""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime
import hashlib
from pathlib import Path
from typing import List, Optional, Tuple

ISO_FORMAT = "%Y-%m-%dT%H:%M:%S"


def utcnow() -> str:
    return datetime.utcnow().strftime(ISO_FORMAT)


@dataclass
class TaskRecord:
    id: str
    raw_url: str
    normalized_url: str
    status: str
    export_format: str
    timeout: int
    created_at: str
    updated_at: str
    output_path: Optional[str]
    total_comments: Optional[int]
    error: Optional[str]
    bvid: Optional[str]
    aid: Optional[str]
    title: Optional[str]
    user_id: str


@dataclass
class UserRecord:
    id: str
    username: str
    password_hash: str


class TaskStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    raw_url TEXT NOT NULL,
                    normalized_url TEXT NOT NULL,
                    status TEXT NOT NULL,
                    export_format TEXT NOT NULL,
                    timeout INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    output_path TEXT,
                    total_comments INTEGER,
                    error TEXT,
                    bvid TEXT,
                    aid TEXT,
                    title TEXT,
                    user_id TEXT DEFAULT 'default'
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id TEXT PRIMARY KEY,
                    username TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS comment_tags (
                    comment_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    tags TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (comment_id, user_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS user_agents (
                    user_id TEXT NOT NULL,
                    label TEXT NOT NULL,
                    ua TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (user_id, label)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS auth_states (
                    user_id TEXT NOT NULL,
                    label TEXT NOT NULL,
                    path TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (user_id, label)
                )
                """
            )
            # 这里确保旧表也有 user_id 列
            columns = [row[1] for row in conn.execute("PRAGMA table_info(tasks)").fetchall()]
            if "user_id" not in columns:
                conn.execute("ALTER TABLE tasks ADD COLUMN user_id TEXT DEFAULT 'default'")
            conn.commit()

    def _row_to_task(self, row: sqlite3.Row) -> TaskRecord:
        return TaskRecord(
            id=row["id"],
            raw_url=row["raw_url"],
            normalized_url=row["normalized_url"],
            status=row["status"],
            export_format=row["export_format"],
            timeout=row["timeout"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            output_path=row["output_path"],
            total_comments=row["total_comments"],
            error=row["error"],
            bvid=row["bvid"],
            aid=row["aid"],
            title=row["title"],
            user_id=row["user_id"] or "default",
        )

    def _row_to_user(self, row: sqlite3.Row) -> UserRecord:
        return UserRecord(id=row["id"], username=row["username"], password_hash=row["password_hash"])

    def _hash_password(self, password: str) -> str:
        return hashlib.sha256(password.encode("utf-8")).hexdigest()

    def add_task(
        self,
        *,
        task_id: str,
        raw_url: str,
        normalized_url: str,
        export_format: str,
        timeout: int,
        user_id: str = "default",
        bvid: Optional[str] = None,
    ) -> TaskRecord:
        now = utcnow()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO tasks (
                    id, raw_url, normalized_url, status, export_format, timeout,
                    created_at, updated_at, user_id, bvid
                ) VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?)
                """,
                (task_id, raw_url, normalized_url, export_format, timeout, now, now, user_id, bvid),
            )
            conn.commit()
        task = self.get_task(task_id)
        if not task:
            raise RuntimeError("Failed to persist task")
        return task

    def update_task(self, task_id: str, **fields) -> Optional[TaskRecord]:
        if not fields:
            return self.get_task(task_id)
        fields["updated_at"] = utcnow()
        keys = ", ".join(f"{k} = ?" for k in fields)
        values = list(fields.values())
        values.append(task_id)
        with self._connect() as conn:
            conn.execute(f"UPDATE tasks SET {keys} WHERE id = ?", values)
            conn.commit()
        return self.get_task(task_id)

    def get_task(self, task_id: str) -> Optional[TaskRecord]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        return self._row_to_task(row) if row else None

    def list_tasks(
        self,
        *,
        search: Optional[str],
        page: int,
        page_size: int,
        user_id: str,
    ) -> Tuple[List[TaskRecord], int]:
        where = ""
        params: List[object] = []
        if search:
            where = "WHERE raw_url LIKE ? OR normalized_url LIKE ? OR IFNULL(title, '') LIKE ?"
            like = f"%{search}%"
            params.extend([like, like, like])
        if where:
            where += " AND user_id = ?"
        else:
            where = "WHERE user_id = ?"
        params.append(user_id)
        with self._connect() as conn:
            total = conn.execute(f"SELECT COUNT(*) FROM tasks {where}", params).fetchone()[0]
            params_with_pagination = params + [page_size, (page - 1) * page_size]
            rows = conn.execute(
                f"SELECT * FROM tasks {where} ORDER BY datetime(created_at) DESC LIMIT ? OFFSET ?",
                params_with_pagination,
            ).fetchall()
        return [self._row_to_task(row) for row in rows], total

    def list_open_tasks(self) -> List[TaskRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM tasks WHERE status IN ('pending', 'running') ORDER BY datetime(created_at)"
            ).fetchall()
        return [self._row_to_task(row) for row in rows]

    def delete_task(self, task_id: str) -> Optional[TaskRecord]:
        record = self.get_task(task_id)
        if not record:
            return None
        with self._connect() as conn:
            conn.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
            conn.commit()
        return record

    def get_user_by_username(self, username: str) -> Optional[UserRecord]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
        return self._row_to_user(row) if row else None

    def get_user_by_id(self, user_id: str) -> Optional[UserRecord]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        return self._row_to_user(row) if row else None

    def create_user(self, user_id: str, username: str, password: str) -> UserRecord:
        password_hash = self._hash_password(password)
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO users (id, username, password_hash) VALUES (?, ?, ?)",
                (user_id, username, password_hash),
            )
            conn.commit()
        user = self.get_user_by_id(user_id)
        if not user:
            raise RuntimeError("Failed to create user")
        return user

    def has_any_user(self) -> bool:
        with self._connect() as conn:
            row = conn.execute("SELECT 1 FROM users LIMIT 1").fetchone()
        return bool(row)

    def verify_user(self, username: str, password: str) -> Optional[UserRecord]:
        user = self.get_user_by_username(username)
        if not user:
            return None
        return user if user.password_hash == self._hash_password(password) else None

    def adopt_default_tasks(self, user_id: str) -> None:
        with self._connect() as conn:
            conn.execute("UPDATE tasks SET user_id = ? WHERE user_id = 'default'", (user_id,))
            conn.commit()

    def set_comment_tags(self, *, comment_id: str, user_id: str, tags: list[str]) -> None:
        normalized = [t.strip() for t in tags if t.strip()]
        tags_str = ",".join(normalized)
        now = utcnow()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO comment_tags (comment_id, user_id, tags, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(comment_id, user_id) DO UPDATE SET tags = excluded.tags, updated_at = excluded.updated_at
                """,
                (comment_id, user_id, tags_str, now),
            )
            conn.commit()

    def get_comment_tags(self, *, comment_ids: list[str], user_id: str) -> dict[str, list[str]]:
        if not comment_ids:
            return {}
        placeholders = ",".join("?" for _ in comment_ids)
        sql = f"SELECT comment_id, tags FROM comment_tags WHERE user_id = ? AND comment_id IN ({placeholders})"
        params: list[str] = [user_id] + comment_ids
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        result: dict[str, list[str]] = {}
        for row in rows:
            tags = [t for t in (row["tags"] or "").split(",") if t]
            result[row["comment_id"]] = tags
        return result

    def get_comment_ids_by_tag(self, *, user_id: str, tag: str) -> list[str]:
        like = f"%{tag.strip()}%"
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT comment_id FROM comment_tags WHERE user_id = ? AND tags LIKE ?",
                (user_id, like),
            ).fetchall()
        return [row["comment_id"] for row in rows]

    def save_user_agent(self, *, user_id: str, label: str, ua: str) -> None:
        now = utcnow()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO user_agents (user_id, label, ua, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id, label) DO UPDATE SET ua=excluded.ua, updated_at=excluded.updated_at
                """,
                (user_id, label.strip() or "default", ua.strip(), now),
            )
            conn.commit()

    def list_user_agents(self, *, user_id: str) -> list[tuple[str, str]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT label, ua FROM user_agents WHERE user_id = ? ORDER BY updated_at DESC",
                (user_id,),
            ).fetchall()
        return [(row["label"], row["ua"]) for row in rows]

    def delete_user_agent(self, *, user_id: str, label: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM user_agents WHERE user_id = ? AND label = ?", (user_id, label))
            conn.commit()

    def save_auth_state(self, *, user_id: str, label: str, path: str) -> None:
        now = utcnow()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO auth_states (user_id, label, path, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id, label) DO UPDATE SET path=excluded.path, updated_at=excluded.updated_at
                """,
                (user_id, label.strip() or "default", path, now),
            )
            conn.commit()

    def list_auth_states(self, *, user_id: str) -> list[tuple[str, str]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT label, path FROM auth_states WHERE user_id = ? ORDER BY updated_at DESC",
                (user_id,),
            ).fetchall()
        return [(row["label"], row["path"]) for row in rows]

    def delete_auth_state(self, *, user_id: str, label: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM auth_states WHERE user_id = ? AND label = ?", (user_id, label))
            conn.commit()

    def delete_all_auth_states(self, *, user_id: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM auth_states WHERE user_id = ?", (user_id,))
            conn.commit()


__all__ = ["TaskRecord", "UserRecord", "TaskStore"]
