"""Async Postgres sink for persisting scraped comments."""
from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Iterable, Optional, Sequence, Tuple

import asyncpg

DEFAULT_TABLE = "comments"
_TABLE_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _parse_timestamp(value: Any) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value)
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _safe_table_name(name: str) -> str:
    candidate = name.strip() or DEFAULT_TABLE
    if not _TABLE_PATTERN.match(candidate):
        raise ValueError("Invalid table name for Postgres sink")
    return candidate


class PostgresSink:
    def __init__(self, dsn: str, *, table: str = DEFAULT_TABLE) -> None:
        if not dsn:
            raise ValueError("Postgres DSN is required")
        self.dsn = dsn
        self.table = _safe_table_name(table)
        self.pool: Optional[asyncpg.Pool] = None

    async def start(self) -> None:
        if self.pool:
            return
        self.pool = await asyncpg.create_pool(self.dsn, min_size=1, max_size=5)
        await self._ensure_schema()

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def _ensure_schema(self) -> None:
        assert self.pool is not None
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table} (
            comment_id TEXT PRIMARY KEY,
            parent_comment_id TEXT,
            user_id TEXT,
            user_name TEXT,
            content TEXT,
            publish_time TIMESTAMP,
            like_count INTEGER,
            is_sub_reply BOOLEAN,
            bvid TEXT,
            aid TEXT,
            video_title TEXT,
            scraped_at TIMESTAMPTZ DEFAULT NOW()
        )
        """
        index_sql = [
            f"CREATE INDEX IF NOT EXISTS idx_{self.table}_bvid ON {self.table} (bvid)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.table}_parent ON {self.table} (parent_comment_id)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.table}_time ON {self.table} (publish_time DESC)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.table}_likes ON {self.table} (like_count DESC)",
        ]
        async with self.pool.acquire() as conn:
            await conn.execute(create_sql)
            for stmt in index_sql:
                await conn.execute(stmt)

    def _prepare_rows(
        self, comments: Sequence[dict], *, bvid: str, aid: str, title: Optional[str]
    ) -> Iterable[Tuple[Any, ...]]:
        for comment in comments:
            comment_id = str(comment.get("comment_id") or "").strip()
            if not comment_id:
                continue
            parent_id = str(comment.get("parent_comment_id") or "").strip() or None
            user_id = str(comment.get("user_id") or "").strip() or None
            user_name = (comment.get("user_name") or "").strip() or None
            content = comment.get("content") or ""
            publish_time = _parse_timestamp(comment.get("publish_time"))
            like_raw = comment.get("like_count")
            try:
                like_count = int(like_raw)
            except (TypeError, ValueError):
                like_count = 0
            is_sub_reply = bool(comment.get("is_sub_reply"))
            yield (
                comment_id,
                parent_id,
                user_id,
                user_name,
                content,
                publish_time,
                like_count,
                is_sub_reply,
                bvid,
                aid,
                title,
            )

    async def save_comments(
        self, comments: Sequence[dict], *, bvid: str, aid: str, title: Optional[str] = None
    ) -> int:
        if not comments:
            return 0
        if not self.pool:
            await self.start()
        rows = list(self._prepare_rows(comments, bvid=bvid, aid=aid, title=title))
        if not rows:
            return 0
        upsert_sql = f"""
        INSERT INTO {self.table} (
            comment_id, parent_comment_id, user_id, user_name, content,
            publish_time, like_count, is_sub_reply, bvid, aid, video_title
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
        )
        ON CONFLICT (comment_id) DO UPDATE SET
            parent_comment_id = EXCLUDED.parent_comment_id,
            user_id = EXCLUDED.user_id,
            user_name = EXCLUDED.user_name,
            content = EXCLUDED.content,
            publish_time = EXCLUDED.publish_time,
            like_count = EXCLUDED.like_count,
            is_sub_reply = EXCLUDED.is_sub_reply,
            bvid = EXCLUDED.bvid,
            aid = EXCLUDED.aid,
            video_title = EXCLUDED.video_title,
            scraped_at = NOW()
        """
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.executemany(upsert_sql, rows)
        return len(rows)

    async def fetch_by_ids(self, *, bvid: str, ids: Sequence[str]) -> dict[str, dict]:
        if not ids:
            return {}
        if not self.pool:
            await self.start()
        sub_count_expr = f"(SELECT COUNT(*) FROM {self.table} c2 WHERE c2.parent_comment_id = c1.comment_id)"
        sql = f"""
        SELECT
            c1.comment_id, c1.parent_comment_id, c1.user_id, c1.user_name,
            c1.content, c1.publish_time, c1.like_count, c1.is_sub_reply,
            c1.bvid, c1.aid, c1.video_title,
            {sub_count_expr} AS sub_count
        FROM {self.table} c1
        WHERE c1.bvid = $1 AND c1.comment_id = ANY($2::text[])
        """
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, bvid, list(ids))
        results: dict[str, dict] = {}
        for row in rows:
            ts = row["publish_time"]
            results[row["comment_id"]] = {
                "comment_id": row["comment_id"],
                "parent_comment_id": row["parent_comment_id"] or "",
                "user_id": row["user_id"] or "",
                "user_name": row["user_name"] or "",
                "content": row["content"] or "",
                "publish_time": ts.isoformat(sep=" ", timespec="seconds") if ts else "",
                "like_count": row["like_count"] or 0,
                "is_sub_reply": bool(row["is_sub_reply"]),
                "bvid": row["bvid"],
                "aid": row["aid"],
                "video_title": row["video_title"],
                "sub_count": row["sub_count"] or 0,
            }
        return results

    async def fetch_children(
        self, *, bvid: str, parent_ids: Sequence[str], limit_per_parent: int = 3
    ) -> dict[str, list[dict]]:
        if not parent_ids:
            return {}
        if not self.pool:
            await self.start()
        limit = max(1, min(limit_per_parent, 20))
        sql = f"""
        SELECT *
        FROM (
            SELECT
                c1.*,
                ROW_NUMBER() OVER (PARTITION BY parent_comment_id ORDER BY publish_time DESC, comment_id) AS rn
            FROM {self.table} c1
            WHERE c1.bvid = $1 AND c1.parent_comment_id = ANY($2::text[])
        ) ranked
        WHERE rn <= $3
        ORDER BY parent_comment_id, publish_time DESC, comment_id
        """
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, bvid, list(parent_ids), limit)
        grouped: dict[str, list[dict]] = {}
        for row in rows:
            ts = row["publish_time"]
            item = {
                "comment_id": row["comment_id"],
                "parent_comment_id": row["parent_comment_id"] or "",
                "user_id": row["user_id"] or "",
                "user_name": row["user_name"] or "",
                "content": row["content"] or "",
                "publish_time": ts.isoformat(sep=" ", timespec="seconds") if ts else "",
                "like_count": row["like_count"] or 0,
                "is_sub_reply": bool(row["is_sub_reply"]),
                "bvid": row["bvid"],
                "aid": row["aid"],
                "video_title": row["video_title"],
                "sub_count": 0,
            }
            grouped.setdefault(item["parent_comment_id"], []).append(item)
        return grouped

    async def query_comments(
        self,
        *,
        bvid: str,
        page: int,
        page_size: int,
        sort: str,
        order: str,
        keyword: Optional[str],
        user: Optional[str],
        kind: str,
        allowed_ids: Optional[Sequence[str]] = None,
    ) -> Tuple[list[dict], int]:
        if not self.pool:
            await self.start()
        sort_map = {
            "time": "c1.publish_time",
            "likes": "c1.like_count",
            "sub": "sub_count",
        }
        sort_col = sort_map.get(sort, "c1.publish_time")
        order_sql = "ASC" if order == "asc" else "DESC"
        where = ["c1.bvid = $1"]
        params: list[Any] = [bvid]
        idx = 2
        if allowed_ids:
            where.append(f"c1.comment_id = ANY(${idx}::text[])")
            params.append(list(allowed_ids))
            idx += 1
        if kind == "main":
            where.append("c1.is_sub_reply = FALSE")
        elif kind == "sub":
            where.append("c1.is_sub_reply = TRUE")
        if keyword:
            where.append(f"(c1.content ILIKE ${idx})")
            params.append(f"%{keyword}%")
            idx += 1
        if user:
            where.append(f"(c1.user_name ILIKE ${idx} OR c1.user_id = ${idx + 1})")
            params.extend([f"%{user}%", user])
            idx += 2
        where_sql = " AND ".join(where)
        sub_count_expr = f"(SELECT COUNT(*) FROM {self.table} c2 WHERE c2.parent_comment_id = c1.comment_id)"
        count_sql = f"SELECT COUNT(*) FROM {self.table} c1 WHERE {where_sql}"
        limit_placeholder = idx
        offset_placeholder = idx + 1
        select_sql = f"""
        SELECT
            c1.comment_id, c1.parent_comment_id, c1.user_id, c1.user_name,
            c1.content, c1.publish_time, c1.like_count, c1.is_sub_reply,
            c1.bvid, c1.aid, c1.video_title,
            {sub_count_expr} AS sub_count
        FROM {self.table} c1
        WHERE {where_sql}
        ORDER BY {sort_col} {order_sql}, c1.comment_id
        LIMIT ${limit_placeholder} OFFSET ${offset_placeholder}
        """
        params_for_count = list(params)
        params.extend([page_size, (page - 1) * page_size])
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            total = await conn.fetchval(count_sql, *params_for_count)
            rows = await conn.fetch(select_sql, *params)
        results = []
        for row in rows:
            ts = row["publish_time"]
            results.append(
                {
                    "comment_id": row["comment_id"],
                    "parent_comment_id": row["parent_comment_id"] or "",
                    "user_id": row["user_id"] or "",
                    "user_name": row["user_name"] or "",
                    "content": row["content"] or "",
                    "publish_time": ts.isoformat(sep=" ", timespec="seconds") if ts else "",
                    "like_count": row["like_count"] or 0,
                    "is_sub_reply": bool(row["is_sub_reply"]),
                    "bvid": row["bvid"],
                    "aid": row["aid"],
                    "video_title": row["video_title"],
                    "sub_count": row["sub_count"] or 0,
                }
            )
        return results, int(total or 0)

    async def aggregate_stats(self, *, bvid: str) -> dict:
        if not self.pool:
            await self.start()
        sql = f"""
        SELECT
            COUNT(*) FILTER (WHERE NOT is_sub_reply) AS main_count,
            COUNT(*) FILTER (WHERE is_sub_reply) AS sub_count,
            COUNT(DISTINCT user_id) AS unique_users,
            COALESCE(MAX(like_count), 0) AS max_likes,
            COALESCE(AVG(like_count), 0) AS avg_likes
        FROM {self.table}
        WHERE bvid = $1
        """
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(sql, bvid)
        total = (row["main_count"] or 0) + (row["sub_count"] or 0)
        return {
            "total": total,
            "main_count": row["main_count"] or 0,
            "sub_count": row["sub_count"] or 0,
            "unique_users": row["unique_users"] or 0,
            "max_likes": row["max_likes"] or 0,
            "avg_likes": float(row["avg_likes"] or 0),
        }

    async def fetch_all(self, *, bvid: str, order: str = "time") -> list[dict]:
        if not self.pool:
            await self.start()
        sort_col = "publish_time" if order == "time" else "like_count"
        sub_count_expr = f"(SELECT COUNT(*) FROM {self.table} c2 WHERE c2.parent_comment_id = c1.comment_id)"
        sql = f"""
        SELECT
            c1.comment_id, c1.parent_comment_id, c1.user_id, c1.user_name,
            c1.content, c1.publish_time, c1.like_count, c1.is_sub_reply,
            c1.bvid, c1.aid, c1.video_title,
            {sub_count_expr} AS sub_count
        FROM {self.table} c1
        WHERE c1.bvid = $1
        ORDER BY c1.{sort_col} DESC, c1.comment_id
        """
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, bvid)
        results = []
        for row in rows:
            ts = row["publish_time"]
            results.append(
                {
                    "comment_id": row["comment_id"],
                    "parent_comment_id": row["parent_comment_id"] or "",
                    "user_id": row["user_id"] or "",
                    "user_name": row["user_name"] or "",
                    "content": row["content"] or "",
                    "publish_time": ts.isoformat(sep=" ", timespec="seconds") if ts else "",
                    "like_count": row["like_count"] or 0,
                    "is_sub_reply": bool(row["is_sub_reply"]),
                    "bvid": row["bvid"],
                    "aid": row["aid"],
                    "video_title": row["video_title"],
                    "sub_count": row["sub_count"] or 0,
                }
            )
        return results


__all__ = ["PostgresSink", "DEFAULT_TABLE"]
