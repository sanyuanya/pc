"""Utility helpers to run transparent raffles on top of scraped comments."""
from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence


@dataclass
class RaffleSummary:
    winners: List[dict]
    candidate_count: int
    unique_candidates: int
    unique_by_user: bool
    seed: Optional[str]


def _normalize_comment(entry: dict) -> dict:
    """Ensure comment fields exist so the UI can render consistent metadata."""

    comment_id = str(entry.get("comment_id") or entry.get("rpid") or "")
    entry["comment_id"] = comment_id
    entry["user_id"] = str(entry.get("user_id") or entry.get("mid") or "")
    entry["user_name"] = entry.get("user_name") or entry.get("uname") or ""
    entry["content"] = (entry.get("content") or entry.get("message") or "").strip()
    entry["origin_url"] = entry.get("origin_url")
    return entry


def run_raffle(
    comments: Sequence[dict],
    *,
    count: int,
    unique_by_user: bool,
    seed: Optional[str] = None,
) -> RaffleSummary:
    """Uniformly sample winners from the comment list without replacement."""

    if count <= 0:
        raise ValueError("count must be > 0")
    cleaned = [_normalize_comment(dict(entry)) for entry in comments if entry]
    if not cleaned:
        return RaffleSummary([], 0, 0, unique_by_user, seed)
    seen = set()
    pool: List[dict] = []
    for entry in cleaned:
        key = entry["user_id"] if unique_by_user else entry["comment_id"]
        if unique_by_user and key:
            if key in seen:
                continue
            seen.add(key)
        pool.append(entry)
    candidate_count = len(pool)
    if candidate_count == 0:
        return RaffleSummary([], len(cleaned), 0, unique_by_user, seed)
    rng = random.SystemRandom() if seed is None else random.Random(seed)
    winners: List[dict] = []
    available = pool.copy()
    while available and len(winners) < count:
        idx = rng.randrange(len(available))
        winners.append(available.pop(idx))
    return RaffleSummary(
        winners=winners,
        candidate_count=len(pool),
        unique_candidates=len(seen) if unique_by_user else len(pool),
        unique_by_user=unique_by_user,
        seed=seed,
    )


__all__ = ["run_raffle", "RaffleSummary"]
