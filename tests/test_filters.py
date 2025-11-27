from pc import web


def _sample_comments():
    return [
        {
            "comment_id": "1",
            "parent_comment_id": "",
            "user_id": "u1",
            "user_name": "Alice",
            "content": "hello world",
            "publish_time": "2024-01-02 10:00:00",
            "like_count": 5,
            "is_sub_reply": False,
            "sub_count": 2,
        },
        {
            "comment_id": "2",
            "parent_comment_id": "1",
            "user_id": "u2",
            "user_name": "Bob",
            "content": "replying to Alice",
            "publish_time": "2024-01-01 10:00:00",
            "like_count": 1,
            "is_sub_reply": True,
            "sub_count": 0,
        },
        {
            "comment_id": "3",
            "parent_comment_id": "",
            "user_id": "u3",
            "user_name": "Alice",
            "content": "second top level",
            "publish_time": "2024-01-03 10:00:00",
            "like_count": 3,
            "is_sub_reply": False,
            "sub_count": 0,
        },
    ]


def test_filter_and_paginate_sorts_and_filters():
    comments = _sample_comments()
    # Filter by keyword and sort by likes
    page_items, total = web._filter_and_paginate(
        comments,
        page=1,
        page_size=10,
        sort="likes",
        order="desc",
        keyword="reply",
        user=None,
        kind="all",
    )
    assert total == 1
    assert page_items[0]["comment_id"] == "2"

    # Only main comments, sort by time ascending
    page_items, total = web._filter_and_paginate(
        comments,
        page=1,
        page_size=10,
        sort="time",
        order="asc",
        keyword=None,
        user=None,
        kind="main",
    )
    assert total == 2
    assert [c["comment_id"] for c in page_items] == ["1", "3"]

    # Filter by user name (case-insensitive)
    page_items, total = web._filter_and_paginate(
        comments,
        page=1,
        page_size=10,
        sort="time",
        order="desc",
        keyword=None,
        user="alice",
        kind="all",
    )
    assert total == 2


def test_compute_stats_counts_users_and_likes():
    comments = _sample_comments()
    stats = web._compute_stats(comments)
    assert stats["total"] == 3
    assert stats["main_count"] == 2
    assert stats["sub_count"] == 1
    assert stats["unique_users"] == 3
    assert stats["max_likes"] == 5
    assert stats["avg_likes"] == 3.0
