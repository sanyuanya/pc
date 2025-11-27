import pytest

from pc import postgres


def test_prepare_rows_formats_values() -> None:
    sink = postgres.PostgresSink("postgresql://user:pass@localhost:5432/db")
    comments = [
        {
            "comment_id": "42",
            "parent_comment_id": "",
            "user_id": 99,
            "user_name": " Tester ",
            "content": "hello",
            "publish_time": "2024-01-02 03:04:05",
            "like_count": "7",
            "is_sub_reply": False,
        },
        {"comment_id": "", "content": "skip me"},
    ]

    rows = list(sink._prepare_rows(comments, bvid="BV1abc", aid="100", title="Video"))

    assert len(rows) == 1
    row = rows[0]
    assert row[0] == "42"
    assert row[5].isoformat() == "2024-01-02T03:04:05"
    assert row[6] == 7
    assert row[8] == "BV1abc"
    assert row[10] == "Video"


def test_safe_table_name_validation() -> None:
    assert postgres._safe_table_name("valid_table") == "valid_table"
    with pytest.raises(ValueError):
        postgres._safe_table_name("bad-name")


def test_parse_timestamp_handles_invalid() -> None:
    assert postgres._parse_timestamp("not-a-date") is None
    assert postgres._parse_timestamp("") is None
