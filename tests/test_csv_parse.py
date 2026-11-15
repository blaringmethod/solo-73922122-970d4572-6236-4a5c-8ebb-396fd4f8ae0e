"""附件 CSV 严格解析的单元测试。"""
from __future__ import annotations

import pytest

from app.collector import CrawlError, _parse_attachment

HEADER = "product_code,product_name,batch_no,produced_at,reason\n"


def test_parse_valid_utf8_csv():
    raw = (HEADER + "SP-1,牛奶,B001,2026-10-01,超标\n"
                    "SP-1,牛奶,B002,2026-10-02,超标\n").encode("utf-8")
    rows = _parse_attachment("R9", raw)
    assert len(rows) == 2
    assert rows[0] == {
        "announcement_id": "R9", "product_code": "SP-1", "batch_no": "B001",
        "product_name": "牛奶", "produced_at": "2026-10-01", "reason": "超标",
    }


def test_reject_invalid_utf8():
    with pytest.raises(CrawlError) as exc:
        _parse_attachment("R9", b"\xff\xfe" + HEADER.encode("utf-8"))
    assert exc.value.code == "ATTACHMENT_CORRUPT"


def test_reject_missing_required_column():
    raw = b"product_code,batch_no\nSP-1,B001\n"
    with pytest.raises(CrawlError) as exc:
        _parse_attachment("R9", raw)
    assert exc.value.code == "ATTACHMENT_BAD_FORMAT"


def test_reject_empty_batch_row():
    raw = (HEADER + "SP-1,牛奶,,2026-10-01,超标\n").encode("utf-8")
    with pytest.raises(CrawlError) as exc:
        _parse_attachment("R9", raw)
    assert exc.value.code == "ATTACHMENT_BAD_FORMAT"


def test_reject_empty_attachment():
    with pytest.raises(CrawlError) as exc:
        _parse_attachment("R9", HEADER.encode("utf-8"))
    assert exc.value.code == "ATTACHMENT_BAD_FORMAT"
