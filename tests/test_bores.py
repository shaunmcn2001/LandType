import datetime as dt
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.bores import (  # noqa: E402
    get_bore_icon,
    make_bore_icon_key,
    normalize_bore_drill_date,
    normalize_bore_number,
)


def test_normalize_bore_number_strips_whitespace_and_case():
    assert normalize_bore_number(" rn 0123 ") == "RN0123"


def test_make_bore_icon_key_requires_codes():
    assert make_bore_icon_key("ex", "ab") == "EX,AB"
    assert make_bore_icon_key("", "ab") is None


def test_get_bore_icon_returns_metadata():
    icon = get_bore_icon("EX", "AB")
    assert icon is not None
    assert icon.key == "EX,AB"
    assert icon.image_url and icon.image_url.endswith("c0bd63a150090e7dad0f5d587d3fc664")


def test_normalize_bore_drill_date_from_epoch_ms():
    sample = dt.datetime(1960, 7, 1, tzinfo=dt.timezone.utc)
    millis = int(sample.timestamp() * 1000)
    assert normalize_bore_drill_date(millis) == "1960-07-01"

