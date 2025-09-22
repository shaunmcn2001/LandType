"""Utilities for Queensland groundwater bore metadata."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Tuple, cast

from .config import BORE_ICON_MAP, BORE_LAYER_ID, BORE_SERVICE_URL


def _clean_code(value: str) -> str:
    return (value or "").strip().upper()


def make_bore_icon_key(status_code: str, bore_type_code: str) -> Optional[str]:
    """Return the canonical key used for icon lookups (STATUS,TYPE)."""

    status = _clean_code(status_code)
    bore_type = _clean_code(bore_type_code)
    if not status or not bore_type:
        return None
    return f"{status},{bore_type}"


def normalize_bore_number(value: Any) -> str:
    """Normalise a bore number (RN) to a compact uppercase string."""

    if value is None:
        return ""
    compact = "".join(ch for ch in str(value).strip() if ch.isalnum())
    return compact.upper()


def normalize_bore_drill_date(value: Any) -> Optional[str]:
    """Convert drill dates to ISO-8601 (YYYY-MM-DD)."""

    if value in (None, ""):
        return None
    if isinstance(value, dt.datetime):
        return value.date().isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, (int, float)):
        try:
            return dt.datetime.utcfromtimestamp(float(value) / 1000.0).date().isoformat()
        except (OSError, OverflowError, ValueError):
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            parsed = dt.datetime.fromisoformat(text)
            if parsed.tzinfo is not None:
                parsed = parsed.astimezone(dt.timezone.utc)
            return parsed.date().isoformat()
        except ValueError:
            return text
    return None


@dataclass(frozen=True)
class BoreIconDefinition:
    status_code: str
    bore_type_code: str
    label: str
    symbol: Dict[str, Any]

    @property
    def key(self) -> Optional[str]:
        return make_bore_icon_key(self.status_code, self.bore_type_code)

    @property
    def image_url(self) -> Optional[str]:
        icon_id = (self.symbol or {}).get("url")
        if not icon_id:
            return None
        return f"{BORE_SERVICE_URL.rstrip('/')}/{BORE_LAYER_ID}/images/{icon_id}"

    @property
    def image_data(self) -> Optional[str]:
        return (self.symbol or {}).get("imageData")

    @property
    def content_type(self) -> Optional[str]:
        return (self.symbol or {}).get("contentType")


_ICON_BY_PAIR: Dict[Tuple[str, str], BoreIconDefinition] = {}
_ICON_BY_KEY: Dict[str, BoreIconDefinition] = {}

for (status, bore_type), meta in BORE_ICON_MAP.items():
    status_norm = _clean_code(status)
    type_norm = _clean_code(bore_type)
    meta_mapping = cast(Mapping[str, Any], meta)
    symbol_meta = cast(Mapping[str, Any], meta_mapping.get("symbol") or {})
    definition = BoreIconDefinition(
        status_code=status_norm,
        bore_type_code=type_norm,
        label=str(meta_mapping.get("label", "")),
        symbol=dict(symbol_meta),
    )
    pair_key = (status_norm, type_norm)
    _ICON_BY_PAIR[pair_key] = definition
    if definition.key:
        _ICON_BY_KEY[definition.key] = definition


def get_bore_icon(status_code: str, bore_type_code: str) -> Optional[BoreIconDefinition]:
    """Look up icon metadata for a status/type pair."""

    status = _clean_code(status_code)
    bore_type = _clean_code(bore_type_code)
    if not status or not bore_type:
        return None
    return _ICON_BY_PAIR.get((status, bore_type))


def get_bore_icon_by_key(icon_key: str) -> Optional[BoreIconDefinition]:
    key = _clean_code(icon_key)
    if not key:
        return None
    return _ICON_BY_KEY.get(key)


__all__ = [
    "BoreIconDefinition",
    "get_bore_icon",
    "get_bore_icon_by_key",
    "make_bore_icon_key",
    "normalize_bore_drill_date",
    "normalize_bore_number",
]

