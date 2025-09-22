# app/main.py
import base64
import binascii
import csv
import datetime as dt
import html
import io
import logging
import math
import os
import tempfile
import zipfile
from dataclasses import dataclass, replace
from io import BytesIO
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple
from urllib.parse import quote

from fastapi import Body, FastAPI, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel, Field
from shapely.geometry import mapping as shp_mapping, shape as shp_shape
from shapely.validation import make_valid

from .arcgis import (
    fetch_bores_intersecting_envelope,
    fetch_easements_intersecting_envelope,
    fetch_features_intersecting_envelope,
    fetch_landtypes_intersecting_envelope,
    fetch_parcel_geojson,
    fetch_water_layers_intersecting_envelope,
    normalize_lotplan,
)
from .colors import color_from_code
from .config import (
    BORE_DRILL_DATE_FIELD,
    BORE_NUMBER_FIELD,
    BORE_REPORT_URL_FIELD,
    BORE_STATUS_CODE_FIELD,
    BORE_STATUS_LABEL_FIELD,
    BORE_TYPE_CODE_FIELD,
    BORE_TYPE_LABEL_FIELD,
    EASEMENT_AREA_FIELD,
    EASEMENT_FEATURE_NAME_FIELD,
    EASEMENT_LOTPLAN_FIELD,
    EASEMENT_PARCEL_TYPE_FIELD,
    EASEMENT_TENURE_FIELD,
    VEG_CODE_FIELD_DEFAULT,
    VEG_LAYER_ID_DEFAULT,
    VEG_NAME_FIELD_DEFAULT,
    VEG_SERVICE_URL_DEFAULT,
)
from .bores import (
    get_bore_icon_by_key,
    make_bore_icon_key,
    normalize_bore_drill_date,
    normalize_bore_number,
)
from .geometry import (
    bbox_3857,
    prepare_clipped_shapes,
    to_shapely_union,
)
from .kml import (
    PointPlacemark,
    build_kml,
    build_kml_folders,
    build_kml_nested_folders,
    write_kmz,
)
from .raster import make_geotiff_rgba

logging.basicConfig(level=logging.INFO)
app = FastAPI(
    title="QLD Land Types (rewritten)",
    description="Unified single/bulk exporter for Land Types + optional Vegetation (GeoTIFF, KMZ).",
    version="3.0.2",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

def _hex(rgb):
    r,g,b = rgb
    return "#{:02x}{:02x}{:02x}".format(int(r),int(g),int(b))

def _sanitize_filename(s: Optional[str]) -> str:
    base = "".join(c for c in (s or "").strip() if c.isalnum() or c in ("_", "-", ".", " "))
    return (base or "download").strip()


def _content_disposition(filename: str) -> str:
    ascii_name = _sanitize_filename(filename.replace("–", "-") if filename else filename)
    if not ascii_name:
        ascii_name = "download"
    encoded = quote(filename or "download", safe="")
    return f"attachment; filename=\"{ascii_name}\"; filename*=UTF-8''{encoded}"


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_bore_properties(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    props = raw or {}

    bore_number = normalize_bore_number(
        props.get("bore_number")
        or props.get(BORE_NUMBER_FIELD)
        or props.get("rn")
        or props.get("rn_char")
    )
    if not bore_number:
        return None

    status_code = _clean_text(
        props.get("status")
        or props.get("status_code")
        or props.get(BORE_STATUS_CODE_FIELD)
        or props.get("facility_status")
    )
    status_label = _clean_text(
        props.get("status_label")
        or props.get(BORE_STATUS_LABEL_FIELD)
        or props.get("statusLabel")
        or props.get("facility_status_decode")
    )

    bore_type_code = _clean_text(
        props.get("type")
        or props.get("type_code")
        or props.get(BORE_TYPE_CODE_FIELD)
        or props.get("facility_type")
    )
    bore_type_label = _clean_text(
        props.get("type_label")
        or props.get(BORE_TYPE_LABEL_FIELD)
        or props.get("typeLabel")
        or props.get("facility_type_decode")
    )

    drilled_date = normalize_bore_drill_date(
        props.get("drilled_date") or props.get(BORE_DRILL_DATE_FIELD)
    )
    report_url = _clean_text(
        props.get("report_url") or props.get(BORE_REPORT_URL_FIELD)
    )

    icon_key = props.get("icon_key")
    if not icon_key:
        icon_key = make_bore_icon_key(status_code, bore_type_code)

    def _or_none(value: str) -> Optional[str]:
        return value or None

    return {
        "bore_number": bore_number,
        "status": _or_none(status_code),
        "status_label": _or_none(status_label) or _or_none(status_code),
        "type": _or_none(bore_type_code),
        "type_label": _or_none(bore_type_label) or _or_none(bore_type_code),
        "drilled_date": drilled_date,
        "report_url": _or_none(report_url),
        "icon_key": icon_key,
    }


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        text = str(value)
    except Exception:
        return None
    text = text.strip()
    if not text:
        return None
    text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def _clip_to_parcel_union(geom, parcel_union):
    if geom.is_empty:
        return None
    if parcel_union is None or parcel_union.is_empty:
        return geom
    try:
        if not parcel_union.intersects(geom):
            return None
    except Exception:
        pass
    try:
        clipped = parcel_union.intersection(geom)
    except Exception:
        try:
            clipped = parcel_union.intersection(make_valid(geom))
        except Exception:
            try:
                clipped = make_valid(parcel_union).intersection(make_valid(geom))
            except Exception:
                clipped = geom
    if clipped.is_empty:
        return None
    return clipped


def _normalize_easement_properties(raw: Dict[str, Any], lotplan: str) -> Dict[str, Any]:
    props = raw or {}

    owner_lp = normalize_lotplan(
        props.get("lotplan")
        or props.get(EASEMENT_LOTPLAN_FIELD)
        or props.get("lot_plan")
        or lotplan
    )

    parcel_type = _clean_text(
        props.get("parcel_type")
        or props.get(EASEMENT_PARCEL_TYPE_FIELD)
    )
    name = _clean_text(
        props.get("name")
        or props.get(EASEMENT_FEATURE_NAME_FIELD)
    )
    alias = _clean_text(
        props.get("alias")
        or props.get("feat_alias")
        or props.get("feature_alias")
    )
    tenure = _clean_text(
        props.get("tenure")
        or props.get(EASEMENT_TENURE_FIELD)
    )

    area_value = props.get("area_m2")
    if area_value is None:
        area_value = props.get(EASEMENT_AREA_FIELD)
    area_m2 = _safe_float(area_value)

    out: Dict[str, Any] = {
        "lotplan": owner_lp or lotplan,
        "parcel_type": parcel_type or None,
        "name": name or alias or None,
        "tenure": tenure or None,
    }

    if alias:
        out["alias"] = alias

    if area_m2 is not None:
        out["area_m2"] = area_m2
        out["area_ha"] = area_m2 / 10000.0

    return out


def _clean_bound_value(value: Any) -> Optional[float]:
    number = _safe_float(value)
    if number is None:
        return None
    if isinstance(number, float) and math.isnan(number):
        return None
    return number


def _bounds_dict_from_geom(bounds_geom, fallback=None) -> Dict[str, Optional[float]]:
    candidate = bounds_geom
    if candidate is None or getattr(candidate, "is_empty", True):
        candidate = fallback
    if candidate is None or getattr(candidate, "is_empty", True):
        return {"west": None, "south": None, "east": None, "north": None}
    west, south, east, north = candidate.bounds
    return {
        "west": _clean_bound_value(west),
        "south": _clean_bound_value(south),
        "east": _clean_bound_value(east),
        "north": _clean_bound_value(north),
    }


BORE_FOLDER_NAME = "Groundwater Bores"
_ICON_EXTENSIONS: Dict[str, str] = {
    "image/png": "png",
    "image/jpeg": "jpg",
    "image/jpg": "jpg",
}

_EXT_CONTENT_TYPES: Dict[str, str] = {}
for _mime, _ext in _ICON_EXTENSIONS.items():
    if _ext:
        _EXT_CONTENT_TYPES.setdefault(_ext.lower(), _mime)


def _slugify_icon_key(icon_key: str) -> str:
    key = (icon_key or "").strip().lower().replace(",", "_")
    return "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in key) or "icon"


def _icon_href_for_key(icon_key: str, content_type: Optional[str]) -> str:
    slug = _slugify_icon_key(icon_key)
    ext = _ICON_EXTENSIONS.get((content_type or "").lower(), "png")
    return f"icons/{slug}.{ext}"


def _icon_content_type_from_href(icon_href: str) -> str:
    _, ext = os.path.splitext(icon_href or "")
    ext_clean = ext.lower().lstrip(".")
    if ext_clean:
        return _EXT_CONTENT_TYPES.get(ext_clean, "image/png")
    return "image/png"


def _data_uri_for_icon(icon_href: str, data: Optional[bytes]) -> Optional[str]:
    if not data:
        return None
    try:
        content_type = _icon_content_type_from_href(icon_href)
        encoded = base64.b64encode(data).decode("ascii")
    except Exception:
        return None
    return f"data:{content_type};base64,{encoded}"


def _inline_point_icon_hrefs(
    points: Sequence[PointPlacemark], assets: Mapping[str, bytes]
) -> List[PointPlacemark]:
    if not points:
        return []
    if not assets:
        return list(points)

    cache: Dict[str, Optional[str]] = {}
    updated: List[PointPlacemark] = []
    for point in points:
        icon_href = point.icon_href
        if not icon_href:
            updated.append(point)
            continue
        if icon_href not in cache:
            raw_data = assets.get(icon_href)
            cache[icon_href] = _data_uri_for_icon(icon_href, raw_data)
        data_uri = cache[icon_href]
        if data_uri:
            updated.append(replace(point, icon_href=data_uri))
        else:
            updated.append(point)
    return updated


def _format_bore_description(props: Dict[str, Any]) -> str:
    def combine(label: Optional[str], code: Optional[str]) -> Optional[str]:
        label_clean = (label or "").strip()
        code_clean = (code or "").strip()
        if label_clean and code_clean and label_clean.upper() != code_clean.upper():
            return f"{label_clean} ({code_clean})"
        return label_clean or code_clean or None

    parts: List[str] = []
    status_text = combine(props.get("status_label"), props.get("status"))
    if status_text:
        parts.append(f"<b>Status:</b> {html.escape(status_text)}")
    type_text = combine(props.get("type_label"), props.get("type"))
    if type_text:
        parts.append(f"<b>Type:</b> {html.escape(type_text)}")
    drilled = props.get("drilled_date")
    if drilled:
        parts.append(f"<b>Drilled:</b> {html.escape(str(drilled))}")
    report_url = props.get("report_url")
    if report_url:
        safe_url = html.escape(str(report_url), quote=True)
        parts.append(f'<a href="{safe_url}" target="_blank" rel="noopener">View bore report</a>')
    return "<br/>".join(parts)


def _prepare_bore_placemarks(
    parcel_geom,
    bore_fc: Dict[str, Any],
) -> Tuple[List[PointPlacemark], Dict[str, bytes]]:
    placemarks: List[PointPlacemark] = []
    assets: Dict[str, bytes] = {}
    seen_numbers: Set[str] = set()

    for bore in bore_fc.get("features", []):
        try:
            geom = shp_shape(bore.get("geometry"))
        except Exception:
            continue
        if geom.is_empty or geom.geom_type != "Point":
            continue
        if parcel_geom is not None:
            try:
                if not geom.intersects(parcel_geom):
                    continue
            except Exception:
                pass
        props = _normalize_bore_properties(bore.get("properties") or {})
        if not props:
            continue
        bore_number = props.get("bore_number")
        if not bore_number or bore_number in seen_numbers:
            continue
        seen_numbers.add(bore_number)

        icon_key = props.get("icon_key")
        style_id = None
        icon_href = None
        if icon_key:
            icon_def = get_bore_icon_by_key(icon_key)
            image_data = icon_def.image_data if icon_def else None
            if image_data:
                try:
                    icon_bytes = base64.b64decode(image_data)
                except (binascii.Error, ValueError):
                    icon_bytes = None
                if icon_bytes:
                    icon_href = _icon_href_for_key(icon_key, icon_def.content_type if icon_def else None)
                    assets.setdefault(icon_href, icon_bytes)
                    style_id = f"bore_{_slugify_icon_key(icon_key)}"

        description_html = _format_bore_description(props)
        placemarks.append(
            PointPlacemark(
                name=bore_number,
                description_html=description_html,
                lon=float(geom.x),
                lat=float(geom.y),
                style_id=style_id,
                icon_href=icon_href,
            )
        )

    return placemarks, assets


def _format_water_value(key: str, value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        key_lower = (key or "").lower()
        if "date" in key_lower:
            try:
                ts = float(value)
                if ts > 10_000_000_000:
                    ts = ts / 1000.0
                return dt.datetime.utcfromtimestamp(ts).date().isoformat()
            except Exception:
                pass
        return str(value)
    text = _clean_text(value)
    return text


def _format_water_description(props: Dict[str, Any]) -> str:
    name = props.get("display_name") or props.get("name") or props.get("layer_title") or "Water feature"
    lines: List[str] = [f"<b>{html.escape(str(name))}</b>"]
    layer_title = props.get("layer_title")
    if layer_title:
        lines.append(f"<span class=\"muted\">Layer:</span> {html.escape(str(layer_title))}")
    lotplan = props.get("lotplan")
    if lotplan:
        lines.append(f"<span class=\"muted\">Lot/Plan:</span> {html.escape(str(lotplan))}")

    skip_keys = {
        "display_name",
        "name",
        "code",
        "layer_id",
        "layer_title",
        "source_layer_name",
        "lotplan",
        "icon_key",
    }

    extra: List[Tuple[str, str]] = []
    for key, raw_value in (props or {}).items():
        if key in skip_keys:
            continue
        if raw_value is None:
            continue
        if isinstance(raw_value, (list, tuple)):
            values = [_clean_text(v) for v in raw_value if _clean_text(v)]
            if not values:
                continue
            value_text = ", ".join(values)
        else:
            value_text = _format_water_value(key, raw_value)
            value_text = _clean_text(value_text)
            if not value_text:
                continue
        label = key.replace("_", " ").strip().title()
        extra.append((label, value_text))

    extra.sort()
    for label, value_text in extra[:8]:
        lines.append(f"<span class=\"muted\">{html.escape(label)}:</span> {html.escape(value_text)}")

    return "<br/>".join(lines)


@dataclass(frozen=True)
class WaterLayerKMZ:
    layer_id: int
    layer_title: str
    source_layer_name: str
    geometry_type: Optional[str]
    shapes: Tuple[tuple, ...]
    points: Tuple[PointPlacemark, ...]
    feature_collection: Dict[str, Any]


def _prepare_water_layers(
    parcel_fc: Dict[str, Any],
    water_layers_raw: Sequence[Dict[str, Any]],
    lotplan: Optional[str],
) -> List[WaterLayerKMZ]:
    if not water_layers_raw:
        return []

    prepared: List[WaterLayerKMZ] = []

    for layer in water_layers_raw:
        layer_id = int(layer.get("layer_id", -1))
        layer_title = layer.get("layer_title") or f"Layer {layer_id}"
        source_layer_name = layer.get("source_layer_name") or layer_title
        feature_collection = layer.get("feature_collection") or {}
        features = list(feature_collection.get("features", []))
        if not features:
            continue

        props_lookup: Dict[str, Dict[str, Any]] = {}
        features_for_clip: List[Dict[str, Any]] = []
        fc_for_clip = {"type": "FeatureCollection", "features": features_for_clip}
        for feature in features:
            geometry = feature.get("geometry")
            if not geometry:
                continue
            props = dict(feature.get("properties") or {})
            code = _clean_text(props.get("code"))
            if not code:
                continue
            display_name = props.get("name") or layer_title
            props["name"] = display_name
            props.setdefault("display_name", display_name)
            props.setdefault("layer_id", layer_id)
            props.setdefault("layer_title", layer_title)
            props.setdefault("source_layer_name", source_layer_name)
            if lotplan:
                props.setdefault("lotplan", lotplan)
            props_lookup[code] = props
            features_for_clip.append(
                {"type": "Feature", "geometry": geometry, "properties": props}
            )

        if not props_lookup:
            continue

        clipped = prepare_clipped_shapes(parcel_fc, fc_for_clip)
        if not clipped:
            continue

        shapes: List[tuple] = []
        points: List[PointPlacemark] = []
        clipped_features: List[Dict[str, Any]] = []

        for geom4326, code, name, area_ha in clipped:
            props = dict(props_lookup.get(code, {}))
            props.setdefault("name", name)
            if lotplan:
                props.setdefault("lotplan", lotplan)
            try:
                geom_mapping = shp_mapping(geom4326)
            except Exception:
                continue
            clipped_features.append(
                {
                    "type": "Feature",
                    "geometry": geom_mapping,
                    "properties": props,
                }
            )

            geom_type = getattr(geom4326, "geom_type", "")
            if geom_type == "Point":
                description_html = _format_water_description(props)
                points.append(
                    PointPlacemark(
                        name=props.get("display_name") or props.get("name") or code,
                        description_html=description_html,
                        lon=float(geom4326.x),
                        lat=float(geom4326.y),
                    )
                )
            elif geom_type == "MultiPoint":
                description_html = _format_water_description(props)
                for part in getattr(geom4326, "geoms", []):
                    if part is None or getattr(part, "is_empty", False):
                        continue
                    points.append(
                        PointPlacemark(
                            name=props.get("display_name") or props.get("name") or code,
                            description_html=description_html,
                            lon=float(part.x),
                            lat=float(part.y),
                        )
                    )
            else:
                shapes.append((geom4326, code, props.get("name") or name, area_ha))

        prepared.append(
            WaterLayerKMZ(
                layer_id=layer_id,
                layer_title=layer_title,
                source_layer_name=source_layer_name,
                geometry_type=layer.get("geometry_type"),
                shapes=tuple(shapes),
                points=tuple(points),
                feature_collection={
                    "type": "FeatureCollection",
                    "features": clipped_features,
                },
            )
        )

    return prepared


def _render_parcel_kml(
    lotplan: str,
    lt_clipped,
    veg_clipped,
    bore_points: Sequence[PointPlacemark],
) -> str:
    folder_name = f"QLD Land Types – {lotplan}"
    if veg_clipped:
        groups: List[Any] = [
            (lt_clipped, color_from_code, f"Land Types – {lotplan}"),
            (veg_clipped, color_from_code, f"Vegetation – {lotplan}"),
        ]
        if bore_points:
            groups.append(([], color_from_code, BORE_FOLDER_NAME, list(bore_points)))
        return build_kml_folders(groups, doc_name=f"QLD Export – {lotplan}")

    if bore_points:
        return build_kml(
            lt_clipped,
            color_fn=color_from_code,
            folder_name=folder_name,
            point_placemarks=list(bore_points),
            point_folder_name=BORE_FOLDER_NAME,
        )
    return build_kml(lt_clipped, color_fn=color_from_code, folder_name=folder_name)


def _kmz_bytes(kml_text: str, assets: Dict[str, bytes]) -> bytes:
    mem = BytesIO()
    with zipfile.ZipFile(mem, "w", compression=zipfile.ZIP_DEFLATED) as ztmp:
        ztmp.writestr("doc.kml", kml_text.encode("utf-8"))
        for name, data in (assets or {}).items():
            if not name or data is None:
                continue
            ztmp.writestr(name, data)
    return mem.getvalue()


@dataclass(frozen=True)
class PropertyReportKMZ:
    lotplan: str
    filename: str
    kml_text: str
    kmz_bytes: bytes
    landtypes: Tuple[tuple, ...]
    vegetation: Tuple[tuple, ...]
    easements: Tuple[tuple, ...]
    easement_color_map: Mapping[str, str]
    water_layers: Tuple[WaterLayerKMZ, ...]
    bore_points: Tuple[PointPlacemark, ...]
    bore_assets: Mapping[str, bytes]


def _default_veg_config() -> Tuple[str, Optional[int], str, Optional[str]]:
    veg_url = (VEG_SERVICE_URL_DEFAULT or "").strip()
    veg_layer = VEG_LAYER_ID_DEFAULT
    veg_name = (VEG_NAME_FIELD_DEFAULT or "").strip()
    veg_code = (VEG_CODE_FIELD_DEFAULT or "").strip() or None
    return veg_url, veg_layer, veg_name, veg_code


def build_property_report_kmz(
    lotplan: str,
    *,
    simplify_tolerance: float = 0.0,
    veg_service_url: Optional[str] = None,
    veg_layer_id: Optional[int] = None,
    veg_name_field: Optional[str] = None,
    veg_code_field: Optional[str] = None,
) -> PropertyReportKMZ:
    lotplan_norm = normalize_lotplan(lotplan)
    if not lotplan_norm:
        raise HTTPException(status_code=400, detail="Lotplan is required.")

    parcel_fc = fetch_parcel_geojson(lotplan_norm)
    parcel_union = to_shapely_union(parcel_fc)
    env = bbox_3857(parcel_union)

    thematic_fc = fetch_landtypes_intersecting_envelope(env)
    lt_clipped = prepare_clipped_shapes(parcel_fc, thematic_fc)

    bore_fc = fetch_bores_intersecting_envelope(env)
    bore_points, bore_assets = _prepare_bore_placemarks(parcel_union, bore_fc)

    water_layers_raw = fetch_water_layers_intersecting_envelope(env)
    water_layers = _prepare_water_layers(parcel_fc, water_layers_raw, lotplan_norm)

    veg_url = (veg_service_url or "").strip()
    veg_layer = veg_layer_id
    veg_name = (veg_name_field or "").strip()
    veg_code = (veg_code_field or "").strip() or None
    if not veg_url or veg_layer is None or not veg_name:
        veg_url, veg_layer, veg_name, veg_code = _default_veg_config()

    veg_clipped: List[tuple] = []
    if veg_url and veg_layer is not None and veg_name:
        veg_fc = fetch_features_intersecting_envelope(
            veg_url,
            veg_layer,
            env,
            out_fields="*",
        )
        for feature in veg_fc.get("features", []):
            props = feature.get("properties") or {}
            code = str(props.get(veg_code or "code") or props.get("code") or "").strip()
            name = str(props.get(veg_name or "name") or props.get("name") or code).strip()
            props["code"] = code or name or "UNK"
            category_name = name or code or "Unknown"
            props["name"] = f"Category {category_name}"
        veg_clipped = prepare_clipped_shapes(parcel_fc, veg_fc)

    easement_fc = fetch_easements_intersecting_envelope(env)
    easement_features: List[Dict[str, Any]] = []
    easement_meta: Dict[str, Dict[str, Any]] = {}
    for feature in (easement_fc or {}).get("features", []):
        geometry = feature.get("geometry")
        if not geometry:
            continue
        props = _normalize_easement_properties(feature.get("properties") or {}, lotplan_norm)
        owner_lp = props.get("lotplan") or lotplan_norm
        parcel_type = props.get("parcel_type") or ""
        tenure = props.get("tenure") or ""
        alias = props.get("alias") or ""
        display_name = props.get("name") or alias or "Easement"
        identifier_parts = [owner_lp or "", parcel_type, tenure, alias, display_name]
        sanitized_parts = [(part or "").replace("|", "/") for part in identifier_parts]
        identifier = "|".join(sanitized_parts).strip("|") or (owner_lp or lotplan_norm or "Easement")
        color_key = parcel_type or tenure or owner_lp or "Easement"
        easement_meta[identifier] = {
            "lotplan": owner_lp,
            "parcel_type": parcel_type,
            "tenure": tenure,
            "alias": alias,
            "display_name": display_name,
            "color_key": color_key,
            "area_ha": props.get("area_ha"),
        }
        easement_features.append(
            {
                "type": "Feature",
                "geometry": geometry,
                "properties": {
                    "code": identifier,
                    "name": display_name,
                },
            }
        )

    easement_clipped_raw = prepare_clipped_shapes(
        parcel_fc,
        {"type": "FeatureCollection", "features": easement_features},
    )

    if simplify_tolerance and simplify_tolerance > 0:
        def _simplify(data: Iterable[tuple]) -> Tuple[tuple, ...]:
            simplified: List[tuple] = []
            for geom4326, code, name, area_ha in data:
                try:
                    g2 = geom4326.simplify(simplify_tolerance, preserve_topology=True)
                except Exception:
                    g2 = geom4326
                if g2.is_empty:
                    continue
                simplified.append((g2, code, name, area_ha))
            return tuple(simplified) if simplified else tuple(data)

        lt_clipped = list(_simplify(lt_clipped))
        if veg_clipped:
            veg_clipped = list(_simplify(veg_clipped))
        if easement_clipped_raw:
            easement_clipped_raw = list(_simplify(easement_clipped_raw))

    easement_clipped: List[tuple] = []
    easement_color_lookup: Dict[str, str] = {}
    for geom4326, identifier, display_name, area_ha in easement_clipped_raw:
        meta = easement_meta.get(identifier, {})

        def _display(value: Optional[str]) -> str:
            text = (value or "").strip()
            return text or "-"

        owner_lp = _display(meta.get("lotplan") or lotplan_norm)
        parcel_type = _display(meta.get("parcel_type"))
        tenure = _display(meta.get("tenure"))
        alias = _display(meta.get("alias"))

        area_value = 0.0
        if area_ha is not None:
            try:
                area_value = float(area_ha)
            except (TypeError, ValueError):
                area_value = 0.0
        if area_value == 0.0:
            fallback_area = _safe_float(meta.get("area_ha"))
            if fallback_area is not None:
                area_value = float(fallback_area)

        desc_parts = [
            f"Lot/Plan: {owner_lp}",
            f"Parcel Type: {parcel_type}",
            f"Tenure: {tenure}",
            f"Alias: {alias}",
            f"Area: {area_value:.2f} ha",
        ]
        code_text = " | ".join(desc_parts)
        color_key = meta.get("color_key") or identifier or "Easement"
        easement_color_lookup[code_text] = color_key
        display_label = meta.get("display_name") or display_name or "Easement"
        area_for_tuple = area_ha if area_ha is not None else area_value
        easement_clipped.append((geom4326, code_text, display_label, area_for_tuple))

    def _easement_color_fn(code: str) -> Tuple[int, int, int]:
        base = easement_color_lookup.get(code, code)
        return color_from_code(base)

    top_level_groups: List[Tuple[str, List[tuple]]] = []
    if lt_clipped:
        top_level_groups.append(("Land Types", [(lt_clipped, color_from_code, None)]))
    if veg_clipped:
        top_level_groups.append(("Vegetation", [(veg_clipped, color_from_code, None)]))
    if easement_clipped:
        top_level_groups.append(("Easements", [(easement_clipped, _easement_color_fn, None)]))

    water_children: List[tuple] = []
    if bore_points:
        water_children.append(([], color_from_code, BORE_FOLDER_NAME, list(bore_points)))
    for layer in water_layers:
        def _water_color_fn(code: str, _layer_id=layer.layer_id) -> Tuple[int, int, int]:
            return color_from_code(f"WATER-{_layer_id}")

        water_children.append(
            (list(layer.shapes), _water_color_fn, layer.layer_title, list(layer.points))
        )

    if water_children:
        top_level_groups.append(("Water", water_children))

    doc_name = f"Property Report – {lotplan_norm}"
    kml_text = build_kml_nested_folders(top_level_groups, doc_name=doc_name)

    has_water = any(
        (layer.shapes or layer.points or layer.feature_collection.get("features"))
        for layer in water_layers
    )

    if not (lt_clipped or veg_clipped or easement_clipped or bore_points or has_water):
        raise HTTPException(status_code=404, detail="No features intersect this parcel.")

    tmpdir = tempfile.mkdtemp(prefix="kmz_")
    filename = f"Property Report – {lotplan_norm}.kmz"
    out_path = os.path.join(tmpdir, filename)
    try:
        write_kmz(kml_text, out_path, assets=bore_assets)
        with open(out_path, "rb") as fh:
            kmz_bytes = fh.read()
    finally:
        try:
            if os.path.exists(out_path):
                os.remove(out_path)
        except Exception:
            pass
        try:
            os.rmdir(tmpdir)
        except Exception:
            pass

    return PropertyReportKMZ(
        lotplan=lotplan_norm,
        filename=filename,
        kml_text=kml_text,
        kmz_bytes=kmz_bytes,
        landtypes=tuple(lt_clipped or []),
        vegetation=tuple(veg_clipped or []),
        easements=tuple(easement_clipped or []),
        easement_color_map=dict(easement_color_lookup),
        water_layers=tuple(water_layers),
        bore_points=tuple(bore_points),
        bore_assets=dict(bore_assets),
    )

@app.head("/")
def home_head(): return Response(status_code=200)

@app.get("/", response_class=HTMLResponse)
def home():
    # Replace configuration placeholders with actual values
    html_template = """<!doctype html>
<html><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>QLD Land Types (rewritten)</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" crossorigin=""/>
<style>
:root{--bg:#0b1220;--card:#121a2b;--text:#e8eefc;--muted:#9fb2d8;--accent:#6aa6ff}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:16px/1.45 system-ui,-apple-system,Segoe UI,Roboto,Inter,Arial,sans-serif}
.wrap{max-width:1100px;margin:28px auto;padding:0 16px}.card{background:var(--card);border:1px solid #1f2a44;border-radius:16px;box-shadow:0 10px 30px rgba(0,0,0,.25);padding:18px}
h1{margin:4px 0 10px;font-size:26px}p{margin:0 0 14px;color:var(--muted)}label{display:block;margin:10px 0 6px;color:var(--muted);font-size:14px}
input[type=text],input[type=number],textarea,select{width:100%;padding:10px 12px;border-radius:12px;border:1px solid #2b3960;background:#0e1526;color:var(--text)}
textarea{min-height:110px;resize:vertical}.row{display:flex;gap:12px;flex-wrap:wrap}.row>*{flex:1 1 200px}.btns{margin-top:12px;display:flex;gap:10px;flex-wrap:wrap}
button,.ghost{appearance:none;border:0;border-radius:12px;padding:10px 14px;font-weight:600;cursor:pointer}
button.primary{background:var(--accent);color:#071021}a.ghost{color:var(--accent);text-decoration:none;border:1px solid #294a86;background:#0d1730}
.note{margin-top:8px;font-size:13px;color:#89a3d6}#map{height:520px;border-radius:14px;margin-top:14px;border:1px solid #203055}
.out{margin-top:12px;border-top:1px solid #203055;padding-top:10px;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;white-space:pre-wrap}
.badge{display:inline-block;padding:.2rem .5rem;border-radius:999px;background:#11204a;color:#9fc1ff;font-size:12px;margin-left:8px}
.chip{display:inline-flex;align-items:center;gap:6px;padding:.2rem .6rem;border-radius:999px;background:#11204a;color:#9fc1ff;font-size:12px}
.muted{color:#9fb2d8}.box{border:1px solid #203055;border-radius:12px;padding:10px;background:#0e1526;margin-top:6px}
</style>
</head>
<body>
<div class="wrap"><div class="card">
  <h1>QLD Land Types <span class="badge">EPSG:4326</span> <span id="mode" class="chip">Mode: Single</span></h1>
  <p>Paste one or many <strong>Lot / Plan</strong> codes. Download a <strong>Property Report (KMZ)</strong> with land types, vegetation, bores, and easements for every parcel.</p>

  <div class="row">
    <div style="flex: 2 1 420px;">
      <label for="items">Lot / Plan (single OR multiple — new line, comma, or semicolon separated)</label>
      <textarea id="items" placeholder="13SP181800
1RP12345
2RP54321"></textarea>
      <div class="muted" id="parseinfo">Detected 0 items.</div>
    </div>
    <div>
      <label for="name">Name (single) or Prefix (bulk)</label>
      <input id="name" type="text" placeholder="e.g. UpperCoomera_13SP181800 or Job_4021" />
      <div class="box muted">Exports generate a KMZ you can open in Google Earth or other GIS viewers.</div>
    </div>
  </div>



  <div class="btns">
    <button class="primary" id="btn-export-report">Download Property Report (KMZ)</button>
    <a class="ghost" id="btn-json" href="#">Preview JSON (single)</a>
    <a class="ghost" id="btn-load" href="#">Load on Map</a>
  </div>

  <div class="note">JSON preview requires exactly one lot/plan. The map supports one or many. API docs: <a href="/docs">/docs</a></div>
  <div id="map"></div><div id="out" class="out"></div>
</div></div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" crossorigin="" defer></script>
<script>
const $items = document.getElementById('items'),
      $name = document.getElementById('name'),
      $mode = document.getElementById('mode'),
      $out = document.getElementById('out'),
      $parseinfo = document.getElementById('parseinfo'),
      $btnExportReport = document.getElementById('btn-export-report'),
      $btnJson = document.getElementById('btn-json'),
      $btnLoad = document.getElementById('btn-load');

const DEFAULT_MAX_PX = 4096;
const DEFAULT_SIMPLIFY = 0;
const DEFAULT_BORE_COLOR = '#38bdf8';
const BORE_STATUS_COLORS = {
  EX: '#22c55e',
  AU: '#2563eb',
  AD: '#ef4444',
  IN: '#f59e0b'
};
const WATER_LAYER_COLORS = {
  20: '#0ea5e9',
  21: '#38bdf8',
  22: '#3b82f6',
  23: '#1d4ed8',
  24: '#0ea5e9',
  25: '#22d3ee',
  26: '#0891b2',
  27: '#2563eb',
  28: '#1e40af',
  30: '#0ea5e9',
  31: '#0284c7',
  33: '#14b8a6',
  34: '#0f766e',
  35: '#0d9488',
  37: '#06b6d4'
};
const ESCAPE_HTML_LOOKUP = {
  "&": "&amp;",
  "<": "&lt;",
  ">": "&gt;",
  '"': "&quot;",
  "'": "&#39;"
};

function colorForBoreStatus(status){
  const key = (status || '').toString().trim().toUpperCase();
  return BORE_STATUS_COLORS[key] || DEFAULT_BORE_COLOR;
}

function colorForWaterLayer(id){
  const key = Number(id);
  return WATER_LAYER_COLORS[key] || '#22d3ee';
}

function boreClassName(key){
  if (!key) return 'bore-marker';
  return `bore-marker bore-${String(key).toLowerCase().replace(/[^a-z0-9]+/g,'-')}`;
}

function escHtml(value){
  return (value == null ? '' : String(value)).replace(/[&<>"']/g, ch => ESCAPE_HTML_LOOKUP[ch] || ch);
}

function formatWaterPopup(props){
  const data = props || {};
  const lines = [];
  const name = data.display_name || data.name || data.layer_title || 'Water feature';
  lines.push(`<strong>${escHtml(name)}</strong>`);
  if (data.layer_title){ lines.push(`<span class="muted">Layer:</span> ${escHtml(data.layer_title)}`); }
  if (data.lotplan){ lines.push(`<span class="muted">Lot/Plan:</span> ${escHtml(data.lotplan)}`); }

  const skip = new Set(['display_name','name','layer_title','layer_id','source_layer_name','lotplan','code']);
  const entries = [];
  for (const [key, raw] of Object.entries(data)){
    if (skip.has(key) || raw == null || raw === '') continue;
    let text = '';
    if (Array.isArray(raw)){
      const vals = raw.map(v => escHtml(v)).filter(Boolean);
      if (!vals.length) continue;
      text = vals.join(', ');
    } else {
      if (typeof raw === 'number' && key.toLowerCase().includes('date')){
        const stamp = raw > 1e12 ? raw : raw * 1000;
        const dt = new Date(stamp);
        if (!Number.isNaN(dt.getTime())){
          text = dt.toISOString().slice(0, 10);
        }
      }
      if (!text){ text = escHtml(String(raw)); }
      if (!text) continue;
    }
    const label = key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
    entries.push([label, text]);
  }
  entries.sort((a,b)=>a[0].localeCompare(b[0]));
  for (const [label, text] of entries.slice(0,6)){
    lines.push(`<span class="muted">${escHtml(label)}:</span> ${text}`);
  }
  return lines.join('<br/>');
}

function normText(s){ return (s || '').trim(); }
function parseItems(text){
  const src = (text || '')
    .toUpperCase()
    .replace(/\bLOT\b/g,' ')
    .replace(/\bPLAN\b/g,' ')
    .replace(/\bON\b/g,' ')
    .replace(/[^A-Z0-9]+/g,' ');
  const seen = new Set(); const out = [];
  const rx = /(\\d+)\\s*([A-Z]+[A-Z0-9]+)/g; let m;
  while((m = rx.exec(src)) !== null){
    const code = `${m[1]}${m[2]}`;
    if(!seen.has(code)){ seen.add(code); out.push(code); }
  }
  return out;
}
function updateMode(){
  const items = parseItems($items.value);
  const n = items.length;
  $parseinfo.textContent = `Detected ${n} item${n===1?'':'s'}.`;
  if (n === 0){
    $mode.textContent = "Mode: None";
    $btnJson.style.opacity='.5'; $btnJson.style.pointerEvents='none';
    $btnLoad.style.opacity='.5'; $btnLoad.style.pointerEvents='none';
  } else if (n === 1){
    $mode.textContent = "Mode: Single";
    $btnJson.style.opacity='1'; $btnJson.style.pointerEvents='auto';
    $btnLoad.style.opacity='1'; $btnLoad.style.pointerEvents='auto';
  } else {
    $mode.textContent = `Mode: Bulk (${n})`;
    $btnJson.style.opacity='.5'; $btnJson.style.pointerEvents='none';
    $btnLoad.style.opacity='1'; $btnLoad.style.pointerEvents='auto';
  }
}

let map=null, parcelLayer=null, ltLayer=null, boreLayer=null, easementLayer=null, waterLayers=[];
function ensureMap(){
  try{
    if (map) return;
    if (!window.L) return;
    map = L.map('map', { zoomControl: true });
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { attribution: '&copy; OpenStreetMap' }).addTo(map);
    map.setView([-23.5, 146.0], 5);
  }catch(e){ console.warn('Map init failed:', e); }
}
function styleForCode(code, colorHex){ return { color:'#0c1325', weight:1, fillColor:colorHex, fillOpacity:0.6 }; }
function clearLayers(){
  try{
    if(map && parcelLayer){ map.removeLayer(parcelLayer); parcelLayer=null; }
    if(map && ltLayer){ map.removeLayer(ltLayer); ltLayer=null; }
    if(map && boreLayer){ map.removeLayer(boreLayer); boreLayer=null; }
    if(map && easementLayer){ map.removeLayer(easementLayer); easementLayer=null; }
    if(map && waterLayers.length){ waterLayers.forEach(layer => map.removeLayer(layer)); waterLayers=[]; }
  }catch{}
}
function mkVectorUrl(lotplan){ return `/vector?lotplan=${encodeURIComponent(lotplan)}`; }

async function loadVector(){
  const items = parseItems($items.value);
  if (!items.length){ $out.textContent = 'Enter at least one Lot/Plan to load map.'; return; }
  ensureMap(); if (!map){ $out.textContent = 'Map library not loaded yet. Try again in a moment.'; return; }
  const multi = items.length > 1;
  $out.textContent = multi ? `Loading vector data for ${items.length} lots/plans…` : 'Loading vector data…';
  try{
    let res, data;
    if (multi){
      res = await fetch('/vector/bulk', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ lotplans: items }) });
    } else {
      res = await fetch(mkVectorUrl(items[0]));
    }
    data = await res.json();
    if (!res.ok){
      const msg = data && (data.detail || data.error) ? (data.detail || data.error) : 'Unexpected server response.';
      $out.textContent = `Error ${res.status}: ${msg}`;
      return;
    }
    if (data.error){ $out.textContent = 'Error: ' + data.error; return; }
    clearLayers();
    const parcelData = data.parcels || data.parcel;
    if (parcelData){
      parcelLayer = L.geoJSON(parcelData, {
        style: { color: '#ffcc00', weight:2, fillOpacity:0 },
        onEachFeature: (feature, layer) => {
          const p = feature.properties || {};
          if (p.lotplan){ layer.bindPopup(`<strong>Lot/Plan:</strong> ${p.lotplan}`); }
        }
      }).addTo(map);
    }
    const ltData = data.landtypes;
    if (ltData && ltData.features && ltData.features.length){
      ltLayer = L.geoJSON(ltData, { style: f => styleForCode(f.properties.code, f.properties.color_hex),
        onEachFeature: (feature, layer) => {
          const p = feature.properties || {};
          const html = `<b>${p.name || 'Unknown'}</b><br/>Code: <code>${p.code || 'UNK'}</code><br/>Area: ${(p.area_ha ?? 0).toFixed(2)} ha${p.lotplan ? `<br/>Lot/Plan: ${p.lotplan}` : ''}`;
          layer.bindPopup(html);
        }}).addTo(map);
    }
    const boreData = data.bores;
    if (boreData && Array.isArray(boreData.features) && boreData.features.length){
      boreLayer = L.geoJSON(boreData, {
        pointToLayer: (feature, latlng) => {
          const props = feature.properties || {};
          const color = colorForBoreStatus(props.status);
          const cls = boreClassName(props.icon_key || props.status);
          return L.circleMarker(latlng, {
            radius: 6,
            color,
            weight: 1.5,
            fillColor: color,
            fillOpacity: 0.85,
            className: cls
          });
        },
        onEachFeature: (feature, layer) => {
          const props = feature.properties || {};
          const lines = [];
          const num = props.bore_number || 'Unknown';
          lines.push(`<strong>Bore ${escHtml(num)}</strong>`);
          const statusText = props.status_label || props.status;
          if (statusText){ lines.push(`<span class="muted">Status:</span> ${escHtml(statusText)}`); }
          const typeText = props.type_label || props.type;
          if (typeText){ lines.push(`<span class="muted">Type:</span> ${escHtml(typeText)}`); }
          if (props.drilled_date){ lines.push(`<span class="muted">Drilled:</span> ${escHtml(props.drilled_date)}`); }
          layer.bindPopup(lines.join('<br/>'));
          if (props.bore_number){ layer.options.title = `Bore ${props.bore_number}`; }
        }
      }).addTo(map);
    }
    const easementData = data.easements;
    if (easementData && Array.isArray(easementData.features) && easementData.features.length){
      const easementStyle = { color: '#58b0ff', weight: 2, dashArray: '6 4', fillColor: '#58b0ff', fillOpacity: 0.15 };
      easementLayer = L.geoJSON(easementData, {
        style: () => easementStyle,
        onEachFeature: (feature, layer) => {
          const props = feature.properties || {};
          const lines = [];
          const title = props.name || props.alias;
          lines.push(`<strong>${escHtml(title || 'Easement')}</strong>`);
          if (props.alias && props.name){ lines.push(`<span class="muted">Alias:</span> ${escHtml(props.alias)}`); }
          if (props.lotplan){ lines.push(`<span class="muted">Lot/Plan:</span> ${escHtml(props.lotplan)}`); }
          if (props.parcel_type){ lines.push(`<span class="muted">Parcel Type:</span> ${escHtml(props.parcel_type)}`); }
          if (props.tenure){ lines.push(`<span class="muted">Tenure:</span> ${escHtml(props.tenure)}`); }
          const areaParts = [];
          const areaHa = typeof props.area_ha === 'number' && !Number.isNaN(props.area_ha) ? props.area_ha : null;
          const areaM2 = typeof props.area_m2 === 'number' && !Number.isNaN(props.area_m2) ? props.area_m2 : null;
          if (areaHa != null){ areaParts.push(`${areaHa.toFixed(4)} ha`); }
          if (areaM2 != null){ areaParts.push(`${areaM2.toLocaleString()} m²`); }
          if (areaParts.length){ lines.push(`<span class="muted">Area:</span> ${areaParts.join(' / ')}`); }
          layer.bindPopup(lines.join('<br/>'));
        }
      }).addTo(map);
    }
    const waterData = data.water && Array.isArray(data.water.layers) ? data.water.layers : [];
    for (const entry of waterData){
      if (!entry || !entry.features || !Array.isArray(entry.features.features) || !entry.features.features.length) continue;
      const color = colorForWaterLayer(entry.layer_id);
      const geo = L.geoJSON(entry.features, {
        style: feature => {
          const geomType = feature && feature.geometry ? feature.geometry.type : null;
          if (geomType === 'LineString' || geomType === 'MultiLineString'){
            return { color, weight: 2.5, opacity: 0.9 };
          }
          if (geomType === 'Polygon' || geomType === 'MultiPolygon'){
            return { color: '#0c1325', weight: 1.2, fillColor: color, fillOpacity: 0.35 };
          }
          return { color };
        },
        pointToLayer: (feature, latlng) => L.circleMarker(latlng, {
          radius: 5,
          color,
          weight: 1.4,
          fillColor: color,
          fillOpacity: 0.85
        }),
        onEachFeature: (feature, layer) => {
          const html = formatWaterPopup(feature && feature.properties);
          if (html){ layer.bindPopup(html); }
        }
      }).addTo(map);
      waterLayers.push(geo);
    }
    const b = data.bounds4326;
    if (b){
      map.fitBounds([[b.south, b.west],[b.north, b.east]], { padding:[20,20] });
    } else if (easementLayer && easementLayer.getBounds){
      map.fitBounds(easementLayer.getBounds(), { padding:[20,20] });
    } else if (parcelLayer && parcelLayer.getBounds){
      map.fitBounds(parcelLayer.getBounds(), { padding:[20,20] });
    } else if (ltLayer && ltLayer.getBounds){
      map.fitBounds(ltLayer.getBounds(), { padding:[20,20] });
    } else if (boreLayer && boreLayer.getBounds){
      map.fitBounds(boreLayer.getBounds(), { padding:[20,20] });
    }
    const summary = {
      lotplans: data.lotplans || (data.lotplan ? [data.lotplan] : []),
      legend: data.legend || [],
      bounds4326: data.bounds4326 || null,
      easement_count: easementData && Array.isArray(easementData.features) ? easementData.features.length : 0
    };
    $out.textContent = JSON.stringify(summary, null, 2);
  }catch(err){ $out.textContent = 'Network error: ' + err; }
}

async function previewJson(){
  const items = parseItems($items.value);
  if (items.length !== 1){ $out.textContent = 'Provide exactly one Lot/Plan for JSON preview.'; return; }
  const lot = items[0];
  $out.textContent='Requesting JSON summary…';
  try{
    const url = `/export?lotplan=${encodeURIComponent(lot)}&max_px=${DEFAULT_MAX_PX}&download=false`;
    const res = await fetch(url); const txt = await res.text();
    try{ const data = JSON.parse(txt); $out.textContent = JSON.stringify(data, null, 2);}catch{ $out.textContent = `Error ${res.status}: ${txt}`; }
  }catch(err){ $out.textContent = 'Network error: ' + err; }
}

async function exportPropertyReport(){
  const items = parseItems($items.value);
  if (!items.length){ $out.textContent = 'Enter at least one Lot/Plan.'; return; }
  const body = {
    lotplans: items,
    simplify_tolerance: DEFAULT_SIMPLIFY,
  };
  const name = normText($name.value) || null;
  if (name){ body.filename = name; }
  const multi = items.length > 1;
  $out.textContent = multi ? `Exporting Property Report for ${items.length} items…` : 'Exporting Property Report…';
  try{
    const res = await fetch('/export/any', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
    const disp = res.headers.get('content-disposition') || '';
    const ok = res.ok;
    const blob = await res.blob();
    if (!ok){ const txt = await blob.text(); $out.textContent = `Error ${res.status}: ${txt}`; return; }
    const m = /filename="([^"]+)"/i.exec(disp);
    let dl = m ? m[1] : `property_report_${Date.now()}`;
    if (multi && name && !dl.startsWith(name)) dl = `${name}_${dl}`;
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = dl;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
    $out.textContent = 'Download complete.';
  }catch(err){ $out.textContent = 'Network error: ' + err; }
}

$items.addEventListener('input', updateMode);
$items.addEventListener('keyup', updateMode);
$items.addEventListener('change', updateMode);
$btnLoad.addEventListener('click', (e)=>{ e.preventDefault(); loadVector(); });
$btnJson.addEventListener('click', (e)=>{ e.preventDefault(); previewJson(); });
$btnExportReport.addEventListener('click', (e)=>{ e.preventDefault(); exportPropertyReport(); });
updateMode(); setTimeout(()=>{ ensureMap(); $items.focus(); }, 30);
</script>
</body></html>"""
    
    # Replace configuration placeholders with actual values
    return html_template.replace("%VEG_URL%", VEG_SERVICE_URL_DEFAULT).replace("%VEG_LAYER%", str(VEG_LAYER_ID_DEFAULT)).replace("%VEG_NAME%", VEG_NAME_FIELD_DEFAULT).replace("%VEG_CODE%", VEG_CODE_FIELD_DEFAULT or "")

@app.get("/health")
def health(): return {"ok": True}

@app.get("/export")
def export_geotiff(lotplan: str = Query(...), max_px: int = Query(4096, ge=256, le=8192), download: bool = Query(True)):
    lotplan = normalize_lotplan(lotplan)
    parcel_fc = fetch_parcel_geojson(lotplan)
    parcel_union = to_shapely_union(parcel_fc)
    env = bbox_3857(parcel_union)
    lt_fc = fetch_landtypes_intersecting_envelope(env)
    clipped = prepare_clipped_shapes(parcel_fc, lt_fc)
    if not clipped:
        if download: raise HTTPException(status_code=404, detail="No Land Types intersect this parcel.")
        return JSONResponse({"lotplan": lotplan, "error": "No Land Types intersect this parcel."}, status_code=404)
    tmpdir = tempfile.mkdtemp(prefix="tiff_")
    out_path = os.path.join(tmpdir, f"{lotplan}_landtypes.tif")
    result = make_geotiff_rgba(clipped, out_path, max_px=max_px)
    if download:
        data = open(out_path, "rb").read()
        os.remove(out_path); os.rmdir(tmpdir)
        return StreamingResponse(
            BytesIO(data),
            media_type="image/tiff",
            headers={"Content-Disposition": f'attachment; filename="{lotplan}_landtypes.tif"'},
        )
    else:
        public = {k:v for k,v in result.items() if k != "path"}
        legend: Dict[str, Dict[str, Any]] = {}
        for _g, code, name, area_ha in clipped:
            c = _hex(color_from_code(code))
            legend.setdefault(code, {"code":code,"name":name,"color_hex":c,"area_ha":0.0})
            legend[code]["area_ha"] += float(area_ha)
        return JSONResponse({"lotplan": lotplan, "legend": sorted(legend.values(), key=lambda d: (-d["area_ha"], d["code"])), **public})



@app.get("/vector")
def vector_geojson(lotplan: str = Query(...)):
    lotplan = normalize_lotplan(lotplan)
    parcel_fc = fetch_parcel_geojson(lotplan)
    parcel_union = to_shapely_union(parcel_fc)
    env = bbox_3857(parcel_union)
    lt_fc = fetch_landtypes_intersecting_envelope(env)
    clipped = prepare_clipped_shapes(parcel_fc, lt_fc)
    bore_fc = fetch_bores_intersecting_envelope(env)
    easement_fc = fetch_easements_intersecting_envelope(env)
    water_layers_raw = fetch_water_layers_intersecting_envelope(env)
    water_layers = _prepare_water_layers(parcel_fc, water_layers_raw, lotplan)

    for feature in parcel_fc.get("features", []):
        props = feature.get("properties") or {}
        if props.get("lotplan") == lotplan:
            continue
        new_props = dict(props)
        new_props["lotplan"] = lotplan
        feature["properties"] = new_props

    features: List[Dict[str, Any]] = []
    legend_map: Dict[str, Dict[str, Any]] = {}
    for geom4326, code, name, area_ha in clipped:
        color_hex = _hex(color_from_code(code))
        features.append(
            {
                "type": "Feature",
                "geometry": shp_mapping(geom4326),
                "properties": {
                    "code": code,
                    "name": name,
                    "area_ha": float(area_ha),
                    "color_hex": color_hex,
                    "lotplan": lotplan,
                },
            }
        )
        legend_map.setdefault(
            code,
            {"code": code, "name": name, "color_hex": color_hex, "area_ha": 0.0},
        )
        legend_map[code]["area_ha"] += float(area_ha)

    bore_features: List[Dict[str, Any]] = []
    seen_bores: Set[str] = set()
    for bore in bore_fc.get("features", []):
        try:
            geom = shp_shape(bore.get("geometry"))
        except Exception:
            continue
        if geom.is_empty:
            continue
        norm_props = _normalize_bore_properties(bore.get("properties") or {})
        if not norm_props:
            continue
        bore_number = norm_props.get("bore_number")
        if not bore_number or bore_number in seen_bores:
            continue
        seen_bores.add(bore_number)
        norm_props["lotplan"] = lotplan
        bore_features.append(
            {
                "type": "Feature",
                "geometry": shp_mapping(geom),
                "properties": norm_props,
            }
        )

    easement_features: List[Dict[str, Any]] = []
    for easement in easement_fc.get("features", []):
        try:
            geom = shp_shape(easement.get("geometry"))
        except Exception:
            continue
        if geom.is_empty:
            continue
        clipped_geom = _clip_to_parcel_union(geom, parcel_union)
        if clipped_geom is None or clipped_geom.is_empty:
            continue
        props = _normalize_easement_properties(easement.get("properties") or {}, lotplan)
        easement_features.append(
            {
                "type": "Feature",
                "geometry": shp_mapping(clipped_geom),
                "properties": props,
            }
        )

    water_layers_payload: List[Dict[str, Any]] = []
    total_water_features = 0
    for layer in water_layers:
        fc = layer.feature_collection
        features_list = list(fc.get("features", []))
        total_water_features += len(features_list)
        water_layers_payload.append(
            {
                "layer_id": layer.layer_id,
                "layer_title": layer.layer_title,
                "source_layer_name": layer.source_layer_name,
                "features": {"type": "FeatureCollection", "features": features_list},
            }
        )

    bounds_features: List[Dict[str, Any]] = []
    bounds_features.extend(parcel_fc.get("features", []))
    bounds_features.extend(features)
    bounds_features.extend(bore_features)
    bounds_features.extend(easement_features)
    for layer_entry in water_layers_payload:
        layer_features = layer_entry.get("features", {}).get("features", [])
        bounds_features.extend(layer_features)

    bounds_fc = {"type": "FeatureCollection", "features": bounds_features}
    bounds_geom = to_shapely_union(bounds_fc)
    bounds_dict = _bounds_dict_from_geom(bounds_geom, parcel_union)
    has_data = bool(features or bore_features or easement_features or total_water_features)
    status_code = 200 if has_data else 404
    payload = {
        "lotplan": lotplan,
        "parcel": parcel_fc,
        "landtypes": {"type": "FeatureCollection", "features": features},
        "bores": {"type": "FeatureCollection", "features": bore_features},
        "easements": {"type": "FeatureCollection", "features": easement_features},
        "water": {"layers": water_layers_payload},
        "legend": sorted(legend_map.values(), key=lambda d: (-d["area_ha"], d["code"])),
        "bounds4326": bounds_dict,
    }
    if status_code != 200:
        payload["error"] = "No Land Types intersect this parcel."
    return JSONResponse(payload, status_code=status_code)


class VectorBulkRequest(BaseModel):
    lotplans: List[str] = Field(..., min_length=1)


@app.post("/vector/bulk")
def vector_geojson_bulk(payload: VectorBulkRequest):
    seen = set()
    lotplans: List[str] = []
    for raw in payload.lotplans or []:
        code = (raw or "").strip()
        if not code:
            continue
        lp = normalize_lotplan(code)
        if lp in seen:
            continue
        seen.add(lp)
        lotplans.append(lp)

    if not lotplans:
        raise HTTPException(status_code=400, detail="No valid lot/plan codes provided.")

    parcel_features: List[Dict[str, Any]] = []
    landtype_features: List[Dict[str, Any]] = []
    bore_features: List[Dict[str, Any]] = []
    easement_features: List[Dict[str, Any]] = []
    legend_map: Dict[str, Dict[str, Any]] = {}
    bounds = None
    seen_bore_numbers: Set[str] = set()
    water_layers_map: Dict[int, Dict[str, Any]] = {}

    def expand_bounds(current, geom):
        if geom.is_empty:
            return current
        minx, miny, maxx, maxy = geom.bounds
        if current is None:
            return [minx, miny, maxx, maxy]
        current[0] = min(current[0], minx)
        current[1] = min(current[1], miny)
        current[2] = max(current[2], maxx)
        current[3] = max(current[3], maxy)
        return current

    for lotplan in lotplans:
        parcel_fc = fetch_parcel_geojson(lotplan)
        parcel_union = to_shapely_union(parcel_fc)
        env = bbox_3857(parcel_union)

        for feature in parcel_fc.get("features", []):
            try:
                geom = shp_shape(feature.get("geometry"))
            except Exception:
                continue
            if geom.is_empty:
                continue
            bounds = expand_bounds(bounds, geom)
            props = dict(feature.get("properties") or {})
            props["lotplan"] = lotplan
            parcel_features.append({
                "type": "Feature",
                "geometry": shp_mapping(geom),
                "properties": props,
            })

        lt_fc = fetch_landtypes_intersecting_envelope(env)
        clipped = prepare_clipped_shapes(parcel_fc, lt_fc)
        bore_fc = fetch_bores_intersecting_envelope(env)
        easement_fc = fetch_easements_intersecting_envelope(env)
        water_layers_raw = fetch_water_layers_intersecting_envelope(env)
        water_layers = _prepare_water_layers(parcel_fc, water_layers_raw, lotplan)

        for bore in bore_fc.get("features", []):
            try:
                geom = shp_shape(bore.get("geometry"))
            except Exception:
                continue
            if geom.is_empty:
                continue
            norm_props = _normalize_bore_properties(bore.get("properties") or {})
            if not norm_props:
                continue
            bore_number = norm_props.get("bore_number")
            if not bore_number or bore_number in seen_bore_numbers:
                continue
            seen_bore_numbers.add(bore_number)
            norm_props["lotplan"] = lotplan
            bore_features.append({
                "type": "Feature",
                "geometry": shp_mapping(geom),
                "properties": norm_props,
            })
            bounds = expand_bounds(bounds, geom)

        for easement in easement_fc.get("features", []):
            try:
                geom = shp_shape(easement.get("geometry"))
            except Exception:
                continue
            if geom.is_empty:
                continue
            clipped_geom = _clip_to_parcel_union(geom, parcel_union)
            if clipped_geom is None or clipped_geom.is_empty:
                continue
            props = _normalize_easement_properties(easement.get("properties") or {}, lotplan)
            easement_features.append(
                {
                    "type": "Feature",
                    "geometry": shp_mapping(clipped_geom),
                    "properties": props,
                }
            )
            bounds = expand_bounds(bounds, clipped_geom)

        for layer in water_layers:
            fc = layer.feature_collection
            features_list = list(fc.get("features", []))
            if not features_list:
                continue
            entry = water_layers_map.setdefault(
                layer.layer_id,
                {
                    "layer_id": layer.layer_id,
                    "layer_title": layer.layer_title,
                    "source_layer_name": layer.source_layer_name,
                    "features": [],
                },
            )
            entry["features"].extend(features_list)
            for feature in features_list:
                try:
                    geom = shp_shape(feature.get("geometry"))
                except Exception:
                    continue
                if geom.is_empty:
                    continue
                bounds = expand_bounds(bounds, geom)

        for geom4326, code, name, area_ha in clipped:
            if geom4326.is_empty:
                continue
            bounds = expand_bounds(bounds, geom4326)
            color_hex = _hex(color_from_code(code))
            landtype_features.append({
                "type": "Feature",
                "geometry": shp_mapping(geom4326),
                "properties": {
                    "code": code,
                    "name": name,
                    "area_ha": float(area_ha),
                    "color_hex": color_hex,
                    "lotplan": lotplan,
                },
            })
            legend_map.setdefault(code, {
                "code": code,
                "name": name,
                "color_hex": color_hex,
                "area_ha": 0.0,
            })
            legend_map[code]["area_ha"] += float(area_ha)

    if (
        not parcel_features
        and not landtype_features
        and not bore_features
        and not easement_features
        and not any(entry.get("features") for entry in water_layers_map.values())
    ):
        raise HTTPException(status_code=404, detail="No features found for the provided lots/plans.")

    bounds_dict = None
    if bounds is not None:
        west, south, east, north = bounds
        bounds_dict = {"west": west, "south": south, "east": east, "north": north}

    water_layers_payload = []
    for layer_id, entry in sorted(water_layers_map.items()):
        water_layers_payload.append(
            {
                "layer_id": entry["layer_id"],
                "layer_title": entry["layer_title"],
                "source_layer_name": entry["source_layer_name"],
                "features": {"type": "FeatureCollection", "features": list(entry.get("features", []))},
            }
        )

    return JSONResponse({
        "lotplans": lotplans,
        "parcels": {"type": "FeatureCollection", "features": parcel_features},
        "landtypes": {"type": "FeatureCollection", "features": landtype_features},
        "bores": {"type": "FeatureCollection", "features": bore_features},
        "easements": {"type": "FeatureCollection", "features": easement_features},
        "water": {"layers": water_layers_payload},
        "legend": sorted(legend_map.values(), key=lambda d: (-d["area_ha"], d["code"])),
        "bounds4326": bounds_dict,
    })

@app.get("/export_kmz")
def export_kmz(
    lotplan: str = Query(...),
    simplify_tolerance: float = Query(0.0, ge=0.0, le=0.001),
    veg_service_url: Optional[str] = Query(VEG_SERVICE_URL_DEFAULT, alias="veg_url"),
    veg_layer_id: Optional[int] = Query(VEG_LAYER_ID_DEFAULT, alias="veg_layer"),
    veg_name_field: Optional[str] = Query(VEG_NAME_FIELD_DEFAULT, alias="veg_name"),
    veg_code_field: Optional[str] = Query(VEG_CODE_FIELD_DEFAULT, alias="veg_code"),
):
    report = build_property_report_kmz(
        lotplan,
        simplify_tolerance=simplify_tolerance,
        veg_service_url=veg_service_url,
        veg_layer_id=veg_layer_id,
        veg_name_field=veg_name_field,
        veg_code_field=veg_code_field,
    )

    return StreamingResponse(
        BytesIO(report.kmz_bytes),
        media_type="application/vnd.google-earth.kmz",
        headers={"Content-Disposition": _content_disposition(report.filename)},
    )

@app.get("/export_kml")
def export_kml(
    lotplan: str = Query(...),
    simplify_tolerance: float = Query(0.0, ge=0.0, le=0.001),
    veg_service_url: Optional[str] = Query(VEG_SERVICE_URL_DEFAULT, alias="veg_url"),
    veg_layer_id: Optional[int] = Query(VEG_LAYER_ID_DEFAULT, alias="veg_layer"),
    veg_name_field: Optional[str] = Query(VEG_NAME_FIELD_DEFAULT, alias="veg_name"),
    veg_code_field: Optional[str] = Query(VEG_CODE_FIELD_DEFAULT, alias="veg_code"),
):
    lotplan = normalize_lotplan(lotplan)
    parcel_fc = fetch_parcel_geojson(lotplan)
    parcel_union = to_shapely_union(parcel_fc)
    env = bbox_3857(parcel_union)

    lt_fc = fetch_landtypes_intersecting_envelope(env)
    lt_clipped = prepare_clipped_shapes(parcel_fc, lt_fc)
    if not lt_clipped:
        raise HTTPException(status_code=404, detail="No Land Types intersect this parcel.")

    bore_fc = fetch_bores_intersecting_envelope(env)
    bore_points, bore_assets = _prepare_bore_placemarks(parcel_union, bore_fc)

    veg_clipped = []
    if veg_service_url and veg_layer_id is not None:
        veg_fc = fetch_features_intersecting_envelope(
            veg_service_url, veg_layer_id, env, out_fields="*"
        )
        # standardise fields
        for f in veg_fc.get("features", []):
            props = f.get("properties") or {}
            code = str(props.get(veg_code_field or "code") or props.get("code") or "").strip()
            name = str(props.get(veg_name_field or "name") or props.get("name") or code).strip()
            props["code"] = code or name or "UNK"
            # Format vegetation names as "Category *"
            category_name = name or code or "Unknown"
            props["name"] = f"Category {category_name}"
        veg_clipped = prepare_clipped_shapes(parcel_fc, veg_fc)

    if simplify_tolerance and simplify_tolerance > 0:
        def _simp(data):
            out = []
            for geom4326, code, name, area_ha in data:
                g2 = geom4326.simplify(simplify_tolerance, preserve_topology=True)
                if not g2.is_empty:
                    out.append((g2, code, name, area_ha))
            return out or data
        lt_clipped = _simp(lt_clipped)
        if veg_clipped:
            veg_clipped = _simp(veg_clipped)

    if bore_points:
        bore_points = _inline_point_icon_hrefs(bore_points, bore_assets)

    kml = _render_parcel_kml(lotplan, lt_clipped, veg_clipped, bore_points)

    filename = f"{lotplan}_landtypes" + ("_veg" if veg_clipped else "") + ".kml"
    return Response(
        content=kml,
        media_type="application/vnd.google-earth.kml+xml",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

class ExportAnyRequest(BaseModel):
    lotplan: Optional[str] = Field(None)
    lotplans: Optional[List[str]] = Field(None)
    filename: Optional[str] = Field(None)
    filename_prefix: Optional[str] = Field(None)
    simplify_tolerance: float = Field(0.0, ge=0.0, le=0.001)


def _prefixed_report_filename(lotplan: str, prefix: Optional[str]) -> str:
    base = f"Property Report – {lotplan}.kmz"
    if not prefix:
        return base
    clean = _sanitize_filename(prefix)
    if not clean:
        return base
    if clean.lower().endswith(".kmz"):
        clean = clean[:-4]
    return f"{clean} – {base}"


def _create_bulk_kmz(
    items: Sequence[str],
    *,
    simplify_tolerance: float,
    veg_service_url: Optional[str],
    veg_layer_id: Optional[int],
    veg_name_field: Optional[str],
    veg_code_field: Optional[str],
    filename: Optional[str] = None,
) -> StreamingResponse:
    nested_groups = []
    kmz_assets: Dict[str, bytes] = {}

    for lp in items:
        report = build_property_report_kmz(
            lp,
            simplify_tolerance=simplify_tolerance,
            veg_service_url=veg_service_url,
            veg_layer_id=veg_layer_id,
            veg_name_field=veg_name_field,
            veg_code_field=veg_code_field,
        )

        subgroups: List[tuple] = []
        if report.landtypes:
            subgroups.append((list(report.landtypes), color_from_code, "Land Types"))
        if report.vegetation:
            subgroups.append((list(report.vegetation), color_from_code, "Vegetation"))
        if report.easements:
            mapping = dict(report.easement_color_map)

            def _color_fn(code: str, _mapping=mapping) -> Tuple[int, int, int]:
                base = _mapping.get(code, code)
                return color_from_code(base)

            subgroups.append((list(report.easements), _color_fn, "Easements"))

        water_children: List[tuple] = []
        if report.bore_points:
            water_children.append(([], color_from_code, BORE_FOLDER_NAME, list(report.bore_points)))
        for layer in report.water_layers:
            def _water_color_fn(code: str, _layer_id=layer.layer_id) -> Tuple[int, int, int]:
                return color_from_code(f"WATER-{_layer_id}")

            water_children.append(
                (list(layer.shapes), _water_color_fn, layer.layer_title, list(layer.points))
            )

        if water_children:
            subgroups.append(([], color_from_code, "Water", [], water_children))

        nested_groups.append((report.lotplan, subgroups))

        for name, data in report.bore_assets.items():
            if name not in kmz_assets:
                kmz_assets[name] = data

    if not nested_groups:
        raise HTTPException(status_code=404, detail="No data found for the provided lots/plans.")

    doc_label = _sanitize_filename(filename) if filename else None
    if doc_label:
        if doc_label.lower().endswith(".kmz"):
            doc_label = doc_label[:-4]
        if "Property Report" not in doc_label:
            doc_label = f"Property Report – {doc_label}"
    else:
        doc_label = f"Property Report – {len(items)} lots"

    kml = build_kml_nested_folders(nested_groups, doc_name=doc_label)

    tmpdir = tempfile.mkdtemp(prefix="kmz_")
    download_name = doc_label
    out_path = os.path.join(tmpdir, f"{download_name}.kmz")
    try:
        write_kmz(kml, out_path, assets=kmz_assets)
        with open(out_path, "rb") as fh:
            kmz_bytes = fh.read()
    finally:
        try:
            if os.path.exists(out_path):
                os.remove(out_path)
        except Exception:
            pass
        try:
            os.rmdir(tmpdir)
        except Exception:
            pass

    return StreamingResponse(
        BytesIO(kmz_bytes),
        media_type="application/vnd.google-earth.kmz",
        headers={"Content-Disposition": _content_disposition(f"{download_name}.kmz")},
    )


def _create_property_report_zip(
    items: Sequence[str],
    *,
    simplify_tolerance: float,
    veg_service_url: Optional[str],
    veg_layer_id: Optional[int],
    veg_name_field: Optional[str],
    veg_code_field: Optional[str],
    filename_prefix: Optional[str] = None,
) -> StreamingResponse:
    reports: List[PropertyReportKMZ] = []
    for lp in items:
        report = build_property_report_kmz(
            lp,
            simplify_tolerance=simplify_tolerance,
            veg_service_url=veg_service_url,
            veg_layer_id=veg_layer_id,
            veg_name_field=veg_name_field,
            veg_code_field=veg_code_field,
        )
        reports.append(report)

    zip_buf = BytesIO()
    prefix_clean = _sanitize_filename(filename_prefix) if filename_prefix else None

    with zipfile.ZipFile(zip_buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for report in reports:
            entry_name = report.filename
            if prefix_clean:
                entry_name = _prefixed_report_filename(report.lotplan, prefix_clean)
            zf.writestr(entry_name, report.kmz_bytes)

    zip_buf.seek(0)
    stamp = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    base_name = prefix_clean or "Property Reports"
    zip_name = f"{base_name}_{stamp}.zip"

    return StreamingResponse(
        zip_buf,
        media_type="application/zip",
        headers={"Content-Disposition": _content_disposition(zip_name)},
    )


@app.post("/export/any")
def export_any(payload: ExportAnyRequest = Body(...)):
    items: List[str] = []
    if payload.lotplans:
        seen: Set[str] = set()
        for candidate in payload.lotplans:
            lp = normalize_lotplan(candidate)
            if not lp or lp in seen:
                continue
            seen.add(lp)
            items.append(lp)
    if payload.lotplan:
        lp_single = normalize_lotplan(payload.lotplan)
        if lp_single and lp_single not in items:
            items.append(lp_single)

    if not items:
        raise HTTPException(status_code=400, detail="Provide lotplan or lotplans.")

    simplify = payload.simplify_tolerance or 0.0
    veg_url, veg_layer, veg_name, veg_code = _default_veg_config()

    if len(items) == 1:
        report = build_property_report_kmz(
            items[0],
            simplify_tolerance=simplify,
            veg_service_url=veg_url,
            veg_layer_id=veg_layer,
            veg_name_field=veg_name,
            veg_code_field=veg_code,
        )
        prefix = _sanitize_filename(payload.filename) if payload.filename else None
        download_name = _prefixed_report_filename(report.lotplan, prefix)
        return StreamingResponse(
            BytesIO(report.kmz_bytes),
            media_type="application/vnd.google-earth.kmz",
            headers={"Content-Disposition": _content_disposition(download_name)},
        )

    if payload.filename_prefix:
        return _create_property_report_zip(
            items,
            simplify_tolerance=simplify,
            veg_service_url=veg_url,
            veg_layer_id=veg_layer,
            veg_name_field=veg_name,
            veg_code_field=veg_code,
            filename_prefix=payload.filename_prefix,
        )

    return _create_bulk_kmz(
        items,
        simplify_tolerance=simplify,
        veg_service_url=veg_url,
        veg_layer_id=veg_layer,
        veg_name_field=veg_name,
        veg_code_field=veg_code,
        filename=payload.filename,
    )
