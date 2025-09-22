# app/arcgis.py
from __future__ import annotations

import json
import re
from typing import Any, Dict, Iterable, List

import requests

from .config import (
    ARCGIS_MAX_RECORDS,
    ARCGIS_TIMEOUT,
    BORE_DRILL_DATE_FIELD,
    BORE_LAYER_ID,
    BORE_NUMBER_FIELD,
    BORE_REPORT_URL_FIELD,
    BORE_SERVICE_URL,
    BORE_STATUS_CODE_FIELD,
    BORE_STATUS_LABEL_FIELD,
    BORE_TYPE_CODE_FIELD,
    BORE_TYPE_LABEL_FIELD,
    EASEMENT_AREA_FIELD,
    EASEMENT_FEATURE_NAME_FIELD,
    EASEMENT_LAYER_ID,
    EASEMENT_LOTPLAN_FIELD,
    EASEMENT_PARCEL_TYPE_FIELD,
    EASEMENT_SERVICE_URL,
    EASEMENT_TENURE_FIELD,
    LANDTYPES_CODE_FIELD,
    LANDTYPES_LAYER_ID,
    LANDTYPES_NAME_FIELD,
    LANDTYPES_SERVICE_URL,
    PARCEL_LAYER_ID,
    PARCEL_LOT_FIELD,
    PARCEL_LOTPLAN_FIELD,
    PARCEL_PLAN_FIELD,
    PARCEL_SERVICE_URL,
    WATER_LAYER_CONFIG,
    WATER_LAYER_IDS,
    WATER_LAYER_TITLES,
    WATER_SERVICE_URL,
)
from .bores import (
    get_bore_icon,
    make_bore_icon_key,
    normalize_bore_drill_date,
    normalize_bore_number,
)


def _layer_query_url(service_url: str, layer_id: int) -> str:
    return f"{service_url.rstrip('/')}/{int(layer_id)}/query"

def _ensure_fc(obj: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(obj, dict) or obj.get("type") != "FeatureCollection":
        raise RuntimeError("ArcGIS did not return GeoJSON FeatureCollection")
    obj.setdefault("features", [])
    return obj

def _merge_fc(accum: Dict[str, Any], more: Dict[str, Any]) -> Dict[str, Any]:
    if not accum:
        return more
    accum.setdefault("features", [])
    accum["features"].extend(more.get("features", []))
    return accum

def _arcgis_geojson_query(service_url: str, layer_id: int, params: Dict[str, Any], paginate: bool = True) -> Dict[str, Any]:
    url = _layer_query_url(service_url, layer_id)
    base: Dict[str, Any] = {"f": "geojson", "returnGeometry": "true"}
    base.update(params or {})
    result_offset: int = int(base.pop("resultOffset", 0))
    result_record_count: int = int(base.pop("resultRecordCount", ARCGIS_MAX_RECORDS))

    sess = requests.Session()
    out_fc: Dict[str, Any] = {}
    while True:
        q = dict(base)
        q["resultOffset"] = result_offset
        q["resultRecordCount"] = result_record_count
        r = sess.get(url, params=q, timeout=ARCGIS_TIMEOUT)
        r.raise_for_status()
        fc = r.json()
        _ensure_fc(fc)
        out_fc = _merge_fc(out_fc, fc)
        feats = fc.get("features", [])
        if paginate and len(feats) >= result_record_count:
            result_offset += result_record_count
        else:
            break
    if not out_fc:
        out_fc = {"type": "FeatureCollection", "features": []}
    return out_fc

_LOTPLAN_RE = re.compile(r"^\s*(?:LOT\s*)?(\d+)\s*(?:PLAN\s*)?([A-Z]+[A-Z0-9]+)\s*$", re.IGNORECASE)


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _parse_lotplan(lp: str):
    if not lp:
        return None, None
    m = _LOTPLAN_RE.match((lp or "").strip().upper())
    if not m:
        return None, None
    return m.group(1), m.group(2)

def normalize_lotplan(lp: str) -> str:
    """Return canonical LOT+PLAN string (e.g. '13SP181800')."""
    lot, plan = _parse_lotplan(lp)
    if lot and plan:
        return f"{lot}{plan}"
    return (lp or "").strip().upper()

def fetch_parcel_geojson(lotplan: str) -> Dict[str, Any]:
    lp = normalize_lotplan(lotplan)
    if not lp:
        return {"type":"FeatureCollection","features":[]}
    if not PARCEL_SERVICE_URL or PARCEL_LAYER_ID < 0:
        raise RuntimeError("Parcel service not configured.")
    common = {"outFields":"*","outSR":4326}

    # Combined LOTPLAN field first
    if PARCEL_LOTPLAN_FIELD:
        where = f"UPPER({PARCEL_LOTPLAN_FIELD})='{lp}'"
        fc = _arcgis_geojson_query(PARCEL_SERVICE_URL, PARCEL_LAYER_ID, dict(common, where=where), paginate=False)
        if fc.get("features"): return fc

    # Split LOT + PLAN fallback
    if PARCEL_LOT_FIELD and PARCEL_PLAN_FIELD:
        lot, plan = _parse_lotplan(lp)
        if lot and plan:
            where = f"UPPER({PARCEL_LOT_FIELD})='{lot}' AND UPPER({PARCEL_PLAN_FIELD})='{plan}'"
            fc = _arcgis_geojson_query(PARCEL_SERVICE_URL, PARCEL_LAYER_ID, dict(common, where=where), paginate=False)
            if fc.get("features"): return fc

    return {"type":"FeatureCollection","features":[]}

def _standardise_code_name(fc: Dict[str, Any], code_field: str, name_field: str) -> Dict[str, Any]:
    feats = fc.get("features", [])
    out = []
    for f in feats:
        p = f.get("properties") or {}
        code = str(p.get(code_field, "")).strip()
        name = str(p.get(name_field, "")).strip()
        if not code and name: code = name
        if not name and code: name = code
        p["code"] = code or "UNK"
        p["name"] = name or (code or "Unknown")
        out.append({"type":"Feature","geometry":f.get("geometry"),"properties":p})
    return {"type":"FeatureCollection","features":out}

def fetch_landtypes_intersecting_envelope(env_3857) -> Dict[str, Any]:
    if not LANDTYPES_SERVICE_URL or LANDTYPES_LAYER_ID < 0:
        raise RuntimeError("Land Types service not configured.")
    xmin, ymin, xmax, ymax = env_3857
    geometry = {"xmin": float(xmin),"ymin": float(ymin), "xmax": float(xmax),"ymax": float(ymax), "spatialReference":{"wkid":3857}}
    params = {
        "where": "1=1",
        "geometry": json.dumps(geometry),
        "geometryType": "esriGeometryEnvelope",
        "inSR": 3857,
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "*",
        "outSR": 4326,
    }
    fc = _arcgis_geojson_query(LANDTYPES_SERVICE_URL, LANDTYPES_LAYER_ID, params, paginate=True)
    return _standardise_code_name(fc, LANDTYPES_CODE_FIELD, LANDTYPES_NAME_FIELD)

def fetch_features_intersecting_envelope(service_url: str, layer_id: int, env_3857, out_sr: int = 4326, out_fields: str = "*", where: str = "1=1") -> Dict[str, Any]:
    xmin, ymin, xmax, ymax = env_3857
    geometry = {"xmin": float(xmin),"ymin": float(ymin), "xmax": float(xmax),"ymax": float(ymax), "spatialReference":{"wkid":3857}}
    params = {
        "where": where or "1=1",
        "geometry": json.dumps(geometry),
        "geometryType": "esriGeometryEnvelope",
        "inSR": 3857,
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": out_fields or "*",
        "outSR": out_sr,
    }
    return _arcgis_geojson_query(service_url, int(layer_id), params, paginate=True)


def _join_fields(fields: Iterable[str]) -> str:
    out: List[str] = []
    seen = set()
    for field in fields:
        if not field:
            continue
        key = field.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return ",".join(out)


def fetch_easements_intersecting_envelope(env_3857) -> Dict[str, Any]:
    if not EASEMENT_SERVICE_URL or EASEMENT_LAYER_ID < 0:
        raise RuntimeError("Easement service not configured.")

    out_fields = _join_fields(
        [
            EASEMENT_LOTPLAN_FIELD,
            EASEMENT_PARCEL_TYPE_FIELD,
            EASEMENT_FEATURE_NAME_FIELD,
            EASEMENT_TENURE_FIELD,
            EASEMENT_AREA_FIELD,
        ]
    )

    return fetch_features_intersecting_envelope(
        EASEMENT_SERVICE_URL,
        EASEMENT_LAYER_ID,
        env_3857,
        out_sr=4326,
        out_fields=out_fields or "*",
    )


def fetch_bores_intersecting_envelope(env_3857) -> Dict[str, Any]:
    """Fetch bore features intersecting an envelope in EPSG:3857."""

    if not BORE_SERVICE_URL or BORE_LAYER_ID < 0:
        raise RuntimeError("Bore service not configured.")

    out_fields = _join_fields(
        [
            BORE_NUMBER_FIELD,
            BORE_STATUS_LABEL_FIELD,
            BORE_STATUS_CODE_FIELD,
            BORE_TYPE_LABEL_FIELD,
            BORE_TYPE_CODE_FIELD,
            BORE_DRILL_DATE_FIELD,
            BORE_REPORT_URL_FIELD,
        ]
    )

    fc = fetch_features_intersecting_envelope(
        BORE_SERVICE_URL,
        BORE_LAYER_ID,
        env_3857,
        out_sr=4326,
        out_fields=out_fields,
    )

    features_out: List[Dict[str, Any]] = []
    for feat in fc.get("features", []):
        props = feat.get("properties") or {}
        raw_number = props.get(BORE_NUMBER_FIELD)
        bore_number = normalize_bore_number(raw_number)
        if not bore_number:
            continue

        status_code = _clean_text(props.get(BORE_STATUS_CODE_FIELD))
        type_code = _clean_text(props.get(BORE_TYPE_CODE_FIELD))
        status_label = _clean_text(props.get(BORE_STATUS_LABEL_FIELD))
        type_label = _clean_text(props.get(BORE_TYPE_LABEL_FIELD))
        drilled_date = normalize_bore_drill_date(props.get(BORE_DRILL_DATE_FIELD))
        report_url = _clean_text(props.get(BORE_REPORT_URL_FIELD))

        icon = get_bore_icon(status_code, type_code)
        icon_key = icon.key if icon and icon.key else make_bore_icon_key(status_code, type_code)

        features_out.append(
            {
                "type": "Feature",
                "geometry": feat.get("geometry"),
                "properties": {
                    "bore_number": bore_number,
                    "status_label": status_label or None,
                    "type_label": type_label or None,
                    "status": status_code or None,
                    "type": type_code or None,
                    "drilled_date": drilled_date,
                    "report_url": report_url or None,
                    "icon_key": icon_key,
                },
            }
        )

    return {"type": "FeatureCollection", "features": features_out}


def _water_feature_code(layer_id: int, props: Dict[str, Any], index: int) -> str:
    primary = (
        props.get("code")
        or props.get("pfi")
        or props.get("ufi")
        or props.get("watercourse_id")
        or props.get("water_id")
        or props.get("objectid")
        or props.get("OBJECTID")
        or props.get("fid")
        or props.get("FID")
        or props.get("id")
    )
    code = _clean_text(primary)
    if not code:
        code = str(index)
    return f"W{layer_id}-{code}"


def fetch_water_layers_intersecting_envelope(env_3857):
    """Fetch configured surface-water layers intersecting the parcel envelope."""

    if not WATER_SERVICE_URL or not WATER_LAYER_IDS:
        return []

    results = []
    for layer_id in WATER_LAYER_IDS:
        meta = WATER_LAYER_CONFIG.get(layer_id, {})
        try:
            fc = fetch_features_intersecting_envelope(
                WATER_SERVICE_URL,
                layer_id,
                env_3857,
                out_sr=4326,
                out_fields="*",
            )
        except Exception:
            continue

        features_out: List[Dict[str, Any]] = []
        for idx, feature in enumerate(fc.get("features", []), start=1):
            geometry = feature.get("geometry")
            if not geometry:
                continue
            props_raw = feature.get("properties") or {}
            props = dict(props_raw)

            code = _water_feature_code(layer_id, props, idx)
            name = _clean_text(
                props.get("name")
                or props.get("feature_name")
                or props.get("watercourse_name")
                or props.get("feature_type")
                or props.get("type")
                or props.get("perenniality")
            )

            layer_title = meta.get("title") or WATER_LAYER_TITLES.get(layer_id) or f"Layer {layer_id}"
            if not name:
                name = layer_title

            reference_id = _clean_text(
                props.get("pfi")
                or props.get("ufi")
                or props.get("watercourse_id")
                or props.get("water_id")
                or props.get("objectid")
                or props.get("OBJECTID")
                or props.get("fid")
                or props.get("FID")
            )

            props.update(
                {
                    "code": code,
                    "name": name,
                    "layer_id": layer_id,
                    "layer_title": layer_title,
                    "source_layer_name": meta.get("service_name") or layer_title,
                }
            )
            if reference_id and reference_id != code:
                props.setdefault("reference_id", reference_id)

            features_out.append(
                {
                    "type": "Feature",
                    "geometry": geometry,
                    "properties": props,
                }
            )

        if not features_out:
            continue

        results.append(
            {
                "layer_id": layer_id,
                "layer_title": meta.get("title") or WATER_LAYER_TITLES.get(layer_id) or f"Layer {layer_id}",
                "source_layer_name": meta.get("service_name") or WATER_LAYER_TITLES.get(layer_id) or f"Layer {layer_id}",
                "geometry_type": meta.get("geometry_type"),
                "feature_collection": {"type": "FeatureCollection", "features": features_out},
            }
        )

    return results
