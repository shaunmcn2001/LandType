# app/arcgis.py
from __future__ import annotations

import json
import re
from typing import Any, Dict

import requests

from .config import (
    ARCGIS_MAX_RECORDS,
    ARCGIS_TIMEOUT,
    LANDTYPES_CODE_FIELD,
    LANDTYPES_LAYER_ID,
    LANDTYPES_NAME_FIELD,
    LANDTYPES_SERVICE_URL,
    PARCEL_LAYER_ID,
    PARCEL_LOT_FIELD,
    PARCEL_LOTPLAN_FIELD,
    PARCEL_PLAN_FIELD,
    PARCEL_SERVICE_URL,
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
    base = {"f": "geojson", "returnGeometry": "true"}
    base.update(params or {})
    result_offset = int(base.pop("resultOffset", 0))
    result_record_count = int(base.pop("resultRecordCount", ARCGIS_MAX_RECORDS))

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
