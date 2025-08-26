# app/arcgis.py
"""
ArcGIS helpers for:
- Parcel lookup by Lot/Plan  → fetch_parcel_geojson(lotplan)
- Land Types by envelope     → fetch_landtypes_intersecting_envelope(env_3857)
- Generic by envelope        → fetch_features_intersecting_envelope(service_url, layer_id, env_3857, ...)

All functions return a GeoJSON FeatureCollection with geometries in EPSG:4326.

You can override defaults with environment variables:

PARCEL_SERVICE_URL        (e.g. DCDB/Parcels Feature/MapServer base)
PARCEL_LAYER_ID           (int)
PARCEL_LOTPLAN_FIELD      (e.g. LOTPLAN)
PARCEL_LOT_FIELD          (e.g. LOT)
PARCEL_PLAN_FIELD         (e.g. PLAN)

LANDTYPES_SERVICE_URL     (MapServer base for Land Types)
LANDTYPES_LAYER_ID        (int)
LANDTYPES_CODE_FIELD      (e.g. CODE, MAP_CODE)
LANDTYPES_NAME_FIELD      (e.g. NAME, MAP_NAME)
"""

from __future__ import annotations
import os, re, json, math
from typing import Dict, Any, Optional, Tuple, List
import requests

# ------------------------------
# Config (override via env vars)
# ------------------------------

# Parcels (DCDB) — leave these as your known-good defaults; override in deploy env if needed
PARCEL_SERVICE_URL = os.getenv("PARCEL_SERVICE_URL", "").strip()
PARCEL_LAYER_ID = int(os.getenv("PARCEL_LAYER_ID", "-1")) if os.getenv("PARCEL_LAYER_ID") else -1
PARCEL_LOTPLAN_FIELD = os.getenv("PARCEL_LOTPLAN_FIELD", "LOTPLAN").strip() or None
PARCEL_LOT_FIELD = os.getenv("PARCEL_LOT_FIELD", "").strip() or None
PARCEL_PLAN_FIELD = os.getenv("PARCEL_PLAN_FIELD", "").strip() or None

# Land Types
LANDTYPES_SERVICE_URL = os.getenv("LANDTYPES_SERVICE_URL", "").strip()
LANDTYPES_LAYER_ID = int(os.getenv("LANDTYPES_LAYER_ID", "-1")) if os.getenv("LANDTYPES_LAYER_ID") else -1
LANDTYPES_CODE_FIELD = os.getenv("LANDTYPES_CODE_FIELD", "CODE").strip() or "CODE"
LANDTYPES_NAME_FIELD = os.getenv("LANDTYPES_NAME_FIELD", "NAME").strip() or "NAME"

# HTTP defaults
DEFAULT_TIMEOUT = int(os.getenv("ARCGIS_TIMEOUT", "45"))
MAX_RECORDS = int(os.getenv("ARCGIS_MAX_RECORDS", "2000"))  # typical ArcGIS limit

# ------------------------------
# Low-level HTTP helpers
# ------------------------------

def _layer_query_url(service_url: str, layer_id: int) -> str:
    return f"{service_url.rstrip('/')}/{int(layer_id)}/query"

def _ensure_fc(obj: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(obj, dict) or obj.get("type") != "FeatureCollection":
        raise RuntimeError("ArcGIS did not return GeoJSON FeatureCollection")
    return obj

def _merge_fc(accum: Dict[str, Any], more: Dict[str, Any]) -> Dict[str, Any]:
    if not accum:
        return more
    accum.setdefault("features", [])
    more_feats = more.get("features", [])
    accum["features"].extend(more_feats)
    return accum

def _arcgis_geojson_query(
    service_url: str,
    layer_id: int,
    params: Dict[str, Any],
    timeout: int = DEFAULT_TIMEOUT,
    paginate: bool = True,
) -> Dict[str, Any]:
    """
    Perform an ArcGIS layer /query returning f=geojson, with optional pagination using resultOffset.
    """
    url = _layer_query_url(service_url, layer_id)
    base = {
        "f": "geojson",
        "returnGeometry": "true",
    }
    base.update(params or {})

    session = requests.Session()
    result_offset = int(base.pop("resultOffset", 0))
    result_record_count = int(base.pop("resultRecordCount", MAX_RECORDS))

    out_fc: Dict[str, Any] = {}
    while True:
        q = dict(base)
        q["resultOffset"] = result_offset
        q["resultRecordCount"] = result_record_count
        r = session.get(url, params=q, timeout=timeout)
        r.raise_for_status()

        # Some ArcGIS servers (rarely) return text; handle parse errors nicely
        try:
            fc = r.json()
        except ValueError as e:
            raise RuntimeError(f"Non-JSON response from ArcGIS: {e}")

        # GeoJSON FeatureCollection expected
        _ensure_fc(fc)
        out_fc = _merge_fc(out_fc, fc)

        exceeded = False
        # ArcGIS sometimes includes exceededTransferLimit flags on JSON (not always on GeoJSON);
        # we rely on result counts: stop when returned fewer than requested.
        features = fc.get("features", [])
        if paginate and len(features) >= result_record_count:
            result_offset += result_record_count
        else:
            break

    if not out_fc:
        out_fc = {"type": "FeatureCollection", "features": []}
    return out_fc

# ------------------------------
# Utilities
# ------------------------------

_LOTPLAN_RE = re.compile(r"^\s*(\d+)\s*([A-Z]+[A-Z0-9]+)\s*$", re.IGNORECASE)

def _parse_lotplan(lp: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Splits '13SP181800' -> ('13', 'SP181800')
    Returns (lot, plan) or (None, None) if not parseable.
    """
    if not lp:
        return None, None
    m = _LOTPLAN_RE.match(lp.strip().upper())
    if not m:
        return None, None
    return m.group(1), m.group(2)

def _build_envelope_3857(env_3857) -> Dict[str, Any]:
    xmin, ymin, xmax, ymax = env_3857
    return {
        "xmin": float(xmin), "ymin": float(ymin),
        "xmax": float(xmax), "ymax": float(ymax),
        "spatialReference": {"wkid": 3857},
    }

def _standardise_code_name(fc: Dict[str, Any], code_field: str, name_field: str) -> Dict[str, Any]:
    feats = fc.get("features", [])
    out_feats = []
    for f in feats:
        props = f.get("properties") or {}
        code = str(props.get(code_field, "")).strip()
        name = str(props.get(name_field, "")).strip()
        # Make sure code/name exist; fallbacks
        if not code and name:
            code = name
        if not name and code:
            name = code
        # Preserve originals but ensure 'code' and 'name' are present
        props = dict(props)
        props["code"] = code or "UNK"
        props["name"] = name or (code or "Unknown")
        out_feats.append({
            "type": "Feature",
            "geometry": f.get("geometry"),
            "properties": props,
        })
    return {"type": "FeatureCollection", "features": out_feats}

# ------------------------------
# Public API
# ------------------------------

def fetch_parcel_geojson(lotplan: str, *, timeout: int = DEFAULT_TIMEOUT) -> Dict[str, Any]:
    """
    Look up a parcel by Lot/Plan and return its geometry as GeoJSON FeatureCollection (EPSG:4326).
    Tries:
      1) Combined field (e.g. LOTPLAN == '13SP181800')
      2) Split fields (e.g. LOT == '13' AND PLAN == 'SP181800')
    """
    lp = (lotplan or "").strip().upper()
    if not lp:
        raise ValueError("lotplan is required")

    if not PARCEL_SERVICE_URL or PARCEL_LAYER_ID < 0:
        raise RuntimeError("Parcel service not configured. Set PARCEL_SERVICE_URL and PARCEL_LAYER_ID.")

    where = None
    params_common = {
        "outFields": "*",
        "outSR": 4326,
    }

    # 1) Combined LOTPLAN field
    if PARCEL_LOTPLAN_FIELD:
        where = f"UPPER({PARCEL_LOTPLAN_FIELD})='{lp}'"
        fc = _arcgis_geojson_query(
            PARCEL_SERVICE_URL, PARCEL_LAYER_ID,
            params=dict(params_common, where=where),
            timeout=timeout,
            paginate=False,
        )
        if fc.get("features"):
            return fc

    # 2) Split LOT + PLAN fields
    if PARCEL_LOT_FIELD and PARCEL_PLAN_FIELD:
        lot, plan = _parse_lotplan(lp)
        if lot and plan:
            where = f"UPPER({PARCEL_LOT_FIELD})='{lot}' AND UPPER({PARCEL_PLAN_FIELD})='{plan}'"
            fc = _arcgis_geojson_query(
                PARCEL_SERVICE_URL, PARCEL_LAYER_ID,
                params=dict(params_common, where=where),
                timeout=timeout,
                paginate=False,
            )
            if fc.get("features"):
                return fc

    # Nothing found -> empty FC (caller may handle 404)
    return {"type": "FeatureCollection", "features": []}

def fetch_landtypes_intersecting_envelope(
    env_3857: Tuple[float, float, float, float],
    *,
    timeout: int = DEFAULT_TIMEOUT,
) -> Dict[str, Any]:
    """
    Query Land Types layer for features intersecting the 3857 envelope. Returns FeatureCollection (EPSG:4326)
    with properties guaranteed to include 'code' and 'name'.
    """
    if not LANDTYPES_SERVICE_URL or LANDTYPES_LAYER_ID < 0:
        raise RuntimeError("Land Types service not configured. Set LANDTYPES_SERVICE_URL and LANDTYPES_LAYER_ID.")

    geometry = _build_envelope_3857(env_3857)
    params = {
        "where": "1=1",
        "geometry": json.dumps(geometry),
        "geometryType": "esriGeometryEnvelope",
        "inSR": 3857,
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "*",
        "outSR": 4326,
    }
    fc = _arcgis_geojson_query(
        LANDTYPES_SERVICE_URL, LANDTYPES_LAYER_ID, params=params, timeout=timeout, paginate=True
    )
    # Make sure properties expose 'code' and 'name'
    fc_std = _standardise_code_name(fc, LANDTYPES_CODE_FIELD, LANDTYPES_NAME_FIELD)
    return fc_std

def fetch_features_intersecting_envelope(
    service_url: str,
    layer_id: int,
    env_3857: Tuple[float, float, float, float],
    *,
    out_sr: int = 4326,
    out_fields: str = "*",
    where: str = "1=1",
    timeout: int = DEFAULT_TIMEOUT,
) -> Dict[str, Any]:
    """
    Generic envelope query for any MapServer/FeatureServer layer.
    Returns GeoJSON FeatureCollection with geometries in out_sr (default 4326).
    """
    if not service_url or layer_id is None:
        raise ValueError("service_url and layer_id are required")

    geometry = _build_envelope_3857(env_3857)
    params = {
        "where": where or "1=1",
        "geometry": json.dumps(geometry),
        "geometryType": "esriGeometryEnvelope",
        "inSR": 3857,
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": out_fields or "*",
        "outSR": out_sr,
    }
    fc = _arcgis_geojson_query(service_url, int(layer_id), params=params, timeout=timeout, paginate=True)
    return fc
