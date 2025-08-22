import json
import logging
from typing import Dict, Any, Tuple

import requests

log = logging.getLogger(__name__)

# ArcGIS REST endpoints (QLD Gov)
CADASTRE_LAYER = "https://spatial-gis.information.qld.gov.au/arcgis/rest/services/PlanningCadastre/LandParcelPropertyFramework/MapServer/4"
LANDTYPES_LAYER = "https://spatial-gis.information.qld.gov.au/arcgis/rest/services/Environment/LandTypes/MapServer/1"

# We query/operate in EPSG:3857 (service native), then reproject to EPSG:4326 for the GeoTIFF.
SR_3857 = {"wkid": 102100}
SR_4326 = {"wkid": 4326}

def _get(url: str, params: Dict[str, Any], timeout: int = 30) -> Dict[str, Any]:
    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def fetch_parcel_geojson(lotplan: str) -> Dict[str, Any]:
    """Fetch a single parcel polygon by lotplan from the DCDB MapServer/4.
    Returns GeoJSON FeatureCollection in EPSG:3857."""
    params = {
        "f": "geojson",
        "where": f"UPPER(lotplan)=UPPER('{lotplan}')",
        "outFields": "*",
        "returnGeometry": "true",
        "outSR": "102100",
    }
    data = _get(CADASTRE_LAYER + "/query", params)
    if "features" not in data or len(data["features"]) == 0:
        raise ValueError(f"No parcel found for Lot/Plan '{lotplan}'")
    return data

def fetch_landtypes_intersecting_envelope(envelope_3857: Tuple[float, float, float, float]) -> Dict[str, Any]:
    """Query Land Types by envelope to avoid heavy polygon geometry param encoding.
    Returns GeoJSON FeatureCollection (EPSG:3857)."""
    xmin, ymin, xmax, ymax = envelope_3857
    geometry = {
        "xmin": xmin, "ymin": ymin, "xmax": xmax, "ymax": ymax,
        "spatialReference": SR_3857
    }
    params = {
        "f": "geojson",
        "geometry": json.dumps(geometry),
        "geometryType": "esriGeometryEnvelope",
        "inSR": "102100",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "LT_CODE_1,LT_NAME_1,PERCENT1,AREA_HA",
        "returnGeometry": "true",
        "outSR": "102100"
    }
    data = _get(LANDTYPES_LAYER + "/query", params)
    if "features" not in data:
        raise RuntimeError("Unexpected Land Types query response.")
    return data
