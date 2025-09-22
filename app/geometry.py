# app/geometry.py
from __future__ import annotations

from typing import Any, Dict, List, Tuple, cast

from pyproj import Transformer
from shapely.geometry import GeometryCollection, shape
from shapely.ops import transform as shp_transform
from shapely.ops import unary_union
from shapely.validation import make_valid


def to_shapely_union(fc: Dict[str, Any]):
    geoms: List[Any] = []
    for f in (fc or {}).get("features", []):
        try:
            g = shape(f.get("geometry"))
            if not g.is_empty:
                geoms.append(g)
        except Exception:
            continue
    if not geoms: return GeometryCollection()
    try:
        return unary_union(geoms)
    except Exception:
        geoms2 = [make_valid(g) for g in geoms]
        return unary_union(geoms2)

def bbox_3857(geom4326) -> Tuple[float,float,float,float]:
    if geom4326.is_empty:
        return (0,0,0,0)
    minx, miny, maxx, maxy = geom4326.bounds
    tr = Transformer.from_crs(4326, 3857, always_xy=True)
    x1, y1 = tr.transform(minx, miny)
    x2, y2 = tr.transform(maxx, maxy)
    xmin, xmax = sorted((x1, x2))
    ymin, ymax = sorted((y1, y2))
    return (xmin, ymin, xmax, ymax)

def shapely_transform(geom, transformer: Transformer):
    return shp_transform(lambda x, y, z=None: transformer.transform(x, y), geom)

def _area_ha(geom4326) -> float:
    # Use equal-area CRS for area
    tr = Transformer.from_crs(4326, 6933, always_xy=True)
    try:
        g_eq = shapely_transform(geom4326, tr)
        return abs(g_eq.area) / 10000.0
    except Exception:
        tr2 = Transformer.from_crs(4326, 3857, always_xy=True)
        g2 = shapely_transform(geom4326, tr2)
        return abs(g2.area) / 10000.0

def prepare_clipped_shapes(parcel_fc: Dict[str, Any], thematic_fc: Dict[str, Any]) -> List[tuple]:
    parcel_u = to_shapely_union(parcel_fc)
    if parcel_u.is_empty: return []
    out: List[tuple] = []
    for f in (thematic_fc or {}).get("features", []):
        props = f.get("properties") or {}
        code = str(props.get("code") or props.get("CODE") or props.get("MAP_CODE") or props.get("CLASS_CODE") or props.get("lt_code_1") or "UNK")
        name = str(props.get("name") or props.get("NAME") or props.get("MAP_NAME") or props.get("CLASS_NAME") or props.get("lt_name_1") or code)
        try:
            g = shape(f.get("geometry"))
        except Exception:
            continue
        if g.is_empty: continue
        try:
            inter = parcel_u.intersection(g)
        except Exception:
            try:
                inter = parcel_u.intersection(make_valid(g))
            except Exception:
                continue
        if inter.is_empty: continue
        out.append((inter, code, name, float(_area_ha(inter))))
    # dissolve by code+name
    aggregated: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for g, c, n, a in out:
        key = (c, n)
        entry = aggregated.setdefault(key, {"geom": None, "area": 0.0})
        geom_existing = entry.get("geom")
        if geom_existing is None:
            entry["geom"] = g
        else:
            entry["geom"] = geom_existing.union(g)
        entry["area"] = float(entry.get("area", 0.0)) + float(a)

    final = []
    for (code, name), entry in aggregated.items():
        geom_obj = entry.get("geom")
        if geom_obj is None or getattr(geom_obj, "is_empty", False):
            continue
        final.append((geom_obj, code, name, float(entry.get("area", 0.0))))
    return final

def merge_clipped_shapes_across_lots(all_clipped_data: List[List[tuple]]) -> List[tuple]:
    """Merge clipped shapes from multiple lots by code+name, creating single polygons where possible."""
    if not all_clipped_data:
        return []
    
    # Collect all shapes by (code, name) key
    by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for clipped_data in all_clipped_data:
        for geom, code, name, area_ha in clipped_data:
            key = (code, name)
            entry = by_key.setdefault(key, {"geoms": [], "total_area": 0.0})
            geoms_list = cast(List[Any], entry.setdefault("geoms", []))
            geoms_list.append(geom)
            entry["total_area"] = float(entry.get("total_area", 0.0)) + float(area_ha)
    
    # Merge geometries for each key
    merged: List[tuple] = []
    for (code, name), data in by_key.items():
        geoms = cast(List[Any], data.get("geoms", []))
        total_area = float(data.get("total_area", 0.0))
        
        if not geoms:
            continue
            
        try:
            # Use unary_union to merge all geometries with the same code+name
            merged_geom = unary_union(geoms)
            if merged_geom.is_empty:
                continue
            
            # If it's a MultiPolygon with only one polygon, convert to Polygon
            if hasattr(merged_geom, 'geom_type') and merged_geom.geom_type == 'MultiPolygon':
                if len(merged_geom.geoms) == 1:
                    merged_geom = merged_geom.geoms[0]
            
            merged.append((merged_geom, code, name, total_area))
            
        except Exception:
            # If merging fails, use the first geometry as fallback
            if geoms:
                merged.append((geoms[0], code, name, total_area))
    
    return merged
