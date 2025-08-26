# app/geometry.py
from __future__ import annotations
from typing import Dict, Any, Tuple, List
from shapely.geometry import shape, mapping, GeometryCollection
from shapely.ops import unary_union
from shapely.validation import make_valid
from pyproj import Transformer
from shapely.ops import transform as shp_transform

def to_shapely_union(fc: Dict[str, Any]):
    geoms = []
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
    out = []
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
        area_ha = _area_ha(inter)
        out.append((inter, code, name, float(area_ha)))
    # dissolve by code+name
    by_key = {}
    for g, c, n, a in out:
        key = (c, n)
        if key not in by_key:
            by_key[key] = [g, a]
        else:
            by_key[key][0] = by_key[key][0].union(g)
            by_key[key][1] += a
    final = [(geom, c, n, by_key[(c,n)][1]) for (c, n), (geom, _) in by_key.items() if not geom.is_empty]
    return final
