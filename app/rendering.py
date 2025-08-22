from typing import Dict, Tuple, List
import numpy as np
import rasterio
from rasterio.transform import from_bounds
from rasterio.features import rasterize
from shapely.geometry import shape, mapping, Polygon, MultiPolygon
from shapely.ops import unary_union
from shapely import force_2d
from pyproj import Transformer

from .colors import color_from_code

def to_shapely_union(geojson_fc: Dict):
    geoms = []
    for f in geojson_fc["features"]:
        g = shape(f["geometry"])
        g = force_2d(g)
        geoms.append(g)
    return unary_union(geoms)

def bbox_3857(geom) -> Tuple[float, float, float, float]:
    minx, miny, maxx, maxy = geom.bounds
    return (minx, miny, maxx, maxy)

def _reproject_geom_generic(geom, src_epsg: int, dst_epsg: int):
    transformer = Transformer.from_crs(f"EPSG:{src_epsg}", f"EPSG:{dst_epsg}", always_xy=True)
    from shapely.ops import transform as shp_transform
    return shp_transform(lambda x, y, z=None: transformer.transform(x, y), geom)

def reproject_geom(geom, src_epsg: int, dst_epsg: int):
    """
    Explicit coordinate-list reprojection for Polygons/MultiPolygons
    to avoid any topology surprises; fallback to generic transform otherwise.
    """
    geom = force_2d(geom)
    gmap = mapping(geom)
    if gmap["type"] == "Polygon":
        transformer = Transformer.from_crs(f"EPSG:{src_epsg}", f"EPSG:{dst_epsg}", always_xy=True)
        rings = []
        for ring in gmap["coordinates"]:
            rings.append([transformer.transform(x, y) for (x, y) in ring])
        return Polygon(rings[0], holes=rings[1:]) if rings else None
    elif gmap["type"] == "MultiPolygon":
        transformer = Transformer.from_crs(f"EPSG:{src_epsg}", f"EPSG:{dst_epsg}", always_xy=True)
        polys = []
        for poly in gmap["coordinates"]:
            rings = []
            for ring in poly:
                rings.append([transformer.transform(x, y) for (x, y) in ring])
            polys.append(Polygon(rings[0], holes=rings[1:]))
        return MultiPolygon(polys)
    else:
        return _reproject_geom_generic(geom, src_epsg, dst_epsg)

def _pick(props: Dict, *keys: str, default=None):
    """Return the first present (and truthy) property among keys."""
    for k in keys:
        v = props.get(k)
        if v not in (None, ""):
            return v
    return default

def prepare_clipped_shapes(parcel_fc_3857: Dict, landtypes_fc_3857: Dict):
    """
    Returns a list of tuples:
      (geom_4326, code, name, area_ha)
    - Intersection computed in EPSG:3857 (for correct area), then reprojected to EPSG:4326.
    - Attribute lookup is case-insensitive (handles lt_code_1 vs LT_CODE_1).
    """
    parcel = to_shapely_union(parcel_fc_3857)
    if parcel.is_empty:
        return []

    results: List[Tuple[Polygon, str, str, float]] = []

    for feat in landtypes_fc_3857["features"]:
        props_raw = (feat.get("properties", {}) or {})
        # <<< key normalization: make a UPPER-CASE copy so lookups are case-insensitive
        props = { (k.upper() if isinstance(k, str) else k): v for k, v in props_raw.items() }

        def pick(*keys: str, default=None):
            for k in keys:
                v = props.get(k)
                if v not in (None, ""):
                    return v
            return default

        # Land Types commonly expose LT_CODE_1 / LT_NAME_1, but sometimes lower-case or alt names
        code = pick("LT_CODE_1", "LT_CODE", "LANDTYPE_CODE", "LTYPE_CODE", default="UNK")
        name = pick("LT_NAME_1", "LT_NAME", "LANDTYPE_NAME", "LTYPE_NAME", default="Unknown")

        g = force_2d(shape(feat["geometry"]))
        if not g.is_valid or g.is_empty:
            continue

        inter = g.intersection(parcel)
        if inter.is_empty:
            continue

        # Area in hectares (native 3857 units are meters)
        area_ha = float(inter.area / 10000.0)

        inter4326 = reproject_geom(inter, 3857, 4326)
        if inter4326 is None or inter4326.is_empty:
            continue

        results.append((inter4326, str(code), str(name), area_ha))

    return results

def choose_raster_size(bounds4326: Tuple[float, float, float, float], max_px: int = 4096) -> Tuple[int, int]:
    west, south, east, north = bounds4326
    width_deg = max(east - west, 1e-9)
    height_deg = max(north - south, 1e-9)
    aspect = width_deg / height_deg if height_deg != 0 else 1.0
    if aspect >= 1:
        width = max_px
        height = max(1, int(round(max_px / aspect)))
    else:
        height = max_px
        width = max(1, int(round(max_px * aspect)))
    return width, height

def make_geotiff_rgba(shapes_clipped_4326, out_path: str, max_px: int = 4096):
    """
    Rasterize clipped land type polygons to an RGBA GeoTIFF in EPSG:4326.
    Transparent background with per-code deterministic color.
    Returns summary dict {legend, bounds, path, width, height}.
    Legend is de-duplicated by code and includes summed area_ha.
    """
    if not shapes_clipped_4326:
        raise ValueError("No intersecting land type polygons found for this parcel.")

    # Overall bounds in 4326
    union = unary_union([g for (g, _code, _name, _ha) in shapes_clipped_4326])
    west, south, east, north = union.bounds
    width, height = choose_raster_size((west, south, east, north), max_px=max_px)
    transform = from_bounds(west, south, east, north, width, height)

    # Assign ids per unique code
    codes_order: List[str] = []
    for (_g, code, _name, _ha) in shapes_clipped_4326:
        if code not in codes_order:
            codes_order.append(code)
    code_to_id = {c: i + 1 for i, c in enumerate(codes_order)}  # 0 = background

    # Rasterize to class ids
    shapes = [(mapping(g), code_to_id[c]) for (g, c, _n, _ha) in shapes_clipped_4326]
    class_raster = rasterize(
        shapes=shapes,
        out_shape=(height, width),
        transform=transform,
        fill=0,
        dtype="uint16",
        all_touched=False,
    )

    # Build RGBA
    r = np.zeros((height, width), dtype=np.uint8)
    g = np.zeros((height, width), dtype=np.uint8)
    b = np.zeros((height, width), dtype=np.uint8)
    a = np.zeros((height, width), dtype=np.uint8)

    for code, idx in code_to_id.items():
        mask = class_raster == idx
        cr, cg, cb = color_from_code(code)
        r[mask] = cr
        g[mask] = cg
        b[mask] = cb
        a[mask] = 255

    profile = {
        "driver": "GTiff",
        "height": height,
        "width": width,
        "count": 4,
        "dtype": "uint8",
        "crs": "EPSG:4326",
        "transform": transform,
        "tiled": False,
        "compress": "deflate",
        "interleave": "pixel",
    }

    with rasterio.open(out_path, "w", **profile) as dst:
        dst.write(r, 1)
        dst.write(g, 2)
        dst.write(b, 3)
        dst.write(a, 4)

    # De-duplicate legend and sum area per code
    legend_map = {}
    for (_geom, code, name, area_ha) in shapes_clipped_4326:
        if code not in legend_map:
            legend_map[code] = {
                "code": code,
                "name": name,
                "color": color_from_code(code),
                "area_ha": 0.0,
            }
        legend_map[code]["area_ha"] += float(area_ha)

    # Stable order: by descending area, then code
    legend = sorted(legend_map.values(), key=lambda d: (-d["area_ha"], d["code"]))

    return {
        "legend": legend,
        "bounds4326": {"west": west, "south": south, "east": east, "north": north},
        "width": width,
        "height": height,
        "path": out_path,
    }
