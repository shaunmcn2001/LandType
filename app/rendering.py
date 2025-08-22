from typing import List, Dict, Tuple
import numpy as np
import rasterio
from rasterio.transform import from_bounds
from rasterio.features import rasterize
from shapely.geometry import shape, mapping
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

def reproject_geom(geom, src_epsg: int, dst_epsg: int):
    transformer = Transformer.from_crs(f"EPSG:{src_epsg}", f"EPSG:{dst_epsg}", always_xy=True)
    def _reproj_coords(coords):
        for x, y in coords:
            yield transformer.transform(x, y)
    geom_m = mapping(geom)
    if geom_m["type"] == "Polygon":
        rings = []
        for ring in geom_m["coordinates"]:
            rings.append(list(_reproj_coords(ring)))
        from shapely.geometry import Polygon
        return Polygon(rings[0], holes=rings[1:]) if len(rings) > 0 else None
    elif geom_m["type"] == "MultiPolygon":
        polys = []
        for poly in geom_m["coordinates"]:
            rings = []
            for ring in poly:
                rings.append(list(_reproj_coords(ring)))
            polys.append(rings)
        from shapely.geometry import MultiPolygon, Polygon
        return MultiPolygon([Polygon(r[0], holes=r[1:]) for r in polys])
    else:
        from shapely.ops import transform as shp_transform
        return shp_transform(lambda x, y, z=None: transformer.transform(x, y), geom)

def prepare_clipped_shapes(parcel_fc_3857: Dict, landtypes_fc_3857: Dict):
    parcel = to_shapely_union(parcel_fc_3857)
    results = []
    for feat in landtypes_fc_3857["features"]:
        props = feat.get("properties", {})
        code = str(props.get("LT_CODE_1", "UNK"))
        name = str(props.get("LT_NAME_1", "Unknown"))
        g = force_2d(shape(feat["geometry"]))
        if not g.is_valid or g.is_empty:
            continue
        inter = g.intersection(parcel)
        if inter.is_empty:
            continue
        inter4326 = reproject_geom(inter, 3857, 4326)
        results.append((inter4326, code, name))
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
    if not shapes_clipped_4326:
        raise ValueError("No intersecting land type polygons found for this parcel.")
    union = unary_union([g for g, _, _ in shapes_clipped_4326])
    west, south, east, north = union.bounds
    width, height = choose_raster_size((west, south, east, north), max_px=max_px)
    transform = from_bounds(west, south, east, north, width, height)

    codes = []
    for _, code, _ in shapes_clipped_4326:
        if code not in codes:
            codes.append(code)
    code_to_id = {c: i + 1 for i, c in enumerate(codes)}  # 0 background

    shapes = [(mapping(g), code_to_id[c]) for g, c, _ in shapes_clipped_4326]
    class_raster = rasterize(
        shapes=shapes,
        out_shape=(height, width),
        transform=transform,
        fill=0,
        dtype="uint16",
        all_touched=False,
    )

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
        "interleave": "pixel"
    }

    with rasterio.open(out_path, "w", **profile) as dst:
        dst.write(r, 1)
        dst.write(g, 2)
        dst.write(b, 3)
        dst.write(a, 4)

    legend = []
    for (geom, code, name) in shapes_clipped_4326:
        legend.append({"code": code, "name": name, "color": color_from_code(code)})

    return {
        "legend": legend,
        "bounds4326": {"west": west, "south": south, "east": east, "north": north},
        "width": width,
        "height": height,
        "path": out_path,
    }
