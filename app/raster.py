# app/raster.py
from __future__ import annotations

import os
from typing import Any, Dict, List

import numpy as np
import rasterio
from rasterio.features import rasterize
from rasterio.transform import from_bounds
from shapely.geometry import mapping

from .colors import color_from_code


def make_geotiff_rgba(clipped: List[tuple], out_path: str, max_px: int = 4096) -> Dict[str, Any]:
    """
    Rasterize the clipped polygons (EPSG:4326) into an RGBA GeoTIFF in EPSG:4326.
    Each tuple: (geom4326, code, name, area_ha). Colors are derived from code.
    Returns a small dict including path and size.
    """
    if not clipped:
        raise ValueError("No polygons to rasterize.")

    # Union bounds in 4326
    from shapely.ops import unary_union
    geom_union = unary_union([g for g, _, _, _ in clipped])
    minx, miny, maxx, maxy = geom_union.bounds
    width_deg = maxx - minx
    height_deg = maxy - miny
    if width_deg <= 0 or height_deg <= 0:
        raise ValueError("Invalid bounds for rasterization.")

    # Compute output width/height respecting max_px
    if width_deg >= height_deg:
        width = max(1, min(max_px, int(round(max_px))))
        height = max(1, int(round((height_deg / width_deg) * width)))
        if height > max_px:
            height = max_px
            width = max(1, int(round((width_deg / height_deg) * height)))
    else:
        height = max(1, min(max_px, int(round(max_px))))
        width = max(1, int(round((width_deg / height_deg) * height)))
        if width > max_px:
            width = max_px
            height = max(1, int(round((height_deg / width_deg) * width)))

    transform = from_bounds(minx, miny, maxx, maxy, width, height)

    # Prepare RGBA arrays
    R = np.zeros((height, width), dtype=np.uint8)
    G = np.zeros((height, width), dtype=np.uint8)
    B = np.zeros((height, width), dtype=np.uint8)
    A = np.zeros((height, width), dtype=np.uint8)

    # Paint in order; later features overwrite earlier ones
    for geom, code, name, area_ha in clipped:
        color = color_from_code(code)
        mask = rasterize(
            [(mapping(geom), 1)],
            out_shape=(height, width),
            transform=transform,
            fill=0,
            all_touched=False,
            dtype=np.uint8,
        )
        R[mask == 1] = int(color[0])
        G[mask == 1] = int(color[1])
        B[mask == 1] = int(color[2])
        A[mask == 1] = 200  # semi-opaque

    profile = {
        "driver": "GTiff",
        "width": width,
        "height": height,
        "count": 4,
        "dtype": "uint8",
        "crs": "EPSG:4326",
        "transform": transform,
        "tiled": False,
        "interleave": "pixel",
        "compress": "deflate",
    }
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with rasterio.open(out_path, "w", **profile) as dst:
        dst.write(R, 1)
        dst.write(G, 2)
        dst.write(B, 3)
        dst.write(A, 4)

    return {"path": out_path, "width": width, "height": height, "bounds": [minx, miny, maxx, maxy]}
