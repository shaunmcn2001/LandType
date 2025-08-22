import os
import tempfile
import logging
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse

from .arcgis import fetch_parcel_geojson, fetch_landtypes_intersecting_envelope
from .rendering import to_shapely_union, bbox_3857, prepare_clipped_shapes, make_geotiff_rgba

logging.basicConfig(level=logging.INFO)
app = FastAPI(
    title="QLD Land Types → GeoTIFF",
    description="Enter a QLD Lot/Plan (e.g. 13SP181800); get Land Types over the parcel boundary as a GeoTIFF for Google Earth.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/export")
def export_geotiff(
    lotplan: str = Query(..., description="QLD Lot/Plan, e.g. 13SP181800"),
    max_px: int = Query(4096, ge=256, le=8192, description="Max raster dimension (px)"),
    download: bool = Query(True, description="Return file download (True) or JSON summary (False)"),
):
    try:
        parcel_fc = fetch_parcel_geojson(lotplan.strip())
        parcel_union = to_shapely_union(parcel_fc)
        env = bbox_3857(parcel_union)
        lt_fc = fetch_landtypes_intersecting_envelope(env)
        clipped = prepare_clipped_shapes(parcel_fc, lt_fc)

        if not clipped:
            raise HTTPException(status_code=404, detail="No Land Types intersect this parcel.")

        tmpdir = tempfile.mkdtemp(prefix="geotiff_")
        out_path = os.path.join(tmpdir, f"{lotplan}_landtypes.tif")

        result = make_geotiff_rgba(clipped, out_path, max_px=max_px)

        if download:
            filename = os.path.basename(out_path)
            headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
            return FileResponse(out_path, media_type="image/tiff", filename=filename, headers=headers)
        else:
            result_public = {k: v for k, v in result.items() if k != "path"}
            return JSONResponse({"lotplan": lotplan, **result_public})
    except HTTPException:
        raise
    except Exception as e:
        logging.exception("Export error")
        raise HTTPException(status_code=500, detail=str(e))
