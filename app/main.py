from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, Field
from typing import List, Optional, Tuple, Dict, Any
from io import BytesIO
import zipfile
import csv
import datetime as dt

app = FastAPI(title="QLD Land Types → GeoTIFF")

# ──────────────────────────────────────────────────────────────────────────────
# ⚠️ Hook points to your existing implementation
# Replace the bodies of the two “TODO” functions below with calls into your
# current code that powers GET /export. The rest of this file just wraps that
# logic to add a custom filename and bulk export.
# ──────────────────────────────────────────────────────────────────────────────

def _export_one_geotiff(lotplan: str, max_px: int = 4096) -> Tuple[bytes, Dict[str, Any]]:
    """
    Return (tiff_bytes, meta) for a single lotplan.
    meta should include at least a bbox and any useful attributes used in your JSON mode.
    TODO: Wire this into your existing rasterization code used by GET /export.
    """
    # Example shape of meta:
    # meta = {"lotplan": lotplan, "bounds_epsg4326": [minx, miny, maxx, maxy], "area_m2": 1234}
    raise NotImplementedError("Hook _export_one_geotiff must call your existing export logic.")


def _export_one_json(lotplan: str, max_px: int = 4096) -> Dict[str, Any]:
    """
    Return the JSON summary your current /export returns when download=false.
    TODO: Wire this into your existing JSON-returning path.
    """
    raise NotImplementedError("Hook _export_one_json must call your existing JSON logic.")


# ──────────────────────────────────────────────────────────────────────────────
# Single export (adds: filename=)
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/export")
def export(
    lotplan: str = Query(..., description="e.g. 13SP181800"),
    max_px: int = Query(4096, ge=256, le=32768, description="Max raster dimension"),
    download: bool = Query(True, description="If false, return JSON summary"),
    filename: Optional[str] = Query(None, description="Optional file name for the TIFF (without extension)")
):
    """
    Existing endpoint, unchanged behavior by default.
    New: you may pass &filename=My_Overlay to control the downloaded name.
    """
    if not lotplan or lotplan.strip() == "":
        raise HTTPException(status_code=400, detail="lotplan is required")

    if not download:
        # JSON-only path
        data = _export_one_json(lotplan.strip(), max_px=max_px)
        return JSONResponse(content=data)

    # GeoTIFF path
    try:
        tiff_bytes, _meta = _export_one_geotiff(lotplan.strip(), max_px=max_px)
    except HTTPException as e:
        # Allow your underlying code to bubble up 404 etc.
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed for {lotplan}: {e}")

    buf = BytesIO(tiff_bytes)
    dl_name = (filename or f"{lotplan}_landtypes") + ".tif"

    headers = {
        "Content-Disposition": f'attachment; filename="{dl_name}"'
    }
    return StreamingResponse(buf, media_type="image/tiff", headers=headers)


# ──────────────────────────────────────────────────────────────────────────────
# Bulk export (new)
# ──────────────────────────────────────────────────────────────────────────────

class BulkRequest(BaseModel):
    lotplans: List[str] = Field(..., description="List of Queensland Lot/Plan codes, e.g. ['13SP181800','1RP12345']")
    max_px: int = Field(4096, ge=256, le=32768, description="Max raster dimension")
    download: bool = Field(True, description="If false, returns JSON array of summaries instead of ZIP")
    filename_prefix: Optional[str] = Field(None, description="Optional prefix for file names inside the ZIP")

class BulkResultItem(BaseModel):
    lotplan: str
    ok: bool
    message: Optional[str] = None
    bounds_epsg4326: Optional[List[float]] = None
    area_m2: Optional[float] = None

@app.post("/export/bulk")
def export_bulk(payload: BulkRequest = Body(...)):
    """
    Accepts many lotplans and returns:
      - If download=true (default): a ZIP with one .tif per successful lot/plan + manifest.csv
      - If download=false: JSON list of per-lot summaries (or errors).
    """
    # Clean & de-duplicate while preserving order
    seen = set()
    lotplans = []
    for lp in (lp.strip().upper() for lp in payload.lotplans):
        if not lp:
            continue
        if lp in seen:
            continue
        seen.add(lp)
        lotplans.append(lp)

    if not lotplans:
        raise HTTPException(status_code=400, detail="No valid lotplans provided.")

    if not payload.download:
        # JSON-only: mirror single-export JSON for each lotplan
        out: List[Dict[str, Any]] = []
        for lp in lotplans:
            try:
                data = _export_one_json(lp, max_px=payload.max_px)
                out.append({"lotplan": lp, "ok": True, **data})
            except HTTPException as e:
                out.append({"lotplan": lp, "ok": False, "message": e.detail})
            except Exception as e:
                out.append({"lotplan": lp, "ok": False, "message": str(e)})
        return JSONResponse(content=out)

    # ZIP with TIFFs + manifest
    zip_buf = BytesIO()
    manifest_rows = []  # list of dicts

    with zipfile.ZipFile(zip_buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for lp in lotplans:
            try:
                tiff_bytes, meta = _export_one_geotiff(lp, max_px=payload.max_px)
                # Build filename inside the zip
                base = f"{payload.filename_prefix + '_' if payload.filename_prefix else ''}{lp}_landtypes"
                tif_name = f"{base}.tif"

                zf.writestr(tif_name, tiff_bytes)

                manifest_rows.append({
                    "lotplan": lp,
                    "status": "ok",
                    "file": tif_name,
                    "bounds_epsg4326": meta.get("bounds_epsg4326"),
                    "area_m2": meta.get("area_m2")
                })
            except HTTPException as e:
                manifest_rows.append({
                    "lotplan": lp,
                    "status": f"error:{e.status_code}",
                    "file": "",
                    "bounds_epsg4326": "",
                    "area_m2": "",
                    "message": e.detail
                })
            except Exception as e:
                manifest_rows.append({
                    "lotplan": lp,
                    "status": "error:500",
                    "file": "",
                    "bounds_epsg4326": "",
                    "area_m2": "",
                    "message": str(e)
                })

        # Write manifest.csv
        manifest_csv = BytesIO()
        fieldnames = ["lotplan", "status", "file", "bounds_epsg4326", "area_m2", "message"]
        writer = csv.DictWriter(manifest_csv, fieldnames=fieldnames)
        writer.writeheader()
        for row in manifest_rows:
            # Ensure all keys are present for CSV
            for key in fieldnames:
                row.setdefault(key, "")
            writer.writerow(row)
        zf.writestr("manifest.csv", manifest_csv.getvalue())

    zip_buf.seek(0)
    stamp = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    dl_name = f"landtypes_bulk_{stamp}.zip"
    headers = {"Content-Disposition": f'attachment; filename="{dl_name}"'}
    return StreamingResponse(zip_buf, media_type="application/zip", headers=headers)


# ──────────────────────────────────────────────────────────────────────────────
# Health (existing)
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"ok": True}
