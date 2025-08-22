import os
import tempfile
import logging
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse

from .arcgis import fetch_parcel_geojson, fetch_landtypes_intersecting_envelope
from .rendering import to_shapely_union, bbox_3857, prepare_clipped_shapes, make_geotiff_rgba
from fastapi import Query
from .arcgis import fetch_parcel_geojson, fetch_landtypes_intersecting_envelope
from .rendering import to_shapely_union, bbox_3857
from .kml import build_kml, write_kmz
from .colors import color_from_code


logging.basicConfig(level=logging.INFO)
app = FastAPI(
    title="QLD Land Types → GeoTIFF",
    description="Enter a QLD Lot/Plan (e.g. 13DP1246224 or 13SP181800); get Land Types over the parcel boundary as a GeoTIFF for Google Earth.",
    version="1.2.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ---------- UI: Homepage ----------
@app.get("/", response_class=HTMLResponse)
def home():
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>QLD Land Types → GeoTIFF</title>
  <style>
    :root { --bg:#0b1220; --card:#121a2b; --text:#e8eefc; --muted:#9fb2d8; --accent:#6aa6ff; }
    *{box-sizing:border-box} body{margin:0;background:var(--bg);color:var(--text);font:16px/1.45 system-ui,-apple-system,Segoe UI,Roboto,Inter,Arial,sans-serif}
    .wrap{max-width:880px;margin:40px auto;padding:0 16px}
    .card{background:var(--card);border:1px solid #1f2a44;border-radius:16px;box-shadow:0 10px 30px rgba(0,0,0,.25);padding:24px}
    h1{margin:0 0 6px;font-size:28px} p{margin:0 0 18px;color:var(--muted)}
    label{display:block;margin:14px 0 6px;color:var(--muted);font-size:14px}
    input[type=text],input[type=number]{width:100%;padding:12px 14px;border-radius:12px;border:1px solid #2b3960;background:#0e1526;color:var(--text);outline:none}
    .row{display:flex;gap:12px;flex-wrap:wrap}
    .row > *{flex:1 1 220px}
    .btns{margin-top:16px;display:flex;gap:10px;flex-wrap:wrap}
    button,.ghost{appearance:none;border:0;border-radius:12px;padding:12px 16px;font-weight:600;cursor:pointer}
    button.primary{background:var(--accent);color:#071021}
    a.ghost{color:var(--accent);text-decoration:none;border:1px solid #294a86;background:#0d1730}
    .note{margin-top:10px;font-size:13px;color:#89a3d6}
    .out{margin-top:18px;border-top:1px solid #203055;padding-top:14px;font-family:ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;white-space:pre-wrap}
    .badge{display:inline-block;padding:.2rem .5rem;border-radius:999px;background:#11204a;color:#9fc1ff;font-size:12px;margin-left:8px}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1>QLD Land Types → GeoTIFF <span class="badge">EPSG:4326</span></h1>
      <p>Enter a Queensland <strong>Lot / Plan</strong> like <code>13DP1246224</code> or <code>13SP181800</code>. We’ll fetch the parcel, intersect Land Types, and return a Google‑Earth‑ready GeoTIFF (RGBA, transparent background). Legend now includes area per land type (ha).</p>

      <label for="lotplan">Lot / Plan</label>
      <input id="lotplan" type="text" placeholder="e.g. 13DP1246224" autocomplete="off" />

      <div class="row">
        <div>
          <label for="maxpx">Max raster dimension (px)</label>
          <input id="maxpx" type="number" min="256" max="8192" value="4096" />
        </div>
        <div>
          <label>Output</label>
          <div class="btns">
            <button class="primary" id="btn-download">Download GeoTIFF</button>
            <a class="ghost" id="btn-json" href="#">View JSON summary</a>
            <a class="ghost" id="btn-kmz" href="#">Download KMZ (clickable)</a>
          </div>
        </div>
      </div>

      <div class="note">
        Tips: Input is normalised to UPPERCASE. Try <code>13SP181800</code> for a quick test. See <a href="/docs">/docs</a> for API.
      </div>

      <div id="out" class="out"></div>
    </div>
  </div>

  <script>
    const $lot = document.getElementById('lotplan');
    const $max = document.getElementById('maxpx');
    const $btnDl = document.getElementById('btn-download');
    const $btnJs = document.getElementById('btn-json');
    const $out = document.getElementById('out');

    function normLot(s){ return (s || '').trim().toUpperCase(); }

    function mkUrl(download){
      const lotplan = encodeURIComponent(normLot($lot.value));
      const maxpx = encodeURIComponent(($max.value || '4096').trim());
      return `/export?lotplan=${lotplan}&max_px=${maxpx}&download=${download ? 'true' : 'false'}`;
    }

    $btnDl.addEventListener('click', (e) => {
      e.preventDefault();
      const lot = normLot($lot.value);
      if(!lot){ $out.textContent = 'Enter a Lot/Plan first.'; return; }
      window.location.href = mkUrl(true);
      $out.textContent = 'Generating GeoTIFF… If a download doesn’t start, check logs or try JSON first.';
    });

    $btnJs.addEventListener('click', async (e) => {
      e.preventDefault();
      const lot = normLot($lot.value);
      if(!lot){ $out.textContent = 'Enter a Lot/Plan first.'; return; }
      $out.textContent = 'Requesting JSON summary…';
      try{
        const res = await fetch(mkUrl(false));
        const txt = await res.text();
        try {
          const data = JSON.parse(txt);
          $out.textContent = JSON.stringify(data, null, 2);
        } catch {
          $out.textContent = `Error ${res.status}: ${txt}`;
        }
      }catch(err){
        $out.textContent = 'Network error: ' + err;
      }
    });
  const $btnKMZ = document.getElementById('btn-kmz');
  function mkKmzUrl(){
    const lotplan = encodeURIComponent(normLot($lot.value));
    return `/export_kmz?lotplan=${lotplan}`;
  }
  $btnKMZ.addEventListener('click', (e) => {
    e.preventDefault();
    const lot = normLot($lot.value);
    if(!lot){ $out.textContent = 'Enter a Lot/Plan first.'; return; }
    window.location.href = mkKmzUrl();
    $out.textContent = 'Generating KMZ…';
  });
    setTimeout(()=>{ $lot.focus(); }, 50);
  </script>
</body>
</html>"""

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/export")
def export_geotiff(
    lotplan: str = Query(..., description="QLD Lot/Plan, e.g. 13DP1246224 or 13SP181800"),
    max_px: int = Query(4096, ge=256, le=8192, description="Max raster dimension (px)"),
    download: bool = Query(True, description="Return file download (True) or JSON summary (False)"),
):
    try:
        lotplan = lotplan.strip().upper()

        parcel_fc = fetch_parcel_geojson(lotplan)
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

@app.get("/debug/landtype-fields")
def debug_landtype_fields(lotplan: str = Query(..., description="QLD Lot/Plan")):
    lotplan = lotplan.strip().upper()
    parcel_fc = fetch_parcel_geojson(lotplan)
    env = bbox_3857(to_shapely_union(parcel_fc))
    lt_fc = fetch_landtypes_intersecting_envelope(env)
    # Return the keys of the first few features so we see what's really there
    props_list = []
    for feat in lt_fc.get("features", [])[:5]:
        props = feat.get("properties", {}) or {}
        props_list.append(sorted(list(props.keys())))
    return {"lotplan": lotplan, "sample_property_keys": props_list}
@app.get("/export_kmz")
def export_kmz(
    lotplan: str = Query(..., description="QLD Lot/Plan, e.g. 13DP1246224 or 13SP181800"),
    simplify_tolerance: float = Query(0.0, ge=0.0, le=0.001, description="Optional geometry simplify tolerance in degrees (e.g., 0.00005 ≈ 5 m)"),
):
    """
    Returns a KMZ with clickable polygons. Each placemark popup shows Name, Code, Area (ha).
    Optional: simplify geometries to reduce size.
    """
    try:
        lotplan = lotplan.strip().upper()
        parcel_fc = fetch_parcel_geojson(lotplan)
        parcel_union = to_shapely_union(parcel_fc)
        env = bbox_3857(parcel_union)
        lt_fc = fetch_landtypes_intersecting_envelope(env)
        clipped = prepare_clipped_shapes(parcel_fc, lt_fc)
        if not clipped:
            raise HTTPException(status_code=404, detail="No Land Types intersect this parcel.")

        # Optionally simplify to shrink KMZ (works in EPSG:4326 since we already reprojected)
        if simplify_tolerance and simplify_tolerance > 0:
            simplified = []
            for geom4326, code, name, area_ha in clipped:
                g2 = geom4326.simplify(simplify_tolerance, preserve_topology=True)
                if not g2.is_empty:
                    simplified.append((g2, code, name, area_ha))
            clipped = simplified or clipped

        kml = build_kml(clipped, color_fn=color_from_code)

        import tempfile, os
        tmpdir = tempfile.mkdtemp(prefix="kmz_")
        out_path = os.path.join(tmpdir, f"{lotplan}_landtypes.kmz")
        write_kmz(kml, out_path)

        filename = os.path.basename(out_path)
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        return FileResponse(out_path, media_type="application/vnd.google-earth.kmz", filename=filename, headers=headers)
    except HTTPException:
        raise
    except Exception as e:
        logging.exception("KMZ export error")
        raise HTTPException(status_code=500, detail=str(e))
