import os, tempfile, logging, zipfile, csv, datetime as dt
from io import BytesIO
from typing import List, Optional, Dict, Any, Tuple

from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse, StreamingResponse
from pydantic import BaseModel, Field

from .arcgis import fetch_parcel_geojson, fetch_landtypes_intersecting_envelope
from .rendering import to_shapely_union, bbox_3857, prepare_clipped_shapes, make_geotiff_rgba
from .colors import color_from_code
from .kml import build_kml, write_kmz

logging.basicConfig(level=logging.INFO)
app = FastAPI(
    title="QLD Land Types → GeoTIFF + Map + KMZ",
    description="Enter a QLD Lot/Plan; download GeoTIFF, interactive vectors, or clickable KMZ for Google Earth.",
    version="1.5.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST"],  # allow POST for bulk
    allow_headers=["*"],
)

def rgb_to_hex(rgb):
    r, g, b = rgb
    return "#{:02x}{:02x}{:02x}".format(r, g, b)

def _sanitize_filename(s: str) -> str:
    base = "".join(c for c in s.strip() if c.isalnum() or c in ("_", "-", ".", " ")).strip()
    return base or "download"

def _render_one_tiff_and_meta(lotplan: str, max_px: int) -> Tuple[bytes, Dict[str, Any]]:
    """
    Builds a GeoTIFF for a single lot/plan and returns (tiff_bytes, meta).
    Meta includes simple bounds and total area (ha).
    """
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
    try:
        result = make_geotiff_rgba(clipped, out_path, max_px=max_px)
        with open(out_path, "rb") as f:
            tiff_bytes = f.read()
    finally:
        try:
            if os.path.exists(out_path): os.remove(out_path)
            if os.path.isdir(tmpdir): os.rmdir(tmpdir)
        except Exception:
            pass

    # Compute simple meta
    west, south, east, north = parcel_union.bounds
    total_area_ha = sum(float(a_ha) for _, _, _, a_ha in clipped)
    meta = {
        "bounds_epsg4326": [west, south, east, north],
        "area_ha_total": total_area_ha,
        # include anything the raster function already reported, excluding any path
        **{k: v for k, v in result.items() if k != "path"}
    }
    return tiff_bytes, meta

@app.get("/", response_class=HTMLResponse)
def home():
    return """<!doctype html>
<html lang="en"><head>
  <meta charset="utf-8" /><meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>QLD Land Types → GeoTIFF + Map + KMZ</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" crossorigin=""/>
  <style>
    :root { --bg:#0b1220; --card:#121a2b; --text:#e8eefc; --muted:#9fb2d8; --accent:#6aa6ff; }
    *{box-sizing:border-box} body{margin:0;background:var(--bg);color:var(--text);font:16px/1.45 system-ui,-apple-system,Segoe UI,Roboto,Inter,Arial,sans-serif}
    .wrap{max-width:1100px;margin:28px auto;padding:0 16px}.card{background:var(--card);border:1px solid #1f2a44;border-radius:16px;box-shadow:0 10px 30px rgba(0,0,0,.25);padding:18px}
    h1{margin:4px 0 10px;font-size:26px} p{margin:0 0 14px;color:var(--muted)} label{display:block;margin:10px 0 6px;color:var(--muted);font-size:14px}
    input[type=text],input[type=number],textarea{width:100%;padding:10px 12px;border-radius:12px;border:1px solid #2b3960;background:#0e1526;color:var(--text)}
    textarea{min-height:110px;resize:vertical}
    .row{display:flex;gap:12px;flex-wrap:wrap}.row > *{flex:1 1 200px}.btns{margin-top:12px;display:flex;gap:10px;flex-wrap:wrap}
    button,.ghost{appearance:none;border:0;border-radius:12px;padding:10px 14px;font-weight:600;cursor:pointer}
    button.primary{background:var(--accent);color:#071021} a.ghost{color:var(--accent);text-decoration:none;border:1px solid #294a86;background:#0d1730}
    .note{margin-top:8px;font-size:13px;color:#89a3d6} #map{height:520px;border-radius:14px;margin-top:14px;border:1px solid #203055}
    .out{margin-top:12px;border-top:1px solid #203055;padding-top:10px;font-family:ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;white-space:pre-wrap}
    .badge{display:inline-block;padding:.2rem .5rem;border-radius:999px;background:#11204a;color:#9fc1ff;font-size:12px;margin-left:8px}
  </style>
</head><body>
  <div class="wrap"><div class="card">
    <h1>QLD Land Types → GeoTIFF + Map + KMZ <span class="badge">EPSG:4326</span></h1>
    <p>Enter a Queensland <strong>Lot / Plan</strong> (e.g. <code>13DP1246224</code> or <code>13SP181800</code>). Download a GeoTIFF, load interactive polygons, or download a KMZ with clickable attributes for Google Earth.</p>
    <div class="row">
      <div><label for="lotplan">Lot / Plan</label><input id="lotplan" type="text" placeholder="e.g. 13DP1246224" autocomplete="off" /></div>
      <div><label for="maxpx">Max raster dimension (px) for GeoTIFF</label><input id="maxpx" type="number" min="256" max="8192" value="4096" /></div>
      <div><label for="filename">Custom file name (optional, no extension)</label><input id="filename" type="text" placeholder="e.g. UpperCoomera_13SP181800" /></div>
    </div>

    <div class="btns">
      <button class="primary" id="btn-download">Download GeoTIFF</button>
      <a class="ghost" id="btn-json" href="#">View JSON summary</a>
      <a class="ghost" id="btn-load" href="#">Load on Map</a>
      <a class="ghost" id="btn-kmz" href="#">Download KMZ (clickable)</a>
    </div>

    <div class="row" style="margin-top:18px">
      <div><label for="bulk">Bulk Lot/Plan list (one per line, commas/semicolons also OK)</label>
        <textarea id="bulk" placeholder="13SP181800
1RP12345
2RP54321"></textarea>
      </div>
      <div><label for="prefix">Filename prefix for ZIP contents (optional)</label><input id="prefix" type="text" placeholder="e.g. Job_4021" /></div>
    </div>

    <div class="btns">
      <button class="primary" id="btn-bulk-zip">Bulk GeoTIFF (ZIP)</button>
      <a class="ghost" id="btn-bulk-json" href="#">Bulk JSON only</a>
    </div>

    <div class="note">Input is normalised to UPPERCASE. Try <code>13SP181800</code> for a quick test. API docs: <a href="/docs">/docs</a></div>
    <div id="map"></div><div id="out" class="out"></div>
  </div></div>
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" crossorigin=""></script>
  <script>
    const $lot = document.getElementById('lotplan'), $max = document.getElementById('maxpx'),
          $btnDl = document.getElementById('btn-download'), $btnJs = document.getElementById('btn-json'),
          $btnLoad = document.getElementById('btn-load'), $btnKMZ = document.getElementById('btn-kmz'),
          $out = document.getElementById('out'), $fn = document.getElementById('filename'),
          $bulk = document.getElementById('bulk'), $prefix = document.getElementById('prefix'),
          $btnBulkZip = document.getElementById('btn-bulk-zip'), $btnBulkJson = document.getElementById('btn-bulk-json');

    function normLot(s){ return (s || '').trim().toUpperCase(); }
    function normText(s){ return (s || '').trim(); }

    function mkExportUrl(download){
      const lotplan = encodeURIComponent(normLot($lot.value));
      const maxpx = encodeURIComponent(($max.value || '4096').trim());
      const fn = normText($fn.value);
      const fname = fn ? `&filename=${encodeURIComponent(fn)}` : '';
      return `/export?lotplan=${lotplan}&max_px=${maxpx}&download=${download ? 'true' : 'false'}${fname}`;
    }
    function mkVectorUrl(){ const lotplan = encodeURIComponent(normLot($lot.value)); return `/vector?lotplan=${lotplan}`; }
    function mkKmzUrl(){
      const lotplan = encodeURIComponent(normLot($lot.value));
      const fn = normText($fn.value);
      const fname = fn ? `&filename=${encodeURIComponent(fn)}` : '';
      return `/export_kmz?lotplan=${lotplan}${fname}`;
    }

    const map = L.map('map', { zoomControl: true });
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { attribution: '&copy; OpenStreetMap' }).addTo(map);
    map.setView([-23.5, 146.0], 5);

    let parcelLayer=null, ltLayer=null;
    function styleForCode(code, colorHex){ return { color:'#0c1325', weight:1, fillColor:colorHex, fillOpacity:0.6 }; }
    function clearLayers(){ if(parcelLayer){ map.removeLayer(parcelLayer); parcelLayer=null; } if(ltLayer){ map.removeLayer(ltLayer); ltLayer=null; } }

    async function loadVector(){
      const lot = normLot($lot.value); if(!lot){ $out.textContent = 'Enter a Lot/Plan first.'; return; }
      $out.textContent = 'Loading vector data…';
      try{
        const res = await fetch(mkVectorUrl()); const data = await res.json();
        if (data.error){ $out.textContent = 'Error: ' + data.error; return; }
        clearLayers();
        parcelLayer = L.geoJSON(data.parcel, { style: { color: '#ffcc00', weight:2, fillOpacity:0 } }).addTo(map);
        ltLayer = L.geoJSON(data.landtypes, { style: f => styleForCode(f.properties.code, f.properties.color_hex),
          onEachFeature: (feature, layer) => {
            const p = feature.properties || {};
            const html = `<b>${p.name || 'Unknown'}</b><br/>Code: <code>${p.code || 'UNK'}</code><br/>Area: ${(p.area_ha ?? 0).toFixed(2)} ha`;
            layer.bindPopup(html);
          }}).addTo(map);
        const b = data.bounds4326; if (b){ map.fitBounds([[b.south, b.west],[b.north, b.east]], { padding:[20,20] }); }
        $out.textContent = JSON.stringify({ lotplan: data.lotplan, legend: data.legend, bounds4326: data.bounds4326 }, null, 2);
      }catch(err){ $out.textContent = 'Network error: ' + err; }
    }

    function parseBulk(text){
      const raw = (text || '').split(/\\r?\\n|,|;/);
      const clean = raw.map(s => s.trim().toUpperCase()).filter(Boolean);
      const seen = new Set(); const out = [];
      for(const v of clean){ if(!seen.has(v)){ seen.add(v); out.push(v); } }
      return out;
    }

    async function downloadBlobAs(res, filename){
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url; a.download = filename;
      document.body.appendChild(a); a.click(); a.remove();
      URL.revokeObjectURL(url);
    }

    $btnDl.addEventListener('click', (e)=>{ e.preventDefault(); const lot = normLot($lot.value); if(!lot){ $out.textContent='Enter a Lot/Plan first.'; return; } window.location.href = mkExportUrl(true); $out.textContent='Generating GeoTIFF…'; });
    $btnJs.addEventListener('click', async (e)=>{ e.preventDefault(); const lot = normLot($lot.value); if(!lot){ $out.textContent='Enter a Lot/Plan first.'; return; } $out.textContent='Requesting JSON summary…'; try{ const res = await fetch(mkExportUrl(false)); const txt = await res.text(); try{ const data = JSON.parse(txt); $out.textContent = JSON.stringify(data, null, 2);}catch{ $out.textContent = `Error ${res.status}: ${txt}`; } }catch(err){ $out.textContent = 'Network error: ' + err; } });
    $btnLoad.addEventListener('click', (e)=>{ e.preventDefault(); loadVector(); });
    $btnKMZ.addEventListener('click', (e)=>{ e.preventDefault(); const lot = normLot($lot.value); if(!lot){ $out.textContent='Enter a Lot/Plan first.'; return; } window.location.href = mkKmzUrl(); $out.textContent='Generating KMZ…'; });

    $btnBulkZip.addEventListener('click', async (e)=>{
      e.preventDefault();
      const items = parseBulk($bulk.value);
      if (!items.length){ $out.textContent = 'Enter at least one Lot/Plan in the bulk box.'; return; }
      $out.textContent = `Submitting ${items.length} lot/plan codes for ZIP…`;
      try{
        const body = {
          lotplans: items,
          max_px: parseInt($max.value || '4096', 10),
          filename_prefix: normText($prefix.value) || null,
          download: true
        };
        const res = await fetch('/export/bulk', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
        if (!res.ok){ const txt = await res.text(); $out.textContent = `Error ${res.status}: ${txt}`; return; }
        const stamp = new Date().toISOString().replace(/[-:]/g,'').slice(0,15)+'Z';
        const base = normText($prefix.value) ? normText($prefix.value) + '_' : '';
        await downloadBlobAs(res, `${base}landtypes_bulk_${stamp}.zip`);
        $out.textContent = 'ZIP downloaded.';
      }catch(err){ $out.textContent = 'Network error: ' + err; }
    });

    $btnBulkJson.addEventListener('click', async (e)=>{
      e.preventDefault();
      const items = parseBulk($bulk.value);
      if (!items.length){ $out.textContent = 'Enter at least one Lot/Plan in the bulk box.'; return; }
      $out.textContent = `Submitting ${items.length} lot/plan codes for JSON…`;
      try{
        const body = {
          lotplans: items,
          max_px: parseInt($max.value || '4096', 10),
          filename_prefix: null,
          download: false
        };
        const res = await fetch('/export/bulk', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
        const data = await res.json();
        if (!res.ok){ $out.textContent = 'Error: ' + JSON.stringify(data); return; }
        $out.textContent = JSON.stringify(data, null, 2);
      }catch(err){ $out.textContent = 'Network error: ' + err; }
    });

    setTimeout(()=>{ $lot.focus(); }, 50);
  </script>
</body></html>"""

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/export")
def export_geotiff(
    lotplan: str = Query(..., description="QLD Lot/Plan, e.g. 13DP1246224 or 13SP181800"),
    max_px: int = Query(4096, ge=256, le=8192, description="Max raster dimension (px)"),
    download: bool = Query(True, description="Return file download (True) or JSON summary (False)"),
    filename: Optional[str] = Query(None, description="Custom file name for the TIFF (no extension)"),
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
            # set custom filename if provided
            if filename:
                dl = _sanitize_filename(filename)
                if not dl.lower().endswith(".tif"): dl += ".tif"
            else:
                dl = os.path.basename(out_path)
            # FileResponse will set Content-Disposition when filename is provided
            return FileResponse(out_path, media_type="image/tiff", filename=dl)
        else:
            result_public = {k: v for k, v in result.items() if k != "path"}
            return JSONResponse({"lotplan": lotplan, **result_public})
    except HTTPException:
        raise
    except Exception as e:
        logging.exception("Export error")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/vector")
def vector_geojson(lotplan: str = Query(..., description="QLD Lot/Plan")):
    try:
        lotplan = lotplan.strip().upper()
        parcel_fc = fetch_parcel_geojson(lotplan)
        parcel_union = to_shapely_union(parcel_fc)
        env = bbox_3857(parcel_union)
        lt_fc = fetch_landtypes_intersecting_envelope(env)
        clipped = prepare_clipped_shapes(parcel_fc, lt_fc)
        if not clipped:
            return JSONResponse({"error": "No Land Types intersect this parcel."}, status_code=404)

        features = []
        legend_map = {}
        from shapely.geometry import mapping as shp_mapping
        for geom4326, code, name, area_ha in clipped:
            color_rgb = color_from_code(code)
            color_hex = rgb_to_hex(color_rgb)
            features.append({
                "type": "Feature",
                "geometry": shp_mapping(geom4326),
                "properties": {"code": code, "name": name, "area_ha": float(area_ha), "color_hex": color_hex}
            })
            if code not in legend_map:
                legend_map[code] = {"code": code, "name": name, "color_hex": color_hex, "area_ha": 0.0}
            legend_map[code]["area_ha"] += float(area_ha)

        union_bounds = to_shapely_union({"type":"FeatureCollection","features":[{"type":"Feature","geometry":f["geometry"],"properties":{}} for f in features]}).bounds
        west, south, east, north = union_bounds
        return JSONResponse({
            "lotplan": lotplan,
            "parcel": parcel_fc,
            "landtypes": { "type":"FeatureCollection", "features": features },
            "legend": sorted(legend_map.values(), key=lambda d: (-d["area_ha"], d["code"])),
            "bounds4326": {"west": west, "south": south, "east": east, "north": north}
        })
    except Exception as e:
        logging.exception("Vector export error")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/export_kmz")
def export_kmz(
    lotplan: str = Query(..., description="QLD Lot/Plan, e.g. 13DP1246224 or 13SP181800"),
    simplify_tolerance: float = Query(0.0, ge=0.0, le=0.001, description="Simplify polygons (deg); try 0.00005 ≈ 5 m"),
    filename: Optional[str] = Query(None, description="Custom file name for KMZ (no extension)"),
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

        if simplify_tolerance and simplify_tolerance > 0:
            simplified = []
            for geom4326, code, name, area_ha in clipped:
                g2 = geom4326.simplify(simplify_tolerance, preserve_topology=True)
                if not g2.is_empty:
                    simplified.append((g2, code, name, area_ha))
            clipped = simplified or clipped

        kml = build_kml(clipped,color_fn=color_from_code,folder_name=f"QLD Land Types – {lotplan}")
        tmpdir = tempfile.mkdtemp(prefix="kmz_")
        out_path = os.path.join(tmpdir, f"{lotplan}_landtypes.kmz")
        write_kmz(kml, out_path)

        if filename:
            dl = _sanitize_filename(filename)
            if not dl.lower().endswith(".kmz"): dl += ".kmz"
        else:
            dl = os.path.basename(out_path)
        return FileResponse(out_path, media_type="application/vnd.google-earth.kmz", filename=dl)
    except HTTPException:
        raise
    except Exception as e:
        logging.exception("KMZ export error")
        raise HTTPException(status_code=500, detail=str(e))

# ─────────────────────────────
# Bulk export (ZIP or JSON)
# ─────────────────────────────
class BulkRequest(BaseModel):
    lotplans: List[str] = Field(..., description="List of QLD Lot/Plan codes")
    max_px: int = Field(4096, ge=256, le=8192)
    download: bool = Field(True, description="If false, returns JSON list of summaries instead of ZIP")
    filename_prefix: Optional[str] = Field(None, description="Optional prefix for file names inside the ZIP")

@app.post("/export/bulk")
def export_bulk(payload: BulkRequest = Body(...)):
    # normalise and de-duplicate
    seen = set()
    lotplans: List[str] = []
    for lp in (lp.strip().upper() for lp in payload.lotplans):
        if not lp: continue
        if lp in seen: continue
        seen.add(lp); lotplans.append(lp)
    if not lotplans:
        raise HTTPException(status_code=400, detail="No valid lotplans provided.")

    if not payload.download:
        # Return JSON array of per-lot summaries (uses same raster function so results match /export?download=false)
        out: List[Dict[str, Any]] = []
        for lp in lotplans:
            try:
                # Build to temp to get same summary keys
                tiff_bytes, meta = _render_one_tiff_and_meta(lp, payload.max_px)
                out.append({"lotplan": lp, "ok": True, **{k: v for k, v in meta.items() if k != "path"}})
            except HTTPException as e:
                out.append({"lotplan": lp, "ok": False, "message": e.detail})
            except Exception as e:
                out.append({"lotplan": lp, "ok": False, "message": str(e)})
        return JSONResponse(content=out)

    # ZIP stream with TIFFs + manifest.csv
    prefix = _sanitize_filename(payload.filename_prefix) if payload.filename_prefix else None
    zip_buf = BytesIO()
    manifest_rows: List[Dict[str, Any]] = []

    with zipfile.ZipFile(zip_buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for lp in lotplans:
            try:
                tiff_bytes, meta = _render_one_tiff_and_meta(lp, payload.max_px)
                base = f"{prefix}_" if prefix else ""
                tif_name = f"{base}{lp}_landtypes.tif"
                zf.writestr(tif_name, tiff_bytes)

                manifest_rows.append({
                    "lotplan": lp,
                    "status": "ok",
                    "file": tif_name,
                    "bounds_epsg4326": meta.get("bounds_epsg4326"),
                    "area_ha_total": meta.get("area_ha_total")
                })
            except HTTPException as e:
                manifest_rows.append({
                    "lotplan": lp,
                    "status": f"error:{e.status_code}",
                    "file": "",
                    "bounds_epsg4326": "",
                    "area_ha_total": "",
                    "message": e.detail
                })
            except Exception as e:
                manifest_rows.append({
                    "lotplan": lp,
                    "status": "error:500",
                    "file": "",
                    "bounds_epsg4326": "",
                    "area_ha_total": "",
                    "message": str(e)
                })

        # write manifest.csv
        mem_csv = BytesIO()
        fieldnames = ["lotplan","status","file","bounds_epsg4326","area_ha_total","message"]
        writer = csv.DictWriter(mem_csv, fieldnames=fieldnames)
        writer.writeheader()
        for row in manifest_rows:
            for k in fieldnames:
                row.setdefault(k, "")
            writer.writerow(row)
        zf.writestr("manifest.csv", mem_csv.getvalue())

    zip_buf.seek(0)
    stamp = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    dl_name = f"{(prefix + '_' ) if prefix else ''}landtypes_bulk_{stamp}.zip"
    headers = {"Content-Disposition": f'attachment; filename="{dl_name}"'}
    return StreamingResponse(zip_buf, media_type="application/zip", headers=headers)
