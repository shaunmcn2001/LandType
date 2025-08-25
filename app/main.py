import os, tempfile, logging
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse

from .arcgis import fetch_parcel_geojson, fetch_landtypes_intersecting_envelope
from .rendering import to_shapely_union, bbox_3857, prepare_clipped_shapes, make_geotiff_rgba
from .colors import color_from_code
from .kml import build_kml, write_kmz

logging.basicConfig(level=logging.INFO)
app = FastAPI(
    title="QLD Land Types → GeoTIFF + Map + KMZ",
    description="Enter a QLD Lot/Plan; download GeoTIFF, interactive vectors, or clickable KMZ for Google Earth.",
    version="1.4.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

def rgb_to_hex(rgb):
    r, g, b = rgb
    return "#{:02x}{:02x}{:02x}".format(r, g, b)

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
    input[type=text],input[type=number]{width:100%;padding:10px 12px;border-radius:12px;border:1px solid #2b3960;background:#0e1526;color:var(--text)}
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
    </div>
    <div class="btns">
      <button class="primary" id="btn-download">Download GeoTIFF</button>
      <a class="ghost" id="btn-json" href="#">View JSON summary</a>
      <a class="ghost" id="btn-load" href="#">Load on Map</a>
      <a class="ghost" id="btn-kmz" href="#">Download KMZ (clickable)</a>
    </div>
    <div class="note">Input is normalised to UPPERCASE. Try <code>13SP181800</code> for a quick test. API docs: <a href="/docs">/docs</a></div>
    <div id="map"></div><div id="out" class="out"></div>
  </div></div>
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" crossorigin=""></script>
  <script>
    const $lot = document.getElementById('lotplan'), $max = document.getElementById('maxpx'),
          $btnDl = document.getElementById('btn-download'), $btnJs = document.getElementById('btn-json'),
          $btnLoad = document.getElementById('btn-load'), $btnKMZ = document.getElementById('btn-kmz'),
          $out = document.getElementById('out');

    function normLot(s){ return (s || '').trim().toUpperCase(); }
    function mkExportUrl(download){ const lotplan = encodeURIComponent(normLot($lot.value)); const maxpx = encodeURIComponent(($max.value || '4096').trim()); return `/export?lotplan=${lotplan}&max_px=${maxpx}&download=${download ? 'true' : 'false'}`; }
    function mkVectorUrl(){ const lotplan = encodeURIComponent(normLot($lot.value)); return `/vector?lotplan=${lotplan}`; }
    function mkKmzUrl(){ const lotplan = encodeURIComponent(normLot($lot.value)); return `/export_kmz?lotplan=${lotplan}`; }

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

    $btnDl.addEventListener('click', (e)=>{ e.preventDefault(); const lot = normLot($lot.value); if(!lot){ $out.textContent='Enter a Lot/Plan first.'; return; } window.location.href = mkExportUrl(true); $out.textContent='Generating GeoTIFF…'; });
    $btnJs.addEventListener('click', async (e)=>{ e.preventDefault(); const lot = normLot($lot.value); if(!lot){ $out.textContent='Enter a Lot/Plan first.'; return; } $out.textContent='Requesting JSON summary…'; try{ const res = await fetch(mkExportUrl(false)); const txt = await res.text(); try{ const data = JSON.parse(txt); $out.textContent = JSON.stringify(data, null, 2);}catch{ $out.textContent = `Error ${res.status}: ${txt}`; } }catch(err){ $out.textContent = 'Network error: ' + err; } });
    $btnLoad.addEventListener('click', (e)=>{ e.preventDefault(); loadVector(); });
    $btnKMZ.addEventListener('click', (e)=>{ e.preventDefault(); const lot = normLot($lot.value); if(!lot){ $out.textContent='Enter a Lot/Plan first.'; return; } window.location.href = mkKmzUrl(); $out.textContent='Generating KMZ…'; });
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

        filename = os.path.basename(out_path)
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        return FileResponse(out_path, media_type="application/vnd.google-earth.kmz", filename=filename, headers=headers)
    except HTTPException:
        raise
    except Exception as e:
        logging.exception("KMZ export error")
        raise HTTPException(status_code=500, detail=str(e))
