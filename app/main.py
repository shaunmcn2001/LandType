# app/main.py
import os, tempfile, logging, zipfile, csv, datetime as dt
from io import BytesIO
from enum import Enum
from typing import List, Optional, Dict, Any, Tuple

from fastapi import FastAPI, HTTPException, Query, Body, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse, StreamingResponse
from pydantic import BaseModel, Field

from .arcgis import (
    fetch_parcel_geojson,
    fetch_landtypes_intersecting_envelope,
    fetch_features_intersecting_envelope,  # ensure present in app/arcgis.py
)
from .rendering import to_shapely_union, bbox_3857, prepare_clipped_shapes, make_geotiff_rgba
from .colors import color_from_code
from .kml import build_kml, write_kmz

logging.basicConfig(level=logging.INFO)
app = FastAPI(
    title="QLD Land Types → GeoTIFF + KMZ (Unified + Vegetation)",
    description="Single or bulk export from one box: Land Types & optional Vegetation (GeoTIFF/KMZ).",
    version="2.8.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Vegetation defaults (env-overridable)
VEG_SERVICE_URL_DEFAULT = os.getenv("VEG_SERVICE_URL", "https://spatial-gis.information.qld.gov.au/arcgis/rest/services/Biota/VegetationManagement/MapServer").strip()
VEG_LAYER_ID_DEFAULT = int(os.getenv("VEG_LAYER_ID", "109"))
VEG_NAME_FIELD_DEFAULT = os.getenv("VEG_NAME_FIELD", "CLASS_NAME").strip()
VEG_CODE_FIELD_DEFAULT = os.getenv("VEG_CODE_FIELD", "CLASS_CODE").strip() or None

def rgb_to_hex(rgb):
    r, g, b = rgb
    return "#{:02x}{:02x}{:02x}".format(int(r), int(g), int(b))

def _sanitize_filename(s: Optional[str]) -> str:
    base = "".join(c for c in (s or "").strip() if c.isalnum() or c in ("_", "-", ".", " "))
    return (base or "download").strip()

def _require_parcel_fc(lotplan: str) -> Dict[str, Any]:
    lotplan = (lotplan or "").strip().upper()
    if not lotplan:
        raise HTTPException(status_code=400, detail="lotplan is required")
    fc = fetch_parcel_geojson(lotplan)
    if not fc or not isinstance(fc, dict) or fc.get("type") != "FeatureCollection" or not fc.get("features"):
        raise HTTPException(status_code=404, detail=f"Parcel not found for lot/plan '{lotplan}'.")
    return fc

def _build_kml_compat(clipped, folder_label: str):
    for kw in ("folder_name", "doc_name", "document_name", "name"):
        try:
            return build_kml(clipped, color_fn=color_from_code, **{kw: folder_label})
        except TypeError as e:
            msg = str(e)
            if "unexpected keyword argument" in msg or "got multiple values" in msg:
                continue
            raise
    return build_kml(clipped, color_fn=color_from_code)

def _render_one_tiff_and_meta(lotplan: str, max_px: int) -> Tuple[bytes, Dict[str, Any]]:
    lotplan = (lotplan or "").strip().upper()
    parcel_fc = _require_parcel_fc(lotplan)
    parcel_union = to_shapely_union(parcel_fc)
    env = bbox_3857(parcel_union)
    lt_fc = fetch_landtypes_intersecting_envelope(env)
    clipped = prepare_clipped_shapes(parcel_fc, lt_fc)
    if not clipped:
        raise HTTPException(status_code=404, detail="No Land Types intersect this parcel.")
    tmpdir = tempfile.mkdtemp(prefix="geotiff_")
    out_path = os.path.join(tmpdir, f"{lotplan}_landtypes.tif")
    try:
        _ = make_geotiff_rgba(clipped, out_path, max_px=max_px)
        with open(out_path, "rb") as f:
            tiff_bytes = f.read()
    finally:
        try:
            if os.path.exists(out_path): os.remove(out_path)
            if os.path.isdir(tmpdir): os.rmdir(tmpdir)
        except Exception:
            pass
    west, south, east, north = parcel_union.bounds
    total_area_ha = sum(float(a_ha) for _, _, _, a_ha in clipped)
    meta = {"lotplan": lotplan, "bounds_epsg4326": [west, south, east, north], "area_ha_total": total_area_ha}
    return tiff_bytes, meta

def _standardise_code_name(fc: Dict[str, Any], code_field: Optional[str], name_field: str) -> Dict[str, Any]:
    feats = fc.get("features", [])
    for f in feats:
        props = f.get("properties") or {}
        code = str(props.get(code_field, "")).strip() if code_field else ""
        name = str(props.get(name_field, "")).strip() if name_field else ""
        if not code: code = name or "UNK"
        if not name: name = code
        f["properties"] = {"code": code, "name": name}
    fc["features"] = feats
    return fc

def _render_one_veg_kmz_and_meta(
    lotplan: str,
    env_3857,
    simplify_tolerance: float,
    veg_service_url: str,
    veg_layer_id: int,
    veg_name_field: str,
    veg_code_field: Optional[str],
    parcel_fc: Dict[str, Any],
) -> Tuple[bytes, Dict[str, Any]]:
    veg_fc = fetch_features_intersecting_envelope(veg_service_url, veg_layer_id, env_3857)
    veg_fc = _standardise_code_name(veg_fc, veg_code_field, veg_name_field)
    clipped = prepare_clipped_shapes(parcel_fc, veg_fc)
    if not clipped:
        raise HTTPException(status_code=404, detail="No Vegetation polygons intersect this parcel.")
    if simplify_tolerance and simplify_tolerance > 0:
        simplified = []
        for geom4326, code, name, area_ha in clipped:
            g2 = geom4326.simplify(simplify_tolerance, preserve_topology=True)
            if not g2.is_empty: simplified.append((g2, code, name, area_ha))
        clipped = simplified or clipped
    kml = _build_kml_compat(clipped, f"Vegetation – {lotplan}")
    tmpdir = tempfile.mkdtemp(prefix="vegkmz_")
    out_path = os.path.join(tmpdir, f"{lotplan}_vegetation.kmz")
    try:
        write_kmz(kml, out_path)
        with open(out_path, "rb") as f:
            kmz_bytes = f.read()
    finally:
        try:
            if os.path.exists(out_path): os.remove(out_path)
            if os.path.isdir(tmpdir): os.rmdir(tmpdir)
        except Exception:
            pass
    return kmz_bytes, {"lotplan": lotplan}

def _render_one_veg_tiff_and_meta(
    lotplan: str,
    env_3857,
    max_px: int,
    veg_service_url: str,
    veg_layer_id: int,
    veg_name_field: str,
    veg_code_field: Optional[str],
    parcel_fc: Dict[str, Any],
) -> Tuple[bytes, Dict[str, Any]]:
    veg_fc = fetch_features_intersecting_envelope(veg_service_url, veg_layer_id, env_3857)
    veg_fc = _standardise_code_name(veg_fc, veg_code_field, veg_name_field)
    clipped = prepare_clipped_shapes(parcel_fc, veg_fc)
    if not clipped:
        raise HTTPException(status_code=404, detail="No Vegetation polygons intersect this parcel.")
    tmpdir = tempfile.mkdtemp(prefix="vegtif_")
    out_path = os.path.join(tmpdir, f"{lotplan}_vegetation.tif")
    try:
        _ = make_geotiff_rgba(clipped, out_path, max_px=max_px)
        with open(out_path, "rb") as f:
            tiff_bytes = f.read()
    finally:
        try:
            if os.path.exists(out_path): os.remove(out_path)
            if os.path.isdir(tmpdir): os.rmdir(tmpdir)
        except Exception:
            pass
    return tiff_bytes, {"lotplan": lotplan}

@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logging.exception("Unhandled error during %s %s", request.method, request.url)
    return JSONResponse(status_code=500, content={"error": "internal_server_error", "detail": str(exc)})

@app.get("/", response_class=HTMLResponse)
def home():
    html = """<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>QLD Land Types → GeoTIFF + KMZ (Unified + Vegetation)</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" crossorigin=""/>
<style>
:root{--bg:#0b1220;--card:#121a2b;--text:#e8eefc;--muted:#9fb2d8;--accent:#6aa6ff}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:16px/1.45 system-ui,-apple-system,Segoe UI,Roboto,Inter,Arial,sans-serif}
.wrap{max-width:1100px;margin:28px auto;padding:0 16px}.card{background:var(--card);border:1px solid #1f2a44;border-radius:16px;box-shadow:0 10px 30px rgba(0,0,0,.25);padding:18px}
h1{margin:4px 0 10px;font-size:26px}p{margin:0 0 14px;color:var(--muted)}label{display:block;margin:10px 0 6px;color:var(--muted);font-size:14px}
input[type=text],input[type=number],textarea,select{width:100%;padding:10px 12px;border-radius:12px;border:1px solid #2b3960;background:#0e1526;color:var(--text)}
textarea{min-height:110px;resize:vertical}.row{display:flex;gap:12px;flex-wrap:wrap}.row>*{flex:1 1 200px}.btns{margin-top:12px;display:flex;gap:10px;flex-wrap:wrap}
button,.ghost{appearance:none;border:0;border-radius:12px;padding:10px 14px;font-weight:600;cursor:pointer}
button.primary{background:var(--accent);color:#071021}a.ghost{color:var(--accent);text-decoration:none;border:1px solid #294a86;background:#0d1730}
.note{margin-top:8px;font-size:13px;color:#89a3d6}#map{height:520px;border-radius:14px;margin-top:14px;border:1px solid #203055}
.out{margin-top:12px;border-top:1px solid #203055;padding-top:10px;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;white-space:pre-wrap}
.badge{display:inline-block;padding:.2rem .5rem;border-radius:999px;background:#11204a;color:#9fc1ff;font-size:12px;margin-left:8px}
.chip{display:inline-flex;align-items:center;gap:6px;padding:.2rem .6rem;border-radius:999px;background:#11204a;color:#9fc1ff;font-size:12px}
.muted{color:#9fb2d8}.box{border:1px solid #203055;border-radius:12px;padding:10px;background:#0e1526;margin-top:6px}
</style>
</head><body>
<div class="wrap"><div class="card">
  <h1>QLD Land Types <span class="badge">EPSG:4326</span> <span id="mode" class="chip">Mode: Single</span></h1>
  <p>Paste one or many <strong>Lot / Plan</strong> codes. Choose formats, optionally include <strong>Vegetation</strong> outputs.</p>

  <div class="row">
    <div style="flex: 2 1 420px;">
      <label for="items">Lot / Plan (single OR multiple — new line, comma, or semicolon separated)</label>
      <textarea id="items" placeholder="13SP181800
1RP12345
2RP54321"></textarea>
      <div class="muted" id="parseinfo">Detected 0 items.</div>
    </div>
    <div>
      <label for="fmt">Land Types format</label>
      <select id="fmt">
        <option value="tiff" selected>GeoTIFF</option>
        <option value="kmz">KMZ (clickable)</option>
        <option value="both">Both (ZIP)</option>
      </select>

      <label for="name">Name (single) or Prefix (bulk)</label>
      <input id="name" type="text" placeholder="e.g. UpperCoomera_13SP181800 or Job_4021" />

      <label for="maxpx">Max raster dimension (px) for GeoTIFF</label>
      <input id="maxpx" type="number" min="256" max="8192" value="4096" />

      <label for="simp">KMZ simplify tolerance (deg) <span class="muted">(try 0.00005 ≈ 5 m)</span></label>
      <input id="simp" type="number" step="0.00001" min="0" max="0.001" value="0" />
    </div>
  </div>

  <div class="box">
    <label><input type="checkbox" id="veg_tiff"> Include Vegetation <b>GeoTIFF</b></label>
    <label><input type="checkbox" id="veg_kmz"> Include Vegetation <b>KMZ</b></label>
    <div class="row">
      <div><label for="veg_url">Vegetation MapServer URL</label><input id="veg_url" type="text" value="https://spatial-gis.information.qld.gov.au/arcgis/rest/services/Biota/VegetationManagement/MapServer"/></div>
      <div><label for="veg_layer">Layer ID</label><input id="veg_layer" type="number" min="0" value="109"/></div>
    </div>
    <div class="row">
      <div><label for="veg_name">Vegetation name field</label><input id="veg_name" type="text" value="CLASS_NAME"/></div>
      <div><label for="veg_code">Vegetation code field (optional)</label><input id="veg_code" type="text" value="CLASS_CODE"/></div>
    </div>
    <div class="muted">Tip: If you're unsure of the field names, try leaving code empty and set name to the display field.</div>
  </div>

  <div class="btns">
    <button class="primary" id="btn-export">Export</button>
    <a class="ghost" id="btn-json" href="#">Preview JSON (single)</a>
    <a class="ghost" id="btn-load" href="#">Load on Map (single)</a>
  </div>

  <div class="note">API docs: <a href="/docs">/docs</a>.  JSON/Map actions are enabled only when exactly one code is provided.</div>
  <div id="map"></div><div id="out" class="out"></div>
</div></div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" crossorigin=""></script>
<script>
const $items = document.getElementById('items'), $fmt = document.getElementById('fmt'),
      $name = document.getElementById('name'), $max = document.getElementById('maxpx'),
      $simp = document.getElementById('simp'), $mode = document.getElementById('mode'),
      $out = document.getElementById('out'), $parseinfo = document.getElementById('parseinfo'),
      $btnExport = document.getElementById('btn-export'), $btnJson = document.getElementById('btn-json'),
      $btnLoad = document.getElementById('btn-load'),
      $vegT = document.getElementById('veg_tiff'), $vegK = document.getElementById('veg_kmz'),
      $vegURL = document.getElementById('veg_url'), $vegLayer = document.getElementById('veg_layer'),
      $vegName = document.getElementById('veg_name'), $vegCode = document.getElementById('veg_code');

function normText(s){ return (s || '').trim(); }
function parseItems(text){
  const raw = (text || '').split(/\r?\n|,|;/);
  const clean = raw.map(s => s.trim().toUpperCase()).filter(Boolean);
  const seen = new Set(); const out = [];
  for(const v of clean){ if(!seen.has(v)){ seen.add(v); out.push(v); } }
  return out;
}

// Map
const map = L.map('map', { zoomControl: true });
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { attribution: '&copy; OpenStreetMap' }).addTo(map);
map.setView([-23.5, 146.0], 5);
let parcelLayer=null, ltLayer=null;
function styleForCode(code, colorHex){ return { color:'#0c1325', weight:1, fillColor:colorHex, fillOpacity:0.6 }; }
function clearLayers(){ if(parcelLayer){ map.removeLayer(parcelLayer); parcelLayer=null; } if(ltLayer){ map.removeLayer(ltLayer); ltLayer=null; } }

function updateMode(){
  const items = parseItems($items.value);
  const n = items.length;
  const dupInfo = (normText($items.value) && n === 0) ? " (duplicates/invalid removed)" : "";
  $parseinfo.textContent = `Detected ${n} item${n===1?'':'s'}.` + dupInfo;

  if (n === 1){
    $mode.textContent = "Mode: Single";
    $btnJson.style.pointerEvents='auto'; $btnJson.style.opacity='1';
    $btnLoad.style.pointerEvents='auto'; $btnLoad.style.opacity='1';
  } else {
    $mode.textContent = `Mode: Bulk (${n})`;
    $btnJson.style.pointerEvents='none'; $btnJson.style.opacity='.5';
    $btnLoad.style.pointerEvents='none'; $btnLoad.style.opacity='.5';
  }
}

async function downloadBlobAs(res, filename){
  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = filename;
  document.body.appendChild(a); a.click(); a.remove();
  URL.revokeObjectURL(url);
}

function mkVectorUrl(lotplan){ return `/vector?lotplan=${encodeURIComponent(lotplan)}`; }

async function loadVector(){
  const items = parseItems($items.value);
  if (items.length !== 1){ $out.textContent = 'Provide exactly one Lot/Plan to load map.'; return; }
  const lot = items[0];
  $out.textContent = 'Loading vector data…';
  try{
    const res = await fetch(mkVectorUrl(lot)); const data = await res.json();
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

async function previewJson(){
  const items = parseItems($items.value);
  if (items.length !== 1){ $out.textContent = 'Provide exactly one Lot/Plan for JSON preview.'; return; }
  const lot = items[0];
  $out.textContent='Requesting JSON summary…';
  try{
    const url = `/export?lotplan=${encodeURIComponent(lot)}&max_px=${encodeURIComponent(($max.value || '4096').trim())}&download=false`;
    const res = await fetch(url); const txt = await res.text();
    try{ const data = JSON.parse(txt); $out.textContent = JSON.stringify(data, null, 2);}catch{ $out.textContent = `Error ${res.status}: ${txt}`; }
  }catch(err){ $out.textContent = 'Network error: ' + err; }
}

async function exportAny(){
  const items = parseItems($items.value);
  if (!items.length){ $out.textContent = 'Enter at least one Lot/Plan.'; return; }
  const fmt = $fmt.value;
  const max_px = parseInt($max.value || '4096', 10);
  const simp = parseFloat($simp.value || '0') || 0;
  const name = normText($name.value) || null;

  const body = {
    format: fmt, max_px: max_px, simplify_tolerance: simp,
    include_veg_tiff: $vegT.checked, include_veg_kmz: $vegK.checked,
    veg_service_url: normText($vegURL.value) || null,
    veg_layer_id: $vegLayer.value ? parseInt($vegLayer.value, 10) : null,
    veg_name_field: normText($vegName.value) || null,
    veg_code_field: normText($vegCode.value) || null,
  };
  if (items.length === 1){
    body.lotplan = items[0];
    if (name) body.filename = name;
  } else {
    body.lotplans = items;
    if (name) body.filename_prefix = name;
  }

  $out.textContent = items.length === 1 ? 'Exporting…' : `Exporting ${items.length} items…`;
  try{
    const res = await fetch('/export/any', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
    const disp = res.headers.get('content-disposition') || '';
    if (!res.ok){ const txt = await res.text(); $out.textContent = `Error ${res.status}: ${txt}`; return; }
    const m = /filename="([^"]+)"/i.exec(disp);
    let dl = m ? m[1] : `export_${Date.now()}`;
    if (items.length > 1 && name && !dl.startsWith(name)) dl = `${name}_${dl}`;
    await downloadBlobAs(res, dl);
    $out.textContent = 'Download complete.';
  }catch(err){ $out.textContent = 'Network error: ' + err; }
}

$items.addEventListener('input', updateMode);
document.getElementById('btn-load').addEventListener('click', (e)=>{ e.preventDefault(); loadVector(); });
document.getElementById('btn-json').addEventListener('click', (e)=>{ e.preventDefault(); previewJson(); });
document.getElementById('btn-export').addEventListener('click', (e)=>{ e.preventDefault(); exportAny(); });
updateMode(); setTimeout(()=>{ $items.focus(); }, 50);
</script>
</body></html>"""
    html = (html
            .replace("https://spatial-gis.information.qld.gov.au/arcgis/rest/services/Biota/VegetationManagement/MapServer", VEG_SERVICE_URL_DEFAULT)
            .replace("109", str(VEG_LAYER_ID_DEFAULT))
            .replace("CLASS_NAME", VEG_NAME_FIELD_DEFAULT)
            .replace("CLASS_CODE", "" if VEG_CODE_FIELD_DEFAULT is None else VEG_CODE_FIELD_DEFAULT))
    return HTMLResponse(html)

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/export")
def export_geotiff(
    lotplan: str = Query(...),
    max_px: int = Query(4096, ge=256, le=8192),
    download: bool = Query(True),
    filename: Optional[str] = Query(None),
):
    try:
        lotplan = lotplan.strip().upper()
        if download:
            tiff_bytes, meta = _render_one_tiff_and_meta(lotplan, max_px)
            name = _sanitize_filename(filename) if filename else f"{meta['lotplan']}_landtypes"
            if not name.lower().endswith(".tif"): name += ".tif"
            return StreamingResponse(BytesIO(tiff_bytes), media_type="image/tiff",
                                     headers={"Content-Disposition": f'attachment; filename="{name}"'})
        else:
            _bytes, meta = _render_one_tiff_and_meta(lotplan, max_px)
            return JSONResponse({"lotplan": meta["lotplan"], **meta})
    except HTTPException:
        raise
    except Exception as e:
        logging.exception("Export error")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/vector")
def vector_geojson(lotplan: str = Query(...)):
    try:
        lotplan = lotplan.strip().upper()
        parcel_fc = _require_parcel_fc(lotplan)
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
            color_hex = rgb_to_hex(color_from_code(code))
            features.append({
                "type": "Feature",
                "geometry": shp_mapping(geom4326),
                "properties": {"code": code, "name": name, "area_ha": float(area_ha), "color_hex": color_hex}
            })
            legend_map.setdefault(code, {"code": code, "name": name, "color_hex": color_hex, "area_ha": 0.0})
            legend_map[code]["area_ha"] += float(area_ha)

        union_bounds = to_shapely_union({
            "type":"FeatureCollection",
            "features":[{"type":"Feature","geometry":f["geometry"],"properties":{}} for f in features]
        }).bounds
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
    lotplan: str = Query(...),
    simplify_tolerance: float = Query(0.0, ge=0.0, le=0.001),
    filename: Optional[str] = Query(None),
):
    try:
        lotplan = lotplan.strip().upper()
        parcel_fc = _require_parcel_fc(lotplan)
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
                if not g2.is_empty: simplified.append((g2, code, name, area_ha))
            clipped = simplified or clipped
        kml = _build_kml_compat(clipped, f"QLD Land Types – {lotplan}")
        tmpdir = tempfile.mkdtemp(prefix="kmz_")
        out_path = os.path.join(tmpdir, f"{lotplan}_landtypes.kmz")
        try:
            write_kmz(kml, out_path)
            with open(out_path, "rb") as f:
                kmz_bytes = f.read()
        finally:
            try:
                if os.path.exists(out_path): os.remove(out_path)
                if os.path.isdir(tmpdir): os.rmdir(tmpdir)
            except Exception:
                pass
        name = _sanitize_filename(filename) if filename else f"{lotplan}_landtypes"
        if not name.lower().endswith(".kmz"): name += ".kmz"
        return StreamingResponse(BytesIO(kmz_bytes), media_type="application/vnd.google-earth.kmz",
                                 headers={"Content-Disposition": f'attachment; filename="{name}"'})
    except HTTPException:
        raise
    except Exception as e:
        logging.exception("KMZ export error")
        raise HTTPException(status_code=500, detail=str(e))

class FormatEnum(str, Enum):
    tiff = "tiff"
    kmz = "kmz"
    both = "both"

class ExportAnyRequest(BaseModel):
    lotplan: Optional[str] = Field(None)
    lotplans: Optional[List[str]] = Field(None)
    max_px: int = Field(4096, ge=256, le=8192)
    format: FormatEnum = Field(FormatEnum.tiff)
    filename: Optional[str] = Field(None)
    filename_prefix: Optional[str] = Field(None)
    simplify_tolerance: float = Field(0.0, ge=0.0, le=0.001)
    include_veg_tiff: bool = False
    include_veg_kmz: bool = False
    veg_service_url: Optional[str] = None
    veg_layer_id: Optional[int] = None
    veg_name_field: Optional[str] = None
    veg_code_field: Optional[str] = None

@app.post("/export/any")
def export_any(payload: ExportAnyRequest = Body(...)):
    items: List[str] = []
    if payload.lotplans:
        seen = set()
        for lp in (lp.strip().upper() for lp in payload.lotplans):
            if not lp: continue
            if lp in seen: continue
            seen.add(lp); items.append(lp)
    if payload.lotplan:
        lp = payload.lotplan.strip().upper()
        if lp and lp not in items:
            items.append(lp)
    if not items:
        raise HTTPException(status_code=400, detail="Provide lotplan or lotplans.")

    want_veg = payload.include_veg_tiff or payload.include_veg_kmz
    veg_url = (payload.veg_service_url or VEG_SERVICE_URL_DEFAULT or "").strip()
    veg_layer = payload.veg_layer_id if payload.veg_layer_id is not None else VEG_LAYER_ID_DEFAULT
    veg_name = (payload.veg_name_field or VEG_NAME_FIELD_DEFAULT or "").strip()
    veg_code = (payload.veg_code_field or VEG_CODE_FIELD_DEFAULT or "").strip() or None
    if want_veg:
        if not veg_url or veg_layer is None or not veg_name:
            raise HTTPException(status_code=400, detail="Vegetation enabled but veg_service_url, veg_layer_id, or veg_name_field missing.")

    multi_files = (len(items) > 1) or (payload.format == FormatEnum.both) or want_veg
    prefix = _sanitize_filename(payload.filename_prefix) if payload.filename_prefix else None

    if not multi_files:
        lp = items[0]
        if payload.format == FormatEnum.tiff:
            tiff_bytes, _ = _render_one_tiff_and_meta(lp, payload.max_px)
            name = _sanitize_filename(payload.filename) if payload.filename else f"{lp}_landtypes"
            if not name.lower().endswith(".tif"): name += ".tif"
            return StreamingResponse(BytesIO(tiff_bytes), media_type="image/tiff",
                                     headers={"Content-Disposition": f'attachment; filename="{name}"'})
        if payload.format == FormatEnum.kmz:
            parcel_fc = _require_parcel_fc(lp); parcel_union = to_shapely_union(parcel_fc); env = bbox_3857(parcel_union)
            lt_fc = fetch_landtypes_intersecting_envelope(env)
            clipped = prepare_clipped_shapes(parcel_fc, lt_fc)
            if not clipped: raise HTTPException(status_code=404, detail="No Land Types intersect this parcel.")
            if payload.simplify_tolerance and payload.simplify_tolerance > 0:
                simplified = []
                for geom4326, code, name, area_ha in clipped:
                    g2 = geom4326.simplify(payload.simplify_tolerance, preserve_topology=True)
                    if not g2.is_empty: simplified.append((g2, code, name, a_ha))
                clipped = simplified or clipped
            kml = _build_kml_compat(clipped, f"QLD Land Types – {lp}")
            tmpdir = tempfile.mkdtemp(prefix="kmz_"); out_path = os.path.join(tmpdir, f"{lp}_landtypes.kmz")
            try:
                write_kmz(kml, out_path); kmz_bytes = open(out_path, "rb").read()
            finally:
                try:
                    if os.path.exists(out_path): os.remove(out_path)
                    if os.path.isdir(tmpdir): os.rmdir(tmpdir)
                except Exception: pass
            name = _sanitize_filename(payload.filename) if payload.filename else f"{lp}_landtypes"
            if not name.lower().endswith(".kmz"): name += ".kmz"
            return StreamingResponse(BytesIO(kmz_bytes), media_type="application/vnd.google-earth.kmz",
                                     headers={"Content-Disposition": f'attachment; filename="{name}"'})
        raise HTTPException(status_code=400, detail="Unsupported format for single export.")

    zip_buf = BytesIO()
    manifest_rows: List[Dict[str, Any]] = []

    with zipfile.ZipFile(zip_buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for lp in items:
            row: Dict[str, Any] = {"lotplan": lp}
            try:
                parcel_fc = _require_parcel_fc(lp)
                parcel_union = to_shapely_union(parcel_fc)
                env = bbox_3857(parcel_union)
            except HTTPException as e:
                row.update({"status": f"error:{e.status_code}", "message": e.detail})
                manifest_rows.append(row); continue
            except Exception as e:
                row.update({"status": "error:500", "message": str(e)})
                manifest_rows.append(row); continue

            if payload.format in (FormatEnum.tiff, FormatEnum.both):
                try:
                    tiff_bytes, _ = _render_one_tiff_and_meta(lp, payload.max_px)
                    name_tif = f"{(prefix+'_') if prefix else ''}{lp}_landtypes.tif"
                    zf.writestr(name_tif, tiff_bytes)
                    row["file_tiff"] = name_tif; row["status_tiff"] = "ok"
                except Exception as e:
                    row["status_tiff"] = f"error:{getattr(e, 'status_code', 500)}"; row["tiff_message"] = str(e)

            if payload.format in (FormatEnum.kmz, FormatEnum.both):
                try:
                    lt_fc = fetch_landtypes_intersecting_envelope(env)
                    clipped = prepare_clipped_shapes(parcel_fc, lt_fc)
                    if not clipped:
                        raise HTTPException(status_code=404, detail="No Land Types intersect this parcel.")
                    if payload.simplify_tolerance and payload.simplify_tolerance > 0:
                        simplified = []
                        for geom4326, code, name, area_ha in clipped:
                            g2 = geom4326.simplify(payload.simplify_tolerance, preserve_topology=True)
                            if not g2.is_empty: simplified.append((g2, code, name, area_ha))
                        clipped = simplified or clipped
                    kml = _build_kml_compat(clipped, f"QLD Land Types – {lp}")
                    name_kmz = f"{(prefix+'_') if prefix else ''}{lp}_landtypes.kmz"
                    mem = BytesIO()
                    with zipfile.ZipFile(mem, "w", compression=zipfile.ZIP_DEFLATED) as ztmp:
                        ztmp.writestr("doc.kml", kml.encode("utf-8"))
                    zf.writestr(name_kmz, mem.getvalue())
                    row["file_kmz"] = name_kmz; row["status_kmz"] = "ok"
                except Exception as e:
                    row["status_kmz"] = f"error:{getattr(e, 'status_code', 500)}"; row["kmz_message"] = str(e)

            if want_veg and payload.include_veg_tiff:
                try:
                    veg_tiff, _ = _render_one_veg_tiff_and_meta(
                        lp, env, payload.max_px, veg_url, veg_layer, veg_name, veg_code, parcel_fc
                    )
                    name_vtif = f"{(prefix+'_') if prefix else ''}{lp}_vegetation.tif"
                    zf.writestr(name_vtif, veg_tiff)
                    row["file_veg_tiff"] = name_vtif; row["status_veg_tiff"] = "ok"
                except Exception as e:
                    row["status_veg_tiff"] = f"error:{getattr(e, 'status_code', 500)}"; row["veg_tiff_message"] = str(e)

            if want_veg and payload.include_veg_kmz:
                try:
                    veg_kmz, _ = _render_one_veg_kmz_and_meta(
                        lp, env, payload.simplify_tolerance, veg_url, veg_layer, veg_name, veg_code, parcel_fc
                    )
                    name_vkmz = f"{(prefix+'_') if prefix else ''}{lp}_vegetation.kmz"
                    zf.writestr(name_vkmz, veg_kmz)
                    row["file_veg_kmz"] = name_vkmz; row["status_veg_kmz"] = "ok"
                except Exception as e:
                    row["status_veg_kmz"] = f"error:{getattr(e, 'status_code', 500)}"; row["veg_kmz_message"] = str(e)

            manifest_rows.append(row)

        mem_csv = BytesIO()
        import csv as _csv
        fieldnames = [
            "lotplan",
            "status_tiff","file_tiff","tiff_message",
            "status_kmz","file_kmz","kmz_message",
            "status_veg_tiff","file_veg_tiff","veg_tiff_message",
            "status_veg_kmz","file_veg_kmz","veg_kmz_message",
        ]
        writer = _csv.DictWriter(mem_csv, fieldnames=fieldnames); writer.writeheader()
        for row in manifest_rows:
            for k in fieldnames: row.setdefault(k, "")
            writer.writerow(row)
        zf.writestr("manifest.csv", mem_csv.getvalue())

    zip_buf.seek(0)
    stamp = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    base = f"{prefix+'_' if prefix else ''}landtypes_{payload.format.value}"
    return StreamingResponse(zip_buf, media_type="application/zip",
                             headers={"Content-Disposition": f'attachment; filename="{base}_with_veg_{stamp}.zip"'})
