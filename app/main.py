# app/main.py
import os, io, csv, zipfile, tempfile, logging, datetime as dt
from io import BytesIO
from enum import Enum
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, HTTPException, Query, Body, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse, StreamingResponse
from pydantic import BaseModel, Field

from .config import VEG_SERVICE_URL_DEFAULT, VEG_LAYER_ID_DEFAULT, VEG_NAME_FIELD_DEFAULT, VEG_CODE_FIELD_DEFAULT
from .arcgis import (
    fetch_parcel_geojson,
    fetch_landtypes_intersecting_envelope,
    fetch_features_intersecting_envelope,
    normalize_lotplan,
)
from .geometry import to_shapely_union, bbox_3857, prepare_clipped_shapes
from .raster import make_geotiff_rgba
from .colors import color_from_code
from .kml import build_kml, build_kml_folders, write_kmz

logging.basicConfig(level=logging.INFO)
app = FastAPI(
    title="QLD Land Types (rewritten)",
    description="Unified single/bulk exporter for Land Types + optional Vegetation (GeoTIFF, KMZ).",
    version="3.0.2",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

def _hex(rgb):
    r,g,b = rgb
    return "#{:02x}{:02x}{:02x}".format(int(r),int(g),int(b))

def _sanitize_filename(s: Optional[str]) -> str:
    base = "".join(c for c in (s or "").strip() if c.isalnum() or c in ("_", "-", ".", " "))
    return (base or "download").strip()

@app.head("/")
def home_head(): return Response(status_code=200)

@app.get("/", response_class=HTMLResponse)
def home():
    # Replace configuration placeholders with actual values
    html_template = """<!doctype html>
<html><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>QLD Land Types (rewritten)</title>
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
</head>
<body>
<div class="wrap"><div class="card">
  <h1>QLD Land Types <span class="badge">EPSG:4326</span> <span id="mode" class="chip">Mode: Single</span></h1>
  <p>Paste one or many <strong>Lot / Plan</strong> codes. Choose formats, optionally include <strong>Vegetation</strong>.</p>

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
      <div><label for="veg_url">Vegetation MapServer URL</label><input id="veg_url" type="text" value="%VEG_URL%"/></div>
      <div><label for="veg_layer">Layer ID</label><input id="veg_layer" type="number" min="0" value="%VEG_LAYER%"/></div>
    </div>
    <div class="row">
      <div><label for="veg_name">Vegetation name field</label><input id="veg_name" type="text" value="%VEG_NAME%"/></div>
      <div><label for="veg_code">Vegetation code field (optional)</label><input id="veg_code" type="text" value="%VEG_CODE%"/></div>
    </div>
  </div>

  <div class="btns">
    <button class="primary" id="btn-export">Export</button>
    <a class="ghost" id="btn-json" href="#">Preview JSON (single)</a>
    <a class="ghost" id="btn-load" href="#">Load on Map (single)</a>
  </div>

  <div class="note">JSON/Map actions require exactly one lot/plan. API docs: <a href="/docs">/docs</a></div>
  <div id="map"></div><div id="out" class="out"></div>
</div></div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" crossorigin="" defer></script>
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
  const src = (text || '')
    .toUpperCase()
    .replace(/\bLOT\b/g,' ')
    .replace(/\bPLAN\b/g,' ')
    .replace(/\bON\b/g,' ')
    .replace(/[^A-Z0-9]+/g,' ');
  const seen = new Set(); const out = [];
  const rx = /(\\d+)\\s*([A-Z]+[A-Z0-9]+)/g; let m;
  while((m = rx.exec(src)) !== null){
    const code = `${m[1]}${m[2]}`;
    if(!seen.has(code)){ seen.add(code); out.push(code); }
  }
  return out;
}
function updateMode(){
  const items = parseItems($items.value);
  const n = items.length;
  $parseinfo.textContent = `Detected ${n} item${n===1?'':'s'}.`;
  if (n === 1){
    $mode.textContent = "Mode: Single"; $btnJson.style.opacity='1'; $btnJson.style.pointerEvents='auto'; $btnLoad.style.opacity='1'; $btnLoad.style.pointerEvents='auto';
  } else {
    $mode.textContent = `Mode: Bulk (${n})`; $btnJson.style.opacity='.5'; $btnJson.style.pointerEvents='none'; $btnLoad.style.opacity='.5'; $btnLoad.style.pointerEvents='none';
  }
}

let map=null, parcelLayer=null, ltLayer=null;
function ensureMap(){
  try{
    if (map) return;
    if (!window.L) return;
    map = L.map('map', { zoomControl: true });
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { attribution: '&copy; OpenStreetMap' }).addTo(map);
    map.setView([-23.5, 146.0], 5);
  }catch(e){ console.warn('Map init failed:', e); }
}
function styleForCode(code, colorHex){ return { color:'#0c1325', weight:1, fillColor:colorHex, fillOpacity:0.6 }; }
function clearLayers(){ try{ if(map && parcelLayer){ map.removeLayer(parcelLayer); parcelLayer=null; } if(map && ltLayer){ map.removeLayer(ltLayer); ltLayer=null; } }catch{} }
function mkVectorUrl(lotplan){ return `/vector?lotplan=${encodeURIComponent(lotplan)}`; }

async function loadVector(){
  const items = parseItems($items.value);
  if (items.length !== 1){ $out.textContent = 'Provide exactly one Lot/Plan to load map.'; return; }
  ensureMap(); if (!map){ $out.textContent = 'Map library not loaded yet. Try again in a moment.'; return; }
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
  const body = {
    format: $fmt.value,
    max_px: parseInt($max.value || '4096', 10),
    simplify_tolerance: parseFloat($simp.value || '0') || 0,
    include_veg_tiff: !!$vegT.checked, include_veg_kmz: !!$vegK.checked,
    veg_service_url: normText($vegURL.value||''), veg_layer_id: $vegLayer.value ? parseInt($vegLayer.value,10) : null,
    veg_name_field: normText($vegName.value||''), veg_code_field: normText($vegCode.value||''),
  };
  const name = normText($name.value) || null;
  if (items.length === 1){ body.lotplan = items[0]; if (name) body.filename = name; } else { body.lotplans = items; if (name) body.filename_prefix = name; }
  $out.textContent = items.length === 1 ? 'Exporting…' : `Exporting ${items.length} items…`;
  try{
    const res = await fetch('/export/any', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
    const disp = res.headers.get('content-disposition') || '';
    const ok = res.ok;
    const blob = await res.blob();
    if (!ok){ const txt = await blob.text(); $out.textContent = `Error ${res.status}: ${txt}`; return; }
    const m = /filename="([^"]+)"/i.exec(disp);
    let dl = m ? m[1] : `export_${Date.now()}`;
    if (items.length > 1 && name && !dl.startsWith(name)) dl = `${name}_${dl}`;
    const url = URL.createObjectURL(blob); const a = document.createElement('a'); a.href = url; a.download = dl; document.body.appendChild(a); a.click(); a.remove(); URL.revokeObjectURL(url);
    $out.textContent = 'Download complete.';
  }catch(err){ $out.textContent = 'Network error: ' + err; }
}

$items.addEventListener('input', updateMode);
$items.addEventListener('keyup', updateMode);
$items.addEventListener('change', updateMode);
document.getElementById('btn-load').addEventListener('click', (e)=>{ e.preventDefault(); loadVector(); });
document.getElementById('btn-json').addEventListener('click', (e)=>{ e.preventDefault(); previewJson(); });
document.getElementById('btn-export').addEventListener('click', (e)=>{ e.preventDefault(); exportAny(); });
updateMode(); setTimeout(()=>{ ensureMap(); $items.focus(); }, 30);
</script>
</body></html>"""
    
    # Replace configuration placeholders with actual values
    return html_template.replace("%VEG_URL%", VEG_SERVICE_URL_DEFAULT).replace("%VEG_LAYER%", str(VEG_LAYER_ID_DEFAULT)).replace("%VEG_NAME%", VEG_NAME_FIELD_DEFAULT).replace("%VEG_CODE%", VEG_CODE_FIELD_DEFAULT or "")

@app.get("/health")
def health(): return {"ok": True}

@app.get("/export")
def export_geotiff(lotplan: str = Query(...), max_px: int = Query(4096, ge=256, le=8192), download: bool = Query(True)):
    lotplan = normalize_lotplan(lotplan)
    parcel_fc = fetch_parcel_geojson(lotplan)
    parcel_union = to_shapely_union(parcel_fc)
    env = bbox_3857(parcel_union)
    lt_fc = fetch_landtypes_intersecting_envelope(env)
    clipped = prepare_clipped_shapes(parcel_fc, lt_fc)
    if not clipped:
        if download: raise HTTPException(status_code=404, detail="No Land Types intersect this parcel.")
        return JSONResponse({"lotplan": lotplan, "error": "No Land Types intersect this parcel."}, status_code=404)
    tmpdir = tempfile.mkdtemp(prefix="tiff_")
    out_path = os.path.join(tmpdir, f"{lotplan}_landtypes.tif")
    result = make_geotiff_rgba(clipped, out_path, max_px=max_px)
    if download:
        data = open(out_path, "rb").read()
        os.remove(out_path); os.rmdir(tmpdir)
        return StreamingResponse(
            BytesIO(data),
            media_type="image/tiff",
            headers={"Content-Disposition": f'attachment; filename="{lotplan}_landtypes.tif"'},
        )
    else:
        public = {k:v for k,v in result.items() if k != "path"}
        legend = {}
        for _g, code, name, area_ha in clipped:
            c = _hex(color_from_code(code))
            legend.setdefault(code, {"code":code,"name":name,"color_hex":c,"area_ha":0.0})
            legend[code]["area_ha"] += float(area_ha)
        return JSONResponse({"lotplan": lotplan, "legend": sorted(legend.values(), key=lambda d: (-d["area_ha"], d["code"])), **public})

@app.get("/vector")
def vector_geojson(lotplan: str = Query(...)):
    lotplan = normalize_lotplan(lotplan)
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
        color_hex = _hex(color_from_code(code))
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

@app.get("/export_kmz")
def export_kmz(lotplan: str = Query(...), simplify_tolerance: float = Query(0.0, ge=0.0, le=0.001)):
    lotplan = normalize_lotplan(lotplan)
    parcel_fc = fetch_parcel_geojson(lotplan)
    parcel_union = to_shapely_union(parcel_fc)
    env = bbox_3857(parcel_union)
    lt_fc = fetch_landtypes_intersecting_envelope(env)
    clipped = prepare_clipped_shapes(parcel_fc, lt_fc)
    if not clipped: raise HTTPException(status_code=404, detail="No Land Types intersect this parcel.")

    if simplify_tolerance and simplify_tolerance > 0:
        simplified = []
        for geom4326, code, name, area_ha in clipped:
            g2 = geom4326.simplify(simplify_tolerance, preserve_topology=True)
            if not g2.is_empty: simplified.append((g2, code, name, area_ha))
        clipped = simplified or clipped

    kml = build_kml(clipped, color_fn=color_from_code, folder_name=f"QLD Land Types – {lotplan}")
    tmpdir = tempfile.mkdtemp(prefix="kmz_")
    out_path = os.path.join(tmpdir, f"{lotplan}_landtypes.kmz")
    write_kmz(kml, out_path)
    data = open(out_path, "rb").read()
    os.remove(out_path); os.rmdir(tmpdir)
    return StreamingResponse(
        BytesIO(data),
        media_type="application/vnd.google-earth.kmz",
        headers={"Content-Disposition": f'attachment; filename="{lotplan}_landtypes.kmz"'},
    )

@app.get("/export_kml")
def export_kml(
    lotplan: str = Query(...),
    simplify_tolerance: float = Query(0.0, ge=0.0, le=0.001),
    include_veg: bool = Query(False),
    veg_service_url: Optional[str] = Query(VEG_SERVICE_URL_DEFAULT, alias="veg_url"),
    veg_layer_id: Optional[int] = Query(VEG_LAYER_ID_DEFAULT, alias="veg_layer"),
    veg_name_field: Optional[str] = Query(VEG_NAME_FIELD_DEFAULT, alias="veg_name"),
    veg_code_field: Optional[str] = Query(VEG_CODE_FIELD_DEFAULT, alias="veg_code"),
):
    lotplan = normalize_lotplan(lotplan)
    parcel_fc = fetch_parcel_geojson(lotplan)
    parcel_union = to_shapely_union(parcel_fc)
    env = bbox_3857(parcel_union)

    lt_fc = fetch_landtypes_intersecting_envelope(env)
    lt_clipped = prepare_clipped_shapes(parcel_fc, lt_fc)
    if not lt_clipped:
        raise HTTPException(status_code=404, detail="No Land Types intersect this parcel.")

    veg_clipped = []
    if include_veg and veg_service_url and veg_layer_id is not None:
        veg_fc = fetch_features_intersecting_envelope(
            veg_service_url, veg_layer_id, env, out_fields="*"
        )
        # standardise fields
        for f in veg_fc.get("features", []):
            props = f.get("properties") or {}
            code = str(props.get(veg_code_field or "code") or props.get("code") or "").strip()
            name = str(props.get(veg_name_field or "name") or props.get("name") or code).strip()
            props["code"] = code or name or "UNK"
            props["name"] = name or code or "Unknown"
        veg_clipped = prepare_clipped_shapes(parcel_fc, veg_fc)

    if simplify_tolerance and simplify_tolerance > 0:
        def _simp(data):
            out = []
            for geom4326, code, name, area_ha in data:
                g2 = geom4326.simplify(simplify_tolerance, preserve_topology=True)
                if not g2.is_empty:
                    out.append((g2, code, name, area_ha))
            return out or data
        lt_clipped = _simp(lt_clipped)
        if veg_clipped:
            veg_clipped = _simp(veg_clipped)

    if veg_clipped:
        kml = build_kml_folders(
            [
                (lt_clipped, color_from_code, f"Land Types – {lotplan}"),
                (veg_clipped, color_from_code, f"Vegetation – {lotplan}"),
            ],
            doc_name=f"QLD Export – {lotplan}",
        )
    else:
        kml = build_kml(lt_clipped, color_fn=color_from_code, folder_name=f"QLD Land Types – {lotplan}")

    filename = f"{lotplan}_landtypes" + ("_veg" if veg_clipped else "") + ".kml"
    return Response(
        content=kml,
        media_type="application/vnd.google-earth.kml+xml",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

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
        for lp in (normalize_lotplan(lp) for lp in payload.lotplans):
            if not lp: continue
            if lp in seen: continue
            seen.add(lp); items.append(lp)
    if payload.lotplan:
        lp = normalize_lotplan(payload.lotplan)
        if lp and lp not in items:
            items.append(lp)
    if not items: raise HTTPException(status_code=400, detail="Provide lotplan or lotplans.")

    want_veg = payload.include_veg_tiff or payload.include_veg_kmz
    veg_url = (payload.veg_service_url or VEG_SERVICE_URL_DEFAULT or "").strip()
    veg_layer = payload.veg_layer_id if payload.veg_layer_id is not None else VEG_LAYER_ID_DEFAULT
    veg_name = (payload.veg_name_field or VEG_NAME_FIELD_DEFAULT or "").strip()
    veg_code = (payload.veg_code_field or VEG_CODE_FIELD_DEFAULT or "").strip() or None
    if want_veg and (not veg_url or veg_layer is None or not veg_name):
        raise HTTPException(status_code=400, detail="Vegetation enabled but veg_service_url, veg_layer_id, or veg_name_field missing.")

    prefix = _sanitize_filename(payload.filename_prefix) if payload.filename_prefix else None
    multi_files = (len(items) > 1) or (payload.format == FormatEnum.both) or want_veg

    if not multi_files:
        lp = items[0]
        if payload.format == FormatEnum.tiff:
            parcel_fc = fetch_parcel_geojson(lp); parcel_union = to_shapely_union(parcel_fc); env = bbox_3857(parcel_union)
            thematic_fc = fetch_landtypes_intersecting_envelope(env)
            clipped = prepare_clipped_shapes(parcel_fc, thematic_fc)
            if not clipped: raise HTTPException(status_code=404, detail="No Land Types intersect this parcel.")
            tmpdir = tempfile.mkdtemp(prefix="tif_"); out_path = os.path.join(tmpdir, f"{lp}_landtypes.tif")
            _ = make_geotiff_rgba(clipped, out_path, max_px=payload.max_px)
            data = open(out_path, "rb").read(); os.remove(out_path); os.rmdir(tmpdir)
            fname = _sanitize_filename(payload.filename) if payload.filename else f"{lp}_landtypes"
            if not fname.lower().endswith(".tif"): fname += ".tif"
            return StreamingResponse(
                BytesIO(data),
                media_type="image/tiff",
                headers={"Content-Disposition": f'attachment; filename="{fname}"'},
            )
        elif payload.format == FormatEnum.kmz:
            parcel_fc = fetch_parcel_geojson(lp); parcel_union = to_shapely_union(parcel_fc); env = bbox_3857(parcel_union)
            thematic_fc = fetch_landtypes_intersecting_envelope(env)
            clipped = prepare_clipped_shapes(parcel_fc, thematic_fc)
            if not clipped: raise HTTPException(status_code=404, detail="No Land Types intersect this parcel.")
            if payload.simplify_tolerance and payload.simplify_tolerance > 0:
                simplified = []
                for geom4326, code, name, area_ha in clipped:
                    g2 = geom4326.simplify(payload.simplify_tolerance, preserve_topology=True)
                    if not g2.is_empty: simplified.append((g2, code, name, area_ha))
                clipped = simplified or clipped
            kml = build_kml(clipped, color_fn=color_from_code, folder_name=f"QLD Land Types – {lp}")
            tmpdir = tempfile.mkdtemp(prefix="kmz_"); out_path = os.path.join(tmpdir, f"{lp}_landtypes.kmz")
            write_kmz(kml, out_path); data = open(out_path, "rb").read(); os.remove(out_path); os.rmdir(tmpdir)
            fname = _sanitize_filename(payload.filename) if payload.filename else f"{lp}_landtypes"
            if not fname.lower().endswith(".kmz"): fname += ".kmz"
            return StreamingResponse(
                BytesIO(data),
                media_type="application/vnd.google-earth.kmz",
                headers={"Content-Disposition": f'attachment; filename="{fname}"'},
            )
        else:
            raise HTTPException(status_code=400, detail="Unsupported format.")

    # multi/zip
    zip_buf = BytesIO()
    manifest_rows: List[Dict[str, Any]] = []

    with zipfile.ZipFile(zip_buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for lp in items:
            row: Dict[str, Any] = {"lotplan": lp}
            try:
                parcel_fc = fetch_parcel_geojson(lp)
                parcel_union = to_shapely_union(parcel_fc)
                env = bbox_3857(parcel_union)
            except Exception as e:
                row.update({"status": "error:parcel", "message": str(e)})
                manifest_rows.append(row); continue

            # Land Types
            try:
                thematic_fc = fetch_landtypes_intersecting_envelope(env)
                clipped = prepare_clipped_shapes(parcel_fc, thematic_fc)
                if clipped:
                    if payload.format in (FormatEnum.tiff, FormatEnum.both):
                        tmpdir = tempfile.mkdtemp(prefix="tif_"); path = os.path.join(tmpdir, f"{lp}_landtypes.tif")
                        _ = make_geotiff_rgba(clipped, path, max_px=payload.max_px)
                        zf.writestr(f"{(prefix+'_') if prefix else ''}{lp}_landtypes.tif", open(path,"rb").read())
                        try: os.remove(path); os.rmdir(tmpdir)
                        except: pass
                        row["status_tiff"]="ok"; row["file_tiff"]=f"{(prefix+'_') if prefix else ''}{lp}_landtypes.tif"
                    if payload.format in (FormatEnum.kmz, FormatEnum.both):
                        if payload.simplify_tolerance and payload.simplify_tolerance > 0:
                            simplified = []
                            for geom4326, code, name, area_ha in clipped:
                                g2 = geom4326.simplify(payload.simplify_tolerance, preserve_topology=True)
                                if not g2.is_empty: simplified.append((g2, code, name, area_ha))
                            clipped = simplified or clipped
                        kml = build_kml(clipped, color_fn=color_from_code, folder_name=f"QLD Land Types – {lp}")
                        mem = BytesIO()
                        with zipfile.ZipFile(mem, "w", compression=zipfile.ZIP_DEFLATED) as ztmp:
                            ztmp.writestr("doc.kml", kml.encode("utf-8"))
                        zf.writestr(f"{(prefix+'_') if prefix else ''}{lp}_landtypes.kmz", mem.getvalue())
                        row["status_kmz"]="ok"; row["file_kmz"]=f"{(prefix+'_') if prefix else ''}{lp}_landtypes.kmz"
                else:
                    row["status_tiff"]="skip"; row["status_kmz"]="skip"; row["message"]="No Land Types intersect."
            except Exception as e:
                row["lt_error"]=str(e)

            # Vegetation (optional)
            if want_veg:
                try:
                    veg_fc = fetch_features_intersecting_envelope(veg_url, veg_layer, env, out_sr=4326, out_fields="*")
                    feats = veg_fc.get("features", [])
                    for f in feats:
                        p = f.get("properties") or {}
                        code = (p.get(veg_code) if veg_code else "") or (p.get(veg_name) or "UNK")
                        name = p.get(veg_name) or code
                        p["code"] = str(code); p["name"] = str(name)
                        f["properties"] = p
                    veg_fc["features"] = feats
                    vclipped = prepare_clipped_shapes(parcel_fc, veg_fc)
                    if vclipped:
                        if payload.include_veg_tiff:
                            tmpdir = tempfile.mkdtemp(prefix="tif_"); path = os.path.join(tmpdir, f"{lp}_vegetation.tif")
                            _ = make_geotiff_rgba(vclipped, path, max_px=payload.max_px)
                            zf.writestr(f"{(prefix+'_') if prefix else ''}{lp}_vegetation.tif", open(path,"rb").read())
                            try: os.remove(path); os.rmdir(tmpdir)
                            except: pass
                            row["status_veg_tiff"]="ok"; row["file_veg_tiff"]=f"{(prefix+'_') if prefix else ''}{lp}_vegetation.tif"
                        if payload.include_veg_kmz:
                            kml = build_kml(vclipped, color_fn=color_from_code, folder_name=f"Vegetation – {lp}")
                            mem = BytesIO()
                            with zipfile.ZipFile(mem, "w", compression=zipfile.ZIP_DEFLATED) as ztmp:
                                ztmp.writestr("doc.kml", kml.encode("utf-8"))
                            zf.writestr(f"{(prefix+'_') if prefix else ''}{lp}_vegetation.kmz", mem.getvalue())
                            row["status_veg_kmz"]="ok"; row["file_veg_kmz"]=f"{(prefix+'_') if prefix else ''}{lp}_vegetation.kmz"
                    else:
                        row["status_veg_tiff"]="skip"; row["status_veg_kmz"]="skip"; row["veg_message"]="No vegetation polygons intersect."
                except Exception as e:
                    row["veg_error"]=str(e)

            manifest_rows.append(row)

        # CSV manifest
        mem_csv = io.StringIO(newline='')
        writer = csv.DictWriter(mem_csv, fieldnames=[
            "lotplan",
            "status_tiff","file_tiff","lt_error",
            "status_kmz","file_kmz",
            "status_veg_tiff","file_veg_tiff","veg_message",
            "status_veg_kmz","file_veg_kmz","veg_error",
            "message"
        ])
        writer.writeheader()
        for r in manifest_rows:
            writer.writerow({k: r.get(k,"") for k in writer.fieldnames})
        zf.writestr("manifest.csv", mem_csv.getvalue().encode("utf-8"))

    zip_buf.seek(0)
    stamp = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    base = f"{prefix+'_' if prefix else ''}export_bundle"
    return StreamingResponse(
        zip_buf,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{base}_{stamp}.zip"'},
    )
