# app/main.py
import base64
import binascii
import csv
import datetime as dt
import html
import io
import logging
import math
import os
import tempfile
import zipfile
from dataclasses import replace
from enum import Enum
from io import BytesIO
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from fastapi import Body, FastAPI, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel, Field
from shapely.geometry import mapping as shp_mapping, shape as shp_shape
from shapely.validation import make_valid

from .arcgis import (
    fetch_bores_intersecting_envelope,
    fetch_easements_intersecting_envelope,
    fetch_features_intersecting_envelope,
    fetch_landtypes_intersecting_envelope,
    fetch_parcel_geojson,
    normalize_lotplan,
)
from .colors import color_from_code
from .config import (
    BORE_DRILL_DATE_FIELD,
    BORE_NUMBER_FIELD,
    BORE_REPORT_URL_FIELD,
    BORE_STATUS_CODE_FIELD,
    BORE_STATUS_LABEL_FIELD,
    BORE_TYPE_CODE_FIELD,
    BORE_TYPE_LABEL_FIELD,
    EASEMENT_AREA_FIELD,
    EASEMENT_FEATURE_NAME_FIELD,
    EASEMENT_LOTPLAN_FIELD,
    EASEMENT_PARCEL_TYPE_FIELD,
    EASEMENT_TENURE_FIELD,
    VEG_CODE_FIELD_DEFAULT,
    VEG_LAYER_ID_DEFAULT,
    VEG_NAME_FIELD_DEFAULT,
    VEG_SERVICE_URL_DEFAULT,
)
from .bores import (
    get_bore_icon_by_key,
    make_bore_icon_key,
    normalize_bore_drill_date,
    normalize_bore_number,
)
from .geometry import (
    bbox_3857,
    merge_clipped_shapes_across_lots,
    prepare_clipped_shapes,
    to_shapely_union,
)
from .kml import (
    PointPlacemark,
    build_kml,
    build_kml_folders,
    build_kml_nested_folders,
    write_kmz,
)
from .raster import make_geotiff_rgba

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


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_bore_properties(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    props = raw or {}

    bore_number = normalize_bore_number(
        props.get("bore_number")
        or props.get(BORE_NUMBER_FIELD)
        or props.get("rn")
        or props.get("rn_char")
    )
    if not bore_number:
        return None

    status_code = _clean_text(
        props.get("status")
        or props.get("status_code")
        or props.get(BORE_STATUS_CODE_FIELD)
        or props.get("facility_status")
    )
    status_label = _clean_text(
        props.get("status_label")
        or props.get(BORE_STATUS_LABEL_FIELD)
        or props.get("statusLabel")
        or props.get("facility_status_decode")
    )

    bore_type_code = _clean_text(
        props.get("type")
        or props.get("type_code")
        or props.get(BORE_TYPE_CODE_FIELD)
        or props.get("facility_type")
    )
    bore_type_label = _clean_text(
        props.get("type_label")
        or props.get(BORE_TYPE_LABEL_FIELD)
        or props.get("typeLabel")
        or props.get("facility_type_decode")
    )

    drilled_date = normalize_bore_drill_date(
        props.get("drilled_date") or props.get(BORE_DRILL_DATE_FIELD)
    )
    report_url = _clean_text(
        props.get("report_url") or props.get(BORE_REPORT_URL_FIELD)
    )

    icon_key = props.get("icon_key")
    if not icon_key:
        icon_key = make_bore_icon_key(status_code, bore_type_code)

    def _or_none(value: str) -> Optional[str]:
        return value or None

    return {
        "bore_number": bore_number,
        "status": _or_none(status_code),
        "status_label": _or_none(status_label) or _or_none(status_code),
        "type": _or_none(bore_type_code),
        "type_label": _or_none(bore_type_label) or _or_none(bore_type_code),
        "drilled_date": drilled_date,
        "report_url": _or_none(report_url),
        "icon_key": icon_key,
    }


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        text = str(value)
    except Exception:
        return None
    text = text.strip()
    if not text:
        return None
    text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def _clip_to_parcel_union(geom, parcel_union):
    if geom.is_empty:
        return None
    if parcel_union is None or parcel_union.is_empty:
        return geom
    try:
        if not parcel_union.intersects(geom):
            return None
    except Exception:
        pass
    try:
        clipped = parcel_union.intersection(geom)
    except Exception:
        try:
            clipped = parcel_union.intersection(make_valid(geom))
        except Exception:
            try:
                clipped = make_valid(parcel_union).intersection(make_valid(geom))
            except Exception:
                clipped = geom
    if clipped.is_empty:
        return None
    return clipped


def _normalize_easement_properties(raw: Dict[str, Any], lotplan: str) -> Dict[str, Any]:
    props = raw or {}

    owner_lp = normalize_lotplan(
        props.get("lotplan")
        or props.get(EASEMENT_LOTPLAN_FIELD)
        or props.get("lot_plan")
        or lotplan
    )

    parcel_type = _clean_text(
        props.get("parcel_type")
        or props.get(EASEMENT_PARCEL_TYPE_FIELD)
    )
    name = _clean_text(
        props.get("name")
        or props.get(EASEMENT_FEATURE_NAME_FIELD)
    )
    alias = _clean_text(
        props.get("alias")
        or props.get("feat_alias")
        or props.get("feature_alias")
    )
    tenure = _clean_text(
        props.get("tenure")
        or props.get(EASEMENT_TENURE_FIELD)
    )

    area_value = props.get("area_m2")
    if area_value is None:
        area_value = props.get(EASEMENT_AREA_FIELD)
    area_m2 = _safe_float(area_value)

    out: Dict[str, Any] = {
        "lotplan": owner_lp or lotplan,
        "parcel_type": parcel_type or None,
        "name": name or alias or None,
        "tenure": tenure or None,
    }

    if alias:
        out["alias"] = alias

    if area_m2 is not None:
        out["area_m2"] = area_m2
        out["area_ha"] = area_m2 / 10000.0

    return out


def _clean_bound_value(value: Any) -> Optional[float]:
    number = _safe_float(value)
    if number is None:
        return None
    if isinstance(number, float) and math.isnan(number):
        return None
    return number


def _bounds_dict_from_geom(bounds_geom, fallback=None) -> Dict[str, Optional[float]]:
    candidate = bounds_geom
    if candidate is None or getattr(candidate, "is_empty", True):
        candidate = fallback
    if candidate is None or getattr(candidate, "is_empty", True):
        return {"west": None, "south": None, "east": None, "north": None}
    west, south, east, north = candidate.bounds
    return {
        "west": _clean_bound_value(west),
        "south": _clean_bound_value(south),
        "east": _clean_bound_value(east),
        "north": _clean_bound_value(north),
    }


BORE_FOLDER_NAME = "Groundwater Bores"
_ICON_EXTENSIONS: Dict[str, str] = {
    "image/png": "png",
    "image/jpeg": "jpg",
    "image/jpg": "jpg",
}

_EXT_CONTENT_TYPES: Dict[str, str] = {}
for _mime, _ext in _ICON_EXTENSIONS.items():
    if _ext:
        _EXT_CONTENT_TYPES.setdefault(_ext.lower(), _mime)


def _slugify_icon_key(icon_key: str) -> str:
    key = (icon_key or "").strip().lower().replace(",", "_")
    return "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in key) or "icon"


def _icon_href_for_key(icon_key: str, content_type: Optional[str]) -> str:
    slug = _slugify_icon_key(icon_key)
    ext = _ICON_EXTENSIONS.get((content_type or "").lower(), "png")
    return f"icons/{slug}.{ext}"


def _icon_content_type_from_href(icon_href: str) -> str:
    _, ext = os.path.splitext(icon_href or "")
    ext_clean = ext.lower().lstrip(".")
    if ext_clean:
        return _EXT_CONTENT_TYPES.get(ext_clean, "image/png")
    return "image/png"


def _data_uri_for_icon(icon_href: str, data: Optional[bytes]) -> Optional[str]:
    if not data:
        return None
    try:
        content_type = _icon_content_type_from_href(icon_href)
        encoded = base64.b64encode(data).decode("ascii")
    except Exception:
        return None
    return f"data:{content_type};base64,{encoded}"


def _inline_point_icon_hrefs(
    points: Sequence[PointPlacemark], assets: Mapping[str, bytes]
) -> List[PointPlacemark]:
    if not points:
        return []
    if not assets:
        return list(points)

    cache: Dict[str, Optional[str]] = {}
    updated: List[PointPlacemark] = []
    for point in points:
        icon_href = point.icon_href
        if not icon_href:
            updated.append(point)
            continue
        if icon_href not in cache:
            raw_data = assets.get(icon_href)
            cache[icon_href] = _data_uri_for_icon(icon_href, raw_data)
        data_uri = cache[icon_href]
        if data_uri:
            updated.append(replace(point, icon_href=data_uri))
        else:
            updated.append(point)
    return updated


def _format_bore_description(props: Dict[str, Any]) -> str:
    def combine(label: Optional[str], code: Optional[str]) -> Optional[str]:
        label_clean = (label or "").strip()
        code_clean = (code or "").strip()
        if label_clean and code_clean and label_clean.upper() != code_clean.upper():
            return f"{label_clean} ({code_clean})"
        return label_clean or code_clean or None

    parts: List[str] = []
    status_text = combine(props.get("status_label"), props.get("status"))
    if status_text:
        parts.append(f"<b>Status:</b> {html.escape(status_text)}")
    type_text = combine(props.get("type_label"), props.get("type"))
    if type_text:
        parts.append(f"<b>Type:</b> {html.escape(type_text)}")
    drilled = props.get("drilled_date")
    if drilled:
        parts.append(f"<b>Drilled:</b> {html.escape(str(drilled))}")
    report_url = props.get("report_url")
    if report_url:
        safe_url = html.escape(str(report_url), quote=True)
        parts.append(f'<a href="{safe_url}" target="_blank" rel="noopener">View bore report</a>')
    return "<br/>".join(parts)


def _prepare_bore_placemarks(
    parcel_geom,
    bore_fc: Dict[str, Any],
) -> Tuple[List[PointPlacemark], Dict[str, bytes]]:
    placemarks: List[PointPlacemark] = []
    assets: Dict[str, bytes] = {}
    seen_numbers: Set[str] = set()

    for bore in bore_fc.get("features", []):
        try:
            geom = shp_shape(bore.get("geometry"))
        except Exception:
            continue
        if geom.is_empty or geom.geom_type != "Point":
            continue
        if parcel_geom is not None:
            try:
                if not geom.intersects(parcel_geom):
                    continue
            except Exception:
                pass
        props = _normalize_bore_properties(bore.get("properties") or {})
        if not props:
            continue
        bore_number = props.get("bore_number")
        if not bore_number or bore_number in seen_numbers:
            continue
        seen_numbers.add(bore_number)

        icon_key = props.get("icon_key")
        style_id = None
        icon_href = None
        if icon_key:
            icon_def = get_bore_icon_by_key(icon_key)
            image_data = icon_def.image_data if icon_def else None
            if image_data:
                try:
                    icon_bytes = base64.b64decode(image_data)
                except (binascii.Error, ValueError):
                    icon_bytes = None
                if icon_bytes:
                    icon_href = _icon_href_for_key(icon_key, icon_def.content_type if icon_def else None)
                    assets.setdefault(icon_href, icon_bytes)
                    style_id = f"bore_{_slugify_icon_key(icon_key)}"

        description_html = _format_bore_description(props)
        placemarks.append(
            PointPlacemark(
                name=bore_number,
                description_html=description_html,
                lon=float(geom.x),
                lat=float(geom.y),
                style_id=style_id,
                icon_href=icon_href,
            )
        )

    return placemarks, assets


def _render_parcel_kml(
    lotplan: str,
    lt_clipped,
    veg_clipped,
    bore_points: Sequence[PointPlacemark],
) -> str:
    folder_name = f"QLD Land Types – {lotplan}"
    if veg_clipped:
        groups = [
            (lt_clipped, color_from_code, f"Land Types – {lotplan}"),
            (veg_clipped, color_from_code, f"Vegetation – {lotplan}"),
        ]
        if bore_points:
            groups.append(([], color_from_code, BORE_FOLDER_NAME, list(bore_points)))
        return build_kml_folders(groups, doc_name=f"QLD Export – {lotplan}")

    if bore_points:
        return build_kml(
            lt_clipped,
            color_fn=color_from_code,
            folder_name=folder_name,
            point_placemarks=list(bore_points),
            point_folder_name=BORE_FOLDER_NAME,
        )
    return build_kml(lt_clipped, color_fn=color_from_code, folder_name=folder_name)


def _kmz_bytes(kml_text: str, assets: Dict[str, bytes]) -> bytes:
    mem = BytesIO()
    with zipfile.ZipFile(mem, "w", compression=zipfile.ZIP_DEFLATED) as ztmp:
        ztmp.writestr("doc.kml", kml_text.encode("utf-8"))
        for name, data in (assets or {}).items():
            if not name or data is None:
                continue
            ztmp.writestr(name, data)
    return mem.getvalue()

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
  <p>Paste one or many <strong>Lot / Plan</strong> codes. Downloads include <strong>GeoTIFF</strong> and <strong>KMZ</strong> outputs. <strong>Vegetation data is always included.</strong></p>

  <div class="row">
    <div style="flex: 2 1 420px;">
      <label for="items">Lot / Plan (single OR multiple — new line, comma, or semicolon separated)</label>
      <textarea id="items" placeholder="13SP181800
1RP12345
2RP54321"></textarea>
      <div class="muted" id="parseinfo">Detected 0 items.</div>
    </div>
    <div>
      <label for="name">Name (single) or Prefix (bulk)</label>
      <input id="name" type="text" placeholder="e.g. UpperCoomera_13SP181800 or Job_4021" />
      <div class="box muted">Exports always include both GeoTIFF and KMZ files.</div>
    </div>
  </div>



  <div class="btns">
    <button class="primary" id="btn-export-tiff">Download GeoTIFF</button>
    <button class="primary" id="btn-export-kmz">Download KMZ</button>
    <a class="ghost" id="btn-json" href="#">Preview JSON (single)</a>
    <a class="ghost" id="btn-load" href="#">Load on Map</a>
  </div>

  <div class="note">JSON preview requires exactly one lot/plan. The map supports one or many. API docs: <a href="/docs">/docs</a></div>
  <div id="map"></div><div id="out" class="out"></div>
</div></div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" crossorigin="" defer></script>
<script>
const $items = document.getElementById('items'),
      $name = document.getElementById('name'),
      $mode = document.getElementById('mode'),
      $out = document.getElementById('out'),
      $parseinfo = document.getElementById('parseinfo'),
      $btnExportTiff = document.getElementById('btn-export-tiff'),
      $btnExportKmz = document.getElementById('btn-export-kmz'),
      $btnJson = document.getElementById('btn-json'),
      $btnLoad = document.getElementById('btn-load');

const DEFAULT_MAX_PX = 4096;
const DEFAULT_SIMPLIFY = 0;
const DEFAULT_BORE_COLOR = '#38bdf8';
const BORE_STATUS_COLORS = {
  EX: '#22c55e',
  AU: '#2563eb',
  AD: '#ef4444',
  IN: '#f59e0b'
};
const ESCAPE_HTML_LOOKUP = {
  "&": "&amp;",
  "<": "&lt;",
  ">": "&gt;",
  '"': "&quot;",
  "'": "&#39;"
};

function colorForBoreStatus(status){
  const key = (status || '').toString().trim().toUpperCase();
  return BORE_STATUS_COLORS[key] || DEFAULT_BORE_COLOR;
}

function boreClassName(key){
  if (!key) return 'bore-marker';
  return `bore-marker bore-${String(key).toLowerCase().replace(/[^a-z0-9]+/g,'-')}`;
}

function escHtml(value){
  return (value == null ? '' : String(value)).replace(/[&<>"']/g, ch => ESCAPE_HTML_LOOKUP[ch] || ch);
}

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
  if (n === 0){
    $mode.textContent = "Mode: None";
    $btnJson.style.opacity='.5'; $btnJson.style.pointerEvents='none';
    $btnLoad.style.opacity='.5'; $btnLoad.style.pointerEvents='none';
  } else if (n === 1){
    $mode.textContent = "Mode: Single";
    $btnJson.style.opacity='1'; $btnJson.style.pointerEvents='auto';
    $btnLoad.style.opacity='1'; $btnLoad.style.pointerEvents='auto';
  } else {
    $mode.textContent = `Mode: Bulk (${n})`;
    $btnJson.style.opacity='.5'; $btnJson.style.pointerEvents='none';
    $btnLoad.style.opacity='1'; $btnLoad.style.pointerEvents='auto';
  }
}

let map=null, parcelLayer=null, ltLayer=null, boreLayer=null;
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
function clearLayers(){
  try{
    if(map && parcelLayer){ map.removeLayer(parcelLayer); parcelLayer=null; }
    if(map && ltLayer){ map.removeLayer(ltLayer); ltLayer=null; }
    if(map && boreLayer){ map.removeLayer(boreLayer); boreLayer=null; }
  }catch{}
}
function mkVectorUrl(lotplan){ return `/vector?lotplan=${encodeURIComponent(lotplan)}`; }

async function loadVector(){
  const items = parseItems($items.value);
  if (!items.length){ $out.textContent = 'Enter at least one Lot/Plan to load map.'; return; }
  ensureMap(); if (!map){ $out.textContent = 'Map library not loaded yet. Try again in a moment.'; return; }
  const multi = items.length > 1;
  $out.textContent = multi ? `Loading vector data for ${items.length} lots/plans…` : 'Loading vector data…';
  try{
    let res, data;
    if (multi){
      res = await fetch('/vector/bulk', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ lotplans: items }) });
    } else {
      res = await fetch(mkVectorUrl(items[0]));
    }
    data = await res.json();
    if (!res.ok){
      const msg = data && (data.detail || data.error) ? (data.detail || data.error) : 'Unexpected server response.';
      $out.textContent = `Error ${res.status}: ${msg}`;
      return;
    }
    if (data.error){ $out.textContent = 'Error: ' + data.error; return; }
    clearLayers();
    const parcelData = data.parcels || data.parcel;
    if (parcelData){
      parcelLayer = L.geoJSON(parcelData, {
        style: { color: '#ffcc00', weight:2, fillOpacity:0 },
        onEachFeature: (feature, layer) => {
          const p = feature.properties || {};
          if (p.lotplan){ layer.bindPopup(`<strong>Lot/Plan:</strong> ${p.lotplan}`); }
        }
      }).addTo(map);
    }
    const ltData = data.landtypes;
    if (ltData && ltData.features && ltData.features.length){
      ltLayer = L.geoJSON(ltData, { style: f => styleForCode(f.properties.code, f.properties.color_hex),
        onEachFeature: (feature, layer) => {
          const p = feature.properties || {};
          const html = `<b>${p.name || 'Unknown'}</b><br/>Code: <code>${p.code || 'UNK'}</code><br/>Area: ${(p.area_ha ?? 0).toFixed(2)} ha${p.lotplan ? `<br/>Lot/Plan: ${p.lotplan}` : ''}`;
          layer.bindPopup(html);
        }}).addTo(map);
    }
    const boreData = data.bores;
    if (boreData && Array.isArray(boreData.features) && boreData.features.length){
      boreLayer = L.geoJSON(boreData, {
        pointToLayer: (feature, latlng) => {
          const props = feature.properties || {};
          const color = colorForBoreStatus(props.status);
          const cls = boreClassName(props.icon_key || props.status);
          return L.circleMarker(latlng, {
            radius: 6,
            color,
            weight: 1.5,
            fillColor: color,
            fillOpacity: 0.85,
            className: cls
          });
        },
        onEachFeature: (feature, layer) => {
          const props = feature.properties || {};
          const lines = [];
          const num = props.bore_number || 'Unknown';
          lines.push(`<strong>Bore ${escHtml(num)}</strong>`);
          const statusText = props.status_label || props.status;
          if (statusText){ lines.push(`<span class="muted">Status:</span> ${escHtml(statusText)}`); }
          const typeText = props.type_label || props.type;
          if (typeText){ lines.push(`<span class="muted">Type:</span> ${escHtml(typeText)}`); }
          if (props.drilled_date){ lines.push(`<span class="muted">Drilled:</span> ${escHtml(props.drilled_date)}`); }
          layer.bindPopup(lines.join('<br/>'));
          if (props.bore_number){ layer.options.title = `Bore ${props.bore_number}`; }
        }
      }).addTo(map);
    }
    const b = data.bounds4326;
    if (b){
      map.fitBounds([[b.south, b.west],[b.north, b.east]], { padding:[20,20] });
    } else if (parcelLayer && parcelLayer.getBounds){
      map.fitBounds(parcelLayer.getBounds(), { padding:[20,20] });
    } else if (ltLayer && ltLayer.getBounds){
      map.fitBounds(ltLayer.getBounds(), { padding:[20,20] });
    } else if (boreLayer && boreLayer.getBounds){
      map.fitBounds(boreLayer.getBounds(), { padding:[20,20] });
    }
    const summary = {
      lotplans: data.lotplans || (data.lotplan ? [data.lotplan] : []),
      legend: data.legend || [],
      bounds4326: data.bounds4326 || null
    };
    $out.textContent = JSON.stringify(summary, null, 2);
  }catch(err){ $out.textContent = 'Network error: ' + err; }
}

async function previewJson(){
  const items = parseItems($items.value);
  if (items.length !== 1){ $out.textContent = 'Provide exactly one Lot/Plan for JSON preview.'; return; }
  const lot = items[0];
  $out.textContent='Requesting JSON summary…';
  try{
    const url = `/export?lotplan=${encodeURIComponent(lot)}&max_px=${DEFAULT_MAX_PX}&download=false`;
    const res = await fetch(url); const txt = await res.text();
    try{ const data = JSON.parse(txt); $out.textContent = JSON.stringify(data, null, 2);}catch{ $out.textContent = `Error ${res.status}: ${txt}`; }
  }catch(err){ $out.textContent = 'Network error: ' + err; }
}

async function exportAny(targetFormat){
  const items = parseItems($items.value);
  if (!items.length){ $out.textContent = 'Enter at least one Lot/Plan.'; return; }
  const format = targetFormat === 'kmz' ? 'kmz' : 'tiff';
  const body = {
    format,
    max_px: DEFAULT_MAX_PX,
    simplify_tolerance: DEFAULT_SIMPLIFY,
    include_veg_tiff: true, include_veg_kmz: true,
    veg_service_url: '%VEG_URL%', veg_layer_id: %VEG_LAYER%,
    veg_name_field: '%VEG_NAME%', veg_code_field: '%VEG_CODE%',
  };
  const name = normText($name.value) || null;
  if (items.length === 1){ body.lotplan = items[0]; if (name) body.filename = name; } else { body.lotplans = items; if (name) body.filename_prefix = name; }
  const label = format === 'kmz' ? 'KMZ' : 'GeoTIFF';
  $out.textContent = items.length === 1 ? `Exporting ${label}…` : `Exporting ${label} for ${items.length} items…`;
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
$btnLoad.addEventListener('click', (e)=>{ e.preventDefault(); loadVector(); });
$btnJson.addEventListener('click', (e)=>{ e.preventDefault(); previewJson(); });
$btnExportTiff.addEventListener('click', (e)=>{ e.preventDefault(); exportAny('tiff'); });
$btnExportKmz.addEventListener('click', (e)=>{ e.preventDefault(); exportAny('kmz'); });
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
    bore_fc = fetch_bores_intersecting_envelope(env)
    easement_fc = fetch_easements_intersecting_envelope(env)

    for feature in parcel_fc.get("features", []):
        props = feature.get("properties") or {}
        if props.get("lotplan") == lotplan:
            continue
        new_props = dict(props)
        new_props["lotplan"] = lotplan
        feature["properties"] = new_props

    features: List[Dict[str, Any]] = []
    legend_map: Dict[str, Dict[str, Any]] = {}
    for geom4326, code, name, area_ha in clipped:
        color_hex = _hex(color_from_code(code))
        features.append(
            {
                "type": "Feature",
                "geometry": shp_mapping(geom4326),
                "properties": {
                    "code": code,
                    "name": name,
                    "area_ha": float(area_ha),
                    "color_hex": color_hex,
                    "lotplan": lotplan,
                },
            }
        )
        legend_map.setdefault(
            code,
            {"code": code, "name": name, "color_hex": color_hex, "area_ha": 0.0},
        )
        legend_map[code]["area_ha"] += float(area_ha)

    bore_features: List[Dict[str, Any]] = []
    seen_bores: Set[str] = set()
    for bore in bore_fc.get("features", []):
        try:
            geom = shp_shape(bore.get("geometry"))
        except Exception:
            continue
        if geom.is_empty:
            continue
        norm_props = _normalize_bore_properties(bore.get("properties") or {})
        if not norm_props:
            continue
        bore_number = norm_props.get("bore_number")
        if not bore_number or bore_number in seen_bores:
            continue
        seen_bores.add(bore_number)
        norm_props["lotplan"] = lotplan
        bore_features.append(
            {
                "type": "Feature",
                "geometry": shp_mapping(geom),
                "properties": norm_props,
            }
        )

    easement_features: List[Dict[str, Any]] = []
    for easement in easement_fc.get("features", []):
        try:
            geom = shp_shape(easement.get("geometry"))
        except Exception:
            continue
        if geom.is_empty:
            continue
        clipped_geom = _clip_to_parcel_union(geom, parcel_union)
        if clipped_geom is None or clipped_geom.is_empty:
            continue
        props = _normalize_easement_properties(easement.get("properties") or {}, lotplan)
        easement_features.append(
            {
                "type": "Feature",
                "geometry": shp_mapping(clipped_geom),
                "properties": props,
            }
        )

    bounds_fc = {"type": "FeatureCollection", "features": []}
    bounds_fc["features"].extend(parcel_fc.get("features", []))
    bounds_fc["features"].extend(features)
    bounds_fc["features"].extend(bore_features)
    bounds_fc["features"].extend(easement_features)
    bounds_geom = to_shapely_union(bounds_fc)
    bounds_dict = _bounds_dict_from_geom(bounds_geom, parcel_union)
    status_code = 200 if features else 404
    payload = {
        "lotplan": lotplan,
        "parcel": parcel_fc,
        "landtypes": {"type": "FeatureCollection", "features": features},
        "bores": {"type": "FeatureCollection", "features": bore_features},
        "easements": {"type": "FeatureCollection", "features": easement_features},
        "legend": sorted(legend_map.values(), key=lambda d: (-d["area_ha"], d["code"])),
        "bounds4326": bounds_dict,
    }
    if status_code != 200:
        payload["error"] = "No Land Types intersect this parcel."
    return JSONResponse(payload, status_code=status_code)


class VectorBulkRequest(BaseModel):
    lotplans: List[str] = Field(..., min_length=1)


@app.post("/vector/bulk")
def vector_geojson_bulk(payload: VectorBulkRequest):
    seen = set()
    lotplans: List[str] = []
    for raw in payload.lotplans or []:
        code = (raw or "").strip()
        if not code:
            continue
        lp = normalize_lotplan(code)
        if lp in seen:
            continue
        seen.add(lp)
        lotplans.append(lp)

    if not lotplans:
        raise HTTPException(status_code=400, detail="No valid lot/plan codes provided.")

    parcel_features: List[Dict[str, Any]] = []
    landtype_features: List[Dict[str, Any]] = []
    bore_features: List[Dict[str, Any]] = []
    easement_features: List[Dict[str, Any]] = []
    legend_map: Dict[str, Dict[str, Any]] = {}
    bounds = None
    seen_bore_numbers: Set[str] = set()

    def expand_bounds(current, geom):
        if geom.is_empty:
            return current
        minx, miny, maxx, maxy = geom.bounds
        if current is None:
            return [minx, miny, maxx, maxy]
        current[0] = min(current[0], minx)
        current[1] = min(current[1], miny)
        current[2] = max(current[2], maxx)
        current[3] = max(current[3], maxy)
        return current

    for lotplan in lotplans:
        parcel_fc = fetch_parcel_geojson(lotplan)
        parcel_union = to_shapely_union(parcel_fc)
        env = bbox_3857(parcel_union)

        for feature in parcel_fc.get("features", []):
            try:
                geom = shp_shape(feature.get("geometry"))
            except Exception:
                continue
            if geom.is_empty:
                continue
            bounds = expand_bounds(bounds, geom)
            props = dict(feature.get("properties") or {})
            props["lotplan"] = lotplan
            parcel_features.append({
                "type": "Feature",
                "geometry": shp_mapping(geom),
                "properties": props,
            })

        lt_fc = fetch_landtypes_intersecting_envelope(env)
        clipped = prepare_clipped_shapes(parcel_fc, lt_fc)
        bore_fc = fetch_bores_intersecting_envelope(env)
        easement_fc = fetch_easements_intersecting_envelope(env)

        for bore in bore_fc.get("features", []):
            try:
                geom = shp_shape(bore.get("geometry"))
            except Exception:
                continue
            if geom.is_empty:
                continue
            norm_props = _normalize_bore_properties(bore.get("properties") or {})
            if not norm_props:
                continue
            bore_number = norm_props.get("bore_number")
            if not bore_number or bore_number in seen_bore_numbers:
                continue
            seen_bore_numbers.add(bore_number)
            norm_props["lotplan"] = lotplan
            bore_features.append({
                "type": "Feature",
                "geometry": shp_mapping(geom),
                "properties": norm_props,
            })
            bounds = expand_bounds(bounds, geom)

        for easement in easement_fc.get("features", []):
            try:
                geom = shp_shape(easement.get("geometry"))
            except Exception:
                continue
            if geom.is_empty:
                continue
            clipped_geom = _clip_to_parcel_union(geom, parcel_union)
            if clipped_geom is None or clipped_geom.is_empty:
                continue
            props = _normalize_easement_properties(easement.get("properties") or {}, lotplan)
            easement_features.append(
                {
                    "type": "Feature",
                    "geometry": shp_mapping(clipped_geom),
                    "properties": props,
                }
            )
            bounds = expand_bounds(bounds, clipped_geom)

        for geom4326, code, name, area_ha in clipped:
            if geom4326.is_empty:
                continue
            bounds = expand_bounds(bounds, geom4326)
            color_hex = _hex(color_from_code(code))
            landtype_features.append({
                "type": "Feature",
                "geometry": shp_mapping(geom4326),
                "properties": {
                    "code": code,
                    "name": name,
                    "area_ha": float(area_ha),
                    "color_hex": color_hex,
                    "lotplan": lotplan,
                },
            })
            legend_map.setdefault(code, {
                "code": code,
                "name": name,
                "color_hex": color_hex,
                "area_ha": 0.0,
            })
            legend_map[code]["area_ha"] += float(area_ha)

    if not parcel_features and not landtype_features and not bore_features and not easement_features:
        raise HTTPException(status_code=404, detail="No features found for the provided lots/plans.")

    bounds_dict = None
    if bounds is not None:
        west, south, east, north = bounds
        bounds_dict = {"west": west, "south": south, "east": east, "north": north}

    return JSONResponse({
        "lotplans": lotplans,
        "parcels": {"type": "FeatureCollection", "features": parcel_features},
        "landtypes": {"type": "FeatureCollection", "features": landtype_features},
        "bores": {"type": "FeatureCollection", "features": bore_features},
        "easements": {"type": "FeatureCollection", "features": easement_features},
        "legend": sorted(legend_map.values(), key=lambda d: (-d["area_ha"], d["code"])),
        "bounds4326": bounds_dict,
    })

@app.get("/export_kmz")
def export_kmz(
    lotplan: str = Query(...),
    simplify_tolerance: float = Query(0.0, ge=0.0, le=0.001),
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

    bore_fc = fetch_bores_intersecting_envelope(env)
    bore_points, bore_assets = _prepare_bore_placemarks(parcel_union, bore_fc)

    veg_clipped = []
    if veg_service_url and veg_layer_id is not None:
        veg_fc = fetch_features_intersecting_envelope(
            veg_service_url, veg_layer_id, env, out_fields="*"
        )
        # standardise fields
        for f in veg_fc.get("features", []):
            props = f.get("properties") or {}
            code = str(props.get(veg_code_field or "code") or props.get("code") or "").strip()
            name = str(props.get(veg_name_field or "name") or props.get("name") or code).strip()
            props["code"] = code or name or "UNK"
            # Format vegetation names as "Category *"
            category_name = name or code or "Unknown"
            props["name"] = f"Category {category_name}"
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

    kml = _render_parcel_kml(lotplan, lt_clipped, veg_clipped, bore_points)

    tmpdir = tempfile.mkdtemp(prefix="kmz_")
    out_path = os.path.join(tmpdir, f"{lotplan}_landtypes.kmz")
    write_kmz(kml, out_path, assets=bore_assets)
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

    bore_fc = fetch_bores_intersecting_envelope(env)
    bore_points, bore_assets = _prepare_bore_placemarks(parcel_union, bore_fc)

    veg_clipped = []
    if veg_service_url and veg_layer_id is not None:
        veg_fc = fetch_features_intersecting_envelope(
            veg_service_url, veg_layer_id, env, out_fields="*"
        )
        # standardise fields
        for f in veg_fc.get("features", []):
            props = f.get("properties") or {}
            code = str(props.get(veg_code_field or "code") or props.get("code") or "").strip()
            name = str(props.get(veg_name_field or "name") or props.get("name") or code).strip()
            props["code"] = code or name or "UNK"
            # Format vegetation names as "Category *"
            category_name = name or code or "Unknown"
            props["name"] = f"Category {category_name}"
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

    if bore_points:
        bore_points = _inline_point_icon_hrefs(bore_points, bore_assets)

    kml = _render_parcel_kml(lotplan, lt_clipped, veg_clipped, bore_points)

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

def _create_bulk_kmz(items: List[str], payload: ExportAnyRequest, prefix: Optional[str], 
                     veg_url: str, veg_layer: int, veg_name: str, veg_code: Optional[str]):
    """Create a single KMZ file containing folders for each lot/plan with nested land types and vegetation."""
    
    nested_groups = []
    all_lt_clipped = []  # Collect all land type data across lots
    all_veg_clipped = []  # Collect all vegetation data across lots
    all_bore_points: List[PointPlacemark] = []
    kmz_assets: Dict[str, bytes] = {}
    seen_all_bores: Set[str] = set()
    
    for lp in items:
        try:
            parcel_fc = fetch_parcel_geojson(lp)
            parcel_union = to_shapely_union(parcel_fc)
            env = bbox_3857(parcel_union)
            
            # Get land types
            thematic_fc = fetch_landtypes_intersecting_envelope(env)
            lt_clipped = prepare_clipped_shapes(parcel_fc, thematic_fc)
            
            # Get bores
            bore_fc = fetch_bores_intersecting_envelope(env)
            bore_points, bore_assets = _prepare_bore_placemarks(parcel_union, bore_fc)
            for name, data in bore_assets.items():
                if name not in kmz_assets:
                    kmz_assets[name] = data
            for point in bore_points:
                if point.name and point.name not in seen_all_bores:
                    seen_all_bores.add(point.name)
                    all_bore_points.append(point)

            # Get vegetation
            veg_clipped = []
            if veg_url and veg_layer is not None:
                veg_fc = fetch_features_intersecting_envelope(veg_url, veg_layer, env, out_fields="*")
                for f in veg_fc.get("features", []):
                    props = f.get("properties") or {}
                    code = str(props.get(veg_code or "code") or props.get("code") or "").strip()
                    name = str(props.get(veg_name or "name") or props.get("name") or code).strip()
                    props["code"] = code or name or "UNK"
                    # Format vegetation names as "Category *"
                    category_name = name or code or "Unknown"
                    props["name"] = f"Category {category_name}"
                veg_clipped = prepare_clipped_shapes(parcel_fc, veg_fc)
            
            # Apply simplification if requested
            if payload.simplify_tolerance and payload.simplify_tolerance > 0:
                def _simp(data):
                    out = []
                    for geom4326, code, name, area_ha in data:
                        g2 = geom4326.simplify(payload.simplify_tolerance, preserve_topology=True)
                        if not g2.is_empty:
                            out.append((g2, code, name, area_ha))
                    return out or data
                lt_clipped = _simp(lt_clipped)
                if veg_clipped:
                    veg_clipped = _simp(veg_clipped)
            
            # Store data for merging across lots
            if lt_clipped:
                all_lt_clipped.append(lt_clipped)
            if veg_clipped:
                all_veg_clipped.append(veg_clipped)

            # Create nested structure: lot folder with land types and veg subfolders
            subfolders = []
            if lt_clipped:
                subfolders.append((lt_clipped, color_from_code, "Land Types"))
            if veg_clipped:
                subfolders.append((veg_clipped, color_from_code, "Veg"))
            if bore_points:
                subfolders.append(([], color_from_code, BORE_FOLDER_NAME, bore_points))

            if subfolders:
                nested_groups.append((lp, subfolders))
                
        except Exception:
            # Skip lots that fail to process
            continue
    
    if not nested_groups:
        raise HTTPException(status_code=404, detail="No data found for any of the provided lot/plans.")
    
    # Create merged layers across all lots
    merged_folders = []
    
    # Merge land types across all lots
    if all_lt_clipped:
        merged_lt = merge_clipped_shapes_across_lots(all_lt_clipped)
        if merged_lt:
            merged_folders.append(("Merged Land Types (All Properties)", [(merged_lt, color_from_code, "Land Types")]))
    
    # Merge vegetation across all lots
    if all_veg_clipped:
        merged_veg = merge_clipped_shapes_across_lots(all_veg_clipped)
        if merged_veg:
            merged_folders.append(("Merged Vegetation (All Properties)", [(merged_veg, color_from_code, "Vegetation")]))
    
    if all_bore_points:
        merged_folders.append((
            "Groundwater Bores (All Properties)",
            [([], color_from_code, BORE_FOLDER_NAME, all_bore_points)],
        ))

    # Combine merged folders with individual lot folders
    final_nested_groups = merged_folders + nested_groups
    
    # Create KML with nested folder structure
    doc_name = f"QLD Bulk Export – {len(items)} lots"
    if prefix:
        doc_name = f"{prefix} – {doc_name}"
        
    kml = build_kml_nested_folders(final_nested_groups, doc_name=doc_name)
    
    # Create KMZ file
    tmpdir = tempfile.mkdtemp(prefix="bulk_kmz_")
    fname = f"{prefix+'_' if prefix else ''}bulk_export_{len(items)}_lots.kmz"
    out_path = os.path.join(tmpdir, fname)
    write_kmz(kml, out_path, assets=kmz_assets)
    data = open(out_path, "rb").read()
    os.remove(out_path); os.rmdir(tmpdir)
    
    return StreamingResponse(
        BytesIO(data),
        media_type="application/vnd.google-earth.kmz",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )

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

    # Always include vegetation
    veg_url = (payload.veg_service_url or VEG_SERVICE_URL_DEFAULT or "").strip()
    veg_layer = payload.veg_layer_id if payload.veg_layer_id is not None else VEG_LAYER_ID_DEFAULT
    veg_name = (payload.veg_name_field or VEG_NAME_FIELD_DEFAULT or "").strip()
    veg_code = (payload.veg_code_field or VEG_CODE_FIELD_DEFAULT or "").strip() or None
    if not veg_url or veg_layer is None or not veg_name:
        raise HTTPException(status_code=400, detail="Vegetation service configuration missing.")

    prefix = _sanitize_filename(payload.filename_prefix) if payload.filename_prefix else None
    
    # For bulk KMZ exports, create a single KMZ with nested folders
    if len(items) > 1 and payload.format == FormatEnum.kmz:
        return _create_bulk_kmz(items, payload, prefix, veg_url, veg_layer, veg_name, veg_code)
    
    # For single item or other formats, use existing logic but always include vegetation
    multi_files = (len(items) > 1) or (payload.format == FormatEnum.both) or payload.include_veg_tiff or payload.include_veg_kmz

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
            lt_clipped = prepare_clipped_shapes(parcel_fc, thematic_fc)
            if not lt_clipped: raise HTTPException(status_code=404, detail="No Land Types intersect this parcel.")

            bore_fc = fetch_bores_intersecting_envelope(env)
            bore_points, bore_assets = _prepare_bore_placemarks(parcel_union, bore_fc)

            # Always include vegetation for single KMZ
            veg_clipped = []
            if veg_url and veg_layer is not None:
                veg_fc = fetch_features_intersecting_envelope(veg_url, veg_layer, env, out_fields="*")
                for f in veg_fc.get("features", []):
                    props = f.get("properties") or {}
                    code = str(props.get(veg_code or "code") or props.get("code") or "").strip()
                    name = str(props.get(veg_name or "name") or props.get("name") or code).strip()
                    props["code"] = code or name or "UNK"
                    # Format vegetation names as "Category *"
                    category_name = name or code or "Unknown"
                    props["name"] = f"Category {category_name}"
                veg_clipped = prepare_clipped_shapes(parcel_fc, veg_fc)
                
            if payload.simplify_tolerance and payload.simplify_tolerance > 0:
                def _simp(data):
                    out = []
                    for geom4326, code, name, area_ha in data:
                        g2 = geom4326.simplify(payload.simplify_tolerance, preserve_topology=True)
                        if not g2.is_empty: out.append((g2, code, name, area_ha))
                    return out or data
                lt_clipped = _simp(lt_clipped)
                if veg_clipped: veg_clipped = _simp(veg_clipped)

            kml = _render_parcel_kml(lp, lt_clipped, veg_clipped, bore_points)

            tmpdir = tempfile.mkdtemp(prefix="kmz_"); out_path = os.path.join(tmpdir, f"{lp}_landtypes.kmz")
            write_kmz(kml, out_path, assets=bore_assets); data = open(out_path, "rb").read(); os.remove(out_path); os.rmdir(tmpdir)
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
                bore_fc = fetch_bores_intersecting_envelope(env)
                bore_points, bore_assets = _prepare_bore_placemarks(parcel_union, bore_fc)
                if clipped:
                    if payload.format in (FormatEnum.tiff, FormatEnum.both):
                        tmpdir = tempfile.mkdtemp(prefix="tif_"); path = os.path.join(tmpdir, f"{lp}_landtypes.tif")
                        _ = make_geotiff_rgba(clipped, path, max_px=payload.max_px)
                        zf.writestr(f"{(prefix+'_') if prefix else ''}{lp}_landtypes.tif", open(path,"rb").read())
                        try: os.remove(path); os.rmdir(tmpdir)
                        except Exception: pass
                        row["status_tiff"]="ok"; row["file_tiff"]=f"{(prefix+'_') if prefix else ''}{lp}_landtypes.tif"
                    if payload.format in (FormatEnum.kmz, FormatEnum.both):
                        if payload.simplify_tolerance and payload.simplify_tolerance > 0:
                            simplified = []
                            for geom4326, code, name, area_ha in clipped:
                                g2 = geom4326.simplify(payload.simplify_tolerance, preserve_topology=True)
                                if not g2.is_empty: simplified.append((g2, code, name, area_ha))
                            clipped = simplified or clipped
                        kml = _render_parcel_kml(lp, clipped, [], bore_points)
                        kmz_data = _kmz_bytes(kml, bore_assets)
                        zf.writestr(f"{(prefix+'_') if prefix else ''}{lp}_landtypes.kmz", kmz_data)
                        row["status_kmz"]="ok"; row["file_kmz"]=f"{(prefix+'_') if prefix else ''}{lp}_landtypes.kmz"
                else:
                    row["status_tiff"]="skip"; row["status_kmz"]="skip"; row["message"]="No Land Types intersect."
            except Exception as e:
                row["lt_error"]=str(e)

            # Vegetation (always included now)
            try:
                veg_fc = fetch_features_intersecting_envelope(veg_url, veg_layer, env, out_sr=4326, out_fields="*")
                feats = veg_fc.get("features", [])
                for f in feats:
                    p = f.get("properties") or {}
                    code = (p.get(veg_code) if veg_code else "") or (p.get(veg_name) or "UNK")
                    name = p.get(veg_name) or code
                    p["code"] = str(code)
                    # Format vegetation names as "Category *"
                    category_name = str(name)
                    p["name"] = f"Category {category_name}"
                    f["properties"] = p
                veg_fc["features"] = feats
                vclipped = prepare_clipped_shapes(parcel_fc, veg_fc)
                if vclipped:
                    if payload.include_veg_tiff:
                        tmpdir = tempfile.mkdtemp(prefix="tif_"); path = os.path.join(tmpdir, f"{lp}_vegetation.tif")
                        _ = make_geotiff_rgba(vclipped, path, max_px=payload.max_px)
                        zf.writestr(f"{(prefix+'_') if prefix else ''}{lp}_vegetation.tif", open(path,"rb").read())
                        try: os.remove(path); os.rmdir(tmpdir)
                        except Exception: pass
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
