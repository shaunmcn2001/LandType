# LandType (Hard-coded QLD)

FastAPI app for exporting Queensland **Land Types** (and optional **Vegetation**) as GeoTIFF & KMZ.
This build bakes in real QLD MapServer endpoints in `app/config.py` so it works without env vars.

## Features

### Bulk KMZ Export with Merged Layers
When exporting multiple properties as KMZ, the system now includes:
- **Merged Land Types (All Properties)** - All land type polygons with the same name/code merged across all properties
- **Merged Vegetation (All Properties)** - All vegetation polygons with the same name/code merged across all properties
- Individual property folders with their specific land types and vegetation

This enables users to:
- Measure combined areas across multiple properties in Google Earth
- View property overviews without MultiPolygon complexity
- Analyze patterns across multiple lots

## Run
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Open http://localhost:8000

## Deploy on Render
- Add a `Procfile` (included): `web: uvicorn app.main:app --host 0.0.0.0 --port 8000`
- If rasterio wheels fail on Render's native, use a Docker image with GDAL/rasterio preinstalled or temporarily use KMZ-only.

## Endpoints
- `/` — UI
- `/vector?lotplan=13SP181800` — Parcel + Land Types GeoJSON
- `/export?lotplan=...&download=true` — Single Land Types GeoTIFF
- `/export_kmz?lotplan=...` — Single Land Types KMZ
- `/export_kml?lotplan=...&include_veg=true` — Land Types KML, optionally with Vegetation
- `POST /export/any` — Single/bulk, with optional vegetation, emits ZIP when multiple/combined.
