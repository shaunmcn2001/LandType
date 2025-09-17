# LandType Agent Guide

## Project overview
- **App focus:** Parcel land-type explorer for Queensland cadastre lots with map preview, ArcGIS overlays, and GeoTIFF/KML/KMZ exports (including merged multi-lot layers) to support offline use in tools like Google Earth.
- **Primary stack:** FastAPI backend with ArcGIS REST queries (see `app/arcgis.py`), raster processing helpers for exports, and a Leaflet-based frontend served from Jinja templates.
- **Environment/setup:**
  - Python 3.11 recommended; create a venv and install dependencies via `pip install -r requirements.txt`.
  - No environment variables are required in the default configuration because service URLs are hard-coded in `app/config.py`.
  - Run locally with `uvicorn app.main:app --host 0.0.0.0 --port 8000` and open http://localhost:8000.

## Tests and quality checks
- Execute the automated suite with `pytest` (covers export helpers and API behavior).
- Run `ruff check .` and `mypy .` when touching Python code to maintain linting/static-analysis expectations.
- Ensure all commands complete successfully before committing; commits should contain logically grouped, tested changes with clear messages.

## Key code locations
- **Backend entrypoint:** `app/main.py` wires FastAPI routes for parcel lookup, raster/KML exports, and merged bundle downloads.
- **ArcGIS client:** `app/arcgis.py` centralizes REST query builders and pagination helpers for Queensland map services.
- **KML/KMZ generation:** `app/kml.py` transforms feature collections into styled KML documents used in both single and bulk exports.
- **Frontend map UI:** `app/templates/index.html` hosts the Leaflet map, parcel search form, and client-side scripting for calling the API and handling downloads.

