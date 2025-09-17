# LandType Agent Guide

## Project overview
- **App focus:** Parcel land-type explorer for Queensland cadastre lots with map preview, ArcGIS overlays (including the groundwater-bore layer), and GeoTIFF/KML/KMZ exports (including merged multi-lot layers) to support offline use in tools like Google Earth.
- **Primary stack:** FastAPI backend with ArcGIS REST queries (see `app/arcgis.py`), raster processing helpers for exports, and a Leaflet-based frontend served from Jinja templates.
- **Overlay assets:** KML/KMZ exports share icon styling assets that live alongside helper modules (see `app/kml.py` and groundwater-bore icon definitions).
- **Domain helpers:** Groundwater-bore logic lives in `app/bores.py` with helpers like `fetch_bores_intersecting_envelope` for querying ArcGIS services.
- **Environment/setup:**
  - Python 3.11 recommended; create a venv and install dependencies via `pip install -r requirements.txt`.
  - No environment variables are required in the default configuration because service URLs are hard-coded in `app/config.py`.
  - Run locally with `uvicorn app.main:app --host 0.0.0.0 --port 8000` and open http://localhost:8000.
  - When iterating on the bore overlay, ensure the external ArcGIS groundwater bore service remains reachable (tests rely on recorded responses; regenerate fixtures if endpoints change).

## Tests and quality checks
- Execute the automated suite with `pytest` (covers export helpers and API behavior).
- Run `ruff check .` and `mypy .` when touching Python code to maintain linting/static-analysis expectations.
- Ensure all commands complete successfully before committing; commits should contain logically grouped, tested changes with clear messages.

## Key code locations
- **Backend entrypoint:** `app/main.py` wires FastAPI routes for parcel lookup, raster/KML exports, and merged bundle downloads.
- **ArcGIS client:** `app/arcgis.py` centralizes REST query builders and pagination helpers for Queensland map services.
- **KML/KMZ generation:** `app/kml.py` transforms feature collections into styled KML documents used in both single and bulk exports.
- **Frontend map UI:** `app/templates/index.html` hosts the Leaflet map, parcel search form, and client-side scripting for calling the API and handling downloads.
- **Groundwater bores:** `app/bores.py` encapsulates bore queries and icon styling—run `pytest tests/test_bores.py` and the KMZ smoke tests when touching this layer to verify both API behavior and export rendering.

