# QLD Land Types → GeoTIFF (Google Earth-ready)

This service accepts a **Queensland Lot/Plan** (e.g. `13SP181800`), fetches the corresponding **parcel** from the DCDB, intersects it with **Land Types** (GLM/ILT) polygons, and returns a **georeferenced RGBA GeoTIFF** (EPSG:4326) suitable for use in **Google Earth / Google Earth Pro** (Image Overlay).

## Quick start (local)

```bash
git clone https://github.com/<your-username>/qld-landtype-geotiff.git
cd qld-landtype-geotiff
docker build -t qld-landtype-geotiff .
docker run -p 8000:8000 qld-landtype-geotiff
# open http://localhost:8000/health
```

### Export a GeoTIFF
```bash
# Download a GeoTIFF for Lot/Plan 13SP181800 (returns a file)
curl -L "http://localhost:8000/export?lotplan=13SP181800&max_px=4096&download=true" -o 13SP181800_landtypes.tif
```

### JSON only (bounds + legend)
```bash
curl "http://localhost:8000/export?lotplan=13SP181800&download=false"
```

## Deploy on Render

1. Push this repo to GitHub.
2. In Render, **New > Web Service**, pick your repo.
3. Environment: **Docker** (Render reads `render.yaml`).
4. Click **Create Web Service**.

Render will build the Docker image and start the app on port 8000.

## How it works

- **Parcel**: Queried from `LandParcelPropertyFramework / Cadastral parcels (Layer 4)` by `lotplan`.
- **Land Types**: Queried from `Environment / LandTypes (Layer 1)` by parcel envelope (3857) and clipped in code to exact parcel boundary.
- **Rasterize**: Clipped polygons reprojected to EPSG:4326 and burned to an **RGBA GeoTIFF** (transparent background). Colors per land type are generated deterministically from `LT_CODE_1` so maps are stable across runs.

## Endpoints

- `GET /health` → `{ "ok": true }`
- `GET /export?lotplan=13SP181800&max_px=4096&download=true`
  - `max_px`: Max raster dimension (default 4096; reduce if the parcel is huge).
  - `download`: If `false`, returns JSON summary (legend + bounds) instead of the TIFF.

## Data sources (QLD Government)

- **Cadastral parcels (DCDB)**: PlanningCadastre / LandParcelPropertyFramework (Layer 4).  
  Service root: https://spatial-gis.information.qld.gov.au/arcgis/rest/services/PlanningCadastre/LandParcelPropertyFramework/MapServer  
- **Land Types (GLM / ILT)**: Environment / LandTypes (Layer 1).  
  Service root: https://spatial-gis.information.qld.gov.au/arcgis/rest/services/Environment/LandTypes/MapServer

## Notes

- Output colors are deterministic per `LT_CODE_1` (not the QGov renderer colors).
- If a parcel returns **no** Land Types (rare), the API responds `404`.
- This app does not cache. For heavy use, consider adding result caching and a persistent volume.

## License & Attribution

- Code: MIT (yours to choose).
- Data © State of Queensland (Department of Resources / DAF). Check terms for reuse/attribution.
