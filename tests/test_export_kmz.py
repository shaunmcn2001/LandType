import io
import sys
import zipfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from shapely.geometry import Point, Polygon, mapping

sys.path.append(str(Path(__file__).resolve().parents[1]))
import app.main as main  # noqa: E402
from app.config import (  # noqa: E402
    BORE_NUMBER_FIELD,
    BORE_STATUS_CODE_FIELD,
    BORE_TYPE_CODE_FIELD,
)


@pytest.mark.integration
def test_export_kmz_includes_bore_icons(monkeypatch):
    polygon = Polygon([(0, 0), (0, 1), (1, 1), (1, 0)])
    parcel_fc = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": mapping(polygon),
                "properties": {"lotplan": "1TEST"},
            }
        ],
    }

    monkeypatch.setattr(main, "fetch_parcel_geojson", lambda lp: parcel_fc)
    monkeypatch.setattr(main, "to_shapely_union", lambda fc: polygon)
    monkeypatch.setattr(main, "bbox_3857", lambda geom: (0, 0, 1, 1))

    def fake_prepare_clipped_shapes(parcel, features):
        return [(polygon, "LT1", "Test Land Type", 1.0)]

    monkeypatch.setattr(main, "prepare_clipped_shapes", fake_prepare_clipped_shapes)
    monkeypatch.setattr(
        main,
        "fetch_landtypes_intersecting_envelope",
        lambda env: {"type": "FeatureCollection", "features": []},
    )

    bore_point = Point(0.5, 0.5)
    bore_fc = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": mapping(bore_point),
                "properties": {
                    BORE_NUMBER_FIELD: "RN123",
                    BORE_STATUS_CODE_FIELD: "EX",
                    BORE_TYPE_CODE_FIELD: "AB",
                },
            }
        ],
    }

    monkeypatch.setattr(main, "fetch_bores_intersecting_envelope", lambda env: bore_fc)

    client = TestClient(main.app)
    response = client.get("/export_kmz", params={"lotplan": "1TEST", "veg_url": ""})

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/vnd.google-earth.kmz")

    with zipfile.ZipFile(io.BytesIO(response.content)) as kmz:
        names = kmz.namelist()
        assert "doc.kml" in names
        icon_entries = [name for name in names if name.startswith("icons/")]
        assert icon_entries, "expected bore icon assets in KMZ archive"
        for name in icon_entries:
            data = kmz.read(name)
            assert data, f"KMZ asset {name} is empty"
